#!/usr/bin/python

"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.

This module provides API for distributing generator functions and
their dependencies (files, Python functions, classes) to create remote
coroutines (for distributed programming/computing). Note that all
coroutines execute in the same thread and run on same processor. If
CPU intensive computations are to be run on systems with multiple
processors, then this program should be run with multiple instances
(see below for '-c' option to this program). This program, however,
doesn't schedule jobs - client needs to schedule them to use
processors effectively. Alternately, dispy project
(http://dispy.sourceforge.net) could be used for compute intensive
tasks.

See 'discoro_client.py' for an example.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2014 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

import os
import sys
import inspect
import traceback
import functools

import asyncoro.disasyncoro as asyncoro

__all__ = ['Computation', 'discoro_server']

class Computation(object):
    """Code fragments to distribute to remote asyncoro schedulers to
    create (remote) coroutines.
    """

    _asyncoro = None

    def __init__(self, computation, depends=[], cleanup=True):
        """'computation' must be generator function; coroutine is
        created at remote asyncoro with this method. 'depends' is a
        list of code fragments that are needed by 'computation'; each
        element in this list should be either a module, a function,
        path name of a file, a Class or an object (in which case the
        code for its class is sent).
        """
        if Computation._asyncoro is None:
            Computation._asyncoro = asyncoro.AsynCoro.instance()
        if inspect.isgeneratorfunction(computation):
            func = computation
            if sys.version_info.major >= 3:
                self._name = func.__name__
            else:
                self._name = func.func_name
            self._code = inspect.getsource(func).lstrip()
        else:
            raise Exception('Invalid computation; must be generator')

        self._cleanup = cleanup
        depend_ids = set()
        self._xfer_files = []
        for dep in depends:
            if isinstance(dep, str) or inspect.ismodule(dep):
                if inspect.ismodule(dep):
                    dep = dep.__file__
                    if dep.endswith('.pyc'):
                        dep = dep[:-1]
                    if not (dep.endswith('.py') and os.path.isfile(dep)):
                        raise Exception('Invalid module "%s" - must be python source.' % dep)
                if dep in depend_ids:
                    continue
                try:
                    fd = open(dep, 'rb')
                    fd.close()
                    self._xfer_files.append(dep)
                    depend_ids.add(dep)
                except:
                    raise Exception('File "%s" is not valid' % dep)
            elif inspect.isfunction(dep) or inspect.isclass(dep) or hasattr(dep, '__class__'):
                if inspect.isfunction(dep) or inspect.isclass(dep):
                    pass
                elif hasattr(dep, '__class__') and inspect.isclass(dep.__class__):
                    dep = dep.__class__
                if id(dep) in depend_ids:
                    continue
                self._code += '\n' + inspect.getsource(dep).lstrip()
                depend_ids.add(id(dep))
            else:
                self._code = None
                raise Exception('Invalid function: %s' % dep)
        # make sure code can be compiled
        compile(self._code, '<string>', 'exec')

    def setup(self, peer, timeout=None, func=None, args=(), kwargs={}):
        """Sends computation to peer. This method should be called
        once for each peer. Gives 0 if the setup is successful.

        Must be used with yield as 'v = yield computation.setup(peer)'
        """
        if not self._code:
            raise StopIteration(-1)
        def _setup(self, peer, timeout, func, name, args, kwargs, coro=None):
            peer.send({'cmd':'setup', 'client':coro, 'computation':self, 'func':func,
                       'name':name, 'args':args, 'kwargs':kwargs})
            reply = yield coro.receive(timeout=timeout)
            if reply != 0:
                raise StopIteration(-1)
            for i, xf in enumerate(self._xfer_files):
                reply = yield Computation._asyncoro.send_file(peer.location, xf, timeout=timeout)
                if reply < 0:
                    asyncoro.logger.debug('failed to transfer file %s: %s' % (xf, reply))
                    self._xfer_files = self._xfer_files[:i]
                    yield self.close(peer)
                    raise StopIteration(-1)
            raise StopIteration(0)
        name = None
        if func:
            if inspect.isfunction(func):
                if sys.version_info.major >= 3:
                    name = func.__name__
                else:
                    name = func.func_name
                func = inspect.getsource(func).lstrip()
            else:
                logger.warning('Invalid setup function ignored')
                func = None
                args = ()
                kwargs = {}

        coro = asyncoro.Coro(_setup, self, peer, timeout, func, name, args, kwargs)
        yield coro.finish()

    def run(self, peer, *args, **kwargs):
        """Once 'setup', the computation can be run as many times as
        necessary. Each 'run' creates a coroutine at peer and returns
        reference to that remote coroutine if successful; otherwise,
        gives -1.

        Must be used with yield as 'rcoro = yield computation.run(peer, ...)'
        """
        def _run(self, peer, coro=None):
            peer.send({'cmd':'run', 'client':coro, 'computation':self._name,
                       'args':args, 'kwargs':kwargs})
            yield coro.receive(timeout=5)
        coro = asyncoro.Coro(_run, self, peer)
        yield coro.finish()

    def close(self, peer, func=None, args=(), kwargs={}):
        """Removes computation at peer.

        Must be used with yield as 'yield computation.close(peer)'
        """
        msg = {'cmd':'close', 'computation':self._name}
        if func:
            if inspect.isfunction(func):
                if sys.version_info.major >= 3:
                    name = func.__name__
                else:
                    name = func.func_name
                func = inspect.getsource(func).lstrip()
                msg['name'] = name
                msg['args'] = args
                msg['kwargs'] = kwargs
            else:
                logger.warning('invalid close function ignored')
                func = None
        msg['func'] = func
        peer.send(msg)
        if self._cleanup:
            for xf in self._xfer_files:
                yield Computation._asyncoro.del_file(peer.location, xf)

    def ping(self, peer, timeout=2):
        """Check if peer is reachable/alive. Gives 0 if the peer is
        alive.

        Must be used with yield as 'v = yield computation.ping(peer)'
        """
        if (yield peer.deliver({'cmd':'ping'}, timeout=timeout)) == 1:
            raise StopIteration(0)
        raise StopIteration(-1)

def discoro_server(_discoro_name='discoro_server'):
    """Server that receives computations and runs coroutines for it.
    """

    _discoro_coro = asyncoro.AsynCoro.cur_coro()
    _discoro_coro.register(_discoro_name)
    asyncoro.logger.debug('discoro %s started' % _discoro_coro)
    # os.chdir(asyncoro.AsynCoro.instance().dest_path)
    _discoro_computations = {}
    _discoro_globals = _discoro_locals = _discoro_cmd = _discoro_client = None
    _discoro_msg = _discoro_computation = _discoro_job_coro = None
    _discoro_job_coros = set()
    _discoro_globals = list(globals().keys())
    _discoro_locals = list(locals().keys())
    _discoro_globals.extend(_discoro_locals)

    while True:
        _discoro_msg = yield _discoro_coro.receive()
        try:
            _discoro_cmd = _discoro_msg['cmd']
        except:
            if isinstance(_discoro_msg, asyncoro.MonitorException):
                asyncoro.logger.debug('job %s done' % _discoro_msg.args[0])
                _discoro_job_coros.discard(_discoro_msg.args[0])
                continue
            else:
                asyncoro.logger.debug(traceback.format_exc())
                _discoro_cmd = None
        if _discoro_cmd == 'run':
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, asyncoro.Coro):
                continue
            try:
                _discoro_computation = _discoro_computations[_discoro_msg['computation']]
                job_coro = asyncoro.Coro(globals()[_discoro_computation._name],
                                         *_discoro_msg['args'], **_discoro_msg['kwargs'])
            except:
                asyncoro.logger.debug('invalid computation to run')
                asyncoro.logger.debug(traceback.format_exc())
                job_coro = -1
            else:
                asyncoro.logger.debug('job %s created' % job_coro)
                _discoro_job_coros.add(job_coro)
                if (yield _discoro_coro.monitor(job_coro)) != 0:
                    _discoro_job_coros.discard(job_coro)
            _discoro_client.send(job_coro)
            del job_coro
        elif _discoro_cmd == 'ping':
            # TODO: maintain heartbeat times and remove computations from dead clients?
            pass
        elif _discoro_cmd == 'setup':
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, asyncoro.Coro):
                continue
            try:
                _discoro_computation = _discoro_msg['computation']
                exec(compile(_discoro_computation._code, '<string>', 'exec'))
                globals().update(locals())
                if _discoro_msg['func']:
                    try:
                        exec(compile(_discoro_msg['func'], '<string>', 'exec'))
                        locals()[_discoro_msg['name']](*_discoro_msg['args'], **_discoro_msg['kwargs'])
                    except:
                        logger.warning('setup function failed: %s' % traceback.format_exc())
            except:
                asyncoro.logger.warning('invalid computation')
                asyncoro.logger.debug(traceback.format_exc())
                _discoro_client.send(-1)
                continue
            asyncoro.logger.debug('computation "%s" from "%s"' % \
                                  (_discoro_computation._name, _discoro_msg['client']))
            setattr(_discoro_computation, '_client', _discoro_client)
            setattr(_discoro_computation, '_globals',
                    [_discoro_var for _discoro_var in globals().keys() \
                     if _discoro_var not in _discoro_globals])
            setattr(_discoro_computation, '_locals',
                    [_discoro_var for _discoro_var in locals().keys() \
                     if _discoro_var not in _discoro_locals])
            _discoro_globals.extend(_discoro_computation._globals)
            _discoro_locals.extend(_discoro_computation._locals)
            _discoro_computations[_discoro_computation._name] = _discoro_computation
            _discoro_client.send(0)
        elif _discoro_cmd == 'close':
            try:
                _discoro_computation = _discoro_computations.pop(_discoro_msg['computation'], None)
                asyncoro.logger.debug('deleting computation "%s"' % _discoro_computation._name)
                if _discoro_msg['func']:
                    try:
                        exec(compile(_discoro_msg['func'], '<string>', 'exec'))
                        locals()[_discoro_msg['name']](*_discoro_msg['args'], **_discoro_msg['kwargs'])
                    except:
                        logger.warning('cleanup function failed: %s' % traceback.format_exc())
                for _discoro_var in _discoro_computation._globals:
                    globals().pop(_discoro_var, None)
                for _discoro_var in _discoro_computation._locals:
                    locals().pop(_discoro_var, None)
            except:
                asyncoro.logger.warning('invalid computation "%s" to delete' % \
                                        _discoro_msg.get('computation', None))
        elif _discoro_cmd == 'quit':
            break
        else:
            asyncoro.logger.warning('invalid command "%s" ignored' % _discoro_cmd)
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, asyncoro.Coro):
                continue
            _discoro_client.send(-1)

    # wait until all computations are done; process only 'ping' and 'close'
    while _discoro_job_coros:
        _discoro_msg = yield _discoro_coro.receive()
        try:
            _discoro_cmd = _discoro_msg['cmd']
        except:
            if isinstance(_discoro_msg, asyncoro.MonitorException):
                asyncoro.logger.debug('job %s done' % _discoro_msg.args[0])
                _discoro_job_coros.discard(_discoro_msg.args[0])
                continue
            else:
                asyncoro.logger.debug(traceback.format_exc())
            _discoro_cmd = None
        if _discoro_cmd == 'ping':
            # TODO: maintain heartbeat times and remove computations from dead clients?
            pass
        elif _discoro_cmd == 'close':
            try:
                _discoro_computation = _discoro_computations.pop(_discoro_msg['computation'], None)
                asyncoro.logger.debug('deleting computation "%s"' % _discoro_computation._name)
                for _discoro_var in _discoro_computation._globals:
                    globals().pop(_discoro_var, None)
                for _discoro_var in _discoro_computation._locals:
                    locals().pop(_discoro_var, None)
                if not _discoro_computations:
                    break
            except:
                asyncoro.logger.warning('invalid computation to delete')
        else:
            asyncoro.logger.warning('invalid command "%s" ignored' % _discoro_cmd)
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, asyncoro.Coro):
                continue
            _discoro_client.send(-1)

    for _discoro_computation in _discoro_computations.values():
        asyncoro.logger.debug('deleting computation "%s"' % _discoro_computation._name)
        for _discoro_var in _discoro_computation._globals:
            globals().pop(_discoro_var, None)
        for _discoro_var in _discoro_computation._locals:
            locals().pop(_discoro_var, None)

    for _discoro_var in _discoro_locals:
        if _discoro_var == '_discoro_locals':
            continue
        globals().pop(_discoro_var, None)
    globals().pop('_discoro_locals', None)

def _discoro_process(_discoro_config, _discoro_queue):
    _discoro_scheduler = asyncoro.AsynCoro(**_discoro_config)
    _discoro_coro = asyncoro.Coro(discoro_server)
    _discoro_queue.put(asyncoro.serialize(_discoro_coro))
    _discoro_scheduler.finish()

if __name__ == '__main__':

    """
    If '-c' option is used with a positive number, discoro server is
    run that many instances, so CPU intesive coroutines can be invoked
    on them. If the number is negative, that many processors are not
    used (from the available processors). The default value for this
    option is '0', in which case all the available processors are
    used.

    '-n' option can be used to specify prefix name for asyncoro
    schedulers. This name is appended with hyphen followed by a unique
    number when AsynCoro is created. Note that the names in a cluster
    must be unique; otherwise, 'locate' may give inconsistent results.

    If '-d' option is used, debug logging is enabled.

    Remaining options are as per AsynCoro in disasyncoro module.
    """

    import logging, argparse, multiprocessing, time
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cpus', dest='cpus', type=int, default=0,
                        help='number of CPUs/discoro instances to run; ' \
                        'if negative, that many CPUs are not used')
    parser.add_argument('-i', '--ip_addr', dest='node', default=None,
                        help='IP address or host name of this node')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addr', default=None,
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('-u', '--udp_port', dest='udp_port', type=int, default=51350,
                        help='UDP port number to use')
    parser.add_argument('-n', '--name', dest='name', default='discoro_server',
                        help='(symbolic) name given to AsynCoro schdulers on this node')
    parser.add_argument('--dest_path', dest='dest_path', default=None,
                        help='path where files sent by peers are stored')
    parser.add_argument('--max_file_size', dest='max_file_size', default=None, type=int,
                        help='maximum file size of any file transferred')
    parser.add_argument('-s', '--secret', dest='secret', default='',
                        help='authentication secret for handshake with peers')
    parser.add_argument('--certfile', dest='certfile', default=None,
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default=None,
                        help='file containing SSL key')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    _discoro_config = vars(parser.parse_args(sys.argv[1:]))
    del parser

    if _discoro_config['loglevel']:
        asyncoro.logger.setLevel(logging.DEBUG)
    else:
        asyncoro.logger.setLevel(logging.INFO)
    del _discoro_config['loglevel']

    _discoro_cpus = multiprocessing.cpu_count()
    if _discoro_config['cpus'] > 0:
        if _discoro_config['cpus'] > _discoro_cpus:
            raise Exception('CPU count must be <= %s' % _discoro_cpus)
        _discoro_cpus = _discoro_config['cpus']
    elif _discoro_config['cpus'] < 0:
        if -_discoro_config['cpus'] >= _discoro_cpus:
            raise Exception('CPU count must be > -%s' % _discoro_cpus)
        _discoro_cpus += _discoro_config['cpus']
    del _discoro_config['cpus']

    _discoro_name = _discoro_config['name']
    _discoro_queue = multiprocessing.Queue()
    _discoro_processes = []
    for _discoro_proc_id in range(2, _discoro_cpus + 1):
        _discoro_config['name'] = _discoro_name + '-%s' % _discoro_proc_id
        _discoro_processes.append(multiprocessing.Process(target=_discoro_process,
                                                          args=(_discoro_config, _discoro_queue)))
        _discoro_processes[-1].start()
        time.sleep(0.05)

    _discoro_proc_id = 1
    _discoro_config['name'] = _discoro_name + '-%s' % _discoro_proc_id
    _discoro_scheduler = asyncoro.AsynCoro(**_discoro_config)
    _discoro_coro = asyncoro.Coro(discoro_server)

    while True:
        try:
            if sys.stdin.readline().strip().lower() in ('quit', 'exit'):
                break
        except KeyboardInterrupt:
            break

    asyncoro.logger.debug('terminating servers')
    while not _discoro_queue.empty():
        asyncoro.unserialize(_discoro_queue.get()).send({'cmd':'quit'})
    _discoro_coro.send({'cmd':'quit'})

    for _i, _discoro_proc in enumerate(_discoro_processes, start=1):
        if _discoro_proc.is_alive():
            asyncoro.logger.info('  -- waiting for process %s to finish' % _i)
            _discoro_proc.join()
