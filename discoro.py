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

if sys.version_info.major >= 3:
    import disasyncoro3 as asyncoro
else:
    import disasyncoro as asyncoro

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
            raise Exception('Invalid computation; must be generator: %s' % (computation.func_name))

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

    def setup(self, peer, timeout=None):
        """Sends computation to peer. This method should be called
        once for each peer. Gives 0 if the setup is successful.

        Must be used with yield as 'v = yield computation.setup(peer)'
        """
        if not self._code:
            raise StopIteration(-1)
        def _setup(self, peer, timeout, coro=None):
            peer.send({'cmd':'setup', 'client':coro, 'computation':self})
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
        coro = asyncoro.Coro(_setup, self, peer, timeout)
        yield coro.finish()

    def run(self, peer, *args, **kwargs):
        """Once 'setup', the computation can be run as many times as
        necessary. Each 'run' creates a coroutine at peer and returns
        reference to that remote coroutine if successful; otherwise,
        gives None.

        Must be used with yield as 'rcoro = yield computation.run(peer, ...)'
        """
        def _run(self, peer, coro=None):
            peer.send({'cmd':'run', 'client':coro, 'computation':self._name,
                       'args':args, 'kwargs':kwargs})
            yield coro.receive(timeout=5)
        coro = asyncoro.Coro(_run, self, peer)
        yield coro.finish()

    def close(self, peer):
        """Removes computation at peer.

        Must be used with yield as 'yield computation.close(peer)'
        """
        peer.send({'cmd':'close', 'computation':self._name})
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
    # os.chdir(asyncoro.AsynCoro.instance().dest_path_prefix)
    _discoro_computations = {}
    _discoro_computation_vars = {}
    _discoro_globalvars = _discoro_localvars = _discoro_cmd = _discoro_client = None
    _discoro_msg = _discoro_computation = _discoro_job_coro = None
    _discoro_globalvars = list(globals().keys())
    _discoro_localvars = list(locals().keys())
    _discoro_globalvars.extend(_discoro_localvars)

    while True:
        _discoro_msg = yield _discoro_coro.receive()
        try:
            _discoro_cmd = _discoro_msg['cmd']
        except:
            asyncoro.logger.debug(traceback.format_exc())
            continue
        if _discoro_cmd == 'run':
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, asyncoro.Coro):
                continue
            try:
                _discoro_computation = _discoro_computations[_discoro_msg['computation']]
                _discoro_job_coro = asyncoro.Coro(globals()[_discoro_computation._name],
                                                  *_discoro_msg['args'], **_discoro_msg['kwargs'])
            except:
                asyncoro.logger.debug('invalid computation to run')
                asyncoro.logger.debug(traceback.format_exc())
                _discoro_job_coro = None
            _discoro_client.send(_discoro_job_coro)
        elif _discoro_cmd == 'ping':
            # TODO: maintain heartbeat times and remove computations from dead clients?
            pass
        elif _discoro_cmd == 'setup':
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, asyncoro.Coro):
                continue
            try:
                _discoro_computation = _discoro_msg['computation']
                _discoro_computation._code = compile(_discoro_computation._code, '<string>', 'exec')
                exec(_discoro_computation._code)
                globals().update(locals())
            except:
                asyncoro.logger.warning('invalid computation')
                asyncoro.logger.debug(traceback.format_exc())
                _discoro_client.send(-1)
                continue
            asyncoro.logger.debug('computation "%s" from "%s"' % \
                                  (_discoro_computation._name, _discoro_msg['client']))
            _discoro_computations[_discoro_computation._name] = _discoro_computation
            _discoro_gvars = [_discoro_var for _discoro_var in globals().keys() \
                              if _discoro_var not in _discoro_globalvars]
            _discoro_lvars = [_discoro_var for _discoro_var in locals().keys() \
                              if _discoro_var not in _discoro_localvars]
            _discoro_computation_vars[_discoro_computation._name] = (_discoro_gvars, _discoro_lvars)
            _discoro_globalvars.extend(_discoro_gvars)
            _discoro_localvars.extend(_discoro_lvars)
            _discoro_client.send(0)
        elif _discoro_cmd == 'close':
            try:
                _discoro_computation = _discoro_computations.pop(_discoro_msg['computation'], None)
                asyncoro.logger.debug('deleting computation "%s"' % _discoro_computation._name)
                _discoro_gvars, _discoro_lvars = \
                                _discoro_computation_vars.pop(_discoro_computation._name, ([], []))
                for _discoro_var in _discoro_gvars:
                    globals().pop(_discoro_var, None)
                for _discoro_var in _discoro_lvars:
                    locals().pop(_discoro_var, None)
            except:
                asyncoro.logger.warning('invalid computation to delete')
                _discoro_client = _discoro_msg.get('client', None)
                if not isinstance(_discoro_client, asyncoro.Coro):
                    continue
                _discoro_client.send(0)
        elif _discoro_cmd == 'quit':
            break
        else:
            asyncoro.logger.warning('invalid command "%s" ignored' % _discoro_cmd)

    # wait until all computations are done; process only 'ping' and 'close'
    _discoro_coro.set_daemon()
    while True:
        _discoro_msg = yield _discoro_coro.receive()
        try:
            _discoro_cmd = _discoro_msg['cmd']
        except:
            asyncoro.logger.debug(traceback.format_exc())
            continue
        if _discoro_cmd == 'ping':
            # TODO: maintain heartbeat times and remove computations from dead clients?
            pass
        elif _discoro_cmd == 'close':
            try:
                _discoro_computation = _discoro_computations.pop(_discoro_msg['computation'], None)
                asyncoro.logger.debug('deleting computation "%s"' % _discoro_computation._name)
                _discoro_gvars, _discoro_lvars = \
                                _discoro_computation_vars.pop(_discoro_computation._name, ([], []))
                for _discoro_var in _discoro_gvars:
                    globals().pop(_discoro_var, None)
                for _discoro_var in _discoro_lvars:
                    locals().pop(_discoro_var, None)
            except:
                asyncoro.logger.warning('invalid computation to delete')
        else:
            asyncoro.logger.warning('invalid command "%s" ignored' % _discoro_cmd)
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, asyncoro.Coro):
                continue
            _discoro_client.send(-1)

def _discoro_process(_discoro_config, _discoro_queue):
    _discoro_scheduler = asyncoro.AsynCoro(**_discoro_config)
    _discoro_coro = asyncoro.Coro(discoro_server)
    _discoro_queue.put(asyncoro.serialize(_discoro_coro))
    _discoro_scheduler.terminate(await_non_daemons=True)

if __name__ == '__main__':

    """
    If '-c' option is used with a positive number, discoro server is
    run that many instances, so CPU intesive coroutines can be invoked
    on them. If the number is negative, that many processors are not
    used (from the available processors). The default value for this
    option is '1', so only instance runs. If this value is given as
    '0', then all the available processors are used.

    If '-d' option is used, debug logging is enabled.

    Remaining options are similar to disasyncoro.
    """

    import logging, argparse, multiprocessing, time
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cpus', dest='cpus', type=int, default=1,
                        help='number of CPUs/discoro instances to run; ' \
                        'if negative, that many CPUs are not used')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('-n', '--node', dest='node', default=None,
                        help='node name or IP address')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addr', default=None,
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('-u', '--udp_port', dest='udp_port', type=int, default=51350,
                        help='UDP port number to use')
    parser.add_argument('--dest_path_prefix', dest='dest_path_prefix', default=None,
                        help='path prefix where files sent by peers are stored')
    parser.add_argument('--max_file_size', dest='max_file_size', default=None, type=int,
                        help='maximum file size of any file transferred')
    parser.add_argument('-s', '--secret', dest='secret', default='',
                        help='authentication secret for handshake with peers')
    parser.add_argument('--certfile', dest='certfile', default=None,
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default=None,
                        help='file containing SSL key')
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

    _discoro_queue = multiprocessing.Queue()
    _discoro_processes = []
    for _discoro_proc_id in range(1, _discoro_cpus):
        _discoro_processes.append(multiprocessing.Process(target=_discoro_process,
                                                          args=(_discoro_config, _discoro_queue)))
        _discoro_processes[-1].start()
        time.sleep(0.05)

    _discoro_proc_id = 0
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

    for _i, _discoro_proc in enumerate(_discoro_processes):
        if _discoro_proc.is_alive():
            asyncoro.logger.info('  -- waiting for process %s to finish' % _i)
            _discoro_proc.join()
