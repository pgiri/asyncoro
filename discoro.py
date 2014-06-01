"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.

This module provides API for distributing generator functions and
their dependencies to create remote coroutines (for distributed
programming). Note that all coroutines execute in the same thread, so
CPU intensive computations are not suitable with this framework.
dispy project (http://dispy.sourceforge.net) may be better choice for
such tasks; alternately, this 'discoro' program can be run multiple
instances on same machine (one instance per processor, for example)
and distribute computations to each of them, one computation per
discoro.
"""

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
        """'computation' must be generator method; coroutine is
        created at remote asyncoro with this method. 'depends' is a
        list of code fragments that are needed by 'computation'; each
        element in this list should be either module, function, path
        name of file, Class or object (in which case the code for the
        class is sent).
        """
        if Computation._asyncoro is None:
            Computation._asyncoro = asyncoro.AsynCoro.instance()
        if inspect.isgeneratorfunction(computation):
            func = computation
            if sys.version_info.major >= 3:
                self._name = func.__name__
            else:
                self._name = func.func_name
            lines = inspect.getsourcelines(func)[0]
            lines[0] = lines[0].lstrip()
            self._code = ''.join(lines)
        else:
            raise Exception('Invalid computation; must be generator: %s' % (computation.func_name))

        self._cleanup = cleanup
        depend_ids = {}
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
                    depend_ids[dep] = dep
                except:
                    raise Exception('File "%s" is not valid' % dep)
            elif inspect.isfunction(dep) or inspect.isclass(dep) or hasattr(dep, '__class__'):
                if inspect.isfunction(dep) or inspect.isclass(dep):
                    pass
                elif hasattr(dep, '__class__') and inspect.isclass(dep.__class__):
                    dep = dep.__class__
                if id(dep) in depend_ids:
                    continue
                lines = inspect.getsourcelines(dep)[0]
                lines[0] = lines[0].lstrip()
                self._code += '\n' + ''.join(lines)
                depend_ids[id(dep)] = id(dep)
            else:
                self._code = None
                raise Exception('Invalid function: %s' % dep)
        if self._code:
            # make sure code can be compiled
            code = compile(self._code, '<string>', 'exec')
            del code

    def setup(self, peer, timeout=None):
        """Sends computation fragment to peer ('discoro'
        coroutine). This method should be called once for each peer.
        """
        if not self._code:
            raise StopIteration(-1)
        def _setup(self, peer, timeout, coro=None):
            peer.send({'cmd':'setup', 'coro':coro, 'computation':self})
            reply = yield coro.receive(timeout=timeout)
            if reply != 0:
                raise StopIteration(-1)
            for i, xf in enumerate(self._xfer_files):
                reply = yield Computation._asyncoro.send_file(peer.location, xf, timeout=timeout)
                if reply != 0:
                    asyncoro.logger.debug('failed to transfer file %s' % xf)
                    self._xfer_files = self._xfer_files[:i]
                    self.close(peer)
                    raise StopIteration(-1)
            raise StopIteration(0)
        coro = asyncoro.Coro(_setup, self, peer, timeout)
        yield coro.finish()

    def run(self, peer, *args, **kwargs):
        """Once 'setup', the computation can be run as many times as
        necessary. Each 'run' creates a coroutine at peer and returns
        reference to that remote coroutine.
        """
        def _run(self, peer, coro=None):
            peer.send({'cmd':'run', 'coro':coro, 'computation':self._name,
                       'args':args, 'kwargs':kwargs})
            yield coro.receive(timeout=5)
        coro = asyncoro.Coro(_run, self, peer)
        yield coro.finish()

    def close(self, peer):
        """Removes computation at peer.
        """
        peer.send({'cmd':'close', 'computation':self._name})
        if self._cleanup:
            for xf in self._xfer_files:
                yield Computation._asyncoro.del_file(peer.location, xf)

def discoro_server(name='discoro_server'):
    """Server that receives computations and runs coroutines for it.
    """
    _discoro_coro = asyncoro.AsynCoro.cur_coro()
    _discoro_coro.register(name)
    # _discoro_coro.set_daemon()
    # os.chdir(asyncoro.AsynCoro.instance().dest_path_prefix)
    _discoro_computations = {}
    _discoro_computation_vars = {}
    _discoro_globalvars = _discoro_localvars = _discoro_cmd = _discoro_rcoro = None
    _discoro_msg = _discoro_computation = _discoro_func = _discoro_job_coro = None
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
            try:
                assert isinstance(_discoro_msg['coro'], asyncoro.Coro)
                _discoro_computation = _discoro_computations[_discoro_msg['computation']]
                _discoro_func = globals()[_discoro_computation._name]
                _discoro_job_coro = asyncoro.Coro(_discoro_func, *_discoro_msg['args'],
                                                  **_discoro_msg['kwargs'])
            except:
                asyncoro.logger.debug('invalid computation to run')
                asyncoro.logger.debug(traceback.format_exc())
                _discoro_msg['coro'].send(None)
                continue
            else:
                _discoro_msg['coro'].send(_discoro_job_coro)
        elif _discoro_cmd == 'ping':
            # _discoro_client = _discoro_msg['coro']
            # TODO: maintain heartbeat times and remove computations from dead clients?
            pass
        elif _discoro_cmd == 'setup':
            try:
                _discoro_rcoro = _discoro_msg['coro']
                assert isinstance(_discoro_rcoro, asyncoro.Coro)
                _discoro_computation = _discoro_msg['computation']
                _discoro_computation._code = compile(_discoro_computation._code, '<string>', 'exec')
                exec(_discoro_computation._code)
                globals().update(locals())
            except:
                asyncoro.logger.warning('invalid computation')
                asyncoro.logger.debug(traceback.format_exc())
                _discoro_rcoro.send(-1)
                continue
            asyncoro.logger.debug('computation "%s" from "%s"' % \
                                  (_discoro_computation._name, _discoro_msg['coro']))
            _discoro_computations[_discoro_computation._name] = _discoro_computation
            _discoro_gvars = [_discoro_var for _discoro_var in globals().keys() \
                              if _discoro_var not in _discoro_globalvars]
            _discoro_lvars = [_discoro_var for _discoro_var in locals().keys() \
                              if _discoro_var not in _discoro_localvars]
            _discoro_computation_vars[_discoro_computation._name] = (_discoro_gvars, _discoro_lvars)
            _discoro_globalvars.extend(_discoro_gvars)
            _discoro_localvars.extend(_discoro_lvars)
            _discoro_rcoro.send(0)
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
            asyncoro.logger.warning('invalid command ignored')

if __name__ == '__main__':
    import logging
    asyncoro.logger.setLevel(logging.DEBUG)
    scheduler = asyncoro.AsynCoro.instance()
    discoro = asyncoro.Coro(discoro_server)
    while True:
        try:
            cmd = sys.stdin.readline().strip().lower()
            if cmd == 'quit' or cmd == 'exit':
                break
        except KeyboardInterrupt:
            break
    # TODO: wait until all computations are done?
    discoro.terminate()
