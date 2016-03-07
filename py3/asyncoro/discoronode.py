#!/usr/bin/python3

"""This file is part of asyncoro; see http://asyncoro.sourceforge.net for
details.

This program can be used to start discoro server processes so discoro scheduler
(see 'discoro.py') can send computations to these server processes for executing
distributed communicating proceses (coroutines). All coroutines in a server
execute in the same thread, so multiple CPUs are not used by one server. If CPU
intensive computations are to be run on systems with multiple processors, then
this program should be run with multiple instances (see below for '-c' option to
this program).

See 'discoro_client*.py' and 'discomp*.py' files for example use cases.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2014 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"


def _discoro_proc():
    # coroutine
    """Server process receives computations and runs coroutines for it.
    """

    import os
    import shutil
    import traceback
    import sys
    import time

    try:
        import psutil
    except:
        psutil = None

    import asyncoro.disasyncoro as asyncoro
    from asyncoro import Coro
    from asyncoro.discoro import MinPulseInterval, MaxPulseInterval, \
         DiscoroNodeInfo, DiscoroNodeStatus

    _discoro_coro = asyncoro.AsynCoro.cur_coro()
    _discoro_config = yield _discoro_coro.receive()
    assert _discoro_config['req'] == 'config'
    _discoro_coro.register('discoro_server')
    _discoro_name = asyncoro.AsynCoro.instance().name
    asyncoro.AsynCoro.instance().dest_path = os.path.join('discoro',
                                                          'server%s' % (_discoro_config['id']))
    _discoro_dest_path = asyncoro.AsynCoro.instance().dest_path
    _discoro_pid_path = os.path.join(_discoro_dest_path, '..',
                                     'server%s.pid' % (_discoro_config['id']))
    _discoro_pid_path = os.path.normpath(_discoro_pid_path)
    # TODO: is file locking necessary?
    if os.path.exists(_discoro_pid_path):
        with open(_discoro_pid_path, 'r') as _discoro_req:
            _discoro_var = _discoro_req.read()
        _discoro_var = int(_discoro_var)
        if not _discoro_config['phoenix']:
            print('\n   Another discoronode seems to be running;\n'
                  '   make sure server with PID %d quit and remove "%s"\n' %
                  (_discoro_var, _discoro_pid_path))
            _discoro_var = os.getpid()

        import signal
        try:
            os.kill(_discoro_var, signal.SIGTERM)
        except:
            pass
        else:
            time.sleep(0.1)
            try:
                if os.waitpid(_discoro_var, os.WNOHANG)[0] != _discoro_var:
                    asyncoro.logger.warning('Killing process %d failed' % _discoro_var)
            except:
                pass
        del signal
    if os.path.isdir(_discoro_dest_path):
        shutil.rmtree(_discoro_dest_path)
    os.makedirs(_discoro_dest_path)
    os.chdir(_discoro_dest_path)
    sys.path.insert(0, _discoro_dest_path)
    with open(_discoro_pid_path, 'w') as _discoro_var:
        _discoro_var.write('%s' % os.getpid())
    asyncoro.logger.debug('discoro server "%s" started at %s; '
                          'computation files will be saved in "%s"' %
                          (_discoro_name, _discoro_coro.location, _discoro_dest_path))
    _discoro_req = _discoro_client = _discoro_auth = _discoro_msg = None
    _discoro_timer_coro = _discoro_pulse_coro = _discoro_timer_proc = _discoro_peer_status = None
    _discoro_monitor_coro = _discoro_monitor_proc = _discoro_node_status = None
    _discoro_computation = _discoro_func = _discoro_var = None
    _discoro_job_coros = set()
    _discoro_busy_time = time.time()
    _discoro_globals = {}
    _discoro_locals = {}
    _discoro_modules = dict(sys.modules)
    _discoro_globals.update(globals())
    _discoro_locals.update(locals())

    def _discoro_timer_proc(coro=None):
        coro.set_daemon()
        last_pulse = time.time()
        interval = None
        while True:
            reset = yield coro.sleep(interval)
            if reset:
                if not isinstance(_discoro_pulse_coro, Coro):
                    interval = None
                    continue
                interval = reset
                last_pulse = time.time()
                continue
            if not _discoro_pulse_coro:
                continue
            msg = {'ncoros': len(_discoro_job_coros), 'location': coro.location}
            if _discoro_node_status:
                msg['node_status'] = DiscoroNodeStatus(coro.location.addr, psutil.cpu_percent(),
                                                       psutil.virtual_memory().percent,
                                                       psutil.disk_usage(_discoro_dest_path).percent)

            if _discoro_pulse_coro.send(msg) == 0:
                last_pulse = time.time()
            elif (time.time() - last_pulse) > (5 * interval) and _discoro_computation:
                asyncoro.logger.warning('scheduler is not reachable; closing computation "%s"' %
                                        _discoro_computation._auth)
                _discoro_coro.send({'req': 'close', 'auth': _discoro_computation._auth})

            if ((not _discoro_job_coros) and _discoro_computation.zombie_period and
               ((time.time() - _discoro_busy_time) > _discoro_computation.zombie_period)):
                asyncoro.logger.debug('%s: zombie computation "%s"' %
                                      (coro.location, _discoro_computation._auth))
                # TODO: close? For now wait for "too many" timeouts to close

    def _discoro_peer_status(coro=None):
        coro.set_daemon()
        while True:
            status = yield coro.receive()
            if isinstance(status, asyncoro.PeerStatus) and \
               status.status == asyncoro.PeerStatus.Offline and \
               _discoro_pulse_coro and _discoro_pulse_coro.location == status.location:
                asyncoro.logger.debug('scheduler at %s quit; closing computation %s' %
                                      (status.location, _discoro_computation._auth))
                msg = {'req': 'close', 'auth': _discoro_computation._auth}
                _discoro_coro.send(msg)

    def _discoro_monitor_proc(coro=None):
        nonlocal _discoro_busy_time
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            if isinstance(msg, asyncoro.MonitorException):
                _discoro_busy_time = time.time()
                asyncoro.logger.debug('job %s done' % msg.args[0])
                _discoro_job_coros.discard(msg.args[0])
            else:
                asyncoro.logger.warning('%s: invalid monitor message ignored' % coro.location)

    _discoro_timer_coro = Coro(_discoro_timer_proc)
    _discoro_monitor_coro = Coro(_discoro_monitor_proc)
    asyncoro.AsynCoro.instance().peer_status(Coro(_discoro_peer_status))

    while True:
        _discoro_msg = yield _discoro_coro.receive()
        if not isinstance(_discoro_msg, dict):
            continue
        _discoro_req = _discoro_msg.get('req', None)

        if _discoro_req == 'run':
            _discoro_client = _discoro_msg.get('client', None)
            _discoro_auth = _discoro_msg.get('auth', None)
            _discoro_func = _discoro_msg.get('func', None)
            if not isinstance(_discoro_client, Coro) or not _discoro_computation or \
               _discoro_auth != _discoro_computation._auth:
                asyncoro.logger.warning('invalid run: %s' % (type(_discoro_func)))
                if isinstance(_discoro_client, Coro):
                    _discoro_client.send(None)
                continue
            try:
                _discoro_func = asyncoro.unserialize(_discoro_func)
                if _discoro_func.code:
                    exec(_discoro_func.code, globals())
                job_coro = Coro(globals()[_discoro_func.name],
                                *(_discoro_func.args), **(_discoro_func.kwargs))
            except:
                asyncoro.logger.debug('invalid computation to run')
                # _discoro_func = Scheduler._Function(_discoro_func.name, None,
                #                                     _discoro_func.args, _discoro_func.kwargs)
                job_coro = (sys.exc_info()[0], getattr(_discoro_func, 'name', _discoro_func),
                            traceback.format_exc())
            else:
                asyncoro.logger.debug('job %s created' % job_coro)
                _discoro_job_coros.add(job_coro)
                job_coro.notify(_discoro_monitor_coro)
                _discoro_var = _discoro_msg.get('notify', None)
                if isinstance(_discoro_var, Coro):
                    job_coro.notify(_discoro_var)
            _discoro_busy_time = time.time()
            _discoro_client.send(job_coro)
            del job_coro
        elif _discoro_req == 'setup':
            _discoro_client = _discoro_msg.get('client', None)
            _discoro_pulse_coro = _discoro_msg.get('pulse_coro', None)
            if not isinstance(_discoro_client, Coro) or not isinstance(_discoro_pulse_coro, Coro):
                continue
            if _discoro_computation is not None:
                asyncoro.logger.debug('invalid "setup" - busy')
                _discoro_client.send(-1)
                continue
            os.chdir(_discoro_dest_path)
            try:
                _discoro_computation = _discoro_msg['computation']
                exec('import asyncoro.disasyncoro as asyncoro', globals())
                if __name__ == '__mp_main__':  # Windows multiprocessing process
                    exec('import asyncoro.disasyncoro as asyncoro',
                         sys.modules['__mp_main__'].__dict__)
                if _discoro_computation._code:
                    exec(_discoro_computation._code, globals())
                    if __name__ == '__mp_main__':  # Windows multiprocessing process
                        exec(_discoro_computation._code, sys.modules['__mp_main__'].__dict__)
            except:
                _discoro_computation = None
                asyncoro.logger.warning('invalid computation')
                asyncoro.logger.debug(traceback.format_exc())
                _discoro_client.send(-1)
                continue
            if psutil and _discoro_msg.get('node_status', None):
                _discoro_node_status = True
            if isinstance(_discoro_computation.pulse_interval, int) and \
               MinPulseInterval <= _discoro_computation.pulse_interval <= MaxPulseInterval:
                _discoro_computation.pulse_interval = _discoro_computation.pulse_interval
            else:
                _discoro_computation.pulse_interval = MinPulseInterval
            _discoro_timer_coro.resume(_discoro_computation.pulse_interval)
            _discoro_busy_time = time.time()
            asyncoro.logger.debug('computation "%s" from %s' %
                                  (_discoro_computation._auth, _discoro_msg['client'].location))
            _discoro_client.send(0)
        elif _discoro_req == 'close':
            _discoro_auth = _discoro_msg.get('auth', None)
            if not _discoro_computation or (_discoro_auth != _discoro_computation._auth and
                                            _discoro_auth != _discoro_config['auth']):
                continue
            asyncoro.logger.debug('%s deleting computation "%s"' %
                                  (_discoro_coro.location, _discoro_computation._auth))
            if _discoro_auth != _discoro_computation._auth and _discoro_pulse_coro:
                _discoro_pulse_coro.send({'status': 'ServerClosed',
                                          'location': _discoro_coro.location})
            for _discoro_var in _discoro_job_coros:
                _discoro_var.terminate()
            _discoro_job_coros = set()

            if __name__ == '__mp_main__':  # Windows multiprocessing process
                for _discoro_var in list(globals()):
                    if _discoro_var not in _discoro_globals:
                        globals().pop(_discoro_var, None)
                        sys.modules['__mp_main__'].__dict__.pop(_discoro_var, None)
                globals().update(_discoro_globals)
                sys.modules['__mp_main__'].__dict__.update(_discoro_globals)
            else:
                for _discoro_var in list(globals()):
                    if _discoro_var not in _discoro_globals:
                        globals().pop(_discoro_var, None)
                globals().update(_discoro_globals)

            for _discoro_var in list(sys.modules.keys()):
                if _discoro_var not in _discoro_modules:
                    sys.modules.pop(_discoro_var, None)
            sys.modules.update(_discoro_modules)

            for _discoro_var in os.listdir(_discoro_dest_path):
                _discoro_var = os.path.join(_discoro_dest_path, _discoro_var)
                if os.path.isdir(_discoro_var) and not os.path.islink(_discoro_var):
                    shutil.rmtree(_discoro_var, ignore_errors=True)
                else:
                    os.remove(_discoro_var)
            if not os.path.isdir(_discoro_dest_path):
                try:
                    os.remove(_discoro_dest_path)
                except:
                    pass
                os.makedirs(_discoro_dest_path)
            if not os.path.isfile(_discoro_pid_path):
                try:
                    if os.path.islink(_discoro_pid_path):
                        os.remove(_discoro_pid_path)
                    else:
                        shutil.rmtree(_discoro_pid_path)
                    with open(_discoro_pid_path, 'w') as _discoro_var:
                        _discoro_var.write('%s' % os.getpid())
                except:
                    asyncoro.logger.warning('PID file "%s" is invalid' % _discoro_pid_path)
            os.chdir(_discoro_dest_path)
            asyncoro.AsynCoro.instance().dest_path = _discoro_dest_path
            _discoro_computation = _discoro_client = _discoro_pulse_coro = None
            _discoro_node_status = None
            if _discoro_config['serve'] > 0:
                _discoro_config['serve'] -= 1
                if _discoro_config['serve'] == 0:
                    break
            _discoro_timer_coro.resume(MinPulseInterval)
        elif _discoro_req == 'node_info':
            if psutil:
                info = DiscoroNodeInfo(
                    _discoro_name, _discoro_coro.location.addr,
                    psutil.cpu_count(), psutil.cpu_percent(),
                    {_discoro_var: getattr(psutil.virtual_memory(), _discoro_var)
                     for _discoro_var in ['total', 'percent']},
                    {_discoro_var: getattr(psutil.disk_usage(_discoro_dest_path), _discoro_var)
                     for _discoro_var in ['total', 'percent']}
                    )
                if _discoro_msg.get('node_status', None):
                    _discoro_node_status = True
            else:
                info = DiscoroNodeInfo(_discoro_name, _discoro_coro.location.addr,
                                       -1, -1, None, None)
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, Coro):
                continue
            _discoro_client.send(info)
        elif _discoro_req == 'status':
            if _discoro_msg.get('auth', None) != _discoro_config['auth']:
                asyncoro.logger.debug('ignoring info: %s' % (_discoro_msg.get('auth')))
                continue
            if _discoro_pulse_coro:
                print('  Server %s running %d coroutines for computation at %s' %
                      (_discoro_coro.location, len(_discoro_job_coros),
                       _discoro_pulse_coro.location))
            else:
                print('  Server %s not used by any computation' % (_discoro_coro.location))
        elif _discoro_req == 'quit':
            if _discoro_msg.get('auth', None) != _discoro_config['auth']:
                asyncoro.logger.debug('ignoring quit: %s' % (_discoro_msg.get('auth')))
                continue
            if _discoro_pulse_coro:
                _discoro_pulse_coro.send({'status': 'ServerClosed',
                                          'location': _discoro_coro.location})
            break
        elif _discoro_req == 'terminate':
            if _discoro_msg.get('auth', None) != _discoro_config['auth']:
                asyncoro.logger.debug('ignoring terminate: %s' % (_discoro_msg.get('auth')))
                continue
            if _discoro_pulse_coro:
                _discoro_pulse_coro.send({'status': 'ServerTerminated',
                                          'location': _discoro_coro.location})
            if _discoro_computation:
                msg = {'req': 'close', 'auth': _discoro_computation._auth}
                _discoro_config['serve'] = 1
                _discoro_coro.send(msg)
            else:
                break
        else:
            asyncoro.logger.warning('invalid command "%s" ignored' % _discoro_req)
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, Coro):
                continue
            _discoro_client.send(-1)

    # wait until all computations are done; process only 'close'
    while _discoro_job_coros:
        _discoro_msg = yield _discoro_coro.receive()
        if not isinstance(_discoro_msg, dict):
            continue
        _discoro_req = _discoro_msg.get('req', None)

        if _discoro_req == 'close':
            _discoro_auth = _discoro_msg.get('auth', None)
            if not _discoro_computation or _discoro_auth != _discoro_computation._auth:
                continue
            asyncoro.logger.debug('%s deleting computation "%s"' %
                                  (_discoro_coro.location, _discoro_computation._auth))

            if __name__ == '__mp_main__':  # Windows multiprocessing process
                for _discoro_var in list(globals()):
                    if _discoro_var not in _discoro_globals:
                        globals().pop(_discoro_var, None)
                        sys.modules['__mp_main__'].__dict__.pop(_discoro_var, None)
                globals().update(_discoro_globals)
                sys.modules['__mp_main__'].__dict__.update(_discoro_globals)
            else:
                for _discoro_var in list(globals()):
                    if _discoro_var not in _discoro_globals:
                        globals().pop(_discoro_var, None)
                globals().update(_discoro_globals)

            break
        else:
            asyncoro.logger.warning('invalid command "%s" ignored' % _discoro_req)
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, Coro):
                continue
            _discoro_client.send(-1)

    for _discoro_var in os.listdir(_discoro_dest_path):
        _discoro_var = os.path.join(_discoro_dest_path, _discoro_var)
        if os.path.isdir(_discoro_var) and not os.path.islink(_discoro_var):
            shutil.rmtree(_discoro_var, ignore_errors=True)
        else:
            os.remove(_discoro_var)
    if os.path.isfile(_discoro_pid_path):
        os.remove(_discoro_pid_path)
    _discoro_config['mp_queue'].put(_discoro_config['auth'])
    asyncoro.logger.debug('discoro server %s quit' % _discoro_coro.location)


def _discoro_process(_discoro_config, _discoro_name, _discoro_server_id,
                     _discoro_tcp_port, _discoro_mp_queue):
    import os
    import hashlib
    import time
    import logging
    import asyncoro.disasyncoro as asyncoro

    _discoro_serve = _discoro_config.pop('serve', -1)
    _discoro_phoenix = _discoro_config.pop('phoenix', False)
    _discoro_auth = hashlib.sha1(''.join(hex(_)[2:] for _ in os.urandom(10)).encode()).hexdigest()
    _discoro_config['name'] = '%s-%s' % (_discoro_name, _discoro_server_id)
    _discoro_config['tcp_port'] = _discoro_tcp_port
    if _discoro_config['loglevel']:
        asyncoro.logger.setLevel(logging.DEBUG)
    else:
        asyncoro.logger.setLevel(logging.INFO)
    del _discoro_config['loglevel']

    _discoro_scheduler = asyncoro.AsynCoro(**_discoro_config)
    _discoro_coro = asyncoro.Coro(_discoro_proc)
    # delete variables created in main
    for _discoro_var in list(globals().keys()):
        if _discoro_var.startswith('_discoro_'):
            globals().pop(_discoro_var)

    _discoro_coro.send({'req': 'config', 'id': _discoro_server_id,
                        'phoenix': _discoro_phoenix, 'serve': _discoro_serve,
                        'auth': _discoro_auth, 'mp_queue': _discoro_mp_queue})
    del hashlib, logging, os, _discoro_serve, _discoro_phoenix
    req_queue, _discoro_mp_queue = _discoro_mp_queue, None

    while True:
        try:
            req = req_queue.get()
        except:
            req = 'terminate'

        if req == 'status':
            _discoro_coro.send({'req': 'status', 'auth': _discoro_auth})
        elif req == 'quit':
            _discoro_coro.send({'req': 'quit', 'auth': _discoro_auth})
        elif req == 'close':
            _discoro_coro.send({'req': 'close', 'auth': _discoro_auth})
        elif req == 'terminate':
            _discoro_coro.send({'req': 'quit', 'auth': _discoro_auth})
            _discoro_coro.send({'req': 'terminate', 'auth': _discoro_auth})
        elif isinstance(req, str):
            if req == _discoro_auth:
                break
            else:
                asyncoro.logger.warning('Ignoring invalid request: "%s"' % req)
        else:
            asyncoro.logger.warning('Ignoring invalid request: "%s"' % type(req))

    _discoro_scheduler.finish()


if __name__ == '__main__':

    """
    If '-c' option is used with a positive number, discoro server is run that
    many instances, so CPU intesive coroutines can be invoked on them. If the
    number is negative, that many processors are not used (from the available
    processors). The default value for this option is '0', in which case all the
    available processors are used.

    '-n' option can be used to specify prefix name for asyncoro schedulers. This
    name is appended with hyphen followed by a unique number when AsynCoro is
    created. Note that the names in a cluster must be unique; otherwise,
    'locate' may give inconsistent results.

    If '-d' option is used, debug logging is enabled.

    Remaining options are as per AsynCoro in disasyncoro module.
    """

    import sys
    import time
    import argparse
    import multiprocessing
    import socket
    import os
    import collections
    try:
        import readline
    except:
        pass

    try:
        import psutil
        del psutil
    except:
        print('\n   \'psutil\' module is not available; '
              'CPU, memory, disk status will not be sent!\n')

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cpus', dest='cpus', type=int, default=0,
                        help='number of CPUs/discoro instances to run; '
                        'if negative, that many CPUs are not used')
    parser.add_argument('-i', '--ip_addr', dest='node', default=None,
                        help='IP address or host name of this node')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addr', default=None,
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('-u', '--udp_port', dest='udp_port', type=int, default=51350,
                        help='UDP port number to use')
    parser.add_argument('--tcp_ports', dest='tcp_ports', action='append', default=[],
                        help='TCP port numbers to use')
    parser.add_argument('-n', '--name', dest='name', default=None,
                        help='(symbolic) name given to AsynCoro schdulers on this node')
    parser.add_argument('--dest_path', dest='dest_path', default=None,
                        help='path prefix to where files sent by peers are stored')
    parser.add_argument('--max_file_size', dest='max_file_size', default=None, type=int,
                        help='maximum file size of any file transferred')
    parser.add_argument('-s', '--secret', dest='secret', default='',
                        help='authentication secret for handshake with peers')
    parser.add_argument('--certfile', dest='certfile', default=None,
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default=None,
                        help='file containing SSL key')
    parser.add_argument('--serve', dest='serve', default=-1, type=int,
                        help='number of clients to serve before exiting')
    parser.add_argument('--daemon', action='store_true', dest='daemon', default=False,
                        help='if given, input is not read from terminal')
    parser.add_argument('--phoenix', action='store_true', dest='phoenix', default=False,
                        help='if given, server processes from previous run will be killed '
                        'and new server process started')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    _discoro_config = vars(parser.parse_args(sys.argv[1:]))

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

    _discoro_tcp_ports = set()
    tcp_port = tcp_ports = None
    for tcp_port in _discoro_config.pop('tcp_ports', []):
        tcp_ports = tcp_port.split('-')
        if len(tcp_ports) == 1:
            _discoro_tcp_ports.add(int(tcp_ports[0]))
        elif len(tcp_ports) == 2:
            _discoro_tcp_ports = _discoro_tcp_ports.union(range(int(tcp_ports[0]),
                                                                int(tcp_ports[1]) + 1))
        else:
            raise Exception('Invalid TCP port range "%s"' % tcp_ports)
    _discoro_tcp_ports = sorted(_discoro_tcp_ports)

    if _discoro_tcp_ports:
        for tcp_port in range(_discoro_tcp_ports[-1] + 1,
                              _discoro_tcp_ports[-1] + 1 + _discoro_cpus - len(_discoro_tcp_ports)):
            _discoro_tcp_ports.append(tcp_port)
    else:
        _discoro_tcp_ports = [0] * _discoro_cpus

    _discoro_name = _discoro_config['name']
    if not _discoro_name:
        _discoro_name = socket.gethostname()
        if not _discoro_name:
            _discoro_name = 'discoro_server'

    _discoro_daemon = _discoro_config.pop('daemon', False)
    if not _discoro_daemon:
        try:
            if os.getpgrp() != os.tcgetpgrp(sys.stdin.fileno()):
                _discoro_daemon = True
        except:
            pass

    _discoro_server_infos = []
    _discoro_ServerInfo = collections.namedtuple('DiscoroServerInfo', ['Proc', 'Queue'])
    _discoro_mp_queue = None
    for _discoro_server_id in range(1, _discoro_cpus+1):
        _discoro_mp_queue = multiprocessing.Queue()
        _discoro_server_info = _discoro_ServerInfo(
            multiprocessing.Process(target=_discoro_process,
                                    args=(_discoro_config, _discoro_name, _discoro_server_id,
                                          _discoro_tcp_ports[_discoro_server_id - 1],
                                          _discoro_mp_queue)),
            _discoro_mp_queue)
        _discoro_server_infos.append(_discoro_server_info)

    # delete variables not needed anymore
    del _discoro_mp_queue, _discoro_tcp_ports, tcp_port, tcp_ports
    del parser, argparse, multiprocessing, socket, collections, os

    for _discoro_server_info in _discoro_server_infos:
        _discoro_server_info.Proc.start()
        time.sleep(0.05)

    if not _discoro_daemon:
        import threading

        def _discoro_cmd_reader():
            while True:
                time.sleep(0.25)
                try:
                    _discoro_cmd = input(
                        '\nEnter "status" to get status\n'
                        '  "close" to close current computation (kill any running jobs)\n'
                        '  "quit" or "exit" to stop accepting new jobs and quit when done\n'
                        '  "terminate" to kill current jobs and quit: '
                        )
                except:
                    _discoro_cmd = 'terminate'
                else:
                    _discoro_cmd = _discoro_cmd.strip().lower()

                print('')
                if _discoro_cmd == 'status':
                    for _discoro_server_info in _discoro_server_infos:
                        _discoro_server_info.Queue.put('status')
                elif _discoro_cmd in ('quit', 'exit'):
                    for _discoro_server_info in _discoro_server_infos:
                        _discoro_server_info.Queue.put('quit')
                    break
                elif _discoro_cmd == 'terminate':
                    for _discoro_server_info in _discoro_server_infos:
                        _discoro_server_info.Queue.put('terminate')
                    break
                elif _discoro_cmd == 'close':
                    for _discoro_server_info in _discoro_server_infos:
                        _discoro_server_info.Queue.put('close')

        _discoro_cmd = threading.Thread(target=_discoro_cmd_reader)
        _discoro_cmd.daemon = True
        _discoro_cmd.start()
    else:
        _discoro_cmd = None

    while True:
        try:
            for _discoro_server_info in _discoro_server_infos:
                if _discoro_server_info.Proc.is_alive():
                    _discoro_server_info.Proc.join()
            break
        except:
            for _discoro_server_info in _discoro_server_infos:
                _discoro_server_info.Queue.put('terminate')

    exit(0)
