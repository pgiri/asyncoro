#!/usr/bin/python

"""This file is part of asyncoro; see http://asyncoro.sourceforge.net for
details.

This program can be used to start discoro server processes so discoro scheduler
(see 'discoro.py') can send computations to these server processes for executing
distributed communicating proceses (coroutines). All coroutines in a server
execute in the same thread, so multiple CPUs are not used by one server. If CPU
intensive computations are to be run on systems with multiple processors, then
this program should be run with multiple instances (see below for '-c' option to
this program).

See 'discoro_client*.py' files for example use cases.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2014 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"


def _discoro_server_proc():
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
        DiscoroNodeInfo, DiscoroNodeAvailInfo

    class Nonlocals(object):
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    _discoro_coro = asyncoro.AsynCoro.cur_coro()
    _discoro_config = yield _discoro_coro.receive()
    assert _discoro_config['req'] == 'config'
    _discoro_coro.register('discoro_server')
    _discoro_timer_coro = _discoro_config['timer_coro']
    yield asyncoro.AsynCoro.instance().peer(_discoro_timer_coro.location)

    if _discoro_config['min_pulse_interval'] > 0:
        MinPulseInterval = _discoro_config['min_pulse_interval']
    if _discoro_config['max_pulse_interval'] > 0:
        MaxPulseInterval = _discoro_config['max_pulse_interval']
    _discoro_msg_timeout = _discoro_config.pop('msg_timeout')

    _discoro_name = asyncoro.AsynCoro.instance().name
    asyncoro.AsynCoro.instance().dest_path = os.path.join('discoro', _discoro_name)
    _discoro_dest_path = asyncoro.AsynCoro.instance().dest_path
    _discoro_pid_path = os.path.join(_discoro_dest_path, '..', '%s.pid' % _discoro_name)
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
                    asyncoro.logger.warning('Killing process %d failed', _discoro_var)
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

    def _discoro_peer(peer, coro=None):
        yield asyncoro.AsynCoro.instance().peer(peer)

    for _discoro_var in _discoro_config.pop('peers', []):
        Coro(_discoro_peer, _discoro_var)
    del _discoro_peer

    for _discoro_var in ['req', 'phoenix', 'min_pulse_interval', 'max_pulse_interval']:
        del _discoro_config[_discoro_var]

    asyncoro.logger.info('discoro server "%s" started at %s; '
                         'computation files will be saved in "%s"',
                         _discoro_name, _discoro_coro.location, _discoro_dest_path)
    _discoro_req = _discoro_client = _discoro_auth = _discoro_msg = None
    _discoro_scheduler_coro = _discoro_peer_status = _discoro_zombie_timeout = None
    _discoro_monitor_coro = _discoro_monitor_proc = _discoro_schedule_coro = None
    _discoro_computation = _discoro_func = _discoro_var = None
    _discoro_job_coros = set()
    _discoro_nonlocals = Nonlocals(busy_time=time.time())
    _discoro_globals = {}
    _discoro_locals = {}
    _discoro_modules = dict(sys.modules)
    _discoro_globals.update(globals())
    _discoro_locals.update(locals())

    def _discoro_peer_status(coro=None):
        coro.set_daemon()
        while 1:
            status = yield coro.receive()
            if (isinstance(status, asyncoro.PeerStatus) and
                status.status == asyncoro.PeerStatus.Offline and
               _discoro_scheduler_coro and _discoro_scheduler_coro.location == status.location):
                asyncoro.logger.debug('scheduler at %s quit; closing computation %s',
                                      status.location, _discoro_computation._auth)
                _discoro_coro.send({'req': 'close', 'auth': _discoro_config['auth']})

    def _discoro_monitor_proc(coro=None):
        coro.set_daemon()
        while 1:
            msg = yield coro.receive(timeout=_discoro_zombie_timeout)
            if isinstance(msg, asyncoro.MonitorException):
                _discoro_nonlocals.busy_time = time.time()
                asyncoro.logger.debug('coro %s done', msg.args[0])
                _discoro_job_coros.discard(msg.args[0])
            elif ((not _discoro_job_coros) and _discoro_zombie_timeout and
                  ((time.time() - _discoro_nonlocals.busy_time) > _discoro_zombie_timeout)):
                asyncoro.logger.debug('%s: zombie computation "%s"',
                                      coro.location, _discoro_computation._auth)
                _discoro_coro.send({'req': 'close', 'auth': _discoro_config['auth']})

    def _discoro_schedule_proc(client, job_coro, coro=None):
        if (yield client.deliver(job_coro, timeout=_discoro_msg_timeout)) == 1:
            Coro._asyncoro._add(job_coro)
        else:
            _discoro_job_coros.discard(job_coro)

    _discoro_monitor_coro = Coro(_discoro_monitor_proc)
    asyncoro.AsynCoro.instance().peer_status(Coro(_discoro_peer_status))

    while 1:
        _discoro_msg = yield _discoro_coro.receive()
        if not isinstance(_discoro_msg, dict):
            continue
        _discoro_req = _discoro_msg.get('req', None)

        if _discoro_req == 'run':
            _discoro_client = _discoro_msg.get('client', None)
            _discoro_auth = _discoro_msg.get('auth', None)
            _discoro_func = _discoro_msg.get('func', None)
            if (not isinstance(_discoro_client, Coro) or not _discoro_computation or
                _discoro_auth != _discoro_computation._auth):
                asyncoro.logger.warning('invalid run: %s', type(_discoro_func))
                if isinstance(_discoro_client, Coro):
                    _discoro_client.send(None)
                continue
            try:
                _discoro_func = asyncoro.unserialize(_discoro_func)
                if _discoro_func.code:
                    exec(_discoro_func.code) in globals()
                job_coro = Coro(globals()[_discoro_func.name],
                                *(_discoro_func.args), **(_discoro_func.kwargs))
            except:
                asyncoro.logger.debug('invalid computation to run')
                job_coro = (sys.exc_info()[0], getattr(_discoro_func, 'name', _discoro_func),
                            traceback.format_exc())
                _discoro_client.send(job_coro)
            else:
                asyncoro.logger.debug('coro %s created', job_coro)
                _discoro_job_coros.add(job_coro)
                job_coro.notify(_discoro_monitor_coro)
                _discoro_var = _discoro_msg.get('notify', None)
                if isinstance(_discoro_var, Coro):
                    job_coro.notify(_discoro_var)
                if Coro._asyncoro._remove(job_coro) == 0:
                    Coro(_discoro_schedule_proc, _discoro_client, job_coro)
            _discoro_nonlocals.busy_time = time.time()
            del job_coro
        elif _discoro_req == 'setup':
            _discoro_client = _discoro_msg.get('client', None)
            _discoro_scheduler_coro = _discoro_msg.get('pulse_coro', None)
            if (not isinstance(_discoro_client, Coro) or
                not isinstance(_discoro_scheduler_coro, Coro)):
                continue
            if _discoro_computation is not None:
                asyncoro.logger.debug('invalid "setup" - busy')
                _discoro_client.send(-1)
                continue
            os.chdir(_discoro_dest_path)
            try:
                _discoro_computation = _discoro_msg['computation']
                exec('import asyncoro.disasyncoro as asyncoro') in globals()
                if _discoro_computation._code:
                    exec(_discoro_computation._code) in globals()
            except:
                _discoro_computation = None
                asyncoro.logger.warning('invalid computation')
                asyncoro.logger.debug(traceback.format_exc())
                _discoro_client.send(-1)
                continue
            if (isinstance(_discoro_computation.pulse_interval, int) and
                MinPulseInterval <= _discoro_computation.pulse_interval <= MaxPulseInterval):
                _discoro_computation.pulse_interval = _discoro_computation.pulse_interval
            else:
                _discoro_computation.pulse_interval = MinPulseInterval
            _discoro_timer_coro.send({'scheduler_coro': _discoro_scheduler_coro,
                                      'interval': _discoro_computation.pulse_interval,
                                      'disk_path': _discoro_dest_path,
                                      'auth': _discoro_computation._auth})
            _discoro_nonlocals.busy_time = time.time()
            _discoro_zombie_timeout = _discoro_computation.zombie_period
            asyncoro.logger.debug('%s: Computation "%s" from %s with zombie period %s',
                                  _discoro_coro.location, _discoro_computation._auth,
                                  _discoro_msg['client'].location, _discoro_zombie_timeout)
            if not _discoro_zombie_timeout:
                _discoro_zombie_timeout = None
            _discoro_monitor_coro.send(None)
            _discoro_client.send(0)
        elif _discoro_req == 'close':
            _discoro_auth = _discoro_msg.get('auth', None)
            if not _discoro_computation or (_discoro_auth != _discoro_computation._auth and
                                            _discoro_auth != _discoro_config['auth']):
                continue
            asyncoro.logger.debug('%s deleting computation "%s"',
                                  _discoro_coro.location, _discoro_computation._auth)
            for _discoro_var in _discoro_job_coros:
                _discoro_var.terminate()
            _discoro_job_coros.clear()

            for _discoro_var in list(globals()):
                if _discoro_var not in _discoro_globals:
                    globals().pop(_discoro_var, None)
            globals().update(_discoro_globals)

            for _discoro_var in sys.modules.keys():
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
                    asyncoro.logger.warning('PID file "%s" is invalid', _discoro_pid_path)
            _discoro_timer_coro.send({'scheduler_coro': None, 'interval': None,
                                      'disk_path': '', 'auth': _discoro_computation._auth})
            if _discoro_auth != _discoro_computation._auth and _discoro_scheduler_coro:
                _discoro_scheduler_coro.send({'status': 'ServerClosed',
                                              'location': _discoro_coro.location})
            os.chdir(_discoro_dest_path)
            asyncoro.AsynCoro.instance().dest_path = _discoro_dest_path
            _discoro_computation = _discoro_client = _discoro_scheduler_coro = None
            _discoro_zombie_timeout = None
            if _discoro_config['serve'] > 0:
                _discoro_config['serve'] -= 1
                if _discoro_config['serve'] == 0:
                    break
        elif _discoro_req == 'node_info':
            if psutil:
                _discoro_var = DiscoroNodeAvailInfo(_discoro_coro.location.addr,
                                                    100.0 - psutil.cpu_percent(),
                                                    psutil.virtual_memory().available,
                                                    psutil.disk_usage(_discoro_dest_path).free,
                                                    100.0 - psutil.swap_memory().percent)
            else:
                _discoro_var = None
            # _discoro_name is host name followed by '-' and ID
            _discoro_var = DiscoroNodeInfo(_discoro_name[:_discoro_name.rfind('-')],
                                           _discoro_coro.location.addr, _discoro_var)
            _discoro_client = _discoro_msg.get('client', None)
            if isinstance(_discoro_client, Coro):
                _discoro_client.send(_discoro_var)
        elif _discoro_req == 'status':
            if _discoro_msg.get('auth', None) != _discoro_config['auth']:
                asyncoro.logger.debug('ignoring info: %s', _discoro_msg.get('auth'))
                continue
            if _discoro_scheduler_coro:
                print('  Server "%s" @ %s running %d coroutines for %s' %
                      (_discoro_name, _discoro_coro.location, len(_discoro_job_coros),
                       _discoro_scheduler_coro.location))
            else:
                print('  Server "%s" @ %s not used by any computation' %
                      (_discoro_name, _discoro_coro.location))
        elif _discoro_req == 'quit':
            if _discoro_msg.get('auth', None) != _discoro_config['auth']:
                asyncoro.logger.debug('ignoring quit: %s', _discoro_msg.get('auth'))
                continue
            if _discoro_scheduler_coro:
                _discoro_scheduler_coro.send({'status': 'ServerClosed',
                                              'location': _discoro_coro.location})
            break
        elif _discoro_req == 'terminate':
            if _discoro_msg.get('auth', None) != _discoro_config['auth']:
                asyncoro.logger.debug('ignoring terminate: %s', _discoro_msg.get('auth'))
                continue
            if _discoro_computation:
                msg = {'req': 'close', 'auth': _discoro_config['auth']}
                _discoro_config['serve'] = 1
                _discoro_coro.send(msg)
            else:
                break
        else:
            asyncoro.logger.warning('invalid command "%s" ignored', _discoro_req)
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, Coro):
                continue
            _discoro_client.send(-1)

    # wait until all computations are done; process only 'close'
    while _discoro_job_coros:
        _discoro_msg = yield _discoro_coro.receive(60)
        if not _discoro_job_coros and not _discoro_msg:
            _discoro_msg = {'req': 'close', 'auth': _discoro_computation._auth}
        if not isinstance(_discoro_msg, dict):
            continue
        _discoro_req = _discoro_msg.get('req', None)

        if _discoro_req == 'close' or not _discoro_job_coros:
            _discoro_auth = _discoro_msg.get('auth', None)
            if _discoro_auth != _discoro_computation._auth:
                continue
            asyncoro.logger.debug('%s deleting computation "%s"',
                                  _discoro_coro.location, _discoro_computation._auth)

            for _discoro_var in list(globals()):
                if _discoro_var not in _discoro_globals:
                    globals().pop(_discoro_var, None)
            globals().update(_discoro_globals)

            break
        else:
            asyncoro.logger.warning('invalid command "%s" ignored', _discoro_req)
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, Coro):
                continue
            _discoro_client.send(-1)

    if _discoro_scheduler_coro:
        _discoro_scheduler_coro.send({'status': 'ServerClosed',
                                      'location': _discoro_coro.location})
    for _discoro_var in os.listdir(_discoro_dest_path):
        _discoro_var = os.path.join(_discoro_dest_path, _discoro_var)
        if os.path.isdir(_discoro_var) and not os.path.islink(_discoro_var):
            shutil.rmtree(_discoro_var, ignore_errors=True)
        else:
            os.remove(_discoro_var)
    if os.path.isfile(_discoro_pid_path):
        os.remove(_discoro_pid_path)
    _discoro_config['mp_queue'].put({'req': 'quit', 'auth': _discoro_config['auth']})
    asyncoro.logger.debug('discoro server "%s" @ %s terminated',
                          _discoro_name, _discoro_coro.location)


def _discoro_process(_discoro_config, _discoro_server_id, _discoro_mp_queue, _discoro_auth):
    import os
    import logging
    import asyncoro.disasyncoro as asyncoro

    if _discoro_config['loglevel']:
        asyncoro.logger.setLevel(logging.DEBUG)
    else:
        asyncoro.logger.setLevel(logging.INFO)
    del _discoro_config['loglevel']

    _discoro_config_msg = {'req': 'config', 'id': _discoro_server_id,
                           'phoenix': _discoro_config.pop('phoenix', False),
                           'serve': _discoro_config.pop('serve', -1),
                           'peers': _discoro_config.pop('peers', []),
                           'msg_timeout': _discoro_config.pop('msg_timeout', asyncoro.MsgTimeout),
                           'min_pulse_interval': _discoro_config.pop('min_pulse_interval'),
                           'max_pulse_interval': _discoro_config.pop('max_pulse_interval'),
                           'auth': _discoro_auth, 'mp_queue': _discoro_mp_queue}
    _discoro_scheduler = asyncoro.AsynCoro(**_discoro_config)
    _discoro_coro = asyncoro.Coro(_discoro_server_proc)
    # delete variables created in main
    for _discoro_var in globals().keys():
        if _discoro_var.startswith('_discoro_'):
            globals().pop(_discoro_var)

    del logging, os, _discoro_config, _discoro_var

    req_queue, _discoro_mp_queue = _discoro_mp_queue, None

    while 1:
        try:
            req = req_queue.get()
        except KeyboardInterrupt:
            req = {'req': 'terminate', 'auth': _discoro_auth}
            req_queue.put(req)

        if not isinstance(req, dict) or req.get('auth') != _discoro_auth:
            asyncoro.logger.warning('Ignoring invalid request: "%s"', type(req))
            continue

        cmd = req.get('req')
        if cmd == 'status' or cmd == 'close':
            _discoro_coro.send(req)
        elif cmd == 'start':
            _discoro_config_msg['timer_coro'] = req.get('timer_coro', None)
            _discoro_coro.send(_discoro_config_msg)
            del _discoro_config_msg
        elif cmd == 'quit' or cmd == 'terminate':
            _discoro_coro.send(req)
            break

    _discoro_scheduler.finish()
    exit(0)


if __name__ == '__main__':

    """
    See http://asyncoro.sourceforge.net/discoro.html#node-servers for details on
    options to start this program.
    """

    import sys
    import time
    import argparse
    import multiprocessing
    import socket
    import os
    import collections
    import hashlib
    import logging
    try:
        import readline
    except:
        pass
    import asyncoro.disasyncoro as asyncoro

    try:
        import psutil
    except ImportError:
        print('\n   \'psutil\' module is not available; '
              'CPU, memory, disk status will not be sent!\n')
        psutil = None
    else:
        psutil.cpu_percent(0.1)

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
    parser.add_argument('--msg_timeout', dest='msg_timeout', default=asyncoro.MsgTimeout, type=int,
                        help='timeout for delivering messages')
    parser.add_argument('--min_pulse_interval', dest='min_pulse_interval', default=0, type=int,
                        help='minimum pulse interval clients can use in number of seconds')
    parser.add_argument('--max_pulse_interval', dest='max_pulse_interval', default=0, type=int,
                        help='maximum pulse interval clients can use in number of seconds')
    parser.add_argument('--daemon', action='store_true', dest='daemon', default=False,
                        help='if given, input is not read from terminal')
    parser.add_argument('--phoenix', action='store_true', dest='phoenix', default=False,
                        help='if given, server processes from previous run will be killed '
                        'and new server process started')
    parser.add_argument('--discover_peers', action='store_true', dest='discover_peers',
                        default=False, help='if given, peers are discovered during startup')
    parser.add_argument('--peer', dest='peers', action='append', default=[],
                        help='peer location (in the form node:TCPport) to communicate')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    _discoro_config = vars(parser.parse_args(sys.argv[1:]))

    if _discoro_config['msg_timeout'] < 1:
        raise Exception('msg_timeout must be at least 1')
    if _discoro_config['min_pulse_interval']:
        if _discoro_config['min_pulse_interval'] < _discoro_config['msg_timeout']:
            raise Exception('min_pulse_interval must be at least msg_timeout')
    if (_discoro_config['max_pulse_interval'] and _discoro_config['min_pulse_interval'] and
       _discoro_config['max_pulse_interval'] < _discoro_config['min_pulse_interval']):
        raise Exception('max_pulse_interval must be at least min_pulse_interval')

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
                              _discoro_tcp_ports[-1] + 1 +
                              (_discoro_cpus + 1) - len(_discoro_tcp_ports)):
            _discoro_tcp_ports.append(tcp_port)
    else:
        _discoro_tcp_ports = [0] * (_discoro_cpus + 1)
    del tcp_port, tcp_ports

    peers, _discoro_config['peers'] = _discoro_config['peers'], []
    peer = None
    for peer in peers:
        peer = peer.split(':')
        if len(peer) != 2:
            raise Exception('peer %s is not valid' % ':'.join(peer))
        _discoro_config['peers'].append(asyncoro.Location(peer[0], peer[1]))
    del peer, peers

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
    _discoro_auth = hashlib.sha1(os.urandom(10).encode('hex')).hexdigest()

    # delete variables not needed anymore
    del parser
    for _discoro_var in ['argparse', 'socket', 'os']:
        del sys.modules[_discoro_var], globals()[_discoro_var]
    del _discoro_var

    _discoro_server_infos = []
    _discoro_ServerInfo = collections.namedtuple('DiscoroServerInfo', ['Proc', 'Queue'])
    _discoro_mp_queue = None
    for _discoro_server_id in range(1, _discoro_cpus+1):
        _discoro_config['name'] = '%s-%s' % (_discoro_name, _discoro_server_id)
        _discoro_config['tcp_port'] = _discoro_tcp_ports[_discoro_server_id]
        _discoro_mp_queue = multiprocessing.Queue()
        _discoro_server_info = _discoro_ServerInfo(
            multiprocessing.Process(target=_discoro_process,
                                    args=(dict(_discoro_config), _discoro_server_id,
                                          _discoro_mp_queue, _discoro_auth)),
            _discoro_mp_queue)
        _discoro_server_infos.append(_discoro_server_info)
        _discoro_server_info.Proc.start()

    def _discoro_timer_proc(msg_timeout, coro=None):
        from asyncoro.discoro import DiscoroNodeAvailInfo
        coro.set_daemon()
        last_pulse = last_proc_check = time.time()
        interval = scheduler_coro = cur_peer = cur_auth = None
        while 1:
            msg = yield coro.receive(timeout=interval)
            if msg:
                auth = msg.get('auth', None)
                if cur_auth and (auth != cur_auth):
                    asyncoro.logger.warning('Timer: invalid computation authentication: %s != %s',
                                            cur_auth, auth)
                    continue
                if scheduler_coro == msg.get('scheduler_coro', None):
                    continue
                scheduler_coro = msg.get('scheduler_coro', None)
                if not isinstance(scheduler_coro, asyncoro.Coro):
                    interval = scheduler_coro = cur_auth = None
                    continue
                cur_auth = auth
                interval = msg.get('interval', None)
                disk_path = msg.get('disk_path', '.')
                if cur_peer != scheduler_coro.location:
                    cur_peer = scheduler_coro.location
                    yield asyncoro.AsynCoro.instance().peer(cur_peer)
            if not scheduler_coro:
                continue

            msg = {'location': coro.location}
            if psutil:
                msg['node_status'] = DiscoroNodeAvailInfo(
                    coro.location.addr, 100.0 - psutil.cpu_percent(),
                    psutil.virtual_memory().available, psutil.disk_usage(disk_path).free,
                    100.0 - psutil.swap_memory().percent)

            now = time.time()
            if (yield scheduler_coro.deliver(msg, timeout=msg_timeout)) == 1:
                last_pulse = now
            elif (now - last_pulse) > (5 * interval):
                asyncoro.logger.warning('scheduler is not reachable; closing computation')
                for _discoro_server_info in _discoro_server_infos:
                    _discoro_server_info.Queue.put({'req': 'close', 'auth': _discoro_auth})

            if (now - last_proc_check) > (3 * interval):
                last_proc_check = now
                for _discoro_server_info in _discoro_server_infos:
                    if not _discoro_server_info.Proc.is_alive():
                        # TODO: inform scheduler, start new process?
                        asyncoro.logger.warning('Process %s is dead?: %s',
                                                _discoro_server_info.Proc.pid,
                                                _discoro_server_info.Proc.exitcode)

    _discoro_server_id = 0
    _discoro_config['name'] = '%s-%s' % (_discoro_name, _discoro_server_id)
    _discoro_config['tcp_port'] = _discoro_tcp_ports[_discoro_server_id]
    _discoro_config.pop('phoenix', False)
    _discoro_config.pop('serve', -1)
    _discoro_config.pop('peers', [])
    _discoro_msg_timeout = _discoro_config.pop('msg_timeout')
    _discoro_config.pop('min_pulse_interval')
    _discoro_config.pop('max_pulse_interval')
    _discoro_config['discover_peers'] = False
    if _discoro_config['loglevel']:
        asyncoro.logger.setLevel(logging.DEBUG)
    else:
        asyncoro.logger.setLevel(logging.INFO)
    del _discoro_config['loglevel']
    _discoro_scheduler = asyncoro.AsynCoro(**_discoro_config)
    _discoro_timer_coro = asyncoro.Coro(_discoro_timer_proc, _discoro_msg_timeout)
    for _discoro_server_info in _discoro_server_infos:
        _discoro_server_info.Queue.put({'req': 'start', 'auth': _discoro_auth,
                                        'timer_coro': _discoro_timer_coro})

    del multiprocessing, collections, _discoro_mp_queue, _discoro_tcp_ports, _discoro_config

    if not _discoro_daemon:
        def _discoro_cmd_reader(coro=None):
            coro.set_daemon()
            async_threads = asyncoro.AsyncThreadPool(1)
            while 1:
                yield coro.sleep(0.25)
                try:
                    _discoro_cmd = yield async_threads.async_task(
                        raw_input,
                        '\nEnter "status" to get status\n'
                        '  "close" to close current computation (kill any running jobs)\n'
                        '  "quit" to stop accepting new jobs and quit when done\n'
                        '  "terminate" to kill current jobs and quit: ')
                except:
                    _discoro_cmd = 'terminate'
                else:
                    _discoro_cmd = _discoro_cmd.strip().lower()

                print('')
                if _discoro_cmd == 'status' or _discoro_cmd == 'close':
                    for _discoro_server_info in _discoro_server_infos:
                        _discoro_server_info.Queue.put({'req': _discoro_cmd, 'auth': _discoro_auth})
                elif _discoro_cmd in ('quit', 'terminate'):
                    for _discoro_server_info in _discoro_server_infos:
                        _discoro_server_info.Queue.put({'req': _discoro_cmd, 'auth': _discoro_auth})
                    break

        asyncoro.Coro(_discoro_cmd_reader)

    while 1:
        try:
            for _discoro_server_info in _discoro_server_infos:
                if _discoro_server_info.Proc.is_alive():
                    _discoro_server_info.Proc.join()
            break
        except:
            for _discoro_server_info in _discoro_server_infos:
                _discoro_server_info.Queue.put({'req': 'terminate', 'auth': _discoro_auth})

    exit(0)
