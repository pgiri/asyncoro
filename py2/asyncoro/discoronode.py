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
    from asyncoro.disasyncoro import Coro, SysCoro, Location
    from asyncoro.discoro import MinPulseInterval, MaxPulseInterval, \
        DiscoroNodeInfo, DiscoroNodeAvailInfo

    _discoro_coro = asyncoro.AsynCoro.cur_coro()
    _discoro_config = yield _discoro_coro.receive()
    _discoro_node_coro = asyncoro.deserialize(_discoro_config['node_coro'])
    _discoro_node_auth = _discoro_config['node_auth']
    _discoro_scheduler_coro = _discoro_config.pop('scheduler_coro', None)
    if _discoro_scheduler_coro:
        _discoro_scheduler_coro = asyncoro.deserialize(_discoro_scheduler_coro)

    if _discoro_config['min_pulse_interval'] > 0:
        MinPulseInterval = _discoro_config['min_pulse_interval']
    if _discoro_config['max_pulse_interval'] > 0:
        MaxPulseInterval = _discoro_config['max_pulse_interval']
    _discoro_busy_time = _discoro_config.pop('busy_time')
    asyncoro.MsgTimeout = _discoro_config.pop('msg_timeout')

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
            os.kill(_discoro_var, signal.SIGKILL)
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
        _discoro_var = asyncoro.deserialize(_discoro_var)
        Coro(_discoro_peer, _discoro_var)
    del _discoro_peer

    for _discoro_var in ['phoenix', 'min_pulse_interval', 'max_pulse_interval']:
        del _discoro_config[_discoro_var]

    asyncoro.logger.info('discoro server %s started at %s; '
                         'computation files will be saved in "%s"',
                         _discoro_config['id'], _discoro_coro.location, _discoro_dest_path)
    _discoro_req = _discoro_client = _discoro_auth = _discoro_msg = None
    _discoro_scheduler_notify = _discoro_peer_status = None
    _discoro_monitor_coro = _discoro_monitor_proc = None
    _discoro_computation = _discoro_func = _discoro_var = None
    _discoro_job_coros = set()
    _discoro_jobs_done = asyncoro.Event()
    _discoro_globals = {}
    _discoro_locals = {}
    _discoro_modules = dict(sys.modules)
    _discoro_globals.update(globals())
    _discoro_locals.update(locals())

    def _discoro_peer_status(coro=None):
        coro.set_daemon()
        while 1:
            status = yield coro.receive()
            if not isinstance(status, asyncoro.PeerStatus):
                asyncoro.logger.warning('Invalid peer status %s ignored', type(status))
                continue
            if status.status == asyncoro.PeerStatus.Online:
                if (_discoro_node_coro and _discoro_node_coro.location == status.location):
                    _discoro_coro.register('discoro_server')
            else:  # status.status == asyncoro.PeerStatus.Offline
                if (_discoro_scheduler_coro and
                    _discoro_scheduler_coro.location == status.location):
                    if _discoro_computation:
                        _discoro_coro.send({'req': 'close', 'auth': _discoro_computation._auth})

    def _discoro_monitor_proc(zombie_period, coro=None):
        coro.set_daemon()
        while 1:
            msg = yield coro.receive(timeout=zombie_period)
            if isinstance(msg, asyncoro.MonitorException):
                asyncoro.logger.debug('coro %s done', msg.args[0])
                _discoro_job_coros.discard(msg.args[0])
                if not _discoro_job_coros:
                    _discoro_jobs_done.set()
                _discoro_busy_time.value = int(time.time())
            elif not msg:
                if _discoro_job_coros:
                    _discoro_busy_time.value = int(time.time())
            else:
                asyncoro.logger.warning('invalid message to monitor ignored: %s', type(msg))

    asyncoro.AsynCoro.instance().peer_status(SysCoro(_discoro_peer_status))
    yield asyncoro.AsynCoro.instance().peer(_discoro_node_coro.location)
    yield asyncoro.AsynCoro.instance().peer(_discoro_scheduler_coro.location)

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
                _discoro_func = asyncoro.deserialize(_discoro_func)
                if _discoro_func.code:
                    exec(_discoro_func.code) in globals()
            except:
                asyncoro.logger.debug('invalid computation to run')
                job_coro = (sys.exc_info()[0], getattr(_discoro_func, 'name', _discoro_func),
                            traceback.format_exc())
                _discoro_client.send(job_coro)
            else:
                Coro._asyncoro._lock.acquire()
                try:
                    job_coro = Coro(globals()[_discoro_func.name],
                                    *(_discoro_func.args), **(_discoro_func.kwargs))
                except:
                    job_coro = (sys.exc_info()[0], getattr(_discoro_func, 'name', _discoro_func),
                                traceback.format_exc())
                else:
                    _discoro_job_coros.add(job_coro)
                    _discoro_busy_time.value = int(time.time())
                    asyncoro.logger.debug('coro %s created', job_coro)
                    job_coro.notify(_discoro_monitor_coro)
                    job_coro.notify(_discoro_scheduler_notify)
                _discoro_client.send(job_coro)
                Coro._asyncoro._lock.release()
            del job_coro

        elif _discoro_req == 'setup':
            _discoro_client = _discoro_msg.get('client', None)
            _discoro_scheduler_notify = _discoro_msg.get('notify', None)
            if (not isinstance(_discoro_client, Coro) or
                not isinstance(_discoro_scheduler_notify, Coro) or
                _discoro_scheduler_coro != _discoro_msg.get('status_coro', None)):
                # TODO: close and quit?
                continue
            if _discoro_computation is not None:
                asyncoro.logger.debug('invalid "setup" - busy')
                _discoro_client.send(-1)
                continue
            os.chdir(_discoro_dest_path)
            try:
                _discoro_computation = _discoro_msg['computation']
                if _discoro_computation._code:
                    exec(_discoro_computation._code) in globals()
            except:
                _discoro_computation = None
                asyncoro.logger.warning('invalid computation')
                asyncoro.logger.debug(traceback.format_exc())
                _discoro_client.send(-1)
                continue
            yield asyncoro.AsynCoro.instance().peer(_discoro_computation._location)
            if (isinstance(_discoro_computation._pulse_interval, int) and
                MinPulseInterval <= _discoro_computation._pulse_interval <= MaxPulseInterval):
                _discoro_computation._pulse_interval = _discoro_computation._pulse_interval
            else:
                _discoro_computation._pulse_interval = MinPulseInterval
            _discoro_var = int(_discoro_computation.zombie_period)
            if _discoro_config['zombie_period']:
                _discoro_var = min(_discoro_var, _discoro_config['zombie_period'])
            if _discoro_var:
                _discoro_var /= 3
            else:
                _discoro_var = None
            if (yield _discoro_node_coro.deliver(
                {'req': 'server_setup', 'id': _discoro_config['id'], 'coro': _discoro_coro,
                 'scheduler_coro': _discoro_scheduler_coro, 'auth': _discoro_computation._auth,
                 'interval': _discoro_computation._pulse_interval,
                 'zombie_period': _discoro_var},
                timeout=asyncoro.MsgTimeout)) != 1:
                _discoro_client.send(-1)
                break

            _discoro_busy_time.value = int(time.time())
            _discoro_monitor_coro = SysCoro(_discoro_monitor_proc, _discoro_var)
            asyncoro.logger.debug('%s: Computation "%s" from %s with zombie period %s',
                                  _discoro_config['id'], _discoro_computation._auth,
                                  _discoro_msg['client'].location, _discoro_computation.zombie_period)
            _discoro_client.send(0)

        elif _discoro_req == 'close' or _discoro_req == 'quit':
            _discoro_auth = _discoro_msg.get('auth', None)
            if (_discoro_computation and _discoro_auth == _discoro_computation._auth):
                pass
            elif (_discoro_msg.get('node_auth', None) == _discoro_config['node_auth']):
                if _discoro_scheduler_coro:
                    _discoro_scheduler_coro.send({'status': 'ServerClosed',
                                                  'location': _discoro_coro.location})
                while _discoro_job_coros:
                    asyncoro.logger.debug('%s: Waiting for %s coroutines to terminate '
                                          'before closing computation',
                                          _discoro_config['id'], len(_discoro_job_coros))
                    if (yield _discoro_jobs_done.wait(timeout=5)):
                        break
            else:
                continue
            _discoro_var = _discoro_msg.get('client', None)
            if isinstance(_discoro_var, Coro):
                _discoro_var.send(0)
            break

        elif _discoro_req == 'terminate':
            _discoro_auth = _discoro_msg.get('node_auth', None)
            if (_discoro_auth != _discoro_config['node_auth']):
                continue
            if _discoro_scheduler_coro:
                _discoro_scheduler_coro.send({'status': 'ServerTerminated',
                                              'location': _discoro_coro.location})
            break

        elif _discoro_req == 'status':
            if _discoro_msg.get('node_auth', None) != _discoro_config['node_auth']:
                continue
            if _discoro_scheduler_coro:
                print('  discoro server "%s" @ %s with PID %s running %d coroutines for %s' %
                      (_discoro_config['id'], _discoro_coro.location, os.getpid(),
                       len(_discoro_job_coros), _discoro_scheduler_coro.location))
            else:
                print('  discoro server "%s" @ %s with PID %s not used by any computation' %
                      (_discoro_config['id'], _discoro_coro.location, os.getpid()))
        else:
            asyncoro.logger.warning('invalid command "%s" ignored', _discoro_req)
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, Coro):
                continue
            _discoro_client.send(-1)

    # kill any pending jobs
    while _discoro_job_coros:
        for _discoro_job_coro in _discoro_job_coros:
            _discoro_job_coro.terminate()
        asyncoro.logger.debug('%s: Waiting for %s coroutines to terminate '
                              'before closing computation',
                              _discoro_config['id'], len(_discoro_job_coros))
        if (yield _discoro_jobs_done.wait(timeout=5)):
            break
    if os.path.isfile(_discoro_pid_path):
        os.remove(_discoro_pid_path)
    asyncoro.logger.debug('discoro server %s @ %s done',
                          _discoro_config['id'], _discoro_coro.location)


def _discoro_process(_discoro_config, _discoro_mp_queue):

    import sys
    import time
    import traceback

    for _discoro_var in sys.modules.keys():
        if _discoro_var.startswith('asyncoro'):
            sys.modules.pop(_discoro_var)
    globals().pop('asyncoro', None)

    global asyncoro
    import asyncoro.disasyncoro as asyncoro

    if _discoro_config['loglevel']:
        asyncoro.logger.setLevel(asyncoro.logger.DEBUG)
        asyncoro.logger.show_ms(True)
    else:
        asyncoro.logger.setLevel(asyncoro.logger.INFO)
    del _discoro_config['loglevel']

    server_id = _discoro_config['id']
    mp_queue, _discoro_mp_queue = _discoro_mp_queue, None
    config = {}
    for _discoro_var in ['udp_port', 'tcp_port', 'node', 'ext_ip_addr', 'name', 'discover_peers',
                         'secret', 'certfile', 'keyfile', 'dest_path', 'max_file_size']:
        config[_discoro_var] = _discoro_config.pop(_discoro_var, None)

    while 1:
        try:
            _discoro_scheduler = asyncoro.AsynCoro(**config)
        except:
            print('discoro server %s failed; retrying in 5 seconds' % server_id)
            print(traceback.format_exc())
            for _discoro_var in sys.modules.keys():
                if _discoro_var.startswith('asyncoro'):
                    sys.modules.pop(_discoro_var)
            import asyncoro.disasyncoro as asyncoro
            time.sleep(5)
        else:
            break
    _discoro_coro = asyncoro.SysCoro(_discoro_server_proc)
    assert isinstance(_discoro_coro, asyncoro.Coro)
    mp_queue.put((server_id, asyncoro.serialize(_discoro_coro)))
    _discoro_coro.send(_discoro_config)

    # delete variables created in main
    for _discoro_var in globals().keys():
        if _discoro_var.startswith('_discoro_'):
            globals().pop(_discoro_var)

    _discoro_config = None
    del config, _discoro_var

    _discoro_coro.value()
    _discoro_scheduler.ignore_peers(ignore=True)
    [asyncoro.Coro(_discoro_scheduler.close_peer, location)
     for location in _discoro_scheduler.peers()]
    _discoro_scheduler.finish()
    mp_queue.put((server_id, None))
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
    import threading
    import socket
    import os
    import hashlib
    import traceback
    import re
    try:
        import readline
    except:
        pass
    import asyncoro.disasyncoro as asyncoro
    from asyncoro.discoro import MinPulseInterval, MaxPulseInterval

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', dest='config', default='',
                        help='use configuration in given file')
    parser.add_argument('--save_config', dest='save_config', default='',
                        help='save configuration in given file and exit')
    parser.add_argument('-c', '--cpus', dest='cpus', type=int, default=0,
                        help='number of CPUs/discoro instances to run; '
                        'if negative, that many CPUs are not used')
    parser.add_argument('-i', '--ip_addr', dest='node', default='',
                        help='IP address or host name of this node')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addr', default='',
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('-u', '--udp_port', dest='udp_port', type=int, default=51350,
                        help='UDP port number to use')
    parser.add_argument('--tcp_ports', dest='tcp_ports', action='append', default=[],
                        help='TCP port numbers to use')
    parser.add_argument('-n', '--name', dest='name', default='',
                        help='(symbolic) name given to AsynCoro schdulers on this node')
    parser.add_argument('--dest_path', dest='dest_path', default='',
                        help='path prefix to where files sent by peers are stored')
    parser.add_argument('--max_file_size', dest='max_file_size', default='',
                        help='maximum file size of any file transferred')
    parser.add_argument('-s', '--secret', dest='secret', default='',
                        help='authentication secret for handshake with peers')
    parser.add_argument('--certfile', dest='certfile', default='',
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default='',
                        help='file containing SSL key')
    parser.add_argument('--serve', dest='serve', default=-1, type=int,
                        help='number of clients to serve before exiting')
    parser.add_argument('--msg_timeout', dest='msg_timeout', default=asyncoro.MsgTimeout, type=int,
                        help='timeout for delivering messages')
    parser.add_argument('--min_pulse_interval', dest='min_pulse_interval',
                        default=MinPulseInterval, type=int,
                        help='minimum pulse interval clients can use in number of seconds')
    parser.add_argument('--max_pulse_interval', dest='max_pulse_interval',
                        default=MaxPulseInterval, type=int,
                        help='maximum pulse interval clients can use in number of seconds')
    parser.add_argument('--zombie_period', dest='zombie_period', default=0, type=int,
                        help='maximum number of seconds for client to not run computation')
    parser.add_argument('--ping_interval', dest='ping_interval', default=0, type=int,
                        help='interval in number of seconds for node to broadcast its address')
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

    _discoro_var = _discoro_config.pop('config')
    if _discoro_var:
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read(_discoro_var)
        cfg = dict(cfg.items('DEFAULT'))
        cfg['cpus'] = int(cfg['cpus'])
        cfg['udp_port'] = int(cfg['udp_port'])
        cfg['serve'] = int(cfg['serve'])
        cfg['msg_timeout'] = int(cfg['msg_timeout'])
        cfg['min_pulse_interval'] = int(cfg['min_pulse_interval'])
        cfg['max_pulse_interval'] = int(cfg['max_pulse_interval'])
        cfg['zombie_period'] = int(cfg['zombie_period'])
        cfg['ping_interval'] = int(cfg['ping_interval'])
        cfg['daemon'] = cfg['daemon'] == 'True'
        cfg['phoenix'] = cfg['phoenix'] == 'True'
        cfg['discover_peers'] = cfg['discover_peers'] == 'True'
        cfg['loglevel'] = cfg['loglevel'] == 'True'
        cfg['tcp_ports'] = [_discoro_var.strip()[1:-1] for _discoro_var in
                            cfg['tcp_ports'][1:-1].split(',')]
        cfg['tcp_ports'] = [_discoro_var for _discoro_var in cfg['tcp_ports'] if _discoro_var]
        cfg['peers'] = [_discoro_var.strip()[1:-1] for _discoro_var in
                        cfg['peers'][1:-1].split(',')]
        cfg['peers'] = [_discoro_var for _discoro_var in cfg['peers'] if _discoro_var]
        for key, value in _discoro_config.items():
            if _discoro_config[key] != parser.get_default(key) or key not in cfg:
                cfg[key] = _discoro_config[key]
        _discoro_config = cfg
        del key, value, cfg
    del parser

    _discoro_var = _discoro_config.pop('save_config')
    if _discoro_var:
        import configparser
        cfg = configparser.ConfigParser(_discoro_config)
        cfgfp = open(_discoro_var, 'w')
        cfg.write(cfgfp)
        cfgfp.close()
        exit(0)

    if _discoro_config['msg_timeout'] < 1:
        raise Exception('msg_timeout must be at least 1')
    if (_discoro_config['min_pulse_interval'] and
        _discoro_config['min_pulse_interval'] < _discoro_config['msg_timeout']):
        raise Exception('min_pulse_interval must be at least msg_timeout')
    if (_discoro_config['max_pulse_interval'] and _discoro_config['min_pulse_interval'] and
       _discoro_config['max_pulse_interval'] < _discoro_config['min_pulse_interval']):
        raise Exception('max_pulse_interval must be at least min_pulse_interval')
    if _discoro_config['zombie_period']:
        if _discoro_config['zombie_period'] < _discoro_config['min_pulse_interval']:
            raise Exception('zombie_period must be at least min_pulse_interval')
    else:
        _discoro_config['zombie_period'] = 0

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
            _discoro_tcp_ports.append(int(tcp_port))
        # _discoro_tcp_ports = _discoro_tcp_ports[:(_discoro_cpus + 1)]
    else:
        _discoro_tcp_ports = [0] * (_discoro_cpus + 1)
    del tcp_port, tcp_ports

    peers, _discoro_config['peers'] = _discoro_config['peers'], []
    peer = None
    for peer in peers:
        peer = peer.split(':')
        if len(peer) != 2:
            raise Exception('peer "%s" is not valid' % ':'.join(peer))
        _discoro_config['peers'].append(asyncoro.serialize(asyncoro.Location(peer[0], peer[1])))
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

    if _discoro_config['max_file_size']:
        _discoro_var = re.match(r'(\d+)([kKmMgGtT]?)', _discoro_config['max_file_size'])
        if not _discoro_var or len(_discoro_var.group(0)) != len(_discoro_config['max_file_size']):
            raise Exception('Invalid max_file_size option')
        _discoro_config['max_file_size'] = int(_discoro_var.group(1))
        if _discoro_var.group(2):
            _discoro_var = _discoro_var.group(2).lower()
            _discoro_config['max_file_size'] *= 1024**({'k': 1, 'm': 2, 'g': 3,
                                                        't': 4}[_discoro_var])
    else:
        _discoro_config['max_file_size'] = 0

    _discoro_node_auth = hashlib.sha1(os.urandom(10).encode('hex')).hexdigest()

    # delete variables not needed anymore
    for _discoro_var in ['argparse', 'socket']:
        del sys.modules[_discoro_var], globals()[_discoro_var]
    del _discoro_var

    class _discoro_Struct(object):

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def __setattr__(self, name, value):
            if hasattr(self, name):
                self.__dict__[name] = value
            else:
                raise AttributeError('Invalid attribute "%s"' % name)

    _discoro_busy_time = multiprocessing.Value('I', 0)
    _discoro_mp_queue = multiprocessing.Queue()
    _discoro_servers = [None] * (_discoro_cpus + 1)
    _discoro_server_locations = {}
    for _discoro_server_id in range(1, _discoro_cpus + 1):
        _discoro_server = _discoro_Struct(id=_discoro_server_id, proc=None, coro=None, auth=None,
                                          name='%s-%s' % (_discoro_name, _discoro_server_id))
        _discoro_servers[_discoro_server_id] = _discoro_server

    def _discoro_node_proc(coro=None):
        import os
        try:
            import psutil
        except ImportError:
            print('\n   \'psutil\' module is not available; '
                  'CPU, memory, disk status will not be sent!\n')
            psutil = None
        else:
            psutil.cpu_percent(0.1)
        from asyncoro.discoro import DiscoroNodeAvailInfo, DiscoroNodeInfo, MaxPulseInterval
        global _discoro_servers, _discoro_config
        # coro.set_daemon()
        coro_scheduler = asyncoro.AsynCoro.instance()
        last_pulse = last_proc_check = last_ping = time.time()
        scheduler_coro = zombie_period = cur_computation_auth = None
        if _discoro_config['max_pulse_interval']:
            MaxPulseInterval = _discoro_config['max_pulse_interval']
        interval = MaxPulseInterval
        ping_interval = _discoro_config.pop('ping_interval')
        msg_timeout = _discoro_config['msg_timeout']
        disk_path = coro_scheduler.dest_path
        coro.register('discoro_node')

        def _discoro_start_server(server, phoenix=False):
            if not scheduler_coro:
                return
            if server.proc:
                if server.proc.is_alive():
                    asyncoro.logger.warning('discoro server %s is still running, not starting it',
                                            server.proc.pid)
                    return
                pid_path = os.path.join(_discoro_scheduler.dest_path, '%s.pid' % server.id)
                if os.path.exists(pid_path):
                    try:
                        os.remove(pid_path)
                    except:
                        print('Could not remove PID file %s' % pid_path)
                        return

            server_config = dict(_discoro_config)
            server_config['id'] = server.id
            server_config['name'] = '%s-%s' % (_discoro_name, server.id)
            server_config['node'] = _discoro_node_coro.location.addr
            server_config['tcp_port'] = _discoro_tcp_ports[server.id]
            server_config['node_coro'] = asyncoro.serialize(_discoro_node_coro)
            server_config['node_auth'] = _discoro_node_auth
            server_config['phoenix'] = phoenix
            server_config['busy_time'] = _discoro_busy_time
            server_config['peers'] = _discoro_config['peers'][:]
            server_config['scheduler_coro'] = asyncoro.serialize(scheduler_coro)
            server.proc = multiprocessing.Process(target=_discoro_process,
                                                  name=server_config['name'],
                                                  args=(server_config, _discoro_mp_queue))
            _discoro_servers[server.id] = server
            server.proc.start()
            asyncoro.logger.debug('discoro server %s started with PID %s',
                                  server.id, server.proc.pid)
            return

        def monitor_peers(coro=None):
            coro.set_daemon()
            while 1:
                msg = yield coro.receive()
                if not isinstance(msg, asyncoro.PeerStatus):
                    continue
                if msg.status == asyncoro.PeerStatus.Offline:
                    if (scheduler_coro and scheduler_coro.location == msg.location):
                        _discoro_node_coro.send({'req': 'release', 'auth': cur_computation_auth})
            raise StopIteration

        def mp_queue_server():
            while 1:
                proc_id, proc_coro = _discoro_mp_queue.get()
                if not proc_id:
                    break
                _discoro_node_coro.send({'req': 'server_status', 'auth': _discoro_node_auth,
                                         'id': proc_id, 'coro': proc_coro})
            _discoro_mp_queue.close()
            return

        qserver = threading.Thread(target=mp_queue_server)
        qserver.daemon = True
        qserver.start()
        coro_scheduler.peer_status(asyncoro.Coro(monitor_peers))
        coro_scheduler.discover_peers()
        for peer in _discoro_config['peers']:
            asyncoro.Coro(coro_scheduler.peer, asyncoro.deserialize(peer))

        while 1:
            msg = yield coro.receive(timeout=interval)
            now = time.time()
            if msg:
                try:
                    req = msg['req']
                except:
                    req = ''

                if req == 'server_status':
                    auth = msg.get('auth', None)
                    if auth == _discoro_node_auth:
                        server = _discoro_servers[msg['id']]
                        if msg['coro']:
                            server.coro = asyncoro.deserialize(msg['coro'])
                            _discoro_server_locations[server.coro.location] = server
                            # if scheduler_coro and cur_computation_auth:
                            #     server.coro.send({'req': 'peer', 'node_auth': _discoro_node_auth})
                        else:
                            _discoro_server_locations.pop(server.coro.location, None)
                            server.coro = server.proc = server.auth = None
                            if _discoro_config['serve']:
                                if scheduler_coro:
                                    _discoro_start_server(server)
                            elif all(not server or not server.proc for server in _discoro_servers):
                                _discoro_mp_queue.put((None, None))
                                break

                elif req == 'server_setup':
                    auth = msg.get('auth', None)
                    new_computation = False
                    if cur_computation_auth:
                        if cur_computation_auth == auth:
                            pass
                        elif all(not server.auth for server in _discoro_servers if server):
                            new_computation = True
                            cur_computation_auth = auth
                        else:
                            asyncoro.logger.warning('Invalid computation authentication: %s != %s',
                                                    cur_computation_auth, auth)
                    else:
                        if not cur_computation_auth:
                            new_computation = True
                            cur_computation_auth = auth

                    try:
                        server = _discoro_servers[msg['id']]
                        server.auth = auth
                    except:
                        pass
                    else:
                        if not server.coro:
                            server.coro = msg['coro']
                            _discoro_server_locations[server.coro.location] = server
                        if new_computation:
                            if _discoro_config['serve'] > 0:
                                _discoro_config['serve'] -= 1
                        interval = msg.get('interval', None)
                        zombie_period = msg.get('zombie_period', None)
                        if zombie_period:
                            zombie_period *= 3
                        last_pulse = now

                elif req == 'reserve':
                    # request from scheduler
                    client = msg.get('client', None)
                    if (isinstance(client, asyncoro.Coro) and _discoro_config['serve'] and
                        not scheduler_coro and
                        isinstance(msg.get('status_coro', None), asyncoro.Coro)):
                        if psutil:
                            info = DiscoroNodeAvailInfo(coro.location.addr,
                                                        100.0 - psutil.cpu_percent(),
                                                        psutil.virtual_memory().available,
                                                        psutil.disk_usage(disk_path).free,
                                                        100.0 - psutil.swap_memory().percent)
                        else:
                            info = None
                        info = {'servers': [server.coro.location for server in _discoro_servers
                                            if server and server.coro and not server.auth],
                                'info': DiscoroNodeInfo(_discoro_name, coro.location.addr, info)}
                        if (yield client.deliver(info, timeout=min(msg_timeout, interval))) == 1:
                            scheduler_coro = msg['status_coro']
                            for server in _discoro_servers:
                                if server and not server.proc:
                                    _discoro_start_server(server)
                            _discoro_busy_time.value = int(time.time())
                    else:
                        if isinstance(client, asyncoro.Coro):
                            client.send(None)

                elif req == 'release':
                    auth = msg.get('auth', None)
                    if auth == cur_computation_auth:
                        cur_computation_auth = None
                        scheduler_coro = None
                        interval = MaxPulseInterval
                        ack = 'closed'
                    else:
                        ack = 'ignored'
                    client = msg.get('client', None)
                    if isinstance(client, asyncoro.Coro):
                        client.send(ack)

                elif req == 'quit' or req == 'terminate':
                    auth = msg.get('auth', None)
                    if auth == _discoro_node_auth:
                        _discoro_config['serve'] = 0
                        for server in _discoro_servers:
                            if server and server.coro:
                                server.coro.send({'req': req, 'node_auth': _discoro_node_auth})
                        if all(not server or not server.proc for server in _discoro_servers):
                            _discoro_mp_queue.put((None, None))
                            break

                else:
                    asyncoro.logger.warning('Invalid message ignored')

            if scheduler_coro:
                scoro = scheduler_coro  # copy in case scheduler closes meanwhile
                msg = {'location': coro.location}
                if psutil:
                    msg['node_status'] = DiscoroNodeAvailInfo(
                        coro.location.addr, 100.0 - psutil.cpu_percent(),
                        psutil.virtual_memory().available, psutil.disk_usage(disk_path).free,
                        100.0 - psutil.swap_memory().percent)

                sent = yield scoro.deliver(msg, timeout=msg_timeout)
                if sent == 1:
                    last_pulse = now
                elif (now - last_pulse) > (5 * interval):
                    asyncoro.logger.warning('Scheduler is not reachable; closing computation "%s"',
                                            cur_computation_auth)
                    for server in _discoro_servers:
                        if server and server.coro:
                            server.coro.send({'req': 'quit', 'node_auth': _discoro_node_auth})
                    asyncoro.Coro(coro_scheduler.close_peer, scoro.location)
                    scheduler_coro = None
                    # cur_computation_auth = None

                if (zombie_period and ((now - _discoro_busy_time.value) > zombie_period) and
                    cur_computation_auth):
                    asyncoro.logger.warning('Closing zombie computation "%s"', cur_computation_auth)
                    for server in _discoro_servers:
                        if server and server.coro:
                            server.coro.send({'req': 'quit', 'node_auth': _discoro_node_auth})
                    # asyncoro.Coro(coro_scheduler.close_peer, scoro.location)
                    # scheduler_coro = None
                    # cur_computation_auth = None

                if (now - last_proc_check) > MaxPulseInterval:
                    last_proc_check = now
                    for server in _discoro_servers:
                        if not server or (server.proc and server.proc.is_alive()):
                            continue
                        asyncoro.logger.warning('Process %s is dead!', server.id)
                        server.proc = None
                        if _discoro_config['serve'] and cur_computation_auth:
                            _discoro_start_server(server, phoenix=True)

            if ping_interval and (now - last_ping) > ping_interval:
                coro_scheduler.discover_peers()

    _discoro_server_config = {}
    for _discoro_var in ['udp_port', 'tcp_port', 'node', 'ext_ip_addr', 'name',
                         'discover_peers', 'secret', 'certfile', 'keyfile', 'dest_path',
                         'max_file_size']:
        _discoro_server_config[_discoro_var] = _discoro_config.get(_discoro_var, None)
    _discoro_server_config['discover_peers'] = False
    _discoro_server_id = 0
    _discoro_server_config['name'] = '%s-%s' % (_discoro_name, _discoro_server_id)
    _discoro_server_config['tcp_port'] = int(_discoro_tcp_ports[_discoro_server_id])
    if _discoro_config['loglevel']:
        asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
        asyncoro.logger.show_ms(True)
    else:
        asyncoro.logger.setLevel(asyncoro.Logger.INFO)
    _discoro_scheduler = asyncoro.AsynCoro(**_discoro_server_config)
    _discoro_scheduler.dest_path = os.path.join(_discoro_scheduler.dest_path, 'discoro')
    _discoro_node_coro = asyncoro.Coro(_discoro_node_proc)

    del _discoro_server_config

    if not _discoro_daemon:
        def _discoro_cmd_reader(coro=None):
            coro.set_daemon()
            async_threads = asyncoro.AsyncThreadPool(1)
            while 1:
                yield coro.sleep(0.25)
                try:
                    _discoro_cmd = yield async_threads.async_task(
                        raw_input,
                        '\nEnter\n'
                        '  "status" to get status\n'
                        '  "close" to stop accepting new jobs and\n'
                        '          close current computation when current jobs are finished\n'
                        '  "quit" to "close" current computation and exit discoronode\n'
                        '  "terminate" to kill current jobs and "quit": ')
                except GeneratorExit:
                    break
                except:
                    _discoro_cmd = ''
                else:
                    _discoro_cmd = _discoro_cmd.strip().lower()
                    if not _discoro_cmd:
                        _discoro_cmd = 'status'

                print('')
                if _discoro_cmd == 'status' or _discoro_cmd == 'close':
                    for _discoro_server in _discoro_servers:
                        if not _discoro_server:
                            continue
                        if _discoro_server.coro:
                            _discoro_server.coro.send({'req': _discoro_cmd,
                                                       'node_auth': _discoro_node_auth})
                        else:
                            print('  discoro server "%s" is not currently used' %
                                  _discoro_server.name)
                elif _discoro_cmd in ('quit', 'terminate'):
                    _discoro_node_coro.send({'req': _discoro_cmd, 'auth': _discoro_node_auth})
                    break
                else:
                    for _discoro_server in _discoro_servers:
                        if (_discoro_server and _discoro_server.proc and
                            _discoro_server.proc.is_alive()):
                            print('  Process %s is still running' % _discoro_server.id)

        asyncoro.Coro(_discoro_cmd_reader)

    _discoro_node_coro.value()

    exit(0)
