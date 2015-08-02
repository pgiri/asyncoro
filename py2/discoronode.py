#!/usr/bin/python

"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.

This program can be used to start discoro server processes so discoro
scheduler (see 'discoro.py') can send computations to these processes
for executing distributed communicating proceses (coroutines). All
coroutines in a process execute in the same thread in a process, so
multiple CPUs are not used by one process. If CPU intensive
computations are to be run on systems with multiple processors, then
this program should be run with multiple instances (see below for '-c'
option to this program).

See 'discoro_client.py' for an example.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2014 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

import os
import sys
import traceback
import time
import shutil

import asyncoro.disasyncoro as asyncoro

__all__ = ['discoro_proc']


def discoro_proc():
    # coroutine
    """Server process receives computations and runs coroutines for it.
    """

    import asyncoro.disasyncoro as asyncoro
    from asyncoro import Coro
    from asyncoro.discoro import MinPulseInterval, MaxPulseInterval

    class Nonlocals(object):
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    _discoro_coro = asyncoro.AsynCoro.cur_coro()
    _discoro_coro.register('discoro_proc')
    _discoro_name = asyncoro.AsynCoro.instance().name
    asyncoro.AsynCoro.instance().dest_path = os.path.join(
        'discoro', 'proc%s' % (_discoro_name[_discoro_name.rindex('-'):]))
    _discoro_dest_path = asyncoro.AsynCoro.instance().dest_path
    _discoro_pid_path = os.path.join(_discoro_dest_path, '..', 'proc%s.pid' %
                                     _discoro_name[_discoro_name.rindex('-'):])
    _discoro_pid_path = os.path.normpath(_discoro_pid_path)
    # TODO: is file locking necessary?
    if os.path.exists(_discoro_pid_path):
        pid = open(_discoro_pid_path, 'r').read()
        print('\n   Another discoronode seems to be running;\n'
              '   make sure process with ID %s quit and remove "%s"\n' % (pid, _discoro_pid_path))
        import signal
        os.kill(os.getpid(), signal.SIGTERM)
    if os.path.isdir(_discoro_dest_path):
        shutil.rmtree(_discoro_dest_path)
    os.makedirs(_discoro_dest_path)
    os.chdir(_discoro_dest_path)
    with open(_discoro_pid_path, 'w') as _discoro_var:
        _discoro_var.write('%s' % os.getpid())
    asyncoro.logger.debug('discoro process "%s" started at %s; '
                          'computation files will be saved in "%s"' %
                          (_discoro_name, _discoro_coro.location, _discoro_dest_path))
    _discoro_req = _discoro_client = _discoro_auth = _discoro_msg = None
    _discoro_timer_coro = _discoro_pulse_coro = _discoro_timer_proc = _discoro_peer_status = None
    _discoro_monitor_coro = _discoro_monitor_proc = None
    _discoro_computation = _discoro_func = _discoro_var = None
    _discoro_job_coros = set()
    _discoro_nonlocals = Nonlocals(busy_time=time.time())
    _discoro_globals = {}
    _discoro_locals = {}
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
            if _discoro_pulse_coro.send(msg) == 0:
                last_pulse = time.time()
            elif (time.time() - last_pulse) > (5 * interval) and _discoro_computation:
                asyncoro.logger.warning('scheduler is not reachable; closing computation "%s"' %
                                        _discoro_computation._auth)
                _discoro_coro.send({'req': 'close', 'auth': _discoro_computation._auth})

            if ((not _discoro_job_coros) and _discoro_computation.zombie_period and
               ((time.time() - _discoro_nonlocals.busy_time) > _discoro_computation.zombie_period)):
                asyncoro.logger.debug('%s: closing zombie computation "%s"' %
                                      (coro.location, _discoro_computation._auth))
                _discoro_pulse_coro.send({'status': 'ProcClosed', 'location': coro.location})

    def _discoro_peer_status(coro=None):
        coro.set_daemon()
        while True:
            status = yield coro.receive()
            if isinstance(status, asyncoro.PeerStatus):
                if _discoro_computation and \
                   _discoro_computation.scheduler.location == status.location:
                    asyncoro.logger.debug('scheduler at %s quit; closing computation %s' %
                                          (status.location, _discoro_computation._auth))
                    msg = {'req': 'close', 'auth': _discoro_computation._auth}
                    _discoro_coro.send(msg)

    def _discoro_monitor_proc(coro=None):
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            if isinstance(msg, asyncoro.MonitorException):
                _discoro_nonlocals.busy_time = time.time()
                asyncoro.logger.debug('job %s done' % msg.args[0])
                _discoro_job_coros.discard(msg.args[0])
            else:
                asyncoro.logger.warning('%s: invalid monitor message ignored' % coro.location)

    _discoro_timer_coro = Coro(_discoro_timer_proc)
    _discoro_monitor_coro = Coro(_discoro_monitor_proc)
    asyncoro.AsynCoro.instance().peer_status(Coro(_discoro_peer_status))

    while True:
        _discoro_msg = yield _discoro_coro.receive()
        try:
            _discoro_req = _discoro_msg['req']
        except:
            asyncoro.logger.debug(traceback.format_exc())
            _discoro_req = None

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
                    exec(_discoro_func.code) in globals()
                job_coro = Coro(globals()[_discoro_func.name],
                                *(_discoro_func.args), **(_discoro_func.kwargs))
            except:
                asyncoro.logger.debug('invalid computation to run')
                # _discoro_func = Scheduler._Function(_discoro_func.name, None,
                #                                     _discoro_func.args, _discoro_func.kwargs)
                job_coro = (sys.exc_info()[0], _discoro_func, traceback.format_exc())
            else:
                asyncoro.logger.debug('job %s created' % job_coro)
                _discoro_job_coros.add(job_coro)
                job_coro.notify(_discoro_monitor_coro)
                _discoro_var = _discoro_msg.get('notify', None)
                if isinstance(_discoro_var, Coro):
                    job_coro.notify(_discoro_var)
            _discoro_nonlocals.busy_time = time.time()
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
                exec(_discoro_computation._code) in globals()
            except:
                _discoro_computation = None
                asyncoro.logger.warning('invalid computation')
                asyncoro.logger.debug(traceback.format_exc())
                _discoro_client.send(-1)
                continue
            if isinstance(_discoro_computation.pulse_interval, int) and \
               MinPulseInterval <= _discoro_computation.pulse_interval <= MaxPulseInterval:
                _discoro_computation.pulse_interval = _discoro_computation.pulse_interval
            else:
                _discoro_computation.pulse_interval = MinPulseInterval
            _discoro_timer_coro.resume(_discoro_computation.pulse_interval)
            _discoro_nonlocals.busy_time = time.time()
            asyncoro.logger.debug('computation "%s" from %s' %
                                  (_discoro_computation._auth, _discoro_msg['client'].location))
            _discoro_client.send(0)
        elif _discoro_req == 'close':
            _discoro_auth = _discoro_msg.get('auth', None)
            if not _discoro_computation or _discoro_auth != _discoro_computation._auth:
                continue
            asyncoro.logger.debug('%s deleting computation "%s"' %
                                  (_discoro_coro.location, _discoro_computation._auth))
            # TODO: is it better to quit this process and start another?
            for _discoro_var in _discoro_job_coros:
                _discoro_var.terminate()
            _discoro_job_coros = set()
            for _discoro_var in list(globals()):
                if _discoro_var not in _discoro_globals:
                    # asyncoro.logger.warning('Removing global variable "%s"' % _discoro_var)
                    globals().pop(_discoro_var, None)
            globals().update(_discoro_globals)
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
            _discoro_timer_coro.resume(MinPulseInterval)
        elif _discoro_req == 'quit':
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
        try:
            _discoro_req = _discoro_msg['req']
        except:
            if isinstance(_discoro_msg, asyncoro.MonitorException):
                asyncoro.logger.debug('job %s done' % _discoro_msg.args[0])
                _discoro_job_coros.discard(_discoro_msg.args[0])
                continue
            else:
                asyncoro.logger.debug(traceback.format_exc())
            _discoro_req = None
        if _discoro_req == 'close':
            _discoro_auth = _discoro_msg.get('auth', None)
            if not _discoro_computation or _discoro_auth != _discoro_computation._auth:
                continue
            asyncoro.logger.debug('%s deleting computation "%s"' %
                                  (_discoro_coro.location, _discoro_computation._auth))
            for _discoro_var in list(globals()):
                if _discoro_var not in _discoro_globals:
                    # asyncoro.logger.warning('Removing global variable "%s"' % _discoro_var)
                    globals().pop(_discoro_var, None)
            # for _discoro_var in list(locals()):
            #     if _discoro_var not in _discoro_locals:
            #         locals().pop(_discoro_var, None)
            break
        else:
            asyncoro.logger.warning('invalid command "%s" ignored' % _discoro_req)
            _discoro_client = _discoro_msg.get('client', None)
            if not isinstance(_discoro_client, Coro):
                continue
            _discoro_client.send(-1)
    if os.path.isfile(_discoro_pid_path):
        os.remove(_discoro_pid_path)
    os.chdir(_discoro_dest_path)
    # shutil.rmtree(_discoro_dest_path, ignore_errors=True)


def _discoro_process(_discoro_config, _discoro_queue):
    _discoro_scheduler = asyncoro.AsynCoro(**_discoro_config)
    _discoro_queue.put(asyncoro.serialize(asyncoro.Coro(discoro_proc)))
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

    import logging
    import argparse
    import multiprocessing
    import socket

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
    if not _discoro_name:
        _discoro_name = socket.gethostname()
        if not _discoro_name:
            _discoro_name = 'discoro_proc'

    _discoro_queue = multiprocessing.Queue()
    _discoro_processes = []
    for _discoro_proc_id in range(1, _discoro_cpus):
        _discoro_config['name'] = _discoro_name + '-%s' % _discoro_proc_id
        _discoro_processes.append(multiprocessing.Process(target=_discoro_process,
                                                          args=(_discoro_config, _discoro_queue)))
        _discoro_processes[-1].start()
        time.sleep(0.05)
    _discoro_proc_id = _discoro_cpus
    _discoro_config['name'] = _discoro_name + '-%s' % _discoro_proc_id
    _discoro_scheduler = asyncoro.AsynCoro(**_discoro_config)
    _discoro_coro = asyncoro.Coro(discoro_proc)

    while True:
        try:
            if sys.stdin.readline().strip().lower() in ('quit', 'exit'):
                break
        except KeyboardInterrupt:
            break

    asyncoro.logger.debug('terminating processes')
    # make sure all processes quit; otherwise, multiple keyboard
    # interrupts can leave processes hanging, with unpredictable behavior
    while True:
        try:
            while not _discoro_queue.empty():
                asyncoro.unserialize(_discoro_queue.get()).send({'req': 'quit'})
            _discoro_coro.send({'req': 'quit'})

            for _discoro_proc_id, _discoro_proc in enumerate(_discoro_processes, start=1):
                if _discoro_proc.is_alive():
                    asyncoro.logger.info('  -- waiting for process %s to finish' % _discoro_proc_id)
                    _discoro_proc.join()
            break
        except KeyboardInterrupt:
            print('waiting for processes to quit')
            continue
