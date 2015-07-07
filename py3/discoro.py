#!/usr/bin/python

"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.

This module provides API for distributing generator functions and
their dependencies (files, Python functions, classes) to create remote
coroutines for distributed communicating processes. This should be
used with 'discoronode.py' program.

See 'discoro_client.py' for an example.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2014-2015 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

import os
import sys
import inspect
import hashlib
import collections
import time
import shutil
import atexit

import asyncoro.disasyncoro as asyncoro
from asyncoro import Coro, logger

__all__ = ['Scheduler', 'Computation']

MsgTimeout = 10
MinPulseInterval = MsgTimeout
MaxPulseInterval = 10 * MinPulseInterval

# status about nodes / processes are sent with this structure
StatusMessage = collections.namedtuple('StatusMessage', ['status', 'location'])

# for internal use
_Function = collections.namedtuple('_Function', ['name', 'code', 'args', 'kwargs'])


class Scheduler(object):

    # status indications ('status' attribute of StatusMessage)
    NodeDiscovered = 1
    NodeInitialized = 2
    NodeClosed = 3
    NodeIgnore = 4
    NodeDisconnected = 5
    ProcDiscovered = 11
    ProcInitialized = 12
    ProcClosed = 13
    ProcIgnore = 14
    ProcDisconnected = 15

    __metaclass__ = asyncoro.MetaSingleton

    class _Node(object):

        def __init__(self, addr, scheduler):
            self.addr = addr
            self.procs = {}
            self.ncoros = 0
            self.status = Scheduler.NodeDiscovered
            self.scheduler = scheduler

        def run(self, func, client):
            if self.status != Scheduler.NodeInitialized:
                client.send(None)
                raise StopIteration
            node = self.scheduler._nodes.get(self.addr, None)
            where = None
            load = None
            for proc in node.procs.values():
                if proc.status != Scheduler.ProcInitialized:
                    continue
                if load is None or len(proc.coros) < load:
                    where = proc
                    load = len(proc.coros)
            if where:
                logger.debug('running on %s' % self.addr)
                yield where.run(func, client)
            else:
                client.send(None)

    class _Proc(object):

        def __init__(self, name, location, scheduler):
            self.name = name
            self.location = location
            self.coros = {}
            self.done = {}
            self.status = Scheduler.ProcDiscovered
            self.server = None
            self.xfer_files = []
            self.last_pulse = time.time()
            self.scheduler = scheduler

        def run(self, func, client):
            if self.status != Scheduler.ProcInitialized:
                raise StopIteration(None)
            node = self.scheduler._nodes.get(self.location.addr, None)
            computation = self.scheduler._cur_computation
            assert isinstance(func, _Function)
            if not node or not computation:
                raise StopIteration(None)

            def _run(self, func, coro=None):
                self.server.send({'req': 'run', 'auth': computation._auth, 'func': func,
                                  'client': coro, 'notify': self.scheduler._status_coro})
                rcoro = yield coro.receive(timeout=computation.timeout)
                if isinstance(rcoro, Coro):
                    done = self.done.pop(str(rcoro), None)
                    if done is None:
                        # TODO: keep func too for fault-tolerance
                        self.coros[str(rcoro)] = rcoro
                        node.ncoros += 1
                    else:
                        rcoro = done
                raise StopIteration(rcoro)

            rcoro = yield Coro(_run, self, func).finish()
            yield client.deliver(rcoro)

    def __init__(self, **kwargs):
        self._nodes = {}
        self._cur_computation = None
        self.__cur_client_auth = None
        self.__pulse_interval = MinPulseInterval
        self.__sched_event = asyncoro.Event()
        self.__terminate = False

        kwargs['name'] = 'discoro_scheduler'
        self.asyncoro = asyncoro.AsynCoro.instance(**kwargs)
        self.__dest_path = os.path.join(self.asyncoro.dest_path, 'discoro', 'scheduler')
        if not os.path.isdir(self.__dest_path):
            os.makedirs(self.__dest_path)
        self.asyncoro.dest_path = self.__dest_path
        self.__scheduler_coro = Coro(self.scheduler_proc)
        self.__client_coro = Coro(self.client_proc)
        self.__timer_coro = Coro(self.timer_proc)
        self._status_coro = Coro(self.status_proc)
        self.asyncoro.peer_status(self._status_coro)

    def status_proc(self, coro=None):
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            if isinstance(msg, asyncoro.MonitorException):
                rcoro = msg.args[0]
                node = self._nodes.get(rcoro.location.addr, None)
                if not node:
                    logger.warning('node %s is invalid' % rcoro.location.addr)
                    continue
                proc = node.procs.get(str(rcoro.location), None)
                if not proc:
                    for proc in node.procs.values():
                        logger.warning('proc "%s" is invalid: "%s"' %
                                       (rcoro.location, proc.location))
                    continue
                if proc.coros.pop(str(rcoro), None) is None:
                    logger.warning('rcoro "%s" is invalid at "%s"' % (rcoro, proc.location))
                    proc.done[str(rcoro)] = msg
                    continue
                if self._cur_computation and self._cur_computation.status_coro:
                    self._cur_computation.status_coro.send(msg)
                node.ncoros -= 1

            elif isinstance(msg, asyncoro.PeerStatus):
                computation = self._cur_computation
                if msg.status:  # peer came online
                    proc = Scheduler._Proc(msg.name, msg.location, self)
                    node = self._nodes.get(msg.location.addr, None)
                    if node:
                        node.procs[str(msg.location)] = proc
                        if computation:
                            Coro(self.__setup_proc, proc, computation)
                    else:
                        node = Scheduler._Node(msg.location.addr, self)
                        self._nodes[msg.location.addr] = node
                        node.procs[str(msg.location)] = proc
                        if computation:
                            if computation.status_coro:
                                status_msg = StatusMessage(Scheduler.NodeDiscovered,
                                                           msg.location.addr)
                                computation.status_coro.send(status_msg)
                            Coro(self.__setup_node, node, computation)
                    if computation and computation.status_coro:
                        status_msg = StatusMessage(Scheduler.ProcDiscovered, msg.location)
                        computation.status_coro.send(status_msg)
                else:  # peer terminated
                    node = self._nodes.get(msg.location.addr, None)
                    if node:
                        proc = node.procs.pop(str(msg.location), None)
                        if proc:
                            # TODO: (re)start process elsewhere (fault-tolerant)?
                            Coro(self.__close_proc, proc)
                            if computation and computation.status_coro:
                                status_msg = StatusMessage(Scheduler.ProcDiscovered, msg.location)
                                computation.status_coro.send(status_msg)
                        if not node.procs:
                            Coro(self.__close_node, node)
                            self._nodes.pop(msg.location.addr, None)
                            if computation and computation.status_coro:
                                status_msg = StatusMessage(Scheduler.NodeDisconnected,
                                                           msg.location.addr)
                                computation.status_coro.send(status_msg)
                    elif computation and msg.location == computation._pulse_coro.location:
                        logger.warning('client %s terminated; closing computation %s' %
                                       (msg.location, self.__cur_client_auth))
                        Coro(self.__close_computation)
            else:
                logger.warning('invalid status message ignored')

    def timer_proc(self, coro=None):
        coro.set_daemon()
        client_pulse = time.time()
        proc_check = time.time()
        while True:
            msg = yield coro.receive(timeout=self.__pulse_interval)
            now = time.time()
            if isinstance(msg, dict):  # message from a node's proc
                ncoros = msg.get('ncoros', -1)
                if ncoros >= 0:
                    loc = msg.get('location', None)
                    if isinstance(loc, asyncoro.Location):
                        node = self._nodes.get(loc.addr, None)
                        if node:
                            proc = node.procs.get(str(loc), None)
                            if proc:
                                proc.last_pulse = now
                                if ncoros != len(proc.coros):
                                    logger.debug('process %s running %s coroutines, '
                                                 'scheduler running %s' %
                                                 (proc.location, ncoros, len(proc.coros)))
            if (now - client_pulse) > self.__pulse_interval and self._cur_computation:
                if self._cur_computation._pulse_coro.send('pulse') == 0:
                    client_pulse = now
                else:
                    if (now - client_pulse) > (5 * self.__pulse_interval):
                        logger.debug('client %s not responding; closing it' %
                                     self.__cur_client_auth)
                        Coro(self.__close_computation)

            if (now - proc_check) > (5 * self.__pulse_interval):
                for node in self._nodes.values():
                    if node.status != Scheduler.NodeInitialized:
                        continue
                    for proc in node.procs.values():
                        if proc.status != Scheduler.ProcInitialized:
                            continue
                        if (now - proc.last_pulse) > (5 * self.__pulse_interval):
                            logger.warning('Process %s is zombie!' % proc.location)
                            Coro(self.__close_proc, proc)

    def __run(self, func, client):
        host = None
        load = None
        for node in self._nodes.values():
            if node.status != Scheduler.NodeInitialized:
                continue
            node_load = float(node.ncoros) / len(node.procs)
            if load is None or node_load < load:
                host = node
                load = node_load
        if host:
            yield host.run(func, client)
        else:
            yield None

    @staticmethod
    def auth_code():
        # TODO: use uuid?
        return hashlib.sha1(bytes(''.join(hex(x)[2:] for x in os.urandom(10)), 'ascii')).hexdigest()

    def scheduler_proc(self, coro=None):
        coro.set_daemon()
        while not self.__terminate:
            if self._cur_computation:
                self.__sched_event.clear()
                yield self.__sched_event.wait()
                continue
            self._cur_computation, client = yield coro.receive()

            logger.debug('Computation %s scheduled' % self._cur_computation._auth)
            if isinstance(self._cur_computation.pulse_interval, int) and \
               MinPulseInterval <= self._cur_computation.pulse_interval <= MaxPulseInterval:
                self.__pulse_interval = self._cur_computation.pulse_interval
            else:
                self.__pulse_interval = MinPulseInterval

            self.__cur_client_auth = self._cur_computation._auth
            self._cur_computation._auth = Scheduler.auth_code()
            msg = {'resp': 'scheduled', 'auth': self.__cur_client_auth}
            if (yield client.deliver(msg, timeout=self._cur_computation.timeout)) != 1:
                logger.warning('client not reachable?')
                self._cur_client_auth = None
                self._cur_computation = None
                continue

            for node in self._nodes.values():
                for i, proc in enumerate(node.procs.values()):
                    if i == 0:
                        node.status = Scheduler.NodeDiscovered
                        if self._cur_computation.status_coro:
                            status_msg = StatusMessage(Scheduler.NodeDiscovered, proc.location.addr)
                            self._cur_computation.status_coro.send(status_msg)
                    proc.status = Scheduler.ProcDiscovered
                    if self._cur_computation.status_coro:
                        status_msg = StatusMessage(Scheduler.ProcDiscovered, proc.location)
                        self._cur_computation.status_coro.send(status_msg)
            for node in self._nodes.values():
                # TODO: check if node is allowed
                if self._cur_computation:
                    Coro(self.__setup_node, node, self._cur_computation)


    def client_proc(self, coro=None):
        coro.set_daemon()
        coro.register('discoro_scheduler')
        computations = {}
        while not self.__terminate:
            msg = yield coro.receive()
            req = msg.get('req', None)
            client = msg.get('client', None)
            auth = msg.get('auth', None)
            if not isinstance(client, Coro):
                logger.warning('Ignoring invalid client request "%s"' % req)
                continue

            if req == 'run':
                func = msg.get('func', None)
                if not func or self.__cur_client_auth != auth:
                    logger.warning('Ignoring invalid request to run computation')
                    client.send(None)
                    continue
                where = msg.get('where', None)
                if not where:
                    Coro(self.__run, func, client)
                elif isinstance(where, str):
                    node = self._nodes.get(where, None)
                    if node:
                        Coro(node.run, func, client)
                    else:
                        client.send(None)
                elif isinstance(where, asyncoro.Location):
                    node = self._nodes.get(where.addr)
                    if node:
                        proc = node.procs.get(str(where))
                        if proc:
                            Coro(proc.run, func, client)
                        else:
                            client.send(None)
                    else:
                        client.send(None)
                else:
                    client.send(None)

            elif req == 'run_each':
                where = msg.get('where', None)
                func = msg.get('func', None)
                if not func or self.__cur_client_auth != auth:
                    logger.warning('Ignoring invalid request to run computation')
                    where = None
                if where == 'node':
                    n = sum(1 for node in self._nodes.values()
                            if node.status == Scheduler.NodeInitialized)
                    # TODO: use short timeout in case computation's timeout is too big?
                    if (yield client.deliver(n, self._cur_computation.timeout)) != 1:
                        continue
                    for node in self._nodes.values():
                        Coro(node.run, func, client)
                elif where == 'proc':
                    n = sum(1 for node in self._nodes.values()
                            if node.status == Scheduler.NodeInitialized
                            for proc in node.procs.values()
                            if proc.status == Scheduler.ProcInitialized)
                    # TODO: use short timeout in case computation's timeout is too big?
                    if (yield client.deliver(n, self._cur_computation.timeout)) != 1:
                        continue
                    for node in self._nodes.values():
                        for proc in node.procs.values():
                            Coro(proc.run, func, client)
                else:
                    # node_procs
                    node = self._nodes.get(where)
                    if node and node.status == Scheduler.NodeInitialized:
                        n = sum(1 for proc in node.procs.values()
                                if proc.status == Scheduler.ProcInitialized)
                    else:
                        n = 0
                    if (yield client.deliver(n, self._cur_computation.timeout)) != 1:
                        continue
                    if n > 0:
                        for proc in node.procs.values():
                            Coro(proc.run, func, client)

            elif req == 'schedule':
                try:
                    computation = asyncoro.unserialize(msg['computation'])
                    assert isinstance(computation, Computation)
                    assert isinstance(computation._pulse_coro, Coro)
                    if computation._pulse_coro.location == self.asyncoro.location:
                        computation._pulse_coro._id = int(computation._pulse_coro._id)
                        if computation.status_coro:
                            computation.status_coro._id = int(computation.status_coro._id)
                except:
                    logger.warning('ignoring invalid computation request')
                    client.send(None)
                    continue
                computation._auth = Scheduler.auth_code()
                try:
                    os.mkdir(os.path.join(self.__dest_path, computation._auth))
                except:
                    logger.debug('Could not create "%s"' %
                                 os.path.join(self.__dest_path, computation._auth))
                    client.send(None)
                    continue
                # TODO: save it on disk instead
                computations[computation._auth] = computation
                client.send(computation._auth)

            elif req == 'await':
                computation = computations.pop(auth, None)
                if not computation:
                    client.send(None)
                    continue
                if computation._pulse_coro.location != self.asyncoro.location:
                    computation._xfer_files = [os.path.join(self.__dest_path, computation._auth,
                                                            os.path.basename(xf))
                                               for xf in computation._xfer_files]
                for xf in computation._xfer_files:
                    if not os.path.isfile(xf):
                        logger.warning('File "%s" for computation %s is not valid' %
                                       (xf, computation._auth))
                        computation = None
                        break
                if computation is None:
                    client.send(None)
                else:
                    self.__scheduler_coro.send((computation, client))
                    self.__sched_event.set()

            elif req == 'close_computation':
                if self.__cur_client_auth != auth:
                    logger.warning('Ignoring invalid request to close computation')
                    continue
                Coro(self.__close_computation)

            elif req == 'nodes_list':
                # TODO: allowed to query anytime, even if current
                # computation is not the one querying?
                if self.__cur_client_auth == auth:
                    nodes = [node.addr for node in self._nodes.values()
                             if node.status == Scheduler.NodeInitialized]
                else:
                    nodes = []
                client.send(nodes)

            elif req == 'procs_list':
                # TODO: allowed to query anytime, even if current
                # computation is not the one querying?
                if self.__cur_client_auth == auth:
                    procs = [proc.location for node in self._nodes.values()
                             if node.status == Scheduler.NodeInitialized
                             for proc in node.procs.values()
                             if proc.status == Scheduler.ProcInitialized]
                else:
                    procs = []
                client.send(procs)

            else:
                logger.warning('Ignoring invalid client request "%s"' % req)

    def __setup_node(self, node, computation, coro=None):
        if node.status == Scheduler.NodeIgnore:
            raise StopIteration(0)
        proc_setups = []
        for proc in node.procs.values():
            if proc.status == Scheduler.ProcDiscovered:
                proc_setups.append(Coro(self.__setup_proc, proc, computation))
        for proc_setup in proc_setups:
            yield proc_setup.finish()
        if node.status != Scheduler.NodeInitialized:
            for proc in node.procs.values():
                if proc.status == Scheduler.ProcInitialized:
                    node.status = Scheduler.NodeInitialized
                    if computation.status_coro:
                        computation.status_coro.send(StatusMessage(Scheduler.NodeInitialized,
                                                                   proc.location.addr))
                    break
        raise StopIteration(0)

    def __setup_proc(self, proc, computation, coro=None):
        if proc.status == Scheduler.ProcIgnore:
            raise StopIteration(0)
        if proc.status == Scheduler.ProcInitialized:
            logger.warning('proc %s already initialized' % proc.location)
            raise StopIteration(0)
        proc.status = Scheduler.ProcIgnore
        if not proc.server:
            proc.server = yield Coro.locate('discoro_proc', proc.location, timeout=2)
        proc.server.send({'req': 'setup', 'client': coro, 'computation': computation,
                          'pulse_coro': self.__timer_coro})
        ret = yield coro.receive(timeout=computation.timeout)
        if ret:
            logger.warning('setup of %s failed: %s' % (proc.server, ret))
            raise StopIteration(ret)
        if computation._pulse_coro.location != proc.location:
            for xf in computation._xfer_files:
                reply = yield self.asyncoro.send_file(proc.location, xf,
                                                      timeout=computation.timeout)
                if reply < 0:
                    logger.debug('failed to transfer file %s: %s' % (xf, reply))
                    Coro(self.__close_proc, proc)
                    raise StopIteration(-1)
        proc.status = Scheduler.ProcInitialized
        if computation.status_coro:
            computation.status_coro.send(StatusMessage(Scheduler.ProcInitialized, proc.location))
        raise StopIteration(0)

    def __close_node(self, node, coro=None):
        computation = self._cur_computation
        if not computation:
            logger.warning('Closing node %s ignored' % node.addr)
            raise StopIteration(-1)
        for proc in node.procs.values():
            yield self.__close_proc(proc, coro=coro)
        node.ncoros = 0
        node.status = Scheduler.NodeClosed
        if computation and computation.status_coro:
            computation.status_coro.send(StatusMessage(Scheduler.NodeClosed, proc.location.addr))

    def __close_proc(self, proc, coro=None):
        computation = self._cur_computation
        if not computation:
            logger.warning('Closing proc %s ignored' % proc.location)
            raise StopIteration(-1)
        if proc.coros:
            logger.warning('%s coros running at %s' % (len(proc.coros), proc.location))
        # TODO: check/indicate error?
        yield proc.server.deliver({'req': 'close', 'auth': computation._auth},
                                  timeout=computation.timeout)
        proc.status = Scheduler.ProcClosed
        proc.xfer_files = []
        if computation and computation.status_coro:
            computation.status_coro.send(StatusMessage(Scheduler.ProcClosed, proc.location))
        raise StopIteration(0)

    def __close_computation(self, coro=None):
        for node in self._nodes.values():
            yield self.__close_node(node)
        computation_path = os.path.join(self.__dest_path, self.__cur_client_auth)
        if os.path.isdir(computation_path):
            shutil.rmtree(computation_path, ignore_errors=True)
        self._cur_computation = None
        self.__cur_client_auth = None
        self.__sched_event.set()
        raise StopIteration(0)


class Computation(object):
    """Code fragments to distribute to remote asyncoro schedulers to
    create (remote) coroutines.
    """

    def __init__(self, components, status_coro=None, timeout=MsgTimeout,
                 pulse_interval=MinPulseInterval):
        """'computation' must be (generator) function, or list of
        (generator) functions. 'depends' is a list of code fragments
        that are needed by 'computation'; each element in this list
        should be either a module, a function, path name of a file, a
        Class or an object (in which case the code for its class is
        sent).
        """
        if pulse_interval < MinPulseInterval or pulse_interval > MaxPulseInterval:
            raise Exception('"pulse_interval" must be at least %s and at most %s' %
                            (MinPulseInterval, MaxPulseInterval))
        if timeout < 1 or timeout > 120:
            raise Exception('"timeout" must be at least 1 and at most 120')
        if status_coro is not None and not isinstance(status_coro, Coro):
            raise Exception('status_coro must be coroutine')

        # TODO: add option to leave transferred files on the nodes
        # after computation is done (such as "cleanup" in dispy)?

        if not isinstance(components, list):
            components = [components]

        self._code = ''
        self._xfer_funcs = set()
        self._xfer_files = []
        self._node_xfers = []
        self.status_coro = status_coro
        self._auth = None
        self.scheduler = None
        self._pulse_coro = None
        self.pulse_interval = pulse_interval
        self.timeout = timeout
        depends = set()
        for dep in components:
            if isinstance(dep, str) or inspect.ismodule(dep):
                if inspect.ismodule(dep):
                    dep = dep.__file__
                    if dep.endswith('.pyc'):
                        dep = dep[:-1]
                    dep = os.path.abspath(dep)
                    if not (dep.endswith('.py') and os.path.isfile(dep)):
                        raise Exception('Invalid module "%s" - must be python source.' % dep)
                if dep in depends:
                    continue
                try:
                    fd = open(dep, 'rb')
                    fd.close()
                except:
                    raise Exception('File "%s" is not valid' % dep)
                depends.add(dep)
                self._xfer_files.append(dep)
            elif inspect.isgeneratorfunction(dep) or inspect.isfunction(dep) or \
               inspect.isclass(dep) or hasattr(dep, '__class__'):
                if inspect.isgeneratorfunction(dep) or inspect.isfunction(dep):
                    name = dep.__name__
                elif inspect.isclass(dep):
                    name = dep.__name__
                elif hasattr(dep, '__class__') and inspect.isclass(dep.__class__):
                    dep = dep.__class__
                    name = dep.__name__
                if name in depends:
                    continue
                depends.add(name)
                self._xfer_funcs.add(name)
                self._code += '\n' + inspect.getsource(dep).lstrip()
            else:
                raise Exception('Invalid computation: %s' % dep)
        # check code can be compiled
        compile(self._code, '<string>', 'exec')

    def schedule(self, location=None, timeout=None):
        """Schedule computation for execution. Must be used with
        'yield' as 'result = yield compute.schedule()'. If scheduler
        is executing other computations, this will block until
        scheduler processes them (computations are processed in the
        order submitted).
        """

        if self._auth is not None:
            raise StopIteration(0)

        if not self.scheduler:
            self.scheduler = yield Coro.locate('discoro_scheduler', location=location,
                                               timeout=self.timeout)
            if not isinstance(self.scheduler, asyncoro.Coro):
                raise StopIteration(-1)

        def _schedule(self, coro=None):
            self._pulse_coro = Coro(self._pulse_proc)
            msg = {'req': 'schedule', 'computation': asyncoro.serialize(self), 'client': coro}
            if (yield self.scheduler.deliver(msg, timeout=self.timeout)) != 1:
                logger.debug('schedule failed')
                yield self.close()
                raise StopIteration(None)
            self._auth = yield coro.receive(timeout=self.timeout)
            if not isinstance(self._auth, str):
                yield self.close()
                raise StopIteration(-1)
            if isinstance(self._auth, str):
                atexit.register(self.close)
                if coro.location != self.scheduler.location:
                    for xf in self._xfer_files:
                        if (yield asyncoro.AsynCoro.send_file(
                           self.scheduler.location, xf, dir=self._auth, timeout=self.timeout)) < 0:
                            logger.warning('Could not send file "%s" to scheduler' % xf)
                            yield self.close()
                            raise StopIteration(-1)
            msg = {'req': 'await', 'auth': self._auth, 'client': coro}
            if (yield self.scheduler.deliver(msg, timeout=self.timeout)) != 1:
                yield self.close()
                raise StopIteration(-1)
            while True:
                resp = yield coro.receive(timeout=timeout)
                if isinstance(resp, dict) and resp.get('auth') == self._auth and \
                   resp.get('resp') == 'scheduled':
                    raise StopIteration(0)
                else:
                    logger.warning('invalid message ignored - waiting for scheduled')

        yield Coro(_schedule, self).finish()

    # TODO: add a way to send 'depends' to run methods?

    def run_at(self, where, func, *args, **kwargs):
        """Run given generator function 'func' with arguments 'args'
        and 'kwargs' at 'where'. Must be used with 'yield' as 'rcoro =
        yield compute.run_at(loc, genf, ...)'. If the request is
        successful, 'rcoro' will be a (remote) coroutine.

        If 'where' is a string, it is assumed to be IP address of a
        node, in which case the function is scheduled at that node on
        a process with least load (i.e., process with least number of
        pending coroutines). If 'where' is a Location instance, it is
        assumed to be process location in which case the function is
        scheduled at that process.

        'func' must be generator function, as it is used to run
        coroutine at remote location.
        """
        if isinstance(func, str):
            name = func
        else:
            name = func.__name__

        if name in self._xfer_funcs:
            code = None
        else:
            # if not inspect.isgeneratorfunction(func):
            #     logger.warning('"%s" is not a valid generator function' % name)
            #     raise StopIteration(None)
            code = inspect.getsource(func).lstrip()

        def _run(self, coro=None):
            msg = {'req': 'run', 'auth': self._auth, 'where': where,
                   'func': _Function(name, code, args, kwargs), 'client': coro}
            if (yield self.scheduler.deliver(msg, timeout=self.timeout)) == 1:
                rcoro = yield coro.receive(self.timeout)
            else:
                rcoro = None
            raise StopIteration(rcoro)

        yield Coro(_run, self).finish()

    def run_each(self, where, func, *args, **kwargs):
        """Run given generator function 'func' with arguments 'args'
        and 'kwargs' at each node or process. Must be used with
        'yield' as 'rcoros = yield compute.run_each(loc, genf,
        ...)'. 'rcoros' will be list of (remote) coroutines.

        'where' is same as in the case of 'run_at'; if it is string,
        the function is scheduled at every node and if it is a
        Location instance, the function is scheduled at every process
        (on every node).
        """
        if isinstance(func, str):
            name = func
        else:
            name = func.func_name

        if name in self._xfer_funcs:
            code = None
        else:
            # if not inspect.isgeneratorfunction(func):
            #     logger.warning('"%s" is not a valid generator function' % name)
            #     raise StopIteration([])
            code = inspect.getsource(func).lstrip()

        def _run(self, coro=None):
            msg = {'req': 'run_each', 'auth': self._auth, 'where': where,
                   'func': _Function(name, code, args, kwargs), 'client': coro}
            n = yield self.scheduler.deliver(msg, timeout=self.timeout)
            if n != 1:
                raise StopIteration([])
            n = yield coro.receive(timeout=self.timeout)
            if not n:
                raise StopIteration([])
            rcoros = [(yield coro.receive(timeout=self.timeout)) for i in range(n)]
            # TODO: if failed, rcoro would be MonitorException; pass
            # to user without filtering?
            rcoros = [rcoro for rcoro in rcoros if isinstance(rcoro, Coro)]
            raise StopIteration(rcoros)

        yield Coro(_run, self).finish()

    def run(self, func, *args, **kwargs):
        """Run given generator function 'func' with arguments 'args'
        and 'kwargs' at a process with least load at a node with least
        load. Must be used with 'yield' as 'rcoro = yield
        compute.run(genf, ...)'. If the request is successful, 'rcoro'
        will be a (remote) coroutine.
        """
        yield self.run_at(None, func, *args, **kwargs)

    def run_nodes(self, func, *args, **kwargs):
        """Run given generator function 'func' with arguments 'args'
        and 'kwargs' at a process with least load at every node. Must
        be used with 'yield' as 'rcoros = yield
        compute.run_nodes(genf, ...)'. 'rcoros' will be a list of
        (remote) coroutines.
        """
        yield self.run_each('node', func, *args, **kwargs)

    def run_procs(self, func, *args, **kwargs):
        """Run given generator function 'func' with arguments 'args'
        and 'kwargs' at every process (at every node). Must be used
        with 'yield' as 'rcoros = yield compute.run_procs(genf,
        ...)'. 'rcoros' will be a list of (remote) coroutines.
        """
        yield self.run_each('proc', func, *args, **kwargs)

    def run_node_procs(self, addr, func, *args, **kwargs):
        """Run given generator function 'func' with arguments 'args'
        and 'kwargs' at every process at given node at 'addr'. Must be
        used with 'yield' as 'rcoros = yield
        compute.run_node_procs(addr, genf, ...)'. 'rcoros' will be a
        list of (remote) coroutines at that node.
        """
        yield self.run_each(addr, func, *args, **kwargs)

    # TODO: add 'map' methods to run with arguments as iterators
    # (e.g., list of tuples)

    def nodes(self):
        """Get list of addresses of nodes initialized for this
        computation. Must be used with 'yield' as 'yield
        compute.nodes()'.
        """

        def _nodes_list(self, coro=None):
            msg = {'req': 'nodes_list', 'auth': self._auth, 'client': coro}
            if (yield self.scheduler.deliver(msg, timeout=self.timeout)) == 1:
                yield coro.receive(self.timeout)
            else:
                raise StopIteration([])

        yield Coro(_nodes_list, self).finish()

    def procs(self):
        """Get list of Location instances of processes initialized for
        this computation. Must be used with 'yield' as 'yield
        compute.procs()'.
        """

        def _procs_list(self, coro=None):
            msg = {'req': 'procs_list', 'auth': self._auth, 'client': coro}
            if (yield self.scheduler.deliver(msg, timeout=self.timeout)) == 1:
                yield coro.receive(self.timeout)
            else:
                raise StopIteration([])

        yield Coro(_procs_list, self).finish()

    def close(self):
        """Close computation. Must be used with 'yield' as 'yield
        compute.close()'.
        """

        def _close(self, coro=None):
            msg = {'req': 'close_computation', 'auth': self._auth, 'client': coro}
            yield self.scheduler.deliver(msg, timeout=self.timeout)
            self._auth = None

        if self._auth:
            yield Coro(_close, self).finish()
        if self._pulse_coro:
            self._pulse_coro.send(None)

    def _pulse_proc(self, coro=None):
        """For internal use only.
        """
        last_pulse = time.time()
        while True:
            msg = yield coro.receive(timeout=(2 * self.pulse_interval))
            if msg == 'pulse':
                last_pulse = time.time()
            elif msg is None:
                logger.debug('scheduler not reachable?')
                if (time.time() - last_pulse) > (5 * self.pulse_interval):
                    logger.warning('scheduler is zombie!')
                if self._auth:
                    yield self.close()
                break
            else:
                logger.debug('ignoring invalid pulse message')


if __name__ == '__main__':
    """The scheduler can be started either within a client program (if
    no other client programs use the nodes simultaneously), or can be
    run on a node with the options described below (usually no options
    are necessary, so the scheduler can be strated with just
    'discoro.py')
    """

    import logging
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--ip_addr', dest='node', default=None,
                        help='IP address or host name of this node')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addr', default=None,
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('-u', '--udp_port', dest='udp_port', type=int, default=51350,
                        help='UDP port number to use')
    parser.add_argument('-n', '--name', dest='name', default=None,
                        help='(symbolic) name given to schduler')
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
    config = vars(parser.parse_args(sys.argv[1:]))
    del parser

    if not config['name']:
        config['name'] = 'discoro_scheduler'

    if config['loglevel']:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    del config['loglevel']

    asyncoro.AsynCoro(**config)
    scheduler = Scheduler()
    while True:
        try:
            if sys.stdin.readline().strip().lower() in ('quit', 'exit'):
                break
        except KeyboardInterrupt:
            break

    logger.debug('terminating scheduler')
