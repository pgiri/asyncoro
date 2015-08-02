#!/usr/bin/python

"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.

This module provides API for creating distributed communicating
processes. 'Computation' class should be used to package computation
components (Python generator functions, Python functions, files,
classes, modules) and then schedule runs that create remote coroutines
at remote processes running 'discoronode.py'.

See 'discoro_client*.py' files in 'examples' directory for various use
cases.
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

__all__ = ['Scheduler', 'Computation', 'StatusMessage']

MsgTimeout = 10
MinPulseInterval = MsgTimeout
MaxPulseInterval = 10 * MinPulseInterval

# status about nodes / processes are sent with this structure
StatusMessage = collections.namedtuple('StatusMessage', ['status', 'location'])

# for internal use only
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

    ComputationClosed = 25

    """This class is for use by Computation class (see below) only.
    Other than the status indications above, none of its attributes
    are to be accessed directly.
    """

    __metaclass__ = asyncoro.MetaSingleton

    class _Node(object):

        def __init__(self, addr, scheduler):
            self.addr = addr
            self.procs = {}
            self.ncoros = 0
            self.status = None
            self.scheduler = scheduler

        def run(self, func, client):
            if self.status != Scheduler.NodeInitialized:
                client.send(None)
                raise StopIteration
            where = None
            load = None
            for proc in self.procs.itervalues():
                if proc.status != Scheduler.ProcInitialized:
                    continue
                if load is None or len(proc.coros) < load:
                    where = proc
                    load = len(proc.coros)
            if where:
                yield where.run(func, client)
            else:
                client.send(None)

    class _Proc(object):

        def __init__(self, name, location, scheduler):
            self.name = name
            self.location = location
            self.coros = {}
            self.done = {}
            self.status = None
            self.server = None
            self.xfer_files = []
            self.last_pulse = time.time()
            self.scheduler = scheduler

        def run(self, func, client):
            if self.status != Scheduler.ProcInitialized:
                raise StopIteration(None)
            node = self.scheduler._nodes.get(self.location.addr, None)
            computation = self.scheduler._cur_computation
            if not node or not computation:
                raise StopIteration(None)

            def _run(self, func, coro=None):
                self.server.send({'req': 'run', 'auth': computation._auth, 'func': func,
                                  'client': coro, 'notify': self.scheduler._status_coro})
                rcoro = yield coro.receive(timeout=computation.timeout)
                if isinstance(rcoro, Coro):
                    key = str(rcoro)
                    done = self.done.pop(key, None)
                    if done is None:
                        # TODO: keep func too for fault-tolerance
                        self.coros[key] = rcoro
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
        clean = kwargs.pop('clean', False)
        self.__zombie_period = kwargs.pop('zombie_period', None)
        nodes = kwargs.pop('nodes', [])
        self.asyncoro = asyncoro.AsynCoro.instance(**kwargs)
        if self.asyncoro.name == 'discoro_scheduler':
            self.__dest_path = os.path.join(self.asyncoro.dest_path, 'discoro', 'scheduler')
            if clean:
                shutil.rmtree(self.__dest_path, ignore_errors=True)
            if not os.path.isdir(self.__dest_path):
                os.makedirs(self.__dest_path)
            self.asyncoro.dest_path = self.__dest_path
        else:
            self.__dest_path = self.asyncoro.dest_path
        self.__scheduler_coro = Coro(self.__scheduler_proc, nodes)
        self.__client_coro = Coro(self.__client_proc)
        self.__timer_coro = Coro(self.__timer_proc)
        self._status_coro = Coro(self.__status_proc)
        atexit.register(self.__close)

    def __close(self):
        # TODO: wait for pending coroutines?
        self.__timer_coro.terminate()
        self.__client_coro.terminate()
        self._status_coro.terminate()

    def __status_proc(self, coro=None):
        coro.set_daemon()
        self.asyncoro.peer_status(coro)
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
                    logger.warning('proc "%s" is invalid' % (rcoro.location))
                    continue
                if proc.coros.pop(str(rcoro), None) is None:
                    # logger.warning('rcoro "%s" is invalid at "%s"' % (rcoro, proc.location))
                    proc.done[str(rcoro)] = msg
                    # TODO: prune 'done'
                    continue
                if self._cur_computation and self._cur_computation.status_coro:
                    self._cur_computation.status_coro.send(msg)
                node.ncoros -= 1

            elif isinstance(msg, asyncoro.PeerStatus):
                computation = self._cur_computation
                if msg.status == asyncoro.PeerStatus.Online:
                    proc = Scheduler._Proc(msg.name, msg.location, self)
                    node = self._nodes.get(msg.location.addr, None)
                    if node:
                        node.procs[str(msg.location)] = proc
                        Coro(self.__setup_proc, proc, computation)
                    else:
                        node = Scheduler._Node(msg.location.addr, self)
                        self._nodes[msg.location.addr] = node
                        node.procs[str(msg.location)] = proc
                        self.__setup_node(node, computation)
                else:
                    # msg.status == asyncoro.PeerStatus.Offline
                    node = self._nodes.get(msg.location.addr, None)
                    if node:
                        proc = node.procs.pop(str(msg.location), None)
                        if proc:
                            if computation and computation.status_coro:
                                status_msg = StatusMessage(Scheduler.ProcDisconnected,
                                                           proc.location)
                                computation.status_coro.send(status_msg)
                            # TODO: (re)start process elsewhere (fault-tolerant)?
                            if node.procs:
                                Coro(self.__close_proc, proc)
                            else:
                                self._nodes.pop(proc.location.addr, None)
                                if computation and computation.status_coro:
                                    status_msg = StatusMessage(Scheduler.NodeDisconnected,
                                                               proc.location.addr)
                                    computation.status_coro.send(status_msg)
                                Coro(self.__close_node, node)
                    elif computation and msg.location == computation._pulse_coro.location:
                        logger.warning('client %s terminated; closing computation %s' %
                                       (msg.location, self.__cur_client_auth))
                        Coro(self.__close_computation)

            else:
                logger.warning('invalid status message ignored')

    def __timer_proc(self, coro=None):
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

                elif msg.get('status', None) == 'ProcClosed':
                    location = msg.get('location', None)
                    asyncoro.logger.debug('Process %s closed' % location)
                    if isinstance(location, asyncoro.Location):
                        node = self._nodes.get(location.addr, None)
                        if node:
                            proc = node.procs.get(str(location), None)
                            if proc:
                                yield self.__close_proc(proc)
                                if all(p.status != Scheduler.ProcInitialized
                                       for p in node.procs.itervalues()):
                                    node.status = Scheduler.NodeClosed
                                    if self._cur_computation and \
                                           self._cur_computation.status_coro:
                                        self._cur_computation.status_coro.send(
                                            StatusMessage(Scheduler.NodeClosed, node.addr))
                                    if all(n.status != Scheduler.NodeInitialized
                                           for n in self._nodes.itervalues()):
                                        if self._cur_computation and \
                                               self._cur_computation.status_coro:
                                            self._cur_computation.status_coro.send(
                                                StatusMessage(Scheduler.ComputationClosed,
                                                              coro.location))
                                        Coro(self.__close_computation)

            if (now - client_pulse) > self.__pulse_interval and self._cur_computation:
                if self._cur_computation._pulse_coro.send('pulse') == 0:
                    client_pulse = now
                else:
                    if (now - client_pulse) > (5 * self.__pulse_interval):
                        logger.debug('client %s not responding; closing it' %
                                     self.__cur_client_auth)
                        Coro(self.__close_computation)

            if (now - proc_check) > (5 * self.__pulse_interval):
                for node in self._nodes.itervalues():
                    if node.status != Scheduler.NodeInitialized:
                        continue
                    for proc in node.procs.itervalues():
                        if proc.status != Scheduler.ProcInitialized:
                            continue
                        if (now - proc.last_pulse) > (5 * self.__pulse_interval):
                            logger.warning('Process %s is zombie!' % proc.location)
                            Coro(self.__close_proc, proc)

    def __run(self, func, client):
        host = None
        load = None
        for node in self._nodes.itervalues():
            if node.status != Scheduler.NodeInitialized:
                continue
            node_load = float(node.ncoros) / len(node.procs)
            if load is None or node_load < load:
                host = node
                load = node_load
        if host:
            yield host.run(func, client)
        else:
            client.send(None)

    @staticmethod
    def auth_code():
        # TODO: use uuid?
        return hashlib.sha1(os.urandom(10).encode('hex')).hexdigest()

    def __scheduler_proc(self, nodes, coro=None):
        coro.set_daemon()
        for node in nodes:
            yield asyncoro.AsynCoro.instance().peer(node, broadcast=True)
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

            for node in self._nodes.itervalues():
                for proc in node.procs.itervalues():
                    if (proc.status == Scheduler.ProcClosed or
                       proc.status == Scheduler.ProcIgnore):
                        proc.status = Scheduler.ProcDiscovered
                        if self._cur_computation.status_coro:
                            status_msg = StatusMessage(proc.status, proc.location)
                            self._cur_computation.status_coro.send(status_msg)
                    if (node.status != Scheduler.NodeDiscovered and
                       proc.status == Scheduler.ProcDiscovered):
                        node.status = Scheduler.NodeDiscovered
                        if self._cur_computation.status_coro:
                            status_msg = StatusMessage(node.status, node.addr)
                            self._cur_computation.status_coro.send(status_msg)

            for node in self._nodes.itervalues():
                # TODO: check if node is allowed
                if self._cur_computation:
                    self.__setup_node(node, self._cur_computation)

    def __client_proc(self, coro=None):
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
                    nodes = [node for node in self._nodes.itervalues()
                             if node.status == Scheduler.NodeInitialized]
                    if (yield client.deliver(len(nodes), self._cur_computation.timeout)) != 1:
                        continue
                    for node in nodes:
                        Coro(node.run, func, client)
                elif where == 'proc':
                    procs = [proc for node in self._nodes.itervalues()
                             if node.status == Scheduler.NodeInitialized
                             for proc in node.procs.itervalues()
                             if proc.status == Scheduler.ProcInitialized]
                    if (yield client.deliver(len(procs), self._cur_computation.timeout)) != 1:
                        continue
                    for proc in procs:
                        Coro(proc.run, func, client)
                else:
                    # node_procs
                    node = self._nodes.get(where)
                    if node and node.status == Scheduler.NodeInitialized:
                        procs = [proc for proc in node.procs.itervalues()
                                 if proc.status == Scheduler.ProcInitialized]
                    else:
                        procs = []
                    if (yield client.deliver(len(procs), self._cur_computation.timeout)) != 1:
                        continue
                    for proc in procs:
                        Coro(proc.run, func, client)

            elif req == 'schedule':
                try:
                    computation = asyncoro.unserialize(msg['computation'])
                    assert isinstance(computation, Computation) or \
                        computation.__class__.__name__ == 'Computation'
                    assert isinstance(computation._pulse_coro, Coro)
                    if computation._pulse_coro.location == self.asyncoro.location:
                        computation._pulse_coro._id = int(computation._pulse_coro._id)
                        if computation.status_coro:
                            computation.status_coro._id = int(computation.status_coro._id)
                except:
                    logger.warning('ignoring invalid computation request')
                    client.send(None)
                    continue
                while True:
                    computation._auth = Scheduler.auth_code()
                    if not os.path.exists(os.path.join(self.__dest_path, computation._auth)):
                        break
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
                if computation._pulse_coro.location.addr != self.asyncoro.location.addr:
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
                    # TODO: allow zombie_period to be set?
                    computation.zombie_period = self.__zombie_period
                    self.__scheduler_coro.send((computation, client))
                    self.__sched_event.set()

            elif req == 'close_computation':
                if self.__cur_client_auth == auth:
                    Coro(self.__close_computation)
                else:
                    computation = computations.pop(auth, None)
                    if computation:
                        computation_path = os.path.join(self.__dest_path, auth)
                        if os.path.isdir(computation_path):
                            shutil.rmtree(computation_path, ignore_errors=True)
                    else:
                        logger.warning('Ignoring invalid request to close computation')

            elif req == 'nodes_list':
                # TODO: allowed to query anytime, even if current
                # computation is not the one querying?
                if self.__cur_client_auth == auth:
                    nodes = [node.addr for node in self._nodes.itervalues()
                             if node.status == Scheduler.NodeInitialized]
                else:
                    nodes = []
                client.send(nodes)

            elif req == 'procs_list':
                # TODO: allowed to query anytime, even if current
                # computation is not the one querying?
                if self.__cur_client_auth == auth:
                    procs = [proc.location for node in self._nodes.itervalues()
                             if node.status == Scheduler.NodeInitialized
                             for proc in node.procs.itervalues()
                             if proc.status == Scheduler.ProcInitialized]
                else:
                    procs = []
                client.send(procs)

            else:
                logger.warning('Ignoring invalid client request "%s"' % req)

    def __setup_node(self, node, computation, coro=None):
        if node.status == Scheduler.NodeIgnore:
            return
        for proc in node.procs.itervalues():
            if proc.status == Scheduler.ProcDiscovered or proc.status is None:
                Coro(self.__setup_proc, proc, computation)

    def __setup_proc(self, proc, computation, coro=None):
        if proc.status == Scheduler.ProcIgnore:
            raise StopIteration(0)
        if proc.status == Scheduler.ProcInitialized:
            logger.warning('proc %s already initialized' % proc.location)
            raise StopIteration(0)
        proc.status = Scheduler.ProcIgnore
        if not proc.server:
            if computation:
                timeout = computation.timeout
            else:
                timeout = MsgTimeout
            proc.server = yield Coro.locate('discoro_proc', proc.location, timeout=timeout)
            if not isinstance(proc.server, Coro):
                logger.debug('server at %s is not valid' % (proc.location))
                # TODO: asuume temporary issue instead of removing it?
                node = self._nodes.get(proc.location.addr, None)
                if node:
                    node.procs.pop(str(proc.location), None)
                raise StopIteration(-1)
            if computation and computation.status_coro:
                computation.status_coro.send(StatusMessage(Scheduler.ProcDiscovered, proc.location))
        if not computation:
            proc.status = Scheduler.ProcDiscovered
            raise StopIteration(0)
        proc.server.send({'req': 'setup', 'client': coro, 'computation': computation,
                          'pulse_coro': self.__timer_coro})
        ret = yield coro.receive(timeout=computation.timeout)
        if ret:
            logger.warning('setup of %s failed: %s' % (proc.server, ret))
            raise StopIteration(ret)
        for xf in computation._xfer_files:
            reply = yield self.asyncoro.send_file(proc.location, xf,
                                                  timeout=computation.timeout)
            if reply < 0:
                logger.debug('failed to transfer file %s: %s' % (xf, reply))
                Coro(self.__close_proc, proc)
                raise StopIteration(-1)
        proc.status = Scheduler.ProcInitialized
        proc.last_pulse = time.time()
        node = self._nodes[proc.location.addr]
        if node.status != Scheduler.NodeInitialized:
            node.status = Scheduler.NodeInitialized
            if computation.status_coro:
                computation.status_coro.send(StatusMessage(node.status, node.addr))
        if computation.status_coro:
            computation.status_coro.send(StatusMessage(proc.status, proc.location))
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
            computation.status_coro.send(StatusMessage(node.status, node.addr))

    def __close_proc(self, proc, coro=None):
        computation = self._cur_computation
        if not computation or proc.status != Scheduler.ProcInitialized:
            logger.warning('Closing proc %s ignored' % proc.location)
            raise StopIteration(-1)
        if proc.coros:
            logger.warning('%s coros running at %s' % (len(proc.coros), proc.location))
        asyncoro.logger.debug('Closing process %s' % proc.location)
        # TODO: check/indicate error?
        yield proc.server.deliver({'req': 'close', 'auth': computation._auth},
                                  timeout=computation.timeout)
        proc.status = Scheduler.ProcClosed
        proc.xfer_files = []
        proc.coros = {}
        proc.done = {}
        if computation and computation.status_coro:
            computation.status_coro.send(StatusMessage(proc.status, proc.location))
        raise StopIteration(0)

    def __close_computation(self, coro=None):
        computation = self._cur_computation
        if computation:
            computation.status_coro = None
        for node in self._nodes.values():
            yield self.__close_node(node)
        if self.__cur_client_auth:
            computation_path = os.path.join(self.__dest_path, self.__cur_client_auth)
            if os.path.isdir(computation_path):
                shutil.rmtree(computation_path, ignore_errors=True)
        self._cur_computation = None
        self.__cur_client_auth = None
        self.__sched_event.set()
        raise StopIteration(0)


class Computation(object):
    """Packages components to distribute to remote asyncoro schedulers
    to create (remote) coroutines.
    """

    def __init__(self, components, status_coro=None, timeout=MsgTimeout,
                 pulse_interval=MinPulseInterval, zombie_period=None):
        """'components' should be a list, each element of which is
        either a module, a (generator or normal) function, path name
        of a file, a class or an object (in which case the code for
        its class is sent).

        'status_coro', if not None, should be a coroutine. The
        scheduler sends status messages indicating when a remote
        process has been initialized (so it is ready to run jobs),
        closed etc., and exit status of remote coroutines. See
        'discoro_client*.py' files in examples directory.

        'timeout' is maximum number of seconds to complete a
        communication (transfer of messages). If client / scheduler /
        remote processes couldn't send / receive a message within this
        period, the operation is aborted. Bigger values may be used
        when communicating over slower connections.

        'pulse_interval' is interval (number of seconds) used for
        heart beat messages to check if client / scheduler / process
        is alive. If the other side doesn't reply to 5 heart beat
        messages, it is treated as dead.

        'zombie_period' is maximum number of seconds a server process
        stays idle (i.e., no jobs running on that process) before the
        computation is automatically closed (on that process). Once
        closed, the computation can't use that process anymore. This
        discards unused clients so other pending (queued) computations
        can use compute resources. If 'zombie_period' is None, the
        processes don't check for idle period and don't close
        computation (until the user program explicitly closes
        it). When scheduler is shared (run as separate program),
        'zombie_period' is set to 10 * MaxPulseInterval.
        """

        if pulse_interval < MinPulseInterval or pulse_interval > MaxPulseInterval:
            raise Exception('"pulse_interval" must be at least %s and at most %s' %
                            (MinPulseInterval, MaxPulseInterval))
        if timeout < 1 or timeout > MaxPulseInterval:
            raise Exception('"timeout" must be at least 1 and at most %s' % MaxPulseInterval)
        if status_coro is not None and not isinstance(status_coro, Coro):
            raise Exception('status_coro must be coroutine')
        if zombie_period and zombie_period < MaxPulseInterval:
            raise Exception('zombie_period must be >= %s' % MaxPulseInterval)

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
        self.zombie_period = zombie_period
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
                    name = dep.func_name
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
            raise StopIteration(-1)
        if self.status_coro is not None and not isinstance(self.status_coro, Coro):
            raise StopIteration(-1)

        if not self.scheduler:
            self.scheduler = yield Coro.locate('discoro_scheduler', location=location,
                                               timeout=self.timeout)
            if not isinstance(self.scheduler, Coro):
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
                        if (yield asyncoro.AsynCoro.instance().send_file(
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
            name = func.func_name

        if name in self._xfer_funcs:
            code = None
        else:
            # if not inspect.isgeneratorfunction(func):
            #     logger.warning('"%s" is not a valid generator function' % name)
            #     raise StopIteration(None)
            code = inspect.getsource(func).lstrip()

        def _run(self, coro=None):
            msg = {'req': 'run', 'auth': self._auth, 'where': where, 'client': coro,
                   'func': asyncoro.serialize(_Function(name, code, args, kwargs))}
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
            msg = {'req': 'run_each', 'auth': self._auth, 'where': where, 'client': coro,
                   'func': asyncoro.serialize(_Function(name, code, args, kwargs))}
            n = yield self.scheduler.deliver(msg, timeout=self.timeout)
            if n != 1:
                raise StopIteration([])
            n = yield coro.receive(timeout=self.timeout)
            if not n:
                raise StopIteration([])
            rcoros = [(yield coro.receive(timeout=self.timeout)) for i in range(n)]
            # TODO: if failed, rcoro would be either None or tuple;
            # pass to user without filtering?
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
            yield self._pulse_coro.send('quit')

    def _pulse_proc(self, coro=None):
        """For internal use only.
        """
        last_pulse = time.time()
        while True:
            msg = yield coro.receive(timeout=(2 * self.pulse_interval))
            if msg == 'pulse':
                last_pulse = time.time()
            elif msg == 'quit':
                break
            elif msg is None:
                logger.debug('scheduler not reachable?')
                if (time.time() - last_pulse) > (5 * self.pulse_interval):
                    logger.warning('scheduler is zombie!')
                    if self._auth:
                        self._pulse_coro = None
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
    parser.add_argument('--node', action='append', dest='nodes', default=[],
                        help='additional remote nodes (names or IP address) to use')
    parser.add_argument('--zombie_period', dest='zombie_period', type=int, default=1800,
                        help='maximum time in seconds computation is idle')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('--clean', action='store_true', dest='clean', default=False,
                        help='if given, files copied from or generated by clients will be removed')
    config = vars(parser.parse_args(sys.argv[1:]))
    del parser

    if config['zombie_period'] and config['zombie_period'] < MaxPulseInterval:
        raise Exception('zombie_period must be >= %s' % MaxPulseInterval)

    if not config['name']:
        config['name'] = 'discoro_scheduler'

    if config['loglevel']:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    del config['loglevel']

    scheduler = Scheduler(**config)
    while True:
        try:
            if sys.stdin.readline().strip().lower() in ('quit', 'exit'):
                break
        except KeyboardInterrupt:
            break

    logger.info('terminating discoro scheduler')
