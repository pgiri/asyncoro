#!/usr/bin/python

"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.

This module provides API for creating distributed communicating
processes. 'Computation' class should be used to package computation
components (Python generator functions, Python functions, files,
classes, modules) and then schedule runs that create remote coroutines
at remote server processes running 'discoronode.py'.

See 'discoro_client*.py' files in 'examples' directory for various use
cases.
"""

import os
import sys
import inspect
import hashlib
import collections
import time
import socket
import shutil
import operator

import asyncoro.disasyncoro as asyncoro
from asyncoro import Coro, SysCoro, logger

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2014-2015 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

__all__ = ['Scheduler', 'Computation', 'DiscoroStatus', 'DiscoroCoroInfo',
           'DiscoroNodeInfo', 'DiscoroServerInfo', 'DiscoroNodeAvailInfo']

MsgTimeout = asyncoro.MsgTimeout
MinPulseInterval = 10
MaxPulseInterval = 10 * MinPulseInterval

# status about nodes / servers are sent with this structure
DiscoroStatus = collections.namedtuple('DiscoroStatus', ['status', 'info'])
DiscoroCoroInfo = collections.namedtuple('DiscoroCoroInfo', ['coro', 'args', 'kwargs', 'start_time'])
DiscoroNodeInfo = collections.namedtuple('DiscoroNodeInfo', ['name', 'addr', 'avail_info'])
DiscoroServerInfo = collections.namedtuple('DiscoroServerInfo', ['name', 'location'])

# for internal use only
_DiscoroFunction = collections.namedtuple('_DiscoroFunction', ['name', 'code', 'args', 'kwargs'])


class DiscoroNodeAvailInfo(object):
    """Node availability status is indicated with this class.  'cpu' is
    available CPU in percent in the range 0 to 100. 0 indicates node is busy
    executing tasks on all CPUs and 100 indicates node is not busy at all.
    """

    def __init__(self, addr, cpu, memory, disk, swap):
        self.addr = addr
        self.cpu = cpu
        self.memory = memory
        self.disk = disk
        self.swap = swap


class Computation(object):
    """Packages components to distribute to remote asyncoro schedulers to create
    (remote) coroutines.
    """

    def __init__(self, components, status_coro=None, timeout=MsgTimeout,
                 pulse_interval=(2*MinPulseInterval), ping_interval=None, zombie_period=0):
        """'components' should be a list, each element of which is either a
        module, a (generator or normal) function, path name of a file, a class
        or an object (in which case the code for its class is sent).

        'status_coro', if not None, should be a coroutine. The scheduler sends
        status messages indicating when a remote server process has been
        initialized (so it is ready to run jobs), closed etc., and exit status
        of remote coroutines. See 'discoro_client*.py' files in examples
        directory.

        'timeout' is maximum number of seconds to complete a communication
        (transfer of messages). If client / scheduler / remote servers couldn't
        send / receive a message within this period, the operation is
        aborted. Bigger values may be used when communicating over slower
        connections.

        'pulse_interval' is interval (number of seconds) used for heart beat
        messages to check if client / scheduler / server is alive. If the other
        side doesn't reply to 5 heart beat messages, it is treated as dead.

        'zombie_period' is maximum number of seconds a server process stays idle
        (i.e., no coroutines running on that server) before the computation is
        automatically closed (on that server). Once closed, the computation
        can't use that server anymore. This discards unused clients so other
        pending (queued) computations can use compute resources. If
        'zombie_period' is 0, the servers don't check for idle period and
        don't close computation (until the user program explicitly closes
        it).
        """

        if status_coro is not None and not isinstance(status_coro, Coro):
            raise Exception('"status_coro" must be coroutine')
        if timeout < 1 or timeout > MaxPulseInterval:
            raise Exception('"timeout" must be at least 1 and at most %s' % MaxPulseInterval)
        if pulse_interval < MinPulseInterval or pulse_interval > MaxPulseInterval:
            raise Exception('"pulse_interval" must be at least %s and at most %s' %
                            (MinPulseInterval, MaxPulseInterval))
        if ping_interval and ping_interval < MinPulseInterval:
            raise Exception('"ping_interval" must be at least %s', MinPulseInterval)
        if (not isinstance(zombie_period, (int, float)) or 0 > zombie_period < MaxPulseInterval):
            raise Exception('"zombie_period" must be either 0 or >= %s' % MaxPulseInterval)

        if not isinstance(components, list):
            components = [components]

        self._code = ''
        self._xfer_funcs = set()
        self._xfer_files = []
        self.status_coro = status_coro
        self._auth = None
        self.scheduler = None
        self._pulse_coro = None
        self._pulse_interval = pulse_interval
        self._ping_interval = ping_interval
        self.timeout = timeout
        self.zombie_period = zombie_period
        depends = set()
        cwd = os.getcwd()
        for dep in components:
            if isinstance(dep, str) or inspect.ismodule(dep):
                if inspect.ismodule(dep):
                    name = dep.__file__
                    if name.endswith('.pyc'):
                        name = name[:-1]
                    if not name.endswith('.py'):
                        raise Exception('Invalid module "%s" - must be python source.' % dep)
                    if name.startswith(cwd):
                        dst = os.path.dirname(name[len(cwd)+1:])
                    elif dep.__package__:
                        dst = dep.__package__.replace('.', os.sep)
                    else:
                        dst = os.path.dirname(dep.__name__.replace('.', os.sep))
                else:
                    name = dep
                    if name.startswith(cwd):
                        dst = name[len(cwd)+1:]
                    else:
                        dst = '.'
                if name in depends:
                    continue
                try:
                    with open(name, 'rb') as fd:
                        pass
                except:
                    raise Exception('File "%s" is not valid' % name)
                self._xfer_files.append((name, dst, os.sep))
                depends.add(name)
            elif (inspect.isgeneratorfunction(dep) or inspect.isfunction(dep) or
                  inspect.isclass(dep) or hasattr(dep, '__class__')):
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
        """Schedule computation for execution. Must be used with 'yield' as
        'result = yield compute.schedule()'. If scheduler is executing other
        computations, this will block until scheduler processes them
        (computations are processed in the order submitted).
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
            self._pulse_coro = SysCoro(self._pulse_proc)
            msg = {'req': 'schedule', 'computation': asyncoro.serialize(self), 'client': coro}
            self.scheduler.send(msg)
            self._auth = yield coro.receive(timeout=self.timeout)
            if not isinstance(self._auth, str):
                logger.debug('Could not send computation to scheduler %s: %s',
                             self.scheduler, self._auth)
                raise StopIteration(-1)
            SysCoro.scheduler().atexit(10, lambda: SysCoro(self.close).value())
            if coro.location != self.scheduler.location:
                for xf, dst, sep in self._xfer_files:
                    drive, xf = os.path.splitdrive(xf)
                    if xf.startswith(sep):
                        xf = os.path.join(os.sep, *(xf.split(sep)))
                    else:
                        xf = os.path.join(*(xf.split(sep)))
                    xf = drive + xf
                    dst = os.path.join(self._auth, os.path.join(*(dst.split(sep))))
                    if (yield asyncoro.AsynCoro.instance().send_file(
                       self.scheduler.location, xf, dir=dst, timeout=self.timeout)) < 0:
                        logger.warning('Could not send file "%s" to scheduler', xf)
                        yield self.close()
                        raise StopIteration(-1)
            msg = {'req': 'await', 'auth': self._auth, 'client': coro}
            self.scheduler.send(msg)
            resp = yield coro.receive(timeout=timeout)
            if (isinstance(resp, dict) and resp.get('auth') == self._auth and
               resp.get('resp') == 'scheduled'):
                raise StopIteration(0)
            else:
                yield self.close()
                raise StopIteration(-1)

        yield Coro(_schedule, self).finish()

    def run_at(self, where, gen, *args, **kwargs):
        """Run given generator function 'gen' with arguments 'args' and 'kwargs'
        at 'where'.  If the request is successful, 'rcoro' will be a (remote)
        coroutine; check result with 'isinstance(rcoro, asyncoro.Coro)'. Must be
        used with 'yield' as 'rcoro = yield compute.run_at(loc, genf, ...)'.

        If 'where' is a string, it is assumed to be IP address of a node, in
        which case the coroutine is scheduled at that node on a server with
        least load (i.e., server with least number of pending coroutines). If
        'where' is a Location instance, it is assumed to be server location in
        which case the coroutine is scheduled at that server.

        'gen' must be generator function, as it is used to run coroutine at
        remote location.
        """
        if isinstance(gen, str):
            name = gen
        else:
            name = gen.func_name

        if name in self._xfer_funcs:
            code = None
        else:
            # if not inspect.isgeneratorfunction(gen):
            #     logger.warning('"%s" is not a valid generator function', name)
            #     raise StopIteration(None)
            code = inspect.getsource(gen).lstrip()

        def _run(self, coro=None):
            msg = {'req': 'run', 'auth': self._auth, 'where': where, 'client': coro,
                   'func': asyncoro.serialize(_DiscoroFunction(name, code, args, kwargs))}
            if (yield self.scheduler.deliver(msg, timeout=self.timeout)) == 1:
                rcoro = yield coro.receive(self.timeout)
                if self.status_coro and isinstance(rcoro, Coro):
                    msg = DiscoroCoroInfo(rcoro, args, kwargs, time.time())
                    self.status_coro.send(DiscoroStatus(Scheduler.CoroCreated, msg))
            else:
                rcoro = None
            raise StopIteration(rcoro)

        yield Coro(_run, self).finish()

    def run_each(self, where, gen, *args, **kwargs):
        """Run given generator function 'gen' with arguments 'args' and 'kwargs'
        at each node or server.  If the request is successful, 'rcoro' will be a
        (remote) coroutine; check result with 'isinstance(rcoro,
        asyncoro.Coro)'. Must be used with 'yield' as 'rcoros = yield
        compute.run_each(loc, genf, ...)'.

        If 'where' is 'node', the function is scheduled at every node, if
        'where' is 'server', the function is scheduled at every server (on every
        node); otherwise, 'where' must be IP address, in which case the function
        is scheduled at every server at that node.
        """
        if isinstance(gen, str):
            name = gen
        else:
            name = gen.func_name

        if name in self._xfer_funcs:
            code = None
        else:
            # if not inspect.isgeneratorfunction(gen):
            #     logger.warning('"%s" is not a valid generator function', name)
            #     raise StopIteration([])
            code = inspect.getsource(gen).lstrip()

        def _run(self, coro=None):
            msg = {'req': 'run_each', 'auth': self._auth, 'where': where, 'client': coro,
                   'func': asyncoro.serialize(_DiscoroFunction(name, code, args, kwargs))}
            # TODO: timeout should be for all operations combined?
            n = yield self.scheduler.deliver(msg, timeout=self.timeout)
            if n != 1:
                raise StopIteration([])
            n = yield coro.receive(timeout=self.timeout)
            rcoros = []
            for i in xrange(n):
                rcoro = yield coro.receive(timeout=self.timeout)
                if isinstance(rcoro, Coro):
                    rcoros.append(rcoro)
                    if self.status_coro:
                        msg = DiscoroCoroInfo(rcoro, args, kwargs, time.time())
                        self.status_coro.send(DiscoroStatus(Scheduler.CoroCreated, msg))
            raise StopIteration(rcoros)

        yield Coro(_run, self).finish()

    def run(self, gen, *args, **kwargs):
        """Run given generator function 'gen' with arguments 'args' and 'kwargs'
        at a server with least load at a node with least load.

        Must be used with 'yield' as 'rcoro = yield compute.run(genf, ...)'. If
        the request is successful, 'rcoro' will be a (remote) coroutine.
        """
        yield self.run_at(None, gen, *args, **kwargs)

    def run_nodes(self, gen, *args, **kwargs):
        """Run given generator function 'gen' with arguments 'args' and 'kwargs'
        at a server with least load at every node.

        Must be used with 'yield' as 'rcoros = yield compute.run_nodes(genf,
        ...)'. 'rcoros' will be a list of (remote) coroutines.
        """
        yield self.run_each('node', gen, *args, **kwargs)

    def run_servers(self, gen, *args, **kwargs):
        """Run given generator function 'gen' with arguments 'args' and 'kwargs'
        at every server (at every node).

        Must be used with 'yield' as 'rcoros = yield compute.run_servers(genf,
        ...)'. 'rcoros' will be a list of (remote) coroutines.
        """
        yield self.run_each('server', gen, *args, **kwargs)

    def run_node_servers(self, host, gen, *args, **kwargs):
        """Run given generator function 'gen' with arguments 'args' and 'kwargs'
        at every server at given node at 'host'. 'host' must IP address or host
        name of that node.

        Must be used with 'yield' as 'rcoros = yield
        compute.run_node_servers(host, genf, ...)'. 'rcoros' will be a list of
        (remote) coroutines scheduled at servers on that node.
        """
        if isinstance(host, str) and len(host) > 0:
            # if host starts with digit, assume IP address
            if not host[0].isdigit():
                host = socket.gethostbyname(host)
            yield self.run_each(host, gen, *args, **kwargs)
        else:
            raise StopIteration([])

    # TODO: add 'map' methods to run with arguments as iterators
    # (e.g., list of tuples)

    def nodes(self):
        """Get list of addresses of nodes initialized for this computation. Must
        be used with 'yield' as 'yield compute.nodes()'.
        """

        def _nodes_list(self, coro=None):
            msg = {'req': 'nodes_list', 'auth': self._auth, 'client': coro}
            if (yield self.scheduler.deliver(msg, timeout=self.timeout)) == 1:
                yield coro.receive(self.timeout)
            else:
                raise StopIteration([])

        yield Coro(_nodes_list, self).finish()

    def servers(self):
        """Get list of Location instances of servers initialized for this
        computation. Must be used with 'yield' as 'yield compute.servers()'.
        """

        def _servers_list(self, coro=None):
            msg = {'req': 'servers_list', 'auth': self._auth, 'client': coro}
            if (yield self.scheduler.deliver(msg, timeout=self.timeout)) == 1:
                yield coro.receive(self.timeout)
            else:
                raise StopIteration([])

        yield Coro(_servers_list, self).finish()

    def close(self):
        """Close computation. Must be used with 'yield' as 'yield
        compute.close()'.
        """

        def _close(self, done, coro=None):
            msg = {'req': 'close_computation', 'auth': self._auth, 'client': coro}
            yield self.scheduler.deliver(msg, timeout=self.timeout)
            msg = yield coro.receive(timeout=self.timeout)
            if msg != 'closed':
                logger.warning('%s: closing computation failed?', self._auth)
            self._auth = None
            if self._pulse_coro:
                yield self._pulse_coro.send('quit')
                self._pulse_coro = None
            done.set()

        if self._auth:
            done = asyncoro.Event()
            SysCoro(_close, self, done)
            yield done.wait()

    def _pulse_proc(self, coro=None):
        """For internal use only.
        """
        coro.set_daemon()
        last_pulse = time.time()
        while 1:
            msg = yield coro.receive(timeout=(2 * self._pulse_interval))
            if msg == 'pulse':
                last_pulse = time.time()
            elif msg == 'quit':
                break
            elif msg is None:
                if self.zombie_period and (time.time() - last_pulse) > self.zombie_period:
                    logger.warning('scheduler is zombie!')
                    if self._auth:
                        self._pulse_coro = None
                        yield self.close()
                    break
            else:
                logger.debug('ignoring invalid pulse message')


class Scheduler(object):

    # status indications ('status' attribute of DiscoroStatus)
    NodeDiscovered = 1
    NodeInitialized = 2
    NodeClosed = 3
    NodeIgnore = 4
    NodeDisconnected = 5

    ServerDiscovered = 11
    ServerInitialized = 12
    ServerClosed = 13
    ServerIgnore = 14
    ServerDisconnected = 15

    CoroCreated = 20
    ComputationScheduled = 23
    ComputationClosed = 25

    __metaclass__ = asyncoro.Singleton
    _instance = None

    """This class is for use by Computation class (see below) only.  Other than
    the status indications above, none of its attributes are to be accessed
    directly.
    """

    __status_coro = None

    class _Node(object):

        def __init__(self, name, addr):
            self.name = name
            self.addr = addr
            self.avail_info = None
            self.servers = {}
            self.ncoros = 0
            self.load = 0.0
            self.status = None

        def run(self, func, computation, client):
            where = None
            load = None
            for server in self.servers.itervalues():
                if server.status != Scheduler.ServerInitialized:
                    continue
                if load is None or len(server.rcoros) < load:
                    where = server
                    load = len(server.rcoros)
            if where:
                yield where.run(func, computation, self, client)
            else:
                client.send(None)

    class _Server(object):

        def __init__(self, name, location):
            self.name = name
            self.location = location
            self.coro = None
            self.status = None
            self.rcoros = {}
            self.xfer_files = []
            self.last_pulse = time.time()
            self.askew_results = {}

        def run(self, func, computation, node, client):
            def _run(self, func, coro=None):
                self.coro.send({'req': 'run', 'auth': computation._auth, 'func': func,
                                'client': coro})
                rcoro = yield coro.receive(timeout=computation.timeout)
                if isinstance(rcoro, Coro):
                    # TODO: keep func too for fault-tolerance
                    self.rcoros[rcoro] = rcoro
                    if self.askew_results:
                        msg = self.askew_results.pop(rcoro, None)
                        if msg:
                            Scheduler.__status_coro.send(msg)
                        else:
                            node.ncoros += 1
                            node.load = float(node.ncoros) / len(node.servers)
                    else:
                        node.ncoros += 1
                        node.load = float(node.ncoros) / len(node.servers)
                else:
                    logger.debug('failed to create rcoro: %s / %s',
                                 str(rcoro), computation.timeout)
                raise StopIteration(rcoro)

            rcoro = yield SysCoro(_run, self, func).finish()
            yield client.deliver(rcoro)

    def __init__(self, **kwargs):
        self.__class__._instance = self
        self._nodes = {}
        self._cur_computation = None
        self.__cur_client_auth = None
        self.__pulse_interval = MinPulseInterval
        self.__ping_interval = None
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
        self.__scheduler_coro = SysCoro(self.__scheduler_proc, nodes)
        self.__client_coro = SysCoro(self.__client_proc)
        self.__timer_coro = SysCoro(self.__timer_proc)
        Scheduler.__status_coro = self.__status_coro = SysCoro(self.__status_proc)
        self.__client_coro.register('discoro_scheduler')

    def status(self):
        pending = sum(node.ncoros for node in self._nodes.itervalues())
        servers = reduce(operator.add, [node.servers.keys() for node in self._nodes.itervalues()], [])
        return {'Client': self._cur_computation._pulse_coro.location if self._cur_computation else '',
                'Pending': pending, 'Nodes': self._nodes.keys(), 'Servers': servers
                }

    def print_status(self):
        status = self.status()
        print('')
        print('  Client: %s' % status['Client'])
        print('  Pending: %s' % status['Pending'])
        print('  nodes: %s' % len(status['Nodes']))
        print('  servers: %s' % len(status['Servers']))

    def __status_proc(self, coro=None):
        coro.set_daemon()
        self.asyncoro.peer_status(coro)
        while 1:
            msg = yield coro.receive()
            if isinstance(msg, asyncoro.MonitorException):
                rcoro = msg.args[0]
                if not isinstance(rcoro, Coro):
                    logger.warning('ignoring invalid rcoro %s', type(rcoro))
                    continue
                node = self._nodes.get(rcoro.location.addr, None)
                if not node:
                    logger.warning('node %s is invalid', rcoro.location.addr)
                    continue
                server = node.servers.get(rcoro.location, None)
                if not server:
                    logger.warning('server "%s" is invalid', rcoro.location)
                    continue
                if server.rcoros.pop(rcoro, None) is None:
                    # Due to 'yield' used to create rcoro, scheduler may not
                    # have updated self._rcoros before the coroutine's
                    # MonitorException is received, so put it in
                    # 'askew_results'. The scheduling coroutine will resend it
                    # when it receives rcoro
                    server.askew_results[rcoro] = msg
                    continue

                if self._cur_computation and self._cur_computation.status_coro:
                    if len(msg.args) > 2:
                        msg.args = (msg.args[0], msg.args[1])
                    self._cur_computation.status_coro.send(msg)
                node.ncoros -= 1
                node.load = float(node.ncoros) / len(node.servers)

            elif isinstance(msg, asyncoro.PeerStatus):
                computation = self._cur_computation
                if msg.status == asyncoro.PeerStatus.Online:
                    server = Scheduler._Server(msg.name, msg.location)
                    node = self._nodes.get(msg.location.addr, None)
                    if not node:
                        node = Scheduler._Node(msg.name, msg.location.addr)
                        self._nodes[msg.location.addr] = node
                    node.servers[msg.location] = server
                    if node.status != Scheduler.NodeIgnore:
                        SysCoro(self.__setup_server, server)
                else:
                    # msg.status == asyncoro.PeerStatus.Offline
                    node = self._nodes.get(msg.location.addr, None)
                    if node:
                        server = node.servers.pop(msg.location, None)
                        if server:
                            SysCoro(self.__close_server, server)
                    elif computation and msg.location == computation._pulse_coro.location:
                        logger.warning('client %s terminated; closing computation %s',
                                       msg.location, self.__cur_client_auth)
                        SysCoro(self.__close_computation)

            else:
                logger.warning('invalid status message ignored')

    def __timer_proc(self, coro=None):
        coro.set_daemon()
        server_check = client_pulse = last_ping = time.time()
        async_scheduler = coro.scheduler()
        while 1:
            try:
                msg = yield coro.receive(timeout=self.__pulse_interval)
            except GeneratorExit:
                break
            now = time.time()
            if isinstance(msg, dict):  # message from a node's server
                loc = msg.get('location', None)
                if isinstance(loc, asyncoro.Location):
                    node = self._nodes.get(loc.addr, None)
                    if node:
                        for server in node.servers.itervalues():
                            server.last_pulse = now
                        if msg.get('ncoros', 0) != node.ncoros:
                            asyncoro.logger.warning('Mismatch of remote coroutines running at '
                                                    '%s: %s != %s', loc.addr,
                                                    msg.get('ncoros', 0), node.ncoros)
                        node_status = msg.get('node_status', None)
                        if (node_status and self._cur_computation and
                           self._cur_computation.status_coro):
                            self._cur_computation.status_coro.send(node_status)

                elif msg.get('status', None) in ('ServerClosed', 'ServerTerminated'):
                    location = msg.get('location', None)
                    asyncoro.logger.debug('Server %s closed', location)
                    if isinstance(location, asyncoro.Location):
                        node = self._nodes.get(location.addr, None)
                    else:
                        node = None
                    if node:
                        if msg['status'] == 'ServerTermiated':
                            server = node.servers.pop(location, None)
                        else:
                            server = node.servers.get(location, None)
                    else:
                        server = None
                    if server:
                        yield self.__close_server(server, coro=coro)
                        if all(p.status != Scheduler.ServerInitialized
                               for p in node.servers.itervalues()):
                            node.status = Scheduler.NodeClosed
                            if self._cur_computation and \
                               self._cur_computation.status_coro:
                                self._cur_computation.status_coro.send(
                                    DiscoroStatus(Scheduler.NodeClosed, node.addr))
                            if all(n.status != Scheduler.NodeInitialized
                                   for n in self._nodes.itervalues()):
                                if self._cur_computation and \
                                   self._cur_computation.status_coro:
                                    self._cur_computation.status_coro.send(
                                        DiscoroStatus(Scheduler.ComputationClosed,
                                                      coro.location))
                                SysCoro(self.__close_computation)

            if (now - client_pulse) > self.__pulse_interval and self.__cur_client_auth:
                if self._cur_computation._pulse_coro.send('pulse') == 0:
                    client_pulse = now
                elif (self._cur_computation.zombie_period and
                      (now - client_pulse) > self._cur_computation.zombie_period):
                    logger.warning('Closing zombie computation %s', self.__cur_client_auth)
                    SysCoro(self.__close_computation)

            if (self._cur_computation and self._cur_computation.zombie_period and
               (now - server_check) > self._cur_computation.zombie_period):
                server_check = now
                for node in self._nodes.itervalues():
                    if node.status != Scheduler.NodeInitialized:
                        continue
                    for server in node.servers.itervalues():
                        if server.status != Scheduler.ServerInitialized:
                            continue
                        if (now - server.last_pulse) > self._cur_computation.zombie_period:
                            logger.warning('Server %s is zombie!', server.location)
                            SysCoro(self.__close_server, server)

            if self.__ping_interval and ((now - last_ping) > self.__ping_interval):
                last_ping = now
                SysCoro(async_scheduler.discover_peers)

    def __run(self, func, client):
        host = None
        load = None
        for node in self._nodes.itervalues():
            if node.status != Scheduler.NodeInitialized:
                continue
            if load is None or node.load < load:
                host = node
                load = node.load
        if host:
            yield host.run(func, self._cur_computation, client)
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

            logger.debug('Computation %s scheduled', self._cur_computation._auth)
            self.__pulse_interval = self._cur_computation._pulse_interval
            self.__ping_interval = self._cur_computation._ping_interval

            self.__cur_client_auth = self._cur_computation._auth
            self._cur_computation._auth = Scheduler.auth_code()
            msg = {'resp': 'scheduled', 'auth': self.__cur_client_auth}
            if (yield client.deliver(msg, timeout=min(self._cur_computation.timeout, MsgTimeout))) != 1:
                logger.warning('client not reachable?')
                self._cur_client_auth = None
                self._cur_computation = None
                continue

            if self._cur_computation.status_coro:
                status_msg = DiscoroStatus(Scheduler.ComputationScheduled, id(self._cur_computation))
                self._cur_computation.status_coro.send(status_msg)
            for node in self._nodes.itervalues():
                if node.avail_info:
                    node.status = Scheduler.NodeDiscovered
                    if self._cur_computation.status_coro:
                        status_msg = DiscoroStatus(
                            node.status, DiscoroNodeInfo(node.name, node.addr, node.avail_info)
                            )
                        self._cur_computation.status_coro.send(status_msg)
                else:
                    continue
                for server in node.servers.itervalues():
                    if isinstance(server.coro, Coro):
                        server.status = Scheduler.ServerDiscovered
                        if self._cur_computation.status_coro:
                            status_msg = DiscoroStatus(
                                server.status, DiscoroServerInfo(server.name, server.location)
                                )
                            self._cur_computation.status_coro.send(status_msg)

            self.__timer_coro.send(None)
            for node in self._nodes.itervalues():
                # TODO: check if node is allowed
                if self._cur_computation:
                    self.__setup_node(node)

    def __client_proc(self, coro=None):
        coro.set_daemon()
        computations = {}
        while not self.__terminate:
            msg = yield coro.receive()
            if not isinstance(msg, dict):
                continue
            req = msg.get('req', None)
            client = msg.get('client', None)
            auth = msg.get('auth', None)
            if not isinstance(client, Coro):
                logger.warning('Ignoring invalid client request "%s"', req)
                continue

            if req == 'run':
                func = msg.get('func', None)
                if not func or self.__cur_client_auth != auth:
                    logger.warning('Ignoring invalid request to run computation')
                    client.send(None)
                    continue
                where = msg.get('where', None)
                if not where:
                    SysCoro(self.__run, func, client)
                elif isinstance(where, str):
                    node = self._nodes.get(where, None)
                    if node:
                        SysCoro(node.run, func, self._cur_computation, client)
                    else:
                        client.send(None)
                elif isinstance(where, asyncoro.Location):
                    node = self._nodes.get(where.addr)
                    if node:
                        server = node.servers.get(where)
                        if server:
                            SysCoro(server.run, func, self._cur_computation, node, client)
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
                        SysCoro(node.run, func, self._cur_computation, client)
                elif where == 'server':
                    node_servers = [(node, server) for node in self._nodes.itervalues()
                                    if node.status == Scheduler.NodeInitialized
                                    for server in node.servers.itervalues()
                                    if server.status == Scheduler.ServerInitialized]
                    if (yield client.deliver(len(node_servers), self._cur_computation.timeout)) != 1:
                        continue
                    for node, server in node_servers:
                        SysCoro(server.run, func, self._cur_computation, node, client)
                else:
                    node = self._nodes.get(where)
                    if node and node.status == Scheduler.NodeInitialized:
                        servers = [server for server in node.servers.itervalues()
                                   if server.status == Scheduler.ServerInitialized]
                    else:
                        servers = []
                    if (yield client.deliver(len(servers), self._cur_computation.timeout)) != 1:
                        continue
                    for server in servers:
                        SysCoro(server.run, func, self._cur_computation, node, client)

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
                    assert isinstance(computation._pulse_interval, (float, int))
                    assert (MinPulseInterval <= computation._pulse_interval <= MaxPulseInterval)
                    if computation._ping_interval:
                        assert computation._ping_interval >= MinPulseInterval
                except:
                    logger.warning('ignoring invalid computation request')
                    client.send(None)
                    continue
                while 1:
                    computation._auth = Scheduler.auth_code()
                    if not os.path.exists(os.path.join(self.__dest_path, computation._auth)):
                        break
                try:
                    os.mkdir(os.path.join(self.__dest_path, computation._auth))
                except:
                    logger.debug('Could not create "%s"',
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
                    computation._xfer_files = [(os.path.join(self.__dest_path, computation._auth,
                                                             os.path.join(*(dst.split(sep))),
                                                             xf.split(sep)[-1]),
                                                os.path.join(*(dst.split(sep))), os.sep)
                                               for xf, dst, sep in computation._xfer_files]
                for xf, dst, sep in computation._xfer_files:
                    if not os.path.isfile(xf):
                        logger.warning('File "%s" for computation %s is not valid',
                                       xf, computation._auth)
                        computation = None
                        break
                if computation is None:
                    client.send(None)
                else:
                    # TODO: allow zombie_period to be set?
                    if self.__zombie_period:
                        computation.zombie_period = self.__zombie_period
                    self.__scheduler_coro.send((computation, client))
                    self.__sched_event.set()

            elif req == 'close_computation':
                if self.__cur_client_auth == auth:
                    SysCoro(self.__close_computation, client=client)
                else:
                    computation = computations.pop(auth, None)
                    if computation:
                        computation_path = os.path.join(self.__dest_path, auth)
                        if os.path.isdir(computation_path):
                            shutil.rmtree(computation_path, ignore_errors=True)
                    else:
                        logger.warning('Ignoring invalid request to close computation')
                    client.send('closed')

            elif req == 'nodes_list':
                # TODO: allowed to query anytime, even if current
                # computation is not the one querying?
                if self.__cur_client_auth == auth:
                    nodes = [node.addr for node in self._nodes.itervalues()
                             if node.status == Scheduler.NodeInitialized]
                else:
                    nodes = []
                client.send(nodes)

            elif req == 'servers_list':
                # TODO: allowed to query anytime, even if current
                # computation is not the one querying?
                if self.__cur_client_auth == auth:
                    servers = [server.location for node in self._nodes.itervalues()
                               if node.status == Scheduler.NodeInitialized
                               for server in node.servers.itervalues()
                               if server.status == Scheduler.ServerInitialized]
                else:
                    servers = []
                client.send(servers)

            else:
                logger.warning('Ignoring invalid client request "%s"', req)

    def __setup_node(self, node, coro=None):
        if node.status == Scheduler.NodeIgnore:
            return
        for server in node.servers.itervalues():
            if (server.status == Scheduler.ServerDiscovered or
               server.status == Scheduler.ServerClosed or server.status is None):
                SysCoro(self.__setup_server, server)

    def __setup_server(self, server, coro=None):
        if server.status in (Scheduler.ServerInitialized, Scheduler.ServerIgnore):
            raise StopIteration(0)
        server.status = Scheduler.ServerIgnore
        node = self._nodes.get(server.location.addr, None)
        if not node:
            raise StopIteration(0)
        if not server.coro:
            if self._cur_computation:
                timeout = self._cur_computation.timeout
            else:
                timeout = MsgTimeout
            for _ in range(3):
                server.coro = yield Coro.locate('discoro_server', server.location, timeout=timeout)
                if isinstance(server.coro, Coro):
                    break
                yield coro.sleep(0.2)
            else:
                # logger.debug('server at %s is not valid', server.location)
                # TODO: asuume temporary issue instead of removing it?
                node.servers.pop(server.location, None)
                raise StopIteration(-1)
            if not node.avail_info:
                server.coro.send({'req': 'node_info', 'client': coro})
                node_info = yield coro.receive(timeout=MsgTimeout)
                if node_info and not node.avail_info:
                    node.avail_info = node_info.avail_info
                    node.name = node_info.name
                    node.status = Scheduler.NodeDiscovered
                    if self._cur_computation and self._cur_computation.status_coro:
                        status_msg = DiscoroStatus(
                            node.status, DiscoroNodeInfo(node.name, node.addr, node.avail_info)
                            )
                        self._cur_computation.status_coro.send(status_msg)
                        status_msg = DiscoroStatus(
                            Scheduler.ServerDiscovered,
                            DiscoroServerInfo(server.name, server.location))
                        self._cur_computation.status_coro.send(status_msg)
            if not self._cur_computation:
                server.status = Scheduler.ServerDiscovered
                raise StopIteration(0)
            if self._cur_computation.status_coro:
                status_msg = DiscoroStatus(Scheduler.ServerDiscovered,
                                           DiscoroServerInfo(server.name, server.location))
                self._cur_computation.status_coro.send(status_msg)

        if not self._cur_computation:
            raise StopIteration(0)
        server.coro.send({'req': 'setup', 'client': coro, 'computation': self._cur_computation,
                          'status': self.__timer_coro, 'notify': self.__status_coro})
        ret = yield coro.receive(timeout=self._cur_computation.timeout, alarm_value=-1)
        if ret:
            logger.warning('setup of %s failed: %s', server.coro, ret)
            raise StopIteration(ret)
        for xf, dst, sep in self._cur_computation._xfer_files:
            reply = yield self.asyncoro.send_file(server.location, xf, dir=dst,
                                                  timeout=self._cur_computation.timeout)
            if reply < 0:
                logger.debug('failed to transfer file %s: %s', xf, reply)
                SysCoro(self.__close_server, server)
                raise StopIteration(-1)
        server.status = Scheduler.ServerInitialized
        server.last_pulse = time.time()
        if node.status != Scheduler.NodeInitialized:
            node.status = Scheduler.NodeInitialized
            if self._cur_computation.status_coro:
                self._cur_computation.status_coro.send(DiscoroStatus(node.status, node.addr))
        if self._cur_computation.status_coro:
            self._cur_computation.status_coro.send(DiscoroStatus(server.status, server.location))
        raise StopIteration(0)

    def __close_node(self, node, coro=None):
        if not self._cur_computation:
            logger.warning('Closing node %s ignored', node.addr)
            raise StopIteration(-1)
        close_coros = [SysCoro(self.__close_server, server) for server in node.servers.values()]
        for close_coro in close_coros:
            yield close_coro.finish()

    def __close_server(self, server, coro=None):
        computation = self._cur_computation
        if not computation or server.status != Scheduler.ServerInitialized:
            logger.debug('Closing server %s ignored', server.location)
            raise StopIteration(-1)
        node = self._nodes.get(server.location.addr, None)
        if not node:
            raise StopIteration(-1)
        disconnected = server.location not in node.servers
        if disconnected:
            if computation and computation.status_coro:
                computation.status_coro.send(DiscoroStatus(Scheduler.ServerDisconnected,
                                                           server.location))
        else:
            server.coro.send({'req': 'close', 'auth': computation._auth, 'client': coro})
            yield coro.receive(timeout=computation.timeout)
        if server.rcoros:  # wait a bit for server to terminate coros
            for _ in range(5):
                yield coro.sleep(0.1)
                if not server.rcoros:
                    break
        if server.rcoros:
            logger.warning('%s coros running at %s', len(server.rcoros), server.location)
            if computation and computation.status_coro:
                for rcoro in server.rcoros.itervalues():
                    status = asyncoro.MonitorException(rcoro, (Scheduler.ServerClosed, None))
                    computation.status_coro.send(status)

        server.status = Scheduler.ServerClosed
        server.xfer_files = []
        server.rcoros.clear()
        server.askew_results.clear()
        if computation and computation.status_coro:
            computation.status_coro.send(DiscoroStatus(server.status, server.location))
        if disconnected and not node.servers:
            node.ncoros = 0
            node.load = 0.0
            node.status = Scheduler.NodeClosed
            if computation and computation.status_coro:
                computation.status_coro.send(DiscoroStatus(node.status, node.addr))
        raise StopIteration(0)

    def __close_computation(self, client=None, coro=None):
        computation = self._cur_computation
        if self.__cur_client_auth:
            computation_path = os.path.join(self.__dest_path, self.__cur_client_auth)
            if os.path.isdir(computation_path):
                shutil.rmtree(computation_path, ignore_errors=True)
        close_coros = [SysCoro(self.__close_node, node) for node in self._nodes.values()]
        for close_coro in close_coros:
            yield close_coro.finish()
        if computation and computation.status_coro:
            computation.status_coro.send(DiscoroStatus(Scheduler.ComputationClosed,
                                                       id(computation)))
            computation.status_coro = None
        self._cur_computation = self.__cur_client_auth = None
        self.__sched_event.set()
        if client:
            client.send('closed')
        raise StopIteration(0)


if __name__ == '__main__':
    """The scheduler can be started either within a client program (if no other
    client programs use the nodes simultaneously), or can be run on a node with
    the options described below (usually no options are necessary, so the
    scheduler can be strated with just 'discoro.py')
    """

    import logging
    import argparse
    try:
        import readline
    except ImportError:
        pass

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
    parser.add_argument('--zombie_period', dest='zombie_period', type=int,
                        default=(10 * MaxPulseInterval),
                        help='maximum time in seconds computation is idle')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('--clean', action='store_true', dest='clean', default=False,
                        help='if given, files copied from or generated by clients will be removed')
    parser.add_argument('--daemon', action='store_true', dest='daemon', default=False,
                        help='if given, input is not read from terminal')
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

    daemon = config.pop('daemon', False)
    scheduler = Scheduler(**config)

    if not daemon:
        try:
            if os.getpgrp() != os.tcgetpgrp(sys.stdin.fileno()):
                daemon = True
        except:
            pass

    if daemon:
        # create a coroutine that hangs (so asyncoro won't quit)
        def hang(coro=None):
            yield coro.suspend()
        asyncoro.Coro(hang).value()
    else:
        while 1:
            try:
                cmd = raw_input(
                    '\n\nEnter "quit" or "exit" to terminate discoro scheduler\n'
                    '      "status" to show status of scheduler: '
                    )
                cmd = cmd.strip().lower()
                if cmd in ('quit', 'exit'):
                    break
                if cmd == 'status':
                    scheduler.print_status()
            except KeyboardInterrupt:
                break

    logger.info('terminating discoro scheduler')
