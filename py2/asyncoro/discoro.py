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
import re

import asyncoro.disasyncoro as asyncoro
from asyncoro import Coro, SysCoro, logger

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2014-2015 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

__all__ = ['Scheduler', 'Computation', 'DiscoroStatus', 'DiscoroCoroInfo',
           'DiscoroNodeInfo', 'DiscoroNodeAvailInfo', 'DiscoroNodeAllocate']

MsgTimeout = asyncoro.MsgTimeout
MinPulseInterval = 10
MaxPulseInterval = 10 * MinPulseInterval

# status about nodes / servers are sent with this structure
DiscoroStatus = collections.namedtuple('DiscoroStatus', ['status', 'info'])
DiscoroCoroInfo = collections.namedtuple('DiscoroCoroInfo', ['coro', 'args', 'kwargs', 'start_time'])
DiscoroNodeInfo = collections.namedtuple('DiscoroNodeInfo', ['name', 'addr', 'cpus', 'platform',
                                                             'avail_info'])

# for internal use only
_DiscoroFunction = collections.namedtuple('_DiscoroFunction', ['name', 'code', 'args', 'kwargs'])


class DiscoroNodeAvailInfo(object):
    """Node availability status is indicated with this class.  'cpu' is
    available CPU in percent in the range 0 to 100. 0 indicates node is busy
    executing tasks on all CPUs and 100 indicates node is not busy at all.
    """

    def __init__(self, location, cpu, memory, disk, swap):
        self.location = location
        self.cpu = cpu
        self.memory = memory
        self.disk = disk
        self.swap = swap


class DiscoroNodeAllocate(object):
    """Allocation of nodes can be customized by specifying 'node_allocations' of
    Computation with DiscoroNodeAllocate instances.

    'ip_addr' must be a regular expression, e.g., "192\.168\.1\..*" (note that
    "." without escape stands for "any" character), 'cpus' must be minimum
    number of servers running on that node, 'memory' is available memory in
    bytes, 'disk' is available disk space (on the partition where discoronode
    servers are running from), and 'platform' is regular expression to match
    output of"platform.platform()" on that node, e.g., "Linux.*x86_64" to accept
    only nodes that run 64-bit Linux.
    """

    def __init__(self, node, platform='', cpus=0, memory=0, disk=0):
        if node.find('*') < 0:
            try:
                node = socket.gethostbyname(node)
            except:
                node = ''

        if node:
            self.ip_rex = node.replace('.', '\\.').replace('*', '.*')
        else:
            logger.warning('node "%s" is invalid', node)
            self.ip_rex = ''
        self.platform = platform.lower()
        self.cpus = cpus
        self.memory = memory
        self.disk = disk

    def allocate(self, ip_addr, name, platform, cpus, memory, disk):
        """When a node is found, scheduler calls this method with IP address,
        name, CPUs, memory and disk available on that node. This method should
        return a number indicating number of CPUs to use. If return value is 0,
        the node is not used; if the return value is < 0, this allocation is
        ignored (next allocation in the 'node_allocations' list, if any, is
        applied).
        """
        if not re.match(self.ip_rex, ip_addr):
            return -1
        if (self.platform and not re.search(self.platform, platform)):
            return -1
        if ((self.memory and memory and self.memory > memory) or
            (self.disk and disk and self.disk > disk)):
            return 0
        if self.cpus > 0:
            if self.cpus > cpus:
                return 0
            return self.cpus
        elif self.cpus == 0:
            return 0
        else:
            cpus += self.cpus
            if cpus < 0:
                return 0
            return cpus


class Computation(object):
    """Packages components to distribute to remote asyncoro schedulers to create
    (remote) coroutines.
    """

    def __init__(self, components, status_coro=None, timeout=MsgTimeout,
                 pulse_interval=(2*MinPulseInterval), ping_interval=None, zombie_period=0,
                 node_allocations=[], node_available=None, node_setup=None,
                 peers_communicate=False):
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
        if ((not isinstance(node_allocations, list)) or
            any(not isinstance(_, (DiscoroNodeAllocate, str)) for _ in node_allocations)):
            raise Exception('"node_allocations" must be list of DiscoroNodeAllocate instances')
        if node_available and not inspect.isgeneratorfunction(node_available):
            raise Exception('"node_available" must be a coroutine (generator function)')
        if node_setup and not inspect.isgeneratorfunction(node_setup):
            raise Exception('"node_setup" must be a coroutine (generator function)')

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
        self._node_allocations = [node if isinstance(node, DiscoroNodeAllocate)
                                  else DiscoroNodeAllocate(node) for node in node_allocations]
        self._node_available = node_available
        if node_setup:
            components.append(node_setup)
            self._node_setup = node_setup.func_name
        else:
            self._node_setup = None
        self._peers_communicate = bool(peers_communicate)
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
                        dst = os.path.dirname(name[len(cwd+os.sep):])
                    elif dep.__package__:
                        dst = dep.__package__.replace('.', os.sep)
                    else:
                        dst = os.path.dirname(dep.__name__.replace('.', os.sep))
                else:
                    name = os.path.abspath(dep)
                    if name.startswith(cwd):
                        dst = os.path.dirname(name[len(cwd+os.sep):])
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
            raise StopIteration(0)
        self._auth = ''
        if self.status_coro is not None and not isinstance(self.status_coro, Coro):
            raise StopIteration(-1)

        self.scheduler = yield Coro.locate('discoro_scheduler', location=location,
                                           timeout=self.timeout)
        if not isinstance(self.scheduler, Coro):
            raise StopIteration(-1)

        def _schedule(self, coro=None):
            self._pulse_coro = SysCoro(self._pulse_proc)
            if self._node_available and self.scheduler.location != self._pulse_coro.location:
                logger.warning('"node_available" is disabled with shared scheduler')
                self._node_available = True
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
                        yield self.close()
                    break
            else:
                logger.debug('ignoring invalid pulse message')
        self._pulse_coro = None


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
            self.cpus = 0
            self.platform = None
            self.avail_info = None
            self.servers = {}
            self.ncoros = 0
            self.load = 0.0
            self.status = None
            self.coro = None

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
        self.__cur_node_allocations = []
        self.__cur_node_available = None
        self.__pulse_interval = MinPulseInterval
        self.__ping_interval = None
        self.__sched_event = asyncoro.Event()
        self.__terminate = False
        self._shared = False
        self._node_port = kwargs.pop('discoronode_port', 51351)
        self.__server_locations = set()

        kwargs['name'] = 'discoro_scheduler'
        clean = kwargs.pop('clean', False)
        self.__zombie_period = kwargs.pop('zombie_period', None)
        nodes = kwargs.pop('nodes', [])
        self.asyncoro = asyncoro.AsynCoro.instance(**kwargs)
        self.__dest_path = os.path.join(self.asyncoro.dest_path, 'discoro', 'discoroscheduler')
        if clean:
            shutil.rmtree(self.__dest_path)
        self.asyncoro.dest_path = self.__dest_path

        self.__scheduler_coro = SysCoro(self.__scheduler_proc, nodes)
        self.__client_coro = SysCoro(self.__client_proc)
        self.__timer_coro = SysCoro(self.__timer_proc)
        Scheduler.__status_coro = self.__status_coro = SysCoro(self.__status_proc)
        self.__client_coro.register('discoro_scheduler')
        self.asyncoro.discover_peers(port=self._node_port)

    def status(self):
        pending = sum(node.ncoros for node in self._nodes.itervalues())
        servers = reduce(operator.add, [node.servers.keys()
                                        for node in self._nodes.itervalues()], [])
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
        coro.register('discoro_status')
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
                if msg.status == asyncoro.PeerStatus.Online:
                    if msg.name.endswith('_proc-0'):
                        SysCoro(self.__discover_node, msg)
                else:
                    # msg.status == asyncoro.PeerStatus.Offline
                    node = self._nodes.get(msg.location.addr, None)
                    if node:
                        server = node.servers.pop(msg.location, None)
                        if server:
                            SysCoro(self.__close_server, server, self._cur_computation)
                        elif node.coro and node.coro.location == msg.location:
                            # TODO: inform scheduler / client
                            self._nodes.pop(msg.location.addr)
                        # if not node.servers:
                        #     self._nodes.pop(msg.location.addr)
                    elif (self._cur_computation and
                          self._cur_computation._pulse_coro.location == msg.location):
                        logger.warning('Client %s terminated; closing computation %s',
                                       msg.location, self.__cur_client_auth)
                        SysCoro(self.__close_computation)

            elif isinstance(msg, dict):  # message from a node's server
                status = msg.get('status', None)
                now = time.time()
                if status == 'pulse':
                    location = msg.get('location', None)
                    if not isinstance(location, asyncoro.Location):
                        continue
                    node = self._nodes.get(location.addr, None)
                    if node:
                        for server in node.servers.itervalues():
                            server.last_pulse = now
                        node_status = msg.get('node_status', None)
                        if (node_status and self._cur_computation and
                           self._cur_computation.status_coro):
                            self._cur_computation.status_coro.send(node_status)

                elif status == Scheduler.ServerInitialized:
                    rcoro = msg.get('coro', None)
                    if not isinstance(rcoro, asyncoro.Coro):
                        continue
                    if (self._cur_computation and
                        self._cur_computation._auth == msg.get('auth', None)):

                        node = self._nodes.get(rcoro.location.addr, None)
                        if node and (node.status == Scheduler.NodeInitialized or
                                     node.status == Scheduler.NodeDiscovered):
                            server = node.servers.get(rcoro.location, None)
                            if not server:
                                server = Scheduler._Server(msg.get('name', None), rcoro.location)
                                server.coro = rcoro
                                node.servers[rcoro.location] = server
                            server.status = Scheduler.ServerInitialized
                            server.last_pulse = now
                            if node.status != Scheduler.NodeInitialized:
                                node.status = Scheduler.NodeInitialized
                                if self._cur_computation.status_coro:
                                    info = DiscoroNodeInfo(node.name, node.addr, node.cpus,
                                                           node.platform, node.avail_info)
                                    self._cur_computation.status_coro.send(
                                        DiscoroStatus(node.status, info))
                            if self._cur_computation.status_coro:
                                self._cur_computation.status_coro.send(DiscoroStatus(server.status,
                                                                                     server.location))
                            if self._cur_computation._peers_communicate:
                                server.coro.send({'req': 'peers',
                                                  'auth': self._cur_computation._auth,
                                                  'peers': list(self.__server_locations)})
                                self.__server_locations.add(server.location)

                elif status in (Scheduler.ServerClosed, Scheduler.ServerDisconnected):
                    location = msg.get('location', None)
                    if not isinstance(location, asyncoro.Location):
                        continue
                    node = self._nodes.get(location.addr, None)
                    if not node:
                        continue
                    if status == Scheduler.ServerClosed:
                        server = node.servers.get(location, None)
                        if server:
                            server.status = Scheduler.ServerIgnore
                    else:
                        server = node.servers.pop(location, None)
                        if not node.servers:
                            node.status = Scheduler.NodeIgnore
                    if not server:
                        continue
                    if self._cur_computation:
                        if self._cur_computation._peers_communicate:
                            self.__server_locations.discard(server.location)
                            # TODO: inform other servers
                        if self._cur_computation.status_coro:
                            self._cur_computation.status_coro.send(DiscoroStatus(status, location))
                            if not node.servers:
                                self._cur_computation.status_coro.send(
                                    DiscoroStatus(Scheduler.NodeClosed, node.addr))
                else:
                    logger.warning('Ignoring invalid status message: %s', status)
            else:
                logger.warning('invalid status message ignored')

    def __node_allocate(self, node):
        for node_allocate in self.__cur_node_allocations:
            cpus = node_allocate.allocate(node.addr, node.name, node.platform, node.cpus,
                                          node.avail_info.memory, node.avail_info.disk)
            if cpus < 0:
                continue
            return min(cpus, node.cpus)
        return node.cpus

    def __get_node_info(self, node, coro=None):
        if not node.coro:
            logger.warning('Node %s is not valid', node.addr)
            raise StopIteration
        node.coro.send({'req': 'discoro_node_info', 'client': coro})
        node_info = yield coro.receive(timeout=MsgTimeout)
        if not node_info:
            self._nodes.pop(node.addr, None)
            asyncoro.Coro(asyncoro.AsynCoro.instance().close_peer, node.coro.location)
            raise StopIteration
        node.name = node_info.name
        node.cpus = node_info.cpus
        node.platform = node_info.platform.lower()
        node.avail_info = node_info.avail_info
        if not self._cur_computation:
            raise StopIteration
        cpus = self.__node_allocate(node)
        if not cpus:
            node.status = Scheduler.NodeIgnore
            raise StopIteration
        if self._cur_computation:
            SysCoro(self.__reserve_node, node, cpus)
        else:
            node.status = Scheduler.NodeDiscovered

    def __reserve_node(self, node, cpus, coro=None):
        computation = self._cur_computation
        if not computation:
            raise StopIteration(-1)
        node.coro.send({'req': 'computation', 'computation': computation, 'cpus': cpus,
                        'status_coro': self.__status_coro, 'client': coro})
        cpus = yield coro.receive(timeout=computation.timeout, alarm_value=-1)
        if not cpus:
            logger.warning('setup of %s failed', node.coro)
            # self._nodes.pop(node.addr, None)
            SysCoro(self.__close_node, node, computation)
            raise StopIteration(-1)
        node.cpus = cpus
        if not self._cur_computation:
            raise StopIteration(0)

        xfer_files = computation._xfer_files
        timeout = computation.timeout
        for xf, dst, sep in xfer_files:
            reply = yield self.asyncoro.send_file(node.coro.location, xf, dir=dst,
                                                  timeout=timeout)
            if reply < 0:
                logger.debug('failed to transfer file %s: %s', xf, reply)
                SysCoro(self.__close_node, node, computation)
                raise StopIteration(-1)

        if self.__cur_node_available:
            if self._shared:
                node.status = Scheduler.NodeDiscovered
                info = DiscoroNodeInfo(node.name, node.addr, node.cpus,
                                       node.platform, node.avail_info)
                computation.status_coro.send(DiscoroStatus(node.status, info))
                raise StopIteration(0)
            try:
                params = yield asyncoro.Coro(self.__cur_node_available, node.avail_info).finish()
                assert params is not None
            except:
                node.status = Scheduler.NodeIgnore
                SysCoro(self.__close_node, node, computation)
                raise StopIteration(-1)
            else:
                if not isinstance(params, tuple):
                    if hasattr(params, '__iter__'):
                        params = tuple(params)
                    else:
                        params = (params,)
        else:
            params = ()

        if computation._node_setup:
            if (yield node.coro.deliver({'req': 'setup', 'params': params,
                                         'auth': computation._auth})) != 1:
                SysCoro(self.__close_node, node, computation)
                raise StopIteration(-1)
        node.status = Scheduler.NodeInitialized
        if computation.status_coro:
            info = DiscoroNodeInfo(node.name, node.addr, node.cpus, node.platform,
                                   node.avail_info)
            computation.status_coro.send(DiscoroStatus(node.status, info))

    def __discover_node(self, msg, coro=None):
        for _ in range(10):
            rcoro = yield Coro.locate('discoro_node', location=msg.location,
                                      timeout=MsgTimeout)
            if not isinstance(rcoro, Coro):
                yield coro.sleep(0.1)
                continue
            node = self._nodes.get(msg.location.addr, None)
            if node and node.coro == rcoro:
                raise StopIteration

            if not node:
                node = Scheduler._Node(msg.name, msg.location.addr)
                self._nodes[msg.location.addr] = node
            node.coro = rcoro
            SysCoro(self.__get_node_info, node)
            raise StopIteration

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
                            logger.warning('discoro server %s is zombie!', server.location)
                            SysCoro(self.__close_server, server, self._cur_computation)

            if self.__ping_interval and ((now - last_ping) > self.__ping_interval):
                last_ping = now
                SysCoro(async_scheduler.discover_peers, port=self._node_port)

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

            self.__pulse_interval = self._cur_computation._pulse_interval
            self.__ping_interval = self._cur_computation._ping_interval

            self.__cur_client_auth = self._cur_computation._auth
            self._cur_computation._auth = Scheduler.auth_code()
            self.__cur_node_allocations = self._cur_computation._node_allocations
            self._cur_computation._node_allocations = []
            self.__cur_node_available = self._cur_computation._node_available
            self._cur_computation._node_available = None

            logger.debug('Computation %s / %s scheduled', self.__cur_client_auth,
                         self._cur_computation._auth)
            msg = {'resp': 'scheduled', 'auth': self.__cur_client_auth}
            if (yield client.deliver(msg, timeout=min(self._cur_computation.timeout, MsgTimeout))) != 1:
                logger.warning('client not reachable?')
                self._cur_client_auth = None
                self._cur_computation = None
                continue

            if self._cur_computation.status_coro:
                status_msg = DiscoroStatus(Scheduler.ComputationScheduled, id(self._cur_computation))
                self._cur_computation.status_coro.send(status_msg)

            self.__timer_coro.send(None)
            if self._cur_computation:
                for node in self._nodes.itervalues():
                    SysCoro(self.__get_node_info, node)
        self.__scheduler_coro = None

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
                    computation = asyncoro.deserialize(msg['computation'])
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

            elif req == 'setup_node':
                addr = msg.get('addr', None)
                params = msg.get('params', None)
                if isinstance(addr, asyncoro.Location):
                    addr = addr.addr
                node = self._nodes.get(addr, None)
                status = -1
                if (self.__cur_client_auth == auth and node and
                    node.status == Scheduler.NodeDiscovered and self._cur_computation._node_setup):
                    if (yield node.coro.deliver({'req': 'setup', 'params': params,
                                                 'auth': self._cur_computation._auth})) == 1:
                        status = 0
                    else:
                        SysCoro(self.__close_node, node, self._cur_computation)
                # client.send(status)

            else:
                logger.warning('Ignoring invalid client request "%s"', req)

    def __close_node(self, node, computation, coro=None):
        if not computation or node.status != Scheduler.NodeInitialized:
            logger.warning('Closing node %s ignored: %s', node.addr, node.status)
            raise StopIteration(-1)
        close_coros = [SysCoro(self.__close_server, server, computation)
                       for server in node.servers.itervalues()]
        for close_coro in close_coros:
            yield close_coro.finish()
        if node.coro:
            node.coro.send({'req': 'release', 'auth': computation._auth, 'client': coro})
            yield coro.receive(timeout=computation.timeout)

    def __close_server(self, server, computation, coro=None):
        if self.__server_locations:
            self.__server_locations.discard(server.location)
            # TODO: inform other servers
        if server.status != Scheduler.ServerInitialized or not computation:
            logger.debug('Closing server %s ignored: %s', server.location, server.status)
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
            yield coro.receive(timeout=computation.timeout if computation else MsgTimeout)
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
        node.servers.pop(server.location, None)
        server.coro = None

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
        self.__server_locations.clear()
        if self.__cur_client_auth:
            computation_path = os.path.join(self.__dest_path, self.__cur_client_auth)
            if os.path.isdir(computation_path):
                shutil.rmtree(computation_path, ignore_errors=True)
        if self._cur_computation:
            close_coros = [SysCoro(self.__close_node, node, self._cur_computation)
                           for node in self._nodes.itervalues()]
            for close_coro in close_coros:
                yield close_coro.finish()
        if self._cur_computation and self._cur_computation.status_coro:
            self._cur_computation.status_coro.send(DiscoroStatus(Scheduler.ComputationClosed,
                                                                 id(self._cur_computation)))
        self.__cur_client_auth = self._cur_computation = None
        self.__sched_event.set()
        if client:
            client.send('closed')
        raise StopIteration(0)

    def close(self, coro=None):
        """Close current computation and quit scheduler.

        Must be called with 'yield' as 'yield scheduler.close()' or as
        coroutine.
        """
        if self.__terminate:
            raise StopIteration(-1)
        self.__terminate = True
        self.__sched_event.set()
        yield self.__close_computation(coro=coro)
        raise StopIteration(0)


if __name__ == '__main__':
    """The scheduler can be started either within a client program (if no other
    client programs use the nodes simultaneously), or can be run on a node with
    the options described below (usually no options are necessary, so the
    scheduler can be strated with just 'discoro.py')
    """

    import logging
    import argparse
    import signal
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
    parser.add_argument('-t', '--tcp_port', dest='tcp_port', type=int, default=0,
                        help='TCP port number to use')
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
    parser.add_argument('--discoronode_port', dest='discoronode_port', type=int, default=51351,
                        help='UDP port number used by discoronode')
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
    _discoro_scheduler = Scheduler(**config)
    _discoro_scheduler._shared = True
    del config

    def sighandler(signum, frame):
        Coro(_discoro_scheduler.close).value()
        raise KeyboardInterrupt

    try:
        signal.signal(signal.SIGHUP, sighandler)
        signal.signal(signal.SIGQUIT, sighandler)
    except:
        pass
    signal.signal(signal.SIGINT, sighandler)
    signal.signal(signal.SIGABRT, sighandler)
    signal.signal(signal.SIGTERM, sighandler)
    del sighandler

    if not daemon:
        try:
            if os.getpgrp() != os.tcgetpgrp(sys.stdin.fileno()):
                daemon = True
        except:
            pass

    if daemon:
        del daemon
        while 1:
            try:
                time.sleep(3600)
            except:
                break
    else:
        del daemon
        while 1:
            try:
                _discoro_cmd = raw_input(
                    '\n\nEnter "quit" or "exit" to terminate discoro scheduler\n'
                    '      "status" to show status of scheduler: '
                    )
            except:
                break
            _discoro_cmd = _discoro_cmd.strip().lower()
            if _discoro_cmd in ('quit', 'exit'):
                Coro(_discoro_scheduler.close).value()
                break
            if _discoro_cmd == 'status':
                _discoro_scheduler.print_status()

    logger.info('terminating discoro scheduler')
