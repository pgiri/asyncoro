#!/usr/bin/python3

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
import functools
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

    def __init__(self, components, timeout=MsgTimeout, pulse_interval=(2*MinPulseInterval),
                 ping_interval=None, zombie_period=0, node_allocations=[], status_coro=None,
                 node_setup=None, server_setup=None, disable_nodes=False, disable_servers=False,
                 peers_communicate=False):
        """'components' should be a list, each element of which is either a
        module, a (generator or normal) function, path name of a file, a class
        or an object (in which case the code for its class is sent).

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
        if status_coro and not isinstance(status_coro, Coro):
            raise Exception('status_coro must be Coro instance')
        if node_setup and not inspect.isgeneratorfunction(node_setup):
            raise Exception('"node_setup" must be a coroutine (generator function)')
        if server_setup and not inspect.isgeneratorfunction(server_setup):
            raise Exception('"server_setup" must be a coroutine (generator function)')
        if (disable_nodes or disable_servers) and not status_coro:
            raise Exception('status_coro must be given when nodes or servers are disabled')

        if not isinstance(components, list):
            components = [components]

        self._code = ''
        self._xfer_funcs = set()
        self._xfer_files = []
        self._auth = None
        self.scheduler = None
        self._pulse_coro = None
        self._pulse_interval = pulse_interval
        self._ping_interval = ping_interval
        self.timeout = timeout
        self.zombie_period = zombie_period
        self._node_allocations = [node if isinstance(node, DiscoroNodeAllocate)
                                  else DiscoroNodeAllocate(node) for node in node_allocations]
        self.status_coro = status_coro
        if node_setup:
            components.append(node_setup)
            self._node_setup = node_setup.__name__
        else:
            self._node_setup = None
        if server_setup:
            components.append(server_setup)
            self._server_setup = server_setup.__name__
        else:
            self._server_setup = None
        self._peers_communicate = bool(peers_communicate)
        self._disable_nodes = bool(disable_nodes)
        self._disable_servers = bool(disable_servers)

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
                        dst = os.path.dirname(name[len(cwd):].lstrip(os.sep))
                    elif dep.__package__:
                        dst = dep.__package__.replace('.', os.sep)
                    else:
                        dst = os.path.dirname(dep.__name__.replace('.', os.sep))
                else:
                    name = os.path.abspath(dep)
                    if name.startswith(cwd):
                        dst = os.path.dirname(name[len(cwd):].lstrip(os.sep))
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
        # Under Windows discoro server may send objects with '__mp_main__'
        # scope, so make an alias to '__main__'.  Do so even if scheduler is not
        # running on Windows; it is possible the client is not Windows, but a
        # node is.
        if os.name == 'nt' and '__mp_main__' not in sys.modules:
            sys.modules['__mp_main__'] = sys.modules['__main__']

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

        self.scheduler = yield SysCoro.locate('discoro_scheduler', location=location,
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
            SysCoro.scheduler().atexit(10, lambda: SysCoro(self.close))
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
            msg = yield coro.receive()
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

    def run_at(self, where, gen, *args, **kwargs):
        """Must be used with 'yield' as

        'rcoro = yield computation.run_at(where, gen, ...)'

        Run given generator function 'gen' with arguments 'args' and 'kwargs' at
        remote server 'where'.  If the request is successful, 'rcoro' will be a
        (remote) coroutine; check result with 'isinstance(rcoro,
        asyncoro.Coro)'. The generator is expected to be (mostly) CPU bound and
        until this is finished, another CPU bound coroutine will not be
        submitted at same server.

        If 'where' is a string, it is assumed to be IP address of a node, in
        which case the coroutine is scheduled at that node on a server at that
        node. If 'where' is a Location instance, it is assumed to be server
        location in which case the coroutine is scheduled at that server.

        'gen' must be generator function, as it is used to run coroutine at
        remote location.
        """
        yield self._run_request('run_async', where, 1, gen, *args, **kwargs)

    def run(self, gen, *args, **kwargs):
        """Run CPU bound coroutine at any remote server; see 'run_at'
        above.
        """
        yield self._run_request('run_async', None, 1, gen, *args, **kwargs)

    def run_result_at(self, where, gen, *args, **kwargs):
        """Must be used with 'yield' as

        'rcoro = yield computation.run_result_at(where, gen, ...)'

        Whereas 'run_at' and 'run' return remote coroutine instance,
        'run_result_at' and 'run_result' wait until remote coroutine is
        finished and return the result of that remote coroutine (i.e., either
        the value of 'StopIteration' or the last value 'yield'ed).

        'where', 'gen', 'args', 'kwargs' are as explained in 'run_at'.
        """
        yield self._run_request('run_result', where, 1, gen, *args, **kwargs)

    def run_result(self, gen, *args, **kwargs):
        """Run CPU bound coroutine at any remote server and return result of
        that coroutine; see 'run_result_at' above.
        """
        yield self._run_request('run_result', None, 1, gen, *args, **kwargs)

    def run_async_at(self, where, gen, *args, **kwargs):
        """Must be used with 'yield' as

        'rcoro = yield computation.run_async_at(where, gen, ...)'

        Run given generator function 'gen' with arguments 'args' and 'kwargs' at
        remote server 'where'.  If the request is successful, 'rcoro' will be a
        (remote) coroutine; check result with 'isinstance(rcoro,
        asyncoro.Coro)'. The generator is supposed to be (mostly) I/O bound and
        not consume CPU time. Unlike other 'run' variants, coroutines created
        with 'async' are not "tracked" by scheduler (see online documentation for
        more details).

        If 'where' is a string, it is assumed to be IP address of a node, in
        which case the coroutine is scheduled at that node on a server at that
        node. If 'where' is a Location instance, it is assumed to be server
        location in which case the coroutine is scheduled at that server.

        'gen' must be generator function, as it is used to run coroutine at
        remote location.
        """
        yield self._run_request('run_async', where, 0, gen, *args, **kwargs)

    def run_async(self, gen, *args, **kwargs):
        """Run I/O bound coroutine at any server; see 'run_async_at'
        above.
        """
        yield self._run_request('run_async', None, 0, gen, *args, **kwargs)

    def run_results(self, gen, iter):
        """Must be used with 'yield', as for example,
        'results = yield scheduler.map_results(generator, list_of_tuples)'.

        Execute generator 'gen' with arguments from given iterable. The return
        value is list of results that correspond to executing 'gen' with
        arguments in iterable in the same order.
        """
        coros = []
        append_coro = coros.append
        for params in iter:
            if not isinstance(params, tuple):
                if hasattr(params, '__iter__'):
                    params = tuple(params)
                else:
                    params = (params,)
            append_coro(Coro(self.run_result, gen, *params))
        results = [None] * len(coros)
        for i, coro in enumerate(coros):
            results[i] = yield coro.finish()
        raise StopIteration(results)

    def enable_node(self, ip_addr, *setup_args):
        """If computation disabled nodes (with 'disabled_nodes=True' when
        Computation is constructed), nodes are not automatically used by the
        scheduler until nodes are enabled with 'enable_node'.

        'ip_addr' must be either IP address or host name of the node to be
        enabled.

        'setup_args' is arguments passed to 'node_setup' function specific to
        that node. If 'node_setup' succeeds (i.e., finishes with value 0), the
        node is used for computations.
        """
        if self.scheduler:
            if isinstance(ip_addr, asyncoro.Location):
                ip_addr = ip_addr.addr
            self.scheduler.send({'req': 'enable_node', 'auth': self._auth, 'addr': ip_addr,
                                 'setup_args': setup_args})

    def enable_server(self, location, *setup_args):
        """If computation disabled servers (with 'disabled_servers=True' when
        Computation is constructed), servers are not automatically used by the
        scheduler until they are enabled with 'enable_server'.

        'location' must be Location instance of the server to be enabled.

        'setup_args' is arguments passed to 'server_setup' function specific to
        that server. If 'server_setup' succeeds (i.e., finishes with value 0), the
        server is used for computations.
        """
        if self.scheduler:
            self.scheduler.send({'req': 'enable_server', 'auth': self._auth, 'server': location,
                                 'setup_args': setup_args})

    def _run_request(self, request, where, cpu, gen, *args, **kwargs):
        """Internal use only.
        """
        if isinstance(gen, str):
            name = gen
        else:
            name = gen.__name__

        if name in self._xfer_funcs:
            code = None
        else:
            # if not inspect.isgeneratorfunction(gen):
            #     logger.warning('"%s" is not a valid generator function', name)
            #     raise StopIteration([])
            code = inspect.getsource(gen).lstrip()

        def _run(self, coro=None):
            msg = {'req': request, 'auth': self._auth, 'where': where, 'client': coro,
                   'func': asyncoro.serialize(_DiscoroFunction(name, code, args, kwargs)),
                   'cpu': cpu}
            if (yield self.scheduler.deliver(msg, timeout=self.timeout)) == 1:
                reply = yield coro.receive()
                if self.status_coro and isinstance(reply, Coro):
                    msg = DiscoroCoroInfo(reply, args, kwargs, time.time())
                    self.status_coro.send(DiscoroStatus(Scheduler.CoroCreated, msg))
            else:
                reply = None
            raise StopIteration(reply)

        yield Coro(_run, self).finish()

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

    def __getstate__(self):
        state = {}
        for attr in ['_code', '_xfer_funcs', '_xfer_files', '_auth', '_pulse_interval', '_pulse_coro',
                     '_ping_interval', 'timeout', 'zombie_period', 'scheduler', 'status_coro',
                     '_node_allocations', '_node_setup', '_server_setup', '_peers_communicate',
                     '_disable_nodes', '_disable_servers']:
            state[attr] = getattr(self, attr)
        return state

    def __setstate__(self, state):
        for attr, value in state.items():
            setattr(self, attr, value)


class Scheduler(object, metaclass=asyncoro.Singleton):

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
            self.disabled_servers = {}
            self.ncoros = 0
            self.load = 0.0
            self.status = None
            self.coro = None
            self.last_pulse = time.time()
            self.lock = asyncoro.Lock()
            self.avail = asyncoro.Event()
            self.avail.clear()

    class _Server(object):

        def __init__(self, name, location):
            self.name = name
            self.location = location
            self.coro = None
            self.status = None
            self.rcoros = {}
            self.xfer_files = []
            self.askew_results = {}
            self.avail = asyncoro.Event()
            self.avail.clear()
            self.scheduler = Scheduler._instance

        def run(self, req, func, cpu, computation, node, client):
            def _run(self, func, coro=None):
                self.coro.send({'req': 'run', 'auth': computation._auth, 'func': func,
                                'client': coro})
                rcoro = yield coro.receive(timeout=computation.timeout)
                if isinstance(rcoro, Coro):
                    # TODO: keep func too for fault-tolerance
                    if req.endswith('_async'):
                        self.rcoros[rcoro] = (rcoro, cpu, None)
                    else:
                        self.rcoros[rcoro] = (rcoro, cpu, client)
                    if self.askew_results:
                        msg = self.askew_results.pop(rcoro, None)
                        if msg:
                            Scheduler.__status_coro.send(msg)
                else:
                    logger.debug('failed to create rcoro: %s / %s',
                                 str(rcoro), computation.timeout)
                    if cpu:
                        self.avail.set()
                        node.ncoros -= 1
                        node.load = float(node.ncoros) / len(node.servers)
                        self.scheduler._avail_nodes.add(node)
                        self.scheduler._nodes_avail.set()
                        node.avail.set()
                raise StopIteration(rcoro)

            rcoro = yield SysCoro(_run, self, func).finish()
            if req.endswith('_async'):
                yield client.deliver(rcoro)

    def __init__(self, **kwargs):
        self.__class__._instance = self
        self._nodes = {}
        self._disabled_nodes = {}
        self._avail_nodes = set()
        self._nodes_avail = asyncoro.Event()
        self._nodes_avail.clear()

        self._cur_computation = None
        self.__cur_client_auth = None
        self.__cur_node_allocations = []
        self.__pulse_interval = MinPulseInterval
        self.__ping_interval = None
        self.__sched_event = asyncoro.Event()
        self.__terminate = False
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
        SysCoro(self.asyncoro.discover_peers, port=self._node_port)

    def status(self):
        pending = sum(node.ncoros for node in self._nodes.values())
        servers = functools.reduce(operator.add, [list(node.servers.keys())
                                                  for node in self._nodes.values()], [])
        return {'Client': self._cur_computation._pulse_coro.location if self._cur_computation else '',
                'Pending': pending, 'Nodes': list(self._nodes.keys()), 'Servers': servers
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
            now = time.time()
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
                node.last_pulse = now
                info = server.rcoros.pop(rcoro, None)
                if info is None:
                    # Due to 'yield' used to create rcoro, scheduler may not
                    # have updated self._rcoros before the coroutine's
                    # MonitorException is received, so put it in
                    # 'askew_results'. The scheduling coroutine will resend it
                    # when it receives rcoro
                    server.askew_results[rcoro] = msg
                    continue
                assert info[0] == rcoro
                if info[1]:
                    node.ncoros -= 1
                    node.load = float(node.ncoros) / len(node.servers)
                    self._avail_nodes.add(node)
                    self._nodes_avail.set()
                    node.avail.set()
                    server.avail.set()
                if info[2]:
                    info[2].send(msg.args[1][1])
                if self._cur_computation and self._cur_computation.status_coro:
                    if len(msg.args) > 2:
                        msg.args = (msg.args[0], msg.args[1])
                    self._cur_computation.status_coro.send(msg)

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
                if status == 'pulse':
                    location = msg.get('location', None)
                    if not isinstance(location, asyncoro.Location):
                        continue
                    node = self._nodes.get(location.addr, None)
                    if node:
                        node.last_pulse = now
                        node_status = msg.get('node_status', None)
                        if (node_status and self._cur_computation and
                           self._cur_computation.status_coro):
                            self._cur_computation.status_coro.send(node_status)

                elif status == Scheduler.ServerDiscovered:
                    rcoro = msg.get('coro', None)
                    if not isinstance(rcoro, asyncoro.Coro):
                        continue
                    node = self._nodes.get(rcoro.location.addr, None)
                    if not node:
                        node = self._disabled_nodes.get(rcoro.location.addr, None)
                    if not node or (node.status != Scheduler.NodeInitialized and
                                    node.status != Scheduler.NodeDiscovered):
                        continue
                    server = node.servers.get(rcoro.location, None)
                    if not server:
                        server = node.disabled_servers.get(rcoro.location, None)
                    if server:
                        continue
                    server = Scheduler._Server(msg.get('name', None), rcoro.location)
                    server.coro = rcoro
                    server.status = Scheduler.ServerDiscovered
                    node.disabled_servers[rcoro.location] = server
                    if self._cur_computation and self._cur_computation.status_coro:
                        info = DiscoroStatus(server.status, server.location)
                        self._cur_computation.status_coro.send(info)

                elif status == Scheduler.ServerInitialized:
                    rcoro = msg.get('coro', None)
                    if not isinstance(rcoro, asyncoro.Coro):
                        continue
                    if (not self._cur_computation or
                        self._cur_computation._auth != msg.get('auth', None)):
                        continue
                    node = self._nodes.get(rcoro.location.addr, None)
                    if not node:
                        node = self._disabled_nodes.get(rcoro.location.addr, None)
                    if not node or (node.status != Scheduler.NodeInitialized and
                                    node.status != Scheduler.NodeDiscovered):
                        continue
                    server = node.disabled_servers.pop(rcoro.location, None)
                    if not server:
                        server = node.servers.get(rcoro.location, None)
                    if server:
                        if server.status != Scheduler.ServerDiscovered:
                            continue
                    else:
                        server = Scheduler._Server(msg.get('name', None), rcoro.location)
                        server.coro = rcoro

                    server.status = Scheduler.ServerInitialized
                    node.servers[rcoro.location] = server
                    server.avail.set()
                    node.avail.set()
                    self._avail_nodes.add(node)
                    self._nodes_avail.set()
                    node.last_pulse = now
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
                        server.coro.send({'req': 'peers', 'auth': self._cur_computation._auth,
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
                    if not node.servers:
                        self._avail_nodes.discard(node)
                        if not self._avail_nodes:
                            self._nodes_avail.clear()
                        # self._nodes.pop(node.addr, None)
                    if self._cur_computation:
                        if self._cur_computation._peers_communicate:
                            self.__server_locations.discard(server.location)
                            # TODO: inform other servers
                        if self._cur_computation.status_coro:
                            self._cur_computation.status_coro.send(DiscoroStatus(status, location))
                            # if not node.servers:
                            #     info = DiscoroNodeInfo(node.name, node.addr, node.cpus,
                            #                            node.platform, node.avail_info)
                            #     self._cur_computation.status_coro.send(
                            #         Scheduler.NodeClosed, DiscoroStatus(node.status, info))

                elif status == Scheduler.NodeInitialized:
                    location = msg.get('location', None)
                    if not isinstance(location, asyncoro.Location):
                        continue
                    if (not self._cur_computation or
                        self._cur_computation._auth != msg.get('auth', None)):
                        continue
                    node = self._nodes.get(location.addr, None)
                    if not node:
                        node = self._disabled_nodes.pop(location.addr, None)
                    if not node:
                        continue
                    node.status = Scheduler.NodeInitialized
                    self._nodes[location.addr] = node
                    if self._cur_computation.status_coro:
                        info = DiscoroNodeInfo(node.name, node.addr, node.cpus,
                                               node.platform, node.avail_info)
                        self._cur_computation.status_coro.send(
                            DiscoroStatus(node.status, info))

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
        node.status = Scheduler.NodeDiscovered
        if not self._cur_computation:
            raise StopIteration
        if self._cur_computation.status_coro:
            info = DiscoroNodeInfo(node.name, node.addr, node.cpus, node.platform,
                                   node.avail_info)
            self._cur_computation.status_coro.send(DiscoroStatus(node.status, info))
        if not self._cur_computation._disable_nodes:
            SysCoro(self.__reserve_node, node)

    def __reserve_node(self, node, setup_args=(), coro=None):
        computation = self._cur_computation
        if not computation:
            raise StopIteration(-1)
        cpus = self.__node_allocate(node)
        if not cpus:
            node.status = Scheduler.NodeIgnore
            raise StopIteration(0)
        # this coroutine may be invoked in two different paths (when a node is
        # found righ after computation is already scheduled, and when
        # computation is scheduled right after a node is found). To prevent
        # concurrent execution, lock is used
        yield node.lock.acquire()
        if node.status == Scheduler.NodeInitialized:
            node.lock.release()
            raise StopIteration(0)
        node.coro.send({'req': 'computation', 'computation': computation, 'cpus': cpus,
                        'setup_args': setup_args, 'status_coro': self.__status_coro, 'client': coro})
        cpus = yield coro.receive(timeout=computation.timeout, alarm_value=-1)
        if not cpus:
            logger.warning('setup of %s failed', node.coro)
            # self._nodes.pop(node.addr, None)
            node.lock.release()
            SysCoro(self.__close_node, node, computation)
            raise StopIteration(-1)
        node.cpus = cpus
        if computation != self._cur_computation:
            node.lock.release()
            raise StopIteration(0)

        xfer_files = computation._xfer_files
        timeout = computation.timeout
        for xf, dst, sep in xfer_files:
            reply = yield self.asyncoro.send_file(node.coro.location, xf, dir=dst,
                                                  timeout=timeout)
            if reply < 0:
                logger.debug('failed to transfer file %s: %s', xf, reply)
                node.lock.release()
                SysCoro(self.__close_node, node, computation)
                raise StopIteration(-1)

        if computation != self._cur_computation:
            SysCoro(self.__close_node, node, computation)
            raise StopIteration(0)

        node.status = Scheduler.NodeInitialized
        if computation.status_coro:
            info = DiscoroNodeInfo(node.name, node.addr, node.cpus, node.platform, node.avail_info)
            computation.status_coro.send(DiscoroStatus(node.status, info))
        node.lock.release()

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
                if self._cur_computation and self._cur_computation._disable_nodes:
                    self._disabled_nodes[node.addr] = node
                else:
                    self._nodes[node.addr] = node
            node.coro = rcoro
            SysCoro(self.__get_node_info, node)
            raise StopIteration

    def __timer_proc(self, coro=None):
        coro.set_daemon()
        node_check = client_pulse = last_ping = time.time()
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
               (now - node_check) > self._cur_computation.zombie_period):
                node_check = now
                for node in self._nodes.values():
                    if node.status != Scheduler.NodeInitialized:
                        continue
                    if (now - node.last_pulse) > self._cur_computation.zombie_period:
                        logger.warning('discoro node %s is zombie!', node.addr)
                        SysCoro(self.__close_node, node, self._cur_computation)

            if self.__ping_interval and ((now - last_ping) > self.__ping_interval):
                last_ping = now
                SysCoro(async_scheduler.discover_peers, port=self._node_port)

    @staticmethod
    def auth_code():
        # TODO: use uuid?
        return hashlib.sha1(''.join(hex(_)[2:] for _ in os.urandom(10)).encode()).hexdigest()

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
                if self._cur_computation._disable_nodes:
                    self._disabled_nodes.update(self._nodes)
                    self._nodes.clear()
                    nodes = self._disabled_nodes
                else:
                    self._nodes.update(self._disabled_nodes)
                    self._disabled_nodes.clear()
                    nodes = self._nodes
                for node in nodes.values():
                    if self._cur_computation._disable_servers:
                        node.disabled_servers.update(node.servers)
                        node.servers.clear()
                    else:
                        node.servers.update(node.disabled_servers)
                        node.disabled_servers.clear()
                    SysCoro(self.__get_node_info, node)
        self.__scheduler_coro = None

    def __client_proc(self, coro=None):
        def _run(msg, req, client, auth, coro=None):
            if not isinstance(client, Coro):
                logger.warning('Ignoring invalid client request "%s"', req)
                raise StopIteration
            func = msg.get('func', None)
            if not func or self.__cur_client_auth != auth:
                logger.warning('Ignoring invalid request to run computation')
                client.send(None)
                raise StopIteration
            cpu = msg.get('cpu', 1)
            where = msg.get('where', None)
            if not where:
                if cpu:
                    while not self._avail_nodes:
                        # self._nodes_avail.clear()
                        yield self._nodes_avail.wait()
                    node = None
                    load = None
                    for host in self._avail_nodes:
                        if host.avail.is_set() and (load is None or host.load < load):
                            node = host
                            load = host.load
                else:
                    while not self._nodes:
                        self._nodes_avail.clear()
                        yield self._nodes_avail.wait()
                    node = None
                    load = None
                    for host in self._nodes.values():
                        if host.avail.is_set() and (load is None or host.load < load):
                            node = host
                            load = host.load
                server = None
                load = None
                for proc in node.servers.values():
                    if cpu:
                        if proc.avail.is_set() and (load is None or len(proc.rcoros) < load):
                            server = proc
                            load = len(proc.rcoros)
                    elif (load is None or len(proc.rcoros) < load):
                        server = proc
                        load = len(proc.rcoros)
                if not server:
                    client.send(None)
                    raise StopIteration
                if cpu:
                    server.avail.clear()
                    node.ncoros += 1
                    node.load = float(node.ncoros) / len(node.servers)
                    if node.ncoros == len(node.servers):
                        node.avail.clear()
                        self._avail_nodes.discard(node)
                        if not self._avail_nodes:
                            self._nodes_avail.clear()
                yield server.run(req, func, cpu, self._cur_computation, node, client)
            elif isinstance(where, str):
                node = self._nodes.get(where, None)
                if not node:
                    client.send(None)
                    raise StopIteration
                server = None
                load = None
                for proc in node.servers.values():
                    if cpu:
                        if proc.avail.is_set() and (load is None or len(proc.rcoros) < load):
                            server = proc
                            load = len(proc.rcoros)
                    elif (load is None or len(proc.rcoros) < load):
                        server = proc
                        load = len(proc.rcoros)
                if not server:
                    client.send(None)
                    raise StopIteration
                if cpu:
                    server.avail.clear()
                    node.ncoros += 1
                    node.load = float(node.ncoros) / len(node.servers)
                    if node.ncoros == len(node.servers):
                        node.avail.clear()
                        self._avail_nodes.discard(node)
                        if not self._avail_nodes:
                            self._nodes_avail.clear()
                yield server.run(req, func, cpu, self._cur_computation, node, client)
            elif isinstance(where, asyncoro.Location):
                node = self._nodes.get(where.addr)
                if not node:
                    client.send(None)
                    raise StopIteration
                server = node.servers.get(where)
                if not server:
                    client.send(None)
                    raise StopIteration
                if cpu:
                    if server.rcoros:
                        yield server.avail.wait()
                    server.avail.clear()
                    node.ncoros += 1
                    node.load = float(node.ncoros) / len(node.servers)
                    if node.ncoros == len(node.servers):
                        node.avail.clear()
                        self._avail_nodes.discard(node)
                        if not self._avail_nodes:
                            self._nodes_avail.clear()
                yield server.run(req, func, cpu, self._cur_computation, node, client)
            else:
                client.send(None)

        coro.set_daemon()
        computations = {}
        while not self.__terminate:
            msg = yield coro.receive()
            if not isinstance(msg, dict):
                continue
            req = msg.get('req', None)
            client = msg.get('client', None)
            auth = msg.get('auth', None)

            if req.startswith('run_'):
                SysCoro(_run, msg, req, client, auth)

            elif req == 'enable_server':
                req = msg.get('req', None)
                client = msg.get('client', None)
                auth = msg.get('auth', None)
                if self.__cur_client_auth != auth:
                    continue
                loc = msg.get('server', None)
                if not loc:
                    continue
                node = self._nodes.get(loc.addr, None)
                if not node:
                    continue
                server = node.disabled_servers.get(loc, None)
                if not server or not server.coro or server.status != Scheduler.ServerDiscovered:
                    continue
                args = msg.get('setup_args', ())
                server.coro.send({'req': 'enable_server', 'setup_args': args,
                                  'auth': self._cur_computation._auth})

            elif req == 'enable_node':
                req = msg.get('req', None)
                client = msg.get('client', None)
                auth = msg.get('auth', None)
                if self.__cur_client_auth != auth:
                    continue
                addr = msg.get('addr', None)
                if not addr:
                    continue
                node = self._disabled_nodes.pop(addr, None)
                if not node:
                    continue
                if not node or not node.coro or node.status != Scheduler.NodeDiscovered:
                    continue
                setup_args = msg.get('setup_args', ())
                self._nodes[node.addr] = node
                SysCoro(self.__reserve_node, node, setup_args=setup_args)

            elif req == 'schedule':
                if not isinstance(client, Coro):
                    logger.warning('Ignoring invalid client request "%s"', req)
                    continue
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
                if not isinstance(client, Coro):
                    logger.warning('Ignoring invalid client request "%s"', req)
                    continue
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
                if not isinstance(client, Coro):
                    logger.warning('Ignoring invalid client request "%s"', req)
                    continue
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
                    client.send(None)

            elif req == 'nodes_list':
                if not isinstance(client, Coro):
                    logger.warning('Ignoring invalid client request "%s"', req)
                    continue
                # TODO: allowed to query anytime, even if current
                # computation is not the one querying?
                if self.__cur_client_auth == auth:
                    nodes = [node.addr for node in self._nodes.values()
                             if node.status == Scheduler.NodeInitialized]
                else:
                    nodes = []
                client.send(nodes)

            elif req == 'servers_list':
                if not isinstance(client, Coro):
                    logger.warning('Ignoring invalid client request "%s"', req)
                    continue
                # TODO: allowed to query anytime, even if current
                # computation is not the one querying?
                if self.__cur_client_auth == auth:
                    servers = [server.location for node in self._nodes.values()
                               if node.status == Scheduler.NodeInitialized
                               for server in node.servers.values()
                               # if server.status == Scheduler.ServerInitialized
                               ]
                else:
                    servers = []
                client.send(servers)

            else:
                logger.warning('Ignoring invalid client request "%s"', req)

    def __close_node(self, node, computation, coro=None):
        if not computation or node.status != Scheduler.NodeInitialized:
            logger.warning('Closing node %s ignored: %s', node.addr, node.status)
            raise StopIteration(-1)
        while node.ncoros:
            logger.info('Waiting for %s remote coroutines at %s to finish', node.ncoros, node.addr)
            node.avail.clear()
            yield node.avail.wait()
        close_coros = [SysCoro(self.__close_server, server, computation)
                       for server in node.servers.values()]
        close_coros.extend([SysCoro(self.__close_server, server, computation)
                            for server in node.disabled_servers.values()])
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
            if not server.avail.is_set():
                logger.debug('Waiting for remote coroutines at %s to finish', server.location)
                yield server.avail.wait()
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
                for (rcoro, cpu) in server.rcoros.values():
                    status = asyncoro.MonitorException(rcoro, (Scheduler.ServerClosed, None))
                    computation.status_coro.send(status)
                node.ncoros -= len(server.rcoros)
                server.rcoros.clear()

        server.status = Scheduler.ServerClosed
        server.xfer_files = []
        server.askew_results.clear()
        server.coro = None
        node.servers.pop(server.location, None)
        if node.ncoros == len(node.servers):
            self._avail_nodes.discard(node)
            if not self._avail_nodes:
                self._nodes_avail.clear()
            node.avail.clear()

        if computation and computation.status_coro:
            computation.status_coro.send(DiscoroStatus(server.status, server.location))
        if disconnected and not node.servers:
            node.ncoros = 0
            node.load = 0.0
            node.status = Scheduler.NodeClosed
            if computation and computation.status_coro:
                info = DiscoroNodeInfo(node.name, node.addr, node.cpus,
                                       node.platform, node.avail_info)
                computation.status_coro.send(DiscoroStatus(node.status, info))
        raise StopIteration(0)

    def __close_computation(self, client=None, coro=None):
        self.__server_locations.clear()
        if self._cur_computation:
            close_coros = [SysCoro(self.__close_node, node, self._cur_computation)
                           for node in self._nodes.values()]
            for close_coro in close_coros:
                yield close_coro.finish()
        if self.__cur_client_auth:
            computation_path = os.path.join(self.__dest_path, self.__cur_client_auth)
            if os.path.isdir(computation_path):
                shutil.rmtree(computation_path, ignore_errors=True)
        if self._cur_computation and self._cur_computation.status_coro:
            self._cur_computation.status_coro.send(DiscoroStatus(Scheduler.ComputationClosed,
                                                                 id(self._cur_computation)))
        if client:
            client.send('closed')
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
    parser.add_argument('--certfile', dest='certfile', default='',
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default='',
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

    if config['certfile']:
        config['certfile'] = os.path.abspath(config['certfile'])
    else:
        config['certfile'] = None
    if config['keyfile']:
        config['keyfile'] = os.path.abspath(config['keyfile'])
    else:
        config['keyfile'] = None

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
                _discoro_cmd = input(
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
