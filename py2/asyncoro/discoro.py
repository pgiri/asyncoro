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
import asyncoro.discoro

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

    def __init__(self, components, pulse_interval=(5*MinPulseInterval), node_allocations=[],
                 status_coro=None, node_setup=None, server_setup=None,
                 disable_nodes=False, disable_servers=False, peers_communicate=False):
        """'components' should be a list, each element of which is either a
        module, a (generator or normal) function, path name of a file, a class
        or an object (in which case the code for its class is sent).

        'pulse_interval' is interval (number of seconds) used for heart beat
        messages to check if client / scheduler / server is alive. If the other
        side doesn't reply to 5 heart beat messages, it is treated as dead.
        """

        if pulse_interval < MinPulseInterval or pulse_interval > MaxPulseInterval:
            raise Exception('"pulse_interval" must be at least %s and at most %s' %
                            (MinPulseInterval, MaxPulseInterval))
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
        self._node_allocations = [node if isinstance(node, DiscoroNodeAllocate)
                                  else DiscoroNodeAllocate(node) for node in node_allocations]
        self.status_coro = status_coro
        if node_setup:
            components.append(node_setup)
            self._node_setup = node_setup.func_name
        else:
            self._node_setup = None
        if server_setup:
            components.append(server_setup)
            self._server_setup = server_setup.func_name
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

        self.scheduler = yield SysCoro.locate('discoro_scheduler', location=location,
                                              timeout=MsgTimeout)
        if not isinstance(self.scheduler, Coro):
            raise StopIteration(-1)

        def _schedule(self, coro=None):
            self._pulse_coro = SysCoro(self._pulse_proc)
            msg = {'req': 'schedule', 'computation': asyncoro.serialize(self), 'client': coro}
            self.scheduler.send(msg)
            self._auth = yield coro.receive(timeout=MsgTimeout)
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
                       self.scheduler.location, xf, dir=dst, timeout=MsgTimeout)) < 0:
                        logger.warning('Could not send file "%s" to scheduler', xf)
                        yield self.close()
                        raise StopIteration(-1)
            msg = {'req': 'await', 'auth': self._auth, 'client': coro}
            self.scheduler.send(msg)
            resp = yield coro.receive()
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

        def _nodes(self, coro=None):
            msg = {'req': 'nodes', 'auth': self._auth, 'client': coro}
            if (yield self.scheduler.deliver(msg, timeout=MsgTimeout)) == 1:
                yield coro.receive(MsgTimeout)
            else:
                raise StopIteration([])

        yield Coro(_nodes, self).finish()

    def servers(self):
        """Get list of Location instances of servers initialized for this
        computation. Must be used with 'yield' as 'yield compute.servers()'.
        """

        def _servers(self, coro=None):
            msg = {'req': 'servers', 'auth': self._auth, 'client': coro}
            if (yield self.scheduler.deliver(msg, timeout=MsgTimeout)) == 1:
                yield coro.receive(MsgTimeout)
            else:
                raise StopIteration([])

        yield Coro(_servers, self).finish()

    def close(self, await_async=False):
        """Close computation. Must be used with 'yield' as 'yield
        compute.close()'.
        """

        def _close(self, done, coro=None):
            msg = {'req': 'close_computation', 'auth': self._auth, 'client': coro,
                   'await_async': bool(await_async)}
            yield self.scheduler.deliver(msg, timeout=MsgTimeout)
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
            name = gen.func_name

        if name in self._xfer_funcs:
            code = None
        else:
            # if not inspect.isgeneratorfunction(gen):
            #     logger.warning('"%s" is not a valid generator function', name)
            #     raise StopIteration([])
            code = inspect.getsource(gen).lstrip()

        def _run_req(coro=None):
            msg = {'req': 'job', 'auth': self._auth,
                   'job': _DiscoroJob_(request, coro, name, where, cpu, code, args, kwargs)}
            if (yield self.scheduler.deliver(msg, timeout=MsgTimeout)) == 1:
                reply = yield coro.receive()
                if isinstance(reply, Coro):
                    if self.status_coro:
                        msg = DiscoroCoroInfo(reply, args, kwargs, time.time())
                        self.status_coro.send(DiscoroStatus(Scheduler.CoroCreated, msg))
                if not request.endswith('async'):
                    reply = yield coro.receive()
            else:
                reply = None
            raise StopIteration(reply)

        yield Coro(_run_req).finish()

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
            elif msg is None and (time.time() - last_pulse) > (10 * self._pulse_interval):
                logger.warning('scheduler may have gone away!')
            else:
                logger.debug('ignoring invalid pulse message')
        self._pulse_coro = None

    def __getstate__(self):
        state = {}
        for attr in ['_code', '_xfer_funcs', '_xfer_files', '_auth',  'scheduler', 'status_coro',
                     '_pulse_interval', '_pulse_coro', '_node_allocations',
                     '_node_setup', '_server_setup', '_disable_nodes', '_disable_servers',
                      '_peers_communicate']:
            state[attr] = getattr(self, attr)
        return state

    def __setstate__(self, state):
        for attr, value in state.iteritems():
            setattr(self, attr, value)


class _DiscoroJob_(object):
    """Internal use only.
    """
    __slots__ = ('request', 'client', 'name', 'where', 'cpu', 'code', 'args', 'kwargs', 'done')

    def __init__(self, request, client, name, where, cpu, code, args=None, kwargs=None):
        self.request = request
        self.client = client
        self.name = name
        self.where = where
        self.cpu = cpu
        self.code = code
        self.args = asyncoro.serialize(args)
        self.kwargs = asyncoro.serialize(kwargs)
        self.done = None


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
            self.cpus_used = 0
            self.platform = None
            self.avail_info = None
            self.servers = {}
            self.disabled_servers = {}
            self.load = 0.0
            self.status = Scheduler.NodeClosed
            self.coro = None
            self.last_pulse = time.time()
            self.lock = asyncoro.Lock()
            self.avail = asyncoro.Event()
            self.avail.clear()

    class _Server(object):

        def __init__(self, name, location):
            self.name = name
            self.coro = None
            self.status = Scheduler.ServerClosed
            self.rcoros = {}
            self.xfer_files = []
            self.askew_results = {}
            self.avail = asyncoro.Event()
            self.avail.clear()
            self.scheduler = Scheduler._instance

        def run(self, job, computation, node):
            def _run(self, coro=None):
                self.coro.send({'req': 'run', 'auth': computation._auth, 'job': job, 'client': coro})
                rcoro = yield coro.receive(timeout=MsgTimeout)
                # currently fault-tolerancy is not supported, so clear job's
                # args to save space
                job.args = job.kwargs = None
                if isinstance(rcoro, Coro):
                    # TODO: keep func too for fault-tolerance
                    job.done = asyncoro.Event()
                    self.rcoros[rcoro] = (rcoro, job)
                    if self.askew_results:
                        msg = self.askew_results.pop(rcoro, None)
                        if msg:
                            Scheduler.__status_coro.send(msg)
                else:
                    logger.debug('failed to create rcoro: %s', rcoro)
                    if job.cpu:
                        self.avail.set()
                        node.cpus_used -= 1
                        node.load = float(node.cpus_used) / len(node.servers)
                        self.scheduler._avail_nodes.add(node)
                        self.scheduler._nodes_avail.set()
                        node.avail.set()
                raise StopIteration(rcoro)

            rcoro = yield SysCoro(_run, self).finish()
            job.client.send(rcoro)

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
        self.__pulse_interval = kwargs.pop('pulse_interval', MaxPulseInterval)
        self.__ping_interval = kwargs.pop('ping_interval', 0)
        self.__zombie_period = kwargs.pop('zombie_period', 100 * MaxPulseInterval)
        self._node_port = kwargs.pop('discoronode_port', 51351)
        self.__server_locations = set()
        self.__job_scheduler_coro = None

        kwargs['name'] = 'discoro_scheduler'
        clean = kwargs.pop('clean', False)
        nodes = kwargs.pop('nodes', [])
        self.asyncoro = asyncoro.AsynCoro.instance(**kwargs)
        self.__dest_path = os.path.join(self.asyncoro.dest_path, 'discoro', 'discoroscheduler')
        if clean:
            shutil.rmtree(self.__dest_path)
        self.asyncoro.dest_path = self.__dest_path

        self.__computation_sched_event = asyncoro.Event()
        self.__computation_scheduler_coro = SysCoro(self.__computation_scheduler_proc, nodes)
        self.__client_coro = SysCoro(self.__client_proc)
        self.__timer_coro = SysCoro(self.__timer_proc)
        Scheduler.__status_coro = self.__status_coro = SysCoro(self.__status_proc)
        self.__client_coro.register('discoro_scheduler')
        self.asyncoro.discover_peers(port=self._node_port)

    def status(self):
        pending = sum(node.cpus_used for node in self._nodes.itervalues())
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
                if not info:
                    # Due to 'yield' used to create rcoro, scheduler may not
                    # have updated self._rcoros before the coroutine's
                    # MonitorException is received, so put it in
                    # 'askew_results'. The scheduling coroutine will resend it
                    # when it receives rcoro
                    server.askew_results[rcoro] = msg
                    continue
                # assert isinstance(info[1], _DiscoroJob_)
                job = info[1]
                if job.cpu:
                    node.cpus_used -= 1
                    node.load = float(node.cpus_used) / len(node.servers)
                    self._avail_nodes.add(node)
                    self._nodes_avail.set()
                    node.avail.set()
                    server.avail.set()
                if job.request.endswith('async'):
                    if job.done:
                        job.done.set()
                else:
                    job.client.send(msg.args[1][1])
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
                    node = server = None
                    node = self._nodes.get(msg.location.addr, None)
                    if node:
                        server = node.servers.pop(msg.location, None)
                        if server:
                            SysCoro(self.__close_server, server, self._cur_computation)
                        elif node.coro and node.coro.location == msg.location:
                            # TODO: inform scheduler / client
                            self._nodes.pop(msg.location.addr)
                        else:
                            node = None
                        # if not node.servers:
                        #     self._nodes.pop(msg.location.addr)
                    if ((not server and not node) and self._cur_computation and
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
                        info = DiscoroStatus(server.status, server.coro.location)
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
                    node.last_pulse = now
                    if node.status == Scheduler.NodeInitialized:
                        if self._cur_computation.status_coro:
                            self._cur_computation.status_coro.send(
                                DiscoroStatus(server.status, server.coro.location))
                        server.avail.set()
                        node.avail.set()
                        self._avail_nodes.add(node)
                        self._nodes_avail.set()
                    if self._cur_computation._peers_communicate:
                        server.coro.send({'req': 'peers', 'auth': self._cur_computation._auth,
                                          'peers': list(self.__server_locations)})
                        self.__server_locations.add(server.coro.location)

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
                            self.__server_locations.discard(server.coro.location)
                            # TODO: inform other servers
                        if self._cur_computation.status_coro:
                            self._cur_computation.status_coro.send(DiscoroStatus(status, location))
                            # if not node.servers:
                            #     info = DiscoroNodeInfo(node.name, node.addr, node.cpus,
                            #                            node.platform, node.avail_info)
                            #     self._cur_computation.status_coro.send(
                            #         Scheduler.NodeClosed, DiscoroStatus(node.status, info))

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
        # assert node.addr in self._disabled_nodes
        node.coro.send({'req': 'discoro_node_info', 'client': coro})
        node_info = yield coro.receive(timeout=MsgTimeout)
        if not node_info:
            asyncoro.Coro(asyncoro.AsynCoro.instance().close_peer, node.coro.location)
            raise StopIteration
        node.name = node_info.name
        node.cpus = node_info.cpus
        node.platform = node_info.platform.lower()
        node.avail_info = node_info.avail_info
        if self._cur_computation:
            yield self.__init_node(node, coro=coro)

    def __init_node(self, node, setup_args=(), coro=None):
        computation = self._cur_computation
        if not computation or not node.coro:
            raise StopIteration(-1)
        # this coroutine may be invoked in two different paths (when a node is
        # found right after computation is already scheduled, and when
        # computation is scheduled right after a node is found). To prevent
        # concurrent execution (that may reserve / initialize same node more
        # than once), lock is used
        yield node.lock.acquire()
        if node.status == Scheduler.NodeInitialized:
            node.lock.release()
            raise StopIteration(0)

        if node.status == Scheduler.NodeClosed:
            cpus = self.__node_allocate(node)
            if not cpus:
                node.status = Scheduler.NodeIgnore
                node.lock.release()
                raise StopIteration(0)

            node.coro.send({'req': 'reserve', 'cpus': cpus, 'auth': computation._auth,
                            'status_coro': self.__status_coro, 'client': coro,
                            'computation_location': computation._pulse_coro.location})
            cpus = yield coro.receive(timeout=MsgTimeout)
            if not cpus:
                logger.warning('setup of %s failed', node.coro)
                node_coro, node.coro = node.coro, None
                node.lock.release()
                self._disabled_nodes.pop(node.addr, None)
                yield asyncoro.AsynCoro.instance().close_peer(node_coro.location)
                raise StopIteration(-1)

            if computation != self._cur_computation:
                node.status = Scheduler.NodeClosed
                node.coro.send({'req': 'release', 'auth': computation._auth, 'client': None})
                node.lock.release()
                raise StopIteration(-1)

            node.status = Scheduler.NodeDiscovered
            if self._cur_computation and self._cur_computation.status_coro:
                info = DiscoroNodeInfo(node.name, node.addr, node.cpus, node.platform,
                                       node.avail_info)
                self._cur_computation.status_coro.send(DiscoroStatus(node.status, info))
            if self._cur_computation._disable_nodes:
                node.lock.release()
                raise StopIteration(0)

        for xf, dst, sep in computation._xfer_files:
            reply = yield self.asyncoro.send_file(node.coro.location, xf, dir=dst,
                                                  timeout=MsgTimeout)
            if reply < 0 or computation != self._cur_computation:
                logger.debug('failed to transfer file %s: %s', xf, reply)
                node.status = Scheduler.NodeClosed
                node.coro.send({'req': 'release', 'auth': computation._auth, 'client': None})
                node.lock.release()
                raise StopIteration(-1)

        node.coro.send({'req': 'computation', 'computation': computation, 'auth': computation._auth,
                        'setup_args': setup_args, 'client': coro})
        cpus = yield coro.receive(timeout=MsgTimeout)
        if not cpus or computation != self._cur_computation:
            node.status = Scheduler.NodeClosed
            node.coro.send({'req': 'release', 'auth': computation._auth, 'client': None})
            node.lock.release()
            raise StopIteration(-1)
        node.cpus = cpus
        node.status = Scheduler.NodeInitialized
        self._disabled_nodes.pop(node.addr, None)
        self._nodes[node.addr] = node
        if computation.status_coro:
            info = DiscoroNodeInfo(node.name, node.addr, node.cpus, node.platform, node.avail_info)
            computation.status_coro.send(DiscoroStatus(node.status, info))
        node.lock.release()
        if node.servers:
            for server in node.servers.itervalues():
                server.avail.set()
                if computation.status_coro:
                    computation.status_coro.send(DiscoroStatus(server.status, server.coro.location))
            node.avail.set()
            self._avail_nodes.add(node)
            self._nodes_avail.set()

    def __discover_node(self, msg, coro=None):
        for _ in range(10):
            rcoro = yield Coro.locate('discoro_node', location=msg.location, timeout=MsgTimeout)
            if not isinstance(rcoro, Coro):
                yield coro.sleep(0.1)
                continue
            node = self._nodes.get(msg.location.addr, None)
            if not node:
                node = self._disabled_nodes.get(msg.location.addr, None)
            if node and node.coro == rcoro:
                raise StopIteration

            if not node:
                node = Scheduler._Node(msg.name, msg.location.addr)
                self._disabled_nodes[msg.location.addr] = node
            node.coro = rcoro
            yield self.__get_node_info(node, coro=coro)
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
            if self.__cur_client_auth:
                if (now - client_pulse) > self.__pulse_interval:
                    if self._cur_computation._pulse_coro.send('pulse') == 0:
                        client_pulse = now
                    elif ((now - client_pulse) > self.__zombie_period):
                        logger.warning('Closing zombie computation %s', self.__cur_client_auth)
                        SysCoro(self.__close_computation)

                if (now - node_check) > self.__zombie_period:
                    node_check = now
                    for node in self._nodes.itervalues():
                        if (node.status != Scheduler.NodeInitialized and
                            node.status != Scheduler.NodeDiscovered):
                            continue
                        if (now - node.last_pulse) > self.__zombie_period:
                            logger.warning('discoro node %s is zombie!', node.addr)
                            SysCoro(self.__close_node, node, self._cur_computation)

                    if not self._cur_computation._disable_nodes:
                        for node in self._disabled_nodes.itervalues():
                            if node.coro:
                                SysCoro(self.__init_node, node)

            if self.__ping_interval and ((now - last_ping) > self.__ping_interval):
                last_ping = now
                async_scheduler.discover_peers(port=self._node_port)

    @staticmethod
    def auth_code():
        # TODO: use uuid?
        return hashlib.sha1(os.urandom(10).encode('hex')).hexdigest()

    def __computation_scheduler_proc(self, nodes, coro=None):
        coro.set_daemon()
        for node in nodes:
            yield asyncoro.AsynCoro.instance().peer(node, broadcast=True)
        while 1:
            if self._cur_computation:
                self.__computation_sched_event.clear()
                yield self.__computation_sched_event.wait()
                continue

            self._cur_computation, client = yield coro.receive()
            if self.__job_scheduler_coro:
                self.__job_scheduler_coro.terminate()
            self.__pulse_interval = self._cur_computation._pulse_interval

            self.__cur_client_auth = self._cur_computation._auth
            self._cur_computation._auth = Scheduler.auth_code()
            self.__cur_node_allocations = self._cur_computation._node_allocations
            self._cur_computation._node_allocations = []

            self._disabled_nodes.update(self._nodes)
            self._avail_nodes.clear()
            self._nodes_avail.clear()
            for node in self._disabled_nodes.itervalues():
                node.disabled_servers.update(node.servers)
                node.servers.clear()
                node.avail.clear()
                for server in node.disabled_servers.itervalues():
                    server.avail.clear()
            logger.debug('Computation %s / %s scheduled', self.__cur_client_auth,
                         self._cur_computation._auth)
            self.__job_scheduler_coro = SysCoro(self.__job_scheduler_proc)
            msg = {'resp': 'scheduled', 'auth': self.__cur_client_auth}
            if (yield client.deliver(msg, timeout=MsgTimeout)) != 1:
                logger.warning('client not reachable?')
                self._cur_client_auth = None
                self._cur_computation = None
                continue
            for node in self._disabled_nodes.itervalues():
                SysCoro(self.__get_node_info, node)
            self.__timer_coro.send(None)
        self.__computation_scheduler_coro = None

    def __job_scheduler_proc(self, coro=None):
        coro.set_daemon()
        while 1:
            job = yield coro.receive()
            if (not isinstance(job, asyncoro.discoro._DiscoroJob_) or
                not isinstance(job.client, Coro)):
                logger.warning('Ignoring invalid client request "%s"', req)
                continue
            cpu = job.cpu
            where = job.where
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
                        # self._nodes_avail.clear()
                        yield self._nodes_avail.wait()
                    node = None
                    load = None
                    for host in self._nodes.itervalues():
                        if host.avail.is_set() and (load is None or host.load < load):
                            node = host
                            load = host.load
                server = None
                load = None
                for proc in node.servers.itervalues():
                    if cpu:
                        if proc.avail.is_set() and (load is None or len(proc.rcoros) < load):
                            server = proc
                            load = len(proc.rcoros)
                    elif (load is None or len(proc.rcoros) < load):
                        server = proc
                        load = len(proc.rcoros)
                if not server:
                    job.client.send(None)
                    raise StopIteration
                if cpu:
                    server.avail.clear()
                    node.cpus_used += 1
                    node.load = float(node.cpus_used) / len(node.servers)
                    if node.cpus_used == len(node.servers):
                        node.avail.clear()
                        self._avail_nodes.discard(node)
                        if not self._avail_nodes:
                            self._nodes_avail.clear()
                yield server.run(job, self._cur_computation, node)
            elif isinstance(where, str):
                node = self._nodes.get(where, None)
                if not node:
                    job.client.send(None)
                    raise StopIteration
                server = None
                load = None
                for proc in node.servers.itervalues():
                    if cpu:
                        if proc.avail.is_set() and (load is None or len(proc.rcoros) < load):
                            server = proc
                            load = len(proc.rcoros)
                    elif (load is None or len(proc.rcoros) < load):
                        server = proc
                        load = len(proc.rcoros)
                if not server:
                    job.client.send(None)
                    raise StopIteration
                if cpu:
                    server.avail.clear()
                    node.cpus_used += 1
                    node.load = float(node.cpus_used) / len(node.servers)
                    if node.cpus_used == len(node.servers):
                        node.avail.clear()
                        self._avail_nodes.discard(node)
                        if not self._avail_nodes:
                            self._nodes_avail.clear()
                yield server.run(job, self._cur_computation, node)
            elif isinstance(where, asyncoro.Location):
                node = self._nodes.get(where.addr)
                if not node:
                    job.client.send(None)
                    raise StopIteration
                server = node.servers.get(where)
                if not server:
                    job.client.send(None)
                    raise StopIteration
                if cpu:
                    if server.rcoros:
                        yield server.avail.wait()
                    server.avail.clear()
                    node.cpus_used += 1
                    node.load = float(node.cpus_used) / len(node.servers)
                    if node.cpus_used == len(node.servers):
                        node.avail.clear()
                        self._avail_nodes.discard(node)
                        if not self._avail_nodes:
                            self._nodes_avail.clear()
                yield server.run(job, self._cur_computation, node)
            else:
                job.client.send(None)

    def __client_proc(self, coro=None):
        coro.set_daemon()
        computations = {}

        while 1:
            msg = yield coro.receive()
            if not isinstance(msg, dict):
                continue
            req = msg.get('req', None)
            auth = msg.get('auth', None)
            if self.__cur_client_auth != auth:
                if req == 'schedule' or req == 'await':
                    pass
                else:
                    continue

            if req == 'job':
                self.__job_scheduler_coro.send(msg['job'])
                continue

            client = msg.get('client', None)

            if req == 'enable_server':
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
                SysCoro(self.__init_node, node, setup_args=setup_args)

            elif req == 'nodes':
                if isinstance(client, Coro):
                    nodes = [node.addr for node in self._nodes.itervalues()
                             if node.status == Scheduler.NodeInitialized]
                    client.send(nodes)

            elif req == 'servers':
                if isinstance(client, Coro):
                    servers = [server.coro.location for node in self._nodes.itervalues()
                               if node.status == Scheduler.NodeInitialized
                               for server in node.servers.itervalues()
                               # if server.status == Scheduler.ServerInitialized
                               ]
                    client.send(servers)

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
                    self.__computation_scheduler_coro.send((computation, client))
                    self.__computation_sched_event.set()

            elif req == 'close_computation':
                if not isinstance(client, Coro):
                    logger.warning('Ignoring invalid client request "%s"', req)
                    continue
                SysCoro(self.__close_computation, client=client,
                        await_async=msg.get('await_async', False))

            else:
                logger.warning('Ignoring invalid client request "%s"', req)

    def __close_node(self, node, computation, await_async=False, coro=None):
        if (not computation) or (node.status not in (Scheduler.NodeInitialized,
                                                     Scheduler.NodeDiscovered)):
            logger.warning('Closing node %s ignored: %s', node.addr, node.status)
            raise StopIteration(-1)
        while node.cpus_used:
            logger.info('Waiting for %s remote coroutines at %s to finish',
                        node.cpus_used, node.addr)
            node.avail.clear()
            yield node.avail.wait()
        close_coros = [SysCoro(self.__close_server, server, computation, await_async=await_async)
                       for server in node.servers.itervalues()]
        close_coros.extend([SysCoro(self.__close_server, server, computation)
                            for server in node.disabled_servers.itervalues() if server.coro])
        for close_coro in close_coros:
            yield close_coro.finish()
        if node.coro:
            node.coro.send({'req': 'release', 'auth': computation._auth, 'client': coro})
            yield coro.receive(timeout=MsgTimeout)
        node.status = Scheduler.NodeClosed

    def __close_server(self, server, computation, await_async=False, coro=None):
        if ((not computation or not server.coro) or
            (server.status not in (Scheduler.ServerInitialized, Scheduler.ServerDiscovered))):
            logger.debug('Closing server %s ignored: %s', server.coro.location if server.coro else '',
                         server.status)
            raise StopIteration(-1)
        node = self._nodes.get(server.coro.location.addr, None)
        if not node:
            raise StopIteration(-1)
        if self.__server_locations:
            self.__server_locations.discard(server.coro.location)
            # TODO: inform other servers
        disconnected = server.coro.location not in node.servers
        if disconnected:
            if computation and computation.status_coro:
                computation.status_coro.send(DiscoroStatus(Scheduler.ServerDisconnected,
                                                           server.coro.location))
        else:
            if not server.avail.is_set():
                logger.debug('Waiting for remote coroutines at %s to finish', server.coro.location)
                yield server.avail.wait()
            if await_async:
                while server.rcoros:
                    rcoro, job = server.rcoros[next(iter(server.rcoros))]
                    logger.debug('Waiting for %s to finish', rcoro)
                    yield job.done.wait()
            server.coro.send({'req': 'close', 'auth': computation._auth, 'client': coro})
            yield coro.receive(timeout=MsgTimeout)
        if server.rcoros:  # wait a bit for server to terminate coros
            for _ in range(5):
                yield coro.sleep(0.1)
                if not server.rcoros:
                    break
        if server.rcoros:
            logger.warning('%s coros running at %s', len(server.rcoros), server.coro.location)
            if computation and computation.status_coro:
                for (rcoro, cpu) in server.rcoros.itervalues():
                    status = asyncoro.MonitorException(rcoro, (Scheduler.ServerClosed, None))
                    computation.status_coro.send(status)
                node.cpus_used -= len(server.rcoros)
                server.rcoros.clear()

        if computation and computation.status_coro:
            computation.status_coro.send(DiscoroStatus(server.status, server.coro.location))

        node.servers.pop(server.coro.location, None)
        if node.cpus_used == len(node.servers):
            self._avail_nodes.discard(node)
            if not self._avail_nodes:
                self._nodes_avail.clear()
            node.avail.clear()

        if disconnected and not node.servers:
            node.cpus_used = 0
            node.load = 0.0
            node.status = Scheduler.NodeClosed
            if computation and computation.status_coro:
                info = DiscoroNodeInfo(node.name, node.addr, node.cpus,
                                       node.platform, node.avail_info)
                computation.status_coro.send(DiscoroStatus(node.status, info))

        server.status = Scheduler.ServerClosed
        server.xfer_files = []
        server.askew_results.clear()
        server.coro = None
        raise StopIteration(0)

    def __close_computation(self, client=None, await_async=False, coro=None):
        self.__server_locations.clear()
        if self._cur_computation:
            close_coros = [SysCoro(self.__close_node, node, self._cur_computation,
                                   await_async=await_async) for node in self._nodes.itervalues()]
            close_coros.extend([SysCoro(self.__close_node, node, self._cur_computation)
                                for node in self._disabled_nodes.itervalues()
                                if node.status == Scheduler.NodeDiscovered])
            for close_coro in close_coros:
                yield close_coro.finish()
        if self.__cur_client_auth:
            computation_path = os.path.join(self.__dest_path, self.__cur_client_auth)
            if os.path.isdir(computation_path):
                shutil.rmtree(computation_path, ignore_errors=True)
        if self._cur_computation and self._cur_computation.status_coro:
            self._cur_computation.status_coro.send(DiscoroStatus(Scheduler.ComputationClosed,
                                                                 id(self._cur_computation)))
        self.__cur_client_auth = self._cur_computation = None
        self.__computation_sched_event.set()
        if client:
            client.send('closed')
        raise StopIteration(0)

    def close(self, coro=None):
        """Close current computation and quit scheduler.

        Must be called with 'yield' as 'yield scheduler.close()' or as
        coroutine.
        """
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
    parser.add_argument('--pulse_interval', dest='pulse_interval', type=float,
                        default=MaxPulseInterval,
                        help='interval in seconds to send "pulse" messages to check nodes '
                        'and client are connected')
    parser.add_argument('--ping_interval', dest='ping_interval', type=float, default=0,
                        help='interval in seconds to broadcast "ping" message to discover nodes')
    parser.add_argument('--zombie_period', dest='zombie_period', type=int,
                        default=(100 * MaxPulseInterval),
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
        # Coro(_discoro_scheduler.close).value()
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
                break
            if _discoro_cmd == 'status':
                _discoro_scheduler.print_status()

    logger.info('terminating discoro scheduler')
    Coro(_discoro_scheduler.close).value()
