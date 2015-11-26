"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.

'discoro' implements generic coroutine scheduler that creates remote
coroutines at server processes. This scheduler is used by 'run'
methods of computations. This file implements special purpose
schedulers using the same 'run' method.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2015 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

__all__ = ['ProcScheduler', 'NodeScheduler']

import inspect

import asyncoro.disasyncoro as asyncoro
import asyncoro.discoro as discoro
from asyncoro import Coro
from asyncoro.discoro import DiscoroStatus


class ProcScheduler(object):
    """Scheduler for submitting computation jobs.

    When coroutines are created with 'run' methods of Computation
    instances, they are created with a load-balancing algorithm on
    available servers with no limit on how many coroutines are run at
    a server. This works when coroutines are not CPU bound always. If,
    however, coroutines are computations (CPU bound always/mostly),
    then it may be more appropriate to schedule one coroutine at a
    server so creating a new coroutine waits until a server becomes
    available.

    ProcScheduler schedules at most one computation (coroutine) at a
    server process at any time (so a node may execute as many
    computation coroutines as there are server processes running on
    that node, but not more).

    See 'discomp*.py' files in 'examples' directory for some use
    cases.

    NB: When using this scheduler, 'run' method of computation
    shouldn't be used to create (remote) coroutines (unless those
    don't take up CPU), as this scheduler is not aware of those.
    """

    def __init__(self, computation, proc_status=None):
        """'computation' should be an instance of discoro.Computation

        'proc_status' if not None should be a generator function. When
        a server process is initialized or closed, this function is
        executed (as coroutine) with the status and location of server
        as arguments.
        """
        if proc_status:
            if not inspect.isgeneratorfunction(proc_status):
                asyncoro.logger.warning('Invalid proc_status ignored')
                proc_status = None

        self.computation = computation
        self.computation_sign = None
        self.proc_status = proc_status
        self.status_coro = Coro(self._status_proc)
        if not computation.status_coro:
            computation.status_coro = self.status_coro
        self._rcoros = {}
        self._rcoros_done = asyncoro.Event()
        self._servers = {}
        self._server_avail = asyncoro.Event()

    def schedule(self, gen, *args, **kwargs):
        """Similar to 'run' method of computation, except as noted
        above: This method will block until a server process is
        available (i.e., not running another computation).

        Must be used with 'yield', similar to 'run' method of
        Computation instance.
        """
        while not self._servers:
            self._server_avail.clear()
            yield self._server_avail.wait()
        key, val = self._servers.popitem()
        rcoro = yield self.computation.run_at(val, gen, *args, **kwargs)
        if isinstance(rcoro, Coro):
            self._rcoros[str(rcoro)] = (None, 1)
        else:
            self._servers[key] = val
            self._server_avail.set()
        raise StopIteration(rcoro)

    def execute(self, gen, *args, **kwargs):
        """Similar to 'run' method of computation, except as noted
        above: The caller (client coroutine) will block until a server
        process is available (i.e., not running another computation),
        where remote coroutine with given 'gen', 'args' and 'kwargs'
        runs and finishes. The return value is the result of
        computation.

        Must be used with 'yield', similar to 'run' method of
        Computation instance.
        """
        while not self._servers:
            self._server_avail.clear()
            yield self._server_avail.wait()
        key, val = self._servers.popitem()
        rcoro = yield self.computation.run_at(val, gen, *args, **kwargs)
        if isinstance(rcoro, Coro):
            client = asyncoro.AsynCoro.cur_coro()
            self._rcoros[str(rcoro)] = (client, 1)
            client._await_()
        else:
            self._servers[key] = val
            self._server_avail.set()
            raise StopIteration(asyncoro.MonitorException(None, (type(rcoro), rcoro)))

    def execute_at(self, where, gen, *args, **kwargs):
        """Similar to 'run_at' method of computation, except the
        calling coroutine is blocked until the computation finishes
        and exit value of computation is returned. Unlike 'execute',
        the computation is executed right away, even if remote server
        process is executing another computation.

        Must be used with 'yield', similar to 'run_at' method of
        Computation instance.
        """
        rcoro = yield self.computation.run_at(where, gen, *args, **kwargs)
        if isinstance(rcoro, Coro):
            client = asyncoro.AsynCoro.cur_coro()
            self._rcoros[str(rcoro)] = (client, 0)
            client._await_()
        else:
            raise StopIteration(asyncoro.MonitorException(None, (type(rcoro), rcoro)))

    def finish(self, close=False):
        """Wait until all scheduled coroutines finish. If 'close' is
        True, the computation is closed as well.

        Must be used with 'yield' as 'yield job_scheduler.finish()'.
        """
        if self._rcoros:
            self._rcoros_done.clear()
            yield self._rcoros_done.wait()
        if close:
            yield self.computation.close()

    def _status_proc(self, coro=None):
        """Internal use only. Coroutine to process discoro scheduler
        messages.
        """
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            if isinstance(msg, asyncoro.MonitorException):
                if msg.args[1][0] == discoro.Scheduler.ServerClosed:
                    continue
                client, use_count = self._rcoros.pop(str(msg.args[0]), ('missing', 0))
                if client is None:
                    pass
                elif isinstance(client, Coro):
                    client._proceed_(msg.args[1][1])
                elif client == 'missing':
                    # A server may not have updated self._rcoros before
                    # the coroutine's MonitorException is received, so
                    # put it back in message queue with a marker added
                    # to 'args' to indicate number of times it went
                    # through the loop. If it goes through 5 times, it
                    # is an invalid exception and drop it.
                    if len(msg.args) > 2:
                        if msg.args[2] > 5:
                            asyncoro.logger.warning('Inavlid rcoro %s exit status ignored: %s' %
                                                    (msg.args[0], msg.args[2]))
                            continue
                        msg.args = (msg.args[0], msg.args[1], (msg.args[2] + 1))
                    else:
                        msg.args = (msg.args[0], msg.args[1], 1)
                    coro.send(msg)
                    continue

                else:
                    asyncoro.logger.warning('ProcScheduler: invalid status message ignored')
                    continue
                if not use_count:
                    asyncoro.logger.debug('Ignoring exit status of remote coroutine %s' %
                                          msg.args[0])
                    continue
                # assert str(msg.args[0].location) not in self._servers
                self._servers[str(msg.args[0].location)] = msg.args[0].location
                self._server_avail.set()
                if not self._rcoros:
                    self._rcoros_done.set()

            elif isinstance(msg, DiscoroStatus):
                if msg.status == discoro.Scheduler.ServerDiscovered:
                    if self.proc_status:
                        def status_proc(self, msg, coro=None):
                            if (yield Coro(self.proc_status, msg.status, msg.info).finish()) == 0:
                                self._servers[str(msg.info)] = msg.info
                                self._server_avail.set()
                        Coro(status_proc, self, msg)
                    else:
                        self._servers[str(msg.info)] = msg.info
                        self._server_avail.set()
                elif msg.status == discoro.Scheduler.ServerClosed:
                    self._servers.pop(str(msg.info), None)
                    if self.proc_status:
                        Coro(self.proc_status, msg.status, msg.info)
                elif msg.status == discoro.Scheduler.ComputationScheduled:
                    self.computation_sign = msg.info
                elif (msg.status == discoro.Scheduler.ComputationClosed and
                      msg.info == self.computation_sign):
                    raise StopIteration


class NodeScheduler(object):
    """Scheduler for submitting computation jobs.

    NodeScheduler schedules at most one computation (coroutine) at a
    node at any time.

    See 'discomp*.py' files in 'examples' directory for some use
    cases.

    NB: When using this scheduler, 'run' method of computation
    shouldn't be used to create (remote) coroutines (unless those
    don't take up CPU), as this scheduler is not aware of those.
    """

    def __init__(self, computation, node_status=None):
        """'computation' should be an instance of discoro.Computation

        'node_status' if not None should be a generator function. When
        a node is initialized or closed, this function is executed (as
        coroutine) with the status and location of node as arguments.
        """
        if node_status:
            if not inspect.isgeneratorfunction(node_status):
                asyncoro.logger.warning('Invalid node_status ignored')
                node_status = None

        self.computation = computation
        self.computation_sign = None
        self.node_status = node_status
        self.status_coro = Coro(self._status_proc)
        if not computation.status_coro:
            computation.status_coro = self.status_coro
        self._rcoros = {}
        self._rcoros_done = asyncoro.Event()
        self._nodes = {}
        self._node_avail = asyncoro.Event()

    def schedule(self, gen, *args, **kwargs):
        """Similar to 'run' method of computation, except as noted
        above: This method will block until a node is available (i.e.,
        not running another computation).

        Must be used with 'yield', similar to 'run' method of
        Computation instance.
        """
        while not self._nodes:
            self._node_avail.clear()
            yield self._node_avail.wait()
        key, val = self._nodes.popitem()
        rcoro = yield self.computation.run_at(key, gen, *args, **kwargs)
        if isinstance(rcoro, Coro):
            self._rcoros[str(rcoro)] = (None, 1)
        else:
            self._nodes[key] = val
            self._node_avail.set()
        raise StopIteration(rcoro)

    def execute(self, gen, *args, **kwargs):
        """Similar to 'run' method of computation, except as noted
        above: The caller (client coroutine) will block until a node
        is available (i.e., not running another computation), where
        remote coroutine with given 'gen', 'args' and 'kwargs' runs
        and finishes. The return value is the result of computation.

        Must be used with 'yield', similar to 'run' method of
        Computation instance.
        """
        while not self._nodes:
            self._node_avail.clear()
            yield self._node_avail.wait()
        key, val = self._nodes.popitem()
        rcoro = yield self.computation.run_at(key, gen, *args, **kwargs)
        if isinstance(rcoro, Coro):
            client = asyncoro.AsynCoro.cur_coro()
            self._rcoros[str(rcoro)] = (client, 1)
            client._await_()
        else:
            self._nodes[key] = val
            self._node_avail.set()
            raise StopIteration(asyncoro.MonitorException(None, (type(rcoro), rcoro)))

    def execute_at(self, where, gen, *args, **kwargs):
        """Similar to 'run_at' method of computation, except the
        calling coroutine is blocked until the computation finishes
        and exit value of computation is returned. Unlike 'execute',
        the computation is executed right away, even if remote node is
        executing another computation.

        Must be used with 'yield', similar to 'run_at' method of
        Computation instance.
        """
        rcoro = yield self.computation.run_at(where, gen, *args, **kwargs)
        if isinstance(rcoro, Coro):
            client = asyncoro.AsynCoro.cur_coro()
            self._rcoros[str(rcoro)] = (client, 0)
            client._await_()
        else:
            raise StopIteration(asyncoro.MonitorException(None, (type(rcoro), rcoro)))

    def finish(self, close=False):
        """Wait until all scheduled coroutines finish. If 'close' is
        True, the computation is closed as well.

        Must be used with 'yield' as 'yield job_scheduler.finish()'.
        """
        if self._rcoros:
            self._rcoros_done.clear()
            yield self._rcoros_done.wait()
        if close:
            yield self.computation.close()

    def _status_proc(self, coro=None):
        """Internal use only. Coroutine to process discoro scheduler
        messages.
        """
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            if isinstance(msg, asyncoro.MonitorException):
                client, use_count = self._rcoros.pop(str(msg.args[0]), ('missing', 0))
                if client is None:
                    pass
                elif isinstance(client, Coro):
                    client._proceed_(msg.args[1][1])
                elif client == 'missing':
                    # A node may not have updated self._rcoros before
                    # the coroutine's MonitorException is received, so
                    # put it back in message queue with a marker added
                    # to 'args' to indicate number of times it went
                    # through the loop. If it goes through 5 times, it
                    # is an invalid exception and drop it.
                    if len(msg.args) > 2:
                        if msg.args[2] > 5:
                            asyncoro.logger.warning('Inavlid rcoro %s exit status ignored: %s' %
                                                    (msg.args[0], msg.args[2]))
                            continue
                        msg.args = (msg.args[0], msg.args[1], (msg.args[2] + 1))
                    else:
                        msg.args = (msg.args[0], msg.args[1], 1)
                    coro.send(msg)
                    continue

                else:
                    asyncoro.logger.warning('NodeScheduler: invalid status message ignored')
                    continue
                if not use_count:
                    asyncoro.logger.debug('Ignoring exit status of remote coroutine %s' %
                                          msg.args[0])
                    continue
                # assert str(msg.args[0].location.addr) not in self._nodes
                self._nodes[msg.args[0].location.addr] = msg.args[0].location.addr
                self._node_avail.set()
                if not self._rcoros:
                    self._rcoros_done.set()

            elif isinstance(msg, DiscoroStatus):
                if msg.status == discoro.Scheduler.NodeDiscovered:
                    if self.node_status:
                        def status_proc(self, msg, coro=None):
                            if (yield Coro(self.node_status, msg.status, msg.info).finish()) == 0:
                                self._nodes[msg.info] = msg.info
                                self._node_avail.set()
                        Coro(status_proc, self, msg)
                    else:
                        self._nodes[msg.info.addr] = msg.info
                        self._node_avail.set()
                elif msg.status == discoro.Scheduler.NodeClosed:
                    self._nodes.pop(msg.info, None)
                    if self.node_status:
                        Coro(self.node_status, msg.status, msg.info)
                elif msg.status == discoro.Scheduler.ComputationScheduled:
                    self.computation_sign = msg.info
                elif (msg.status == discoro.Scheduler.ComputationClosed and
                      msg.info == self.computation_sign):
                    raise StopIteration
