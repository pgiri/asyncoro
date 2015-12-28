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

__all__ = ['ProcScheduler']

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

        'proc_status' if not None should be a generator function that
        is called (as coroutine) with the status and info, as received
        by status_coro. If status is ServerInitialized and this
        function returns non-zero value, the server is ignored; i.e.,
        jobs scheduled with 'schedule' or 'execute' will not use that
        server. 'execute_at' will use any given server, though.
        """

        if proc_status:
            if not inspect.isgeneratorfunction(proc_status):
                asyncoro.logger.warning('Invalid proc_status ignored')
                proc_status = None

        self.computation = computation
        self.computation_sign = None
        self.status_coro = Coro(self._status_proc)
        if not computation.status_coro:
            computation.status_coro = self.status_coro
        self._proc_status = proc_status
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
        sloc, loc = self._servers.popitem()
        rcoro = yield self.computation.run_at(loc, gen, *args, **kwargs)
        if isinstance(rcoro, Coro):
            self._rcoros[rcoro] = (None, 1)
        else:
            self._servers[sloc] = loc
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
        sloc, loc = self._servers.popitem()
        rcoro = yield self.computation.run_at(loc, gen, *args, **kwargs)
        if isinstance(rcoro, Coro):
            client = asyncoro.AsynCoro.cur_coro()
            self._rcoros[rcoro] = (client, 1)
            client._await_()
        else:
            self._servers[sloc] = loc
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
            self._rcoros[rcoro] = (client, 0)
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
                client, use_count = self._rcoros.pop(msg.args[0], ('missing', 0))
                if client is None:
                    pass
                elif isinstance(client, Coro):
                    client._proceed_(msg.args[1][1])
                elif client == 'missing':
                    # Due to 'yield' used to create rcoro, scheduler
                    # may not have updated self._rcoros before the
                    # coroutine's MonitorException is received, so put
                    # it back in message queue with a marker added to
                    # 'args' to indicate number of times it went
                    # through the loop. If it loops through 5 times,
                    # assume rcoro is not scheduled (by either
                    # 'schedule' or 'execute')
                    if len(msg.args) > 2:
                        if msg.args[2] > 5:
                            asyncoro.logger.debug('Inavlid rcoro %s exit status ignored: %s' %
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
                    # asyncoro.logger.debug('Ignoring exit status of remote coroutine %s' %
                    #                       msg.args[0])
                    continue
                # assert msg.args[0].location not in self._servers
                self._servers[msg.args[0].location] = msg.args[0].location
                self._server_avail.set()
                if not self._rcoros:
                    self._rcoros_done.set()

            elif isinstance(msg, DiscoroStatus):
                if msg.status == discoro.Scheduler.ServerInitialized:
                    if self._proc_status:
                        def status_proc(self, msg, coro=None):
                            if (yield Coro(self._proc_status, msg.status, msg.info).finish()) == 0:
                                self._servers[msg.info] = msg.info
                                self._server_avail.set()
                        Coro(status_proc, self, msg)
                    else:
                        self._servers[msg.info] = msg.info
                        self._server_avail.set()
                elif msg.status == discoro.Scheduler.ServerClosed:
                    self._servers.pop(msg.info, None)
                    if self._proc_status:
                        Coro(self._proc_status, msg.status, msg.info)
                elif msg.status == discoro.Scheduler.ComputationScheduled:
                    self.computation_sign = msg.info
                    if self._proc_status:
                        Coro(self._proc_status, msg.status, msg.info)
                elif (msg.status == discoro.Scheduler.ComputationClosed and
                      msg.info == self.computation_sign):
                    if self._proc_status:
                        Coro(self._proc_status, msg.status, msg.info)
                    raise StopIteration
                elif msg.status != discoro.Scheduler.CoroCreated:
                    if self._proc_status:
                        Coro(self._proc_status, msg.status, msg.info)
