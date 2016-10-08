"""This file is part of asyncoro; see http://asyncoro.sourceforge.net for
details.

'discoro' implements generic coroutine scheduler that creates remote coroutines
at server processes. This scheduler is used by 'run' methods of
computations. This file implements special purpose schedulers using the same
'run' method.
"""

import inspect

import asyncoro.disasyncoro as asyncoro
import asyncoro.discoro as discoro
from asyncoro import Coro
from asyncoro.discoro import DiscoroStatus, DiscoroNodeInfo

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2015 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

__all__ = ['RemoteCoroScheduler']


class RemoteCoroScheduler(object):
    """Scheduler for submitting computation jobs.

    When coroutines are created with 'run' methods of Computation instances,
    they are created with a load-balancing algorithm on available servers with
    no limit on how many coroutines are run at a server. This works when
    coroutines are not CPU bound always. If, however, coroutines are
    computations (CPU bound always/mostly), then it may be more appropriate to
    schedule one coroutine at a server so creating a new coroutine waits until a
    server becomes available.

    RemoteCoroScheduler schedules at most one computation (coroutine) at a
    server process at any time (so a node may execute as many computation
    coroutines as there are server processes running on that node, but not
    more).

    See 'discomp*.py' files in 'examples' directory for some use cases.

    NB: When using this scheduler, 'run' method of computation shouldn't be used
    to create (remote) coroutines (unless those don't take up CPU), as this
    scheduler is not aware of those.
    """

    def __init__(self, computation, status=None, node_available=None,
                 proc_available=None, proc_close=None):
        """'computation' should be an instance of discoro.Computation

        'status' if not None should be a generator function that is called
        (as coroutine) with the status and info, as received by status_coro. If
        status is ServerInitialized and this function returns non-zero value,
        the server is ignored; i.e., jobs scheduled with 'schedule' or 'execute'
        will not use that server.

        'proc_available' if not None should be a generator function that is
        called (as coroutine) with the location of a server process when it
        becomes available (after all 'depends' of computation have been
        transferred). The coroutine runs at the client; it can create remote
        coroutine(s) at the server process, perhaps to setup, such as
        initializing global variables, transfer additional files etc. The
        coroutine should exit with 0 to indicate successful setup; any other
        value is interpretted as failure and not used by scheduler.

        'proc_close' if not None should be a generator function that is called
        (as coroutine) with the status and location of server process when
        server is about to be closed, or already closed. The coroutine runs at
        the client; it can create remote coroutine(s) at server process to
        cleanup, such as delete global variables, transfer files back to client
        etc. The coroutine is called with two parameters: 'status', which is
        either 'discoro.Scheduler.ServerInitialized' when server is about to be
        closed (i.e., server is still available, and remote coroutines can be
        executed), or 'discoro.Scheduler.ServerClosed' when server is already
        closed (e.g., due to zombie_period time elapsed without communication,
        or server was manually closed with command-line etc.), and 'location' of
        server process.
        """

        if status:
            if not inspect.isgeneratorfunction(status):
                asyncoro.logger.warning('Invalid status ignored')
                status = None
        if proc_available:
            if not inspect.isgeneratorfunction(proc_available):
                asyncoro.logger.warning('Invalid proc_available ignored')
                proc_available = None
        if proc_close:
            if not inspect.isgeneratorfunction(proc_close):
                asyncoro.logger.warning('Invalid proc_close ignored')
                proc_close = None
        if not node_available and computation._node_available:
            node_available = computation._node_available
        if node_available:
            if not inspect.isgeneratorfunction(node_available):
                asyncoro.logger.warning('Invalid node_available ignored')
                node_available = None

        self._status = status
        self._proc_available = proc_available
        self._proc_close = proc_close
        self._node_available = node_available
        self._close_servers = {}

        self.computation = computation
        self.computation_sign = None
        self.status_coro = Coro(self._status_proc)
        if isinstance(computation.status_coro, Coro):
            def chain_status_msgs(status_coro, client, coro=None):
                coro.set_daemon()
                while True:
                    msg = yield coro.receive()
                    client.send(msg)
                    status_coro.send(msg)
            computation.status_coro = Coro(chain_status_msgs, self.status_coro,
                                           computation.status_coro)
        else:
            computation.status_coro = self.status_coro
        self._rcoros = {}
        self._rcoros_done = asyncoro.Event()
        self._askew_results = {}
        self._servers = {}
        self._server_avail = asyncoro.Event()
        self._remote_scheduler = False
        self.asyncoro = asyncoro.AsynCoro()
        Coro(computation.schedule)

    def schedule(self, gen, *args, **kwargs):
        """Similar to 'run' method of computation, except as noted above: This
        method will block until a server process is available (i.e., not running
        another computation).

        Must be used with 'yield', similar to 'run' method of Computation
        instance.
        """

        while not self._servers:
            self._server_avail.clear()
            yield self._server_avail.wait()
        sloc, loc = self._servers.popitem()
        rcoro = yield self.computation.run_at(loc, gen, *args, **kwargs)
        if isinstance(rcoro, Coro):
            self._rcoros[rcoro] = (None, 1)
            if self._askew_results:
                msg = self._askew_results.pop(rcoro, None)
                if msg:
                    self.status_coro.send(msg)
        else:
            self._servers[sloc] = loc
            self._server_avail.set()
        raise StopIteration(rcoro)

    def execute(self, gen, *args, **kwargs):
        """Similar to 'run' method of computation, except as noted above: The
        caller (client coroutine) will block until a server process is available
        (i.e., not running another computation), where remote coroutine with
        given 'gen', 'args' and 'kwargs' runs and finishes. The return value is
        the result of computation.

        Must be used with 'yield', similar to 'run' method of Computation
        instance.
        """

        while not self._servers:
            self._server_avail.clear()
            yield self._server_avail.wait()
        sloc, loc = self._servers.popitem()
        rcoro = yield self.computation.run_at(loc, gen, *args, **kwargs)
        if isinstance(rcoro, Coro):
            client = asyncoro.AsynCoro.cur_coro()
            self._rcoros[rcoro] = (client, 1)
            if self._askew_results:
                msg = self._askew_results.pop(rcoro, None)
                if msg:
                    self.status_coro.send(msg)
            client._await_()
        else:
            self._servers[sloc] = loc
            self._server_avail.set()
            raise StopIteration(asyncoro.MonitorException(None, (type(rcoro), rcoro)))

    def execute_at(self, where, gen, *args, **kwargs):
        """Similar to 'run_at' method of computation, except the calling
        coroutine is blocked until the computation finishes and exit value of
        computation is returned. Unlike 'execute', the computation is executed
        right away, even if remote server process is executing another
        computation.

        Must be used with 'yield', similar to 'run_at' method of Computation
        instance.
        """

        rcoro = yield self.computation.run_at(where, gen, *args, **kwargs)
        if isinstance(rcoro, Coro):
            client = asyncoro.AsynCoro.cur_coro()
            self._rcoros[rcoro] = (client, 0)
            if self._askew_results:
                msg = self._askew_results.pop(rcoro, None)
                if msg:
                    self.status_coro.send(msg)
            client._await_()
        else:
            raise StopIteration(asyncoro.MonitorException(None, (type(rcoro), rcoro)))

    def map_results(self, gen, iter):
        """Execute generator 'gen' with arguments from given iterable. The
        return value is list of results that correspond to executing 'gen' with
        arguments in iterable in the same order.

        Must be used with 'yield', as for example,
        'results = yield scheduler.map_results(generator, list_of_tuples)'.
        """
        def exec_proc(gen, *args):
            yield self.execute(gen, *args)

        coros = []
        append_coro = coros.append
        for params in iter:
            if not isinstance(params, tuple):
                if hasattr(params, '__iter__'):
                    params = tuple(params)
                else:
                    params = (params,)
            append_coro(Coro(exec_proc, gen, *params))
        results = [None] * len(coros)
        for i, coro in enumerate(coros):
            result = yield coro.finish()
            results[i] = result
        raise StopIteration(results)

    def submit_at(self, where, gen, *args, **kwargs):
        """Similar to 'run_at' method of computation. If 'where' is None, the
        calling coroutine is blocked until any server is discovered and
        initialized (so computation's 'run_at' will not fail). Unlike
        'schedule', this method doesn't wait for server to be free (i.e., not
        running any other coroutines), nor unlike 'execute_at', the caller is
        not blocked until the coroutine finishes.

        Must be used with 'yield', similar to 'run_at' method of Computation
        instance. The value returned is result of 'run_at' method of computation
        (reference to remote coroutine in case of success, and error otherwise).
        """
        if not where:
            if not self._servers and not self._rcoros:
                yield self._server_avail.wait()
        rcoro = yield self.computation.run_at(where, gen, *args, **kwargs)
        if isinstance(rcoro, Coro):
            self._rcoros[rcoro] = (None, 0)
            if self._askew_results:
                msg = self._askew_results.pop(rcoro, None)
                if msg:
                    self.status_coro.send(msg)
        raise StopIteration(rcoro)

    def submit(self, gen, *args, **kwargs):
        """Submit coroutine at any server; see 'submit_at' above.
        """
        yield self.submit_at(None, gen, *args, **kwargs)

    def finish(self, close=False):
        """Wait until all scheduled coroutines finish. If 'close' is True, the
        computation is closed as well.

        Must be used with 'yield' as 'yield job_scheduler.finish()'.
        """

        self._rcoros_done.clear()
        if self._rcoros:
            yield self._rcoros_done.wait()
        if close:
            if self._proc_close:
                coros = [Coro(self._proc_close, discoro.Scheduler.ServerInitialized, location)
                         for location in self._close_servers]
                self._close_servers = {}
                for coro in coros:
                    yield coro.finish()
            else:
                self._close_servers = {}
        self._rcoros_done.clear()
        if self._rcoros:
            yield self._rcoros_done.wait()
        if close:
            yield self.computation.close()
            self._askew_results.clear()

    def _status_proc(self, coro=None):
        """Internal use only. Coroutine to process discoro scheduler messages.
        """

        coro.set_daemon()
        coro.scheduler().atexit(15, lambda: Coro(self.finish, True).value())
        while 1:
            msg = yield coro.receive()
            if isinstance(msg, asyncoro.MonitorException):
                if msg.args[1][0] == discoro.Scheduler.ServerClosed:
                    continue
                rcoro = msg.args[0]
                client, use_count = self._rcoros.pop(rcoro, ('missing', 0))
                if client is None:
                    pass
                elif isinstance(client, Coro):
                    client._proceed_(msg.args[1][1])
                elif client == 'missing':
                    # Due to 'yield' used to create rcoro, scheduler may not
                    # have updated self._rcoros before the coroutine's
                    # MonitorException is received, so put it in
                    # 'askew_results'. The scheduling coroutine will resend it
                    # when it receives rcoro
                    self._askew_results[rcoro] = msg
                    continue

                else:
                    asyncoro.logger.warning('RemoteCoroScheduler: invalid status message ignored')
                    continue
                if use_count:
                    self._servers[rcoro.location] = rcoro.location
                    self._server_avail.set()
                if not self._rcoros:
                    self._rcoros_done.set()

            elif isinstance(msg, DiscoroStatus):
                if msg.status == discoro.Scheduler.ServerInitialized:
                    if self._proc_available:
                        def setup_proc(self, msg, coro=None):
                            if self._remote_scheduler:
                                yield self.asyncoro.peer(msg.info)
                            if (yield Coro(self._proc_available, msg.info).finish()) == 0:
                                self._close_servers[msg.info] = msg.info
                                self._servers[msg.info] = msg.info
                                self._server_avail.set()
                        Coro(setup_proc, self, msg)
                    elif self._status:
                        def status_proc(self, msg, coro=None):
                            if (yield Coro(self._status, msg.status, msg.info).finish()) == 0:
                                self._servers[msg.info] = msg.info
                                self._server_avail.set()
                        Coro(status_proc, self, msg)
                    else:
                        self._servers[msg.info] = msg.info
                        self._server_avail.set()

                elif msg.status == discoro.Scheduler.ServerClosed:
                    self._servers.pop(msg.info, None)
                    if self._close_servers.pop(msg.info, None) and self._proc_close:
                        Coro(self._proc_close, msg.status, msg.info)
                    elif self._status:
                        Coro(self._status, msg.status, msg.info)
                elif msg.status == discoro.Scheduler.NodeDiscovered:
                    if self._node_available:
                        def setup_node(self, msg, coro=None):
                            if self._remote_scheduler:
                                yield self.asyncoro.peer(msg.info.location)
                            try:
                                params = yield asyncoro.Coro(self._node_available,
                                                             msg.info).finish()
                            except:
                                raise StopIteration

                            if not isinstance(params, tuple):
                                if hasattr(params, '__iter__'):
                                    params = tuple(params)
                                else:
                                    params = (params,)

                            msg = {'req': 'setup_node', 'addr': msg.info.location.addr,
                                   'params': params, 'auth': self.computation._auth,
                                   'client': coro}
                            self.computation.scheduler.send(msg)
                        Coro(setup_node, self, msg)
                elif msg.status == discoro.Scheduler.ComputationScheduled:
                    self.computation_sign = msg.info
                    if self.computation.scheduler.location != self.asyncoro.location:
                        self._remote_scheduler = True
                    if self._status:
                        Coro(self._status, msg.status, msg.info)
                elif (msg.status == discoro.Scheduler.ComputationClosed and
                      msg.info == self.computation_sign):
                    if self._status:
                        Coro(self._status, msg.status, msg.info)
                    raise StopIteration
                elif msg.status != discoro.Scheduler.CoroCreated:
                    if self._status:
                        Coro(self._status, msg.status, msg.info)

# This scheduler was called 'ProcScheduler' in earlier versions
ProcScheduler = RemoteCoroScheduler
