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

import asyncoro.disasyncoro as asyncoro
import asyncoro.discoro as discoro
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

    _Servers = 0
    __ServerAvail = asyncoro.Event()

    def __init__(self, computation):
        self.computation = computation
        self.status_coro = asyncoro.Coro(self.status_proc)
        if not computation.status_coro:
            computation.status_coro = self.status_coro
        self._rcoros = set()
        self._rcoros_done = asyncoro.Event()

    def schedule(self, gen, *args, **kwargs):
        """Similar to 'run' method of computation, except as noted
        above: This method will block until a server process is
        available (i.e., not running another computation).

        Must be used with 'yield', similar to 'run' method of Compute
        instance.
        """
        while not ProcScheduler._Servers:
            ProcScheduler.__ServerAvail.clear()
            yield ProcScheduler.__ServerAvail.wait()
        ProcScheduler._Servers -= 1
        rcoro = yield self.computation.run(gen, *args, **kwargs)
        if isinstance(rcoro, asyncoro.Coro):
            self._rcoros.add(str(rcoro))
        else:
            ProcScheduler._Servers += 1
            ProcScheduler.__ServerAvail.set()
        raise StopIteration(rcoro)

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

    def status_proc(self, coro=None):
        """Coroutine to process discoro scheduler messages.
        """
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            if isinstance(msg, asyncoro.MonitorException):
                if msg.args[1][0] != discoro.Scheduler.ServerClosed:
                    try:
                        self._rcoros.remove(str(msg.args[0]))
                    except KeyError:
                        pass
                    else:
                        ProcScheduler._Servers += 1
                        ProcScheduler.__ServerAvail.set()
                        if not self._rcoros:
                            self._rcoros_done.set()
            elif isinstance(msg, DiscoroStatus):
                if msg.status == discoro.Scheduler.ServerInitialized:
                    ProcScheduler._Servers += 1
                    ProcScheduler.__ServerAvail.set()
                elif msg.status == discoro.Scheduler.ServerClosed:
                    ProcScheduler._Servers -= 1
                elif msg.status == discoro.Scheduler.ComputationClosed and msg.info is None:
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

    _Nodes = 0
    __NodeAvail = asyncoro.Event()

    def __init__(self, computation):
        self.computation = computation
        self.status_coro = asyncoro.Coro(self.status_proc)
        if not computation.status_coro:
            computation.status_coro = self.status_coro
        self._rcoros = set()
        self._rcoros_done = asyncoro.Event()

    def schedule(self, gen, *args, **kwargs):
        """Similar to 'run' method of computation, except as noted
        above: This method will block until a server process is
        available (i.e., not running another computation).

        Must be used with 'yield', similar to 'run' method of Compute
        instance.
        """
        while not NodeScheduler._Nodes:
            NodeScheduler.__NodeAvail.clear()
            yield NodeScheduler.__NodeAvail.wait()
        NodeScheduler._Nodes -= 1
        rcoro = yield self.computation.run(gen, *args, **kwargs)
        if isinstance(rcoro, asyncoro.Coro):
            self._rcoros.add(str(rcoro))
        else:
            NodeScheduler._Nodes += 1
            NodeScheduler.__NodeAvail.set()
        raise StopIteration(rcoro)

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

    def status_proc(self, coro=None):
        """Coroutine to process discoro scheduler messages.
        """
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            if isinstance(msg, asyncoro.MonitorException):
                try:
                    self._rcoros.remove(str(msg.args[0]))
                except KeyError:
                    pass
                else:
                    NodeScheduler._Nodes += 1
                    NodeScheduler.__NodeAvail.set()
                    if not self._rcoros:
                        self._rcoros_done.set()
            elif isinstance(msg, DiscoroStatus):
                if msg.status == discoro.Scheduler.NodeInitialized:
                    NodeScheduler._Nodes += 1
                    NodeScheduler.__NodeAvail.set()
                elif msg.status == discoro.Scheduler.NodeClosed:
                    NodeScheduler._Nodes -= 1
                elif msg.status == discoro.Scheduler.ComputationClosed and msg.info is None:
                    raise StopIteration
