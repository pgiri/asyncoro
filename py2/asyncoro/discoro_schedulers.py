#!/usr/bin/python

"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.

When coroutines are created with 'run' methods of Computation
instances, they are created with a load-balancing algorithm on
available servers with no limit on how many coroutines are run at a
server. This works when coroutines are not CPU bound always. If,
however, coroutines are computations (CPU bound always/mostly), then
it may be more appropriate to schedule one coroutine at a server so
creating a new coroutine waits until a server becomes available.

See 'discomp*.py' files in 'examples' directory for various use
cases.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2015 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

import asyncoro.disasyncoro as asyncoro
import asyncoro.discoro as discoro
from asyncoro.discoro import DiscoroStatus


class ProcScheduler(object):
    """Scheduler for submitting computation jobs.

    ProcScheduler schedules at most one computation (coroutine) at a
    server process at any time (so a node may execute as many
    computation coroutines as there are server processes running on
    that node, but not more).

    NB: When using this scheduler, 'run' method of computation
    shouldn't be used to create (remote) coroutines (unless those
    don't take up CPU), as this scheduler is not aware of those.
    """

    Servers = 0
    ServerEvent = asyncoro.Event()

    def __init__(self, computation):
        self.computation = computation
        computation.status_coro = asyncoro.Coro(self.status_proc)
        self._jobs = 0
        self._jobs_event = asyncoro.Event()

    def submit(self, func, *args, **kwargs):
        """Similar to 'run' method of computation, except as noted
        above: This method will wait until a server process is
        available (i.e., not running another computation).
        """
        while not ProcScheduler.Servers:
            ProcScheduler.ServerEvent.clear()
            yield ProcScheduler.ServerEvent.wait()
        rcoro = yield self.computation.run(func, *args, **kwargs)
        if isinstance(rcoro, asyncoro.Coro):
            ProcScheduler.Servers -= 1
            self._jobs += 1
            self._jobs_event.clear()
        raise StopIteration(rcoro)

    def finish(self, close=False):
        yield self._jobs_event.wait()
        if close:
            yield self.computation.close()

    def status_proc(self, coro=None):
        """Coroutine to process discoro scheduler messages.
        """
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            if isinstance(msg, asyncoro.MonitorException):
                # a job is done
                if msg.args[1][0] != discoro.Scheduler.ServerClosed:
                    ProcScheduler.Servers += 1
                    ProcScheduler.ServerEvent.set()
                    self._jobs -= 1
                    if not self._jobs:
                        self._jobs_event.set()
            elif isinstance(msg, DiscoroStatus):
                if msg.status == discoro.Scheduler.ServerInitialized:
                    ProcScheduler.Servers += 1
                    ProcScheduler.ServerEvent.set()
                elif msg.status == discoro.Scheduler.ServerClosed:
                    ProcScheduler.Servers -= 1
            else:
                asyncoro.logger.debug('Ignoring status message %s' % str(msg))
