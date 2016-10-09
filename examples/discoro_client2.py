# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# Distributed computing example where this client sends computation to
# remote discoro process to run as remote coroutines. At any time at
# most one computation coroutine is scheduled at a process. This
# implementation handles server processes terminating abruptly.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *
from asyncoro.discoro_schedulers import RemoteCoroScheduler
import asyncoro.httpd


# The computation in this example is simulated with 'time.sleep' (during which
# entire asyncoro framework is suspended as well). Usually CPU bound tasks that
# don't 'yield' (such as 'time.sleep') shouldn't be executed in coroutines, as
# asyncoro doesn't preempt currently running task. However, in this case it is
# okay, as each server runs at most one coroutine.

# user computations should be generator functions.
def compute(n, coro=None):
    yield coro.sleep(n)

def client_proc(computation, njobs, coro=None):

    # submit jobs
    for i in range(njobs):
        rcoro = yield rcoro_scheduler.schedule(compute, random.uniform(5, 10))
        if isinstance(rcoro, asyncoro.Coro):
            print('  job %s processed by %s' % (i, rcoro.location))
        else:
            print('rcoro %s failed: %s' % (i, rcoro))

    # wait for all jobs to be done and close computation
    yield rcoro_scheduler.finish(close=True)


if __name__ == '__main__':
    import random, sys, asyncoro.discoro
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    Scheduler()
    # send 'compute' generator function;
    # use MinPulseInterval so node status updates are sent more frequently
    # (instead of default 2*MinPulseInterval)
    computation = Computation([compute], timeout=5, pulse_interval=asyncoro.discoro.MinPulseInterval)

    # to illustrate relaying of status messages to multiple coroutines, httpd is
    # also used in this example; this sets computation's status_coro to httpd's status_coro
    httpd = asyncoro.httpd.HTTPServer(computation)
    # create rcoro scheduler; this replaces computation's current staus_coro (in
    # this case httpd status_coro) with coro that chains messages
    rcoro_scheduler = RemoteCoroScheduler(computation)
    # run 10 (or given number of) jobs
    asyncoro.Coro(client_proc, computation, 10 if len(sys.argv) < 2 else int(sys.argv[1])).value()
    # shutdown httpd only after computation is closed
    httpd.shutdown()
