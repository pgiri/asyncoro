# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# This program sends 'rcoro_proc' to remote server processes to execute
# coroutines on them for distributed and parallel execution.

import asyncoro.disasyncoro as asyncoro
import asyncoro.discoro as discoro
from asyncoro.discoro_schedulers import RemoteCoroScheduler

# this generator function is sent to remote server to run
# coroutines there
def rcoro_proc(n, coro=None):
    yield coro.sleep(n)


def client_proc(computation, njobs, coro=None):
    # use RemoteCoroScheduler to schedule/submit coroutines; scheduler must be
    # created before computation is scheduled (next step below)
    rcoro_scheduler = RemoteCoroScheduler(computation)
    # schedule computation (if scheduler is shared, this waits until
    # prior computations are finished)
    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')

    # create njobs remote coroutines; in 'discoro_client*.py' examples,
    # 'schedule' method of RemoteCoroScheduler is used to run remote coroutines
    # so it may be easier to observe coroutines executing at different servers;
    # however,'submit' method can be used to run more than one coroutine (or,
    # many many coroutines, if necessary) concurrently; however, 'submit' may
    # run all the coroutines at the first server found by the scheduler.

    # if only a few jobs (<= 10), run them sequentially (one coroutine per
    # server); otherwise, run them all concurrently on all the servers found.
    if njobs <= 10:
        for n in range(njobs):
            yield rcoro_scheduler.schedule(rcoro_proc, random.uniform(5, 10))
    else:
        # wait a bit for scheduler to initialize all available servers;
        # alternately, 'proc_status' coroutine can be used to wait untl ready
        yield coro.sleep(1)
        for n in range(njobs):
            rcoro = yield rcoro_scheduler.submit(rcoro_proc, random.uniform(5, 10))

    # scheduler will wait until all remote coroutines finish
    yield rcoro_scheduler.finish(close=True)


if __name__ == '__main__':
    import logging, random, sys
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    discoro.Scheduler()
    computation = discoro.Computation([rcoro_proc])
    # run 10 jobs
    asyncoro.Coro(client_proc, computation, 10 if len(sys.argv) < 2 else int(sys.argv[1])).value()
