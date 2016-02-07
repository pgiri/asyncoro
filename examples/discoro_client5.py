# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# Distributed computing example where this client sends computation to
# remote discoro process to run as remote coroutines. At any time at
# most one computation coroutine is scheduled at a process - as done
# in 'dispy' project. This version hangs in case of server process
# terminates abruptly (faults). See 'discomp2.py' for an alternate
# version.

import logging, random
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro_schedulers import RemoteCoroScheduler


# this generator function is sent to remote discoro servers to run
# coroutines there
def compute(n, client, coro=None):
    print('process at %s received: %s' % (coro.location, n))
    yield coro.sleep(n)
    client.send(n)  # send n to results coro at client


def client_proc(computation, coro=None):

    def results_proc(coro=None):
        coro.set_daemon()
        while True:
            result = yield coro.receive()
            print('result: %s' % result)

    job_scheduler = RemoteCoroScheduler(computation)
    results_coro = asyncoro.Coro(results_proc)

    if (yield computation.schedule()):
        raise Exception('schedule failed')

    # submit jobs
    for i in range(5):
        rcoro = yield job_scheduler.schedule(compute, random.uniform(3, 10), results_coro)

    # wait for all results
    yield job_scheduler.finish()
    # submit additional jobs
    print('submitting additional jobs')
    for i in range(2):
        rcoro = yield job_scheduler.schedule(compute, random.uniform(3, 10), results_coro)
    # wait for results and close computation
    yield job_scheduler.finish(close=True)


if __name__ == '__main__':
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    discoro.Scheduler()
    # send 'compute' generator function
    computation = discoro.Computation([compute], timeout=5)
    # call '.value()' of coroutine created here, otherwise main thread
    # may finish (causing interpreter to start cleanup) before asyncoro
    # scheduler gets a chance to start
    asyncoro.Coro(client_proc, computation).value()
