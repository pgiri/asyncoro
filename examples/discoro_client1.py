# This example uses status messages and message passing to run 'setup' coroutine
# at remote process to prepare it for processing jobs.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *
from asyncoro.discoro_schedulers import RemoteCoroScheduler

# this generator function is sent to remote discoro servers to run
# coroutines there
def compute(i, n, coro=None):
    import time
    yield coro.sleep(n)
    raise StopIteration((i, coro.location, time.asctime())) # result of 'compute' is current time

# client (local) coroutine submits computations
def client_proc(computation, njobs, coro=None):
    # execute n jobs (coroutines) and get their results. Number of jobs created
    # can be more than number of server processes available; the scheduler will
    # use as many processes as necessary/available, running one job at a server
    # process

    # arguments must correspond to arguments for computaiton; multiple arguments
    # (as in this case) can be given as tuples
    args = [(i, random.uniform(2, 5)) for i in range(njobs)]
    results = yield rcoro_scheduler.map_results(compute, args)
    # Coroutines may not be executed in the order of given list of args, but
    # results would be in the same order of given list of args
    for result in results:
        print('    result for %d from %s: %s' % result)
    # yield rcoro_scheduler.finish(close=True)


if __name__ == '__main__':
    import random, sys
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program), start
    # private scheduler:
    Scheduler()
    # package computation fragments
    computation = Computation([compute])
    # use RemoteCoroScheduler to start coroutines at servers (should be done
    # before scheduling computation)
    rcoro_scheduler = RemoteCoroScheduler(computation)
    # run 10 (or given number of) jobs
    asyncoro.Coro(client_proc, computation, 10 if len(sys.argv) < 2 else int(sys.argv[1]))
