# Run 'discoronode.py' program to start processes to execute computations sent
# by this client, along with this program.

# Distributed computing example where this client sends computation ('compute'
# function)to remote discoro servers to run as remote coroutines and obtain
# results. At any time at most one computation coroutine is scheduled at a
# process, as the computation is supposed to be CPU heavy (although in this
# example they are not).

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *

# this generator function is sent to remote discoro servers to run coroutines
# there
def compute(i, n, coro=None):
    import time
    yield coro.sleep(n)
    raise StopIteration((i, coro.location, time.asctime())) # result of 'compute' is current time

# client (local) coroutine submits computations
def client_proc(computation, njobs, coro=None):
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

    # arguments must correspond to arguments for computaiton; multiple arguments
    # (as in this case) can be given as tuples
    args = [(i, random.uniform(2, 5)) for i in range(njobs)]
    results = yield computation.run_results(compute, args)
    # Coroutines may not be executed in the order of given list of args, but
    # results would be in the same order of given list of args
    for result in results:
        print('    result for %d from %s: %s' % result)

    # wait for all jobs to be done and close computation
    yield computation.close()


if __name__ == '__main__':
    import random, sys
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program), start
    # private scheduler:
    Scheduler()
    # package computation fragments
    computation = Computation([compute])
    # run 10 (or given number of) jobs
    asyncoro.Coro(client_proc, computation, 10 if len(sys.argv) < 2 else int(sys.argv[1]))
