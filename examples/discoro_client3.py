# Run 'discoronode.py' program to start processes to execute computations sent
# by this client, along with this program.

# Example where this client sends computation to remote discoro process to run
# as remote coroutines. Remote coroutines and client use message passing to
# exchange objects (instance of class 'C'). Instead of using 'map_results' as
# done in 'discoro_client1.py', remote coroutine sends the result back to client
# with message passing.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *


# objects of C are exchanged between client and servers
class C(object):
    def __init__(self, i):
        self.i = i
        self.n = None

    def __repr__(self):
        return '%d: %s' % (self.i, self.n)


# this generator function is sent to remote discoro servers to run coroutines
# there
def compute(obj, client, coro=None):
    # obj is an instance of C
    import math
    # this coroutine and client can use message passing
    print('process at %s received: %s' % (coro.location, obj.n))
    yield coro.sleep(obj.n)
    obj.n = math.sqrt(obj.n)
    # send result back to client
    yield client.deliver(obj, timeout=5)


def client_proc(computation, njobs, coro=None):
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

    # create a separate coroutine to receive results, so they can be processed
    # as soon as received
    def recv_results(coro=None):
        for i in range(njobs):
            msg = yield coro.receive()
            print('    result for job %d: %s' % (i, msg))

    # remote coroutines send replies as messages to this coro
    results_coro = asyncoro.Coro(recv_results)

    # run njobs; each job will be executed by one discoro server
    for i in range(njobs):
        cobj = C(i)
        cobj.n = random.uniform(5, 10)
        # as noted in 'discoro_client2.py', 'run' method is used to run jobs
        # sequentially; use 'run_async' to run multiple jobs on one server
        # concurrently
        print('  request %d: %s' % (i, cobj.n))
        rcoro = yield computation.run(compute, cobj, results_coro)
        if not isinstance(rcoro, asyncoro.Coro):
            print('failed to create rcoro %s: %s' % (i, rcoro))

    # wait for all results and close computation
    yield computation.close()


if __name__ == '__main__':
    import random, sys
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    Scheduler()
    # send generator function and class C (as the computation uses
    # objects of C)
    computation = Computation([compute, C])
    # run 10 (or given number of) jobs
    asyncoro.Coro(client_proc, computation, 10 if len(sys.argv) < 2 else int(sys.argv[1])).value()
