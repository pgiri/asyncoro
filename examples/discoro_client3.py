# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# Example where this client sends computation to remote discoro process to run
# as remote coroutines. Computations are scheduled with custom scheduler
# (without using RemoteCoroScheduler). Remote coroutines and client can use
# message passing to exchange data.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *
from asyncoro.discoro_schedulers import RemoteCoroScheduler


# objects of C are exchanged between client and servers
class C(object):
    def __init__(self, i):
        self.i = i
        self.n = None

    def __repr__(self):
        return '%d: %s' % (self.i, self.n)


# this generator function is sent to remote discoro servers to run
# coroutines there
def compute(obj, client, coro=None):
    # obj is an instance of C
    import math
    # this coroutine and client can use message passing; client sends
    # data to this coro as a message
    print('process at %s received: %s' % (coro.location, obj.n))
    yield coro.sleep(obj.n)
    obj.n = math.sqrt(obj.n)
    # send result back to client
    yield client.deliver(obj, timeout=5)


def client_proc(computation, njobs, coro=None):
    # create a separate coroutine to receive results, so they can be processed
    # as soon as received
    def recv_results(coro=None):
        for i in range(njobs):
            msg = yield coro.receive()
            print('    result for job %d: %s' % (i, msg))

    results_coro = asyncoro.Coro(recv_results)
    # remote coroutines send replies as messages to this coro
    for i in range(njobs):
        cobj = C(i)
        cobj.n = random.uniform(5, 10)
        # as noted in 'discoro_client1.py', 'schedule' method is used to run
        # jobs sequentially; use 'submit' to run multiple jobs on one server
        # concurrently
        print('  request %d: %s' % (i, cobj.n))
        rcoro = yield rcoro_scheduler.schedule(compute, cobj, results_coro)
        if not isinstance(rcoro, asyncoro.Coro):
            print('failed to create rcoro %s: %s' % (i, rcoro))

    # wait for all results and close computation
    yield rcoro_scheduler.finish(close=True)


if __name__ == '__main__':
    import random
    # asyncoro.logger.setLevel(ayncoro.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    Scheduler()
    # send generator function and class C (as the computation uses
    # objects of C)
    computation = Computation([compute, C])
    # use RemoteCoroScheduler to schedule/submit coroutines; scheduler must be
    # created before computation is scheduled (next step below)
    rcoro_scheduler = RemoteCoroScheduler(computation)
    # create 10 remote coroutines (jobs)
    asyncoro.Coro(client_proc, computation, 10)
