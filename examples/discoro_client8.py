# This example uses status messages and message passing to run 'setup' coroutine
# at remote process to prepare it for processing jobs.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *
from asyncoro.discoro_schedulers import RemoteCoroScheduler

# Unlike in earlier versions of asyncoro, computations can now take time - even
# if computations don't "yield" to scheduler, asyncoro can still send/receive
# messages, respond to timer events in scheduler etc. In this case, computation
# is simulated with 'time.sleep' which blocks user asyncoro thread, but another
# (reactive) asycoro thread processes network traffic, run scheduler coroutines.

def compute_coro(coro=None):
    import time

    client = yield coro.receive() # first message is client coroutine

    result = 0
    while True:
        n = yield coro.receive()
        if n is None:  # end of requests
            client.send(result)
            break
        # long-running computation is simulated with 'time.sleep'; during this
        # time client may send messages to this coroutine, which will be
        # received and put in this coroutine's message queue (by _ReactAsynCoro_)
        time.sleep(n)
        result += n

# client (local) coroutine submits computations
def client_proc(computation, njobs, coro=None):
    # send 5 requests to remote process (compute_coro)
    def send_requests(rcoro, coro=None):
        # first send this local coroutine (to whom rcoro sends result)
        rcoro.send(coro)
        for i in range(5):
            # even if recipient doesn't use "yield" (such as executing long-run
            # computation, or thread-blocking function such as 'time.sleep' as
            # in this case), the message is accepted by another scheduler
            # (_ReactAsynCoro_) at the receiver and put in recipient's message
            # queue
            rcoro.send(random.uniform(10, 20))
            # assume delay in input availability
            yield coro.sleep(random.uniform(2, 5))
        # end of input is indicated with None
        rcoro.send(None)
        result = yield coro.receive() # get result
        print('    %s computed result: %.4f' % (rcoro.location, result))

    for i in range(njobs):
        rcoro = yield rcoro_scheduler.schedule(compute_coro)
        if isinstance(rcoro, asyncoro.Coro):
            print('  job %d processed by %s' % (i, rcoro.location))
            asyncoro.Coro(send_requests, rcoro)

    yield rcoro_scheduler.finish(close=True)


if __name__ == '__main__':
    import random, sys
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program), start
    # private scheduler:
    Scheduler()
    # package computation fragments
    computation = Computation([compute_coro])
    # use RemoteCoroScheduler to start coroutines at servers (should be done
    # before scheduling computation)
    rcoro_scheduler = RemoteCoroScheduler(computation)
    # run n jobs
    asyncoro.Coro(client_proc, computation, 10 if len(sys.argv) < 2 else int(sys.argv[1]))
