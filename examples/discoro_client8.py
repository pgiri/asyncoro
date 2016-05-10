# This example uses status messages and message passing to run 'setup' coroutine
# at remote process to prepare it for processing jobs.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *
from asyncoro.discoro_schedulers import RemoteCoroScheduler

# The computation is simulated with 'time.sleep', during which entire asyncoro
# framework is suspended as well (as asyncoro doesn't preempt running task). To
# avoid blocking the framework (which would prevent it from accepting messages
# from client), the computation is executed in a thread. Coroutine sends
# messages to thread with Queue (thread function can't receive messages from
# client directly).

def compute_coro(coro=None):
    import threading, time
    if sys.version_info.major >= 3:
        import queue
    else:
        import Queue as queue

    client = yield coro.receive() # first message is client coroutine

    # requests from client are received by this coroutine and sent to thread
    # function with Queue. Thread function can't receive from client, as message
    # passing is not available in threads.
    thread_queue = queue.Queue()
    # thread process sets thread_done asyncoro event that the compute_coro waits
    # for (setting the asynchronous event and sending messages are regular
    # functions, so they can be used in thread)
    thread_done = asyncoro.Event()

    def compute_func(): # executed in a thread
        result = 0
        while True:
            n = thread_queue.get()
            if n is None:  # end of requests
                client.send(result)
                break
            time.sleep(n)
            result += n
        thread_done.set()

    thread = threading.Thread(target=compute_func)
    thread.start()

    # receive requests from client and put them in thread_queue so thread can
    # process them
    while True:
        req = yield coro.receive()
        thread_queue.put(req)
        if req is None:
            break

    # wait for thread to finish
    yield thread_done.wait()

# client (local) coroutine submits computations
def client_proc(computation, njobs, coro=None):

    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')

    # send 5 requests to remote process (compute_coro)
    def send_requests(rcoro, coro=None):
        # first send this local coroutine (to whom rcoro sends result)
        rcoro.send(coro)
        for i in range(5):
            rcoro.send(random.uniform(2, 10))
            # assume delay in input availability
            yield coro.sleep(random.uniform(0, 2))
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
    import logging, random, sys
    asyncoro.logger.setLevel(logging.DEBUG)
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
