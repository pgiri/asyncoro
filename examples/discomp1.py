# This example uses status messages and message passing to run 'setup' coroutine
# at remote process to prepare it for processing jobs.

# DiscoroStatus must be imported in global scope as below; otherwise,
# unserializing status messages fails (if external scheduler is used)
from asyncoro.discoro import DiscoroStatus
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro_schedulers import RemoteCoroScheduler

# discoronode expects user computations to be generator functions (to create
# coroutines) that will 'yield' within pulse_interval (default 10
# seconds). Otherwise, the daemon coroutine that sends pulse messages to
# scheduler will not get a chance to do so, causing scheduler to assume the node
# may be unreachable and abandon the computation. In this example, user
# computations are executed in threads. The computation is simulated with
# 'time.sleep'. (Note that 'time.sleep' shouldn't be used in coroutines, as this
# will block entire asyncoro framework.) The thread function can send messages,
# but can't receive messages.

# 'compute_func' is executed with thread, so it is a regular Python function,
# where 'time.sleep' is used to simulate computation; note that asynchronous
# methods in asyncoro can't be used in thread function.
def compute_func(n, client, thread_done):
    time.sleep(n)
    client.send(time.asctime())
    thread_done.set()
    return

# 'compute_coro' coroutine executes 'compute_func'
def compute_coro(n, client, coro=None):
    import threading
    # thread process sets thread_done asyncoro event that the compute_proc waits
    # for (setting the event and sending messages are regular functions, so they
    # can be used in thread)
    thread_done = asyncoro.Event()
    thread = threading.Thread(target=compute_func, args=(n, client, thread_done))
    thread.start()
    yield thread_done.wait()

# client (local) coroutine submits computations
def client_proc(computation, njobs, coro=None):
    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')

    # remote coroutines send results to this coroutine
    def results_proc(coro=None):
        done = 0
        while done < njobs:
            msg = yield coro.receive()
            print('result: %s' % msg)
            done += 1
    results_coro = asyncoro.Coro(results_proc)

    submitted = 0
    while submitted < njobs:
        rcoro = yield rcoro_scheduler.schedule(compute_coro, random.uniform(5, 10), results_coro)
        if isinstance(rcoro, asyncoro.Coro):
            submitted += 1

    # wait for results to be received
    yield results_coro.finish()
    yield rcoro_scheduler.finish(close=True)


if __name__ == '__main__':
    import logging, random, sys
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    discoro.Scheduler()
    computation = discoro.Computation([compute_coro, compute_func])
    # run 10 jobs
    client = asyncoro.Coro(client_proc, computation, 10)
    # use RemoteCoroScheduler to start coroutines at servers
    rcoro_scheduler = RemoteCoroScheduler(computation)
    # call '.value()' of coroutine created here, otherwise main thread
    # may finish (causing interpreter to start cleanup) before asyncoro
    # scheduler gets a chance to start
    client.value()
