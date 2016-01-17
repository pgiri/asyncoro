# This example uses status messages and message passing to run 'setup' coroutine
# at remote process to prepare it for processing jobs.

# DiscoroStatus must be imported in global scope as below; otherwise,
# unserializing status messages fails (if external scheduler is used)
from asyncoro.discoro import DiscoroStatus
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro_schedulers import RemoteCoroScheduler


# This function sends message to client when the setup is done and then waits
# for (client's) message to cleanup.
def proc_setup(client, coro=None):
    global time, thread_pool # 'thread_pool' used in rcoro_proc
    import time
    # in this case at most one computation (rcoro_proc) runs, so pool
    # is created with 1 thread
    thread_pool = asyncoro.AsyncThreadPool(1)
    if (yield client.deliver('ready', timeout=10)) == 1:
        # data will be kept in memory until 'cleanup' is received
        msg = yield coro.receive()
        # assert msg == 'cleanup'
    thread_pool.terminate()
    del time, thread_pool


# discoronode expects user computations to be generator functions (to create
# coroutines) that will 'yield' within pulse_interval (default 10
# seconds). Otherwise, the daemon coroutine that sends pulse messages to
# scheduler will not get a chance to do so, causing scheduler to assume the node
# may be unreachable and abandon the computation. In this example, user
# computations are executed in threads, using AsyncThreadPool. The computation
# is simulated with 'time.sleep'. (Note that 'time.sleep' shouldn't be used in
# coroutines, as this will block entire asyncoro framework.) The thread function
# can send messages, but can't receive messages.

# This generator function is sent to remote discoro process to run coroutines
# there. Note that compute_proc and proc_setup run in the same process (and
# share same address space), so global variables are shared (updates in one
# coroutine are visible in other coroutnies in the same process).
def compute_proc(n, client, coro=None):
    # execute 'time.sleep' with thread_pool initialized in proc_setup
    def thread_proc():
        time.sleep(n)
        client.send((coro.location, time.asctime()))
        return

    yield thread_pool.async_task(thread_proc)

def client_proc(computation, njobs, coro=None):
    proc_setup_coros = set() # processes used are kept track to cleanup when done

    # coroutine receives status messages from rcoro_scheduler. If the status is
    # ServerInitialized, send the file to server and wait for initialization
    def status_proc(status, info, coro=None):
        if status != discoro.Scheduler.ServerInitialized:
            raise StopIteration(0)
        rcoro = yield rcoro_scheduler.submit_at(info, proc_setup, coro)
        if isinstance(rcoro, asyncoro.Coro):
            msg = yield coro.receive()
            if msg == 'ready':
                proc_setup_coros.add(rcoro)
                raise StopIteration(0) # success indicated with 0
            else:
                raise StopIteration(-1) # scheduler won't use this server
        else:
            print('Setup of %s failed' % info)
            raise StopIteration(-1) # scheduler won't use this server

    # use RemoteCoroScheduler to start coroutines at servers
    rcoro_scheduler = RemoteCoroScheduler(computation, status_proc)
    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')

    # remote coroutines send results to this coroutine
    def results_proc(coro=None):
        done = 0
        while done < njobs:
            msg = yield coro.receive()
            if isinstance(msg, tuple) and len(msg) == 2:
                print('result from %s: %s' % (msg[0], msg[1]))
                done += 1
    results_coro = asyncoro.Coro(results_proc)

    submitted = 0
    while submitted < njobs:
        rcoro = yield rcoro_scheduler.schedule(compute_proc, random.uniform(5, 10), results_coro)
        if isinstance(rcoro, asyncoro.Coro):
            submitted += 1

    # wait for results to be received
    yield results_coro.finish()
    # cleanup processes
    for proc_setup_coro in proc_setup_coros:
        yield proc_setup_coro.deliver('cleanup', timeout=5)
    yield rcoro_scheduler.finish(close=True)


if __name__ == '__main__':
    import logging, random, sys
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    discoro.Scheduler()
    computation = discoro.Computation([compute_proc])
    # call '.value()' of coroutine created here, otherwise main thread
    # may finish (causing interpreter to start cleanup) before asyncoro
    # scheduler gets a chance to start
    # run 10 jobs
    asyncoro.Coro(client_proc, computation, 10).value()
