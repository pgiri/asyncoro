# This example uses status messages and message passing to run 'setup'
# coroutine at remote process to prepare it for processing jobs.

# This example is a variation of discomp4.py; it uses message passing to get
# stream of requests from client to be processed in a thread (not a coroutine,
# but regular Python function) and send results back to client.

# DiscoroStatus must be imported in global scope as below; otherwise,
# unserializing status messages fails (if external scheduler is used)
from asyncoro.discoro import DiscoroStatus
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro_schedulers import RemoteCoroScheduler


# This coroutine is executed to initialize remote server so it can process jobs.
def proc_setup(coro=None):
    global time, queue, math, thread_pool
    import time, math, sys
    if sys.version_info.major >= 3:
        import queue
    else:
        import Queue as queue
    # in this case at most one computation (compute_proc) runs, so pool
    # is created with 1 thread
    thread_pool = asyncoro.AsyncThreadPool(1)
    yield 0


# This coroutine is executed to cleanup a process when all jobs are done
def proc_cleanup(coro=None):
    global time, thread_pool, queue
    thread_pool.terminate()
    del time, queue, thread_pool
    yield 0

# discoronode expects user computations to be generator functions (to
# create coroutines) that will 'yield' within pulse_interval (default
# 10 seconds). Otherwise, the daemon coroutine that sends pulse
# messages to scheduler will not get a chance to do so, causing
# scheduler to assume the node may be unreachable and abandon the
# computation. In this example, user computations are executed in
# threads, using AsyncThreadPool. The computation is simulated with
# 'time.sleep'. (Note that 'time.sleep' shouldn't be used in
# coroutines, as this will block entire asyncoro framework.)

# This generator function is sent to remote discoro process to run coroutines
# there. Note that compute_proc and proc_setup run in the same process (and
# share same address space), so global variables initialized in proc_setup are
# shared in compute_proc (and any other coroutines running on same process) are
# shared (updates in one coroutine are visible in other coroutnies in the same
# process).
def compute_proc(client, coro=None):
    # requests from client are received by the coroutine and sent to thread with
    # Queue. Thread can't receive from client, as message passing is not
    # available in threads.
    thread_queue = queue.Queue()

    def thread_proc(): # executed in a thread
        n = 0
        while True:
            req = thread_queue.get()
            if req is None:  # end of requests
                client.send(None)
                break
            time.sleep(req)
            client.send(math.sqrt(req))  # send sqrt of input as result
            n += 1
        return n

    # async_task blocks until thread function is finished, so execute it in
    # another coroutine
    def async_task(coro=None):
        yield thread_pool.async_task(thread_proc)
    thread_coro = asyncoro.Coro(async_task)

    # receive requests from client and put them in thread_queue so thread can
    # process them
    while True:
        req = yield coro.receive()
        thread_queue.put(req)
        if req is None:
            break

    n = yield thread_coro.finish()
    # to illustrate async_task result is what thread_proc returned, number of
    # inputs processed is shown; this message appears on server process
    print('%s processed %s requests' % (coro.location, n))


def client_proc(computation, njobs, coro=None):
    used_servers = {} # keep track of which servers to cleanup

    # use status coroutine to initialize remote server processes
    def status_proc(status, info, coro=None):
        # this coroutine is executed with status (either
        # ServerInitialized or ServerClosed) and location of server
        # print('status: %s / %s' % (status, str(info)))
        if status != discoro.Scheduler.ServerInitialized:
            raise StopIteration(0)
        # print('server %s is available' % info)
        # run 'proc_setup' to read file in to memory
        if (yield job_scheduler.execute_at(info, proc_setup)) != 0:
            print('Could not setup %s' % (info))
            raise StopIteration(-1)
        # print('server %s is ready' % info)
        used_servers[info] = info
        raise StopIteration(0) # indicate server initialized with exit value 0

    # RemoteCoroScheduler is used to run at most one coroutine at a server
    # process This should be created before scheduling computation
    job_scheduler = RemoteCoroScheduler(computation, status_proc)

    if (yield computation.schedule()):
        raise Exception('schedule failed')

    # send 5 requests to remote process (compute_proc)
    def send_requests(rcoro, coro=None):
        for i in range(5):
            rcoro.send(random.uniform(2, 10))
            # assume delay in input availability
            yield coro.sleep(random.uniform(0, 2))
        # end of input is indicated with None
        rcoro.send(None)

    # get results (messages sent by 'thread_proc' on remote process)
    def get_results(i, coro=None):
        while True:
            result = yield coro.receive()
            if result is None: # end of output
                break
            print('job %s result: %s' % (i, result))

    # create remote jobs
    def create_job(i, coro=None):
        # first create reader to get results
        results_coro = asyncoro.Coro(get_results, i)
        # create remote coroutine
        rcoro = yield job_scheduler.schedule(compute_proc, results_coro)
        if isinstance(rcoro, asyncoro.Coro):
            print('  job %s processed by %s' % (i, rcoro))
            # create coroutine to send requests
            asyncoro.Coro(send_requests, rcoro)
            # wait for all results to be received
            yield results_coro.finish()
            print('  job %s done' % i)
        else:
            print('rcoro %s failed: %s' % (i, rcoro))
            results_coro.terminate()

    # create njobs jobs
    for job in [asyncoro.Coro(create_job, i) for i in range(1, njobs+1)]:
        yield job.finish()

    # cleanup servers
    for job in [asyncoro.Coro(job_scheduler.execute_at, loc, proc_cleanup)
                for loc in used_servers.values()]:
        yield job.finish()

    yield job_scheduler.finish(close=True)

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
    # run 10 (or given number of) jobs
    asyncoro.Coro(client_proc, computation, 10 if len(sys.argv) < 2 else int(sys.argv[1])).value()
