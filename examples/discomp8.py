# This example uses status messages and message passing to run 'setup'
# coroutine at remote process to prepare it for processing jobs.

# This example uses message passing to get stream of requests from client to be
# processed in a thread (not a coroutine, but regular Python function) and send
# results back to client.

# DiscoroStatus must be imported in global scope as below; otherwise,
# unserializing status messages fails (if external scheduler is used)
from asyncoro.discoro import DiscoroStatus
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro_schedulers import RemoteCoroScheduler

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
    import threading, math, time, sys
    if sys.version_info.major >= 3:
        import queue
    else:
        import Queue as queue

    # requests from client are received by the coroutine and sent to thread with
    # Queue. Thread can't receive from client, as message passing is not
    # available in threads.
    thread_queue = queue.Queue()
    # thread process sets thread_done asyncoro event that the compute_proc waits
    # for (setting the event and sending messages are regular functions, so they
    # can be used in thread)
    thread_done = asyncoro.Event()

    def thread_proc(): # executed in a thread
        while True:
            req = thread_queue.get()
            if req is None:  # end of requests
                client.send(None)
                break
            time.sleep(req)
            client.send(math.sqrt(req))  # send sqrt of input as result
        thread_done.set()

    thread = threading.Thread(target=thread_proc)
    thread.start()

    # receive requests from client and put them in thread_queue so thread can
    # process them
    while True:
        req = yield coro.receive()
        thread_queue.put(req)
        if req is None:
            break

    yield thread_done.wait()


def client_proc(computation, njobs, coro=None):
    # RemoteCoroScheduler is used to run at most one coroutine at a server
    # process This should be created before scheduling computation
    job_scheduler = RemoteCoroScheduler(computation)

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

    # create coroutines to send requests, receive results and remote coroutine
    # to process requests
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
