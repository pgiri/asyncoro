# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# The remote servers are initialized (to create AsyncThreadPool) with a setup
# coroutine and that pool is later used to execute computations.

# DiscoroStatus must be imported in global scope as below; otherwise,
# unserializing status messages fails (if external scheduler is used)
from asyncoro.discoro import DiscoroStatus
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro

# This implementation uses various features for illustrative purposes: In other
# examples RemoteCoroScheduler is used to easily create remote coroutines,
# whereas in this example, status messages from discoro scheduler are used to
# create coroutines, as well as get their exit status, asynchronous thread pools
# are used to create threads so coroutine can wait for tasks to finish, etc. See
# 'discomp3.py', 'discomp8.py' for simpler ways.

# This function sends message to client when the setup is done and
# then waits for (client's) message to cleanup.
def proc_setup(client, coro=None):
    global time, thread_pool # 'time' and 'thread_pool' used in compute_proc
    import time
    # in this case at most one computation (compute_proc) runs, so pool
    # is created with 1 thread
    thread_pool = asyncoro.AsyncThreadPool(1)
    if (yield client.deliver('ready', timeout=10)) == 1:
        # data will be kept in memory until 'cleanup' is received
        msg = yield coro.receive()
        # assert msg == 'cleanup'
    thread_pool.terminate()
    del time, thread_pool


# discoronode expects user computations to be generator functions (to
# create coroutines) that will 'yield' within pulse_interval (default
# 10 seconds). Otherwise, the daemon coroutine that sends pulse
# messages to scheduler will not get a chance to do so, causing
# scheduler to assume the node may be unreachable and abandon the
# computation. In this example, user computations are executed in
# threads, using AsyncThreadPool. The computation is simulated with
# 'time.sleep'. (Note that 'time.sleep' shouldn't be used in
# coroutines, as this will block entire asyncoro framework.)

# This generator function is sent to remote discoro process to run
# coroutines there. Note that compute_proc and proc_setup run in the
# same process (and share same address space), so global variables are
# shared (updates in one coroutine are visible in other coroutnies in
# the same process).

# 'compute_func' is regular Python function (not coroutine generator method)
def compute_func(n, coro): # executed in a thread
    time.sleep(n)
    return (coro.location, n)

# 'compute_coro' is coroutine that executes 'compute_func' in a thread from pool
# created in 'proc_setup'
def compute_coro(n, coro=None):
    yield thread_pool.async_task(compute_func, n, coro)
    # return value from thread_proc is sent as result with MonitorException

# client (local) coroutine submits computations
def client_proc(computation, njobs, coro=None):
    cleanup_coros = set() # processes used are kept track to cleanup when done
    status = {'submitted': 0, 'done': 0}

    # submit job at given location
    def submit_job(where, coro=None):
        if status['submitted'] < njobs:
            rcoro = yield computation.run_at(where, compute_coro, random.uniform(5, 10))
            if isinstance(rcoro, asyncoro.Coro):
                status['submitted'] += 1

    # client coroutine to setup a remote process (with proc_setup coroutine)
    def init_proc(where, coro=None):
        rcoro = yield computation.run_at(where, proc_setup, coro)
        if isinstance(rcoro, asyncoro.Coro):
            # wait till coroutine has read data in to memory
            msg = yield coro.receive()
            if msg == 'ready':
                cleanup_coros.add(rcoro) # this will be cleaned up at the end
                asyncoro.Coro(submit_job, rcoro.location)
        else:
            print('Setup of %s failed' % where)

    computation.status_coro = coro
    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')

    while True:
        msg = yield coro.receive()
        if isinstance(msg, asyncoro.MonitorException):
            # a process finished job
            rcoro = msg.args[0]
            if msg.args[1][0] == StopIteration and len(msg.args[1][1]) == 2:
                print('result from %s: %s' % (msg.args[1][1][0], msg.args[1][1][1]))
            else:
                print('%s failed: %s' % (rcoro.location, str(msg.args[1])))
            status['done'] += 1
            if status['done'] == njobs:
                break
            if status['submitted'] < njobs:
                # schedule another job at this process
                asyncoro.Coro(submit_job, rcoro.location)
        elif isinstance(msg, DiscoroStatus):
            # asyncoro.logger.debug('Node/Server status: %s, %s' % (msg.status, msg.info))
            if msg.status == discoro.Scheduler.ServerInitialized and status['submitted'] < njobs:
                # a new process is available; initialize it
                asyncoro.Coro(init_proc, msg.info)
        else:
            asyncoro.logger.debug('Ignoring status message %s' % str(msg))

    # cleanup processes
    for rcoro in cleanup_coros:
        if (yield rcoro.deliver('cleanup', timeout=5)) != 1:
            print('cleanup failed for %s' % rcoro)
    yield computation.close()


if __name__ == '__main__':
    import logging, random, sys
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    discoro.Scheduler()
    computation = discoro.Computation([compute_coro, compute_func])
    # call '.value()' of coroutine created here, otherwise main thread
    # may finish (causing interpreter to start cleanup) before asyncoro
    # scheduler gets a chance to start
    # run 10 jobs
    asyncoro.Coro(client_proc, computation, 10).value()
