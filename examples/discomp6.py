# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# Distributed computing example where this client sends computation to
# remote discoro process to run as remote coroutines. At any time at
# most one computation coroutine is scheduled at a process (due to
# ProcScheduler). This example shows how to use 'execute' method of
# ProcScheduler to submit comutations and get their results easily.

# This example can be combined with in-memory processing (see
# 'discoro_client5.py') and streaming (see 'discoro_client6.py') for
# efficient processing of data and communication.

import logging, random
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro_schedulers import ProcScheduler

# 'proc_setup' is executed at remote server process to read the data in
# given file (transferred by client, see below) in to memory (global
# variable). 'compute' then uses the data in memory instead of
# reading from file every time.
def proc_setup(data_file, coro=None):
    global os, hashlib, data
    import os, hashlib
    fd = open(data_file, 'rb')
    data = fd.read()
    fd.close()
    os.remove(data_file)
    # generator functions must have at least one 'yield'
    yield 0 # indicate successful initialization with exit value 0

# 'proc_cleanup' is executed at remote server process to cleanup
# (delete global variables initialized in 'proc_setup' in this case)
def proc_cleanup(coro=None):
    global hashlib, data
    del hashlib, data
    yield 0

# 'compute' is executed at remote server process repeatedly to compute
# checksum of data in memory, initialized by 'proc_setup'
def compute(alg, n, coro=None):
    global data, hashlib
    yield coro.sleep(n)
    checksum = getattr(hashlib, alg)()
    checksum.update(data)
    yield (alg, checksum.hexdigest())


def client_proc(computation, data_file, coro=None):
    used_servers = {} # keep track of which servers to cleanup

    # coroutine to (concurrently) execute computations
    def exec_proc(gen, *args, **kwargs):
        # execute computation; result of computation is result of
        # 'yield' which is also result of this coroutine (obtained
        # with 'finish' method below)
        yield job_scheduler.execute(gen, *args, **kwargs)
        # results can be processed here (as they become available), or
        # await in sequence as done below

    def status_proc(status, info, coro=None):
        # this coroutine is executed with status (either
        # ServerInitialized or ServerClosed) and location of server
        if status != discoro.Scheduler.ServerInitialized:
            raise StopIteration(0)
        # send data file to server
        if (yield asyncoro.AsynCoro().send_file(info, data_file, timeout=10)) < 0:
            print('Could not send data file "%s" to %s' % (data_file, info))
            raise StopIteration(-1)
        # run 'proc_setup' to read file in to memory
        if (yield job_scheduler.execute_at(info, proc_setup, data_file)) == 0:
            used_servers[info] = info
            raise StopIteration(0) # indicate server initialized with exit value 0
        raise StopIteration(-1)

    # Use ProcScheduler to run at most one coroutine at a server process
    # This should be created before scheduling computation
    job_scheduler = ProcScheduler(computation, status_proc)

    if (yield computation.schedule()):
        raise Exception('schedule failed')

    # execute 10 jobs (coroutines) and get their results. Note that
    # number of jobs created can be more than number of server
    # processes available; the scheduler will use as many processes as
    # necessary/available, running one job at a server process at a time
    algs = ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512']
    jobs = [asyncoro.Coro(exec_proc, compute, algs[i % len(algs)], random.uniform(1, 3))
            for i in range(10)]
    for job in jobs:
        result = yield job.finish()
        if isinstance(result, tuple) and len(result) == 2:
            print('%ssum: %s' % (result[0], result[1]))
        else:
            print('rcoro %s failed: %s' % (job, result))

    jobs = [asyncoro.Coro(job_scheduler.execute_at, info, proc_cleanup)
            for info in used_servers.values()]

    yield job_scheduler.finish(close=True)


if __name__ == '__main__':
    import sys
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    discoro.Scheduler()
    # send 'compute' generator function
    computation = discoro.Computation([compute])
    inp_file = sys.argv[0] if len(sys.argv) == 1 else sys.argv[1]
    asyncoro.Coro(client_proc, computation, inp_file).value()
