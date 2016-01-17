# This example uses status messages and message passing to run 'setup' coroutine
# at remote process to prepare it for processing jobs.

# DiscoroStatus must be imported in global scope as below; otherwise,
# unserializing status messages fails (if external scheduler is used)
from asyncoro.discoro import DiscoroStatus
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro_schedulers import RemoteCoroScheduler


# This generator function is sent to remote discoro process to initialize
# it. The function reads data from given file (which would've been sent by
# client). Instead of writing another function to cleanup (and schedule it after
# all jobs are done), this function sends message to client when the setup is
# ready and then waits for (client's) message to cleanup. Note that in this case
# same file is read in to memory by all processes on a node, so this approach is
# not suitable if the file is very large; e.g., the file may be split and each
# piece may be processed by a process exclusively.
def proc_setup(data_file, client, coro=None):
    global os, hashlib, data # 'hashlib' and 'data' are used in rcoro_proc
    import os, hashlib
    fd = open(data_file, 'rb')
    data = fd.read()
    fd.close()
    os.remove(data_file)
    if (yield client.deliver('ready', timeout=10)) == 1:
        msg = yield coro.receive()
        # assert msg == 'cleanup'
    del data, os, hashlib


# This generator function is sent to remote discoro process to run coroutines
# there. Note that rcoro_proc and proc_setup run in the same process (and share
# same address space), so global variables are shared (updates in one coroutine
# are visible in other coroutnies in the same process). There is no need for
# locking when updating shared (global) variables, as there is no forced
# preemption with asyncoro.
def rcoro_proc(alg, n, results_coro, coro=None):
    global data, hashlib
    yield coro.sleep(n)
    checksum = getattr(hashlib, alg)()
    checksum.update(data)
    results_coro.send((alg, checksum.hexdigest()))


def client_proc(computation, data_file, njobs, coro=None):
    proc_setup_coros = set() # processes used are kept track to cleanup when done

    # coroutine receives status messages from rcoro_scheduler. If the status is
    # ServerInitialized, send the file to server and wait for initialization
    def status_proc(status, info, coro=None):
        if status != discoro.Scheduler.ServerInitialized:
            raise StopIteration(0)
        if (yield asyncoro.AsynCoro().send_file(info, data_file, timeout=10)) < 0:
            print('Could not send data file "%s" to %s' % (data_file, info))
            raise StopIteration
        rcoro = yield rcoro_scheduler.submit_at(info, proc_setup, data_file, coro)
        if isinstance(rcoro, asyncoro.Coro):
            msg = yield coro.receive()
            if msg == 'ready':
                proc_setup_coros.add(rcoro)
                raise StopIteration(0) # success indicated with 0
            else:
                raise StopIteration(-1) # scheduler won't use this server
        else:
            print('Setup of %s failed' % where)
            raise StopIteration(-1) # scheduler won't use this server

    rcoro_scheduler = RemoteCoroScheduler(computation, status_proc)

    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')

    # remote coroutines send results to this coroutine
    def results_proc(coro=None):
        done = 0
        while done < njobs:
            msg = yield coro.receive()
            if isinstance(msg, tuple) and len(msg) == 2:
                print('%s checksum: %s' % (msg[0], msg[1]))
                done += 1
    results_coro = asyncoro.Coro(results_proc)

    algs = ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512']
    submitted = 0
    while submitted < njobs:
        alg = algs[submitted % len(algs)]
        rcoro = yield rcoro_scheduler.schedule(rcoro_proc, alg, random.uniform(1, 5),
                                               results_coro)
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
    # if scheduler is not already running (on a node as a program), start it
    # (private scheduler):
    discoro.Scheduler()
    computation = discoro.Computation([rcoro_proc])
    inp_file = sys.argv[0] if len(sys.argv) == 1 else sys.argv[1]
    # run 10 jobs
    asyncoro.Coro(client_proc, computation, inp_file, 10).value()
