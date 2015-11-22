# This example uses status messages and message passing to run 'setup'
# coroutine at remote process to prepare it for processing jobs (in
# this case, to read data in to memory so it can be processed
# efficiently).

# DiscoroStatus must be imported in global scope as below; otherwise,
# unserializing status messages fails (if external scheduler is used)
from asyncoro.discoro import DiscoroStatus
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro


# This generator function is sent to remote discoro process to
# initialize it. The function reads data from given file (which
# would've been sent by client). Instead of writing another function
# to cleanup (and schedule it after all jobs are done), this function
# sends message client when the setup is ready and then waits for
# (client's) message to cleanup. Note that in this case same file is
# read in to memory by all processes on a node, so this approach is
# not suitable if the file is very large; e.g., the file may be split
# and each piece may be processed by a process exclusively.
def proc_setup(data_file, client, coro=None):
    global hashlib, data # 'hashlib' and 'data' are used in rcoro_proc
    import os, hashlib
    fd = open(data_file, 'rb')
    data = fd.read()
    fd.close()
    os.remove(data_file)
    if (yield client.deliver('ready', timeout=10)) == 1:
        # data will be kept in memory until 'cleanup' is received
        msg = yield coro.receive()
        # assert msg == 'cleanup'
    del data, os, hashlib


# This generator function is sent to remote discoro process to run
# coroutines there. Note that rcoro_proc and proc_setup run in the same
# process (and share same address space), so global variables are
# shared (updates in one coroutine are visible in other coroutnies in
# the same process). Note that there is no need for locking when
# updating shared (global) variables with asyncoro.
def rcoro_proc(alg, n, coro=None):
    yield coro.sleep(n)
    checksum = getattr(hashlib, alg)()
    checksum.update(data) # file contents available in 'data' global variable
    yield (alg, checksum.hexdigest())


def client_proc(computation, data_file, njobs, coro=None):
    cleanup_rcoros = set() # processes used are kept track to cleanup when done
    algs = ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512']
    status = {'submitted': 0, 'done': 0}

    # submit job at given location
    def submit_job(where, coro=None):
        if status['submitted'] < njobs:
            alg = algs[status['submitted'] % len(algs)]
            rcoro = yield computation.run_at(where, rcoro_proc, alg, random.uniform(1, 5))
            if isinstance(rcoro, asyncoro.Coro):
                status['submitted'] += 1

    # client coroutine to setup a remote process; it first sends data
    # file to the process, then starts 'proc_setup' coroutine there,
    # then submits a job
    def init_proc(where, coro=None):
        if (yield asyncoro.AsynCoro().send_file(where, data_file, timeout=10)) < 0:
            print('Could not send data file "%s" to %s' % (data_file, where))
            raise StopIteration
        rcoro = yield computation.run_at(where, proc_setup, data_file, coro)
        if isinstance(rcoro, asyncoro.Coro):
            # wait till coroutine has read data in to memory
            msg = yield coro.receive()
            assert msg == 'ready'
            cleanup_rcoros.add(rcoro) # this will be cleaned up at the end
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
            if msg.args[1][0] == StopIteration:
                result = msg.args[1][1]
                if isinstance(result, tuple) and len(result) == 2:
                    print('%s: %s computed %s: %s' % (status['done'], rcoro.location,
                                                      result[0], result[1]))
            else:
                asyncoro.logger.warning('%s terminated with "%s"' %
                                        (rcoro.location, str(msg.args[1])))
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
    for rcoro in cleanup_rcoros:
        if (yield rcoro.deliver('cleanup', timeout=5)) != 1:
            print('cleanup failed for %s' % rcoro)
    yield computation.close()


if __name__ == '__main__':
    import logging, random, sys
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    discoro.Scheduler()
    computation = discoro.Computation([rcoro_proc])
    inp_file = sys.argv[0] if len(sys.argv) == 1 else sys.argv[1]
    # call '.value()' of coroutine created here, otherwise main thread
    # may finish (causing interpreter to start cleanup) before asyncoro
    # scheduler gets a chance to start
    # run 10 jobs
    asyncoro.Coro(client_proc, computation, inp_file, 10).value()
