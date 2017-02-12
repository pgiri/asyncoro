# This example shows how to use message passing, transferring files.
# The input files are assumed to be in current working directory as
# 'input0.dat', 'input1.dat', ..., 'input9.dat' (10 input files).
# 'submit_jobs_proc' creates jobs as and when a discoro process is found,
# or a process finishes a job. Each job is processed by a remote coroutine
# (rcoro) and a corresponding local coroutine (with 'client_proc')
# interacts with that rcoro to send the input data to where rcoro is,
# an instance of class C to give the information needed to process the
# file. Client then waits for rcoro to finish processing job, send the resulting
# output file in the form 'outputX.dat' and result (in this case, it is
# the return value of 'send_file').

# Note that the objective is to illustrate features, so implementation
# is not ideal. Error checking is skipped at a few places for brevity.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *

# objects of C are sent by a client to remote coroutine
class C(object):
    def __init__(self, i, n, client):
        self.i = i
        self.n = n
        self.client = client


# this generator function is sent to remote server to run
# coroutines there
def rcoro_proc(coro=None):
    import os
    # receive object from client_proc coroutine
    cobj = yield coro.receive()
    if not cobj:
        raise StopIteration
    # Input file is already copied at where this rcoro is running (by client).
    # For given input file, create an output file with each line in the output
    # file computed as length of corresponding line in input file
    inp_file = 'input%s.dat' % cobj.i
    out_file = 'output%s.dat' % cobj.i
    with open(inp_file, 'r') as inp_fd:
        with open(out_file, 'w') as out_fd:
            for lineno, line in enumerate(inp_fd, start=1):
                out_fd.write('%d: %d\n' % (lineno, len(line)-1))
    # 'sleep' to simulate computing
    yield coro.sleep(cobj.n)
    # transfer the output file to client's asyncoro
    result = yield asyncoro.AsynCoro().send_file(cobj.client.location, out_file, overwrite=True,
                                                 timeout=30)
    cobj.client.send(result)
    os.remove(inp_file)
    os.remove(out_file)


# this generator function is used to create local coroutine (at the
# client) to communicate with a remote coroutine
def client_proc(job_id, rcoro, coro=None):
    # send input file to rcoro.location; this will be saved to discoro process's
    # working directory
    if (yield asyncoro.AsynCoro().send_file(rcoro.location, 'input%s.dat' % job_id,
                                            timeout=30)) < 0:
        print('Could not send input data to %s' % rcoro.location)
        # terminate remote coro
        rcoro.send(None)
        raise StopIteration(-1)
    # send info about input
    obj = C(job_id, random.uniform(5, 8), coro)
    if (yield rcoro.deliver(obj)) != 1:
        print('Could not send input to %s' % rcoro.location)
        raise StopIteration(-1)
    # rcoro sends result to this coroutine as message
    result = yield coro.receive()
    if result < 0:
        print('Processing %s failed' % obj.i)
        raise StopIteration(-1)
    # rcoro saves results file at this client, which is saved in asyncoro's
    # dest_path, not current working directory!
    output = os.path.join(asyncoro.AsynCoro().dest_path, 'output%s.dat' % obj.i)
    # move file to cwd
    target = os.path.join(os.getcwd(), os.path.basename(output))
    os.rename(output, target)
    print('    job %s output is in %s' % (obj.i, target))


def submit_jobs_proc(computation, njobs, coro=None):
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

    for i in range(njobs):
        # create remote coroutine
        rcoro = yield computation.submit(rcoro_proc)
        if isinstance(rcoro, asyncoro.Coro):
            # create local coroutine to send input file and data to rcoro
            asyncoro.Coro(client_proc, i, rcoro)
        else:
            print('  job %s failed: %s' % (i, rcoro))

    yield computation.close()


if __name__ == '__main__':
    import random, os, sys
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    if os.path.dirname(sys.argv[0]):
        os.chdir(os.path.dirname(sys.argv[0]))
    # run 10 (or given number of) jobs
    njobs = 0
    for i in range(10 if len(sys.argv) < 2 else int(sys.argv[1])):
        if os.path.isfile('input%s.dat' % i):
            njobs += 1
        else:
            break
    if not njobs:
        raise Exception('No input data files to process')

    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    Scheduler()
    # unlike in earlier examples, rcoro_proc is not sent with
    # computation (as it is not included in 'components';
    # instead, it is sent each time a job is submitted,
    # which is a bit inefficient
    computation = Computation([C])

    asyncoro.Coro(submit_jobs_proc, computation, njobs)
