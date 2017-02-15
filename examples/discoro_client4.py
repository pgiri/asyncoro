# Run 'discoronode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This example shows how to use message passing, transferring files.
# The input files are assumed to be in current working directory as
# 'input0.dat', 'input1.dat', ..., 'input9.dat' (10 input files).
# 'run_jobs_proc' creates jobs as and when a discoro process is found,
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
    def __init__(self, i, data_file, n, client):
        self.i = i
        self.data_file = data_file
        self.result_file = None
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
    cobj.result_file = 'result-%s' % cobj.data_file
    with open(cobj.data_file, 'r') as data_fd:
        with open(cobj.result_file, 'w') as result_fd:
            for lineno, line in enumerate(data_fd, start=1):
                result_fd.write('%d: %d\n' % (lineno, len(line)-1))
    # 'sleep' to simulate computing
    yield coro.sleep(cobj.n)
    # transfer the result file to client
    status = yield asyncoro.AsynCoro().send_file(cobj.client.location, cobj.result_file,
                                                 overwrite=True, timeout=30)
    if status:
        print('Could not send %s to %s' % (cobj.result_file, cobj.client.location))
        cobj.result_file = None
    cobj.client.send(cobj)
    os.remove(cobj.data_file)
    os.remove(cobj.result_file)


# this generator function is used to create local coroutine (at the
# client) to communicate with a remote coroutine
def client_proc(job_id, data_file, rcoro, coro=None):
    # send input file to rcoro.location; this will be saved to discoro process's
    # working directory
    if (yield asyncoro.AsynCoro().send_file(rcoro.location, data_file, timeout=10)) < 0:
        print('Could not send input data to %s' % rcoro.location)
        # terminate remote coro
        rcoro.send(None)
        raise StopIteration(-1)
    # send info about input
    obj = C(job_id, data_file, random.uniform(5, 8), coro)
    if (yield rcoro.deliver(obj)) != 1:
        print('Could not send input to %s' % rcoro.location)
        raise StopIteration(-1)
    # rcoro sends result to this coroutine as message
    result = yield coro.receive()
    if not result.result_file:
        print('Processing %s failed' % obj.i)
        raise StopIteration(-1)
    # rcoro saves results file at this client, which is saved in asyncoro's
    # dest_path, not current working directory!
    result_file = os.path.join(asyncoro.AsynCoro().dest_path, result.result_file)
    # move file to cwd
    target = os.path.join(os.getcwd(), os.path.basename(result_file))
    os.rename(result_file, target)
    print('    job %s output is in %s' % (obj.i, target))


def run_jobs_proc(computation, data_files, coro=None):
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

    for i in range(len(data_files)):
        data_file = data_files[i]
        # create remote coroutine
        rcoro = yield computation.run(rcoro_proc)
        if isinstance(rcoro, asyncoro.Coro):
            # create local coroutine to send input file and data to rcoro
            asyncoro.Coro(client_proc, i, data_file, rcoro)
        else:
            print('  job %s failed: %s' % (i, rcoro))

    yield computation.close()


if __name__ == '__main__':
    import random, os, sys, glob
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    if os.path.dirname(sys.argv[0]):
        os.chdir(os.path.dirname(sys.argv[0]))
    data_files = glob.glob('discoro_client*.py')
    if not data_files:
        raise Exception('No data files to process')
    if len(sys.argv) > 1:
        data_files = data_files[:int(sys.argv[1])]

    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    Scheduler()
    # unlike in earlier examples, rcoro_proc is not sent with
    # computation (as it is not included in 'components';
    # instead, it is sent each time a job is submitted,
    # which is a bit inefficient
    computation = Computation([C])

    asyncoro.Coro(run_jobs_proc, computation, data_files)
