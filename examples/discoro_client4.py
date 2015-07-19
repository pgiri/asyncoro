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

# StatusMessage must be imported in global scope as below; otherwise,
# unserializing status messages fails (if external scheduler is used)
from asyncoro.discoro import StatusMessage
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro

# objects of C are sent by a client to remote coroutine
class C(object):
    def __init__(self, i, n, client):
        self.i = i
        self.n = n
        self.client = client

# this generator function is used to create local coroutine (at the
# client) to communicate with a remote coroutine
def client_proc(job_id, rcoro, coro=None):
    # send input file; this will be saved to where discoro process is
    # (i.e., rcoro.location)
    if (yield asyncoro.AsynCoro().send_file(rcoro.location, 'input%s.dat' % job_id,
                                            timeout=30)) < 0:
        print('Could not send input data to %s' % rcoro.location)
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
    # rcoro uses 'send_file'; this saves the file in asyncoro's
    # dest_path, not current working directory!
    output = os.path.join(asyncoro.AsynCoro().dest_path, 'output%s.dat' % obj.i)
    # if necessary, move file to os.getcwd()
    # os.rename(output, os.getcwd())
    print('%s processed' % (obj.i))

# this generator function is sent to remote server to run
# coroutines there
def rcoro_proc(coro=None):
    import os
    # receive object from client_proc coroutine
    cobj = yield coro.receive()
    # for given input file, create an output file with each line in
    # the output file computed as length of corresponding line in input file
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

def submit_jobs_proc(computation, njobs, coro=None):
    computation.status_coro = coro
    # if scheduler is shared (i.e., running as program), nothing needs
    # to be done (its location can optionally be given to 'schedule');
    # othrwise, start scheduler: discoro.Scheduler()
    # discoro.Scheduler()
    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')
    submitted = 0
    done = 0
    while True:
        msg = yield coro.receive()
        if isinstance(msg, asyncoro.MonitorException):
            # a job is done
            rcoro = msg.args[0]
            done += 1
            if done >= njobs and submitted == done:
                break
            if submitted < njobs:
                # submit another job
                rcoro = yield computation.run_at(rcoro.location, rcoro_proc)
                if isinstance(rcoro, asyncoro.Coro):
                    asyncoro.Coro(client_proc, submitted, rcoro)
                submitted += 1
        elif isinstance(msg, StatusMessage):
            # asyncoro.logger.debug('Node/Process status: %s, %s' % (msg.status, msg.location))
            if msg.status == discoro.Scheduler.ProcInitialized and submitted < njobs:
                # a new process found; submit a job
                rcoro = yield computation.run_at(msg.location, rcoro_proc)
                if isinstance(rcoro, asyncoro.Coro):
                    asyncoro.Coro(client_proc, submitted, rcoro)
                submitted += 1
        else:
            asyncoro.logger.debug('Ignoring status message %s' % str(msg))
    yield computation.close()

if __name__ == '__main__':
    import logging, random, os, threading
    asyncoro.logger.setLevel(logging.DEBUG)
    # unlike in earlier examples, rcoro_proc is not sent with
    # computation; instead, it is sent each time a job is submitted,
    # which is a bit inefficient
    computation = discoro.Computation([C])
    # run 10 jobs
    asyncoro.Coro(submit_jobs_proc, computation, 10).value()
