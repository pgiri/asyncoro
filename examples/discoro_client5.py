# Run 'discoronode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This example runs a program (discomp7_proc.py) on a remote server. The program
# reads from standard input and writes to standard output. Subprocess and
# asynchronous pipes are used to write / read data from the program, and message
# passing is used to get data from client (which is sent to the program) and to
# send output from the program back to the client.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *
import asyncoro.discoro_schedulers

# rcoro_proc is sent to remote server to execute discomp7_proc.py program. It
# uses message passing to get data from client that is written to pipe and read
# output from program that is sent back to client.
def rcoro_proc(client, program, coro=None):
    import sys
    import os
    import subprocess
    import asyncoro.asyncfile

    if program.endswith('.py'):
        # Computation dependencies are saved in parent directory
        program = [sys.executable, os.path.join('..', program)]
    # start program as a subprocess (to read from and write to pipe)
    if os.name == 'nt':  # create pipe with asyncfile under Windows
        pipe = asyncoro.asyncfile.Popen(program, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    else:
        pipe = subprocess.Popen(program, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    # convert to asynchronous pipe; see 'pipe_csum.py' and 'pipe_grep.py' for
    # chaining pipes
    pipe = asyncoro.asyncfile.AsyncPipe(pipe)

    # reader reads (output) from pipe and sends to client as messages
    def reader_proc(coro=None):
        while True:
            line = yield pipe.readline()
            if not line:
                break
            # send output to client
            client.send(line)
        pipe.stdout.close()
        if os.name == 'nt':
            pipe.close()
        client.send(None)

    reader = asyncoro.Coro(reader_proc)

    # writer gets messages from client and writes them (input) to pipe
    while True:
        data = yield coro.receive()
        if not data:
            break
        # write data as lines to program
        yield pipe.write(data + '\n'.encode(), full=True)
    pipe.stdin.close()
    # wait for all data to be read (subprocess to end)
    yield reader.finish()
    raise StopIteration(pipe.poll())

# client (local) coroutine submits computations
def client_proc(computation, program_path, n, coro=None):
    # send 10 random numbers to remote process (rcoro_proc)
    def send_input(rcoro, coro=None):
        for i in range(10):
            # encode strings so works with both Python 2.7 and 3
            rcoro.send(('%.2f' % random.uniform(0, 5)).encode())
            # assume delay in input availability
            yield coro.sleep(random.uniform(0, 2))
        # end of input is indicated with None
        rcoro.send(None)

    # read output (messages sent by 'reader_proc' on remote process)
    def get_output(i, coro=None):
        while True:
            line = yield coro.receive()
            if not line: # end of output
                break
            print('      job %s output: %s' % (i, line.strip().decode()))

    def create_job(i, coro=None):
        # create reader and send to rcoro so it can send messages to reader
        client_reader = asyncoro.Coro(get_output, i)
        # schedule rcoro on (available) remote server
        rcoro = yield rcoro_scheduler.schedule(rcoro_proc, client_reader, program_path)
        if isinstance(rcoro, asyncoro.Coro):
            print('  job %s processed by %s' % (i, rcoro.location))
            # sender sends input data to rcoro
            asyncoro.Coro(send_input, rcoro)
            # wait for all data to be received
            yield client_reader.finish()
            print('  job %s done' % i)
        else:  # failed to schedule
            client_reader.terminate()

    # create n jobs
    for i in range(1, n+1):
        asyncoro.Coro(create_job, i)

    yield rcoro_scheduler.finish(close=True)


if __name__ == '__main__':
    import random, sys, os
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)

    # discoro saves depedency files in node's directory. If the file at client
    # is at or below current directory (in directory hierarchy), then the file
    # will be saved with same relative path at client (e.g., if file is
    # './file1', then file is saved in node directory, and if file is
    # './dir/file2' then file is saved in 'dir' directory under node's
    # directory). However, if the file is not under current directory, then file
    # is saved in node's directory itself (without any path).

    # Either (simpler case) require program is executed from the directory where
    # it is (in this case dependencies are saved in node's directory with the
    # same paths at the cilent):

    if os.path.dirname(sys.argv[0]):
        print('This program must be executed from "%s" directory' %
              (os.path.abspath(os.path.dirname(sys.argv[0]))))
        exit(-1)

    program = 'discoro_client5_proc.py' # program to distribute and execute
    program = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), program))

    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    Scheduler()
    # send rcoro_proc and program
    computation = Computation([rcoro_proc, program])
    # use RemoteCoroScheduler to run jobs
    rcoro_scheduler = asyncoro.discoro_schedulers.RemoteCoroScheduler(computation)

    # Or (see 'Either' above), get the path as discoro would: If current
    # directory is a parent of program's path, get relative path to it, or just
    # the file name otherwise.
    # if program.startswith(os.getcwd()):
    #     program = program[len(os.getcwd()+os.sep):]
    # else:
    #     program = os.path.basename(program)

    # run n (defailt 5) instances of program
    asyncoro.Coro(client_proc, computation, program, 5 if len(sys.argv) < 2 else int(sys.argv[1]))
