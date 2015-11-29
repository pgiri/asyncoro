# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# This example runs a program (discomp7_proc.py) on a remote
# server. The program reads from standard input and writes to standard
# output. Subprocess and asynchronous pipes are used to write / read
# data from the program, and message passing is used to get data from
# client (which is sent to the program) and to send output from the
# program back to the client.

import logging, random
from asyncoro.discoro import DiscoroStatus
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro

# rcoro_proc is sent to remote server to execute discomp7_proc.py
# program. It uses message passing to get data from client that is
# written to pipe and read output from program that is sent back to
# client.
def rcoro_proc(client, program, coro=None):
    import sys
    import subprocess
    import asyncoro.asyncfile

    if program.endswith('.py'):
        program = [sys.executable, program]
    # start program as a subprocess (to read from and write to pipe)
    pipe = subprocess.Popen(program, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    # convert to asynchronous pipe
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
        client.send(None)

    reader = asyncoro.Coro(reader_proc)

    # writer gets messages from client and writes them (input) to pipe
    while True:
        data = yield coro.receive()
        if not data:
            break
        # write data as lines to program
        yield pipe.write(str(data) + '\n', full=True)
    pipe.stdin.close()
    # wait for all data to be read
    yield reader.finish()
    raise StopIteration(pipe.poll())

def client_proc(computation, coro=None):

    # send 10 random numbers to remote process (rcoro_proc)
    def send_input(rcoro, coro=None):
        for i in range(10):
            rcoro.send(random.uniform(1, 4))
            yield coro.sleep(1)
        # end of input is indicated with None
        rcoro.send(None)

    # read output (messages sent by 'reader_proc' on remote process)
    def get_output(coro=None):
        while True:
            line = yield coro.receive()
            if not line:
                break
            print('output: %s' % line.strip())

    computation.status_coro = coro
    if (yield computation.schedule()):
        raise Exception('schedule failed')

    # in this example only one remote process is started
    client_reader = asyncoro.Coro(get_output)
    rcoro = None
    # process status messages from scheduler
    while True:
        msg = yield coro.receive()
        if isinstance(msg, DiscoroStatus):
            if not rcoro and msg.status == discoro.Scheduler.ServerInitialized:
                rcoro = yield computation.run_at(msg.info, rcoro_proc,
                                                 client_reader, 'discomp7_proc.py')
                if isinstance(rcoro, asyncoro.Coro):
                    print('rcoro %s created' % rcoro)
                    asyncoro.Coro(send_input, rcoro)
                else:
                    print('starting remote process at %s failed: %s' % (msg.info, rcoro))
                    rcoro = None
        elif isinstance(msg, asyncoro.MonitorException):
            # assert str(msg.args[0]) == str(rcoro)
            if msg.args[1][0] == StopIteration:
                print('rcoro %s done: %s' % (msg.args[0], msg.args[1][1]))
            else:
                print('rcoro %s failed: %s / %s' % (msg.args[0], msg.args[1][0], msg.args[1][1]))
            break

    # wait for all data to be received
    yield client_reader.finish()
    yield computation.close()


if __name__ == '__main__':
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    discoro.Scheduler()
    # send rcoro_proc and discomp7_proc.py
    computation = discoro.Computation([rcoro_proc, 'discomp7_proc.py'])
    asyncoro.Coro(client_proc, computation).value()
