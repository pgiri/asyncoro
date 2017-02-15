# Run 'discoronode.py' program to start servers to execute computations sent by
# this client, along with this program.

# This example shows how to use 'httpd' module to start HTTP server so cluster /
# node / server / remote coroutine status can be monitored in a web browser at
# http://127.0.0.1:8181

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *


# objects of C are exchanged between client and servers
class C(object):
    def __init__(self, i):
        self.i = i
        self.n = None

    def __repr__(self):
        return '%d: %s' % (self.i, self.n)


# this generator function is sent to remote discoro servers to run
# coroutines there
def compute(obj, client, coro=None):
    # obj is an instance of C
    yield coro.sleep(obj.n)


def client_proc(computation, coro=None):
    if (yield computation.schedule()):
        raise Exception('schedule failed')

    i = 0
    while True:
        cmd = yield coro.receive()
        if cmd is None:
            break
        i += 1
        c = C(i)
        try:
            c.n = float(cmd)
        except:
            print('  "%s" is ignored')
            continue
        else:
            # unlike in discoro_client*.py, here 'run_async' is used to run
            # as many coroutines as given on servers (i.e., possibly more than
            # one coroutine on a server at any time).
            yield computation.run_async(compute, c, coro)

    yield computation.close()


if __name__ == '__main__':
    import os, asyncoro.discoro, asyncoro.httpd, sys, random
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    Scheduler()
    # send generator function and class C (as the computation uses
    # objects of C)
    # use MinPulseInterval so node status updates are sent more frequently
    # (instead of default 2*MinPulseInterval)
    computation = Computation([compute, C], pulse_interval=asyncoro.discoro.MinPulseInterval)
    # create http server to monitor nodes, servers, coroutines
    http_server = asyncoro.httpd.HTTPServer(computation)
    coro = asyncoro.Coro(client_proc, computation)
    print('   Enter "quit" or "exit" to end the program, or ')
    print('   Enter a number to schedule a coroutine on one of the servers')
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input

    while True:
        try:
            cmd = read_input().strip().lower()
            if cmd in ('quit', 'exit'):
                break
        except:
            break
        coro.send(cmd)
    coro.send(None)
    http_server.shutdown()
