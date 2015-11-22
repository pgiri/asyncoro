# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# Example where this client sends computation to remote discoro
# process to run as remote coroutines. Remote coroutines and client
# can use message passing to exchange data.

import logging, random
import asyncoro.discoro as discoro
from asyncoro.discoro import DiscoroStatus
import asyncoro.disasyncoro as asyncoro


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
    import math
    # this coroutine and client can use message passing; client sends
    # data to this coro as a message
    n = yield coro.receive()
    print('process at %s received: %s' % (coro.location, n))
    obj.n = math.sqrt(n)
    yield coro.sleep(obj.n)
    # send result back to client
    yield client.deliver(obj, timeout=5)


# status messages indicating nodes, processes as well as remote
# coroutines finish status are sent to this coroutine
def status_proc(client, coro=None):
    procs_ready = 0
    coro.set_daemon()
    while True:
        msg = yield coro.receive()
        if isinstance(msg, asyncoro.MonitorException):
            if msg.args[1][0] != StopIteration:
                print('rcoro %s failed: %s / %s' % (msg.args[0], msg.args[1][0], msg.args[1][1]))
        elif isinstance(msg, DiscoroStatus):
            if msg.status == discoro.Scheduler.ServerInitialized:
                # wait until 2 processes ready
                procs_ready += 1
                if procs_ready == 2:
                    client.send('processes ready')


def client_proc(computation, coro=None):
    # scheduler sends node / process status messages to status_coro
    computation.status_coro = asyncoro.Coro(status_proc, coro)

    # distribute computation to server
    if (yield computation.schedule()):
        raise Exception('schedule failed')

    # wait until processes are initialized; alternately, create
    # coroutines from 'status_proc' itself
    msg = yield coro.receive()
    assert msg == 'processes ready'

    rcoros = []
    # create remote coroutines
    for i in range(3):
        c = C(i)
        rcoro = yield computation.run(compute, c, coro)
        if isinstance(rcoro, asyncoro.Coro):
            rcoros.append(rcoro)
        else:
            print('failed to create remote coroutine for %s: %s' % (c, rcoro))

    # yield coro.sleep(10)
    # send data to remote coroutines (as messages)
    for rcoro in rcoros:
        rcoro.send(random.uniform(5, 20))

    # remote coroutines send replies as messages to this coro
    for rcoro in rcoros:
        reply = yield coro.receive()
        print('reply: %s' % (reply))

    yield computation.close()


if __name__ == '__main__':
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    discoro.Scheduler()
    # send generator function and class C (as the computation uses
    # objects of C)
    computation = discoro.Computation([compute, C])
    # call '.value()' of coroutine created here, otherwise main thread
    # may finish (causing interpreter to start cleanup) before asyncoro
    # scheduler gets a chance to start
    asyncoro.Coro(client_proc, computation).value()
