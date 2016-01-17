# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# Example where this client sends computation to remote discoro process to run
# as remote coroutines. Computations are scheduled with custom scheduler
# (without using RemoteCoroScheduler). Remote coroutines and client can use
# message passing to exchange data.

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
    print('process at %s received: %s' % (coro.location, obj.n))
    yield coro.sleep(obj.n)
    obj.n = math.sqrt(obj.n)
    # send result back to client
    yield client.deliver(obj, timeout=5)


# status messages indicating nodes, processes as well as remote
# coroutines finish status are sent to this coroutine
def status_proc(computation, njobs, client, coro=None):
    coro.set_daemon()

    while True:
        msg = yield coro.receive()
        if isinstance(msg, asyncoro.MonitorException):
            rcoro = msg.args[0]
            if msg.args[1][0] != StopIteration:
                print('rcoro %s failed: %s / %s' % (msg.args[0], msg.args[1][0], msg.args[1][1]))
            if njobs > 0: # submit another job
                c = C(njobs)
                c.n = random.uniform(5, 10)
                rcoro = yield computation.run_at(rcoro.location, compute, c, client)
                if isinstance(rcoro, asyncoro.Coro):
                    njobs -= 1
        elif isinstance(msg, DiscoroStatus):
            print('Status: %s / %s' % (msg.info, msg.status))
            if msg.status == discoro.Scheduler.ServerInitialized and njobs > 0: # submit a job
                c = C(njobs)
                c.n = random.uniform(5, 10)
                rcoro = yield computation.run_at(msg.info, compute, c, client)
                if isinstance(rcoro, asyncoro.Coro):
                    njobs -= 1


def client_proc(computation, njobs, coro=None):
    # scheduler sends node / process status messages to status_coro
    computation.status_coro = asyncoro.Coro(status_proc, computation, njobs, coro)

    # distribute computation to server
    if (yield computation.schedule()):
        raise Exception('schedule failed')

    # remote coroutines send replies as messages to this coro
    for i in range(njobs):
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
    # create 10 remote coroutines (jobs)
    asyncoro.Coro(client_proc, computation, 10).value()
