# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# Example where this client sends computation to remote discoro
# process to run as remote coroutines. Remote coroutines and client
# can use message passing to exchange data.

import sys, logging, random
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro

# objects of C are exchanged between client and servers
class C(object):
    def __init__(self, i):
        self.i = i
        self.n = None

    def __repr__(self):
        return '%d: %.3f' % (self.i, self.n)

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

def status_proc(client, coro=None):
    # monitor status; see MonitorStatus for more comprehensive use cases
    procs_ready = 0
    coro.set_daemon()
    while True:
        msg = yield coro.receive()
        # wait until 3 processes ready
        if isinstance(msg, discoro.StatusMessage):
            # print('Status: %s / %s' % (msg.location, msg.status))
            if msg.status == discoro.Scheduler.ProcInitialized:
                procs_ready += 1
                if procs_ready == 3:
                    client.send('run')

def client_proc(computation, coro=None):
    # scheduler sends node / process status messages to status_coro
    computation.status_coro = asyncoro.Coro(status_proc, coro)

    # if no other clients use discoro, scheduler can be started in
    # client itself; alternately, scheduler can be run separately on a node
    discoro.Scheduler()

    # distribute computation to server
    if (yield computation.schedule()):
        raise Exception('schedule failed')

    # wait until processes are initialized; alternately, create
    # coroutines from 'status_proc' itself
    msg = yield coro.receive()
    assert msg == 'run'

    rcoros = []
    # create remote coroutines
    for i in range(3):
        c = C(i)
        rcoro = yield computation.run(compute, c, coro)
        if isinstance(rcoro, asyncoro.Coro):
            rcoros.append(rcoro)
        else:
            print('failed to create remote coroutine for %s' % c)

    # send data to remote coroutines (as messages)
    for rcoro in rcoros:
        rcoro.send(random.uniform(5, 20))

    # remote coroutines send replies as messages to this 'coro'
    for rcoro in rcoros:
        reply = yield coro.receive()
        print('reply: %s' % (reply))

    yield computation.close()
    yield coro.sleep(1)

if __name__ == '__main__':
    asyncoro.logger.setLevel(logging.DEBUG)
    # send generator function and class C (as the function uses
    # objects of C); 'depends' can include files, functions, objets
    computation = discoro.Computation([compute, C], timeout=5)
    asyncoro.Coro(client_proc, computation)
