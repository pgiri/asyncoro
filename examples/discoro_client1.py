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

def client_proc(computation, coro=None):
    # wait a bit for scheduler to detect processes; alternately status
    # messages can be used (see other discoro_client*.py for examples)
    yield coro.sleep(1)

    # schedule computation (if scheduler is shared, this waits until
    # prior computations are finished)
    if (yield computation.schedule()):
        raise Exception('schedule failed')

    # wait a bit for scheduler to send computation to processes
    yield coro.sleep(2)
    rcoros = []
    # create remote coroutines
    for i in range(3):
        c = C(i)
        rcoro = yield computation.run(compute, c, coro)
        if isinstance(rcoro, asyncoro.Coro):
            rcoros.append(rcoro)
        else:
            print('%s failed: %s' % (i, str(rcoro)))

    # send data to remote coroutines (as messages)
    for rcoro in rcoros:
        rcoro.send(random.uniform(5, 20))

    # remote coroutines send replies as messages to this 'coro'
    for rcoro in rcoros:
        reply = yield coro.receive()
        print('reply: %s' % (reply))

    yield computation.close()

if __name__ == '__main__':
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    discoro.Scheduler()
    # send generator function and class C (as the function uses
    # objects of C); 'depends' can include files, functions, objets
    computation = discoro.Computation([compute, C], timeout=5)
    asyncoro.Coro(client_proc, computation).value()
