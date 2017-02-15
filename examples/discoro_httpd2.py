# Run 'discoronode.py' program to start servers to execute computations sent by
# this client, along with this program.

# This example is similar to 'discoro_httpd1.py'. It sets up 'status_coro' of
# computation to get status messages from discoro scheduler (which are actally
# relayed by httpd's status_coro) to show when remote coroutine is created and
# finished.

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


# status messages indicating nodes, servers and remote coroutines
# finish status are sent to this coroutine
def status_proc(coro=None):
    coro.set_daemon()
    while True:
        msg = yield coro.receive()
        # (re)send all status messages to http server
        if isinstance(msg, asyncoro.MonitorException):
            if msg.args[1][0] == StopIteration:
                print('    rcoro %s done' % (msg.args[0]))
            else:
                print('  rcoro %s failed: %s / %s' % (msg.args[0], msg.args[1][0], msg.args[1][1]))
        elif isinstance(msg, DiscoroStatus):
            if msg.status == Scheduler.CoroCreated:
                print('rcoro %s started' % msg.info.coro)
            # else:
            #     print('Status: %s / %s' % (msg.status, msg.info))

        elif isinstance(msg, DiscoroNodeAvailInfo):
            pass
        else:
            print('status msg ignored: %s' % type(msg))


def client_proc(computation, coro=None):
    # schedule computation with the scheduler
    if (yield computation.schedule()):
        raise Exception('schedule failed')

    i = 0
    while True:
        cmd = yield coro.receive()
        if cmd is None:
            break
        i += 1
        c = C(i)
        c.n = random.uniform(20, 50)
        # unlike in discoro_client*.py, here 'run_async' is used to run as
        # many coroutines as given on servers (i.e., possibly more than one
        # coroutine on a server at any time).
        rcoro = yield computation.run_async(compute, c, coro)
        if isinstance(rcoro, asyncoro.Coro):
            print('  %s: rcoro %s created' % (i, rcoro))
        else:
            print('  %s: rcoro failed: %s' % (i, rcoro))

    yield computation.close()


if __name__ == '__main__':
    import os, asyncoro.discoro, asyncoro.httpd, sys, random
    asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    Scheduler()
    # send generator function and class C (as the computation uses
    # objects of C)
    # use MinPulseInterval so node status updates are sent more frequently
    # (instead of default 2*MinPulseInterval)
    computation = Computation([compute, C], status_coro=asyncoro.Coro(status_proc),
                              pulse_interval=asyncoro.discoro.MinPulseInterval)

    # create http server to monitor nodes, servers, coroutines
    http_server = asyncoro.httpd.HTTPServer(computation)
    coro = asyncoro.Coro(client_proc, computation)
    print('   Enter "quit" or "exit" to end the program, or ')
    print('   Enter anything else to schedule a coroutine on one of the servers')
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
