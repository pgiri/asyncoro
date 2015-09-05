# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# The remote coroutines exit with the replies (similar to "return" in
# regular functions) to demonstrate 'MonitorStatus' in
# 'monitor_coros.py'

import sys, logging, random
import asyncoro.discoro as discoro
from asyncoro.discoro import DiscoroStatus
import asyncoro.disasyncoro as asyncoro

# this generator function is sent to remote discoro servers to run
# coroutines there
def compute(coro=None):
    # this coroutine and client can use message passing; client sends
    # data to this coro as a message
    n = yield coro.receive()
    print('process at %s received: %s' % (coro.location, n))
    yield coro.sleep(n)
    # This value is sent as MonitorException to 'status_proc' at
    # client. Note that if shared (external) scheduler is used, it is
    # not aware of user classes (only the client and remote processes
    # have the definitions of user classes), so if exit values are
    # user objects, then the scheduler won't be able to unpickle the
    # value and fail. Moreover, the information can get lost due to
    # network failure (i.e,. value may not be received by client). So
    # it is safer to use 'deliver' to send replies (as done in other
    # examples) and if that fails, either wait a bit and try again, or
    # save the value in a file that can be retrieved by user later.
    raise StopIteration(n)

# This local coroutine creates 3 remote coroutines at remote discoro
# server at 'where'. Remote coroutines are awaited for exit values
# with 'MonitorCoros' helper. When all 3 remote coroutines finish,
# sends 'done' message to 'status_coro'
def client_proc(where, status_coro, coro=None):
    rcoros = []
    # create remote coroutines. Note that all 3 coroutines run
    # concurrently at the process, which may not be ideal if the jobs
    # are mostly CPU bound (if there are other processes available, it
    # may be more efficient to distribute the jobs to all available
    # processes instead)
    for i in range(3):
        rcoro = yield computation.run_at(where, compute)
        if isinstance(rcoro, asyncoro.Coro):
            rcoros.append(rcoro)
        else:
            print('failed to create remote coroutine for %s: %s' % (i, rcoro))

    # send data to remote coroutines (as messages)
    for rcoro in rcoros:
        rcoro.send(random.uniform(5, 20))

    # get exit value for each rcoro
    for rcoro in rcoros:
        value = yield monitored.finish(rcoro)
        print('value from %s: %s' % (rcoro.location, value))
    # inform status_coro
    status_coro.send('proc_done')

# status messages indicating nodes, processes as well as remote
# coroutines finish status are sent to this coroutine
def status_proc(computation, coro=None):
    # scheduler sends node / process status messages to status_coro
    computation.status_coro = coro

    if (yield computation.schedule()):
        raise Exception('schedule failed')

    procs_used = 0
    procs_done = 0
    while True:
        msg = yield coro.receive()
        if isinstance(msg, asyncoro.MonitorException):
            # send coroutine status message to 'MonitorCoros' object
            monitored.monitor_coro.send(msg)
        elif isinstance(msg, DiscoroStatus):
            # print('Status: %s / %s' % (msg.info, msg.status))
            if msg.status == discoro.Scheduler.ServerInitialized and procs_used < 2:
                asyncoro.Coro(client_proc, msg.info, coro)
                procs_used += 1
            elif msg.status == discoro.Scheduler.ComputationClosed:
                print('Computation closed!')
                break
        elif msg == 'proc_done':
            # sent by 'client_proc' coroutines
            procs_done += 1
            if procs_done == procs_used and procs_used >= 2:
                break

    yield computation.close()

if __name__ == '__main__':
    import os, threading, monitor_coros
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    discoro.Scheduler()
    computation = discoro.Computation([compute])
    # create 'MonitorCoros' object so remote coros can be waited for in 'client_proc'
    monitored = monitor_coros.MonitorCoros()
    asyncoro.Coro(status_proc, computation).value()
