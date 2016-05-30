# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# Distributed computing example where this client sends computation to
# remote discoro process to run as remote coroutines. At any time at
# most one computation coroutine is scheduled at a process. This
# implementation handles server processes terminating abruptly.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *
from asyncoro.discoro_schedulers import RemoteCoroScheduler
import asyncoro.httpd


# The computation in this example is simulated with 'time.sleep' (during which
# entire asyncoro framework is suspended as well). Usually CPU bound tasks that
# don't 'yield' (such as 'time.sleep') shouldn't be executed in coroutines, as
# asyncoro doesn't preempt currently running task, the framework is not
# responsive to any messages being sent to it (say, from client). In this
# example, no messages are not sent to/from computation, so it is okay to block
# asyncoro while computation is executed; see discomro_client8.py where
# computations are executed with threads so messages can be sent/received while
# computation is executed.

# discoronode expects user computations to be generator functions (that have at
# least one 'yield' statement) to create coroutines.
def compute(i, n, coro=None):
    # compute factorial of n; during this evaluation asyncoro framework cannot
    # receive / send messages, switch coroutines etc.
    factorial = 1
    for k in range(2, n):
        factorial *= k
    # coroutines should've at least one 'yield'. Note that factorial computed is
    # rather large and sending it to client may take time, which may casue
    # timeout issues over slow networks; instead of sending large amount of data
    # as messages, it may be preferable to send as file transfer
    yield (n, factorial) # value yielded here is sent as result to client

def client_proc(computation, njobs, coro=None):

    # 'status_proc' receives status messages from discoro scheduler and sends
    # them to both RemoteCoroScheduler and httpd
    def status_proc(coro=None):
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            # send message to RemoteCoroScheduler's status_proc:
            rcoro_scheduler.status_coro.send(msg)
            # and to httpd's status_coro:
            httpd.status_coro.send(msg)
            if isinstance(msg, asyncoro.MonitorException):
                rcoro = msg.args[0]
                result = msg.args[1][1]
                if msg.args[1][0] == StopIteration:
                    print('    result for job %s from %s' %
                          (result[0], rcoro.location))
                else:
                    print('    %s failed: %s' % (rcoro.location, str(result)))

    # to illustrate relaying of status messages to multiple coroutines, httpd is
    # also used in this example:
    httpd = asyncoro.httpd.HTTPServer(computation)
    # replace computation's status_coro (from rcoro_scheduler's status_coro) to
    # 'status_proc' above
    computation.status_coro = asyncoro.Coro(status_proc)

    if (yield computation.schedule()):
        raise Exception('schedule failed')

    # submit jobs
    for i in range(njobs):
        rcoro = yield rcoro_scheduler.schedule(compute, i, 100000 + i)
        if isinstance(rcoro, asyncoro.Coro):
            print('  job %s processed by %s' % (i, rcoro.location))
        else:
            print('rcoro %s failed: %s' % (i, rcoro))

    # wait for all jobs to be done and close computation
    yield rcoro_scheduler.finish(close=True)
    httpd.shutdown()


if __name__ == '__main__':
    import logging, random, sys, asyncoro.discoro
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    Scheduler()
    # send 'compute' generator function;
    # use MinPulseInterval so node status updates are sent more frequently
    # (instead of default 2*MinPulseInterval)
    computation = Computation([compute], timeout=5, pulse_interval=asyncoro.discoro.MinPulseInterval)
    rcoro_scheduler = RemoteCoroScheduler(computation)
    # run 10 (or given number of) jobs
    asyncoro.Coro(client_proc, computation, 10 if len(sys.argv) < 2 else int(sys.argv[1]))
