# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# Distributed computing example where this client sends computation to
# remote discoro process to run as remote coroutines. At any time at
# most one computation coroutine is scheduled at a process. This
# implementation handles server processes terminating abruptly.

import logging, random
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro_schedulers import RemoteCoroScheduler
import asyncoro.httpd


# this generator function is sent to remote discoro servers to run
# coroutines there
def compute(n, coro=None):
    import time
    yield coro.sleep(n)
    raise StopIteration(time.asctime()) # result of 'compute' is current time


def client_proc(computation, coro=None):

    def status_proc(coro=None):
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            # send message to RemoteCoroScheduler's status_proc:
            job_scheduler.status_coro.send(msg)
            # and to httpd's status_coro:
            httpd.status_coro.send(msg)
            if isinstance(msg, asyncoro.MonitorException):
                if msg.args[1][0] == StopIteration:
                    print('result from %s: %s' % (msg.args[0].location, msg.args[1][1]))
                else:
                    # if computation is reentrant, resubmit this job
                    # (keep track of submitted rcoro, args and kwargs)
                    print('%s failed: %s' % (msg.args[0], msg.args[1][1]))

    job_scheduler = RemoteCoroScheduler(computation)
    computation.status_coro = asyncoro.Coro(status_proc)

    if (yield computation.schedule()):
        raise Exception('schedule failed')

    # submit jobs
    for i in range(3):
        rcoro = yield job_scheduler.schedule(compute, random.uniform(10, 20))

    # wait for all jobs to be done and close computation
    yield job_scheduler.finish(close=True)


if __name__ == '__main__':
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    discoro.Scheduler()
    # send 'compute' generator function
    computation = discoro.Computation([compute], timeout=5)
    # to illustrate passing status messages to multiple coroutines,
    # httpd is also used in this example:
    httpd = asyncoro.httpd.HTTPServer(computation)
    # call '.value()' of coroutine created here, otherwise main thread
    # may finish (causing interpreter to start cleanup) before asyncoro
    # scheduler gets a chance to start
    asyncoro.Coro(client_proc, computation).value()
    httpd.shutdown()
