# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# Example where this client sends computation to remote discoro
# process to run as remote coroutines. At any time at most one
# computation coroutine is scheduled at a process. This implementation
# handles server processes terminating abruptly.

import logging, random
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro
from discoro_schedulers import ProcScheduler


# this generator function is sent to remote discoro servers to run
# coroutines there
def compute(n, coro=None):
    print('process at %s received: %s' % (coro.location, n))
    yield coro.sleep(n)
    raise StopIteration(n) # result of 'compute' is n


def client_proc(computation, coro=None):

    def status_proc(coro=None):
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            # send message to ProcScheduler's status_proc
            job_scheduler_status_coro.send(msg)
            if isinstance(msg, asyncoro.MonitorException):
                if msg.args[1][0] == StopIteration:
                    print('result: %s' % msg.args[1][1])
                else:
                    # if computation is reentrant, resubmit this job
                    # (keep track of submitted rcoro, args and kwargs)
                    print('%s failed: %s' % (msg.args[0], msg.args[1][1]))

    job_scheduler = ProcScheduler(computation)
    # ProcScheduler sets 'status_coro'; save it and set to status_proc above
    job_scheduler_status_coro = computation.status_coro
    computation.status_coro = asyncoro.Coro(status_proc)

    if (yield computation.schedule()):
        raise Exception('schedule failed')

    # submit jobs
    for i in range(5):
        rcoro = yield job_scheduler.submit(compute, random.uniform(3, 10))

    # wait for all jobs to be done
    yield job_scheduler.finish(close=True)


if __name__ == '__main__':
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    discoro.Scheduler()
    # send 'compute' generator function; 'depends' can include files, functions, objets
    computation = discoro.Computation([compute], timeout=5)
    asyncoro.Coro(client_proc, computation).value()
