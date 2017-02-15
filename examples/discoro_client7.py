# Run 'discoronode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This program is similar to 'discoro_client1.py', but instead of using
# 'run_result' (which is simpler), it uses status message
# notifications from discoro scheduler to run jobs at specific remote discoro
# servers. This template can be used to implement cusomized scheduler to run
# remote coroutines.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *

# this generator function is sent to remote server to run coroutines there
def rcoro_proc(n, coro=None):
    yield coro.sleep(n)
    raise StopIteration(n)


# Instead of using computation's 'run_result' method to get results (which is
# easier), in this example status messages from discoro scheduler are used to
# start remote coroutines and get their results
def status_proc(computation, njobs, coro=None):
    # set computation's status_coro to receive status messages from discoro
    # scheduler (this should be done before httpd is created, in case it is
    # used).
    computation.status_coro = coro
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations
    # wait for jobs to be created and finished
    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')

    npending = njobs

    # in this example at most one coroutine is submitted at a server; depending
    # on computation / needs, many coroutines can be simlutaneously submitted /
    # running at a server (with 'computation.run_async').
    while True:
        msg = yield coro.receive()
        if isinstance(msg, DiscoroStatus):
            # print('Status: %s / %s' % (msg.info, msg.status))
            if msg.status == Scheduler.ServerInitialized and njobs > 0: # run a job
                n = random.uniform(5, 10)
                rcoro = yield computation.run_at(msg.info, rcoro_proc, n)
                if isinstance(rcoro, asyncoro.Coro):
                    print('  rcoro_proc started at %s with %s' % (rcoro.location, n))
                    njobs -= 1
        elif isinstance(msg, asyncoro.MonitorException):
            # previously submitted remote coroutine finished
            rcoro = msg.args[0]
            if msg.args[1][0] == StopIteration: # exit status type
                print('      rcoro_proc at %s finished with %s' % (rcoro.location, msg.args[1][1]))
            else:
                print('      rcoro_proc at %s failed: %s / %s' %
                      (rcoro.location, msg.args[1][0], msg.args[1][1]))

            npending -= 1
            if npending == 0:
                break
            if njobs > 0: # run another job
                n = random.uniform(5, 10)
                rcoro = yield computation.run_at(rcoro.location, rcoro_proc, n)
                if isinstance(rcoro, asyncoro.Coro):
                    print('  rcoro_proc started at %s with %s' % (rcoro.location, n))
                    njobs -= 1

    yield computation.close()


if __name__ == '__main__':
    import random, sys
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    Scheduler()
    njobs = 10 if len(sys.argv) < 2 else int(sys.argv[1])
    computation = Computation([rcoro_proc])
    asyncoro.Coro(status_proc, computation, njobs)
