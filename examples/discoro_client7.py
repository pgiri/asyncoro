# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# This program is similar to 'discoro_client1.py', but instead of using
# RemoteCoroScheduler, it uses status message notifications from discoro
# scheduler to submit jobs at remote discoronode processes.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *

# this generator function is sent to remote server to run coroutines there
def rcoro_proc(n, coro=None):
    yield coro.sleep(n)
    raise StopIteration(n)


# Instead of using RemoteCoroScheduler (which is easier), in this example status
# messages from discoro scheduler are used to start remote coroutines and get
# their exit status
def status_proc(computation, njobs, coro=None):
    npending = njobs

    # in this example at most one coroutine is submitted at a server; depending
    # on computation / needs, many coroutines can be simlutaneously submitted /
    # running at a server.
    while True:
        msg = yield coro.receive()
        if isinstance(msg, DiscoroStatus):
            # print('Status: %s / %s' % (msg.info, msg.status))
            if msg.status == Scheduler.ServerInitialized and njobs > 0: # submit a job
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
            if njobs > 0: # submit another job
                n = random.uniform(5, 10)
                rcoro = yield computation.run_at(rcoro.location, rcoro_proc, n)
                if isinstance(rcoro, asyncoro.Coro):
                    print('  rcoro_proc started at %s with %s' % (rcoro.location, n))
                    njobs -= 1

def client_proc(computation, njobs, coro=None):
    computation.status_coro = asyncoro.Coro(status_proc, computation, njobs)
    # since RemoteCoroScheduler is not used, schedule computation
    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')
    # wait for jobs to be created and finished
    yield computation.status_coro.finish()
    yield computation.close()


if __name__ == '__main__':
    import random
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    Scheduler()
    computation = Computation([rcoro_proc])
    # run 10 jobs
    asyncoro.Coro(client_proc, computation, 10)
