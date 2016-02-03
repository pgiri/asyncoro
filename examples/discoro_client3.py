# Run 'discoronode.py' program to start processes to execute
# computations sent by this client, along with this program.

# This program is similar to 'discoro_client1.py', but instead of using
# RemoteCoroScheduler, it uses status message notifications from discoro
# scheduler to submit jobs at remote discoronode processes.

# DiscoroStatus must be imported in global scope as below; otherwise,
# unserializing status messages fails (if external scheduler is used)
from asyncoro.discoro import DiscoroStatus
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro

# this generator function is sent to remote server to run coroutines there
def rcoro_proc(n, coro=None):
    yield coro.sleep(n)
    raise StopIteration(n)


# Instead of using RemoteCoroScheduler (which is easier), in this example status
# messages from discoro scheduler are used to start remote coroutines and get
# their exit status
def status_proc(computation, njobs, coro=None):
    npending = njobs

    while True:
        msg = yield coro.receive()
        if isinstance(msg, asyncoro.MonitorException):
            rcoro = msg.args[0]
            if msg.args[1][0] == StopIteration:
                print('  rcoro_proc at %s finished with %s' % (rcoro.location, msg.args[1][1]))
            else:
                print('  rcoro_proc at %s failed: %s / %s' %
                      (rcoro.location, msg.args[1][0], msg.args[1][1]))

            npending -= 1
            if npending == 0:
                break
            if njobs > 0: # submit another job
                n = random.uniform(5, 10)
                rcoro = yield computation.run_at(rcoro.location, rcoro_proc, n)
                if isinstance(rcoro, asyncoro.Coro):
                    print('rcoro_proc started at %s with %s' % (rcoro.location, n))
                    njobs -= 1
        elif isinstance(msg, DiscoroStatus):
            # print('Status: %s / %s' % (msg.info, msg.status))
            if msg.status == discoro.Scheduler.ServerInitialized and njobs > 0: # submit a job
                n = random.uniform(5, 10)
                rcoro = yield computation.run_at(msg.info, rcoro_proc, n)
                if isinstance(rcoro, asyncoro.Coro):
                    print('rcoro_proc started at %s with %s' % (rcoro.location, n))
                    njobs -= 1

def client_proc(computation, njobs, coro=None):
    computation.status_coro = asyncoro.Coro(status_proc, computation, njobs)
    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')
    # wait for jobs to be created and finished
    yield computation.status_coro.finish()
    yield computation.close()


if __name__ == '__main__':
    import logging, random
    asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    discoro.Scheduler()
    computation = discoro.Computation([rcoro_proc])
    # call '.value()' of coroutine created here, otherwise main thread
    # may finish (causing interpreter to start cleanup) before asyncoro
    # scheduler gets a chance to start
    # run 10 jobs
    asyncoro.Coro(client_proc, computation, 10).value()
