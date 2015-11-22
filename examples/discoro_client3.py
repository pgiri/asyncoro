# This program uses status message notifications to submit jobs to
# discoro processes.

# DiscoroStatus must be imported in global scope as below; otherwise,
# unserializing status messages fails (if external scheduler is used)
from asyncoro.discoro import DiscoroStatus
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro


# this generator function is sent to remote server to run
# coroutines there
def rcoro_proc(n, coro=None):
    yield coro.sleep(n)
    raise StopIteration(n)


def client_proc(computation, njobs, coro=None):
    status = {'submitted': 0, 'done': 0}

    def submit_job(where, coro=None):
        arg = random.uniform(5, 10)
        rcoro = yield computation.run_at(where, rcoro_proc, arg)
        if isinstance(rcoro, asyncoro.Coro):
            print('%s processing %s' % (rcoro.location, arg))
        else:
            print('Job %s failed: %s' % (status['submitted'], str(rcoro)))
        status['submitted'] += 1

    computation.status_coro = coro
    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')
    # job submitter assumes that one process can run one coroutine at a time
    while True:
        msg = yield coro.receive()
        if isinstance(msg, asyncoro.MonitorException):
            # a process finished job
            rcoro = msg.args[0]
            if msg.args[1][0] == StopIteration:
                print('Remote coroutine %s finished with %s' % (rcoro.location, msg.args[1][1]))
            else:
                asyncoro.logger.warning('Remote coroutine %s terminated with "%s"' %
                                        (rcoro.location, str(msg.args[1])))
            status['done'] += 1
            # because jobs are submitted with 'yield' with coroutines,
            # and 'submitted' is incremented after 'yield', it is
            # likely that more than 'njobs' are submitted
            if status['done'] >= njobs and status['done'] == status['submitted']:
                break
            if status['submitted'] < njobs:
                # schedule another job at this process
                asyncoro.Coro(submit_job, rcoro.location)
        elif isinstance(msg, DiscoroStatus):
            asyncoro.logger.debug('Node/Server status: %s, %s' % (msg.status, msg.info))
            if msg.status == discoro.Scheduler.ServerInitialized:
                # a new process is ready (if special initialization is
                # required for preparing process, schedule it)
                asyncoro.Coro(submit_job, msg.info)
        else:
            asyncoro.logger.debug('Ignoring status message %s' % str(msg))
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
