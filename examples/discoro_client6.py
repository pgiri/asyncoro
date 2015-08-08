# This example uses status messages and message passing to run remote
# coroutines to process streaming data.

# StatusMessage must be imported in global scope as below; otherwise,
# unserializing status messages fails (if external scheduler is used)
from asyncoro.discoro import StatusMessage
import asyncoro.discoro as discoro
import asyncoro.disasyncoro as asyncoro

# This generator function is sent to remote discoro to computes
# average of moving window of given size.
def rcoro_avg_proc(window_size, coro=None):
    import numpy as np
    data = np.empty(window_size, dtype=float)
    data.fill(0.0)
    cumsum = 0.0
    while True:
        n = yield coro.receive()
        if n is None:
            break
        cumsum += (n - data[0])
        # avg = (cumsum / window_size)
        data = np.roll(data, -1)
        data[-1] = n
    raise StopIteration(0)

# This generator function is sent to remote discoro process to save
# the received data in a file (on the remote peer).
def rcoro_save_proc(coro=None):
    import os
    import tempfile
    # save data in /tmp/tickdata
    with open(os.path.join(os.sep, tempfile.gettempdir(), 'tickdata'), 'w') as fd:
        while True:
            n = yield coro.receive()
            if n is None:
                break
            fd.write('%s\n' % n)
    raise StopIteration(0)

# This process runs locally. It sends (random) data to remote coroutines.
def client_proc(count, rcoro_avg, rcoro_save, coro=None):
    import random
    # if data is sent frequently (say, many times a second), enable
    # streaming data to remote peer; this is more efficient as
    # connections are kept open (so the cost of opening and closing
    # connections is avoided), but keeping too many connections open
    # may not be possible
    yield asyncoro.AsynCoro.instance().peer(rcoro_avg.location, stream_send=True)
    yield asyncoro.AsynCoro.instance().peer(rcoro_save.location, stream_send=True)
    for i in range(count):
        n = random.uniform(-1, 1)
        # data can be sent to remote coroutines either with 'send' or
        # 'deliver'; 'send' is more efficient but no guarantee data
        # has been sent successfully whereas 'deliver' indicates
        # errors right away; alternately, messages can be sent with a
        # channel, which is more convenient if there are multiple
        # (unknown) recipients
        rcoro_avg.send(n)
        rcoro_save.send(n)
        yield coro.sleep(0.01)
    rcoro_avg.send(None)
    rcoro_save.send(None)

# This process runs locally. It processes status messages to start
# coroutines, watches for (remote) coroutine finish status so
# computation can be terminated.
def status_proc(computation, coro=None):
    computation.status_coro = coro
    # if scheduler is shared (i.e., running as program), nothing needs
    # to be done (its location can optionally be given to 'schedule');
    # othrwise, start private scheduler:
    discoro.Scheduler()
    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')
    rcoro_avg = None
    rcoro_save = None
    client_coro = None
    while True:
        msg = yield coro.receive()
        if isinstance(msg, asyncoro.MonitorException):
            # a process finished job
            rcoro = msg.args[0]
            if msg.args[1][0] == StopIteration:
                if str(rcoro) == str(rcoro_avg):
                    rcoro_avg = None
                elif str(rcoro) == str(rcoro_save):
                    rcoro_save = None
                if rcoro_avg is None and rcoro_save is None:
                    break
            else:
                asyncoro.logger.warning('%s terminated with "%s"' %
                                        (rcoro.location, str(msg.args[1])))
        elif isinstance(msg, StatusMessage):
            asyncoro.logger.debug('Node/Process status: %s, %s' % (msg.status, msg.location))
            if msg.status == discoro.Scheduler.ProcInitialized:
                # a new process is available
                if rcoro_avg is None:
                    rcoro_avg = yield computation.run_at(msg.location, rcoro_avg_proc, 100)
                elif rcoro_save is None:
                    rcoro_save = yield computation.run_at(msg.location, rcoro_save_proc)
                    client_coro = asyncoro.Coro(client_proc, 1000, rcoro_avg, rcoro_save)
        else:
            asyncoro.logger.debug('Ignoring status message %s' % str(msg))

    yield computation.close()

if __name__ == '__main__':
    import logging, random, sys
    asyncoro.logger.setLevel(logging.DEBUG)
    computation = discoro.Computation([])
    inp_file = sys.argv[0] if len(sys.argv) == 1 else sys.argv[1]
    # run 10 jobs
    asyncoro.Coro(status_proc, computation).value()
