# This example uses status messages and message passing to run remote coroutines
# to process streaming data for live/real-time analysis. This example uses
# 'deque' module to implement circular buffer.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *
from asyncoro.discoro_schedulers import RemoteCoroScheduler


# This generator function is sent to remote discoro to analyze data
# and generate apprporiate signals that are sent to a coroutine
# running on client. The signal in this simple case is average of
# moving window of given size is below or above a threshold.
def rcoro_avg_proc(channel, threshold, trend_coro, window_size, coro=None):
    import collections
    # subscribe to channel (at client)
    yield channel.subscribe(coro)
    # create circular buffer
    data = collections.deque(maxlen=window_size)
    for i in range(window_size):
        data.append(0.0)
    cumsum = 0.0
    # first message is 'start' command; see 'client_proc'
    assert (yield coro.receive()) == 'start'
    while True:
        i, n = yield coro.receive()
        if n is None:
            break
        cumsum += (n - data[0])
        avg = (cumsum / window_size)
        if avg > threshold:
            trend_coro.send((i, 'high', float(avg)))
        elif avg < -threshold:
            trend_coro.send((i, 'low', float(avg)))
        data.append(n)
    raise StopIteration(0)


# This generator function is sent to remote discoro process to save
# the received data in a file (on the remote peer).
def rcoro_save_proc(channel, coro=None):
    import os
    import tempfile
    # subscribe to channel (at client)
    yield channel.subscribe(coro)
    # first message is 'start' command (to make sure all recipients started)
    assert (yield coro.receive()) == 'start'
    # save data in 'tickdata' where computation files are saved (this will be
    # deleted when computation is done, so it must be copied elsewhere if
    # necessary)
    with open('tickdata', 'w') as fd:
        while True:
            i, n = yield coro.receive()
            if n is None:
                break
            fd.write('%s: %s\n' % (i, n))
    raise StopIteration(0)


# This coroutine runs on client. It gets trend messages from remote
# coroutine that computes moving window average.
def trend_proc(coro=None):
    coro.set_daemon()
    while True:
        trend = yield coro.receive()
        print('trend signal at % 4d: %s / %.2f' % (trend[0], trend[1], trend[2]))


# This process runs locally. It creates two remote coroutines at two discoronode
# server processes, two local coroutines, one to receive trend signal from one
# of the remote coroutines, and another to send data to two remote coroutines
def client_proc(computation, coro=None):
    # in discoro_client6.py, data is sent to each remote coroutine; here, data
    # is broadcast over channel and remote coroutines subscribe to it
    data_channel = asyncoro.Channel('data_channel')
    # not necessary to register channel in this case, as it is sent to remote
    # coroutines; if they were to 'locate' it, it should be registered
    # data_channel.register()

    trend_coro = asyncoro.Coro(trend_proc)

    rcoro_avg = yield rcoro_scheduler.schedule(rcoro_avg_proc, data_channel, 0.4, trend_coro, 10)
    assert isinstance(rcoro_avg, asyncoro.Coro)
    rcoro_save = yield rcoro_scheduler.schedule(rcoro_save_proc, data_channel)
    assert isinstance(rcoro_save, asyncoro.Coro)

    # make sure both remote coroutines have subscribed to channel ('deliver'
    # should return 2 if they both are)
    assert (yield data_channel.deliver('start', n=2)) == 2

    # if data is sent frequently (say, many times a second), enable
    # streaming data to remote peer; this is more efficient as
    # connections are kept open (so the cost of opening and closing
    # connections is avoided), but keeping too many connections open
    # consumes system resources
    yield asyncoro.AsynCoro.instance().peer(rcoro_avg.location, stream_send=True)
    yield asyncoro.AsynCoro.instance().peer(rcoro_save.location, stream_send=True)

    # send 1000 items of random data to remote coroutines
    for i in range(1000):
        n = random.uniform(-1, 1)
        item = (i, n)
        # data can be sent to remote coroutines either with 'send' or
        # 'deliver'; 'send' is more efficient but no guarantee data
        # has been sent successfully whereas 'deliver' indicates
        # errors right away
        data_channel.send(item)
        yield coro.sleep(0.02)
    item = (i, None)
    data_channel.send(item)

    yield rcoro_scheduler.finish(close=True)
    data_channel.close()


if __name__ == '__main__':
    import random
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    # if scheduler is shared (i.e., running as program), nothing needs
    # to be done (its location can optionally be given to 'schedule');
    # othrwise, start private scheduler:
    Scheduler()
    computation = Computation([])
    # use RemoteCoroScheduler to schedule/submit coroutines; scheduler must be
    # created before computation is scheduled (next step below)
    rcoro_scheduler = RemoteCoroScheduler(computation)
    asyncoro.Coro(client_proc, computation)
