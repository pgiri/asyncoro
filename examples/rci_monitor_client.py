#!/usr/bin/env python

# to be used with 'rci_monitor_server.py'
# client requests execution of coroutines on (remote) server.

import sys, random
# import disasyncoro to use distributed version of AsynCoro
import asyncoro.disasyncoro as asyncoro

def monitor_proc(n, coro=None):
    # this coro gets exceptions from (remote) coroutines created in coro1
    done = 0
    while True:
        msg = yield coro.receive()
        if isinstance(msg, asyncoro.MonitorException):
            rcoro = msg.args[0]
            value_type, value = msg.args[1]
            if value_type == StopIteration:
                asyncoro.logger.debug('RCI %s finished with: %s', rcoro, value)
            else:
                asyncoro.logger.debug('RCI %s failed with: %s', rcoro, value.args[0])
            done += 1
            if done == n:
                break
        else:
            asyncoro.logger.warning('ignoring invalid message')

def rci_test(coro=None):
    # if server is on remote network, automatic discovery won't work,
    # so add it explicitly
    # yield scheduler.peer('192.168.21.5')

    # find where 'rci_1' is registered (with given name in any
    # known peer)
    rci1 = yield asyncoro.RCI.locate('rci_1')
    print('RCI is at %s' % rci1.location)
    # alternately, location can be explicitly created with
    # asyncoro.Location or obtained with 'locate' of asyncoro etc.
    loc = yield scheduler.locate('server')
    rci1 = yield asyncoro.RCI.locate('rci_1', loc)
    print('RCI is at %s' % rci1.location)

    n = 5
    monitor = asyncoro.Coro(monitor_proc, n)
    for x in range(n):
        rcoro = yield rci1('test%s' % x, b=x)
        asyncoro.logger.debug('RCI %s created' % rcoro)
        # set 'monitor' as monitor for this coroutine
        yield monitor.monitor(rcoro)
        # send a message
        rcoro.send('msg:%s' % x)
        yield coro.sleep(random.uniform(0, 1))

asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
scheduler = asyncoro.AsynCoro(name='client', secret='test')
asyncoro.Coro(rci_test)
