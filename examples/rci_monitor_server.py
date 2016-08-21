#!/usr/bin/env python

# server program for client sending requests to execute coroutines

# run this program and then client either on same node or different
# node on local network. Server and client can also be run on two
# different networks but client must call 'scheduler.peer' method
# appropriately.

import sys
# import disasyncoro to use distributed version of AsynCoro
import asyncoro.disasyncoro as asyncoro

def rci_1(a, b=1, coro=None):
    asyncoro.logger.debug('running %s/%s with %s, %s', coro.name, id(coro), a, b)
    msg = yield coro.receive()
    if b % 2 == 0:
        yield coro.sleep(b)
        asyncoro.logger.debug('%s/%s done', coro.name, id(coro))
        # (remote) monitor (if any) gets this exception (to be
        # interpreted as normal termination)
        raise StopIteration(msg)
    else:
        # (remote) monitor (if any) gets this exception, too
        raise Exception('invalid invocation: %s' % b)

asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
# 'secret' is set so only peers that use same secret can communicate
scheduler = asyncoro.AsynCoro(name='server', secret='test')
# register rci_1 so remote clients can request execution
rci1 = asyncoro.RCI(rci_1)
rci1.register()

if sys.version_info.major > 2:
    read_input = input
else:
    read_input = raw_input
while True:
    try:
        line = read_input().strip().lower()
        if line in ('quit', 'exit'):
            break
    except:
        break
