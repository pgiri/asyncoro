#!/usr/bin/env python

# client sends messages to a remote coroutine
# use with its server 'remote_coro_server.py'

import sys
# import disasyncoro to use distributed version of AsynCoro
import asyncoro.disasyncoro as asyncoro

def sender(coro=None):
    # if server is in remote network, add it; set 'stream_send' to
    # True for streaming messages to it
    # yield scheduler.peer('remote.peer.ip', stream_send=True)
    rcoro = yield asyncoro.Coro.locate('server_coro')
    print('server is at %s' % rcoro.location)
    for x in range(10):
        rcoro.send('message %s' % x)

asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
# scheduler = asyncoro.AsynCoro(secret='key')
asyncoro.Coro(sender)
