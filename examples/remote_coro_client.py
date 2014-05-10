#!/usr/bin/env python

# client sends messages to a remote coroutine
# use with its server 'remote_coro_server.py'

import sys, logging
if sys.version_info.major >= 3:
    import disasyncoro3 as asyncoro
else:
    import disasyncoro as asyncoro

def sender(coro=None):
    # if server is in remote network, add it; set 'stream_send' to
    # True for streaming messages to it
    # yield scheduler.peer('remote.peer.ip', stream_send=True)
    rcoro = yield asyncoro.Coro.locate('server_coro')
    for x in range(10):
        rcoro.send('message %s' % x)

asyncoro.logger.setLevel(logging.DEBUG)
# call with 'udp_port=0' to start network services
scheduler = asyncoro.AsynCoro(udp_port=0, secret='key')
asyncoro.Coro(sender)
