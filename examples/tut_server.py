# server program for processing requests received with message passing
# (asynchronous concurrent programming) from remote client
# (tut_client.py) on same network;
# see http://asyncoro.sourceforge.net/tutorial.html for details.

import sys
import asyncoro.disasyncoro as asyncoro

def server_proc(coro=None):
    coro.set_daemon()
    coro.register('server_coro')
    while True:
        msg = yield coro.receive()
        print('processing %s' % (msg))

asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
scheduler = asyncoro.AsynCoro(udp_port=0)
server = asyncoro.Coro(server_proc)
if sys.version_info.major > 2:
    read_input = input
else:
    read_input = raw_input
while True:
    try:
        cmd = read_input().strip().lower()
        if cmd in ('quit', 'exit'):
            break
    except:
        break
