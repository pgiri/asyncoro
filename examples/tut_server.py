# server program for processing requests received with message passing
# (asynchronous concurrent programming) from remote client
# (tut_client.py) on same network;
# see http://asyncoro.sourceforge.net/tutorial.html for details.

import sys, random, logging
import asyncoro.disasyncoro as asyncoro

def server_proc(coro=None):
    coro.set_daemon()
    coro.register('server_coro')
    for x in range(10):
        msg = yield coro.receive()
        print('processing %s' % (msg))
    coro.unregister('server_coro')

asyncoro.logger.setLevel(logging.DEBUG)
scheduler = asyncoro.AsynCoro(udp_port=0)
server = asyncoro.Coro(server_proc)
while True:
    cmd = sys.stdin.readline().strip().lower()
    if cmd == 'quit' or cmd == 'exit':
        break
