# client program for sending requests to remote server (tut_server.py)
# using message passing (asynchronous concurrent programming);
# see http://asyncoro.sourceforge.net/tutorial.html for details.

import sys, random
import asyncoro.disasyncoro as asyncoro

def client_proc(n, coro=None):
    global msg_id
    server = yield asyncoro.Coro.locate('server_coro')
    for x in range(3):
        # yield coro.suspend(random.uniform(0.5, 3))
        msg_id += 1
        server.send('%d: %d / %d' % (msg_id, n, x))

msg_id = 0
asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
scheduler = asyncoro.AsynCoro(udp_port=0)
# create 10 clients; each client sends 3 messages
for i in range(10):
    asyncoro.Coro(client_proc, i)
