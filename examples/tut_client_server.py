#!/usr/bin/env python

# client and server coroutines communicating with message passing
# (asynchronous concurrent programming);
# see http://asyncoro.sourceforge.net/tutorial.html for details.

import sys, random
import asyncoro

def server_proc(coro=None):
    coro.set_daemon()
    while True:
        msg = yield coro.receive()
        print('processing %s' % (msg))

msg_id = 0

def client_proc(server, n, coro=None):
    global msg_id
    for x in range(3):
        yield coro.suspend(random.uniform(0.5, 3))
        msg_id += 1
        server.send('%d: %d / %d' % (msg_id, n, x))

# create server
server = asyncoro.Coro(server_proc)
# create 10 clients
for i in range(10):
    asyncoro.Coro(client_proc, server, i)
