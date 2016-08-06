#!/usr/bin/env python

# client program for sending requests to server (tut_sock_server.py)
# with sockets (asynchronous network programming);
# see http://asyncoro.sourceforge.net/tutorial.html for details.

import sys, socket, random
import asyncoro

def client(host, port, n, coro=None):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock = asyncoro.AsyncSocket(sock)
    yield sock.connect((host, port))
    msg = '%d: ' % n + '-' * random.randint(100,300) + '/'
    msg = msg.encode()
    yield sock.sendall(msg)
    sock.close()

# run 10 client coroutines
n = 10 if len(sys.argv) < 2 else int(sys.argv[1])
for i in range(n):
    asyncoro.Coro(client, '127.0.0.1', 8010, i)
