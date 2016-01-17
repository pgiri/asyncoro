#!/usr/bin/env python

# server program for client (tut_sock_client.py) sending requests with
# sockets (asynchronous network programming);
# see http://asyncoro.sourceforge.net/tutorial.html for details.

# run this program and then client either on same node. If they are on
# different computers, 'host' address must be changed appropriately.

import sys, socket
import asyncoro

def process(conn, coro=None):
    global n
    if sys.version_info.major >= 3:
        eol = ord('/')
    else:
        eol = '/'
    data = ''.encode()
    while True:
        data += yield conn.recv(128)
        if data[-1] == eol:
            break
    conn.close()
    n += 1
    print('recieved: %s' % data)

def server(host, port, coro=None):
    coro.set_daemon()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock = asyncoro.AsyncSocket(sock)
    # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(128)

    while True:
        conn, addr = yield sock.accept()
        asyncoro.Coro(process, conn)

n = 0
asyncoro.Coro(server, '127.0.0.1', 8010)

if sys.version_info.major > 2:
    read_input = input
else:
    read_input = raw_input
while True:
    cmd = read_input().strip().lower()
    if cmd == 'exit' or cmd == 'quit':
        break
print('n = %d' % n)
