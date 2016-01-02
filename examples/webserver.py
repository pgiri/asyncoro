#!/usr/bin/env python

# Ping Pong Benchmark server used at http://nichol.as/asynchronous-servers-in-python

# test with httperf (adjust num-conns appropriately):
# httperf --timeout=1 --client=0/1 --server=localhost --port=8010 --uri=/ \
#     --rate=400 --send-buffer=4096 --recv-buffer=16384 --num-calls=1 --num-conns=2000

import sys, socket
import asyncoro

def process(conn, coro=None):
    msg = "HTTP/1.0 200 OK\r\nContent-Length: 5\r\n\r\nPong!\r\n"
    if sys.version_info.major >= 3:
        msg = bytes(msg, 'ascii')
    yield conn.sendall(msg)
    conn.close()

def server(host, port, coro=None):
    coro.set_daemon()
    sock = asyncoro.AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(5000)

    while True:
        conn, addr = yield sock.accept()
        asyncoro.Coro(process, conn)

if __name__ == '__main__':
    asyncoro.Coro(server, '127.0.0.1', 8010)

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
