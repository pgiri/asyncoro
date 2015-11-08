#!/usr/bin/env python

import asyncoro, socket, logging, sys, time

def client_recv(conn, sender, coro=None):
    while True:
        line = yield conn.recv_msg()
        if not line:
            sender.terminate()
            break
        print(line.decode())

def client_send(conn, coro=None):
    thread_pool = asyncoro.AsyncThreadPool(1)
    while True:
        line = yield thread_pool.async_task(sys.stdin.readline)
        if not line:
            break
        yield conn.send_msg(line[:-1].encode())

if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 1234))
    conn = asyncoro.AsynCoroSocket(sock)
    sender = asyncoro.Coro(client_send, conn)
    recvr = asyncoro.Coro(client_recv, conn, sender)
    sender.value()
    recvr.terminate()
