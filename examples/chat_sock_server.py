#!/usr/bin/env python

# chat server; must be used with 'chat_sock_client.py'

import sys, socket, collections
# messages are sent as strings, instead of bytes, so this program
# needs to be modified to work with Python 3
import asyncoro

def client_proc(conn, addr, server, coro=None):
    # given conn is synchronous, convert it to asynchronous
    conn = asyncoro.AsyncSocket(conn)
    server.send(('joined'.encode(), (conn, addr)))

    while True:
        line = yield conn.recv_msg()
        if not line:
            server.send(('left'.encode(), (conn, addr)))
            break
        server.send(('broadcast'.encode(), (conn, line)))

def server_proc(coro=None):
    clients = set()
    while True:
        cmd, item = yield coro.receive()
        cmd = cmd.decode()
        if cmd == 'broadcast':
            conn, msg = item
            msg = ('%s says: %s' % (conn.fileno(), msg.decode())).encode()
            for client in clients:
                if conn != client:
                    yield client.send_msg(msg)
        elif cmd == 'joined':
            conn, addr = item
            yield conn.send_msg(('id: %s' % conn.fileno()).encode())
            msg = ('%s joined' % conn.fileno()).encode()
            for client in clients:
                yield client.send_msg(msg)
            clients.add(conn)
        elif cmd == 'left':
            conn, addr = item
            clients.discard(conn)
            msg = ('%s left' % conn.fileno()).encode()
            conn.close()
            for client in clients:
                yield client.send_msg(msg)
        elif cmd == 'terminate':
            break

if __name__ == '__main__':
    asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    # host name or IP address of this node is arg1
    if len(sys.argv) > 1:
        host = sys.argv[1]
    else:
        host = ''

    # port to use is arg2
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    else:
        port = 1234

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(128)

    asyncoro.logger.info('server at %s', str(sock.getsockname()))
    server = asyncoro.Coro(server_proc)
    try:
        while True:
            conn, addr = sock.accept()
            # each connection is handled by a coroutine
            asyncoro.Coro(client_proc, conn, addr, server)
    except:
        pass
    server.send(('terminate'.encode(), None))
