# UDP clients and server.

# Note that UDP packets may be lost, in which case server may not
# receive all requests (and program may need to be terminated
# explicitly).

import sys, socket
import asyncoro

def server_proc(n, sock, coro=None):
    for i in range(n):
        msg, addr = yield sock.recvfrom(1024)
        print('Received "%s" from %s:%s' % (msg, addr[0], addr[1]))
    sock.close()

def client_proc(host, port, coro=None):
    sock = asyncoro.AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
    msg = 'client socket: %s' % (sock.fileno())
    if sys.version_info.major >= 3:
        msg = bytes(msg, 'ascii')
    yield sock.sendto(msg, (host, port))
    sock.close()

if __name__ == '__main__':
    sock = asyncoro.AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
    sock.bind(('127.0.0.1', 0))
    host, port = sock.getsockname()

    n = 50
    server_coro = asyncoro.Coro(server_proc, n, sock)
    for i in range(n):
        asyncoro.Coro(client_proc, host, port)
    server_coro.value()
