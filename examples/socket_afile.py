# Asynchronous file example. Works with Linux, OS X and other Unix
# variants, but not Windows, as in Windows sockets don't support file
# I/O for asynchronous I/O.

# The file descriptor is associated with socket. The client sends data
# in chunks and server reads lines from the data receivd from
# client. Both compute checksum to check that data is received
# correctly.

# argv[1] must be a text file

import socket, hashlib, sys, os
import asyncoro
import asyncoro.asyncfile

def client_proc(host, port, input, coro=None):
    # client reads input file and sends data in chunks
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock = asyncoro.AsyncSocket(sock)
    yield sock.connect((host, port))
    # data can be written to this asynchronous socket; however, for
    # illustration, convert its file descriptor to asynchronous file
    # and write to that instead
    afd = asyncoro.asyncfile.AsyncFile(sock)
    input = open(input)
    csum = hashlib.sha1()
    while True:
        data = os.read(input.fileno(), 16*1024)
        if not data:
            break
        csum.update(data)
        n = yield afd.write(data, full=True)
    afd.close()
    print('client sha1 csum: %s' % csum.hexdigest())

def server_proc(conn, coro=None):
    # conn is a synchronous socket (as it is obtained from synchronous
    # 'accept'); it's file-descriptor is converted to asynchronous
    # file to read data from that
    afd = asyncoro.asyncfile.AsyncFile(conn)
    csum = hashlib.sha1()
    nlines = 0
    while True:
        # read lines from data
        line = yield afd.readline()
        if not line:
            break
        csum.update(line)
        nlines += 1
    afd.close()
    print('server sha1 csum: %s' % (csum.hexdigest()))
    print('lines: %s' % (nlines))

asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(('localhost', 0))
sock.listen(5)
host, port = sock.getsockname()
print('host: %s, port: %s' % (host, port))

asyncoro.Coro(client_proc, host, port, sys.argv[1] if len(sys.argv) > 1 else sys.argv[0])

conn, addr = sock.accept()
asyncoro.Coro(server_proc, conn)
