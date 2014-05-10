import sys, time

# program to test performance of creating many coroutines
# and message passing in local coroutines.
if sys.version_info.major > 2:
    import asyncoro3 as asyncoro
else:
    import asyncoro

def client_proc(i, server, coro=None):
    # wait until all processes are created
    yield coro.sleep(1)
    # each client sends 3 messages
    for j in range(2):
        server.send((i, j))
    server.send((i, None))

def server_proc(n, coro=None):
    k = 0
    while True:
        i, j = yield coro.receive()
        if j is None:
            # client 'i' is done
            k += 1
            if k == n:
                break

if __name__ == '__main__':
    if len(sys.argv) > 1:
        n = int(sys.argv[1])
    else:
        n = 10000

    start = time.time()
    server = asyncoro.Coro(server_proc, n)
    # create given number of client coroutines
    for i in range(n):
        asyncoro.Coro(client_proc, i, server)
    print('creation took %.3f sec' % (time.time() - start))
    proc_start = time.time()
    # wait for server to finish
    server.value()
    print('processing took %.3f sec' % (time.time() - proc_start))
