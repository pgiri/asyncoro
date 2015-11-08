# use with 'hotswap.py'

# 'server_proc2' replaces 'server' coroutine's function
def server_proc2(fname, coro=None):
    import math
    func = getattr(math, fname)
    coro.set_daemon()
    coro.hot_swappable(True)
    while True:
        try:
            n = yield coro.receive()
            print('  * %s received: %s,  %s=%.2f' % (coro, n, fname, func(n)))
        except asyncoro.HotSwapException as exc:
            gen = exc.args[0]
            if gen.__name__.startswith('server_proc'):
                print('\nreplacing with function: %s, %s\n' % \
                      (gen.__name__, gen.gi_code.co_argcount))
                raise
            else:
                print('\n** ignoring hot swap function %s' % (gen.__name__))

# 'client_proc2' replaces 'client' coroutine's function
def client_proc2(server, i=42, coro=None):
    coro.set_daemon()
    coro.hot_swappable(True)
    while True:
        print('  * %s: sending %s' % (coro, i))
        server.send(i)
        i += 2
        yield coro.sleep(random.uniform(2, 6))

