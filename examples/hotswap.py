# Demonstratiion of hot swap feature

# when 'client' command is given, the function of coroutine 'client'
# ('client_proc') is replaced with 'client_proc2' function in the file
# 'hotswap_funcs.py'; similarly, if 'server' command is given,
# server's function is replaced

import sys, random
import asyncoro

def server_proc(coro=None):
    coro.set_daemon()
    # indicate that this function can be swapped
    coro.hot_swappable(True)
    while True:
        try:
            n = yield coro.receive()
            print('%s received: %s' % (coro, n))
        except asyncoro.HotSwapException as exc:
            func = exc.args[0]
            print('\nnew function: %s, %s\n' % (func.__name__, func.gi_code.co_argcount))
            # replace only if new function meets certain criteria
            if func.__name__.startswith('server_proc'):
                raise
            else:
                print('\n** ignoring hot swap function %s' % (func.__name__))

def client_proc(server, i=1, coro=None):
    coro.set_daemon()
    coro.hot_swappable(True)
    # client doesn't process HotswapException
    while True:
        print('%s: sending %s' % (coro, i))
        server.send(i)
        i += 1
        yield coro.sleep(random.uniform(1, 3))

def swap(func_name, file_name, coro, *args, **kwargs):
    try:
        exec(open(file_name).read())
        func = locals()[func_name]
        coro.hot_swap(func, *args, **kwargs)
    except:
        print('failed to load "%s" from "%s"' % (func_name, file_name))

if __name__ == '__main__':
    asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    server = asyncoro.Coro(server_proc)
    client = asyncoro.Coro(client_proc, server)
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input
    while True:
        try:
            cmd = read_input().strip().lower()
            if cmd.startswith('client'):
                swap('client_proc2', 'hotswap_funcs.py', client, server)
            elif cmd.startswith('server'):
                swap('server_proc2', 'hotswap_funcs.py', server, random.choice(['log', 'sqrt']))
            elif cmd in ('quit', 'exit'):
                break
        except:
            break
