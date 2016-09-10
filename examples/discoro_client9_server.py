# Run 'discoronode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This example illustrates in-memory processing with 'proc_available' to read
# date in to memory by each (remote) server process. Remote coroutines
# ('compute' in this case) then process data in memory. This example works with
# POSIX (Linux, OS X etc.) and Windows. Note that, as data is read in to each
# server process, a node may have multiple copies of data in memory of each
# process on that node, so this approach is not practical / efficient when data
# is large. See 'discoro_client9_node.py' which uses 'node_available' and
# 'node_setup' to read data in to memory at node (and thus only one copy is in
# memory).

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *
from asyncoro.discoro_schedulers import RemoteCoroScheduler

def proc_available(location, coro=None):
    # 'proc_available' is executed locally (at client) when a server process is
    # available. 'location' is Location instance of server. When this coroutine
    # is executed, 'depends' of computation would've been transferred.

    # In this case, 'setup_server' is executed at remote server process to read
    # the data in given file (transferred by client) in to memory (global
    # variable). 'compute' then uses the data in memory instead of reading from
    # file every time.
    def setup_server(data_file, coro=None):  # executed on remote server
        # variables declared as 'global' will be available in coroutines
        global hashlib, data
        import os, hashlib
        with open(data_file, 'rb') as fd:
            data = fd.read()
        os.remove(data_file)  # data_file is not needed anymore
        # generator functions must have at least one 'yield'
        yield 0 # indicate successful initialization with exit value 0

    # rest is executed locally
    import os
    # data_file could've been sent with the computation 'depends'; however, to
    # illustrate how files can be sent separately, file is transferred during
    # setup. Different servers can also be sent different files, for example, to
    # distribute the data among servers.
    if (yield asyncoro.AsynCoro().send_file(location, data_file, timeout=5)) < 0:
        print('Could not send data file "%s" to %s' % (data_file, location))
        raise StopIteration(-1)

    # run 'setup_server' to read file in to memory (on remote server)
    if (yield rcoro_scheduler.execute_at(location, setup_server, os.path.basename(data_file))) != 0:
        print('Could not setup %s, %s' % (data_file, location))
        raise StopIteration(-1)
    raise StopIteration(0)

def proc_close(status, location, coro=None):
    # 'proc_close' is executed locally (at client) when either server process is
    # being closed (when computation is closed), in which case 'status' would be
    # 'ServerInitialized', or server process is closed as an exception (e.g.,
    # due to network failures no communication took place for zombie_period
    # seconds, server was closed/terminated manually etc.), in which case
    # 'status' would be 'ServerClosed'.

    # 'cleanup_server' is executed at remote server process to cleanup
    # (delete global variables initialized in 'setup_server' in this case)
    def cleanup_server(coro=None):  # executed on remote server
        global hashlib, data
        del hashlib, data
        yield 0  # generator functions should have at least one 'yield'

    if status == Scheduler.ServerInitialized:
        yield rcoro_scheduler.execute_at(location, cleanup_server)

# 'compute' is executed at remote server process repeatedly to compute
# checksum of data in memory, initialized by 'setup_server'
def compute(alg, n, coro=None):
    global data, hashlib
    yield coro.sleep(n)
    checksum = getattr(hashlib, alg)()
    checksum.update(data)
    raise StopIteration((alg, checksum.hexdigest()))


def client_proc(computation, coro=None):
    # execute 10 jobs (coroutines) and get their results. Note that
    # number of jobs created can be more than number of server
    # processes available; the scheduler will use as many processes as
    # necessary/available, running one job at a server process
    algorithms = ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512']
    args = [(algorithms[i % len(algorithms)], random.uniform(1, 3)) for i in range(10)]
    results = yield rcoro_scheduler.map_results(compute, args)
    for i, result in enumerate(results):
        if isinstance(result, tuple) and len(result) == 2:
            print('    %ssum: %s' % (result[0], result[1]))
        else:
            print('  rcoro failed for %s: %s' % (args[i][0], str(result)))

    yield rcoro_scheduler.finish(close=True)


if __name__ == '__main__':
    import random, functools, sys
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    Scheduler()
    data_file = sys.argv[0] if len(sys.argv) == 1 else sys.argv[1]

    # send 'compute' generator function; data_file can also be sent with
    # 'depends', but in this case, the client sends it separately when server is
    # initialized (to illustrate how client can transfer files).
    computation = Computation([compute])
    # Use RemoteCoroScheduler to run at most one coroutine at a server process
    # This should be created before scheduling computation
    rcoro_scheduler = RemoteCoroScheduler(computation, proc_available=proc_available,
                                          proc_close=proc_close)
    asyncoro.Coro(client_proc, computation)
