# Run 'discoronode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This example illustrates in-memory processing with 'node_setup' to read date
# in to memory. Remote coroutines ('compute' in this case) then process data in
# memory. This example works with POSIX (Linux, OS X etc.), but not Windows. See
# 'discoro_client9_server.py' which uses 'proc_available' to read data in to
# memory by each remote server process.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *
from asyncoro.discoro_schedulers import RemoteCoroScheduler

def node_available(avail_info, coro=None):
    import os
    # 'node_available' is executed locally (at client) when a node is
    # available. 'location' is Location instance of node. When this coroutine
    # is executed, 'depends' of computation would've been transferred.

    # data_file could've been sent with the computation 'depends'; however, to
    # illustrate how files can be sent separately (e.g., to transfer different
    # files to different nodes), file is transferred with
    # 'node_available'.
    if (yield asyncoro.AsynCoro().send_file(avail_info.location, data_file, timeout=5)) < 0:
        print('Could not send data file "%s" to %s' % (data_file, avail_info.location))
        raise StopIteration(-1)

    # value of coroutine (last value yield'ed or value of 'raise StopIteration')
    # will be passed to node_setup as argument(s).
    yield (os.path.basename(data_file),)

def node_setup(data_file):
    # 'node_setup' is executed on a node with the arguments returned by
    # 'node_available'. This coroutine should return 0 to indicate successful
    # initialization.

    # variables declared as 'global' will be available in coroutines
    global os, hashlib, data
    import os, hashlib
    with open(data_file, 'rb') as fd:
        data = fd.read()
    os.remove(data_file)  # data_file is not needed anymore
    yield 0  # coroutine must have at least one 'yield' and 0 indicates success

# 'compute' is executed at remote server process repeatedly to compute checksum
# of data in memory, initialized by 'node_setup'
def compute(alg, n, coro=None):
    global data, hashlib
    yield coro.sleep(n)
    checksum = getattr(hashlib, alg)()
    checksum.update(data)
    raise StopIteration((alg, checksum.hexdigest()))


def client_proc(computation, coro=None):

    # execute 10 jobs (coroutines) and get their results. Note that number of
    # jobs created can be more than number of server processes available; the
    # scheduler will use as many processes as necessary/available, running one
    # job at a server process
    algorithms = ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512']
    args = [(algorithms[i % len(algorithms)], random.uniform(1, 3)) for i in range(20)]
    results = yield rcoro_scheduler.map_results(compute, args)
    for i, result in enumerate(results):
        if isinstance(result, tuple) and len(result) == 2:
            print('    %ssum: %s' % (result[0], result[1]))
        else:
            print('  rcoro failed for %s: %s' % (args[i][0], str(result)))

    yield rcoro_scheduler.finish(close=True)


if __name__ == '__main__':
    import logging, random, functools, sys, os
    # asyncoro.logger.setLevel(logging.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    Scheduler()
    data_file = sys.argv[0] if len(sys.argv) == 1 else sys.argv[1]
    data_file = os.path.relpath(data_file)

    # send 'compute' generator function; data_file can also be sent with
    # 'depends', but in this case, the client sends it separately when node is
    # available (to illustrate how client can transfer files).

    # Since this example doesn't work with Windows, 'node_allocations' feature
    # is used to filter out nodes running Windows.
    node_allocations = [DiscoroNodeAllocate(node='*', platform='Windows', cpus=0)]
    computation = Computation([compute], node_available=node_available, node_setup=node_setup,
                              node_allocations=node_allocations)
    # Use RemoteCoroScheduler to run at most one coroutine at a server process
    # This should be created before scheduling computation
    rcoro_scheduler = RemoteCoroScheduler(computation)
    asyncoro.Coro(client_proc, computation)
