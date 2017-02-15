# Run 'discoronode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This example illustrates in-memory processing with 'server_available' to read
# date in to memory by each (remote) server process. Remote coroutines
# ('compute' in this case) then process data in memory. This example works with
# POSIX (Linux, OS X etc.) and Windows. Note that, as data is read in to each
# server process, a node may have multiple copies of same data in memory of each
# process on that node, so this approach is not practical / efficient when data
# is large. See 'discoro_client9_node.py' which uses 'node_available' and
# 'node_setup' to read data in to memory at node (and thus only one copy is in
# memory).

# In this example different files are sent to remote servers to compute checksum
# of their data (thus there is no duplicate data in servers at a node in this
# case).

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *

def server_available(location, data_file, coro=None):
    import os
    # 'server_available' is executed locally (at client) when a server process
    # is available. 'location' is Location instance of server. When this
    # coroutine is executed, 'depends' of computation would've been transferred.
    # data_file could've been sent with the computation 'depends'; however, to
    # illustrate how files can be sent separately (to distribute data fragments
    # among servers), files are transferred to servers in this
    # example
    print('sending %s to %s' % (data_file, location))
    if (yield asyncoro.AsynCoro().send_file(location, data_file, timeout=5, overwrite=True)) < 0:
        print('Could not send data file "%s" to %s' % (data_file, location))
        raise StopIteration(-1)

    # 'setup_server' is executed on remote server at 'location' with argument
    # data_file
    yield computation.enable_server(location, data_file)
    raise StopIteration(0)

# 'setup_server' is executed at remote server process to read the data in given
# file (transferred by client) in to memory (global variable). 'compute' then
# uses the data in memory instead of reading from file every time.
def setup_server(data_file, coro=None):  # executed on remote server
    # variables declared as 'global' will be available in coroutines for
    # read/write to all computations on a server.
    global hashlib, data, file_name
    import os, hashlib
    file_name = data_file
    print('%s processing %s' % (coro.location, data_file))
    # note that files transferred to server are in the directory where
    # computations are executed (cf 'node_setup' in discoro_client9_node.py)
    with open(data_file, 'rb') as fd:
        data = fd.read()
    os.remove(data_file)  # data_file is not needed anymore
    # generator functions must have at least one 'yield'
    yield 0 # indicate successful initialization with exit value 0

# 'compute' is executed at remote server process repeatedly to compute
# checksum of data in memory, initialized by 'setup_server'
def compute(alg, n, coro=None):
    global data, hashlib, file_name
    yield coro.sleep(n)
    checksum = getattr(hashlib, alg)()
    checksum.update(data)
    raise StopIteration((file_name, alg, checksum.hexdigest()))


# local coroutine to process status messages from scheduler
def status_proc(coro=None):
    coro.set_daemon()
    i = 0
    while 1:
        msg = yield coro.receive()
        if not isinstance(msg, DiscoroStatus):
            continue
        if msg.status == Scheduler.ServerDiscovered:
            asyncoro.Coro(server_available, msg.info, data_files[i])
            i += 1

def client_proc(computation, coro=None):
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

    # execute 10 jobs (coroutines) and get their results. Note that
    # number of jobs created can be more than number of server
    # processes available; the scheduler will use as many processes as
    # necessary/available, running one job at a server process
    algorithms = ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512']
    args = [(algorithms[i % len(algorithms)], random.uniform(5, 10)) for i in range(15)]
    results = yield computation.run_results(compute, args)
    for i, result in enumerate(results):
        if isinstance(result, tuple) and len(result) == 3:
            print('    %ssum for %s: %s' % (result[1], result[0], result[2]))
        else:
            print('  rcoro failed for %s: %s' % (args[i][0], str(result)))

    yield computation.close()


if __name__ == '__main__':
    import random, functools, sys, os, glob
    asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    if os.path.dirname(sys.argv[0]):
        os.chdir(os.path.dirname(sys.argv[0]))
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    Scheduler()

    data_files = glob.glob('discoro_client*.py')

    # send 'compute' generator function; the client sends data files when server
    # is discovered (to illustrate how client can distribute data).
    computation = Computation([compute], status_coro=asyncoro.Coro(status_proc),
                              disable_servers=True, server_setup=setup_server)
    asyncoro.Coro(client_proc, computation)
