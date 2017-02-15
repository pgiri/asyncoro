# Run 'discoronode.py' program on Amazon EC2 cloud computing and run this
# program on local computer.

# in this example, ssh is used for port forwarding make sure EC2 instance allows
# inbound TCP port 51347 (and any additional ports, depending on how many CPUs
# are used by discoronode) assume '54.204.242.185' is external IP address of EC2
# instance; login to that node with the PEM key: 'ssh -i my-key.pem
# 4567:127.0.0.1:4567 54.204.242.185' run discoronode on EC2 at port (starting
# with) 51347 with: 'discoronode.py -d --ext_ip_addr 54.204.242.185 --tcp_ports
# 51347'

# Distributed computing example where this client sends computation to remote
# discoro process to run as remote coroutines. At any time at most one
# computation coroutine is scheduled at a process (due to
# RemoteCoroScheduler). This example shows how to use 'execute' method of
# RemoteCoroScheduler to run comutations and get their results easily.

# This example can be combined with in-memory processing (see
# 'discoro_client9_node.py') and streaming (see 'discoro_client6.py') for
# efficient processing of data and communication.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *


# this generator function is sent to remote discoro servers to run
# coroutines there
def compute(n, coro=None):
    import time
    yield coro.sleep(n)
    raise StopIteration(time.asctime()) # result of 'compute' is current time


def client_proc(computation, njobs, coro=None):
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

    # pair EC2 node with this client with:
    yield asyncoro.AsynCoro().peer(asyncoro.Location('54.204.242.185', 51347))
    # if multiple nodes are used, 'broadcast' option can be used to pair with
    # all nodes with just one statement as:
    # yield asyncoro.AsynCoro().peer(asyncoro.Location('54.204.242.185', 51347), broadcast=True)

    # execute n jobs (coroutines) and get their results. Note that number of
    # jobs created can be more than number of server processes available; the
    # scheduler will use as many processes as necessary/available, running one
    # job at a server process
    args = [random.uniform(3, 10) for _ in range(njobs)]
    results = yield computation.run_results(compute, args)
    for result in results:
        print('result: %s' % result)

    yield computation.close()


if __name__ == '__main__':
    import sys, random
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    asyncoro.AsynCoro(node='127.0.0.1', tcp_port=4567)
    njobs = 10 if len(sys.argv) == 1 else int(sys.argv[1])
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    Scheduler()
    # send 'compute' generator function
    computation = Computation([compute])
    asyncoro.Coro(client_proc, computation, njobs)
