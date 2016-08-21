# Run 'discoronode.py' program on Amazon EC2 cloud computing
# and run this program on local computer.

# in this example, ssh is used for port forwarding
# make sure EC2 instance allows inbound TCP port 51347 (and any additional ports,
# depending on how many CPUs are used by discoronode)
# assume '54.204.242.185' is external IP address of EC2 instance;
# login to that node with the PEM key:
# 'ssh -i my-key.pem 4567:127.0.0.1:4567 54.204.242.185'
# run discoronode on EC2 at port (starting with) 51347 with:
# 'discoronode.py -d --ext_ip_addr 54.204.242.185 --tcp_ports 51347'

# Distributed computing example where this client sends computation to
# remote discoro process to run as remote coroutines. At any time at
# most one computation coroutine is scheduled at a process (due to
# RemoteCoroScheduler). This example shows how to use 'execute' method of
# RemoteCoroScheduler to submit comutations and get their results easily.

# This example can be combined with in-memory processing (see
# 'discoro_client5.py') and streaming (see 'discoro_client6.py') for
# efficient processing of data and communication.

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import *
from asyncoro.discoro_schedulers import RemoteCoroScheduler


# this generator function is sent to remote discoro servers to run
# coroutines there
def compute(n, coro=None):
    import time
    yield coro.sleep(n)
    raise StopIteration(time.asctime()) # result of 'compute' is current time


def client_proc(computation, n, coro=None):
    # pair EC2 node with this client with:
    yield asyncoro.AsynCoro().peer(asyncoro.Location('54.204.242.185', 51347))
    # if multiple nodes are used, 'broadcast' option can be used to pair with
    # all nodes with just one statement as:
    # yield asyncoro.AsynCoro().peer(asyncoro.Location('54.204.242.185', 51347), broadcast=True)

    # coroutine to call RemoteCoroScheduler's "execute" method
    def exec_proc(gen, *args, **kwargs):
        # execute computation; result of computation is result of
        # 'yield' which is also result of this coroutine (obtained
        # with 'finish' method below)
        yield job_scheduler.execute(gen, *args, **kwargs)
        # results can be processed here (as they become available), or
        # await in sequence as done below

    # Use RemoteCoroScheduler to run at most one coroutine at a server process
    # This should be created before scheduling computation
    job_scheduler = RemoteCoroScheduler(computation)

    # execute n jobs (coroutines) and get their results. Note that
    # number of jobs created can be more than number of server
    # processes available; the scheduler will use as many processes as
    # necessary/available, running one job at a server process
    jobs = [asyncoro.Coro(exec_proc, compute, random.uniform(3, 10)) for _ in range(n)]
    for job in jobs:
        print('result: %s' % (yield job.finish()))

    yield job_scheduler.finish(close=True)


if __name__ == '__main__':
    import sys, random
    asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    asyncoro.AsynCoro(node='127.0.0.1', tcp_port=4567)
    n = 10 if len(sys.argv) == 1 else int(sys.argv[1])
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    Scheduler()
    # send 'compute' generator function
    computation = Computation([compute])
    asyncoro.Coro(client_proc, computation, n)
