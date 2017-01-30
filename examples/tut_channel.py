# program for broadcasting messages over channel; see
# http://asyncoro.sourceforge.net/tutorial.html for details.

import sys, random
import asyncoro

def seqsum(coro=None):
    # compute sum of numbers received over channel
    result = 0
    while True:
        msg = yield coro.receive()
        if msg is None:
            break
        result += msg
    print('sum: %f' % result)

def seqprod(coro=None):
    # compute product of numbers received over channel
    result = 1
    while True:
        msg = yield coro.receive()
        if msg is None:
            break
        result *= msg
    print('prod: %f' % result)

def client_proc(coro=None):
    # create channel
    channel = asyncoro.Channel('sum_prod')
    # create coroutines to compute sum and product of numbers sent
    sum_coro = asyncoro.Coro(seqsum)
    prod_coro = asyncoro.Coro(seqprod)
    # subscribe coroutines to channel so they receive messages
    yield channel.subscribe(sum_coro)
    yield channel.subscribe(prod_coro)
    # send 4 numbers to channel
    for x in range(4):
        r = random.uniform(0.5, 3)
        channel.send(r)
        print('sent %f' % r)
    # send None to indicate end of data
    channel.send(None)
    yield channel.unsubscribe(sum_coro)
    yield channel.unsubscribe(prod_coro)

asyncoro.Coro(client_proc)
