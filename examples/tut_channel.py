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
    channel = asyncoro.Channel('sum_prod')
    sum_coro = asyncoro.Coro(seqsum)
    prod_coro = asyncoro.Coro(seqprod)
    yield channel.subscribe(sum_coro)
    yield channel.subscribe(prod_coro)
    for x in range(4):
        r = random.uniform(0.5, 3)
        channel.send(r)
        print('sent %f' % r)
    channel.send(None)
    yield channel.unsubscribe(sum_coro)
    yield channel.unsubscribe(prod_coro)

asyncoro.Coro(client_proc)
