#!/usr/bin/env python

# program for creating coroutines (asynchronous concurrent
# programming); see http://asyncoro.sourceforge.net/tutorial.html for
# details.

import sys, random, time
import asyncoro

def coro_proc(n, coro=None):
    s = random.uniform(0.5, 3)
    print('%f: coroutine %d sleeping for %f seconds' % (time.time(), n, s))
    yield coro.sleep(s)
    print('%f: coroutine %d terminating' % (time.time(), n))

# create 10 clients
for i in range(10):
    asyncoro.Coro(coro_proc, i)
