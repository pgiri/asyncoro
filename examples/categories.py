"""Simple example demonstrating categorizing messages to coroutines.
"""

import sys, random, logging
import asyncoro

def client_proc(server, coro=None):
    for i in range(12):
        asyncoro.logger.debug('sending "%s"' % i)
        server.send(i)
        yield coro.sleep(random.uniform(0, 2))
    server.send('done')

def server_proc(coro=None):
    def even(msg):
        if msg % 2 == 0:
            return 'even'
        return None

    yield coro.sleep(3)
    # add 'even' method to categorize even numbers
    categories = asyncoro.CategorizeMessages(coro)
    categories.add(even)
    while True:
        # if messages arrive (or already queued) with 'even' category
        # within 1 second, receive them
        msg = yield categories.receive('even', timeout=1)
        if msg is None:
            # otherwise receive any message (without waiting, i.e.,
            # already queued)
            msg = yield categories.receive(timeout=0)
            asyncoro.logger.debug('         message: %s' % msg)
        else:
            asyncoro.logger.debug('    even message: %s' % msg)
            if msg == 8:
                # remove 'even' category, so future messages will no
                # longer be in 'even' category; note that there may
                # already be messages in 'even' category, so process
                # them if necessary
                categories.remove(even)
        if msg == 'done':
            break

if __name__ == '__main__':
    asyncoro.logger.setLevel(logging.DEBUG)
    server = asyncoro.Coro(server_proc)
    client = asyncoro.Coro(client_proc, server)
