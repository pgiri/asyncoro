#!/usr/bin/env python

# run at least two instances of this program on either same node or
# multiple nodes on local network, along with 'chat_chan_server.py';
# text typed in a client is broadcast over a channel to all clients

import sys, logging
import asyncoro.disasyncoro as asyncoro

def recv_proc(client_id, coro=None):
    coro.set_daemon()
    while True:
        msg, who = yield coro.receive()
        if who == client_id:
            continue
        print('    %s %s' % (who, msg))

def send_proc(coro=None):
    # if server is in a remote network, use 'peer' as (optionally
    # enabling streaming for efficiency):
    # yield asyncoro.AsynCoro.instance().peer('server node/ip')
    server = yield asyncoro.Coro.locate('server')
    yield server.send(('join', coro))
    client_id = yield coro.receive()
    
    # channel is at same location as server coroutine
    channel = yield asyncoro.Channel.locate('channel', server.location)
    recv_coro = asyncoro.Coro(recv_proc, client_id)
    yield channel.subscribe(recv_coro)
    # since readline is synchronous (blocking) call, use async thread
    async_threads = asyncoro.AsyncThreadPool(1)
    while True:
        msg = yield async_threads.async_task(sys.stdin.readline)
        msg = msg.strip()
        if msg == 'quit':
            break
        channel.send((msg, client_id))
    server.send(('quit', client_id))
    yield channel.unsubscribe(recv_coro)

if __name__ == '__main__':
    # asyncoro.logger.setLevel(logging.DEBUG)
    asyncoro.Coro(send_proc)
