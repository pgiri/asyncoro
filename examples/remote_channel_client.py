#!/usr/bin/env python

# example where client and server communicate through a channel.
# use with its server 'remote_channel_server.py'

import sys
# import disasyncoro to use distributed version of AsynCoro
import asyncoro.disasyncoro as asyncoro

def sender_proc(rchannel, coro=None):
    # send messages to channel; 'deliver' is used with n=2, so
    # messages will not be sent until both receivers subscribe to
    # channel
    for x in range(10):
        msg = {'msg':'message %s' % x, 'sender':coro}
        n = yield rchannel.deliver(msg, n=2)
        print('delivered to: %s' % n)
    rchannel.send(None)

def receiver_proc2(coro=None):
    # if server is in remote network, add it explicitly
    # yield scheduler.peer('remote.peer.ip', stream_send=True)
    # get reference to remote channel in server
    rchannel = yield asyncoro.Channel.locate('2clients')
    # this coro subscribes to the channel, so gets messages to server
    # channel
    print('server is at %s' % rchannel.location)
    if (yield rchannel.subscribe(coro)) != 0:
        raise Exception('subscription failed')
    asyncoro.Coro(sender_proc, rchannel)
    while True:
        msg = yield coro.receive()
        if msg is None:
            break
        print('Received "%s" from %s at %s' % \
              (msg['msg'], msg['sender'].name, msg['sender'].location))
    yield rchannel.unsubscribe(coro)

asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
# scheduler = asyncoro.AsynCoro()
asyncoro.Coro(receiver_proc2)
