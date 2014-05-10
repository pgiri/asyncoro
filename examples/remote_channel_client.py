#!/usr/bin/env python

# example where client and server communicate through a channel.
# use with its server

import sys, logging
if sys.version_info.major >= 3:
    import disasyncoro3 as asyncoro
else:
    import disasyncoro as asyncoro

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

asyncoro.logger.setLevel(logging.DEBUG)
# call with 'udp_port=0' to start network services
scheduler = asyncoro.AsynCoro(udp_port=0)
asyncoro.Coro(receiver_proc2)
