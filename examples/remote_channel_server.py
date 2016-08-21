#!/usr/bin/env python

# server program where client sends messages to this server
# using channel send/receive

# run this program and then client either on same node or different
# node on local network. Server and client can also be run on two
# different networks but client must call 'scheduler.peer' method
# appropriately.

import sys
# import disasyncoro to use distributed version of AsynCoro
import asyncoro.disasyncoro as asyncoro

def receiver_proc(coro=None):
    coro.set_daemon()
    # until subscribed, 'deliver' in client will block
    yield coro.sleep(10)
    yield channel.subscribe(coro)
    while True:
        msg = yield coro.receive()
        if msg:
            print('Received "%s" from %s at %s' % \
                  (msg['msg'], msg['sender'].name, msg['sender'].location))

asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
# scheduler = asyncoro.AsynCoro()
channel = asyncoro.Channel('2clients')
# register channel so client can get a reference to it
channel.register()

asyncoro.Coro(receiver_proc)
if sys.version_info.major > 2:
    read_input = input
else:
    read_input = raw_input
while True:
    try:
        cmd = read_input()
        if cmd.strip().lower() in ('quit', 'exit'):
            break
    except:
        break
