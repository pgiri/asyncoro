#!/usr/bin/env python

# chat server; must be used with 'chat_chan_client.py'

import sys, logging
# import disasyncoro to use distributed version of AsynCoro
import asyncoro.disasyncoro as asyncoro

asyncoro.MaxConnectionErrors = 3

def server_proc(coro=None):
    # to illustrate 'transform' function of channel, messages are modified
    def txfm_msgs(name, msg_cid):
        msg, client_id = msg_cid
        # assert name == 'channel'
        # e.g., drop shoutings
        if msg.isupper():
            return None
        if msg == 'joined':
            msg += ' :-)'
        elif msg == 'bye':
            msg = 'left :-('
        else:
            msg = 'says: %s' % msg
        return (msg, client_id)

    channel = asyncoro.Channel('channel', transform=txfm_msgs)
    channel.register()
    coro.set_daemon()
    coro.register('server')
    client_id = 1
    while True:
        cmd, who = yield coro.receive()
        # join/quit messages can be sent by clients themselves, but
        # for illustration server sends them instead
        if cmd == 'join':
            channel.send(('joined', client_id))
            who.send(client_id)
            client_id += 1
        elif cmd == 'quit':
            channel.send(('bye', who))
        elif cmd == 'terminate':
            break
    channel.unregister()
    coro.unregister()

if __name__ == '__main__':
    # asyncoro.logger.setLevel(logging.DEBUG)
    server = asyncoro.Coro(server_proc)
    while True:
        try:
            cmd = sys.stdin.readline().strip().lower()
            if cmd == 'quit' or cmd == 'exit':
                break
        except KeyboardInterrupt:
            break
    server.send(('terminate', None))
    server.value() # wait for server to finish
