#!/usr/bin/env python

# chat server; must be used with 'chat_chan_client.py'

import sys
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

    channel = asyncoro.Channel('chat_channel', transform=txfm_msgs)
    channel.register()
    coro.set_daemon()
    coro.register('chat_server')
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
    # asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
    server = asyncoro.Coro(server_proc)
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
    server.send(('terminate', None))
    server.value() # wait for server to finish
