# Asynchronous pipe example using chained Popen

import sys, subprocess, traceback, platform
import asyncoro
import asyncoro.asyncfile
    
def writer(apipe, inp, coro=None):
    fd = open(inp)
    while True:
        line = fd.readline()
        if not line:
            break
        yield apipe.stdin.write(line.encode())
    apipe.stdin.close()

def line_reader(apipe, coro=None):
    nlines = 0
    while True:
        try:
            line = yield apipe.readline()
        except:
            asyncoro.logger.debug('read failed')
            asyncoro.logger.debug(traceback.format_exc())
            break
        nlines += 1
        if not line:
            break
        print(line.decode())
    raise StopIteration(nlines)

# asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)
if platform.system() == 'Windows':
    # asyncfile.Popen must be used instead of subprocess.Popen
    p1 = asyncoro.asyncfile.Popen([r'\cygwin64\bin\grep.exe', '-i', 'error'],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    p2 = asyncoro.asyncfile.Popen([r'\cygwin64\bin\wc.exe'], stdin=p1.stdout, stdout=subprocess.PIPE)
    async_pipe = asyncoro.asyncfile.AsyncPipe(p1, p2)
    asyncoro.Coro(writer, async_pipe, r'\tmp\grep.inp')
    asyncoro.Coro(line_reader, async_pipe)
else:
    p1 = subprocess.Popen(['grep', '-i', 'error'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(['wc'], stdin=p1.stdout, stdout=subprocess.PIPE)
    async_pipe = asyncoro.asyncfile.AsyncPipe(p1, p2)
    asyncoro.Coro(writer, async_pipe, '/var/log/syslog')
    asyncoro.Coro(line_reader, async_pipe)

    # alternate example:

    # p1 = subprocess.Popen(['tail', '-f', '/var/log/kern.log'], stdin=None, stdout=subprocess.PIPE)
    # p2 = subprocess.Popen(['grep', '--line-buffered', '-i', 'error'],
    #                       stdin=p1.stdout, stdout=subprocess.PIPE)
    # async_pipe = asyncoro.asyncfile.AsyncPipe(p2)
    # asyncoro.Coro(line_reader, async_pipe)
