# Asynchronous pipe example using chained Popen

import sys, logging, subprocess, traceback

if sys.version_info.major > 2:
    import asyncoro3 as asyncoro
    import asyncfile3 as asyncfile
else:
    import asyncoro
    import asyncfile
    
def line_reader(apipe, coro=None):
    nlines = 0
    while True:
        try:
            line = yield apipe.readline()
        except:
            asyncoro.logger.debug('read failed')
            asyncoro.logger.debug(traceback.format_exc())
            break
        if not line:
            break
        print('%s' % line),
        nlines += 1
    print('lines: %s' % nlines)
    raise StopIteration(nlines)

# asyncoro.logger.setLevel(logging.DEBUG)
p1 = subprocess.Popen(['tail', '-f', '/var/log/kern.log'], stdin=None, stdout=subprocess.PIPE)
p2 = subprocess.Popen(['grep', '--line-buffered', '-i', 'error'],
                      stdin=p1.stdout, stdout=subprocess.PIPE)

apipe = asyncfile.AsyncPipe(p1, p2)
# writer is not needed
asyncoro.Coro(line_reader, apipe)
