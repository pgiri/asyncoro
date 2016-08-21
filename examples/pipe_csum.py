# Asynchronous pipe example with "communicate" method that is similar
# to Popen's "communicate". Same example is used to show how custom
# write/read processes can be provided to feed / read from the
# asynchronous pipe

# argv[1] must be a text file

import sys, os, traceback, subprocess, platform
import asyncoro
import asyncoro.asyncfile
    
def communicate(input, coro=None):
    if platform.system() == 'Windows':
        # asyncfile.Popen must be used instead of subprocess.Popen
        pipe = asyncoro.asyncfile.Popen([r'\cygwin64\bin\sha1sum.exe'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    else:
        pipe = subprocess.Popen(['sha1sum'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    # convert pipe to asynchronous version
    async_pipe = asyncoro.asyncfile.AsyncPipe(pipe)
    # 'communicate' takes either the data or file descriptor with data
    # (if file is too large to read in full) as input
    input = open(input)
    stdout, stderr = yield async_pipe.communicate(input)
    print('communicate sha1sum: %s' % stdout)

def custom_feeder(input, coro=None):
    def write_proc(fin, pipe, coro=None):
        while True:
            data = yield os.read(fin.fileno(), 8*1024)
            if not data:
                break
            n = yield pipe.write(data, full=True)
            assert n == len(data)
        fin.close()
        pipe.stdin.close()

    def read_proc(pipe, coro=None):
        # output from sha1sum is small, so read until EOF
        data = yield pipe.stdout.read()
        pipe.stdout.close()
        raise StopIteration(data)

    if platform.system() == 'Windows':
        # asyncfile.Popen must be used instead of subprocess.Popen
        pipe = asyncoro.asyncfile.Popen([r'\cygwin64\bin\sha1sum.exe'],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    else:
        pipe = subprocess.Popen(['sha1sum'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    async_pipe = asyncoro.asyncfile.AsyncPipe(pipe)
    reader = asyncoro.Coro(read_proc, async_pipe)
    writer = asyncoro.Coro(write_proc, open(input), async_pipe)
    stdout = yield reader.finish()
    print('     feeder sha1sum: %s' % stdout)

# asyncoro.logger.setLevel(asyncoro.Logger.DEBUG)

# simpler version using 'communicate'
coro = asyncoro.Coro(communicate, sys.argv[1] if len(sys.argv) > 1 else sys.argv[0])
coro.value() # wait for it to finish

# alternate version with custom read and write processes
asyncoro.Coro(custom_feeder, sys.argv[1] if len(sys.argv) > 1 else sys.argv[0])
