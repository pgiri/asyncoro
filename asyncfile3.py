"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2014, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"
__status__ = "Development"
__version__ = "0.1"

__all__ = ['AsyncFile', 'AsyncPipe']

import subprocess
import fcntl
import functools
import os
import sys
import traceback
import errno

import asyncoro3 as asyncoro
from asyncoro3 import _AsyncPoller, logger

class AsyncFile(object):
    """Asynchronous interface for file-like objects in Linux and other
    Unix variants. This won't work on regular files, as they are
    non-blocking and can't be used for polling to signal read/write
    events.

    Tested with AsyncPipe and sockets under Linux and OS X.
    """

    _asyncoro = None
    _notifier = None

    def __init__(self, fd):
        """'fd' is either a file object (e.g., obtained with 'open')
        or a file number (e.g., obtained with socket's fileno()).
        """
        if AsyncFile._asyncoro is None:
            AsyncFile._asyncoro = asyncoro.AsynCoro.instance()
            AsyncFile._notifier = asyncoro._AsyncNotifier.instance()
        if hasattr(fd, 'fileno'):
            self._fd = fd
            self._fileno = fd.fileno()
        elif isinstance(fd, int):
            self._fd = None
            self._fileno = fd
        else:
            ValueError('invalid file descriptor')
        self._timeout = None
        self._read_task = None
        self._write_task = None
        self._read_coro = None
        self._write_coro = None
        self._buflist = []
        flags = fcntl.fcntl(self._fileno, fcntl.F_GETFL)
        fcntl.fcntl(self._fileno, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    def read(self, size=0, timeout=None):
        def _read(self, count):
            try:
                if count > 0:
                    buf = os.read(self._fileno, count)
                else:
                    buf = os.read(self._fileno, 4*1024)
            except (OSError, IOError) as exc:
                if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    return
                else:
                    raise
            except:
                AsyncFile._notifier.clear(self, _AsyncPoller._Read)
                self._read_task = None
                coro, self._read_coro = self._read_coro, None
                coro.throw(*sys.exc_info())
            else:
                if buf:
                    self._buflist.append(buf)
            if count > 0:
                if self._buflist:
                    buf = b''.join(self._buflist)
                    if len(buf) > count:
                        self._buflist = [buf[count:]]
                        buf = buf[:count]
                    else:
                        self._buflist = []
                AsyncFile._notifier.clear(self, _AsyncPoller._Read)
                self._read_coro._proceed_(buf)
                self._read_coro = None
            else:
                if not buf:
                    buf = b''.join(self._buflist)
                    self._buflist = []
                    AsyncFile._notifier.clear(self, _AsyncPoller._Read)
                    self._read_coro._proceed_(buf)
                    self._read_coro = None

        if not size or size < 0:
            size = 0
        elif self._buflist:
            buf = b''.join(self._buflist)
            self._buflist = []
            if len(buf) > size:
                self._buflist = [buf[size:]]
                buf = buf[:size]
            if buf:
                return buf
        self._timeout = timeout
        self._read_task = functools.partial(_read, self, size)
        self._read_coro = AsyncFile._asyncoro.cur_coro()
        self._read_coro._await_()
        AsyncFile._notifier.add(self, _AsyncPoller._Read)

    def readline(self, size=0, timeout=None):
        if not size or size < 0:
            size = 0
        if self._buflist:
            buf = b''.join(self._buflist)
            self._buflist = []
            if not buf:
                buf = yield self.read(size=size, timeout=timeout)
        else:
            buf = yield self.read(size=size, timeout=timeout)
        if not buf:
            raise StopIteration(buf)
        buflist = []
        while True:
            if size > 0:
                pos = buf.find(b'\n', 0, size)
                size -= len(buf)
                if size <= 0 and pos < 0:
                    pos = size + len(buf) - 1
            else:
                pos = buf.find(b'\n')
            if pos >= 0:
                if buflist:
                    buf = b''.join(buflist) + buf
                    pos += sum(len(b) for b in buflist)
                if len(buf) > pos:
                    self._buflist.insert(0, buf[pos+1:])
                    buf = buf[:pos+1]
                raise StopIteration(buf)
            buflist.append(buf)
            buf = yield self.read(size=size, timeout=timeout)
            if not buf:
                buf = b''.join(buflist)
                raise StopIteration(buf)

    def write(self, buf, timeout=None):
        def _write(self, buf):
            try:
                n = os.write(self._fileno, buf)
            except (OSError, IOError) as exc:
                if exc.errno in (errno.EAGAIN, errno.EINTR):
                    n = 0
                else:
                    AsyncFile._notifier.clear(self, _AsyncPoller._Write)
                    self._write_task = None
                    coro, self._write_coro = self._write_coro, None
                    coro.throw(*sys.exc_info())
                    return
            AsyncFile._notifier.clear(self, _AsyncPoller._Write)
            self._write_coro._proceed_(n)
            self._write_coro = None

        self._timeout = timeout
        self._write_task = functools.partial(_write, self, buf)
        self._write_coro = AsyncFile._asyncoro.cur_coro()
        self._write_coro._await_()
        AsyncFile._notifier.add(self, _AsyncPoller._Write)

    def close(self):
        if self._fileno:
            AsyncFile._notifier.unregister(self)
            if self._fd:
                self._fd.close()
            self._fd = self._fileno = None
            self._read_coro = self._write_coro = None
            self._read_task = self._write_task = None
            self._buflist = []

    def _eof(self):
        if self._read_task and self._read_coro:
            self._read_task()

    def _timed_out(self):
        if self._read_coro:
            self._read_coro.throw(Exception('timedout'))
        if self._write_coro:
            self._write_coro.throw(Exception('timedout'))

class AsyncPipe(object):
    """Asynchronous interface for (connected) pipes.
    """
    def __init__(self, first, last=None):
        """'first' is a Popen object. 'last', if given, is another
        Popen object that is the end of the joints to 'first'.

        'write' operations send data to first's stdin and 'read'
        operations get data from last's stdout/stderr.
        """
        if not last:
            last = first
        if not isinstance(first, subprocess.Popen) or not isinstance(last, subprocess.Popen):
            raise ValueError('argument must be subprocess.Popen object')
        if first.stdin:
            self.stdin = AsyncFile(first.stdin)
        else:
            self.stdin = None
        if last.stdout:
            self.stdout = AsyncFile(last.stdout)
        else:
            self.stdout = None
        if last.stderr:
            self.stderr = AsyncFile(last.stderr)
        else:
            self.stderr = None

    def write(self, buf, timeout=None):
        yield self.stdin.write(buf, timeout=timeout)

    def read(self, size=None, timeout=None):
        yield self.stdout.read(size=size, timeout=timeout)

    def readline(self, size=None, timeout=None):
        yield self.stdout.readline(size=size, timeout=timeout)

    def read_stderr(self, size=None, timeout=None):
        yield self.stderr.read(size=size, timeout=timeout)

    def readline_stderr(self, size=None, timeout=None):
        yield self.stderr.readline(size=size, timeout=timeout)

    def communicate(self, input=None):
        """Similar to Popen's communicate. Must be used with 'yield' as
        'stdout, stderr = yield async_pipe.communicate()'

        'input' must be either a string (data) or an object with
        'read' method (i.e., regular file object or AsyncFile object).
        """
        def write_proc(fd, input, coro=None):
            size = 16*1024
            if isinstance(input, str):
                buf = memoryview(input.encode())
                left = len(input)
                while len(buf) > 0:
                    n = yield fd.write(buf[:min(size, left)])
                    if n == 0:
                        logger.warning('waiting for reader to catch up?')
                        yield coro.sleep(0.1)
                        continue
                    buf = buf[n:]
                    left -= n
                buf.release()
            else:
                while True:
                    # TODO: how to know if 'input' is file object for
                    # on-disk file?
                    if hasattr(input, 'seek'):
                        data = os.read(input.fileno(), size)
                    else:
                        data = yield input.read(size)
                    if not data:
                        break
                    if isinstance(data, str):
                        data = data.encode()
                    buf = memoryview(data)
                    left = len(data)
                    while len(buf) > 0:
                        n = yield fd.write(buf[:min(size, left)])
                        if n == 0:
                            # this shouldn't happen?
                            logger.warning('waiting for reader to catch up?')
                            yield coro.sleep(0.1)
                            continue
                        buf = buf[n:]
                        left -= n
                    buf.release()
            fd.close()

        def read_proc(fd, coro=None):
            size = 4*1024
            buflist = []
            while True:
                buf = yield fd.read(size)
                if not buf:
                    break
                buflist.append(buf)
            fd.close()
            data = b''.join(buflist)
            raise StopIteration(data)

        if self.stdout:
            stdout_coro = asyncoro.Coro(read_proc, self.stdout)
        if self.stderr:
            stderr_coro = asyncoro.Coro(read_proc, self.stderr)
        if input and self.stdin:
            stdin_coro = asyncoro.Coro(write_proc, self.stdin, input)
            yield stdin_coro.wait()

        raise StopIteration((yield stdout_coro.wait()) if self.stdout else None,
                            (yield stderr_coro.wait()) if self.stderr else None)
