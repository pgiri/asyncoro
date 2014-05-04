"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.
"""

__all__ = ['AsyncFile', 'AsyncPipe']

import subprocess
import fcntl
import os
import sys
import errno
from functools import partial as partial_func

import asyncoro
from asyncoro import _AsyncPoller

class AsyncFile(object):
    """Asynchronous interface for file-like objects in Linux and other
    Unix variants. This won't work on regular files, as they are
    non-blocking and can't be used for polling to signal read/write
    events.

    Tested with AsyncPipe and sockets under Linux and OS X; it should
    work on other Unix variants, but not Windows.
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

    def read(self, size=0, full=False, timeout=None):
        """Read at most 'size' bytes from file; if 'size' <= 0, all
        data up to EOF is read and returned. If 'full' is True,
        exactly 'size' bytes are returned (unless EOF or timeout occur
        before). If 'timeout' is given, Exception('timedout') will be
        thrown in coroutine if read is not complete before timeout.

        Must be used in a coroutine with 'yield' as
        'data = yield fd.read(1024)'
        """
        def _read(self, count, full):
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
                return

            if buf:
                if full and count > 0:
                    count -= len(buf)
                    # assert count >= 0
                    if count == 0:
                        full = False
                self._buflist.append(buf)
                if full:
                    self._read_task = partial_func(_read, self, count, full)
                    return
            if self._buflist:
                buf = ''.join(self._buflist)
                self._buflist = []
            AsyncFile._notifier.clear(self, _AsyncPoller._Read)
            self._read_coro._proceed_(buf)
            self._read_coro = None

        if not size or size < 0:
            size = 0
            full = True
        elif self._buflist:
            buf = ''.join(self._buflist)
            self._buflist = []
            if len(buf) > size:
                self._buflist = [buf[size:]]
                buf = buf[:size]
            if buf and ((not full) or (len(buf) == size)):
                return buf
        self._timeout = timeout
        self._read_task = partial_func(_read, self, size, full)
        self._read_coro = AsyncFile._asyncoro.cur_coro()
        self._read_coro._await_()
        AsyncFile._notifier.add(self, _AsyncPoller._Read)

    def readline(self, size=0, sizehint=100, timeout=None):
        """Read a line up to 'size' and return. 'size' and 'timeout'
        are as per 'read' method above. 'sizehint' indicates
        approximate number of bytes expected in a line. Too big/small
        value affects performance, otherwise has no effect.

        Must be used with 'yield' as 'line = yield fd.readline()'
        """
        if not size or size < 0:
            size = 0
        if self._buflist:
            buf = ''.join(self._buflist)
            self._buflist = []
            if not buf:
                buf = yield self.read(size=sizehint, timeout=timeout)
        else:
            buf = yield self.read(size=sizehint, timeout=timeout)
        if not buf:
            raise StopIteration(buf)

        buflist = []
        while True:
            if size > 0:
                pos = buf.find('\n', 0, size)
                size -= len(buf)
                if size <= 0 and pos < 0:
                    pos = size + len(buf) - 1
            else:
                pos = buf.find('\n')
            if pos >= 0:
                if buflist:
                    buf = ''.join(buflist) + buf
                    pos += sum(len(b) for b in buflist)
                if len(buf) > pos:
                    self._buflist.insert(0, buf[pos+1:])
                    buf = buf[:pos+1]
                raise StopIteration(buf)
            buflist.append(buf)
            buf = yield self.read(size=sizehint, timeout=timeout)
            if not buf:
                buf = ''.join(buflist)
                raise StopIteration(buf)

    def write(self, buf, full=False, timeout=None):
        """Write data in 'buf' to fd. If 'full' is True, the function
        waits till all data in buf is written; otherwise, it waits
        until one write completes. It returns length of data written.

        Must be used with 'yield' as
        'n = yield fd.write(buf)' to write (some) data in buf.
        """
        def _write(self, view, written, full):
            try:
                n = os.write(self._fileno, view)
            except (OSError, IOError) as exc:
                if exc.errno in (errno.EAGAIN, errno.EINTR):
                    n = 0
                else:
                    AsyncFile._notifier.clear(self, _AsyncPoller._Write)
                    self._write_task = None
                    coro, self._write_coro = self._write_coro, None
                    coro.throw(*sys.exc_info())
                    return
            written += n
            if n == len(view) or not full:
                AsyncFile._notifier.clear(self, _AsyncPoller._Write)
                self._write_coro._proceed_(written)
                self._write_coro = None
            else:
                view = view[n:]
                self._write_task = partial_func(_write, self, view, written, full)

        if full:
            view = memoryview(buf)
        else:
            view = buf
        self._timeout = timeout
        self._write_task = partial_func(_write, self, view, 0, full)
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

    def write(self, buf, full=False, timeout=None):
        yield self.stdin.write(buf, full=full, timeout=timeout)

    def read(self, size=0, timeout=None):
        yield self.stdout.read(size=size, timeout=timeout)

    def readline(self, size=0, sizehint=100, timeout=None):
        yield self.stdout.readline(size=size, sizehint=sizehint, timeout=timeout)

    def read_stderr(self, size=0, timeout=None):
        yield self.stderr.read(size=size, timeout=timeout)

    def readline_stderr(self, size=0, sizehint=100, timeout=None):
        yield self.stderr.readline(size=size, sizehint=sizehint, timeout=timeout)

    def communicate(self, input=None):
        """Similar to Popen's communicate. Must be used with 'yield' as
        'stdout, stderr = yield async_pipe.communicate()'

        'input' must be either data or an object with 'read' method
        (i.e., regular file object or AsyncFile object).
        """
        def write_proc(fd, input, coro=None):
            size = 16*1024
            if isinstance(input, str):
                n = yield fd.write(input, full=True)
                if n != len(input):
                    raise Exception('write failed')
            else:
                # TODO: how to know if 'input' is file object for
                # on-disk file?
                if hasattr(input, 'seek') and hasattr(input, 'fileno'):
                    read_func = partial_func(os.read, input.fileno())
                else:
                    read_func = input.read
                while True:
                    data = yield read_func(size)
                    if not data:
                        break
                    n = yield fd.write(data, full=True)
                    if n != len(data):
                        raise Exception('write failed')
                input.close()
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
            data = ''.join(buflist)
            raise StopIteration(data)

        if self.stdout:
            stdout_coro = asyncoro.Coro(read_proc, self.stdout)
        if self.stderr:
            stderr_coro = asyncoro.Coro(read_proc, self.stderr)
        if input and self.stdin:
            stdin_coro = asyncoro.Coro(write_proc, self.stdin, input)
            yield stdin_coro.finish()

        raise StopIteration((yield stdout_coro.finish()) if self.stdout else None,
                            (yield stderr_coro.finish()) if self.stderr else None)
