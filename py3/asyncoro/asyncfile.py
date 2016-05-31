"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.

This module provides API for asynchronous file and pipe processing.
They work with Windows, Linux, OS X and likely other UNIX
variants. Note that regular (on-disk) files don't support asynchronous
I/O, as they are non-blocking and can't be used for polling to signal
read/write events - they are always ready to be read/written.

Under Windows, pipes must be opened with Popen in this module instead
of Popen in subprocess module.

See 'pipe_csum.py', 'pipe_grep.py' and 'socket_afile.py' for examples.
"""

import subprocess
import os
import sys
import errno
import platform
from functools import partial as partial_func

import asyncoro
from asyncoro import _AsyncPoller, AsynCoro, Coro

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2014 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

__all__ = ['AsyncFile', 'AsyncPipe']

if platform.system() == 'Windows':
    __all__ += ['pipe', 'Popen']

    import itertools
    import win32file
    import win32pipe
    import win32event
    import win32con
    import winerror
    import winnt
    import pywintypes
    import msvcrt

    # pywin32 doesn't define FILE_FLAG_FIRST_PIPE_INSTANCE
    FILE_FLAG_FIRST_PIPE_INSTANCE = 0x00080000
    _pipe_id = itertools.count()

    def pipe(bufsize=8192):
        """Creates overlapped (asynchronous) pipe.
        """
        name = r'\\.\pipe\asyncoro-pipe-%d-%d' % (os.getpid(), next(_pipe_id))
        openmode = (win32pipe.PIPE_ACCESS_INBOUND | win32file.FILE_FLAG_OVERLAPPED |
                    FILE_FLAG_FIRST_PIPE_INSTANCE)
        pipemode = (win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_READMODE_BYTE)
        rh = wh = None
        try:
            rh = win32pipe.CreateNamedPipe(
                name, openmode, pipemode, 1, bufsize, bufsize,
                win32pipe.NMPWAIT_USE_DEFAULT_WAIT, None)

            wh = win32file.CreateFile(
                name, win32file.GENERIC_WRITE | winnt.FILE_READ_ATTRIBUTES, 0, None,
                win32file.OPEN_EXISTING, win32file.FILE_FLAG_OVERLAPPED, None)

            overlapped = pywintypes.OVERLAPPED()
            # 'yield' can't be used in constructor so use sync wait
            # (in this case it is should be okay)
            overlapped.hEvent = win32event.CreateEvent(None, 0, 0, None)
            rc = win32pipe.ConnectNamedPipe(rh, overlapped)
            if rc == winerror.ERROR_PIPE_CONNECTED:
                win32event.SetEvent(overlapped.hEvent)
            rc = win32event.WaitForSingleObject(overlapped.hEvent, 1000)
            overlapped = None
            if rc != win32event.WAIT_OBJECT_0:
                asyncoro.logger.warning('connect failed: %s' % rc)
                raise Exception(rc)
            return (rh, wh)
        except:
            if rh is not None:
                win32file.CloseHandle(rh)
            if wh is not None:
                win32file.CloseHandle(wh)
            raise

    class Popen(subprocess.Popen):
        """Asynchronous version of subprocess.Popen - stdin, stdout
        and stderr support overlapped I/O.
        """
        def __init__(self, args, stdin=None, stdout=None, stderr=None, **kwargs):
            self.stdin = self.stdout = self.stderr = None

            stdin_rh = stdin_wh = stdout_rh = stdout_wh = stderr_rh = stderr_wh = None

            if stdin == subprocess.PIPE:
                stdin_rh, stdin_wh = pipe()
                stdin_rfd = msvcrt.open_osfhandle(stdin_rh.Detach(), os.O_RDONLY)
                self.stdin_rh = stdin_rh
            else:
                stdin_rfd = stdin
                self.stdin_rh = None

            if stdout == subprocess.PIPE:
                stdout_rh, stdout_wh = pipe()
                stdout_wfd = msvcrt.open_osfhandle(stdout_wh, 0)
            else:
                stdout_wfd = stdout

            if stderr == subprocess.PIPE:
                stderr_rh, stderr_wh = pipe()
                stderr_wfd = msvcrt.open_osfhandle(stderr_wh, 0)
            elif stderr == subprocess.STDOUT:
                stderr_wfd = stdout_wfd
            else:
                stderr_wfd = stderr

            try:
                super(Popen, self).__init__(args, stdin=stdin_rfd, stdout=stdout_wfd,
                                            stderr=stderr_wfd, **kwargs)
            except:
                for handle in (stdin_rh, stdin_wh, stdout_rh, stdout_wh, stderr_rh, stderr_wh):
                    if handle is not None:
                        win32file.CloseHandle(handle)
                raise
            else:
                if stdin_wh is not None:
                    self.stdin = AsyncFile(stdin_wh, mode='w')
                if stdout_rh is not None:
                    self.stdout = AsyncFile(stdout_rh, mode='r')
                if stderr_rh is not None:
                    self.stderr = AsyncFile(stderr_rh, mode='r')
            finally:
                if stdin == subprocess.PIPE:
                    os.close(stdin_rfd)
                if stdout == subprocess.PIPE:
                    os.close(stdout_wfd)
                if stderr == subprocess.PIPE:
                    os.close(stderr_wfd)

        def close(self):
            """It is advised to call 'close' on the pipe so both
            handles of pipe are closed.
            """
            if isinstance(self.stdin, AsyncFile):
                self.stdin.close()
                self.stdin = None
            if self.stdin_rh:
                win32pipe.DisconnectNamedPipe(self.stdin_rh)
                win32file.CloseHandle(self.stdin_rh)
                self.stdin_rh = None
            if isinstance(self.stdout, AsyncFile):
                self.stdout.close()
                self.stdout = None
            if isinstance(self.stderr, AsyncFile):
                self.stderr.close()
                self.stderr = None

        def terminate(self):
            """Close pipe and terminate child process.
            """
            self.close()
            super(Popen, self).terminate()

        def __del__(self):
            self.terminate()

    class _AsyncFile(object):
        """Asynchronous file interface. Under Windows asynchronous I/O
        works on regular (on-disk) files, but not very useful, as
        regular files are always ready to read / write. They are
        useful when used as file objects in asynchronous pipes.
        """

        _notifier = None

        def __init__(self, path_handle, mode='r', share=None):
            """If 'path_handle' is a string, opens that file for
            asynchronous I/O; if it is a handle (pipe client / server,
            for example), sets up for asynchronous I/O. 'mode' is as
            per 'open' Python function, although limited to
            basic/common modes.
            """
            if not _AsyncFile._notifier:
                _AsyncFile._notifier = asyncoro._AsyncNotifier.instance()
            self._asyncoro = AsynCoro.scheduler()
            self._overlap = pywintypes.OVERLAPPED()
            if isinstance(path_handle, str):
                self._path = path_handle
                if mode.startswith('r'):
                    access = win32file.GENERIC_READ
                    if share is None:
                        share = win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE
                    create = win32file.OPEN_EXISTING
                    if '+' in mode:
                        access |= win32file.GENERIC_WRITE
                elif mode.startswith('w'):
                    access = win32file.GENERIC_WRITE
                    if share is None:
                        share = win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE
                    create = win32file.CREATE_ALWAYS
                    if '+' in mode:
                        access |= win32file.GENERIC_READ
                elif mode.startswith('a'):
                    access = win32file.GENERIC_WRITE
                    if share is None:
                        share = win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE
                    create = win32file.OPEN_ALWAYS
                    if '+' in mode:
                        access |= win32file.GENERIC_READ
                        # TODO: if reading, offset should be 0?
                    sb = os.stat(path_handle)
                    self._overlap.Offset = sb.st_size
                else:
                    self._overlap = None
                    raise ValueError('invalid mode "%s"' % mode)

                flags = win32file.FILE_FLAG_OVERLAPPED

                try:
                    self._handle = win32file.CreateFile(path_handle, access, share, None, create,
                                                        flags, None)
                except:
                    self._overlap = None
                    raise
                if mode.startswith('r'):
                    flags = os.O_RDONLY
                elif mode.startswith('a'):
                    flags = os.O_APPEND
                else:
                    flags = 0
                self._fileno = msvcrt.open_osfhandle(self._handle, flags)
            else:
                self._handle = path_handle
                # pipe mode should be either 'r' or 'w'
                flags = os.O_RDONLY if mode.startswith('r') else 0
                self._fileno = msvcrt.open_osfhandle(self._handle, flags)

            self._buflist = []
            self._read_result = None
            self._write_result = None
            self._timeout = None
            self._timeout_id = None
            _AsyncFile._notifier.register(self._handle)

        def read(self, size=0, full=False, timeout=None):
            """Read at most 'size' bytes from file; if 'size' <= 0,
            all data up to EOF is read and returned. If 'full' is
            True, exactly 'size' bytes are returned (unless EOF or
            timeout occur before). If EOF is encountered before any
            more data is available, empty buffer is returned.

            If no data has been read before timeout, then
            IOError('timedout') will be thrown.

            If timeout is given and full is True and timeout expires
            before all the data could be read, it returns partial data
            read before timeout if any data has been read.

            Must be used in a coroutine with 'yield' as
            'data = yield fd.read(1024)'
            """
            def _read(self, size, full, rc, n):
                if rc or n == 0:
                    if self._timeout:
                        _AsyncFile._notifier._del_timeout(self)
                    self._overlap.object = self._read_result = None
                    if rc != winerror.ERROR_OPERATION_ABORTED:
                        if (self._buflist or rc == winerror.ERROR_HANDLE_EOF or
                           rc == winerror.ERROR_BROKEN_PIPE):
                            buf, self._buflist = b''.join(self._buflist), []
                            self._read_coro._proceed_(buf)
                            return
                        self._read_coro.throw(IOError(rc, 'ReadFile', str(rc)))
                    self._overlap.object = self._read_coro = self._read_result = None
                    return

                buf = self._read_result[:n]
                if size > 0:
                    size -= len(buf)
                    assert size >= 0
                    if size == 0:
                        full = False
                self._buflist.append(buf)
                self._overlap.Offset += n
                if full:
                    self._overlap.object = partial_func(_read, self, size, full)
                    try:
                        rc, _ = win32file.ReadFile(self._handle, self._read_result, self._overlap)
                    except pywintypes.error as exc:
                        rc = exc.winerror
                    if rc and rc != winerror.ERROR_IO_PENDING:
                        buf, self._buflist = b''.join(self._buflist), []
                        self._overlap.object = self._read_result = None
                        if self._timeout:
                            _AsyncFile._notifier._del_timeout(self)
                        self._read_coro._proceed_(buf)
                        self._read_coro = None
                    return
                if self._buflist:
                    buf, self._buflist = b''.join(self._buflist), []
                if self._timeout:
                    _AsyncFile._notifier._del_timeout(self)
                self._overlap.object = self._read_result = None
                self._read_coro._proceed_(buf)
                self._read_coro = None

            if not size or size < 0:
                count = 16384
                full = True
            else:
                if self._buflist:
                    buf, self._buflist = b''.join(self._buflist), []
                    if len(buf) > size:
                        buf, self._buflist = buf[:size], [buf[size:]]
                    if (not full) or (len(buf) == size):
                        return buf
                    self._buflist = [buf]
                    size -= len(buf)
                count = size
            self._read_result = win32file.AllocateReadBuffer(count)
            self._overlap.object = partial_func(_read, self, size, full)
            if not self._asyncoro:
                self._asyncoro = AsynCoro.scheduler()
            self._read_coro = AsynCoro.cur_coro(self._asyncoro)
            self._read_coro._await_()
            try:
                rc, _ = win32file.ReadFile(self._handle, self._read_result, self._overlap)
            except pywintypes.error as exc:
                if exc.winerror == winerror.ERROR_BROKEN_PIPE:
                    buf, self._buflist = b''.join(self._buflist), []
                    self._read_coro._proceed_(buf)
                    self._read_result = self._read_coro = self._overlap.object = None
                    return
                else:
                    rc = exc.winerror
            if rc and rc != winerror.ERROR_IO_PENDING:
                self._overlap.object = self._read_result = self._read_coro = None
                raise IOError(rc, 'ReadFile', str(rc))
            if timeout:
                self._timeout = timeout
                _AsyncFile._notifier._add_timeout(self)

        def write(self, buf, full=False, timeout=None):
            """Write data in 'buf' to file. If 'full' is True, the function
            waits till all data in buf is written; otherwise, it waits
            until one write completes. It returns length of data written.

            If no data has been written before timeout, then
            IOError('timedout') will be thrown.

            If timeout is given and full is True and timeout expires
            before all the data could be written, it returns length of
            data written before timeout if any data has been written.

            Must be used with 'yield' as
            'n = yield fd.write(buf)' to write (some) data in buf.
            """
            def _write(self, written, full, rc, n):
                if rc or n == 0:
                    if self._timeout:
                        _AsyncFile._notifier._del_timeout(self)
                    if rc != winerror.ERROR_OPERATION_ABORTED:
                        if written:
                            self._write_coro._proceed_(written)
                        else:
                            self._write_coro.throw(IOError(rc, 'WriteFile', str(rc)))
                    self._write_result.release()
                    self._overlap.object = self._write_coro = self._write_result = None
                    return

                written += n
                self._overlap.Offset += n
                self._write_result = self._write_result[n:]
                if not full or len(self._write_result) == 0:
                    self._write_result.release()
                    self._overlap.object = self._write_result = None
                    if self._timeout:
                        _AsyncFile._notifier._del_timeout(self)
                    self._write_coro._proceed_(written)
                    self._write_coro = None
                    return

                self._overlap.object = partial_func(_write, self, written, full)
                try:
                    rc, _ = win32file.WriteFile(self._handle, self._write_result, self._overlap)
                except pywintypes.error as exc:
                    rc = exc.winerror
                if rc and rc != winerror.ERROR_IO_PENDING:
                    self._write_result.release()
                    self._overlap.object = self._write_result = None
                    if self._timeout:
                        _AsyncFile._notifier._del_timeout(self)
                    if written:
                        self._write_coro._proceed_(written)
                    else:
                        self._write_coro.throw(IOError(rc, 'WriteFile', str(rc)))
                    self._write_coro = None
                return

            self._write_result = memoryview(buf)
            self._overlap.object = partial_func(_write, self, 0, full)
            if not self._asyncoro:
                self._asyncoro = AsynCoro.scheduler()
            self._write_coro = AsynCoro.cur_coro(self._asyncoro)
            self._write_coro._await_()
            try:
                rc, _ = win32file.WriteFile(self._handle, self._write_result, self._overlap)
            except pywintypes.error as exc:
                if exc.winerror == winerror.ERROR_BROKEN_PIPE:
                    self._write_result.release()
                    self._write_coro._proceed_(0)
                    self._write_result = self._write_coro = self._overlap.object = None
                    return
                else:
                    rc = exc.winerror
            if rc and rc != winerror.ERROR_IO_PENDING:
                self._write_result.release()
                self._overlap.object = self._write_result = self._write_coro = None
                raise IOError(rc, 'WriteFile', str(rc))
            if timeout:
                self._timeout = timeout
                _AsyncFile._notifier._add_timeout(self)

        def seek(self, offset, whence=os.SEEK_SET):
            """Similar to 'seek' of file descriptor; works only for
            regular files.
            """
            if whence == os.SEEK_SET:
                self._overlap.Offset = offset
            elif whence == os.SEEK_CUR:
                self._overlap.Offset += offset
            else:
                assert whence == os.SEEK_END
                if isinstance(self._path, str):
                    sb = os.stat(self._path)
                    self._overlap.Offset = sb.st_size + offset
                else:
                    self._overlap.Offset = offset

        def tell(self):
            """Similar to 'tell' of file descriptor; works only for
            regular files.
            """
            return self._overlap.Offset

        def fileno(self):
            """Similar to 'fileno' of file descriptor; works only for
            regular files.
            """
            return self._fileno

        def close(self):
            """Similar to 'close' of file descriptor.
            """
            if self._handle:
                try:
                    flags = win32pipe.GetNamedPipeInfo(self._handle)[0]
                except:
                    flags = 0

                if flags & win32con.PIPE_SERVER_END:
                    win32pipe.DisconnectNamedPipe(self._handle)
                # TODO: if pipe, need to call FlushFileBuffers?

                def _close_(self, rc, n):
                    win32file.CloseHandle(self._handle)
                    self._overlap = None
                    _AsyncFile._notifier.unregister(self._handle)
                    self._handle = None
                    self._read_result = self._write_result = None
                    self._read_coro = self._write_coro = None
                    self._buflist = []

                if self._overlap.object:
                    self._overlap.object = partial_func(_close_, self)
                    win32file.CancelIo(self._handle)
                else:
                    _close_(self, 0, 0)

        def _timed_out(self):
            """Internal use only.
            """
            if self._read_coro:
                if self._buflist:
                    buf, self._buflist = b''.join(self._buflist), []
                    self._read_coro._proceed_(buf)
                    self._read_coro = None
                else:
                    self._read_coro.throw(IOError('timedout'))
                    self._read_coro = None
                win32file.CancelIo(self._handle)

else:
    import fcntl

    class _AsyncFile(object):
        """Asynchronous interface for file-like objects in Linux and other
        Unix variants.

        Tested with AsyncPipe and sockets under Linux and OS X; it should
        work on other Unix variants.
        """

        _notifier = None

        def __init__(self, fd):
            """'fd' is either a file object (e.g., obtained with 'open')
            or a file number (e.g., obtained with socket's fileno()).
            """
            if _AsyncFile._notifier is None:
                _AsyncFile._notifier = asyncoro._AsyncNotifier.instance()
            self._asyncoro = AsynCoro.scheduler()
            if hasattr(fd, 'fileno'):
                if hasattr(fd, '_fileno'):
                    _AsyncFile._notifier.unregister(fd)
                self._fd = fd
                self._fileno = fd.fileno()
            elif isinstance(fd, int):
                self._fd = None
                self._fileno = fd
            else:
                raise ValueError('invalid file descriptor')
            self._timeout = None
            self._read_task = None
            self._write_task = None
            self._read_coro = None
            self._write_coro = None
            self._buflist = []
            flags = fcntl.fcntl(self._fileno, fcntl.F_GETFL)
            fcntl.fcntl(self._fileno, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        def read(self, size=0, full=False, timeout=None):
            """Read at most 'size' bytes from file; if 'size' <= 0,
            all data up to EOF is read and returned. If 'full' is
            True, exactly 'size' bytes are returned (unless EOF or
            timeout occur before). If EOF is encountered before any
            more data is available, empty buffer is returned.

            If no data has been read before timeout, then
            IOError('timedout') will be thrown.

            If timeout is given and full is True and timeout expires
            before all the data could be read, it returns partial data
            read before timeout if any data has been read.

            Must be used in a coroutine with 'yield' as
            'data = yield fd.read(1024)'
            """
            def _read(self, size, full):
                if size > 0:
                    count = size
                else:
                    count = 16384
                try:
                    buf = os.read(self._fileno, count)
                except (OSError, IOError) as exc:
                    if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                        return
                    else:
                        raise
                except:
                    _AsyncFile._notifier.clear(self, _AsyncPoller._Read)
                    self._read_task = None
                    coro, self._read_coro = self._read_coro, None
                    coro.throw(*sys.exc_info())
                    return

                if buf:
                    if size > 0:
                        size -= len(buf)
                        # assert size >= 0
                        if size == 0:
                            full = False
                    self._buflist.append(buf)
                    if full:
                        self._read_task = partial_func(_read, self, size, full)
                        return
                if self._buflist:
                    buf, self._buflist = b''.join(self._buflist), []
                _AsyncFile._notifier.clear(self, _AsyncPoller._Read)
                self._read_coro._proceed_(buf)
                self._read_coro = self._read_task = None

            if not size or size < 0:
                size = 0
                full = True
            elif self._buflist:
                buf, self._buflist = b''.join(self._buflist), []
                if len(buf) > size:
                    buf, self._buflist = buf[:size], [buf[size:]]
                if (not full) or (len(buf) == size):
                    return buf
                self._buflist = [buf]
                size -= len(buf)
            self._timeout = timeout
            self._read_task = partial_func(_read, self, size, full)
            if not self._asyncoro:
                self._asyncoro = AsynCoro.scheduler()
            self._read_coro = AsynCoro.cur_coro(self._asyncoro)
            self._read_coro._await_()
            _AsyncFile._notifier.add(self, _AsyncPoller._Read)

        def write(self, buf, full=False, timeout=None):
            """Write data in 'buf' to file. If 'full' is True, the function
            waits till all data in buf is written; otherwise, it waits
            until one write completes. It returns length of data written.

            If no data has been written before timeout, then
            IOError('timedout') will be thrown.

            If timeout is given and full is True and timeout expires
            before all the data could be written, it returns length of
            data written before timeout if any data has been written.

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
                        _AsyncFile._notifier.clear(self, _AsyncPoller._Write)
                        if full:
                            view.release()
                        self._write_task = None
                        coro, self._write_coro = self._write_coro, None
                        coro.throw(*sys.exc_info())
                        return
                written += n
                if n == len(view) or not full:
                    _AsyncFile._notifier.clear(self, _AsyncPoller._Write)
                    if full:
                        view.release()
                    self._write_coro._proceed_(written)
                    self._write_coro = self._write_task = None
                else:
                    view = view[n:]
                    self._write_task = partial_func(_write, self, view, written, full)

            if full:
                view = memoryview(buf)
            else:
                view = buf
            self._timeout = timeout
            self._write_task = partial_func(_write, self, view, 0, full)
            if not self._asyncoro:
                self._asyncoro = AsynCoro.scheduler()
            self._write_coro = AsynCoro.cur_coro(self._asyncoro)
            self._write_coro._await_()
            _AsyncFile._notifier.add(self, _AsyncPoller._Write)

        def close(self):
            """Close file descriptor.
            """
            if self._fileno:
                _AsyncFile._notifier.unregister(self)
                if self._fd:
                    self._fd.close()
                self._fd = self._fileno = None
                self._read_coro = self._write_coro = None
                self._read_task = self._write_task = None
                self._buflist = []

        def _eof(self):
            """Internal use only.
            """
            if self._read_task and self._read_coro:
                self._read_task()

        def _timed_out(self):
            """Internal use only.
            """
            if self._read_coro:
                if self._read_task and self._buflist:
                    buf, self._buflist = b''.join(self._buflist), []
                    _AsyncFile._notifier.clear(self, _AsyncPoller._Read)
                    self._read_coro._proceed_(buf)
                else:
                    self._read_coro.throw(IOError('timedout'))
                self._read_coro = self._read_task = None
            if self._write_coro:
                written = 0
                if self._write_task:
                    written = self._write_task.args[2]
                    if isinstance(self._write_task.args[1], memoryview):
                        self._write_task.args[1].release()
                _AsyncFile._notifier.clear(self, _AsyncPoller._Write)
                self._write_coro._proceed_(written)
                self._write_coro = self._write_task = None


class AsyncFile(_AsyncFile):
    """See _AsyncFile above.
    """
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
            buf, self._buflist = b''.join(self._buflist), []
        else:
            buf = yield self.read(size=sizehint, timeout=timeout)
            if not buf:
                raise StopIteration(buf)

        buflist = []
        while 1:
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
                    buf, self._buflist = buf[:pos+1], [buf[pos+1:]]
                raise StopIteration(buf)
            buflist.append(buf)
            buf = yield self.read(size=sizehint, timeout=timeout)
            if not buf:
                buf = b''.join(buflist)
                raise StopIteration(buf)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.close()
        return True


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
        self.first = first
        self.last = last
        if platform.system() == 'Windows':
            if not isinstance(first, Popen) or not isinstance(last, Popen):
                raise ValueError('argument must be asyncfile.Popen object')
            if first.stdin:
                self.stdin = first.stdin
            else:
                self.stdin = None
            if last.stdout:
                self.stdout = last.stdout
            else:
                self.stdout = None
            if last.stderr:
                self.stderr = last.stderr
            else:
                self.stderr = None
        else:
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

    def __getattr__(self, name):
        if self.last:
            return getattr(self.last, name)
        elif self.first:
            return getattr(self.first, name)
        else:
            raise RuntimeError('AsyncPipe is invalid')

    def write(self, buf, full=False, timeout=None):
        """Write data in buf to stdin of pipe. See 'write' method of
        AsyncFile for details.
        """
        yield self.stdin.write(buf, full=full, timeout=timeout)

    def read(self, size=0, timeout=None):
        """Read data from stdout of pipe. See 'read' method of
        AsyncFile for details.
        """
        yield self.stdout.read(size=size, timeout=timeout)

    def readline(self, size=0, sizehint=100, timeout=None):
        """Read a line from stdout of pipe. See 'readline' method of
        AsyncFile for details.
        """
        yield self.stdout.readline(size=size, sizehint=sizehint, timeout=timeout)

    def read_stderr(self, size=0, timeout=None):
        """Read data from stderr of pipe. See 'read' method of
        AsyncFile for details.
        """
        yield self.stderr.read(size=size, timeout=timeout)

    def readline_stderr(self, size=0, sizehint=100, timeout=None):
        """Read a line from stderr of pipe. See 'readline' method of
        AsyncFile for details.
        """
        yield self.stderr.readline(size=size, sizehint=sizehint, timeout=timeout)

    def communicate(self, input=None):
        """Similar to Popen's communicate. Must be used with 'yield' as
        'stdout, stderr = yield async_pipe.communicate()'

        'input' must be either data or an object with 'read' method
        (i.e., regular file object or AsyncFile object).
        """
        def write_proc(fd, input, coro=None):
            size = 16384
            if isinstance(input, str) or isinstance(input, bytes):
                n = yield fd.write(input, full=True)
                if n != len(input):
                    raise IOError('write failed')
            else:
                # TODO: how to know if 'input' is file object for
                # on-disk file?
                if hasattr(input, 'seek') and hasattr(input, 'fileno'):
                    read_func = partial_func(os.read, input.fileno())
                else:
                    read_func = input.read
                while 1:
                    data = yield read_func(size)
                    if not data:
                        break
                    if isinstance(data, str):
                        data = data.encode()
                    n = yield fd.write(data, full=True)
                    if n != len(data):
                        raise IOError('write failed')
                input.close()
            fd.close()

        def read_proc(fd, coro=None):
            size = 16384
            buflist = []
            while 1:
                buf = yield fd.read(size)
                if not buf:
                    break
                buflist.append(buf)
            fd.close()
            data = b''.join(buflist)
            raise StopIteration(data)

        if self.stdout:
            stdout_coro = Coro(read_proc, self.stdout)
        if self.stderr:
            stderr_coro = Coro(read_proc, self.stderr)
        if input and self.stdin:
            stdin_coro = Coro(write_proc, self.stdin, input)
            yield stdin_coro.finish()

        raise StopIteration((yield stdout_coro.finish()) if self.stdout else None,
                            (yield stderr_coro.finish()) if self.stderr else None)

    def poll(self):
        """Similar to 'poll' of Popen.
        """
        if self.last:
            return self.last.poll()
        elif self.first:
            return self.first.poll()

    def close(self):
        """Close pipe.
        """
        self.first = None
        self.last = None

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.close()
        return True
