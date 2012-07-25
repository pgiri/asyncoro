asyncoro
========

asyncoro is a Python framework for concurrent and/or distributed
and/or network programming with asynchronous completions and
coroutines. Asynchronous completions implemented in asyncoro are
socket I/O operations (non-blocking sockets), database cursors, sleep
timers and locking primitives. Programs developed with asyncoro have
same logic and structure as Python programs with threads, except for a
few syntactic changes. asyncoro supports socket I/O notification
mechanisms epoll, kqueue, /dev/poll (and poll and select, where
necessary), and Windows I/O Completion Ports (IOCP) for high
performance and scalability, and SSL for security. asyncoro features
include remote execution of coroutines, coroutines monitoring other
coroutines, coroutines communicating with messages, message channels
etc., for concurrent, distributed, fault-tolerant programming.

asyncoro.py is to be used with Python version 2.7+ and asyncoro3.py is
to be used with Pythong version 3.1+. asyncoro has been tested with
Linux, Mac OS X and Windows. asyncoro is implemented with standard
modules in Python. Under Windows efficient polling notifier I/O
Completion Ports is supported only if pywin32
(http://pywin32.sourceforge.net) is installed; otherwise, inefficient
'select' notifier is used.

See http://asyncoro.sourceforge.net for more details.
