 asyncoro
##########

asyncoro is a Python framework for concurrent, distributed,
asynchronous network programming.

Unlike with other asynchronous frameworks, programs developed with
asyncoro have **same logic and structure** as programs with threads,
except for a few syntactic changes - mostly using *yield* with
asynchronous completions that give control to asyncoro's scheduler,
which interleaves executions of coroutines, similar to the way an
operating system executes multiple processes. In addition, asyncoro
has many additional features, including message passing for
communication, distributed computing/programming etc.

Unlike threads, creating processes (coroutines) with asyncoro is very
efficient. Moreover, with asyncoro context switch occurs only when
coroutines use *yield* (typically with an asychronous call), so there
is no need for locking and there is no overhead of unnecessary context
switches.

asyncoro works with Python versions 2.7+ and 3.1+. It has been tested
with Linux, Mac OS X and Windows; it may work on other platforms, too.

Features
--------

* No callbacks or event loops! No need to lock critical sections either,

* Efficient polling mechanisms epoll, kqueue, /dev/poll, Windows
  I/O Completion Ports (IOCP) for high performance and
  scalability,

* Asynchronous (non-blocking) sockets and pipes, for concurrent
  processing of I/O,

* SSL for security,

* Asynchronous timers, including non-blocking sleep,

* Asynchronous locking primitives similar to Python threading module,

* `Message passing <http://en.wikipedia.org/wiki/Message_passing>`_
  for (local and remote) coroutines to exchange messages one-to-one
  with `Message Queue Pattern
  <http://en.wikipedia.org/wiki/Message_queue>`_ or through
  broadcasting channels with `Publish-Subscribe Pattern
  <http://en.wikipedia.org/wiki/Publish/subscribe>`_,

* `Location transparency
  <http://en.wikipedia.org/wiki/Location_transparency>`_ with naming
  and locating (local and remote) resources,

* Remote execution of coroutines for distributed/parallel programming
  with Remote Coroutine Invocation and message passing,

* Monitoring and restarting of (local or remote) coroutines, for
  fault detection and fault-tolerance,

* Hot-swapping of coroutine functions, for dynamic system
  reconfiguration,

* Distributing computation fragments for remote execution of
  coroutines,

* Thread pools with asynchronous task completions, for executing
  time consuming synchronous tasks,

Installation
------------
To install asyncoro for Python 2.7+, run::

   pip install asyncoro

or to install asyncoro for Python 3.1+, run::

   pip3 install asyncoro

Dependencies
------------

asyncoro is implemented with standard modules in Python. Under Windows
efficient polling notifier I/O Completion Ports (IOCP) is supported
only if `pywin32
<http://sourceforge.net/projects/pywin32/files/pywin32/>`_ is
installed; otherwise, inefficient *select* notifier is used.


Authors
-------
* Giridhar Pemmasani

Links
-----
* `Project page <http://asyncoro.sourceforge.net>`_.
* `Tutorial/Examples <http://asyncoro.sourceforge.net/tutorial.html>`_.
