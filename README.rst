asyncoro
########

`asyncoro <http://asyncoro.sourceforge.net>`_ is a Python framework for
asynchronous, concurrent, distributed programming with coroutines, asynchronous
completions and message passing.

Unlike with other asynchronous frameworks, programs developed with asyncoro have
**same logic and structure** as programs with threads, except for a few
syntactic changes - mostly using *yield* with asynchronous completions that give
control to asyncoro's scheduler, which interleaves executions of coroutines,
similar to the way an operating system executes multiple processes. In addition,
asyncoro has many additional features, including message passing for
communication, distributed computing/programming etc.

Unlike threads, creating processes (coroutines) with asyncoro is very
efficient. Moreover, with asyncoro context switch occurs only when coroutines
use *yield* (typically with an asychronous call), so there is no need for
locking and there is no overhead of unnecessary context switches.

asyncoro works with Python versions 2.7+ and 3.1+. It has been tested with
Linux, Mac OS X and Windows; it may work on other platforms, too.

Features
--------

* No callbacks or event loops! No need to lock critical sections either,

* Efficient polling mechanisms epoll, kqueue, /dev/poll, Windows
  I/O Completion Ports (IOCP) for high performance and
  scalability,

* Asynchronous (non-blocking) sockets and pipes, for concurrent
  processing of I/O,

* SSL for security,

* Asynchronous locking primitives similar to Python threading module,

* Asynchronous timers and timeouts,

* `Message passing <http://en.wikipedia.org/wiki/Message_passing>`_
  for (local and remote) coroutines to exchange messages one-to-one
  with `Message Queue Pattern
  <http://en.wikipedia.org/wiki/Message_queue>`_ or through
  broadcasting channels with `Publish-Subscribe Pattern
  <http://en.wikipedia.org/wiki/Publish/subscribe>`_,

* `Location transparency
  <http://en.wikipedia.org/wiki/Location_transparency>`_ with naming
  and locating (local and remote) resources,

* Distributing computation components (code and data) for execution of
  distributed communicating processes, for wide range of use cases, covering
  `SIMD, MISD, MIMD <https://en.wikipedia.org/wiki/Flynn%27s_taxonomy>`_ system
  architectures at the process level, `web interface
  <http://asyncoro.sourceforge.net/discoro.html#client-browser-interface>`_ to
  monitor cluster/application status/performance; `in-memory processing
  <https://en.wikipedia.org/wiki/In-memory_processing>`_, data streaming,
  real-time (live) analytics and cloud computing are supported as well,

* Monitoring and restarting of (local or remote) coroutines, for fault detection
  and fault-tolerance,

* Hot-swapping of coroutine functions, for dynamic system reconfiguration,

* Thread pools with asynchronous task completions, for executing (external)
  synchronous tasks, such as reading standard input.

Dependencies
------------

asyncoro is implemented with standard modules in Python.

If `psutil <https://pypi.python.org/pypi/psutil>`_ is available on nodes, node
availability status (CPU, memory and disk) is sent in status messages, and shown
in web browser so node/application performance can be monitored.

Under Windows efficient polling notifier I/O Completion Ports (IOCP) is
supported only if `pywin32
<http://sourceforge.net/projects/pywin32/files/pywin32/>`_ is available;
otherwise, inefficient *select* notifier is used.

Installation
------------
To install asyncoro, run::

   python -m pip install asyncoro

Authors
-------
* Giridhar Pemmasani

Links
-----
* `Project page <http://asyncoro.sourceforge.net>`_.
* `Tutorial/Examples <http://asyncoro.sourceforge.net/tutorial.html>`_.
* `GitHub (Code Repository) <https://github.com/pgiri/asyncoro>`_.
