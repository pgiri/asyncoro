asyncoro
********

:version: 1.4
:site: `Home`_
:pythons: 2.7+, 3.1+
:platforms: Windows, Linux, Mac OS X


asyncoro makes asynchronous programming in Python 2.7+ and Python 3.1+
easy. Using it looks like:

.. code-block:: python

    import asyncoro, resource
    def coro_proc(coro=None):
        yield coro.suspend()

    coros = [asyncoro.Coro(coro_proc) for i in xrange(100000)]
    time.sleep(5)
    ru = resource.getrusage(resource.RUSAGE_SELF)
    print('Max RSS: %.1f MB' % (ru.ru_maxrss / 1024.0))
    for coro in coros:
        coro.resume()

Read below for more details, or go `Home`_ for even more details.

**Table of Contents**

.. contents::
    :local:
    :depth: 2
    :backlinks: none

Features
========

* Asynchronous (non-blocking) sockets, for concurrent processing of
  (heavy) network traffic
* Efficient polling mechanisms epoll, kqueue, /dev/poll (and poll and
  select if necessary), and Windows I/O Completion Ports (IOCP) for
  high performance and scalability
* SSL for security
* Asynchronous timers, including non-blocking sleep
* Asynchronous locking primitives similar to Python threading module
* Message passing, for (local and remote) coroutines to exchange
  messages one-to-one or through broadcasting channels
* Remote execution of coroutines for distributed/parallel programming
  (using message passing)
* Monitoring and restarting of (local or remote) coroutines, for fault
  detection and fault-tolerance
* Hot-swapping of coroutine functions, for dynamic system
  reconfiguration
* Thread pools with asynchronous task completions, for executing time
  consuming synchronous tasks
* Asynchronous database cursor operations (using asynchronous thread pool)

License
=======

asyncoro is distriubuted under the MIT license.


.. _Home: http://asyncoro.sourceforge.net for more details.
