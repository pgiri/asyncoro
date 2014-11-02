"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.

This module adds API for distributed programming to AsynCoro.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2012-2014 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

import socket
import inspect
import traceback
import sys
import os
import stat
import hashlib
import random
import collections
import copy
import tempfile
import weakref

import asyncoro
from asyncoro import *

__version__ = asyncoro.__version__
__all__ = asyncoro.__all__ + ['RCI']

# if connections to a peer are not successful consecutively
# MaxConnectionErrors times, peer is assumed dead and removed
MaxConnectionErrors = 10

class _NetRequest(object):
    """Internal use only.
    """

    __slots__ = ('name', 'kwargs', 'src', 'dst', 'auth', 'event', 'id', 'reply', 'timeout')

    def __init__(self, name, kwargs={}, src=None, dst=None, auth=None, timeout=None):
        self.name = name
        self.kwargs = kwargs
        self.src = src
        self.dst = dst
        self.auth = auth
        self.id = None
        self.event = None
        self.reply = None
        self.timeout = timeout

    def __getstate__(self):
        state = {'name':self.name, 'kwargs':self.kwargs, 'src':self.src, 'dst':self.dst,
                 'auth':self.auth, 'id':self.id, 'reply':self.reply, 'timeout':self.timeout}
        return state

    def __setstate__(self, state):
        for k, v in state.iteritems():
            setattr(self, k, v)

class _Peer(object):
    """Internal use only.
    """

    __slots__ = ('name', 'location', 'auth', 'keyfile', 'certfile', 'stream', 'conn',
                 'reqs', 'reqs_pending', 'req_coro')

    peers = {}
    callback = None

    def __init__(self, name, location, auth, keyfile, certfile):
        self.name = name
        self.location = location
        self.auth = auth
        self.keyfile = keyfile
        self.certfile = certfile
        self.stream = False
        self.conn = None
        self.reqs = collections.deque()
        self.reqs_pending = Event()
        _Peer.peers[(location.addr, location.port)] = self
        self.req_coro = Coro(self.req_proc)
        if _Peer.callback:
            try:
                _Peer.callback(name, location, 1)
            except:
                logger.warning('peer status callback failed for %s@%s' % (name, location))
                logger.debug(traceback.format_exc())

    @staticmethod
    def send_req(req):
        peer = _Peer.peers.get((req.dst.addr, req.dst.port), None)
        if peer is None:
            logger.debug('invalid peer: %s, %s' % (req.dst, req.name))
            return -1
        peer.reqs.append(req)
        peer.reqs_pending.set()
        return 0

    def req_proc(self, coro=None):
        coro.set_daemon()
        conn_errors = 0
        while 1:
            if not self.reqs:
                if not self.stream and self.conn:
                    self.conn.shutdown(socket.SHUT_WR)
                    self.conn.close()
                    self.conn = None
                self.reqs_pending.clear()
                yield self.reqs_pending.wait()
            req = self.reqs.popleft()
            if not self.conn:
                self.conn = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                        keyfile=self.keyfile, certfile=self.certfile)
                if req.timeout:
                    self.conn.settimeout(req.timeout)
                try:
                    yield self.conn.connect((self.location.addr, self.location.port))
                except GeneratorExit:
                    if self.conn:
                        self.conn.shutdown(socket.SHUT_WR)
                        self.conn.close()
                        self.conn = None
                    self.req_coro = None
                    break
                except:
                    if self.conn:
                        # self.conn.shutdown(socket.SHUT_WR)
                        self.conn.close()
                        self.conn = None
                    req.reply = None
                    if req.event:
                        req.event.set()
                    conn_errors += 1
                    if conn_errors >= MaxConnectionErrors:
                        logger.warning('too many connection errors to %s; removing it' % \
                                       self.location)
                        self.req_coro = None
                        # TODO: remove from _stream_peers?
                        # AsynCoro.instance()._stream_peers.pop((self.location.addr, self.location.port), None)
                        _Peer.remove(self.location)
                        break
                    continue
                else:
                    conn_errors = 0
            else:
                self.conn.settimeout(req.timeout)

            req.auth = self.auth
            try:
                yield self.conn.send_msg(serialize(req))
                reply = yield self.conn.recv_msg()
                req.reply = unserialize(reply)
            except socket.error as exc:
                logger.debug('could not send "%s" to %s', req.name, self.location)
                # logger.debug(traceback.format_exc())
                if len(exc.args) == 1 and exc.args[0] == 'hangup':
                    logger.warning('peer "%s" not reachable' % self.location)
                    # TODO: remove peer?
                self.conn.shutdown(socket.SHUT_WR)
                self.conn.close()
                self.conn = None
                req.reply = None
            except socket.timeout:
                self.conn.shutdown(socket.SHUT_WR)
                self.conn.close()
                self.conn = None
                req.reply = None
            except GeneratorExit:
                if self.conn:
                    self.conn.shutdown(socket.SHUT_WR)
                    self.conn.close()
                    self.conn = None
                break
            except:
                # logger.debug(traceback.format_exc())
                if self.conn:
                    self.conn.shutdown(socket.SHUT_WR)
                    self.conn.close()
                    self.conn = None
                req.reply = None
            finally:
                if req.event:
                    req.event.set()

    @staticmethod
    def remove(location):
        peer = _Peer.peers.pop((location.addr, location.port), None)
        if peer:
            peer.stream = False
            if peer.req_coro:
                peer.req_coro.terminate()
                peer.req_coro = None
            if _Peer.callback:
                try:
                    _Peer.callback(peer.name, peer.location, 0)
                except:
                    logger.warning('peer status callback failed')
                    logger.debug(traceback.format_exc())

    @staticmethod
    def status_callback(callback):
        if callback:
            if inspect.isfunction(callback):
                _Peer.callback = weakref.proxy(callback,
                                               lambda ref: setattr(_Peer, 'callback', None))
                for peer in _Peer.peers.itervalues():
                    try:
                        _Peer.callback(peer.name, peer.location, 1)
                    except:
                        logger.warning('peer status callback failed')
                        logger.debug(traceback.format_exc())
            else:
                logger.warning('invalid peer status callback ignored')
        else:
            _Peer.callback = None

class RCI(object):
    """Remote Coro (Callable) Interface.

    Methods registered with RCI can be executed as coroutines on
    request (by remotely running coroutines).
    """

    __slots__ = ('_name', '_location', '_method')

    _asyncoro = None

    def __init__(self, method, name=None):
        """'method' must be generator method; this is used to create
        coroutines. If 'name' is not given, method's function name is
        used for registering.
        """
        if not inspect.isgeneratorfunction(method):
            raise RuntimeError('method must be generator function')
        self._method = method
        if name:
            self._name = name
        else:
            self._name = method.__name__
        if RCI._asyncoro is None:
            RCI._asyncoro = AsynCoro.instance()
        self._location = RCI._asyncoro._location

    @property
    def location(self):
        """Get Location instance where this RCI is running.
        """
        return copy.copy(self._location)

    @property
    def name(self):
        """Get name of RCI.
        """
        return self._name

    @staticmethod
    def locate(name, location=None, timeout=None):
        """Must be used with 'yield' as
        'rci = yield RCI.locate("name")'.

        Returns RCI instance to registered RCI at a remote peer so
        its method can be used to execute coroutines at that peer.

        If 'location' is given, RCI is looked up at that specific
        peer; otherwise, all known peers are queried for given name.
        """
        if RCI._asyncoro is None:
            RCI._asyncoro = AsynCoro.instance()
        if location is None:
            req = _NetRequest('locate_rci', kwargs={'name':name},
                              src=RCI._asyncoro._location, timeout=None)
            req.event = Event()
            req.id = id(req)
            RCI._asyncoro._requests[req.id] = req
            for (addr, port), peer in _Peer.peers.items():
                if req.event.is_set():
                    break
                yield RCI._asyncoro._async_reply(req, peer, dst=Location(addr, port))
            else:
                if (yield req.event.wait(timeout)) is False:
                    RCI._asyncoro._requests.pop(req.id, None)
                    req.reply = None
            rci = req.reply
        else:
            req = _NetRequest('locate_rci', kwargs={'name':name}, dst=location, timeout=timeout)
            rci = yield RCI._asyncoro._sync_reply(req)
        raise StopIteration(rci)

    def register(self):
        """RCI must be registered so it can be located.
        """
        if self._location != RCI._asyncoro._location:
            return -1
        if not inspect.isgeneratorfunction(self._method):
            return -1
        RCI._asyncoro._lock.acquire()
        if RCI._asyncoro._rcis.get(self._name, None) is None:
            RCI._asyncoro._rcis[self._name] = self
            RCI._asyncoro._lock.release()
            return 0
        else:
            RCI._asyncoro._lock.release()
            return -1

    def unregister(self):
        """Unregister registered RCI; see 'register' above.
        """
        if self._location != RCI._asyncoro._location:
            return -1
        RCI._asyncoro._lock.acquire()
        if RCI._asyncoro._rcis.pop(self._name, None) is None:
            RCI._asyncoro._lock.release()
            return -1
        else:
            RCI._asyncoro._lock.release()
            return 0

    def __call__(self, *args, **kwargs):
        """Must be used with 'yeild' as 'rcoro = yield rci(*args, **kwargs)'.

        Run RCI (method at remote location) with args and kwargs. Both
        args and kwargs must be serializable. Returns (remote) Coro
        instance.
        """
        req = _NetRequest('run_rci', kwargs={'name':self._name, 'args':args, 'kwargs':kwargs},
                          dst=self._location, timeout=2)
        reply = yield RCI._asyncoro._sync_reply(req)
        if isinstance(reply, Coro):
            raise StopIteration(reply)
        elif reply is None:
            raise StopIteration(None)
        else:
            raise Exception(reply)

    def __getstate__(self):
        state = {'_name':self._name, '_location':self._location}
        return state

    def __setstate__(self, state):
        self._name = state['_name']
        self._location = state['_location']

    def __repr__(self):
        s = '%s' % (self._name)
        if self._location:
            s = '%s@%s' % (s, self._location)
        return s

class AsynCoro(asyncoro.AsynCoro):
    """This adds network services to asyncoro.AsynCoro so it can
    communicate with peers.

    If 'node' is not None, it must be either hostname or IP address
    where asyncoro runs network services. If 'udp_port' is not None,
    it is port number where asyncoro runs network services. If
    'udp_port' is 0, the default port number 51350 is used. If
    multiple instances of asyncoro are to be running on same host,
    they all can be started with the same 'udp_port', so that asyncoro
    instances automatically find each other.

    'name' is used in locating peers. They must be unique. If 'name'
    is not given, it is set to string 'node:tcp_port'.

    'ext_ip_addr' is the IP address of NAT firewall/gateway if
    asyncoro is behind that firewall/gateway.

    'secret' is string that is used to hash which is used for
    authentication, so only peers that have same secret can
    communicate.

    'certfile' and 'keyfile' are path names for files containing SSL
    certificates; see Python 'ssl' module.

    'dest_path' is path to directory (folder) where transferred files
    are saved. If path doesn't exist, asyncoro creates directory with
    that path.

    'max_file_size' is maximum length of file in bytes allowed for
    transferred files. If it is 0 or None (default), there is no
    limit.
    """

    __metaclass__ = MetaSingleton
    __instance = None

    def __init__(self, udp_port=0, tcp_port=0, node=None, ext_ip_addr=None,
                 name=None, secret='', certfile=None, keyfile=None, notifier=None,
                 dest_path=None, max_file_size=None):
        if self.__class__.__instance is None:
            super(AsynCoro, self).__init__(notifier=notifier)
            self.__class__.__instance = self
            if node:
                node = socket.gethostbyname(node)
            else:
                node = socket.gethostbyname(socket.gethostname())
            if not udp_port:
                udp_port = 51350
            if not dest_path:
                dest_path = os.path.join(os.sep, tempfile.gettempdir(), 'asyncoro')
            self.dest_path = os.path.abspath(dest_path)
            # TODO: avoid race condition (use locking to check/create atomically?)
            if not os.path.isdir(self.dest_path):
                try:
                    os.makedirs(self.dest_path)
                except:
                    # likely another asyncoro created this directory
                    if not os.path.isdir(self.dest_path):
                        logger.warning('failed to create "%s"' % self.dest_path)
                        logger.debug(traceback.format_exc())
            self.max_file_size = max_file_size
            self._certfile = certfile
            self._keyfile = keyfile
            self._udp_sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
            if hasattr(socket, 'SO_REUSEADDR'):
                self._udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, 'SO_REUSEPORT'):
                self._udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            self._udp_sock.bind(('', udp_port))
            self._tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if tcp_port:
                if hasattr(socket, 'SO_REUSEADDR'):
                    self._tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if hasattr(socket, 'SO_REUSEPORT'):
                    self._tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            self._tcp_sock.bind((node, tcp_port))
            self._location = Location(*self._tcp_sock.getsockname())
            if not self._location.port:
                raise Exception('could not start network server at %s' % (self._location))
            if name:
                self._name = name
            else:
                self._name = str(self._location)
            if ext_ip_addr:
                try:
                    ext_ip_addr = socket.gethostbyname(ext_ip_addr)
                except:
                    logger.warning('invalid ext_ip_addr ignored')
                else:
                    self._location.addr = ext_ip_addr

            self._secret = secret
            if secret is None:
                self._signature = None
                self._auth_code = None
            else:
                self._signature = os.urandom(20).encode('hex')
                self._auth_code = hashlib.sha1(self._signature + secret).hexdigest()
            self._stream_peers = {}
            self._rcoros = {}
            self._rchannels = {}
            self._rcis = {}
            self._requests = {}
            self._tcp_sock.listen(32)
            logger.info('network server %s@ %s, udp_port=%s', '"%s" ' % name if name else '',
                        self._location, self._udp_sock.getsockname()[1])
            self._tcp_sock = AsyncSocket(self._tcp_sock, keyfile=self._keyfile,
                                         certfile=self._certfile)
            self._tcp_coro = Coro(self._tcp_proc)
            self._udp_coro = Coro(self._udp_proc)

    def finish(self):
        """Wait until all non-daemon coroutines finish and then
        shutdown the scheduler.
        
        Should be called from main program (or a thread, but _not_
        from coroutines).
        """
        super(AsynCoro, self).finish()
        if self._tcp_sock:
            self._tcp_sock.close()
            self._tcp_sock = None
            self._tcp_coro = None
        if self._udp_sock:
            self._udp_sock.close()
            self._udp_sock = None
            self._udp_coro = None
        self._stream_peers = {}
        self._rcoros = {}
        self._rchannels = {}
        self._rcis = {}
        self._requests = {}
        if os.path.isdir(self.dest_path) and len(os.listdir(self.dest_path)) == 0:
            os.rmdir(self.dest_path)

    def locate(self, name, timeout=None):
        """Must be used with 'yield' as
        'loc = yield scheduler.locate("peer")'.

        Find and return location of peer with 'name'.
        """
        for peer in _Peer.peers.itervalues():
            if peer.name == name:
                loc = peer.location
                break
        else:
            req = _NetRequest('locate_peer', kwargs={'name':name})
            req.event = Event()
            req.id = id(req)
            self._requests[req.id] = req
            for (addr, port), peer in _Peer.peers.items():
                if req.event.is_set():
                    break
                yield self._async_reply(req, peer, dst=Location(addr, port))
            else:
                if (yield req.event.wait(timeout)) is False:
                    self._requests.pop(req.id, None)
                    req.reply = None
            loc = req.reply
        raise StopIteration(loc)

    def peer(self, node, udp_port=0, tcp_port=0, stream_send=False):
        """Must be used with 'yield', as
        'status = yield scheduler.peer("node1")'.

        Add asyncoro running at node, udp_port as peer to
        communicate. Peers on a local network can find each other
        automatically, but if they are on different networks, 'peer'
        can be used so they find each other.

        If 'tcp_port' is set, asyncoro will contact peer at given port
        (on 'node') using TCP, so UDP is not needed to discover node.

        If 'stream_send' is True, this asyncoro uses same connection
        again and again to send messages (i.e., as a stream) to peer
        'node' (instead of one message per connection). If 'tcp_port'
        is 0, then messages to all asyncoro instances on the given
        node will be streamed. If 'tcp_port' is a port number, then
        messages to only asyncoro running on that port will be
        streamed.
        """
        try:
            node = socket.gethostbyname(node)
        except:
            logger.warning('invalid node: "%s"', str(node))
            raise StopIteration(-1)
        if not udp_port:
            udp_port = 51350
        stream_peers = [(addr, port, peer) for (addr, port), peer in _Peer.peers.iteritems() \
                        if (addr == node and (tcp_port == 0 or tcp_port == port))]
        if stream_send:
            for addr, port, peer in stream_peers:
                peer.stream = True
            self._stream_peers[(node, tcp_port)] = True
        else:
            for addr, port, peer in stream_peers:
                self._stream_peers.pop((addr, port), None)
                peer.stream = False
            self._stream_peers.pop((node, tcp_port), None)

        if (node, tcp_port) in _Peer.peers:
            req = _NetRequest('stream', kwargs={'send':stream_send},
                              src=self._location, dst=Location(node, tcp_port))
            raise StopIteration(_Peer.send_req(req))

        if tcp_port:
            req = _NetRequest('ping', kwargs={'loc':self._location, 'signature':self._signature,
                                              'name':self._name, 'version':__version__},
                              dst=Location(node, tcp_port))
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            sock.settimeout(2)
            try:
                yield sock.connect((node, tcp_port))
                yield sock.send_msg(serialize(req))
            except:
                pass
            sock.close()
        else:
            ping_msg = {'location':self._location, 'signature':self._signature,
                        'version':__version__}
            ping_msg = 'ping:' + serialize(ping_msg)
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
            sock.settimeout(2)
            try:
                yield sock.sendto(ping_msg, (node, udp_port))
            except:
                pass
            sock.close()

        raise StopIteration(0)

    def peer_status(self, callback):
        _Peer.status_callback(callback)

    def send_file(self, location, file, dir=None, overwrite=False, timeout=None):
        """Must be used with 'yield' as
        'val = yield scheduler.send_file(location, "file1")'.

        Transfer 'file' to peer at 'location'. If 'dir' is not None,
        it must be a relative path (not absolute path), in which case,
        file will be saved at peer's dest_path + dir. Returns -1 in
        case of error, 0 if the file is transferred, 1 if the same
        file is already at the destination with same size, timestamp
        and permissions (so file is not transferred) and os.stat
        structure if a file with same name is at the destination with
        different size/timestamp/permissions, but 'overwrite' is
        False. If return value is 0, the sender may want to delete
        file with 'del_file' later.
        """
        try:
            stat_buf = os.stat(file)
        except:
            raise StopIteration(-1)
        if not ((stat.S_IMODE(stat_buf.st_mode) & stat.S_IREAD) and stat.S_ISREG(stat_buf.st_mode)):
            raise StopIteration(-1)
        if isinstance(dir, str) and dir:
            dir = dir.strip()
            # reject absolute path for dir
            if os.path.join(os.sep, dir) == dir:
                raise StopIteration(-1)
        peer = _Peer.peers.get((location.addr, location.port), None)
        if peer is None:
            logger.debug('%s is not a valid peer', location)
            raise StopIteration(-1)
        kwargs = {'file':os.path.basename(file), 'stat_buf':stat_buf,
                  'overwrite':overwrite == True, 'dir':dir}
        req = _NetRequest('send_file', kwargs=kwargs, dst=location, timeout=timeout)
        sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                           keyfile=self._keyfile, certfile=self._certfile)
        try:
            yield sock.connect((location.addr, location.port))
            req.auth = peer.auth
            yield sock.send_msg(serialize(req))
            reply = yield sock.recv_msg()
            reply = unserialize(reply)
            if reply == 0:
                fd = open(file, 'rb')
                while True:
                    data = fd.read(10240000)
                    if not data:
                        break
                    yield sock.sendall(data)
                fd.close()
                resp = yield sock.recv_msg()
                resp = unserialize(resp)
                if resp == 0:
                    reply = 0
                else:
                    reply = -1
        except socket.error as exc:
            reply = -1
            logger.debug('could not send "%s" to %s', req.name, location)
            if len(exc.args) == 1 and exc.args[0] == 'hangup':
                logger.warning('peer "%s" not reachable' % location)
                # TODO: remove peer?
        except socket.timeout:
            raise StopIteration(-1)
        except:
            reply = -1
        finally:
            sock.close()
        raise StopIteration(reply)

    def del_file(self, location, file, dir=None, timeout=None):
        """Must be used with 'yield' as
        'loc = yield scheduler.del_file(location, "file1")'.

        Delete 'file' from peer at 'location'. 'dir' must be
        same as that used for 'send_file'.
        """
        if isinstance(dir, str) and dir:
            dir = dir.strip()
            # reject absolute path for dir
            if os.path.join(os.sep, dir) == dir:
                raise StopIteration(-1)
        kwargs = {'file':os.path.basename(file), 'dir':dir}
        req = _NetRequest('del_file', kwargs=kwargs, dst=location, timeout=timeout)
        reply = yield self._sync_reply(req)
        if reply is None:
            reply = -1
        raise StopIteration(reply)

    def _udp_proc(self, coro=None):
        """Internal use only.
        """
        coro.set_daemon()
        ping_sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
        ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        ping_sock.settimeout(2)
        ping_msg = {'location':self._location, 'signature':self._signature, 'version':__version__}
        ping_msg = 'ping:' + serialize(ping_msg)
        try:
            yield ping_sock.sendto(ping_msg, ('<broadcast>', self._udp_sock.getsockname()[1]))
        except:
            pass
        ping_sock.close()

        while True:
            msg, addr = yield self._udp_sock.recvfrom(1024)
            if not msg.startswith('ping:'):
                logger.warning('ignoring UDP message from %s:%s', addr[0], addr[1])
                continue
            try:
                info = unserialize(msg[len('ping:'):])
                assert info['version'] == __version__
                req_peer = info['location']
                if self._secret is None:
                    auth_code = None
                else:
                    auth_code = hashlib.sha1(info['signature'] + self._secret).hexdigest()
                if info['location'] == self._location:
                    continue
                peer = _Peer.peers.get((req_peer.addr, req_peer.port), None)
                if peer and peer.auth == auth_code:
                    continue
            except:
                continue

            req = _NetRequest('ping', kwargs={'loc':self._location, 'signature':self._signature,
                                              'name':self._name, 'version':__version__},
                              dst=req_peer, auth=auth_code)
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                               keyfile=self._keyfile, certfile=self._certfile)
            sock.settimeout(2)
            try:
                yield sock.connect((req_peer.addr, req_peer.port))
                yield sock.send_msg(serialize(req))
            except:
                pass
            sock.close()

    def _tcp_proc(self, coro=None):
        """Internal use only.
        """
        coro.set_daemon()
        while True:
            conn, addr = yield self._tcp_sock.accept()
            Coro(self._tcp_task, conn, addr)

    def _tcp_task(self, conn, addr, coro=None):
        """Internal use only.
        """
        while True:
            try:
                msg = yield conn.recv_msg()
            except:
                break
            if not msg:
                break
            req = None
            try:
                req = unserialize(msg)
                assert req.auth == self._auth_code
            except:
                if not req:
                    logger.debug('invalid message from %s:%s' % (addr[0], addr[1]))
                    break
                if req.name != 'ping':
                    logger.warning('invalid request %s from %s ignored: "%s", "%s"',
                                   req.name, req.src, req.auth, self._auth_code)
                    break

            if req.dst is not None and req.dst != self._location:
                logger.debug('invalid request "%s" to %s, %s (%s), %s',
                             req.name, req.src, req.dst, self._location, req.id)
                break

            if req.src == self._location:
                async_reply = req
                req = self._requests.pop(async_reply.id, None)
                if req is None:
                    logger.debug('ignoring request "%s"/%s', async_reply.name, async_reply.id)
                    break
                req.reply = async_reply.reply
                del async_reply
                req.event.set()
                break

            if req.name == 'send':
                # synchronous message
                assert req.src is None
                reply = -1
                if req.dst != self._location:
                    logger.warning('ignoring invalid "send" (%s != %s)' % (req.dst, self._location))
                else:
                    coro = req.kwargs.get('coro', None)
                    if coro is not None:
                        coro = self._coros.get(int(coro), None)
                        if isinstance(coro, Coro):
                            reply = coro.send(req.kwargs['message'])
                        else:
                            logger.warning('ignoring message to invalid coro %s',
                                           req.kwargs['coro'])
                    else:
                        channel = req.kwargs.get('channel', None)
                        if channel is not None:
                            channel = self._channels.get(channel, None)
                            if isinstance(channel, Channel):
                                reply = channel.send(req.kwargs['message'])
                            else:
                                logger.warning('ignoring message to channel "%s"',
                                               req.kwargs['channel'])
                        else:
                            logger.warning('ignoring invalid recipient to "send"')
                yield conn.send_msg(serialize(reply))
            elif req.name == 'deliver':
                # synchronous message
                assert req.src is None
                reply = -1
                if req.dst != self._location:
                    logger.warning('ignoring invalid "deliver" (%s != %s)' % (req.dst, self._location))
                else:
                    coro = req.kwargs.get('coro', None)
                    if coro is not None:
                        coro = self._coros.get(int(coro), None)
                        if isinstance(coro, Coro):
                            coro.send(req.kwargs['message'])
                            reply = 1
                        else:
                            logger.warning('ignoring message to invalid coro %s',
                                           req.kwargs['coro'])
                    else:
                        channel = req.kwargs.get('channel', None)
                        if channel is not None:
                            channel = self._channels.get(channel, None)
                            if isinstance(channel, Channel):
                                reply = yield channel.deliver(
                                    req.kwargs['message'], timeout=req.timeout, n=req.kwargs['n'])
                            else:
                                logger.warning('ignoring message to channel "%s"',
                                               req.kwargs['channel'])
                        else:
                            logger.warning('ignoring invalid recipient to "send"')
                yield conn.send_msg(serialize(reply))
            elif req.name == 'run_rci':
                # synchronous message
                assert req.src is None
                if req.dst != self._location:
                    reply = Exception('invalid RCI invocation')
                else:
                    rci = self._rcis.get(req.kwargs['name'], None)
                    if rci is None:
                        reply = Exception('RCI "%s" is not registered' % req.kwargs['name'])
                    else:
                        args = req.kwargs['args']
                        kwargs = req.kwargs['kwargs']
                        try:
                            reply = Coro(rci._method, *args, **kwargs)
                        except:
                            reply = Exception(traceback.format_exc())
                yield conn.send_msg(serialize(reply))
            elif req.name == 'locate_channel':
                channel = self._rchannels.get(req.kwargs['name'], None)
                if channel is not None or req.dst == self._location:
                    if req.src:
                        peer = _Peer.peers.get((req.src.addr, req.src.port), None)
                        if peer:
                            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                               keyfile=self._keyfile, certfile=self._certfile)
                            try:
                                yield sock.connect((req.src.addr, req.src.port))
                                req.auth = peer.auth
                                req.reply = channel
                                yield sock.send_msg(serialize(req))
                            except:
                                pass
                            sock.close()
                    else:
                        yield conn.send_msg(serialize(channel))
            elif req.name == 'locate_coro':
                coro = self._rcoros.get(req.kwargs['name'], None)
                if coro is not None or req.dst == self._location:
                    if req.src:
                        peer = _Peer.peers.get((req.src.addr, req.src.port), None)
                        if peer:
                            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                               keyfile=self._keyfile, certfile=self._certfile)
                            try:
                                yield sock.connect((req.src.addr, req.src.port))
                                req.auth = peer.auth
                                req.reply = coro
                                yield sock.send_msg(serialize(req))
                            except:
                                pass
                            sock.close()
                    else:
                        yield conn.send_msg(serialize(coro))
            elif req.name == 'locate_rci':
                rci = self._rcis.get(req.kwargs['name'], None)
                if rci is not None or req.dst == self._location:
                    if req.src:
                        peer = _Peer.peers.get((req.src.addr, req.src.port), None)
                        if peer:
                            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                               keyfile=self._keyfile, certfile=self._certfile)
                            try:
                                yield sock.connect((req.src.addr, req.src.port))
                                req.auth = peer.auth
                                req.reply = rci
                                yield sock.send_msg(serialize(req))
                            except:
                                pass
                            sock.close()
                    else:
                        yield conn.send_msg(serialize(rci))
            elif req.name == 'monitor':
                # synchronous message
                assert req.src is None
                assert req.dst == self._location
                reply = -1
                monitor = req.kwargs.get('monitor', None)
                coro = req.kwargs.get('coro', None)
                if coro is not None:
                    coro = self._coros.get(int(coro), None)
                if isinstance(coro, Coro) and isinstance(monitor, Coro):
                    assert monitor._location != self._location
                    reply = self._monitor(monitor, coro)
                yield conn.send_msg(serialize(reply))
            elif req.name == 'terminate_coro':
                reply = -1
                coro = req.kwargs.get('coro', None)
                if coro is not None:
                    coro = self._coros.get(int(coro), None)
                if isinstance(coro, Coro):
                    coro.terminate()
                    reply = 0
                yield conn.send_msg(serialize(reply))
            elif req.name == 'ping':
                try:
                    assert req.kwargs['version'] == __version__
                    assert req.kwargs['name']
                    peer_loc = req.kwargs['loc']
                    if self._secret is None:
                        auth_code = None
                    else:
                        auth_code = hashlib.sha1(req.kwargs['signature'] + self._secret).hexdigest()
                except:
                    # logger.debug(traceback.format_exc())
                    break
                if peer_loc == self._location:
                    break
                peer = _Peer.peers.get((peer_loc.addr, peer_loc.port), None)
                if peer and peer.auth == auth_code:
                    logger.debug('%s: ignoring peer: %s' % (self._location, peer_loc))
                    break
                pong = _NetRequest('pong',
                                   kwargs={'loc':self._location, 'signature':self._signature,
                                           'name':self._name, 'version':__version__},
                                   dst=peer_loc, auth=auth_code)
                sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                   keyfile=self._keyfile, certfile=self._certfile)
                sock.settimeout(2)
                try:
                    yield sock.connect((peer_loc.addr, peer_loc.port))
                    yield sock.send_msg(serialize(pong))
                    reply = yield sock.recv_msg()
                    assert reply == 'ack'
                except:
                    logger.debug('%s: ignoring peer: %s' % (self._location, peer_loc))
                    break
                finally:
                    sock.close()

                logger.debug('%s: found asyncoro "%s" at %s' % (self._location, req.kwargs['name'],
                                                                peer_loc))
                if (peer_loc.addr, peer_loc.port) in _Peer.peers:
                    break
                peer = _Peer(req.kwargs['name'], peer_loc, auth_code, self._keyfile, self._certfile)
                if (peer_loc.addr, peer_loc.port) in self._stream_peers or \
                       (peer_loc.addr, 0) in self._stream_peers:
                    peer.stream = True

                for loc_req in self._requests.itervalues():
                    if loc_req.name == 'locate_peer' and \
                           loc_req.kwargs['name'] == req.kwargs['name']:
                        loc_req.reply = peer.location
                        loc_req.event.set()
                        break

                # send pending (async) requests
                pending_reqs = [(i, req) for i, req in self._requests.iteritems() \
                                if req.dst is None or req.dst == peer_loc]
                for rid, pending_req in pending_reqs:
                    sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                       keyfile=self._keyfile, certfile=self._certfile)
                    if pending_req.timeout:
                        sock.settimeout(pending_req.timeout)
                    try:
                        yield sock.connect((peer_loc.addr, peer_loc.port))
                        pending_req.auth = auth_code
                        yield sock.send_msg(serialize(pending_req))
                    except:
                        # logger.debug(traceback.format_exc())
                        pass
                    sock.close()
            elif req.name == 'pong':
                try:
                    assert req.kwargs['version'] == __version__
                    assert req.kwargs['name']
                    peer_loc = req.kwargs['loc']
                    if self._secret is None:
                        auth_code = None
                    else:
                        auth_code = hashlib.sha1(req.kwargs['signature'] + self._secret).hexdigest()
                    # assert peer_loc == req.src
                    peer = _Peer.peers.get((peer_loc.addr, peer_loc.port), None)
                    if peer and peer.auth == auth_code:
                        logger.debug('%s: ignoring peer: %s' % (self._location, peer_loc))
                        yield conn.send_msg('nak')
                        break
                    yield conn.send_msg('ack')
                except:
                    logger.debug('%s: ignoring peer: %s' % (self._location, peer_loc))
                    # logger.debug(traceback.format_exc())
                    break

                logger.debug('%s: found asyncoro "%s" at %s' % (self._location, req.kwargs['name'],
                                                                peer_loc))
                if (peer_loc.addr, peer_loc.port) in _Peer.peers:
                    break
                peer = _Peer(req.kwargs['name'], peer_loc, auth_code, self._keyfile, self._certfile)
                if (peer_loc.addr, peer_loc.port) in self._stream_peers or \
                       (peer_loc.addr, 0) in self._stream_peers:
                    peer.stream = True

                # send pending (async) requests
                pending_reqs = [(i, req) for i, req in self._requests.iteritems() \
                                if req.dst is None or req.dst == peer_loc]
                for rid, pending_req in pending_reqs:
                    sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                       keyfile=self._keyfile, certfile=self._certfile)
                    if pending_req.timeout:
                        sock.settimeout(pending_req.timeout)
                    try:
                        yield sock.connect((peer_loc.addr, peer_loc.port))
                        pending_req.auth = auth_code
                        yield sock.send_msg(serialize(pending_req))
                    except:
                        # logger.debug(traceback.format_exc())
                        pass
                    sock.close()
            elif req.name == 'subscribe':
                # synchronous message
                assert req.src is None
                assert req.dst == self._location
                reply = -1
                channel = req.kwargs.get('channel', None)
                if channel is not None:
                    channel = self._channels.get(channel, None)
                    if isinstance(channel, Channel) and channel._location == self._location:
                        subscriber = req.kwargs.get('subscriber', None)
                        if subscriber is not None:
                            if isinstance(subscriber, Coro):
                                if subscriber._location == self._location:
                                    subscriber = self._coros.get(int(subscriber._id), None)
                                reply = yield channel.subscribe(subscriber)
                            elif isinstance(subsriber, Channel):
                                if subscriber._location == self._location:
                                    subscriber = self._channels.get(subscriber._name, None)
                                reply = yield channel.subscribe(subscriber)
                yield conn.send_msg(serialize(reply))
            elif req.name == 'unsubscribe':
                # synchronous message
                assert req.src is None
                assert req.dst == self._location
                reply = -1
                channel = req.kwargs.get('channel', None)
                if channel is not None:
                    channel = self._channels.get(channel, None)
                    if isinstance(channel, Channel) and channel._location == self._location:
                        subscriber = req.kwargs.get('subscriber', None)
                        if subscriber is not None:
                            if isinstance(subscriber, Coro):
                                if subscriber._location == self._location:
                                    subscriber = self._coros.get(int(subscriber._id), None)
                                reply = yield channel.unsubscribe(subscriber)
                            elif isinstance(subsriber, Channel):
                                if subscriber._location == self._location:
                                    subscriber = self._channels.get(subscriber._name, None)
                                reply = yield channel.unsubscribe(subscriber)
                yield conn.send_msg(serialize(reply))
            elif req.name == 'locate_peer':
                if req.kwargs['name'] == self._name or req.dst == self._location:
                    if req.kwargs['name'] == self._name:
                        loc = self._location
                    elif req.dst == self._location:
                        loc = None
                    if req.src:
                        peer = _Peer.peers.get((req.src.addr, req.src.port), None)
                        if peer:
                            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                               keyfile=self._keyfile, certfile=self._certfile)
                            try:
                                yield sock.connect((req.src.addr, req.src.port))
                                req.auth = peer.auth
                                req.reply = loc
                                yield sock.send_msg(serialize(req))
                            except:
                                pass
                            sock.close()
                    else:
                        yield conn.send_msg(serialize(loc))
            elif req.name == 'send_file':
                # synchronous message
                assert req.src is None
                assert req.dst == self._location
                tgt = os.path.basename(req.kwargs['file'])
                dir = req.kwargs['dir']
                if isinstance(dir, str):
                    tgt = os.path.join(dir, tgt)
                tgt = os.path.abspath(os.path.join(self.dest_path, tgt))
                stat_buf = req.kwargs['stat_buf']
                resp = 0
                if self.max_file_size and stat_buf.st_size > self.max_file_size:
                    logger.warning('file "%s" too big (%s) - must be smaller than %s',
                                   req.kwargs['file'], stat_buf.st_size, self.max_file_size)
                    resp = -1
                elif not tgt.startswith(self.dest_path):
                    resp = -1
                elif os.path.isfile(tgt):
                    sbuf = os.stat(tgt)
                    if abs(stat_buf.st_mtime - sbuf.st_mtime) <= 1 and \
                           stat_buf.st_size == sbuf.st_size and \
                           stat.S_IMODE(stat_buf.st_mode) == stat.S_IMODE(sbuf.st_mode):
                        resp = 1
                    elif not req.kwargs['overwrite']:
                        resp = sbuf

                if resp == 0:
                    try:
                        if not os.path.isdir(os.path.dirname(tgt)):
                            os.makedirs(os.path.dirname(tgt))
                        fd = open(tgt, 'wb')
                    except:
                        logger.debug('failed to create "%s" : %s', tgt, traceback.format_exc())
                        resp = -1
                yield conn.send_msg(serialize(resp))
                if resp == 0:
                    n = 0
                    try:
                        while n < stat_buf.st_size:
                            data = yield conn.recvall(min(stat_buf.st_size-n, 10240000))
                            if not data:
                                break
                            fd.write(data)
                            n += len(data)
                    except:
                        logger.warning('copying file "%s" failed', tgt)
                    fd.close()
                    if n < stat_buf.st_size:
                        os.remove(tgt)
                        resp = -1
                    else:
                        resp = 0
                        logger.debug('saved file %s', tgt)
                        os.utime(tgt, (stat_buf.st_atime, stat_buf.st_mtime))
                        os.chmod(tgt, stat.S_IMODE(stat_buf.st_mode))
                    yield conn.send_msg(serialize(resp))
            elif req.name == 'del_file':
                # synchronous message
                assert req.src is None
                assert req.dst == self._location
                tgt = os.path.basename(req.kwargs['file'])
                dir = req.kwargs['dir']
                if isinstance(dir, str) and dir:
                    tgt = os.path.join(dir, tgt)
                tgt = os.path.join(self.dest_path, tgt)
                if tgt.startswith(self.dest_path) and os.path.isfile(tgt):
                    os.remove(tgt)
                    d = os.path.dirname(tgt)
                    try:
                        while d > self.dest_path and os.path.isdir(d):
                            os.rmdir(d)
                            d = os.path.dirname(d)
                    except:
                        # logger.debug(traceback.format_exc())
                        pass
                    reply = 0
                else:
                    reply = -1
                yield conn.send_msg(serialize(reply))
            elif req.name == 'peer_closed':
                # synchronous message
                assert req.src is None
                peer_loc = req.kwargs.get('loc', None)
                if peer_loc:
                    logger.debug('peer %s terminated' % (peer_loc))
                    # TODO: remove from _stream_peers?
                    # self._stream_peers.pop((peer_loc.addr, peer_loc.port), None)
                    _Peer.remove(peer_loc)
                try:
                    yield conn.send_msg(serialize('ack'))
                except:
                    pass
                break
            elif req.name == 'stream':
                yield conn.send_msg(serialize('ack'))
                # if not req.kwargs['send']:
                #     break
            else:
                logger.warning('invalid request "%s" ignored', req.name)

        conn.close()

    def _async_reply(self, req, peer, dst=None):
        """Internal use only.
        """
        if dst is None:
            dst = req.dst
        sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                           keyfile=self._keyfile, certfile=self._certfile)
        if req.timeout:
            sock.settimeout(req.timeout)
        try:
            yield sock.connect((dst.addr, dst.port))
            req.auth = peer.auth
            yield sock.send_msg(serialize(req))
        except socket.error as exc:
            logger.debug('could not send "%s" to %s', req.name, dst)
            if len(exc.args) == 1 and exc.args[0] == 'hangup':
                logger.warning('peer "%s" not reachable' % dst)
                # TODO: remove peer?
        except:
            logger.debug('could not send "%s" to %s', req.name, dst)
        sock.close()

    def _sync_reply(self, req, alarm_value=None):
        """Internal use only.
        """
        assert req.src is None
        req.event = Event()
        if _Peer.send_req(req) != 0:
            raise StopIteration(-1)
        if (yield req.event.wait(req.timeout)) is False:
            raise StopIteration(alarm_value)
        raise StopIteration(req.reply)

    def _register_channel(self, channel, name):
        """Internal use only.
        """
        cur = self._rchannels.get(name, None)
        if cur is None or self._channels.get(cur.name, None) is None:
            self._rchannels[name] = channel
            return 0
        else:
            logger.warning('channel "%s" is already registered', name)
            return -1

    def _unregister_channel(self, channel, name):
        """Internal use only.
        """
        if self._rchannels.pop(name, None) is channel:
            return 0
        else:
            # logger.warning('channel "%s" is not registered', name)
            return -1

    def _register_coro(self, coro, name):
        """Internal use only.
        """
        cur = self._rcoros.get(name, None)
        if cur is None or self._coros.get(cur._id, None) is None:
            self._rcoros[name] = coro
            return 0
        else:
            logger.warning('coro "%s" is already registered', name)
            return -1

    def _unregister_coro(self, coro, name):
        """Internal use only.
        """
        if self._rcoros.pop(name, None) is coro:
            return 0
        else:
            # logger.warning('coro "%s" is not registered', name)
            return -1

    def __repr__(self):
        s = str(self._location)
        if s == self._name:
            return s
        else:
            return '"%s" @ %s' % (self._name, s)

asyncoro._NetRequest = _NetRequest
asyncoro._Peer = _Peer
asyncoro.AsynCoro = AsynCoro
