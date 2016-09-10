"""This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.

This module adds API for distributed programming to AsynCoro.
"""

import socket
import inspect
import traceback
import os
import stat
import hashlib
import collections
import copy
import tempfile
import threading
import errno
import atexit
try:
    import netifaces
except ImportError:
    netifaces = None

import asyncoro
from asyncoro import *

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2012-2014 Giridhar Pemmasani"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

__version__ = asyncoro.__version__
__all__ = asyncoro.__all__ + ['RCI']

# if connections to a peer are not successful consecutively
# MaxConnectionErrors times, peer is assumed dead and removed
MaxConnectionErrors = 10
MsgTimeout = asyncoro.MsgTimeout


class PeerStatus(object):
    """'peer_status' method of AsynCoro can be used to be notified of
    status of peers (other AsynCoro's to communicate for distributed
    programming). The status notifications are sent as messages to the
    regisered coroutine. Each message is an instance of this class.
    """

    Online = 1
    Offline = 0

    def __init__(self, location, name, status):
        self.location = location
        self.name = name
        self.status = status


class AsynCoro(asyncoro.AsynCoro, metaclass=Singleton):
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

    If 'discover_peers' is True (default), this node broadcasts message to
    detect other peers. If it is False, message is not broadcasted.

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

    _instance = None
    _sys_asyncoro = None

    def __init__(self, *args, **kwargs):
        AsynCoro._instance = self
        atexit.register(self.finish)
        super(self.__class__, self).__init__()
        RCI._asyncoro = _SysAsynCoro_._asyncoro = self
        self._sys_asyncoro = _SysAsynCoro_(*args, **kwargs)
        self.__class__._sys_asyncoro = self._sys_asyncoro
        self._location = self._sys_asyncoro._location
        self._name = self._sys_asyncoro._name
        self._certfile = self._sys_asyncoro._certfile
        self._keyfile = self._sys_asyncoro._keyfile
        self.__dest_path_prefix = self.__dest_path = self._sys_asyncoro.dest_path

    @classmethod
    def instance(cls, *args, **kwargs):
        """Returns (singleton) instance of AsynCoro.
        """
        if not cls._instance:
            cls._instance = cls(*args, **kwargs)
        return cls._instance

    @property
    def dest_path(self):
        return self.__dest_path

    @dest_path.setter
    def dest_path(self, path):
        if not self._sys_asyncoro:
            raise ValueError('AsynCoro not initialized!')
        path = os.path.normpath(path)
        if not path.startswith(self.__dest_path_prefix):
            path = os.path.join(self.__dest_path_prefix,
                                os.path.splitdrive(path)[1].lstrip(os.sep))
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
        self._sys_asyncoro.dest_path = self.__dest_path = path

    def finish(self):
        if AsynCoro._instance:
            super(self.__class__, self).finish()
            AsynCoro._instance = None
            self._sys_asyncoro.finish()
            self._notifier.terminate()
            logger.shutdown()

    def locate(self, name, timeout=None):
        """Must be used with 'yield' as
        'loc = yield scheduler.locate("peer")'.

        Find and return location of peer with 'name'.
        """
        if not self._sys_asyncoro:
            raise StopIteration(None)
        _Peer._lock.acquire()
        for peer in _Peer.peers.values():
            if peer.name == name:
                loc = peer.location
                break
        else:
            loc = None
        _Peer._lock.release()
        if not loc:
            req = _NetRequest('locate_peer', kwargs={'name': name})
            req.event = Event()
            req_id = id(req)
            self._lock.acquire()
            self._pending_reqs[req_id] = req
            self._lock.release()
            _Peer.send_req_to(req, None)
            if (yield req.event.wait(timeout)) is False:
                req.reply = None
            loc = req.reply
            self._lock.acquire()
            self._pending_reqs.pop(req_id, None)
            self._lock.release()
        raise StopIteration(loc)

    def peer(self, loc, udp_port=0, stream_send=False, broadcast=False):
        """Must be used with 'yield', as
        'status = yield scheduler.peer("loc")'.

        Add asyncoro running at 'loc' as peer to communicate. Peers on
        a local network can find each other automatically, but if they
        are on different networks, 'peer' can be used so they find
        each other. 'loc' can be either an instance of Location or
        host name or IP address. If 'loc' is Location instance and
        'port' is 0, or 'loc' is host name or IP address, then all
        asyncoros running at the host will have streaming mode set as
        per 'stream_send'.

        If 'stream_send' is True, this asyncoro uses same connection
        again and again to send messages (i.e., as a stream) to peer
        'host' (instead of one message per connection).

        If 'broadcast' is True, the client information is broadcast on
        the network of peer. This can be used if client is on remote
        network and needs to communicate with all asyncoro's available
        on the network of peer (at 'loc').
        """

        if not self._sys_asyncoro:
            raise StopIteration(-1)

        def _peer(coro=None):
            SysCoro(self._sys_asyncoro.peer, coro, loc, udp_port, stream_send, broadcast)
            yield coro.recv()

        yield Coro(_peer).finish()

    def peer_status(self, coro):
        """This method can be used to be notified of status of peers
        (other AsynCoro's to communicate for distributed
        programming). The status notifications are sent as messages to
        the regisered coroutine. Each message is an instance of
        PeerStatus.
        """
        _Peer.peer_status(coro)

    def peers(self):
        """Returns list of current peers (as Location instances).
        """
        return _Peer.get_peers()

    def close_peer(self, location, timeout=MsgTimeout):
        """Must be used with 'yield', as
        'yield scheduler.close_peer("loc")'.

        Close peer at 'location'.
        """
        if isinstance(location, Location):
            _Peer._lock.acquire()
            peer = _Peer.peers.get((location.addr, location.port), None)
            _Peer._lock.release()
            if peer:

                def sys_proc(peer, timeout, done, coro=None):
                    yield _Peer.close_peer(peer, timeout=timeout, coro=coro)
                    done.set()

                event = Event()
                SysCoro(sys_proc, peer, timeout, event)
                yield event.wait()

    def ignore_peers(self, ignore):
        """Don't respond to 'ping' from peers if 'ignore=True'. This can be used
        during shutdown, or to limit peers to communicate.
        """
        if self._sys_asyncoro:
            self._sys_asyncoro.ignore_peers(ignore)

    def discover_peers(self, port=None):
        """This method can be invoked (periodically?) to broadcast message to
        discover peers, if there is a chance initial broadcast message may be
        lost (as these messages are sent over UDP).
        """
        if not self._sys_asyncoro:
            return
        SysCoro(self._sys_asyncoro.discover_peers, port=port)

    def send_file(self, location, file, dir=None, overwrite=False, timeout=MsgTimeout):
        """Must be used with 'yield' as
        'val = yield scheduler.send_file(location, "file1")'.

        Transfer 'file' to peer at 'location'. If 'dir' is not None, it must be
        a relative path (not absolute path), in which case, file will be saved
        at peer's dest_path + dir. Returns -1 in case of error, 0 if the file is
        transferred, 1 if the same file is already at the destination with same
        size, timestamp and permissions (so file is not transferred) and os.stat
        structure if a file with same name is at the destination with different
        size/timestamp/permissions, but 'overwrite' is False. 'timeout' is max
        seconds to transfer 1MB of data. If return value is 0, the sender may
        want to delete file with 'del_file' later.
        """
        try:
            stat_buf = os.stat(file)
        except:
            logger.warning('send_file: File "%s" is not valid', file)
            raise StopIteration(-1)
        if not ((stat.S_IMODE(stat_buf.st_mode) & stat.S_IREAD) and stat.S_ISREG(stat_buf.st_mode)):
            logger.warning('send_file: File "%s" is not valid', file)
            raise StopIteration(-1)
        if dir:
            if not isinstance(dir, str):
                logger.warning('send_file: path for dir "%s" is not allowed', dir)
                raise StopIteration(-1)
            dir = dir.strip()
            # reject absolute path for dir
            if os.path.join(os.sep, dir) == dir:
                logger.warning('send_file: Absolute path for dir "%s" is not allowed', dir)
                raise StopIteration(-1)
        else:
            dir = None
        peer = _Peer.get_peer(location)
        if peer is None:
            logger.debug('%s is not a valid peer', location)
            raise StopIteration(-1)
        kwargs = {'file': os.path.basename(file), 'stat_buf': stat_buf,
                  'overwrite': overwrite is True, 'dir': dir, 'sep': os.sep}
        req = _NetRequest('send_file', kwargs=kwargs, dst=location, timeout=timeout)
        sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                           keyfile=self._keyfile, certfile=self._certfile)
        if timeout:
            sock.settimeout(timeout)
        fd = open(file, 'rb')
        try:
            yield sock.connect((location.addr, location.port))
            req.auth = peer.auth
            yield sock.send_msg(serialize(req))
            recvd = yield sock.recv_msg()
            recvd = deserialize(recvd)
            sent = 0
            while sent == recvd:
                data = fd.read(1024000)
                if not data:
                    break
                yield sock.sendall(data)
                sent += len(data)
                recvd = yield sock.recv_msg()
                recvd = deserialize(recvd)
            if recvd == stat_buf.st_size:
                reply = 0
            else:
                reply = -1
        except socket.error as exc:
            reply = -1
            logger.debug('could not send "%s" to %s', req.name, location)
            if len(exc.args) == 1 and exc.args[0] == 'hangup':
                logger.warning('peer "%s" not reachable', location)
                # TODO: remove peer?
        except:
            logger.warning('send_file: Could not send "%s" to %s', file, location)
            reply = -1
        finally:
            sock.close()
            fd.close()
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
        kwargs = {'file': os.path.basename(file), 'dir': dir}
        req = _NetRequest('del_file', kwargs=kwargs, dst=location, timeout=timeout)
        reply = yield _Peer._sync_reply(req)
        if reply is None:
            reply = -1
        raise StopIteration(reply)

    def _sys_call_(self, method, *args, **kwargs):
        swing = {'result': None, 'event': Event()}
        SysCoro(self._sys_asyncoro._swing_call_, swing, method, *args, **kwargs)
        yield swing['event'].wait()
        yield swing['result']


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
        if not RCI._asyncoro:
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
        if not RCI._asyncoro:
            RCI._asyncoro = AsynCoro.instance()
        req = _NetRequest('locate_rci', kwargs={'name': name}, dst=location, timeout=timeout)
        req.event = Event()
        req_id = id(req)
        RCI._asyncoro._lock.acquire()
        RCI._asyncoro._pending_reqs[req_id] = req
        RCI._asyncoro._lock.release()
        _Peer.send_req_to(req, location)
        if (yield req.event.wait(timeout)) is False:
            req.reply = None
        rci = req.reply
        RCI._asyncoro._lock.acquire()
        RCI._asyncoro._pending_reqs.pop(req_id, None)
        RCI._asyncoro._lock.release()
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
        req = _NetRequest('run_rci', kwargs={'name': self._name, 'args': args, 'kwargs': kwargs},
                          dst=self._location, timeout=MsgTimeout)
        reply = yield _Peer._sync_reply(req)
        if isinstance(reply, Coro):
            raise StopIteration(reply)
        elif reply is None:
            raise StopIteration(None)
        else:
            raise Exception(reply)

    def __getstate__(self):
        state = {'_name': self._name, '_location': self._location}
        return state

    def __setstate__(self, state):
        self._name = state['_name']
        self._location = state['_location']

    def __eq__(self, other):
        return (isinstance(other, RCI) and
                self._name == other._name and self._location == other._location)

    def __ne__(self, other):
        return ((not isinstance(other, RCI)) or
                self._name != other._name or self._location != other._location)

    def __repr__(self):
        s = '%s' % (self._name)
        if self._location:
            s = '%s@%s' % (s, self._location)
        return s


class SysCoro(asyncoro.Coro):
    """Coroutine meant for reactive components, always ready to respond to
    events; i.e., takes very little CPU time to process events. This is meant
    for internal use to handle network traffic, timer events etc. in asyncoro
    modules. These coroutines run in seperate AsynCoro thread (_SysAsynCoro_),
    so if user coroutines (Coro instances) take too much CPU time, SysCoro can
    still respond to such events quickly.

    This class is not meant for users, as using this improperly may break
    (parts of) asyncoro.
    """

    _asyncoro = None

    def __init__(self, *args, **kwargs):
        if not SysCoro._asyncoro:
            AsynCoro.instance()
        self._scheduler = SysCoro._asyncoro
        super(SysCoro, self).__init__(*args, **kwargs)


class _NetRequest(object):
    """Internal use only.
    """

    __slots__ = ('name', 'kwargs', 'dst', 'auth', 'event', 'reply', 'timeout')

    def __init__(self, name, kwargs={}, dst=None, auth=None, timeout=None):
        self.name = name
        self.kwargs = kwargs
        self.dst = dst
        self.auth = auth
        self.event = None
        self.reply = None
        self.timeout = timeout

    def __getstate__(self):
        state = {'name': self.name, 'kwargs': self.kwargs, 'dst': self.dst,
                 'auth': self.auth, 'reply': self.reply, 'timeout': self.timeout}
        return state

    def __setstate__(self, state):
        for k, v in state.items():
            setattr(self, k, v)


class _Peer(object):
    """Internal use only.
    """

    __slots__ = ('name', 'location', 'auth', 'keyfile', 'certfile', 'stream', 'conn',
                 'reqs', 'waiting', 'req_coro')

    peers = {}
    status_coro = None
    _asyncoro = None
    _lock = threading.Lock()

    def __init__(self, name, location, auth, keyfile, certfile):
        self.name = name
        self.location = location
        self.auth = auth
        self.keyfile = keyfile
        self.certfile = certfile
        self.stream = False
        self.conn = None
        self.reqs = collections.deque()
        self.waiting = False
        _Peer._lock.acquire()
        _Peer.peers[(location.addr, location.port)] = self
        _Peer._lock.release()
        self.req_coro = SysCoro(self.req_proc)
        logger.debug('%s: found peer %s', _Peer._asyncoro._location, self.location)
        if _Peer.status_coro:
            _Peer.status_coro.send(PeerStatus(location, name, PeerStatus.Online))

        _SysAsynCoro_._asyncoro._lock.acquire()
        if ((location.addr, location.port) in _SysAsynCoro_._asyncoro._stream_peers or
            (location.addr, 0) in _SysAsynCoro_._asyncoro._stream_peers):
            self.stream = True

        # send pending (async) requests
        for pending_req in _SysAsynCoro_._asyncoro._pending_reqs.values():
            if (pending_req.name == 'locate_peer' and pending_req.kwargs['name'] == self.name):
                pending_req.reply = location
                pending_req.event.set()

            if pending_req.dst:
                if pending_req.dst == location:
                    _Peer.send_req(pending_req)
            else:
                _Peer.send_req_to(pending_req, location)
        _SysAsynCoro_._asyncoro._lock.release()

    @staticmethod
    def get_peers():
        _Peer._lock.acquire()
        peers = [copy.copy(peer.location) for peer in _Peer.peers.values()]
        _Peer._lock.release()
        return peers

    @staticmethod
    def get_peer(location):
        _Peer._lock.acquire()
        peer = _Peer.peers.get((location.addr, location.port), None)
        _Peer._lock.release()
        return peer

    @staticmethod
    def send_req(req):
        _Peer._lock.acquire()
        peer = _Peer.peers.get((req.dst.addr, req.dst.port), None)
        if not peer:
            logger.debug('%s: invalid peer: %s', _Peer._asyncoro.location, req.dst)
            _Peer._lock.release()
            return -1
        peer.reqs.append(req)
        if peer.waiting:
            peer.waiting = False
            peer.req_coro.send(1)
        _Peer._lock.release()
        return 0

    @staticmethod
    def send_req_to(req, dst):
        if dst:
            _Peer._lock.acquire()
            peer = _Peer.peers.get((dst.addr, dst.port), None)
            if not peer:
                logger.debug('%s: invalid peer to: %s', _Peer._asyncoro.location, dst)
                _Peer._lock.release()
                return -1
            peer.reqs.append(req)
            if peer.waiting:
                peer.waiting = False
                peer.req_coro.send(1)
            _Peer._lock.release()
        else:
            _Peer._lock.acquire()
            for peer in _Peer.peers.values():
                peer.reqs.append(req)
                if peer.waiting:
                    peer.waiting = False
                    peer.req_coro.send(1)
            _Peer._lock.release()
        return 0

    @staticmethod
    def _sync_reply(req, alarm_value=None):
        req.event = Event()
        if _Peer.send_req(req) != 0:
            raise StopIteration(-1)
        if (yield req.event.wait(req.timeout)) is False:
            raise StopIteration(alarm_value)
        raise StopIteration(req.reply)

    @staticmethod
    def close_peer(peer, timeout, coro=None):
        req = _NetRequest('close_peer', kwargs={'location': _Peer._asyncoro._location},
                          dst=peer.location, timeout=timeout)
        yield _Peer._sync_reply(req)
        if peer.req_coro:
            yield peer.req_coro.terminate()
            while peer.req_coro:
                yield coro.sleep(0.1)

    @staticmethod
    def shutdown(timeout=MsgTimeout):
        _Peer._lock.acquire()
        for peer in _Peer.peers.values():
            SysCoro(_Peer.close_peer, peer, timeout)
        _Peer._lock.release()

    def req_proc(self, coro=None):
        coro.set_daemon()
        conn_errors = 0
        req = None
        while 1:
            _Peer._lock.acquire()
            if self.reqs:
                _Peer._lock.release()
            else:
                self.waiting = True
                _Peer._lock.release()
                if not self.stream and self.conn:
                    self.conn.shutdown(socket.SHUT_WR)
                    self.conn.close()
                    self.conn = None
                try:
                    yield coro.receive()
                except GeneratorExit:
                    break
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
                        try:
                            self.conn.shutdown(socket.SHUT_WR)
                            self.conn.close()
                        except:
                            pass
                        self.conn = None
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
                        logger.warning('too many connection errors to %s; removing it',
                                       self.location)
                        break
                    continue
                else:
                    if conn_errors:
                        conn_errors = 0
            else:
                self.conn.settimeout(req.timeout)

            req.auth = self.auth
            try:
                yield self.conn.send_msg(serialize(req))
                reply = yield self.conn.recv_msg()
                reply = deserialize(reply)
                if req.event:
                    if reply is not None or req.dst == self.location:
                        req.reply = reply
                        req.event.set()
                else:
                    req.reply = reply
            except socket.error as exc:
                logger.debug('%s: Could not send "%s" to %s', _Peer._asyncoro._location, req.name,
                             self.location)
                # logger.debug(traceback.format_exc())
                if len(exc.args) == 1 and exc.args[0] == 'hangup':
                    logger.warning('peer "%s" not reachable', self.location)
                    # TODO: remove peer?
                try:
                    self.conn.shutdown(socket.SHUT_WR)
                    self.conn.close()
                except:
                    pass
                self.conn = None
                req.reply = None
                if req.event:
                    req.event.set()
            except socket.timeout:
                # logger.debug(traceback.format_exc())
                try:
                    self.conn.shutdown(socket.SHUT_WR)
                    self.conn.close()
                except:
                    pass
                self.conn = None
                req.reply = None
                if req.event:
                    req.event.set()
            except GeneratorExit:
                if self.conn:
                    try:
                        self.conn.shutdown(socket.SHUT_WR)
                        self.conn.close()
                    except:
                        pass
                    self.conn = None
                break
            except:
                # logger.debug(traceback.format_exc())
                if self.conn:
                    try:
                        self.conn.shutdown(socket.SHUT_WR)
                        self.conn.close()
                    except:
                        pass
                    self.conn = None
                req.reply = None
                if req.event:
                    req.event.set()

        if req and isinstance(req.event, Event):
            req.reply = None
            req.event.set()
        for req in self.reqs:
            if isinstance(req.event, Event):
                req.reply = None
                req.event.set()

        self.reqs.clear()
        self.req_coro = None
        if self.conn:
            # self.conn.shutdown(socket.SHUT_WR)
            self.conn.close()
            self.conn = None
        _Peer.remove(self.location)
        raise StopIteration(None)

    @staticmethod
    def remove(location):
        _Peer._lock.acquire()
        peer = _Peer.peers.pop((location.addr, location.port), None)
        _Peer._lock.release()
        if peer:
            logger.debug('%s: peer %s terminated', _Peer._asyncoro.location, peer.location)
            peer.stream = False
            if peer.req_coro:
                peer.req_coro.terminate()
            if _Peer.status_coro:
                _Peer.status_coro.send(PeerStatus(peer.location, peer.name, PeerStatus.Offline))

    @staticmethod
    def peer_status(coro):
        _Peer._lock.acquire()
        if isinstance(coro, Coro):
            # if there is another status_coro, add or replace?
            for peer in _Peer.peers.values():
                try:
                    coro.send(PeerStatus(peer.location, peer.name, PeerStatus.Online))
                except:
                    logger.debug(traceback.format_exc())
                    break
            else:
                _Peer.status_coro = coro
        elif coro is None:
            _Peer.status_coro = None
        else:
            logger.warning('invalid peer status coroutine ignored')
        _Peer._lock.release()


class _SysAsynCoro_(asyncoro.AsynCoro, metaclass=Singleton):
    """Internal use only.
    """

    _instance = None
    _asyncoro = None

    def __init__(self, udp_port=0, tcp_port=0, node=None, ext_ip_addr=None,
                 name=None, discover_peers=True,
                 secret='', certfile=None, keyfile=None, notifier=None,
                 dest_path=None, max_file_size=None):
        super(self.__class__, self).__init__()
        SysCoro._asyncoro = _Peer._asyncoro = self
        if node:
            node = socket.gethostbyname(node)
        else:
            if netifaces:
                for iface in netifaces.interfaces():
                    for link in netifaces.ifaddresses(iface).get(netifaces.AF_INET, []):
                        if link.get('broadcast', None) and link.get('netmask', None):
                            node = socket.gethostbyname(link.get('addr', ''))
                            break
                    else:
                        continue
                    break
            if not node:
                node = socket.gethostbyname(socket.gethostname())
        if not udp_port:
            udp_port = 51350
        if not dest_path:
            dest_path = os.path.join(os.sep, tempfile.gettempdir(), 'asyncoro')
        self.__dest_path = os.path.abspath(os.path.normpath(dest_path))
        self.__dest_path_prefix = dest_path
        # TODO: avoid race condition (use locking to check/create atomically?)
        if not os.path.isdir(self.__dest_path):
            try:
                os.makedirs(self.__dest_path)
            except:
                # likely another asyncoro created this directory
                if not os.path.isdir(self.__dest_path):
                    logger.warning('failed to create "%s"', self.__dest_path)
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
        self._tcp_sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                     keyfile=self._keyfile, certfile=self._certfile)
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
            self._signature = ''.join(hex(_)[2:] for _ in os.urandom(20))
            self._auth_code = hashlib.sha1((self._signature + secret).encode()).hexdigest()
        self._tcp_sock.listen(32)
        logger.info('network server %s@ %s, udp_port=%s', '"%s" ' % name if name else '',
                    self._location, self._udp_sock.getsockname()[1])
        self._broadcast = '<broadcast>'
        if netifaces:
            for iface in netifaces.interfaces():
                for link in netifaces.ifaddresses(iface).get(netifaces.AF_INET, []):
                    if link['addr'] == self._location.addr:
                        self._broadcast = link.get('broadcast', '<broadcast>')
                        break
                else:
                    continue
                break
        self._ignore_peers = False
        self._tcp_coro = SysCoro(self._tcp_proc)
        self._udp_coro = SysCoro(self._udp_proc, discover_peers)

    @staticmethod
    def instance():
        return _SysAsynCoro_._instance

    @property
    def dest_path(self):
        return self.__dest_path

    @dest_path.setter
    def dest_path(self, path):
        self.__dest_path = path

    def finish(self):
        super(self.__class__, self).finish()
        if self._udp_sock:
            self._udp_sock.close()
            self._udp_sock = None
        if self._tcp_sock:
            self._tcp_sock.close()
            self._tcp_sock = None

    def peer(self, client, loc, udp_port=0, stream_send=False, broadcast=False, coro=None):
        """
        _Must_ be called with SysCoro
        """
        def _peer(loc, udp_port, stream_send, broadcast):
            if not isinstance(loc, Location):
                try:
                    loc = socket.gethostbyname(loc)
                except:
                    logger.warning('invalid node: "%s"', str(loc))
                    raise StopIteration(-1)
                loc = Location(loc, 0)

            _SysAsynCoro_._asyncoro._lock.acquire()
            if stream_send:
                self._stream_peers[(loc.addr, loc.port)] = True
            else:
                self._stream_peers.pop((loc.addr, loc.port), None)

            if loc.port:
                _Peer._lock.acquire()
                peer = _Peer.peers.get((loc.addr, loc.port), None)
                _Peer._lock.release()
                if peer:
                    peer.stream = stream_send
                    if not broadcast:
                        _SysAsynCoro_._asyncoro._lock.release()
                        raise StopIteration(0)
            else:
                _Peer._lock.acquire()
                for (addr, port), peer in _Peer.peers.items():
                    if addr == loc.addr:
                        peer.stream = stream_send
                        if not stream_send:
                            self._stream_peers.pop((addr, port), None)
                _Peer._lock.release()
            _SysAsynCoro_._asyncoro._lock.release()

            if loc.port:
                req = _NetRequest('signature', kwargs={'version': __version__}, dst=loc)
                sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
                sock.settimeout(2)
                try:
                    yield sock.connect((loc.addr, loc.port))
                    yield sock.send_msg(serialize(req))
                    sign = yield sock.recv_msg()
                    ret = yield self._acquaint_(loc, sign.decode(), coro=coro)
                except:
                    ret = -1
                finally:
                    sock.close()

                if broadcast and ret == 0:
                    kwargs = {'location': self._location, 'signature': self._signature,
                              'name': self._name, 'version': __version__}
                    _Peer.send_req_to(_NetRequest('relay_ping', kwargs=kwargs, dst=loc), loc)
                raise StopIteration(ret)
            else:
                if not udp_port:
                    udp_port = 51350
                ping_msg = {'location': self._location, 'signature': self._signature,
                            'name': self._name, 'version': __version__, 'broadcast': broadcast}
                ping_msg = 'ping:'.encode() + serialize(ping_msg)
                sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
                sock.settimeout(2)
                try:
                    yield sock.sendto(ping_msg, (loc.addr, udp_port))
                except:
                    pass
                sock.close()
            raise StopIteration(0)

        ret = yield _peer(loc, udp_port, stream_send, broadcast)
        client.send(ret)

    def _acquaint_(self, peer_location, peer_signature, coro=None):
        sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                           keyfile=self._keyfile, certfile=self._certfile)
        sock.settimeout(MsgTimeout)
        req = _NetRequest('peer', kwargs={'signature': self._signature, 'name': self._name,
                                          'from': self._location, 'version': __version__},
                          dst=peer_location)
        req.auth = hashlib.sha1((peer_signature + self._secret).encode()).hexdigest()
        try:
            yield sock.connect((peer_location.addr, peer_location.port))
            yield sock.send_msg(serialize(req))
            peer_info = yield sock.recv_msg()
            peer_info = deserialize(peer_info)
            assert peer_info['version'] == __version__
            _Peer(peer_info['name'], peer_location, req.auth, self._keyfile, self._certfile)
            reply = 0
        except:
            logger.debug(traceback.format_exc())
            reply = -1
        sock.close()
        raise StopIteration(0)

    def discover_peers(self, port=None, coro=None):
        ping_sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
        ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        ping_sock.settimeout(2)
        ping_sock.bind((self._tcp_sock.getsockname()[0], 0))
        ping_msg = {'location': self._location, 'signature': self._signature,
                    'name': self._name, 'version': __version__}
        ping_msg = 'ping:'.encode() + serialize(ping_msg)
        if not port:
            port = self._udp_sock.getsockname()[1]
        try:
            yield ping_sock.sendto(ping_msg, (self._broadcast, port))
        except:
            pass
        ping_sock.close()

    def ignore_peers(self, ignore):
        if ignore:
            self._ignore_peers = True
        else:
            self._ignore_peers = False

    def _udp_proc(self, discover_peers, coro=None):
        coro.set_daemon()
        if discover_peers:
            SysCoro(self.discover_peers)

        while 1:
            msg, addr = yield self._udp_sock.recvfrom(1024)
            if not msg.startswith(b'ping:'):
                logger.warning('ignoring UDP message from %s:%s', addr[0], addr[1])
                continue
            try:
                ping_info = deserialize(msg[len(b'ping:'):])
            except:
                continue
            peer_location = ping_info.get('location', None)
            if not isinstance(peer_location, Location) or peer_location == self._location:
                continue
            if ping_info['version'] != __version__:
                logger.warning('Peer %s version %s is not %s',
                               peer_location, ping_info['version'], __version__)
                continue
            if self._ignore_peers:
                continue
            if self._secret is None:
                auth_code = None
            else:
                auth_code = hashlib.sha1((ping_info.get('signature', '') +
                                          self._secret).encode()).hexdigest()
            _Peer._lock.acquire()
            peer = _Peer.peers.get((peer_location.addr, peer_location.port), None)
            _Peer._lock.release()
            if peer and peer.auth != auth_code:
                _Peer.remove(peer_location)
                peer = None

            if not peer:
                SysCoro(self._acquaint_, peer_location, ping_info['signature'])

    def _tcp_proc(self, coro=None):
        coro.set_daemon()
        while 1:
            conn, addr = yield self._tcp_sock.accept()
            SysCoro(self._tcp_task, conn, addr)

    def _tcp_task(self, conn, addr, coro=None):
        while 1:
            try:
                msg = yield conn.recv_msg()
            except:
                break
            if not msg:
                break
            try:
                req = deserialize(msg)
            except:
                logger.debug('%s ignoring invalid message', self._location)
                break
            if req.auth != self._auth_code:
                if req.name == 'signature':
                    yield conn.send_msg(self._signature.encode())
                else:
                    logger.warning('invalid request "%s" ignored', req.name)
                break

            # if req.dst and req.dst != self._location:
            #     logger.debug('invalid request "%s" to %s (%s)', req.name, req.dst, self._location)
            #     break

            if req.name == 'send':
                # synchronous message
                reply = -1
                if req.dst != self._location:
                    logger.warning('ignoring invalid "send" (%s != %s)', req.dst, self._location)
                else:
                    coro = req.kwargs.get('coro', None)
                    if coro:
                        name = req.kwargs.get('name', ' ')
                        if name[0] == '~':
                            Coro._asyncoro._lock.acquire()
                            coro = Coro._asyncoro._coros.get(int(coro), None)
                            Coro._asyncoro._lock.release()
                            if coro and coro._name == name:
                                reply = coro.send(req.kwargs['message'])
                            else:
                                logger.warning('ignoring invalid recipient to "send"')
                        elif name[0] == '!':
                            coro = self._coros.get(int(coro))
                            if coro and coro._name == name:
                                reply = coro.send(req.kwargs['message'])
                            else:
                                logger.warning('ignoring invalid recipient to "send"')
                        else:
                            logger.warning('invalid "send" message ignored')
                    else:
                        channel = req.kwargs.get('channel', None)
                        if channel[0] == '~':
                            Channel._asyncoro._lock.acquire()
                            channel = Channel._asyncoro._channels.get(channel)
                            Channel._asyncoro._lock.release()
                            if channel:
                                reply = channel.send(req.kwargs['message'])
                            else:
                                logger.warning('ignoring invalid recipient to "send"')
                        elif channel[0] == '!':
                            channel = self._channels.get(channel)
                            if isinstance(channel, Channel):
                                reply = channel.send(req.kwargs['message'])
                            else:
                                logger.warning('invalid "send" message ignored')
                        else:
                            logger.warning('ignoring invalid recipient to "send"')
                yield conn.send_msg(serialize(reply))

            elif req.name == 'deliver':
                # synchronous message
                reply = -1
                if req.dst != self._location:
                    logger.warning('ignoring invalid "deliver" (%s != %s)', req.dst, self._location)
                else:
                    coro = req.kwargs.get('coro', None)
                    if coro:
                        name = req.kwargs.get('name', ' ')
                        if name[0] == '~':
                            Coro._asyncoro._lock.acquire()
                            coro = Coro._asyncoro._coros.get(int(coro))
                            Coro._asyncoro._lock.release()
                            if coro and coro.send(req.kwargs['message']) == 0:
                                reply = 1
                        elif name[0] == '!':
                            coro = self._coros.get(int(coro))
                            if coro and coro.send(req.kwargs['message']) == 0:
                                reply = 1
                            else:
                                logger.warning('invalid "deliver" message ignored')
                    else:
                        channel = req.kwargs.get('channel')
                        if channel:
                            if channel[0] == '~':
                                Channel._asyncoro._lock.acquire()
                                channel = Channel._asyncoro._channels.get(channel)
                                Channel._asyncoro._lock.release()
                                if channel:
                                    reply = yield channel.deliver(
                                        req.kwargs['message'], timeout=req.timeout,
                                        n=req.kwargs['n'])
                            elif channel[0] == '!':
                                channel = self._channels.get(channel)
                                if isinstance(channel, Channel):
                                    reply = yield channel.deliver(
                                        req.kwargs['message'], timeout=req.timeout,
                                        n=req.kwargs['n'])
                            else:
                                logger.warning('invalid "deliver" message ignored')
                        else:
                            logger.warning('invalid "deliver" message ignored')
                yield conn.send_msg(serialize(reply))

            elif req.name == 'run_rci':
                # synchronous message
                if req.dst != self._location:
                    reply = Exception('invalid RCI invocation')
                else:
                    RCI._asyncoro._lock.acquire()
                    rci = RCI._asyncoro._rcis.get(req.kwargs['name'], None)
                    RCI._asyncoro._lock.release()
                    if rci:
                        args = req.kwargs['args']
                        kwargs = req.kwargs['kwargs']
                        try:
                            reply = Coro(rci._method, *args, **kwargs)
                        except:
                            reply = Exception(traceback.format_exc())
                    else:
                        reply = Exception('RCI "%s" is not registered' % req.kwargs['name'])
                yield conn.send_msg(serialize(reply))

            elif req.name == 'locate_coro':
                Coro._asyncoro._lock.acquire()
                coro = Coro._asyncoro._rcoros.get(req.kwargs['name'], None)
                Coro._asyncoro._lock.release()
                if not coro:
                    coro = self._rcoros.get(req.kwargs['name'], None)
                yield conn.send_msg(serialize(coro))

            elif req.name == 'locate_channel':
                Channel._asyncoro._lock.acquire()
                channel = Channel._asyncoro._rchannels.get('~' + req.kwargs['name'], None)
                Channel._asyncoro._lock.release()
                if not channel:
                    channel = self._rchannels.get('!' + req.kwargs['name'], None)
                yield conn.send_msg(serialize(channel))

            elif req.name == 'locate_rci':
                RCI._asyncoro._lock.acquire()
                rci = RCI._asyncoro._rcis.get(req.kwargs['name'], None)
                RCI._asyncoro._lock.release()
                yield conn.send_msg(serialize(rci))

            elif req.name == 'monitor':
                # synchronous message
                assert req.dst == self._location
                reply = -1
                monitor = req.kwargs.get('monitor', None)
                coro = req.kwargs.get('coro', None)
                name = req.kwargs.get('name', None)
                if coro and name:
                    if name[0] == '~':
                        Coro._asyncoro._lock.acquire()
                        coro = Coro._asyncoro._coros.get(int(coro), None)
                        if coro and coro._name == name:
                            reply = Coro._asyncoro._monitor(monitor, coro)
                        Coro._asyncoro._lock.release()
                    elif name == '!':
                        coro = self._coros.get(int(coro), None)
                        if coro and coro._name == name:
                            reply = self._monitor(monitor, coro)
                yield conn.send_msg(serialize(reply))

            elif req.name == 'terminate_coro':
                reply = -1
                coro = req.kwargs.get('coro', None)
                name = req.kwargs.get('name', None)
                if coro and name:
                    if name[0] == '~':
                        Coro._asyncoro._lock.acquire()
                        coro = Coro._asyncoro._coros.get(int(coro), None)
                        Coro._asyncoro._lock.release()
                    elif name[0] == '!':
                        coro = self._coros.get(int(coro), None)
                if isinstance(coro, Coro):
                    reply = coro.terminate()
                yield conn.send_msg(serialize(reply))

            elif req.name == 'subscribe':
                # synchronous message
                assert req.dst == self._location
                reply = -1
                channel = req.kwargs.get('channel', ' ')
                if channel[0] == '~':
                    Channel._asyncoro._lock.acquire()
                    channel = Channel._asyncoro._channels.get(channel, None)
                    Channel._asyncoro._lock.release()
                elif channel[0] == '!':
                    channel = self._channels.get(channel, None)
                if isinstance(channel, Channel) and channel._location == self._location:
                    subscriber = req.kwargs.get('subscriber', None)
                    if isinstance(subscriber, Coro):
                        if subscriber._location == self._location:
                            Coro._asyncoro._lock.acquire()
                            subscriber = Coro._asyncoro._coros.get(int(subscriber._id), None)
                            Coro._asyncoro._lock.release()
                        reply = yield channel.subscribe(subscriber)
                    elif isinstance(subsriber, Channel):
                        if subscriber._location == self._location:
                            Channel._asyncoro._lock.acquire()
                            subscriber = self._channels.get(subscriber._name, None)
                            Channel._asyncoro._lock.release()
                        reply = yield channel.subscribe(subscriber)
                yield conn.send_msg(serialize(reply))

            elif req.name == 'unsubscribe':
                # synchronous message
                assert req.dst == self._location
                reply = -1
                channel = req.kwargs.get('channel', ' ')
                if channel[0] == '~':
                    Channel._asyncoro._lock.acquire()
                    channel = Channel._asyncoro._channels.get(channel, None)
                    Channel._asyncoro._lock.release()
                elif channel[0] == '!':
                    channel = self._channels.get(channel, None)
                if isinstance(channel, Channel) and channel._location == self._location:
                    subscriber = req.kwargs.get('subscriber', None)
                    if isinstance(subscriber, Coro):
                        if subscriber._location == self._location:
                            Coro._asyncoro._lock.acquire()
                            subscriber = Coro._asyncoro._coros.get(int(subscriber._id), None)
                            Coro._asyncoro._lock.release()
                        reply = yield channel.unsubscribe(subscriber)
                    elif isinstance(subsriber, Channel):
                        if subscriber._location == self._location:
                            Channel._asyncoro._lock.acquire()
                            subscriber = self._channels.get(subscriber._name, None)
                            Channel._asyncoro._lock.release()
                        reply = yield channel.unsubscribe(subscriber)
                yield conn.send_msg(serialize(reply))

            elif req.name == 'locate_peer':
                if req.kwargs['name'] == self._name:
                    loc = self._location
                elif req.dst == self._location:
                    loc = None
                yield conn.send_msg(serialize(loc))

            elif req.name == 'send_file':
                # synchronous message
                assert req.dst == self._location
                sep = req.kwargs['sep']
                tgt = req.kwargs['file'].split(sep)[-1]
                if req.kwargs['dir']:
                    dir = os.path.join(*(req.kwargs['dir'].split(sep)))
                    if dir:
                        tgt = os.path.join(dir, tgt)
                tgt = os.path.abspath(os.path.join(self.__dest_path, tgt))
                stat_buf = req.kwargs['stat_buf']
                resp = 0
                if self.max_file_size and stat_buf.st_size > self.max_file_size:
                    logger.warning('file "%s" too big (%s) - must be smaller than %s',
                                   req.kwargs['file'], stat_buf.st_size, self.max_file_size)
                    resp = -1
                elif not tgt.startswith(self.__dest_path):
                    resp = -1
                elif os.path.isfile(tgt):
                    sbuf = os.stat(tgt)
                    if abs(stat_buf.st_mtime - sbuf.st_mtime) <= 1 and \
                       stat_buf.st_size == sbuf.st_size and \
                       stat.S_IMODE(stat_buf.st_mode) == stat.S_IMODE(sbuf.st_mode):
                        resp = stat_buf.st_size
                    elif not req.kwargs['overwrite']:
                        resp = -1

                if resp == 0:
                    try:
                        if not os.path.isdir(os.path.dirname(tgt)):
                            os.makedirs(os.path.dirname(tgt))
                        fd = open(tgt, 'wb')
                    except:
                        logger.debug('failed to create "%s" : %s', tgt, traceback.format_exc())
                        resp = -1
                if resp == 0:
                    recvd = 0
                    try:
                        while recvd < stat_buf.st_size:
                            yield conn.send_msg(serialize(recvd))
                            data = yield conn.recvall(min(stat_buf.st_size-recvd, 1024000))
                            if not data:
                                break
                            fd.write(data)
                            recvd += len(data)
                    except:
                        logger.warning('copying file "%s" failed', tgt)
                    finally:
                        fd.close()
                    if recvd == stat_buf.st_size:
                        os.utime(tgt, (stat_buf.st_atime, stat_buf.st_mtime))
                        os.chmod(tgt, stat.S_IMODE(stat_buf.st_mode))
                        resp = recvd
                    else:
                        os.remove(tgt)
                        resp = -1
                yield conn.send_msg(serialize(resp))

            elif req.name == 'del_file':
                # synchronous message
                assert req.dst == self._location
                tgt = os.path.basename(req.kwargs['file'])
                dir = req.kwargs['dir']
                if isinstance(dir, str) and dir:
                    tgt = os.path.join(dir, tgt)
                tgt = os.path.join(self.__dest_path, tgt)
                if tgt.startswith(self.__dest_path) and os.path.isfile(tgt):
                    os.remove(tgt)
                    d = os.path.dirname(tgt)
                    try:
                        while d > self.__dest_path and os.path.isdir(d):
                            os.rmdir(d)
                            d = os.path.dirname(d)
                    except:
                        # logger.debug(traceback.format_exc())
                        pass
                    reply = 0
                else:
                    reply = -1
                yield conn.send_msg(serialize(reply))

            elif req.name == 'peer':
                # synchronous message
                if req.kwargs.get('version', None) != __version__:
                    yield conn.send_msg(serialize(-1))
                    break
                auth = hashlib.sha1((req.kwargs['signature'] + self._secret).encode()).hexdigest()
                peer_loc = req.kwargs['from']
                yield conn.send_msg(serialize({'version': __version__, 'name': self._name}))
                _Peer(req.kwargs['name'], peer_loc, auth, self._keyfile, self._certfile)

            elif req.name == 'close_peer':
                # synchronous message
                peer_loc = req.kwargs.get('location', None)
                if peer_loc:
                    # TODO: remove from _stream_peers?
                    # _SysAsynCoro_._asyncoro._stream_peers.pop((peer_loc.addr, peer_loc.port))
                    _Peer.remove(peer_loc)
                yield conn.send_msg(b'closed')
                break

            elif req.name == 'acquaint':
                if req.kwargs.get('version', None) != __version__:
                    yield conn.send_msg(serialize(-1))
                    break
                peer_location = req.kwargs.get('location', None)
                if not isinstance(peer_location, Location) or peer_location == self._location:
                    yield conn.send_msg(serialize(-1))
                    break
                _Peer._lock.acquire()
                peer = _Peer.peers.get((peer_location.addr, peer_location.port), None)
                _Peer._lock.release()
                if self._secret is None:
                    auth_code = None
                else:
                    auth_code = hashlib.sha1((ping_info.get('signature', '') +
                                              self._secret).encode()).hexdigest()
                if peer and peer.auth != auth_code:
                    _Peer.remove(peer_location)
                    peer = None
                if not peer:
                    SysCoro(self._acquaint_, peer_location, req.kwargs['signature'])
                yield conn.send_msg(serialize(0))

            elif req.name == 'relay_ping':
                yield conn.send_msg(serialize(0))
                ping_sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
                ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                ping_sock.settimeout(2)
                ping_msg = 'ping:'.encode() + serialize(req.kwargs)
                try:
                    yield ping_sock.sendto(ping_msg,
                                           (self._broadcast, self._udp_sock.getsockname()[1]))
                except:
                    pass
                finally:
                    ping_sock.close()

            else:
                logger.warning('invalid request "%s" ignored', req.name)

        conn.close()

    def _swing_call_(self, swing, method, *args, **kwargs):
        swing['result'] = yield method(*args, **kwargs)
        swing['event'].set()

    def __repr__(self):
        s = str(self._location)
        if s == self._name:
            return s
        else:
            return '"%s" @ %s' % (self._name, s)


asyncoro._NetRequest = _NetRequest
asyncoro._Peer = _Peer
asyncoro.SysCoro = SysCoro
asyncoro.AsynCoro = AsynCoro
