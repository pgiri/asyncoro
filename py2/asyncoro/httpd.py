"""
This file is part of asyncoro project.
See http://asyncoro.sourceforge.net for details.
"""

import sys
import os
import threading
import json
import cgi
import time
import socket
import ssl
import re
import traceback

import asyncoro.disasyncoro as asyncoro
from asyncoro.discoro import DiscoroStatus, DiscoroNodeAvailInfo
import asyncoro.discoro as discoro

if sys.version_info.major > 2:
    import http.server as BaseHTTPServer
    from urllib.parse import urlparse
else:
    import BaseHTTPServer
    from urlparse import urlparse


__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2015, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

__all__ = ['HTTPServer']


if sys.version_info.major >= 3:
    def dict_iter(arg, iterator):
        return getattr(arg, iterator)()
else:
    def dict_iter(arg, iterator):
        return getattr(arg, 'iter' + iterator)()


class HTTPServer(object):

    NodeStatus = {discoro.Scheduler.NodeDiscovered: 'Discovered',
                  discoro.Scheduler.NodeInitialized: 'Initialized',
                  discoro.Scheduler.NodeClosed: 'Closed',
                  discoro.Scheduler.NodeIgnore: 'Ignore',
                  discoro.Scheduler.NodeDisconnected: 'Disconnected'}
    ServerStatus = {discoro.Scheduler.ServerDiscovered: 'Discovered',
                    discoro.Scheduler.ServerInitialized: 'Initialized',
                    discoro.Scheduler.ServerClosed: 'Closed',
                    discoro.Scheduler.ServerIgnore: 'Ignore',
                    discoro.Scheduler.ServerDisconnected: 'Disconnected'}

    class _Node(object):
        def __init__(self, name, addr):
            self.name = name
            self.addr = addr
            self.status = None
            self.servers = {}
            self.update_time = time.time()
            self.coros_submitted = 0
            self.coros_done = 0
            self.avail_info = None

    class _Server(object):
        def __init__(self, location):
            self.location = location
            self.status = None
            self.coros = {}
            self.coros_submitted = 0
            self.coros_done = 0

    class _HTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

        def __init__(self, ctx, DocumentRoot, *args):
            self._ctx = ctx
            self.DocumentRoot = DocumentRoot
            BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args)

        def log_message(self, fmt, *args):
            # asyncoro.logger.debug('HTTP client %s: %s', self.client_address[0], fmt % args)
            return

        @staticmethod
        def json_encode_nodes(arg):
            nodes = [dict(node.__dict__) for node in dict_iter(arg, 'values')]
            for node in nodes:
                node['servers'] = len(node['servers'])
                node['avail_info'] = node['avail_info'].__dict__
            return nodes

        def do_GET(self):
            if self.path == '/cluster_updates':
                self._ctx._lock.acquire()
                nodes = self.__class__.json_encode_nodes(self._ctx._updates)
                self._ctx._updates.clear()
                self._ctx._lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(nodes).encode())
                return
            elif self.path == '/cluster_status':
                self._ctx._lock.acquire()
                nodes = self.__class__.json_encode_nodes(self._ctx._nodes)
                self._ctx._lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(nodes).encode())
                return
            else:
                parsed_path = urlparse(self.path)
                path = parsed_path.path.lstrip('/')
                if not path or path == 'index.html':
                    path = 'cluster.html'
                path = os.path.join(self.DocumentRoot, path)
                try:
                    with open(path) as fd:
                        data = fd.read()
                    if path.endswith('.html'):
                        if path.endswith('.html'):
                            data = data % {'TIMEOUT': str(self._ctx._poll_sec)}
                        content_type = 'text/html'
                    elif path.endswith('.js'):
                        content_type = 'text/javascript'
                    elif path.endswith('.css'):
                        content_type = 'text/css'
                    elif path.endswith('.ico'):
                        content_type = 'image/x-icon'
                    self.send_response(200)
                    self.send_header('Content-Type', content_type)
                    if content_type == 'text/css' or content_type == 'text/javascript':
                        self.send_header('Cache-Control', 'private, max-age=86400')
                    self.end_headers()
                    self.wfile.write(data.encode())
                    return
                except:
                    asyncoro.logger.warning('HTTP client %s: Could not read/send "%s"',
                                            self.client_address[0], path)
                    asyncoro.logger.debug(traceback.format_exc())
                self.send_error(404)
                return
            asyncoro.logger.debug('Bad GET request from %s: %s',
                                  self.client_address[0], self.path)
            self.send_error(400)
            return

        def do_POST(self):
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                    environ={'REQUEST_METHOD': 'POST'})
            if self.path == '/server_info':
                server = None
                max_coros = 0
                for item in form.list:
                    if item.name == 'location':
                        m = re.match('^(\d+[\.\d]+):(\d+)$', item.value)
                        if m:
                            node = self._ctx._nodes.get(m.group(1))
                            if node:
                                server = node.servers.get(m.group(0))
                    elif item.name == 'limit':
                        try:
                            max_coros = int(item.value)
                        except:
                            pass
                if server:
                    if 0 < max_coros < len(server.coros):
                        rcoros = []
                        for i, rcoro in enumerate(dict_iter(server.coros, 'values')):
                            if i >= max_coros:
                                break
                            rcoros.append(rcoro)
                    else:
                        rcoros = server.coros.values()
                    rcoros = [{'coro': str(rcoro.coro), 'name': rcoro.coro.name,
                               'args': ', '.join(str(arg) for arg in rcoro.args),
                               'kwargs': ', '.join('%s=%s' % (key, val)
                                                   for key, val in rcoro.kwargs.items()),
                               'start_time': rcoro.start_time
                               } for rcoro in rcoros]
                    info = {'location': str(server.location),
                            'status': server.status, 'coros_submitted': server.coros_submitted,
                            'coros_done': server.coros_done, 'coros': rcoros,
                            'update_time': node.update_time}
                else:
                    info = {}
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(info).encode())
                return
            elif self.path == '/node_info':
                addr = None
                for item in form.list:
                    if item.name == 'host':
                        if re.match('^(\d+[\.\d]+)$', item.value):
                            addr = item.value
                        else:
                            try:
                                addr = socket.gethostbyname(item.value)
                            except:
                                addr = item.value
                        break
                node = self._ctx._nodes.get(addr)
                if node:
                    info = {'addr': node.addr, 'name': node.name,
                            'status': node.status, 'update_time': node.update_time,
                            'avail_info': node.avail_info.__dict__,
                            'coros_submitted': node.coros_submitted, 'coros_done': node.coros_done,
                            'servers': [
                                {'location': str(server.location),
                                 'coros_submitted': server.coros_submitted,
                                 'coros_done': server.coros_done,
                                 'coros_running': len(server.coros),
                                 'update_time': node.update_time
                                 } for server in node.servers.values()
                                ]
                            }
                else:
                    info = {}
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(info).encode())
                return
            elif self.path == '/terminate_coros':
                coros = []
                for item in form.list:
                    if item.name == 'coro':
                        try:
                            coros.append(item.value)
                        except ValueError:
                            asyncoro.logger.debug('Terminate: coro "%s" is invalid', item.value)

                terminated = []
                self._ctx._lock.acquire()
                for coro in coros:
                    s = coro.split('@')
                    if len(s) != 2:
                        continue
                    location = s[1]
                    s = location.split(':')
                    if len(s) != 2:
                        continue
                    node = self._ctx._nodes.get(s[0])
                    if not node:
                        continue
                    server = node.servers.get(location)
                    if not server:
                        continue
                    rcoro = server.coros.get(coro)
                    if rcoro and rcoro.coro.terminate() == 0:
                        terminated.append(coro)
                self._ctx._lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(terminated).encode())
                return

            elif self.path == '/set_poll_sec':
                for item in form.list:
                    if item.name != 'timeout':
                        continue
                    try:
                        timeout = int(item.value)
                        if timeout < 1:
                            timeout = 0
                    except:
                        asyncoro.logger.warning('HTTP client %s: invalid timeout "%s" ignored',
                                                self.client_address[0], item.value)
                        timeout = 0
                    self._ctx._poll_sec = timeout
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/html')
                    self.end_headers()
                    return
            asyncoro.logger.debug('Bad POST request from %s: %s',
                                  self.client_address[0], self.path)
            self.send_error(400)
            return

    def __init__(self, computation, host='', port=8181, poll_sec=10, DocumentRoot=None,
                 keyfile=None, certfile=None):
        self._lock = threading.Lock()
        if not DocumentRoot:
            DocumentRoot = os.path.join(os.path.dirname(__file__), 'data')
        self._nodes = {}
        self._updates = {}
        if poll_sec < 1:
            asyncoro.logger.warning('invalid poll_sec value %s; it must be at least 1', poll_sec)
            poll_sec = 1
        self._poll_sec = poll_sec
        self._server = BaseHTTPServer.HTTPServer((host, port), lambda *args:
                                  HTTPServer._HTTPRequestHandler(self, DocumentRoot, *args))
        if certfile:
            self._server.socket = ssl.wrap_socket(self._server.socket, keyfile=keyfile,
                                                  certfile=certfile, server_side=True)
        self._httpd_thread = threading.Thread(target=self._server.serve_forever)
        self._httpd_thread.daemon = True
        self._httpd_thread.start()
        self.status_coro = asyncoro.SysCoro(self.status_proc)
        self.computation = computation
        if not computation.status_coro:
            computation.status_coro = self.status_coro
        asyncoro.logger.info('Started HTTP%s server at %s',
                             's' if certfile else '', str(self._server.socket.getsockname()))

    def status_proc(self, coro=None):
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            if isinstance(msg, asyncoro.MonitorException):
                rcoro = msg.args[0]
                node = self._nodes.get(rcoro.location.addr)
                if node:
                    server = node.servers.get(str(rcoro.location))
                    if server:
                        if server.coros.pop(str(rcoro), None) is not None:
                            server.coros_done += 1
                            node.coros_done += 1
                            node.update_time = time.time()
                            self._updates[node.addr] = node
            elif isinstance(msg, DiscoroStatus):
                if msg.status == discoro.Scheduler.CoroCreated:
                    rcoro = msg.info
                    node = self._nodes.get(rcoro.coro.location.addr)
                    if node:
                        server = node.servers.get(str(rcoro.coro.location))
                        if server:
                            server.coros[str(rcoro.coro)] = rcoro
                            server.coros_submitted += 1
                            node.coros_submitted += 1
                            node.update_time = time.time()
                            self._updates[node.addr] = node
                elif msg.status == discoro.Scheduler.ServerInitialized:
                    node = self._nodes.get(msg.info.addr)
                    if node:
                        server = node.servers.get(str(msg.info), None)
                        if not server:
                            server = HTTPServer._Server(msg.info)
                            node.servers[str(server.location)] = server
                        server.status = HTTPServer.ServerStatus[msg.status]
                        node.update_time = time.time()
                        self._updates[node.addr] = node
                elif (msg.status == discoro.Scheduler.ServerClosed or
                      msg.status == discoro.Scheduler.ServerDisconnected):
                    node = self._nodes.get(msg.info.addr)
                    if node:
                        server = node.servers.get(str(msg.info), None)
                        if server:
                            server.status = HTTPServer.ServerStatus[msg.status]
                            node.update_time = time.time()
                            self._updates[node.addr] = node
                elif msg.status == discoro.Scheduler.NodeInitialized:
                    node = self._nodes.get(msg.info.addr)
                    if not node:
                        node = HTTPServer._Node(msg.info.name, msg.info.addr)
                        node.avail_info = msg.info.avail_info
                        node.avail_info.location = None
                        self._nodes[msg.info.addr] = node
                    node.status = HTTPServer.NodeStatus[msg.status]
                    node.update_time = time.time()
                    self._updates[node.addr] = node
                elif (msg.status == discoro.Scheduler.NodeClosed or
                      msg.status == discoro.Scheduler.NodeDisconnected):
                    node = self._nodes.get(msg.info.addr)
                    if node:
                        node.status = HTTPServer.NodeStatus[msg.status]
                        node.update_time = time.time()
                        self._updates[node.addr] = node
            elif isinstance(msg, DiscoroNodeAvailInfo):
                node = self._nodes.get(msg.location.addr, None)
                if node:
                    node.avail_info = msg
                    node.avail_info.location = None
                    node.update_time = time.time()
                    self._updates[node.addr] = node
            else:
                asyncoro.logger.warning('Status message ignored: %s', type(msg))

    def shutdown(self, wait=True):
        """This method should be called by user program to close the
        http server. If 'wait' is True the server waits for poll_sec
        so the http client gets all the updates before server is
        closed.
        """
        if wait:
            asyncoro.logger.info('HTTP server waiting for %s seconds for client updates '
                                 'before quitting', self._poll_sec)
            if asyncoro.AsynCoro().cur_coro():
                def _shutdown(coro=None):
                    yield coro.sleep(self._poll_sec + 0.5)
                    self._server.shutdown()
                    self._server.server_close()
                asyncoro.Coro(_shutdown)
            else:
                time.sleep(self._poll_sec + 0.5)
                self._server.shutdown()
                self._server.server_close()
        else:
            self._server.shutdown()
            self._server.server_close()
