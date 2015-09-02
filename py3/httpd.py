"""
This file is part of asyncoro project.
See http://asyncoro.sourceforge.net for details.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2015, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"

__all__ = ['HTTPServer']

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
import asyncoro.discoro as discoro
from asyncoro.discoro import StatusMessage

if sys.version_info.major > 2:
    import http.server as BaseHTTPServer
    from urllib.parse import urlparse
else:
    import BaseHTTPServer
    from urlparse import urlparse


class HTTPServer(object):

    class _ClusterInfo(object):
        def __init__(self, compute):
            self.nodes = {}
            # TODO: maintain updates for each client separately, so
            # multiple clients can view the status?
            self.updates = {}

    class _Node(object):
        def __init__(self, ip_addr, name=None):
            if not name:
                name = ip_addr
            self.ip_addr = ip_addr
            self.name = name
            self.status = None
            self.procs = {}
            self.update_time = time.time()

    class _Proc(object):
        def __init__(self, location):
            self.location = location
            self.status = None
            self.coros = {}
            self.coros_submitted = 0
            self.coros_done = 0

    class _HTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
        def __init__(self, ctx, DocumentRoot, *args):
            self._ctx = ctx
            self._ctx._http_handler = self
            self.DocumentRoot = DocumentRoot
            BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args)

        def log_message(self, fmt, *args):
            # asyncoro.logger.debug('HTTP client %s: %s' % (self.client_address[0], fmt % args))
            return

        def do_GET(self):
            if self.path == '/cluster_updates':
                self._ctx._lock.acquire()
                updates = [
                    {'ip_addr': node.ip_addr, 'name': node.name, 'procs': len(node.procs),
                     'update_time': node.update_time,
                     'coros_submitted': sum(proc.coros_submitted for proc in node.procs.values()),
                     'coros_done': sum(proc.coros_done for proc in node.procs.values())}
                    for node in self._ctx.updates.values()
                    ]
                self._ctx.updates = {}
                self._ctx._lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(updates).encode())
                return
            elif self.path == '/cluster_status':
                self._ctx._lock.acquire()
                status = [
                    {'ip_addr': node.ip_addr, 'name': node.name, 'procs': len(node.procs),
                     'update_time': node.update_time,
                     'coros_submitted': sum(proc.coros_submitted for proc in node.procs.values()),
                     'coros_done': sum(proc.coros_done for proc in node.procs.values())}
                    for node in self._ctx.nodes.values()
                    ]
                self._ctx._lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(status).encode())
                return
            else:
                parsed_path = urlparse(self.path)
                path = parsed_path.path.lstrip('/')
                if path == '' or path == 'index.html':
                    path = 'cluster.html'
                path = os.path.join(self.DocumentRoot, path)
                try:
                    f = open(path)
                    data = f.read()
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
                    f.close()
                    return
                except:
                    asyncoro.logger.warning('HTTP client %s: Could not read/send "%s"',
                                            self.client_address[0], path)
                    asyncoro.logger.debug(traceback.format_exc())
                self.send_error(404)
                return
            asyncoro.logger.debug('Bad GET request from %s: %s' %
                                  (self.client_address[0], self.path))
            self.send_error(400)
            return

        def do_POST(self):
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                    environ={'REQUEST_METHOD': 'POST'})
            if self.path == '/proc_info':
                proc = None
                max_coros = 0
                for item in form.list:
                    if item.name == 'location':
                        m = re.match('^(\d+[\.\d]+):(\d+)$', item.value)
                        if m:
                            node = self._ctx.nodes.get(m.group(1))
                            if node:
                                proc = node.procs.get('%s:%s' % (m.group(1), m.group(2)))
                    elif item.name == 'limit':
                        try:
                            max_coros = int(item.value)
                        except:
                            pass
                if proc:
                    if 0 < max_coros < len(proc.coros):
                        if sys.version_info.major == 2:
                            iterrcoros = proc.coros.itervalues()
                        else:
                            iterrcoros = proc.coros.values()
                        rcoros = []
                        for i, rcoro in enumerate(iterrcoros):
                            if i >= max_coros:
                                break
                            rcoros.append(rcoro)
                    else:
                        rcoros = proc.coros.values()
                    rcoros = [{'coro': str(rcoro.coro), 'name': rcoro.coro.name,
                               'args': ', '.join(str(arg) for arg in rcoro.args),
                               'kwargs': ', '.join('%s=%s' % (key, val)
                                                   for key, val in rcoro.kwargs.items()),
                               'start_time': rcoro.start_time}
                              for rcoro in rcoros]
                    info = {'location': str(proc.location),
                            'coros_submitted': proc.coros_submitted,
                            'coros_done': proc.coros_done,
                            'coros': rcoros, 'update_time': node.update_time}
                else:
                    info = {}
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(info).encode())
                return
            elif self.path == '/node_info':
                ip_addr = None
                for item in form.list:
                    if item.name == 'host':
                        if re.match('^(\d+[\.\d]+)$', item.value):
                            ip_addr = item.value
                        else:
                            try:
                                ip_addr = socket.gethostbyname(item.value)
                            except:
                                ip_addr = item.value
                        break
                node = self._ctx.nodes.get(ip_addr)
                if node:
                    info = {'ip_addr': node.ip_addr, 'name': node.name,
                            'update_time': node.update_time,
                            'coros_submitted': sum(proc.coros_submitted
                                                   for proc in node.procs.values()),
                            'coros_done': sum(proc.coros_done
                                              for proc in node.procs.values()),
                            'procs': [
                                {'location': str(proc.location),
                                 'coros_submitted': proc.coros_submitted,
                                 'coros_done': proc.coros_done,
                                 'coros_running': len(proc.coros),
                                 'update_time': node.update_time}
                                for proc in node.procs.values()
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
                            asyncoro.logger.debug('Terminate: coro "%s" is invalid' % item.value)

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
                    node = self._ctx.nodes.get(s[0])
                    if not node:
                        continue
                    proc = node.procs.get(location)
                    if not proc:
                        continue
                    rcoro = proc.coros.get(coro)
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
            asyncoro.logger.debug('Bad POST request from %s: %s' %
                                  (self.client_address[0], self.path))
            self.send_error(400)
            return

    def __init__(self, host='', port=8181, poll_sec=10, DocumentRoot=None,
                 keyfile=None, certfile=None):
        self._lock = threading.Lock()
        if not DocumentRoot:
            DocumentRoot = os.path.join(os.path.dirname(__file__), 'data')
        self.nodes = {}
        self.updates = {}
        if poll_sec < 1:
            asyncoro.logger.warning('invalid poll_sec value %s; it must be at least 1' % poll_sec)
            poll_sec = 1
        self._poll_sec = poll_sec
        self._http_handler = None
        self._server = BaseHTTPServer.HTTPServer((host, port), lambda *args:
                                  self.__class__._HTTPRequestHandler(self, DocumentRoot, *args))
        if certfile:
            self._server.socket = ssl.wrap_socket(self._server.socket, keyfile=keyfile,
                                                  certfile=certfile, server_side=True)
        self._httpd_thread = threading.Thread(target=self._server.serve_forever)
        self._httpd_thread.daemon = True
        self._httpd_thread.start()
        self.status_coro = asyncoro.Coro(self.status_proc)
        asyncoro.logger.info('Started HTTP%s server at %s' %
                             ('s' if certfile else '', str(self._server.socket.getsockname())))

    def status_proc(self, coro=None):
        coro.set_daemon()
        while True:
            msg = yield coro.receive()
            if isinstance(msg, asyncoro.MonitorException):
                rcoro = msg.args[0]
                node = self.nodes.get(rcoro.location.addr)
                if node:
                    proc = node.procs.get(str(rcoro.location))
                    if proc:
                        if proc.coros.pop(str(rcoro), None) is not None:
                            proc.coros_done += 1
                            node.update_time = time.time()
                            self.updates[node.ip_addr] = node
            elif isinstance(msg, StatusMessage):
                if msg.status == discoro.Scheduler.CoroCreated:
                    rcoro = msg.info
                    node = self.nodes.get(rcoro.coro.location.addr)
                    if node:
                        proc = node.procs.get(str(rcoro.coro.location))
                        if proc:
                            proc.coros[str(rcoro.coro)] = rcoro
                            proc.coros_submitted += 1
                            node.update_time = time.time()
                            self.updates[node.ip_addr] = node
                elif msg.status == discoro.Scheduler.ProcInitialized:
                    node = self.nodes.get(msg.info.addr)
                    if not node:
                        node = HTTPServer._Node(msg.info.addr)
                        node.status = discoro.Scheduler.NodeInitialized
                        self.nodes[msg.info.addr] = node
                    proc = HTTPServer._Proc(msg.info)
                    proc.status = msg.status
                    node.procs[str(msg.info)] = proc
                    node.update_time = time.time()
                    self.updates[node.ip_addr] = node
                elif msg.status in (discoro.Scheduler.ProcClosed, discoro.Scheduler.ProcIgnore,
                                    discoro.Scheduler.ProcDisconnected):
                    node = self.nodes.get(msg.info.addr)
                    if node:
                        node.procs.pop(str(msg.info))
                        if not node.procs:
                            self.nodes.pop(msg.info.addr)
                        node.update_time = time.time()
                        self.updates[node.ip_addr] = node
                elif msg.status in (discoro.Scheduler.NodeInitialized,
                                    discoro.Scheduler.NodeClosed, discoro.Scheduler.NodeIgnore,
                                    discoro.Scheduler.NodeDisconnected):
                    node = self.nodes.get(msg.info)
                    if node:
                        node.status = msg.status
                        node.update_time = time.time()
            else:
                asyncoro.logger.warning('Status message ignored: %s' % type(msg))

    def shutdown(self, wait=True):
        """This method should be called by user program to close the
        http server.
        """
        if wait:
            asyncoro.logger.debug(
                'HTTP server waiting for %s seconds for client updates before quitting',
                self._poll_sec)
            time.sleep(self._poll_sec)
        self._server.shutdown()
        self._server.server_close()
