"""Local TCP fault proxy; never changes host networking or firewall rules."""
from __future__ import annotations

import select
import socket
import socketserver
import threading


class TCPFaultProxy:
    def __init__(self, upstream: tuple[str, int]):
        self.blocked = threading.Event()
        self.used = threading.Event()
        proxy = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                if proxy.blocked.is_set():
                    return
                try:
                    with socket.create_connection(upstream, timeout=2) as target:
                        self.request.settimeout(2)
                        target.settimeout(2)
                        peers = {self.request: target, target: self.request}
                        while not proxy.blocked.is_set():
                            ready, _, _ = select.select(list(peers), [], [], 0.05)
                            for source in ready:
                                data = source.recv(65536)
                                if not data:
                                    return
                                peers[source].sendall(data)
                                proxy.used.set()
                except OSError:
                    pass

        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True

        self.server = Server(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.blocked.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
