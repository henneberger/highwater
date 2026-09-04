import socket
import socketserver
import threading
import unittest

if __package__:
    from .network_faults import TCPFaultProxy
else:
    from network_faults import TCPFaultProxy


class NetworkFaultProxyTest(unittest.TestCase):
    def test_partition_cuts_established_connections_and_heals(self):
        class Echo(socketserver.BaseRequestHandler):
            def handle(self):
                while data := self.request.recv(1024):
                    self.request.sendall(data)

        class Server(socketserver.ThreadingTCPServer):
            daemon_threads = True

        server = Server(("127.0.0.1", 0), Echo)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        proxy = TCPFaultProxy(server.server_address)
        self.addCleanup(proxy.close)
        endpoint = proxy.server.server_address
        with socket.create_connection(endpoint, timeout=2) as connection:
            connection.sendall(b"before")
            self.assertEqual(connection.recv(6), b"before")
            self.assertTrue(proxy.used.is_set())
            proxy.blocked.set()
            self.assertEqual(connection.recv(1), b"")
        with socket.create_connection(endpoint, timeout=2) as connection:
            self.assertEqual(connection.recv(1), b"")
        proxy.blocked.clear()
        with socket.create_connection(endpoint, timeout=2) as connection:
            connection.sendall(b"after")
            self.assertEqual(connection.recv(5), b"after")
