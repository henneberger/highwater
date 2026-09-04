import io
import json
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

from highwater import Client
from highwater.client import ProcessHandle
from highwater.errors import StreamBackpressure


class HAClientTest(unittest.IsolatedAsyncioTestCase):
    async def test_owner_failover_retries_identical_event(self):
        client = Client()
        handle = ProcessHandle(client, "counter", "unused", key_field="key", direct_ingress=True)
        unavailable = io.BytesIO(b"<html>upstream unavailable</html>")
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'[{"disposition":"duplicate"}]'
        with patch("highwater.client.urlopen", side_effect=[
            HTTPError(client.target, 503, "unavailable", {}, unavailable), response,
        ]) as send:
            result = await handle.send({"key": "a", "delta": 1}, event_id="stable-id")
        self.assertEqual(result["disposition"], "duplicate")
        first, second = [call.args[0].data for call in send.call_args_list]
        self.assertEqual(first, second)
        self.assertEqual(json.loads(first)["records"][0]["event_id"], "stable-id")
        self.assertTrue(unavailable.closed)

    async def test_packed_ingress_exposes_retryable_proxy_failure(self):
        client = Client()
        with patch("highwater.client.urlopen", side_effect=HTTPError(
            client.target, 502, "bad gateway", {}, io.BytesIO(b"bad gateway")
        )):
            with self.assertRaises(StreamBackpressure):
                await client._request_bytes("/processes/counter/events", b"batch")

    async def test_authentication_failure_is_not_retryable(self):
        client = Client()
        with patch("highwater.client.urlopen", side_effect=HTTPError(
            client.target, 401, "unauthorized", {}, io.BytesIO(b'{}')
        )) as send:
            with self.assertRaises(RuntimeError):
                await client._request("GET", "/processes/counter")
        self.assertEqual(send.call_count, 1)
