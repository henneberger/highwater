from __future__ import annotations

import asyncio
import io
import unittest
from unittest.mock import AsyncMock, patch
from urllib.error import HTTPError

from highwater import Registry
from highwater.rust_worker import RustWorker, _RequestError


class WorkerReliabilityTest(unittest.IsolatedAsyncioTestCase):
    async def test_cancellation_stops_renewal_during_handler(self):
        await self.check_renewal_cleanup(cancel=True)

    async def test_execution_error_stops_renewal(self):
        await self.check_renewal_cleanup(cancel=False)

    async def check_renewal_cleanup(self, *, cancel):
        worker = RustWorker(Registry(), process_only=True)
        started = asyncio.Event()
        stopped = asyncio.Event()
        release = asyncio.Event()

        async def renew(batch, stop):
            started.set()
            try:
                await stop.wait()
            finally:
                stopped.set()

        async def execute(batches):
            await started.wait()
            await release.wait()
            raise ValueError("invalid activation")

        with patch.object(worker, "_request", return_value={}), patch.object(
            worker, "_renew_process_batch", side_effect=renew
        ), patch.object(worker, "_execute_process_batches", side_effect=execute):
            task = asyncio.create_task(worker._process_once())
            await asyncio.wait_for(started.wait(), 1)
            if cancel:
                task.cancel()
            else:
                release.set()
            with self.assertRaises(asyncio.CancelledError if cancel else ValueError):
                await task
            self.assertTrue(stopped.is_set(), "abandoned activation is still renewing")

    async def test_transient_outage_recovers_with_bounded_backoff(self):
        worker = RustWorker(Registry(), process_only=True)
        poll = AsyncMock(side_effect=[
            OSError("connection refused"), _RequestError(503, "unavailable"),
            *[OSError("offline") for _ in range(6)],
            True, OSError("offline again"), asyncio.CancelledError(),
        ])
        with patch.object(worker, "_process_once", poll), patch(
            "highwater.rust_worker.asyncio.sleep", new_callable=AsyncMock
        ) as sleep:
            with self.assertRaises(asyncio.CancelledError):
                await worker.run_forever()
        self.assertEqual(poll.await_count, 11)
        self.assertEqual(
            [call.args[0] for call in sleep.await_args_list],
            [0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 5.0, 5.0, 0.1],
        )

    async def test_authentication_failure_is_not_retried(self):
        worker = RustWorker(Registry(), process_only=True)
        with patch.object(worker, "_process_once", side_effect=_RequestError(403, "denied")) as poll:
            with self.assertRaisesRegex(_RequestError, "denied"):
                await worker.run_forever()
            self.assertEqual(poll.await_count, 1)

    async def test_failed_lane_waits_for_other_work_before_retry(self):
        worker = RustWorker(Registry())
        started = asyncio.Event()
        release = asyncio.Event()

        async def workflow():
            started.set()
            await release.wait()
            return True

        with patch.object(worker, "_process_once", side_effect=OSError("offline")) as poll, patch.object(
            worker, "_workflow_once", side_effect=workflow
        ), patch.object(worker, "_activity_once", return_value=False), patch.object(
            worker, "_query_once", return_value=False
        ):
            task = asyncio.create_task(worker.run_forever())
            try:
                await asyncio.wait_for(started.wait(), 1)
                self.assertEqual(poll.await_count, 1)
                self.assertFalse(task.done())
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

    def test_non_json_proxy_error_preserves_http_status(self):
        worker = RustWorker(Registry())
        response = io.BytesIO(b"<html>service unavailable</html>")
        error = HTTPError("http://localhost", 503, "unavailable", {}, response)
        with patch("highwater.rust_worker.urlopen", side_effect=error):
            with self.assertRaises(_RequestError) as raised:
                worker._request("/poll", {})
        self.assertEqual(raised.exception.status, 503)
        self.assertTrue(response.closed)
