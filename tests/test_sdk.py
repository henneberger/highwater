import asyncio
import os
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from unittest.mock import patch

import highwater

from highwater import (
    Client,
    Comparison,
    DeduplicateSpec,
    Event,
    EventTimeGate,
    FilterSpec,
    IntervalJoinSpec,
    ProcessSpec,
    ProcessContext,
    ProcessOptions,
    Registry,
    NonDeterminismError,
    TemporalJoinSpec,
    TemporalJoinType,
    WindowAggregateSpec,
    WindowAggregation,
    execute_activity,
    wait_for_watermark,
    workflow,
    streaming,
)
from highwater.workflow_runner import WorkflowRunner
from highwater.rust_worker import RustWorker


class SdkTest(unittest.TestCase):
    def test_client_uses_explicit_or_environment_api_key(self):
        with patch.dict(os.environ, {"HIGHWATER_API_KEY": "environment-key"}):
            self.assertEqual(
                Client()._headers("application/json")["Authorization"],
                "Bearer environment-key",
            )
            self.assertEqual(
                Client(api_key="explicit-key")._headers("application/json")["Authorization"],
                "Bearer explicit-key",
            )

    def test_streaming_annotations_do_not_expose_temporal_style_defn(self):
        self.assertTrue(callable(streaming.process))
        self.assertTrue(callable(streaming.event))
        self.assertTrue(callable(streaming.batch))
        self.assertTrue(callable(streaming.task))
        self.assertFalse(hasattr(streaming, "defn"))
        self.assertFalse(hasattr(highwater, "activity"))
        self.assertFalse(hasattr(highwater, "process"))

    def test_stream_writer_resumes_cursor_and_claims_fenced_epoch(self):
        class RecordingClient(Client):
            def __init__(self):
                super().__init__()
                self.requests = []

            async def _request(self, method, path, body=None):
                self.requests.append((method, path, body))
                if path.endswith("/cursor"):
                    return {"next_offset": 7, "checkpoint": "upstream-6"}
                if path.endswith("/claim"):
                    return {"epoch": 3}
                return {"record": body, "disposition": "accepted"}

        async def write():
            client = RecordingClient()
            writer = client.stream_writer("events", source_id="connector", partition=2)
            await writer.publish(
                {"value": 1},
                event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                checkpoint="upstream-7",
            )
            return client.requests, writer

        requests, writer = asyncio.run(write())
        publish = next(body for method, path, body in requests if path.endswith("/records"))
        self.assertEqual(publish["source_offset"], 7)
        self.assertEqual(publish["source_epoch"], 3)
        self.assertEqual(publish["source_checkpoint"], "upstream-7")
        self.assertEqual(publish["event_time"], 1767225600.0)
        self.assertEqual(writer.checkpoint, "upstream-7")

    def test_stream_writer_commits_upstream_checkpoint_with_batch(self):
        class RecordingClient(Client):
            def __init__(self):
                super().__init__()
                self.batch = None

            async def _request(self, method, path, body=None):
                if path.endswith("/cursor"):
                    return {"next_offset": 11, "checkpoint": "event-10"}
                if path.endswith("/claim"):
                    return {"epoch": 4}
                if path.endswith("/records/batch"):
                    self.batch = body["records"]
                    return [{"disposition": "accepted"} for _ in self.batch]
                raise AssertionError(path)

        async def write():
            client = RecordingClient()
            writer = client.stream_writer("public-events", source_id="public-feed")
            await writer.publish_many([
                {"value": {"n": 11}, "event_time": 11, "checkpoint": "event-11"},
                {"value": {"n": 12}, "event_time": 12, "checkpoint": "event-12"},
            ])
            return client, writer

        client, writer = asyncio.run(write())

        self.assertEqual([record["source_offset"] for record in client.batch], [11, 12])
        self.assertEqual(
            [record["source_checkpoint"] for record in client.batch],
            ["event-11", "event-12"],
        )
        self.assertEqual(writer.next_offset, 13)
        self.assertEqual(writer.checkpoint, "event-12")

    def test_wikimedia_change_uses_public_event_identity_and_time(self):
        from examples.wikimedia_recent_changes import _decode_change

        change, record = _decode_change("sse-42", {
            "wiki": "enwiki",
            "title": "Durable stream processing",
            "type": "edit",
            "timestamp": 1_767_225_600,
            "length": {"old": 100, "new": 135},
            "meta": {"id": "event-42", "uri": "https://example.test/wiki/Page"},
        })

        self.assertEqual(change.page_key, "enwiki:Durable stream processing")
        self.assertEqual(change.length_delta, 35)
        self.assertEqual(record["event_id"], "event-42")
        self.assertEqual(record["key"], "enwiki")
        self.assertEqual(record["checkpoint"], "sse-42")
        self.assertEqual(record["event_time"], 1_767_225_600)

    def test_wikimedia_resume_skips_only_finalized_event_time(self):
        from examples.wikimedia_recent_changes import _checkpoint_at_watermark

        checkpoint = (
            '[{"topic":"codfw.mediawiki.recentchange","partition":0,"offset":-1},'
            '{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1000}]'
        )

        resumed = _checkpoint_at_watermark(checkpoint, 2.5)

        self.assertEqual(
            resumed,
            '[{"topic":"codfw.mediawiki.recentchange","partition":0,"offset":-1},'
            '{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":2500}]',
        )

    def test_deploys_stream_filter_spec(self):
        class RecordingClient(Client):
            async def _request(self, method, path, body=None):
                return {"method": method, "path": path, "body": body}

        response = asyncio.run(RecordingClient().deploy(FilterSpec(
            operator_id="high-temperature",
            stream="sensors",
            workflow="AlertWorkflow",
            field="temperature",
            comparison=Comparison.GREATER_THAN,
            operand=100,
        )))

        self.assertEqual(response["path"], "/stream-filters")
        self.assertEqual(response["body"]["comparison"], Comparison.GREATER_THAN)
        self.assertEqual(response["body"]["operand"], 100)

    def test_deploys_temporal_join_spec(self):
        @workflow.defn
        class JoinWorkflow:
            @workflow.run
            async def run(self, joined):
                return joined

        class RecordingClient(Client):
            async def _request(self, method, path, body=None):
                return {"method": method, "path": path, "body": body}

        response = asyncio.run(RecordingClient().deploy(TemporalJoinSpec(
            operator_id="orders-with-rates",
            probe_stream="orders",
            version_stream="rates",
            workflow=JoinWorkflow,
            join_type=TemporalJoinType.LEFT,
        )))

        self.assertEqual(response["method"], "POST")
        self.assertEqual(response["path"], "/temporal-joins")
        self.assertEqual(response["body"], {
            "join_id": "orders-with-rates",
            "probe_stream": "orders",
            "version_stream": "rates",
            "workflow_type": "JoinWorkflow",
            "task_queue": "default",
            "join_type": TemporalJoinType.LEFT,
        })

    def test_deploys_window_aggregate_spec(self):
        class RecordingClient(Client):
            async def _request(self, method, path, body=None):
                return {"method": method, "path": path, "body": body}

        response = asyncio.run(RecordingClient().deploy(WindowAggregateSpec(
            operator_id="ten-second-sums",
            stream="measurements",
            workflow="WindowSumWorkflow",
            window_size=10,
            start_at=0,
            aggregation=WindowAggregation.SUM,
        )))

        self.assertEqual(response["path"], "/stream-schedules")
        self.assertEqual(response["body"], {
            "schedule_id": "ten-second-sums",
            "stream": "measurements",
            "workflow_type": "WindowSumWorkflow",
            "window_size": 10,
            "slide": None,
            "start_at": 0,
            "task_queue": "default",
            "emit_empty_windows": False,
            "aggregation": WindowAggregation.SUM,
            "value_field": None,
        })

    def test_deploys_interval_join_spec(self):
        class RecordingClient(Client):
            async def _request(self, method, path, body=None):
                return {"method": method, "path": path, "body": body}

        response = asyncio.run(RecordingClient().deploy(IntervalJoinSpec(
            operator_id="login-purchases",
            left_stream="logins",
            right_stream="purchases",
            workflow="IntervalJoinWorkflow",
            lower_bound=0,
            upper_bound=5,
        )))

        self.assertEqual(response["path"], "/interval-joins")
        self.assertEqual(response["body"]["lower_bound"], 0)
        self.assertEqual(response["body"]["upper_bound"], 5)

    def test_deploys_deduplicate_spec(self):
        class RecordingClient(Client):
            async def _request(self, method, path, body=None):
                return {"method": method, "path": path, "body": body}

        response = asyncio.run(RecordingClient().deploy(DeduplicateSpec(
            operator_id="first-command",
            stream="commands",
            workflow="CommandWorkflow",
        )))

        self.assertEqual(response["path"], "/deduplicates")
        self.assertEqual(response["body"]["operator_id"], "first-command")

    def test_connects_operator_to_native_stream_edge(self):
        class RecordingClient(Client):
            async def _request(self, method, path, body=None):
                return {"method": method, "path": path, "body": body}

        response = asyncio.run(
            RecordingClient().connect_operator("high-temperature", "alert-changes")
        )

        self.assertEqual(response["method"], "POST")
        self.assertEqual(response["path"], "/operator-edges")
        self.assertEqual(response["body"], {
            "operator_id": "high-temperature",
            "output_stream": "alert-changes",
        })

    def test_deploys_keyed_process_with_event_time_gate(self):
        class RecordingClient(Client):
            async def _request(self, method, path, body=None):
                return {"method": method, "path": path, "body": body}

        response = asyncio.run(RecordingClient().deploy(ProcessSpec(
            process_id="accounts",
            input="account-events",
            workflow="AccountProcessWorkflow",
            build_id="accounts-v1",
            event_time_gate=EventTimeGate.COMPLETE,
            max_concurrency=8,
            capacity=100,
        )))

        self.assertEqual(response["path"], "/processes")
        self.assertEqual(response["body"], {
            "process_id": "accounts",
            "stream": "account-events",
            "workflow_type": "AccountProcessWorkflow",
            "key_field": None,
            "event_time_field": None,
            "state_version": 1,
            "build_id": "accounts-v1",
            "migrations_from": (),
            "task_queue": "default",
            "event_time_gate": EventTimeGate.COMPLETE,
            "max_concurrent_keys": 8,
            "mailbox_capacity": 100,
            "retry_concurrency": 8,
            "max_attempts": 5,
            "discard_input_on_success": False,
            "batch_max_size": 64,
            "batch_max_delay": 0.005,
        })

    def test_typed_process_transitions_state_and_emits_output(self):
        @dataclass(frozen=True)
        class Deposit:
            account_id: str
            amount: int

        @streaming.process
        class Account:
            @streaming.event
            async def apply(self, event: Deposit, ctx: ProcessContext[dict]):
                state = ctx.state_or({"balance": 0})
                next_state = {"balance": state["balance"] + event.amount}
                return streaming.transition(state=next_state, emit=next_state)

        registry = Registry()
        registry.register_workflow(Account)
        runner = WorkflowRunner(registry)
        events = [Event(1, "account", "WORKFLOW_STARTED", {
            "workflow_type": "Account",
            "args": [{
                "process_id": "accounts",
                "key": "a",
                "event_time": 10.0,
                "state": None,
                "record": {"value": {"account_id": "a", "amount": 5}},
            }],
            "run_number": 1,
        }, 1.0)]

        activation = asyncio.run(runner.activate("account", "Account", events))

        self.assertEqual(activation.commands[0].type, "COMPLETE_WORKFLOW")
        self.assertEqual(activation.commands[0].attributes["result"], {
            "__highwater_transition__": True,
            "state": {"balance": 5},
            "emit": {"balance": 5},
        })

    def test_stateful_process_migrates_and_emits(self):
        @streaming.process(
            key="account_id",
            event_time="occurred_at",
            wait_until=streaming.complete,
            state_version=2,
            build_id="accounts-v2",
        )
        @dataclass
        class Account:
            balance: int = 0
            currency: str = "USD"

            @streaming.migrate(from_version=1)
            def migrate_v1(self, state):
                return {**state, "currency": "USD"}

            @streaming.event
            async def apply(self, event):
                self.balance += event["amount"]
                return {"balance": self.balance, "currency": self.currency}

        registry = Registry()
        registry.register_workflow(Account)
        runner = WorkflowRunner(registry)
        events = [Event(1, "account", "WORKFLOW_STARTED", {
            "workflow_type": "Account",
            "args": [{
                "process_id": "accounts",
                "key": "a",
                "event_time": 10.0,
                "state": {"balance": 4},
                "state_version": 1,
                "target_state_version": 2,
                "build_id": "accounts-v2",
                "record": {"value": {"account_id": "a", "amount": 5}},
            }],
            "run_number": 1,
        }, 1.0)]

        activation = asyncio.run(runner.activate("account", "Account", events))

        self.assertEqual(activation.commands[0].attributes["result"]["state"], {
            "balance": 9,
            "currency": "USD",
        })
        self.assertEqual(registry.build_ids(), ["accounts-v2"])

    def test_process_batch_returns_one_durable_transition_per_event(self):
        @dataclass(frozen=True)
        class Document:
            document_id: str
            text: str

        @streaming.process(key="document_id", build_id="embeddings-v1")
        class Embeddings:
            @streaming.batch(max_size=32, max_delay=0.02)
            async def embed(self, documents: list[Document]):
                return [
                    {"document_id": document.document_id, "embedding": [len(document.text)]}
                    for document in documents
                ]

        envelopes = [
            {
                "process_id": "embeddings",
                "key": document_id,
                "event_time": 10.0,
                "state": None,
                "state_version": None,
                "record": {"value": {"document_id": document_id, "text": text}},
            }
            for document_id, text in [("a", "hello"), ("b", "world!")]
        ]
        results = asyncio.run(
            Embeddings.__highwater_process_batch_run__(Embeddings(), envelopes)
        )

        self.assertEqual([result["state"] for result in results], [{}, {}])
        self.assertEqual(
            [result["emit"]["embedding"] for result in results],
            [[5], [6]],
        )
        self.assertEqual(Embeddings.__highwater_batch_max_size__, 32)
        self.assertEqual(Embeddings.__highwater_batch_max_delay__, 0.02)

    def test_process_batch_isolates_poison_event(self):
        class BatchProcess:
            calls = []

            @staticmethod
            async def batch_run(_instance, envelopes):
                keys = [envelope["key"] for envelope in envelopes]
                BatchProcess.calls.append(keys)
                if "poison" in keys:
                    raise ValueError("bad event")
                return [{"key": key} for key in keys]

        worker = RustWorker(Registry())
        envelopes = [{"key": key} for key in ["a", "poison", "b", "c"]]

        results = asyncio.run(worker._execute_process_chunk(
            BatchProcess,
            envelopes,
            BatchProcess.batch_run,
        ))

        self.assertEqual(results[0], {"result": {"key": "a"}})
        self.assertIn("ValueError: bad event", results[1]["failure"])
        self.assertEqual(results[2], {"result": {"key": "b"}})
        self.assertEqual(results[3], {"result": {"key": "c"}})
        self.assertGreater(len(BatchProcess.calls), 1)

    def test_process_handle_extracts_key_and_hides_stream_publish(self):
        @streaming.process
        class Account:
            @streaming.event
            async def apply(self, event, ctx):
                return streaming.transition(state=event)

        class RecordingClient(Client):
            def __init__(self):
                super().__init__()
                self.requests = []

            async def _request(self, method, path, body=None):
                self.requests.append((method, path, body))
                return {"disposition": "accepted"}

        async def send():
            client = RecordingClient()
            handle = await client.deploy_process(
                Account,
                input="account-events",
                options=ProcessOptions(key="account_id"),
            )
            await handle.send({"account_id": "a", "amount": 5}, event_time=10)
            return client.requests

        requests = asyncio.run(send())
        publish = next(body for _, path, body in requests if path.endswith("/records"))
        self.assertEqual(publish["key"], "a")
        self.assertEqual(publish["value"]["amount"], 5)

    def test_runner_replays_history_and_emits_commands(self):
        @streaming.task
        def double(value):
            return value * 2

        @workflow.defn
        class Example:
            @workflow.run
            async def run(self, value):
                return await execute_activity(double, value)

        registry = Registry()
        registry.register_activity(double)
        registry.register_workflow(Example)
        runner = WorkflowRunner(registry)
        events = [Event(1, "example", "WORKFLOW_STARTED", {
            "workflow_type": "Example", "args": [3], "run_number": 1,
        }, 1.0)]

        activation = asyncio.run(runner.activate("example", "Example", events))

        self.assertEqual([command.type for command in activation.commands], ["SCHEDULE_ACTIVITY"])
        self.assertEqual(activation.commands[0].attributes["args"], [3])

    def test_runner_replays_activity_retry_bookkeeping_before_completion(self):
        @streaming.task
        def double(value):
            return value * 2

        @workflow.defn
        class Retried:
            @workflow.run
            async def run(self, value):
                return await execute_activity(double, value)

        registry = Registry()
        registry.register_activity(double)
        registry.register_workflow(Retried)
        runner = WorkflowRunner(registry)
        events = [Event(1, "retried", "WORKFLOW_STARTED", {
            "workflow_type": "Retried", "args": [3], "run_number": 1,
        }, 1.0)]
        scheduled = asyncio.run(runner.activate("retried", "Retried", events)).commands[0]
        history = events + [
            Event(2, "retried", "ACTIVITY_SCHEDULED", scheduled.attributes, 2.0),
            Event(3, "retried", "ACTIVITY_RETRY_SCHEDULED", {
                "command_id": 1, "failed_attempt": 1, "next_attempt": 2,
            }, 3.0),
            Event(4, "retried", "ACTIVITY_RETRY_SCHEDULED", {
                "command_id": 1, "failed_attempt": 2, "next_attempt": 3,
            }, 4.0),
            Event(5, "retried", "ACTIVITY_COMPLETED", {
                "command_id": 1, "result": 6,
            }, 5.0),
        ]

        replayed = asyncio.run(runner.activate("retried", "Retried", history))

        self.assertEqual([command.type for command in replayed.commands], ["COMPLETE_WORKFLOW"])
        self.assertEqual(replayed.commands[0].attributes["result"], 6)

        wrong_command = history.copy()
        wrong_command[2] = Event(3, "retried", "ACTIVITY_RETRY_SCHEDULED", {
            "command_id": 2, "failed_attempt": 1, "next_attempt": 2,
        }, 3.0)
        with self.assertRaisesRegex(NonDeterminismError, "wrong command"):
            asyncio.run(runner.activate("retried", "Retried", wrong_command))

    def test_runner_emits_durable_watermark_timer(self):
        @workflow.defn
        class EventTimeWorkflow:
            @workflow.run
            async def run(self):
                await wait_for_watermark("events", 42)
                return "ready"

        registry = Registry()
        registry.register_workflow(EventTimeWorkflow)
        runner = WorkflowRunner(registry)
        events = [Event(1, "event-time", "WORKFLOW_STARTED", {
            "workflow_type": "EventTimeWorkflow", "args": [], "run_number": 1,
        }, 1.0)]

        activation = asyncio.run(runner.activate(
            "event-time", "EventTimeWorkflow", events,
        ))

        self.assertEqual(
            [command.type for command in activation.commands],
            ["START_WATERMARK_TIMER"],
        )
        self.assertEqual(activation.commands[0].attributes["event_time"], 42.0)

        replayed = asyncio.run(runner.activate(
            "event-time",
            "EventTimeWorkflow",
            events + [
                Event(2, "event-time", "WATERMARK_TIMER_STARTED", {
                    "stream": "events", "event_time": 42.0, "command_id": 1,
                }, 2.0),
                Event(3, "event-time", "WATERMARK_TIMER_FIRED", {
                    "stream": "events", "event_time": 42.0, "command_id": 1,
                    "watermark": 42.0, "finalized": False,
                }, 3.0),
            ],
        ))
        self.assertEqual(
            [command.type for command in replayed.commands],
            ["COMPLETE_WORKFLOW"],
        )


if __name__ == "__main__":
    unittest.main()
