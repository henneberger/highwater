from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from examples.durable_order_pipeline import OrderEvent, OrderIntake, reserve_order_inventory
from highwater import Event, Registry
from highwater.workflow_runner import WorkflowRunner


class DurableOrderPipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        registry = Registry()
        registry.register_workflow(OrderIntake)
        self.runner = WorkflowRunner(registry)

    def transition(self, state: dict | None, event: OrderEvent) -> dict:
        activation = asyncio.run(
            self.runner.activate(
                f"order/{event.kind}/{event.occurred_at}",
                "OrderIntake",
                [
                    Event(
                        1,
                        "order",
                        "WORKFLOW_STARTED",
                        {
                            "workflow_type": "OrderIntake",
                            "args": [
                                {
                                    "process_id": "orders",
                                    "key": event.order_id,
                                    "event_time": event.occurred_at,
                                    "state": state,
                                    "state_version": None if state is None else 1,
                                    "record": {"value": event.__dict__},
                                }
                            ],
                            "run_number": 1,
                        },
                        1,
                    )
                ],
            )
        )
        self.assertEqual(activation.commands[0].type, "COMPLETE_WORKFLOW")
        return activation.commands[0].attributes["result"]

    def test_intake_emits_only_after_submit(self) -> None:
        order_id = "order-1"
        first = self.transition(None, OrderEvent(order_id, "add_item", 1, "coffee", 2, 1_200))
        self.assertIsNone(first["emit"])
        second = self.transition(
            first["state"],
            OrderEvent(order_id, "add_item", 2, "filters", 1, 800),
        )
        self.assertIsNone(second["emit"])

        submitted = self.transition(
            second["state"],
            OrderEvent(
                order_id,
                "submit",
                3,
                payment_reference="payment-demo-4242",
                address="1 Highwater Way",
            ),
        )

        self.assertTrue(submitted["state"]["submitted"])
        self.assertEqual(submitted["emit"]["status"], "ready")
        self.assertEqual(submitted["emit"]["total"], 3_200)

    def test_inventory_activity_fails_transiently_then_uses_stable_id(self) -> None:
        order = {"order_id": "order-1", "lines": [{"sku": "coffee", "quantity": 1}]}
        with patch(
            "examples.durable_order_pipeline.current_activity",
            return_value=SimpleNamespace(attempt=1),
        ):
            with self.assertRaisesRegex(RuntimeError, "transient 503"):
                reserve_order_inventory(order)
        with patch(
            "examples.durable_order_pipeline.current_activity",
            return_value=SimpleNamespace(attempt=2),
        ):
            reservation = reserve_order_inventory(order)

        self.assertEqual(reservation["reservation_id"], "inventory:order-1")
        self.assertEqual(reservation["attempts"], 2)


if __name__ == "__main__":
    unittest.main()
