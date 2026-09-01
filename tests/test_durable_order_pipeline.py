from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from examples.durable_order_pipeline import OrderEvent, OrderIntake, fulfill_order
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
        customer_id = "customer-1"
        first = self.transition(
            None,
            OrderEvent(order_id, customer_id, "add_item", 1, "coffee", 2, 1_200),
        )
        self.assertIsNone(first["emit"])
        second = self.transition(
            first["state"],
            OrderEvent(order_id, customer_id, "add_item", 2, "filters", 1, 800),
        )
        self.assertIsNone(second["emit"])

        submitted = self.transition(
            second["state"],
            OrderEvent(
                order_id,
                customer_id,
                "submit",
                3,
                payment_reference="payment-demo-4242",
                address="1 Highwater Way",
            ),
        )

        self.assertTrue(submitted["state"]["submitted"])
        self.assertEqual(submitted["emit"]["status"], "ready")
        self.assertEqual(submitted["emit"]["customer_id"], customer_id)
        self.assertEqual(submitted["emit"]["total"], 3_200)

    def test_fulfillment_task_retries_as_one_unit(self) -> None:
        order = {
            "order_id": "order-1",
            "lines": [{"sku": "coffee", "quantity": 1}],
            "total": 1_200,
            "address": "1 Highwater Way",
        }
        customer = {"version": "standard-v1", "active": True}
        with patch(
            "examples.durable_order_pipeline.current_activity",
            return_value=SimpleNamespace(attempt=1),
        ):
            with self.assertRaisesRegex(RuntimeError, "transient 503"):
                fulfill_order(order, customer)
        with patch(
            "examples.durable_order_pipeline.current_activity",
            return_value=SimpleNamespace(attempt=2),
        ):
            result = fulfill_order(order, customer)

        self.assertEqual(result["reservation_id"], "inventory:order-1")
        self.assertEqual(result["charge_id"], "charge:order-1")
        self.assertEqual(result["tracking"], "tracking:order-1")
        self.assertEqual(result["attempts"], 2)


if __name__ == "__main__":
    unittest.main()
