import unittest

from examples.continuous_order_enrichment import EVENT_TIME_EPOCH, event_for_offset


class ContinuousOrderEnrichmentTest(unittest.TestCase):
    def test_event_sequence_is_recoverable_from_source_offset(self):
        first, first_profile, first_customer, first_profile_time = event_for_offset(0, 15)
        second, second_profile, second_customer, second_profile_time = event_for_offset(1, 15)
        submitted, submitted_profile, submitted_customer, submitted_profile_time = (
            event_for_offset(2, 15)
        )
        next_order, _, _, next_profile_time = event_for_offset(3, 15)

        self.assertEqual(
            [first.kind, second.kind, submitted.kind],
            ["add_item", "add_item", "submit"],
        )
        self.assertEqual(first.order_id, second.order_id)
        self.assertEqual(second.order_id, submitted.order_id)
        self.assertEqual(first_customer, second_customer)
        self.assertEqual(second_customer, submitted_customer)
        self.assertEqual(first_profile, second_profile)
        self.assertEqual(second_profile, submitted_profile)
        self.assertEqual(first_profile_time, second_profile_time)
        self.assertEqual(second_profile_time, submitted_profile_time)
        self.assertEqual(first.occurred_at, EVENT_TIME_EPOCH + 3.75)
        self.assertEqual(next_order.occurred_at, EVENT_TIME_EPOCH + 18.75)
        self.assertEqual(next_profile_time, EVENT_TIME_EPOCH + 15)


if __name__ == "__main__":
    unittest.main()
