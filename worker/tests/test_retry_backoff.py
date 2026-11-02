"""Retry backoff for throttled and timed-out items.

A fixed delay retries a saturated key on a fixed beat. The delay doubles with each
delivery of the same item and carries jitter, so items throttled together do not
come back together.
"""

from __future__ import annotations

from runner.aws import SqsQueue
from runner.local import LocalQueue
from runner.worker import MAX_RETRY_DELAY_SECONDS, WorkItem, retry_delay


class TestRetryDelay:
    def test_doubles_per_delivery(self) -> None:
        assert [retry_delay(30, n, 1.0) for n in (1, 2, 3)] == [30, 60, 120]

    def test_jitter_spreads_over_the_upper_half(self) -> None:
        assert retry_delay(30, 1, 0.0) == 15
        assert 15 <= retry_delay(30, 1, 0.5) < 30

    def test_is_capped(self) -> None:
        assert retry_delay(30, 20, 1.0) == MAX_RETRY_DELAY_SECONDS

    def test_zero_base_stays_zero(self) -> None:
        assert retry_delay(0, 3, 0.7) == 0

    def test_a_missing_count_is_treated_as_the_first_delivery(self) -> None:
        assert retry_delay(30, 0, 1.0) == 30


class TestReceiveCount:
    def test_read_from_the_message_attributes(self) -> None:
        item = WorkItem.from_message(
            {
                "Body": '{"itemId": "a", "prompt": "go"}',
                "ReceiptHandle": "r",
                "Attributes": {"ApproximateReceiveCount": "3"},
            }
        )
        assert item.receive_count == 3

    def test_defaults_to_one_without_attributes(self) -> None:
        item = WorkItem.from_message({"Body": '{"itemId": "a", "prompt": "go"}', "ReceiptHandle": "r"})
        assert item.receive_count == 1

    def test_sqs_receive_asks_for_it(self) -> None:
        class Client:
            def receive_message(self, **parameters):
                self.parameters = parameters
                return {}

        client = Client()
        SqsQueue(client=client, queue_url="q").receive()
        assert client.parameters["MessageSystemAttributeNames"] == ["ApproximateReceiveCount"]

    def test_the_local_queue_counts_deliveries(self) -> None:
        queue = LocalQueue(visibility_timeout=0)
        queue.seed("a", "go")
        first = queue.receive()
        queue.release(first.receipt)
        assert first.receive_count == 1
        assert queue.receive().receive_count == 2
