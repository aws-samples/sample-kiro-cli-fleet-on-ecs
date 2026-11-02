"""The visibility keeper: a short window, renewed while a run is in progress.

The queue's visibility timeout is a window the worker keeps pushing out, not a bound on
how long a run may take. These tests pin the three things that make that safe: the window
is set as soon as the item is held, it keeps being renewed, and renewal stops before the
item is acknowledged so it cannot overwrite a retry delay.
"""

from __future__ import annotations

import threading
import time

import pytest

from runner.aws import SqsQueue
from runner.local import LocalQueue
from runner.visibility import (
    DEFAULT_WINDOW_SECONDS,
    VISIBILITY_HOLD_MARGIN,
    VisibilityKeeper,
)


class RecordingQueue:
    def __init__(self, fail: bool = False) -> None:
        self.calls: list[tuple[str, int]] = []
        self.fail = fail
        self._lock = threading.Lock()

    def extend(self, receipt: str, seconds: int) -> None:
        with self._lock:
            self.calls.append((receipt, seconds))
        if self.fail:
            raise RuntimeError("ReceiptHandleIsInvalid")


class TestKeeper:
    def test_extends_once_before_returning_from_start(self) -> None:
        queue = RecordingQueue()
        keeper = VisibilityKeeper(queue, "r", window_seconds=180, interval_seconds=60)
        keeper.start()
        try:
            assert queue.calls == [("r", 180)]
        finally:
            keeper.stop()

    def test_keeps_extending_on_the_interval(self) -> None:
        queue = RecordingQueue()
        with VisibilityKeeper(queue, "r", window_seconds=180, interval_seconds=0.01):
            time.sleep(0.1)
        assert len(queue.calls) >= 3
        assert all(call == ("r", 180) for call in queue.calls)

    def test_nothing_is_extended_after_stop(self) -> None:
        queue = RecordingQueue()
        with VisibilityKeeper(queue, "r", window_seconds=180, interval_seconds=0.01):
            time.sleep(0.05)
        count = len(queue.calls)
        time.sleep(0.05)
        assert len(queue.calls) == count

    def test_the_default_interval_is_a_third_of_the_window(self) -> None:
        keeper = VisibilityKeeper(RecordingQueue(), "r", window_seconds=180)
        assert keeper._interval == 60

    def test_a_failing_extension_is_counted_and_does_not_raise(self) -> None:
        queue = RecordingQueue(fail=True)
        with VisibilityKeeper(
            queue, "r", window_seconds=180, interval_seconds=0.01
        ) as keeper:
            time.sleep(0.05)
        assert keeper.failures >= 2
        assert keeper.extensions == 0

    def test_rejects_a_non_positive_window(self) -> None:
        with pytest.raises(ValueError):
            VisibilityKeeper(RecordingQueue(), "r", window_seconds=0)

    def test_a_stuck_worker_stops_renewing_at_the_maximum_hold(self) -> None:
        # Fake clock: each read advances 100 s, so the hold passes 300 s within a few beats.
        now = iter(range(0, 100_000, 100))
        queue = RecordingQueue()
        with VisibilityKeeper(
            queue,
            "r",
            window_seconds=60,
            interval_seconds=0.01,
            max_hold_seconds=300,
            clock=lambda: next(now),
        ) as keeper:
            time.sleep(0.1)
        assert keeper.gave_up
        count = len(queue.calls)
        assert count < 10, "kept renewing past the maximum hold"

    def test_the_maximum_hold_matches_the_old_fixed_timeout(self) -> None:
        assert 900 * VISIBILITY_HOLD_MARGIN == 1350

    def test_default_window_is_shorter_than_a_run(self) -> None:
        # The point of the change: recovery after a crash is one window, not one run.
        assert DEFAULT_WINDOW_SECONDS < 900


class TestQueues:
    def test_sqs_extend_sets_the_visibility_from_now(self) -> None:
        class Client:
            def change_message_visibility(self, **parameters):
                self.parameters = parameters

        client = Client()
        SqsQueue(client=client, queue_url="q").extend("r", 180)
        assert client.parameters == {
            "QueueUrl": "q",
            "ReceiptHandle": "r",
            "VisibilityTimeout": 180,
        }

    def test_the_local_queue_hides_an_extended_item(self) -> None:
        queue = LocalQueue(visibility_timeout=0.05)
        queue.seed("a", "go")
        item = queue.receive()
        queue.extend(item.receipt, 60)
        time.sleep(0.1)
        assert queue.receive() is None, "an extended item became visible again"

    def test_an_unextended_item_reappears(self) -> None:
        queue = LocalQueue(visibility_timeout=0.05)
        queue.seed("a", "go")
        queue.receive()
        time.sleep(0.1)
        assert queue.receive() is not None
