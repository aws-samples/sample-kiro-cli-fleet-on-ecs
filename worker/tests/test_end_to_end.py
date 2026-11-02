"""The whole path, on one machine.

Seed work, claim a subscription, run a workflow per item, verify the result, store
it, acknowledge the queue, hand the capacity back. No AWS account and no deployed
infrastructure: the queue and the result store are local implementations of the same
protocols the SQS and S3 ones will satisfy, so the loop under test is the loop that
runs in the fleet.

The workflow itself is stubbed here, because what these tests are about is the order
of operations around it — which is where work gets lost. A run against the real
Kiro CLI lives in test_end_to_end_real_cli.py and needs a credential.
"""

from __future__ import annotations

import io
import json
import logging
import signal
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from leaseclient.claim import Allocator, ClaimConfig, Leases
from leaseclient.credits import apply_external_correction
from leaseclient.metrics import Metrics
from leaseclient.registry import Registry
from leaseclient.schema import SubscriptionState
from leaseclient.session import hold_subscription
from runner.local import LocalQueue, LocalResultStore
from runner.usage import UsageFailure, UsageReading
from runner.worker import Worker
from runner.workflow import OUTPUT_DIR, RunResult, Verdict

WINDOW_END = datetime(2099, 11, 1, tzinfo=UTC)


@pytest.fixture
def sink() -> io.StringIO:
    return io.StringIO()


@pytest.fixture
def registry(pool, dynamodb) -> Registry:
    return Registry(dynamodb, pool.subscriptions_table)


@pytest.fixture
def allocator(pool, dynamodb, secretsmanager, sink, registry) -> Allocator:
    return Allocator(
        registry=registry,
        leases=Leases(dynamodb, pool.leases_table),
        secrets=secretsmanager,
        metrics=Metrics(stream=sink),
        config=ClaimConfig(base_backoff=timedelta(0), max_backoff=timedelta(0)),
        sleep=lambda _: None,
    )


@pytest.fixture
def store(tmp_path: Path) -> LocalResultStore:
    return LocalResultStore(root=tmp_path / "results", reference=tmp_path / "reference")


def writing_workflow(text: str = "done", filename: str = "summary.md"):
    """A stand-in that behaves like a successful run: writes output, answers on stdout."""

    def run(prompt: str, api_key: str, workspace: Path, **_kwargs) -> RunResult:
        assert api_key, "the workflow must receive a credential"
        output = workspace / OUTPUT_DIR
        output.mkdir(parents=True, exist_ok=True)
        (output / filename).write_text(f"{text}: {prompt}\n")
        return RunResult(
            verdict=Verdict.SUCCEEDED,
            exit_code=0,
            stdout=f"answered: {prompt}",
            stderr="",
            outputs=(output / filename,),
        )

    return run


def failing_workflow(verdict: Verdict, stderr: str = "", exit_code: int = 1):
    def run(prompt: str, api_key: str, workspace: Path, **_kwargs) -> RunResult:
        return RunResult(
            verdict=verdict, exit_code=exit_code, stdout="", stderr=stderr, outputs=()
        )

    return run


def a_worker(session, queue, store, registry, sink, tmp_path, run) -> Worker:
    return Worker(
        session=session,
        queue=queue,
        results=store,
        registry=registry,
        metrics=Metrics(stream=sink),
        workspace=tmp_path / "workspace",
        run=run,
    )


class TestTheHappyPath:
    def test_a_batch_is_drained_and_results_are_stored(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        seeded = pool.seed_subscription()
        queue = LocalQueue()
        for i in range(5):
            queue.seed(f"item-{i}", f"summarize file {i}")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_worker(
                session, queue, store, registry, sink, tmp_path, writing_workflow()
            ).drain()

        assert report.completed == 5
        assert report.failed == 0
        assert queue.depth == 0
        for i in range(5):
            (result,) = store.results_for(f"item-{i}")
            assert f"summarize file {i}" in result.read_text()

        # Capacity returned, lease gone.
        assert pool.active_leases(seeded.subscription_id) == 0
        assert pool.lease_count(seeded.subscription_id) == 0

    def test_the_workflow_receives_the_claimed_subscriptions_key(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        seeded = pool.seed_subscription()
        queue = LocalQueue()
        queue.seed("item-0", "go")
        seen: list[str] = []

        def capture(prompt, api_key, workspace, **kwargs):
            seen.append(api_key)
            return writing_workflow()(prompt, api_key, workspace, **kwargs)

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_worker(session, queue, store, registry, sink, tmp_path, capture).drain()

        assert seen == [seeded.key_material]

    def test_each_item_gets_a_clean_workspace(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """One item must not read or overwrite another's files."""
        pool.seed_subscription()
        queue = LocalQueue()
        queue.seed("item-a", "first")
        queue.seed("item-b", "second")
        contents: list[list[str]] = []

        def observe(prompt, api_key, workspace, **kwargs):
            output = workspace / OUTPUT_DIR
            contents.append([p.name for p in output.iterdir()])
            return writing_workflow()(prompt, api_key, workspace, **kwargs)

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_worker(session, queue, store, registry, sink, tmp_path, observe).drain()

        assert contents == [[], []], "a workspace carried files over between items"

    def test_inputs_are_pulled_into_the_workspace(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription()
        reference = tmp_path / "reference"
        reference.mkdir(parents=True, exist_ok=True)
        (reference / "source.c").write_text("int main(void) { return 0; }\n")

        queue = LocalQueue()
        queue.seed("item-0", "summarize", inputs=("source.c",))
        seen: list[list[str]] = []

        def observe(prompt, api_key, workspace, **kwargs):
            seen.append([p.name for p in (workspace / "input").iterdir()])
            return writing_workflow()(prompt, api_key, workspace, **kwargs)

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_worker(session, queue, store, registry, sink, tmp_path, observe).drain()

        assert seen == [["source.c"]]

    def test_a_worker_stops_when_the_queue_is_empty(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """Which is what makes scale-to-zero possible: the task ends by itself."""
        pool.seed_subscription()

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_worker(
                session, LocalQueue(), store, registry, sink, tmp_path, writing_workflow()
            ).drain()

        assert report.completed == 0


class TestWorkIsNotLost:
    def test_a_failed_item_is_not_deleted(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """Left for redelivery and eventually the dead-letter queue."""
        pool.seed_subscription()
        queue = LocalQueue()
        queue.seed("item-0", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_worker(
                session,
                queue,
                store,
                registry,
                sink,
                tmp_path,
                failing_workflow(Verdict.FAILED, stderr="the workflow crashed"),
            ).drain()

        assert report.failed == 1
        assert queue.depth == 1, "the item was discarded instead of redelivered"
        assert store.results_for("item-0") == []

    def test_a_run_cut_off_by_sigterm_is_handed_back_at_once(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """A stop that ignores scale-in protection (`StopTask`) arrives mid-run. The
        SIGTERM handler raises SystemExit; the item must be visible again immediately,
        not after its visibility window, and nothing is stored or deleted."""
        pool.seed_subscription()
        queue = LocalQueue(visibility_timeout=3600)
        queue.seed("item-0", "go")

        def interrupted(prompt, api_key, workspace, **kwargs):
            raise SystemExit(143)

        with (
            pytest.raises(SystemExit),
            hold_subscription(
                allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
            ) as session,
        ):
            a_worker(session, queue, store, registry, sink, tmp_path, interrupted).drain()

        item = queue.receive()
        assert item is not None, "the item waited out its visibility window"
        assert item.item_id == "item-0"
        assert store.results_for("item-0") == []

    def test_a_run_that_produces_nothing_is_treated_as_a_failure(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """Exit zero with no output is the silent failure this fleet must catch."""
        pool.seed_subscription()
        queue = LocalQueue()
        queue.seed("item-0", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_worker(
                session,
                queue,
                store,
                registry,
                sink,
                tmp_path,
                failing_workflow(Verdict.NO_OUTPUT, exit_code=0),
            ).drain()

        assert report.completed == 0
        assert queue.depth == 1

    def test_a_throttled_item_goes_back_immediately(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription()
        queue = LocalQueue()
        queue.seed("item-0", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_worker(
                session,
                queue,
                store,
                registry,
                sink,
                tmp_path,
                failing_workflow(Verdict.THROTTLED, stderr="Too many requests"),
            ).drain()

        assert report.retried == 1
        assert queue.depth == 1, "the item was discarded rather than retried"
        # Invisible for the delay, so this worker does not take it straight back and
        # spin on it until the redrive policy sets it aside.
        assert queue.in_flight == 1

    def test_a_poisonous_item_is_eventually_set_aside(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """Otherwise one bad item occupies the fleet indefinitely."""
        pool.seed_subscription()
        queue = LocalQueue(max_receives=2)
        queue.seed("item-poison", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            worker = Worker(
                session=session,
                queue=queue,
                results=store,
                registry=registry,
                metrics=Metrics(stream=sink),
                workspace=tmp_path / "workspace",
                # No delay here so the redrive behavior is reached without waiting;
                # the delay itself is asserted separately.
                retry_delay_seconds=0.0,
                run=failing_workflow(Verdict.THROTTLED, stderr="throttled"),
            )
            worker.drain()

        assert queue.depth == 0
        assert len(queue.dead_lettered) == 1


class TestAHeldItemStaysHeld:
    """The worker renews a short visibility window while the run is in progress."""

    def a_windowed_worker(self, session, queue, store, registry, sink, tmp_path, run):
        return Worker(
            session=session,
            queue=queue,
            results=store,
            registry=registry,
            metrics=Metrics(stream=sink),
            workspace=tmp_path / "workspace",
            run=run,
            visibility_window_seconds=60,
            retry_delay_seconds=1.0,
            jitter=lambda: 1.0,
        )

    def test_a_run_longer_than_the_queues_timeout_is_not_handed_out_twice(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription()
        # The queue's own timeout is far shorter than the run; only renewal holds the item.
        queue = LocalQueue(visibility_timeout=0.05)
        queue.seed("item-slow", "go")
        seen_by_another_worker = []
        succeed = writing_workflow()

        def slow(prompt, api_key, workspace, **kwargs):
            time.sleep(0.2)
            seen_by_another_worker.append(queue.receive())
            return succeed(prompt, api_key, workspace, **kwargs)

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = self.a_windowed_worker(
                session, queue, store, registry, sink, tmp_path, slow
            ).drain()

        assert seen_by_another_worker == [None], "the item was visible mid-run"
        assert report.completed == 1
        assert queue.depth == 0

    def test_renewal_stops_before_a_retry_delay_is_set(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription()
        queue = LocalQueue(visibility_timeout=0.05)
        queue.seed("item-0", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            self.a_windowed_worker(
                session,
                queue,
                store,
                registry,
                sink,
                tmp_path,
                failing_workflow(Verdict.THROTTLED, stderr="throttled"),
            ).drain()

        # Hidden for the retry delay (1 s), not the 60 s window: an extension after the
        # release would have replaced the delay the worker chose.
        (message,) = queue._messages
        remaining = message.invisible_until - time.monotonic()
        assert 0 < remaining <= 1.0


class TestABadCredentialTakesTheSubscriptionOutOfRotation:
    def test_the_subscription_is_disabled(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """The key is bad for everyone, so the next worker must not rediscover it."""
        seeded = pool.seed_subscription()
        queue = LocalQueue()
        queue.seed("item-0", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_worker(
                session,
                queue,
                store,
                registry,
                sink,
                tmp_path,
                failing_workflow(
                    Verdict.CREDENTIAL_REJECTED,
                    exit_code=0,
                    stderr="Access denied: The bearer token included in the request is invalid.",
                ),
            ).drain()

        assert registry.get(seeded.subscription_id).state is SubscriptionState.DISABLED

    def test_the_item_goes_back_for_a_worker_with_a_working_key(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription()
        queue = LocalQueue()
        queue.seed("item-0", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_worker(
                session,
                queue,
                store,
                registry,
                sink,
                tmp_path,
                failing_workflow(Verdict.CREDENTIAL_REJECTED, exit_code=0, stderr="Access denied"),
            ).drain()

        assert queue.in_flight == 0
        assert queue.depth == 1

    def test_the_worker_stops_rather_than_burning_the_queue(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription()
        queue = LocalQueue()
        for i in range(5):
            queue.seed(f"item-{i}", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_worker(
                session,
                queue,
                store,
                registry,
                sink,
                tmp_path,
                failing_workflow(Verdict.CREDENTIAL_REJECTED, exit_code=0, stderr="Access denied"),
            ).drain()

        assert report.failed == 1, "the worker kept going after a rejected credential"
        assert queue.depth == 5


class ScriptedUsage:
    """Stands in for `/usage`: returns the scripted readings in order, then the last.

    Records how often it was called and with which key.
    """

    def __init__(self, *readings: "UsageReading | UsageFailure") -> None:
        self._readings = list(readings)
        self.calls = 0
        self.keys: list[str] = []

    def read(self, api_key: str) -> "UsageReading | UsageFailure":
        self.keys.append(api_key)
        index = min(self.calls, len(self._readings) - 1)
        self.calls += 1
        return self._readings[index]


def a_reading(used: str, resets_at: datetime = WINDOW_END) -> UsageReading:
    return UsageReading(Decimal(used), Decimal("10000"), resets_at)


def metric_records(sink: io.StringIO, name: str) -> list[dict]:
    return [r for r in map(json.loads, sink.getvalue().splitlines()) if name in r]


def a_tracking_worker(session, queue, store, registry, sink, tmp_path, run, usage, margin=1):
    return Worker(
        session=session,
        queue=queue,
        results=store,
        registry=registry,
        metrics=Metrics(stream=sink),
        workspace=tmp_path / "workspace",
        run=run,
        usage=usage,
        credit_margin=Decimal(margin),
    )


class TestCreditsAreTracked:
    """`creditsUsed` follows the server's cumulative figure, read before every item."""

    def test_readings_come_before_every_item_and_once_at_drain_end(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription(window_resets_raw=WINDOW_END.isoformat())
        queue = LocalQueue()
        for i in range(3):
            queue.seed(f"item-{i}", "go")
        order: list[str] = []
        usage = ScriptedUsage(a_reading("1"))
        original_read = usage.read

        def read(api_key: str):
            order.append("read")
            return original_read(api_key)

        usage.read = read  # type: ignore[method-assign]

        def run(prompt, api_key, workspace, **kwargs):
            order.append("run")
            return writing_workflow()(prompt, api_key, workspace, **kwargs)

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_tracking_worker(
                session, queue, store, registry, sink, tmp_path, run, usage
            ).drain()

        # Before item 1 (straight after claim and key load), before items 2 and 3,
        # before the receive that finds the queue empty, and once at drain end.
        assert order == ["read", "run", "read", "run", "read", "run", "read", "read"]
        phases = [r["Phase"] for r in metric_records(sink, "UsageReading")]
        assert phases == ["PreItem"] * 4 + ["DrainEnd"]

    def test_the_reading_uses_the_claimed_subscriptions_key(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        seeded = pool.seed_subscription(window_resets_raw=WINDOW_END.isoformat())
        usage = ScriptedUsage(a_reading("1"))

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_tracking_worker(
                session, LocalQueue(), store, registry, sink, tmp_path,
                writing_workflow(), usage,
            ).drain()

        assert set(usage.keys) == {seeded.key_material}

    def test_credits_used_follows_the_readings_and_never_goes_down(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        seeded = pool.seed_subscription(
            credits_used="0", window_resets_raw=WINDOW_END.isoformat()
        )
        queue = LocalQueue()
        for i in range(4):
            queue.seed(f"item-{i}", "go")
        # Out of order, as readings from several workers arrive.
        usage = ScriptedUsage(
            a_reading("10.00"), a_reading("12.50"), a_reading("11.00"),
            a_reading("13.08"), a_reading("12.97"), a_reading("12.00"),
        )
        seen: list[Decimal] = []

        def run(prompt, api_key, workspace, **kwargs):
            seen.append(pool.credits_used(seeded.subscription_id))
            return writing_workflow()(prompt, api_key, workspace, **kwargs)

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_tracking_worker(
                session, queue, store, registry, sink, tmp_path, run, usage
            ).drain()

        assert report.completed == 4
        assert seen == [Decimal("10.00"), Decimal("12.50"), Decimal("12.50"), Decimal("13.08")]
        assert pool.credits_used(seeded.subscription_id) == Decimal("13.08")
        outcomes = [r["Outcome"] for r in metric_records(sink, "UsageReading")]
        assert outcomes.count("Unchanged") == 3

    def test_reaching_the_ceiling_mid_drain_stops_and_leaves_the_rest(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        seeded = pool.seed_subscription(
            credit_budget=100, credits_used="0", window_resets_raw=WINDOW_END.isoformat()
        )
        # max_receives=1: had a remaining item been received and handed back, the
        # receives below would dead-letter it instead of returning it.
        queue = LocalQueue(max_receives=1)
        for i in range(5):
            queue.seed(f"item-{i}", "go")
        # Ceiling = 100 - margin 1 = 99. The third reading reaches it.
        usage = ScriptedUsage(a_reading("10"), a_reading("50"), a_reading("99.00"))

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_tracking_worker(
                session, queue, store, registry, sink, tmp_path, writing_workflow(), usage
            ).drain()

        assert report.completed == 2
        assert report.budget_exhausted is True
        assert pool.credits_used(seeded.subscription_id) == Decimal("99.00")
        # No drain-end reading after a budget stop: the pre-item one was just recorded.
        assert usage.calls == 3

        # The lease is released...
        assert pool.active_leases(seeded.subscription_id) == 0
        assert pool.lease_count(seeded.subscription_id) == 0
        # ...and the remaining items are visible, never received by this worker.
        assert queue.depth == 3
        assert queue.in_flight == 0
        remaining = [queue.receive() for _ in range(3)]
        assert all(item is not None for item in remaining)
        assert queue.dead_lettered == []

        stops = metric_records(sink, "BudgetExhausted")
        assert [r["SubscriptionId"] for r in stops] == [seeded.subscription_id]

        # The next claim refuses the subscription on its budget.
        before = len(sink.getvalue())
        assert allocator.claim("task-2") is None
        outcomes = {
            record["Outcome"]
            for record in map(json.loads, sink.getvalue()[before:].splitlines())
            if "Outcome" in record
        }
        assert outcomes == {"BudgetExhausted", "Saturated"}

    def test_a_decimal_margin_stops_at_the_hundredth(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription(credit_budget=10, window_resets_raw=WINDOW_END.isoformat())
        queue = LocalQueue()
        for i in range(3):
            queue.seed(f"item-{i}", "go")
        usage = ScriptedUsage(a_reading("9.78"), a_reading("9.79"))

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_tracking_worker(
                session, queue, store, registry, sink, tmp_path, writing_workflow(),
                usage, margin=Decimal("0.21"),
            ).drain()

        assert report.completed == 1
        assert report.budget_exhausted is True

    def test_a_failed_reading_lets_the_work_run_and_is_counted(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """Fail open. A failed reading is never recorded as zero."""
        seeded = pool.seed_subscription(
            credits_used="40", window_resets_raw=WINDOW_END.isoformat()
        )
        queue = LocalQueue()
        queue.seed("item-0", "go")
        usage = ScriptedUsage(UsageFailure.NO_CREDITS_LINE)

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_tracking_worker(
                session, queue, store, registry, sink, tmp_path, writing_workflow(), usage
            ).drain()

        assert report.completed == 1
        assert pool.credits_used(seeded.subscription_id) == Decimal("40")
        outcomes = {r["Outcome"] for r in metric_records(sink, "UsageReading")}
        assert outcomes == {"ReadFailed"}

    def test_a_failed_reading_still_checks_the_stored_figure(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """The report or an operator may have raised it since the claim."""
        seeded = pool.seed_subscription(
            credit_budget=100, credits_used="0", window_resets_raw=WINDOW_END.isoformat()
        )
        queue = LocalQueue()
        queue.seed("item-0", "go")
        queue.seed("item-1", "go")

        def run(prompt, api_key, workspace, **kwargs):
            apply_external_correction(registry, seeded.subscription_id, 100)
            return writing_workflow()(prompt, api_key, workspace, **kwargs)

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_tracking_worker(
                session, queue, store, registry, sink, tmp_path, run,
                ScriptedUsage(UsageFailure.TIMED_OUT),
            ).drain()

        assert report.completed == 1
        assert report.budget_exhausted is True
        assert queue.depth == 1

    def test_the_runs_own_figure_is_a_metric_and_not_accounting(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        seeded = pool.seed_subscription(
            credits_used="5", window_resets_raw=WINDOW_END.isoformat()
        )
        queue = LocalQueue()
        queue.seed("item-0", "go")

        def run(prompt, api_key, workspace, **kwargs):
            result = writing_workflow()(prompt, api_key, workspace, **kwargs)
            return replace(result, credits=Decimal("0.05"))

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_tracking_worker(
                session, queue, store, registry, sink, tmp_path, run,
                ScriptedUsage(a_reading("5")),
            ).drain()

        (record,) = metric_records(sink, "RunCredits")
        assert record["RunCredits"] == 0.05
        assert record["ItemId"] == "item-0"
        assert pool.credits_used(seeded.subscription_id) == Decimal("5")
        assert metric_records(sink, "RunCreditsMissing") == []

    def test_a_run_that_printed_no_figure_is_counted_and_logged(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path, caplog
    ) -> None:
        """Measured: one of two identical runs printed no credit figure on either stream.
        Counted so the gap shows; nothing else about the item changes."""
        seeded = pool.seed_subscription(
            credits_used="5", window_resets_raw=WINDOW_END.isoformat()
        )
        queue = LocalQueue()
        queue.seed("item-0", "go")
        token = "B" * 48

        def run(prompt, api_key, workspace, **kwargs):
            result = writing_workflow()(prompt, api_key, workspace, **kwargs)
            return replace(
                result,
                credits=None,
                stderr=f"[tool] Creating a.md\n[tool] status: Completed {token}\n",
            )

        with caplog.at_level(logging.WARNING, logger="runner.worker"):
            with hold_subscription(
                allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
            ) as session:
                report = a_tracking_worker(
                    session, queue, store, registry, sink, tmp_path, run,
                    ScriptedUsage(a_reading("5")),
                ).drain()

        assert report.completed == 1
        (record,) = metric_records(sink, "RunCreditsMissing")
        assert record["RunCreditsMissing"] == 1
        assert record["Verdict"] == "Succeeded"
        assert record["ItemId"] == "item-0"
        assert record["SubscriptionId"] == seeded.subscription_id
        assert metric_records(sink, "RunCredits") == []
        assert "item-0 printed no Credits figure" in caplog.text
        assert "[tool] status: Completed <REDACTED>" in caplog.text
        assert token not in caplog.text
        assert pool.credits_used(seeded.subscription_id) == Decimal("5")

    def test_a_missing_figure_is_warned_once_and_counted_every_time(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path, caplog
    ) -> None:
        """Kiro CLI 2.30 prints no figure on any headless run (0 of 600 measured). One
        warning per worker, not one per item; the metric still counts each run."""
        pool.seed_subscription(
            credits_used="5", window_resets_raw=WINDOW_END.isoformat()
        )
        queue = LocalQueue()
        for index in range(3):
            queue.seed(f"item-{index}", "go")

        def run(prompt, api_key, workspace, **kwargs):
            result = writing_workflow()(prompt, api_key, workspace, **kwargs)
            return replace(result, credits=None, stderr="[tool] status: Completed\n")

        with (
            caplog.at_level(logging.WARNING, logger="runner.worker"),
            hold_subscription(
                allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
            ) as session,
        ):
            report = a_tracking_worker(
                session, queue, store, registry, sink, tmp_path, run,
                ScriptedUsage(a_reading("5"), a_reading("5"), a_reading("5")),
            ).drain()

        assert report.completed == 3
        assert len(metric_records(sink, "RunCreditsMissing")) == 3
        warnings = [r for r in caplog.records if "printed no Credits figure" in r.message]
        assert len(warnings) == 1
        assert warnings[0].levelno == logging.WARNING

    def test_a_failed_run_without_a_figure_carries_its_own_verdict(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """A throttled run never prints a summary. Its missing figure is counted under its
        verdict, so `Verdict=Succeeded` stays a measure of the CLI's output format alone."""
        pool.seed_subscription(credits_used="5", window_resets_raw=WINDOW_END.isoformat())
        queue = LocalQueue()
        queue.seed("item-0", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_tracking_worker(
                session, queue, store, registry, sink, tmp_path,
                failing_workflow(Verdict.THROTTLED, stderr="Too many requests"),
                ScriptedUsage(a_reading("5")),
            ).drain()

        (record,) = metric_records(sink, "RunCreditsMissing")
        assert record["Verdict"] == "Throttled"
        assert record["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [["Verdict"]]

    def test_the_recorded_figure_and_budget_are_published_per_subscription(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        seeded = pool.seed_subscription(
            credit_budget=1_000, window_resets_raw=WINDOW_END.isoformat()
        )

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_tracking_worker(
                session, LocalQueue(), store, registry, sink, tmp_path,
                writing_workflow(), ScriptedUsage(a_reading("12.97")),
            ).drain()

        used = metric_records(sink, "SubscriptionCreditsUsed")
        budget = metric_records(sink, "SubscriptionCreditBudget")
        assert used[-1]["SubscriptionCreditsUsed"] == 12.97
        assert used[-1]["SubscriptionId"] == seeded.subscription_id
        assert budget[-1]["SubscriptionCreditBudget"] == 1000.0

    def test_an_operator_correction_still_replaces_the_figure(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """The human override, the one path that may lower within a window."""
        seeded = pool.seed_subscription(credit_budget=1_000, credits_used="900")

        apply_external_correction(registry, seeded.subscription_id, 1_000)
        assert pool.credits_used(seeded.subscription_id) == 1_000
        assert allocator.claim("task-2") is None

        apply_external_correction(registry, seeded.subscription_id, "250.50")
        assert pool.credits_used(seeded.subscription_id) == Decimal("250.50")
        assert allocator.claim("task-3") is not None


class RecordingProtection:
    """Stands in for the ECS agent client and records each call in a shared order."""

    def __init__(
        self, order: list[str], enabled: bool = True, raises: bool = False,
        stopping_after: int | None = None,
    ) -> None:
        self._order = order
        self._enabled = enabled
        self._raises = raises
        # After this many enables, behave like ECS answering TASK_STOPPING_OR_STOPPED.
        self._stopping_after = stopping_after
        self._enables = 0
        self.stopping = False

    def enable(self) -> bool:
        self._order.append("protect")
        if self._raises:
            raise RuntimeError("the protection client broke")
        self._enables += 1
        if self._stopping_after is not None and self._enables > self._stopping_after:
            self.stopping = True
            return False
        return self._enabled

    def disable(self) -> bool:
        self._order.append("unprotect")
        if self._raises:
            raise RuntimeError("the protection client broke")
        return self._enabled


class RecordingQueue:
    """A LocalQueue that records receives and acknowledgements in the same order."""

    def __init__(self, inner: LocalQueue, order: list[str]) -> None:
        self._inner = inner
        self._order = order

    def receive(self):
        item = self._inner.receive()
        self._order.append("receive" if item is not None else "receive-empty")
        return item

    def delete(self, receipt: str) -> None:
        self._order.append("delete")
        self._inner.delete(receipt)

    def release(self, receipt: str, delay_seconds: float = 0.0) -> None:
        self._order.append("release")
        self._inner.release(receipt, delay_seconds)

    @property
    def depth(self) -> int:
        return self._inner.depth

    @property
    def in_flight(self) -> int:
        return self._inner.in_flight


def recorded_run(order: list[str], run):
    def wrapper(prompt, api_key, workspace, **kwargs):
        order.append("run")
        return run(prompt, api_key, workspace, **kwargs)

    return wrapper


def recorded_usage(order: list[str], *readings) -> ScriptedUsage:
    usage = ScriptedUsage(*readings)
    original_read = usage.read

    def read(api_key: str):
        order.append("read")
        return original_read(api_key)

    usage.read = read  # type: ignore[method-assign]
    return usage


def a_protected_worker(
    session, queue, store, registry, sink, tmp_path, run, protection, usage=None, margin=1
) -> Worker:
    return Worker(
        session=session,
        queue=queue,
        results=store,
        registry=registry,
        metrics=Metrics(stream=sink),
        workspace=tmp_path / "workspace",
        run=run,
        usage=usage,
        credit_margin=Decimal(margin),
        protection=protection,
    )


class TestScaleInProtection:
    """The task is protected from scale-in exactly while it holds an item.

    A run that is stopped part-way has spent its credits and is redelivered, so the item is
    paid for twice. Protection on before the receive and off after the acknowledgement keeps
    ECS from choosing this task until it is between items.
    """

    def test_protection_covers_each_item_from_receive_to_acknowledgement(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription(window_resets_raw=WINDOW_END.isoformat())
        order: list[str] = []
        inner = LocalQueue()
        inner.seed("item-0", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_protected_worker(
                session, RecordingQueue(inner, order), store, registry, sink, tmp_path,
                recorded_run(order, writing_workflow()), RecordingProtection(order),
                usage=recorded_usage(order, a_reading("1")),
            ).drain()

        assert report.completed == 1
        # The budget check first, then protection around receive, run and delete; the same
        # around the receive that finds the queue empty; then the drain-end reading.
        assert order == [
            "read", "protect", "receive", "run", "delete", "unprotect",
            "read", "protect", "receive-empty", "unprotect",
            "read",
        ]

    def test_protection_is_turned_off_after_a_run_that_raises(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription()
        order: list[str] = []
        inner = LocalQueue()
        inner.seed("item-0", "go")

        def boom(prompt, api_key, workspace, **kwargs):
            raise RuntimeError("boom")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            worker = a_protected_worker(
                session, RecordingQueue(inner, order), store, registry, sink, tmp_path,
                recorded_run(order, boom), RecordingProtection(order),
            )
            with pytest.raises(RuntimeError, match="boom"):
                worker.drain()

        assert order == ["protect", "receive", "run", "unprotect"]
        assert inner.depth == 1, "the item was deleted after a run that raised"

    def test_a_task_ecs_is_stopping_takes_no_further_item(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """M2 benchmark: a task refused with TASK_STOPPING_OR_STOPPED still received, was
        killed mid-run, and its item waited out the visibility timeout. It must stop first."""
        pool.seed_subscription()
        order: list[str] = []
        inner = LocalQueue()
        inner.seed("item-0", "go")
        inner.seed("item-1", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_protected_worker(
                session, RecordingQueue(inner, order), store, registry, sink, tmp_path,
                recorded_run(order, writing_workflow()), RecordingProtection(order, stopping_after=1),
            ).drain()

        assert report.completed == 1
        assert order == ["protect", "receive", "run", "delete", "unprotect", "protect"]
        assert inner.depth == 1, "the second item must stay on the queue for another task"

    def test_protection_is_turned_off_after_a_timed_out_run(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription()
        order: list[str] = []
        inner = LocalQueue()
        inner.seed("item-0", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_protected_worker(
                session, RecordingQueue(inner, order), store, registry, sink, tmp_path,
                recorded_run(order, failing_workflow(Verdict.TIMED_OUT, exit_code=None)),
                RecordingProtection(order),
            ).drain()

        assert report.retried == 1
        assert report.verdicts == [Verdict.TIMED_OUT]
        # Handed back with a delay, so the next receive finds nothing.
        assert order == [
            "protect", "receive", "run", "release", "unprotect",
            "protect", "receive-empty", "unprotect",
        ]

    def test_protection_is_turned_off_when_a_rejected_credential_stops_the_worker(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription()
        order: list[str] = []
        inner = LocalQueue()
        inner.seed("item-0", "go")
        inner.seed("item-1", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_protected_worker(
                session, RecordingQueue(inner, order), store, registry, sink, tmp_path,
                recorded_run(
                    order,
                    failing_workflow(
                        Verdict.CREDENTIAL_REJECTED, exit_code=0, stderr="Access denied"
                    ),
                ),
                RecordingProtection(order),
            ).drain()

        assert order == ["protect", "receive", "run", "release", "unprotect"]

    def test_protection_is_turned_off_when_sigterm_arrives_during_a_run(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """`StopTask` or task retirement can still send SIGTERM mid-item. The session's own
        handler returns the lease and exits with 143, the item is handed back at once
        rather than left to reappear after its visibility window, and protection is turned
        off on the way out. Nothing is deleted."""
        seeded = pool.seed_subscription()
        order: list[str] = []
        inner = LocalQueue()
        inner.seed("item-0", "go")

        def stopped(prompt, api_key, workspace, **kwargs):
            signal.raise_signal(signal.SIGTERM)
            raise AssertionError("the SIGTERM handler did not stop the run")

        with pytest.raises(SystemExit) as exited:
            with hold_subscription(
                allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
            ) as session:
                a_protected_worker(
                    session, RecordingQueue(inner, order), store, registry, sink, tmp_path,
                    recorded_run(order, stopped), RecordingProtection(order),
                ).drain()

        assert exited.value.code == 143
        assert order == ["protect", "receive", "run", "release", "unprotect"]
        assert inner.depth == 1, "the item was deleted after SIGTERM"
        assert pool.active_leases(seeded.subscription_id) == 0

    def test_an_empty_receive_turns_protection_off_before_the_drain_ends(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription(window_resets_raw=WINDOW_END.isoformat())
        order: list[str] = []

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_protected_worker(
                session, RecordingQueue(LocalQueue(), order), store, registry, sink,
                tmp_path, recorded_run(order, writing_workflow()), RecordingProtection(order),
                usage=recorded_usage(order, a_reading("1")),
            ).drain()

        assert order == ["read", "protect", "receive-empty", "unprotect", "read"]

    def test_a_worker_at_its_credit_ceiling_does_not_protect_itself_again(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """The budget check comes first, so the worker that stops for credits leaves
        unprotected and ECS may stop it at once."""
        pool.seed_subscription(
            credit_budget=100, credits_used="0", window_resets_raw=WINDOW_END.isoformat()
        )
        order: list[str] = []
        inner = LocalQueue()
        for i in range(3):
            inner.seed(f"item-{i}", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_protected_worker(
                session, RecordingQueue(inner, order), store, registry, sink, tmp_path,
                recorded_run(order, writing_workflow()), RecordingProtection(order),
                usage=recorded_usage(order, a_reading("10"), a_reading("99.00")),
            ).drain()

        assert report.budget_exhausted is True
        assert order == [
            "read", "protect", "receive", "run", "delete", "unprotect",
            "read",
        ]
        assert order.count("protect") == 1
        assert order[-1] == "read"

    def test_a_failed_protection_request_lets_the_item_run(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        pool.seed_subscription()
        order: list[str] = []
        inner = LocalQueue()
        inner.seed("item-0", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_protected_worker(
                session, RecordingQueue(inner, order), store, registry, sink, tmp_path,
                recorded_run(order, writing_workflow()),
                RecordingProtection(order, enabled=False),
            ).drain()

        assert report.completed == 1
        assert len(store.results_for("item-0")) == 1
        assert order[:5] == ["protect", "receive", "run", "delete", "unprotect"]

    def test_a_protection_object_that_raises_does_not_stop_the_run(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path, caplog
    ) -> None:
        pool.seed_subscription()
        order: list[str] = []
        inner = LocalQueue()
        inner.seed("item-0", "go")

        with caplog.at_level(logging.WARNING, logger="runner.worker"):
            with hold_subscription(
                allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
            ) as session:
                report = a_protected_worker(
                    session, RecordingQueue(inner, order), store, registry, sink,
                    tmp_path, recorded_run(order, writing_workflow()),
                    RecordingProtection(order, raises=True),
                ).drain()

        assert report.completed == 1
        assert len(store.results_for("item-0")) == 1
        # On and off for the item, on and off for the empty receive. The stub workflow
        # prints no Credits figure, so `_acknowledge` logs its own warning too.
        protection_warnings = [
            record
            for record in caplog.records
            if record.name == "runner.worker"
            and record.levelno == logging.WARNING
            and "scale-in protection" in record.getMessage()
        ]
        assert len(protection_warnings) == 4

    def test_an_unreachable_agent_lets_the_batch_run_and_counts_each_failed_call(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """The real client, pointed at an agent that is not there: fail-open, end to end."""
        import socket

        from runner.protection import TaskProtection

        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]

        pool.seed_subscription()
        queue = LocalQueue()
        queue.seed("item-0", "go")
        protection = TaskProtection.from_environment(
            workflow_timeout_seconds=900,
            metrics=Metrics(stream=sink),
            environ={"ECS_AGENT_URI": f"http://127.0.0.1:{port}/v4/test-container"},
        )

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_protected_worker(
                session, queue, store, registry, sink, tmp_path, writing_workflow(),
                protection,
            ).drain()

        assert report.completed == 1
        assert queue.depth == 0
        calls = [
            (r["Action"], r["Reason"]) for r in metric_records(sink, "TaskProtectionFailed")
        ]
        assert calls == [("Enable", "Unreachable"), ("Disable", "Unreachable")] * 2

    def test_without_an_agent_the_loop_makes_no_protection_request(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path, monkeypatch
    ) -> None:
        """A local run or plain Docker: no `ECS_AGENT_URI`, so nothing is called."""
        import urllib.request

        from runner.protection import TaskProtection

        def refuse(self, *args, **kwargs):
            raise AssertionError("a protection request was made without an agent")

        monkeypatch.setattr(urllib.request.OpenerDirector, "open", refuse)
        pool.seed_subscription()
        queue = LocalQueue()
        queue.seed("item-0", "go")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            report = a_protected_worker(
                session, queue, store, registry, sink, tmp_path, writing_workflow(),
                TaskProtection.from_environment(
                    workflow_timeout_seconds=900, metrics=Metrics(stream=sink), environ={}
                ),
            ).drain()

        assert report.completed == 1
        assert metric_records(sink, "TaskProtectionFailed") == []


class TestRetryIsSafe:
    def test_a_redelivered_item_overwrites_rather_than_duplicating(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """Deterministic result keys are what make redelivery harmless."""
        pool.seed_subscription()
        queue = LocalQueue()
        queue.seed("item-0", "first pass")

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            a_worker(
                session, queue, store, registry, sink, tmp_path, writing_workflow("v1")
            ).drain()

        queue.seed("item-0", "second pass")
        with hold_subscription(
            allocator, dynamodb, holder_id="task-2", leases_table=pool.leases_table
        ) as session:
            a_worker(
                session, queue, store, registry, sink, tmp_path, writing_workflow("v2")
            ).drain()

        results = store.results_for("item-0")
        assert len(results) == 1, "a retry left two copies"
        assert "v2" in results[0].read_text()


class TestSeveralWorkersOnOnePool:
    def test_the_batch_is_split_without_duplication(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """Two workers, one queue: every item done exactly once."""
        pool.seed_subscriptions(2, max_concurrency=1)
        queue = LocalQueue()
        for i in range(20):
            queue.seed(f"item-{i}", f"work {i}")

        reports = []
        lock = threading.Lock()

        def run_worker(index: int) -> None:
            with hold_subscription(
                allocator,
                dynamodb,
                holder_id=f"task-{index}",
                leases_table=pool.leases_table,
                install_signal_handler=False,
            ) as session:
                report = Worker(
                    session=session,
                    queue=queue,
                    results=store,
                    registry=registry,
                    metrics=Metrics(stream=io.StringIO()),
                    workspace=tmp_path / f"workspace-{index}",
                    run=writing_workflow(),
                ).drain()
            with lock:
                reports.append(report)

        threads = [threading.Thread(target=run_worker, args=(i,)) for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert sum(r.completed for r in reports) == 20
        assert queue.depth == 0
        for i in range(20):
            assert len(store.results_for(f"item-{i}")) == 1

    def test_a_third_worker_is_turned_away_when_the_pool_is_full(
        self, pool, dynamodb, allocator, registry, store, sink, tmp_path
    ) -> None:
        """Saturation is the answer, and the fleet outgrowing the pool is normal."""
        from leaseclient.session import PoolSaturated

        pool.seed_subscription(max_concurrency=1)

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ):
            with pytest.raises(PoolSaturated):
                with hold_subscription(
                    allocator,
                    dynamodb,
                    holder_id="task-2",
                    leases_table=pool.leases_table,
                    install_signal_handler=False,
                ):
                    pytest.fail("a second worker should not have got capacity")


class DelayRecordingQueue:
    """Records the delay of each release, then makes the item visible at once."""

    def __init__(self, inner: LocalQueue) -> None:
        self._inner = inner
        self.delays: list[float] = []

    def receive(self):
        return self._inner.receive()

    def delete(self, receipt: str) -> None:
        self._inner.delete(receipt)

    def release(self, receipt: str, delay_seconds: float = 0.0) -> None:
        self.delays.append(delay_seconds)
        self._inner.release(receipt)

    @property
    def depth(self) -> int:
        return self._inner.depth

    @property
    def in_flight(self) -> int:
        return self._inner.in_flight


def test_the_worker_backs_off_exponentially(pool, dynamodb, allocator, registry, store, tmp_path):
    pool.seed_subscription()
    inner = LocalQueue(max_receives=3)
    inner.seed("item-0", "go")
    queue = DelayRecordingQueue(inner)

    with hold_subscription(allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table) as session:
        Worker(
            session=session,
            queue=queue,
            results=store,
            registry=registry,
            metrics=Metrics(stream=io.StringIO()),
            workspace=tmp_path / "workspace",
            retry_delay_seconds=30.0,
            run=failing_workflow(Verdict.THROTTLED, stderr="Too many requests"),
            jitter=lambda: 0.0,
        ).drain()

    # Three deliveries before the redrive limit, each waiting twice as long as the last.
    assert queue.delays == [15.0, 30.0, 60.0]
    assert len(inner.dead_lettered) == 1
