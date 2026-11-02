"""Seeding a batch, and the signal that scales the fleet.

Both are small, and both can be wrong in ways nothing downstream reports. A seeding pass that
generates identifiers makes a re-seed double a batch, and every item still succeeds. A scaling
signal that divides by the running count publishes nothing when the fleet is empty, and an
empty fleet is precisely when something has to start.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest

from leaseclient.registry import SubscriptionRecord, SubscriptionState
from scaling.handler import FleetSignal, narrow_capacity, read_signal
from seeding.handler import InvalidManifest, Seeder, WorkItem, identifier_for


class FakeS3:
    def __init__(self, manifest: dict[str, Any], keys: list[str] | None = None) -> None:
        self._manifest = manifest
        self._keys = keys or []

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        class Body:
            def __init__(self, payload: bytes) -> None:
                self._payload = payload

            def read(self) -> bytes:
                return self._payload

        return {"Body": Body(json.dumps(self._manifest).encode())}

    def get_paginator(self, _name: str) -> Any:
        keys = self._keys

        class Paginator:
            def paginate(self, **_kwargs: Any) -> list[dict[str, Any]]:
                return [{"Contents": [{"Key": k} for k in keys]}]

        return Paginator()


class FakeSqs:
    def __init__(self, fail_ids: set[str] | None = None) -> None:
        self.sent: list[dict[str, Any]] = []
        self.batches = 0
        self._fail_ids = fail_ids or set()

    def send_message_batch(self, QueueUrl: str, Entries: list[dict]) -> dict:  # noqa: N803
        self.batches += 1
        failed = [{"Id": e["Id"], "Message": "throttled"} for e in Entries if e["Id"] in self._fail_ids]
        self.sent.extend(e for e in Entries if e["Id"] not in self._fail_ids)
        return {"Failed": failed} if failed else {}


def seeder(manifest: dict[str, Any], keys: list[str] | None = None, **kwargs) -> Seeder:
    return Seeder(s3=FakeS3(manifest, keys), sqs=FakeSqs(), queue_url="q", **kwargs)


class TestItemIdentifiersAreDerived:
    def test_the_same_input_always_yields_the_same_identifier(self) -> None:
        """What makes re-seeding safe. Result keys are built from this, so a generated
        identifier would make a second seeding of one manifest produce a second set of
        results rather than overwrite the first — and every item would succeed."""
        assert identifier_for("inputs/module.py") == identifier_for("other/module.py")

    def test_it_stays_readable(self) -> None:
        # It appears in an S3 prefix an operator reads. A hash tells them nothing about
        # which input failed.
        assert identifier_for("deep/path/billing_service.py") == "billing_service.py"

    def test_awkward_characters_are_reduced_rather_than_dropped(self) -> None:
        assert identifier_for("inputs/a b&c.py") == "a-b-c.py"

    def test_a_key_with_nothing_usable_is_refused(self) -> None:
        with pytest.raises(InvalidManifest, match="cannot derive"):
            identifier_for("inputs/...")


class TestReadingAManifest:
    def test_an_explicit_list_is_expanded(self) -> None:
        s = seeder({"items": [{"prompt": "do a", "inputs": ["x/one.py"]}, {"prompt": "do b"}]})

        items = s.items_from_manifest("bucket", "manifest.json")

        assert [i.item_id for i in items] == ["one.py", "item-00001"]

    def test_a_prefix_becomes_one_item_per_object(self) -> None:
        # The shape fan-out work actually has: many inputs, one unchanging instruction.
        # Writing that out by hand for two thousand files is how a manifest goes stale.
        s = seeder(
            {"inputPrefix": "inputs/", "prompt": "summarize"},
            keys=["inputs/a.py", "inputs/b.py", "inputs/"],
        )

        items = s.items_from_manifest("bucket", "manifest.json")

        assert [i.item_id for i in items] == ["a.py", "b.py"]
        assert all(i.prompt == "summarize" for i in items)

    def test_a_duplicate_identifier_is_refused(self) -> None:
        """Two items sharing one identifier overwrite each other's results. Keeping one
        silently is worse than refusing a manifest its author can fix."""
        s = seeder({"items": [{"itemId": "same", "prompt": "a"}, {"itemId": "same", "prompt": "b"}]})

        with pytest.raises(InvalidManifest, match="more than once"):
            s.items_from_manifest("bucket", "manifest.json")

    def test_two_inputs_reducing_to_one_identifier_are_refused(self) -> None:
        s = seeder({"inputPrefix": "in/", "prompt": "p"}, keys=["in/a/f.py", "in/b/f.py"])

        with pytest.raises(InvalidManifest, match="reduce to the identifier"):
            s.items_from_manifest("bucket", "manifest.json")

    def test_a_prefix_without_a_prompt_is_refused(self) -> None:
        s = seeder({"inputPrefix": "inputs/"}, keys=["inputs/a.py"])

        with pytest.raises(InvalidManifest, match="requires a 'prompt'"):
            s.items_from_manifest("bucket", "manifest.json")

    def test_a_manifest_describing_nothing_is_refused(self) -> None:
        s = seeder({"workflow": "something"})

        with pytest.raises(InvalidManifest, match="neither 'items' nor 'inputPrefix'"):
            s.items_from_manifest("bucket", "manifest.json")

    def test_a_prefix_matching_no_objects_is_refused(self) -> None:
        # Otherwise seeding reports success for a batch of nothing, and the fleet sits idle
        # while an operator waits for results.
        s = seeder({"inputPrefix": "nothing/", "prompt": "p"}, keys=[])

        with pytest.raises(InvalidManifest, match="no objects under"):
            s.items_from_manifest("bucket", "manifest.json")


class TestSending:
    def test_messages_go_in_batches_of_ten(self) -> None:
        s = seeder({"items": []})
        items = [WorkItem(f"i{n}", "p", ()) for n in range(25)]

        sent = s.send(items)

        assert sent == 25
        assert s.sqs.batches == 3

    def test_a_partial_failure_is_counted_honestly(self) -> None:
        """A batch that mostly worked is the case where a specific missing item disappears.
        The count has to reflect what was accepted, not what was attempted."""
        s = Seeder(s3=FakeS3({}), sqs=FakeSqs(fail_ids={"2"}), queue_url="q")

        sent = s.send([WorkItem(f"i{n}", "p", ()) for n in range(5)])

        assert sent == 4

    def test_the_body_carries_what_a_worker_reads(self) -> None:
        s = seeder({"items": []})

        s.send([WorkItem("one.py", "do it", ("inputs/one.py",))])

        body = json.loads(s.sqs.sent[0]["MessageBody"])
        assert body == {
            "itemId": "one.py",
            "prompt": "do it",
            "inputs": ["inputs/one.py"],
        }


class FakeSqsAttributes:
    def __init__(self, visible: int, in_flight: int) -> None:
        self._visible = visible
        self._in_flight = in_flight

    def get_queue_attributes(self, **_kwargs: Any) -> dict[str, Any]:
        return {
            "Attributes": {
                "ApproximateNumberOfMessages": str(self._visible),
                "ApproximateNumberOfMessagesNotVisible": str(self._in_flight),
            }
        }


class FakeEcs:
    def __init__(
        self, running: int | None, desired: int | None = None, pending: int = 0
    ) -> None:
        self._running = running
        # Defaults to the running count, which is what a settled service looks like.
        self._desired = running if desired is None else desired
        self._pending = pending
        self.updates: list[dict[str, Any]] = []

    def describe_services(self, **_kwargs: Any) -> dict[str, Any]:
        if self._running is None:
            # A service that does not exist yet. The signal function runs on a schedule and
            # can fire before the service is up.
            return {"services": []}
        return {
            "services": [
                {
                    "runningCount": self._running,
                    "desiredCount": self._desired,
                    "pendingCount": self._pending,
                }
            ]
        }

    def update_service(self, **kwargs: Any) -> None:
        self.updates.append(kwargs)
        self._desired = kwargs["desiredCount"]


class FakeRegistry:
    """Enabled subscriptions with a budget of 1000, and `used` credits each (default 0).

    `used` is a list so a test can change it between handler runs, the way a budget raise
    or a window reset changes the stored item between two scheduled minutes.
    """

    def __init__(
        self,
        concurrencies: list[int],
        used: list[str] | None = None,
        budgets: list[str] | None = None,
    ) -> None:
        self._concurrencies = concurrencies
        self.used = used if used is not None else ["0"] * len(concurrencies)
        self.budgets = budgets if budgets is not None else ["1000"] * len(concurrencies)

    def list_enabled(self) -> list[SubscriptionRecord]:
        return [
            SubscriptionRecord(
                subscription_id=f"sub-{n:02d}",
                secret_ref="arn:aws:secretsmanager:us-east-1:123456789012:secret:x-AbCdEf",
                max_concurrency=c,
                credit_budget=self.budgets[n],
                credits_used=self.used[n],
                state=SubscriptionState.ENABLED,
            )
            for n, c in enumerate(self._concurrencies)
        ]


class TestTheScalingSignal:
    def test_an_empty_fleet_still_produces_a_finite_number(self) -> None:
        """The property the whole thing rests on. Dividing by the running count would make
        the metric that is supposed to start the first task uncomputable until a task
        exists, so the fleet would never leave zero."""
        signal = FleetSignal(queue_depth=40, running_tasks=0, pool_concurrency=4)

        assert signal.backlog_per_task == 40

    def test_backlog_is_per_task_rather_than_depth(self) -> None:
        # Depth alone cannot say whether the fleet is keeping up: the same thousand items
        # behind two tasks and behind two hundred are different situations.
        assert FleetSignal(1000, 2, 8).backlog_per_task == 500
        assert FleetSignal(1000, 200, 8).backlog_per_task == 5

    def test_pending_tasks_share_the_backlog(self) -> None:
        """Tasks placed but not yet running are already on their way. Leaving them out would
        publish the whole backlog until they run, and target tracking could scale past them."""
        signal = FleetSignal(queue_depth=5, running_tasks=0, pool_concurrency=4, pending_tasks=3)

        assert signal.backlog_per_task == pytest.approx(5 / 3)
        assert FleetSignal(10, 2, 8, pending_tasks=3).backlog_per_task == 2

    def test_items_in_flight_count_as_backlog(self) -> None:
        """An item being worked on is not finished. Counting only visible messages reads a
        fleet mid-batch as idle, and scale-in then removes the tasks doing the work."""
        signal = read_signal(
            sqs=FakeSqsAttributes(visible=3, in_flight=7),
            ecs=FakeEcs(running=2),
            registry=FakeRegistry([2, 2]),
            queue_url="q",
            cluster="c",
            service="s",
            credit_margin=Decimal(1),
        )

        assert signal.queue_depth == 10
        assert signal.backlog_per_task == 5

    def test_with_budget_left_the_ceiling_is_the_pool_s_declared_concurrency(self) -> None:
        signal = read_signal(
            sqs=FakeSqsAttributes(1, 0),
            ecs=FakeEcs(running=1),
            registry=FakeRegistry([2, 3, 1]),
            queue_url="q",
            cluster="c",
            service="s",
            credit_margin=Decimal(1),
        )

        assert signal.pool_concurrency == 6
        assert signal.claimable_concurrency == 6
        assert signal.capacity_ceiling == 6

    def test_a_service_that_does_not_exist_yet_reads_as_zero(self) -> None:
        # The signal function runs on a schedule and may fire before the service is up.
        signal = read_signal(
            sqs=FakeSqsAttributes(5, 0),
            ecs=FakeEcs(running=None),
            registry=FakeRegistry([2]),
            queue_url="q",
            cluster="c",
            service="s",
            credit_margin=Decimal(1),
        )

        assert signal.running_tasks == 0
        assert signal.backlog_per_task == 5

    def test_an_empty_pool_yields_a_ceiling_of_zero(self) -> None:
        """Every task would fail to claim, so the honest ceiling is no tasks rather than a
        fleet that starts and churns."""
        assert FleetSignal(100, 0, 0).capacity_ceiling == 0


class RecordingAutoscaling:
    """Records writes and reads back the suspension the last write left, as the service does."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.describes = 0

    def register_scalable_target(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)

    def describe_scalable_targets(self, **_kwargs: Any) -> dict[str, Any]:
        self.describes += 1
        if not self.calls:
            return {"ScalableTargets": []}
        return {"ScalableTargets": [{"SuspendedState": self.calls[-1].get("SuspendedState", {})}]}


class FakeCloudWatch:
    """Target tracking's scale-in alarm, in whatever state a test sets."""

    def __init__(self, state: str | None = "ALARM") -> None:
        self.state = state
        self.prefixes: list[str] = []

    def describe_alarms(self, **kwargs: Any) -> dict[str, Any]:
        self.prefixes.append(kwargs["AlarmNamePrefix"])
        if self.state is None:
            return {"MetricAlarms": []}
        return {"MetricAlarms": [{"StateValue": self.state}]}


class TestHoldingScaleIn:
    """Measured: a start from zero was cut from 25 tasks to 2 within the minute by the
    scale-in alarm still in ALARM from the idle period."""

    def test_a_start_suspends_scale_in(self) -> None:
        from scaling.handler import hold_scale_in

        assert hold_scale_in(True, False, 50, lambda: pytest.fail("not read on a start"))

    def test_it_stays_suspended_while_the_stale_alarm_is_in_alarm(self) -> None:
        from scaling.handler import hold_scale_in

        assert hold_scale_in(False, True, 50, lambda: True)

    def test_it_is_lifted_once_the_alarm_has_cleared(self) -> None:
        from scaling.handler import hold_scale_in

        assert not hold_scale_in(False, True, 50, lambda: False)

    def test_an_empty_queue_lifts_it_without_reading_the_alarm(self) -> None:
        from scaling.handler import hold_scale_in

        assert not hold_scale_in(False, True, 0, lambda: pytest.fail("not read"))

    def test_nothing_is_suspended_that_was_not_started(self) -> None:
        """A later ALARM needs fifteen new low minutes: target tracking as designed."""
        from scaling.handler import hold_scale_in

        assert not hold_scale_in(False, False, 50, lambda: pytest.fail("not read"))

    def test_a_failed_alarm_read_keeps_it_suspended(self) -> None:
        from scaling.handler import hold_scale_in

        def broken() -> bool:
            raise RuntimeError("throttled")

        assert hold_scale_in(False, True, 50, broken)

    def test_a_missing_alarm_lifts_it(self) -> None:
        from scaling.handler import hold_scale_in

        assert not hold_scale_in(False, True, 50, lambda: None)

    def test_the_alarm_is_found_by_the_prefix_application_auto_scaling_uses(self) -> None:
        from scaling.handler import low_alarm_in_alarm

        cloudwatch = FakeCloudWatch("OK")

        assert low_alarm_in_alarm(cloudwatch, "service/c/s") is False
        assert cloudwatch.prefixes == ["TargetTracking-service/c/s-AlarmLow-"]
        assert low_alarm_in_alarm(FakeCloudWatch(None), "service/c/s") is None
        assert low_alarm_in_alarm(FakeCloudWatch("ALARM"), "service/c/s") is True


class TestNarrowingCapacity:
    def test_the_lower_of_the_two_bounds_wins(self) -> None:
        # The quota bound is static and in the stack; this is the pool bound, which changes
        # when an operator enables a subscription and so cannot be in a deployment.
        client = RecordingAutoscaling()

        assert narrow_capacity(client, "service/c/s", configured_maximum=10, ceiling=6) == 6
        assert narrow_capacity(client, "service/c/s", configured_maximum=4, ceiling=6) == 4

    def test_the_minimum_stays_at_zero(self) -> None:
        """Writing the maximum must not disturb the property the cost argument rests on."""
        client = RecordingAutoscaling()

        narrow_capacity(client, "service/c/s", configured_maximum=10, ceiling=6)

        assert client.calls[0]["MinCapacity"] == 0

    def test_it_names_the_service_scalable_dimension(self) -> None:
        client = RecordingAutoscaling()

        narrow_capacity(client, "service/c/s", configured_maximum=10, ceiling=6)

        assert client.calls[0]["ScalableDimension"] == "ecs:service:DesiredCount"
        assert client.calls[0]["ServiceNamespace"] == "ecs"

    def test_the_suspension_is_written_explicitly_on_every_call(self) -> None:
        """Never left to an omitted field, and only scale-in is ever suspended."""
        client = RecordingAutoscaling()

        narrow_capacity(client, "service/c/s", configured_maximum=10, ceiling=6)
        narrow_capacity(
            client, "service/c/s", configured_maximum=10, ceiling=6, scale_in_suspended=True
        )

        assert [c["SuspendedState"] for c in client.calls] == [
            {
                "DynamicScalingInSuspended": suspended,
                "DynamicScalingOutSuspended": False,
                "ScheduledScalingSuspended": False,
            }
            for suspended in (False, True)
        ]


class TestTheThreeLayersHaveData:
    """The post describes three observability layers. Two of them had nothing behind them.

    Layer one, the infrastructure signals, was always published. Layer two — "session data and
    prompt logs, exported from each task before it exits" — wrote a transcript to the task's own
    filesystem, which goes when the task does, so nothing was exported. Layer three — "a stream
    of structured workflow-progress events" — put the verdict in a log line and emitted no
    metric, so the distribution across a batch was unavailable.

    A dashboard was blocked on what looked like a scope decision and was actually missing data.
    """

    def test_a_run_outcome_is_a_metric_and_not_only_a_log_line(self) -> None:
        """The distribution carries the diagnosis. 2% NoOutput is a prompt to look at and 2%
        Throttled is a subscription to add; a success count cannot tell them apart."""
        import io
        import json

        from leaseclient.metrics import Metrics

        stream = io.StringIO()
        Metrics(namespace="Test", stream=stream).run_outcome("NoOutput", "item-000")

        record = json.loads(stream.getvalue())
        assert record["RunOutcome"] == 1
        assert record["Verdict"] == "NoOutput"
        assert record["ItemId"] == "item-000"

    def test_the_verdict_is_a_dimension_and_the_item_is_not(self) -> None:
        """Eight verdicts is a bounded dimension. One per item would be a custom metric per work
        item, which is a bill that grows with the batch and a dashboard nobody can read."""
        import io
        import json

        from leaseclient.metrics import Metrics

        stream = io.StringIO()
        Metrics(namespace="Test", stream=stream).run_outcome("Succeeded", "item-000")

        dimensions = json.loads(stream.getvalue())["_aws"]["CloudWatchMetrics"][0]["Dimensions"]
        assert dimensions == [["Verdict"]]

    def test_a_disabled_subscription_is_published_without_a_dimension_per_subscription(
        self,
    ) -> None:
        """An alarm should fire whichever subscription it was. Dimensioning by subscription
        would mean an alarm each, or one that misses the subscription nobody created."""
        import io
        import json

        from leaseclient.metrics import Metrics

        stream = io.StringIO()
        Metrics(namespace="Test", stream=stream).subscription_disabled("sub-01", "CredentialRejected")

        record = json.loads(stream.getvalue())
        assert record["SubscriptionDisabled"] == 1
        assert record["SubscriptionId"] == "sub-01"
        assert record["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [[]]

    def test_the_scratch_directory_is_named_once(self) -> None:
        """The guard ignores it and the worker reads the transcript out of it. As two literals
        they could drift, and the symptom would be the guard failing every run for a directory
        the fleet itself created."""
        from runner.guard import IGNORED_TOP_LEVEL, RUN_DIR

        assert RUN_DIR in IGNORED_TOP_LEVEL


class TestTheTwoResultStoresAgree:
    """A local run is evidence about a task only if the pieces underneath behave the same.

    The S3 store puts one object per file and relies on deterministic keys, so two calls for one
    item accumulate. The local store used to clear the item's directory on every call, so the
    second call destroyed the first — the transcript was written and then deleted by the result
    that followed it. What made it hard to see is that the symptom was an absence: it looked like
    the transcript had never been produced, while it sat on disk the whole time.

    This is the third divergence between the local and deployed paths found in this repository,
    so the seam gets an assertion rather than another comment.
    """

    def test_two_stores_for_one_item_accumulate(self, tmp_path) -> None:
        from runner.local import LocalResultStore

        root = tmp_path / "run"
        (root / "output").mkdir(parents=True)
        (root / "output" / "result.md").write_text("the result")
        (root / "transcript").mkdir()
        (root / "transcript" / "stdout.txt").write_text("what it did")

        store = LocalResultStore(root=tmp_path / "results")
        store.store("item-000", (root / "transcript" / "stdout.txt",), root)
        store.store("item-000", (root / "output" / "result.md",), root / "output")

        landed = sorted(p.name for p in store.results_for("item-000"))
        assert landed == ["result.md", "stdout.txt"]

    def test_storing_the_same_path_twice_overwrites_rather_than_duplicates(
        self, tmp_path
    ) -> None:
        """What makes a redelivered work item safe: the same key, written again."""
        from runner.local import LocalResultStore

        root = tmp_path / "run"
        root.mkdir()
        (root / "result.md").write_text("first")

        store = LocalResultStore(root=tmp_path / "results")
        store.store("item-000", (root / "result.md",), root)
        (root / "result.md").write_text("second")
        store.store("item-000", (root / "result.md",), root)

        results = store.results_for("item-000")
        assert len(results) == 1
        assert results[0].read_text() == "second"


class TestDrainingToZero:
    """Why target tracking is not enough to end a batch, and what the extra call must not do.

    Target tracking's scale-in alarm is created with fifteen evaluation periods — measured from
    a deployed policy, whose alarm reported "15 datapoints were less than the threshold". So a
    drained queue reaches zero in about fifteen minutes, and the fleet does not idle quietly
    while it waits: a worker that finds an empty queue exits, the service replaces it, and that
    repeats. Observed as desired staying at two for sixteen minutes after the last result was
    stored, with tasks cycling the whole time.
    """

    def test_an_empty_queue_sets_the_service_to_zero(self) -> None:
        from scaling.handler import drain_to_zero

        class Recording:
            def __init__(self) -> None:
                self.calls: list[dict] = []

            def update_service(self, **kwargs) -> None:
                self.calls.append(kwargs)

        client = Recording()
        assert drain_to_zero(client, "cluster", "service", desired=2) is True
        assert client.calls == [
            {"cluster": "cluster", "service": "service", "desiredCount": 0}
        ]

    def test_a_service_already_at_zero_is_left_alone(self) -> None:
        """Called every minute, so a no-op has to be a no-op rather than a write."""
        from scaling.handler import drain_to_zero

        class Recording:
            def __init__(self) -> None:
                self.calls: list[dict] = []

            def update_service(self, **kwargs) -> None:
                self.calls.append(kwargs)

        client = Recording()
        assert drain_to_zero(client, "cluster", "service", desired=0) is False
        assert client.calls == []

    def test_items_in_flight_count_as_depth_so_a_batch_is_not_cut_short(self) -> None:
        """The safety property. An item in flight is being worked on by a task that would be
        stopped, which pays for the work without keeping it — so the drain condition has to see
        in-flight messages as depth, not as an empty queue.
        """
        from scaling.handler import read_signal

        signal = read_signal(
            sqs=FakeSqsAttributes(visible=0, in_flight=2),
            ecs=FakeEcs(running=2),
            registry=FakeRegistry([2]),
            queue_url="q",
            cluster="c",
            service="s",
            credit_margin=Decimal(1),
        )

        assert signal.queue_depth == 2, "in-flight work must not read as a drained queue"

    def test_the_desired_count_is_read_rather_than_inferred_from_running(self) -> None:
        """The running count cannot answer whether the service is already at zero: it cycles
        between zero and the desired count while workers are replaced."""
        from scaling.handler import read_signal

        signal = read_signal(
            sqs=FakeSqsAttributes(visible=0, in_flight=0),
            ecs=FakeEcs(running=0, desired=2),
            registry=FakeRegistry([2]),
            queue_url="q",
            cluster="c",
            service="s",
            credit_margin=Decimal(1),
        )

        assert signal.running_tasks == 0
        assert signal.desired_tasks == 2


def idle(visible: int, in_flight: int = 0) -> FleetSignal:
    return FleetSignal(
        queue_depth=visible + in_flight,
        running_tasks=0,
        pool_concurrency=2,
        desired_tasks=0,
        pending_tasks=0,
        visible_messages=visible,
    )


class TestStartingFromZero:
    """Why target tracking cannot start a small batch, and what the direct start must not do.

    At zero tasks the metric is the whole backlog, and target tracking scales out only when the
    metric is above its target. Measured on a deployed fleet with the target at 2: two items
    queued, `queueDepth 2, runningTasks 0` every minute for twelve minutes, and the service left
    at zero. Three items did start, about four minutes after seeding.
    """

    @pytest.mark.parametrize("visible", [1, 2])
    def test_a_batch_at_or_below_the_target_starts_a_task(self, visible: int) -> None:
        from scaling.handler import tasks_to_start

        assert tasks_to_start(idle(visible), target_backlog_per_task=2, ceiling=2) == 1

    def test_the_count_is_what_target_tracking_would_settle_on(self) -> None:
        from scaling.handler import tasks_to_start

        assert tasks_to_start(idle(5), target_backlog_per_task=2, ceiling=10) == 3
        assert tasks_to_start(idle(4), target_backlog_per_task=2, ceiling=10) == 2

    def test_the_ceiling_is_respected(self) -> None:
        """The same maximum the scalable target is held at: the quota bound and the pool's
        declared concurrency. A task above it would fail to claim and churn."""
        from scaling.handler import tasks_to_start

        assert tasks_to_start(idle(40), target_backlog_per_task=2, ceiling=2) == 2

    def test_a_pool_declaring_no_concurrency_starts_nothing(self) -> None:
        from scaling.handler import tasks_to_start

        assert tasks_to_start(idle(3), target_backlog_per_task=2, ceiling=0) == 0

    @pytest.mark.parametrize(
        ("running", "desired", "pending"),
        [(1, 1, 0), (0, 2, 0), (0, 1, 1), (0, 0, 1), (1, 0, 0)],
    )
    def test_a_fleet_already_up_or_on_its_way_is_left_to_target_tracking(
        self, running: int, desired: int, pending: int
    ) -> None:
        """Anything desired, running or pending means something is already acting, and a
        second writer would race it."""
        from scaling.handler import tasks_to_start

        signal = FleetSignal(
            queue_depth=3,
            running_tasks=running,
            pool_concurrency=2,
            desired_tasks=desired,
            pending_tasks=pending,
            visible_messages=3,
        )

        assert tasks_to_start(signal, target_backlog_per_task=2, ceiling=2) == 0

    def test_in_flight_messages_alone_start_nothing(self) -> None:
        """With no task up, an in-flight message belongs to a task that has gone. It comes
        back when its visibility timeout ends, and is counted then."""
        from scaling.handler import tasks_to_start

        assert tasks_to_start(idle(0, in_flight=2), target_backlog_per_task=2, ceiling=2) == 0

    def test_the_pending_count_is_read(self) -> None:
        signal = read_signal(
            sqs=FakeSqsAttributes(visible=2, in_flight=1),
            ecs=FakeEcs(running=0, desired=1, pending=1),
            registry=FakeRegistry([2]),
            queue_url="q",
            cluster="c",
            service="s",
            credit_margin=Decimal(1),
        )

        assert signal.pending_tasks == 1
        assert signal.visible_messages == 2
        assert signal.queue_depth == 3

    def test_a_start_of_nothing_makes_no_call(self) -> None:
        from scaling.handler import start_from_zero

        ecs = FakeEcs(running=0)

        assert start_from_zero(ecs, "c", "s", 0) is False
        assert ecs.updates == []


@pytest.fixture
def fleet(monkeypatch: pytest.MonkeyPatch):
    """The scaling handler wired to fakes: an idle service, a queue, and a pool."""
    import scaling.handler as scaling_handler

    def build(
        visible: int,
        concurrencies: list[int],
        max_tasks: int = 10,
        registry: FakeRegistry | None = None,
    ):
        ecs = FakeEcs(running=0, desired=0)
        clients = {
            "sqs": FakeSqsAttributes(visible=visible, in_flight=0),
            "ecs": ecs,
            "dynamodb": object(),
            "application-autoscaling": RecordingAutoscaling(),
            # Idle long enough for the scale-in alarm to be in ALARM, as measured.
            "cloudwatch": FakeCloudWatch("ALARM"),
        }
        pool = registry if registry is not None else FakeRegistry(concurrencies)
        monkeypatch.setattr(scaling_handler.boto3, "client", lambda name: clients[name])
        monkeypatch.setattr(scaling_handler, "Registry", lambda _client: pool)
        for name, value in {
            "WORK_QUEUE_URL": "q",
            "CLUSTER_NAME": "cluster",
            "SERVICE_NAME": "service",
            "MAX_TASKS": str(max_tasks),
            "TARGET_BACKLOG_PER_TASK": "2",
        }.items():
            monkeypatch.setenv(name, value)
        # The workers' default applies unless a test sets the margin.
        monkeypatch.delenv("CREDIT_MARGIN", raising=False)
        scaling_handler._test_clients = clients
        return scaling_handler, ecs

    return build


class TestTheStartIsNotScaledInByTheIdleAlarm:
    """Minute by minute, the sequence measured on a deployed fleet, with the fix."""

    def test_scale_in_is_suspended_before_the_start_and_lifted_after_the_alarm_clears(
        self, fleet
    ) -> None:
        scaling_handler, ecs = fleet(visible=50, concurrencies=[25], max_tasks=25)
        autoscaling = scaling_handler._test_clients["application-autoscaling"]
        cloudwatch = scaling_handler._test_clients["cloudwatch"]

        def suspended() -> bool:
            return autoscaling.calls[-1]["SuspendedState"]["DynamicScalingInSuspended"]

        # Minute 1: the start. The suspension is written before the desired count moves.
        first = scaling_handler.handler({})
        assert first["startedFromZero"] == 25
        assert first["scaleInSuspended"] is True
        assert suspended()

        # Minute 2: the idle alarm is still in ALARM. Scale-in stays suspended.
        ecs._running = 25
        assert scaling_handler.handler({})["scaleInSuspended"] is True

        # Minute 3: a datapoint at the target has cleared it. Target tracking is back.
        cloudwatch.state = "OK"
        assert scaling_handler.handler({})["scaleInSuspended"] is False
        assert not suspended()

        # Minute 4: not suspended, so the alarm is not read again.
        reads = len(cloudwatch.prefixes)
        cloudwatch.state = "ALARM"
        assert scaling_handler.handler({})["scaleInSuspended"] is False
        assert len(cloudwatch.prefixes) == reads

    def test_a_failing_alarm_read_keeps_the_hold_and_is_counted(
        self, fleet, capsys: pytest.CaptureFixture[str]
    ) -> None:
        scaling_handler, ecs = fleet(visible=50, concurrencies=[25], max_tasks=25)
        cloudwatch = scaling_handler._test_clients["cloudwatch"]
        scaling_handler.handler({})
        ecs._running = 25

        def denied(**_kwargs: Any) -> dict[str, Any]:
            raise RuntimeError("AccessDenied")

        cloudwatch.describe_alarms = denied

        capsys.readouterr()  # drop the start minute's records
        assert scaling_handler.handler({})["scaleInSuspended"] is True
        records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        assert [r["ScaleInAlarmReadFailed"] for r in records if "ScaleInAlarmReadFailed" in r] == [1]
        assert [r["ScaleInSuspended"] for r in records if "ScaleInSuspended" in r] == [1]

    def test_a_drained_queue_lifts_the_suspension(self, fleet) -> None:
        scaling_handler, _ = fleet(visible=4, concurrencies=[5])
        scaling_handler.handler({})
        scaling_handler._test_clients["sqs"]._visible = 0

        result = scaling_handler.handler({})

        assert result["drainedToZero"] is True
        assert result["scaleInSuspended"] is False


class TestTheHandlerStartsAnIdleFleetOnce:
    """The handler end to end against fakes, run on consecutive minutes."""

    def test_two_items_at_zero_start_one_task_and_the_next_minute_does_nothing(
        self, fleet, capsys: pytest.CaptureFixture[str]
    ) -> None:
        scaling_handler, ecs = fleet(visible=2, concurrencies=[2])

        first = scaling_handler.handler({})
        # The task is placed but not running yet: desired is now 1, so nothing is repeated.
        second = scaling_handler.handler({})

        assert ecs.updates == [
            {"cluster": "cluster", "service": "service", "desiredCount": 1}
        ]
        assert first["startedFromZero"] == 1
        assert second["startedFromZero"] == 0

        records = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if "StartedFromZero" in line
        ]
        assert [r["StartedFromZero"] for r in records] == [1]

    def test_while_the_started_tasks_are_pending_the_signal_is_at_the_target(
        self, fleet, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """The review case: five items, target 2, three tasks started. Divided by running tasks
        alone, the minutes they spend pending would each publish 5, and target tracking could
        take the count to ceil(3 * 5 / 2) = 8, clamped to the ceiling."""
        scaling_handler, ecs = fleet(visible=5, concurrencies=[10])

        scaling_handler.handler({})
        ecs._pending = ecs._desired  # placed, not running yet
        scaling_handler.handler({})

        published = [r["BacklogPerTask"] for r in emitted(capsys, "BacklogPerTask")]
        assert ecs.updates == [
            {"cluster": "cluster", "service": "service", "desiredCount": 3}
        ]
        assert published[0] == 5
        assert published[1] == pytest.approx(5 / 3)
        assert published[1] <= 2

    def test_the_effective_ceiling_is_the_lower_of_quota_and_pool(self, fleet) -> None:
        scaling_handler, ecs = fleet(visible=40, concurrencies=[2, 1], max_tasks=10)

        result = scaling_handler.handler({})

        assert result["maxCapacity"] == 3
        assert ecs.updates[0]["desiredCount"] == 3

    def test_an_empty_pool_starts_nothing(self, fleet) -> None:
        scaling_handler, ecs = fleet(visible=3, concurrencies=[])

        result = scaling_handler.handler({})

        assert result["startedFromZero"] == 0
        assert ecs.updates == []

    def test_a_non_positive_target_is_refused(self, fleet, monkeypatch) -> None:
        scaling_handler, _ = fleet(visible=1, concurrencies=[2])
        monkeypatch.setenv("TARGET_BACKLOG_PER_TASK", "0")

        with pytest.raises(ValueError, match="above zero"):
            scaling_handler.handler({})


def emitted(capsys: pytest.CaptureFixture[str], metric: str) -> list[dict[str, Any]]:
    return [
        record
        for record in (json.loads(line) for line in capsys.readouterr().out.splitlines())
        if metric in record
    ]


class TestAnExhaustedPoolStartsNothing:
    """A subscription over its credit ceiling cannot be claimed, so it adds no capacity.

    Measured before the scaler counted credits: the only subscription over its ceiling, two
    items queued, and a task started about every minute. Each tried to claim for about ten
    seconds (`ClaimOutcome BudgetExhausted` five times), logged "no subscription available",
    and exited, until the queue was purged.
    """

    def test_work_waiting_on_an_exhausted_pool_starts_nothing_and_caps_at_zero(
        self, fleet, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
    ) -> None:
        # 999 is the ceiling at the default margin of 1: a claim needs used < 999.
        scaling_handler, ecs = fleet(
            visible=2, concurrencies=[2], registry=FakeRegistry([2], used=["999"])
        )

        result = scaling_handler.handler({})

        assert ecs.updates == []
        assert result["startedFromZero"] == 0
        assert result["maxCapacity"] == 0
        assert result["poolDeclaredConcurrency"] == 2
        assert result["poolClaimableConcurrency"] == 0
        assert result["poolExhausted"] is True
        assert [r["PoolExhausted"] for r in emitted(capsys, "PoolExhausted")] == [1]
        assert "credit ceiling" in caplog.text

    def test_one_exhausted_subscription_leaves_only_the_other_in_the_ceiling(
        self, fleet, capsys: pytest.CaptureFixture[str]
    ) -> None:
        scaling_handler, ecs = fleet(
            visible=40,
            concurrencies=[2, 3],
            registry=FakeRegistry([2, 3], used=["1000", "0"]),
        )

        result = scaling_handler.handler({})

        assert result["poolDeclaredConcurrency"] == 5
        assert result["poolClaimableConcurrency"] == 3
        assert result["maxCapacity"] == 3
        assert ecs.updates[0]["desiredCount"] == 3
        assert result["poolExhausted"] is False
        assert emitted(capsys, "PoolExhausted") == []

    def test_raising_the_budget_starts_the_fleet_on_the_next_run(self, fleet) -> None:
        pool = FakeRegistry([2], used=["999"])
        scaling_handler, ecs = fleet(visible=2, concurrencies=[2], registry=pool)

        assert scaling_handler.handler({})["startedFromZero"] == 0

        pool.budgets[0] = "2000"
        result = scaling_handler.handler({})

        assert result["maxCapacity"] == 2
        assert result["poolExhausted"] is False
        assert ecs.updates == [
            {"cluster": "cluster", "service": "service", "desiredCount": 1}
        ]

    def test_a_window_reset_starts_the_fleet_on_the_next_run(self, fleet) -> None:
        pool = FakeRegistry([2], used=["1000"])
        scaling_handler, ecs = fleet(visible=2, concurrencies=[2], registry=pool)

        assert scaling_handler.handler({})["startedFromZero"] == 0

        pool.used[0] = "0"  # what the reconciler writes when windowResetsAt passes

        assert scaling_handler.handler({})["startedFromZero"] == 1
        assert len(ecs.updates) == 1

    def test_an_empty_pool_is_not_reported_as_exhausted(
        self, fleet, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Nothing is enabled, so there is no budget to raise: a different fix for the operator.
        scaling_handler, _ = fleet(visible=2, concurrencies=[])

        assert scaling_handler.handler({})["poolExhausted"] is False
        assert emitted(capsys, "PoolExhausted") == []

    def test_an_exhausted_pool_with_nothing_waiting_is_not_reported(
        self, fleet, capsys: pytest.CaptureFixture[str]
    ) -> None:
        scaling_handler, _ = fleet(
            visible=0, concurrencies=[2], registry=FakeRegistry([2], used=["1000"])
        )

        assert scaling_handler.handler({})["poolExhausted"] is False
        assert emitted(capsys, "PoolExhausted") == []


class TestTheMarginIsTheWorkers:
    """The scaler and the claim must draw the line at the same place.

    If the scaler's margin were smaller than the workers', it would count a subscription the
    claim refuses, and the churn above comes back for that band of credits.
    """

    def test_the_handler_reads_credit_margin_as_the_worker_does(
        self, fleet, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pool = FakeRegistry([2], used=["999.5"])
        scaling_handler, _ = fleet(visible=2, concurrencies=[2], registry=pool)

        # Default margin 1: the ceiling is 999, and 999.5 is over it.
        assert scaling_handler.handler({})["poolClaimableConcurrency"] == 0

        monkeypatch.setenv("CREDIT_MARGIN", "0.25")
        # Ceiling 999.75: claimable.
        assert scaling_handler.handler({})["poolClaimableConcurrency"] == 2

    def test_unset_it_is_the_worker_default(self, fleet) -> None:
        from leaseclient.claim import DEFAULT_CREDIT_MARGIN

        ceiling = Decimal(1000) - DEFAULT_CREDIT_MARGIN
        below = FakeRegistry([2], used=[str(ceiling - Decimal("0.01"))])
        at = FakeRegistry([2], used=[str(ceiling)])

        scaling_handler, _ = fleet(visible=2, concurrencies=[2], registry=below)
        assert scaling_handler.handler({})["poolClaimableConcurrency"] == 2
        scaling_handler, _ = fleet(visible=2, concurrencies=[2], registry=at)
        assert scaling_handler.handler({})["poolClaimableConcurrency"] == 0

    def test_a_margin_the_worker_refuses_is_refused_here_too(
        self, fleet, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        scaling_handler, _ = fleet(visible=2, concurrencies=[2])
        monkeypatch.setenv("CREDIT_MARGIN", "0.001")

        with pytest.raises(ValueError, match="CREDIT_MARGIN"):
            scaling_handler.handler({})

    def test_claimable_is_exactly_where_a_reservation_is_granted(self, pool) -> None:
        """Against the real registry and its conditional write, at the boundary."""
        from leaseclient.registry import Registry

        margin = Decimal(1)
        for subscription_id, concurrency, budget, used, state in [
            ("sub-under", 2, 1000, "998.99", SubscriptionState.ENABLED),
            ("sub-at", 3, 1000, "999", SubscriptionState.ENABLED),
            ("sub-small", 4, 10, "9.5", SubscriptionState.ENABLED),
            ("sub-off", 5, 1000, "0", SubscriptionState.DISABLED),
        ]:
            pool.seed_subscription(
                subscription_id,
                max_concurrency=concurrency,
                credit_budget=budget,
                credits_used=used,
                state=state,
            )
        registry = Registry(pool.dynamodb)

        signal = read_signal(
            sqs=FakeSqsAttributes(visible=2, in_flight=0),
            ecs=FakeEcs(running=0),
            registry=registry,
            queue_url="q",
            cluster="c",
            service="s",
            credit_margin=margin,
        )
        granted = {
            record.subscription_id: registry.try_reserve_record(record, margin).granted
            for record in registry.list_enabled()
        }

        assert granted == {"sub-under": True, "sub-at": False, "sub-small": False}
        assert signal.pool_concurrency == 9
        assert signal.claimable_concurrency == 2

