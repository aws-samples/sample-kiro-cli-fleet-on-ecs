"""Recovery, credit corrections and window resets, and the reconciler's two triggers."""

from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from leaseclient import schema
from leaseclient.claim import Allocator, ClaimConfig, Lease, Leases
from leaseclient.credits import apply_external_correction, reset_window_if_due
from leaseclient.metrics import Metrics
from leaseclient.registry import InvalidSubscription, Registry
from reconciler.handler import EXPIRY_GRACE, TTL_PRINCIPAL, Reconciler, handler


@pytest.fixture
def sink() -> io.StringIO:
    return io.StringIO()


@pytest.fixture
def registry(pool, dynamodb) -> Registry:
    return Registry(dynamodb, pool.subscriptions_table)


@pytest.fixture
def leases(pool, dynamodb) -> Leases:
    return Leases(dynamodb, pool.leases_table)


@pytest.fixture
def allocator(pool, dynamodb, secretsmanager, sink, registry, leases) -> Allocator:
    return Allocator(
        registry=registry,
        leases=leases,
        secrets=secretsmanager,
        metrics=Metrics(stream=sink),
        config=ClaimConfig(base_backoff=timedelta(0), max_backoff=timedelta(0)),
        sleep=lambda _: None,
    )


@pytest.fixture
def reconciler(registry, leases, sink) -> Reconciler:
    return Reconciler(registry=registry, leases=leases, metrics=Metrics(stream=sink))


def ttl_removal(subscription_id: str, lease_id: str = "lease-1") -> dict:
    """A stream record shaped like a time-to-live deletion."""
    return {
        "eventName": "REMOVE",
        "userIdentity": {"type": "Service", "principalId": TTL_PRINCIPAL},
        "dynamodb": {
            "OldImage": {
                schema.LEASE_ID: {"S": lease_id},
                schema.SUBSCRIPTION_ID: {"S": subscription_id},
            }
        },
    }


def application_removal(subscription_id: str) -> dict:
    """The same event, but from an explicit delete rather than expiry."""
    record = ttl_removal(subscription_id)
    del record["userIdentity"]
    return record


class TestExpiryPath:
    def test_an_expired_lease_returns_its_capacity(
        self, pool, reconciler
    ) -> None:
        seeded = pool.seed_subscription(max_concurrency=5, active_leases=3)

        result = reconciler.handle_stream([ttl_removal(seeded.subscription_id)])

        assert result.expired_leases == 1
        assert pool.active_leases(seeded.subscription_id) == 2

    def test_an_application_delete_is_not_credited_again(
        self, pool, reconciler
    ) -> None:
        """A normal release already returned the capacity; a second decrement would
        invent capacity that does not exist."""
        seeded = pool.seed_subscription(max_concurrency=5, active_leases=3)

        result = reconciler.handle_stream([application_removal(seeded.subscription_id)])

        assert result.expired_leases == 0
        assert result.skipped == 1
        assert pool.active_leases(seeded.subscription_id) == 3

    def test_each_expired_lease_is_credited_once(self, pool, reconciler) -> None:
        seeded = pool.seed_subscription(max_concurrency=5, active_leases=3)

        reconciler.handle_stream(
            [
                ttl_removal(seeded.subscription_id, "lease-1"),
                ttl_removal(seeded.subscription_id, "lease-2"),
            ]
        )

        assert pool.active_leases(seeded.subscription_id) == 1

    def test_a_counter_already_at_zero_is_left_alone(self, pool, reconciler) -> None:
        seeded = pool.seed_subscription(active_leases=0)

        result = reconciler.handle_stream([ttl_removal(seeded.subscription_id)])

        assert result.skipped == 1
        assert pool.active_leases(seeded.subscription_id) == 0

    def test_non_removal_events_are_ignored(self, pool, reconciler) -> None:
        seeded = pool.seed_subscription(active_leases=2)
        insert = ttl_removal(seeded.subscription_id)
        insert["eventName"] = "INSERT"

        reconciler.handle_stream([insert])

        assert pool.active_leases(seeded.subscription_id) == 2

    def test_a_removal_without_a_subscription_is_reported_not_crashed(
        self, reconciler
    ) -> None:
        record = ttl_removal("sub-01")
        del record["dynamodb"]["OldImage"][schema.SUBSCRIPTION_ID]

        result = reconciler.handle_stream([record])

        assert result.skipped == 1

    def test_recovery_is_measured(self, pool, reconciler, sink) -> None:
        seeded = pool.seed_subscription(active_leases=1)

        reconciler.handle_stream([ttl_removal(seeded.subscription_id)])

        assert "LeasesRecovered" in sink.getvalue()
        assert "Expiry" in sink.getvalue()


class TestDriftCorrection:
    def test_a_counter_higher_than_the_leases_is_lowered(
        self, pool, reconciler
    ) -> None:
        """What a crashed worker leaves behind: capacity held by nothing."""
        seeded = pool.seed_subscription(max_concurrency=5, active_leases=3)

        result = reconciler.reconcile()

        assert result.corrected_subscriptions == 1
        assert pool.active_leases(seeded.subscription_id) == 0

    def test_a_counter_lower_than_the_leases_is_raised(
        self, pool, allocator, dynamodb, reconciler
    ) -> None:
        """The other direction, which would otherwise let the ceiling be exceeded."""
        seeded = pool.seed_subscription(max_concurrency=5)
        allocator.claim("task-1")
        allocator.claim("task-2")
        # Simulate a lost decrement having gone the wrong way.
        dynamodb.update_item(
            TableName=pool.subscriptions_table,
            Key={schema.SUBSCRIPTION_ID: {"S": seeded.subscription_id}},
            UpdateExpression=f"SET {schema.ACTIVE_LEASES} = :n",
            ExpressionAttributeValues={":n": {"N": "0"}},
        )

        reconciler.reconcile()

        assert pool.active_leases(seeded.subscription_id) == 2

    def test_an_agreeing_counter_is_left_alone(
        self, pool, allocator, reconciler
    ) -> None:
        seeded = pool.seed_subscription(max_concurrency=5)
        allocator.claim("task-1")

        result = reconciler.reconcile()

        assert result.corrected_subscriptions == 0
        assert pool.active_leases(seeded.subscription_id) == 1

    def test_capacity_is_never_taken_from_a_live_lease(
        self, pool, allocator, reconciler
    ) -> None:
        """Correction must not revoke capacity from a worker mid-run."""
        seeded = pool.seed_subscription(max_concurrency=5)
        allocator.claim("task-1")
        allocator.claim("task-2")

        reconciler.reconcile()

        assert pool.active_leases(seeded.subscription_id) >= 2
        assert pool.lease_count(seeded.subscription_id) == 2

    def test_correction_defers_when_the_counter_moved(
        self, pool, registry, reconciler
    ) -> None:
        """A claim landing between the read and the write makes the read stale, so the
        fix belongs to the next pass rather than to a lock."""
        seeded = pool.seed_subscription(max_concurrency=5, active_leases=3)

        assert registry.correct_active_leases(seeded.subscription_id, 2, 0) is False
        assert pool.active_leases(seeded.subscription_id) == 3

    def test_overlapping_passes_do_not_double_correct(
        self, pool, reconciler
    ) -> None:
        seeded = pool.seed_subscription(max_concurrency=5, active_leases=3)

        first = reconciler.reconcile()
        second = reconciler.reconcile()

        assert first.corrected_subscriptions == 1
        assert second.corrected_subscriptions == 0
        assert pool.active_leases(seeded.subscription_id) == 0

    def test_leases_are_never_deleted_by_correction(
        self, pool, allocator, reconciler
    ) -> None:
        pool.seed_subscription(max_concurrency=5)
        allocator.claim("task-1")

        reconciler.reconcile()

        assert len(pool.all_leases()) == 1


class TestExpiredButNotYetDeleted:
    """#15: DynamoDB deletes expired items "typically within a few days", so a dead task's
    lease can outlive its expiry by days. The scheduled pass must not wait for that."""

    def _dead_lease(self, leases, subscription_id, minutes_ago=10, lease_id="lease-dead"):
        expired_at = datetime.now(UTC) - timedelta(minutes=minutes_ago)
        lease = Lease(lease_id, subscription_id, "ref", expired_at)
        leases.record(lease, holder_id="task-gone")
        return lease

    def test_an_expired_lease_is_deleted_and_its_slot_returned(
        self, pool, leases, allocator, reconciler, sink
    ) -> None:
        seeded = pool.seed_subscription(max_concurrency=1, active_leases=1)
        self._dead_lease(leases, seeded.subscription_id)
        assert allocator.claim("task-new") is None, "the dead lease blocks the only slot"

        result = reconciler.reconcile()

        assert result.expired_leases == 1
        assert pool.lease_count(seeded.subscription_id) == 0
        assert pool.active_leases(seeded.subscription_id) == 0
        assert result.corrected_subscriptions == 0, "expiry, not drift"
        assert '"Mechanism": "Expiry"' in sink.getvalue()
        assert allocator.claim("task-new") is not None

    def test_a_live_lease_is_left_alone(self, pool, allocator, reconciler) -> None:
        seeded = pool.seed_subscription(max_concurrency=2)
        allocator.claim("task-1")

        result = reconciler.reconcile()

        assert result.expired_leases == 0
        assert pool.lease_count(seeded.subscription_id) == 1
        assert pool.active_leases(seeded.subscription_id) == 1

    def test_a_lease_just_past_expiry_waits_out_the_grace(
        self, pool, leases, reconciler
    ) -> None:
        """Clock skew between a task and the function must not cost a live holder."""
        seeded = pool.seed_subscription(max_concurrency=1, active_leases=1)
        edge = Lease(
            "lease-edge", seeded.subscription_id, "ref", datetime.now(UTC) - EXPIRY_GRACE / 2
        )
        leases.record(edge, holder_id="task-slow")

        result = reconciler.reconcile()

        assert result.expired_leases == 0
        assert pool.lease_count(seeded.subscription_id) == 1
        assert pool.active_leases(seeded.subscription_id) == 1

    def test_a_lease_renewed_between_read_and_delete_counts_as_live(
        self, pool, leases, dynamodb, reconciler
    ) -> None:
        """The delete is conditional; when the condition fails the lease is counted."""
        seeded = pool.seed_subscription(max_concurrency=1, active_leases=1)
        self._dead_lease(leases, seeded.subscription_id)

        class RenewsFirst:
            exceptions = dynamodb.exceptions

            def __getattr__(self, name):
                return getattr(dynamodb, name)

            def delete_item(self, **kwargs):
                # The holder's heartbeat lands just before the delete.
                renewed = int((datetime.now(UTC) + timedelta(minutes=2)).timestamp())
                dynamodb.update_item(
                    TableName=kwargs["TableName"],
                    Key=kwargs["Key"],
                    UpdateExpression=f"SET {schema.EXPIRES_AT} = :e",
                    ExpressionAttributeValues={":e": {"N": str(renewed)}},
                )
                return dynamodb.delete_item(**kwargs)

        leases._ddb = RenewsFirst()

        result = reconciler.reconcile()

        assert result.expired_leases == 0
        assert pool.lease_count(seeded.subscription_id) == 1
        assert pool.active_leases(seeded.subscription_id) == 1

    def test_a_lease_released_mid_pass_is_not_decremented_twice(
        self, pool, allocator, dynamodb, leases, reconciler
    ) -> None:
        """A release deletes the lease, then decrements. If the pass reads in between and
        stops counting the lease, both would lower the counter."""
        seeded = pool.seed_subscription(max_concurrency=2)
        lease = allocator.claim("task-1")

        class ReleasedBeforeRead:
            exceptions = dynamodb.exceptions

            def __getattr__(self, name):
                return getattr(dynamodb, name)

            def get_item(self, **kwargs):
                dynamodb.delete_item(TableName=kwargs["TableName"], Key=kwargs["Key"])
                return dynamodb.get_item(**kwargs)

        leases._ddb = ReleasedBeforeRead()
        reconciler.reconcile()
        assert pool.active_leases(seeded.subscription_id) == 1, "pass left the counter alone"

        # The release finishes its decrement.
        Registry(dynamodb, pool.subscriptions_table).release_reservation(lease.subscription_id)
        assert pool.active_leases(seeded.subscription_id) == 0

    def test_one_subscriptions_error_does_not_stop_the_pass(
        self, pool, leases, dynamodb, reconciler
    ) -> None:
        """A throttle on one subscription's leases skips that subscription only."""
        from botocore.exceptions import ClientError

        failing = pool.seed_subscription(max_concurrency=1, active_leases=1)
        healthy = pool.seed_subscription(max_concurrency=1, active_leases=1)
        self._dead_lease(leases, failing.subscription_id, lease_id="lease-a")
        self._dead_lease(leases, healthy.subscription_id, lease_id="lease-b")

        class ThrottledOnOne:
            exceptions = dynamodb.exceptions

            def __getattr__(self, name):
                return getattr(dynamodb, name)

            def get_item(self, **kwargs):
                if kwargs["Key"][schema.LEASE_ID]["S"] == "lease-a":
                    raise ClientError(
                        {"Error": {"Code": "ProvisionedThroughputExceededException"}}, "GetItem"
                    )
                return dynamodb.get_item(**kwargs)

        leases._ddb = ThrottledOnOne()

        result = reconciler.reconcile()

        assert result.skipped == 1
        assert pool.active_leases(failing.subscription_id) == 1, "left alone"
        assert pool.active_leases(healthy.subscription_id) == 0, "still recovered"

    def test_the_reap_is_not_credited_again_by_the_stream(
        self, pool, leases, reconciler
    ) -> None:
        """The pass's delete is an application delete, which the stream skips."""
        seeded = pool.seed_subscription(max_concurrency=2, active_leases=2)
        self._dead_lease(leases, seeded.subscription_id)
        alive = Lease(
            "lease-live", seeded.subscription_id, "ref", datetime.now(UTC) + timedelta(minutes=2)
        )
        leases.record(alive, holder_id="task-alive")

        reconciler.reconcile()
        assert pool.active_leases(seeded.subscription_id) == 1

        reconciler.handle_stream([application_removal(seeded.subscription_id)])
        assert pool.active_leases(seeded.subscription_id) == 1


class TestCrashRecoveryEndToEnd:
    def test_a_vanished_worker_does_not_shrink_the_pool(
        self, pool, allocator, dynamodb, reconciler
    ) -> None:
        """The failure this whole mechanism exists to prevent: capacity eroding over a
        long run because workers died without releasing."""
        seeded = pool.seed_subscription(max_concurrency=2)

        for _ in range(2):
            lease = allocator.claim("doomed-worker")
            # The worker vanishes: the lease expires and is removed, but nothing
            # decremented the counter.
            Leases(dynamodb, pool.leases_table).delete(lease.lease_id)

        assert pool.active_leases(seeded.subscription_id) == 2
        assert allocator.claim("new-worker") is None, "pool is exhausted, as expected"

        reconciler.reconcile()

        assert pool.active_leases(seeded.subscription_id) == 0
        assert allocator.claim("new-worker") is not None


class TestCreditCorrection:
    def test_an_external_correction_overwrites_the_snapshot(
        self, pool, registry
    ) -> None:
        seeded = pool.seed_subscription(credits_used=900)

        apply_external_correction(registry, seeded.subscription_id, 750)

        assert pool.credits_used(seeded.subscription_id) == 750

    def test_negative_consumption_is_rejected(self, pool, registry) -> None:
        seeded = pool.seed_subscription()

        with pytest.raises(InvalidSubscription):
            apply_external_correction(registry, seeded.subscription_id, -1)


class TestCreditWindow:
    def test_a_direct_reset_without_a_stored_window_reports_no_reset(
        self, pool, registry
    ) -> None:
        """Called without `current` on a subscription that has no window: no crash, no
        invented reset date, consumption untouched."""
        seeded = pool.seed_subscription(credits_used=500, with_window=False)

        assert registry.reset_window(seeded.subscription_id, datetime.now(UTC)) is False
        assert pool.credits_used(seeded.subscription_id) == 500
        assert pool.window_resets_raw(seeded.subscription_id) is None

    def test_a_due_window_is_reset_and_advanced(self, pool, registry) -> None:
        seeded = pool.seed_subscription(
            credits_used=9_000, window_resets_in=-timedelta(hours=1)
        )
        before = registry.get(seeded.subscription_id).window_resets_at

        assert reset_window_if_due(registry, registry.get(seeded.subscription_id)) is True

        after = registry.get(seeded.subscription_id)
        assert after.credits_used == 0
        assert after.window_resets_at > before

    def test_a_future_window_is_left_alone(self, pool, registry) -> None:
        seeded = pool.seed_subscription(
            credits_used=500, window_resets_in=timedelta(days=10)
        )

        assert reset_window_if_due(registry, registry.get(seeded.subscription_id)) is False
        assert pool.credits_used(seeded.subscription_id) == 500

    def test_a_long_outage_catches_up_rather_than_shifting_forever(
        self, pool, registry
    ) -> None:
        """Advancing from the recorded instant, not from now, keeps windows aligned."""
        seeded = pool.seed_subscription(window_resets_in=-timedelta(days=95))

        reset_window_if_due(registry, registry.get(seeded.subscription_id))

        after = registry.get(seeded.subscription_id).window_resets_at
        assert after > datetime.now(UTC)
        assert after < datetime.now(UTC) + timedelta(days=31)

    def test_the_default_step_is_one_calendar_month(self, pool, registry) -> None:
        """Kiro resets on a date. A 30-day step from 1 February lands on 3 March."""
        february = datetime(2027, 2, 1, tzinfo=UTC)
        seeded = pool.seed_subscription(window_resets_raw=february.isoformat())

        reset_window_if_due(
            registry,
            registry.get(seeded.subscription_id),
            now=datetime(2027, 2, 1, 0, 5, tzinfo=UTC),
        )

        assert registry.get(seeded.subscription_id).window_resets_at == datetime(
            2027, 3, 1, tzinfo=UTC
        )

    def test_a_month_end_anchor_is_clamped_without_drifting(self) -> None:
        from leaseclient.credits import add_months, next_reset

        january_31 = datetime(2027, 1, 31, tzinfo=UTC)

        assert add_months(january_31, 1) == datetime(2027, 2, 28, tzinfo=UTC)
        # Stepped from the anchor, not from the clamped date: 31 March, not 28 March.
        assert next_reset(
            january_31, datetime(2027, 3, 1, tzinfo=UTC)
        ) == datetime(2027, 3, 31, tzinfo=UTC)

    def test_a_fixed_day_step_is_still_honoured(self, pool, registry, monkeypatch) -> None:
        from leaseclient.credits import window_from_environment

        monkeypatch.setenv("CREDIT_WINDOW_DAYS", "30")
        february = datetime(2027, 2, 1, tzinfo=UTC)
        seeded = pool.seed_subscription(window_resets_raw=february.isoformat())

        reset_window_if_due(
            registry,
            registry.get(seeded.subscription_id),
            now=datetime(2027, 2, 1, 0, 5, tzinfo=UTC),
            window=window_from_environment(),
        )

        assert registry.get(seeded.subscription_id).window_resets_at == datetime(
            2027, 3, 3, tzinfo=UTC
        )

    def test_a_reset_does_not_zero_a_window_a_worker_already_adopted(
        self, pool, registry
    ) -> None:
        """The reconciler read November; a worker then recorded December's figure."""
        november = datetime(2027, 11, 1, tzinfo=UTC)
        december = datetime(2027, 12, 1, tzinfo=UTC)
        moment = datetime(2027, 11, 1, 0, 2, tzinfo=UTC)
        seeded = pool.seed_subscription(
            credits_used="9000", window_resets_raw=november.isoformat()
        )
        read_by_reconciler = registry.get(seeded.subscription_id)
        registry.record_usage(seeded.subscription_id, "0.40", december, now=moment)

        assert reset_window_if_due(registry, read_by_reconciler, now=moment) is False
        assert pool.credits_used(seeded.subscription_id) == Decimal("0.40")

    def test_the_scheduled_pass_resets_due_windows(self, pool, reconciler) -> None:
        pool.seed_subscription(credits_used=100, window_resets_in=-timedelta(hours=1))

        result = reconciler.reconcile()

        assert result.windows_reset == 1


class TestTheEntryPoint:
    def test_a_stream_event_takes_the_expiry_path(
        self, pool, monkeypatch, registry, leases, sink
    ) -> None:
        seeded = pool.seed_subscription(active_leases=2)
        monkeypatch.setattr(
            "reconciler.handler._from_environment",
            lambda: Reconciler(registry, leases, Metrics(stream=sink)),
        )

        result = handler({"Records": [ttl_removal(seeded.subscription_id)]})

        assert result["expiredLeases"] == 1
        assert pool.active_leases(seeded.subscription_id) == 1

    def test_a_scheduled_event_takes_the_correction_path(
        self, pool, monkeypatch, registry, leases, sink
    ) -> None:
        seeded = pool.seed_subscription(active_leases=3)
        monkeypatch.setattr(
            "reconciler.handler._from_environment",
            lambda: Reconciler(registry, leases, Metrics(stream=sink)),
        )

        result = handler({"source": "aws.events", "detail-type": "Scheduled Event"})

        assert result["correctedSubscriptions"] == 1
        assert pool.active_leases(seeded.subscription_id) == 0

    def test_an_empty_records_list_is_treated_as_scheduled(
        self, pool, monkeypatch, registry, leases, sink
    ) -> None:
        pool.seed_subscription(active_leases=1)
        monkeypatch.setattr(
            "reconciler.handler._from_environment",
            lambda: Reconciler(registry, leases, Metrics(stream=sink)),
        )

        assert handler({"Records": []})["correctedSubscriptions"] == 1
