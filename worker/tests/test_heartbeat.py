"""Lease liveness and the session that ties allocation to a run."""

from __future__ import annotations

import io
import signal
import threading
from datetime import timedelta

import pytest

from leaseclient import schema
from leaseclient.claim import Allocator, ClaimConfig, Leases
from leaseclient.heartbeat import MAX_CONSECUTIVE_FAILURES, Heartbeat, LeaseLost
from leaseclient.metrics import Metrics
from leaseclient.registry import Registry
from leaseclient.session import PoolSaturated, hold_subscription
from tests.conftest import FAKE_KEY_MATERIAL_PREFIX


@pytest.fixture
def sink() -> io.StringIO:
    return io.StringIO()


@pytest.fixture
def allocator(pool, dynamodb, secretsmanager, sink) -> Allocator:
    return Allocator(
        registry=Registry(dynamodb, pool.subscriptions_table),
        leases=Leases(dynamodb, pool.leases_table),
        secrets=secretsmanager,
        metrics=Metrics(stream=sink),
        config=ClaimConfig(
            lease_lifetime=timedelta(seconds=120),
            base_backoff=timedelta(0),
            max_backoff=timedelta(0),
        ),
        sleep=lambda _: None,
    )


def a_heartbeat(pool, dynamodb, lease, holder_id="task-1", **overrides) -> Heartbeat:
    settings = {
        "lifetime": timedelta(seconds=120),
        "interval": timedelta(seconds=30),
        **overrides,
    }
    return Heartbeat(
        dynamodb,
        lease=lease,
        holder_id=holder_id,
        table_name=pool.leases_table,
        **settings,
    )


def stored_expiry(pool, lease_id: str) -> int:
    (item,) = [
        lease for lease in pool.all_leases() if lease[schema.LEASE_ID] == lease_id
    ]
    return int(item[schema.EXPIRES_AT])


class TestExtension:
    def test_an_extension_moves_expiry_further_out(
        self, pool, dynamodb, allocator
    ) -> None:
        pool.seed_subscription()
        lease = allocator.claim("task-1")
        before = stored_expiry(pool, lease.lease_id)

        heartbeat = a_heartbeat(pool, dynamodb, lease, lifetime=timedelta(seconds=600))
        assert heartbeat.extend_once() is True

        assert stored_expiry(pool, lease.lease_id) > before

    def test_an_extension_records_when_it_happened(
        self, pool, dynamodb, allocator
    ) -> None:
        pool.seed_subscription()
        lease = allocator.claim("task-1")
        (before,) = pool.all_leases()

        a_heartbeat(pool, dynamodb, lease).extend_once()

        (after,) = pool.all_leases()
        assert after[schema.HEARTBEAT_AT] >= before[schema.HEARTBEAT_AT]

    def test_the_interval_leaves_room_for_consecutive_failures(self, allocator) -> None:
        """Derived from the lifetime rather than configured separately: set
        independently they can be made inconsistent, and the failure mode is a lease
        expiring under a worker still doing useful work."""
        config = allocator.config

        assert config.heartbeat_interval * 4 == config.lease_lifetime
        assert config.heartbeat_interval * (MAX_CONSECUTIVE_FAILURES + 1) <= (
            config.lease_lifetime
        )


class TestLoss:
    def test_a_deleted_lease_is_reported_as_lost(
        self, pool, dynamodb, allocator
    ) -> None:
        pool.seed_subscription()
        lease = allocator.claim("task-1")
        heartbeat = a_heartbeat(pool, dynamodb, lease)
        Leases(dynamodb, pool.leases_table).delete(lease.lease_id)

        assert heartbeat.extend_once() is False
        assert heartbeat.lost is True

    def test_a_lost_lease_is_not_recreated(self, pool, dynamodb, allocator) -> None:
        """Recreating it would be inventing capacity."""
        pool.seed_subscription()
        lease = allocator.claim("task-1")
        Leases(dynamodb, pool.leases_table).delete(lease.lease_id)

        a_heartbeat(pool, dynamodb, lease).extend_once()

        assert pool.all_leases() == []

    def test_a_lease_taken_over_by_another_holder_is_reported_as_lost(
        self, pool, dynamodb, allocator
    ) -> None:
        """Without the holder check, an extension could resurrect a reclaimed lease
        and put two workers on the same reserved capacity."""
        pool.seed_subscription()
        lease = allocator.claim("original-holder")
        dynamodb.update_item(
            TableName=pool.leases_table,
            Key={schema.LEASE_ID: {"S": lease.lease_id}},
            UpdateExpression="SET #holder = :new",
            ExpressionAttributeNames={"#holder": schema.HOLDER_ID},
            ExpressionAttributeValues={":new": {"S": "someone-else"}},
        )

        heartbeat = a_heartbeat(pool, dynamodb, lease, holder_id="original-holder")

        assert heartbeat.extend_once() is False
        assert heartbeat.lost is True

    def test_raise_if_lost_stops_the_worker(self, pool, dynamodb, allocator) -> None:
        pool.seed_subscription()
        lease = allocator.claim("task-1")
        heartbeat = a_heartbeat(pool, dynamodb, lease)
        Leases(dynamodb, pool.leases_table).delete(lease.lease_id)
        heartbeat.extend_once()

        with pytest.raises(LeaseLost, match="reclaimed"):
            heartbeat.raise_if_lost()

    def test_raise_if_lost_is_silent_while_held(
        self, pool, dynamodb, allocator
    ) -> None:
        pool.seed_subscription()
        lease = allocator.claim("task-1")

        a_heartbeat(pool, dynamodb, lease).raise_if_lost()


class TestTransientFailure:
    def test_a_transient_error_does_not_lose_the_lease(
        self, pool, dynamodb, allocator, monkeypatch
    ) -> None:
        pool.seed_subscription()
        lease = allocator.claim("task-1")
        heartbeat = a_heartbeat(pool, dynamodb, lease)

        calls = {"n": 0}
        real_update = dynamodb.update_item

        def flaky(**kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("connection reset")
            return real_update(**kwargs)

        monkeypatch.setattr(dynamodb, "update_item", flaky)

        assert heartbeat.extend_once() is False
        assert heartbeat.lost is False, "a transient error must not presume loss"
        assert heartbeat.extend_once() is True
        assert heartbeat.lost is False

    def test_a_successful_extension_resets_the_failure_count(
        self, pool, dynamodb, allocator, monkeypatch
    ) -> None:
        pool.seed_subscription()
        lease = allocator.claim("task-1")
        heartbeat = a_heartbeat(pool, dynamodb, lease)
        real_update = dynamodb.update_item
        fail = {"on": True}

        def flaky(**kwargs):
            if fail["on"]:
                raise OSError("connection reset")
            return real_update(**kwargs)

        monkeypatch.setattr(dynamodb, "update_item", flaky)
        heartbeat.extend_once()
        heartbeat.extend_once()
        fail["on"] = False

        assert heartbeat.extend_once() is True
        fail["on"] = True
        # Two failures again should still not reach the threshold.
        heartbeat.extend_once()
        heartbeat.extend_once()
        assert heartbeat.lost is False


class TestBackgroundThread:
    def test_the_thread_extends_on_its_interval(
        self, pool, dynamodb, allocator
    ) -> None:
        pool.seed_subscription()
        lease = allocator.claim("task-1")
        heartbeat = a_heartbeat(
            pool, dynamodb, lease, interval=timedelta(milliseconds=20)
        )

        with heartbeat:
            deadline = threading.Event()
            deadline.wait(0.4)

        assert heartbeat.extensions >= 2

    def test_stopping_ends_the_thread(self, pool, dynamodb, allocator) -> None:
        pool.seed_subscription()
        lease = allocator.claim("task-1")
        heartbeat = a_heartbeat(
            pool, dynamodb, lease, interval=timedelta(milliseconds=10)
        )

        heartbeat.start()
        heartbeat.stop()

        assert heartbeat._thread is not None
        assert not heartbeat._thread.is_alive()

    def test_a_heartbeat_cannot_be_restarted(self, pool, dynamodb, allocator) -> None:
        """A stopped heartbeat means the work is over or the lease is gone."""
        pool.seed_subscription()
        lease = allocator.claim("task-1")
        heartbeat = a_heartbeat(pool, dynamodb, lease)
        heartbeat.start()
        heartbeat.stop()

        with pytest.raises(RuntimeError, match="already been started"):
            heartbeat.start()


class TestSession:
    def test_a_session_yields_the_key_for_the_child_environment(
        self, pool, dynamodb, allocator
    ) -> None:
        seeded = pool.seed_subscription()

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            assert session.environment() == {"KIRO_API_KEY": seeded.key_material}
            assert session.subscription_id == seeded.subscription_id

    def test_capacity_is_returned_when_the_session_ends(
        self, pool, dynamodb, allocator
    ) -> None:
        seeded = pool.seed_subscription()

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ):
            assert pool.active_leases(seeded.subscription_id) == 1

        assert pool.active_leases(seeded.subscription_id) == 0
        assert pool.lease_count(seeded.subscription_id) == 0

    def test_capacity_is_returned_when_the_run_raises(
        self, pool, dynamodb, allocator
    ) -> None:
        """The reason this is a context manager and not three calls in order."""
        seeded = pool.seed_subscription()

        with pytest.raises(RuntimeError, match="workflow blew up"):
            with hold_subscription(
                allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
            ):
                raise RuntimeError("workflow blew up")

        assert pool.active_leases(seeded.subscription_id) == 0

    def test_a_failing_release_does_not_mask_the_original_failure(
        self, pool, dynamodb, allocator, monkeypatch
    ) -> None:
        pool.seed_subscription()
        monkeypatch.setattr(
            allocator, "release", lambda _: (_ for _ in ()).throw(OSError("no network"))
        )

        with pytest.raises(RuntimeError, match="the real problem"):
            with hold_subscription(
                allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
            ):
                raise RuntimeError("the real problem")

    def test_a_saturated_pool_is_signalled_distinctly(
        self, pool, dynamodb, allocator
    ) -> None:
        """A worker should exit zero on this, not crash and be replaced."""
        pool.seed_subscription(max_concurrency=1, active_leases=1)

        with pytest.raises(PoolSaturated):
            with hold_subscription(
                allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
            ):
                pytest.fail("the body must not run when the pool is saturated")

    def test_the_session_repr_does_not_carry_the_key(
        self, pool, dynamodb, allocator
    ) -> None:
        pool.seed_subscription()

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ) as session:
            assert FAKE_KEY_MATERIAL_PREFIX not in repr(session)

    def test_sigterm_returns_capacity(self, pool, dynamodb, allocator) -> None:
        """Container stop sends SIGTERM before SIGKILL; releasing there is the
        difference between the next worker starting now and waiting out a lifetime."""
        seeded = pool.seed_subscription()

        with pytest.raises(SystemExit):
            with hold_subscription(
                allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
            ):
                signal.raise_signal(signal.SIGTERM)

        assert pool.active_leases(seeded.subscription_id) == 0

    def test_the_previous_signal_handler_is_restored(
        self, pool, dynamodb, allocator
    ) -> None:
        pool.seed_subscription()
        original = signal.getsignal(signal.SIGTERM)

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ):
            assert signal.getsignal(signal.SIGTERM) is not original

        assert signal.getsignal(signal.SIGTERM) is original

    def test_release_happens_once_even_with_several_triggers(
        self, pool, dynamodb, allocator
    ) -> None:
        seeded = pool.seed_subscription(max_concurrency=5, active_leases=2)

        with hold_subscription(
            allocator, dynamodb, holder_id="task-1", leases_table=pool.leases_table
        ):
            pass

        # Started at 2, one claimed, one released: back to 2, not below.
        assert pool.active_leases(seeded.subscription_id) == 2
