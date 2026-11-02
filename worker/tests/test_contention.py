"""The property the component exists to provide, verified against a real engine.

These tests do not run against the in-process AWS fake. That is not a preference,
it is a measurement: a deliberately non-atomic read-then-write reservation passes
the same scenario against the fake unchanged, while against DynamoDB Local it hands
out every request regardless of the declared ceiling. A contention test that cannot
tell those two implementations apart is not testing anything.

`test_the_scenario_discriminates` keeps that honest. It runs the naive
implementation and asserts it overcommits, so if the environment ever stops
reproducing contention, the suite says so rather than reporting false confidence.
"""

from __future__ import annotations

import io
import random
import threading
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from leaseclient import schema
from leaseclient.claim import Allocator, ClaimConfig, Leases
from leaseclient.credits import reset_window_if_due
from leaseclient.metrics import Metrics
from leaseclient.registry import Registry, Reservation
from tests.conftest import StubSecrets

pytestmark = pytest.mark.contention

CEILING = 3
POOL_SIZE = 4
CAPACITY = POOL_SIZE * CEILING


class NaiveRegistry(Registry):
    """Read, decide, write. The obvious implementation, and wrong under contention.

    Present only so the tests can prove they would catch it.
    """

    def try_reserve(self, subscription_id: str, *, credit_ceiling: int) -> Reservation:
        record = self.get(subscription_id)
        if record.active_leases >= record.max_concurrency:
            return Reservation(subscription_id, granted=False)
        self._ddb.update_item(
            TableName=self._table,
            Key={schema.SUBSCRIPTION_ID: {"S": subscription_id}},
            UpdateExpression=f"SET {schema.ACTIVE_LEASES} = :n",
            ExpressionAttributeValues={":n": {"N": str(record.active_leases + 1)}},
        )
        return Reservation(subscription_id, granted=True)


def race(
    pool,
    client_factory,
    claimants: int,
    registry_class=Registry,
) -> list:
    """Start `claimants` workers at the same instant and collect what they got.

    Each worker builds its own client: sharing one would serialize exactly the
    interleaving the test needs to produce.
    """
    granted: list = []
    lock = threading.Lock()
    barrier = threading.Barrier(claimants)

    def work(index: int) -> None:
        allocator = Allocator(
            registry=registry_class(client_factory(), pool.subscriptions_table),
            leases=Leases(client_factory(), pool.leases_table),
            secrets=StubSecrets(),
            metrics=Metrics(stream=io.StringIO()),
            # One attempt: a retry would let a worker pick up capacity another
            # released, which blurs what the ceiling admitted at the same moment.
            config=ClaimConfig(max_attempts=1),
            sleep=lambda _: None,
        )
        barrier.wait()
        lease = allocator.claim(f"task-{index}")
        if lease is not None:
            with lock:
                granted.append(lease)

    threads = [threading.Thread(target=work, args=(i,)) for i in range(claimants)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return granted


class TestNoOvercommit:
    def test_no_subscription_exceeds_its_declared_ceiling(
        self, contended_pool, new_engine_client
    ) -> None:
        contended_pool.seed_subscriptions(POOL_SIZE, max_concurrency=CEILING)

        race(contended_pool, new_engine_client, claimants=CAPACITY * 4)

        for record in Registry(
            new_engine_client(), contended_pool.subscriptions_table
        ).describe_all():
            assert record.active_leases <= CEILING, (
                f"{record.subscription_id} holds {record.active_leases} "
                f"against a ceiling of {CEILING}"
            )

    def test_exactly_the_declared_capacity_is_handed_out(
        self, contended_pool, new_engine_client
    ) -> None:
        """Neither overcommitted nor left idle, with four times as many claimants."""
        contended_pool.seed_subscriptions(POOL_SIZE, max_concurrency=CEILING)

        granted = race(contended_pool, new_engine_client, claimants=CAPACITY * 4)

        assert len(granted) == CAPACITY

    def test_every_granted_lease_is_backed_by_a_record(
        self, contended_pool, new_engine_client
    ) -> None:
        """A counter without a lease reconciles; a lease without one is overcommit."""
        contended_pool.seed_subscriptions(POOL_SIZE, max_concurrency=CEILING)

        granted = race(contended_pool, new_engine_client, claimants=CAPACITY * 4)

        stored = contended_pool.all_leases()
        assert {lease.lease_id for lease in granted} == {
            item[schema.LEASE_ID] for item in stored
        }

    def test_the_counter_agrees_with_the_leases_recorded(
        self, contended_pool, new_engine_client
    ) -> None:
        """Drift here would mean the pool slowly loses or invents capacity."""
        contended_pool.seed_subscriptions(POOL_SIZE, max_concurrency=CEILING)

        race(contended_pool, new_engine_client, claimants=CAPACITY * 4)

        for record in Registry(
            new_engine_client(), contended_pool.subscriptions_table
        ).describe_all():
            assert record.active_leases == contended_pool.lease_count(
                record.subscription_id
            )

    def test_a_single_subscription_admits_only_its_ceiling(
        self, contended_pool, new_engine_client
    ) -> None:
        """The narrowest case: all contention on one item."""
        contended_pool.seed_subscription("sub-only", max_concurrency=CEILING)

        granted = race(contended_pool, new_engine_client, claimants=40)

        assert len(granted) == CEILING
        assert contended_pool.active_leases("sub-only") == CEILING


class TestReleaseUnderContention:
    def test_capacity_returns_intact_after_a_concurrent_round(
        self, contended_pool, new_engine_client
    ) -> None:
        contended_pool.seed_subscriptions(POOL_SIZE, max_concurrency=CEILING)
        granted = race(contended_pool, new_engine_client, claimants=CAPACITY * 2)

        allocator = Allocator(
            registry=Registry(new_engine_client(), contended_pool.subscriptions_table),
            leases=Leases(new_engine_client(), contended_pool.leases_table),
            secrets=StubSecrets(),
            metrics=Metrics(stream=io.StringIO()),
        )
        threads = [
            threading.Thread(target=allocator.release, args=(lease,))
            for lease in granted
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        for record in Registry(
            new_engine_client(), contended_pool.subscriptions_table
        ).describe_all():
            assert record.active_leases == 0
        assert contended_pool.all_leases() == []


class TestTheScenarioDiscriminates:
    """Guards the tests above against becoming vacuous.

    If the environment stops reproducing contention, every test in this file would
    pass for an implementation known to be wrong. This one fails instead.
    """

    def test_a_naive_reservation_overcommits(
        self, contended_pool, new_engine_client
    ) -> None:
        contended_pool.seed_subscriptions(POOL_SIZE, max_concurrency=CEILING)
        claimants = CAPACITY * 4

        granted = race(
            contended_pool,
            new_engine_client,
            claimants=claimants,
            registry_class=NaiveRegistry,
        )

        assert len(granted) > CAPACITY, (
            "a read-then-write reservation handed out no more than the real capacity, "
            "so this environment is not reproducing write contention and the "
            "no-overcommit tests above prove nothing"
        )


class TestUsageReadingsUnderContention:
    """Many workers record readings of one subscription at once, in any order.

    The stored figure must end at the largest reading. A read-then-write recorder
    would end at whichever write landed last.
    """

    def test_concurrent_out_of_order_readings_end_at_the_maximum(
        self, contended_pool, new_engine_client
    ) -> None:
        resets = datetime(2099, 11, 1, tzinfo=UTC)
        contended_pool.seed_subscription(
            "sub-usage", credits_used="0", window_resets_raw=resets.isoformat()
        )
        readings = [Decimal(n) / 100 for n in range(1000, 1060)]
        random.Random(7).shuffle(readings)
        barrier = threading.Barrier(len(readings))

        def record(value: Decimal) -> None:
            registry = Registry(new_engine_client(), contended_pool.subscriptions_table)
            barrier.wait()
            registry.record_usage("sub-usage", value, resets)

        threads = [threading.Thread(target=record, args=(v,)) for v in readings]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert contended_pool.credits_used("sub-usage") == max(readings)

    def test_adoption_racing_a_window_reset_ends_consistent(
        self, contended_pool, new_engine_client
    ) -> None:
        """At a month boundary the reconciler resets and a worker adopts the new window.

        Either order must end with the new window and a figure that belongs to it:
        the adopted reading, or zero followed by that reading raising it.
        """
        november = datetime(2099, 11, 1, tzinfo=UTC)
        december = datetime(2099, 12, 1, tzinfo=UTC)
        moment = datetime(2099, 11, 1, 0, 1, tzinfo=UTC)

        for attempt in range(10):
            subscription_id = f"sub-boundary-{attempt}"
            contended_pool.seed_subscription(
                subscription_id, credits_used="9000", window_resets_raw=november.isoformat()
            )
            barrier = threading.Barrier(2)

            def reset() -> None:
                registry = Registry(new_engine_client(), contended_pool.subscriptions_table)
                record = registry.get(subscription_id)
                barrier.wait()
                reset_window_if_due(registry, record, now=moment)

            def adopt() -> None:
                registry = Registry(new_engine_client(), contended_pool.subscriptions_table)
                barrier.wait()
                registry.record_usage(subscription_id, Decimal("0.40"), december, now=moment)

            threads = [threading.Thread(target=reset), threading.Thread(target=adopt)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            # Reset first: zero, then the reading raises it to 0.40. Adoption first:
            # 0.40, and the reset's condition on November no longer matches.
            assert contended_pool.window_resets_raw(subscription_id) == december.isoformat()
            assert contended_pool.credits_used(subscription_id) == Decimal("0.40")
