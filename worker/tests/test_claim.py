"""The allocation protocol.

The test that matters in this file is the contention one. Everything else describes
behavior; that one demonstrates the property the component exists to provide, and a
version of this suite that only exercises the claim path serially would prove
nothing about it.
"""

from __future__ import annotations

import io
import json
import threading
from datetime import timedelta

import pytest

from leaseclient.claim import Allocator, ClaimConfig, Leases, SecretUnavailable
from leaseclient.metrics import ClaimOutcome, Metrics
from leaseclient.registry import Registry
from leaseclient.schema import SubscriptionState
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
        # No real waiting: the backoff schedule is asserted separately.
        config=ClaimConfig(base_backoff=timedelta(0), max_backoff=timedelta(0)),
        sleep=lambda _: None,
    )


def outcomes(sink: io.StringIO) -> list[str]:
    return [
        json.loads(line)["Outcome"]
        for line in sink.getvalue().splitlines()
        if line.strip() and "Outcome" in json.loads(line)
    ]


class TestGrant:
    def test_a_claim_returns_a_lease_on_an_eligible_subscription(
        self, pool, allocator
    ) -> None:
        seeded = pool.seed_subscription()

        lease = allocator.claim("task-1")

        assert lease is not None
        assert lease.subscription_id == seeded.subscription_id
        assert lease.secret_ref == seeded.secret_ref
        assert lease.lease_id

    def test_claiming_consumes_capacity_and_records_the_lease(
        self, pool, allocator
    ) -> None:
        seeded = pool.seed_subscription()

        allocator.claim("task-1")

        assert pool.active_leases(seeded.subscription_id) == 1
        assert pool.lease_count(seeded.subscription_id) == 1

    def test_the_lease_records_who_holds_it(self, pool, allocator) -> None:
        """Without a traceable holder, a stranded lease cannot be attributed."""
        pool.seed_subscription()

        allocator.claim("task-arn-1234")

        (lease,) = pool.all_leases()
        assert lease["holderId"] == "task-arn-1234"

    def test_the_lease_expires_in_the_future(self, pool, allocator) -> None:
        pool.seed_subscription()

        lease = allocator.claim("task-1")

        assert lease.expires_at.timestamp() > 0
        (stored,) = pool.all_leases()
        assert int(stored["expiresAt"]) == int(lease.expires_at.timestamp())

    def test_an_ineligible_candidate_is_skipped_for_an_eligible_one(
        self, pool, allocator
    ) -> None:
        pool.seed_subscription("sub-full", max_concurrency=1, active_leases=1)
        pool.seed_subscription("sub-free")

        lease = allocator.claim("task-1")

        assert lease.subscription_id == "sub-free"

    def test_a_disabled_subscription_is_never_offered(self, pool, allocator) -> None:
        pool.seed_subscription("sub-off", state=SubscriptionState.DISABLED)
        pool.seed_subscription("sub-on")

        assert allocator.claim("task-1").subscription_id == "sub-on"


class TestSaturation:
    def test_an_exhausted_pool_returns_none(self, pool, allocator) -> None:
        """Saturation is an answer, not a failure."""
        pool.seed_subscription(max_concurrency=1, active_leases=1)

        assert allocator.claim("task-1") is None

    def test_an_empty_pool_returns_none(self, allocator) -> None:
        assert allocator.claim("task-1") is None

    def test_saturation_leaves_no_reservation_behind(self, pool, allocator) -> None:
        seeded = pool.seed_subscription(max_concurrency=1, active_leases=1)

        allocator.claim("task-1")

        assert pool.active_leases(seeded.subscription_id) == 1
        assert pool.all_leases() == []

    def test_saturation_is_reported_as_a_distinct_outcome(
        self, pool, allocator, sink
    ) -> None:
        pool.seed_subscription(max_concurrency=1, active_leases=1)

        allocator.claim("task-1")

        assert ClaimOutcome.SATURATED in outcomes(sink)

    def test_attempts_are_bounded(self, pool, dynamodb, secretsmanager, sink) -> None:
        attempts = []
        allocator = Allocator(
            registry=Registry(dynamodb, pool.subscriptions_table),
            leases=Leases(dynamodb, pool.leases_table),
            secrets=secretsmanager,
            metrics=Metrics(stream=sink),
            config=ClaimConfig(max_attempts=3),
            sleep=lambda seconds: attempts.append(seconds),
        )
        pool.seed_subscription(max_concurrency=1, active_leases=1)

        assert allocator.claim("task-1") is None
        # Three attempts means two waits between them.
        assert len(attempts) == 2

    def test_capacity_released_during_backoff_becomes_visible(
        self, pool, dynamodb, secretsmanager, sink
    ) -> None:
        """The candidate list is re-read on every attempt, not cached."""
        seeded = pool.seed_subscription(max_concurrency=1, active_leases=1)
        registry = Registry(dynamodb, pool.subscriptions_table)

        def release_during_backoff(_seconds):
            registry.release_reservation(seeded.subscription_id)

        allocator = Allocator(
            registry=registry,
            leases=Leases(dynamodb, pool.leases_table),
            secrets=secretsmanager,
            metrics=Metrics(stream=sink),
            config=ClaimConfig(max_attempts=3),
            sleep=release_during_backoff,
        )

        assert allocator.claim("task-1") is not None


class TestBackoff:
    def test_delays_grow_and_are_jittered(
        self, pool, dynamodb, secretsmanager, sink
    ) -> None:
        """Hundreds of tasks start together, so a fixed schedule would synchronize
        their retries into contending waves."""
        waits: list[float] = []
        allocator = Allocator(
            registry=Registry(dynamodb, pool.subscriptions_table),
            leases=Leases(dynamodb, pool.leases_table),
            secrets=secretsmanager,
            metrics=Metrics(stream=sink),
            config=ClaimConfig(
                max_attempts=6,
                base_backoff=timedelta(seconds=1),
                max_backoff=timedelta(seconds=30),
            ),
            sleep=waits.append,
        )
        pool.seed_subscription(max_concurrency=1, active_leases=1)

        allocator.claim("task-1")

        assert len(waits) == 5
        # Full jitter: each wait lies in [0, capped exponential].
        for attempt, wait in enumerate(waits):
            assert 0 <= wait <= min(2**attempt, 30)
        # Jitter means the sequence is not a fixed doubling.
        assert waits != [1.0, 2.0, 4.0, 8.0, 16.0]

    def test_the_delay_is_capped(self, pool, dynamodb, secretsmanager, sink) -> None:
        waits: list[float] = []
        allocator = Allocator(
            registry=Registry(dynamodb, pool.subscriptions_table),
            leases=Leases(dynamodb, pool.leases_table),
            secrets=secretsmanager,
            metrics=Metrics(stream=sink),
            config=ClaimConfig(
                max_attempts=12,
                base_backoff=timedelta(seconds=1),
                max_backoff=timedelta(seconds=2),
            ),
            sleep=waits.append,
        )
        pool.seed_subscription(max_concurrency=1, active_leases=1)

        allocator.claim("task-1")

        assert all(wait <= 2 for wait in waits)


class TestSecretResolution:
    def test_the_key_is_readable_for_a_held_lease(self, pool, allocator) -> None:
        seeded = pool.seed_subscription()
        lease = allocator.claim("task-1")

        assert allocator.resolve_key(lease) == seeded.key_material

    def test_an_unresolvable_secret_sends_the_claim_to_the_next_candidate(
        self, pool, allocator
    ) -> None:
        pool.seed_subscription("sub-no-secret", with_secret=False)
        pool.seed_subscription("sub-ok")

        lease = allocator.claim("task-1")

        assert lease.subscription_id == "sub-ok"

    def test_an_unresolvable_secret_gives_the_reservation_back(
        self, pool, allocator
    ) -> None:
        """A leaked reservation here would shrink the pool permanently."""
        broken = pool.seed_subscription("sub-no-secret", with_secret=False)

        assert allocator.claim("task-1") is None
        assert pool.active_leases(broken.subscription_id) == 0
        assert pool.lease_count(broken.subscription_id) == 0

    def test_no_lease_is_handed_out_for_an_unusable_key(self, pool, allocator) -> None:
        """A worker holding capacity it cannot authenticate with is worse than none:
        an invalid credential does not reliably produce a non-zero exit."""
        pool.seed_subscription(with_secret=False)

        assert allocator.claim("task-1") is None
        assert pool.all_leases() == []

    def test_resolution_failure_does_not_echo_the_reference_or_the_value(
        self, pool, allocator
    ) -> None:
        seeded = pool.seed_subscription(with_secret=False)
        lease_ref = seeded.secret_ref

        with pytest.raises(SecretUnavailable) as caught:
            allocator._verify_secret_resolves(lease_ref)

        assert FAKE_KEY_MATERIAL_PREFIX not in str(caught.value)


class TestDistribution:
    def test_leases_spread_across_equally_configured_subscriptions(
        self, pool, allocator
    ) -> None:
        """A fixed candidate order would load the first subscription and idle the rest."""
        pool.seed_subscriptions(4, max_concurrency=1)

        claimed = {allocator.claim(f"task-{i}").subscription_id for i in range(4)}

        assert len(claimed) == 4

    def test_repeated_cycles_do_not_concentrate_on_one_subscription(
        self, pool, allocator
    ) -> None:
        pool.seed_subscriptions(3, max_concurrency=5)
        used: dict[str, int] = {}

        for i in range(30):
            lease = allocator.claim(f"task-{i}")
            used[lease.subscription_id] = used.get(lease.subscription_id, 0) + 1
            allocator.release(lease)

        assert len(used) == 3, "some subscriptions were never used"
        assert max(used.values()) < 30, "all leases landed on one subscription"


class TestRelease:
    def test_release_returns_capacity_and_removes_the_lease(
        self, pool, allocator
    ) -> None:
        seeded = pool.seed_subscription()
        lease = allocator.claim("task-1")

        allocator.release(lease)

        assert pool.active_leases(seeded.subscription_id) == 0
        assert pool.lease_count(seeded.subscription_id) == 0

    def test_released_capacity_is_immediately_reusable(self, pool, allocator) -> None:
        """Otherwise the next worker waits out a lifetime for capacity already free."""
        pool.seed_subscription(max_concurrency=1)
        first = allocator.claim("task-1")
        assert allocator.claim("task-2") is None

        allocator.release(first)

        assert allocator.claim("task-3") is not None

    def test_release_is_idempotent(self, pool, allocator) -> None:
        seeded = pool.seed_subscription(max_concurrency=5)
        lease = allocator.claim("task-1")

        allocator.release(lease)
        allocator.release(lease)

        assert pool.active_leases(seeded.subscription_id) == 0


class TestTheCreditMargin:
    """Decimal, because `/usage` reports to 0.01 and a sized margin is a fraction."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [("0.21", "0.21"), ("1", "1"), ("2.5", "2.5"), ("0", "0"), (" 0.10 ", "0.10")],
    )
    def test_fractions_are_accepted(self, raw: str, expected: str) -> None:
        from decimal import Decimal

        from leaseclient.claim import parse_credit_margin

        assert parse_credit_margin(raw) == Decimal(expected)

    @pytest.mark.parametrize("raw", [None, "", "   "])
    def test_unset_is_the_default(self, raw) -> None:
        from leaseclient.claim import DEFAULT_CREDIT_MARGIN, parse_credit_margin

        assert parse_credit_margin(raw) == DEFAULT_CREDIT_MARGIN

    @pytest.mark.parametrize("raw", ["0.001", "abc", "-1", "NaN", "Infinity"])
    def test_anything_else_is_a_configuration_error(self, raw: str) -> None:
        from leaseclient.claim import parse_credit_margin

        with pytest.raises(ValueError, match="CREDIT_MARGIN"):
            parse_credit_margin(raw)

    def test_a_fractional_margin_decides_a_claim_at_the_hundredth(
        self, pool, dynamodb, secretsmanager, sink
    ) -> None:
        from decimal import Decimal

        under = pool.seed_subscription("sub-under", credit_budget=10, credits_used="9.78")
        at = pool.seed_subscription("sub-at", credit_budget=10, credits_used="9.79")
        allocator = Allocator(
            registry=Registry(dynamodb, pool.subscriptions_table),
            leases=Leases(dynamodb, pool.leases_table),
            secrets=secretsmanager,
            metrics=Metrics(stream=sink),
            config=ClaimConfig(
                max_attempts=1,
                base_backoff=timedelta(0),
                max_backoff=timedelta(0),
                credit_margin=Decimal("0.21"),
            ),
            sleep=lambda _: None,
        )

        first = allocator.claim("task-1")
        second = allocator.claim("task-2")

        assert first is not None and first.subscription_id == under.subscription_id
        # sub-under still has headroom (max_concurrency 5), so it takes the second too;
        # sub-at was refused on budget both times.
        assert second is not None and second.subscription_id == under.subscription_id
        assert pool.active_leases(at.subscription_id) == 0

    def test_the_task_reads_a_fractional_margin(self, monkeypatch, tmp_path) -> None:
        from decimal import Decimal

        from runner.main import Settings

        for name, value in {
            "SUBSCRIPTIONS_TABLE": "subs",
            "LEASES_TABLE": "leases",
            "WORK_QUEUE_URL": "http://localhost/queue",
            "RESULTS_BUCKET": "results",
            "WORKFLOW_DIR": str(tmp_path),
            "CREDIT_MARGIN": "0.21",
        }.items():
            monkeypatch.setenv(name, value)

        assert Settings.from_environment().credit_margin == Decimal("0.21")

    def test_an_unusable_margin_stops_the_task_as_configuration(
        self, monkeypatch, tmp_path
    ) -> None:
        from runner.main import EXIT_CONFIGURATION, main

        for name, value in {
            "SUBSCRIPTIONS_TABLE": "subs",
            "LEASES_TABLE": "leases",
            "WORK_QUEUE_URL": "http://localhost/queue",
            "RESULTS_BUCKET": "results",
            "WORKFLOW_DIR": str(tmp_path),
            "CREDIT_MARGIN": "0.005",
        }.items():
            monkeypatch.setenv(name, value)

        assert main([]) == EXIT_CONFIGURATION
