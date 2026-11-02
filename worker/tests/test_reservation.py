"""The atomic reservation primitive.

This is the operation the whole design rests on: a single conditional update that
both checks headroom and consumes it, so that correctness under contention needs
no coordination between workers. The tests that matter here are the ones that
contend.
"""

from __future__ import annotations

import pytest

from leaseclient import schema
from leaseclient.registry import RejectionReason, Registry, SubscriptionRecord
from leaseclient.schema import SubscriptionState


@pytest.fixture
def registry(pool, dynamodb) -> Registry:
    return Registry(dynamodb, pool.subscriptions_table)


def reserve(registry: Registry, pool, seeded, margin: int = 0):
    """Reserve the way the claim path does: from a record already read.

    The budget ceiling is resolved by the caller because a DynamoDB condition
    expression cannot do arithmetic.
    """
    record = registry.get(seeded.subscription_id)
    return registry.try_reserve_record(record, margin)


class TestGrant:
    def test_a_reservation_consumes_one_unit_of_concurrency(
        self, pool, registry
    ) -> None:
        seeded = pool.seed_subscription(max_concurrency=5)

        outcome = reserve(registry, pool, seeded)

        assert outcome.granted
        assert outcome.reason is None
        assert pool.active_leases(seeded.subscription_id) == 1

    def test_the_outcome_is_truthy_when_granted(self, pool, registry) -> None:
        """Callers loop over candidates; the outcome reads as a condition."""
        seeded = pool.seed_subscription()

        assert reserve(registry, pool, seeded)

    def test_reservations_accumulate_up_to_the_ceiling(self, pool, registry) -> None:
        seeded = pool.seed_subscription(max_concurrency=3)

        granted = [reserve(registry, pool, seeded) for _ in range(3)]

        assert all(granted)
        assert pool.active_leases(seeded.subscription_id) == 3

    def test_reserving_touches_no_other_attribute(self, pool, registry) -> None:
        seeded = pool.seed_subscription(credits_used=100, credit_budget=1_000)
        before = pool.subscription(seeded.subscription_id)

        reserve(registry, pool, seeded)

        after = pool.subscription(seeded.subscription_id)
        assert after[schema.CREDITS_USED] == before[schema.CREDITS_USED]
        assert after[schema.SECRET_REF] == before[schema.SECRET_REF]


class TestRejection:
    def test_rejected_at_the_concurrency_ceiling(self, pool, registry) -> None:
        seeded = pool.seed_subscription(max_concurrency=2, active_leases=2)

        outcome = reserve(registry, pool, seeded)

        assert not outcome.granted
        assert outcome.reason is RejectionReason.NO_CONCURRENCY
        assert pool.active_leases(seeded.subscription_id) == 2

    def test_rejected_when_the_budget_is_spent(self, pool, registry) -> None:
        seeded = pool.seed_subscription(credit_budget=1_000, credits_used=1_000)

        outcome = reserve(registry, pool, seeded)

        assert outcome.reason is RejectionReason.BUDGET_EXHAUSTED

    def test_rejected_inside_the_safety_margin(self, pool, registry) -> None:
        """Consumption lags, so the budget boundary is not a safe place to stop."""
        seeded = pool.seed_subscription(credit_budget=1_000, credits_used=995)

        assert reserve(registry, pool, seeded, margin=0).granted
        assert (
            reserve(registry, pool, seeded, margin=100).reason
            is RejectionReason.BUDGET_EXHAUSTED
        )

    def test_rejected_when_disabled(self, pool, registry) -> None:
        seeded = pool.seed_subscription(state=SubscriptionState.DISABLED)

        outcome = reserve(registry, pool, seeded)

        assert outcome.reason is RejectionReason.DISABLED

    def test_disabled_is_reported_even_when_also_at_the_ceiling(
        self, pool, registry
    ) -> None:
        """An operator acts on the administrative state, not the incidental one."""
        seeded = pool.seed_subscription(
            max_concurrency=1, active_leases=1, state=SubscriptionState.DISABLED
        )

        assert reserve(registry, pool, seeded).reason is (
            RejectionReason.DISABLED
        )

    def test_rejected_when_the_subscription_is_absent(self, registry) -> None:
        outcome = registry.try_reserve("sub-never-registered", credit_ceiling=10_000)

        assert not outcome.granted
        assert outcome.reason is RejectionReason.ABSENT

    def test_rejection_is_not_an_exception(self, pool, registry) -> None:
        """Once the pool is busy, rejection is the common case, not a failure."""
        seeded = pool.seed_subscription(max_concurrency=1, active_leases=1)

        outcome = reserve(registry, pool, seeded)

        assert outcome.granted is False

    def test_a_malformed_record_does_not_crash_the_reason_derivation(
        self, pool, registry, dynamodb
    ) -> None:
        dynamodb.put_item(
            TableName=pool.subscriptions_table,
            Item={
                schema.SUBSCRIPTION_ID: {"S": "sub-broken"},
                schema.STATE: {"S": str(SubscriptionState.DISABLED)},
            },
        )

        outcome = registry.try_reserve("sub-broken", credit_ceiling=10_000)

        assert not outcome.granted
        assert outcome.reason is RejectionReason.UNKNOWN


class TestRelease:
    def test_release_returns_one_unit(self, pool, registry) -> None:
        seeded = pool.seed_subscription(max_concurrency=5, active_leases=3)

        assert registry.release_reservation(seeded.subscription_id) is True
        assert pool.active_leases(seeded.subscription_id) == 2

    def test_release_frees_capacity_for_the_next_caller(self, pool, registry) -> None:
        seeded = pool.seed_subscription(max_concurrency=1)
        reserve(registry, pool, seeded)
        assert not reserve(registry, pool, seeded).granted

        registry.release_reservation(seeded.subscription_id)

        assert reserve(registry, pool, seeded).granted

    def test_a_duplicated_release_cannot_drive_the_count_negative(
        self, pool, registry
    ) -> None:
        seeded = pool.seed_subscription(max_concurrency=5, active_leases=1)

        assert registry.release_reservation(seeded.subscription_id) is True
        assert registry.release_reservation(seeded.subscription_id) is False
        assert pool.active_leases(seeded.subscription_id) == 0

    def test_releasing_an_unheld_subscription_is_reported_not_raised(
        self, pool, registry
    ) -> None:
        """Expiry may already have reclaimed it; the caller's intent is satisfied."""
        seeded = pool.seed_subscription(active_leases=0)

        assert registry.release_reservation(seeded.subscription_id) is False

    def test_releasing_an_absent_subscription_is_reported_not_raised(
        self, registry
    ) -> None:
        assert registry.release_reservation("sub-never-registered") is False

    def test_release_works_on_a_disabled_subscription(self, pool, registry) -> None:
        """Capacity held on a subscription taken out of rotation must still come back."""
        seeded = pool.seed_subscription(active_leases=2)
        registry.set_state(seeded.subscription_id, SubscriptionState.DISABLED)

        assert registry.release_reservation(seeded.subscription_id) is True
        assert pool.active_leases(seeded.subscription_id) == 1


class TestReserveReleaseCycle:
    def test_the_counter_returns_to_zero_over_repeated_cycles(
        self, pool, registry
    ) -> None:
        seeded = pool.seed_subscription(max_concurrency=2)

        for _ in range(20):
            assert reserve(registry, pool, seeded).granted
            assert registry.release_reservation(seeded.subscription_id)

        assert pool.active_leases(seeded.subscription_id) == 0

    def test_the_ceiling_holds_across_interleaved_reserves_and_releases(
        self, pool, registry
    ) -> None:
        seeded = pool.seed_subscription(max_concurrency=3)
        held = 0

        for step in range(30):
            if step % 3 == 2 and held:
                registry.release_reservation(seeded.subscription_id)
                held -= 1
            elif reserve(registry, pool, seeded).granted:
                held += 1
            assert pool.active_leases(seeded.subscription_id) == held
            assert held <= 3


class TestWhyTheCeilingIsResolvedByTheCaller:
    """Pins the platform constraint that gives try_reserve its shape.

    Folding the safety margin into the condition, as `creditsUsed < creditBudget -
    :margin`, reads better and is not valid: DynamoDB allows arithmetic in an
    update expression but not in a condition. Without this test the simplification
    looks safe and would fail only against a real table.
    """

    def test_arithmetic_in_a_condition_expression_is_rejected(
        self, pool, dynamodb
    ) -> None:
        seeded = pool.seed_subscription(credit_budget=1_000, credits_used=0)

        with pytest.raises(Exception) as caught:
            dynamodb.update_item(
                TableName=pool.subscriptions_table,
                Key={schema.SUBSCRIPTION_ID: {"S": seeded.subscription_id}},
                UpdateExpression=(
                    f"SET {schema.ACTIVE_LEASES} = {schema.ACTIVE_LEASES} + :one"
                ),
                ConditionExpression=(
                    f"{schema.CREDITS_USED} < {schema.CREDIT_BUDGET} - :margin"
                ),
                ExpressionAttributeValues={
                    ":one": {"N": "1"},
                    ":margin": {"N": "100"},
                },
            )

        assert "ConditionalCheckFailed" not in type(caught.value).__name__, (
            "the expression must be rejected as malformed, not evaluated"
        )

    def test_the_supported_form_compares_against_a_resolved_value(
        self, pool, registry
    ) -> None:
        """Which is why the caller subtracts the margin before calling."""
        seeded = pool.seed_subscription(credit_budget=1_000, credits_used=950)
        record = registry.get(seeded.subscription_id)

        assert record.credit_ceiling(0) == 1_000
        assert record.credit_ceiling(100) == 900
        assert registry.try_reserve(
            seeded.subscription_id, credit_ceiling=record.credit_ceiling(0)
        ).granted
        assert (
            registry.try_reserve(
                seeded.subscription_id, credit_ceiling=record.credit_ceiling(100)
            ).reason
            is RejectionReason.BUDGET_EXHAUSTED
        )


class TestReserveFromARecord:
    def test_resolves_the_ceiling_from_the_record(self, pool, registry) -> None:
        seeded = pool.seed_subscription(credit_budget=1_000, credits_used=980)
        record = registry.get(seeded.subscription_id)

        assert registry.try_reserve_record(record, margin=0).granted
        assert (
            registry.try_reserve_record(record, margin=50).reason
            is RejectionReason.BUDGET_EXHAUSTED
        )

    def test_a_stale_budget_is_bounded_by_the_margin(self, pool, registry) -> None:
        """An operator changing the budget mid-flight costs one generation, not correctness."""
        seeded = pool.seed_subscription(credit_budget=1_000, credits_used=0)
        stale = registry.get(seeded.subscription_id)

        # Operator lowers the budget after the candidate was read.
        pool.seed_subscription(
            seeded.subscription_id, credit_budget=100, credits_used=0, with_secret=False
        )

        # The stale ceiling is used, so this attempt still succeeds.
        assert registry.try_reserve_record(stale, margin=0).granted

        # The next read sees the new budget.
        assert registry.get(seeded.subscription_id).credit_budget == 100
