"""Registry record type, validation, administration, and lookup."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from leaseclient import schema
from leaseclient.registry import (
    InvalidSubscription,
    Registry,
    SubscriptionNotFound,
    SubscriptionRecord,
    UsageWrite,
    utc_now,
    validate_secret_ref,
)
from leaseclient.schema import SubscriptionState
from tests.conftest import FAKE_KEY_MATERIAL_PREFIX


def a_record(**overrides) -> SubscriptionRecord:
    defaults = {
        "subscription_id": "sub-01",
        "secret_ref": "kiro/subscription/sub-01",
        "max_concurrency": 5,
        "credit_budget": 10_000,
    }
    return SubscriptionRecord(**{**defaults, **overrides})


class TestRecordValidation:
    def test_a_valid_record_defaults_usage_to_zero(self) -> None:
        record = a_record()

        assert record.active_leases == 0
        assert record.credits_used == 0
        assert record.state is SubscriptionState.ENABLED

    @pytest.mark.parametrize("value", [0, -1, -100])
    def test_max_concurrency_below_one_is_rejected(self, value: int) -> None:
        """A subscription that can hold no leases should be disabled, not zero-capped."""
        with pytest.raises(InvalidSubscription, match="max_concurrency"):
            a_record(max_concurrency=value)

    def test_negative_credit_budget_is_rejected(self) -> None:
        with pytest.raises(InvalidSubscription, match="credit_budget"):
            a_record(credit_budget=-1)

    @pytest.mark.parametrize("field", ["credits_used", "active_leases"])
    def test_negative_usage_counters_are_rejected(self, field: str) -> None:
        with pytest.raises(InvalidSubscription, match=field):
            a_record(**{field: -1})

    @pytest.mark.parametrize("value", ["", "   "])
    def test_blank_subscription_id_is_rejected(self, value: str) -> None:
        with pytest.raises(InvalidSubscription, match="subscription_id"):
            a_record(subscription_id=value)


class TestSecretRefValidation:
    @pytest.mark.parametrize(
        "ref",
        [
            "kiro/subscription/sub-01",
            "plain-name",
            "arn:aws:secretsmanager:us-east-1:123456789012:secret:kiro/sub-01-AbCdEf",
            "arn:aws-cn:secretsmanager:cn-north-1:123456789012:secret:kiro/sub-01",
        ],
    )
    def test_accepts_names_and_secrets_manager_arns(self, ref: str) -> None:
        validate_secret_ref(ref)

    @pytest.mark.parametrize("ref", ["", "   "])
    def test_rejects_blank(self, ref: str) -> None:
        with pytest.raises(InvalidSubscription, match="non-empty"):
            validate_secret_ref(ref)

    def test_rejects_whitespace(self) -> None:
        with pytest.raises(InvalidSubscription, match="whitespace"):
            validate_secret_ref("kiro/subscription with space")

    def test_rejects_an_arn_for_a_different_service(self) -> None:
        """A parameter store or KMS ARN here is a configuration mistake, not a secret."""
        with pytest.raises(InvalidSubscription, match="Secrets Manager"):
            validate_secret_ref("arn:aws:ssm:us-east-1:123456789012:parameter/kiro/key")

    def test_rejects_a_pasted_blob(self) -> None:
        """Guards against a key being passed where a reference belongs."""
        with pytest.raises(InvalidSubscription, match="not its value"):
            validate_secret_ref("A" * 3000)

    def test_rejects_characters_a_secret_name_cannot_contain(self) -> None:
        with pytest.raises(InvalidSubscription, match="secret name or ARN"):
            validate_secret_ref("kiro/subscription/sub01!#$")


class TestSerialization:
    def test_round_trips_through_a_stored_item(self) -> None:
        original = a_record(
            credits_used=4_210,
            active_leases=3,
            window_resets_at=utc_now() + timedelta(days=7),
            state=SubscriptionState.DISABLED,
        )

        assert SubscriptionRecord.from_item(original.to_item()) == original

    def test_omits_the_reset_instant_when_unset(self) -> None:
        assert schema.WINDOW_RESETS_AT not in a_record().to_item()

    def test_numbers_are_stored_as_dynamodb_numbers(self) -> None:
        item = a_record().to_item()

        assert item[schema.MAX_CONCURRENCY] == {"N": "5"}
        assert item[schema.CREDIT_BUDGET] == {"N": "10000"}

    def test_a_malformed_item_is_rejected_with_context(self) -> None:
        with pytest.raises(InvalidSubscription, match="not a subscription"):
            SubscriptionRecord.from_item({schema.SUBSCRIPTION_ID: {"S": "sub-01"}})

    def test_an_item_violating_an_invariant_is_rejected_on_read(self) -> None:
        """Reading cannot resurrect a record that could not have been written."""
        item = a_record().to_item()
        item[schema.MAX_CONCURRENCY] = {"N": "0"}

        with pytest.raises(InvalidSubscription):
            SubscriptionRecord.from_item(item)


class TestEligibility:
    def test_allocatable_when_enabled_with_headroom(self) -> None:
        assert a_record().is_allocatable()

    def test_not_allocatable_at_the_concurrency_ceiling(self) -> None:
        assert not a_record(max_concurrency=2, active_leases=2).is_allocatable()

    def test_not_allocatable_when_disabled(self) -> None:
        assert not a_record(state=SubscriptionState.DISABLED).is_allocatable()

    def test_not_allocatable_when_the_budget_is_spent(self) -> None:
        assert not a_record(credit_budget=100, credits_used=100).is_allocatable()

    def test_the_margin_stops_allocation_before_the_budget_boundary(self) -> None:
        """Because consumption lags, the boundary itself is not a safe place to stop."""
        record = a_record(credit_budget=100, credits_used=95)

        assert record.is_allocatable(margin=0)
        assert not record.is_allocatable(margin=5)

    def test_claimable_ignores_held_concurrency_and_nothing_else(self) -> None:
        """What the scaler counts: a full subscription frees up; a spent or disabled one does
        not."""
        assert a_record(max_concurrency=2, active_leases=2).is_claimable()
        assert not a_record(state=SubscriptionState.DISABLED).is_claimable()
        assert not a_record(credit_budget=100, credits_used=95).is_claimable(margin=5)
        assert a_record(credit_budget=100, credits_used=Decimal("94.99")).is_claimable(
            margin=5
        )


class TestAdministration:
    def test_register_stores_a_retrievable_subscription(self, pool, dynamodb) -> None:
        registry = Registry(dynamodb, pool.subscriptions_table)

        registry.register("sub-01", "kiro/subscription/sub-01", 5, 10_000)

        stored = registry.get("sub-01")
        assert stored.subscription_id == "sub-01"
        assert stored.active_leases == 0
        assert stored.credits_used == 0

    def test_register_rejects_an_invalid_limit_before_writing(
        self, pool, dynamodb
    ) -> None:
        registry = Registry(dynamodb, pool.subscriptions_table)

        with pytest.raises(InvalidSubscription):
            registry.register("sub-01", "kiro/subscription/sub-01", 0, 10_000)

        with pytest.raises(SubscriptionNotFound):
            registry.get("sub-01")

    def test_disabling_leaves_usage_counters_untouched(self, pool, dynamodb) -> None:
        """Live leases must survive a subscription being taken out of rotation."""
        seeded = pool.seed_subscription(max_concurrency=5, active_leases=3)
        registry = Registry(dynamodb, pool.subscriptions_table)

        registry.set_state(seeded.subscription_id, SubscriptionState.DISABLED)

        stored = registry.get(seeded.subscription_id)
        assert stored.state is SubscriptionState.DISABLED
        assert stored.active_leases == 3

    def test_a_disabled_subscription_can_be_re_enabled(self, pool, dynamodb) -> None:
        seeded = pool.seed_subscription(state=SubscriptionState.DISABLED)
        registry = Registry(dynamodb, pool.subscriptions_table)

        registry.set_state(seeded.subscription_id, SubscriptionState.ENABLED)

        assert registry.get(seeded.subscription_id).state is SubscriptionState.ENABLED

    def test_rotating_the_reference_keeps_everything_else(self, pool, dynamodb) -> None:
        seeded = pool.seed_subscription(active_leases=2)
        registry = Registry(dynamodb, pool.subscriptions_table)

        registry.set_secret_ref(seeded.subscription_id, "kiro/subscription/rotated")

        stored = registry.get(seeded.subscription_id)
        assert stored.secret_ref == "kiro/subscription/rotated"
        assert stored.active_leases == 2

    def test_rotation_rejects_a_value_where_a_reference_belongs(
        self, pool, dynamodb
    ) -> None:
        seeded = pool.seed_subscription()
        registry = Registry(dynamodb, pool.subscriptions_table)

        with pytest.raises(InvalidSubscription):
            registry.set_secret_ref(seeded.subscription_id, f"{'K' * 3000}")

        assert registry.get(seeded.subscription_id).secret_ref == seeded.secret_ref

    @pytest.mark.parametrize("operation", ["state", "secret_ref"])
    def test_updating_an_absent_subscription_is_reported(
        self, pool, dynamodb, operation: str
    ) -> None:
        registry = Registry(dynamodb, pool.subscriptions_table)

        with pytest.raises(SubscriptionNotFound, match="sub-missing"):
            if operation == "state":
                registry.set_state("sub-missing", SubscriptionState.DISABLED)
            else:
                registry.set_secret_ref("sub-missing", "kiro/subscription/x")

    def test_describe_all_reports_every_subscription(self, pool, dynamodb) -> None:
        enabled = pool.seed_subscriptions(3)
        disabled = pool.seed_subscription(state=SubscriptionState.DISABLED)
        registry = Registry(dynamodb, pool.subscriptions_table)

        described = {r.subscription_id for r in registry.describe_all()}

        assert described == {s.subscription_id for s in enabled} | {
            disabled.subscription_id
        }

    def test_describe_all_exposes_usage_against_declared_limits(
        self, pool, dynamodb
    ) -> None:
        pool.seed_subscription("sub-01", max_concurrency=5, active_leases=4, credits_used=900, credit_budget=1_000)
        registry = Registry(dynamodb, pool.subscriptions_table)

        (record,) = registry.describe_all()

        assert (record.active_leases, record.max_concurrency) == (4, 5)
        assert (record.credits_used, record.credit_budget) == (900, 1_000)

    def test_no_administration_path_accepts_key_material(self, pool, dynamodb) -> None:
        """The protection is structural: there is no parameter for a secret value."""
        registry = Registry(dynamodb, pool.subscriptions_table)
        import inspect

        for method in (registry.register, registry.set_secret_ref, registry.put_subscription):
            parameters = inspect.signature(method).parameters
            assert not any(
                name in parameters
                for name in ("secret", "secret_string", "key", "api_key", "secret_value")
            ), f"{method.__name__} exposes a parameter that could carry key material"


class TestEnabledLookup:
    def test_returns_only_enabled_subscriptions(self, pool, dynamodb) -> None:
        enabled = pool.seed_subscriptions(3)
        pool.seed_subscription(state=SubscriptionState.DISABLED)
        registry = Registry(dynamodb, pool.subscriptions_table)

        returned = {r.subscription_id for r in registry.list_enabled()}

        assert returned == {s.subscription_id for s in enabled}

    def test_returns_full_records_without_a_follow_up_read(
        self, pool, dynamodb
    ) -> None:
        """The index projects everything, so a candidate is usable as returned."""
        seeded = pool.seed_subscription("sub-01", max_concurrency=3, credit_budget=2_000)
        registry = Registry(dynamodb, pool.subscriptions_table)

        (candidate,) = registry.list_enabled()

        assert candidate.secret_ref == seeded.secret_ref
        assert candidate.max_concurrency == 3
        assert candidate.credit_budget == 2_000

    def test_does_not_scan_the_table(self, pool, dynamodb, monkeypatch) -> None:
        """A scan would read every disabled record in order to discard it."""
        pool.seed_subscriptions(2)
        registry = Registry(dynamodb, pool.subscriptions_table)

        def fail_on_scan(*args, **kwargs):
            raise AssertionError("list_enabled must query the state index, not scan")

        monkeypatch.setattr(dynamodb, "scan", fail_on_scan)

        assert len(registry.list_enabled()) == 2

    def test_reflects_a_subscription_being_disabled(self, pool, dynamodb) -> None:
        seeded = pool.seed_subscriptions(2)
        registry = Registry(dynamodb, pool.subscriptions_table)

        registry.set_state(seeded[0].subscription_id, SubscriptionState.DISABLED)

        remaining = {r.subscription_id for r in registry.list_enabled()}
        assert remaining == {seeded[1].subscription_id}

    def test_an_empty_pool_returns_nothing_rather_than_failing(
        self, pool, dynamodb
    ) -> None:
        assert Registry(dynamodb, pool.subscriptions_table).list_enabled() == []

    def test_one_malformed_record_does_not_make_the_pool_unreadable(
        self, pool, dynamodb, caplog
    ) -> None:
        """A single bad row must not stop the whole fleet from allocating."""
        good = pool.seed_subscriptions(2)
        dynamodb.put_item(
            TableName=pool.subscriptions_table,
            Item={
                schema.SUBSCRIPTION_ID: {"S": "sub-broken"},
                schema.STATE: {"S": str(SubscriptionState.ENABLED)},
                # missing every other attribute
            },
        )
        registry = Registry(dynamodb, pool.subscriptions_table)

        returned = {r.subscription_id for r in registry.list_enabled()}

        assert returned == {s.subscription_id for s in good}
        assert "sub-broken" in caplog.text

    def test_paginates_beyond_a_single_page(self, pool, dynamodb) -> None:
        seeded = pool.seed_subscriptions(120)
        registry = Registry(dynamodb, pool.subscriptions_table)

        assert len(registry.list_enabled()) == len(seeded)


class TestNoKeyMaterialLeaks:
    def test_records_never_carry_key_material(self, pool, dynamodb) -> None:
        seeded = pool.seed_subscription()
        registry = Registry(dynamodb, pool.subscriptions_table)

        record = registry.get(seeded.subscription_id)

        assert FAKE_KEY_MATERIAL_PREFIX not in repr(record)
        assert FAKE_KEY_MATERIAL_PREFIX not in str(record.to_item())

    def test_a_validation_failure_does_not_echo_a_long_value(self) -> None:
        """An exception message must not become the leak."""
        blob = f"{FAKE_KEY_MATERIAL_PREFIX}-{'x' * 3000}"

        with pytest.raises(InvalidSubscription) as caught:
            validate_secret_ref(blob)

        assert FAKE_KEY_MATERIAL_PREFIX not in str(caught.value)


# --- credits are Decimal, end to end ------------------------------------------

NOVEMBER = datetime(2026, 11, 1, tzinfo=UTC)
NOVEMBER_RAW = NOVEMBER.isoformat()
DECEMBER = datetime(2026, 12, 1, tzinfo=UTC)
IN_OCTOBER = datetime(2026, 10, 20, 12, tzinfo=UTC)
IN_NOVEMBER = datetime(2026, 11, 20, 12, tzinfo=UTC)


class TestDecimalCredits:
    def test_a_two_place_figure_round_trips_exactly(self) -> None:
        record = a_record(credits_used=Decimal("12.97"), credit_budget=Decimal("10000.50"))

        item = record.to_item()
        assert item[schema.CREDITS_USED] == {"N": "12.97"}
        assert SubscriptionRecord.from_item(item).credits_used == Decimal("12.97")

    def test_an_integer_item_written_before_decimals_still_reads(self) -> None:
        item = a_record().to_item()
        item[schema.CREDIT_BUDGET] = {"N": "1000"}
        item[schema.CREDITS_USED] = {"N": "250"}

        record = SubscriptionRecord.from_item(item)

        assert record.credit_budget == Decimal("1000")
        assert record.credits_used == Decimal("250")
        assert isinstance(record.credits_used, Decimal)

    def test_integers_from_callers_are_coerced(self) -> None:
        assert isinstance(a_record(credits_used=5).credits_used, Decimal)

    def test_never_exponent_notation_in_a_stored_number(self) -> None:
        item = a_record(credit_budget=Decimal("1E+3")).to_item()

        assert item[schema.CREDIT_BUDGET] == {"N": "1000"}

    @pytest.mark.parametrize("value", ["NaN", "Infinity", "-0.01", "abc"])
    def test_non_finite_negative_or_non_numeric_is_rejected(self, value: str) -> None:
        with pytest.raises(InvalidSubscription, match="credits_used"):
            a_record(credits_used=value)

    def test_a_fractional_margin_decides_at_the_hundredth(self) -> None:
        assert a_record(credit_budget=10, credits_used=Decimal("9.78")).is_allocatable(
            margin=Decimal("0.21")
        )
        assert not a_record(
            credit_budget=10, credits_used=Decimal("9.79")
        ).is_allocatable(margin=Decimal("0.21"))

    def test_a_stored_decimal_reads_back_through_the_pool(self, pool, dynamodb) -> None:
        seeded = pool.seed_subscription(credits_used="12.97")

        assert Registry(dynamodb, pool.subscriptions_table).get(
            seeded.subscription_id
        ).credits_used == Decimal("12.97")


class TestRecordingAUsageReading:
    """The worker's write. Monotonic within a window; a new window is adopted."""

    @pytest.fixture
    def registry(self, pool, dynamodb) -> Registry:
        return Registry(dynamodb, pool.subscriptions_table)

    def test_a_higher_reading_raises(self, pool, registry) -> None:
        seeded = pool.seed_subscription(credits_used="10", window_resets_raw=NOVEMBER_RAW)

        result = registry.record_usage(
            seeded.subscription_id, Decimal("12.97"), NOVEMBER, now=IN_OCTOBER
        )

        assert result is UsageWrite.RAISED
        assert pool.credits_used(seeded.subscription_id) == Decimal("12.97")

    def test_a_lower_reading_never_lowers(self, pool, registry) -> None:
        seeded = pool.seed_subscription(credits_used="13.10", window_resets_raw=NOVEMBER_RAW)

        result = registry.record_usage(
            seeded.subscription_id, Decimal("13.08"), NOVEMBER, now=IN_OCTOBER
        )

        assert result is UsageWrite.UNCHANGED
        assert pool.credits_used(seeded.subscription_id) == Decimal("13.10")

    def test_an_equal_reading_is_unchanged(self, pool, registry) -> None:
        seeded = pool.seed_subscription(credits_used="13.10", window_resets_raw=NOVEMBER_RAW)

        assert (
            registry.record_usage(
                seeded.subscription_id, Decimal("13.10"), NOVEMBER, now=IN_OCTOBER
            )
            is UsageWrite.UNCHANGED
        )

    def test_out_of_order_readings_end_at_the_largest(self, pool, registry) -> None:
        seeded = pool.seed_subscription(credits_used="0", window_resets_raw=NOVEMBER_RAW)

        for value in ("13.08", "12.97", "13.20", "13.10", "12.00"):
            registry.record_usage(
                seeded.subscription_id, Decimal(value), NOVEMBER, now=IN_OCTOBER
            )

        assert pool.credits_used(seeded.subscription_id) == Decimal("13.20")

    def test_a_reading_from_an_ended_window_is_dropped(self, pool, registry) -> None:
        """Last month's total written into this month would block the subscription."""
        seeded = pool.seed_subscription(credits_used="0.40", window_resets_raw=DECEMBER.isoformat())

        result = registry.record_usage(
            seeded.subscription_id, Decimal("9000"), NOVEMBER, now=IN_NOVEMBER
        )

        assert result is UsageWrite.STALE
        assert pool.credits_used(seeded.subscription_id) == Decimal("0.40")

    def test_a_new_window_is_adopted_and_may_lower(self, pool, registry) -> None:
        # The server says the window ends 1 December; the stored one is November.
        seeded = pool.seed_subscription(credits_used="9000", window_resets_raw=NOVEMBER_RAW)

        result = registry.record_usage(
            seeded.subscription_id, Decimal("0.40"), DECEMBER, now=IN_NOVEMBER
        )

        assert result is UsageWrite.WINDOW_ADOPTED
        assert pool.credits_used(seeded.subscription_id) == Decimal("0.40")
        assert pool.window_resets_raw(seeded.subscription_id) == DECEMBER.isoformat()

    def test_a_missing_window_is_adopted(self, pool, registry) -> None:
        """The README seed item has no window; its first reading gives it one."""
        seeded = pool.seed_subscription(credits_used="0", with_window=False)

        result = registry.record_usage(
            seeded.subscription_id, Decimal("3.50"), NOVEMBER, now=IN_OCTOBER
        )

        assert result is UsageWrite.WINDOW_ADOPTED
        assert pool.window_resets_raw(seeded.subscription_id) == NOVEMBER_RAW

    def test_the_same_instant_spelled_differently_is_still_raise_only(
        self, pool, registry
    ) -> None:
        seeded = pool.seed_subscription(
            credits_used="20", window_resets_raw="2026-11-01T00:00:00Z"
        )

        lower = registry.record_usage(
            seeded.subscription_id, Decimal("5"), NOVEMBER, now=IN_OCTOBER
        )
        higher = registry.record_usage(
            seeded.subscription_id, Decimal("25"), NOVEMBER, now=IN_OCTOBER
        )

        assert lower is UsageWrite.UNCHANGED
        assert higher is UsageWrite.RAISED
        assert pool.credits_used(seeded.subscription_id) == Decimal("25")
        assert pool.window_resets_raw(seeded.subscription_id) == NOVEMBER_RAW

    def test_a_missing_subscription_is_reported(self, pool, registry) -> None:
        with pytest.raises(SubscriptionNotFound):
            registry.record_usage("sub-missing", Decimal("1"), NOVEMBER, now=IN_OCTOBER)

    def test_a_negative_reading_is_rejected(self, pool, registry) -> None:
        seeded = pool.seed_subscription(window_resets_raw=NOVEMBER_RAW)

        with pytest.raises(InvalidSubscription):
            registry.record_usage(seeded.subscription_id, Decimal("-1"), NOVEMBER, now=IN_OCTOBER)


class TestRaisingFromTheReport:
    @pytest.fixture
    def registry(self, pool, dynamodb) -> Registry:
        return Registry(dynamodb, pool.subscriptions_table)

    def test_a_higher_figure_raises(self, pool, registry) -> None:
        seeded = pool.seed_subscription(credits_used="10", window_resets_raw=NOVEMBER_RAW)
        record = registry.get(seeded.subscription_id)

        assert registry.raise_credits_from_report(record, Decimal("42.50")) is True
        assert pool.credits_used(seeded.subscription_id) == Decimal("42.50")

    def test_it_never_lowers(self, pool, registry) -> None:
        seeded = pool.seed_subscription(credits_used="50", window_resets_raw=NOVEMBER_RAW)
        record = registry.get(seeded.subscription_id)

        assert registry.raise_credits_from_report(record, Decimal("42.50")) is False
        assert pool.credits_used(seeded.subscription_id) == Decimal("50")

    def test_it_refuses_a_window_that_moved_since_the_scan(self, pool, registry) -> None:
        seeded = pool.seed_subscription(credits_used="0", window_resets_raw=NOVEMBER_RAW)
        record = registry.get(seeded.subscription_id)
        registry.reset_window(seeded.subscription_id, DECEMBER)

        assert registry.raise_credits_from_report(record, Decimal("900")) is False
        assert pool.credits_used(seeded.subscription_id) == Decimal("0")


class TestTheReportMapping:
    def test_the_user_id_is_stored_and_read(self, pool, dynamodb) -> None:
        seeded = pool.seed_subscription()
        registry = Registry(dynamodb, pool.subscriptions_table)

        registry.set_report_user_id(seeded.subscription_id, "user-0123")

        assert registry.get(seeded.subscription_id).report_user_id == "user-0123"

    @pytest.mark.parametrize("value", ["", "has space", "x" * 129])
    def test_a_user_id_that_is_not_an_identifier_is_rejected(
        self, pool, dynamodb, value: str
    ) -> None:
        seeded = pool.seed_subscription()
        registry = Registry(dynamodb, pool.subscriptions_table)

        with pytest.raises(InvalidSubscription, match="report_user_id"):
            registry.set_report_user_id(seeded.subscription_id, value)

    def test_a_missing_subscription_is_reported(self, pool, dynamodb) -> None:
        with pytest.raises(SubscriptionNotFound):
            Registry(dynamodb, pool.subscriptions_table).set_report_user_id("nope", "u")
