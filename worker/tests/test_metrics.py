"""Metric emission.

Two things are worth asserting: the records are valid Embedded Metric Format, so
CloudWatch actually extracts them rather than storing unparsed log lines, and the
emission path stays off the network, because it runs at the start of every task in
the fleet.
"""

from __future__ import annotations

import io
import json

import pytest

from leaseclient.metrics import (
    DEFAULT_NAMESPACE,
    ENV_NAMESPACE,
    MAX_FIELD_LENGTH,
    ClaimOutcome,
    Metrics,
    MetricUnit,
    RecoveryMechanism,
    UnpublishableValue,
    assert_publishable,
)
from tests.conftest import FAKE_KEY_MATERIAL_PREFIX


@pytest.fixture
def sink() -> io.StringIO:
    return io.StringIO()


@pytest.fixture
def metrics(sink: io.StringIO) -> Metrics:
    return Metrics(stream=sink)


def records(sink: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in sink.getvalue().splitlines() if line.strip()]


def only(sink: io.StringIO) -> dict:
    (record,) = records(sink)
    return record


def metadata(record: dict) -> dict:
    (definition,) = record["_aws"]["CloudWatchMetrics"]
    return definition


class TestEmbeddedFormat:
    def test_a_record_carries_the_aws_metadata_block(self, metrics, sink) -> None:
        metrics.claim_candidates_examined(3)

        record = only(sink)
        assert "_aws" in record
        assert isinstance(record["_aws"]["Timestamp"], int)

    def test_the_metric_value_sits_at_the_top_level(self, metrics, sink) -> None:
        """CloudWatch reads the value from the property named by the metric."""
        metrics.claim_candidates_examined(7)

        record = only(sink)
        assert record["ClaimCandidatesExamined"] == 7
        assert metadata(record)["Metrics"] == [
            {"Name": "ClaimCandidatesExamined", "Unit": "Count"}
        ]

    def test_dimensions_appear_both_as_names_and_as_properties(
        self, metrics, sink
    ) -> None:
        """A dimension not present as a property is dropped silently by CloudWatch."""
        metrics.claim_outcome(ClaimOutcome.SUCCESS)

        record = only(sink)
        assert metadata(record)["Dimensions"] == [["Outcome"]]
        assert record["Outcome"] == "Success"

    def test_a_metric_without_dimensions_declares_an_empty_set(
        self, metrics, sink
    ) -> None:
        metrics.claim_latency(12.5)

        assert metadata(only(sink))["Dimensions"] == [[]]

    def test_the_namespace_defaults_and_is_overridable(self, sink, monkeypatch) -> None:
        assert Metrics(stream=sink).namespace == DEFAULT_NAMESPACE

        monkeypatch.setenv(ENV_NAMESPACE, "Custom/Namespace")
        assert Metrics(stream=sink).namespace == "Custom/Namespace"

    def test_records_are_newline_delimited(self, metrics, sink) -> None:
        """One JSON object per line, or the log driver forwards an unparseable blob."""
        metrics.claim_candidates_examined(1)
        metrics.claim_latency(2.0)

        assert len(records(sink)) == 2


class TestTheMetricsPublished:
    @pytest.mark.parametrize("outcome", list(ClaimOutcome))
    def test_every_claim_outcome_is_emittable(self, metrics, sink, outcome) -> None:
        """Saturation must be separable from contention in the aggregate."""
        metrics.claim_outcome(outcome)

        assert only(sink)["Outcome"] == str(outcome)

    @pytest.mark.parametrize("mechanism", list(RecoveryMechanism))
    def test_recovery_is_attributed_to_a_mechanism(
        self, metrics, sink, mechanism
    ) -> None:
        metrics.leases_recovered(2, mechanism)

        record = only(sink)
        assert record["LeasesRecovered"] == 2
        assert record["Mechanism"] == str(mechanism)

    def test_utilization_is_a_percentage_of_declared_capacity(
        self, metrics, sink
    ) -> None:
        metrics.subscription_utilization("sub-01", active_leases=3, max_concurrency=4)

        record = only(sink)
        assert record["SubscriptionUtilization"] == 75.0
        assert metadata(record)["Metrics"][0]["Unit"] == str(MetricUnit.PERCENT)
        assert record["SubscriptionId"] == "sub-01"

    def test_utilization_is_skipped_rather_than_dividing_by_zero(
        self, metrics, sink
    ) -> None:
        metrics.subscription_utilization("sub-01", active_leases=0, max_concurrency=0)

        assert records(sink) == []

    def test_only_utilization_is_dimensioned_by_subscription(
        self, metrics, sink
    ) -> None:
        """Per-subscription dimensions on claim outcomes would scale cost with pool size."""
        metrics.claim_outcome(ClaimOutcome.SUCCESS)
        metrics.claim_candidates_examined(2)
        metrics.claim_latency(1.0)
        metrics.leases_recovered(1, RecoveryMechanism.EXPIRY)

        for record in records(sink):
            assert "SubscriptionId" not in record

    def test_task_protection_failure_is_one_series_with_its_cause_as_properties(
        self, metrics, sink
    ) -> None:
        """One alarm should catch every cause, so neither field is a dimension."""
        from leaseclient.metrics import ProtectionAction, ProtectionFailure

        metrics.task_protection_failed(ProtectionAction.ENABLE, ProtectionFailure.UNREACHABLE)

        record = only(sink)
        assert record["TaskProtectionFailed"] == 1
        assert record["Action"] == "Enable"
        assert record["Reason"] == "Unreachable"
        assert metadata(record)["Dimensions"] == [[]]
        assert metadata(record)["Metrics"][0]["Unit"] == str(MetricUnit.COUNT)
        assert "SubscriptionId" not in record


class TestTimedClaim:
    def test_latency_is_emitted_on_success(self, metrics, sink) -> None:
        with metrics.timed_claim():
            pass

        assert "ClaimLatency" in only(sink)

    def test_latency_is_emitted_even_when_the_claim_raises(
        self, metrics, sink
    ) -> None:
        """A slow failure is the measurement that matters under pressure."""
        with pytest.raises(RuntimeError):
            with metrics.timed_claim():
                raise RuntimeError("boom")

        assert "ClaimLatency" in only(sink)

    def test_latency_is_a_non_negative_number_of_milliseconds(
        self, metrics, sink
    ) -> None:
        with metrics.timed_claim():
            pass

        record = only(sink)
        assert record["ClaimLatency"] >= 0
        assert metadata(record)["Metrics"][0]["Unit"] == str(MetricUnit.MILLISECONDS)


class TestRefusingUnsafeValues:
    def test_numbers_are_always_publishable(self) -> None:
        assert_publishable("x", 1)
        assert_publishable("x", 1.5)

    @pytest.mark.parametrize("value", ["sub-01", "Success", "KiroFleet/Pool", "a.b:c"])
    def test_identifiers_and_enum_names_are_publishable(self, value: str) -> None:
        assert_publishable("x", value)

    def test_a_long_value_is_refused(self) -> None:
        with pytest.raises(UnpublishableValue, match="over the"):
            assert_publishable("x", "a" * (MAX_FIELD_LENGTH + 1))

    def test_a_value_with_unexpected_characters_is_refused(self) -> None:
        with pytest.raises(UnpublishableValue, match="should not carry"):
            assert_publishable("x", "has spaces and !@#")

    def test_key_material_shaped_values_do_not_reach_the_stream(
        self, metrics, sink
    ) -> None:
        """The bound on field length is what makes this hold."""
        with pytest.raises(UnpublishableValue):
            metrics.emit(
                "ClaimOutcome",
                1,
                MetricUnit.COUNT,
                {"Outcome": f"{FAKE_KEY_MATERIAL_PREFIX}-{'x' * 200}"},
            )

        assert FAKE_KEY_MATERIAL_PREFIX not in sink.getvalue()

    def test_the_refusal_message_does_not_echo_the_value(self) -> None:
        blob = f"{FAKE_KEY_MATERIAL_PREFIX}-{'x' * 200}"

        with pytest.raises(UnpublishableValue) as caught:
            assert_publishable("Outcome", blob)

        assert FAKE_KEY_MATERIAL_PREFIX not in str(caught.value)

    def test_a_non_string_non_number_is_refused(self) -> None:
        with pytest.raises(UnpublishableValue, match="string or a number"):
            assert_publishable("x", {"nested": "object"})


class TestEmissionIsBestEffort:
    def test_a_broken_stream_does_not_break_the_run(self, caplog) -> None:
        """A metric must never be the reason a task fails."""

        class Broken(io.StringIO):
            def write(self, _: str) -> int:
                raise OSError("stream closed")

        Metrics(stream=Broken()).claim_outcome(ClaimOutcome.SUCCESS)

        assert "failed to emit metric" in caplog.text

    def test_emission_makes_no_network_call(self, metrics, sink, monkeypatch) -> None:
        """Emission runs at the start of every task; a synchronous call here is paid
        hundreds of times per batch and fails when the fleet is largest."""
        import socket

        def fail(*args, **kwargs):
            raise AssertionError("metric emission must not open a socket")

        monkeypatch.setattr(socket.socket, "connect", fail)
        monkeypatch.setattr(socket, "create_connection", fail)

        metrics.claim_outcome(ClaimOutcome.SUCCESS)
        metrics.subscription_utilization("sub-01", 1, 5)

        assert len(records(sink)) == 2


class TestCreditMetrics:
    """Decimal in, number out, and dimensions that stay bounded."""

    def test_a_reading_is_dimensioned_by_outcome_only(self, metrics, sink) -> None:
        from leaseclient.metrics import UsagePhase, UsageReadingOutcome

        metrics.usage_reading(UsageReadingOutcome.READ_FAILED, "sub-01", UsagePhase.PRE_ITEM)

        (record,) = records(sink)
        assert record["UsageReading"] == 1
        assert record["Outcome"] == "ReadFailed"
        assert record["Phase"] == "PreItem"
        assert record["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [["Outcome"]]

    def test_decimal_figures_are_emitted_as_numbers(self, metrics, sink) -> None:
        from decimal import Decimal

        from leaseclient.metrics import CreditSource

        metrics.subscription_credits_used("sub-01", Decimal("12.97"), CreditSource.USAGE)
        metrics.subscription_credit_budget("sub-01", Decimal("10000"))
        metrics.run_credits("sub-01", "item-0", Decimal("0.05"))

        used, budget, run = records(sink)
        assert used["SubscriptionCreditsUsed"] == 12.97
        assert used["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [["SubscriptionId"]]
        assert used["_aws"]["CloudWatchMetrics"][0]["Metrics"][0]["Unit"] == "None"
        assert budget["SubscriptionCreditBudget"] == 10000.0
        assert run["RunCredits"] == 0.05
        # Per run, so not dimensioned by subscription or item.
        assert run["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [[]]

    def test_a_missing_run_figure_is_counted_by_verdict(self, metrics, sink) -> None:
        """As `RunOutcome` is, so a dashboard can keep the successful runs apart from the
        timeouts and crashes that never print a summary."""
        metrics.run_credits_missing("sub-01", "item-0", "Succeeded")
        metrics.run_credits_missing("sub-01", "item-1", "TimedOut")

        succeeded, timed_out = records(sink)
        assert succeeded["RunCreditsMissing"] == 1
        assert succeeded["Verdict"] == "Succeeded"
        assert timed_out["Verdict"] == "TimedOut"
        assert succeeded["SubscriptionId"] == "sub-01"
        assert succeeded["ItemId"] == "item-0"
        # The verdict is the only dimension; subscription and item stay properties.
        assert succeeded["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [["Verdict"]]
        assert succeeded["_aws"]["CloudWatchMetrics"][0]["Metrics"][0]["Unit"] == "Count"

    def test_a_budget_stop_is_one_series_for_the_whole_pool(self, metrics, sink) -> None:
        metrics.budget_exhausted("sub-01")

        (record,) = records(sink)
        assert record["BudgetExhausted"] == 1
        assert record["SubscriptionId"] == "sub-01"
        assert record["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [[]]
