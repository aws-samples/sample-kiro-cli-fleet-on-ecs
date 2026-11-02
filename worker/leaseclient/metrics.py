"""Metric emission for the allocation path.

Written to stdout in CloudWatch Embedded Metric Format. The task's log driver
forwards it and CloudWatch extracts the metrics, so nothing on the claim path makes
an API call. That matters because the claim path runs at the start of every task in
the fleet: an extra synchronous call there would be paid hundreds of times per
batch, and it would fail exactly when the fleet is largest.

Emission is best effort. A metric that cannot be written must never break a run
that is otherwise fine, so failures here are swallowed after being logged. The one
exception is a value that must not be published, which raises: silently emitting
something that looks like a credential is worse than failing loudly in a test.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from contextlib import contextmanager
from decimal import Decimal
from enum import StrEnum
from typing import Any, Final

logger = logging.getLogger(__name__)

ENV_NAMESPACE: Final = "METRICS_NAMESPACE"
DEFAULT_NAMESPACE: Final = "KiroFleet/SubscriptionPool"

MAX_FIELD_LENGTH: Final = 128
"""No metric dimension or property legitimately carries a long value.

A bound here is the practical guard against key material reaching a log stream:
identifiers and enum names are short, credentials are not.
"""

_PUBLISHABLE = re.compile(r"^[A-Za-z0-9_.:/-]{1,128}$")


class MetricUnit(StrEnum):
    COUNT = "Count"
    MILLISECONDS = "Milliseconds"
    PERCENT = "Percent"
    NONE = "None"


class UsageReadingOutcome(StrEnum):
    """What one `/usage` reading achieved. `ReadFailed` is the one to watch."""

    RECORDED = "Recorded"
    UNCHANGED = "Unchanged"
    WINDOW_ADOPTED = "WindowAdopted"
    STALE = "Stale"
    READ_FAILED = "ReadFailed"
    WRITE_FAILED = "WriteFailed"


class UsagePhase(StrEnum):
    PRE_ITEM = "PreItem"
    DRAIN_END = "DrainEnd"


class CreditSource(StrEnum):
    USAGE = "Usage"
    REPORT = "Report"


class ClaimOutcome(StrEnum):
    """Distinguishes why a claim ended, so saturation is separable from contention."""

    SUCCESS = "Success"
    NO_CONCURRENCY = "NoConcurrency"
    BUDGET_EXHAUSTED = "BudgetExhausted"
    SATURATED = "Saturated"


class RecoveryMechanism(StrEnum):
    EXPIRY = "Expiry"
    DRIFT_CORRECTION = "DriftCorrection"


class ProtectionAction(StrEnum):
    ENABLE = "Enable"
    DISABLE = "Disable"


class ProtectionFailure(StrEnum):
    """Why a scale-in protection request did not take effect."""

    UNREACHABLE = "Unreachable"
    HTTP_ERROR = "HttpError"
    REFUSED = "Refused"
    DEPLOYMENT_BLOCKED = "DeploymentBlocked"
    TASK_STOPPING = "TaskStopping"
    """ECS is already stopping this task, so the worker takes no further item."""
    BAD_RESPONSE = "BadResponse"


class UnpublishableValue(RuntimeError):
    """A value was rejected before it could reach a log stream."""


def assert_publishable(name: str, value: Any) -> None:
    """Refuse anything that does not look like an identifier or an enum name.

    This is a shape and length check, not a credential detector. It cannot know what
    a key looks like, but it can insist that metric fields stay in the small,
    boring space they are supposed to occupy, which is where a leaked credential
    would stand out.
    """
    if isinstance(value, bool) or isinstance(value, (int, float)):
        return
    if not isinstance(value, str):
        raise UnpublishableValue(
            f"{name} must be a string or a number, got {type(value).__name__}"
        )
    if len(value) > MAX_FIELD_LENGTH:
        raise UnpublishableValue(
            f"{name} is {len(value)} characters, over the {MAX_FIELD_LENGTH} limit. "
            "Metric fields carry identifiers, never payloads or credentials."
        )
    if not _PUBLISHABLE.match(value):
        raise UnpublishableValue(
            f"{name} contains characters a metric field should not carry"
        )


class Metrics:
    """Emits Embedded Metric Format records to a stream, stdout by default."""

    def __init__(self, namespace: str | None = None, stream=None) -> None:
        self._namespace = namespace or os.environ.get(ENV_NAMESPACE, DEFAULT_NAMESPACE)
        self._stream = stream if stream is not None else sys.stdout

    @property
    def namespace(self) -> str:
        return self._namespace

    def emit(
        self,
        metric: str,
        value: int | float,
        unit: MetricUnit,
        dimensions: dict[str, str] | None = None,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Write one metric. Never raises for I/O reasons; raises for unsafe values."""
        dimensions = dimensions or {}
        properties = properties or {}

        assert_publishable(metric, metric)
        for key, field in (*dimensions.items(), *properties.items()):
            assert_publishable(key, key)
            assert_publishable(key, field)

        record: dict[str, Any] = {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": self._namespace,
                        # A single dimension set: every dimension present applies to
                        # this metric. Multiple sets would multiply custom metric
                        # cost for no diagnostic gain here.
                        "Dimensions": [list(dimensions)] if dimensions else [[]],
                        "Metrics": [{"Name": metric, "Unit": str(unit)}],
                    }
                ],
            },
            metric: value,
            **dimensions,
            **properties,
        }

        try:
            self._stream.write(json.dumps(record) + "\n")
            self._stream.flush()
        except Exception:
            # A metric must not break a run that is otherwise fine.
            logger.warning("failed to emit metric %s", metric, exc_info=True)

    # --- the metrics this component publishes --------------------------------

    def claim_outcome(self, outcome: ClaimOutcome) -> None:
        self.emit("ClaimOutcome", 1, MetricUnit.COUNT, {"Outcome": str(outcome)})

    def claim_candidates_examined(self, count: int) -> None:
        """How many subscriptions were tried before one was reserved.

        The direct measure of contention: as the pool fills, this rises before
        throughput falls, so it leads the symptom rather than trailing it.
        """
        self.emit("ClaimCandidatesExamined", count, MetricUnit.COUNT)

    def claim_latency(self, milliseconds: float) -> None:
        self.emit("ClaimLatency", round(milliseconds, 3), MetricUnit.MILLISECONDS)

    def run_outcome(self, verdict: str, item_id: str) -> None:
        """How one workflow run ended, as a metric rather than only a log line.

        The distribution is the thing worth having: a batch that is 2% `NoOutput` is a
        workflow that needs its prompt looked at, and a batch that is 2% `Throttled` is a
        pool that needs another subscription. Both look identical in a success count, and
        both are invisible in prose logs across several hundred runs.

        Dimensioned by verdict and not by item, so the cardinality is the eight values the
        enum defines. The item travels as a property, so one run is still findable in the
        logs when its verdict is the interesting one.
        """
        self.emit(
            "RunOutcome",
            1,
            MetricUnit.COUNT,
            {"Verdict": verdict},
            {"ItemId": item_id},
        )

    def subscription_disabled(self, subscription_id: str, reason: str) -> None:
        """A subscription was taken out of rotation for the whole fleet, by a worker.

        The one event here that needs a person. Everything else this class publishes is a
        rate or a distribution to look at later; this says the pool just shrank and will not
        grow back on its own, because nothing re-enables a subscription automatically.

        It is deliberately not dimensioned by subscription: an alarm on this metric should
        fire whichever one it was, and a per-subscription dimension would mean an alarm per
        subscription or an alarm that misses the one you did not create. The identifier
        travels as a property, so a log query still answers "which one".
        """
        self.emit(
            "SubscriptionDisabled",
            1,
            MetricUnit.COUNT,
            properties={"SubscriptionId": subscription_id, "Reason": reason},
        )

    def leases_recovered(self, count: int, mechanism: RecoveryMechanism) -> None:
        self.emit(
            "LeasesRecovered",
            count,
            MetricUnit.COUNT,
            {"Mechanism": str(mechanism)},
        )

    def subscription_utilization(
        self, subscription_id: str, active_leases: int, max_concurrency: int
    ) -> None:
        """Held capacity as a percentage of declared capacity, per subscription.

        The only metric dimensioned by subscription. Putting the subscription on
        claim outcomes would multiply custom metric cost by pool size, and the
        outcome distribution is a fleet-level question anyway.
        """
        if max_concurrency <= 0:
            return
        percent = 100.0 * active_leases / max_concurrency
        self.emit(
            "SubscriptionUtilization",
            round(percent, 2),
            MetricUnit.PERCENT,
            {"SubscriptionId": subscription_id},
        )

    # --- credits ---------------------------------------------------------------
    #
    # Credit figures are Decimal everywhere else and float here: a metric is for
    # looking at, and nothing reads one back into accounting.

    def usage_reading(
        self, outcome: UsageReadingOutcome, subscription_id: str, phase: UsagePhase
    ) -> None:
        """One `/usage` reading and what it did. Dimensioned by outcome only."""
        self.emit(
            "UsageReading",
            1,
            MetricUnit.COUNT,
            {"Outcome": str(outcome)},
            {"SubscriptionId": subscription_id, "Phase": str(phase)},
        )

    def subscription_credits_used(
        self, subscription_id: str, used: Decimal, source: CreditSource
    ) -> None:
        """The recorded cumulative figure, per subscription, after a check or a pass."""
        self.emit(
            "SubscriptionCreditsUsed",
            float(used),
            MetricUnit.NONE,
            {"SubscriptionId": subscription_id},
            {"Source": str(source)},
        )

    def subscription_credit_budget(self, subscription_id: str, budget: Decimal) -> None:
        self.emit(
            "SubscriptionCreditBudget",
            float(budget),
            MetricUnit.NONE,
            {"SubscriptionId": subscription_id},
        )

    def run_credits(self, subscription_id: str, item_id: str, credits: Decimal) -> None:
        """The `▸ Credits:` figure one run printed. Observability only, never accounting.

        It covers the primary agent only and is shown to two decimal places, so it
        is an input for sizing the margin and not a figure to add up.
        """
        self.emit(
            "RunCredits",
            float(credits),
            MetricUnit.NONE,
            properties={"SubscriptionId": subscription_id, "ItemId": item_id},
        )

    def run_credits_missing(
        self, subscription_id: str, item_id: str, verdict: str
    ) -> None:
        """A run printed no `Credits:` figure on either stream.

        Without this count a `RunCredits` series looks complete when it is not, and a
        margin sized from its p95 is sized from the runs that happened to print one.

        Dimensioned by verdict, as `RunOutcome` is. A run that timed out, was throttled
        or crashed never prints a summary, so only `Verdict=Succeeded` measures the
        runs the p95 leaves out; the rest are failures counted elsewhere.
        """
        self.emit(
            "RunCreditsMissing",
            1,
            MetricUnit.COUNT,
            {"Verdict": verdict},
            {"SubscriptionId": subscription_id, "ItemId": item_id},
        )

    def budget_exhausted(self, subscription_id: str) -> None:
        """A worker stopped draining because the subscription reached its ceiling."""
        self.emit(
            "BudgetExhausted",
            1,
            MetricUnit.COUNT,
            properties={"SubscriptionId": subscription_id},
        )

    def report_files_read(self, count: int) -> None:
        """Zero on a day the report should exist means the report pipeline is broken."""
        self.emit("ReportFilesRead", count, MetricUnit.COUNT)

    # --- scale-in protection -----------------------------------------------------

    def task_protection_failed(
        self, action: ProtectionAction, reason: ProtectionFailure
    ) -> None:
        """A request to turn this task's scale-in protection on or off did not take effect.

        The item ran anyway, unprotected, as every item did before protection existed; this
        is the count that says how often. Not dimensioned, so one alarm covers every cause;
        the action and the reason travel as properties for a log query.
        """
        self.emit(
            "TaskProtectionFailed",
            1,
            MetricUnit.COUNT,
            properties={"Action": str(action), "Reason": str(reason)},
        )

    @contextmanager
    def timed_claim(self):
        """Times a claim attempt and emits the latency however it ends.

        A claim that ends in saturation took time too, and excluding it would make
        the latency series look better exactly when the pool is under pressure.
        """
        started = time.perf_counter()
        try:
            yield
        finally:
            self.claim_latency((time.perf_counter() - started) * 1000)


NULL_METRICS: Final = Metrics(stream=open(os.devnull, "w"))
"""For call sites that have no emitter yet. Not for use in the fleet."""
