"""Recovering capacity that nothing gave back.

Workers are tasks. They are stopped by scale-in, by crashes, and by the platform
reclaiming capacity, and none of those run a release path. So expiry, enforced by the
datastore, is what actually guarantees capacity returns; explicit release is only an
optimization on top of it.

Expiry deletes the lease. It does not decrement the counter that the lease was holding
capacity in, which is what this does.

Two triggers, because they guarantee different things:

  - The lease stream is the fast path. A TTL deletion arrives as a removal event and
    the counter is decremented within seconds.
  - The schedule is the actual guarantee. DynamoDB gives no upper bound on when a TTL
    deletion happens, and a stream record can be missed, so a periodic pass deletes each
    subscription's leases that have already expired, then compares its counter against
    the leases that are left and corrects it.

The scheduled pass is therefore not a backstop for the stream. It is the mechanism;
the stream just makes it prompt.

The pass deletes only a lease that is past its expiry by `EXPIRY_GRACE`, and only on
that condition, so it removes nothing the time-to-live service would not remove. Its
deletes are application deletes, so the stream skips them rather than decrementing the
counter a second time. A holder that loses its lease this way sees its next heartbeat
fail and stops taking work, exactly as after a TTL deletion.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Final

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from leaseclient import schema
from leaseclient.claim import Leases
from leaseclient.credits import reset_window_if_due, window_from_environment
from leaseclient.lambda_logging import configure_logging
from leaseclient.metrics import Metrics, RecoveryMechanism
from leaseclient.registry import Registry

logger = logging.getLogger(__name__)

TTL_PRINCIPAL: Final = "dynamodb.amazonaws.com"
"""Marks a removal performed by the time-to-live service rather than by an application.

Without this distinction a normal release, which deletes the lease and decrements the
counter itself, would be decremented a second time when its removal reached the stream.
"""

EXPIRY_GRACE: Final = timedelta(seconds=30)
"""How far past its expiry a lease must be before the scheduled pass deletes it.

Covers clock skew between a task and this function. A holder heartbeats every quarter
of the lease lifetime, so a lease this far past expiry has missed every renewal.
"""


@dataclass
class ReconcileResult:
    """What one invocation recovered or corrected."""

    expired_leases: int = 0
    corrected_subscriptions: int = 0
    windows_reset: int = 0
    skipped: int = 0
    details: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "expiredLeases": self.expired_leases,
            "correctedSubscriptions": self.corrected_subscriptions,
            "windowsReset": self.windows_reset,
            "skipped": self.skipped,
            "details": self.details,
        }


class Reconciler:
    def __init__(
        self,
        registry: Registry,
        leases: Leases,
        metrics: Metrics,
        window: timedelta | None = None,
    ) -> None:
        # None steps credit windows by one calendar month, as Kiro's reset date does.
        self._registry = registry
        self._leases = leases
        self._metrics = metrics
        self._window = window

    # --- fast path: a lease expired ------------------------------------------

    def handle_stream(self, records: list[dict[str, Any]]) -> ReconcileResult:
        result = ReconcileResult()

        for record in records:
            if record.get("eventName") != "REMOVE":
                continue
            if not _removed_by_ttl(record):
                # An application deleted it, which already returned the capacity.
                result.skipped += 1
                continue

            subscription_id = _subscription_of(record)
            if subscription_id is None:
                logger.warning("a removed lease carried no subscription; cannot credit it")
                result.skipped += 1
                continue

            if self._registry.release_reservation(subscription_id):
                result.expired_leases += 1
                result.details.append(f"expired lease on {subscription_id}")
                logger.info(
                    "recovered capacity on %s from an expired lease", subscription_id
                )
            else:
                # The counter was already at zero. Not an error: the scheduled pass
                # may have corrected it first.
                result.skipped += 1

        if result.expired_leases:
            self._metrics.leases_recovered(
                result.expired_leases, RecoveryMechanism.EXPIRY
            )
        return result

    # --- the guarantee: periodic comparison ----------------------------------

    def reconcile(self, now: datetime | None = None) -> ReconcileResult:
        """Delete expired leases, compare every counter against the rest, and correct.

        Never lowers a counter below the number of live leases observed, so a worker
        holding capacity cannot have it taken away mid-run. Correction is conditional on
        the counter still being what was read, so a claim landing in between simply defers
        the fix to the next pass.

        Capacity returned because this pass deleted an expired lease is counted as expiry
        recovery, like a TTL deletion; only a difference beyond that counts as drift. The
        metric counts capacity given back to the counter, not deletions: a reaped lease the
        counter had already stopped counting returns nothing, so it adds nothing to the
        metric (it is still reported in `expired_leases` and the log).
        """
        result = ReconcileResult()
        moment = now or datetime.now(UTC)
        reaped_total = 0

        for record in self._registry.describe_all():
            try:
                true_count, reaped = self._leases.reap_expired(
                    record.subscription_id, moment - EXPIRY_GRACE
                )
            except (ClientError, BotoCoreError):
                # A throttle or a transient error on one subscription's leases must not stop
                # the pass for the rest. Its counter is left alone until the next pass, which
                # is the safe direction: nothing is lowered on a count that was not taken.
                logger.warning(
                    "could not read leases for %s; skipping its correction this pass",
                    record.subscription_id,
                    exc_info=True,
                )
                result.skipped += 1
                if reset_window_if_due(self._registry, record, moment, self._window):
                    result.windows_reset += 1
                continue
            if reaped:
                result.expired_leases += reaped
                result.details.append(
                    f"reaped {reaped} expired lease(s) on {record.subscription_id}"
                )
                logger.info(
                    "deleted %d expired lease(s) on %s", reaped, record.subscription_id
                )

            if true_count != record.active_leases:
                # Guard: never below what was actually observed holding capacity.
                target = max(true_count, 0)
                if self._registry.correct_active_leases(
                    record.subscription_id, record.active_leases, target
                ):
                    drift = record.active_leases - target
                    returned_by_reap = min(reaped, max(drift, 0))
                    reaped_total += returned_by_reap
                    if drift != returned_by_reap:
                        result.corrected_subscriptions += 1
                    result.details.append(
                        f"{record.subscription_id}: {record.active_leases} -> {target}"
                    )
                    logger.info(
                        "corrected %s: counter said %d, leases said %d (drift %+d)",
                        record.subscription_id,
                        record.active_leases,
                        target,
                        -drift,
                    )
                else:
                    result.skipped += 1

            if reset_window_if_due(self._registry, record, moment, self._window):
                result.windows_reset += 1

        if reaped_total:
            self._metrics.leases_recovered(reaped_total, RecoveryMechanism.EXPIRY)
        if result.corrected_subscriptions:
            self._metrics.leases_recovered(
                result.corrected_subscriptions, RecoveryMechanism.DRIFT_CORRECTION
            )
        return result


def _removed_by_ttl(record: dict[str, Any]) -> bool:
    identity = record.get("userIdentity") or {}
    return (
        identity.get("type") == "Service"
        and identity.get("principalId") == TTL_PRINCIPAL
    )


def _subscription_of(record: dict[str, Any]) -> str | None:
    """Read the subscription from the removed lease's old image.

    The new image is absent for a removal, which is why the stream is configured to
    include the old one.
    """
    image = (record.get("dynamodb") or {}).get("OldImage") or {}
    value = image.get(schema.SUBSCRIPTION_ID) or {}
    return value.get("S")


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """One entry point, two event shapes.

    Dispatching on shape rather than on separate functions keeps the deployed artifact
    single, so the two triggers cannot drift apart in what they do.
    """
    configure_logging()
    reconciler = _from_environment()

    records = event.get("Records")
    if records:
        result = reconciler.handle_stream(records)
    else:
        result = reconciler.reconcile()

    logger.info("reconciliation result: %s", result.as_dict())
    return result.as_dict()


def _from_environment() -> Reconciler:
    dynamodb = boto3.client("dynamodb")
    return Reconciler(
        registry=Registry(dynamodb, schema.subscriptions_table_name()),
        leases=Leases(dynamodb, schema.leases_table_name()),
        metrics=Metrics(),
        window=window_from_environment(),
    )
