"""Acquiring a subscription for one worker.

A worker starting up asks for a subscription and receives one that provably has
concurrency headroom and an unexhausted budget, or is told the pool is saturated.
Saturation is an ordinary answer, not an error: it means the fleet has scaled past
the pool, and the right response is for the task to exit cleanly rather than crash
and be replaced.

The order of operations here is deliberate. Capacity is reserved before the lease
recording it is written. If the process dies between the two, the counter is
momentarily high with no lease behind it, which reconciliation resolves. The
opposite order would allow a lease with no reservation, which is real overcommit.
Given a choice of failure, prefer the one that reconciles toward correctness.
"""

from __future__ import annotations

import logging
import random
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Final

from leaseclient import schema
from leaseclient.metrics import ClaimOutcome, Metrics, RecoveryMechanism
from leaseclient.registry import RejectionReason, Registry, SubscriptionRecord

logger = logging.getLogger(__name__)

_JITTER: Final = random.SystemRandom()
"""Draws the claim backoff jitter. Not a security use, only spreading retries; the OS source
is used anyway, so the choice never has to be argued in a review."""

DEFAULT_LEASE_LIFETIME: Final = timedelta(seconds=120)
DEFAULT_MAX_ATTEMPTS: Final = 5
DEFAULT_BASE_BACKOFF: Final = timedelta(seconds=1)
DEFAULT_MAX_BACKOFF: Final = timedelta(seconds=30)
DEFAULT_CREDIT_MARGIN: Final = Decimal("1")
"""Credits held back below the budget, to cover what a reading cannot see yet.

What a `/usage` reading misses: runs in flight (one per held lease) and runs that
finished within the ~45 s before the figure moves. With `C` the largest
`maxConcurrency` in the pool and `P` the p95 credits of one run, the margin is
`2·C·P + 0.01` (the last term is the two-decimal display). The measured trivial
prompt costs about 0.05, which gives 0.21 at `C = 2`; one whole credit is the
default because the example workflows have not been measured. Set `CREDIT_MARGIN`
from the `RunCredits` p95 on the dashboard.
"""

CREDIT_MARGIN_PLACES: Final = 2
"""`/usage` shows two decimal places, so a finer margin would be false precision."""


def parse_credit_margin(raw: str | None) -> Decimal:
    """`CREDIT_MARGIN` as a `Decimal`: finite, not negative, at most two places.

    Raises `ValueError` for anything else, which the task entry point reports as a
    configuration error.
    """
    if raw is None or not raw.strip():
        return DEFAULT_CREDIT_MARGIN
    try:
        margin = Decimal(raw.strip())
    except InvalidOperation:
        raise ValueError(f"CREDIT_MARGIN is not a number: {raw!r}") from None
    if not margin.is_finite():
        raise ValueError(f"CREDIT_MARGIN must be finite, got {raw!r}")
    if margin < 0:
        raise ValueError(f"CREDIT_MARGIN must not be negative, got {raw!r}")
    exponent = margin.as_tuple().exponent
    if isinstance(exponent, int) and exponent < -CREDIT_MARGIN_PLACES:
        raise ValueError(
            f"CREDIT_MARGIN has more than {CREDIT_MARGIN_PLACES} decimal places "
            f"({raw!r}); /usage reports credits to 0.01"
        )
    return margin


class SecretUnavailable(RuntimeError):
    """Key material could not be resolved for an otherwise eligible subscription."""


@dataclass(frozen=True)
class ClaimConfig:
    """Tuning for the allocation protocol.

    `credit_margin` exists because the recorded `creditsUsed` lags real
    consumption: a reading misses runs in flight and runs that finished in the
    last ~45 s. See `DEFAULT_CREDIT_MARGIN` for how to size it.
    """

    lease_lifetime: timedelta = DEFAULT_LEASE_LIFETIME
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    base_backoff: timedelta = DEFAULT_BASE_BACKOFF
    max_backoff: timedelta = DEFAULT_MAX_BACKOFF
    credit_margin: Decimal = DEFAULT_CREDIT_MARGIN

    @property
    def heartbeat_interval(self) -> timedelta:
        """A quarter of the lifetime, so three consecutive failures are survivable.

        Derived rather than configured separately: an interval and a lifetime set
        independently can be made inconsistent, and the failure is a lease that
        expires under a worker still doing useful work.
        """
        return self.lease_lifetime / 4


@dataclass(frozen=True)
class Lease:
    """One worker's active allocation of one subscription."""

    lease_id: str
    subscription_id: str
    secret_ref: str
    expires_at: datetime

    def to_item(self, holder_id: str, heartbeat_at: datetime) -> dict[str, Any]:
        return {
            schema.LEASE_ID: {"S": self.lease_id},
            schema.SUBSCRIPTION_ID: {"S": self.subscription_id},
            schema.HOLDER_ID: {"S": holder_id},
            schema.HEARTBEAT_AT: {"S": heartbeat_at.isoformat()},
            schema.EXPIRES_AT: {"N": str(int(self.expires_at.timestamp()))},
        }


class Leases:
    """The lease table. Records who holds what, and until when."""

    def __init__(self, dynamodb, table_name: str | None = None) -> None:
        self._ddb = dynamodb
        self._table = table_name or schema.leases_table_name()

    @property
    def table_name(self) -> str:
        return self._table

    def record(self, lease: Lease, holder_id: str) -> None:
        self._ddb.put_item(
            TableName=self._table,
            Item=lease.to_item(holder_id, heartbeat_at=datetime.now(UTC)),
        )

    def delete(self, lease_id: str) -> None:
        self._ddb.delete_item(
            TableName=self._table, Key={schema.LEASE_ID: {"S": lease_id}}
        )

    def count_for(self, subscription_id: str) -> int:
        """Live leases recorded against a subscription, for reconciliation."""
        paginator = self._ddb.get_paginator("query")
        pages = paginator.paginate(
            TableName=self._table,
            IndexName=schema.INDEX_BY_SUBSCRIPTION,
            KeyConditionExpression=f"{schema.SUBSCRIPTION_ID} = :sid",
            ExpressionAttributeValues={":sid": {"S": subscription_id}},
            Select="COUNT",
        )
        return sum(page["Count"] for page in pages)

    def reap_expired(self, subscription_id: str, cutoff: datetime) -> tuple[int, int]:
        """Delete this subscription's leases that expired before `cutoff`; count the rest.

        Returns `(live, reaped)`. Without this, a lease that expired but that the
        time-to-live service has not deleted yet still counts as live, and DynamoDB gives
        no upper bound on when that deletion happens: capacity a dead task held would stay
        held for as long as the deletion takes.

        Each delete is conditional on the lease still having expired, so a holder whose
        heartbeat lands in between keeps it. Anything this cannot classify (no expiry
        attribute, a failed condition, a lease that vanished between the query and the
        read) counts as live, so the count is never lower than the truth.

        The index projects keys only, so each lease is read back for its expiry. A
        subscription holds at most its concurrency in leases, which keeps this small.
        """
        paginator = self._ddb.get_paginator("query")
        pages = paginator.paginate(
            TableName=self._table,
            IndexName=schema.INDEX_BY_SUBSCRIPTION,
            KeyConditionExpression=f"{schema.SUBSCRIPTION_ID} = :sid",
            ExpressionAttributeValues={":sid": {"S": subscription_id}},
        )
        lease_ids = [
            item[schema.LEASE_ID]["S"] for page in pages for item in page.get("Items", [])
        ]
        limit = str(int(cutoff.timestamp()))
        live = reaped = 0
        for lease_id in lease_ids:
            key = {schema.LEASE_ID: {"S": lease_id}}
            item = self._ddb.get_item(
                TableName=self._table,
                Key=key,
                ProjectionExpression="#expires",
                ExpressionAttributeNames={"#expires": schema.EXPIRES_AT},
            ).get("Item")
            if item is None:
                # Released or TTL-deleted between the query and the read. Whoever removed it
                # also decrements the counter, so counting it as gone here would decrement it
                # a second time. Count it as live; the next pass sees it gone.
                live += 1
                continue
            expires = item.get(schema.EXPIRES_AT, {}).get("N")
            if expires is None or int(expires) > int(limit):
                live += 1
                continue
            try:
                self._ddb.delete_item(
                    TableName=self._table,
                    Key=key,
                    ConditionExpression="#expires <= :limit",
                    ExpressionAttributeNames={"#expires": schema.EXPIRES_AT},
                    ExpressionAttributeValues={":limit": {"N": limit}},
                )
                reaped += 1
            except self._ddb.exceptions.ConditionalCheckFailedException:
                live += 1  # extended in between, or already gone: never undercount
        return live, reaped


class Allocator:
    """Hands one subscription to one worker, or reports saturation."""

    def __init__(
        self,
        registry: Registry,
        leases: Leases,
        secrets,
        metrics: Metrics,
        config: ClaimConfig | None = None,
        sleep=time.sleep,
        shuffle=random.shuffle,
    ) -> None:
        self._registry = registry
        self._leases = leases
        self._secrets = secrets
        self._metrics = metrics
        self._config = config or ClaimConfig()
        self._sleep = sleep
        self._shuffle = shuffle

    @property
    def config(self) -> ClaimConfig:
        return self._config

    def claim(self, holder_id: str) -> Lease | None:
        """Acquire a subscription, or return None if the pool is saturated.

        None is a normal outcome. A worker that receives it should exit zero: the
        fleet has more tasks than the pool can serve, and a task that crashes
        instead would be replaced and try again, turning saturation into a loop.
        """
        with self._metrics.timed_claim():
            return self._claim(holder_id)

    def _claim(self, holder_id: str) -> Lease | None:
        examined = 0

        for attempt in range(self._config.max_attempts):
            # Re-read on every attempt, so capacity released by another worker
            # while this one was backing off becomes visible.
            candidates = self._registry.list_enabled()
            self._shuffle(candidates)

            for candidate in candidates:
                examined += 1
                lease = self._try_candidate(candidate, holder_id)
                if lease is not None:
                    self._metrics.claim_candidates_examined(examined)
                    self._metrics.claim_outcome(ClaimOutcome.SUCCESS)
                    logger.info(
                        "claimed %s for %s, lease %s",
                        candidate.subscription_id,
                        holder_id,
                        lease.lease_id,
                    )
                    return lease

            if attempt < self._config.max_attempts - 1:
                self._sleep(self._backoff(attempt).total_seconds())

        self._metrics.claim_candidates_examined(examined)
        self._metrics.claim_outcome(ClaimOutcome.SATURATED)
        logger.info(
            "pool saturated after %d attempts across %d candidate examinations",
            self._config.max_attempts,
            examined,
        )
        return None

    def _try_candidate(
        self, candidate: SubscriptionRecord, holder_id: str
    ) -> Lease | None:
        reservation = self._registry.try_reserve_record(
            candidate, self._config.credit_margin
        )
        if not reservation.granted:
            self._record_rejection(reservation.reason)
            return None

        # Reserved. From here every failure path must give the reservation back.
        lease = Lease(
            lease_id=str(uuid.uuid4()),
            subscription_id=candidate.subscription_id,
            secret_ref=candidate.secret_ref,
            expires_at=datetime.now(UTC) + self._config.lease_lifetime,
        )
        try:
            self._leases.record(lease, holder_id)
            self._verify_secret_resolves(candidate.secret_ref)
        except Exception:
            self._registry.release_reservation(candidate.subscription_id)
            self._leases.delete(lease.lease_id)
            logger.warning(
                "released reservation on %s after a failure during claim",
                candidate.subscription_id,
                exc_info=True,
            )
            return None

        self._metrics.subscription_utilization(
            candidate.subscription_id,
            active_leases=candidate.active_leases + 1,
            max_concurrency=candidate.max_concurrency,
        )
        return lease

    def _verify_secret_resolves(self, secret_ref: str) -> None:
        """Confirm the key can be read before handing out a lease on it.

        A lease whose key is unusable is worse than no lease: the worker would hold
        capacity, fail to authenticate, and, because an invalid credential does not
        reliably produce a non-zero exit, could discard its work item silently.
        Failing here sends the claim to the next candidate instead.

        The value is fetched and dropped. It is never logged, returned, or stored.
        """
        try:
            response = self._secrets.get_secret_value(SecretId=secret_ref)
        except Exception as exc:
            raise SecretUnavailable(
                f"could not resolve the secret referenced by this subscription: "
                f"{type(exc).__name__}"
            ) from None

        if not response.get("SecretString"):
            raise SecretUnavailable("the referenced secret holds no string value")

    def resolve_key(self, lease: Lease) -> str:
        """Read the key material for a held lease.

        Deliberately takes a lease rather than a subscription id, so there is no way
        to ask for a key the caller does not hold. The returned value must go
        straight into the child process environment and nowhere else.
        """
        response = self._secrets.get_secret_value(SecretId=lease.secret_ref)
        secret = response.get("SecretString")
        if not secret:
            raise SecretUnavailable("the referenced secret holds no string value")
        return secret

    def release(self, lease: Lease) -> None:
        """Return capacity immediately, rather than waiting out the lease lifetime."""
        self._leases.delete(lease.lease_id)
        returned = self._registry.release_reservation(lease.subscription_id)
        if returned:
            self._metrics.leases_recovered(1, RecoveryMechanism.EXPIRY)
        logger.info(
            "released lease %s on %s (counter decremented: %s)",
            lease.lease_id,
            lease.subscription_id,
            returned,
        )

    def _record_rejection(self, reason: RejectionReason | None) -> None:
        if reason is RejectionReason.NO_CONCURRENCY:
            self._metrics.claim_outcome(ClaimOutcome.NO_CONCURRENCY)
        elif reason is RejectionReason.BUDGET_EXHAUSTED:
            self._metrics.claim_outcome(ClaimOutcome.BUDGET_EXHAUSTED)
        # Disabled, absent, and unknown are not emitted as claim outcomes: they are
        # configuration states rather than capacity pressure, and counting them
        # alongside contention would blur the signal the fleet is watched on.

    def _backoff(self, attempt: int) -> timedelta:
        """Exponential with full jitter, capped.

        Jitter matters more than the curve here. Hundreds of tasks start within
        seconds of each other, so a deterministic schedule would synchronize their
        retries into waves that contend with each other instead of spreading out.
        """
        exponential = self._config.base_backoff * (2**attempt)
        capped = min(exponential, self._config.max_backoff)
        return timedelta(seconds=_JITTER.uniform(0, capped.total_seconds()))
