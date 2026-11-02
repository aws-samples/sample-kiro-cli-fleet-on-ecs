"""Reading and writing the subscription pool.

Callers never build DynamoDB expressions. This module owns the mapping between
subscription records and stored items, and it is the only place that knows how a
conditional write is phrased.

It provides the record type and its validation, the administrative and lookup
operations, the reservation primitive that makes allocation safe under contention,
and the conditional writes that record credit consumption.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Final, Self

from leaseclient import schema
from leaseclient.schema import SubscriptionState

logger = logging.getLogger(__name__)

MAX_SECRET_REF_LENGTH: Final = 2048
"""Generous upper bound. Present to reject a pasted blob, not to police naming."""

MAX_REPORT_USER_ID_LENGTH: Final = 128

Credits = Decimal | int | str
"""What a credit figure may be passed as. Stored and compared as `Decimal`.

`/usage` shows two decimal places (`12.97`), so an integer type would round every
reading. A float would compare `12.97` against a stored `12.97` as unequal.
"""


def as_credits(value: Credits, name: str = "credits") -> Decimal:
    """A credit figure as a finite, non-negative `Decimal`, or `InvalidSubscription`."""
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, str)):
        raise InvalidSubscription(
            f"{name} must be a Decimal, int or numeric string, got {type(value).__name__}"
        )
    try:
        number = Decimal(value)
    except ArithmeticError as exc:
        raise InvalidSubscription(f"{name} is not a number: {value!r}") from exc
    if not number.is_finite():
        raise InvalidSubscription(f"{name} must be finite, got {value!r}")
    if number < 0:
        raise InvalidSubscription(f"{name} must not be negative, got {value}")
    return number


def dynamodb_number(value: Decimal | int) -> dict[str, str]:
    """A DynamoDB `N` value. Fixed-point, because `str(Decimal)` can produce `1E+3`."""
    return {"N": format(Decimal(value), "f")}


def canonical_instant(moment: datetime) -> str:
    """How every automated writer stores `windowResetsAt`."""
    return moment.astimezone(UTC).isoformat()

_SECRET_NAME = re.compile(r"^[A-Za-z0-9/_+=.@-]+$")
_SECRETS_MANAGER_ARN = re.compile(
    r"^arn:aws[a-z-]*:secretsmanager:[a-z0-9-]+:\d{12}:secret:[A-Za-z0-9/_+=.@-]+$"
)


class RegistryError(Exception):
    """Base for problems this module reports."""


class InvalidSubscription(RegistryError):
    """A record was rejected before it could be written."""


class SubscriptionNotFound(RegistryError):
    """An operation named a subscription that is not in the pool."""


@dataclass(frozen=True)
class SubscriptionRecord:
    """One subscription in the pool, with its declared limits and current usage.

    Invariants are checked on construction, so an instance of this type is always
    writable. A record that would overcommit or that carries a nonsensical limit
    cannot be created at all.

    `max_concurrency` is an operational cap the operator chooses, deliberately set
    below what a subscription could momentarily sustain. It is not a discovered
    service limit, and nothing here tries to discover one.

    `credits_used` is the subscription's cumulative consumption in the current
    window, as last recorded. Workers record the absolute figure `/usage` reports,
    and the daily activity report can raise it; neither ever adds a per-run figure.
    It lags real consumption by the time since the last reading, which is what the
    claim margin covers. It is an estimate for a guardrail, not a balance.

    `window_resets_at_raw` is the stored string, kept so a conditional write can
    compare against exactly what is stored even if an operator wrote a different
    spelling of the same instant.
    """

    subscription_id: str
    secret_ref: str
    max_concurrency: int
    credit_budget: Decimal
    credits_used: Decimal = Decimal(0)
    active_leases: int = 0
    window_resets_at: datetime | None = None
    state: SubscriptionState = SubscriptionState.ENABLED
    report_user_id: str | None = None
    window_resets_at_raw: str | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        if not self.subscription_id or not self.subscription_id.strip():
            raise InvalidSubscription("subscription_id must be a non-empty string")

        validate_secret_ref(self.secret_ref)

        if self.max_concurrency < 1:
            raise InvalidSubscription(
                f"max_concurrency must be at least 1, got {self.max_concurrency}. "
                "A subscription that can hold no leases should be DISABLED instead."
            )
        # Frozen, so coercion goes through object.__setattr__. Integers from older
        # items and callers stay valid; they become Decimal here.
        object.__setattr__(
            self, "credit_budget", as_credits(self.credit_budget, "credit_budget")
        )
        object.__setattr__(
            self, "credits_used", as_credits(self.credits_used, "credits_used")
        )
        if self.report_user_id is not None:
            validate_report_user_id(self.report_user_id)
        if self.active_leases < 0:
            raise InvalidSubscription(
                f"active_leases must not be negative, got {self.active_leases}"
            )

    # --- eligibility ---------------------------------------------------------

    def has_concurrency_headroom(self) -> bool:
        return self.active_leases < self.max_concurrency

    def credit_ceiling(self, margin: Decimal | int = 0) -> Decimal:
        """The consumption level at which this subscription stops being allocatable.

        Computed here rather than inside the reservation's condition, because
        DynamoDB condition expressions do not support arithmetic: subtraction is
        available in an update expression but not in a condition. The caller
        therefore resolves the ceiling from the record it already holds, which the
        state index projects in full, so nothing extra is read.
        """
        return self.credit_budget - Decimal(margin)

    def has_budget_headroom(self, margin: Decimal | int = 0) -> bool:
        """Whether the budget, less a safety margin, is unexhausted.

        The margin exists because any figure written to `credits_used` lags real
        consumption. Without it, a subscription would be allocated right up to a
        boundary computed from stale data.
        """
        return self.credits_used < self.credit_ceiling(margin)

    def is_claimable(self, margin: Decimal | int = 0) -> bool:
        """Whether a claim could be granted here once a concurrency slot is free.

        The part of the reservation's condition that a free slot does not change:
        enabled, and under the budget less the margin. The scaling function counts
        only these subscriptions' concurrency, so it never starts a task that would
        be refused for credits.
        """
        return self.state is SubscriptionState.ENABLED and self.has_budget_headroom(
            margin
        )

    def is_allocatable(self, margin: Decimal | int = 0) -> bool:
        return self.is_claimable(margin) and self.has_concurrency_headroom()

    # --- serialization -------------------------------------------------------

    def to_item(self) -> dict[str, Any]:
        item: dict[str, Any] = {
            schema.SUBSCRIPTION_ID: {"S": self.subscription_id},
            schema.SECRET_REF: {"S": self.secret_ref},
            schema.MAX_CONCURRENCY: {"N": str(self.max_concurrency)},
            schema.ACTIVE_LEASES: {"N": str(self.active_leases)},
            schema.CREDIT_BUDGET: dynamodb_number(self.credit_budget),
            schema.CREDITS_USED: dynamodb_number(self.credits_used),
            schema.STATE: {"S": str(self.state)},
        }
        if self.window_resets_at is not None:
            item[schema.WINDOW_RESETS_AT] = {"S": self.window_resets_at.isoformat()}
        if self.report_user_id is not None:
            item[schema.REPORT_USER_ID] = {"S": self.report_user_id}
        return item

    @classmethod
    def from_item(cls, item: dict[str, Any]) -> Self:
        try:
            resets_raw = item.get(schema.WINDOW_RESETS_AT, {}).get("S")
            return cls(
                subscription_id=item[schema.SUBSCRIPTION_ID]["S"],
                secret_ref=item[schema.SECRET_REF]["S"],
                max_concurrency=int(item[schema.MAX_CONCURRENCY]["N"]),
                # Decimal reads both "1000" (items written before credits were
                # fractional) and "12.97".
                credit_budget=Decimal(item[schema.CREDIT_BUDGET]["N"]),
                credits_used=Decimal(item[schema.CREDITS_USED]["N"]),
                active_leases=int(item[schema.ACTIVE_LEASES]["N"]),
                window_resets_at=(
                    datetime.fromisoformat(resets_raw) if resets_raw else None
                ),
                state=SubscriptionState(item[schema.STATE]["S"]),
                report_user_id=item.get(schema.REPORT_USER_ID, {}).get("S"),
                window_resets_at_raw=resets_raw,
            )
        except (KeyError, ValueError, TypeError, ArithmeticError) as exc:
            raise InvalidSubscription(f"stored item is not a subscription: {exc}") from exc


def validate_secret_ref(secret_ref: str) -> None:
    """Reject anything that is not plausibly a reference to a stored secret.

    This is a shape check, not a secret detector. The real protection against key
    material reaching the registry is structural: no operation in this module
    accepts a secret *value*, only a reference, and resolution happens at claim
    time against the secret store. A caller determined to pass a key as if it were
    a name can still do so, which is why the check is described honestly rather
    than presented as a guarantee.
    """
    if not secret_ref or not secret_ref.strip():
        raise InvalidSubscription("secret_ref must be a non-empty string")

    if len(secret_ref) > MAX_SECRET_REF_LENGTH:
        raise InvalidSubscription(
            f"secret_ref exceeds {MAX_SECRET_REF_LENGTH} characters. "
            "Pass a reference to a secret, not its value."
        )

    if secret_ref != secret_ref.strip() or any(c.isspace() for c in secret_ref):
        raise InvalidSubscription("secret_ref must not contain whitespace")

    if secret_ref.startswith("arn:"):
        if not _SECRETS_MANAGER_ARN.match(secret_ref):
            raise InvalidSubscription(
                "secret_ref looks like an ARN but not a Secrets Manager secret ARN"
            )
        return

    if not _SECRET_NAME.match(secret_ref):
        raise InvalidSubscription(
            "secret_ref must be a Secrets Manager secret name or ARN"
        )


def validate_report_user_id(user_id: str) -> None:
    """The activity report's `UserId` for a subscription: an identifier, nothing more."""
    if not isinstance(user_id, str) or not user_id:
        raise InvalidSubscription("report_user_id must be a non-empty string")
    if len(user_id) > MAX_REPORT_USER_ID_LENGTH:
        raise InvalidSubscription(
            f"report_user_id exceeds {MAX_REPORT_USER_ID_LENGTH} characters"
        )
    if any(c.isspace() for c in user_id):
        raise InvalidSubscription("report_user_id must not contain whitespace")


class UsageWrite(StrEnum):
    """What recording one `/usage` reading did to the stored figure."""

    RAISED = "Raised"
    UNCHANGED = "Unchanged"
    WINDOW_ADOPTED = "WindowAdopted"
    STALE = "Stale"
    CONFLICT = "Conflict"


class RejectionReason(StrEnum):
    """Why a subscription could not be reserved.

    Derived locally from the item as it was at the moment the condition failed,
    rather than by reading it again: the contended item is the last place to add a
    read, and a second read would show a different state anyway.
    """

    NO_CONCURRENCY = "NoConcurrency"
    BUDGET_EXHAUSTED = "BudgetExhausted"
    DISABLED = "Disabled"
    ABSENT = "Absent"
    UNKNOWN = "Unknown"


@dataclass(frozen=True)
class Reservation:
    """The outcome of one attempt to reserve capacity on one subscription."""

    subscription_id: str
    granted: bool
    reason: RejectionReason | None = None

    def __bool__(self) -> bool:
        return self.granted


def _reason_from_item(
    item: dict[str, Any] | None, credit_ceiling: Decimal | int
) -> RejectionReason:
    """Work out which clause of the condition failed.

    Checked in the order that matters operationally. A disabled subscription is
    reported as disabled even if it is also at its ceiling, because that is the
    fact an operator acts on.
    """
    if not item:
        return RejectionReason.ABSENT
    try:
        record = SubscriptionRecord.from_item(item)
    except InvalidSubscription:
        return RejectionReason.UNKNOWN

    if record.state is not SubscriptionState.ENABLED:
        return RejectionReason.DISABLED
    if not record.has_concurrency_headroom():
        return RejectionReason.NO_CONCURRENCY
    if record.credits_used >= credit_ceiling:
        return RejectionReason.BUDGET_EXHAUSTED
    return RejectionReason.UNKNOWN


class Registry:
    """The subscription pool, as stored in DynamoDB."""

    def __init__(self, dynamodb, table_name: str | None = None) -> None:
        self._ddb = dynamodb
        self._table = table_name or schema.subscriptions_table_name()

    @property
    def table_name(self) -> str:
        return self._table

    # --- administration ------------------------------------------------------

    def put_subscription(self, record: SubscriptionRecord) -> None:
        """Add or replace a subscription.

        Counters are taken from the record, which defaults them to zero. Replacing
        a live subscription therefore resets its held-lease count, so this is an
        onboarding operation; use `set_state` or `set_secret_ref` to change an
        existing one.
        """
        self._ddb.put_item(TableName=self._table, Item=record.to_item())

    def register(
        self,
        subscription_id: str,
        secret_ref: str,
        max_concurrency: int,
        credit_budget: Credits,
        *,
        window_resets_at: datetime | None = None,
        report_user_id: str | None = None,
    ) -> SubscriptionRecord:
        """Convenience wrapper that constructs, validates, and stores a record."""
        record = SubscriptionRecord(
            subscription_id=subscription_id,
            secret_ref=secret_ref,
            max_concurrency=max_concurrency,
            credit_budget=credit_budget,
            window_resets_at=window_resets_at,
            report_user_id=report_user_id,
        )
        self.put_subscription(record)
        return record

    def set_state(self, subscription_id: str, state: SubscriptionState) -> None:
        """Enable or disable a subscription.

        Disabling stops new leases without touching live ones: the eligibility
        check reads this attribute, and a worker already holding a lease is not
        consulted again. Revoking capacity from a working worker would abandon
        whatever it is part-way through.
        """
        self._update(
            subscription_id,
            update="SET #state = :state",
            names={"#state": schema.STATE},
            values={":state": {"S": str(SubscriptionState(state))}},
        )

    def set_secret_ref(self, subscription_id: str, secret_ref: str) -> None:
        """Point a subscription at different key material, for rotation.

        Takes a reference. There is deliberately no parameter for a secret value.
        """
        validate_secret_ref(secret_ref)
        self._update(
            subscription_id,
            update="SET #ref = :ref",
            names={"#ref": schema.SECRET_REF},
            values={":ref": {"S": secret_ref}},
        )

    def set_report_user_id(self, subscription_id: str, user_id: str) -> None:
        """Map a subscription to its owner's `UserId` in the daily activity report.

        The report is keyed by user, not by API key, so the mapping has to come from
        someone who knows whose key this is. Without it the report ingester skips
        the subscription.
        """
        validate_report_user_id(user_id)
        self._update(
            subscription_id,
            update="SET #user = :user",
            names={"#user": schema.REPORT_USER_ID},
            values={":user": {"S": user_id}},
        )

    def _update(
        self,
        subscription_id: str,
        *,
        update: str,
        names: dict[str, str],
        values: dict[str, Any],
    ) -> None:
        try:
            self._ddb.update_item(
                TableName=self._table,
                Key={schema.SUBSCRIPTION_ID: {"S": subscription_id}},
                UpdateExpression=update,
                ConditionExpression=f"attribute_exists({schema.SUBSCRIPTION_ID})",
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=values,
            )
        except self._ddb.exceptions.ConditionalCheckFailedException as exc:
            raise SubscriptionNotFound(
                f"no subscription {subscription_id!r} in the pool"
            ) from exc

    # --- lookup --------------------------------------------------------------

    def get(self, subscription_id: str) -> SubscriptionRecord:
        response = self._ddb.get_item(
            TableName=self._table,
            Key={schema.SUBSCRIPTION_ID: {"S": subscription_id}},
            ConsistentRead=True,
        )
        if "Item" not in response:
            raise SubscriptionNotFound(f"no subscription {subscription_id!r} in the pool")
        return SubscriptionRecord.from_item(response["Item"])

    def list_enabled(self) -> list[SubscriptionRecord]:
        """Candidates for allocation, read through the state index.

        Queries rather than scans: a scan would read every disabled record in order
        to discard it. The index projects all attributes, so selecting a candidate
        needs no follow-up read of the base table.
        """
        paginator = self._ddb.get_paginator("query")
        pages = paginator.paginate(
            TableName=self._table,
            IndexName=schema.INDEX_BY_STATE,
            KeyConditionExpression="#state = :state",
            ExpressionAttributeNames={"#state": schema.STATE},
            ExpressionAttributeValues={
                ":state": {"S": str(SubscriptionState.ENABLED)}
            },
        )
        return self._collect(pages, context="list_enabled")

    def describe_all(self) -> list[SubscriptionRecord]:
        """Every subscription, for operator inspection. Scans; not on the claim path."""
        paginator = self._ddb.get_paginator("scan")
        pages = paginator.paginate(TableName=self._table)
        return self._collect(pages, context="describe_all")

    def _collect(self, pages, *, context: str) -> list[SubscriptionRecord]:
        """Parse items, skipping any that are malformed rather than aborting.

        One unparseable record must not make the pool unreadable: that would let a
        single bad row stop the whole fleet from allocating. The skip is logged so
        it surfaces; once metric emission lands it should also be counted.
        """
        records: list[SubscriptionRecord] = []
        for page in pages:
            for item in page.get("Items", []):
                try:
                    records.append(SubscriptionRecord.from_item(item))
                except InvalidSubscription as exc:
                    identifier = item.get(schema.SUBSCRIPTION_ID, {}).get("S", "<no id>")
                    logger.warning(
                        "%s: skipping malformed subscription %s: %s",
                        context,
                        identifier,
                        exc,
                    )
        return records


    # --- reservation ---------------------------------------------------------
    #
    # The primitive that makes allocation safe under contention.

    def try_reserve(
        self, subscription_id: str, *, credit_ceiling: Decimal | int
    ) -> Reservation:
        """Consume one unit of concurrency, or report why it was not available.

        The check and the consumption are one operation, so no interleaving of
        concurrent callers can push the held count above the declared ceiling.
        This is the whole basis of the design: correctness under contention does
        not depend on workers coordinating with each other.

        A failed condition is an ordinary outcome, not an error. Once the pool is
        busy it is the common outcome, which is why the caller loops over
        candidates rather than treating this as an exception.

        `credit_ceiling` is resolved by the caller, normally as
        `record.credit_ceiling(margin)` on the record read from the state index,
        because a condition expression cannot do arithmetic. If an operator changed
        the budget between that read and this write, the ceiling used here is one
        generation stale; the next attempt sees the new value, and the drift is
        bounded by the margin.
        """
        try:
            self._ddb.update_item(
                TableName=self._table,
                Key={schema.SUBSCRIPTION_ID: {"S": subscription_id}},
                UpdateExpression=(
                    f"SET {schema.ACTIVE_LEASES} = {schema.ACTIVE_LEASES} + :one"
                ),
                ConditionExpression=(
                    f"attribute_exists({schema.SUBSCRIPTION_ID})"
                    " AND #state = :enabled"
                    f" AND {schema.ACTIVE_LEASES} < {schema.MAX_CONCURRENCY}"
                    f" AND {schema.CREDITS_USED} < :ceiling"
                ),
                ExpressionAttributeNames={"#state": schema.STATE},
                ExpressionAttributeValues={
                    ":one": {"N": "1"},
                    ":enabled": {"S": str(SubscriptionState.ENABLED)},
                    ":ceiling": dynamodb_number(credit_ceiling),
                },
                ReturnValuesOnConditionCheckFailure="ALL_OLD",
            )
        except self._ddb.exceptions.ConditionalCheckFailedException as exc:
            reason = _reason_from_item(exc.response.get("Item"), credit_ceiling)
            return Reservation(subscription_id, granted=False, reason=reason)

        return Reservation(subscription_id, granted=True)

    def try_reserve_record(
        self, record: SubscriptionRecord, margin: Decimal | int = 0
    ) -> Reservation:
        """Reserve against a record already in hand, which is the claim path's shape."""
        return self.try_reserve(
            record.subscription_id, credit_ceiling=record.credit_ceiling(margin)
        )

    # --- recording consumption -----------------------------------------------
    #
    # Two automated writers, the worker's `/usage` readings and the daily activity
    # report, and one human override. The automated writes are monotonic within a
    # window: each is a single conditional update that only raises the stored value,
    # so readings arriving out of order, from any number of workers, end at the
    # largest. Only a window change lowers it.

    def record_usage(
        self,
        subscription_id: str,
        used: Credits,
        resets_at: datetime,
        now: datetime | None = None,
    ) -> UsageWrite:
        """Record an absolute `/usage` reading for the window ending at `resets_at`.

        `resets_at` is the server's reset date from the same reading, so the reading
        carries its own window:

        - a reading whose window has already ended is dropped (`STALE`), because
          writing it would put last month's total into the new window;
        - a reading for the stored window raises the stored value, or leaves a
          higher one alone (`RAISED` / `UNCHANGED`);
        - a reading for a different window adopts that window and its value
          (`WINDOW_ADOPTED`). This is the only automated write besides the
          reconciler's reset that can lower `creditsUsed`.

        Normally one UpdateItem. The failed condition returns the stored item, so
        deciding what to do next costs no extra read.
        """
        value = as_credits(used, "credits used")
        moment = now or utc_now()
        if resets_at <= moment:
            return UsageWrite.STALE
        reading_window = canonical_instant(resets_at)

        for _ in range(2):
            try:
                self._conditional_credits_write(
                    subscription_id,
                    value,
                    condition="attribute_exists(#id) AND #resets = :r AND #used < :v",
                    values={":r": {"S": reading_window}},
                    set_window=None,
                    return_old=True,
                )
                return UsageWrite.RAISED
            except self._ddb.exceptions.ConditionalCheckFailedException as exc:
                stored = exc.response.get("Item")

            if not stored:
                raise SubscriptionNotFound(
                    f"no subscription {subscription_id!r} in the pool"
                )
            stored_raw = stored.get(schema.WINDOW_RESETS_AT, {}).get("S")
            if stored_raw == reading_window:
                # Same window, and the stored value is already at least this high.
                return UsageWrite.UNCHANGED

            if stored_raw is None:
                window_condition = "attribute_exists(#id) AND attribute_not_exists(#resets)"
                window_values: dict[str, Any] = {}
            else:
                window_condition = "attribute_exists(#id) AND #resets = :stored"
                window_values = {":stored": {"S": stored_raw}}

            if stored_raw is not None and _same_instant(stored_raw, resets_at):
                # The same window spelled differently, for example by an operator who
                # wrote `Z` for `+00:00`. Still raise-only; the spelling is normalised.
                try:
                    self._conditional_credits_write(
                        subscription_id,
                        value,
                        condition=f"{window_condition} AND #used < :v",
                        values=window_values,
                        set_window=reading_window,
                    )
                    return UsageWrite.RAISED
                except self._ddb.exceptions.ConditionalCheckFailedException:
                    return UsageWrite.UNCHANGED

            try:
                self._conditional_credits_write(
                    subscription_id,
                    value,
                    condition=window_condition,
                    values=window_values,
                    set_window=reading_window,
                )
                return UsageWrite.WINDOW_ADOPTED
            except self._ddb.exceptions.ConditionalCheckFailedException:
                # Another writer moved the window between the two writes. Start again
                # from the same-window raise, once.
                continue

        logger.warning(
            "could not record a usage reading for %s: the window kept moving",
            subscription_id,
        )
        return UsageWrite.CONFLICT

    def raise_credits_from_report(
        self, record: SubscriptionRecord, month_to_date: Credits
    ) -> bool:
        """Raise `creditsUsed` to the activity report's figure, if that is higher.

        Conditional on the window being the one the figure was computed for, which
        is `record.window_resets_at_raw` from the same scan. A report computed for
        October therefore cannot land in November, and the report never lowers
        anything. False when the condition fails: a reading is already higher, or
        the window moved since the scan.
        """
        if record.window_resets_at_raw is None:
            return False
        try:
            self._conditional_credits_write(
                record.subscription_id,
                as_credits(month_to_date, "month to date"),
                condition="attribute_exists(#id) AND #resets = :stored AND #used < :v",
                values={":stored": {"S": record.window_resets_at_raw}},
                set_window=None,
            )
        except self._ddb.exceptions.ConditionalCheckFailedException:
            return False
        return True

    def override_credits_used(self, subscription_id: str, used: Credits) -> None:
        """Replace `creditsUsed` with an operator's figure, unconditionally.

        The one human override, behind `credits.apply_external_correction`. Unlike
        the automated writes it can lower the value within a window, which is the
        point: it is how a person corrects a figure they know to be wrong. Nothing
        automated calls it.
        """
        value = as_credits(used, "credits used")
        self._update(
            subscription_id,
            update="SET #used = :used",
            names={"#used": schema.CREDITS_USED},
            values={":used": dynamodb_number(value)},
        )

    def _conditional_credits_write(
        self,
        subscription_id: str,
        value: Decimal,
        *,
        condition: str,
        values: dict[str, Any],
        set_window: str | None,
        return_old: bool = False,
    ) -> None:
        update = "SET #used = :v"
        expression_values = {":v": dynamodb_number(value), **values}
        if set_window is not None:
            update += ", #resets = :r"
            expression_values[":r"] = {"S": set_window}
        arguments: dict[str, Any] = {}
        if return_old:
            arguments["ReturnValuesOnConditionCheckFailure"] = "ALL_OLD"
        self._ddb.update_item(
            TableName=self._table,
            Key={schema.SUBSCRIPTION_ID: {"S": subscription_id}},
            UpdateExpression=update,
            ConditionExpression=condition,
            ExpressionAttributeNames={
                "#id": schema.SUBSCRIPTION_ID,
                "#used": schema.CREDITS_USED,
                "#resets": schema.WINDOW_RESETS_AT,
            },
            ExpressionAttributeValues=expression_values,
            **arguments,
        )

    def reset_window(
        self, subscription_id: str, resets_at: datetime, current: str | None = None
    ) -> bool:
        """Zero consumption and move the window forward.

        Conditional on the stored reset instant being the one that was read, so two
        reconcilers running at once cannot zero a window twice and hand out a budget
        that was already spent.

        `current` is the stored string the caller decided on. Pass it: re-reading it
        here would let a reset land after a worker had already adopted the new window
        from `/usage`, and zero the reading it just recorded.
        """
        if current is None:
            stored = self.get(subscription_id).window_resets_at
            if stored is None:
                # No window to move forward. Writing one here would invent a reset
                # date nobody configured, so report that nothing was reset.
                return False
            current = stored.isoformat()
        try:
            self._ddb.update_item(
                TableName=self._table,
                Key={schema.SUBSCRIPTION_ID: {"S": subscription_id}},
                UpdateExpression="SET #used = :zero, #resets = :next",
                ConditionExpression="attribute_exists(#id) AND #resets = :current",
                ExpressionAttributeNames={
                    "#id": schema.SUBSCRIPTION_ID,
                    "#used": schema.CREDITS_USED,
                    "#resets": schema.WINDOW_RESETS_AT,
                },
                ExpressionAttributeValues={
                    ":zero": {"N": "0"},
                    ":next": {"S": resets_at.isoformat()},
                    ":current": {"S": current},
                },
            )
        except self._ddb.exceptions.ConditionalCheckFailedException:
            logger.debug("window for %s was reset by someone else", subscription_id)
            return False
        return True

    def correct_active_leases(
        self, subscription_id: str, observed: int, true_count: int
    ) -> bool:
        """Set the held count to what the leases actually say, if nothing moved.

        Conditional on the count still being the value that was read. A claim landing
        in between makes the condition fail, which is correct: the read is stale, and
        the next pass will see the new state. Failing is cheaper and safer than any
        locking scheme.
        """
        try:
            self._ddb.update_item(
                TableName=self._table,
                Key={schema.SUBSCRIPTION_ID: {"S": subscription_id}},
                UpdateExpression=f"SET {schema.ACTIVE_LEASES} = :true",
                ConditionExpression=(
                    f"attribute_exists({schema.SUBSCRIPTION_ID})"
                    f" AND {schema.ACTIVE_LEASES} = :observed"
                ),
                ExpressionAttributeValues={
                    ":true": {"N": str(true_count)},
                    ":observed": {"N": str(observed)},
                },
            )
        except self._ddb.exceptions.ConditionalCheckFailedException:
            logger.debug(
                "held count for %s changed while correcting; leaving it to the next pass",
                subscription_id,
            )
            return False
        return True

    def release_reservation(self, subscription_id: str) -> bool:
        """Return one unit of concurrency. Reports whether it actually decremented.

        Guarded on the count exceeding zero, so a duplicated release cannot drive
        it negative. A release that finds nothing to return is not an error: the
        reservation may already have been reclaimed by expiry, and the caller's
        intent, that this holder no longer occupies capacity, is satisfied either
        way.
        """
        try:
            self._ddb.update_item(
                TableName=self._table,
                Key={schema.SUBSCRIPTION_ID: {"S": subscription_id}},
                UpdateExpression=(
                    f"SET {schema.ACTIVE_LEASES} = {schema.ACTIVE_LEASES} - :one"
                ),
                ConditionExpression=(
                    f"attribute_exists({schema.SUBSCRIPTION_ID})"
                    f" AND {schema.ACTIVE_LEASES} > :zero"
                ),
                ExpressionAttributeValues={":one": {"N": "1"}, ":zero": {"N": "0"}},
            )
        except self._ddb.exceptions.ConditionalCheckFailedException:
            logger.debug(
                "release found nothing to return for %s; already released or reclaimed",
                subscription_id,
            )
            return False
        return True


def _same_instant(stored_raw: str, moment: datetime) -> bool:
    try:
        stored = datetime.fromisoformat(stored_raw)
    except ValueError:
        return False
    if stored.tzinfo is None:
        return False
    return stored == moment


def with_usage(
    record: SubscriptionRecord,
    *,
    active_leases: int | None = None,
    credits_used: Credits | None = None,
) -> SubscriptionRecord:
    """A copy with usage counters changed, for tests and for local reasoning."""
    return replace(
        record,
        active_leases=record.active_leases if active_leases is None else active_leases,
        credits_used=record.credits_used if credits_used is None else credits_used,
    )


def utc_now() -> datetime:
    return datetime.now(UTC)
