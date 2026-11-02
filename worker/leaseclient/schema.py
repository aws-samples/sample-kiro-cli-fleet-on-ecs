"""Table shape for the subscription pool.

The tables themselves are created by the CDK application, not by this module. What
lives here are the names and index definitions that both sides must agree on, so
that a rename is a single edit rather than a search across two languages.

Physical table names are supplied at runtime through the environment, because a
sample that hardcodes resource names cannot be deployed twice into one account.
"""

from __future__ import annotations

import os
from enum import StrEnum
from typing import Any, Final

# --- environment contract ----------------------------------------------------

ENV_SUBSCRIPTIONS_TABLE: Final = "SUBSCRIPTIONS_TABLE"
ENV_LEASES_TABLE: Final = "LEASES_TABLE"


class MissingConfiguration(RuntimeError):
    """A required environment variable is absent.

    Raised eagerly rather than defaulted. A worker that silently falls back to a
    guessed table name would either fail obscurely or, worse, operate against the
    wrong pool.
    """


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise MissingConfiguration(
            f"{name} is not set. It is supplied by the ECS task definition."
        )
    return value


def subscriptions_table_name() -> str:
    return _required_env(ENV_SUBSCRIPTIONS_TABLE)


def leases_table_name() -> str:
    return _required_env(ENV_LEASES_TABLE)


# --- Subscriptions table -----------------------------------------------------

SUBSCRIPTION_ID: Final = "subscriptionId"
SECRET_REF: Final = "secretRef"
MAX_CONCURRENCY: Final = "maxConcurrency"
ACTIVE_LEASES: Final = "activeLeases"
CREDIT_BUDGET: Final = "creditBudget"
CREDITS_USED: Final = "creditsUsed"
WINDOW_RESETS_AT: Final = "windowResetsAt"
STATE: Final = "state"
REPORT_USER_ID: Final = "reportUserId"
"""The `UserId` the activity report uses for this subscription's owner. Set by an operator."""

INDEX_BY_STATE: Final = "byState"
"""Lets an allocator read only enabled subscriptions.

The pool holds tens to low hundreds of items, so a single partition per state
value is acceptable; the alternative, scanning with a filter, reads disabled
records to discard them and is strictly worse at every pool size.
"""


class SubscriptionState(StrEnum):
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


# --- Leases table ------------------------------------------------------------

LEASE_ID: Final = "leaseId"
HOLDER_ID: Final = "holderId"
HEARTBEAT_AT: Final = "heartbeatAt"
EXPIRES_AT: Final = "expiresAt"

INDEX_BY_SUBSCRIPTION: Final = "bySubscription"
"""Lets reconciliation count live leases per subscription.

Projects keys only: the correction pass needs a count, never an attribute.
"""

TTL_ATTRIBUTE: Final = EXPIRES_AT
"""Expiry is enforced by DynamoDB, not by a process.

Workers are stopped by scale-in and by crashes, so a design in which capacity
returns only when a release path runs would leak capacity continuously.
"""


# --- definitions shared with tests -------------------------------------------
#
# Returned as CreateTable keyword arguments so that tests can stand up tables
# that match what CDK provisions. Production code never calls CreateTable.


def subscriptions_table_definition(table_name: str) -> dict[str, Any]:
    return {
        "TableName": table_name,
        "BillingMode": "PAY_PER_REQUEST",
        "KeySchema": [
            {"AttributeName": SUBSCRIPTION_ID, "KeyType": "HASH"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": SUBSCRIPTION_ID, "AttributeType": "S"},
            {"AttributeName": STATE, "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": INDEX_BY_STATE,
                "KeySchema": [
                    {"AttributeName": STATE, "KeyType": "HASH"},
                    {"AttributeName": SUBSCRIPTION_ID, "KeyType": "RANGE"},
                ],
                # ALL, so selecting a candidate needs no follow-up read of the
                # base table before attempting a reservation.
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    }


def leases_table_definition(table_name: str) -> dict[str, Any]:
    return {
        "TableName": table_name,
        "BillingMode": "PAY_PER_REQUEST",
        "KeySchema": [
            {"AttributeName": LEASE_ID, "KeyType": "HASH"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": LEASE_ID, "AttributeType": "S"},
            {"AttributeName": SUBSCRIPTION_ID, "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": INDEX_BY_SUBSCRIPTION,
                "KeySchema": [
                    {"AttributeName": SUBSCRIPTION_ID, "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }
        ],
        # Old image included: the expiry handler needs the removed lease's
        # subscription to know whose counter to decrement.
        "StreamSpecification": {
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
    }


def leases_ttl_specification() -> dict[str, Any]:
    return {"Enabled": True, "AttributeName": TTL_ATTRIBUTE}
