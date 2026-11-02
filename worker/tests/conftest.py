"""Shared test fixtures.

Two jobs. First, guarantee that no test can reach a real AWS account, whatever a
contributor happens to have configured locally. Second, stand up a subscription
pool that matches what the CDK application provisions, so that behavior verified
here is behavior that holds when deployed.

The pool is written with raw DynamoDB attribute values rather than through the
package's own serializer. That is deliberate: a fixture that shares the code under
test cannot catch a serializer that is self-consistently wrong.
"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import boto3
import botocore.config
import pytest
from boto3.dynamodb.types import TypeDeserializer
from moto import mock_aws

from leaseclient import schema

REGION = "us-east-1"
"""Carries no meaning; moto never leaves the process. Chosen for neutrality."""

SUBSCRIPTIONS_TABLE = "test-subscriptions"
LEASES_TABLE = "test-leases"

FAKE_KEY_MATERIAL_PREFIX = "FAKEKEY-DO-NOT-LOG"
"""Sentinel that stands in for subscription key material.

Deliberately greppable and deliberately not shaped like a real credential, so it
trips the leak sweep rather than a secret scanner. Any test output containing this
string means key material escaped a path that should never carry it.
"""

_CREDENTIAL_ENV_VARS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_ROLE_ARN",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
)


@pytest.fixture(autouse=True)
def isolated_aws_environment(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Sever every path boto3 could use to find real credentials.

    Autouse and unconditional. Clearing the environment variables alone is not
    enough, because boto3 falls back to the shared credentials file, to a named
    profile, and to the instance metadata service. Each of those is closed here.
    Dummy credentials are then injected so that moto has something to accept.
    """
    for name in _CREDENTIAL_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    # Point the credential and config files at paths inside the test's temporary
    # directory. They do not exist, and boto3 treats an absent file as empty.
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-config"))

    # Close the metadata service, which would otherwise be attempted and time out.
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)


@pytest.fixture(autouse=True)
def restore_root_log_level():
    """Put the root logger's level back after each test.

    A function handler sets it, as it must under Lambda, and a test that calls one
    would otherwise change what every later test logs.
    """
    root = logging.getLogger()
    level = root.level
    yield
    root.setLevel(level)


@pytest.fixture
def aws(isolated_aws_environment: None):
    """A boto3 session whose AWS calls are intercepted by moto."""
    with mock_aws():
        yield boto3.Session(region_name=REGION)


@pytest.fixture
def dynamodb(aws):
    return aws.client("dynamodb")


@pytest.fixture
def secretsmanager(aws):
    return aws.client("secretsmanager")


@pytest.fixture
def pool(
    dynamodb, secretsmanager, monkeypatch: pytest.MonkeyPatch
) -> "SubscriptionPool":
    """Both tables, provisioned as CDK provisions them, wired into the environment."""
    dynamodb.create_table(**schema.subscriptions_table_definition(SUBSCRIPTIONS_TABLE))
    dynamodb.create_table(**schema.leases_table_definition(LEASES_TABLE))
    dynamodb.update_time_to_live(
        TableName=LEASES_TABLE,
        TimeToLiveSpecification=schema.leases_ttl_specification(),
    )

    monkeypatch.setenv(schema.ENV_SUBSCRIPTIONS_TABLE, SUBSCRIPTIONS_TABLE)
    monkeypatch.setenv(schema.ENV_LEASES_TABLE, LEASES_TABLE)

    return SubscriptionPool(
        dynamodb=dynamodb,
        secretsmanager=secretsmanager,
        subscriptions_table=SUBSCRIPTIONS_TABLE,
        leases_table=LEASES_TABLE,
    )


@dataclass(frozen=True)
class SeededSubscription:
    """What a test needs to know about a subscription it placed in the pool."""

    subscription_id: str
    secret_ref: str
    key_material: str
    max_concurrency: int
    credit_budget: int | Decimal | str


@dataclass
class SubscriptionPool:
    """Seeds and inspects pool state without going through the code under test."""

    dynamodb: Any
    secretsmanager: Any
    subscriptions_table: str
    leases_table: str

    _deserializer = TypeDeserializer()

    def seed_subscription(
        self,
        subscription_id: str | None = None,
        *,
        max_concurrency: int = 5,
        credit_budget: int | Decimal | str = 10_000,
        credits_used: int | Decimal | str = 0,
        active_leases: int = 0,
        state: schema.SubscriptionState = schema.SubscriptionState.ENABLED,
        window_resets_in: timedelta = timedelta(days=30),
        window_resets_raw: str | None = None,
        with_window: bool = True,
        report_user_id: str | None = None,
        with_secret: bool = True,
    ) -> SeededSubscription:
        """Place one subscription in the pool.

        The default concurrency of 5 mirrors the conservative operational cap the
        pattern recommends, and the default budget mirrors a high-tier monthly
        allowance. Both are values a test can override; neither is discovered.
        """
        subscription_id = subscription_id or f"sub-{uuid.uuid4().hex[:8]}"
        key_material = f"{FAKE_KEY_MATERIAL_PREFIX}-{uuid.uuid4().hex}"

        if with_secret and self.secretsmanager is not None:
            # Created rather than fabricated, so the reference is a real ARN
            # produced by the API. Writing an ARN literal into a fixture would
            # mean writing an account-number-shaped value into this repository.
            created = self.secretsmanager.create_secret(
                Name=f"kiro/subscription/{subscription_id}",
                SecretString=key_material,
            )
            secret_ref = created["ARN"]
        elif with_secret:
            # No secret store in this backend; the reference is still well formed
            # and a stub resolves it. See StubSecrets.
            secret_ref = f"kiro/subscription/{subscription_id}"
        else:
            secret_ref = f"kiro/subscription/{subscription_id}/absent"

        resets_at = window_resets_raw or (datetime.now(UTC) + window_resets_in).isoformat()

        item = {
            schema.SUBSCRIPTION_ID: {"S": subscription_id},
            schema.SECRET_REF: {"S": secret_ref},
            schema.MAX_CONCURRENCY: {"N": str(max_concurrency)},
            schema.ACTIVE_LEASES: {"N": str(active_leases)},
            schema.CREDIT_BUDGET: {"N": str(credit_budget)},
            schema.CREDITS_USED: {"N": str(credits_used)},
            schema.STATE: {"S": str(state)},
        }
        if with_window:
            item[schema.WINDOW_RESETS_AT] = {"S": resets_at}
        if report_user_id is not None:
            item[schema.REPORT_USER_ID] = {"S": report_user_id}
        self.dynamodb.put_item(TableName=self.subscriptions_table, Item=item)

        return SeededSubscription(
            subscription_id=subscription_id,
            secret_ref=secret_ref,
            key_material=key_material,
            max_concurrency=max_concurrency,
            credit_budget=credit_budget,
        )

    def seed_subscriptions(self, count: int, **kwargs) -> list[SeededSubscription]:
        return [self.seed_subscription(**kwargs) for _ in range(count)]

    # --- inspection ----------------------------------------------------------

    def subscription(self, subscription_id: str) -> dict[str, Any]:
        response = self.dynamodb.get_item(
            TableName=self.subscriptions_table,
            Key={schema.SUBSCRIPTION_ID: {"S": subscription_id}},
            ConsistentRead=True,
        )
        if "Item" not in response:
            raise AssertionError(f"no subscription {subscription_id!r} in the pool")
        return {k: self._deserializer.deserialize(v) for k, v in response["Item"].items()}

    def active_leases(self, subscription_id: str) -> int:
        return int(self.subscription(subscription_id)[schema.ACTIVE_LEASES])

    def credits_used(self, subscription_id: str) -> Decimal:
        # Decimal, not int: readings carry two decimal places.
        return Decimal(self.subscription(subscription_id)[schema.CREDITS_USED])

    def window_resets_raw(self, subscription_id: str) -> str | None:
        return self.subscription(subscription_id).get(schema.WINDOW_RESETS_AT)

    def lease_count(self, subscription_id: str) -> int:
        """Live leases actually recorded, which is what drift correction compares against."""
        response = self.dynamodb.query(
            TableName=self.leases_table,
            IndexName=schema.INDEX_BY_SUBSCRIPTION,
            KeyConditionExpression=f"{schema.SUBSCRIPTION_ID} = :sid",
            ExpressionAttributeValues={":sid": {"S": subscription_id}},
            Select="COUNT",
        )
        return int(response["Count"])

    def all_leases(self) -> list[dict[str, Any]]:
        response = self.dynamodb.scan(TableName=self.leases_table)
        return [
            {k: self._deserializer.deserialize(v) for k, v in item.items()}
            for item in response["Items"]
        ]


# --- a real engine, for the tests that need one ------------------------------
#
# moto is faithful enough for behavior, and it caught an invalid condition
# expression, but it does not reproduce write contention: a deliberately
# non-atomic read-then-write passes against it unchanged. Measured, not assumed —
# against DynamoDB Local the same naive implementation handed out every request
# regardless of the ceiling.
#
# So the contention tests run against DynamoDB Local. This still satisfies the
# requirement that tests need no AWS account: the container is local and accepts
# any credentials. It does mean the tests skip when Docker is not running, which
# pytest reports because the suite is configured with -ra.

DYNAMODB_LOCAL_ENDPOINT = os.environ.get(
    "DYNAMODB_LOCAL_ENDPOINT", "http://localhost:8000"
)

DYNAMODB_LOCAL_HINT = (
    "DynamoDB Local is not reachable at "
    f"{DYNAMODB_LOCAL_ENDPOINT}. Contention cannot be verified against the in-process "
    "fake, which does not reproduce write races. Start it with:\n"
    "  docker run -d --rm --name ddb-local -p 8000:8000 amazon/dynamodb-local:latest "
    "-jar DynamoDBLocal.jar -inMemory -sharedDb"
)


class StubSecrets:
    """Stands in for Secrets Manager, which DynamoDB Local does not serve.

    Contention tests are about the reservation counter; the key path is covered
    against the in-process fake elsewhere.
    """

    def __init__(self, secret: str | None = None) -> None:
        self._secret = secret or f"{FAKE_KEY_MATERIAL_PREFIX}-stub"

    def get_secret_value(self, SecretId: str) -> dict[str, str]:  # noqa: N803
        return {"SecretString": self._secret}


def _dynamodb_local_client():
    return boto3.client(
        "dynamodb",
        region_name=REGION,
        endpoint_url=DYNAMODB_LOCAL_ENDPOINT,
        aws_access_key_id="local",
        aws_secret_access_key="local",
        config=botocore.config.Config(
            retries={"total_max_attempts": 1}, connect_timeout=2, read_timeout=10
        ),
    )


@pytest.fixture(scope="session")
def dynamodb_local_available() -> bool:
    try:
        _dynamodb_local_client().list_tables()
    except Exception:
        return False
    return True


@pytest.fixture
def real_engine(dynamodb_local_available: bool):
    """A DynamoDB client backed by the actual engine, or a skip."""
    if not dynamodb_local_available:
        pytest.skip(DYNAMODB_LOCAL_HINT)
    return _dynamodb_local_client()


@pytest.fixture
def contended_pool(real_engine, monkeypatch: pytest.MonkeyPatch) -> "SubscriptionPool":
    """A pool on the real engine, with tables unique to this test.

    Named uniquely rather than deleted afterwards: DynamoDB Local keeps everything
    in memory for the life of the container, and creating fresh tables is faster
    and less flaky than tearing them down between tests.
    """
    suffix = uuid.uuid4().hex[:10]
    subscriptions = f"contention-subs-{suffix}"
    leases = f"contention-leases-{suffix}"

    real_engine.create_table(**schema.subscriptions_table_definition(subscriptions))
    real_engine.create_table(**schema.leases_table_definition(leases))

    monkeypatch.setenv(schema.ENV_SUBSCRIPTIONS_TABLE, subscriptions)
    monkeypatch.setenv(schema.ENV_LEASES_TABLE, leases)

    return SubscriptionPool(
        dynamodb=real_engine,
        secretsmanager=None,
        subscriptions_table=subscriptions,
        leases_table=leases,
    )


@pytest.fixture
def new_engine_client():
    """A factory for per-thread clients.

    botocore clients are not documented as thread safe for sharing, and a shared
    client would also serialize where the test needs concurrency.
    """
    return _dynamodb_local_client


# --- leak detection ----------------------------------------------------------
#
# Every other test asserts something specific. This asserts the absence of
# something, across all of them at once, which is the only way to catch a leak that
# nobody thought to look for: an exception message that interpolates a value, a log
# line added in passing, a repr that was never overridden.
#
# Keyed on the sentinel every fixture uses for key material, so a match means a real
# path carried a credential rather than that a pattern looked suspicious.


@pytest.fixture(autouse=True)
def refuse_to_leak_key_material(caplog, capsys):
    """Fail any test whose logs or output contain seeded key material.

    Autouse, so a new test is covered without remembering to opt in. Runs after the
    test body, and reports where it found the value rather than only that it did.
    """
    yield

    captured = capsys.readouterr()

    # Records from the phases that ran, not caplog.text: that property reflects only
    # the phase in progress, which at teardown is teardown, so it would be empty and
    # the check would silently pass.
    records = []
    for phase in ("setup", "call"):
        records.extend(caplog.get_records(phase))
    log_text = "\n".join(record.getMessage() for record in records)

    haystacks = {
        "stdout": captured.out,
        "stderr": captured.err,
        "logs": log_text,
    }

    # Put back what was captured, so a test that inspects output still can and so a
    # failure elsewhere is still readable.
    sys.stdout.write(captured.out)
    sys.stderr.write(captured.err)

    leaked = [where for where, text in haystacks.items() if FAKE_KEY_MATERIAL_PREFIX in text]
    if leaked:
        raise AssertionError(
            f"key material reached {', '.join(leaked)}. Something on this path writes a "
            "credential where it can be read. Find the interpolation rather than "
            "filtering the output."
        )
