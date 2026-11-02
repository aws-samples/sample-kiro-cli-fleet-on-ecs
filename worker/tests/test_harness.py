"""Verifies the test harness itself.

Everything downstream trusts these fixtures. If the tables here diverge from what
CDK provisions, or if credentials are not actually isolated, then every later test
is either testing the wrong shape or capable of reaching a real account. Both
failures are silent, so they are asserted explicitly.
"""

from __future__ import annotations

import os

import boto3
import botocore.config
import botocore.exceptions
import pytest

from leaseclient import schema
from tests.conftest import FAKE_KEY_MATERIAL_PREFIX, REGION


class TestCredentialIsolation:
    def test_no_real_credential_environment_variables_survive(self) -> None:
        assert os.environ["AWS_ACCESS_KEY_ID"] == "testing"
        assert os.environ["AWS_SECRET_ACCESS_KEY"] == "testing"
        assert "AWS_PROFILE" not in os.environ

    def test_credential_and_config_files_point_at_nothing(self) -> None:
        for variable in ("AWS_SHARED_CREDENTIALS_FILE", "AWS_CONFIG_FILE"):
            path = os.environ[variable]
            assert not os.path.exists(path), f"{variable} resolves to a real file"

    def test_no_profile_is_reachable(self) -> None:
        """A stray profile would let a test authenticate for real."""
        assert boto3.Session().available_profiles == []

    def test_instance_metadata_is_disabled(self) -> None:
        assert os.environ["AWS_EC2_METADATA_DISABLED"] == "true"

    def test_unmocked_call_does_not_reach_aws(self) -> None:
        """Outside moto, a call fails locally rather than authenticating.

        Pointed at an endpoint that refuses connections, with retries disabled and
        a short timeout: the default retry policy would spend half a minute backing
        off before reporting what is already known after the first attempt.
        """
        client = boto3.client(
            "dynamodb",
            region_name=REGION,
            endpoint_url="http://localhost:1",
            config=botocore.config.Config(
                retries={"total_max_attempts": 1},
                connect_timeout=1,
                read_timeout=1,
            ),
        )
        with pytest.raises(botocore.exceptions.EndpointConnectionError):
            client.list_tables()


class TestSubscriptionsTable:
    def test_partitioned_by_subscription(self, pool, dynamodb) -> None:
        described = dynamodb.describe_table(TableName=pool.subscriptions_table)["Table"]
        assert described["KeySchema"] == [
            {"AttributeName": schema.SUBSCRIPTION_ID, "KeyType": "HASH"}
        ]

    def test_state_index_projects_everything(self, pool, dynamodb) -> None:
        """Candidate selection must not need a second read to attempt a reservation."""
        described = dynamodb.describe_table(TableName=pool.subscriptions_table)["Table"]
        (index,) = described["GlobalSecondaryIndexes"]

        assert index["IndexName"] == schema.INDEX_BY_STATE
        assert index["Projection"]["ProjectionType"] == "ALL"
        assert index["KeySchema"] == [
            {"AttributeName": schema.STATE, "KeyType": "HASH"},
            {"AttributeName": schema.SUBSCRIPTION_ID, "KeyType": "RANGE"},
        ]

    def test_enabled_subscriptions_are_queryable_without_a_scan(
        self, pool, dynamodb
    ) -> None:
        enabled = pool.seed_subscriptions(2)
        pool.seed_subscription(state=schema.SubscriptionState.DISABLED)

        response = dynamodb.query(
            TableName=pool.subscriptions_table,
            IndexName=schema.INDEX_BY_STATE,
            KeyConditionExpression="#s = :state",
            ExpressionAttributeNames={"#s": schema.STATE},
            ExpressionAttributeValues={
                ":state": {"S": str(schema.SubscriptionState.ENABLED)}
            },
        )

        returned = {item[schema.SUBSCRIPTION_ID]["S"] for item in response["Items"]}
        assert returned == {s.subscription_id for s in enabled}


class TestLeasesTable:
    def test_partitioned_by_lease(self, pool, dynamodb) -> None:
        described = dynamodb.describe_table(TableName=pool.leases_table)["Table"]
        assert described["KeySchema"] == [
            {"AttributeName": schema.LEASE_ID, "KeyType": "HASH"}
        ]

    def test_subscription_index_projects_keys_only(self, pool, dynamodb) -> None:
        """Drift correction counts leases; it never reads their attributes."""
        described = dynamodb.describe_table(TableName=pool.leases_table)["Table"]
        (index,) = described["GlobalSecondaryIndexes"]

        assert index["IndexName"] == schema.INDEX_BY_SUBSCRIPTION
        assert index["Projection"]["ProjectionType"] == "KEYS_ONLY"

    def test_expiry_is_enforced_by_the_datastore(self, pool, dynamodb) -> None:
        """Recovery cannot depend on a process being alive to notice expiry."""
        ttl = dynamodb.describe_time_to_live(TableName=pool.leases_table)[
            "TimeToLiveDescription"
        ]
        assert ttl["TimeToLiveStatus"] == "ENABLED"
        assert ttl["AttributeName"] == schema.EXPIRES_AT

    def test_stream_carries_the_removed_image(self, pool, dynamodb) -> None:
        """The expiry handler reads the vanished lease's subscription from the old image."""
        described = dynamodb.describe_table(TableName=pool.leases_table)["Table"]
        assert described["StreamSpecification"] == {
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        }


class TestSeeding:
    def test_seeded_subscription_starts_with_nothing_held_or_used(self, pool) -> None:
        seeded = pool.seed_subscription()

        assert pool.active_leases(seeded.subscription_id) == 0
        assert pool.credits_used(seeded.subscription_id) == 0
        assert pool.lease_count(seeded.subscription_id) == 0

    def test_declared_limits_are_recorded(self, pool) -> None:
        seeded = pool.seed_subscription(max_concurrency=3, credit_budget=1_000)
        record = pool.subscription(seeded.subscription_id)

        assert int(record[schema.MAX_CONCURRENCY]) == 3
        assert int(record[schema.CREDIT_BUDGET]) == 1_000
        assert record[schema.STATE] == str(schema.SubscriptionState.ENABLED)

    def test_the_registry_holds_a_reference_and_never_the_key(self, pool) -> None:
        seeded = pool.seed_subscription()
        record = pool.subscription(seeded.subscription_id)

        assert record[schema.SECRET_REF] == seeded.secret_ref
        assert FAKE_KEY_MATERIAL_PREFIX not in str(record)

    def test_key_material_is_resolvable_from_the_reference(
        self, pool, secretsmanager
    ) -> None:
        seeded = pool.seed_subscription()

        resolved = secretsmanager.get_secret_value(SecretId=seeded.secret_ref)

        assert resolved["SecretString"] == seeded.key_material
        assert seeded.key_material.startswith(FAKE_KEY_MATERIAL_PREFIX)

    def test_a_subscription_can_be_seeded_without_a_resolvable_secret(
        self, pool, secretsmanager
    ) -> None:
        """Needed to exercise the rollback path when a secret cannot be resolved."""
        seeded = pool.seed_subscription(with_secret=False)

        with pytest.raises(secretsmanager.exceptions.ResourceNotFoundException):
            secretsmanager.get_secret_value(SecretId=seeded.secret_ref)

    def test_subscriptions_get_distinct_identifiers_and_keys(self, pool) -> None:
        seeded = pool.seed_subscriptions(5)

        assert len({s.subscription_id for s in seeded}) == 5
        assert len({s.key_material for s in seeded}) == 5

    def test_a_pool_can_be_seeded_already_saturated(self, pool) -> None:
        """Lets a test assert rejection at the ceiling without first claiming."""
        seeded = pool.seed_subscription(max_concurrency=2, active_leases=2)

        assert pool.active_leases(seeded.subscription_id) == 2


class TestEnvironmentContract:
    def test_table_names_come_from_the_environment(self, pool) -> None:
        assert schema.subscriptions_table_name() == pool.subscriptions_table
        assert schema.leases_table_name() == pool.leases_table

    @pytest.mark.parametrize(
        ("variable", "lookup"),
        [
            (schema.ENV_SUBSCRIPTIONS_TABLE, schema.subscriptions_table_name),
            (schema.ENV_LEASES_TABLE, schema.leases_table_name),
        ],
    )
    def test_a_missing_table_name_fails_loudly(
        self, pool, monkeypatch: pytest.MonkeyPatch, variable: str, lookup
    ) -> None:
        """Guessing a table name could point a worker at the wrong pool."""
        monkeypatch.delenv(variable)

        with pytest.raises(schema.MissingConfiguration, match=variable):
            lookup()
