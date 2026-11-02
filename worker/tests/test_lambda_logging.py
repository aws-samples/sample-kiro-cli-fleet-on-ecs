"""The functions' INFO logs reach the handler the Lambda runtime installed.

Deployed, the reconciler ran nearly two hundred times and its log group held not one
result line. The runtime adds a handler to the root logger before the function's code
is imported and, with the plain-text log format, leaves the root level at WARNING. A
handler that never sets the level, or sets it with `basicConfig` (which does nothing
once a handler exists), drops every INFO record before any handler sees it.

These tests recreate that condition: a handler already on the root logger, the root at
WARNING. Then each function's real entry point runs against fakes, and its own result
line has to arrive.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pytest

import reconciler.handler as reconciler_handler
import scaling.handler as scaling_handler
import seeding.handler as seeding_handler
import usagereport.handler as usagereport_handler
from leaseclient.lambda_logging import configure_logging
from leaseclient.registry import SubscriptionRecord, SubscriptionState
from reconciler.handler import ReconcileResult
from usagereport.handler import IngestResult


class RecordingHandler(logging.Handler):
    """Stands in for the runtime's handler, which writes what it receives to CloudWatch."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def messages(self) -> list[str]:
        return [record.getMessage() for record in self.records]


@pytest.fixture
def lambda_root_logger(monkeypatch: pytest.MonkeyPatch):
    """The root logger as the runtime leaves it before the handler runs."""
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    root = logging.getLogger()
    level = root.level
    runtime = RecordingHandler()
    root.addHandler(runtime)
    root.setLevel(logging.WARNING)
    yield runtime
    root.removeHandler(runtime)
    root.setLevel(level)


class TestEachFunctionLogsItsResult:
    def test_the_reconciler(
        self, lambda_root_logger: RecordingHandler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class FakeReconciler:
            def reconcile(self) -> ReconcileResult:
                return ReconcileResult(corrected_subscriptions=1)

        monkeypatch.setattr(reconciler_handler, "_from_environment", FakeReconciler)

        reconciler_handler.handler({"source": "aws.events"})

        assert any(
            m.startswith("reconciliation result:") for m in lambda_root_logger.messages()
        )

    def test_the_seeder(
        self, lambda_root_logger: RecordingHandler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        manifest = {"items": [{"prompt": "do a"}]}

        class FakeS3:
            def get_object(self, **_kwargs: Any) -> dict[str, Any]:
                class Body:
                    def read(self) -> bytes:
                        return json.dumps(manifest).encode()

                return {"Body": Body()}

        class FakeSqs:
            def send_message_batch(self, **_kwargs: Any) -> dict[str, Any]:
                return {}

        clients = {"s3": FakeS3(), "sqs": FakeSqs()}
        monkeypatch.setattr(seeding_handler.boto3, "client", lambda name: clients[name])
        monkeypatch.setenv("WORK_QUEUE_URL", "q")

        seeding_handler.handler({"bucket": "b", "key": "manifest.json"})

        assert any(m.startswith("seeded ") for m in lambda_root_logger.messages())

    def test_the_scaling_signal(
        self, lambda_root_logger: RecordingHandler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class FakeSqs:
            def get_queue_attributes(self, **_kwargs: Any) -> dict[str, Any]:
                return {
                    "Attributes": {
                        "ApproximateNumberOfMessages": "3",
                        "ApproximateNumberOfMessagesNotVisible": "0",
                    }
                }

        class FakeEcs:
            def describe_services(self, **_kwargs: Any) -> dict[str, Any]:
                return {"services": [{"runningCount": 1, "desiredCount": 1}]}

        class FakeAutoscaling:
            def register_scalable_target(self, **_kwargs: Any) -> None:
                return None

            def describe_scalable_targets(self, **_kwargs: Any) -> dict[str, Any]:
                return {"ScalableTargets": []}

        class FakeRegistry:
            def __init__(self, _dynamodb: Any) -> None:
                pass

            def list_enabled(self) -> list[SubscriptionRecord]:
                return [
                    SubscriptionRecord(
                        subscription_id="sub-00",
                        secret_ref="arn:aws:secretsmanager:us-east-1:123456789012:secret:x-AbCdEf",
                        max_concurrency=2,
                        credit_budget=1000,
                        state=SubscriptionState.ENABLED,
                    )
                ]

        clients = {
            "sqs": FakeSqs(),
            "ecs": FakeEcs(),
            "dynamodb": object(),
            "application-autoscaling": FakeAutoscaling(),
        }
        monkeypatch.setattr(scaling_handler.boto3, "client", lambda name: clients[name])
        monkeypatch.setattr(scaling_handler, "Registry", FakeRegistry)
        for name, value in {
            "WORK_QUEUE_URL": "q",
            "CLUSTER_NAME": "cluster",
            "SERVICE_NAME": "service",
            "MAX_TASKS": "4",
            "TARGET_BACKLOG_PER_TASK": "2",
        }.items():
            monkeypatch.setenv(name, value)

        scaling_handler.handler({})

        assert any(m.startswith("fleet signal ") for m in lambda_root_logger.messages())


    def test_the_activity_report_ingester(
        self, lambda_root_logger: RecordingHandler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class FakeIngester:
            def run(self) -> IngestResult:
                return IngestResult(subscriptions_raised=1, files_read=3)

        monkeypatch.setattr(usagereport_handler, "_from_environment", FakeIngester)

        result = usagereport_handler.handler({"source": "aws.events"})

        assert result["filesRead"] == 3
        assert any(
            m.startswith("activity report result:") for m in lambda_root_logger.messages()
        )


class TestTheSetup:
    def test_the_runtime_handler_is_kept_rather_than_replaced(
        self, lambda_root_logger: RecordingHandler
    ) -> None:
        # Replacing it (`basicConfig(force=True)`) would also make INFO appear, but the
        # lines would lose the request id the runtime's handler adds.
        configure_logging()

        assert lambda_root_logger in logging.getLogger().handlers

    def test_a_warm_invocation_changes_nothing(
        self, lambda_root_logger: RecordingHandler
    ) -> None:
        configure_logging()
        handlers = list(logging.getLogger().handlers)

        configure_logging()

        assert logging.getLogger().handlers == handlers
        assert logging.getLogger().level == logging.INFO

    def test_the_level_follows_the_environment(
        self, lambda_root_logger: RecordingHandler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        configure_logging()

        assert logging.getLogger().level == logging.DEBUG
