"""What a task runs.

Reads its configuration from the environment, claims a subscription, drains the queue, and
exits with a status that says which of several normal things happened. The loop, allocator,
and completion check are the ones the tests cover; only the queue and store differ from a
local run.

Exit statuses are distinct because scale-in makes several outcomes normal and a fleet
cannot tell them apart from a crash otherwise. A task that exits non-zero on saturation is
replaced and tries again, which turns a pool smaller than the fleet into a loop.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Final

import boto3

from leaseclient import schema
from leaseclient.claim import Allocator, ClaimConfig, Leases, parse_credit_margin
from leaseclient.metrics import Metrics
from leaseclient.registry import Registry
from leaseclient.session import PoolSaturated, hold_subscription
from runner.aws import S3ResultStore, SqsQueue
from runner.definition import InvalidWorkflow, WorkflowDefinition, seed_cli_configuration
from runner.protection import TaskProtection
from runner.usage import DEFAULT_USAGE_TIMEOUT_SECONDS, UsageProbe
from runner.visibility import DEFAULT_WINDOW_SECONDS, VISIBILITY_HOLD_MARGIN
from runner.worker import Worker
from runner.workflow import DEFAULT_TIMEOUT_SECONDS, Verdict, run_workflow

logger = logging.getLogger(__name__)

EXIT_OK: Final = 0
EXIT_SATURATED: Final = 64
EXIT_ENVIRONMENT_BROKEN: Final = 70
EXIT_CONFIGURATION: Final = 78


class MissingConfiguration(RuntimeError):
    """A required environment variable is absent."""


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise MissingConfiguration(
            f"{name} is not set. It is supplied by the task definition."
        )
    return value


@dataclass(frozen=True)
class Settings:
    subscriptions_table: str
    leases_table: str
    queue_url: str
    results_bucket: str
    workflow_dir: Path
    reference_bucket: str | None
    workspace_root: Path
    home: Path
    results_prefix: str
    lease_lifetime: timedelta
    credit_margin: Decimal
    retry_delay_seconds: float
    usage_timeout_seconds: float
    visibility_window_seconds: int
    @classmethod
    def from_environment(cls) -> "Settings":
        return cls(
            subscriptions_table=required(schema.ENV_SUBSCRIPTIONS_TABLE),
            leases_table=required(schema.ENV_LEASES_TABLE),
            queue_url=required("WORK_QUEUE_URL"),
            results_bucket=required("RESULTS_BUCKET"),
            workflow_dir=Path(required("WORKFLOW_DIR")),
            reference_bucket=os.environ.get("REFERENCE_BUCKET") or None,
            workspace_root=Path(os.environ.get("WORKSPACE_ROOT", "/workspace")),
            home=Path(os.environ.get("HOME", "/home/worker")),
            results_prefix=os.environ.get("RESULTS_PREFIX", "results"),
            lease_lifetime=timedelta(
                seconds=int(os.environ.get("LEASE_LIFETIME_SECONDS", "120"))
            ),
            # Fractions allowed: `/usage` reports credits to 0.01.
            credit_margin=parse_credit_margin(os.environ.get("CREDIT_MARGIN")),
            retry_delay_seconds=float(os.environ.get("RETRY_DELAY_SECONDS", "30")),
            usage_timeout_seconds=float(
                os.environ.get(
                    "USAGE_TIMEOUT_SECONDS", str(DEFAULT_USAGE_TIMEOUT_SECONDS)
                )
            ),
            # Set by the infrastructure from the queue's own visibility timeout, so the
            # window the worker renews and the one the queue starts with are one value.
            visibility_window_seconds=int(
                os.environ.get("VISIBILITY_WINDOW_SECONDS", str(DEFAULT_WINDOW_SECONDS))
            ),
        )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(levelname)-7s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    try:
        settings = Settings.from_environment()
    except (MissingConfiguration, schema.MissingConfiguration, ValueError) as exc:
        logger.error("%s", exc)
        return EXIT_CONFIGURATION

    try:
        definition = WorkflowDefinition.load(settings.workflow_dir)
    except InvalidWorkflow as exc:
        # The image is wrong, not the work. Distinct from a configuration error only in
        # that it is about the image's contents rather than the task definition.
        logger.error("workflow definition unusable: %s", exc)
        return EXIT_CONFIGURATION

    # Before the CLI starts, because this is where it looks.
    seed_cli_configuration(settings.home, definition)

    metrics = Metrics()
    dynamodb = boto3.client("dynamodb")
    allocator = Allocator(
        registry=Registry(dynamodb, settings.subscriptions_table),
        leases=Leases(dynamodb, settings.leases_table),
        secrets=boto3.client("secretsmanager"),
        metrics=metrics,
        config=ClaimConfig(
            lease_lifetime=settings.lease_lifetime,
            credit_margin=settings.credit_margin,
        ),
    )

    queue = SqsQueue(boto3.client("sqs"), settings.queue_url)
    store = S3ResultStore(
        boto3.client("s3"),
        bucket=settings.results_bucket,
        prefix=settings.results_prefix,
        reference_bucket=settings.reference_bucket,
    )

    # One figure: a run is cut off at it, and scale-in protection for an item outlasts it.
    workflow_timeout = DEFAULT_TIMEOUT_SECONDS

    def run(**kwargs):
        """Everything the workflow declares, supplied in one place.

        The worker loop deliberately knows nothing about the tool grant. It used to accept its
        own `trust_tools` as well, which gave one value two owners, and the two met as a
        duplicate keyword that only appeared on a task: the local runner dropped one of them
        before calling, so local runs passed and the first deployed task raised a TypeError
        after claiming a subscription.
        """
        return run_workflow(
            trust_tools=definition.trust_tools_argument,
            expects_stdout=definition.expects_stdout,
            definition=definition,
            timeout_seconds=workflow_timeout,
            **kwargs,
        )

    try:
        with hold_subscription(
            allocator,
            dynamodb,
            leases_table=settings.leases_table,
        ) as session:
            report = Worker(
                session=session,
                queue=queue,
                results=store,
                registry=Registry(dynamodb, settings.subscriptions_table),
                metrics=metrics,
                workspace=settings.workspace_root,
                retry_delay_seconds=settings.retry_delay_seconds,
                visibility_window_seconds=settings.visibility_window_seconds,
                # A run is cut off at the workflow timeout; holding an item much longer
                # than that means this worker is stuck, so the item is let go.
                visibility_max_hold_seconds=workflow_timeout * VISIBILITY_HOLD_MARGIN,
                run=run,
                # Read before every item and, best-effort, when the drain ends, so
                # `creditsUsed` follows the server's cumulative figure between claims.
                usage=UsageProbe(
                    scratch=settings.workspace_root / ".usage",
                    timeout_seconds=settings.usage_timeout_seconds,
                ),
                credit_margin=settings.credit_margin,
                # Through the ECS agent while an item is held; a no-op without one.
                protection=TaskProtection.from_environment(
                    workflow_timeout_seconds=workflow_timeout, metrics=metrics
                ),
            ).drain()
    except PoolSaturated:
        # The fleet outgrew the pool. A clean exit, so autoscaling is not fighting a task
        # that keeps being replaced.
        logger.info("no subscription available; exiting without work")
        return EXIT_SATURATED

    logger.info(
        "finished: %d completed, %d failed, %d handed back",
        report.completed,
        report.failed,
        report.retried,
    )
    if report.budget_exhausted:
        # A normal exit. The lease is released and the remaining items are still in
        # the queue for a subscription with budget left.
        logger.info(
            "stopped early: %s reached its credit ceiling", report.subscription_id
        )

    if Verdict.STARTUP_FAILED in report.verdicts:
        # No item will run in this image. Reported distinctly so a broken deployment looks
        # like one instead of like a queue of mysteriously failing work.
        logger.error("the workflow environment failed to start")
        return EXIT_ENVIRONMENT_BROKEN

    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
