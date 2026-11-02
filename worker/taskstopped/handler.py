"""How each of the fleet's tasks stopped, counted from outside the task.

A worker publishes its metrics while its process is alive, so a task that crashes or is killed
cannot report its own end. Without this, an abnormal stop is visible only indirectly, as
capacity the reconciler had to recover (`LeasesRecovered`), and a fleet whose tasks crash looks
the same as a fleet that scales in normally.

ECS reports every stop to EventBridge as an `ECS Task State Change` with `lastStatus` `STOPPED`.
A rule scoped to the worker service sends those events here, and this function publishes one
`TaskStopped` count per event, dimensioned by why the task stopped. The stop code and exit code
travel as properties so a log query can tell which case it was; ECS's free-text
`stoppedReason` is logged and not published, because a metric field carries identifiers only.

EventBridge delivers at least once, so a retried delivery can count a stop twice. The figure is
a distribution to look at, not a count anything reconciles against.
"""

from __future__ import annotations

import logging
import os
from enum import StrEnum
from typing import Any, Final

from leaseclient.lambda_logging import configure_logging
from leaseclient.metrics import ENV_NAMESPACE, Metrics, MetricUnit
from runner.main import EXIT_OK, EXIT_SATURATED
from scaling.handler import FLEET_NAMESPACE

logger = logging.getLogger(__name__)

TASK_STOPPED: Final = "TaskStopped"
"""Duplicated in `infra/lib/config.ts`; the agreement test asserts the two match."""

CLEAN_EXITS: Final = frozenset({EXIT_OK, EXIT_SATURATED})
"""The worker's own exit codes for stopping on purpose: drained, budget reached, or no room."""

SIGKILL_EXIT: Final = 137
"""128 + 9. The container did not exit within the stop timeout after SIGTERM."""


class StopReason(StrEnum):
    """Why a task stopped. `Crashed`, `Killed` and `FailedToStart` are the abnormal ones."""

    EXITED = "Exited"
    """The worker exited by itself with a clean exit code."""
    SCALED_IN = "ScaledIn"
    """The service scheduler stopped the task (scale-in or a deployment) and it exited in time."""
    KILLED = "Killed"
    """A stop outlived the stop timeout and ended in SIGKILL; the lease was not returned."""
    CRASHED = "Crashed"
    """The worker exited by itself with an error, or with no exit code ECS could read."""
    FAILED_TO_START = "FailedToStart"
    OTHER = "Other"
    """Any other stop code: an operator stop, a Spot interruption, or a code added later."""


def _exit_code(detail: dict[str, Any]) -> int | None:
    """The worker container's exit code. The task definition has one container."""
    for container in detail.get("containers") or []:
        if "exitCode" in container:
            return int(container["exitCode"])
    return None


def classify(event: dict[str, Any]) -> StopReason:
    detail = event.get("detail") or {}
    stop_code = detail.get("stopCode")
    exit_code = _exit_code(detail)

    if stop_code == "TaskFailedToStart":
        return StopReason.FAILED_TO_START
    if stop_code == "EssentialContainerExited":
        return StopReason.EXITED if exit_code in CLEAN_EXITS else StopReason.CRASHED
    if stop_code == "ServiceSchedulerInitiated":
        return StopReason.KILLED if exit_code == SIGKILL_EXIT else StopReason.SCALED_IN
    return StopReason.OTHER


def record(event: dict[str, Any], metrics: Metrics) -> StopReason:
    detail = event.get("detail") or {}
    reason = classify(event)
    exit_code = _exit_code(detail)

    properties: dict[str, Any] = {}
    if detail.get("stopCode"):
        properties["StopCode"] = detail["stopCode"]
    if exit_code is not None:
        properties["ExitCode"] = exit_code
    if detail.get("taskArn"):
        properties["TaskId"] = str(detail["taskArn"]).rsplit("/", 1)[-1]

    metrics.emit(TASK_STOPPED, 1, MetricUnit.COUNT, {"Reason": str(reason)}, properties)
    logger.info(
        "task %s stopped: %s (stopCode=%s, exitCode=%s, reason=%s)",
        properties.get("TaskId"),
        reason,
        detail.get("stopCode"),
        exit_code,
        detail.get("stoppedReason"),
    )
    return reason


def handler(event: dict[str, Any], _context: Any) -> dict[str, str]:
    configure_logging()
    metrics = Metrics(namespace=os.environ.get(ENV_NAMESPACE) or FLEET_NAMESPACE)
    return {"reason": str(record(event, metrics))}
