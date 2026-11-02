"""How a task stopped, counted from the ECS event rather than from the task.

The events below are trimmed copies of what ECS sends to EventBridge for an `ECS Task State
Change` with `lastStatus` `STOPPED`. Only the fields the handler reads are kept.
"""

from __future__ import annotations

import io
import json
from typing import Any

import pytest

from leaseclient.metrics import Metrics
from runner.main import EXIT_CONFIGURATION, EXIT_ENVIRONMENT_BROKEN, EXIT_OK, EXIT_SATURATED
from scaling.handler import FLEET_NAMESPACE
from taskstopped.handler import TASK_STOPPED, StopReason, classify, handler, record


def stopped(stop_code: str | None, exit_code: int | None = None) -> dict[str, Any]:
    container: dict[str, Any] = {"name": "Worker", "lastStatus": "STOPPED"}
    if exit_code is not None:
        container["exitCode"] = exit_code
    detail: dict[str, Any] = {
        "taskArn": "arn:aws:ecs:us-east-1:123456789012:task/fleet/0123456789abcdef",
        "group": "service:worker",
        "lastStatus": "STOPPED",
        "desiredStatus": "STOPPED",
        "stoppedReason": "a free-text reason that is logged and never published",
        "containers": [container],
    }
    if stop_code is not None:
        detail["stopCode"] = stop_code
    return {
        "source": "aws.ecs",
        "detail-type": "ECS Task State Change",
        "detail": detail,
    }


class TestClassifying:
    @pytest.mark.parametrize("exit_code", [EXIT_OK, EXIT_SATURATED])
    def test_a_worker_that_exits_by_itself_cleanly_exited(self, exit_code: int) -> None:
        # A drained queue, a budget reached, or a pool that has no room: all three are the
        # worker choosing to stop, and all three are documented clean exits.
        assert classify(stopped("EssentialContainerExited", exit_code)) is StopReason.EXITED

    @pytest.mark.parametrize("exit_code", [1, EXIT_ENVIRONMENT_BROKEN, EXIT_CONFIGURATION, 139])
    def test_a_worker_that_exits_by_itself_with_an_error_crashed(self, exit_code: int) -> None:
        assert classify(stopped("EssentialContainerExited", exit_code)) is StopReason.CRASHED

    def test_an_exit_with_no_code_is_a_crash(self) -> None:
        # ECS reports no exit code when it could not read one. Counting that as clean would
        # hide exactly the stops nobody can explain.
        assert classify(stopped("EssentialContainerExited")) is StopReason.CRASHED

    @pytest.mark.parametrize("exit_code", [EXIT_OK, 143, None])
    def test_a_stop_by_the_service_scheduler_is_a_scale_in(self, exit_code: int | None) -> None:
        assert classify(stopped("ServiceSchedulerInitiated", exit_code)) is StopReason.SCALED_IN

    def test_a_scale_in_that_outlived_the_stop_timeout_was_killed(self) -> None:
        # SIGKILL after the stop timeout: the SIGTERM handler never finished, so the lease was
        # not returned and capacity comes back only through expiry.
        assert classify(stopped("ServiceSchedulerInitiated", 137)) is StopReason.KILLED

    def test_a_task_that_never_started_failed_to_start(self) -> None:
        assert classify(stopped("TaskFailedToStart")) is StopReason.FAILED_TO_START

    @pytest.mark.parametrize(
        "stop_code", ["UserInitiated", "SpotInterruption", "TerminationNotice", None, "Unknown"]
    )
    def test_anything_else_is_counted_rather_than_dropped(self, stop_code: str | None) -> None:
        assert classify(stopped(stop_code, EXIT_OK)) is StopReason.OTHER


class TestPublishing:
    def emitted(self, event: dict[str, Any]) -> dict[str, Any]:
        sink = io.StringIO()
        record(event, Metrics(namespace=FLEET_NAMESPACE, stream=sink))
        lines = [json.loads(line) for line in sink.getvalue().splitlines()]
        assert len(lines) == 1
        return lines[0]

    def test_one_count_per_stop_dimensioned_by_reason(self) -> None:
        line = self.emitted(stopped("EssentialContainerExited", 1))
        directive = line["_aws"]["CloudWatchMetrics"][0]

        assert directive["Namespace"] == FLEET_NAMESPACE
        assert directive["Dimensions"] == [["Reason"]]
        assert directive["Metrics"] == [{"Name": TASK_STOPPED, "Unit": "Count"}]
        assert line[TASK_STOPPED] == 1
        assert line["Reason"] == "Crashed"

    def test_the_stop_code_and_exit_code_travel_as_properties(self) -> None:
        line = self.emitted(stopped("ServiceSchedulerInitiated", 137))

        assert line["StopCode"] == "ServiceSchedulerInitiated"
        assert line["ExitCode"] == 137
        assert line["TaskId"] == "0123456789abcdef"

    def test_the_free_text_reason_is_not_published(self) -> None:
        line = self.emitted(stopped("TaskFailedToStart"))

        assert "free-text" not in json.dumps(line)
        assert "ExitCode" not in line

    def test_the_handler_counts_the_event_it_is_given(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        handler(stopped("EssentialContainerExited", EXIT_OK), None)

        lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line]
        published = [line for line in lines if TASK_STOPPED in line]
        assert [line["Reason"] for line in published] == ["Exited"]
