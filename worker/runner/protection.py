"""Scale-in protection for the item in progress, through the ECS agent.

A task that is stopped in the middle of a run loses that run. Its credits are already
spent, its message is redelivered after the visibility timeout, and the item is paid for
twice. Several things stop tasks while work is in flight: a lower ceiling from the scaling
function when a budget runs out, target tracking scaling in, a drain to zero, and a
deployment. ECS chooses which tasks to stop without regard to the subscription they hold
or the item they are on. Task scale-in protection is how a task tells ECS "not me, not
now": a protected task is left alone by service scale-in and by deployments until it
clears the flag or the flag expires. So a worker protects itself for the length of one
item and clears the flag between items, and ECS stops it only between items.

The ECS agent serves the flag at `$ECS_AGENT_URI/task-protection/v1/state`. A `PUT` with
`{"ProtectionEnabled": true, "ExpiresInMinutes": N}` sets it, and the agent calls
`UpdateTaskProtection` with the task role. The answer needs reading with care: a refusal
from ECS (for example `TASK_NOT_VALID`) also comes back as HTTP 200, with a `failure`
object in place of `protection`. A request counts as done only when the agent confirms the
state that was asked for.

Fail-open throughout, with one exception. Protection is an improvement over how every item
ran before it, so it is never a reason not to run an item. With no `ECS_AGENT_URI` (a local
run, plain Docker) nothing is called. A request that fails, times out, or is refused logs one
warning, emits `TaskProtectionFailed`, and the item runs unprotected.

The exception is a refusal with `TASK_STOPPING_OR_STOPPED`. That one is not a failed
request but an answer: ECS has already started stopping this task, so an item received now
is lost when the container is killed, and its message sits out the visibility timeout. The
client records it in `stopping`, and the worker ends its drain instead of receiving.
Measured on the M2 benchmark (bench/README.md, M4): before this, 18 such refusals in one
level led to 12 items stopped mid-run and a 22-minute wait for redelivery.

The expiry is the workflow timeout rounded up to minutes, plus a buffer for downloading
inputs, storing results and the receive around the run. A worker that hangs while
protected therefore becomes eligible for scale-in again before its message is redelivered.

Not covered: `StopTask`, Fargate task retirement, and crashes. Those still stop a task
mid-item, and the item reappears after the visibility timeout as before.

https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection.html
https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection-endpoint.html
"""

from __future__ import annotations

import http.client
import json
import logging
import math
import os
import re
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any, Final, Protocol

from leaseclient.metrics import Metrics, ProtectionAction, ProtectionFailure

logger = logging.getLogger(__name__)

ENV_AGENT_URI: Final = "ECS_AGENT_URI"
STATE_PATH: Final = "/task-protection/v1/state"

EXPIRY_BUFFER_MINUTES: Final = 5
"""Added to the workflow timeout: inputs, results, transcript and the long poll."""

MIN_EXPIRY_MINUTES: Final = 1
MAX_EXPIRY_MINUTES: Final = 2880
"""What `ExpiresInMinutes` accepts: 1 minute to 48 hours."""

DEFAULT_REQUEST_TIMEOUT_SECONDS: Final = 6.0
"""Just above the agent's own 5 s response deadline, so its error arrives before ours."""

MAX_RESPONSE_BYTES: Final = 64 * 1024

DEPLOYMENT_BLOCKED: Final = "DEPLOYMENT_BLOCKED"
TASK_STOPPING: Final = "TASK_STOPPING_OR_STOPPED"

_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]")
_MAX_TOKEN_LENGTH: Final = 64


def expires_in_minutes(workflow_timeout_seconds: float) -> int:
    """How long one item's protection lasts: the run's timeout, plus the buffer."""
    if workflow_timeout_seconds <= 0:
        raise ValueError(
            f"the workflow timeout must be positive, got {workflow_timeout_seconds}"
        )
    minutes = math.ceil(workflow_timeout_seconds / 60) + EXPIRY_BUFFER_MINUTES
    return min(max(minutes, MIN_EXPIRY_MINUTES), MAX_EXPIRY_MINUTES)


class ScaleInProtection(Protocol):
    """What the worker loop needs. Both calls return whether ECS confirmed the state.

    `stopping` is true once ECS has answered that this task is already being stopped; the
    worker then takes no further item.
    """

    stopping: bool

    def enable(self) -> bool: ...

    def disable(self) -> bool: ...


class TaskProtection:
    """Sets and clears this task's scale-in protection through the ECS agent."""

    def __init__(
        self,
        agent_uri: str | None,
        expires_in_minutes: int,
        metrics: Metrics,
        timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        opener: urllib.request.OpenerDirector | None = None,
    ) -> None:
        self._url = agent_uri.rstrip("/") + STATE_PATH if agent_uri else None
        self._expires_in_minutes = expires_in_minutes
        self._metrics = metrics
        self._timeout = timeout_seconds
        self.stopping = False
        # No proxy. The agent is on a link-local address, and a stray `http_proxy` in the
        # image must not send this request somewhere else.
        self._opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}))

    @classmethod
    def from_environment(
        cls,
        workflow_timeout_seconds: float,
        metrics: Metrics,
        environ: Mapping[str, str] | None = None,
        timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
    ) -> "TaskProtection":
        environ = os.environ if environ is None else environ
        protection = cls(
            agent_uri=environ.get(ENV_AGENT_URI) or None,
            expires_in_minutes=expires_in_minutes(workflow_timeout_seconds),
            metrics=metrics,
            timeout_seconds=timeout_seconds,
        )
        if protection.available:
            logger.info(
                "scale-in protection through the ECS agent, %d minutes per item",
                protection.expires_in_minutes,
            )
        else:
            logger.debug("%s is not set; scale-in protection is off", ENV_AGENT_URI)
        return protection

    @property
    def available(self) -> bool:
        return self._url is not None

    @property
    def expires_in_minutes(self) -> int:
        return self._expires_in_minutes

    def enable(self) -> bool:
        return self._put(
            ProtectionAction.ENABLE,
            {"ProtectionEnabled": True, "ExpiresInMinutes": self._expires_in_minutes},
        )

    def disable(self) -> bool:
        return self._put(ProtectionAction.DISABLE, {"ProtectionEnabled": False})

    def _put(self, action: ProtectionAction, body: dict[str, Any]) -> bool:
        """One request. Never raises: every failure is a warning and a metric."""
        if self._url is None:
            return False

        request = urllib.request.Request(
            self._url,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="PUT",
        )
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                status = response.status
                payload = response.read(MAX_RESPONSE_BYTES)
        except urllib.error.HTTPError as exc:
            # Before URLError, which it subclasses. The agent's own errors (no task role
            # credentials, an ECS exception, its call to ECS timing out) arrive here.
            code = _token(_field(_error_body(exc), "error", "Code"))
            return self._failed(action, _reason(code, ProtectionFailure.HTTP_ERROR),
                                f"HTTP {exc.code} {code}")
        except urllib.error.URLError as exc:
            cause = exc.reason if isinstance(exc.reason, BaseException) else exc
            return self._failed(action, ProtectionFailure.UNREACHABLE, type(cause).__name__)
        except (http.client.HTTPException, OSError) as exc:
            return self._failed(action, ProtectionFailure.UNREACHABLE, type(exc).__name__)
        except Exception as exc:
            return self._failed(action, ProtectionFailure.BAD_RESPONSE, type(exc).__name__)

        return self._confirmed(action, body["ProtectionEnabled"], status, payload)

    def _confirmed(
        self, action: ProtectionAction, wanted: bool, status: int, payload: bytes
    ) -> bool:
        if not 200 <= status < 300:
            return self._failed(action, ProtectionFailure.HTTP_ERROR, f"HTTP {status}")
        try:
            document = json.loads(payload)
        except ValueError:
            return self._failed(
                action, ProtectionFailure.BAD_RESPONSE, f"HTTP {status} not JSON"
            )

        failure = document.get("failure") if isinstance(document, dict) else None
        if isinstance(failure, dict):
            # A refusal from ECS, delivered with a success status.
            code = _token(failure.get("Reason"))
            return self._failed(action, _reason(code, ProtectionFailure.REFUSED),
                                f"HTTP {status} {code}")

        if _field(document, "protection", "ProtectionEnabled") is not wanted:
            return self._failed(
                action, ProtectionFailure.BAD_RESPONSE, f"HTTP {status} unconfirmed"
            )

        logger.debug("scale-in protection %s", "on" if wanted else "off")
        return True

    def _failed(
        self, action: ProtectionAction, reason: ProtectionFailure, detail: str
    ) -> bool:
        # The reason and a sanitised status line only: never the response body, which can
        # carry role and account details, and no traceback.
        if reason is ProtectionFailure.TASK_STOPPING:
            self.stopping = True
            logger.warning(
                "ECS is already stopping this task (%s); not taking another item", detail
            )
        elif action is ProtectionAction.ENABLE:
            logger.warning(
                "could not turn on scale-in protection (%s: %s); running the item unprotected",
                reason,
                detail,
            )
        else:
            logger.warning(
                "could not turn off scale-in protection (%s: %s); it lapses on its own "
                "within %d minutes",
                reason,
                detail,
                self._expires_in_minutes,
            )
        self._metrics.task_protection_failed(action, reason)
        return False


def _reason(code: str, otherwise: ProtectionFailure) -> ProtectionFailure:
    """Map an ECS code to a reason. Where ECS puts a code is not documented, so both the
    agent's error body and a refusal inside a success status are read the same way."""
    if code == DEPLOYMENT_BLOCKED:
        return ProtectionFailure.DEPLOYMENT_BLOCKED
    if code == TASK_STOPPING:
        return ProtectionFailure.TASK_STOPPING
    return otherwise


def _error_body(exc: urllib.error.HTTPError) -> Any:
    try:
        return json.loads(exc.read(MAX_RESPONSE_BYTES))
    except Exception:
        return None


def _field(document: Any, outer: str, inner: str) -> Any:
    section = document.get(outer) if isinstance(document, dict) else None
    return section.get(inner) if isinstance(section, dict) else None


def _token(value: Any) -> str:
    """An error code fit for a log line: short, and nothing but identifier characters."""
    if not isinstance(value, str) or not value:
        return "-"
    return _UNSAFE.sub("", value)[:_MAX_TOKEN_LENGTH] or "-"
