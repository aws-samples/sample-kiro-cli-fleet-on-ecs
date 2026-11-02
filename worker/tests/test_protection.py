"""The client for the ECS agent's task scale-in protection endpoint.

Against a local HTTP server that answers the way the agent does, including the two shapes
that are easy to misread: a refusal from ECS that arrives with status 200, and an agent error
that carries the ECS exception's code. What matters is that a confirmed request returns True,
that everything else returns False with one warning and one metric, and that nothing here
ever raises into the worker loop or calls anything without an agent.
"""

from __future__ import annotations

import io
import json
import logging
import socket
import threading
import time
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from runner.protection import (
    ENV_AGENT_URI,
    MAX_EXPIRY_MINUTES,
    TaskProtection,
    expires_in_minutes,
)
from runner.workflow import DEFAULT_TIMEOUT_SECONDS
from leaseclient.metrics import Metrics

TASK_ARN = "arn:aws:ecs:us-east-1:123456789012:task/test-cluster/test-task"
SENTINEL = "SENTINEL-RESPONSE-TEXT"


@dataclass
class Received:
    method: str
    path: str
    content_type: str | None
    body: Any


class FakeAgent:
    """Records each request and answers with whatever the test scripted.

    By default it answers as the agent does on success: the state that was asked for.
    """

    def __init__(self, port: int) -> None:
        self.uri = f"http://127.0.0.1:{port}/v4/test-container"
        self.requests: list[Received] = []
        self.release = threading.Event()
        self.reset()

    def reset(self) -> None:
        self.release.set()
        self.release = threading.Event()
        self.requests.clear()
        self.status: int | None = None
        self.body: bytes | None = None
        self.delay = 0.0

    def answer(self, status: int, body: Any, delay: float = 0.0) -> None:
        self.status = status
        self.body = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.delay = delay

    def respond_to(self, received: Received) -> tuple[int, bytes]:
        if self.status is not None and self.body is not None:
            return self.status, self.body
        wanted = bool(isinstance(received.body, dict) and received.body.get("ProtectionEnabled"))
        return 200, json.dumps(
            {
                "protection": {
                    "ExpirationDate": "2099-01-01T00:20:00Z" if wanted else None,
                    "ProtectionEnabled": wanted,
                    "TaskArn": TASK_ARN,
                }
            }
        ).encode()


class _Handler(BaseHTTPRequestHandler):
    server: "_Server"

    def do_PUT(self) -> None:  # noqa: N802
        agent = self.server.agent
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length)
        try:
            body: Any = json.loads(raw)
        except ValueError:
            body = raw
        received = Received("PUT", self.path, self.headers.get("Content-Type"), body)
        agent.requests.append(received)

        if agent.delay:
            agent.release.wait(agent.delay)
        status, payload = agent.respond_to(received)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    agent: FakeAgent

    def handle_error(self, request, client_address) -> None:
        # A client that gave up (the timeout test) leaves a broken pipe behind. Expected.
        pass


@pytest.fixture(scope="module")
def _agent_server():
    server = _Server(("127.0.0.1", 0), _Handler)
    server.agent = FakeAgent(server.server_address[1])
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.agent
    finally:
        server.agent.release.set()
        server.shutdown()
        server.server_close()


@pytest.fixture
def agent(_agent_server: FakeAgent) -> FakeAgent:
    _agent_server.reset()
    yield _agent_server
    _agent_server.release.set()


@pytest.fixture(autouse=True)
def no_agent_in_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV_AGENT_URI, raising=False)


@pytest.fixture
def sink() -> io.StringIO:
    return io.StringIO()


@pytest.fixture
def metrics(sink: io.StringIO) -> Metrics:
    return Metrics(stream=sink)


def failures(sink: io.StringIO) -> list[dict]:
    return [
        record
        for record in map(json.loads, sink.getvalue().splitlines())
        if "TaskProtectionFailed" in record
    ]


def warnings_from(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == "runner.protection" and record.levelno == logging.WARNING
    ]


def a_client(uri: str | None, metrics: Metrics, **kwargs) -> TaskProtection:
    return TaskProtection(agent_uri=uri, expires_in_minutes=20, metrics=metrics, **kwargs)


class TestTheExpiry:
    @pytest.mark.parametrize(
        ("timeout", "minutes"),
        [(900, 20), (901, 21), (1800, 35), (60, 6), (1, 6)],
    )
    def test_it_covers_the_workflow_timeout_and_a_buffer(self, timeout, minutes) -> None:
        assert expires_in_minutes(timeout) == minutes

    def test_the_shipped_timeout_protects_for_twenty_minutes(self) -> None:
        """Below the 22.5-minute visibility timeout, so a hung protected task is released
        before its message is redelivered."""
        assert expires_in_minutes(DEFAULT_TIMEOUT_SECONDS) == 20

    def test_it_stays_within_what_ecs_accepts(self) -> None:
        assert expires_in_minutes(10**7) == MAX_EXPIRY_MINUTES == 2880

    @pytest.mark.parametrize("timeout", [0, -1])
    def test_a_timeout_that_is_not_positive_is_refused(self, timeout) -> None:
        with pytest.raises(ValueError):
            expires_in_minutes(timeout)


class TestTheRequest:
    def test_enabling_puts_the_flag_and_the_expiry_to_the_agent(
        self, agent, metrics
    ) -> None:
        assert a_client(agent.uri, metrics).enable() is True

        (received,) = agent.requests
        assert received.method == "PUT"
        assert received.path == "/v4/test-container/task-protection/v1/state"
        assert received.content_type == "application/json"
        assert received.body == {"ProtectionEnabled": True, "ExpiresInMinutes": 20}

    def test_the_expiry_follows_the_configured_timeout(self, agent, metrics) -> None:
        protection = TaskProtection.from_environment(
            workflow_timeout_seconds=1800,
            metrics=metrics,
            environ={ENV_AGENT_URI: agent.uri},
        )

        assert protection.expires_in_minutes == 35
        assert protection.enable() is True
        assert agent.requests[0].body["ExpiresInMinutes"] == 35

    def test_disabling_sends_only_the_flag(self, agent, metrics) -> None:
        """The agent refuses unknown fields, so nothing else rides along."""
        assert a_client(agent.uri, metrics).disable() is True

        (received,) = agent.requests
        assert received.body == {"ProtectionEnabled": False}

    def test_a_confirmed_request_logs_no_warning_and_emits_nothing(
        self, agent, metrics, sink, caplog
    ) -> None:
        protection = a_client(agent.uri, metrics)

        with caplog.at_level(logging.DEBUG, logger="runner.protection"):
            assert protection.enable() is True
            assert protection.disable() is True

        assert warnings_from(caplog) == []
        assert sink.getvalue() == ""

    def test_the_agent_uri_is_read_from_the_process_environment(
        self, agent, metrics, monkeypatch
    ) -> None:
        monkeypatch.setenv(ENV_AGENT_URI, agent.uri)

        protection = TaskProtection.from_environment(
            workflow_timeout_seconds=DEFAULT_TIMEOUT_SECONDS, metrics=metrics
        )

        assert protection.available is True
        assert protection.enable() is True
        assert len(agent.requests) == 1

    def test_proxy_settings_are_ignored(self, agent, metrics, monkeypatch) -> None:
        """The agent is link-local. A proxy in the environment must not intercept it."""
        for name in ("no_proxy", "NO_PROXY"):
            monkeypatch.delenv(name, raising=False)
        for name in ("http_proxy", "HTTP_PROXY"):
            monkeypatch.setenv(name, "http://127.0.0.1:9")

        assert a_client(agent.uri, metrics).enable() is True
        assert len(agent.requests) == 1


class TestItFailsOpen:
    def test_an_error_status_is_one_warning_and_one_metric(
        self, agent, metrics, sink, caplog
    ) -> None:
        agent.answer(
            500,
            {
                "requestID": "test-request",
                "error": {
                    "Arn": TASK_ARN,
                    "Code": "AccessDeniedException",
                    "Message": "not authorized to perform: ecs:UpdateTaskProtection",
                },
            },
        )

        with caplog.at_level(logging.WARNING, logger="runner.protection"):
            assert a_client(agent.uri, metrics).enable() is False

        (warning,) = warnings_from(caplog)
        assert "AccessDeniedException" in warning.getMessage()
        assert "HTTP 500" in warning.getMessage()
        assert warning.exc_info is None
        (record,) = failures(sink)
        assert record["TaskProtectionFailed"] == 1
        assert record["Action"] == "Enable"
        assert record["Reason"] == "HttpError"
        assert record["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [[]]

    def test_a_refusal_inside_a_success_status_is_a_failure(
        self, agent, metrics, sink
    ) -> None:
        agent.answer(
            200, {"failure": {"Arn": TASK_ARN, "Detail": None, "Reason": "TASK_NOT_VALID"}}
        )

        assert a_client(agent.uri, metrics).enable() is False

        (record,) = failures(sink)
        assert record["Reason"] == "Refused"

    def test_deployment_blocked_is_counted_under_its_own_reason(
        self, agent, metrics, sink, caplog
    ) -> None:
        agent.answer(
            200,
            {"failure": {"Arn": TASK_ARN, "Detail": None, "Reason": "DEPLOYMENT_BLOCKED"}},
        )

        with caplog.at_level(logging.WARNING, logger="runner.protection"):
            assert a_client(agent.uri, metrics).enable() is False

        (record,) = failures(sink)
        assert record["Reason"] == "DeploymentBlocked"
        (warning,) = warnings_from(caplog)
        assert "DEPLOYMENT_BLOCKED" in warning.getMessage()

    @pytest.mark.parametrize("status,body", [
        (200, {"failure": {"Arn": TASK_ARN, "Detail": None, "Reason": "TASK_STOPPING_OR_STOPPED"}}),
        (400, {"error": {"Arn": TASK_ARN, "Code": "TASK_STOPPING_OR_STOPPED"}}),
    ])
    def test_a_task_being_stopped_is_reported_as_stopping(
        self, agent, metrics, sink, caplog, status, body
    ) -> None:
        """Seen on the fleet as a 200 with a failure object; the error form is read too."""
        agent.answer(status, body)
        client = a_client(agent.uri, metrics)
        assert client.stopping is False

        with caplog.at_level(logging.WARNING, logger="runner.protection"):
            assert client.enable() is False

        assert client.stopping is True
        (record,) = failures(sink)
        assert record["Reason"] == "TaskStopping"
        (warning,) = warnings_from(caplog)
        assert "not taking another item" in warning.getMessage()
        assert "unprotected" not in warning.getMessage()

    def test_other_refusals_do_not_mark_the_task_stopping(self, agent, metrics) -> None:
        agent.answer(
            200, {"failure": {"Arn": TASK_ARN, "Detail": None, "Reason": "TASK_NOT_VALID"}}
        )
        client = a_client(agent.uri, metrics)
        client.enable()
        assert client.stopping is False

    def test_deployment_blocked_as_an_agent_error_is_counted_the_same_way(
        self, agent, metrics, sink
    ) -> None:
        """Where ECS puts it is not documented, so either place counts."""
        agent.answer(500, {"error": {"Arn": TASK_ARN, "Code": "DEPLOYMENT_BLOCKED"}})

        assert a_client(agent.uri, metrics).enable() is False

        (record,) = failures(sink)
        assert record["Reason"] == "DeploymentBlocked"

    @pytest.mark.parametrize(
        "body",
        [
            {"protection": {"ProtectionEnabled": False, "TaskArn": TASK_ARN}},
            b"not json",
            {},
        ],
        ids=["the-other-state", "not-json", "no-protection-object"],
    )
    def test_a_response_that_does_not_confirm_the_state_is_a_failure(
        self, agent, metrics, sink, body
    ) -> None:
        agent.answer(200, body)

        assert a_client(agent.uri, metrics).enable() is False

        (record,) = failures(sink)
        assert record["Reason"] == "BadResponse"

    def test_an_agent_that_does_not_answer_in_time_is_abandoned(
        self, agent, metrics, sink
    ) -> None:
        agent.answer(200, {"protection": {"ProtectionEnabled": True}}, delay=2.0)

        started = time.monotonic()
        assert a_client(agent.uri, metrics, timeout_seconds=0.2).enable() is False
        assert time.monotonic() - started < 1.5

        (record,) = failures(sink)
        assert record["Reason"] == "Unreachable"

    def test_an_agent_that_is_not_listening_is_a_failure_not_an_exception(
        self, metrics, sink
    ) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]

        protection = a_client(f"http://127.0.0.1:{port}/v4/test-container", metrics)

        assert protection.enable() is False
        assert protection.disable() is False
        assert [r["Reason"] for r in failures(sink)] == ["Unreachable", "Unreachable"]

    def test_a_failed_disable_says_when_protection_lapses(
        self, agent, metrics, sink, caplog
    ) -> None:
        agent.answer(504, {"error": {"Arn": TASK_ARN, "Code": "RequestCanceled"}})

        with caplog.at_level(logging.WARNING, logger="runner.protection"):
            assert a_client(agent.uri, metrics).disable() is False

        (warning,) = warnings_from(caplog)
        assert "20 minutes" in warning.getMessage()
        assert "RequestCanceled" in warning.getMessage()
        (record,) = failures(sink)
        assert record["Action"] == "Disable"
        assert record["Reason"] == "HttpError"

    def test_the_warning_never_carries_the_response_text(
        self, agent, metrics, sink, caplog
    ) -> None:
        """The agent's message names the role and the account. Only the code is logged,
        and only its identifier characters."""
        agent.answer(
            500,
            {
                "error": {
                    "Arn": TASK_ARN,
                    "Code": "ThrottlingException <script>",
                    "Message": f"Rate exceeded {SENTINEL}",
                }
            },
        )

        with caplog.at_level(logging.DEBUG):
            assert a_client(agent.uri, metrics).enable() is False

        assert "ThrottlingExceptionscript" in caplog.text
        assert SENTINEL not in caplog.text
        assert "Rate exceeded" not in caplog.text
        assert "<script>" not in caplog.text
        assert TASK_ARN not in caplog.text
        assert SENTINEL not in sink.getvalue()
        assert TASK_ARN not in sink.getvalue()


class TestWithoutAnAgent:
    """A local run, or the container under plain Docker: nothing to call, nothing to say."""

    @pytest.fixture
    def recorder(self, monkeypatch) -> list:
        calls: list = []

        def record(self, *args, **kwargs):
            calls.append(args)
            raise AssertionError("no request should be made without an agent")

        monkeypatch.setattr(urllib.request.OpenerDirector, "open", record)
        return calls

    @pytest.mark.parametrize("environ", [{}, {ENV_AGENT_URI: ""}], ids=["absent", "empty"])
    def test_no_agent_uri_means_no_request_no_metric_and_no_warning(
        self, metrics, sink, caplog, recorder, environ
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger="runner.protection"):
            protection = TaskProtection.from_environment(
                workflow_timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
                metrics=metrics,
                environ=environ,
            )
            assert protection.available is False
            assert protection.enable() is False
            assert protection.disable() is False

        assert recorder == []
        assert sink.getvalue() == ""
        assert warnings_from(caplog) == []

    def test_the_process_environment_without_an_agent_is_the_same(
        self, metrics, sink, recorder
    ) -> None:
        protection = TaskProtection.from_environment(
            workflow_timeout_seconds=DEFAULT_TIMEOUT_SECONDS, metrics=metrics
        )

        assert protection.enable() is False
        assert protection.disable() is False
        assert recorder == []
        assert sink.getvalue() == ""
