"""Reading a subscription's cumulative credit consumption from the Kiro CLI.

`kiro-cli chat --no-interactive "/usage"` prints the subscription's month-to-date
figure, for every client using that subscription, as prose:

    Estimated Usage | resets on 2026-11-01 | KIRO POWER
    Credits (12.97 of 10000 covered in plan)

Measured on Kiro CLI 2.21.4 (default engine): the text goes to stderr with ANSI
colour, except in one of six runs, where it went to stdout without it. The newer
engine prints `Credits (12.80 of 10000 covered in plan), 0.1%` on stdout. No form is
structured: `--output-format json` is rejected, and stream-json carries the same
prose inside a text field. So both streams are merged, ANSI is stripped, and one
strict pattern is matched.

The exit code says nothing. With the backend unreachable the command printed a
warning and no Credits line, after about 22 seconds, and exited 0. A missing Credits
line is therefore a failed reading, never a reading of zero.
"""

from __future__ import annotations

import logging
import re
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Final, Protocol

from runner.workflow import _LONG_TOKEN, _child_environment, strip_ansi

logger = logging.getLogger(__name__)

DEFAULT_USAGE_TIMEOUT_SECONDS: Final = 30.0
"""Covers the slowest success measured on Fargate (9.1 s) and the failure path (21.9 s)."""

_CREDITS_LINE = re.compile(
    r"Credits \(([0-9]+(?:\.[0-9]+)?) of ([0-9]+(?:\.[0-9]+)?) covered in plan\)"
)
_RESETS_ON = re.compile(r"resets on (\d{4})-(\d{2})-(\d{2})")

_EXCERPT_LENGTH: Final = 200


class UsageFailure(StrEnum):
    """Why a reading produced no figure. Each is a failure, none is zero."""

    NO_CREDITS_LINE = "NoCreditsLine"
    NO_RESET_DATE = "NoResetDate"
    TIMED_OUT = "TimedOut"
    NOT_LAUNCHED = "NotLaunched"


@dataclass(frozen=True)
class UsageReading:
    """One parsed `/usage` figure.

    `resets_at` is the reset date read as UTC midnight. That the server rolls over at
    UTC midnight is an assumption, not a measurement; a reading taken in a gap
    between the two is rejected as stale by the registry, which is the safe side.
    """

    used: Decimal
    plan_limit: Decimal
    resets_at: datetime


class UsageReader(Protocol):
    def read(self, api_key: str) -> UsageReading | UsageFailure: ...


def parse_usage(text: str) -> UsageReading | UsageFailure:
    """Parse `/usage` output from either stream, either engine."""
    clean = strip_ansi(text)
    credits = _CREDITS_LINE.search(clean)
    if credits is None:
        return UsageFailure.NO_CREDITS_LINE
    resets = _RESETS_ON.search(clean)
    if resets is None:
        return UsageFailure.NO_RESET_DATE
    try:
        used = Decimal(credits.group(1))
        limit = Decimal(credits.group(2))
        year, month, day = (int(part) for part in resets.groups())
        resets_at = datetime(year, month, day, tzinfo=UTC)
    except (InvalidOperation, ValueError):
        return UsageFailure.NO_CREDITS_LINE
    return UsageReading(used=used, plan_limit=limit, resets_at=resets_at)


class UsageProbe:
    """Runs `/usage` with a subscription's key and parses the result.

    The key goes to the child's environment only, the same way a workflow run gets
    it. HOME is the ambient one, as it was in the Fargate measurement; `scratch` is
    the working directory and TMPDIR.
    """

    def __init__(
        self,
        scratch: Path,
        executable: str = "kiro-cli",
        timeout_seconds: float = DEFAULT_USAGE_TIMEOUT_SECONDS,
    ) -> None:
        self._scratch = Path(scratch)
        self._executable = executable
        self._timeout = timeout_seconds

    def read(self, api_key: str) -> UsageReading | UsageFailure:
        (self._scratch / "tmp").mkdir(parents=True, exist_ok=True)
        command = [self._executable, "chat", "--no-interactive", "/usage"]
        try:
            completed = subprocess.run(
                command,
                cwd=self._scratch,
                env=_child_environment({"KIRO_API_KEY": api_key}, home=self._scratch),
                stdout=subprocess.PIPE,
                # Merged: one measured run wrote the figure to stdout instead of stderr.
                stderr=subprocess.STDOUT,
                text=True,
                timeout=self._timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as expired:
            self._warn(UsageFailure.TIMED_OUT, None, expired.output)
            return UsageFailure.TIMED_OUT
        except OSError as exc:
            logger.warning(
                "could not read /usage: %s did not start (%s)",
                self._executable,
                type(exc).__name__,
            )
            return UsageFailure.NOT_LAUNCHED

        result = parse_usage(completed.stdout or "")
        if isinstance(result, UsageFailure):
            # The exit code is logged and nothing more: it was 0 when the backend
            # could not be reached.
            self._warn(result, completed.returncode, completed.stdout)
        return result

    def _warn(self, reason: UsageFailure, exit_code: int | None, output) -> None:
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        excerpt = _LONG_TOKEN.sub("<REDACTED>", strip_ansi(output or ""))
        excerpt = " ".join(excerpt.split())[:_EXCERPT_LENGTH]
        logger.warning(
            "could not read /usage (%s, exit=%s): %r", reason, exit_code, excerpt
        )
