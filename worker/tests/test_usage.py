"""Reading `/usage`: the parser on captured output, and the probe on a fake CLI.

The samples are the outputs measured against Kiro CLI 2.21.4, kept verbatim apart
from the progress bar, which is shortened. The one fact every test here leans on is
that the exit code means nothing: with the backend unreachable the command exited 0
and printed no figure.
"""

from __future__ import annotations

import os
import stat
import textwrap
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from runner.usage import UsageFailure, UsageProbe, UsageReading, parse_usage
from tests.conftest import FAKE_KEY_MATERIAL_PREFIX

ESC = "\x1b"

V1_STDERR_ANSI = (
    f"{ESC}[1mEstimated Usage{ESC}[0m | resets on 2026-11-01 | {ESC}[38;5;141mKIRO POWER{ESC}[0m\n"
    f"{ESC}[1mCredits{ESC}[0m (12.80 of 10000 covered in plan)\n"
    f"{ESC}[38;5;141m{ESC}[38;5;244m█████{ESC}[0m 0.1%\n"
    f"{ESC}[1mAdditional credits{ESC}[0m\n"
    f"{ESC}[38;5;244mAdd-on credits available for purchase\n"
    f"{ESC}[0m\n"
    "To manage your plan or purchase add-on credits navigate to "
    f"{ESC}[38;5;141mhttps://app.kiro.dev/account/usage{ESC}[0m\n"
    f"Tip: to see context window usage, run {ESC}[38;5;141m/context{ESC}[0m\n"
    f"{ESC}[1G{ESC}[0m{ESC}[0m{ESC}[?25h"
)

V1_STDOUT_FARGATE = textwrap.dedent(
    """
    Estimated Usage | resets on 2026-11-01 | KIRO POWER
    Credits (12.97 of 10000 covered in plan)
    [bar] 0.1%

    Additional credits
    Add-on credits available for purchase

    To manage your plan or purchase add-on credits navigate to https://app.kiro.dev/account/usage

    Tip: to see context window usage, run /context
    """
)

V2_TEXT = textwrap.dedent(
    """\
    Estimated Usage | resets on 2026-11-01 | KIRO POWER
    Credits (12.80 of 10000 covered in plan), 0.1%
    Additional credits:
      Add-on credits available for purchase
    Manage your plan or purchase add-on credits at https://app.kiro.dev/account/usage
    """
)

BACKEND_UNREACHABLE = (
    "⚠️  Warning: Could not retrieve usage information from backend\n"
    "Error: dispatch failure (io error): an i/o error occurred: error sending request "
    "for url (https://management.us-east-1.kiro.dev/?origin=KIRO_CLI&isEmailRequired=false)\n"
)

NOVEMBER = datetime(2026, 11, 1, tzinfo=UTC)


class TestParsing:
    def test_the_default_engine_on_stderr_with_colour(self) -> None:
        reading = parse_usage(V1_STDERR_ANSI)

        assert reading == UsageReading(Decimal("12.80"), Decimal("10000"), NOVEMBER)

    def test_the_default_engine_on_stdout_as_seen_on_fargate(self) -> None:
        assert parse_usage(V1_STDOUT_FARGATE) == UsageReading(
            Decimal("12.97"), Decimal("10000"), NOVEMBER
        )

    def test_the_newer_engine_with_the_percentage_on_the_line(self) -> None:
        assert parse_usage(V2_TEXT) == UsageReading(
            Decimal("12.80"), Decimal("10000"), NOVEMBER
        )

    def test_the_figure_keeps_its_two_decimal_places(self) -> None:
        # A float would make 12.97 compare unequal to a stored 12.97.
        reading = parse_usage(V1_STDOUT_FARGATE)

        assert isinstance(reading, UsageReading)
        assert isinstance(reading.used, Decimal)
        assert str(reading.used) == "12.97"

    def test_an_unreachable_backend_is_a_failure_and_not_zero(self) -> None:
        """The command exits 0 here. Reading it as zero would reopen a spent budget."""
        assert parse_usage(BACKEND_UNREACHABLE) is UsageFailure.NO_CREDITS_LINE

    @pytest.mark.parametrize(
        "garbage",
        [
            "",
            "OK",
            "Credits: 0.05 • Time: 0s",
            "Credits (lots of 10000 covered in plan)",
            "Credits (1.2.3 of 10000 covered in plan)\nresets on 2026-11-01",
            "\x00\xff random bytes",
        ],
    )
    def test_anything_else_is_a_failure(self, garbage: str) -> None:
        assert isinstance(parse_usage(garbage), UsageFailure)

    def test_a_figure_without_a_reset_date_cannot_be_placed_in_a_window(self) -> None:
        assert (
            parse_usage("Credits (12.80 of 10000 covered in plan)")
            is UsageFailure.NO_RESET_DATE
        )


def fake_cli(tmp_path: Path, body: str) -> str:
    """An executable standing in for `kiro-cli`, as a shell script."""
    script = tmp_path / "fake-kiro-cli"
    script.write_text("#!/bin/sh\n" + body)
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return str(script)


@pytest.fixture
def key() -> str:
    return f"{FAKE_KEY_MATERIAL_PREFIX}-probe"


class TestTheProbe:
    def test_the_figure_is_found_on_stderr(self, tmp_path: Path, key: str) -> None:
        exe = fake_cli(
            tmp_path,
            "echo 'Estimated Usage | resets on 2026-11-01 | KIRO POWER' >&2\n"
            "echo 'Credits (3.50 of 10000 covered in plan)' >&2\n",
        )

        reading = UsageProbe(tmp_path / "scratch", executable=exe).read(key)

        assert reading == UsageReading(Decimal("3.50"), Decimal("10000"), NOVEMBER)

    def test_the_figure_is_found_on_stdout(self, tmp_path: Path, key: str) -> None:
        # One measured run in six put it there instead.
        exe = fake_cli(
            tmp_path,
            "echo 'Estimated Usage | resets on 2026-11-01 | KIRO POWER'\n"
            "echo 'Credits (3.50 of 10000 covered in plan), 0.1%'\n",
        )

        reading = UsageProbe(tmp_path / "scratch", executable=exe).read(key)

        assert isinstance(reading, UsageReading)
        assert reading.used == Decimal("3.50")

    def test_exit_zero_without_a_figure_is_a_failure(
        self, tmp_path: Path, key: str, caplog
    ) -> None:
        exe = fake_cli(
            tmp_path,
            "echo 'Warning: Could not retrieve usage information from backend' >&2\n"
            "exit 0\n",
        )

        result = UsageProbe(tmp_path / "scratch", executable=exe).read(key)

        assert result is UsageFailure.NO_CREDITS_LINE
        assert "could not read /usage" in caplog.text
        assert "exit=0" in caplog.text

    def test_a_hang_is_bounded(self, tmp_path: Path, key: str) -> None:
        exe = fake_cli(tmp_path, "sleep 5\n")

        result = UsageProbe(
            tmp_path / "scratch", executable=exe, timeout_seconds=0.3
        ).read(key)

        assert result is UsageFailure.TIMED_OUT

    def test_a_missing_binary_is_a_failure(self, tmp_path: Path, key: str) -> None:
        result = UsageProbe(
            tmp_path / "scratch", executable=str(tmp_path / "no-such-cli")
        ).read(key)

        assert result is UsageFailure.NOT_LAUNCHED

    def test_the_key_reaches_the_child_and_not_the_log(
        self, tmp_path: Path, key: str, caplog
    ) -> None:
        """Written to a file by the child and compared here, so it is never printed."""
        seen = tmp_path / "seen-key"
        exe = fake_cli(
            tmp_path,
            f'printf %s "$KIRO_API_KEY" > "{seen}"\n'
            "echo 'not a usage figure'\n",
        )

        UsageProbe(tmp_path / "scratch", executable=exe).read(key)

        assert seen.read_text() == key
        assert key not in caplog.text
        # The leak sweep in conftest also checks every log and stream after the test.

    def test_the_worker_environment_is_not_inherited(
        self, tmp_path: Path, key: str, monkeypatch
    ) -> None:
        monkeypatch.setenv("SUBSCRIPTIONS_TABLE", "the-pool")
        seen = tmp_path / "seen-env"
        exe = fake_cli(tmp_path, f'env > "{seen}"\n')

        UsageProbe(tmp_path / "scratch", executable=exe).read(key)

        assert "SUBSCRIPTIONS_TABLE" not in seen.read_text()
        assert os.environ["SUBSCRIPTIONS_TABLE"] == "the-pool"
