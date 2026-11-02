"""Deciding whether a headless run worked.

The exit status alone does not answer that. Measured against Kiro CLI 2.21.4, an
invalid credential prints an access-denied message on stderr and exits zero, so a
worker that trusted the status would delete its work item while appearing to succeed.

These tests pin the three-signal decision and the remedy each failure maps to. The
remedy matters as much as the verdict: routing an environment problem to the
dead-letter queue would discard good work, once per item, until the queue was empty.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from runner.workflow import (
    MCP_STARTUP_FAILED_EXIT,
    OUTPUT_DIR,
    Remedy,
    Verdict,
    classify,
)


def an_output(tmp_path: Path) -> tuple[Path, ...]:
    path = tmp_path / OUTPUT_DIR / "result.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("done")
    return (path,)


class TestSuccess:
    def test_all_three_signals_present(self, tmp_path) -> None:
        assert (
            classify(0, "an answer", "", an_output(tmp_path)) is Verdict.SUCCEEDED
        )

    def test_warnings_on_stderr_do_not_fail_a_run(self, tmp_path) -> None:
        """The CLI warns routinely; treating any stderr as failure would reject
        successful runs."""
        assert (
            classify(0, "an answer", "WARNING: something cosmetic", an_output(tmp_path))
            is Verdict.SUCCEEDED
        )


class TestTheSilentFailure:
    def test_an_invalid_credential_is_caught_despite_exit_zero(self, tmp_path) -> None:
        """The case the whole check exists for."""
        verdict = classify(
            0,
            "",
            "Access denied: The bearer token included in the request is invalid.",
            an_output(tmp_path),
        )

        assert verdict is Verdict.CREDENTIAL_REJECTED

    def test_a_rejected_credential_takes_the_subscription_out_of_rotation(self) -> None:
        """Not a retry: the key is bad for whoever claims it next, too."""
        assert (
            Verdict.CREDENTIAL_REJECTED.value
            and _remedy(Verdict.CREDENTIAL_REJECTED) is Remedy.DISABLE_SUBSCRIPTION
        )

    def test_exit_zero_with_no_output_is_a_failure(self) -> None:
        assert classify(0, "an answer", "", ()) is Verdict.NO_OUTPUT

    def test_exit_zero_with_no_answer_is_a_failure(self, tmp_path) -> None:
        assert classify(0, "   ", "", an_output(tmp_path)) is Verdict.NO_OUTPUT

    def test_a_run_not_expected_to_answer_may_be_silent(self, tmp_path) -> None:
        assert (
            classify(0, "", "", an_output(tmp_path), expects_stdout=False)
            is Verdict.SUCCEEDED
        )


class TestEnvironmentFailures:
    def test_the_documented_startup_exit_code_is_recognized(self, tmp_path) -> None:
        """By code, not by message: the code is documented and stable."""
        assert (
            classify(MCP_STARTUP_FAILED_EXIT, "", "Error: MCP servers failed", ())
            is Verdict.STARTUP_FAILED
        )

    def test_a_startup_failure_sends_the_item_elsewhere(self) -> None:
        """The item is blameless; this worker's image or configuration is not."""
        assert _remedy(Verdict.STARTUP_FAILED) is Remedy.RETRY_ELSEWHERE

    def test_a_startup_failure_is_not_dead_lettered(self) -> None:
        """Otherwise a broken deployment quietly discards the whole batch."""
        assert _remedy(Verdict.STARTUP_FAILED) is not Remedy.DEAD_LETTER


class TestThrottling:
    @pytest.mark.parametrize(
        "message",
        [
            "Too many requests",
            "ThrottlingException: rate exceeded",
            "HTTP 429 returned",
            "quota exceeded for this subscription",
        ],
    )
    def test_throttling_is_recognized_and_retried(self, message: str) -> None:
        verdict = classify(1, "", message, ())

        assert verdict is Verdict.THROTTLED
        assert _remedy(verdict) is Remedy.RETRY_LATER

    def test_throttling_is_distinguished_from_a_bad_credential(self) -> None:
        """They need different remedies, so conflating them wastes either the item or
        the subscription."""
        assert classify(1, "", "throttled", ()) is not (
            classify(1, "", "Access denied", ())
        )


class TestOtherOutcomes:
    def test_a_timeout_is_retried_rather_than_discarded(self) -> None:
        verdict = classify(None, "", "", ())

        assert verdict is Verdict.TIMED_OUT
        assert _remedy(verdict) is Remedy.RETRY_LATER

    def test_an_unexplained_non_zero_exit_is_the_workflows_own_problem(self) -> None:
        verdict = classify(2, "", "Traceback ... ValueError", ())

        assert verdict is Verdict.FAILED
        assert _remedy(verdict) is Remedy.DEAD_LETTER


class TestDiagnosisNeverDecidesCompletion:
    def test_a_model_quoting_an_error_on_stdout_does_not_fail_the_run(
        self, tmp_path
    ) -> None:
        """stdout carries the model's words, so it is never matched for diagnosis."""
        verdict = classify(
            0,
            "The document explains that 'Access denied' means the token was rejected.",
            "",
            an_output(tmp_path),
        )

        assert verdict is Verdict.SUCCEEDED

    def test_every_verdict_has_a_remedy(self) -> None:
        """A verdict with no mapped remedy would raise inside the worker loop."""
        for verdict in Verdict:
            assert _remedy(verdict) in set(Remedy)


def _remedy(verdict: Verdict) -> Remedy:
    from runner.workflow import REMEDIES

    return REMEDIES[verdict]


class TestTheToolGrantMustMatchTheCompletionContract:
    """A configuration that cannot succeed should say so, not fail mysteriously.

    Found by running it: with no write tool trusted the workflow could only answer on
    stdout, while the completion check requires a file. Every run failed as NoOutput
    and was routed to the dead-letter queue, which reads like a broken workflow rather
    than a mismatched grant.
    """

    @pytest.mark.parametrize(
        "trust_tools",
        [
            "fs_write",
            "fs_read,fs_write",
            "execute_bash",
            "fs_write,other",
            # Kiro CLI 2.30 canonical names, as the bench fanout workflow grants them.
            "write",
            "read,write,subagent",
            "shell",
            " read , write ",
        ],
    )
    def test_a_write_capable_grant_is_recognized(self, trust_tools: str) -> None:
        from runner.workflow import _can_write

        assert _can_write(trust_tools)

    @pytest.mark.parametrize(
        "trust_tools", ["", "fs_read", "fs_read,report_issue", "read,subagent", "writer"]
    )
    def test_a_grant_with_no_write_tool_is_recognized(self, trust_tools: str) -> None:
        from runner.workflow import _can_write

        assert not _can_write(trust_tools)

    def test_the_mismatch_is_warned_about_before_the_run(
        self, tmp_path, caplog, monkeypatch
    ) -> None:
        import logging
        import subprocess

        from runner import workflow as module

        def never_runs(*args, **kwargs):
            raise FileNotFoundError("kiro-cli")

        monkeypatch.setattr(subprocess, "run", never_runs)

        with caplog.at_level(logging.WARNING):
            module.run_workflow(
                prompt="go", api_key="k", workspace=tmp_path, trust_tools=""
            )

        assert "no write tool is trusted" in caplog.text


class TestTheRunsOwnCreditFigure:
    """The `▸ Credits:` line on stderr. Published as a metric, never used to decide anything."""

    STDERR = "\n\n\x1b[38;5;244m ▸ Credits: 0.05 • Time: 0s\x1b[0m\n\n"

    def test_it_is_read_through_the_colour(self) -> None:
        from decimal import Decimal

        from runner.workflow import run_credits

        assert run_credits(self.STDERR) == Decimal("0.05")

    def test_absent_is_none_rather_than_zero(self) -> None:
        from runner.workflow import run_credits

        assert run_credits("") is None
        assert run_credits("Credits (12.80 of 10000 covered in plan)") is None

    def test_it_does_not_change_a_verdict(self, tmp_path: Path) -> None:
        # Neither failure pattern matches the summary line.
        assert (
            classify(0, "answer", self.STDERR, an_output(tmp_path)) is Verdict.SUCCEEDED
        )


class TestTheCreditFigureAsTheCliActuallyPrintsIt:
    """Byte-exact streams from the worker's command in the worker image (Kiro CLI 2.21.4).

    The same command, image and key, run twice back to back with the repo-file-summary
    workflow on one input, produced two formats. One printed the summary on stderr. The other
    printed plain `[tool]` lines and no credit figure on either stream, which is what an AWS
    run without a `RunCredits` record looked like.
    """

    # Run with the summary: ANSI-heavy stderr, `▸` and `•` in UTF-8.
    WITH_SUMMARY_STDERR = (
        b"\n\x1b[38;5;252m\x1b[0m\x1b[?25l\x1b[?25l\x1b[0m\x1b[0m\x1b[?25l\x1b[0m\x1b[?25l"
        b"\x1b[0m\x1b[0m\x1b[?25l\x1b[0m\x1b[?25l\x1b[0m\x1b[0m\x1b[?25l\x1b[0m\x1b[0m"
        b"\x1b[0m\n\x1b[38;5;8m\n \xe2\x96\xb8 Credits: 0.21 \xe2\x80\xa2 Time: 9s\n\n"
        b"\x1b[0m\x1b[1G\x1b[0m\x1b[0m\x1b[?25h"
    )

    # Run without it: plain text on both streams.
    PLAIN_STDERR = (
        b"\n[tool] Reading listing input\n[tool] status: Completed\n\n"
        b"[tool] Reading alpha.py:1\n[tool] status: Completed\n\n"
        b"[tool] Creating alpha.py.md\n[tool] status: Completed\n"
    )
    PLAIN_STDOUT = (
        b"I'll start by exploring the `./input` directory to see what files I need to "
        b"process.Only one file. Let me read it.Processed 1 file.\n"
    )

    def test_the_summary_on_stderr_is_read(self) -> None:
        from decimal import Decimal

        from runner.workflow import run_credits

        assert run_credits(self.WITH_SUMMARY_STDERR.decode()) == Decimal("0.21")

    def test_the_plain_format_has_no_figure_on_either_stream(self) -> None:
        from runner.workflow import run_credits

        assert run_credits(self.PLAIN_STDERR.decode(), self.PLAIN_STDOUT.decode()) is None

    def test_the_last_stderr_line_of_the_plain_format(self) -> None:
        from runner.workflow import last_line

        assert last_line(self.PLAIN_STDERR.decode()) == "[tool] status: Completed"

    # The variants below are constructed, not captured: the line on stdout (a /usage run once
    # wrote there), a spinner redrawn with carriage returns, and different spacing.

    def test_the_summary_on_stdout_is_read(self) -> None:
        from decimal import Decimal

        from runner.workflow import run_credits

        stdout = self.PLAIN_STDOUT.decode() + "\n \u25b8 Credits: 0.07 \u2022 Time: 3s\n"
        assert run_credits(self.PLAIN_STDERR.decode(), stdout) == Decimal("0.07")

    def test_the_model_saying_credits_on_stdout_is_not_the_figure(self) -> None:
        """stdout carries the model's words. A file whose summary says `Credits: 3` is not a
        run that cost three credits."""
        from runner.workflow import run_credits

        assert run_credits("", "## Notes\nCredits: 3 are charged per call.\n") is None

    def test_stderr_wins_over_stdout(self) -> None:
        from decimal import Decimal

        from runner.workflow import run_credits

        stdout = " \u25b8 Credits: 9.99 \u2022 Time: 1s\n"
        assert run_credits(self.WITH_SUMMARY_STDERR.decode(), stdout) == Decimal("0.21")

    def test_a_spinner_drawn_over_the_line_is_stripped(self) -> None:
        from decimal import Decimal

        from runner.workflow import run_credits

        stderr = "\r\x1b[2K\u280b Thinking...\r\x1b[2K\r \u25b8 Credits: 0.05 \u2022 Time: 0s\r\n"
        assert run_credits(stderr) == Decimal("0.05")

    @pytest.mark.parametrize(
        "line",
        [
            "\u25b8 Credits:0.05 \u2022 Time: 0s",
            "\u25b8 Credits :  0.05\u2022Time:0s",
            "\u25b8 Credits:\t0.05 | Time: 0s",
        ],
    )
    def test_spacing_variants(self, line: str) -> None:
        from decimal import Decimal

        from runner.workflow import run_credits

        assert run_credits(line) == Decimal("0.05")
        assert run_credits("", line) == Decimal("0.05")

    def test_the_last_line_is_masked_and_truncated(self) -> None:
        from runner.workflow import last_line

        token = "A" * 48
        assert last_line(f"first\n\x1b[31mtoken {token}\x1b[0m\n\n") == "token <REDACTED>"
        assert len(last_line("x" * 30 + " " + "y " * 300)) == 200
        assert last_line("") == ""
