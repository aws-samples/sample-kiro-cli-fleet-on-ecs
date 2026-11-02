"""Running one headless Kiro CLI workflow and deciding whether it worked.

The deciding is the hard part. A headless run does not reliably signal failure
through its exit status: measured against Kiro CLI 2.21.4, an invalid credential
produces an access-denied message on stderr and exits zero. A worker that trusted
that would delete its queue message and lose the work item while appearing to
succeed.

So completion is judged on three signals together, none of which depends on the
wording of a message:

  - the process exited zero, with --require-mcp-startup enabled
  - the run produced the output it was supposed to produce
  - stdout is not empty, for a run that is supposed to answer

Message text is used for one thing only: choosing what to do about a failure. An
invalid credential means the subscription should stop being offered to anyone; a
throttle means back off and retry; anything else is the workflow's own problem and
belongs in the dead-letter queue. Getting that classification wrong costs retry
efficiency, not correctness, which is why it is kept away from the completion
decision.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Final

from runner.definition import WorkflowDefinition, seed_cli_configuration
from runner.guard import RUN_DIR, Snapshot, compare

logger = logging.getLogger(__name__)

WORKSPACE: Final = Path("/workspace")
INPUT_DIR: Final = "input"
OUTPUT_DIR: Final = "output"

DEFAULT_TIMEOUT_SECONDS: Final = 900


MCP_STARTUP_FAILED_EXIT: Final = 3
"""What the CLI returns when --require-mcp-startup is set and a server does not start.

A documented, stable signal, which makes it worth more than any message text. It says
the environment this worker runs in is incomplete, so the work item is blameless.
"""


class Verdict(StrEnum):
    SUCCEEDED = "Succeeded"
    CREDENTIAL_REJECTED = "CredentialRejected"
    STARTUP_FAILED = "StartupFailed"
    THROTTLED = "Throttled"
    NO_OUTPUT = "NoOutput"
    WROTE_OUTSIDE_OUTPUT = "WroteOutsideOutput"
    FAILED = "Failed"
    TIMED_OUT = "TimedOut"


class Remedy(StrEnum):
    """What the fleet should do about a verdict."""

    COMPLETE = "Complete"
    DISABLE_SUBSCRIPTION = "DisableSubscription"
    RETRY_ELSEWHERE = "RetryElsewhere"
    RETRY_LATER = "RetryLater"
    DEAD_LETTER = "DeadLetter"


REMEDIES: Final[dict[Verdict, Remedy]] = {
    Verdict.SUCCEEDED: Remedy.COMPLETE,
    # The key is bad for everyone, not just this worker, so take the subscription
    # out of rotation instead of letting the next task rediscover it.
    Verdict.CREDENTIAL_REJECTED: Remedy.DISABLE_SUBSCRIPTION,
    # The image or its configuration is incomplete. The item is fine, this worker is
    # not, so hand the item back and stop rather than failing every item in turn.
    Verdict.STARTUP_FAILED: Remedy.RETRY_ELSEWHERE,
    Verdict.THROTTLED: Remedy.RETRY_LATER,
    Verdict.NO_OUTPUT: Remedy.DEAD_LETTER,
    # The workflow's own fault, and retrying would repeat it. Dead-lettered so the
    # definition gets fixed rather than the item circulating.
    Verdict.WROTE_OUTSIDE_OUTPUT: Remedy.DEAD_LETTER,
    Verdict.FAILED: Remedy.DEAD_LETTER,
    Verdict.TIMED_OUT: Remedy.RETRY_LATER,
}

# Matched against stderr only. stdout carries the model's own words, so matching
# there could be confused by a model that quotes an error back.
_CREDENTIAL_REJECTED = re.compile(
    r"access denied|bearer token|invalid.{0,20}token|unauthor", re.IGNORECASE
)
_THROTTLED = re.compile(
    r"throttl|too many requests|\b429\b|rate.?limit|quota exceeded", re.IGNORECASE
)


_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")

# The end-of-run summary, `▸ Credits: 0.05 • Time: 0s` (Kiro CLI 2.21.4), measured on stderr.
# Kiro CLI 2.30 prints no summary in headless mode: none of 600 measured runs carried one, so
# on 2.30 this finds nothing and `creditsUsed` rests on `/usage` alone, as it always did.
# Two patterns, because the streams carry different things. On stderr the CLI writes only its
# own status, so any `Credits: <n>` is the summary. On stdout the model's words are mixed in,
# and a file being summarised can say `Credits: 3`, so there only the full summary shape counts.
_RUN_CREDITS = re.compile(r"Credits\s*:\s*([0-9]+(?:\.[0-9]+)?)")
_RUN_CREDITS_SUMMARY = re.compile(
    r"Credits\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*\S?\s*Time\s*:"
)


def strip_ansi(text: str) -> str:
    """Remove terminal colour and cursor sequences, which the CLI writes even to a pipe."""
    return _ANSI_ESCAPE.sub("", text)


def _clean(text: str) -> str:
    """ANSI stripped, and carriage returns turned into line breaks.

    A spinner redraws its line with `\\r`; as a line break, what it overwrote stays readable
    and cannot glue itself onto the summary.
    """
    return strip_ansi(text).replace("\r\n", "\n").replace("\r", "\n")


def _last_figure(pattern: re.Pattern[str], text: str) -> Decimal | None:
    matches = pattern.findall(_clean(text))
    if not matches:
        return None
    try:
        # The last one: the summary is printed at exit, after anything else.
        return Decimal(matches[-1])
    except InvalidOperation:
        return None


def run_credits(stderr: str, stdout: str = "") -> Decimal | None:
    """The per-run credit figure a headless run prints, or None if it printed none.

    stderr first, where it was measured; stdout second, because a `/usage` run once put its
    figure on stdout instead. None is a real outcome and not a parse failure to paper over:
    the same command, run twice in the same image, once printed the summary and once printed
    a plain format with no credit figure on either stream.

    Observability only. It covers the primary agent, at two decimal places, so it
    never decides a verdict and is never added to `creditsUsed`.
    """
    found = _last_figure(_RUN_CREDITS, stderr)
    if found is None:
        found = _last_figure(_RUN_CREDITS_SUMMARY, stdout)
    return found


def last_line(text: str, limit: int = 200) -> str:
    """The last non-blank line, ANSI stripped, long tokens masked, at most `limit` characters.

    For a log line that has to say what a run ended on without carrying its output.
    """
    lines = [line.strip() for line in _clean(text).splitlines() if line.strip()]
    if not lines:
        return ""
    return _LONG_TOKEN.sub("<REDACTED>", lines[-1])[:limit]


@dataclass(frozen=True)
class RunResult:
    verdict: Verdict
    exit_code: int | None
    stdout: str
    stderr: str
    outputs: tuple[Path, ...] = field(default_factory=tuple)
    credits: Decimal | None = None

    @property
    def remedy(self) -> Remedy:
        return REMEDIES[self.verdict]

    @property
    def succeeded(self) -> bool:
        return self.verdict is Verdict.SUCCEEDED


def classify(
    exit_code: int | None,
    stdout: str,
    stderr: str,
    outputs: tuple[Path, ...],
    expects_stdout: bool = True,
    wrote_outside_output: bool = False,
) -> Verdict:
    """Decide what happened, without trusting the exit status on its own."""
    # Checked before anything else, including a clean exit. A run that wrote where it was
    # not allowed cannot be called successful whatever else it did, because the results it
    # produced are no longer trustworthy.
    if wrote_outside_output:
        return Verdict.WROTE_OUTSIDE_OUTPUT

    if exit_code is None:
        return Verdict.TIMED_OUT

    # Diagnosis first, so a failure that names its cause gets the right remedy even
    # when the exit status disagrees. This is the case that motivates all of it: an
    # invalid credential exits zero.
    if _CREDENTIAL_REJECTED.search(stderr):
        return Verdict.CREDENTIAL_REJECTED
    if _THROTTLED.search(stderr):
        return Verdict.THROTTLED

    # Checked before the generic failure, and by code rather than by message. A
    # startup failure means this worker's environment is incomplete; treating it as
    # the item's fault would dead-letter perfectly good work, once per item, until
    # the queue was empty.
    if exit_code == MCP_STARTUP_FAILED_EXIT:
        return Verdict.STARTUP_FAILED

    if exit_code != 0:
        return Verdict.FAILED
    if not outputs:
        return Verdict.NO_OUTPUT
    if expects_stdout and not stdout.strip():
        return Verdict.NO_OUTPUT

    return Verdict.SUCCEEDED


def run_workflow(
    prompt: str,
    api_key: str,
    workspace: Path,
    agent: str | None = None,
    trust_tools: str = "",
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    executable: str = "kiro-cli",
    extra_env: dict[str, str] | None = None,
    isolate_home: bool = False,
    expects_stdout: bool = True,
    enforce_output_path: bool = True,
    definition: WorkflowDefinition | None = None,
) -> RunResult:
    """Run one workflow to completion and judge the result.

    `--require-mcp-startup` is not optional. Its documented purpose is to fail when
    an MCP server cannot start, but it also, incidentally, validates the credential:
    fetching MCP settings requires a working one, so with the flag an invalid key
    produces a non-zero exit instead of a silent success. It is the only invocation
    measured to do so.

    `--trust-tools` defaults to the empty string, which trusts nothing. A sample
    should widen that deliberately per workflow rather than inherit a broad default.
    """
    output_path = workspace / OUTPUT_DIR
    output_path.mkdir(parents=True, exist_ok=True)

    # Per-run scratch, so logs and temporary files from one item do not mix with
    # another's.
    run_home = workspace / RUN_DIR
    (run_home / "tmp").mkdir(parents=True, exist_ok=True)

    environment_overrides: dict[str, str] = {}
    if isolate_home:
        scoped = prepare_scoped_home(run_home, executable, definition)
        if scoped is None:
            logger.warning(
                "cannot isolate HOME because %s is not on PATH; using the ambient one",
                executable,
            )
        else:
            environment_overrides["HOME"] = str(scoped)

    command = [
        executable,
        "chat",
        "--no-interactive",
        "--require-mcp-startup",
        f"--trust-tools={trust_tools}",
    ]
    if agent:
        command += ["--agent", agent]
    command.append(prompt)

    # The key is passed to the child and never exported into this process, written
    # to a file, or logged. An inherited environment is the narrowest path available.
    environment = {
        "KIRO_API_KEY": api_key,
        **environment_overrides,
        **(extra_env or {}),
    }

    if not _can_write(trust_tools):
        # The completion check requires output, so a workflow with no write tool
        # cannot pass it. Warned rather than refused, because output could legitimately
        # come from an MCP tool this cannot recognise, but it is almost always the
        # explanation for a run that ends with nothing produced.
        logger.warning(
            "no write tool is trusted (--trust-tools=%r), so this run cannot produce "
            "output and will be judged NoOutput unless an MCP tool writes for it",
            trust_tools or "",
        )

    logger.info(
        "running workflow in %s with tools trusted: %r", workspace, trust_tools or "none"
    )

    # Taken before the process starts, so the comparison afterwards sees exactly what the
    # run changed.
    before = Snapshot.of(workspace) if enforce_output_path else Snapshot()
    try:
        completed = subprocess.run(
            command,
            cwd=workspace,
            env=_child_environment(environment, home=run_home),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        exit_code: int | None = completed.returncode
        stdout, stderr = completed.stdout, completed.stderr
    except subprocess.TimeoutExpired as expired:
        exit_code = None
        stdout = _as_text(expired.stdout)
        stderr = _as_text(expired.stderr)
        logger.warning("workflow exceeded %ss", timeout_seconds)
    except FileNotFoundError:
        return RunResult(
            verdict=Verdict.FAILED,
            exit_code=127,
            stdout="",
            stderr=f"{executable} is not on PATH inside this image",
        )

    outputs = tuple(sorted(p for p in output_path.rglob("*") if p.is_file()))

    violations = compare(before, Snapshot.of(workspace)) if enforce_output_path else None
    verdict = classify(
        exit_code,
        stdout,
        stderr,
        outputs,
        expects_stdout=expects_stdout,
        wrote_outside_output=bool(violations),
    )

    _persist_transcript(run_home, command, exit_code, stdout, stderr, verdict)

    logger.info(
        "workflow verdict %s (exit=%s, outputs=%d, stdout=%d bytes)",
        verdict,
        exit_code,
        len(outputs),
        len(stdout),
    )
    return RunResult(
        verdict, exit_code, stdout, stderr, outputs, credits=run_credits(stderr, stdout)
    )


def _child_environment(overrides: dict[str, str], home: Path) -> dict[str, str]:
    """A minimal environment for the child, plus what it needs.

    Deliberately not the parent's whole environment: the worker's own configuration,
    including table names and queue urls, is none of the workflow's business, and a
    workflow that reads them would couple itself to the fleet.

    HOME is inherited by default and scoped only when asked for, because which is
    correct depends on where the run happens.

    The CLI reads agent and MCP configuration from under HOME. In a container that is
    what the image installs, which is exactly what should be used. On a developer
    machine it is whatever that developer has configured, which is not: measured once
    as exit 3 on MCP servers belonging to an unrelated personal project.

    Scoping HOME needs one extra step. The CLI locates its own executable relative to
    HOME, so a bare scoped HOME makes it fail to launch; linking the directory holding
    the executable into place fixes that, and with it the personal configuration is
    gone and the run reaches the same state a clean container does.

    TMPDIR is always scoped, which is safe and keeps per-run logs together.
    """
    import os

    keep = ("PATH", "HOME", "LANG", "LC_ALL", "TZ", "AWS_REGION")
    base = {name: os.environ[name] for name in keep if name in os.environ}
    base.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin")
    base.setdefault("HOME", "/tmp")
    base["TMPDIR"] = str(home / "tmp")
    return {**base, **overrides}


def prepare_scoped_home(
    root: Path, executable: str, definition: "WorkflowDefinition | None" = None
) -> Path | None:
    """Build a HOME containing the CLI and the workflow's own configuration.

    Returns None when the executable cannot be located, in which case the caller
    should leave HOME alone rather than guess.

    Seeding the definition here is what makes a local run comparable to a fleet run. An
    empty HOME is reproducible but it is not the same environment: on a task the CLI reads
    the workflow's MCP servers and steering from under HOME, so a local run without them
    exercises a different configuration and can only tell you about the loop, not about
    the workflow. With them, the difference between the two runs is the queue and the
    store, which is the point of having a local mode at all.
    """
    resolved = shutil.which(executable)
    if resolved is None:
        return None

    binary_dir = Path(resolved).resolve().parent
    scoped = root / "home"
    (scoped / ".local").mkdir(parents=True, exist_ok=True)
    (scoped / "tmp").mkdir(parents=True, exist_ok=True)

    link = scoped / ".local" / "bin"
    if not link.exists():
        link.symlink_to(binary_dir)

    if definition is not None:
        seed_cli_configuration(scoped, definition)
    return scoped


_LONG_TOKEN = re.compile(r"[A-Za-z0-9_\-]{40,}")


def _persist_transcript(
    run_home: Path,
    command: list[str],
    exit_code: int | None,
    stdout: str,
    stderr: str,
    verdict: Verdict,
) -> None:
    """Write down what the run did, so a failure can be read afterwards.

    Without this, diagnosing a failed run means running it again and hoping it fails
    the same way. Long tokens are masked on the way out: nothing should put a
    credential here, and a file on disk is the wrong place to find out otherwise.
    """
    try:
        transcript = run_home / "transcript"
        transcript.mkdir(parents=True, exist_ok=True)
        (transcript / "summary.txt").write_text(
            f"verdict   : {verdict}\n"
            f"exit code : {exit_code}\n"
            f"command   : {' '.join(command[:-1])} <prompt>\n"
            f"stdout    : {len(stdout)} bytes\n"
            f"stderr    : {len(stderr)} bytes\n"
        )
        (transcript / "stdout.txt").write_text(_LONG_TOKEN.sub("<REDACTED>", stdout))
        (transcript / "stderr.txt").write_text(_LONG_TOKEN.sub("<REDACTED>", stderr))
    except Exception:
        logger.debug("could not persist the run transcript", exc_info=True)


def _as_text(raw) -> str:
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


WRITE_TOOLS: Final = frozenset(
    {"write", "fs_write", "fsWrite", "shell", "execute_bash", "executeBash"}
)
"""Built-in tools that can create a file, by canonical name and by alias.

Kiro CLI 2.30 names them `write` and `shell` and still accepts the 2.21-era aliases
`fs_write` and `execute_bash`. Knowing only the aliases made
`--trust-tools=read,write,subagent` warn on every run although it writes fine.

Used only to warn about a configuration that cannot succeed. An MCP tool may also
write, and this cannot know their names, which is why the check warns rather than
refuses.
"""


def _can_write(trust_tools: str) -> bool:
    granted = {name.strip() for name in trust_tools.split(",") if name.strip()}
    return bool(granted & WRITE_TOOLS)


def kiro_cli_available(executable: str = "kiro-cli") -> bool:
    return shutil.which(executable) is not None
