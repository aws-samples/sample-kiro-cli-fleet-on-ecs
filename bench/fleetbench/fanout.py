"""M1b: the workload that reaches a request-rate limit -- an orchestrator with subagents.

M1 held N single-request streams open at once and found no throttling up to 100. That
answered a question nobody had: the throttling seen in production came from runs where
an orchestrator fanned out to subagents, and the errors named a request rate
(`ThrottlingException`, `Request quota exceeded`, `Kiro rate limit reached`), not a
connection count. An agent sends a new model request after every tool call, so one such
run sends dozens of requests in a few minutes, and a handful of runs at once multiply
that. A single-request stream never gets near it.

So M1b runs the shape that throttled: one orchestrator, K subagents in parallel, each
subagent reading files one tool call at a time and writing a summary, then the
orchestrator combining them. The corpus is generated, neutral prose, so nothing in a
file the model reads can look like a throttle message.

The CLI may retry a throttle internally and still finish, which is why earlier throttling
reports listed "ThrottlingException (recovered)" separately. A run's verdict alone would
hide those, so every run also counts the throttle messages on its stderr. A level with
any throttle message, recovered or not, is a throttled level.

Everything here is pure (prompt, corpus, stderr analysis, summary). The task side that
calls the CLI lives in fleet.py beside M1's.
"""

from __future__ import annotations

import json
import re
import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

TRUST_TOOLS = "read,write,subagent"
"""Read, write, and delegate, by canonical name. Nothing else: no shell, no network tools.

Measured: the alias set `fs_read,fs_write,use_subagent` left the subagent tool untrusted
in non-interactive mode ("[denied] tool permission approval is not supported"), so the
orchestrator never delegated. The launcher can override this with `--trust-tools`.
"""

# Mirrors the worker's own throttle classifier (runner.workflow._THROTTLED), plus the
# phrases seen in earlier throttling reports, so a recovered throttle is counted by the same rule
# that would have failed the run.
_THROTTLE = re.compile(
    r"throttl|too many requests|\b429\b|rate.?limit|quota exceeded", re.IGNORECASE
)
_APPROVAL = re.compile(
    r"^\[denied\]|approval required|requires approval|approval is not supported", re.IGNORECASE
)
# Kiro CLI 2.30 prints one `[tool] <action>` line per tool call and a `[tool] status: ...`
# line when it finishes; older builds said `(using tool: <name>)`.
_TOOL_USE = re.compile(r"^\[tool\](?!\s*status:)|using tool", re.IGNORECASE)
_ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
_LONG_TOKEN = re.compile(r"[A-Za-z0-9_\-]{32,}")

_WORDS = (
    "river stone harbor lantern meadow orchard copper willow canyon ember glacier "
    "pebble timber cobalt valley summit prairie thistle marble falcon cedar delta "
    "quartz island beacon garden shadow ribbon saddle walnut"
).split()


DEFAULT_NOTE_WORDS = 120
"""Words of prose per note. M1b's request-rate runs used this size.

Raising it is the token axis: every file a subagent reads stays in its context, so each
later model request carries all the notes read so far. The request count stays the same
while the input tokens per request grow, which separates a token-throughput limit from a
request-rate limit.
"""


def corpus_files(
    subagents: int, files_per_subagent: int, note_words: int = DEFAULT_NOTE_WORDS
) -> dict[str, str]:
    """Deterministic notes, `notes/<group>-<nn>.md`, one group per subagent.

    Each note is paragraphs of generated text with one numbered fact, so a summary has
    something definite to report and a run that skipped a file is visible.

    The fact is the LAST line. Measured: with the fact on line 3, subagents read only
    lines 1-5 of a 5000-word note, so raising note_words barely raised the input tokens.
    At the end, a read that stops early misses the count, and `read_coverage` shows it.
    """
    files: dict[str, str] = {}
    for g in range(subagents):
        group = chr(ord("a") + g)
        for i in range(1, files_per_subagent + 1):
            seed = g * 97 + i * 13
            words = [_WORDS[(seed + k * 7) % len(_WORDS)] for k in range(note_words)]
            body = "\n\n".join(
                " ".join(words[p : p + 40]) + "." for p in range(0, note_words, 40)
            )
            files[f"notes/{group}-{i:02d}.md"] = (
                f"# Note {group}-{i:02d}\n\n{body}\n\nFact: the count for this note is {seed}.\n"
            )
    return files


def note_lines(note_words: int = DEFAULT_NOTE_WORDS) -> int:
    """Line count of one note at this size (every note has the same shape)."""
    return corpus_files(1, 1, note_words)["notes/a-01.md"].count("\n")


def expected_total(subagents: int, files_per_subagent: int) -> int:
    """Sum of every note's fact, the number output/final.md must report."""
    return sum(
        g * 97 + i * 13 for g in range(subagents) for i in range(1, files_per_subagent + 1)
    )


def final_total_ok(
    workspace: Path, subagents: int, files_per_subagent: int, note_words: int = DEFAULT_NOTE_WORDS
) -> bool | None:
    """Whether output/final.md carries the right total; None when the file is missing."""
    path = workspace / "output" / "final.md"
    if not path.is_file():
        return None
    digits = re.sub(r"(?<=\d),(?=\d{3})", "", path.read_text(errors="replace"))
    return str(expected_total(subagents, files_per_subagent)) in re.findall(r"\d+", digits)


_READING = re.compile(r"^(?:\[tool\]\s*)?Reading\s+(.+)$")
_READ_TARGET = re.compile(r"([\w.-]+\.md)(?::(\d+)(?:-(\d+))?)?")


def read_lines(stderr: str, limit: int = 120) -> list[str]:
    """Every `Reading ...` line the CLI printed, ANSI stripped, cut to 160 characters."""
    out = []
    for line in stderr.replace("\r", "\n").splitlines():
        clean = _ANSI.sub("", line).strip()
        if _READING.match(clean):
            out.append(_LONG_TOKEN.sub("<REDACTED>", clean)[:160])
    return out[:limit]


def read_coverage(lines: Sequence[str], total_lines: int) -> tuple[int, int]:
    """(note reads, full note reads) from `Reading` lines.

    Kiro CLI 2.30 prints `name.md:START-END` for a ranged read and `name.md:1` or no
    suffix for a read to the end of the file. A ranged read counts as full only when it
    reaches the last line.
    """
    reads = full = 0
    for line in lines:
        match = _READING.match(line)
        if not match:
            continue
        for name, start, end in _READ_TARGET.findall(match.group(1)):
            if name.startswith("summary") or name == "final.md":
                continue
            reads += 1
            if end:
                full += int(end) >= total_lines
            else:
                full += not start or int(start) <= 1
    return reads, full


SUBAGENT_AGENT = "note-reader"
"""Name of the workspace-local agent the subagents run as, when one is seeded.

Measured on Kiro CLI 2.21.4: its built-in subagent agents (`default`, `planner`) carry no
pre-approved file tools, so in non-interactive mode every subagent stopped on a permission
prompt and the orchestrator refused to continue, whatever `--trust-tools` said. The as-is
system ran its subagents as agents registered under `.kiro/agents/*.json`; this is that.
"""


def subagent_agent_config(name: str = SUBAGENT_AGENT) -> dict:
    """A subagent definition with only file read and write, both pre-approved."""
    return {
        "name": name,
        "description": "Reads the note files it is given and writes one summary file.",
        "prompt": "You read the note files you are given and write the summary file you are asked for.",
        "tools": ["fs_read", "fs_write"],
        "allowedTools": ["fs_read", "fs_write"],
    }


def write_corpus(
    workspace: Path,
    subagents: int,
    files_per_subagent: int,
    note_words: int = DEFAULT_NOTE_WORDS,
    subagent_agent: str | None = None,
) -> None:
    for relative, text in corpus_files(subagents, files_per_subagent, note_words).items():
        path = workspace / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    if subagent_agent:
        path = workspace / ".kiro" / "agents" / f"{subagent_agent}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(subagent_agent_config(subagent_agent), indent=2))


def prompt(
    subagents: int,
    files_per_subagent: int,
    subagent_agent: str | None = None,
    notes_dir: str = "notes",
) -> str:
    """The orchestrator prompt: delegate every group at once, then combine.

    "One file per tool call" is spelled out because a model left to itself batches reads,
    and the experiment needs the request count a real per-item subagent produces.

    `notes_dir` is where the notes sit relative to the run's working directory: `notes`
    when the bench task writes the corpus itself, the input path when the fleet worker
    pulls it in from the reference bucket (`fleetload`).
    """
    groups = [chr(ord("a") + g) for g in range(subagents)]
    assignments = "\n".join(
        f"- Subagent {g.upper()}: read {notes_dir}/{g}-01.md through "
        f"{notes_dir}/{g}-{files_per_subagent:02d}.md "
        f"and write output/summary-{g}.md"
        for g in groups
    )
    agent_clause = (
        f"Run every subagent as the agent named {subagent_agent}. " if subagent_agent else ""
    )
    return (
        f"You are an orchestrator. Use the subagent tool to start {subagents} subagents "
        "in parallel, in a single response, with these assignments:\n"
        f"{agent_clause}"
        f"{assignments}\n\n"
        "Tell each subagent: read its files one at a time, one file per read tool call, "
        "never several files in one call, and read each file in full, from its first "
        "line to its last, never a line range; then write a summary file listing, for each "
        "note, its title and its count, followed by one sentence about the note.\n\n"
        "Do not read the notes yourself. When every subagent has finished, read each "
        "output/summary-*.md file and write output/final.md with the total of all counts. "
        "Then reply with exactly: done"
    )


@dataclass(frozen=True)
class StderrFindings:
    throttle_hits: int
    approval_blocks: int
    tool_uses: int
    throttle_lines: tuple[str, ...]
    """At most five, ANSI stripped, long tokens masked, 200 characters each."""


def analyse_stderr(stderr: str) -> StderrFindings:
    lines = [
        _ANSI.sub("", line).strip()
        for line in stderr.replace("\r", "\n").splitlines()
        if line.strip()
    ]
    throttled = [line for line in lines if _THROTTLE.search(line)]
    return StderrFindings(
        throttle_hits=len(throttled),
        approval_blocks=sum(1 for line in lines if _APPROVAL.search(line)),
        tool_uses=sum(1 for line in lines if _TOOL_USE.search(line)),
        throttle_lines=tuple(_LONG_TOKEN.sub("<REDACTED>", t)[:200] for t in throttled[:5]),
    )


def masked_tail(text: str, lines: int = 15, width: int = 200) -> list[str]:
    """The last non-blank lines, ANSI stripped, long tokens masked, each cut to `width`."""
    kept = [
        _ANSI.sub("", line).strip()
        for line in text.replace("\r", "\n").splitlines()
        if line.strip()
    ]
    return [_LONG_TOKEN.sub("<REDACTED>", line)[:width] for line in kept[-lines:]]


# --- the level summary the launcher prints and writes -------------------------


@dataclass(frozen=True)
class FanoutLevel:
    level: int
    records: int
    finished: int
    """Runs whose CLI exited 0, throttled on the way or not."""
    throttled_runs: int
    recovered_runs: int
    """Throttled on the way, and still exited 0 with output."""
    throttle_hits: int
    approval_blocks: int
    median_tool_uses: float
    median_seconds: float
    tool_uses_per_minute: float
    """Every tool use across the level divided by the level's wall window. A tool use is
    one model turn, so this is a lower bound on the requests the subscription saw."""


def summarise_level(level: int, records: Sequence[dict]) -> FanoutLevel:
    timed = [r for r in records if r.get("sent_at") is not None and r.get("ended_at") is not None]
    durations = [float(r["ended_at"]) - float(r["sent_at"]) for r in timed]
    tool_uses = [int(r.get("tool_uses") or 0) for r in records]
    window = (
        max(float(r["ended_at"]) for r in timed) - min(float(r["sent_at"]) for r in timed)
        if timed
        else 0.0
    )
    throttled = [r for r in records if int(r.get("throttle_hits") or 0) > 0]
    return FanoutLevel(
        level=level,
        records=len(records),
        finished=sum(1 for r in records if r.get("exit_code") == 0),
        throttled_runs=len(throttled),
        recovered_runs=sum(
            1 for r in throttled if r.get("exit_code") == 0 and int(r.get("outputs") or 0) > 0
        ),
        throttle_hits=sum(int(r.get("throttle_hits") or 0) for r in records),
        approval_blocks=sum(int(r.get("approval_blocks") or 0) for r in records),
        median_tool_uses=statistics.median(tool_uses) if tool_uses else 0.0,
        median_seconds=statistics.median(durations) if durations else 0.0,
        tool_uses_per_minute=(sum(tool_uses) / window * 60.0) if window > 0 else 0.0,
    )


def fanout_markdown(name: str, subagents: int, levels: Sequence[FanoutLevel]) -> str:
    lines = [
        f"# M1b -- {name}: orchestrator + {subagents} subagents, one run per Fargate task",
        "",
        (
            "`throttled runs` counts runs whose stderr carried any throttle message, including "
            "ones the CLI retried and recovered from (`recovered`). `tool uses/min` is every tool "
            "use at the level over the level's wall window -- a lower bound on model requests."
        ),
        "",
        "| concurrent runs | records | exited 0 | throttled runs | recovered | throttle msgs "
        "| approval blocks | median tool uses | median s | tool uses/min |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for f in levels:
        lines.append(
            f"| {f.level} | {f.records} | {f.finished} | {f.throttled_runs} | {f.recovered_runs} "
            f"| {f.throttle_hits} | {f.approval_blocks} | {f.median_tool_uses:.0f} "
            f"| {f.median_seconds:.0f} | {f.tool_uses_per_minute:.0f} |"
        )
    return "\n".join(lines) + "\n"
