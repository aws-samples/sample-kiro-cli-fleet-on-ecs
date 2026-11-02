"""M1e: a workload shaped like the as-is spec generator, with synthetic input.

M1b-M1d ran one orchestrator with two note-reading subagents on the CLI's default model
and never reproduced the reported behavior ("5-6 concurrent orchestrators throw
ThrottlingException"). This workload copies the shape of the workflow that produced
that report instead:

- four agents registered under `.kiro/agents/*.json`: an orchestrator, a data collector,
  a spec generator and a quality reviewer, each with its own model. The original ran the
  spec generator on Opus and the rest on Sonnet, so per-model limits are now in play.
- phases run one subagent at a time (the original's `max_parallel` defaulted to 1, an
  "orchestrator + one subagent" pair), with `--parallel` to fan
  the per-sub-function phases out.
- every phase runs deterministic Python tools through the shell tool, the way as-is ran
  `python tools/*.py`, and ends with a validator call.
- a C source of realistic size is read in full, and the review is followed by one
  feedback rewrite and a final review.

The input is generated: a C file with one function per sub-function, each ending in a
check constant, so the final review's total proves the source was read to the end. No
as-is prompt, tool or data leaves the reference tree; only the shape is copied.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

ORCHESTRATOR = "asis-orchestrator"
COLLECTOR = "asis-data-collector"
GENERATOR = "asis-spec-generator"
REVIEWER = "asis-quality-reviewer"

DEFAULT_ORCHESTRATOR_MODEL = "claude-sonnet-4.6"
DEFAULT_GENERATOR_MODEL = "claude-opus-4.8"
"""The original workflow's models: Opus for the spec generator, Sonnet elsewhere."""

TOOLS = ["fs_read", "fs_write", "execute_bash", "use_subagent"]
"""Kiro CLI 2.21.4 tool names. Subagents get the first three; only the orchestrator delegates."""

_WORDS = [
    "account", "billing", "ledger", "invoice", "balance", "charge", "credit", "debit",
    "period", "rate", "plan", "customer", "contract", "discount", "usage", "record",
    "batch", "cycle", "status", "code", "amount", "total",
]


def check_value(index: int) -> int:
    """The constant at the end of sub-function `index` (1-based)."""
    return index * 37 + 11


def expected_total(sub_functions: int) -> int:
    return sum(check_value(i) for i in range(1, sub_functions + 1))


def source_c(sub_functions: int, lines_per_function: int) -> str:
    """A C file with one function per sub-function; its check constant is its last line."""
    out = ["/* Synthetic billing batch program for the M1e benchmark. */", "#include <stdio.h>", ""]
    for i in range(1, sub_functions + 1):
        out.append(f"/* SF-{i:04d}: batch step {i} */")
        out.append(f"int sf_{i:04d}(int *acc, int n) {{")
        out.append("    int total = 0;")
        for k in range(lines_per_function):
            a = _WORDS[(i * 7 + k * 3) % len(_WORDS)]
            b = _WORDS[(i * 5 + k * 11) % len(_WORDS)]
            out.append(f"    if (acc[{k % 16}] > {k}) total += acc[{k % 16}]; /* {a} {b} */")
        out.append("    return total;")
        out.append("}")
        out.append(f"/* SF-{i:04d} check value: {check_value(i)} */")
        out.append("")
    return "\n".join(out) + "\n"


def _padding(words: int, seed: int) -> str:
    """Neutral guideline prose, so agent prompts carry as-is-sized system text (~8-12 KB)."""
    body = [_WORDS[(seed + k * 7) % len(_WORDS)] for k in range(words)]
    return "\n".join(" ".join(body[p : p + 20]) + "." for p in range(0, words, 20))


_COLLECT = '''\
import json, shutil, sys
from pathlib import Path
Path("data").mkdir(exist_ok=True)
shutil.copy("seed/program.c", "data/program.c")
meta = json.loads(Path("seed/meta.json").read_text())
Path("data/meta.json").write_text(json.dumps(meta, indent=2))
print(json.dumps({"status": "ok", "source": "data/program.c", "sub_functions": meta["sub_functions"]}))
'''

_VALIDATE = '''\
import json, sys
from pathlib import Path
kind, path = sys.argv[1], Path(sys.argv[2])
meta = json.loads(Path("data/meta.json").read_text())
if not path.is_file():
    print(json.dumps({"status": "FAIL", "reason": "missing " + str(path)})); sys.exit(1)
text = path.read_text(errors="replace")
need = meta["sub_functions"] if kind in ("summary", "spec", "final") else [kind]
missing = [s for s in need if s not in text]
print(json.dumps({"status": "FAIL" if missing else "PASS", "kind": kind, "missing": missing}))
sys.exit(1 if missing else 0)
'''

_MERGE = '''\
import json
from pathlib import Path
meta = json.loads(Path("data/meta.json").read_text())
parts = [Path(f"output/br_{s}.md").read_text() for s in meta["sub_functions"] if Path(f"output/br_{s}.md").is_file()]
Path("output/metadata.md").write_text("\\n\\n".join(parts))
print(json.dumps({"status": "ok", "merged": len(parts), "expected": len(meta["sub_functions"])}))
'''


def sub_function_ids(sub_functions: int) -> list[str]:
    return [f"SF-{i:04d}" for i in range(1, sub_functions + 1)]


def agent_configs(orchestrator_model: str, generator_model: str, prompt_words: int) -> dict[str, dict]:
    def agent(name: str, model: str, role: str, tools: list[str], seed: int) -> dict:
        config = {
            "name": name,
            "description": role,
            "prompt": f"{role}\n\nGuidelines:\n{_padding(prompt_words, seed)}",
            "tools": tools,
            "allowedTools": tools,
        }
        if model:
            config["model"] = model
        return config

    sub_tools = TOOLS[:3]
    orchestrator = agent(
        ORCHESTRATOR, orchestrator_model,
        "You orchestrate a specification workflow by delegating each phase to a subagent.",
        TOOLS, 1,
    )
    # M1e saw 8 of 33 runs stop delegating with "Subagent cannot run in non-interactive mode
    # with tool permission request". Pin the subagents the orchestrator may spawn to the three
    # registered (pre-approved) agents, so it cannot fall back to a built-in agent whose tools
    # need approval. Both key spellings are read by Kiro CLI 2.21.4.
    delegates = [COLLECTOR, GENERATOR, REVIEWER]
    orchestrator["toolsSettings"] = {
        key: {"availableAgents": delegates, "trustedAgents": delegates}
        for key in ("use_subagent", "subagent")
    }
    return {
        ORCHESTRATOR: orchestrator,
        COLLECTOR: agent(
            COLLECTOR, orchestrator_model,
            "You collect program data with the provided Python tools and summarise the source.",
            sub_tools, 2,
        ),
        GENERATOR: agent(
            GENERATOR, generator_model,
            "You write functional specifications and business rules from C source code.",
            sub_tools, 3,
        ),
        REVIEWER: agent(
            REVIEWER, orchestrator_model,
            "You review specifications against the C source and report findings.",
            sub_tools, 4,
        ),
    }


def write_workspace(
    workspace: Path,
    sub_functions: int,
    lines_per_function: int,
    orchestrator_model: str,
    generator_model: str,
    prompt_words: int = 1500,
) -> None:
    files = {
        "seed/program.c": source_c(sub_functions, lines_per_function),
        "seed/meta.json": json.dumps({"program_id": "SYNTH0001", "sub_functions": sub_function_ids(sub_functions)}),
        "tools/collect.py": _COLLECT,
        "tools/validate.py": _VALIDATE,
        "tools/merge.py": _MERGE,
    }
    for name, config in agent_configs(orchestrator_model, generator_model, prompt_words).items():
        files[f".kiro/agents/{name}.json"] = json.dumps(config, indent=2)
    for relative, text in files.items():
        path = workspace / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    (workspace / "output").mkdir(exist_ok=True)


def prompt(sub_functions: int, parallel: int) -> str:
    """The orchestrator prompt: as-is phases, one subagent at a time unless `parallel` > 1."""
    ids = ", ".join(sub_function_ids(sub_functions))
    batch = (
        "one subagent at a time; wait for each to finish before starting the next"
        if parallel <= 1
        else f"up to {parallel} subagents in parallel in a single response, then the next batch"
    )
    return f"""You are the orchestrator of a specification workflow for program SYNTH0001.
Delegate every phase with the use_subagent tool. Do not read the source or write outputs yourself.
Every use_subagent call must name one of the agents {COLLECTOR}, {GENERATOR} or {REVIEWER}; never
delegate to the default agent. If a delegation fails, retry it with the named agent instead of doing the work yourself.
Each subagent must read every file it uses in full, from the first line to the last, never a line range.
Each subagent runs shell commands only of the form `python3 tools/<name>.py ...` from the workspace root.

Phase 1 (agent {COLLECTOR}): run `python3 tools/collect.py`, read data/program.c and data/meta.json in full,
write output/source_summary.md describing every sub-function ({ids}) with its check value,
then run `python3 tools/validate.py summary output/source_summary.md`.

Phase 2 (agent {GENERATOR}): read output/source_summary.md and data/program.c in full, write
output/spec.md with one section per sub-function, then run `python3 tools/validate.py spec output/spec.md`.

Phase 3 (agent {GENERATOR}, one subagent per sub-function, {batch}): for sub-function S, read data/program.c
and output/spec.md in full, write output/br_S.md with S's business rules and check value, then run
`python3 tools/validate.py S output/br_S.md`. After all of them, run `python3 tools/merge.py` yourself.

Phase 4 (agent {REVIEWER}, one subagent per sub-function, {batch}): for sub-function S, read data/program.c,
output/spec.md and output/br_S.md in full, write output/review_S.md with findings, then run
`python3 tools/validate.py S output/review_S.md`.

Phase 5 (agent {GENERATOR}): read every output/review_*.md and output/spec.md, rewrite output/spec.md
addressing the findings, then run `python3 tools/validate.py spec output/spec.md`.

Phase 6 (agent {REVIEWER}): read output/spec.md and data/program.c in full and write output/final.md with
a verdict per sub-function and the sum of all check values, then run `python3 tools/validate.py final output/final.md`.

When phase 6 is done, reply with exactly: done"""


def expected_delegations(sub_functions: int) -> int:
    """Subagent calls a run makes when it follows the as-is shape: phases 1, 2, 5, 6 + 2 per SF."""
    return 4 + 2 * sub_functions


def expected_outputs(sub_functions: int) -> list[str]:
    ids = sub_function_ids(sub_functions)
    return (
        ["output/source_summary.md", "output/spec.md", "output/metadata.md", "output/final.md"]
        + [f"output/br_{s}.md" for s in ids]
        + [f"output/review_{s}.md" for s in ids]
    )


def outputs_present(workspace: Path, sub_functions: int) -> int:
    return sum((workspace / p).is_file() for p in expected_outputs(sub_functions))


def final_total_ok(workspace: Path, sub_functions: int) -> bool | None:
    path = workspace / "output" / "final.md"
    if not path.is_file():
        return None
    text = re.sub(r"(?<=\d),(?=\d{3})", "", path.read_text(errors="replace"))
    return str(expected_total(sub_functions)) in re.findall(r"\d+", text)


_SUBAGENT_CALL = re.compile(r"use_subagent|subagent", re.IGNORECASE)
_BASH_CALL = re.compile(r"python3 tools/", re.IGNORECASE)


_INVOKE = re.compile(r"Invoking subagent:\s*([\w.-]+)")
_DENIAL = re.compile(
    r"cannot run in non-interactive mode|tool permission|^\[denied\]|approval|requires interactive",
    re.IGNORECASE,
)


def subagent_invocations(tool_log: str) -> dict[str, int]:
    """How many times each agent name was delegated to, from 'Invoking subagent: <name>' lines."""
    from fleetbench.fanout import _ANSI  # shared ANSI stripping; lazy to avoid a cycle

    counts: dict[str, int] = {}
    for name in _INVOKE.findall(_ANSI.sub("", tool_log)):
        counts[name] = counts.get(name, 0) + 1
    return counts


def delegation_denied(tool_log: str) -> bool:
    """True when the run hit a permission denial (the M1e failure that made runs skip subagents)."""
    return bool(denial_context(tool_log))


def denial_context(tool_log: str, before: int = 6, after: int = 2, limit: int = 30) -> list[str]:
    """Masked lines around each permission denial, so a run that stopped delegating says why.

    The record's tails keep only the last lines; the first denial happens early in the run.
    """
    from fleetbench.fanout import _ANSI, _LONG_TOKEN

    lines = [_ANSI.sub("", x).strip() for x in tool_log.replace("\r", "\n").splitlines() if x.strip()]
    keep: list[int] = []
    for i, line in enumerate(lines):
        if _DENIAL.search(line):
            keep.extend(range(max(0, i - before), min(len(lines), i + after + 1)))
    seen: list[int] = sorted(set(keep))[:limit]
    return [_LONG_TOKEN.sub("<REDACTED>", lines[i])[:240] for i in seen]


def count_calls(tool_log: str) -> tuple[int, int]:
    """(lines mentioning a subagent, lines mentioning a tools/*.py run) -- rough activity proof."""
    lines = tool_log.splitlines()
    return (
        sum(1 for line in lines if _SUBAGENT_CALL.search(line)),
        sum(1 for line in lines if _BASH_CALL.search(line)),
    )
