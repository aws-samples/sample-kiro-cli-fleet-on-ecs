# convention-audit

Audits source files against conventions that live in steering files rather than in the prompt.

This example exists to demonstrate the thing that makes running the CLI worthwhile instead of calling a
model API: **the instructions a team already trusts locally move onto the fleet unchanged.** Steering files
are markdown in a directory. They are the payload.

## Why the rules are not in the prompt

Compare the two examples in this repository.

| | `repo-file-summary` | `convention-audit` |
|---|---|---|
| Where the instructions live | the prompt | `steering/*.md` |
| What changes when a rule changes | the prompt, per workflow | one file, shared by every workflow that carries it |
| Where the rules come from | written for this workflow | copied from what the team already runs |

The second shape is the one that scales across a team. A codebase's conventions are not written for a batch
job; they already exist, in whatever form the team keeps them. If moving them onto a fleet means rewriting
them as prompt text, they drift from the originals immediately, and the fleet starts enforcing last
quarter's rules.

The prompt here is deliberately thin, and says the steering wins where the two disagree.

## What it does

| | |
|---|---|
| Input | one or more files under `./input`, named by the work item |
| Output | `./output/<name>.findings.md` per input file, with fixed sections |
| Tools | `fs_read`, `fs_write` |
| Steering | `naming.md`, `error-handling.md`, `reporting.md`, seeded to `~/.kiro/steering/` |
| MCP | none |

`expectsStdout` is `false`, unlike the other example. This workflow's product is a file; a run that writes
its findings and then says nothing on stdout has done its job. Leaving the flag at `true` would fail those
runs, which is why it is a per-workflow declaration rather than a fleet-wide setting.

## The count line proves the steering was loaded

`reporting.md` requires every findings file to end with:

```
AUDIT-COMPLETE: <number of entries under Findings>
```

That rule appears in the steering and **nowhere in the prompt**, which is deliberate and worth keeping that
way. The model cannot produce that line unless the steering reached it, so the line's presence in a result
answers a question that is otherwise hard to answer: whether the CLI actually applies user-level steering in
a headless session. Checking `/context` would need an interactive session; this needs only the output.

It is also a genuinely useful convention, which is why it stays in the example rather than being a test
fixture. A truncated model response looks like a short report, and a report missing its count line is
unambiguously incomplete — that distinction is what lets a script check several hundred results.

If you edit this example, keep the rule out of `prompt.md`.

## The steering rules here are placeholders

`naming.md` and `error-handling.md` are written to be plausible, not to be adopted. Replace them with your
own. The mechanism is the point; these are what makes the example runnable without shipping anyone's real
standards.

## Status of verification

The loader, the seeding, and the failure modes are covered by tests in `worker/tests/test_runtime.py`,
which assert the files land at the path the CLI reads from. That path is taken from the CLI's own
instruction to the user — it names `~/.kiro/steering/` (user-level) and `.kiro/steering/`
(workspace-level) in its configuration listing.

**Confirmed on a credentialed run:** the CLI applies them in a headless `chat --no-interactive` session.
`steering/reporting.md` asks for an `AUDIT-COMPLETE: <count>` line that appears nowhere in the prompt, so
the model can only write it if the steering was loaded. A run in the arm64 image with a real key wrote
`AUDIT-COMPLETE: 0`, cited the other two files by name, and followed the reporting file's rule against
assigning severities. A test pins that the line stays out of the prompt, because copying it there would
remove the evidence while every other test still passed.

## Running one item locally

```bash
cd worker
uv run --frozen python -m runner.local_batch \
    --items 3 --subscriptions 1 --workers 1 \
    --workflow ../workflows/examples/convention-audit \
    --input-dir ./runner \
    --keep
```

`--workflow` is what makes this exercise the steering: it drives the run from `workflow.json` and seeds the
steering files into the run's isolated `HOME`, which is where a task's CLI reads them from too. Without it
the runner takes a bare `--prompt` and no steering reaches the CLI at all.

Needs `KIRO_API_KEY` for a real run; without one the runner substitutes a stand-in and says so, which
exercises the loop and the input staging but not the CLI.
