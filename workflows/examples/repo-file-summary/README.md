# repo-file-summary

Reads source files and writes one fixed-format note per file.

Chosen as the example because it is the shape most fan-out work takes, many independent inputs against one
unchanging instruction, and because it needs no proprietary data: point it at any repository you can read.

## What it does

| | |
|---|---|
| Input | one or more files under `./input`, named by the work item |
| Output | `./output/<path>.md` per input file, with fixed sections |
| Tools | `fs_read`, `fs_write` |
| MCP | `sequentialthinking` |

The output format is fixed on purpose. A summary in prose would be impossible to check; sections that are
always present, and that say "None" rather than being omitted, make a missing or malformed result obvious
to a script.

## Running one item locally

```bash
cd worker
uv run --frozen python -m runner.local_batch \
    --items 3 --subscriptions 1 --workers 1 \
    --workflow ../workflows/examples/repo-file-summary \
    --input-dir ./leaseclient \
    --keep
```

`--workflow` drives the run from this directory's `workflow.json`, so the prompt, the tool grant, and the
MCP servers are the ones a task would use rather than a command line that resembles them. `--input-dir`
seeds one work item per file, which is the shape fan-out work has.

Requires `KIRO_API_KEY` in the environment for a real run; without it the local runner substitutes a
stand-in workflow and says so.

## What a good result looks like

Every input file has a corresponding note, each note has all four headings, and nothing exists outside
`./output`. The worker checks all three: a run that answers on stdout but writes no file is a failure, and
so is a run that writes somewhere it was not supposed to.
