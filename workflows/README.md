# workflows

What an author provides, and what the fleet guarantees in return.

A workflow is data, not code. It ships in this repository, is baked into the worker image, and is
checksummed onto that image, so the prompt that produced a result can be identified from the result.

## The contract

The fleet gives a workflow:

- a working directory it owns, and nothing outside it
- `./input` holding whatever the work item declared, placed there before the run
- `./output`, empty, where results go
- a Kiro subscription already authenticated, through the environment
- exactly the tools the workflow declared, and no others

A workflow gives the fleet:

- a `workflow.json` declaring what it needs
- a prompt that names where output goes
- output under `./output`, or a failure

Paths are relative, always. That is what lets the same workflow run on a laptop and on a task without
being rewritten, and it is why the directory is fixed rather than configurable.

## `workflow.json`

```json
{
  "name": "repo-file-summary",
  "description": "one line, for an operator reading a log",
  "promptFile": "prompt.md",
  "trustTools": ["fs_read", "fs_write"],
  "requiresMcpServers": ["sequentialthinking"],
  "steering": false,
  "expectsStdout": true
}
```

This is the repository's own format, read by the worker. It is deliberately not the CLI's agent
configuration file: that schema cannot be validated without a credential, because `kiro-cli agent validate`
requires authentication, and shipping a configuration whose correctness cannot be checked at build time
would mean discovering a typo on a task instead of in a build.

`trustTools` becomes `--trust-tools`. It has to include a write tool if the workflow is expected to produce
output, or every run fails as producing nothing: the tool grant is part of the completion contract, not a
separate security setting.

Every tool in `trustTools` runs with the task role, because the agent shares the worker's container and
user. Do not add `shell` (`execute_bash` in older CLI versions) to a workflow whose inputs you did not
produce yourself: input content can steer the agent, and a trusted shell can use the role to read every
subscription key in the pool and overwrite any result. See [Security](../README.md#security).

`requiresMcpServers` names servers that must be present in `mcp.json`. The run always uses
`--require-mcp-startup`, so a missing server fails the task at start rather than part way through.

`expectsStdout` says whether an empty answer counts as a failure. A workflow that only writes files can set
it false.

## `steering/`

The instructions a team already keeps, carried as files rather than rewritten as prompt text. This is the
reason the unit of work is the CLI and not a model API call: steering is markdown in a directory, so it
moves onto the fleet as it is.

```
my-workflow/
├── workflow.json        "steering": true
├── prompt.md
└── steering/
    ├── naming.md
    └── error-handling.md
```

Every `*.md` in the directory is copied, in sorted order, to `~/.kiro/steering/` before the CLI starts —
the user-level location the CLI names in its own configuration listing. Workspace-level steering
(`.kiro/steering/` beside the work) is the other option the CLI supports and is deliberately not used here:
it would sit inside the directory the workflow may write to, where the output guard would see it as
something the run created, and where a run could edit the rules it is being held to.

The directory is replaced on every task, not merged, so the instructions in effect are exactly the ones the
workflow ships. A stale file left behind would mean a run governed partly by a previous workflow's
conventions, and the output would look plausible either way.

`"steering": true` is required. A `steering/` directory without it is an error rather than a silent skip,
because a workflow running without the conventions it was written against still produces output — just not
the output anyone asked for, and nothing about the result says so.

See [`examples/convention-audit`](examples/convention-audit/) for a workflow whose rules live entirely in
steering and whose prompt only says what to produce.

**Skills** use the same mechanism at `~/.kiro/skills/`, which the CLI names alongside steering. The sample
does not ship one, so that path is not exercised here.

## `mcp.json`

Standard Model Context Protocol server configuration:

```json
{ "mcpServers": { "name": { "command": "...", "args": ["..."], "env": {} } } }
```

Note that retrieving MCP settings requires a valid credential: the CLI reports MCP as disabled without
one, so MCP configuration cannot be exercised at image build time. The build verifies that the server
binary is installed and answers the protocol; that the CLI starts it is verified on a task.

## Where output may go

Only `./output`. The worker takes a snapshot of the workspace before the run and compares afterwards; a
file created or modified anywhere else fails the run, and the result is not stored.

Checked by the worker rather than by a CLI hook, for two reasons. A hook is configured through the agent
file, whose schema cannot be validated here. And a check the model cannot see is a check the model cannot
work around.

## Adding a workflow

Create a directory with `workflow.json`, the prompt it names, `steering/` if it carries conventions, and
`mcp.json` if it needs servers. Keep the
prompt explicit about the output path: a prompt that does not say where to write produces a run that writes
nowhere, and the completion check will call that a failure, correctly.
