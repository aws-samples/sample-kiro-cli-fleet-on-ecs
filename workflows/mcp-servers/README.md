# mcp-servers

The MCP servers the shipped workflows declare, pinned here and installed into the worker image.

## Why this directory exists

The example workflow originally configured its server the usual way:

```json
{ "command": "npx", "args": ["-y", "@modelcontextprotocol/server-sequential-thinking"] }
```

A capture of one container run showed why that does not survive contact with a fleet. `registry.npmjs.org`
carried **94% of all network traffic**, 5.6 MB, because `npx` downloads the server on every invocation — once
per work item, on every task.

Two consequences, and the first one is fatal:

**It cannot run in the network this sample deploys.** Egress is restricted to named destinations, and npm is
not one of them. `--require-mcp-startup` would fail, and with it every task. The fleet could not have run its
own example workflow.

**It undid the pinning.** The base image is pinned by digest and the Kiro CLI is verified against a checksum,
and then the agent's tooling arrived from whatever the registry served at the moment a task started. Nothing
recorded which version ran, so a result could not be tied to the tools that produced it.

## How it is pinned

`package-lock.json` pins **112 packages** by integrity hash — the server, and everything it depends on. That
number is the point: `@modelcontextprotocol/server-sequential-thinking` declares three dependencies, and
three becomes a hundred and twelve. Pinning only the top-level version would leave the rest floating.

The image runs `npm ci`, which installs the lockfile exactly and fails rather than resolve something newer.
`--ignore-scripts` is set: a lifecycle script runs arbitrary code at image build time, and nothing here needs
one.

## Adding a server

```bash
cd workflows/mcp-servers
npm install --package-lock-only --ignore-scripts <package>@<exact-version>
```

Commit both files, then rebuild the image. Reference it from a workflow's `mcp.json` by the command name its
package installs, not by a path:

```json
{ "mcpServers": { "name": { "command": "mcp-server-...", "args": [] } } }
```

The entry points are symlinked onto `PATH` in the image, so the same `mcp.json` works in a container and on a
laptop. A config naming `/opt/mcp-servers/...` would have to differ between the two, and then a local run
would stop being evidence about a task.

## Running the workflows locally

The image has these installed; your machine does not. Either install them globally at the pinned versions:

```bash
npm install -g @modelcontextprotocol/server-sequential-thinking@2026.8.31
```

or run the workflow in the container, which is the reproducible environment in any case.

The version above must match `package.json`. If it drifts, a local run exercises a different server than a
task does, which is the failure this directory exists to prevent.
