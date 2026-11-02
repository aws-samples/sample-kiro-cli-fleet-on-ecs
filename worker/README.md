# worker

What runs inside one task: the container image, the entrypoint, the loop that drains work,
and `leaseclient`, which allocates a Kiro subscription from the governed pool.

## Building the image

```bash
./tools/build-worker-image.sh                    # arm64, the default
./tools/build-worker-image.sh --arch x86_64
```

Build from the repository root, not from here: the image carries both the worker and the
workflow definitions.

The build verifies rather than trusts. It fails on a checksum mismatch for the Kiro CLI
artifact, and it fails when the base image's glibc is below what the chosen architecture's
CLI build requires. Both are checked by building deliberately wrong, because a guard that
has never refused anything is not known to work.

Architecture is one build argument. It selects the base platform, the CLI artifact, and the
recorded label. Changing it is a value change plus the matching checksum, not an edit spread
through the Dockerfile.

The CLI version and its checksum are recorded together and changed together. Resolving the
checksum from the published manifest at build time looks tidier and defeats the point: that
manifest describes the *latest* release, so a pinned 2.21.4 broke the moment 2.22.1 shipped.
There is no way to look up a past version's checksum afterwards, so use
`--checksums <version>` to compute them for pasting.

### Image size

Three numbers get called image size and only one is what a task waits for.

| | |
|---|---|
| pull, compressed | 0.69 GB — crosses the network, once per host |
| on disk | 1.4 GB — what the task's filesystem holds |
| `docker images` | 2.2 GB — counted differently again |

About 986 MB uncompressed is the CLI's own three binaries, so this is not a Dockerfile
problem: a probe image carrying only the CLI measures the same as one carrying the full
toolchain. The levers are a pull-through cache and not adding tools a workflow does not use.

Dropping `npm` from the runtime stage accounted for roughly 180 MB of this, and it was not
done for the size. The MCP servers are installed from a lockfile while the image is built,
so a running task has no reason to be able to install anything — and, more to the point, no
need to reach a package registry at all. See
[`workflows/mcp-servers`](../workflows/mcp-servers/) for what that fixed.

## Running a batch

```bash
docker run -d --rm --name ddb-local -p 8000:8000 \
    amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -inMemory -sharedDb

uv run --frozen python -m runner.local_batch --items 8 --workers 5
```

With `KIRO_API_KEY` set it runs the real CLI; without it a stand-in workflow, and it says
which, because a run that never called the CLI proves less. A local run against the real CLI
is a convenience rather than evidence about the fleet: the CLI reads agent and MCP
configuration from under `HOME`, so it picks up whatever you have configured.

In a task, `runner.main` reads its configuration from the environment and uses SQS and S3
behind the same interfaces the local implementations satisfy.

## Verifying the two properties that need a real container

```bash
./tools/compare-container-and-local.sh          # same batch, both ways, same results
./tools/verify-sigterm-returns-capacity.sh      # scale-in returns capacity
```

The first keeps local iteration meaningful: everything is developed on a laptop and measured
from a task, so if the two diverge the tests are about something other than what runs.

The second exists because the failure it catches is invisible. A container's PID 1 does not
get the default disposition for a signal, so without an explicit handler SIGTERM is discarded
and the subscription stays held until its lease expires. The pool keeps working and simply
loses a lease lifetime of capacity on every scale-in, which reads as slow rather than broken.

## Exit statuses

Scale-in makes several outcomes normal, and a fleet cannot tell them from a crash if they
share a status.

| Status | Meaning |
|---|---|
| 0 | work completed, the queue was empty, or a stop was requested and honoured |
| 64 | the pool was saturated — the fleet outgrew it, which is expected |
| 70 | the workflow environment is broken — a deployment problem, not a work item problem |
| 78 | configuration is missing — fails before any work item is touched |

## Tests

```bash
uv run --frozen pytest
```

No AWS account, credentials, or deployed resources. The harness severs every path boto3
could use to reach a real account, including named profiles and the instance metadata
service.

Contention tests need DynamoDB Local and skip with an actionable message without it. They do
not run against the in-process fake, and that is a measurement rather than a preference: a
deliberately non-atomic reservation passes the concurrent scenario against the fake unchanged
while handing out four times the real capacity against DynamoDB Local.

## Layout

| Path | Contents |
|---|---|
| `Dockerfile` | multistage: fetch and verify, then assemble |
| `entrypoint.sh` | asserts configuration, then `exec`s so the worker is PID 1 |
| `leaseclient/` | subscription allocation — see its own README |
| `runner/worker.py` | the loop: take an item, run it, verify, store, acknowledge |
| `runner/workflow.py` | running one workflow and deciding whether it worked |
| `runner/protection.py` | scale-in protection for the item in progress, through the ECS agent |
| `runner/definition.py` | loading a workflow definition and seeding the CLI's configuration |
| `runner/guard.py` | confining writes to the output directory |
| `runner/aws.py` | the SQS and S3 implementations |
| `runner/local.py` | the local implementations, for running without AWS |
| `runner/main.py` | what a task runs |
| `runner/local_batch.py` | what a developer runs |
| `reconciler/` | recovering capacity from workers that vanished |
