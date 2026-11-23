# Developing and testing

Part of the [sample README](../README.md).

## Run it locally

The allocation, worker loop, and result verification run on one machine, with no AWS account. The queue
and result store are local implementations of the same interfaces the SQS and S3 ones satisfy, so the
loop you exercise here is the loop that runs in the fleet.

DynamoDB Local is required, because the reservation is the part worth exercising and an in-process AWS
fake does not reproduce write contention:

```bash
docker run -d --rm --name ddb-local -p 8000:8000 \
    amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -inMemory -sharedDb

cd worker
uv sync --frozen --extra dev
uv run --frozen python -m runner.local_batch --items 8 --subscriptions 3 --concurrency 2 --workers 5
```

To run an actual workflow rather than an ad-hoc prompt, name one. This is the only way to exercise a
workflow's MCP servers and steering locally, because it seeds them into the run's isolated `HOME` the way a
task's are seeded:

```bash
uv run --frozen python -m runner.local_batch \
    --items 3 --subscriptions 1 --workers 1 \
    --workflow ../workflows/examples/convention-audit \
    --input-dir ./runner
```

It registers a subscription pool, seeds work, starts workers that contend for capacity, runs a workflow
per item, verifies each result, and reports whether every item produced a stored result and all capacity
came back.

With `KIRO_API_KEY` set it runs the real Kiro CLI. Without a credential it runs a built-in workflow so
the loop can still be demonstrated, and says which one it used, because a run that never called the CLI
proves less. Pass `--no-cli` to force the built-in one.

A local run isolates `HOME` and seeds the named workflow's configuration into it, so it starts from the
same place a task does rather than from whatever you have configured. It is still not evidence about fleet
behavior — the container is the reproducible environment, and image pulls, scale-in, and contention across
hosts only exist there. What a local run gives you is the loop, the allocation, and the workflow itself.

## Tests and the gates before a deployment

```bash
./tools/ci.sh                 # every gate, in order
./tools/ci.sh --skip-image    # the fast ones, with no container runtime
```

One script rather than a pipeline definition, so the same gates run on a laptop and in CI — a check that
only runs inside a pipeline is a check nobody runs before pushing. `buildspec.yml` and
`.github/workflows/ci.yml` are thin wrappers that call it, and the gates are defined once.

The publication scan runs first despite being the quickest, because it is the only gate whose failure
cannot be undone: a value committed once stays reachable in history after it is removed. The image build
is last, and it is a gate in its own right — it refuses a base image whose glibc is below what the target
architecture's CLI build needs, verifies the CLI against a pinned checksum, and fails if a declared MCP
server is not on `PATH` afterwards.

**Build the image on compute matching the target architecture.** An emulated build succeeds and changes
every timing the image is later used to measure, which is the expensive kind of wrong: nothing fails.

**Build before pushing, every time.** `docker tag` and `docker push` will happily ship whatever
`kiro-fleet-worker:latest` already points at in the local daemon. A stale image produces a fleet that
works and silently lacks whatever was added since — the symptom is a feature that is absent rather than
broken, which is the hardest kind to attribute. Verify what you pushed:

```bash
docker image inspect kiro-fleet-worker:latest --format '{{.Created}}'
```

To run the worker tests alone:

```bash
cd worker
uv run --frozen pytest
```

No AWS account, credentials, or deployed resources are needed. Contention tests need DynamoDB Local and
skip with an actionable message without it.
