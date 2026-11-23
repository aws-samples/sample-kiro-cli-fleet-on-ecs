# Design

Part of the [sample README](../README.md).

## The problem this addresses

Running one Kiro CLI workflow is straightforward. Running several hundred of them concurrently, without
a person watching, raises three problems that have to be solved together, because a solution to any one
of them alone does not survive contact with the other two.

**Capacity that is only occasionally needed.** The workload is bursty: idle for hours, then a few hundred
concurrent runs, then idle again. A standing fleet is billed while idle. Scaling on queue depth to zero
costs nothing between batches, but introduces cold starts and image pull behavior that only show up at
scale.

**Per-subscription limits that are not enforced for you.** Each Kiro subscription has a ceiling on
concurrent streams and a monthly credit budget. Exceeding the concurrency ceiling surfaces as throttling
partway through a run, discarding work already done. Exhausting a budget stops a subscription without
warning. Scaling past one subscription's ceiling therefore means distributing across a pool, and
distributing across a pool means allocating from it without two workers claiming capacity that does not
exist.

**No terminal to watch.** Headless runs produce no interactive session. When a batch of several hundred
finishes, "did it work" is not answerable from an exit code alone, because a workflow can complete
successfully while producing wrong output — for instance if a tool it depends on was missing from the
image. Verification has to be built in rather than added afterward.

## Architecture

```
git commit ──▶ CodePipeline ──▶ CodeBuild ──▶ ECR ──▶ CDK deploy
                                (arm64 image,          (infra + task definition,
                                 pinned toolchain)      service at min capacity 0)

operator ──▶ register subscriptions ──▶ DynamoDB registry + Secrets Manager

run ──▶ seeding Lambda: manifest ──▶ SQS (one message per work item)
                                      │
        scaling function ─────────────┴──▶ target tracking ──▶ Fargate tasks (min 0)
          (backlogPerTask metric; sets the count itself
           off zero and back to zero)
                                                                  │
                                                                  │ per task:
                                                                  │  claim a subscription (atomic)
                                                                  │  load its key from Secrets Manager
                                                                  │  take one message
                                                                  │  sync input S3 ──▶ /workspace
                                                                  │  run kiro-cli headless
                                                                  │  sync /workspace ──▶ results S3
                                                                  │  repeat until the queue drains
                                                                  ▼
                                       results fan-in (S3) + CloudWatch dashboard
```

Deliberate choices worth naming, because each has a cheaper alternative that fails in a specific way:

- **SQS for work items, DynamoDB conditional writes for subscription keys.** Visibility timeout already
  gives exclusive ownership of a work item, so a second claim mechanism there would be redundant. A
  subscription key is different: it needs a counter, because several workers legitimately share one
  subscription up to its ceiling.
- **Concurrency as a hard gate, credits as a guardrail with a margin.** The held-lease count is
  incremented by us and is exact. Credit consumption is reported by the Kiro service, about 45 seconds
  late and without the runs still in flight, so `creditsUsed` is an estimate that the margin covers. It is
  read from `/usage` before every work item and only ever raised within a window. Conflating the two
  produces a pool that stalls unpredictably.
- **Expiry-first recovery.** Fargate tasks are stopped by scale-in and by crashes, so a design where the
  release path must run for capacity to return would leak capacity continuously. Leases expire on a TTL,
  and a scheduled reconciler deletes expired leases itself rather than waiting for DynamoDB's TTL
  deletion, which has no time bound; explicit release is an optimization on top of that.
- **Scale-in protection for the item in progress.** A lower ceiling, target tracking, a drain to zero and
  a deployment all stop tasks, and ECS chooses which ones without regard to the subscription they hold.
  A run stopped part-way has already spent its credits and is redelivered, so the item would cost credits
  twice. Each worker protects its task from scale-in while it holds an item, so ECS stops it only between
  items, unless ECS refused that item's protection request. See
  [Scale-in and the item in progress](scale-in.md).
- **VPC endpoints, no NAT gateway, by default.** Tasks have no internet route; the AWS APIs they call and
  the Kiro service (through the `q` endpoint) are reached over VPC endpoints. See [Security](security.md)
  for the two modes that add a NAT gateway.
- **`arm64` on Graviton**, with every dependency pinned so a run is reproducible.

## Repository layout

| Path | Contents |
|---|---|
| `infra/` | AWS CDK application (TypeScript) |
| `worker/runner/` | The task: drain loop, workflow runner, scale-in protection, visibility renewal |
| `worker/leaseclient/` | Subscription registry client: claim, heartbeat, release, credit readings, metrics |
| `worker/seeding/` | Lambda function turning a manifest into queue messages |
| `worker/reconciler/` | Lambda function recovering leases from crashed workers |
| `worker/scaling/` | Scaling signal function: backlog per task, pool ceiling, start from and drain to zero |
| `worker/usagereport/` | Lambda function raising `creditsUsed` from Kiro's daily activity report |
| `worker/taskstopped/` | Lambda function counting each stopped task by why it stopped, from the ECS event |
| `workflows/` | Prompts as code: example workflow, plus scaffolds for common workflow shapes |
| `tools/` | Image build, secret scan, and the verification entry point |
| `bench/` | Benchmark harness producing the measurements the blog post reports |

## Roadmap

Built in dependency order, each component specified before it was implemented.

| Component | State |
|---|---|
| Subscription lease — registry, atomic claim, heartbeat, release | done; contention verified against DynamoDB Local, which an in-process fake does not reproduce |
| Subscription lease — crash recovery, leak sweep, claim-time budget check, manual credit correction, window reset | done |
| Credit tracking — `/usage` before every item, monotonic writes, mid-drain budget stop, optional daily activity report | done in code and tests. Readings, the claim-time cutoff, an exhausted pool starting nothing, and the stop partway through a batch all run on AWS ([what has been verified](credits.md#what-has-been-verified-on-aws)). The activity report has never been ingested from a file Kiro wrote ([what is not verified](credits.md#what-is-not-verified)) |
| Worker loop — completion check, remedies, local queue and result store | done; runs end to end against the real CLI |
| Worker runtime — image, entrypoint, prompts as code, steering as payload | done; cooperative stop and container/local equivalence both shown |
| Data, queue, network, compute, functions — CDK stacks | done; deployed, and a workflow ran to completion on Fargate |
| Observability — dashboard and alarms | done; one dashboard in three layers, alarms on the two conditions that need a person |
| Verified builds — one entry point, thin CI wrappers | done; both failure paths exercised. A CodePipeline is optional and off by default (`infra/lib/pipeline-stack.ts`, built only when `fleet:pipelineConnectionArn` and `fleet:pipelineRepo` are set): the post never asks for one, and gates inside a pipeline cannot be run before pushing |
| Benchmark — measurement harness and published figures | done; method and results in [`bench/README.md`](../bench/README.md) |

What is verified end to end, on AWS: a manifest is seeded by the deployed function, the fleet leaves zero
on its own, tasks start in subnets with no route to the internet, each claims a subscription, takes a
work item, runs a headless Kiro CLI workflow, confirms it produced what it was supposed to, stores the
result and its transcript under a deterministic key, acknowledges the queue, and returns the capacity —
and the fleet returns to zero. The measurements at scale, on one key and on a pool of five
subscriptions, are in [`bench/README.md`](../bench/README.md).

Running that loop found the first three bugs below, and a later run on AWS found the fourth. Nothing else
could have:

| Bug | Why no test caught it |
|---|---|
| A batch of ten items or fewer never started. The scaling target was 10, and at zero tasks the signal is the whole backlog, so target tracking saw a metric below its target and wanted fewer tasks than the none it had. The README's own three-file example could not run. | Every resource was exactly as specified. The defect was in a number's relationship to the smallest realistic batch, which no template assertion expresses. |
| A drained queue took about sixteen minutes to reach zero, churning task starts throughout: a worker found the queue empty, exited, and the service replaced it. Target tracking's scale-in alarm is created with fifteen evaluation periods, which is not ours to lower. | It needed a real batch to finish and then be watched. The behaviour is correct per resource and wrong as a system. |
| Lowering the target to 2 did not fix the first bug. It only moved it: a batch of one or two items still never started. Two items sat queued for twelve minutes with the service at zero, because the metric (2) was not above the target (2). Three items started, but about four minutes after seeding, most of it the scale-out alarm collecting datapoints. The scaling function now sets an idle fleet's desired count itself, the mirror of draining to zero. | The fix for the first bug was checked with a three-item batch, which is above the target. |
| The first batch after a quiet period was cut from 25 tasks to 2 about 27 seconds after the start. The scale-in alarm had been in ALARM since the fleet went idle, and the start bypassed Application Auto Scaling, so no cooldown applied. Some of the remaining items ran unprotected and were stopped mid-run. The start now suspends scale-in until that alarm clears. | Only a batch after more than fifteen idle minutes shows it. Back-to-back batches keep the alarm out of ALARM. |
| Transcripts were absent from a deployed run while working locally, because the pushed image predated the code that exports them. | Nothing compares a running image against the source it should contain. The symptom was a missing feature, not a failure. |
