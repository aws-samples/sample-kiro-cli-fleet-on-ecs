# Running Kiro CLI workflows remotely at scale on Amazon ECS

A reference implementation of a pattern for executing headless Kiro CLI workflows as a large batch on
Amazon ECS. One workflow run is one Amazon ECS task on AWS Fargate. Work arrives on a queue, the service
scales out to drain it and back to zero when it is empty, and each task draws a Kiro subscription from a
governed pool so that no subscription is pushed past its concurrency ceiling. Workers read each
subscription's cumulative credit figure from the Kiro CLI before every work item, and a subscription stops
being handed out, and stops being drained, when it reaches its budget less a safety margin
([Credits](#credits)).

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
  [Scale-in and the item in progress](#scale-in-and-the-item-in-progress).
- **VPC endpoints, no NAT gateway, by default.** Tasks have no internet route; the AWS APIs they call and
  the Kiro service (through the `q` endpoint) are reached over VPC endpoints. See [Security](#security)
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
| Credit tracking — `/usage` before every item, monotonic writes, mid-drain budget stop, optional daily activity report | done in code and tests. Readings, the claim-time cutoff, an exhausted pool starting nothing, and the stop partway through a batch all run on AWS ([what has been verified](#what-has-been-verified-on-aws)). The activity report has never been ingested from a file Kiro wrote ([what is not verified](#what-is-not-verified)) |
| Worker loop — completion check, remedies, local queue and result store | done; runs end to end against the real CLI |
| Worker runtime — image, entrypoint, prompts as code, steering as payload | done; cooperative stop and container/local equivalence both shown |
| Data, queue, network, compute, functions — CDK stacks | done; deployed, and a workflow ran to completion on Fargate |
| Observability — dashboard and alarms | done; one dashboard in three layers, alarms on the two conditions that need a person |
| Verified builds — one entry point, thin CI wrappers | done; both failure paths exercised. A CodePipeline is optional and off by default (`infra/lib/pipeline-stack.ts`, built only when `fleet:pipelineConnectionArn` and `fleet:pipelineRepo` are set): the post never asks for one, and gates inside a pipeline cannot be run before pushing |
| Benchmark — measurement harness and published figures | done; method and results in [`bench/README.md`](bench/README.md) |

What is verified end to end, on AWS: a manifest is seeded by the deployed function, the fleet leaves zero
on its own, tasks start in subnets with no route to the internet, each claims a subscription, takes a
work item, runs a headless Kiro CLI workflow, confirms it produced what it was supposed to, stores the
result and its transcript under a deterministic key, acknowledges the queue, and returns the capacity —
and the fleet returns to zero. The measurements at scale, on one key and on a pool of five
subscriptions, are in [`bench/README.md`](bench/README.md).

Running that loop found the first three bugs below, and a later run on AWS found the fourth. Nothing else
could have:

| Bug | Why no test caught it |
|---|---|
| A batch of ten items or fewer never started. The scaling target was 10, and at zero tasks the signal is the whole backlog, so target tracking saw a metric below its target and wanted fewer tasks than the none it had. The README's own three-file example could not run. | Every resource was exactly as specified. The defect was in a number's relationship to the smallest realistic batch, which no template assertion expresses. |
| A drained queue took about sixteen minutes to reach zero, churning task starts throughout: a worker found the queue empty, exited, and the service replaced it. Target tracking's scale-in alarm is created with fifteen evaluation periods, which is not ours to lower. | It needed a real batch to finish and then be watched. The behaviour is correct per resource and wrong as a system. |
| Lowering the target to 2 did not fix the first bug. It only moved it: a batch of one or two items still never started. Two items sat queued for twelve minutes with the service at zero, because the metric (2) was not above the target (2). Three items started, but about four minutes after seeding, most of it the scale-out alarm collecting datapoints. The scaling function now sets an idle fleet's desired count itself, the mirror of draining to zero. | The fix for the first bug was checked with a three-item batch, which is above the target. |
| The first batch after a quiet period was cut from 25 tasks to 2 about 27 seconds after the start. The scale-in alarm had been in ALARM since the fleet went idle, and the start bypassed Application Auto Scaling, so no cooldown applied. Some of the remaining items ran unprotected and were stopped mid-run. The start now suspends scale-in until that alarm clears. | Only a batch after more than fifteen idle minutes shows it. Back-to-back batches keep the alarm out of ALARM. |
| Transcripts were absent from a deployed run while working locally, because the pushed image predated the code that exports them. | Nothing compares a running image against the source it should contain. The symptom was a missing feature, not a failure. |

## Before you start

The pattern runs the Kiro CLI in headless mode, which authenticates from an API key rather than a browser
session — [setting `KIRO_API_KEY` is what makes a run headless][headless-blog]. Getting those keys has
preconditions that are easier to discover now than halfway through a deployment.

| You need | Notes |
|---|---|
| A paid Kiro subscription per pool member | API keys require a paid tier. The pool's whole purpose is to spread load across several, so a useful run needs more than one. |
| API key generation enabled for your organization | An administrator turns this on in the Kiro settings of the AWS console. Without it, the API Keys page will not issue a key. |
| One key per subscription, from [app.kiro.dev](https://app.kiro.dev) | The value is shown once, at creation. Keys begin `ksk_`. |
| An AWS account with CDK bootstrapped | Plus Node.js 20+, Python 3.13, [uv](https://docs.astral.sh/uv/), Docker, and the AWS CLI. |
| Fargate quota headroom for the fleet you want | The default allowance for a new account is **6 on-demand vCPUs**, counted in vCPUs rather than tasks. The shipped defaults fit inside it; see [Deploying it](#deploying-it). |

Tested with Kiro CLI 2.30.0 and Node.js 26.13.0 in the worker image (both pinned and checksum-verified in
`tools/build-worker-image.sh`), AWS CDK 2.274.0 (`aws-cdk-lib`) with CDK CLI 2.1146.1, Python 3.13, and
Node.js 22 in CI, deployed in `us-east-1` on `linux/arm64` tasks. Moving the Kiro CLI pin is worth re-checking: the
`/usage` reading and the `--trust-tools` names are what the worker depends on.

Two further constraints are worth knowing before you plan a region, though neither is verified here:

- The sample's egress allowlist (`fleet:kiroEgressDomains` in `infra/cdk.json`) names `us-east-1` Kiro
  endpoints. Deploying elsewhere means changing those names to match.
- The [`sample-kiro-flock`][flock] project reports that the Kiro portal validates an IAM Identity Center
  login only when the IDC instance is in the same region as the Kiro service, and that a mismatch fails
  without a clear error. If you authenticate through IDC, check this before assuming a region.

Read the [Kiro headless mode documentation][headless-docs] for the authoritative account of the flags and
the API key flow; the paragraph above is only what bears on deploying this sample.

[headless-blog]: https://kiro.dev/blog/introducing-headless-mode/
[headless-docs]: https://kiro.dev/docs/cli/headless/
[flock]: https://github.com/aws-samples/sample-kiro-flock

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

### Tests and the gates before a deployment

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

## Deploying it

These are the steps as run, in this order, against an account with nothing in it. The order matters in
two places and both are noted.

```bash
cd infra
npm ci
export CDK_DEFAULT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_DEFAULT_REGION=us-east-1

npx cdk bootstrap "aws://${CDK_DEFAULT_ACCOUNT}/${CDK_DEFAULT_REGION}"
npx cdk deploy --all --require-approval never
```

**If CDK reports `no credentials have been configured` while `aws sts get-caller-identity` works, sign in
again.** With IAM Identity Center profiles, the AWS CLI caches the role credentials it obtained and keeps
answering from that cache after the sign-in session has expired. CDK does not read that cache. Run
`aws sso login --profile <profile>` and retry.

**The first deployment happens before the image exists, and that is fine.** The repository is created by
`ComputeStack`, so there is nowhere to push to until it has been deployed once. A task definition naming a
tag that is not there yet is not an error: nothing validates the image until a task starts, and the
service's desired count is zero. This is the second reason that count is zero, the first being that a
deployment should prepare a fleet rather than run a batch nobody asked for.

Then build the image and push it to the repository the deployment created:

```bash
cd ..
./tools/build-worker-image.sh

REPO=$(aws cloudformation describe-stacks --stack-name KiroFleetCompute \
    --query "Stacks[0].Outputs[?OutputKey=='WorkerImageUri'].OutputValue" --output text)
aws ecr get-login-password | docker login --username AWS --password-stdin "${REPO%%/*}"
docker tag kiro-fleet-worker:latest "$REPO" && docker push "$REPO"
```

### Registering a subscription

A subscription is a key in Secrets Manager plus a row in the registry. The deployment creates one empty
secret under a prefix and the task role can read anything under it, so adding further subscriptions needs
no redeployment.

```bash
SECRET=$(aws cloudformation describe-stacks --stack-name KiroFleetData \
    --query "Stacks[0].Outputs[?OutputKey=='SubscriptionSecretArn'].OutputValue" --output text)
TABLE=$(aws cloudformation describe-stacks --stack-name KiroFleetData \
    --query "Stacks[0].Outputs[?OutputKey=='SubscriptionsTableName'].OutputValue" --output text)

# Typed at a prompt, so it stays out of shell history. Handed to the CLI as a file, so it
# stays out of the process list: an argument such as --secret-string "$KIRO_KEY" can be
# read by anyone who lists processes while the command runs. printf is a shell builtin,
# so writing the file starts no process that could see the value.
read -rs KIRO_KEY
KEY_FILE=$(umask 077; mktemp)
printf '%s' "$KIRO_KEY" > "$KEY_FILE"
unset KIRO_KEY
aws secretsmanager put-secret-value --secret-id "$SECRET" \
    --secret-string "file://$KEY_FILE" >/dev/null
rm -f "$KEY_FILE"

aws dynamodb put-item --table-name "$TABLE" --item "$(cat <<JSON
{"subscriptionId": {"S": "sub-01"},
 "state":          {"S": "ENABLED"},
 "secretRef":      {"S": "${SECRET}"},
 "maxConcurrency": {"N": "2"},
 "activeLeases":   {"N": "0"},
 "creditBudget":   {"N": "1000"},
 "creditsUsed":    {"N": "0"}}
JSON
)"
```

`maxConcurrency` is the operational cap you choose for that subscription, not a limit the service
publishes. Their sum, over the subscriptions that still have budget, is the fleet's real ceiling — the
scaling function holds the autoscaling maximum at it, because a task beyond that point starts, fails to
claim, exits, and is replaced while doing nothing.

### Credits

`creditsUsed` is the subscription's cumulative consumption in its current window, as the Kiro service
reports it. Four things write it:

- **Every worker**, before each work item (including the first, straight after it claims a subscription)
  and, best-effort, once more when its drain ends. It runs `kiro-cli chat --no-interactive "/usage"` with the
  subscription's key, reads `Credits (<used> of <limit> covered in plan)` and the `resets on <date>` header,
  and records the absolute figure. The write is conditional and only raises the stored value, so readings
  from many workers, in any order, end at the largest. A reading for a new window (a later `resets on`
  date) adopts that window and its figure; a reading whose window has already ended is dropped.
- **The daily activity report**, if you enable it (below). It raises `creditsUsed` to the report's
  month-to-date figure where that is higher, which catches usage no worker saw, such as the owner's IDE.
- **The reconciler**, which sets `creditsUsed` to zero when `windowResetsAt` passes and moves the window on
  by **one calendar month**. This changed: it used to step 30 days, which drifts from Kiro's monthly reset
  date. Set `CREDIT_WINDOW_DAYS` on the reconciler to keep a fixed-day step.
- **You**, through `leaseclient.credits.apply_external_correction` or the equivalent `update-item`. This
  replaces the value and can lower it; it is the only path that does.

A claim is refused while `creditsUsed` is at or above `creditBudget` minus `CREDIT_MARGIN`. A worker that
reaches that point before an item stops draining, releases its lease, and leaves the remaining items in the
queue, untouched, for a subscription with budget left. It emits `BudgetExhausted` and exits normally.

The scaling function applies the same test, with the same margin, before it starts anything. A
subscription at its ceiling adds nothing to the fleet's maximum, so when every enabled subscription is
there, the maximum is zero, an idle fleet is not started, and the items wait in the queue. Each minute
that happens with work waiting, the function logs a warning and emits `PoolExhausted`, which the
dashboard draws with the per-subscription credit figures. Raising a budget, or the window resetting,
lifts the maximum on the next minute and the fleet starts again. Before this, a task started about once a
minute, was refused for credits, and exited, for as long as the items stayed queued. Lowering the
maximum below the running count also stops tasks to fit. ECS chooses which tasks to stop without regard
to which subscription they hold, so a task on a subscription with budget left can be one of them.
Scale-in protection keeps in-flight runs safe: a protected task that holds an item finishes it before ECS
can stop the task. A task whose protection request ECS refuses runs that item unprotected. See
[Scale-in and the item in progress](#scale-in-and-the-item-in-progress).

The margin is set once, as `fleet:creditMargin` in `infra/cdk.json` (default `1`), and the stack passes
it as `CREDIT_MARGIN` to both the task definition and the scaling function.

A reading that fails (a timeout, the CLI missing, or output without a Credits line, which is what an
unreachable backend produces while still exiting 0) is never recorded as zero. It is logged, counted as
`UsageReading{Outcome=ReadFailed}`, and the item runs anyway against the last recorded figure. That is a
deliberate fail-open: a Kiro backend hiccup does not stop the fleet. If you want fail-closed, alarm on
`ReadFailed`.

**`CREDIT_MARGIN`** (default `1`, fractions allowed to 0.01) covers what a reading cannot see: one run in
flight per held lease, and runs that finished in the ~45 s before the figure moves. With `C` the largest
`maxConcurrency` in the pool and `P` the 95th percentile credits of one run, set it to `2 × C × P + 0.01`.
Read `P` from the `RunCredits` p95 on the dashboard after a batch; that figure is the `Credits:` summary line a
run prints, published for this and never added to `creditsUsed`. It covers the primary agent only, so add
headroom for workflows with subagents. The CLI does not always print it. Two identical runs on Kiro CLI 2.21.4
produced two output formats, and one of them had no credit figure on either stream; Kiro CLI 2.30.0, the
version the image pins, prints none in headless mode (0 of 600 measured runs), so on 2.30 `RunCredits` stays
empty and `P` has to come from the `/usage` deltas over a batch instead (credits used divided by runs).
The worker looks on stderr, then on stdout. When it finds nothing it logs a warning once per worker, with the
run's last stderr line, and emits `RunCreditsMissing` for every such run, which the dashboard draws beside
`RunCredits` so the p95 is read against the runs it leaves out. The metric carries the run's `Verdict` as a
dimension, as `RunOutcome`
does: a run that timed out, was throttled or crashed never prints a summary, so only `Verdict=Succeeded`
counts runs whose figure the format left out. The margin does not reserve anything for someone using the same
subscription interactively at the same time; their usage appears at the next reading.

Each reading costs one `/usage` call: 5–9 s on Fargate in the measurements behind this, bounded at 30 s
(`USAGE_TIMEOUT_SECONDS`). A drain of N items that ends on an empty queue makes up to N + 2 calls: one
before each item, one before the receive that finds the queue empty, and one at drain end. The last two
run one long-poll apart and usually show the same figure, so they add 5–9 s per task for little new
information. The sample keeps the drain-end reading, but it is best-effort, as the next paragraph explains.

**After a batch, the stored `creditsUsed` trails the true total.** The drain-end reading is best-effort,
and it may be skipped altogether. When the queue drains, the scaling function sets the service to zero on
its next minute, and that scale-in can stop the task before the reading runs. On AWS, a worker logged
`queue drained` at 14:10:06, the scaling function set the service to zero at 14:10:08, and SIGTERM arrived
at 14:10:11, before the drain-end reading. When the reading does run, it usually comes less than ~45 s
after the task's last run, so it does not include that run, and with several tasks it can miss the last
run of each. Either way, nothing reads `/usage` again until the next claim's pre-item reading or the daily
report, and until then the stored value can trail the server's figure by the credits of the batch's last
runs. `CREDIT_MARGIN` covers this gap. On AWS, the run above left 14.17 stored against a fresh `/usage` of
14.68, and an earlier three-item batch with two tasks left 13.42 against 13.74; the default margin of 1
covered both. No delay is added before the drain-end reading: waiting out the lag would cost about 60 s of
Fargate per task to close a gap the margin already covers. When you compare the stored value with a fresh
reading, allow a difference up to `CREDIT_MARGIN`.

To cut a subscription off by hand, write `creditsUsed` at or above the budget, or lower the budget:

```bash
aws dynamodb update-item --table-name "$TABLE" \
    --key '{"subscriptionId": {"S": "sub-01"}}' \
    --update-expression 'SET creditsUsed = :used' \
    --expression-attribute-values '{":used": {"N": "1000"}}'
```

The `sub-01` item registered above has no `windowResetsAt`. Its first successful reading sets it from the
`resets on` date.

#### The daily activity report (optional)

If your organization has enabled Kiro's per-user activity report to S3, a fourth function can read it each
day at 03:30 UTC and raise `creditsUsed` where the report shows more. Deploy with the bucket and the prefix
you entered in the Kiro console:

```bash
npx cdk deploy --all \
    -c fleet:activityReportBucket=<bucket> \
    -c fleet:activityReportPrefix=<prefix> \
    -c fleet:activityReportRegion=<Kiro profile Region>   # defaults to the stack's Region
```

The report is keyed by user, not by API key, so tell the pool whose key each subscription is. Take the
`UserId` from a report row whose `User_Email` is the subscription owner:

```bash
aws dynamodb update-item --table-name "$TABLE" \
    --key '{"subscriptionId": {"S": "sub-01"}}' \
    --update-expression 'SET reportUserId = :u' \
    --expression-attribute-values '{":u": {"S": "<UserId from the report>"}}'
```

The function sums `Credits_Used` across days and client types for the subscription's current window and
raises `creditsUsed` to that total if it is higher. It never lowers it, and it never writes into a window
other than the one it computed. `Overage_Credits_Used` is not counted, because `/usage` reports credits
covered in plan. Subscriptions without `reportUserId` or without a window are skipped. A bucket encrypted
with a customer managed KMS key also needs `kms:Decrypt` added to the function's role.

#### What is not verified

- **The activity report has never been ingested from a real file.** The tests use synthetic CSVs built
  from the documented columns and object layout. The `Date` format, whether `Date` is a UTC day, and
  whether month folders are zero-padded are assumptions; the parser accepts `YYYY-MM-DD`, `MM-DD-YYYY` and
  `YYYY/MM/DD`, and counts rows it cannot read. Check one real file before relying on it.
- **That the report's `UserId` corresponds to the API key's user, and that the report and `/usage` count
  the same credits.** If they differ, taking the larger errs towards stopping early.
- **That `resets on <date>` is a UTC-midnight boundary.** If the server rolls over at another hour,
  readings in the gap are dropped as stale, which is the safe direction.
#### What has been verified on AWS

- **Readings from the private subnet.** Tasks with no route to the internet read `/usage`, recorded it,
  and adopted the window from its reset date, with no failed reading or write. After a batch the stored
  value was within the margin of a fresh reading (0.32 and 0.51 behind, with a margin of 1), lower because
  the last runs had not yet reached `/usage`.
- **The claim-time cutoff.** With the budget set just below the recorded figure plus the margin, tasks
  started, their claims were refused with `BudgetExhausted` (15 attempts), no lease was granted, and the
  queued items stayed in the queue.
- **An exhausted pool starts nothing.** With every subscription past its ceiling and two items queued, the
  service stayed at zero tasks for five minutes and the scaling function reported `PoolExhausted`. Raising
  the budget started a task within a minute, which drained both items.
- **The stop partway through a batch.** A pre-item reading crossed the ceiling during a six-item batch.
  That task stopped taking items, the scaling function lowered the maximum, and the run in flight on the
  other task finished under scale-in protection. The remaining items waited in the queue and drained once
  the budget was raised.

### Scale-in and the item in progress

Several things stop tasks while a batch runs: target tracking scaling in, the scaling function lowering
the maximum when a budget runs out, the drain to zero, and a deployment. ECS chooses which tasks to stop
without regard to which subscription they hold or whether they are in the middle of a run. A run stopped
part-way has already spent its credits, and its message is redelivered after the visibility timeout, so
the item is paid for twice.

So each worker uses
[task scale-in protection](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection.html)
for the length of one item. For every item, in this order:

1. The `/usage` reading and the budget check, unchanged. A worker at its ceiling stops here, unprotected.
2. Protection on: `PUT $ECS_AGENT_URI/task-protection/v1/state` with
   `{"ProtectionEnabled": true, "ExpiresInMinutes": 20}`.
3. Receive.
4. Run, then store the result and delete the message, or hand the message back.
5. Protection off, `{"ProtectionEnabled": false}`, however steps 3 and 4 ended: an empty receive, a
   failed or timed-out run, an exception, or SIGTERM.

The expiry is `ceil(workflow timeout / 60) + 5` minutes, which is 20 minutes for the 900 s timeout. The
item itself is held differently: the queue's visibility timeout is a 3-minute window that the worker
renews every minute while the run is in progress (`runner/visibility.py`), up to 1.5 times the workflow
timeout. A worker that hangs while protected therefore lets its item go after at most about 22.5 minutes,
and becomes eligible for scale-in when its protection expires.

What ECS does with protected tasks:

- **Target tracking and a lower maximum.** Application Auto Scaling lowers the desired count, and ECS
  stops only unprotected tasks, which are the ones between items. Every task that holds an item finishes
  and acknowledges it. A task on an exhausted subscription then reaches its budget stop before it
  protects itself again, and exits. Other tasks are stopped in their gap between items, or keep working
  until the count fits. A task stopped in its gap holds no item, and costs only a later replacement task
  start. If ECS refuses a task's next request with `DEPLOYMENT_BLOCKED`, which the docs say means the
  service has more protected tasks than its desired count (the state a lower maximum creates), that item
  runs unprotected and can be stopped mid-run, as every item could before this change.
  `TaskProtectionFailed` with `Reason=DeploymentBlocked` counts it.
- **Drain to zero.** The scaling function sets the service to zero only when nothing is visible and
  nothing is in flight, so no task holds an item. A task can be protected during its last, empty long
  poll (up to 20 s); ECS waits for that, and the worker exits on the empty queue anyway.
- **Deployments.** ECS does not stop a protected task, so a rolling deployment waits for each task's
  current item (a run is cut off at 900 s) or for its protection to expire. A worker protects itself
  again before each item, so ECS can only catch it in the gap between items: from turning protection
  off, through the next `/usage` reading (5-9 s), until it turns protection on again. How quickly ECS
  acts on a gap that short is not documented. A deployment in the middle of a long batch can therefore
  wait until tasks exit on an empty queue or at their budget stop. CloudFormation fails a stack update
  and rolls it back after 3 hours, so deploy between batches. If ECS answers a worker's request with
  `DEPLOYMENT_BLOCKED` (the service has more protected tasks than its desired count), that item runs
  unprotected, as every item did before this change. If ECS answers `TASK_STOPPING_OR_STOPPED`, the
  task is already being stopped: the worker takes no further item and ends its drain, so nothing is
  received only to be killed mid-run (measured on the M2 benchmark before this rule: 12 items lost
  mid-run in one level, each waiting out the visibility timeout; `bench/README.md`, M4).
- **SIGTERM.** The worker's handler is unchanged: it stops the heartbeat, returns the lease, and exits
  with 143. With protection, scale-in and deployments send it between items, so the task exits holding no
  item. `StopTask`, Fargate task retirement and crashes are not covered by protection and can still stop
  a task mid-item. Protection is turned off on the way out where the process still can, the lease is
  returned, the CLI is killed, and the item is handed back at once, so another task picks it up
  immediately; the credits the interrupted run spent are lost. If the hand-back cannot be made (the
  process is killed outright), the item reappears once its visibility window lapses: within 3 minutes,
  because the
  dead worker no longer renews it (measured before this window, with a fixed 22.5-minute timeout: crash
  recovery took 1,618 s against about 410 s for a clean batch; `bench/README.md`, M4).

Protection fails open. Without `ECS_AGENT_URI` (`runner.local_batch`, or the container under plain
Docker) nothing is called. A request that fails, times out after 6 s, or is refused logs one warning and
emits `TaskProtectionFailed` in the pool namespace, with `Action` (`Enable` or `Disable`) and `Reason`
(`Unreachable`, `HttpError`, `Refused`, `DeploymentBlocked`, `TaskStopping` or `BadResponse`) as
properties, and the item runs unprotected, except for `TaskStopping`, where the worker stops instead. The agent's
[endpoint](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection-endpoint.html)
reports a refusal from ECS with status 200, so the worker counts a request as done only when the answer
confirms the state it asked for. Alarm on `TaskProtectionFailed` above zero if you want to know when a
run was exposed.

The task role is granted `ecs:UpdateTaskProtection` on `task/<cluster>/*`, which is the call the agent
makes for the worker. `ecs:GetTaskProtection`, which the docs also list, is only for reading the state
back, and the worker never does that.

Verified on AWS with the default `private` egress mode (no internet gateway, NAT or firewall):

- **The agent's call reaches ECS from the task subnets** without an `ecs` interface endpoint. Every
  sample taken during a run read `protectionEnabled: true`, and no `TaskProtectionFailed` was emitted.
- **`ecs:UpdateTaskProtection` alone is enough.** The docs also list `ecs:GetTaskProtection` for the task
  role, but the agent's `PUT` handler calls only the update, and no request was denied.
- **The maximum dropping while a run is in flight.** A pre-item reading crossed the ceiling, the scaler
  lowered the maximum to 0, and ECS reported "unable to scale in due to (reason 1 tasks under protection)".
  The run in flight finished and stored its result, protection went off, and SIGTERM arrived about 5 s
  later, between items. This rests on one run that ended about 14 s after the maximum dropped.

Not verified on AWS:

- **Whether ECS returns `DEPLOYMENT_BLOCKED` while autoscaling lowers the count**, and not only during a
  deployment. It did not occur in the check above.
- **The `inspection` and `open` egress modes.** The call goes to the ECS agent and then to ECS, so it is
  not expected to depend on the mode. If a warning names `RequestCanceled` or `TaskProtectionFailed` shows
  `Reason=Unreachable`, add `ecs` to the interface endpoints in `infra/lib/network-stack.ts`, at about
  $14.60 a month for two zones.

### Seeding a batch

Put the inputs in the reference bucket and write a manifest beside them. The seeding function expands a
prefix into one work item per object, which is the shape fan-out work usually has:

```bash
REF=$(aws cloudformation describe-stacks --stack-name KiroFleetData \
    --query "Stacks[0].Outputs[?OutputKey=='ReferenceBucketName'].OutputValue" --output text)

aws s3 cp ./my-inputs "s3://${REF}/inputs/" --recursive
cat > manifest.json <<'JSON'
{ "inputPrefix": "inputs/",
  "prompt": "Read every file under ./input. For each one, write ./output/<the file name>.md ..." }
JSON
aws s3 cp manifest.json "s3://${REF}/manifest.json"

aws lambda invoke --function-name KiroFleetFunctions-seeding \
    --cli-binary-format raw-in-base64-out --payload '{}' /dev/stdout
```

Tasks start by themselves from there. Nothing needs to be told to start. The scaling function runs every
minute; when it finds visible messages and no task desired, running or pending, it sets the service to
`ceil(visible / targetBacklogPerTask)` tasks, at least one and at most the fleet's ceiling (`maxTasks`, or
the pool's summed `maxConcurrency` if lower). It logs the start and publishes `StartedFromZero` in the
`KiroFleet/Fleet` namespace. From there target tracking follows backlog per task as the batch runs, and
the same function sets the service back to zero when the queue, including in-flight messages, is empty.

Target tracking cannot do the first step by itself. At zero tasks its metric is the whole backlog, and it
scales out only when the metric is above `targetBacklogPerTask`, so a batch of one or two items (with the
default of 2) would never start. Larger batches would start only after the scale-out alarm has collected
its datapoints.

The start also suspends target tracking's scale-in. While the fleet idles the metric is 0, so the scale-in
alarm (fifteen datapoints below the target) sits in ALARM, and an alarm in ALARM invokes its policy every
minute. The start sets the desired count outside Application Auto Scaling, so no scale-in cooldown covers
it, and before this was added the stale alarm cut a just-started fleet within the minute: 25 tasks to 2,
measured after a quiet hour. The function sets `DynamicScalingInSuspended` on the scalable target in the
same run, before the desired count moves, and lifts it once the alarm has left ALARM (normally the next
minute) or the queue has drained. Scale-out is never suspended.

> **This has been run.** All six stacks, the image push, a subscription registered, the manifest seeded
> through the deployed function, the fleet leaving zero on its own, the batch completing, and the fleet
> returning to zero. Measured on a three-item batch:
>
> | | |
> |---|---|
> | seeded to fleet leaving zero | 149s with target tracking alone. Since the scaling function starts an idle fleet itself, a one-item batch reached a desired count above zero in 54s and a running task in 88s |
> | leaving zero to queue drained | 95s — including the image pull on a cold host |
> | drained to back at zero | 23s |
>
> The first number is the cold-start cost of scaling from nothing, and it is the price of not paying for
> an idle fleet. Three bugs were found by running this and none of them were visible in the templates or
> the tests; they are described in [Roadmap](#roadmap).

### Watching it

Per-task processor and memory come from Container Insights, which is on by default precisely because the
task size is not tuned against a measurement. The worker emits its own metrics as embedded metric format
on its log stream, so the pool's behaviour — claim outcomes, utilisation, recovered leases — is in
CloudWatch without the workload holding any CloudWatch permission.

```bash
aws logs tail /aws/ecs/KiroFleetCompute/worker --follow
```

A single dashboard puts these in the post's three layers — `KiroFleetObservability-fleet`. Progress is
the queue draining against the fleet's size; outcomes are run verdicts and claim outcomes, verdict by
verdict, because a success count cannot tell a prompt problem from a pool problem; the pool panel shows
utilisation against the ceiling the subscriptions declare. Per-run transcripts are objects rather than a
time series, so the dashboard points at their prefix.

### Sizing

The sample is designed to be runnable with a handful of subscriptions and a low task concurrency cap.
Larger figures are parameters, not defaults, and the binding one is a quota rather than a preference.

A new account is allowed **6 on-demand Fargate vCPUs**, and the quota counts vCPUs rather than tasks, so
the fleet ceiling and the task size are one decision:

```bash
aws service-quotas get-aws-default-service-quota \
    --service-code fargate --quota-code L-3032A538      # 6, for a new account
aws service-quotas get-service-quota \
    --service-code fargate --quota-code L-3032A538      # what yours is now
```

Check the second command before raising `fleet:maxTasks`. The two readings diverge over time, because
increases accumulate and are never given back — which means an account that has been used for a while
reports no constraint at all, and is a misleading place to decide a default from. The shipped defaults
(`fleet:maxTasks: 10` at `fleet:taskCpu: 512`) come to 5 vCPUs and fit a new account; synthesizing a
larger fleet prints a warning naming the quota to raise.

## Removing it

```bash
cd infra
npx cdk destroy --all --force
```

That removes the compute, the network, the queue, and the functions. If you deployed the benchmark
stack (`-c fleet:benchWorkerImage=...`), `--all` does not see it without that context; remove it with
`npx cdk destroy KiroFleetBench -c fleet:benchWorkerImage=<the same tag> --force`.

Four things survive it, three of them on purpose.

| Survives | Why | To remove |
|---|---|---|
| Subscriptions table | The only record of which subscriptions exist, their limits, and their recorded `creditsUsed`. Losing it costs the pool, not a batch; the next readings rebuild `creditsUsed`. | `aws dynamodb delete-table --table-name <name>` |
| Reference bucket | Your inputs. | `aws s3 rb s3://<name> --force` |
| Results bucket | The output of runs that may have taken hours. | See below; versioning gets in the way. |
| Subscription secret | Holds a live key, so it goes to a recovery window rather than away. | `--force-delete-without-recovery` |

The asymmetry is deliberate: scratch is destroyed on teardown, and anything whose loss cannot be undone by
running the batch again is retained. The leases table is *not* retained — losing it costs a lease lifetime
of capacity, which reconciliation recovers.

### Three things that do not remove cleanly

Found by doing it. Each wastes an afternoon if you meet it without warning.

**The results bucket is versioned**, which is what makes a redelivered work item safe to overwrite. It also
means `s3 rb --force` leaves it non-empty and the delete fails, because `--force` removes objects and not
their versions:

```bash
aws s3api list-object-versions --bucket "$BUCKET" \
    --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' > versions.json
aws s3api delete-objects --bucket "$BUCKET" --delete file://versions.json
# Repeat for DeleteMarkers[], then:
aws s3 rb "s3://$BUCKET"
```

**The secret stays for its recovery window.** A plain delete schedules removal seven days out, and the key
is readable for all of them. If the key is live, revoke it at the source as well — deleting the secret does
not invalidate the credential it held:

```bash
aws secretsmanager delete-secret --secret-id "$SECRET" --force-delete-without-recovery
```

**With `fleet:egressControl: 'inspection'`, the firewall refuses to be deleted.** Delete protection is on
by design — a deployment that can remove what constrains egress is one mistake from an open fleet — and the
consequence is that teardown needs it turned off first. The default mode has no firewall and no NAT
gateway, so this does not arise:

```bash
aws network-firewall update-firewall-delete-protection \
    --firewall-name <stack>-egress --no-delete-protection
```

### Check nothing is still billing

Standing charges are the ones a teardown is for, and all three are easy to leave behind:

```bash
aws ec2 describe-nat-gateways --query 'length(NatGateways[?State!=`deleted`])'
aws ec2 describe-vpc-endpoints --query 'length(VpcEndpoints[?VpcEndpointType==`Interface`])'
aws network-firewall list-firewalls --query 'length(Firewalls)'
```

## Security

Key material is never baked into an image or committed to this repository. Tasks load subscription keys
at runtime from AWS Secrets Manager using the task role, after the allocator has decided which
subscription the task holds — nothing at task start knows that yet, which is why the key is not injected
as a container secret. The task execution role and the task role are separate: the execution role can
pull the image and open a log stream and cannot read a subscription key; the task role can take work from
the queue and store results and cannot create work or delete either. Its one ECS permission is
`ecs:UpdateTaskProtection` on the cluster's own tasks, for scale-in protection.

**Do not trust the shell tool for a workflow that reads untrusted input.** The agent (`kiro-cli`) runs in
the same container and as the same user as the worker, so any tool it is trusted with acts with the
task role. A trusted `shell` tool (`execute_bash` in older CLI versions) can therefore call AWS with
that role: read every subscription key in the pool, because the role's Secrets Manager grant covers the
pool prefix (a task learns which subscription it holds only after it claims one), and write any key in
the results bucket. Input content can steer the agent (prompt injection), so treat every file a manifest
lists as untrusted unless you produced it yourself. The example workflows trust only read and write for
this reason, and writes outside `./output` fail the run. If a workflow genuinely needs a shell, run it
only on inputs you control, or move key retrieval out of the task role before you do.

Egress has three modes, set with `fleet:egressControl`. In every mode nothing reaches a task from
outside.

- **`private` (default).** The tasks' subnets have no route to the internet at all: no internet gateway,
  no NAT gateway, no firewall. Every destination the workload needs has a VPC endpoint. The AWS services
  it calls (ECR, SQS, Secrets Manager, CloudWatch Logs, STS) use interface endpoints, S3 and DynamoDB use
  gateway endpoints, and the Kiro service is reached through the `q` interface endpoint, which provides
  private DNS for `runtime.<region>.kiro.dev` (inference), `management.<region>.kiro.dev` and
  `telemetry.<region>.kiro.dev`. A task cannot reach any other host, which is a stronger control than an
  allowlist and costs nothing extra. The measurements in `bench/README.md` ran in this mode.
- **`inspection`.** For a workflow that must reach the internet. Tasks are routed through a NAT gateway
  and an AWS Network Firewall that allows only the domains in `fleet:kiroEgressDomains`. The firewall
  has delete protection on, which affects teardown (see [Removing it](#removing-it)).
- **`open`.** A NAT gateway with the security group restricted to port 443 and no destination control.
  Synthesis warns, because a task can reach any host on 443. That is a real reduction in the security
  property, not a tidying-up.

The endpoint list and the allowlist are measured rather than assumed. A capture of a credentialed run,
reading hostnames and byte counts only, corrected three things the design had wrong: inference goes to
`runtime.<region>.kiro.dev`, which the `q` endpoint covers, rather than to `codewhisperer`, which was
never contacted and whose endpoint is gone; and the MCP server was being downloaded from npm on every
run, which `private` and `inspection` would both block, so the image now installs it. Ongoing traffic is
about 0.39 MB per run, so the endpoints are justified by the image pull (0.69 GB per host) and by keeping
credentials and results off the public internet, not by inference volume.

### What it costs while idle

The fleet scales to zero. The network around it does not, and that distinction is easy to miss:

| | rate (us-east-1) | `private` | `inspection` | `open` |
|---|---|---|---|---|
| Interface endpoints, 7 x 2 AZs | $0.01/hour each per AZ | ~$102 | ~$102 | ~$102 |
| Gateway endpoints (S3, DynamoDB) | free | $0 | $0 | $0 |
| NAT gateway | $0.045/hour | none | ~$33 | ~$33 |
| Firewall endpoint | $0.395/hour | none | ~$288 | none |
| **Monthly, idle** | | **~$102** (about $3.4 a day) | **~$423** (about $14 a day) | **~$135** |

Rates from the AWS Pricing API; check them for your region. Data processing charges come on top and
are small at 0.39 MB per run. In `inspection` the firewall dominates, and it is one endpoint rather than
one per zone for the same reason there is one NAT gateway: a path carrying 0.39 MB per run does not need
zonal independence.

### Before adapting this for production or shared use

This is a sample. The security model assumes one operator running their own batches in an account they
own. A non-exhaustive list of what to address first:

- **Cost controls.** Set AWS Budgets alarms. Kiro subscription credits are billed separately from AWS
  charges, so watching only AWS cost leaves half the picture, and a runaway batch spends both.
- **Subscription key rotation.** Keys are long-lived here and rotation is manual. Decide a rotation
  period and who is notified when a key is disabled.
- **IAM review against your own requirements.** The roles are scoped to named resources, but "what this
  workload needs" is a judgement about your workflows, not ours. A workflow that calls further AWS APIs
  needs grants this sample does not make.
- **Egress allowlist maintenance.** The allowed domains are a point-in-time list. A Kiro endpoint added
  later is a silent failure, so treat the list as something that expires.
- **Workflow prompt review.** A workflow is a prompt plus a tool allowlist, and both decide what an
  unattended agent can do. Review changes to them as you would review code.
- **Result integrity.** Results are versioned but not signed. If a downstream system trusts them, decide
  how it establishes which run produced what.
- **Observability retention and alarm routing.** Log retention is configured; where alarms go is not.
- **Dependency scanning in the build.** The build pins versions and verifies a checksum, but nothing
  audits them for known vulnerabilities on a schedule.

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for reporting a security issue.

## License

This sample is licensed under the MIT-0 License. See [LICENSE](LICENSE).
