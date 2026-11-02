# bench -- reproduction benchmarks (M1~M5)

Measures **per-subscription concurrency (M1)**: how many concurrent agent runs one
Kiro subscription key carries before extra runs only add waiting. M1 is the only one
of the measurement plan's M1~M5 that needs a single key and no five-subscription
pool, which is why it lives on its own.

## What M1 measures

The first harness assumed the limit was a count of open streaming connections with a
429 onset. Measurement did not bear that out: one key held close to 100 concurrent
single-turn calls, and up to 60 concurrent agent runs, without that onset. What one
key does have is a throughput ceiling. Past it the service mostly slows requests down
instead of rejecting them, so run latency grows with concurrency, then timeouts and
occasional throttles follow.

M1 therefore ramps concurrent agent runs on one key, one run per Fargate task
(`fleetbench/fleet.py`, stack `KiroFleetBench`), and records per level the median run
latency, the throughput while runs overlap, and throttle and timeout counts. Each
level is repeated in a different order per round so slow drift in the service does
not land on one level.

Two numbers summarise a ramp. **r1** is tool calls per minute for one run alone, and
**T** is the tool calls per minute the key sustains once it saturates. The knee sits
near T / r1, and past it N concurrent runs of W tool calls each take about N x W / T
minutes.

## Results (kiro-cli 2.30.0, one key, orchestrator + 2 subagents)

Throughput per round = completed runs x median tool calls per run / median run
latency (tool calls per minute while runs overlap). Runs the CLI recovered from a
throttle count as completed. Values are the mean over rounds, with the range in
brackets.

### M1g: 120-word notes, about 37 tool calls per run, 3 rounds

| Concurrent runs | Median run latency | Tool calls/min | Runs with a throttle (not recovered) |
|---:|---:|---:|---:|
| 1 | 66 s (61-76) | 34 | 0 |
| 3 | 61 s (60-63) | 108 | 0 |
| 5 | 74 s (64-84) | 153 | 0 |
| 6 | 84 s (67-113) | 167 | 0 |
| 8 | 97 s (86-108) | 184 | 0 |
| 12 | 178 s (148-196) | 152 | 0 |
| 20 | 332 s (324-341) | 134 | 0 |
| 30 | 466 s (434-498) | 144 | 2 of 84 (0) |
| 40 | 572 s (562-578) | 155 | 9 of 120 (0) |

- One round at 30 and one at 20 lost 6 and 3 tasks to container image pull failures
  before any request was sent. Those rounds are left out of the latency and
  throughput means; their throttle counts are kept.

### M1h: 5,000-word notes read in full, about 40x the input tokens, about 38 tool calls per run, 2 rounds

| Concurrent runs | Median run latency | Tool calls/min | Runs with a throttle (not recovered) |
|---:|---:|---:|---:|
| 1 | 77 s | 29 | 0 |
| 3 | 76 s (75-78) | 89 | 0 |
| 5 | 94 s (86-102) | 122 | 0 |
| 8 | 147 s (145-149) | 123 | 0 |
| 12 | 222 s (219-226) | 120 | 0 |
| 20 | 383 s (376-389) | 118 | 0 |
| 28 | 536 s (523-549) | 114 | 2 of 56 (1) |
| 40 | 702 s (679-726) | 125 | 28 of 80 (3) |

- The 28 row is the 30 level: both rounds lost 2 tasks to image pull failures.
- At 40, the round with 3 failed runs measured 679 s and the round with none 726 s.
- One single-run round at 1 took 391 s with a normal run (no throttle, 39 tool
  calls). One run cannot saturate the key, so it is left out as service-side variance.

### The two workloads side by side

| | M1g | M1h |
|---|---:|---:|
| r1 (tool calls/min, one run alone) | 34 | 29 |
| T (tool calls/min once saturated) | about 135-185 | about 114-125 |
| Predicted knee, T / r1 | about 4.5-5.5 | about 4.2 |
| Measured knee | 5-6 | 4-5 |
| Predicted vs measured latency, N x W / T | 178 / 592 s vs 178 / 572 s at 12 / 40 | 374 / 748 s vs 383 / 702 s at 20 / 40 |
| Runs with a throttle at 30 / 40 | 2 of 84 / 9 of 120, all recovered | 2 of 56 / 28 of 80, 4 not recovered |

Earlier single-round runs (2.30.0) also saw two throttles the CLI did not recover
from (40 concurrent runs; 20 with large inputs) and eight 900-second timeouts at 60
concurrent runs. kiro-cli 2.21.4 and an as-is-shaped workload (four agents, mixed
models, Python tools, six phases) showed no throttling up to 15 concurrent runs.

### Reproducing a ramp

```
uv run --with boto3 python -m fleetbench.fleet launch --workload fanout --subagents 2 \
  --files-per-subagent 15 --note-words 5000 --continue-after-onset --lead 240 \
  --timeout 1800 --levels "1 3 5 8 12 20 30 40" --out results/<name>
```

Drop `--note-words` for the short-input workload. `results/` is git-ignored; the
figures above come from each run's `records*.jsonl` aggregated per level.

## Local harness (single-turn streams)

The original harness below opens N single-turn streams released together from a
barrier on the local machine. It is kept for wiring checks; local runs are bounded by
the machine and the network, not the key, so they do not measure M1.

## Layout

- `fleetbench/ramp.py` -- the pure ramp driver and aggregation. No network, no
  worker dependency; takes the stream call as an argument so it is fully testable.
- `fleetbench/profiles.py` -- the heavy and light stream shapes.
- `fleetbench/report.py` -- writes the JSON series and the markdown curve for blog section 5.
- `fleetbench/adapter.py` -- the real stream call (Kiro CLI via the worker's
  `run_workflow`) and a simulated one for dry runs.
- `fleetbench/__main__.py` -- the CLI.

## Running

Wiring check -- no network, no credential, no credits:

```
uv run --frozen python -m fleetbench --dry-run
```

Real measurement against **one** subscription. The key comes from the environment,
never an argument (an argument lands in shell history and `ps` output):

```
KIRO_API_KEY=... uv run python -m fleetbench --max-concurrency 20
```

The real run needs the worker package importable in the same environment and an
authenticated `kiro-cli` on PATH:

```
uv pip install -e ../worker
```

Output lands in `results/` (git-ignored): `m1-series.json` (re-plottable) and
`m1-curve.md` (the section 5 table). The CLI prints which mode it ran in, because a
simulated curve must never be mistaken for a measured one.

## Tests

```
uv run --frozen pytest
```

The tests drive the ramp with fakes of known limit, so they assert where the harness
*decides* onset is -- its own job -- without touching Kiro.

## M2~M5 (the deployed fleet, five subscriptions)

Measured 2026-10-20/21 on the full pattern: manifest, seeding function, queue, worker,
registry claim. kiro-cli 2.30.0 image (`tools/build-worker-image.sh --cli-version 2.30.0`),
workflow `workflows/bench/fanout` (the M1g item: orchestrator + 2 subagents, 30 notes of
120 words), `maxConcurrency` 5 per key (the M1 knee), `fleet:maxTasks` 25, 0.5 vCPU / 2 GB
arm64 tasks. Driver `fleetbench/fleetload.py`, runner `m2m5.sh` (`m2m5-resume.sh` after an
interruption). A fleet sample (queue depth, desired/running tasks, leases, `creditsUsed`)
is taken every 15 s.

### M2: throughput against pool size

Each level holds 15 items per key (5 concurrent x 3 waves), so a linear pool drains in
the same time at every level. Two rounds, opposite order.

| Keys | Round 1 items/hour | Round 2 items/hour | Round 2 drain | Round 2 items/hour per key |
|---:|---:|---:|---:|---:|
| 1 | 175 | 157 | 344 s | 157 |
| 2 | 318 | 300 | 360 s | 150 |
| 3 | ~445 (reconstructed) | 412 | 393 s | 137 |
| 5 | 162 (distorted, see M4) | 659 | 410 s | 132 |

- Throughput grows close to linearly: 5 keys gave 4.2x one key. Per key it falls to about
  84% at 5 keys, because the drain time rose from 344 s to 410 s with more tasks. Task
  start-up spreading out is the likely cause; not isolated.
- Round 1, 3 keys: the launcher was stopped mid-batch; all 45 items finished, and drain is
  taken from the last result object in the results bucket.
- Round 1, 5 keys: 12 items sat 22.5 minutes in flight (the scale-in defect under M4), so
  its rate measures that defect, not the pool.

### M3: cost

| | |
|---|---|
| Kiro credits | about 1,085 for about 480 runs (retries included): **about 2.25 credits per run**, from each key's `/usage` month-to-date readings |
| Fargate | about 66,800 task-seconds (sampled running count x interval), about 18.6 task-hours, about $0.43 at arm64 list price; includes tasks that started, found no message and exited |
| Fixed | the network stack, whatever the load. In the default `private` egress mode that deployed here it is 7 interface endpoints in 2 AZs, about $0.14 an hour (about $3.4 a day, $100 a month); the `inspection` mode adds a firewall endpoint and a NAT gateway, about $14 a day in total |

Per item, Kiro credits dominate. kiro-cli 2.30.0 prints no `Credits:` line on exit, so the
worker logs "printed no Credits figure"; the `/usage` readings are the source here.

### M4: reliability under crash injection

5 keys, 50 items, 5 running tasks stopped with `ecs stop-task` 180 s after seeding.

- 50 of 50 items produced `final.md`. 55 runs: the 5 items the stopped tasks held ran
  again once each. Results are keyed by item id, so there are no duplicate outputs.
- After the run: 0 lease rows, `activeLeases` 0 on every key, dead-letter queue empty.
- Drain took 1,618 s against about 410 s undisturbed; almost all of it is the queue's
  visibility timeout (1,350 s = 1.5 x the 900 s workflow timeout) before the held messages
  reappear. Recovery is complete, and the visibility timeout sets how long it takes.

**Defect found (not injected).** In round 1 at 5 keys, ECS was scaling in while tasks were
still receiving. 18 times a worker's scale-in protection request was refused with
`TASK_STOPPING_OR_STOPPED`, the worker logged "running the item unprotected", took a
message, and was stopped mid-run; 12 items then waited out the visibility timeout. A
refusal for that reason means the task is already going away, so the worker should not
receive at all. Fixed after this run: the protection client records the refusal as
`TaskStopping` and the worker ends its drain before receiving (`runner/protection.py`,
`runner/worker.py`). Verified on AWS (batch `fix2-k5`, 5 keys, 75 items, desired count
forced down to 10 at 200/290/380 s): 2 refusals with `TASK_STOPPING_OR_STOPPED`, both
tasks ended without receiving, 0 items run unprotected, 75/75 results, and no message sat
in flight with an empty queue for longer than one 16 s sample; drain 379 s, 712 items/hour.

Retry backoff. During M2~M5 an item that failed with a throttle or a timeout went back to the
queue after a fixed 30 s. It now waits `retry_delay`: the base (`RETRY_DELAY_SECONDS`, 30 s)
doubled for each delivery SQS reports in `ApproximateReceiveCount`, capped at 900 s, with
jitter over the upper half. Checked on AWS with the rebuilt image (`results/m2m5/backoff_check.py`,
the worker's own `SqsQueue` against a temporary queue, 4 s base, jitter fixed at 1.0 so each
delay is the full step rather than a random point in its upper half): the message stayed
invisible for 4.0, 8.0, 16.0 and 32.0 s on deliveries 1 to 4, matching the computed delays.
Not exercised through a real throttle in the fleet: no batch produces one on demand.

**Re-measured with the renewed visibility window (batch `m4v`, 2026-10-22).** Same setup
(5 keys, 50 items, stop 20% of running tasks at 180 s), with the queue's visibility timeout
at 180 s and the worker renewing it every 60 s while a run is in progress
(`runner/visibility.py`). The batch did not run as planned: about 100 s after the fleet
reached 25 tasks, target tracking lowered desired to 2 (its low alarm, which had been in
ALARM through the idle period before the batch, acted first; the scaling function's own
signal read 11 items per task). So at 180 s only 5 tasks were left and the chaos step
stopped 1. The scale-in itself produced the interruption M4 is about: protection requests
were refused because more tasks were protected than desired (13 items ran unprotected),
and those tasks were stopped mid-run.

| | m4 (fixed 1,350 s) | m4v (180 s window, renewed) | m4v2 (same, as planned) |
|---|---:|---:|---:|
| Results | 50/50 | 50/50 | 50/50 |
| Runs | 55 | 59 (9 items interrupted, each re-run once) | 55 (5 stopped tasks' items, each re-run once) |
| Interrupted item: first start to re-run | about 1,350 s or more | 220 to 250 s | 179 to 180 s |
| Drain | 1,618 s | 596 s (with the fleet at 2 to 12 tasks after the scale-in) | 432 s |
| Dead-letter queue / lease rows after | 0 / 0 | 0 / 0 | 0 / 0 |
| Credits (5 keys) | | 116.8 | 124.1 |

- m4v2 ran as M4 was meant to: desired stayed at 25 (target tracking's low alarm was OK
  at the start), and the chaos step stopped 5 of 25 running tasks at 180 s. Each of their
  items started again 180 s after its first start, which is one window: the stopped worker
  had renewed at receive and then not again. Drain was 432 s against about 410 s for an
  undisturbed 5-key batch and 1,618 s for m4, so crash recovery now costs about one window.
- In both runs: "failed to extend" 0, "no longer extending" 0; tasks refused protection with
  `TASK_STOPPING_OR_STOPPED` ended their drain without receiving (3 in m4v, 2 in m4v2), and
  m4v2 ran 0 items unprotected.
- The re-run gap is the 180 s window after the item's last renewal plus time in the queue
  while only 2 tasks were running. Under the fixed timeout the same 9 items would have
  waited at least 1,350 s from their first receive.
- Drain is not comparable to m4 as a number: the fleet ran most of the batch at 2 tasks.

Target tracking's low alarm scaled a just-started fleet in, because it was already in ALARM
before the batch: after fifteen idle minutes the metric has read 0 throughout. The scaling
function starts the fleet from zero directly, outside Application Auto Scaling, so no
scale-in cooldown applied, and the alarm, still invoking the policy every minute, cut the
count within the minute (m4v: 25 -> 2 at +27 s; M2 r1 k5: 25 -> 3). Back-to-back batches
never let the alarm reach ALARM, which is why m4v2 and the later M2 levels were unaffected.
(The scale-in to 10 during M2 r1 k5 is different: twelve stuck items held the metric
genuinely low for fifteen minutes.)

Fixed after the measurements: the start suspends target tracking's scale-in on the scalable
target before setting the count, and the next runs lift it once the low alarm has left ALARM
or the queue has drained. Verified (tt-k5, 2026-10-22): 50 items seeded with the low alarm in
ALARM since the previous batch, 5 keys.

| | m4v (before) | tt-k5 (fixed) |
|---|---:|---:|
| Desired after the start | 25, cut to 2 at +27 s | 25 throughout, no scaling activity |
| Scale-in suspended | -- | at the start; lifted 2 minutes later, after the low alarm went OK |
| Items run unprotected | 13 | 0 |
| Drained | 596 s (mostly at 2 tasks) | 264 s (681 items/hour) |
| Results | 50/50 | 50/50, DLQ 0 |

Two tasks ended on `TASK_STOPPING` refusals during the drain, as designed.

Two warnings on every run were false alarms on the measured image: "no write tool is trusted
(--trust-tools='read,write,subagent')" (the guard only knew the 2.21-era `fs_write` name;
outputs were produced), and "printed no Credits figure" (above). Both fixed after the
measurements: the guard knows the 2.30 names (`write`, `shell`), and the credit warning is
logged once per worker while `RunCreditsMissing` still counts every run.

### M5: elasticity from zero

5 keys, 75 items seeded into an idle fleet.

| | |
|---|---:|
| seeded to first running task | 98 s |
| peak tasks | 25 (the ceiling) |
| seeded to queue drained | 344 s (784 items/hour) |
| seeded to fleet back at zero | 361 s (17 s after drain) |

While messages were in flight but none visible (M4 and round 1 at 5 keys), desired stayed
at 25 and tasks kept starting, finding nothing, and exiting (235 such runs in one level):
no credits, but Fargate starts.

### Measuring notes

- SQS approximate counts can read 0 visible / 0 in flight mid-batch (seen 32 s into M4).
  `summarise` takes drain as the first empty sample after the last busy one.
- `creditBudget` is compared with the month-to-date figure, other clients included. The
  first sub-01 key was already at 7,195.81 of 1,000 and was refused before any item ran;
  it was replaced.
