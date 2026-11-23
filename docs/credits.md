# Credits

Part of the [sample README](../README.md).

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
[Scale-in and the item in progress](scale-in.md).

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

## The daily activity report (optional)

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

## What is not verified

- **The activity report has never been ingested from a real file.** The tests use synthetic CSVs built
  from the documented columns and object layout. The `Date` format, whether `Date` is a UTC day, and
  whether month folders are zero-padded are assumptions; the parser accepts `YYYY-MM-DD`, `MM-DD-YYYY` and
  `YYYY/MM/DD`, and counts rows it cannot read. Check one real file before relying on it.
- **That the report's `UserId` corresponds to the API key's user, and that the report and `/usage` count
  the same credits.** If they differ, taking the larger errs towards stopping early.
- **That `resets on <date>` is a UTC-midnight boundary.** If the server rolls over at another hour,
  readings in the gap are dropped as stale, which is the safe direction.
## What has been verified on AWS

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
