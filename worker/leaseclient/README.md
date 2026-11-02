# leaseclient

Allocates Kiro subscriptions from a governed pool to worker tasks.

A worker asks for a subscription and receives one that provably has concurrency headroom and whose
recorded `creditsUsed` is below its budget less a margin, or is told the pool is saturated. Two workers never receive
capacity that does not exist, and a worker that dies without releasing does not permanently shrink the pool.

## The two properties everything else follows from

**Concurrency is exact. Credits are an estimate with a margin.** The held-lease counter is incremented
and decremented here, so it is authoritative and gates admission hard. Credit consumption is reported by
the Kiro service: workers read the cumulative figure from `/usage` and record it, but a reading misses runs
in flight and runs that ended in the last ~45 s. So `creditsUsed` lags, and the budget is enforced at
`creditBudget - margin`. Treating the two the same way produces a pool that stalls unpredictably.

**Failure is the normal case.** Workers are tasks stopped by scale-in, by crashes, and by reclaimed
capacity, none of which run a release path. So expiry, enforced by the datastore, is what guarantees
capacity returns. Explicit release is an optimization on top: without it the next worker waits out a lease
lifetime for capacity that is already free.

## The reservation

One conditional update both checks headroom and consumes it:

```
SET activeLeases = activeLeases + 1
IF  attribute_exists(subscriptionId)
AND state = ENABLED
AND activeLeases < maxConcurrency
AND creditsUsed  < :ceiling
```

Because the check and the consumption are one operation, correctness under contention does not depend on
workers coordinating. A failed condition is the expected outcome once the pool is busy, not an error,
which is why the caller loops over candidates.

`:ceiling` is resolved by the caller as `creditBudget - margin`, because DynamoDB permits arithmetic in an
update expression but not in a condition. The state index projects every attribute, so the budget is
already in hand and nothing extra is read. A budget changed between that read and this write leaves the
ceiling one generation stale, bounded by the margin, and the next attempt sees the new value.

Capacity is reserved **before** the lease recording it is written. Dying between the two leaves a counter
briefly high with no lease behind it, which reconciliation resolves. The opposite order would allow a
lease with no reservation, which is real overcommit. Given a choice of failure, prefer the one that
reconciles toward correctness.

## Recovery

| Mechanism | Trigger | Guarantees |
|---|---|---|
| Explicit release | worker finishes, or receives SIGTERM | capacity returns at once |
| Lease expiry | datastore time-to-live | capacity returns without anything running |
| Stream handler | time-to-live removal event | the counter drops within seconds of expiry |
| Scheduled pass | periodic | the counter matches the leases that actually exist |

The scheduled pass is the guarantee, not a backstop: DynamoDB gives no upper bound on when a time-to-live
deletion happens, and a stream record can be missed. The stream only makes it prompt.

A removal is credited only when the time-to-live service performed it. Without that check, a normal
release would be decremented twice: once by itself, once when its removal reached the stream.

Correction never lowers a counter below the leases it observed, so a worker mid-run cannot lose its
capacity, and never deletes a lease.

## Credits

`creditsUsed` is a subscription's cumulative consumption in its current window. All writes are absolute
figures; nothing adds a per-run figure, because two runs on one subscription each see consumption that
includes the other's.

| Writer | When | Write |
|---|---|---|
| worker (`runner.worker`, `Registry.record_usage`) | before every work item, and once (best-effort) when the drain ends | raise only, within the window `/usage` reports; a later window is adopted, an ended one dropped |
| activity report (`usagereport`, `Registry.raise_credits_from_report`) | daily, if configured | raise only, conditional on the window the figure was computed for |
| reconciler (`credits.reset_window_if_due`) | when `windowResetsAt` passes | zero, and the window moves on one calendar month (or `CREDIT_WINDOW_DAYS`) |
| operator (`credits.apply_external_correction`) | by hand | replace; the only write that can lower within a window |

The worker's raise is one conditional update:

```
SET creditsUsed = :v
IF  attribute_exists(subscriptionId) AND windowResetsAt = :window AND creditsUsed < :v
```

On a failed condition the stored item comes back with the error, so deciding between "already higher",
"different window" and "missing" costs no extra read. Readings from any number of workers, in any order,
end at the largest. This is verified against DynamoDB Local with sixty concurrent writers.

`/usage` is parsed from stdout and stderr merged, with ANSI (`\x1b\[[0-9;?]*[A-Za-z]`) stripped, on
`Credits \(<number> of <number> covered in plan\)` and `resets on YYYY-MM-DD`. A number is digits with an
optional fraction, so `1.2.3` is rejected rather than misread. A missing line is a failed reading, never zero: the
command exits 0 when the backend is unreachable. A failed reading is logged and counted
(`UsageReading{Outcome=ReadFailed}`) and the item runs anyway (fail open).

After each pre-item reading the worker reads the record back and stops draining if `creditsUsed >=
creditBudget - margin`. It stops before receiving, so the remaining messages stay in the queue with their
receive counts unchanged, and the lease is released on the way out.

**The stored value trails the true total after a batch, by design.** The drain-end reading is best-effort
and may be skipped: once the queue drains, the scaling function sets the service to zero on its next
minute, and that scale-in can stop the task first. On AWS, `queue drained` was logged at 14:10:06, the
service was set to zero at 14:10:08, and SIGTERM arrived at 14:10:11, before the reading. When the reading
does run, it usually comes less than ~45 s after the task's last run, so it does not include that run, and
with several tasks it can miss the last run of each. Either way, nothing reads `/usage` again until the
next claim's pre-item reading or the daily report, so until then `creditsUsed` can trail the server's
figure by the credits of the batch's last runs. The margin covers this. On AWS the run above left 14.17
stored against a fresh `/usage` of 14.68, and an earlier three-item batch with two tasks left 13.42 against
13.74; the default margin of 1 covered both.

No delay is added before the drain-end reading, because waiting out the lag would cost about 60 s of
Fargate per task to close a gap the margin already covers. A check of the stored value against a fresh
reading should therefore allow a difference up to the margin, not a tighter tolerance.

Values are `Decimal` in Python and `N` in DynamoDB. Items written with integer values still read.

## Configuration

| Setting | Default | Why |
|---|---|---|
| lease lifetime | 120s | long enough to survive a transient datastore error and its retries |
| heartbeat interval | lifetime / 4 | derived, not configured: set independently the two can be made inconsistent, and the failure is a lease expiring under a working worker. A quarter leaves three consecutive failed extensions survivable |
| max claim attempts | 5 | bounded, so a saturated pool ends in a clean exit rather than a loop |
| backoff | exponential, full jitter, capped at 30s | jitter matters more than the curve: hundreds of tasks start together, and a fixed schedule synchronizes their retries into contending waves |
| credit margin | 1 (`CREDIT_MARGIN`, fractions to 0.01) | covers runs in flight and the ~45 s before `/usage` includes a finished run: `2 × C × P + 0.01`, with `C` the largest `maxConcurrency` and `P` the p95 `RunCredits`. The measured trivial prompt gives 0.21; the default is rounded up because the example workflows are unmeasured. The stack sets it from `fleet:creditMargin`, and passes the same value to the scaling function, which counts a subscription toward the fleet ceiling only below `creditBudget - margin` |
| `/usage` timeout | 30s (`USAGE_TIMEOUT_SECONDS`) | the slowest success measured on Fargate was 9.1 s and the failure path 21.9 s |

Table names come from `SUBSCRIPTIONS_TABLE` and `LEASES_TABLE` and are required eagerly. A worker that
guessed a table name could operate against the wrong pool.

## Key material

Resolved at claim time from the secret store, using the task's own identity, and verified to be readable
before a lease is handed out. A lease whose key is unusable is worse than no lease: the worker would hold
capacity and fail to authenticate, and an invalid credential does not reliably produce a non-zero exit,
so it could discard its work item while appearing to succeed.

The value goes into the child process environment and nowhere else. It is never logged, stored, returned
by any interface the caller does not hold a lease for, or included in an exception message. `Session`
defines its own `repr` so a default one cannot put it in a log line, and a test sweep fails any test whose
logs or output contain seeded key material.

## Running the tests

```bash
cd worker
uv sync --frozen --extra dev
uv run --frozen pytest
```

No AWS account, credentials, or deployed resources. The harness severs every path boto3 could use to
reach a real account, including named profiles and the instance metadata service.

Contention tests need DynamoDB Local:

```bash
docker run -d --rm --name ddb-local -p 8000:8000 \
    amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -inMemory -sharedDb
```

They do not run against the in-process fake, and that is a measurement rather than a preference: a
deliberately non-atomic read-then-write reservation passes the concurrent scenario against the fake
unchanged, while against DynamoDB Local it handed out four times the real capacity. One test runs that
naive implementation and asserts it overcommits, so if contention stops being reproduced the suite fails
instead of reporting confidence it has not earned.
