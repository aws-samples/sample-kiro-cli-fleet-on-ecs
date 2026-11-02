"""Allocation of Kiro subscriptions from a governed pool.

A worker starting up asks this package for a subscription. It receives one that
provably has concurrency headroom and whose recorded credit use is below its
budget minus a margin, or it is told the pool is saturated. Two workers never
receive capacity that does not exist, and a worker that dies without releasing
its allocation does not permanently reduce pool capacity.

Two properties shape everything here:

Concurrency is exact; credits are an estimate with a margin. The held-lease
count is incremented and decremented by this package, so it is authoritative and
is used as a hard gate. Credit consumption is reported by the Kiro service.
Workers read the cumulative figure from `/usage` before every work item and,
best-effort, once when a drain ends, and record it. Automated writes only raise `creditsUsed` within
a window; only a new window or an operator correction lowers it. A reading
misses runs in flight and runs that ended in the last ~45 seconds, so the figure
lags, and the budget is a guardrail enforced at `creditBudget - margin`. See
`leaseclient.credits`.

Failure is the normal case. Workers are tasks that get stopped by scale-in and by
crashes. Lease expiry, enforced by the datastore, is the primary recovery
mechanism; explicit release is an optimization on top of it.
"""

from leaseclient.schema import MissingConfiguration, SubscriptionState

__all__ = ["MissingConfiguration", "SubscriptionState"]
