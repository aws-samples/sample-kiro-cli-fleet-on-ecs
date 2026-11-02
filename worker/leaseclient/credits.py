"""Credits: what writes `creditsUsed`, and why each writer is shaped the way it is.

`creditsUsed` is a subscription's cumulative consumption in its current window, as
the Kiro service reports it. Four things write it:

- **Workers**, before every work item and, best-effort, once when a drain ends
  (scale-in to zero can stop the task before that last reading). A worker runs
  `kiro-cli chat --no-interactive "/usage"` with the subscription's key, parses the
  `Credits (<used> of <limit> covered in plan)` line, and records the absolute
  figure through `Registry.record_usage`. The write only raises the stored value
  within a window, so readings from many workers, in any order, end at the largest.
  A reading that cannot be parsed is a failed reading, never zero: it is logged and
  counted, and the work continues (fail open).
- **The daily activity report** (optional, `usagereport`), which sums the report's
  per-day `Credits_Used` into a month-to-date figure and raises `creditsUsed` to it
  when that is higher. It catches usage no fleet reading has seen yet, such as the
  owner's IDE on days the fleet did not run.
- **The reconciler**, which zeroes it when `windowResetsAt` passes and moves the
  window on by one calendar month (or by `CREDIT_WINDOW_DAYS`, if set).
- **An operator**, through `apply_external_correction`. The only path that can lower
  the value within a window, and the only one that is not conditional.

Nothing sums per-run figures. Two runs on one subscription each see consumption
that includes the other's, so adding per-run figures double-counts. The per-run
`▸ Credits:` line a headless run prints is published as a metric for sizing the
margin and is never written here; it also covers the primary agent only.

A reading lags real consumption: `/usage` takes about 45 seconds to include a
finished run, and runs in flight are not in it at all. After a batch the stored
value can trail the true total by the credits of the batch's last runs, until the
next claim's pre-item reading or the daily report. `CREDIT_MARGIN` covers that
gap, so the budget is a guardrail that stops slightly early, not a balance.
"""

from __future__ import annotations

import calendar
import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Final

from leaseclient.registry import Credits, Registry, SubscriptionRecord

logger = logging.getLogger(__name__)


def add_months(moment: datetime, months: int) -> datetime:
    """The same day and time `months` later (or earlier), clamped to the month's length."""
    year, month_index = divmod(moment.month - 1 + months, 12)
    year += moment.year
    month = month_index + 1
    day = min(moment.day, calendar.monthrange(year, month)[1])
    return moment.replace(year=year, month=month, day=day)


def window_start(resets_at: datetime, window: timedelta | None = None) -> datetime:
    """Where the window that ends at `resets_at` began.

    One calendar month earlier by default, matching how Kiro reports its reset date.
    A fixed `window` is used instead when `CREDIT_WINDOW_DAYS` is set.
    """
    if window is not None:
        return resets_at - window
    return add_months(resets_at, -1)


ENV_CREDIT_WINDOW_DAYS: Final = "CREDIT_WINDOW_DAYS"


def window_from_environment() -> timedelta | None:
    """`CREDIT_WINDOW_DAYS` as a fixed step, or None for one calendar month.

    A calendar month is the default because Kiro resets on a date (`resets on
    2026-11-01`). A 30-day step drifts from it: from 1 February it lands on 3 March
    and keeps an exhausted subscription blocked two extra days.
    """
    raw = os.environ.get(ENV_CREDIT_WINDOW_DAYS)
    if not raw:
        return None
    return timedelta(days=int(raw))


def apply_external_correction(
    registry: Registry, subscription_id: str, used: Credits
) -> None:
    """Overwrite the recorded consumption with an operator's figure.

    Replaces rather than merges, and can lower the value. This is the human
    override; the automated writers never lower within a window.
    """
    registry.override_credits_used(subscription_id, used)
    logger.info(
        "applied an external correction of %s credits to %s", used, subscription_id
    )


def window_is_due(record: SubscriptionRecord, now: datetime | None = None) -> bool:
    now = now or datetime.now(UTC)
    return record.window_resets_at is not None and record.window_resets_at <= now


def next_reset(
    current: datetime, now: datetime, window: timedelta | None = None
) -> datetime:
    """The first reset instant after `now`, stepping from `current`.

    Steps are counted from `current` rather than applied one after another, so a
    month-end anchor is not clamped further on every step (31 Jan, 28 Feb, 28 Mar).
    """
    steps = 1
    while True:
        if window is not None:
            candidate = current + window * steps
        else:
            candidate = add_months(current, steps)
        if candidate > now:
            return candidate
        steps += 1


def reset_window_if_due(
    registry: Registry,
    record: SubscriptionRecord,
    now: datetime | None = None,
    window: timedelta | None = None,
) -> bool:
    """Zero consumption and advance the window when its instant has passed.

    Advances from the recorded instant rather than from now, so a reconciler that runs
    late does not shift every subsequent window later with it. The step is one
    calendar month unless `window` is given.

    This reset is load-bearing: a subscription at its ceiling is never claimed, so no
    worker reads `/usage` for it, and the report never lowers. Without the reset it
    would stay blocked.
    """
    if not window_is_due(record, now):
        return False

    assert record.window_resets_at is not None
    moment = now or datetime.now(UTC)
    # Catches up if the reconciler was down for longer than a whole window.
    upcoming = next_reset(record.window_resets_at, moment, window)

    reset = registry.reset_window(
        record.subscription_id, upcoming, current=record.window_resets_at_raw
    )
    if reset:
        logger.info(
            "credit window for %s reset; next reset at %s",
            record.subscription_id,
            upcoming.isoformat(),
        )
    return reset
