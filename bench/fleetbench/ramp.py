"""The M1 ramp: find where one subscription starts throttling.

M1 is the measurement plan's "crown jewel" and the only one of M1~M5 that needs a
single key and no fleet. The quantity it measures is a *concurrency*, not a rate:
Kiro throttles on the number of streaming connections a subscription holds open at
once, and a tool call releases its connection, so the limit is reached by holding
many streams open simultaneously rather than by calling quickly. That is why the
driver below launches N genuinely-simultaneous streams and steps N upward, instead
of firing requests on a timer.

The driving logic is pure and takes the stream call as an argument (`RunOnce`). The
real call shells out to Kiro CLI (see adapter.py); the tests pass a fake that
throttles above a chosen concurrency. Keeping the two apart is what lets the
property this file exists to prove -- "throttling begins at concurrency K" -- be
tested without a network, a credential, or burning real credits.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import StrEnum
from statistics import median
from typing import Final


class StreamOutcome(StrEnum):
    OK = "Ok"
    """The connection was served. Whether the workflow itself produced useful output
    is not M1's question -- only whether the subscription accepted one more
    concurrent stream."""

    THROTTLED = "Throttled"
    """The subscription refused this stream for being one too many at once (429)."""

    REJECTED = "Rejected"
    """The credential itself was refused. Not a throttle signal, and it will not
    change by pushing harder, so it aborts the ramp rather than counting as data."""

    ERROR = "Error"
    """The attempt failed for some other reason (launch failure, barrier break). Kept
    distinct from OK so a run full of errors cannot masquerade as a clean curve."""


@dataclass(frozen=True)
class StreamResult:
    outcome: StreamOutcome
    latency_ms: float
    detail: str = ""


# One streaming attempt against ONE subscription key, returning how it went. Injected
# so the ramp is driven by the real CLI in production and by a fake in tests.
RunOnce = Callable[[], StreamResult]


_BARRIER_TIMEOUT_SECONDS: Final = 60.0
"""How long a stream waits for its siblings before giving up on starting together.

Generous because a heavy profile's process launch is not instant, but bounded so a
thread that never arrives fails the level loudly instead of hanging the whole run.
"""


@dataclass(frozen=True)
class LevelResult:
    """What happened when `concurrency` streams were held open at once."""

    concurrency: int
    results: tuple[StreamResult, ...]

    @property
    def ok(self) -> int:
        return sum(1 for r in self.results if r.outcome is StreamOutcome.OK)

    @property
    def throttled(self) -> int:
        return sum(1 for r in self.results if r.outcome is StreamOutcome.THROTTLED)

    @property
    def errored(self) -> int:
        return sum(1 for r in self.results if r.outcome is StreamOutcome.ERROR)

    @property
    def rejected(self) -> int:
        return sum(1 for r in self.results if r.outcome is StreamOutcome.REJECTED)

    @property
    def throttled_fraction(self) -> float:
        """Share of attempts that were throttled, over all attempts at this level.

        Over the total, not over OK+throttled: an attempt that errored still occupied
        a slot, and hiding it would flatter the curve.
        """
        if not self.results:
            return 0.0
        return self.throttled / len(self.results)

    @property
    def median_latency_ms(self) -> float | None:
        """Median latency of the streams that were actually served.

        Throttled and rejected attempts return early by definition, so including them
        would report a latency that drops exactly when the subscription is most
        stressed -- the opposite of the truth.
        """
        served = [r.latency_ms for r in self.results if r.outcome is StreamOutcome.OK]
        return median(served) if served else None


@dataclass(frozen=True)
class RampResult:
    profile: str
    levels: tuple[LevelResult, ...]
    onset_threshold: float
    aborted: bool = False

    def onset(self) -> int | None:
        """The lowest concurrency at which throttling reached the onset threshold.

        None means the ramp never crossed it: either the subscription tolerated every
        level tried (push higher), or the run aborted before it could. The caller
        tells those apart with `aborted`.

        A level with no throttles at all is never the onset, even at the default
        threshold of 0.0 -- otherwise "zero throttles" would read as "crossed", and
        the first level would always be reported as where throttling begins.
        """
        for level in self.levels:
            if level.throttled and level.throttled_fraction >= self.onset_threshold:
                return level.concurrency
        return None


def run_level(
    run_once: RunOnce,
    concurrency: int,
    *,
    barrier_timeout_seconds: float = _BARRIER_TIMEOUT_SECONDS,
) -> LevelResult:
    """Hold exactly `concurrency` streams open at the same time and record each.

    The barrier is the point of the function. Without it, a thread pool of N workers
    still starts its tasks a little apart, and a subscription limit on *simultaneous*
    streams could be missed because the early streams finish before the late ones
    begin. Releasing all N from a barrier makes the overlap real, which is the
    condition M1 is about.
    """
    if concurrency < 1:
        raise ValueError("concurrency must be at least 1")

    barrier = threading.Barrier(concurrency)

    def attempt() -> StreamResult:
        try:
            # Wait for every sibling, so all N connections open together rather than
            # in a ragged sequence.
            barrier.wait(timeout=barrier_timeout_seconds)
        except threading.BrokenBarrierError:
            return StreamResult(StreamOutcome.ERROR, 0.0, "barrier broke before start")
        return run_once()

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(attempt) for _ in range(concurrency)]
        results = []
        for future in futures:
            try:
                results.append(future.result())
            except Exception as error:  # noqa: BLE001 - a failed stream is data, not a crash
                results.append(StreamResult(StreamOutcome.ERROR, 0.0, repr(error)))

    return LevelResult(concurrency, tuple(results))


def default_schedule(max_concurrency: int, start_at: int = 1) -> list[int]:
    """Concurrency levels to try: dense low, then widening steps.

    Dense at the bottom because the heavy-stream onset the plan expects is in the low
    double digits, and widening above because once throttling is established there is
    nothing more to learn by testing every integer -- only credits to spend.

    `start_at` skips levels a previous run already measured, so pushing the ceiling
    higher does not pay again for the bottom of the curve. Both ends are always tried.
    """
    if start_at > max_concurrency:
        raise ValueError(f"start_at ({start_at}) is above max_concurrency ({max_concurrency})")
    knees = [1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 25, 30, 40, 50, 60, 80, 100]
    levels = [k for k in knees if start_at <= k <= max_concurrency]
    levels += [start_at, max_concurrency]
    return sorted({k for k in levels if k >= 1})


def run_ramp(
    run_once: RunOnce,
    schedule: Sequence[int],
    *,
    profile: str,
    onset_threshold: float = 0.0,
    stop_after_onset: bool = True,
    on_level: Callable[[LevelResult], None] | None = None,
    level_runner: Callable[[int], LevelResult] | None = None,
) -> RampResult:
    """Step concurrency up through `schedule` until throttling appears.

    `onset_threshold` is the throttled fraction that counts as onset. The default of
    0.0 means "the first throttle at all", which is the conservative reading for a
    cap-safety argument; a noisier environment can require a small positive fraction
    so one stray 429 does not call the onset early.

    Stopping once onset is found is deliberate frugality: the curve's job is to locate
    where throttling begins, and every level past that against a real subscription
    costs credits for a point already made. A credential rejection aborts immediately
    for the same reason -- pushing a dead key harder learns nothing.

    `level_runner` replaces the in-process barrier with something else that holds N
    streams open -- the ECS launcher, where each stream is its own Fargate task -- so
    the stopping rules stay in one place whichever way the streams are produced.
    """
    levels: list[LevelResult] = []
    aborted = False

    for concurrency in schedule:
        if level_runner is not None:
            level = level_runner(concurrency)
        else:
            level = run_level(run_once, concurrency)
        levels.append(level)
        if on_level is not None:
            on_level(level)

        # A level where every stream errored never reached the subscription, so it
        # says nothing about throttling. Climbing further would only repeat the
        # environment's failure at higher concurrency and print it as a clean curve.
        if level.rejected or (level.results and level.errored == len(level.results)):
            aborted = True
            break

        if stop_after_onset and level.throttled_fraction >= onset_threshold and level.throttled:
            break

    return RampResult(
        profile=profile,
        levels=tuple(levels),
        onset_threshold=onset_threshold,
        aborted=aborted,
    )
