"""What the ramp must get right: real overlap, correct onset, honest stopping.

Every assertion here is about the harness's own behaviour, driven by fakes with a
known limit. None of it touches Kiro, so a failure means the measurement machinery
is wrong, which is the only thing these tests can and should catch.
"""

from __future__ import annotations

import itertools

from fleetbench.ramp import (
    StreamOutcome,
    StreamResult,
    default_schedule,
    run_level,
    run_ramp,
)
from tests.conftest import ConcurrencyProbe


class TestRunLevel:
    def test_holds_all_streams_open_at_once(self) -> None:
        """The barrier must make N streams genuinely overlap, not run in sequence."""
        probe = ConcurrencyProbe(cap=100)  # high cap: nothing throttles, we only watch overlap
        level = run_level(probe, concurrency=8)
        assert level.concurrency == 8
        assert len(level.results) == 8
        assert probe.peak == 8, "streams did not overlap; the barrier is not working"

    def test_below_cap_nothing_throttles(self) -> None:
        probe = ConcurrencyProbe(cap=5)
        level = run_level(probe, concurrency=5)
        assert level.throttled == 0
        assert level.ok == 5
        assert level.throttled_fraction == 0.0

    def test_above_cap_everything_throttles(self) -> None:
        probe = ConcurrencyProbe(cap=5)
        level = run_level(probe, concurrency=6)
        assert level.throttled == 6
        assert level.throttled_fraction == 1.0

    def test_median_latency_ignores_throttled_streams(self) -> None:
        probe = ConcurrencyProbe(cap=5)
        served = run_level(probe, concurrency=5)
        assert served.median_latency_ms == 40.0
        throttled = run_level(probe, concurrency=6)
        # Every stream throttled, so there is no served latency to report.
        assert throttled.median_latency_ms is None

    def test_concurrency_below_one_is_refused(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            run_level(ConcurrencyProbe(cap=1), concurrency=0)


class TestRunRamp:
    def test_finds_onset_at_first_throttling_level(self) -> None:
        probe = ConcurrencyProbe(cap=8)
        ramp = run_ramp(probe, [1, 2, 4, 8, 10, 12], profile="heavy")
        # 8 is tolerated, 10 is the first level above the cap.
        assert ramp.onset() == 10
        assert not ramp.aborted

    def test_stops_once_onset_is_found(self) -> None:
        probe = ConcurrencyProbe(cap=8)
        ramp = run_ramp(probe, [1, 2, 4, 8, 10, 12, 15, 20], profile="heavy")
        tried = [level.concurrency for level in ramp.levels]
        # The ramp must not keep spending past the onset at 10.
        assert tried == [1, 2, 4, 8, 10]

    def test_no_throttling_means_no_onset(self) -> None:
        probe = ConcurrencyProbe(cap=100)
        ramp = run_ramp(probe, [1, 5, 10, 20], profile="light")
        assert ramp.onset() is None
        assert not ramp.aborted
        # Without an onset the ramp runs the whole schedule.
        assert [lvl.concurrency for lvl in ramp.levels] == [1, 5, 10, 20]

    def test_zero_throttle_level_is_not_reported_as_onset(self) -> None:
        """At the default threshold of 0.0, a clean level must not read as onset."""
        probe = ConcurrencyProbe(cap=100)
        ramp = run_ramp(probe, [1, 2, 3], profile="light")
        assert ramp.onset() is None

    def test_credential_rejection_aborts_the_ramp(self) -> None:
        steps = itertools.count()

        def reject_on_third() -> StreamResult:
            n = next(steps)
            if n >= 2:
                return StreamResult(StreamOutcome.REJECTED, 1.0, "bad key")
            return StreamResult(StreamOutcome.OK, 10.0, "")

        ramp = run_ramp(reject_on_third, [1, 1, 1, 1, 1], profile="heavy")
        assert ramp.aborted
        # It stopped at the level that saw the rejection rather than running all five.
        assert len(ramp.levels) < 5

    def test_a_level_where_every_stream_errored_aborts_the_ramp(self) -> None:
        """An environment that never reaches Kiro must not print as a clean curve."""

        def always_error() -> StreamResult:
            return StreamResult(StreamOutcome.ERROR, 1.0, "Failed")

        ramp = run_ramp(always_error, [1, 2, 3], profile="light")
        assert ramp.aborted
        assert len(ramp.levels) == 1
        assert ramp.onset() is None

    def test_partial_errors_do_not_abort(self) -> None:
        calls = itertools.count()

        def one_error_per_level() -> StreamResult:
            outcome = StreamOutcome.ERROR if next(calls) % 2 == 0 else StreamOutcome.OK
            return StreamResult(outcome, 10.0, "")

        ramp = run_ramp(one_error_per_level, [2, 2, 2], profile="light")
        assert not ramp.aborted
        assert len(ramp.levels) == 3
    def test_positive_threshold_needs_more_than_one_stray_throttle(self) -> None:
        # Throttle exactly one stream in ten regardless of concurrency: a 10% rate.
        calls = itertools.count()

        def one_in_ten() -> StreamResult:
            n = next(calls)
            outcome = StreamOutcome.THROTTLED if n % 10 == 0 else StreamOutcome.OK
            return StreamResult(outcome, 10.0, "")

        # A 0.5 threshold must not call onset on a 10% throttle rate.
        ramp = run_ramp(
            one_in_ten, [10, 10, 10], profile="heavy", onset_threshold=0.5
        )
        assert ramp.onset() is None


class TestSchedule:
    def test_default_schedule_is_dense_low_and_bounded(self) -> None:
        schedule = default_schedule(20)
        assert schedule[0] == 1
        assert schedule[-1] == 20
        assert schedule == sorted(schedule)
        assert len(set(schedule)) == len(schedule)

    def test_schedule_always_includes_the_max(self) -> None:
        assert default_schedule(7)[-1] == 7
        assert 7 in default_schedule(7)

    def test_start_at_skips_levels_already_measured(self) -> None:
        assert default_schedule(60, start_at=25) == [25, 30, 40, 50, 60]
        # An off-knee start is still tried itself rather than rounded up.
        assert default_schedule(40, start_at=27)[0] == 27

    def test_start_above_max_is_refused(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            default_schedule(10, start_at=20)
