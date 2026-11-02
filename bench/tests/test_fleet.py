"""The ECS half's pure core: task records to a level, and the overlap they prove."""

from fleetbench.fleet import level_from_records, overlap_markdown, overlap_of
from fleetbench.ramp import StreamOutcome


def _record(sent: float, ended: float, outcome: str = "Ok", epoch: float = 100.0) -> dict:
    return {
        "start_epoch": epoch,
        "sent_at": sent,
        "ended_at": ended,
        "latency_ms": (ended - sent) * 1000,
        "outcome": outcome,
        "detail": "NoOutput",
    }


def test_missing_task_records_count_as_errors_not_as_a_smaller_level():
    level = level_from_records(4, [_record(100, 160), _record(100, 170, "Throttled")])
    assert level.concurrency == 4
    assert (level.ok, level.throttled, level.errored) == (1, 1, 2)


def test_overlap_reports_peak_and_the_window_all_streams_shared():
    records = [_record(100.0, 200.0), _record(100.5, 180.0), _record(101.0, 150.0)]
    o = overlap_of(3, records)
    assert o.peak_open == 3
    assert o.all_open_s == 49.0  # from the last start (101) to the first end (150)
    assert o.late == 0


def test_a_straggler_is_late_and_shrinks_proven_overlap():
    records = [_record(100.0, 130.0), _record(140.0, 200.0)]
    o = overlap_of(2, records)
    assert o.late == 1
    assert o.peak_open == 1  # the second stream started after the first ended
    assert o.all_open_s == 0.0


def test_records_without_timing_are_not_counted_as_open():
    o = overlap_of(2, [{"outcome": StreamOutcome.ERROR.value, "detail": "no key"}])
    assert (o.recorded, o.peak_open) == (0, 0)


def test_overlap_markdown_has_a_row_per_level():
    text = overlap_markdown({"heavy": [overlap_of(1, [_record(100, 160)])]})
    assert "## heavy" in text
    assert "| 1 | 1 | 0 |" in text
