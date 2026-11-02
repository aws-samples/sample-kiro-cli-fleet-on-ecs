"""The report must stay faithful to the ramp and honest about what it is.

These tests build ramp results by hand so the report is checked against known
numbers, including the two things a copied-out table most easily loses: the onset
marker and the "measured, not extrapolated" framing.
"""

from __future__ import annotations

import json

from fleetbench import report
from fleetbench.ramp import LevelResult, RampResult, StreamOutcome, StreamResult


def _level(concurrency: int, ok: int, throttled: int) -> LevelResult:
    results = (
        tuple(StreamResult(StreamOutcome.OK, 40.0) for _ in range(ok))
        + tuple(StreamResult(StreamOutcome.THROTTLED, 5.0) for _ in range(throttled))
    )
    return LevelResult(concurrency, results)


def _ramp() -> RampResult:
    return RampResult(
        profile="heavy",
        levels=(
            _level(5, ok=5, throttled=0),
            _level(8, ok=8, throttled=0),
            _level(10, ok=7, throttled=3),
        ),
        onset_threshold=0.0,
    )


class TestSeries:
    def test_series_reports_onset_and_rows(self) -> None:
        data = report.series(_ramp())
        assert data["profile"] == "heavy"
        assert data["onset_concurrency"] == 10
        assert data["measured"] is True
        assert len(data["levels"]) == 3
        assert data["levels"][-1]["throttled"] == 3

    def test_json_round_trips_and_names_the_unit(self) -> None:
        payload = json.loads(report.to_json([_ramp()]))
        assert payload["benchmark"] == "M1"
        assert "concurrent streaming connections" in payload["unit"]
        assert payload["ramps"][0]["onset_concurrency"] == 10


class TestMarkdown:
    def test_marks_the_onset_row(self) -> None:
        text = report.to_markdown([_ramp()])
        assert "onset" in text.lower()
        # The onset row (concurrency 10) carries the marker; a tolerated row does not.
        onset_row = next(line for line in text.splitlines() if line.startswith("| 10"))
        assert "onset" in onset_row.lower()

    def test_states_it_is_measured_not_extrapolated(self) -> None:
        text = report.to_markdown([_ramp()]).lower()
        assert "measured" in text
        assert "single" in text  # single subscription

    def test_no_onset_is_reported_plainly(self) -> None:
        clean = RampResult(
            profile="light",
            levels=(_level(5, ok=5, throttled=0), _level(10, ok=10, throttled=0)),
            onset_threshold=0.0,
        )
        text = report.to_markdown([clean])
        assert "no throttling up to 10" in text

    def test_aborted_ramp_says_so(self) -> None:
        aborted = RampResult(
            profile="heavy",
            levels=(_level(1, ok=0, throttled=0),),
            onset_threshold=0.0,
            aborted=True,
        )
        assert "aborted" in report.to_markdown([aborted]).lower()


class TestArtifacts:
    def test_writes_both_files(self, tmp_path) -> None:
        paths = report.write_artifacts([_ramp()], tmp_path / "out")
        assert paths["json"].exists()
        assert paths["markdown"].exists()
        assert json.loads(paths["json"].read_text())["ramps"][0]["profile"] == "heavy"
