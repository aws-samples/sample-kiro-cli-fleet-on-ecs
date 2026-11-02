"""Turn ramp results into the section 5 artifacts.

Two outputs, for two readers. A JSON series, so the numbers can be re-plotted or
checked later without re-running the benchmark. A markdown table, so the blog's
"What we measure" cell can be filled from a thing a human can read directly.

Both carry the same honesty requirement the measurement plan sets: these are
MEASURED points from a single subscription, not an extrapolation. The markdown says
so in its own text, because a table copied into a draft loses its surrounding
caveats first.
"""

from __future__ import annotations

import json
from pathlib import Path

from fleetbench.ramp import RampResult


def series(ramp: RampResult) -> dict:
    """A ramp as plain data: one row per concurrency level plus the onset summary."""
    return {
        "profile": ramp.profile,
        "onset_threshold": ramp.onset_threshold,
        "onset_concurrency": ramp.onset(),
        "aborted": ramp.aborted,
        "measured": True,
        "levels": [
            {
                "concurrency": level.concurrency,
                "ok": level.ok,
                "throttled": level.throttled,
                "errored": level.errored,
                "rejected": level.rejected,
                "throttled_fraction": round(level.throttled_fraction, 4),
                "median_latency_ms": (
                    round(level.median_latency_ms, 1)
                    if level.median_latency_ms is not None
                    else None
                ),
            }
            for level in ramp.levels
        ],
    }


def to_json(ramps: list[RampResult]) -> str:
    return json.dumps(
        {"benchmark": "M1", "unit": "concurrent streaming connections", "ramps": [series(r) for r in ramps]},
        indent=2,
    )


def _onset_line(ramp: RampResult) -> str:
    if ramp.aborted:
        reason = "credential rejected" if ramp.levels and ramp.levels[-1].rejected else (
            "every stream errored; check the transcripts under results/workspaces"
        )
        return f"- **{ramp.profile}**: aborted ({reason}) -- no onset measured."
    onset = ramp.onset()
    if onset is None:
        highest = ramp.levels[-1].concurrency if ramp.levels else 0
        return (
            f"- **{ramp.profile}**: no throttling up to {highest} concurrent streams "
            f"(push the ramp higher to find the onset)."
        )
    return f"- **{ramp.profile}**: throttling begins at **{onset}** concurrent streams."


def to_markdown(ramps: list[RampResult]) -> str:
    """The section 5 crown-jewel table: concurrency vs throttling, per profile, onset marked.

    One table per profile so the heavy and light curves read independently; a
    combined table would need a column per profile and stops being readable the
    moment a third profile is added.
    """
    lines: list[str] = []
    lines.append("# M1 -- per-subscription throttling curve")
    lines.append("")
    lines.append(
        "Measured on a **single** Kiro subscription by holding N streaming "
        "connections open at once and stepping N up. Each stream sends one model "
        "request, so this probes a concurrent-connection limit only; a request-rate "
        "limit needs a multi-turn workload (M1b, `--workload fanout`). These are "
        "measured points, not extrapolation."
    )
    lines.append("")
    lines.append("## Onset")
    for ramp in ramps:
        lines.append(_onset_line(ramp))
    lines.append("")

    for ramp in ramps:
        onset = ramp.onset()
        lines.append(f"## {ramp.profile}")
        lines.append("")
        lines.append("| concurrent streams | throttled % | throttled / total | median latency (ms) |")
        lines.append("|---:|---:|---:|---:|")
        for level in ramp.levels:
            marker = " **← onset**" if onset is not None and level.concurrency == onset else ""
            latency = (
                f"{level.median_latency_ms:.0f}"
                if level.median_latency_ms is not None
                else "--"
            )
            lines.append(
                f"| {level.concurrency}{marker} "
                f"| {level.throttled_fraction * 100:.0f}% "
                f"| {level.throttled} / {len(level.results)} "
                f"| {latency} |"
            )
        lines.append("")

    return "\n".join(lines)


def write_artifacts(ramps: list[RampResult], out_dir: Path) -> dict[str, Path]:
    """Write the JSON series and the markdown curve, returning where they landed."""
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "m1-series.json"
    md_path = out_dir / "m1-curve.md"
    json_path.write_text(to_json(ramps))
    md_path.write_text(to_markdown(ramps))
    return {"json": json_path, "markdown": md_path}
