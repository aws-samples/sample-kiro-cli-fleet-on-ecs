"""Run the M1 benchmark from the command line.

    # Wiring check, no network and no credential:
    uv run --frozen python -m fleetbench --dry-run

    # Real measurement against ONE subscription (key from the environment, never an
    # argument -- an argument lands in shell history and process listings):
    KIRO_API_KEY=... uv run python -m fleetbench --max-concurrency 20

Mirrors local_batch's contract: with a key present it runs the real CLI, without one
it refuses unless --dry-run is given, and it says in the output which path it took so
a simulated curve is never mistaken for a measured one.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from fleetbench import profiles, report
from fleetbench.adapter import make_cli_run_once, make_simulated_run_once
from fleetbench.ramp import LevelResult, default_schedule, run_ramp

_API_KEY_ENV = "KIRO_API_KEY"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="fleetbench", description=__doc__)
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=20,
        help="highest number of simultaneous streams to try (default: 20)",
    )
    parser.add_argument(
        "--start-at",
        type=int,
        default=1,
        help="lowest number of simultaneous streams to try, to skip levels an earlier "
        "run already measured (default: 1)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=180,
        help="seconds one stream may run before it counts as an error; raise it at high "
        "concurrency, where streams slow down before they throttle (default: 180)",
    )
    parser.add_argument(
        "--profiles",
        default="heavy,light",
        help="comma-separated stream shapes to measure (default: heavy,light)",
    )
    parser.add_argument(
        "--onset-threshold",
        type=float,
        default=0.0,
        help="throttled fraction that counts as onset; 0.0 means the first throttle "
        "(default: 0.0)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("results"),
        help="directory for the JSON series and markdown curve (default: results/)",
    )
    parser.add_argument(
        "--executable",
        default="kiro-cli",
        help="Kiro CLI executable name or path (default: kiro-cli)",
    )
    parser.add_argument(
        "--isolate-home",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="run the CLI with a clean HOME containing only itself, so personal MCP "
        "servers under the ambient HOME cannot fail --require-mcp-startup (default: on)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="simulate a subscription instead of calling Kiro CLI; no network, no credits",
    )
    parser.add_argument(
        "--simulated-cap",
        type=int,
        default=8,
        help="for --dry-run only: throttle above this many concurrent streams (default: 8)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    try:
        wanted = profiles.resolve([p.strip() for p in args.profiles.split(",") if p.strip()])
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    try:
        schedule = default_schedule(args.max_concurrency, start_at=args.start_at)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    api_key = os.environ.get(_API_KEY_ENV, "")

    if not args.dry_run and not api_key:
        print(
            f"error: no {_API_KEY_ENV} in the environment. Set it to measure one real "
            "subscription, or pass --dry-run to exercise the harness without one.",
            file=sys.stderr,
        )
        return 2

    mode = "SIMULATED (no Kiro CLI was called)" if args.dry_run else "measured against Kiro CLI"
    print(f"M1 throttling ramp -- {mode}")
    print(f"schedule: {schedule}")
    print()

    workspace_root = args.out / "workspaces"
    ramps = []
    for profile in wanted:
        def on_level(level: LevelResult, profile_name: str = profile.name) -> None:
            print(
                f"  [{profile_name}] {level.concurrency:>3} streams: "
                f"ok={level.ok} throttled={level.throttled} "
                f"errored={level.errored} rejected={level.rejected}"
            )

        if args.dry_run:
            run_once = make_simulated_run_once(args.simulated_cap)
        else:
            run_once = make_cli_run_once(
                profile, api_key, workspace_root, executable=args.executable,
                isolate_home=args.isolate_home, timeout_seconds=args.timeout,
            )

        print(f"profile: {profile.name}")
        ramp = run_ramp(
            run_once,
            schedule,
            profile=profile.name,
            onset_threshold=args.onset_threshold,
            on_level=on_level,
        )
        ramps.append(ramp)
        print()

    paths = report.write_artifacts(ramps, args.out)
    print(report.to_markdown(ramps))
    print()
    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")
    if args.dry_run:
        print("\nNOTE: these numbers are simulated. Re-run with KIRO_API_KEY set for a measured curve.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
