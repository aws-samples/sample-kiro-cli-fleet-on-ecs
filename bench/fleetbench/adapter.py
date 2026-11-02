"""Turn the ramp's abstract stream call into something real or simulated.

make_cli_run_once drives the benchmark through the worker's own run_workflow, so M1
exercises exactly the invocation the fleet uses -- the same `kiro-cli chat
--no-interactive --require-mcp-startup`, the same throttle classification -- with no
second code path to keep in sync. make_simulated_run_once models a subscription that
throttles above a fixed concurrency, so the harness runs end to end with no network,
no credential, and no credits, the way local_batch falls back to a built-in workflow
when no key is present.

A simulated result says "simulated" in its detail, because a curve that never
touched Kiro proves less than one that did and must never be mistaken for it.
"""

from __future__ import annotations

import shutil
import threading
import time
import uuid
from pathlib import Path

from fleetbench.profiles import Profile
from fleetbench.ramp import RunOnce, StreamOutcome, StreamResult


def make_cli_run_once(
    profile: Profile,
    api_key: str,
    workspace_root: Path,
    *,
    executable: str = "kiro-cli",
    timeout_seconds: int = 180,
    isolate_home: bool = True,
) -> RunOnce:
    """A stream call that shells out to Kiro CLI, mapping its verdict to an outcome.

    The worker package is imported here, not at module load, so the pure ramp core
    and its tests never need it on the path. The import failing is a setup error with
    an actionable message, not a mystery at ramp time.

    Each concurrent stream gets its own workspace directory. The worker's output
    comparison is turned off (`enforce_output_path=False`) because M1 is not about
    what a run writes -- it opens a connection, streams, and closes, and the only
    question is whether the subscription accepted one more at once.

    `isolate_home` defaults on. `--require-mcp-startup` fails the CLI (exit 3) when any
    MCP server configured under the ambient HOME cannot start, and a developer machine
    usually has personal MCP servers configured that have nothing to do with this
    measurement. Isolating HOME gives the CLI a clean one containing only itself, so
    there are no servers to fail and M1 measures the subscription rather than the
    developer's local setup.
    """
    try:
        from runner.workflow import Verdict, kiro_cli_available, run_workflow
    except ImportError as error:  # pragma: no cover - exercised only without the worker installed
        raise RuntimeError(
            "the real M1 run needs the worker package importable (install it in the "
            "same environment: `uv pip install -e ../worker`)"
        ) from error

    if not kiro_cli_available(executable):
        raise RuntimeError(
            f"{executable!r} is not on PATH; the real M1 run needs an authenticated "
            "Kiro CLI. Use --dry-run to exercise the harness without one."
        )

    # A version-manager dispatcher (for example a toolbox-exec shim) picks the tool to
    # run from argv[0] and its own registry under HOME. Isolating HOME hides that
    # registry, so every stream fails in milliseconds with a non-zero exit. Refuse
    # up front and ask for the real binary instead of measuring the shim.
    resolved = shutil.which(executable)
    if isolate_home and resolved and Path(resolved).resolve().name == "toolbox-exec":
        raise RuntimeError(
            f"{executable!r} resolves to a toolbox-exec dispatcher, which cannot run "
            "under an isolated HOME. Pass the real binary with --executable "
            "(for example ~/.toolbox/tools/kiro-cli/<version>/'Kiro CLI.app'/Contents/MacOS/kiro-cli)."
        )

    # Only a run that exited cleanly counts as a served stream. A clean run with no
    # file output is still OK, because M1's prompts write nothing by design. A throttle
    # is the thing being measured, and a credential rejection aborts the ramp. Any
    # other failure -- startup, non-zero exit, timeout -- means the stream never proved
    # the subscription served it, so it is an error, never a tolerated stream.
    def classify(verdict: Verdict) -> StreamOutcome:
        if verdict is Verdict.THROTTLED:
            return StreamOutcome.THROTTLED
        if verdict is Verdict.CREDENTIAL_REJECTED:
            return StreamOutcome.REJECTED
        if verdict in (Verdict.SUCCEEDED, Verdict.NO_OUTPUT):
            return StreamOutcome.OK
        return StreamOutcome.ERROR

    def run_once() -> StreamResult:
        workspace = workspace_root / f"stream-{uuid.uuid4().hex}"
        workspace.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        result = run_workflow(
            prompt=profile.prompt,
            api_key=api_key,
            workspace=workspace,
            trust_tools=profile.trust_tools,
            timeout_seconds=timeout_seconds,
            executable=executable,
            expects_stdout=True,
            enforce_output_path=False,
            isolate_home=isolate_home,
        )
        latency_ms = (time.monotonic() - started) * 1000.0
        return StreamResult(classify(result.verdict), latency_ms, detail=str(result.verdict))

    return run_once


def make_simulated_run_once(
    throttle_above: int,
    *,
    served_latency_ms: float = 40.0,
    throttled_latency_ms: float = 5.0,
    poll_seconds: float = 0.005,
    stable_reads: int = 4,
) -> RunOnce:
    """A subscription that throttles once more than `throttle_above` streams overlap.

    It counts how many calls are in flight and polls until arrivals stop, so every
    sibling released from the ramp's barrier has been counted, then decides on the
    peak overlap it saw: at or below the cap the stream is served (longer latency),
    above it the stream is throttled (returning quickly, as a 429 does). Deciding on
    the peak -- and not decrementing until after the decision -- keeps the step sharp,
    so a dry run cleanly shows where the onset is instead of a timing-fuzzed edge.
    """
    lock = threading.Lock()
    in_flight = 0

    def run_once() -> StreamResult:
        nonlocal in_flight
        with lock:
            in_flight += 1
            observed_peak = in_flight
        try:
            last = -1
            stable = 0
            while stable < stable_reads:
                time.sleep(poll_seconds)
                with lock:
                    current = in_flight
                observed_peak = max(observed_peak, current)
                if current == last:
                    stable += 1
                else:
                    last = current
                    stable = 0
            if observed_peak > throttle_above:
                return StreamResult(StreamOutcome.THROTTLED, throttled_latency_ms, "simulated")
            return StreamResult(StreamOutcome.OK, served_latency_ms, "simulated")
        finally:
            with lock:
                in_flight -= 1

    return run_once
