"""Run the whole pattern on one machine.

    uv run --frozen python -m runner.local_batch --items 5

Registers a pool, seeds work, starts workers, and drains the queue: the same worker
loop, allocator, and lease client that run in the fleet, with the queue and result
store swapped for local implementations. What this exercises is the part that is easy
to get wrong and hard to see in a deployed system — allocation under contention,
result verification, and acknowledgement ordering.

Needs DynamoDB Local, because the reservation is the thing worth exercising and the
in-process fake does not reproduce write contention:

    docker run -d --rm --name ddb-local -p 8000:8000 \
        amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -inMemory -sharedDb

With a Kiro credential present it runs the real CLI. Without one it runs a built-in
workflow that writes a file, so the loop can still be demonstrated end to end. The
difference is reported rather than hidden, because a run that did not call the CLI
proves less.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import timedelta
from pathlib import Path

import boto3
import botocore.config

from leaseclient import schema
from leaseclient.claim import Allocator, ClaimConfig, Leases
from leaseclient.metrics import Metrics
from leaseclient.registry import Registry
from leaseclient.session import PoolSaturated, hold_subscription
from runner.definition import InvalidWorkflow, WorkflowDefinition
from runner.local import LocalQueue, LocalResultStore
from runner.worker import Worker
from runner.workflow import OUTPUT_DIR, RunResult, Verdict, kiro_cli_available, run_workflow

logger = logging.getLogger("local_batch")

DEFAULT_ENDPOINT = os.environ.get("DYNAMODB_LOCAL_ENDPOINT", "http://localhost:8000")


def dynamodb_client(endpoint: str):
    return boto3.client(
        "dynamodb",
        region_name="us-east-1",
        endpoint_url=endpoint,
        aws_access_key_id="local",
        aws_secret_access_key="local",
        config=botocore.config.Config(
            retries={"total_max_attempts": 2}, connect_timeout=2, read_timeout=15
        ),
    )


class KeychainSecrets:
    """Resolves a subscription key from the macOS keychain.

    Chosen so a local run needs no key on disk and no key on a command line. The
    value goes from the keychain into the child process environment and nowhere
    else.
    """

    def __init__(self, service: str) -> None:
        self._service = service

    def get_secret_value(self, SecretId: str) -> dict[str, str]:  # noqa: N803
        result = subprocess.run(
            ["security", "find-generic-password", "-s", self._service, "-w"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"no keychain item named {self._service!r}")
        return {"SecretString": result.stdout.strip()}


class EnvSecrets:
    def __init__(self, value: str) -> None:
        self._value = value

    def get_secret_value(self, SecretId: str) -> dict[str, str]:  # noqa: N803
        return {"SecretString": self._value}


class StubSecrets:
    def get_secret_value(self, SecretId: str) -> dict[str, str]:  # noqa: N803
        return {"SecretString": "FAKEKEY-local-batch-no-credential"}


def make_builtin_workflow(delay_seconds: float = 0.0):
    """Stands in for the CLI when no credential is available.

    Deliberately writes real output and answers on stdout, so the completion check
    exercises the same three signals it would for a real run. The delay exists so a batch
    can be interrupted part way through, which is how scale-in behaviour gets tested.
    """

    def builtin_workflow(prompt: str, api_key: str, workspace: Path, **_kwargs) -> RunResult:
        if delay_seconds:
            time.sleep(delay_seconds)
        output = workspace / OUTPUT_DIR
        output.mkdir(parents=True, exist_ok=True)
        (output / "result.md").write_text(f"# Result\n\nPrompt: {prompt}\n")
        return RunResult(
            verdict=Verdict.SUCCEEDED,
            exit_code=0,
            stdout=f"handled: {prompt}",
            stderr="",
            outputs=(output / "result.md",),
        )

    return builtin_workflow


def register_pool(registry: Registry, size: int, concurrency: int, secret_ref: str) -> list[str]:
    ids = []
    for index in range(size):
        subscription_id = f"sub-{index:02d}"
        registry.register(
            subscription_id=subscription_id,
            secret_ref=secret_ref,
            max_concurrency=concurrency,
            credit_budget=10_000,
        )
        ids.append(subscription_id)
    return ids


def create_tables(ddb, suffix: str) -> tuple[str, str]:
    subscriptions = f"{suffix}-subs"
    leases = f"{suffix}-leases"
    ddb.create_table(**schema.subscriptions_table_definition(subscriptions))
    ddb.create_table(**schema.leases_table_definition(leases))
    ddb.update_time_to_live(
        TableName=leases, TimeToLiveSpecification=schema.leases_ttl_specification()
    )
    return subscriptions, leases


def choose_secrets(args) -> tuple[object, bool, str]:
    """Pick a credential source, and say why.

    The reason is returned rather than inferred, because a silent fall back to the
    built-in workflow looks like a successful run and is not one: it never calls the
    CLI. Reporting which source was used, and why the others were not, is the
    difference between a result and a result you can trust.
    """
    if args.no_cli:
        return StubSecrets(), False, "--no-cli was passed"

    if os.environ.get("KIRO_API_KEY"):
        return EnvSecrets(os.environ["KIRO_API_KEY"]), True, "KIRO_API_KEY is set"

    reasons = ["KIRO_API_KEY is not set in this process"]

    if sys.platform == "darwin" and args.keychain_service:
        probe = subprocess.run(
            ["security", "find-generic-password", "-s", args.keychain_service, "-w"],
            capture_output=True,
            text=True,
            check=False,
        )
        if probe.returncode == 0 and probe.stdout.strip():
            return (
                KeychainSecrets(args.keychain_service),
                True,
                f"keychain item {args.keychain_service!r}",
            )
        reasons.append(
            f"keychain item {args.keychain_service!r} not found "
            f"(security exited {probe.returncode})"
        )

    return StubSecrets(), False, "; ".join(reasons)


def _verdict_text(ok: bool, interrupted: bool) -> str:
    if ok and interrupted:
        return "stopped on request and returned all capacity"
    if ok:
        return "every item produced a stored result and all capacity came back"
    return "see above"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--items", type=int, default=5)
    parser.add_argument("--subscriptions", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=2, help="per subscription")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument(
        "--results-dir",
        default=None,
        help="where results go; defaults to a temporary directory. Set it to compare two "
        "runs, or to read results from outside a container",
    )
    parser.add_argument(
        "--keychain-service",
        default="kiro-subscription-sub-01",
        help="macOS keychain item holding a Kiro API key",
    )
    parser.add_argument(
        "--no-cli",
        action="store_true",
        help="use the built-in workflow even if a credential is available",
    )
    parser.add_argument(
        "--ambient-home",
        action="store_true",
        help="let the CLI read your own agent and MCP configuration (not reproducible)",
    )
    parser.add_argument(
        "--workflow",
        default=None,
        help="a workflow directory from workflows/examples. Its prompt, tool grant, MCP "
        "servers and steering are used, which is the only way to exercise a definition "
        "locally; --prompt and --trust-tools are ignored when this is given",
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        help="files to hand to the batch, one work item per file. Copied into each "
        "item's ./input the way the fleet copies them from the reference bucket",
    )
    parser.add_argument(
        "--prompt",
        default=(
            "Write a one-sentence summary of what a work queue is, and save it to "
            "output/summary.md. Do not write anywhere else."
        ),
        help="must instruct the workflow to write into output/, which is what the "
        "completion check looks for. Ignored when --workflow is given",
    )
    parser.add_argument(
        "--trust-tools",
        default="fs_read,fs_write",
        help="tools the workflow may use without confirmation. Must include a write "
        "tool if the workflow is expected to produce output. Ignored when --workflow "
        "is given",
    )
    parser.add_argument("--keep", action="store_true", help="keep the output directory")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="seconds to spend per item, for exercising interruption and scale-in",
    )
    parser.add_argument(
        "--tables-suffix",
        default=None,
        help="fixed table name suffix, so another process can inspect the pool",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-7s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    ddb = dynamodb_client(args.endpoint)
    try:
        ddb.list_tables()
    except Exception:
        print(
            f"DynamoDB Local is not reachable at {args.endpoint}.\n"
            "Start it with:\n"
            "  docker run -d --rm --name ddb-local -p 8000:8000 \\\n"
            "      amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -inMemory -sharedDb",
            file=sys.stderr,
        )
        return 2

    definition = None
    if args.workflow:
        try:
            definition = WorkflowDefinition.load(args.workflow)
        except InvalidWorkflow as exc:
            print(f"cannot use that workflow: {exc}", file=sys.stderr)
            return 2

    # From the definition when there is one, so a local run and a task are driven by the
    # same file rather than by a command line that happens to resemble it.
    prompt = definition.prompt if definition else args.prompt
    trust_tools = definition.trust_tools_argument if definition else args.trust_tools

    secrets, using_real_cli, credential_source = choose_secrets(args)
    if using_real_cli and not kiro_cli_available():
        credential_source += ", but kiro-cli is not on PATH"
        using_real_cli = False

    def real_cli(**kwargs):
        # Isolated HOME, so a developer's own agent and MCP configuration cannot
        # decide whether this run succeeds. Without it a local run reports a startup
        # failure for MCP servers that have nothing to do with the sample.
        #
        # The definition goes with it: an isolated HOME that the workflow's own MCP and
        # steering were not seeded into is reproducible without being comparable to a
        # task, which is the only reason to run locally.
        return run_workflow(
            isolate_home=not args.ambient_home,
            trust_tools=trust_tools,
            definition=definition,
            expects_stdout=definition.expects_stdout if definition else True,
            **kwargs,
        )

    workflow = real_cli if using_real_cli else make_builtin_workflow(args.delay)

    suffix = args.tables_suffix or uuid.uuid4().hex[:8]
    subscriptions_table, leases_table = create_tables(ddb, suffix)
    os.environ[schema.ENV_SUBSCRIPTIONS_TABLE] = subscriptions_table
    os.environ[schema.ENV_LEASES_TABLE] = leases_table

    registry = Registry(ddb, subscriptions_table)
    register_pool(
        registry,
        size=args.subscriptions,
        concurrency=args.concurrency,
        secret_ref="kiro/subscription/local",
    )

    # One item per input file when inputs are given: that is the shape fan-out work
    # actually has, many independent inputs against one unchanging instruction. Without
    # inputs the prompt alone is the item, which exercises the loop but not the staging.
    inputs_per_item: list[tuple[str, ...]] = []
    if args.input_dir:
        reference = Path(args.input_dir)
        if not reference.is_dir():
            print(f"--input-dir {reference} is not a directory", file=sys.stderr)
            return 2
        files = sorted(p.name for p in reference.iterdir() if p.is_file())
        if not files:
            print(f"--input-dir {reference} holds no files", file=sys.stderr)
            return 2
        inputs_per_item = [(name,) for name in files[: args.items]]

    # The real number of items, which is not `--items` when there were fewer input files
    # than asked for. Using the requested count in the verification below would report a
    # complete batch as incomplete.
    item_count = len(inputs_per_item) if inputs_per_item else args.items

    queue = LocalQueue()
    if inputs_per_item:
        for index, inputs in enumerate(inputs_per_item):
            queue.seed(f"item-{index:03d}", prompt, inputs=inputs)
    else:
        for index in range(args.items):
            queue.seed(f"item-{index:03d}", f"{prompt} (item {index})")

    workdir = Path(tempfile.mkdtemp(prefix="kiro-local-batch-"))
    results_root = Path(args.results_dir) if args.results_dir else workdir / "results"
    store = LocalResultStore(
        root=results_root,
        reference=Path(args.input_dir) if args.input_dir else None,
    )

    capacity = args.subscriptions * args.concurrency
    print(
        f"\npool: {args.subscriptions} subscriptions x {args.concurrency} = {capacity} capacity"
        f"\nworkers: {args.workers}   items: {item_count}"
        f"\nworkflow: {'real kiro-cli' if using_real_cli else 'BUILT-IN STAND-IN'}"
        f"\n  reason: {credential_source}"
        f"\nworkdir: {workdir}\n",
        file=sys.stderr,
    )
    if not using_real_cli:
        print(
            "NOTE: this run will not call Kiro CLI. It exercises allocation, the\n"
            "      worker loop, and result verification, but says nothing about the\n"
            "      CLI itself. Set KIRO_API_KEY in this process to change that.\n",
            file=sys.stderr,
        )

    reports: list = []
    saturated = 0
    lock = threading.Lock()

    # Asked to stop, rather than killed. A container gets SIGTERM before SIGKILL, and as
    # PID 1 a process does not get the default disposition for it: unless a handler is
    # installed the signal is ignored outright, the container is killed after the grace
    # period, and the subscription stays held until its lease expires. Measured: without
    # this, `docker stop` left capacity held and the container exited 137.
    #
    # Workers run on threads here, and signal handlers can only be installed on the main
    # thread, so the handler sets an event the workers watch instead of releasing directly.
    stop = threading.Event()

    def request_stop(signum, _frame):
        if stop.is_set():
            return
        print(
            f"\nsignal {signum} received; finishing current items and returning capacity",
            file=sys.stderr,
        )
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    def run_one(index: int) -> None:
        nonlocal saturated
        allocator = Allocator(
            registry=Registry(dynamodb_client(args.endpoint), subscriptions_table),
            leases=Leases(dynamodb_client(args.endpoint), leases_table),
            secrets=secrets,
            metrics=Metrics(),
            config=ClaimConfig(
                lease_lifetime=timedelta(seconds=120),
                max_attempts=3,
                base_backoff=timedelta(milliseconds=200),
                max_backoff=timedelta(seconds=2),
            ),
        )
        try:
            with hold_subscription(
                allocator,
                dynamodb_client(args.endpoint),
                holder_id=f"local-worker-{index}",
                leases_table=leases_table,
                install_signal_handler=False,
            ) as session:
                report = Worker(
                    session=session,
                    queue=queue,
                    results=store,
                    registry=Registry(dynamodb_client(args.endpoint), subscriptions_table),
                    metrics=Metrics(),
                    workspace=workdir / f"workspace-{index}",
                    retry_delay_seconds=1.0,
                    run=workflow,
                ).drain(stop=stop)
            with lock:
                reports.append(report)
        except PoolSaturated:
            # Expected when workers outnumber capacity. A clean exit, not a failure.
            with lock:
                saturated += 1

    threads = [threading.Thread(target=run_one, args=(i,)) for i in range(args.workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    completed = sum(r.completed for r in reports)
    failed = sum(r.failed for r in reports)
    retried = sum(r.retried for r in reports)
    stored = sum(1 for index in range(item_count) if store.results_for(f"item-{index:03d}"))

    print("\n--- result ---", file=sys.stderr)
    print(f"workers that got capacity : {len(reports)}", file=sys.stderr)
    print(f"workers turned away       : {saturated}", file=sys.stderr)
    print(f"items completed           : {completed}", file=sys.stderr)
    print(f"items with stored results : {stored} / {item_count}", file=sys.stderr)
    print(f"failed / retried          : {failed} / {retried}", file=sys.stderr)
    print(f"queue depth remaining     : {queue.depth}", file=sys.stderr)

    for record in registry.describe_all():
        print(
            f"  {record.subscription_id}: held {record.active_leases}"
            f"/{record.max_concurrency}  state {record.state}",
            file=sys.stderr,
        )

    leaked = [r for r in registry.describe_all() if r.active_leases != 0]
    remaining_leases = ddb.scan(TableName=leases_table, Select="COUNT")["Count"]

    # An interrupted run is not a failed one. It is asked to stop, finishes the item it is
    # on, and hands the rest back — so the queue is legitimately not empty and the only
    # thing that must still hold is that no capacity was stranded.
    interrupted = stop.is_set()
    capacity_returned = not leaked and remaining_leases == 0

    ok = capacity_returned and (
        interrupted or (stored == item_count and queue.depth == 0)
    )
    print(
        f"\nleases left behind        : {remaining_leases}"
        f"\ncapacity fully returned   : {'yes' if not leaked else 'NO'}"
        f"\nworkflow actually run     : {'real kiro-cli' if using_real_cli else 'built-in stand-in'}"
        f"{chr(10) + 'interrupted               : yes, stopped on a signal' if interrupted else ''}"
        f"\n\n{'PASS' if ok else 'FAIL'}: "
        f"{_verdict_text(ok, interrupted)}"
        f"{'' if using_real_cli else '  (with the stand-in workflow, not the CLI)'}",
        file=sys.stderr,
    )

    if args.keep:
        print(f"\noutput kept at {workdir}", file=sys.stderr)
    else:
        shutil.rmtree(workdir, ignore_errors=True)

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
