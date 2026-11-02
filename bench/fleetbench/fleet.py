"""M1 on ECS: one stream per Fargate task, so N streams means N hosts.

The local ramp holds N streams open from one laptop, and at high N the laptop itself
became the bottleneck: N CLI processes on one machine start, authenticate, and stream
slower than the service would serve them, so the service may never see N connections
at once. Here every stream runs in its own task, and every task waits for one shared
start time before it sends anything. The service then sees N connections from N hosts,
the way the fleet uses a subscription.

Two halves, one module:

    # inside a task (the bench image's entry point)
    python3 -m fleetbench.fleet stream --profile heavy --level 40 --start-epoch ... --run-id ...

    # on a laptop, with AWS credentials for the account the BenchStack is in
    uv run --with boto3 python -m fleetbench.fleet launch --stack KiroFleetBench --max-concurrency 100

A task prints exactly one JSON record to its log. The launcher collects the records
for a run and turns them into the same LevelResult the local ramp produces, so the
stopping rules and the report are shared. It also reports the overlap the records
prove -- how many streams were really open at the same moment -- because a curve
labelled "40 concurrent" is only as good as the evidence that 40 were concurrent.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from fleetbench import profiles, report
from fleetbench.ramp import (
    LevelResult,
    RampResult,
    StreamOutcome,
    StreamResult,
    default_schedule,
    run_ramp,
)

RECORD_KIND = "fleetbench.m1.stream"
# The M1e workload's models (see fleetbench.asis); kept here so argparse does
# not import the workload module.
DEFAULT_ASIS_ORCHESTRATOR_MODEL = "claude-sonnet-4.6"
DEFAULT_ASIS_GENERATOR_MODEL = "claude-opus-4.8"

_LATE_TOLERANCE_SECONDS = 2.0
"""A stream that sent more than this after the shared start time counts as late.

Late streams still count toward their level; the overlap figures say how much they
cost, so a level whose tasks straggled is visible rather than silently diluted.
"""


# --- the task side -----------------------------------------------------------


def run_stream(args: argparse.Namespace) -> int:
    """Wait for the shared start, run one stream, print one record. Always exit 0.

    The outcome is data, so it goes in the record rather than the exit code: a task
    that exits non-zero on a throttle would look like an infrastructure failure in
    ECS, and the launcher would have two places to look for one answer.
    """
    from fleetbench.adapter import make_cli_run_once

    profile = profiles.resolve([args.profile])[0]
    record: dict[str, object] = {
        "kind": RECORD_KIND,
        "run_id": args.run_id,
        "profile": profile.name,
        "level": args.level,
        "start_epoch": args.start_epoch,
        "task": _task_id(),
    }

    api_key = os.environ.get("KIRO_API_KEY", "")
    if not api_key:
        record.update(outcome=StreamOutcome.ERROR.value, detail="no KIRO_API_KEY in task")
        print(json.dumps(record), flush=True)
        return 0

    try:
        run_once = make_cli_run_once(
            profile,
            api_key,
            Path(args.workspace_root),
            executable=args.executable,
            timeout_seconds=args.timeout,
            # The task's HOME is already clean: nothing but the image is in it.
            isolate_home=False,
        )
    except RuntimeError as error:
        record.update(outcome=StreamOutcome.ERROR.value, detail=str(error))
        print(json.dumps(record), flush=True)
        return 0

    record["ready_at"] = time.time()
    delay = args.start_epoch - time.time()
    if delay > 0:
        time.sleep(delay)

    sent_at = time.time()
    result = run_once()
    ended_at = time.time()
    record.update(
        sent_at=sent_at,
        ended_at=ended_at,
        latency_ms=result.latency_ms,
        outcome=result.outcome.value,
        detail=result.detail,
    )
    print(json.dumps(record), flush=True)
    return 0


def run_fanout_stream(args: argparse.Namespace) -> int:
    """M1b inside a task: one orchestrator run with subagents, one record. Always exit 0.

    Calls run_workflow directly rather than through make_cli_run_once, because M1b needs
    what M1 throws away: the exit code, the outputs, and the stderr, where a throttle the
    CLI retried and recovered from is the only trace it leaves.
    """
    from runner.workflow import Verdict, run_workflow

    from fleetbench import fanout

    record: dict[str, object] = {
        "kind": RECORD_KIND,
        "run_id": args.run_id,
        "profile": args.profile,
        "workload": "fanout",
        "subagents": args.subagents,
        "files_per_subagent": args.files_per_subagent,
        "note_words": args.note_words,
        "level": args.level,
        "start_epoch": args.start_epoch,
        "task": _task_id(),
    }
    api_key = os.environ.get("KIRO_API_KEY", "")
    if not api_key:
        record.update(outcome=StreamOutcome.ERROR.value, detail="no KIRO_API_KEY in task")
        print(json.dumps(record), flush=True)
        return 0

    workspace = Path(args.workspace_root) / f"run-{uuid.uuid4().hex}"
    workspace.mkdir(parents=True, exist_ok=True)
    fanout.write_corpus(
        workspace,
        args.subagents,
        args.files_per_subagent,
        args.note_words,
        subagent_agent=args.subagent_agent or None,
    )

    record["ready_at"] = time.time()
    delay = args.start_epoch - time.time()
    if delay > 0:
        time.sleep(delay)

    sent_at = time.time()
    result = run_workflow(
        prompt=fanout.prompt(
            args.subagents, args.files_per_subagent, args.subagent_agent or None
        ),
        api_key=api_key,
        workspace=workspace,
        trust_tools=args.trust_tools or fanout.TRUST_TOOLS,
        timeout_seconds=args.timeout,
        executable=args.executable,
        expects_stdout=True,
        # Subagents write where the orchestrator tells them; M1b counts requests, it does
        # not police paths.
        enforce_output_path=False,
        isolate_home=False,
    )
    ended_at = time.time()
    # 2.30 prints tool lines on stderr; 2.21.4 prints them on stdout. Read both.
    tool_log = f"{result.stderr}\n{result.stdout}"
    findings = fanout.analyse_stderr(tool_log)
    coverage = fanout.read_coverage(
        fanout.read_lines(tool_log), fanout.note_lines(args.note_words)
    )
    final_total_ok = fanout.final_total_ok(
        workspace, args.subagents, args.files_per_subagent, args.note_words
    )

    if result.verdict is Verdict.CREDENTIAL_REJECTED:
        outcome = StreamOutcome.REJECTED
    elif findings.throttle_hits:
        outcome = StreamOutcome.THROTTLED
    elif result.exit_code == 0:
        outcome = StreamOutcome.OK
    else:
        outcome = StreamOutcome.ERROR
    record.update(
        sent_at=sent_at,
        ended_at=ended_at,
        latency_ms=(ended_at - sent_at) * 1000.0,
        outcome=outcome.value,
        detail=str(result.verdict),
        exit_code=result.exit_code,
        outputs=len(result.outputs),
        throttle_hits=findings.throttle_hits,
        approval_blocks=findings.approval_blocks,
        tool_uses=findings.tool_uses,
        # Proof the notes were read whole: M1c's first run read lines 1-5 of each note.
        note_lines=fanout.note_lines(args.note_words),
        read_lines=fanout.read_lines(tool_log),
        note_reads=coverage[0],
        full_note_reads=coverage[1],
        # The facts sit on each note's last line, so a correct total proves full reads
        # whatever the CLI version prints about its reads.
        final_total_ok=final_total_ok,
        throttle_lines=list(findings.throttle_lines),
        credits=str(result.credits) if result.credits is not None else None,
        stderr_bytes=len(result.stderr),
        # Masked tails, so a run that did not delegate says why without carrying its output.
        stderr_tail=fanout.masked_tail(result.stderr),
        stdout_tail=fanout.masked_tail(result.stdout),
        output_files=sorted(str(p.relative_to(workspace)) for p in result.outputs)[:20],
    )
    print(json.dumps(record), flush=True)
    return 0


def run_asis_stream(args: argparse.Namespace) -> int:
    """M1e inside a task: one as-is-shaped spec workflow run, one record. Always exit 0."""
    from runner.workflow import Verdict, run_workflow

    from fleetbench import asis, fanout

    record: dict[str, object] = {
        "kind": RECORD_KIND,
        "run_id": args.run_id,
        "profile": args.profile,
        "workload": "asis",
        "sub_functions": args.sub_functions,
        "lines_per_function": args.lines_per_function,
        "parallel": args.parallel,
        "orchestrator_model": args.orchestrator_model,
        "generator_model": args.generator_model,
        "level": args.level,
        "start_epoch": args.start_epoch,
        "task": _task_id(),
    }
    api_key = os.environ.get("KIRO_API_KEY", "")
    if not api_key:
        record.update(outcome=StreamOutcome.ERROR.value, detail="no KIRO_API_KEY in task")
        print(json.dumps(record), flush=True)
        return 0

    workspace = Path(args.workspace_root) / f"run-{uuid.uuid4().hex}"
    workspace.mkdir(parents=True, exist_ok=True)
    asis.write_workspace(
        workspace,
        args.sub_functions,
        args.lines_per_function,
        args.orchestrator_model,
        args.generator_model,
    )

    record["ready_at"] = time.time()
    delay = args.start_epoch - time.time()
    if delay > 0:
        time.sleep(delay)

    sent_at = time.time()
    result = run_workflow(
        prompt=asis.prompt(args.sub_functions, args.parallel),
        api_key=api_key,
        workspace=workspace,
        agent=asis.ORCHESTRATOR,
        trust_tools=args.trust_tools or ",".join(asis.TOOLS),
        timeout_seconds=args.timeout,
        executable=args.executable,
        expects_stdout=True,
        enforce_output_path=False,
        isolate_home=False,
    )
    ended_at = time.time()
    tool_log = f"{result.stderr}\n{result.stdout}"
    findings = fanout.analyse_stderr(tool_log)
    subagent_lines, python_runs = asis.count_calls(tool_log)

    if result.verdict is Verdict.CREDENTIAL_REJECTED:
        outcome = StreamOutcome.REJECTED
    elif findings.throttle_hits:
        outcome = StreamOutcome.THROTTLED
    elif result.exit_code == 0:
        outcome = StreamOutcome.OK
    else:
        outcome = StreamOutcome.ERROR
    record.update(
        sent_at=sent_at,
        ended_at=ended_at,
        latency_ms=(ended_at - sent_at) * 1000.0,
        outcome=outcome.value,
        detail=str(result.verdict),
        exit_code=result.exit_code,
        outputs=asis.outputs_present(workspace, args.sub_functions),
        expected_outputs=len(asis.expected_outputs(args.sub_functions)),
        final_total_ok=asis.final_total_ok(workspace, args.sub_functions),
        throttle_hits=findings.throttle_hits,
        approval_blocks=findings.approval_blocks,
        tool_uses=findings.tool_uses,
        subagent_lines=subagent_lines,
        subagent_invocations=asis.subagent_invocations(tool_log),
        delegation_denied=asis.delegation_denied(tool_log),
        # The two log shapes seen on 2.21.4: 'Invoking subagent:' lines (subagent text kept out
        # of stdout) or none (subagent text streamed into stdout). Kept to test the engine idea.
        log_shape="invoking-lines" if asis.subagent_invocations(tool_log) else "no-invoking-lines",
        asis_shaped=sum(asis.subagent_invocations(tool_log).values())
        >= asis.expected_delegations(args.sub_functions),
        denial_context=asis.denial_context(tool_log),
        python_runs=python_runs,
        throttle_lines=list(findings.throttle_lines),
        credits=str(result.credits) if result.credits is not None else None,
        stderr_bytes=len(result.stderr),
        stderr_tail=fanout.masked_tail(result.stderr),
        stdout_tail=fanout.masked_tail(result.stdout),
        output_files=sorted(
            str(p.relative_to(workspace)) for p in (workspace / "output").glob("*") if p.is_file()
        )[:40],
    )
    print(json.dumps(record), flush=True)
    return 0


def _task_id() -> str:
    """The task's ARN tail when running on ECS, for matching a record to a task."""
    uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
    # Set by the ECS agent to its link-local http endpoint. Anything else is not that
    # endpoint, so it is not fetched.
    if not uri or not uri.startswith("http://"):
        return ""
    try:
        from urllib.request import urlopen

        # nosec B310: the URL is the ECS task metadata endpoint, scheme checked above.
        with urlopen(f"{uri}/task", timeout=2) as response:  # nosec B310
            return str(json.load(response).get("TaskARN", "")).rsplit("/", 1)[-1]
    except Exception:  # noqa: BLE001 - an id is a convenience, never a reason to fail
        return ""


# --- pure: records to results ------------------------------------------------


@dataclass(frozen=True)
class Overlap:
    """What the records prove about concurrency at one level."""

    level: int
    recorded: int
    late: int
    start_spread_s: float
    peak_open: int
    all_open_s: float
    """How long every recorded stream was open at once. Zero means they never all were."""


def level_from_records(level: int, records: Sequence[dict]) -> LevelResult:
    """A LevelResult from one level's task records, padding missing tasks as errors.

    A task that never logged a record still occupied a launch, and leaving it out would
    report a level of 40 as if it had been a clean 37.
    """
    results = [
        StreamResult(
            StreamOutcome(r.get("outcome", StreamOutcome.ERROR.value)),
            float(r.get("latency_ms") or 0.0),
            str(r.get("detail", "")),
        )
        for r in records[:level]
    ]
    results += [StreamResult(StreamOutcome.ERROR, 0.0, "no record from task")] * (
        level - len(results)
    )
    return LevelResult(level, tuple(results))


def overlap_of(level: int, records: Iterable[dict]) -> Overlap:
    """Peak simultaneous open streams, and how long all of them were open together."""
    timed = [
        (float(r["sent_at"]), float(r["ended_at"]), float(r.get("start_epoch", 0)))
        for r in records
        if r.get("sent_at") is not None and r.get("ended_at") is not None
    ]
    if not timed:
        return Overlap(level, 0, 0, 0.0, 0, 0.0)

    events = sorted([(s, 1) for s, _, _ in timed] + [(e, -1) for _, e, _ in timed])
    open_now = peak = 0
    for _, delta in events:
        open_now += delta
        peak = max(peak, open_now)

    starts = [s for s, _, _ in timed]
    ends = [e for _, e, _ in timed]
    late = sum(1 for s, _, epoch in timed if s - epoch > _LATE_TOLERANCE_SECONDS)
    return Overlap(
        level=level,
        recorded=len(timed),
        late=late,
        start_spread_s=max(starts) - min(starts),
        peak_open=peak,
        all_open_s=max(0.0, min(ends) - max(starts)),
    )


def overlap_markdown(by_profile: dict[str, list[Overlap]]) -> str:
    lines = [
        "# M1 on ECS -- proven overlap",
        "",
        (
            "One stream per Fargate task. `peak open` is the most streams the records show "
            "open at the same instant; `all open` is how long every stream at that level was "
            "open together. A level is only as concurrent as these say."
        ),
        "",
    ]
    for profile, overlaps in by_profile.items():
        lines += [
            f"## {profile}",
            "",
            "| level | records | late | start spread (s) | peak open | all open (s) |",
            "|---:|---:|---:|---:|---:|---:|",
        ]
        for o in overlaps:
            lines.append(
                f"| {o.level} | {o.recorded} | {o.late} | {o.start_spread_s:.1f} "
                f"| {o.peak_open} | {o.all_open_s:.0f} |"
            )
        lines.append("")
    return "\n".join(lines)


# --- the launcher side -------------------------------------------------------


class Launcher:
    """Runs one level as N tasks on the BenchStack and collects their records."""

    def __init__(self, args: argparse.Namespace, profile_name: str) -> None:
        self.args = args
        self.profile_name = profile_name
        self._connect()
        outputs = {
            o["OutputKey"]: o["OutputValue"]
            for o in self._aws(
                lambda: self.cfn.describe_stacks(StackName=args.stack)["Stacks"][0]
            ).get("Outputs", [])
        }
        self.cluster = outputs["ClusterName"]
        self.task_definition = outputs["TaskDefinitionArn"]
        self.log_group = outputs["LogGroupName"]
        self.subnets = outputs["SubnetIds"].split(",")
        self.security_group = outputs["SecurityGroupId"]
        self.overlaps: list[Overlap] = []
        self.level_records: list[tuple[int, list[dict]]] = []

    def _connect(self) -> None:
        import boto3  # lazy: the pure core and its tests never need it
        import botocore.session

        core = botocore.session.Session()
        if self.args.aws_config_file:
            core.set_config_variable("config_file", str(self.args.aws_config_file))
        if self.args.aws_profile:
            core.set_config_variable("profile", self.args.aws_profile)
        session = boto3.Session(botocore_session=core)
        self.ecs = session.client("ecs")
        self.logs = session.client("logs")
        self.cfn = session.client("cloudformation")

    def _aws(self, call):  # noqa: ANN001, ANN202 - a thin retry around any client call
        """Retry a call through expired credentials and dropped networks.

        Both earlier ramps died in the launcher, not on ECS: once on an expired token and
        once on a DNS failure, each time leaving finished tasks with nobody collecting
        them. The tasks do not care how long the launcher waits, so waiting -- and
        reconnecting, which picks up credentials renewed in the meantime -- is safe.
        """
        from botocore.exceptions import BotoCoreError, ClientError

        transient = ("ExpiredToken", "RequestExpired", "UnrecognizedClient", "Throttling")
        for attempt in range(1, 61):
            try:
                return call()
            except ClientError as error:
                code = error.response.get("Error", {}).get("Code", "")
                if not any(t in code for t in transient):
                    raise
                reason = code
            except BotoCoreError as error:  # NoCredentials, EndpointConnectionError, ...
                reason = type(error).__name__
            print(f"    AWS call failed ({reason}); retry {attempt}/60 in 30s", flush=True)
            time.sleep(30)
            self._connect()
        raise RuntimeError("AWS calls kept failing for 30 minutes; giving up")

    def _command(self, level: int, start_epoch: float, run_id: str) -> list[str]:
        command = [
            "--profile", self.profile_name,
            "--level", str(level),
            "--start-epoch", f"{start_epoch:.3f}",
            "--run-id", run_id,
            "--timeout", str(self.args.timeout),
        ]
        if self.args.workload == "fanout":
            command += [
                "--workload", "fanout",
                "--subagents", str(self.args.subagents),
                "--files-per-subagent", str(self.args.files_per_subagent),
                "--note-words", str(self.args.note_words),
            ]
            if self.args.trust_tools:
                command.append(f"--trust-tools={self.args.trust_tools}")
            if self.args.subagent_agent:
                command += ["--subagent-agent", self.args.subagent_agent]
        elif self.args.workload == "asis":
            command += [
                "--workload", "asis",
                "--sub-functions", str(self.args.sub_functions),
                "--lines-per-function", str(self.args.lines_per_function),
                "--parallel", str(self.args.parallel),
                "--orchestrator-model", self.args.orchestrator_model,
                "--generator-model", self.args.generator_model,
            ]
            if self.args.trust_tools:
                command.append(f"--trust-tools={self.args.trust_tools}")
        return command

    def __call__(self, level: int) -> LevelResult:
        run_id = uuid.uuid4().hex
        start_epoch = time.time() + self.args.lead
        command = self._command(level, start_epoch, run_id)
        task_arns, failures = self._launch(level, command)
        print(
            f"  [{self.profile_name}] {level:>3}: launched {len(task_arns)} tasks "
            f"({failures} failed to launch), start in {self.args.lead}s, run {run_id[:8]}",
            flush=True,
        )
        self._wait_stopped(task_arns, deadline=start_epoch + self.args.timeout + 300)
        records = self._records(run_id, expected=len(task_arns), since=start_epoch - self.args.lead)
        self.overlaps.append(overlap_of(level, records))
        self.level_records.append((level, records))
        self._save(level, run_id, records)
        return level_from_records(level, records)

    def _save(self, level: int, run_id: str, records: list[dict]) -> None:
        """Every record, as soon as its level ends, so a launcher that dies later loses nothing."""
        self.args.out.mkdir(parents=True, exist_ok=True)
        path = self.args.out / f"records-{self.profile_name}.jsonl"
        with path.open("a") as handle:
            for record in records:
                handle.write(json.dumps({**record, "_level": level, "_run": run_id}) + "\n")

    def _launch(self, level: int, command: list[str]) -> tuple[list[str], int]:
        arns: list[str] = []
        failures = 0
        remaining = level
        while remaining > 0:
            count = min(10, remaining)  # RunTask's per-call ceiling
            response = self._aws(
                lambda count=count: self.ecs.run_task(
                    cluster=self.cluster,
                    taskDefinition=self.task_definition,
                    launchType="FARGATE",
                    count=count,
                    networkConfiguration={
                        "awsvpcConfiguration": {
                            "subnets": self.subnets,
                            "securityGroups": [self.security_group],
                            "assignPublicIp": "ENABLED",
                        }
                    },
                    overrides={"containerOverrides": [{"name": "Stream", "command": command}]},
                )
            )
            arns += [t["taskArn"] for t in response.get("tasks", [])]
            failures += len(response.get("failures", []))
            remaining -= count
        return arns, failures

    def _wait_stopped(self, arns: list[str], *, deadline: float) -> None:
        pending = set(arns)
        while pending and time.time() < deadline:
            time.sleep(15)
            for i in range(0, len(arns), 100):
                batch = [a for a in arns[i : i + 100] if a in pending]
                if not batch:
                    continue
                tasks = self._aws(
                    lambda batch=batch: self.ecs.describe_tasks(cluster=self.cluster, tasks=batch)
                )["tasks"]
                for task in tasks:
                    if task["lastStatus"] == "STOPPED":
                        pending.discard(task["taskArn"])
        if pending:
            print(f"    {len(pending)} tasks still running at the deadline; stopping them")
            for arn in pending:
                self._aws(
                    lambda arn=arn: self.ecs.stop_task(
                        cluster=self.cluster, task=arn, reason="fleetbench deadline"
                    )
                )

    def _records(self, run_id: str, *, expected: int, since: float) -> list[dict]:
        # Logs arrive a few seconds after a task stops; poll briefly rather than once.
        records: dict[str, dict] = {}
        for _ in range(12):
            def collect() -> None:
                paginator = self.logs.get_paginator("filter_log_events")
                for page in paginator.paginate(
                    logGroupName=self.log_group,
                    startTime=int(since * 1000),
                    filterPattern=f'"{run_id}"',
                ):
                    for event in page["events"]:
                        try:
                            record = json.loads(event["message"])
                        except ValueError:
                            continue
                        if record.get("kind") == RECORD_KIND and record.get("run_id") == run_id:
                            records[event["logStreamName"]] = record

            self._aws(collect)
            if len(records) >= expected:
                break
            time.sleep(10)
        return list(records.values())


def launch(args: argparse.Namespace) -> int:
    try:
        if args.workload == "fanout":
            names = [f"fanout-s{args.subagents}"]
        elif args.workload == "asis":
            names = [f"asis-sf{args.sub_functions}-p{args.parallel}"]
        else:
            names = [p.name for p in profiles.resolve(
                [p.strip() for p in args.profiles.split(",") if p.strip()]
            )]
        if args.levels:
            schedule = sorted({int(x) for x in args.levels.split(",") if x.strip()})
        else:
            schedule = default_schedule(args.max_concurrency, start_at=args.start_at)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    title = {"fanout": "M1b fanout", "asis": "M1e as-is shape"}.get(args.workload, "M1")
    print(f"{title} on ECS -- one run per Fargate task, stack {args.stack}")
    print(f"schedule: {schedule}\n", flush=True)

    ramps: list[RampResult] = []
    overlaps: dict[str, list[Overlap]] = {}
    fanout_levels: list = []
    for name in names:
        launcher = Launcher(args, name)

        def on_level(level: LevelResult, name: str = name, launcher: Launcher = launcher) -> None:
            o = launcher.overlaps[-1]
            line = (
                f"  [{name}] {level.concurrency:>3} runs: ok={level.ok} "
                f"throttled={level.throttled} errored={level.errored} rejected={level.rejected} "
                f"| peak open={o.peak_open} all open={o.all_open_s:.0f}s late={o.late}"
            )
            if args.workload in ("fanout", "asis"):
                from fleetbench import fanout

                f = fanout.summarise_level(level.concurrency, launcher.level_records[-1][1])
                fanout_levels.append(f)
                line += (
                    f" | exited0={f.finished} throttle msgs={f.throttle_hits} "
                    f"recovered={f.recovered_runs} approvals={f.approval_blocks} "
                    f"tool uses med={f.median_tool_uses:.0f} rate={f.tool_uses_per_minute:.0f}/min "
                    f"med={f.median_seconds:.0f}s"
                )
            print(line, flush=True)

        print(f"profile: {name}", flush=True)
        ramps.append(
            run_ramp(
                lambda: StreamResult(StreamOutcome.ERROR, 0.0, "unused"),
                schedule,
                profile=name,
                onset_threshold=args.onset_threshold,
                stop_after_onset=not args.continue_after_onset,
                on_level=on_level,
                level_runner=launcher,
            )
        )
        overlaps[name] = launcher.overlaps
        print()

    args.out.mkdir(parents=True, exist_ok=True)
    paths = report.write_artifacts(ramps, args.out)
    overlap_path = args.out / "m1-overlap.md"
    overlap_path.write_text(overlap_markdown(overlaps) + "\n")
    print(report.to_markdown(ramps))
    print(overlap_markdown(overlaps))
    if args.workload in ("fanout", "asis"):
        from fleetbench import fanout

        fanout_path = args.out / ("m1b-fanout.md" if args.workload == "fanout" else "m1e-asis.md")
        fanout_path.write_text(fanout.fanout_markdown(names[0], args.subagents, fanout_levels))
        print(fanout_path.read_text())
        print(f"wrote {fanout_path}")
    print(f"wrote {paths['json']}\nwrote {paths['markdown']}\nwrote {overlap_path}")
    return 0


# --- command line ------------------------------------------------------------


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="fleetbench.fleet", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    stream = sub.add_parser("stream", help="run one stream inside a task")
    stream.add_argument("--profile", required=True)
    stream.add_argument("--level", type=int, required=True)
    stream.add_argument("--start-epoch", type=float, required=True)
    stream.add_argument("--run-id", required=True)
    stream.add_argument("--timeout", type=int, default=600)
    stream.add_argument("--executable", default="kiro-cli")
    stream.add_argument("--workspace-root", default="/workspace")
    stream.add_argument("--workload", choices=["m1", "fanout", "asis"], default="m1")
    stream.add_argument("--sub-functions", type=int, default=4)
    stream.add_argument("--lines-per-function", type=int, default=200)
    stream.add_argument("--parallel", type=int, default=1)
    stream.add_argument("--orchestrator-model", default=DEFAULT_ASIS_ORCHESTRATOR_MODEL)
    stream.add_argument("--generator-model", default=DEFAULT_ASIS_GENERATOR_MODEL)
    stream.add_argument("--subagents", type=int, default=2)
    stream.add_argument("--files-per-subagent", type=int, default=6)
    stream.add_argument("--note-words", type=int, default=120)
    stream.add_argument("--trust-tools", default="")
    stream.add_argument("--subagent-agent", default="")

    launcher = sub.add_parser("launch", help="ramp concurrency as tasks on the BenchStack")
    launcher.add_argument("--stack", default="KiroFleetBench")
    launcher.add_argument(
        "--workload",
        choices=["m1", "fanout", "asis"],
        default="m1",
        help="m1: one model request per stream; fanout (M1b): orchestrator + subagents; "
        "asis (M1e): the as-is spec workflow's shape on synthetic input",
    )
    launcher.add_argument("--sub-functions", type=int, default=4, help="asis: sub-functions")
    launcher.add_argument(
        "--lines-per-function", type=int, default=200, help="asis: C lines per sub-function"
    )
    launcher.add_argument(
        "--parallel", type=int, default=1, help="asis: subagents per batch (as-is max_parallel)"
    )
    launcher.add_argument(
        "--orchestrator-model",
        default=DEFAULT_ASIS_ORCHESTRATOR_MODEL,
        help="asis: model for orchestrator, collector and reviewer; empty = CLI default",
    )
    launcher.add_argument(
        "--generator-model",
        default=DEFAULT_ASIS_GENERATOR_MODEL,
        help="asis: model for the spec generator; empty = CLI default",
    )
    launcher.add_argument("--subagents", type=int, default=2, help="fanout: subagents per run")
    launcher.add_argument(
        "--files-per-subagent", type=int, default=6, help="fanout: files each subagent reads"
    )
    launcher.add_argument(
        "--note-words",
        type=int,
        default=120,
        help="fanout: words per note; raise it for the token axis (input tokens per request)",
    )
    launcher.add_argument("--levels", help="explicit comma-separated levels, e.g. 1,2,4,6,8")
    launcher.add_argument(
        "--trust-tools", default="", help="fanout: override the trusted tool set"
    )
    launcher.add_argument(
        "--subagent-agent",
        default="",
        help="fanout: seed .kiro/agents/<name>.json (fs_read/fs_write pre-approved) and run "
        "subagents as it; needed on Kiro CLI 2.21.4, whose built-in agents stop on approval",
    )
    launcher.add_argument(
        "--continue-after-onset",
        action="store_true",
        help="keep ramping after the first throttled level, to see the curve past onset",
    )
    launcher.add_argument("--max-concurrency", type=int, default=100)
    launcher.add_argument("--start-at", type=int, default=10)
    launcher.add_argument("--profiles", default="heavy,light")
    launcher.add_argument("--timeout", type=int, default=600)
    launcher.add_argument(
        "--lead",
        type=int,
        default=240,
        help="seconds between launching a level and its shared start time; must cover "
        "Fargate provisioning and image pull for the slowest task (default: 240)",
    )
    launcher.add_argument("--onset-threshold", type=float, default=0.0)
    launcher.add_argument("--aws-profile", help="named profile for the launcher's AWS calls")
    launcher.add_argument(
        "--aws-config-file",
        type=Path,
        help="AWS config file to read profiles from, when it is not the SDK's default",
    )
    launcher.add_argument("--out", type=Path, default=Path("results/ecs"))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.command == "stream":
        if args.workload == "fanout":
            return run_fanout_stream(args)
        if args.workload == "asis":
            return run_asis_stream(args)
        return run_stream(args)
    return launch(args)


if __name__ == "__main__":
    raise SystemExit(main())
