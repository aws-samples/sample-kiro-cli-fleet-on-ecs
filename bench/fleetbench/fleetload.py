"""M2~M5 load on the deployed fleet: seed bench-fanout items and watch the fleet run them.

M1 ran its own tasks against one key. M2~M5 measure the pattern itself, so the load goes
through the same path a user's batch takes -- manifest, seeding function, queue, worker,
claim on the registry -- and this module only feeds that path and watches it:

    # once per account: the shared, read-only corpus every item reads
    uv run --with boto3 python -m fleetbench.fleetload corpus

    # one row per subscription, after its key is in Secrets Manager (no key passes here)
    uv run --with boto3 python -m fleetbench.fleetload register --sub-id sub-01 --secret-ref <arn>

    # M2: choose the pool, then seed and watch one level
    uv run --with boto3 python -m fleetbench.fleetload pool --enable sub-01,sub-02
    uv run --with boto3 python -m fleetbench.fleetload run --tag m2-r1-k2 --items 30 --out results/m2

    # M4: the same, stopping a fifth of the running tasks two minutes in
    uv run --with boto3 python -m fleetbench.fleetload run --tag m4 --items 50 \\
        --chaos-fraction 0.2 --chaos-after 120 --out results/m4

`run` writes `<out>/<tag>-timeline.jsonl` (one fleet sample every interval) and
`<out>/<tag>-summary.json`. M3 and M5 are read from those files and from the results
bucket; they need no load of their own beyond M2's levels and one burst.

Everything above the AWS boundary is pure and tested; the AWS side is a thin class.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from fleetbench import fanout

SUBAGENTS = 2
FILES_PER_SUBAGENT = 15
NOTE_WORDS = 120
CORPUS_PREFIX = "bench/fanout/notes/"
"""Reference-bucket prefix holding the corpus. Every item lists it as its one input."""

INPUT_NOTES_DIR = "input/" + CORPUS_PREFIX.rstrip("/")
"""Where the worker puts the corpus in an item's workspace.

The worker downloads a multi-object prefix to `input/<prefix>/<relative path>`
(`runner.aws.S3Results.fetch_inputs`), so the notes land under the prefix itself.
"""

MANIFEST_PREFIX = "bench/manifests/"
SEEDING_SUFFIX = "-seeding"
DEFAULT_MAX_CONCURRENCY = 5
"""Per-key cap: the M1 knee, T / r1, for this workload (about 4.5-5.5)."""
DEFAULT_CREDIT_BUDGET = 800
"""Month-to-date ceiling, not a per-benchmark allowance.

`creditsUsed` is the subscription's month-to-date total as `/usage` reports it, other
clients included, so a budget of what the benchmark itself will spend would refuse a key
that was used earlier in the month. 800 leaves 200 of a 1,000-credit plan in reserve.
"""


# --- pure -------------------------------------------------------------------


def corpus_objects() -> dict[str, str]:
    """Reference-bucket key -> note text, the same notes M1 fanout used."""
    return {
        CORPUS_PREFIX + name.removeprefix("notes/"): text
        for name, text in fanout.corpus_files(SUBAGENTS, FILES_PER_SUBAGENT, NOTE_WORDS).items()
    }


def item_prompt() -> str:
    return fanout.prompt(SUBAGENTS, FILES_PER_SUBAGENT, notes_dir=INPUT_NOTES_DIR)


def expected_total() -> int:
    return fanout.expected_total(SUBAGENTS, FILES_PER_SUBAGENT)


def manifest(tag: str, items: int) -> dict:
    """An explicit-items manifest; identifiers carry the tag so batches never collide.

    Results are keyed by item identifier, so two levels seeded with the same identifiers
    would overwrite each other's results. The tag makes every level its own set.
    """
    if items < 1:
        raise ValueError("a batch needs at least one item")
    if not tag or any(c in tag for c in "/ "):
        raise ValueError(f"tag {tag!r} must be non-empty and contain no '/' or space")
    prompt = item_prompt()
    return {
        "items": [
            {"itemId": f"{tag}-{i:04d}", "prompt": prompt, "inputs": [CORPUS_PREFIX]}
            for i in range(1, items + 1)
        ]
    }


def registry_item(
    sub_id: str,
    secret_ref: str,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    credit_budget: int = DEFAULT_CREDIT_BUDGET,
) -> dict:
    """A DynamoDB item for the subscriptions table (field names: leaseclient.schema).

    Holds a reference to the secret, never the key. `creditsUsed` starts at 0 and the
    worker raises it to the month-to-date figure on its first `/usage` reading.
    """
    if max_concurrency < 1 or credit_budget < 1:
        raise ValueError("maxConcurrency and creditBudget must be positive")
    return {
        "subscriptionId": {"S": sub_id},
        "state": {"S": "ENABLED"},
        "secretRef": {"S": secret_ref},
        "maxConcurrency": {"N": str(max_concurrency)},
        "activeLeases": {"N": "0"},
        "creditBudget": {"N": str(credit_budget)},
        "creditsUsed": {"N": "0"},
    }


def chaos_targets(running: list[str], fraction: float, rng: random.Random) -> list[str]:
    """Which running tasks M4 stops: round(fraction x running), at least one if any run."""
    if not 0 < fraction <= 1:
        raise ValueError("chaos fraction must be in (0, 1]")
    if not running:
        return []
    count = max(1, round(fraction * len(running)))
    return rng.sample(sorted(running), count)


@dataclass
class Sample:
    t: float
    visible: int
    in_flight: int
    desired: int
    running: int
    pending: int
    leases: dict[str, int] = field(default_factory=dict)
    credits_used: dict[str, float] = field(default_factory=dict)

    @property
    def drained(self) -> bool:
        return self.visible == 0 and self.in_flight == 0


def summarise(tag: str, items: int, seeded_at: float, samples: list[Sample]) -> dict:
    """Timings M2 and M5 read: first task, peak tasks, drain, back to zero."""
    def first(pred) -> float | None:
        return next((round(s.t - seeded_at, 1) for s in samples if pred(s)), None)

    peak = max((s.running for s in samples), default=0)
    # SQS counts are approximate: right after seeding, and at random moments, a sample can
    # read 0 visible / 0 in flight while the batch is still there (seen in M4 at 32 s).
    # So "drained" is the first empty sample after the LAST busy one, not after the first.
    last_busy = max((i for i, s in enumerate(samples) if s.visible or s.in_flight), default=None)
    drained = None if last_busy is None else next(
        (round(s.t - seeded_at, 1) for s in samples[last_busy + 1:] if s.drained), None
    )
    after_drain = [s for s in samples if drained is not None and s.t - seeded_at >= drained]
    zero = next(
        (round(s.t - seeded_at, 1) for s in after_drain if s.desired == 0 and s.running == 0),
        None,
    )
    credits = {}
    if samples:
        start, end = samples[0].credits_used, samples[-1].credits_used
        credits = {k: round(end.get(k, 0) - start.get(k, 0), 2) for k in end}
    return {
        "tag": tag,
        "items": items,
        "first_running_s": first(lambda s: s.running > 0),
        "peak_running": peak,
        "peak_running_s": first(lambda s: s.running == peak and peak > 0),
        "drained_s": drained,
        "back_to_zero_s": zero,
        "items_per_hour": round(items / drained * 3600, 1) if drained else None,
        "registry_credit_delta": credits,
    }


# --- AWS --------------------------------------------------------------------


class Fleet:
    """The deployed fleet, found through its stacks' outputs."""

    def __init__(self, args: argparse.Namespace) -> None:
        import boto3  # lazy: the pure core and its tests never need it
        import botocore.session

        core = botocore.session.Session()
        if args.aws_profile:
            core.set_config_variable("profile", args.aws_profile)
        s = boto3.Session(botocore_session=core)
        self.s3, self.sqs, self.ecs = s.client("s3"), s.client("sqs"), s.client("ecs")
        self.ddb, self.lam, cfn = s.client("dynamodb"), s.client("lambda"), s.client("cloudformation")

        out: dict[str, str] = {}
        for name in ("Data", "Queue", "Compute"):
            stack = cfn.describe_stacks(StackName=f"{args.prefix}{name}")["Stacks"][0]
            out.update({o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])})
        self.bucket = out["ReferenceBucketName"]
        self.table = out["SubscriptionsTableName"]
        self.queue_url = out["WorkQueueUrl"]
        self.cluster = out["ClusterName"]
        self.service = out["ServiceName"]
        self.seeding = f"{args.prefix}Functions{SEEDING_SUFFIX}"

    def upload_corpus(self) -> int:
        objects = corpus_objects()
        for key, text in objects.items():
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=text.encode())
        return len(objects)

    def register(self, item: dict) -> None:
        self.ddb.put_item(TableName=self.table, Item=item)

    def set_pool(self, enabled: set[str]) -> dict[str, str]:
        states = {}
        for row in self.ddb.scan(TableName=self.table, ProjectionExpression="subscriptionId")["Items"]:
            sub = row["subscriptionId"]["S"]
            state = "ENABLED" if sub in enabled else "DISABLED"
            self.ddb.update_item(
                TableName=self.table, Key={"subscriptionId": {"S": sub}},
                UpdateExpression="SET #s = :s", ExpressionAttributeNames={"#s": "state"},
                ExpressionAttributeValues={":s": {"S": state}},
            )
            states[sub] = state
        missing = enabled - states.keys()
        if missing:
            raise SystemExit(f"not in the registry: {sorted(missing)}")
        return states

    def seed(self, tag: str, items: int) -> float:
        key = f"{MANIFEST_PREFIX}{tag}.json"
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(manifest(tag, items)).encode())
        seeded_at = time.time()
        resp = self.lam.invoke(FunctionName=self.seeding, Payload=json.dumps({"key": key}).encode())
        body = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            raise SystemExit(f"seeding failed: {body}")
        print(f"seeded {items} items as {tag}: {body}", flush=True)
        return seeded_at

    def running_tasks(self) -> list[str]:
        arns = self.ecs.list_tasks(cluster=self.cluster, serviceName=self.service,
                                   desiredStatus="RUNNING")["taskArns"]
        return arns

    def stop(self, arns: list[str]) -> None:
        for arn in arns:
            self.ecs.stop_task(cluster=self.cluster, task=arn, reason="fleetbench M4 chaos")

    def sample(self) -> Sample:
        attrs = self.sqs.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        svc = self.ecs.describe_services(cluster=self.cluster, services=[self.service])["services"][0]
        leases, credits = {}, {}
        for row in self.ddb.scan(TableName=self.table)["Items"]:
            sub = row["subscriptionId"]["S"]
            leases[sub] = int(row.get("activeLeases", {}).get("N", 0))
            credits[sub] = float(row.get("creditsUsed", {}).get("N", 0))
        return Sample(
            t=time.time(),
            visible=int(attrs["ApproximateNumberOfMessages"]),
            in_flight=int(attrs["ApproximateNumberOfMessagesNotVisible"]),
            desired=svc["desiredCount"], running=svc["runningCount"], pending=svc["pendingCount"],
            leases=leases, credits_used=credits,
        )


def _retrying(call, attempts: int = 40):
    """Ride out expired credentials and dropped networks, as the M1 launcher had to."""
    from botocore.exceptions import BotoCoreError, ClientError

    for attempt in range(1, attempts + 1):
        try:
            return call()
        except (BotoCoreError, ClientError) as error:
            if attempt == attempts:
                raise
            print(f"    AWS call failed ({type(error).__name__}); retry {attempt} in 30s", flush=True)
            time.sleep(30)
    return None


def run(args: argparse.Namespace) -> int:
    fleet = Fleet(args)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    timeline = out / f"{args.tag}-timeline.jsonl"
    samples: list[Sample] = [_retrying(fleet.sample)]
    seeded_at = fleet.seed(args.tag, args.items)
    chaos_done = args.chaos_fraction is None
    deadline = seeded_at + args.timeout
    with timeline.open("a") as f:
        while time.time() < deadline:
            s = _retrying(fleet.sample)
            samples.append(s)
            f.write(json.dumps(s.__dict__) + "\n")
            f.flush()
            if not chaos_done and s.t - seeded_at >= args.chaos_after:
                targets = chaos_targets(_retrying(fleet.running_tasks), args.chaos_fraction,
                                        random.Random(args.tag))
                _retrying(lambda t=targets: fleet.stop(t))
                print(f"chaos: stopped {len(targets)} running tasks", flush=True)
                f.write(json.dumps({"t": time.time(), "chaos_stopped": targets}) + "\n")
                chaos_done = True
            if s.drained and s.desired == 0 and s.running == 0 and s.t - seeded_at > 60:
                break
            time.sleep(args.interval)
    summary = summarise(args.tag, args.items, seeded_at, samples)
    (out / f"{args.tag}-summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    return 0 if summary["drained_s"] is not None else 1


def _parse(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="fleetbench.fleetload", description=__doc__.split("\n")[0])
    p.add_argument("--aws-profile")
    p.add_argument("--prefix", default="KiroFleet", help="stack name prefix (default KiroFleet)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("corpus", help="upload the shared notes to the reference bucket")
    r = sub.add_parser("register", help="add one subscription row (secret reference only)")
    r.add_argument("--sub-id", required=True)
    r.add_argument("--secret-ref", required=True)
    r.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY)
    r.add_argument("--credit-budget", type=int, default=DEFAULT_CREDIT_BUDGET)
    pl = sub.add_parser("pool", help="enable exactly these subscriptions, disable the rest")
    pl.add_argument("--enable", required=True, help="comma-separated subscription ids")
    rn = sub.add_parser("run", help="seed one batch and watch it until the fleet is back at zero")
    rn.add_argument("--tag", required=True)
    rn.add_argument("--items", type=int, required=True)
    rn.add_argument("--out", required=True)
    rn.add_argument("--interval", type=float, default=15.0)
    rn.add_argument("--timeout", type=float, default=3 * 3600)
    rn.add_argument("--chaos-fraction", type=float)
    rn.add_argument("--chaos-after", type=float, default=120.0)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse(argv)
    if args.cmd == "run":
        return run(args)
    fleet = Fleet(args)
    if args.cmd == "corpus":
        print(f"uploaded {fleet.upload_corpus()} notes to s3://{fleet.bucket}/{CORPUS_PREFIX}")
    elif args.cmd == "register":
        fleet.register(registry_item(args.sub_id, args.secret_ref, args.max_concurrency,
                                     args.credit_budget))
        print(f"registered {args.sub_id}")
    elif args.cmd == "pool":
        print(json.dumps(fleet.set_pool({s.strip() for s in args.enable.split(",") if s.strip()})))
    return 0


if __name__ == "__main__":
    sys.exit(main())
