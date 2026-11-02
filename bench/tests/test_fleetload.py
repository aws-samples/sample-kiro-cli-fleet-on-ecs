"""fleetload: the pure parts of the M2~M5 load driver."""

import json
import random
from pathlib import Path

import pytest

from fleetbench import fanout, fleetload
from fleetbench.fleetload import Sample

WORKFLOW_DIR = Path(__file__).resolve().parents[2] / "workflows" / "bench" / "fanout"


def test_corpus_is_the_m1_notes_under_the_reference_prefix() -> None:
    objects = fleetload.corpus_objects()
    m1 = fanout.corpus_files(2, 15, 120)
    assert len(objects) == len(m1) == 30
    assert all(k.startswith(fleetload.CORPUS_PREFIX) for k in objects)
    assert objects[fleetload.CORPUS_PREFIX + "a-01.md"] == m1["notes/a-01.md"]


def test_prompt_points_at_where_the_worker_puts_the_inputs() -> None:
    # The worker downloads a multi-object prefix to input/<prefix>/<relative path>.
    prompt = fleetload.item_prompt()
    assert "input/bench/fanout/notes/a-01.md" in prompt
    assert "input/bench/fanout/notes/b-15.md" in prompt
    assert " notes/a-01.md" not in prompt


def test_m1_prompt_is_unchanged_by_default() -> None:
    assert "read notes/a-01.md through notes/a-15.md" in fanout.prompt(2, 15)


def test_manifest_items_are_unique_per_tag_and_share_one_input() -> None:
    m = fleetload.manifest("m2-r1-k2", 30)
    ids = [i["itemId"] for i in m["items"]]
    assert len(ids) == len(set(ids)) == 30
    assert ids[0] == "m2-r1-k2-0001"
    assert all(i["inputs"] == [fleetload.CORPUS_PREFIX] for i in m["items"])
    json.dumps(m)  # must be serialisable as the seeding function reads it


@pytest.mark.parametrize("tag,items", [("", 1), ("a/b", 1), ("a b", 1), ("ok", 0)])
def test_manifest_rejects_bad_input(tag: str, items: int) -> None:
    with pytest.raises(ValueError):
        fleetload.manifest(tag, items)


def test_registry_item_carries_a_reference_not_a_key() -> None:
    item = fleetload.registry_item("sub-01", "arn:aws:secretsmanager:us-east-1:111:secret:x")
    assert item["maxConcurrency"] == {"N": "5"}
    assert item["creditBudget"] == {"N": "800"}
    assert item["creditsUsed"] == {"N": "0"}
    assert item["state"] == {"S": "ENABLED"}
    assert set(item) == {"subscriptionId", "state", "secretRef", "maxConcurrency",
                         "activeLeases", "creditBudget", "creditsUsed"}


def test_chaos_stops_a_rounded_fraction_and_at_least_one() -> None:
    rng = random.Random(1)
    running = [f"t{i}" for i in range(10)]
    assert len(fleetload.chaos_targets(running, 0.2, rng)) == 2
    assert len(fleetload.chaos_targets(["t0", "t1"], 0.2, rng)) == 1
    assert fleetload.chaos_targets([], 0.2, rng) == []
    with pytest.raises(ValueError):
        fleetload.chaos_targets(running, 0, rng)


def _s(t, visible, in_flight, desired, running, credits=None):
    return Sample(t, visible, in_flight, desired, running, 0, {}, credits or {})


def test_summary_reads_first_task_peak_drain_and_zero() -> None:
    samples = [
        _s(100, 0, 0, 0, 0, {"sub-01": 600.0}),
        _s(160, 10, 0, 3, 0),
        _s(220, 4, 6, 5, 5),
        _s(400, 0, 2, 5, 5),
        _s(460, 0, 0, 5, 3),
        _s(520, 0, 0, 0, 0, {"sub-01": 607.5}),
    ]
    s = fleetload.summarise("x", 10, 100, samples)
    assert s["first_running_s"] == 120
    assert s["peak_running"] == 5 and s["peak_running_s"] == 120
    assert s["drained_s"] == 360
    assert s["back_to_zero_s"] == 420
    assert s["items_per_hour"] == 100.0
    assert s["registry_credit_delta"] == {"sub-01": 7.5}


def test_summary_ignores_a_momentary_zero_reading_from_sqs() -> None:
    # Measured in M4: one sample 32 s after seeding read 0/0 while 50 items were queued.
    samples = [
        _s(100, 0, 0, 0, 0),
        _s(116, 50, 0, 0, 0),
        _s(132, 0, 0, 0, 0),
        _s(148, 50, 0, 3, 0),
        _s(400, 0, 5, 5, 5),
        _s(500, 0, 0, 0, 0),
    ]
    s = fleetload.summarise("m4", 50, 100, samples)
    assert s["drained_s"] == 400
    assert s["back_to_zero_s"] == 400


def test_bench_workflow_definition_loads_without_mcp() -> None:
    manifest = json.loads((WORKFLOW_DIR / "workflow.json").read_text())
    assert manifest["trustTools"] == fanout.TRUST_TOOLS.split(",")
    assert manifest["requiresMcpServers"] == []
    assert (WORKFLOW_DIR / manifest["promptFile"]).read_text().strip()
    # the reference copy of the prompt in prompt.md matches what items actually carry
    reference = (WORKFLOW_DIR / "prompt.md").read_text()
    assert "input/bench/fanout/notes/a-15.md" in reference
