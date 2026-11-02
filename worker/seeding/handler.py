"""Turning a manifest into work items on the queue.

Seeding is the one place a batch is defined, so it is also the place a batch can be defined
wrongly. Two properties matter more than throughput.

**It must not be one long invocation.** A manifest of several thousand items would otherwise
run against a Lambda timeout, and a timeout half way through a seeding pass leaves a batch
nobody can describe: some items queued, no record of which. So a pass sends a bounded number
of items and, if more remain, hands the rest to a fresh invocation. The continuation is a
position in the manifest rather than a cursor in memory.

**An item's identifier has to be derived, not generated.** Result keys are built from it, so
the same manifest seeded twice has to produce the same identifiers — that is what makes a
re-seed overwrite its earlier results rather than double them. A uuid per item would make
re-seeding silently duplicate a whole batch.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Final

import boto3

from leaseclient.lambda_logging import configure_logging

logger = logging.getLogger(__name__)

SEND_BATCH: Final = 10
"""SQS accepts ten messages per `SendMessageBatch` call. Not a tuning parameter."""

DEFAULT_ITEMS_PER_INVOCATION: Final = 500
"""Items one invocation will send before continuing in another.

Chosen well below what the timeout allows. Seeding is not the slow part of a batch — the
runs are — so there is nothing to gain from making each pass as large as possible, and a
smaller pass means less to reason about when one fails.
"""

MANIFEST_FILE: Final = "manifest.json"


class InvalidManifest(RuntimeError):
    """The manifest cannot be turned into work items."""


@dataclass(frozen=True)
class WorkItem:
    item_id: str
    prompt: str
    inputs: tuple[str, ...]

    def as_body(self) -> str:
        return json.dumps(
            {"itemId": self.item_id, "prompt": self.prompt, "inputs": list(self.inputs)}
        )


def identifier_for(key: str) -> str:
    """A stable, readable identifier derived from an input's key.

    Derived so that re-seeding the same manifest overwrites its earlier results instead of
    producing a second copy of the batch. Readable because it ends up in an S3 prefix an
    operator reads, and `item-7f3a9` tells them nothing about which input failed.
    """
    stem = key.rsplit("/", 1)[-1]
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-.")
    if not slug:
        raise InvalidManifest(f"cannot derive an item identifier from {key!r}")
    return slug


@dataclass
class Seeder:
    s3: Any
    sqs: Any
    queue_url: str
    items_per_invocation: int = DEFAULT_ITEMS_PER_INVOCATION

    def items_from_manifest(self, bucket: str, key: str) -> list[WorkItem]:
        """Read the manifest and expand it into every item it describes.

        Two shapes are accepted because they answer different questions. An explicit list
        is right when each item needs its own prompt. A prefix with one prompt is right for
        the shape fan-out work usually has — many inputs, one unchanging instruction — and
        writing that out by hand for two thousand files is how a manifest gets stale.
        """
        try:
            body = self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            manifest = json.loads(body)
        except json.JSONDecodeError as exc:
            raise InvalidManifest(f"s3://{bucket}/{key} is not valid JSON: {exc}") from exc
        except Exception as exc:  # noqa: BLE001 - surfaced with its location
            raise InvalidManifest(f"cannot read s3://{bucket}/{key}: {exc}") from exc

        if "items" in manifest:
            return self._explicit(manifest["items"])
        if "inputPrefix" in manifest:
            return self._expanded(bucket, manifest)
        raise InvalidManifest(
            f"s3://{bucket}/{key} has neither 'items' nor 'inputPrefix'; one is required"
        )

    def _explicit(self, items: Any) -> list[WorkItem]:
        if not isinstance(items, list) or not items:
            raise InvalidManifest("'items' must be a non-empty list")

        expanded: list[WorkItem] = []
        seen: set[str] = set()
        for index, entry in enumerate(items):
            if not isinstance(entry, dict) or "prompt" not in entry:
                raise InvalidManifest(f"item {index} has no 'prompt'")
            inputs = tuple(entry.get("inputs", ()))
            item_id = entry.get("itemId") or (
                identifier_for(inputs[0]) if inputs else f"item-{index:05d}"
            )
            # Checked rather than deduplicated: two items sharing an identifier would
            # overwrite each other's results, and silently keeping one of them is worse
            # than refusing a manifest its author can fix.
            if item_id in seen:
                raise InvalidManifest(
                    f"item identifier {item_id!r} appears more than once; results are "
                    'keyed by it, so the second would overwrite the first'
                )
            seen.add(item_id)
            expanded.append(WorkItem(item_id, entry["prompt"], inputs))
        return expanded

    def _expanded(self, bucket: str, manifest: dict[str, Any]) -> list[WorkItem]:
        prompt = manifest.get("prompt")
        if not prompt:
            raise InvalidManifest("'inputPrefix' requires a 'prompt' to apply to each input")

        source = manifest.get("inputBucket", bucket)
        prefix = manifest["inputPrefix"]
        paginator = self.s3.get_paginator("list_objects_v2")

        items: list[WorkItem] = []
        seen: set[str] = set()
        for page in paginator.paginate(Bucket=source, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                item_id = identifier_for(key)
                if item_id in seen:
                    raise InvalidManifest(
                        f"two inputs under {prefix!r} reduce to the identifier "
                        f"{item_id!r}; rename one, or list the items explicitly"
                    )
                seen.add(item_id)
                items.append(WorkItem(item_id, prompt, (key,)))

        if not items:
            raise InvalidManifest(f"no objects under s3://{source}/{prefix}")
        return items

    def send(self, items: list[WorkItem]) -> int:
        """Enqueue a slice, ten at a time, and report how many were accepted."""
        sent = 0
        for start in range(0, len(items), SEND_BATCH):
            batch = items[start : start + SEND_BATCH]
            response = self.sqs.send_message_batch(
                QueueUrl=self.queue_url,
                Entries=[
                    {"Id": str(index), "MessageBody": item.as_body()}
                    for index, item in enumerate(batch)
                ],
            )
            failed = response.get("Failed") or []
            for failure in failed:
                # Named individually: a partial batch failure is the case where "it mostly
                # worked" hides a specific missing item, and the item is identifiable here
                # and nowhere downstream.
                logger.error(
                    "could not enqueue %s: %s",
                    batch[int(failure["Id"])].item_id,
                    failure.get("Message"),
                )
            sent += len(batch) - len(failed)
        return sent


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Seed a batch, continuing in another invocation if the manifest is long.

    Invoked with `{"bucket": ..., "key": ...}` and optionally `{"from": N}`, which is what
    a continuation looks like. The offset travels in the event rather than in a database,
    because a seeding pass that needs its own state store is a batch that can be
    half-defined in two places.
    """
    configure_logging()

    bucket = event.get("bucket") or os.environ.get("REFERENCE_BUCKET")
    key = event.get("key") or MANIFEST_FILE
    if not bucket:
        raise InvalidManifest(
            "no bucket in the event and REFERENCE_BUCKET is not set, so there is no "
            "manifest to read"
        )

    seeder = Seeder(
        s3=boto3.client("s3"),
        sqs=boto3.client("sqs"),
        queue_url=os.environ["WORK_QUEUE_URL"],
        items_per_invocation=int(
            os.environ.get("ITEMS_PER_INVOCATION", DEFAULT_ITEMS_PER_INVOCATION)
        ),
    )

    items = seeder.items_from_manifest(bucket, key)
    start = int(event.get("from", 0))
    slice_ = items[start : start + seeder.items_per_invocation]
    sent = seeder.send(slice_)
    nextFrom = start + len(slice_)

    result = {
        "manifest": f"s3://{bucket}/{key}",
        "total": len(items),
        "sent": sent,
        "from": start,
        "through": nextFrom,
        "complete": nextFrom >= len(items),
    }

    if not result["complete"]:
        # Asynchronous, so this invocation ends rather than waiting on the next. The
        # function name comes from the runtime so nothing has to be configured with its
        # own identity.
        boto3.client("lambda").invoke(
            FunctionName=os.environ["AWS_LAMBDA_FUNCTION_NAME"],
            InvocationType="Event",
            Payload=json.dumps({"bucket": bucket, "key": key, "from": nextFrom}),
        )
        logger.info("continuing from %d of %d", nextFrom, len(items))

    logger.info("seeded %s", json.dumps(result))
    return result
