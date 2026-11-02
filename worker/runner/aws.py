"""The deployed queue and result store.

Same interfaces the local implementations satisfy, so the worker loop above them is
unchanged. What differs is where durability comes from, and the two choices worth naming
are both about not losing a work item.

Visibility timeout is how a work item is owned. A received message is invisible to other
workers until it is deleted or the timeout lapses, which is why the worker needs no lock of
its own: SQS already provides exactly the ownership a work item requires. Extending it
periodically while a long run is in progress is the one thing the worker has to remember,
or a slow item gets handed to a second worker while the first is still on it.

Result keys are derived from the work item, so a redelivered item overwrites its previous
result rather than adding a second copy. That is what makes retrying safe to allow at all.
"""

from __future__ import annotations

import logging
import mimetypes
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from botocore.exceptions import ClientError

from runner.worker import WorkItem

_NOT_FOUND_CODES: Final = frozenset({"404", "NoSuchKey", "NotFound"})
"""What `HeadObject` reports for a key that is not an object. Anything else is raised."""

logger = logging.getLogger(__name__)

DEFAULT_WAIT_SECONDS: Final = 20
"""Long polling. Short polling would return empty while messages exist, which a worker
that exits on the first empty receive would read as a drained queue."""


@dataclass
class SqsQueue:
    """A work queue backed by Amazon SQS."""

    client: Any
    queue_url: str
    wait_seconds: int = DEFAULT_WAIT_SECONDS
    visibility_timeout: int | None = None

    def receive(self) -> WorkItem | None:
        parameters: dict[str, Any] = {
            "QueueUrl": self.queue_url,
            "MaxNumberOfMessages": 1,
            "WaitTimeSeconds": self.wait_seconds,
            # The receive count drives the retry backoff in the worker.
            "MessageSystemAttributeNames": ["ApproximateReceiveCount"],
        }
        if self.visibility_timeout is not None:
            parameters["VisibilityTimeout"] = self.visibility_timeout

        response = self.client.receive_message(**parameters)
        messages = response.get("Messages") or []
        if not messages:
            return None
        return WorkItem.from_message(messages[0])

    def delete(self, receipt: str) -> None:
        self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt)

    def release(self, receipt: str, delay_seconds: float = 0.0) -> None:
        """Hand an item back, now or after a delay.

        Zero makes it immediately visible, which is right when this worker is stopping and
        another should take it. A positive delay is what stops a worker spinning on an item
        it just failed.
        """
        self.client.change_message_visibility(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt,
            VisibilityTimeout=int(delay_seconds),
        )

    def extend(self, receipt: str, seconds: int) -> None:
        """Keep ownership of an item that is taking a while.

        Without this a run longer than the visibility timeout gets a second worker started
        on the same item, and both then write the same result key.
        """
        self.client.change_message_visibility(
            QueueUrl=self.queue_url, ReceiptHandle=receipt, VisibilityTimeout=seconds
        )

    def depth(self) -> int:
        """Approximate, and only for reporting. Never for deciding when to stop."""
        response = self.client.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
        return int(response["Attributes"]["ApproximateNumberOfMessages"])


@dataclass
class S3ResultStore:
    """Results in Amazon S3, under a key derived from the work item."""

    client: Any
    bucket: str
    prefix: str = "results"
    reference_bucket: str | None = None

    def key_for(self, item_id: str, relative: str) -> str:
        return f"{self.prefix.rstrip('/')}/{item_id}/{relative}"

    def store(self, item_id: str, files: tuple[Path, ...], root: Path) -> str:
        for path in files:
            relative = path.relative_to(root).as_posix()
            content_type, _ = mimetypes.guess_type(path.name)
            self.client.put_object(
                Bucket=self.bucket,
                Key=self.key_for(item_id, relative),
                Body=path.read_bytes(),
                ContentType=content_type or "application/octet-stream",
            )
        location = f"s3://{self.bucket}/{self.key_for(item_id, '')}"
        logger.info("stored %d file(s) for %s at %s", len(files), item_id, location)
        return location

    def fetch_inputs(self, inputs: tuple[str, ...], into: Path) -> None:
        """Pull what the work item declared into the workspace.

        Reads from a separate bucket when one is configured: inputs are shared and read
        only, results are per run and written, and giving the task one grant over both
        would be wider than it needs.
        """
        bucket = self.reference_bucket or self.bucket
        into.mkdir(parents=True, exist_ok=True)

        # The same layout as `LocalResultStore.fetch_inputs`: an object lands at
        # `into/<key>`, and everything under a prefix lands at `into/<prefix>/<rest>`.
        # Decided by what the name resolved to, never by how many objects it matched, so
        # a prefix holding one object is laid out like a prefix holding many.
        for name in inputs:
            exact, keys = self._expand(bucket, name)
            if not keys:
                raise FileNotFoundError(f"no object at s3://{bucket}/{name}")
            prefix = name.rstrip("/")
            for key in keys:
                if exact:
                    target = into / key
                else:
                    target = into / prefix / key[len(name):].lstrip("/")
                target.parent.mkdir(parents=True, exist_ok=True)
                self.client.download_file(bucket, key, str(target))

    def _expand(self, bucket: str, name: str) -> tuple[bool, list[str]]:
        """Whether `name` is one object, and the keys it stands for.

        A name without a trailing slash is tried as an object first. Only "not found"
        falls through to a prefix listing: a denied or throttled `HeadObject` is raised,
        because reporting it as a missing input would send the operator looking for a file
        that is there. The prefix always ends in `/`, so `data/sub` never matches
        `data/subway/...`.
        """
        if not name.endswith("/"):
            try:
                self.client.head_object(Bucket=bucket, Key=name)
                return True, [name]
            except ClientError as error:
                code = str(error.response.get("Error", {}).get("Code", ""))
                if code not in _NOT_FOUND_CODES:
                    raise

        prefix = name if name.endswith("/") else f"{name}/"
        paginator = self.client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(
                obj["Key"] for obj in page.get("Contents", []) if not obj["Key"].endswith("/")
            )
        return False, keys

    def results_for(self, item_id: str) -> list[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(
            Bucket=self.bucket, Prefix=self.key_for(item_id, "")
        ):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)
