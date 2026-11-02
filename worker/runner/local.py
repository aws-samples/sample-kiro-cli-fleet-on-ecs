"""Local implementations of the queue and the result store.

Here so the whole path can be exercised on one machine: seed work, claim a
subscription, run the workflow, store results, return capacity. The SQS and S3
implementations arrive with the infrastructure and satisfy the same protocols, so
the worker loop above them does not change.

The queue reproduces the two SQS behaviors the worker depends on and nothing else:
a received message is invisible to others until it is released or its visibility
expires, and deleting requires the receipt handle from that receive. Ordering,
batching, and durability are out of scope; this is a driver for local runs, not a
queue implementation.
"""

from __future__ import annotations

import json
import logging
import shutil
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from runner.worker import WorkItem

logger = logging.getLogger(__name__)


@dataclass
class _Message:
    body: str
    receipt: str | None = None
    invisible_until: float = 0.0
    receives: int = 0


@dataclass
class LocalQueue:
    """An in-memory queue with SQS-shaped visibility semantics.

    Thread safe, because a local run with several workers is the interesting case:
    it is where two workers can be handed the same item if visibility is wrong.
    """

    visibility_timeout: float = 30.0
    max_receives: int = 3
    _messages: list[_Message] = field(default_factory=list)
    _dead_letter: list[str] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def seed(self, item_id: str, prompt: str, inputs: tuple[str, ...] = ()) -> None:
        body = json.dumps({"itemId": item_id, "prompt": prompt, "inputs": list(inputs)})
        with self._lock:
            self._messages.append(_Message(body=body))

    def receive(self) -> WorkItem | None:
        now = time.monotonic()
        with self._lock:
            for message in self._messages:
                if message.invisible_until > now:
                    continue
                message.receives += 1
                if message.receives > self.max_receives:
                    # Same effect as a redrive policy: stop redelivering and set it
                    # aside, so one poisonous item cannot occupy the fleet forever.
                    self._messages.remove(message)
                    self._dead_letter.append(message.body)
                    logger.warning("moved an item to the dead-letter list")
                    continue
                message.receipt = uuid.uuid4().hex
                message.invisible_until = now + self.visibility_timeout
                return WorkItem.from_message(
                    {
                        "Body": message.body,
                        "ReceiptHandle": message.receipt,
                        "Attributes": {"ApproximateReceiveCount": str(message.receives)},
                    }
                )
        return None

    def delete(self, receipt: str) -> None:
        with self._lock:
            for message in self._messages:
                if message.receipt == receipt:
                    self._messages.remove(message)
                    return
        logger.debug("delete called with an unknown receipt; already gone")

    def release(self, receipt: str, delay_seconds: float = 0.0) -> None:
        """Make an item visible again, now or after a delay.

        The delay is what stops a worker spinning: released with no delay, a
        throttled item is immediately visible to the same worker that just failed on
        it, which picks it up again and fails again until the redrive policy sets it
        aside. This is `ChangeMessageVisibility` with a positive timeout, not zero.
        """
        with self._lock:
            for message in self._messages:
                if message.receipt == receipt:
                    message.invisible_until = (
                        time.monotonic() + delay_seconds if delay_seconds else 0.0
                    )
                    message.receipt = None
                    return

    def extend(self, receipt: str, seconds: int) -> None:
        """Push a held item's visibility out to `seconds` from now, as SQS does."""
        with self._lock:
            for message in self._messages:
                if message.receipt == receipt:
                    message.invisible_until = time.monotonic() + seconds
                    return
        logger.debug("extend called with an unknown receipt; already acknowledged")

    @property
    def depth(self) -> int:
        with self._lock:
            return len(self._messages)

    @property
    def in_flight(self) -> int:
        now = time.monotonic()
        with self._lock:
            return sum(1 for m in self._messages if m.invisible_until > now)

    @property
    def dead_lettered(self) -> list[str]:
        with self._lock:
            return list(self._dead_letter)


@dataclass
class LocalResultStore:
    """Results on the filesystem, under a key derived from the work item.

    The key is deterministic, so a redelivered item overwrites its previous result
    rather than adding a second copy. That is what makes a retry safe to allow.
    """

    root: Path
    reference: Path | None = None

    def __post_init__(self) -> None:
        self.root = Path(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    def store(self, item_id: str, files: tuple[Path, ...], root: Path) -> str:
        """Add files under the item's prefix, overwriting by path.

        Additive, because the S3 implementation is: it puts one object per file and relies on
        deterministic keys for a re-run to overwrite rather than duplicate. This used to clear
        the directory on every call, which made two calls for one item destroy each other —
        the run transcript was written and then deleted by the result that followed it. The
        bug was invisible locally in the sense that mattered: it looked like the transcript
        was never produced, and it existed on disk the whole time.

        Two stores behaving differently is the recurring failure in this repository. A local
        run is only evidence about a task if the pieces underneath it agree.
        """
        destination = self.root / item_id
        destination.mkdir(parents=True, exist_ok=True)

        for path in files:
            relative = path.relative_to(root)
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
        return str(destination)

    def fetch_inputs(self, inputs: tuple[str, ...], into: Path) -> None:
        if self.reference is None:
            raise FileNotFoundError(
                "this store has no reference data configured, but the work item "
                "asked for inputs"
            )
        into.mkdir(parents=True, exist_ok=True)
        for name in inputs:
            source = Path(self.reference) / name
            if not source.exists():
                raise FileNotFoundError(f"reference input {name!r} is not present")
            if source.is_dir():
                shutil.copytree(source, into / name, dirs_exist_ok=True)
            else:
                shutil.copy2(source, into / name)

    def results_for(self, item_id: str) -> list[Path]:
        destination = self.root / item_id
        if not destination.exists():
            return []
        return sorted(p for p in destination.rglob("*") if p.is_file())
