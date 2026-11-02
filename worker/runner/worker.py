"""One worker: claim a subscription, drain the queue, hand the capacity back.

The loop is deliberately dull. Everything interesting has been pushed into the
pieces it calls, so what remains here is the order of operations, which is the part
that determines whether work can be lost.

A work item is deleted from the queue only after its result is durably stored. Any
other order trades a lost work item for a tidier loop. When a run fails, the message
is left alone: its visibility timeout expires, it is redelivered, and after enough
attempts the queue moves it aside. Nothing here deletes a message it could not
account for.

Given a usage reader, the worker also reads the subscription's cumulative credit
figure before every item and, best-effort, once when the drain ends, records it, and stops
draining before it receives another item once the figure reaches the budget less
the margin. Those remaining items are never received, so they stay in the queue
for a subscription with budget left.

Given a protection object, the worker also keeps its task protected from scale-in
while it holds an item: on after the budget check and before the receive, off once
the item is acknowledged or handed back, however it ended. ECS then stops the task
only between items, so a lower ceiling, a drain to zero or a deployment does not cut
off a run that has already spent its credits. The exception is a request ECS refuses,
for example with `DEPLOYMENT_BLOCKED` when more tasks are protected than the desired
count: that item runs unprotected, and the refusal is logged and counted.

Given a visibility window, the worker also holds each item with that short window,
renewed in the background while the run is in progress (`runner.visibility`). A slow
run keeps its item; a task that dies mid-run stops renewing, and its item reappears
within one window instead of after the longest possible run.
"""

from __future__ import annotations

import json
import logging
import os
import random
import shutil
import threading
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, Protocol

from leaseclient.metrics import CreditSource, Metrics, UsagePhase, UsageReadingOutcome
from leaseclient.registry import Registry, UsageWrite
from leaseclient.schema import SubscriptionState
from leaseclient.session import Session
from runner.guard import RUN_DIR
from runner.protection import ScaleInProtection
from runner.usage import UsageFailure, UsageReader
from runner.visibility import VisibilityKeeper
from runner.workflow import (
    INPUT_DIR,
    OUTPUT_DIR,
    Remedy,
    RunResult,
    Verdict,
    last_line,
    run_workflow,
)

logger = logging.getLogger(__name__)

DEFAULT_IDLE_EXITS_AFTER: Final = 1
"""Empty receives before the worker stops.

One is right for a queue that is seeded up front and drained: the first empty
receive means the batch is done, and lingering would bill for nothing. A fleet fed
continuously would raise it.
"""

DEFAULT_RETRY_DELAY_SECONDS: Final = 30.0
"""Base delay before a throttled or timed-out item is attempted again.

A throttle means the subscription is momentarily at its limit, so the delay needs to
outlast that rather than merely reorder the queue. Set to zero and the worker that
just failed takes the same item straight back. The delay doubles with each delivery
of the same item; see `retry_delay`.
"""

MAX_RETRY_DELAY_SECONDS: Final = 900.0
"""Ceiling on the backoff, so a long-lived item is not hidden for hours."""


def retry_delay(base: float, receive_count: int, jitter: float) -> float:
    """Exponential backoff with equal jitter for the next attempt of an item.

    `receive_count` is how many times the queue has delivered the item, so the first
    retry waits about `base`, the second about twice that, and so on up to the cap.
    `jitter` in [0, 1) spreads the wait over its upper half: items throttled together
    then come back at different times instead of hitting the key together again.
    """
    exponent = max(receive_count, 1) - 1
    ceiling = min(MAX_RETRY_DELAY_SECONDS, base * (2**exponent))
    return ceiling / 2 + ceiling / 2 * jitter


@dataclass(frozen=True)
class WorkItem:
    """One unit of work, and the handle needed to acknowledge it."""

    item_id: str
    prompt: str
    receipt: str
    inputs: tuple[str, ...] = ()
    receive_count: int = 1

    @classmethod
    def from_message(cls, message: dict[str, Any]) -> "WorkItem":
        body = json.loads(message["Body"])
        attributes = message.get("Attributes") or {}
        return cls(
            item_id=body["itemId"],
            prompt=body["prompt"],
            receipt=message["ReceiptHandle"],
            inputs=tuple(body.get("inputs", ())),
            receive_count=int(attributes.get("ApproximateReceiveCount", 1)),
        )


class Queue(Protocol):
    """The queue operations a worker needs.

    A protocol rather than a concrete client, so the loop can be driven end to end
    without a queue service while still being the code that runs against SQS.
    """

    def receive(self) -> WorkItem | None: ...

    def delete(self, receipt: str) -> None: ...

    def release(self, receipt: str, delay_seconds: float = 0.0) -> None: ...


class ResultStore(Protocol):
    """Where results go, keyed deterministically so a retry overwrites."""

    def store(self, item_id: str, files: tuple[Path, ...], root: Path) -> str: ...

    def fetch_inputs(self, inputs: tuple[str, ...], into: Path) -> None: ...


@dataclass
class WorkerReport:
    """What one worker did, for the caller and for the tests."""

    subscription_id: str
    completed: int = 0
    failed: int = 0
    retried: int = 0
    verdicts: list[Verdict] = None  # type: ignore[assignment]
    budget_exhausted: bool = False

    def __post_init__(self) -> None:
        if self.verdicts is None:
            self.verdicts = []


class Worker:
    """Drains work items using one held subscription."""

    def __init__(
        self,
        session: Session,
        queue: Queue,
        results: ResultStore,
        registry: Registry,
        metrics: Metrics,
        workspace: Path,
        agent: str | None = None,
        idle_exits_after: int = DEFAULT_IDLE_EXITS_AFTER,
        retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
        run=run_workflow,
        usage: UsageReader | None = None,
        credit_margin: Decimal | int = 0,
        protection: ScaleInProtection | None = None,
        jitter: Callable[[], float] = random.random,
        visibility_window_seconds: int | None = None,
        visibility_max_hold_seconds: float | None = None,
    ) -> None:
        self._jitter = jitter
        self._visibility_window = visibility_window_seconds
        self._visibility_max_hold = visibility_max_hold_seconds
        self._usage = usage
        self._credit_margin = Decimal(credit_margin)
        self._protection = protection
        self._retry_delay_seconds = retry_delay_seconds
        self._session = session
        self._queue = queue
        self._results = results
        self._registry = registry
        self._metrics = metrics
        self._workspace = workspace
        self._agent = agent
        self._idle_exits_after = idle_exits_after
        self._run = run
        # Kiro CLI 2.30 prints no per-run figure at all, so a warning per item would fill
        # the log with the same line. Warned once per worker; the metric still counts
        # every missing figure.
        self._credits_missing_warned = False
    def drain(self, stop: "threading.Event | None" = None) -> WorkerReport:
        """Process items until the queue is empty, or until asked to stop.

        `stop` is a cooperative stop: the worker finishes the item it is on, hands nothing
        back half-done, and returns so its capacity is released. Checked between items
        rather than during one, because interrupting a run mid-flight would leave a partial
        result and pay for the work without keeping it. The local runner uses it.

        A task does not use it. There, scale-in protection keeps scale-in, a lower ceiling,
        a drain to zero and a deployment from stopping the task during an item, so the
        SIGTERM they send arrives between items: the handler returns the lease and the task
        exits holding nothing. A stop that ignores protection (`StopTask`, task retirement)
        can arrive mid-run. The handler then returns the lease and raises, which kills the
        CLI, and the item is handed back at once to be run again elsewhere; the credits that
        run had spent are lost.
        """
        report = WorkerReport(subscription_id=self._session.subscription_id)
        read_at_end = self._drain(report, stop)
        if read_at_end and self._usage is not None:
            # The cumulative figure after this worker's last run, best-effort. Usually
            # it does not include that run yet (the figure lags by about 45 s), and it
            # may not be taken at all: once the queue drains, the scaling function sets
            # the service to zero, and that scale-in can stop the task first. The next
            # claim's pre-item reading, or the daily report, picks the last runs up,
            # and the margin covers the gap.
            self._read_usage(UsagePhase.DRAIN_END)
        return report

    def _drain(self, report: WorkerReport, stop: "threading.Event | None") -> bool:
        """The loop. Returns whether a drain-end reading is worth taking."""
        idle = 0

        while True:
            if stop is not None and stop.is_set():
                logger.info(
                    "stop requested; finished %d item(s) and returning capacity",
                    report.completed,
                )
                return True

            # If the lease was reclaimed, this worker's capacity now belongs to
            # someone else. Stopping mid-batch leaves the remaining items in the
            # queue for a worker that does hold capacity.
            if self._session.lost:
                logger.warning("lease lost; stopping with %d done", report.completed)
                return True

            # Before receive, so an exhausted subscription never holds a message:
            # nothing is handed back, and no receive count moves towards the
            # dead-letter queue.
            if self._usage is not None and self._budget_exhausted():
                report.budget_exhausted = True
                logger.warning(
                    "%s reached its credit ceiling; stopping with %d done and leaving "
                    "the remaining items for another subscription",
                    self._session.subscription_id,
                    report.completed,
                )
                # The pre-item reading was just recorded, so a drain-end one adds nothing.
                return False

            # Protected from scale-in from before the receive until the item is
            # acknowledged or handed back, so a lower ceiling, a drain to zero or a
            # deployment stops this task only between items. After the budget check,
            # so a worker stopping for credits never protects itself.
            self._protect()
            if self._task_stopping():
                # ECS refused protection because it is already stopping this task. An
                # item received now would be killed mid-run and wait out the visibility
                # timeout, so leave the queue to tasks that are staying.
                logger.info(
                    "this task is being stopped; ending with %d done and taking no "
                    "further item",
                    report.completed,
                )
                return True
            try:
                item = self._queue.receive()
                if item is not None:
                    try:
                        with self._keep_visible(item):
                            result = self._process(item)
                    except (SystemExit, KeyboardInterrupt):
                        # A stop that bypassed scale-in protection (`StopTask`, task
                        # retirement) arrived mid-run. The SIGTERM handler has already
                        # returned the lease and raised; `subprocess.run` killed the CLI
                        # on the way out. Hand the item back now rather than leaving it
                        # to reappear after the visibility window.
                        self._hand_back_interrupted(item)
                        raise
                    report.verdicts.append(result.verdict)
                    self._acknowledge(item, result, report)
            finally:
                # Off however the item ended: acknowledged, handed back, timed out,
                # raised, or an empty receive. A task that dies before this is
                # released when its protection expires.
                self._unprotect()

            if item is None:
                idle += 1
                if idle >= self._idle_exits_after:
                    logger.info("queue drained after %d items", report.completed)
                    # A pre-item reading was taken one long-poll ago, so the
                    # drain-end reading usually repeats it. It is kept, best-effort:
                    # scale-in can stop the task before it runs (README, "Credits").
                    return True
                continue
            idle = 0

            if result.verdict is Verdict.CREDENTIAL_REJECTED:
                # The key is bad for every worker, so stop rather than burn the rest
                # of the queue discovering that repeatedly. The key cannot read usage
                # either, so there is no drain-end reading.
                logger.error("credential rejected; stopping this worker")
                return False

            if result.verdict is Verdict.STARTUP_FAILED:
                # The image or its configuration is incomplete, and no item will run
                # here. Stopping is what makes the failure visible as a broken
                # deployment rather than as a queue of mysteriously failing items.
                logger.error(
                    "the workflow environment failed to start; stopping this worker "
                    "rather than failing every item in turn"
                )
                return True

    # --- scale-in protection ----------------------------------------------------
    #
    # Fail open, as the credit checks are. The protection object already logs and
    # counts its own failures; these guards are for one that raises, so it can never
    # stop an item or mask an exception from the run.

    def _protect(self) -> None:
        if self._protection is None:
            return
        try:
            self._protection.enable()
        except Exception:
            logger.warning(
                "turning on scale-in protection raised; running the item unprotected",
                exc_info=True,
            )

    def _task_stopping(self) -> bool:
        """Whether ECS answered that this task is already being stopped.

        Read defensively: a protection object without the attribute, or one that raises,
        counts as not stopping, so this guard can only ever skip a receive, never break one.
        """
        if self._protection is None:
            return False
        try:
            return bool(getattr(self._protection, "stopping", False))
        except Exception:  # noqa: BLE001 - this guard may skip a receive, never break one
            return False

    def _unprotect(self) -> None:
        if self._protection is None:
            return
        try:
            self._protection.disable()
        except Exception:
            logger.warning(
                "turning off scale-in protection raised; it lapses when it expires",
                exc_info=True,
            )

    # --- credits ---------------------------------------------------------------

    def _budget_exhausted(self) -> bool:
        """Read `/usage`, record it, and say whether the ceiling has been reached.

        Fails open throughout. A reading that fails, a write that fails, or a check
        that cannot read the record lets the item run: the budget is a guardrail with
        a margin, and a Kiro backend hiccup must not stop the fleet. Each failure is
        logged and counted.
        """
        self._read_usage(UsagePhase.PRE_ITEM)
        subscription_id = self._session.subscription_id
        try:
            record = self._registry.get(subscription_id)
        except Exception:
            logger.warning(
                "could not check the credit budget of %s; continuing",
                subscription_id,
                exc_info=True,
            )
            return False

        self._metrics.subscription_credits_used(
            subscription_id, record.credits_used, CreditSource.USAGE
        )
        self._metrics.subscription_credit_budget(subscription_id, record.credit_budget)

        if record.has_budget_headroom(self._credit_margin):
            return False
        logger.info(
            "%s has used %s of %s credits (margin %s)",
            subscription_id,
            record.credits_used,
            record.credit_budget,
            self._credit_margin,
        )
        self._metrics.budget_exhausted(subscription_id)
        return True

    def _read_usage(self, phase: UsagePhase) -> None:
        assert self._usage is not None
        subscription_id = self._session.subscription_id
        try:
            reading = self._usage.read(self._session.environment()["KIRO_API_KEY"])
        except Exception:
            logger.warning("the /usage reader raised; continuing", exc_info=True)
            reading = UsageFailure.NOT_LAUNCHED

        if isinstance(reading, UsageFailure):
            self._metrics.usage_reading(
                UsageReadingOutcome.READ_FAILED, subscription_id, phase
            )
            return

        try:
            written = self._registry.record_usage(
                subscription_id, reading.used, reading.resets_at
            )
        except Exception:
            logger.warning(
                "could not record a /usage reading for %s; continuing",
                subscription_id,
                exc_info=True,
            )
            written = UsageWrite.CONFLICT

        outcome = _OUTCOME_OF_WRITE[written]
        if outcome is UsageReadingOutcome.STALE:
            logger.info(
                "dropped a /usage reading for %s: its window ended %s",
                subscription_id,
                reading.resets_at.isoformat(),
            )
        else:
            logger.info(
                "/usage for %s: %s of %s (%s)",
                subscription_id,
                reading.used,
                reading.plan_limit,
                outcome,
            )
        self._metrics.usage_reading(outcome, subscription_id, phase)

    def _keep_visible(self, item: WorkItem) -> AbstractContextManager[object]:
        """Hold the item with a short, renewed visibility window while it runs.

        Ends before `_acknowledge`, which then deletes or releases the item with no
        extension racing it. Without a window, or with a queue that cannot extend, the
        queue's own visibility timeout is all that holds the item.
        """
        extend = getattr(self._queue, "extend", None)
        if self._visibility_window is None or extend is None:
            return nullcontext()
        return VisibilityKeeper(
            self._queue,
            item.receipt,
            self._visibility_window,
            max_hold_seconds=self._visibility_max_hold,
        )

    def _process(self, item: WorkItem) -> RunResult:
        scratch = self._workspace / item.item_id
        self._prepare(scratch, item)
        return self._run(
            prompt=item.prompt,
            api_key=self._session.environment()["KIRO_API_KEY"],
            workspace=scratch,
            agent=self._agent,
        )

    def _hand_back_interrupted(self, item: WorkItem) -> None:
        """Make an item whose run was cut off visible again at once, best-effort.

        Called with the visibility renewal already stopped, so nothing extends the item
        after this. A failure here is logged and swallowed: the item still returns when
        its window ends, and the stop that is in progress must not be masked.
        """
        try:
            self._queue.release(item.receipt)
            logger.warning("%s was interrupted mid-run; handed back", item.item_id)
        except Exception:
            logger.warning(
                "%s was interrupted mid-run and could not be handed back; it returns "
                "when its visibility window ends",
                item.item_id,
                exc_info=True,
            )

    def _prepare(self, scratch: Path, item: WorkItem) -> None:
        """A clean workspace at a fixed path, with inputs pulled in.

        The workflow only ever sees relative paths beneath this directory, which is
        what lets the same workflow run locally and in the fleet without rewriting
        anything. Cleaned between items so one item cannot read or overwrite
        another's files.
        """
        if scratch.exists():
            shutil.rmtree(scratch)
        (scratch / INPUT_DIR).mkdir(parents=True)
        (scratch / OUTPUT_DIR).mkdir(parents=True)
        if item.inputs:
            self._results.fetch_inputs(item.inputs, scratch / INPUT_DIR)

    def _acknowledge(
        self, item: WorkItem, result: RunResult, report: WorkerReport
    ) -> None:
        """Store the result, then delete the message. Never the other way round."""
        # Every run, whatever happened. The distribution is what carries the diagnosis: 2%
        # NoOutput is a prompt to look at and 2% Throttled is a subscription to add, and a
        # success count cannot tell them apart.
        self._metrics.run_outcome(str(result.verdict), item.item_id)
        if result.credits is not None:
            # Observability only: what one run printed, for sizing the margin.
            self._metrics.run_credits(
                self._session.subscription_id, item.item_id, result.credits
            )
        else:
            # Counted so a RunCredits p95 is read against how many runs it is missing. The
            # CLI does not always print the figure: 2.21.4 printed none on one of two
            # identical runs, and 2.30 prints none in headless mode at all. `creditsUsed`
            # never depended on it; it comes from `/usage`.
            log = logger.debug if self._credits_missing_warned else logger.warning
            self._credits_missing_warned = True
            log(
                "%s printed no Credits figure (verdict %s); last stderr line: %r. "
                "Expected on Kiro CLI 2.30, which prints none in headless mode; "
                "credit accounting uses /usage. Further misses are logged at debug.",
                item.item_id,
                result.verdict,
                last_line(result.stderr),
            )
            self._metrics.run_credits_missing(
                self._session.subscription_id, item.item_id, str(result.verdict)
            )

        # Before the branch, so a failed run's transcript survives too — and that is the one
        # worth having. A task's filesystem goes when the task does, so a transcript that is
        # only written locally answers nothing about a batch that finished hours ago.
        self._store_transcript(item)

        if result.succeeded:
            location = self._results.store(
                item.item_id, result.outputs, self._workspace / item.item_id / OUTPUT_DIR
            )
            self._queue.delete(item.receipt)
            report.completed += 1
            logger.info("completed %s -> %s", item.item_id, location)
            return

        remedy = result.remedy
        report.failed += 1

        if remedy is Remedy.RETRY_ELSEWHERE:
            # This worker's environment is incomplete, so the item is blameless.
            # Hand it straight back for a worker that can run it.
            self._queue.release(item.receipt)
            report.retried += 1
        elif remedy is Remedy.DISABLE_SUBSCRIPTION:
            self._disable_subscription(result)
            # Visible at once: this worker is about to stop, and another worker with
            # a working key should pick the item up now rather than wait.
            self._queue.release(item.receipt)
            report.retried += 1
        elif remedy is Remedy.RETRY_LATER:
            # Delayed on purpose. Released with no delay, a throttled item is
            # immediately visible to the worker that just failed on it, which takes
            # it again and fails again until the redrive policy sets it aside. The
            # delay is what turns a retry into a retry rather than a spin, and it
            # doubles per delivery so a key that stays saturated gets more room.
            delay = retry_delay(self._retry_delay_seconds, item.receive_count, self._jitter())
            self._queue.release(item.receipt, delay_seconds=delay)
            report.retried += 1
        else:
            # Left in flight on purpose: the visibility timeout expires, the queue
            # redelivers, and after the configured attempts it moves the item aside.
            # Deleting here would discard a work item nothing has accounted for.
            logger.warning(
                "%s failed (%s); leaving it for redelivery and the dead-letter queue",
                item.item_id,
                result.verdict,
            )

    def _store_transcript(self, item: WorkItem) -> None:
        """Export what the run did, next to what it produced.

        The run writes a transcript to its own scratch directory, which is on the task's
        filesystem and goes when the task does. Keeping it means a failure can be read
        afterwards instead of reproduced — and reproducing an agent run is neither cheap nor
        reliably the same twice.

        Stored under the item's own prefix rather than a separate one, so everything about one
        work item is in one place: `output/...` is what the run produced and `transcript/...`
        is what it did. Long tokens were already masked when it was written; nothing should
        put a credential here, and S3 is the wrong place to discover otherwise.

        Failure to store is logged and not raised. The result is the deliverable; losing the
        transcript should not fail an item that otherwise succeeded.
        """
        root = self._workspace / item.item_id / RUN_DIR
        transcript = root / "transcript"
        if not transcript.is_dir():
            return

        files = tuple(sorted(path for path in transcript.rglob("*") if path.is_file()))
        if not files:
            return

        try:
            self._results.store(item.item_id, files, root)
        except Exception:
            logger.warning(
                "could not store the transcript for %s", item.item_id, exc_info=True
            )

    def _disable_subscription(self, result: RunResult) -> None:
        """Take a subscription with a bad key out of rotation for the whole fleet."""
        subscription_id = self._session.subscription_id
        logger.error(
            "disabling %s: the credential was rejected, so every worker claiming it "
            "would fail the same way",
            subscription_id,
        )
        # Emitted before the write is attempted, and that order is deliberate: the pool has
        # a subscription nothing can use either way, so an operator needs to know whether or
        # not this worker managed to record it. Emitting only on success would make the worse
        # case — rejected credential and the registry not updated — the silent one.
        self._metrics.subscription_disabled(
            subscription_id, reason=str(result.verdict)
        )

        try:
            self._registry.set_state(subscription_id, SubscriptionState.DISABLED)
        except Exception:
            # Not fatal for this worker. The metric above and this log are what reach an
            # operator, and the scheduled reconciliation pass will see the state is wrong.
            logger.warning("could not disable %s", subscription_id, exc_info=True)


_OUTCOME_OF_WRITE: Final[dict[UsageWrite, UsageReadingOutcome]] = {
    UsageWrite.RAISED: UsageReadingOutcome.RECORDED,
    UsageWrite.UNCHANGED: UsageReadingOutcome.UNCHANGED,
    UsageWrite.WINDOW_ADOPTED: UsageReadingOutcome.WINDOW_ADOPTED,
    UsageWrite.STALE: UsageReadingOutcome.STALE,
    UsageWrite.CONFLICT: UsageReadingOutcome.WRITE_FAILED,
}


def workspace_root() -> Path:
    """Where workflows run. Fixed, and overridable for local use."""
    return Path(os.environ.get("WORKSPACE_ROOT", "/workspace"))
