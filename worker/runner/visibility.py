"""Keeping a received item invisible while it runs, and no longer.

The queue owns a work item through its visibility timeout: received, the item is hidden
from other workers until it is deleted or the timeout lapses. One way to size that timeout
is to make it outlast the longest run. That is safe against double delivery, but it is also
how long an item waits after its task dies mid-run before anyone else can take it, and in
the measurements that wait was most of the recovery time (22.5 minutes against a 7-minute
batch).

The other way, used here, is a short window kept alive while the run is in progress: the
worker pushes the item's visibility out to the window as soon as it has it, then again every
third of a window. A live worker never lets the window lapse, so a slow run is not handed to
a second worker. A dead one stops extending, so its item reappears within one window. A
worker that is alive but stuck is bounded too: past a maximum hold (the workflow timeout
plus a margin, the old fixed timeout) the keeper stops renewing.

Stopped before the item is acknowledged. After that the receipt belongs to the queue
operation that follows (a delete, or a release with a retry delay), and an extension racing
it would overwrite the delay the worker just chose.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Final, Protocol, Self

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_SECONDS: Final = 180
"""How long an item stays hidden without a fresh extension.

Also the longest an item waits after its worker dies. Three minutes leaves two extensions
of slack inside each window (one every 60 s), so one or two failed calls cost nothing.
"""

VISIBILITY_HOLD_MARGIN: Final = 1.5
"""Maximum hold as a multiple of the workflow timeout.

The margin covers the work either side of the run (pulling inputs, storing results). It is
the multiplier the queue's fixed timeout used before the window was renewed, so a stuck
worker gives up an item no later than it used to.
"""


class Extendable(Protocol):
    def extend(self, receipt: str, seconds: int) -> None: ...


class VisibilityKeeper:
    """Extends one item's visibility on an interval, in the background.

    Fail open: an extension that raises is logged and retried at the next interval rather
    than stopping the run. If extensions keep failing, the item reappears when the window
    lapses and a second worker may take it; results are keyed by item, so the worst case is
    work done twice, not work lost.
    """

    def __init__(
        self,
        queue: Extendable,
        receipt: str,
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
        interval_seconds: float | None = None,
        max_hold_seconds: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._queue = queue
        self._receipt = receipt
        self._window = int(window_seconds)
        self._interval = (
            interval_seconds if interval_seconds is not None else self._window / 3
        )
        self._max_hold = max_hold_seconds
        self._clock = clock
        self._started_at = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.extensions = 0
        self.failures = 0
        self.gave_up = False

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("this keeper has already been started")
        self._started_at = self._clock()
        # Once now, in this thread, so the window is set from the moment the item is held
        # whatever the queue's own default is.
        self._extend_once()
        self._thread = threading.Thread(
            target=self._run, name="visibility-keeper", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop extending. Returns only once no extension can still be in flight."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *_exc_info) -> None:
        self.stop()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            if self._max_hold is not None and (
                self._clock() - self._started_at + self._window > self._max_hold
            ):
                # The run is cut off at the workflow timeout, so holding the item past this
                # point means the worker itself is stuck. Stop renewing and let the item
                # reappear for a worker that is not.
                self.gave_up = True
                logger.warning(
                    "an item has been held for over %d s; no longer extending its "
                    "visibility, so it reappears within %d s",
                    int(self._clock() - self._started_at),
                    self._window,
                )
                return
            self._extend_once()

    def _extend_once(self) -> None:
        try:
            self._queue.extend(self._receipt, self._window)
        except Exception:
            self.failures += 1
            logger.warning(
                "failed to extend an item's visibility (%d so far); it reappears in at "
                "most %d s if this keeps failing",
                self.failures,
                self._window,
                exc_info=True,
            )
            return
        self.extensions += 1
