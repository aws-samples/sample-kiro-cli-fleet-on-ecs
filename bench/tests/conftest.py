"""Shared fixtures for the bench tests.

The whole point of the ramp core is that it can be verified without a network, a
credential, or the worker package. These fixtures provide fake stream calls with
known throttling behaviour so the tests assert on where the ramp *decides* onset is,
which is the harness's job, rather than on Kiro's real limit, which is not.
"""

from __future__ import annotations

import threading
import time

from fleetbench.ramp import StreamOutcome, StreamResult


class ConcurrencyProbe:
    """A fake stream call that throttles once more than `cap` streams overlap.

    It records the peak overlap it ever observed, so a test can assert that the ramp
    actually held N streams open at once (the barrier working) and not merely that it
    called the function N times in sequence.

    Each call decides on the PEAK overlap it saw during its own lifetime, not an
    instantaneous reading, and it does not decrement until it has decided. That is
    what makes the step function deterministic: every sibling released from the ramp
    barrier is counted before anyone leaves, so at concurrency N each call observes a
    peak of N and the throttle decision is a clean function of N vs cap, free of the
    race between one call's decrement and another's sample.
    """

    def __init__(self, cap: int, *, poll_seconds: float = 0.005, stable_reads: int = 4) -> None:
        self.cap = cap
        self.poll_seconds = poll_seconds
        self.stable_reads = stable_reads
        self._lock = threading.Lock()
        self._in_flight = 0
        self.peak = 0

    def __call__(self) -> StreamResult:
        with self._lock:
            self._in_flight += 1
            observed_peak = self._in_flight
            self.peak = max(self.peak, self._in_flight)
        try:
            # Poll until arrivals stop, so every sibling has been counted before this
            # call decides. Deciding on the running peak means a sibling that finishes
            # and decrements first cannot make a slower call under-count the overlap.
            last = -1
            stable = 0
            while stable < self.stable_reads:
                time.sleep(self.poll_seconds)
                with self._lock:
                    current = self._in_flight
                observed_peak = max(observed_peak, current)
                if current == last:
                    stable += 1
                else:
                    last = current
                    stable = 0
            if observed_peak > self.cap:
                return StreamResult(StreamOutcome.THROTTLED, 5.0, "probe")
            return StreamResult(StreamOutcome.OK, 40.0, "probe")
        finally:
            with self._lock:
                self._in_flight -= 1
