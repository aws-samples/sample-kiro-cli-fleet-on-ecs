"""Keeping a lease alive, and noticing when it is not.

A lease expires on its own. That is the design: workers are tasks stopped by
scale-in and by crashes, so capacity must come back without anything running. The
cost is that a worker doing useful work has to keep saying so.

The interesting case is not extension, it is loss. If a lease is reclaimed while a
worker still believes it holds one, that worker is running against a subscription
whose capacity now belongs to someone else. It must find out, and the only thing it
can do about it is stop.
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime, timedelta
from typing import Final

from leaseclient import schema
from leaseclient.claim import Lease

logger = logging.getLogger(__name__)

MAX_CONSECUTIVE_FAILURES: Final = 3
"""How many extensions may fail before the lease is presumed lost.

Three, because the interval is a quarter of the lifetime: three consecutive
failures still leave one interval before expiry, so a transient error costs
nothing while a real loss is noticed before the capacity is handed to someone else.
"""


class LeaseLost(RuntimeError):
    """The lease is no longer held, so work against it must stop."""


class Heartbeat:
    """Extends one lease on an interval, in the background.

    Not restartable. A heartbeat that has stopped means either the work is over or
    the lease is gone, and neither is recoverable by trying again.
    """

    def __init__(
        self,
        dynamodb,
        lease: Lease,
        holder_id: str,
        lifetime: timedelta,
        interval: timedelta,
        table_name: str | None = None,
        now=lambda: datetime.now(UTC),
    ) -> None:
        self._ddb = dynamodb
        self._table = table_name or schema.leases_table_name()
        self._lease = lease
        self._holder_id = holder_id
        self._lifetime = lifetime
        self._interval = interval
        self._now = now

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lost = threading.Event()
        self._failures = 0
        self._extensions = 0

    @property
    def lost(self) -> bool:
        """Whether the lease has been reclaimed from under this holder."""
        return self._lost.is_set()

    @property
    def extensions(self) -> int:
        return self._extensions

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("this heartbeat has already been started")
        self._thread = threading.Thread(
            target=self._run, name=f"heartbeat-{self._lease.lease_id[:8]}", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def __enter__(self) -> "Heartbeat":
        self.start()
        return self

    def __exit__(self, *_exc_info) -> None:
        self.stop()

    def raise_if_lost(self) -> None:
        """For a worker to call between units of work."""
        if self.lost:
            raise LeaseLost(
                f"lease {self._lease.lease_id} on {self._lease.subscription_id} was "
                "reclaimed; stopping rather than running against capacity now "
                "allocated elsewhere"
            )

    def _run(self) -> None:
        while not self._stop.wait(self._interval.total_seconds()):
            if self._extend_once():
                continue
            if self._failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error(
                    "lease %s presumed lost after %d consecutive failed extensions",
                    self._lease.lease_id,
                    self._failures,
                )
                self._lost.set()
                return

    def extend_once(self) -> bool:
        """One extension, for a caller driving the schedule itself."""
        return self._extend_once()

    def _extend_once(self) -> bool:
        expires_at = self._now() + self._lifetime
        try:
            self._ddb.update_item(
                TableName=self._table,
                Key={schema.LEASE_ID: {"S": self._lease.lease_id}},
                UpdateExpression=(
                    f"SET {schema.EXPIRES_AT} = :expires, "
                    f"{schema.HEARTBEAT_AT} = :beat"
                ),
                # Conditional on the lease still existing and still being held by
                # this holder. Without the holder check, an extension could
                # resurrect a lease that expiry removed and someone else replaced,
                # which would put two workers on the same reserved capacity.
                ConditionExpression=(
                    f"attribute_exists({schema.LEASE_ID}) AND #holder = :holder"
                ),
                ExpressionAttributeNames={"#holder": schema.HOLDER_ID},
                ExpressionAttributeValues={
                    ":expires": {"N": str(int(expires_at.timestamp()))},
                    ":beat": {"S": self._now().isoformat()},
                    ":holder": {"S": self._holder_id},
                },
            )
        except self._ddb.exceptions.ConditionalCheckFailedException:
            # Not transient: the lease is gone or belongs to someone else. Do not
            # recreate it; that would be inventing capacity.
            logger.warning(
                "lease %s is no longer held by %s",
                self._lease.lease_id,
                self._holder_id,
            )
            self._lost.set()
            return False
        except Exception:
            self._failures += 1
            logger.warning(
                "failed to extend lease %s (%d consecutive)",
                self._lease.lease_id,
                self._failures,
                exc_info=True,
            )
            return False

        self._failures = 0
        self._extensions += 1
        return True
