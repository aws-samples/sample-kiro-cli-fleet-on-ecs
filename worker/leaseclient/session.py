"""Holding a subscription for the length of a worker's run.

Ties together the three things a worker must not get wrong: acquire capacity,
keep saying it is alive, and give the capacity back however the run ends. The last
one is the reason this exists as a context manager rather than as three calls a
worker is trusted to make in order.

Release is best effort by design. Expiry is what actually guarantees capacity comes
back, so nothing here assumes release runs. It is an optimization: without it the
next worker waits out a lease lifetime for capacity that is already free.
"""

from __future__ import annotations

import atexit
import logging
import os
import signal
import threading
from contextlib import contextmanager
from types import FrameType

from leaseclient.claim import Allocator, Lease
from leaseclient.heartbeat import Heartbeat

logger = logging.getLogger(__name__)


class PoolSaturated(RuntimeError):
    """No subscription was available. Not a failure; the fleet outgrew the pool."""


class Session:
    """A held lease, with the key material available and liveness maintained."""

    def __init__(self, lease: Lease, heartbeat: Heartbeat, api_key: str) -> None:
        self._lease = lease
        self._heartbeat = heartbeat
        self._api_key = api_key

    @property
    def lease(self) -> Lease:
        return self._lease

    @property
    def subscription_id(self) -> str:
        return self._lease.subscription_id

    @property
    def lost(self) -> bool:
        return self._heartbeat.lost

    def raise_if_lost(self) -> None:
        self._heartbeat.raise_if_lost()

    def environment(self) -> dict[str, str]:
        """The environment a child process needs to authenticate.

        Returned as a mapping to hand to a subprocess, not exported into this
        process. Nothing else should read the key, and it must not be logged or
        written to a file: an inherited environment is the narrowest way to get it
        into the CLI.
        """
        return {"KIRO_API_KEY": self._api_key}

    def __repr__(self) -> str:
        # Explicit, so the key cannot reach a log through a default repr.
        return (
            f"Session(subscription_id={self._lease.subscription_id!r}, "
            f"lease_id={self._lease.lease_id!r})"
        )


@contextmanager
def hold_subscription(
    allocator: Allocator,
    dynamodb,
    holder_id: str | None = None,
    leases_table: str | None = None,
    install_signal_handler: bool = True,
):
    """Claim a subscription, keep it alive, and give it back on the way out.

    Raises PoolSaturated when nothing is available, which a worker should treat as a
    clean exit rather than an error: the fleet has more tasks than the pool can
    serve, and a task that crashes would be replaced and try again.
    """
    holder_id = holder_id or default_holder_id()

    lease = allocator.claim(holder_id)
    if lease is None:
        raise PoolSaturated(
            "no subscription had concurrency headroom and an unexhausted budget"
        )

    heartbeat = Heartbeat(
        dynamodb,
        lease=lease,
        holder_id=holder_id,
        lifetime=allocator.config.lease_lifetime,
        interval=allocator.config.heartbeat_interval,
        table_name=leases_table,
    )

    released = threading.Event()

    def release_once() -> None:
        if released.is_set():
            return
        released.set()
        try:
            allocator.release(lease)
        except Exception:
            # Expiry is the guarantee; a failed release costs a lease lifetime of
            # capacity, not correctness. Never let it mask the original failure.
            logger.warning("release failed for %s", lease.lease_id, exc_info=True)

    # Container stop sends SIGTERM before SIGKILL. Returning capacity there is the
    # difference between the next worker starting now and waiting out a lifetime.
    previous_handler = None
    if install_signal_handler:
        previous_handler = _install_sigterm_handler(release_once, heartbeat)
    atexit.register(release_once)

    heartbeat.start()
    try:
        api_key = allocator.resolve_key(lease)
        yield Session(lease, heartbeat, api_key)
    finally:
        heartbeat.stop()
        release_once()
        atexit.unregister(release_once)
        if previous_handler is not None:
            signal.signal(signal.SIGTERM, previous_handler)


def _install_sigterm_handler(release, heartbeat: Heartbeat):
    """Release on SIGTERM, then let the default behavior proceed.

    Only installed on the main thread; signal handlers cannot be registered
    elsewhere, and a worker that runs allocation off the main thread simply relies
    on expiry.
    """
    if threading.current_thread() is not threading.main_thread():
        return None

    try:
        previous = signal.getsignal(signal.SIGTERM)

        def handler(signum: int, frame: FrameType | None) -> None:
            logger.info("SIGTERM received; returning capacity before exiting")
            heartbeat.stop(timeout=1.0)
            release()
            if callable(previous) and previous not in (
                signal.SIG_IGN,
                signal.SIG_DFL,
            ):
                previous(signum, frame)
            else:
                raise SystemExit(143)

        signal.signal(signal.SIGTERM, handler)
        return previous
    except (ValueError, OSError):
        logger.debug("could not install a SIGTERM handler; relying on expiry")
        return None


def default_holder_id() -> str:
    """Identify this worker in a way that traces back to a running task.

    ECS exposes task metadata through an environment variable; outside a task the
    process id is enough to tell two local runs apart.
    """
    for variable in ("ECS_TASK_ARN", "ECS_CONTAINER_METADATA_URI_V4"):
        value = os.environ.get(variable)
        if value:
            return value
    return f"local-pid-{os.getpid()}"
