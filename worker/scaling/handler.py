"""The signal autoscaling reads, and the ceiling only the registry knows.

A task cannot compute this. Backlog per task needs the number of running tasks, which no task
can see, so one scheduled function looks at the whole fleet and publishes a number the target
tracking policy can follow.

Two things are published, for different reasons.

`BacklogPerTask` is queue depth over the tasks that are up or on their way. Depth alone cannot
drive scaling, because depth says nothing about whether the fleet is keeping up: a thousand
items behind two tasks and behind two hundred are the same number and completely different
situations. The denominator is `max(runningTasks + pendingTasks, 1)` so a fleet at zero still
produces a finite value and can start at all.

`PoolClaimableConcurrency` is the sum of `maxConcurrency` over the subscriptions a claim could
be granted on: enabled, and with `creditsUsed` below `creditBudget - CREDIT_MARGIN`. It is the
second bound on capacity. A task beyond that point cannot acquire a subscription: it starts,
fails to claim, exits, and is replaced. It costs money and does nothing. The bound cannot live
in the scaling policy because it changes when an operator enables a subscription or a budget
runs out, with no deployment involved — so this function narrows the scalable target's maximum
directly. `PoolDeclaredConcurrency` is the same sum without the budget test, published so the
gap between the two is visible.

It also sets the service's desired count itself at the two ends of a batch, where target
tracking is too slow or does nothing: to zero when the queue is drained, and off zero when
work arrives at an idle fleet. When it starts the fleet it suspends target tracking's
scale-in until the scale-in alarm left over from the idle period has cleared
(`hold_scale_in`).
"""

from __future__ import annotations

import logging
import math
import os
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final

import boto3

from leaseclient.claim import parse_credit_margin
from leaseclient.lambda_logging import configure_logging
from leaseclient.metrics import Metrics, MetricUnit
from leaseclient.registry import Registry

logger = logging.getLogger(__name__)

FLEET_NAMESPACE: Final = "KiroFleet/Fleet"
"""Separate from the pool's namespace, which carries what individual tasks emit.

Duplicated in `infra/lib/config.ts`; the agreement test asserts the two match, so a
dashboard or a scaling policy cannot end up pointed at a namespace nothing writes to.
"""

BACKLOG_PER_TASK: Final = "BacklogPerTask"
POOL_DECLARED_CONCURRENCY: Final = "PoolDeclaredConcurrency"
POOL_CLAIMABLE_CONCURRENCY: Final = "PoolClaimableConcurrency"
STARTED_FROM_ZERO: Final = "StartedFromZero"
"""Tasks requested by starting an idle fleet directly. Absent on minutes nothing was started."""
POOL_EXHAUSTED: Final = "PoolExhausted"
"""Work is waiting and every enabled subscription is at its credit ceiling. Absent otherwise."""
SCALE_IN_SUSPENDED: Final = "ScaleInSuspended"
"""Target tracking's scale-in is held off this minute (`hold_scale_in`). Absent otherwise.

Normally one or two minutes after a start from zero. Many consecutive minutes mean the
scale-in alarm has not cleared, or its state cannot be read (see the next metric)."""
SCALE_IN_ALARM_READ_FAILED: Final = "ScaleInAlarmReadFailed"
"""Reading the scale-in alarm failed, so the suspension was kept. Absent otherwise."""


@dataclass
class FleetSignal:
    queue_depth: int
    running_tasks: int
    pool_concurrency: int
    desired_tasks: int = 0
    """What the service is set to, as opposed to what is up.

    Read because the drain decision needs it: a service already at zero needs no call, and
    the running count cannot answer that — tasks cycle between zero and the desired count
    while the service keeps replacing workers that found an empty queue.
    """
    pending_tasks: int = 0
    """Tasks the service has placed that are not running yet.

    Read so that a fleet already on its way up is not mistaken for an idle one.
    """
    visible_messages: int = 0
    """Messages waiting to be received, without the ones in flight.

    Starting from zero counts only these. With no task running, an in-flight message is held
    by a task that has already gone and returns to the queue when its visibility timeout ends.
    """
    claimable_concurrency: int = 0
    """`pool_concurrency` without the subscriptions whose budget is used up.

    Read with the claim path's own rule (`SubscriptionRecord.is_claimable`) and margin, so a
    subscription a claim would refuse for credits adds nothing here.
    """

    @property
    def backlog_per_task(self) -> float:
        """Depth over running and pending tasks, with one as the floor on the denominator.

        Pending tasks count because they are already on their way. A start from zero sets the
        count at what target tracking would settle on, and those tasks are pending for about
        half a minute before they run. Divided by running tasks alone, every minute in that
        window would publish the whole backlog, and if the scale-out alarm collected enough
        such datapoints target tracking would multiply the count up to the ceiling.

        Without the floor an empty fleet divides by zero and publishes nothing, so nothing
        ever starts: the metric that is supposed to cause the first task cannot be computed
        until a task exists.
        """
        return self.queue_depth / max(self.running_tasks + self.pending_tasks, 1)

    @property
    def capacity_ceiling(self) -> int:
        """What the fleet should be allowed to reach, given the pool.

        The claimable concurrency, not the declared one. Zero when no subscription can be
        claimed — none enabled, or every enabled one at its credit ceiling — because every
        task would fail to claim, so the honest ceiling is no tasks at all rather than a
        fleet that churns. Measured before this counted credits: with the only subscription
        over its ceiling and two items queued, a task started about every minute, was refused
        for five attempts, and exited, until the queue was purged.
        """
        return max(self.claimable_concurrency, 0)

    @property
    def pool_exhausted(self) -> bool:
        """Work is waiting, subscriptions are enabled, and none of them has budget left.

        Distinct from an empty pool: here an operator has capacity configured, and what they
        act on is a budget (raise it, or wait for the window to reset).
        """
        return (
            self.visible_messages > 0
            and self.pool_concurrency > 0
            and self.claimable_concurrency <= 0
        )


def read_signal(
    sqs: Any,
    ecs: Any,
    registry: Registry,
    queue_url: str,
    cluster: str,
    service: str,
    *,
    credit_margin: Decimal,
) -> FleetSignal:
    """Read the queue, the service and the pool.

    `credit_margin` is the workers' `CREDIT_MARGIN`, parsed by the same function, so this
    counts a subscription as claimable exactly when a claim would be granted on it.
    """
    attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        # Both, because an item being worked on is not backlog but is also not finished.
        # Counting only visible messages would read a fleet mid-batch as idle and scale it in
        # underneath itself.
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )["Attributes"]
    visible = int(attributes["ApproximateNumberOfMessages"])
    in_flight = int(attributes["ApproximateNumberOfMessagesNotVisible"])

    described = ecs.describe_services(cluster=cluster, services=[service])
    services = described.get("services") or []
    running = int(services[0]["runningCount"]) if services else 0
    desired = int(services[0]["desiredCount"]) if services else 0
    pending = int(services[0].get("pendingCount", 0)) if services else 0

    # One read of the state index. It projects every attribute, so the budget figures come
    # with the records and the budget test needs nothing more.
    enabled = registry.list_enabled()
    declared = sum(record.max_concurrency for record in enabled)
    claimable = sum(
        record.max_concurrency for record in enabled if record.is_claimable(credit_margin)
    )

    return FleetSignal(
        queue_depth=visible + in_flight,
        running_tasks=running,
        pool_concurrency=declared,
        desired_tasks=desired,
        pending_tasks=pending,
        visible_messages=visible,
        claimable_concurrency=claimable,
    )


def tasks_to_start(signal: FleetSignal, target_backlog_per_task: float, ceiling: int) -> int:
    """How many tasks an idle fleet should start for the work now waiting, or zero.

    Target tracking scales out only when the metric is above its target. At zero tasks the
    metric is the whole backlog, so a batch of `targetBacklogPerTask` items or fewer never
    starts: measured as `queueDepth 2, runningTasks 0` every minute for twelve minutes with the
    service left at zero. Larger batches do start, but only after the scale-out alarm has
    collected its datapoints, about four minutes from seeding to a task.

    The count is what target tracking would settle on, `ceil(visible / target)`, at least one,
    and never above `ceiling`, which is the same maximum the scalable target is held at. The
    metric divides by running and pending tasks, so from the next minute, while these tasks
    are still pending and after they run, it is at or below the target (or the count is
    already at the ceiling), and target tracking has no reason to move the count up. Nor
    does its scale-in take the fleet back to zero while work is counted: the ECS
    documentation says it rounds so as never to remove too much
    capacity, and `current * metric / target` rounded up stays at one or more for any metric
    above zero. Reaching zero is left to `drain_to_zero`.

    Only an idle fleet qualifies: nothing desired, nothing running, nothing pending. Any of
    those means the service or target tracking is already acting, and this would be a second
    writer racing it. That condition also makes the call idempotent: once the count is set,
    the next minute sees a desired count above zero and does nothing.
    """
    if signal.desired_tasks or signal.running_tasks or signal.pending_tasks:
        return 0
    if signal.visible_messages <= 0 or ceiling <= 0:
        return 0
    wanted = math.ceil(signal.visible_messages / target_backlog_per_task)
    return min(max(wanted, 1), ceiling)


def start_from_zero(ecs: Any, cluster: str, service: str, count: int) -> bool:
    """Set an idle service to `count` tasks. The mirror of `drain_to_zero`."""
    if count <= 0:
        return False
    ecs.update_service(cluster=cluster, service=service, desiredCount=count)
    logger.info(
        "work is waiting and no task is up; set %s to %d task(s)", service, count
    )
    return True


def drain_to_zero(ecs: Any, cluster: str, service: str, desired: int) -> bool:
    """Set the service to zero when the batch is finished, rather than waiting it out.

    Target tracking cannot do this promptly. Its scale-in alarm is created with fifteen
    evaluation periods — measured, from a deployed policy whose alarm reported "15 datapoints
    were less than the threshold" — so a drained queue takes about fifteen minutes to reach
    zero. That is not a tuning value this stack can lower; Application Auto Scaling sets it.

    Fifteen minutes would be a detail if the fleet idled quietly, and it does not. A worker
    that finds an empty queue exits, the service replaces it, and that repeats: the fleet
    churns through task starts for the whole window, paying for container starts and claiming
    subscriptions to do nothing. Observed directly — desired stayed at two for sixteen minutes
    after the last result was stored, with tasks cycling throughout.

    So the condition is checked here, where the queue depth is already known, and acted on
    immediately. "Drains to zero when the queue empties" then means what it says.

    With nothing visible and nothing in flight, no task holds an item. A task can still be
    protected from scale-in during its last, empty long poll; ECS waits for that receive to
    return and the protection to be turned off, and the worker exits on the empty queue
    anyway.
    """
    if desired == 0:
        return False

    # Both counts, and this is the safety property. An item in flight is being worked on by a
    # task that is about to be stopped, and stopping it mid-run pays for the work without
    # keeping it. Visible-only would scale in on top of a batch still being processed.
    ecs.update_service(cluster=cluster, service=service, desiredCount=0)
    logger.info("queue is empty and nothing is in flight; set %s to zero", service)
    return True


def narrow_capacity(
    autoscaling: Any,
    resource_id: str,
    configured_maximum: int,
    ceiling: int,
    *,
    scale_in_suspended: bool = False,
) -> int:
    """Hold the scalable target's maximum at the lower of the two bounds.

    The quota bound is static and lives in the stack. This is the other one, and it is
    applied here because only the registry knows it. Written every run rather than only on a
    change: the call is idempotent, and comparing first would need this function to trust a
    read of state it does not own. Raising a budget or a window reset therefore lifts the
    maximum on the next run.

    A maximum below the current count is not only a cap on scale-out. Application Auto
    Scaling lowers the desired count to fit, and ECS chooses which tasks to stop without
    regard to which subscription they hold, so tasks on a subscription with budget left can
    be the ones stopped. Scale-in protection is what keeps their runs safe: a worker is
    protected from before it receives an item until the item is acknowledged
    (`runner/protection.py`), so ECS stops only tasks that are between items. A protected
    task finishes its item and is stopped once its protection is off, or exits on its own at
    its budget stop. A task stopped some other way mid-run (`StopTask`, task retirement)
    returns its lease, and its item becomes visible again when the visibility timeout ends.
    A task whose next protection request ECS refuses with `DEPLOYMENT_BLOCKED` (the docs say
    this means more tasks are protected than the desired count, the state a lower maximum
    creates) runs that item unprotected and can be stopped mid-run, as any item could before
    protection. `TaskProtectionFailed` counts it.

    `scale_in_suspended` is written on the same call, every run, for the same reason: the
    value is decided here (`hold_scale_in`), and writing it explicitly means no run leaves a
    suspension behind by omission. Scale-out and scheduled scaling are never suspended.
    """
    maximum = min(configured_maximum, ceiling)
    autoscaling.register_scalable_target(
        ServiceNamespace="ecs",
        ResourceId=resource_id,
        ScalableDimension="ecs:service:DesiredCount",
        MinCapacity=0,
        MaxCapacity=maximum,
        SuspendedState={
            "DynamicScalingInSuspended": scale_in_suspended,
            "DynamicScalingOutSuspended": False,
            "ScheduledScalingSuspended": False,
        },
    )
    return maximum


def scale_in_is_suspended(autoscaling: Any, resource_id: str) -> bool:
    """Whether the previous run left target tracking's scale-in suspended."""
    targets = autoscaling.describe_scalable_targets(
        ServiceNamespace="ecs",
        ResourceIds=[resource_id],
        ScalableDimension="ecs:service:DesiredCount",
    ).get("ScalableTargets") or []
    if not targets:
        return False
    state = targets[0].get("SuspendedState") or {}
    return bool(state.get("DynamicScalingInSuspended"))


def low_alarm_in_alarm(cloudwatch: Any, resource_id: str) -> bool | None:
    """The state of target tracking's scale-in alarm: True in ALARM, None if not found.

    Application Auto Scaling creates the alarm and names it, so it is found by the prefix it
    uses, `TargetTracking-<resource id>-AlarmLow-`, rather than by a name the stack could pass.
    """
    alarms = cloudwatch.describe_alarms(
        AlarmNamePrefix=f"TargetTracking-{resource_id}-AlarmLow-",
        AlarmTypes=["MetricAlarm"],
    ).get("MetricAlarms") or []
    if not alarms:
        return None
    return any(alarm.get("StateValue") == "ALARM" for alarm in alarms)


def hold_scale_in(
    starting: bool,
    currently_suspended: bool,
    queue_depth: int,
    low_alarm: Callable[[], bool | None],
) -> bool:
    """Whether target tracking's scale-in should be suspended for the next minute.

    The problem this answers was measured on a deployed fleet. While the fleet idles the
    metric is 0, so the scale-in alarm (15 datapoints below the target) sits in ALARM, and an
    alarm in ALARM invokes its scaling policy every minute; at zero tasks that removes nothing.
    A start from zero then sets the desired count directly, outside Application Auto Scaling,
    so its scale-in cooldown does not apply, and within the minute the still-ALARM alarm acted
    on fifteen idle datapoints: 25 was cut to 2 (and on another day to 3) about 27 s after the
    start. The tasks that remained held items unprotected (`DEPLOYMENT_BLOCKED`), and some
    were stopped mid-run.

    So scale-in is suspended in the same run that starts the fleet, before the desired count
    is set, and stays suspended until the alarm has left ALARM. That takes one datapoint at or
    above its threshold, normally the next minute. From then on an ALARM needs fifteen new low
    minutes, which is target tracking working as designed, and the suspension is lifted.

    A batch whose start leaves the metric below the alarm's threshold (three items on two
    tasks) keeps scale-in suspended until the queue drains, when `drain_to_zero` ends the batch
    anyway. A failed alarm lookup keeps the suspension too, for the same reason: holding the
    count for a batch is the safe side, and an empty queue lifts it.
    """
    if starting:
        return True
    if not currently_suspended or queue_depth == 0:
        return False
    try:
        state = low_alarm()
    except Exception:
        logger.warning("could not read the scale-in alarm; keeping scale-in suspended", exc_info=True)
        return True
    # Not found: nothing stale can fire, so there is nothing to hold off.
    return bool(state)


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    configure_logging()

    queue_url = os.environ["WORK_QUEUE_URL"]
    cluster = os.environ["CLUSTER_NAME"]
    service = os.environ["SERVICE_NAME"]
    configured_maximum = int(os.environ["MAX_TASKS"])
    target_backlog_per_task = float(os.environ["TARGET_BACKLOG_PER_TASK"])
    if target_backlog_per_task <= 0:
        raise ValueError(
            f"TARGET_BACKLOG_PER_TASK must be above zero, got {target_backlog_per_task}"
        )
    # The workers' setting, read by the workers' parser with the workers' default. The stack
    # passes the one configured value to both.
    credit_margin = parse_credit_margin(os.environ.get("CREDIT_MARGIN"))

    ecs = boto3.client("ecs")
    signal = read_signal(
        sqs=boto3.client("sqs"),
        ecs=ecs,
        registry=Registry(boto3.client("dynamodb")),
        queue_url=queue_url,
        cluster=cluster,
        service=service,
        credit_margin=credit_margin,
    )

    metrics = Metrics(namespace=FLEET_NAMESPACE)
    metrics.emit(BACKLOG_PER_TASK, signal.backlog_per_task, MetricUnit.COUNT)
    metrics.emit(POOL_DECLARED_CONCURRENCY, signal.pool_concurrency, MetricUnit.COUNT)
    metrics.emit(
        POOL_CLAIMABLE_CONCURRENCY, signal.claimable_concurrency, MetricUnit.COUNT
    )
    if signal.pool_exhausted:
        # Nothing will start until a budget is raised or a window resets. Said here because
        # no task runs to say it: the fleet stays at zero and the queue simply does not move.
        logger.warning(
            "%d item(s) waiting, but every enabled subscription is at its credit ceiling "
            "(creditBudget - %s); starting nothing",
            signal.visible_messages,
            credit_margin,
        )
        metrics.emit(POOL_EXHAUSTED, 1, MetricUnit.COUNT)

    resource_id = f"service/{cluster}/{service}"
    autoscaling = boto3.client("application-autoscaling")
    maximum = min(configured_maximum, signal.capacity_ceiling)

    # The two direct writes are exclusive: draining needs an empty queue, including in-flight
    # messages, and starting needs at least one visible message.
    count = 0 if signal.queue_depth == 0 else tasks_to_start(
        signal, target_backlog_per_task, ceiling=maximum
    )
    def read_low_alarm() -> bool | None:
        # Counted here, where the metrics sink is, so a suspension held by failing reads
        # (throttling, a missing grant) shows on a graph instead of only in a log line.
        try:
            return low_alarm_in_alarm(boto3.client("cloudwatch"), resource_id)
        except Exception:
            metrics.emit(SCALE_IN_ALARM_READ_FAILED, 1, MetricUnit.COUNT)
            raise

    suspend_scale_in = hold_scale_in(
        starting=count > 0,
        currently_suspended=(
            count == 0
            and signal.queue_depth > 0
            and scale_in_is_suspended(autoscaling, resource_id)
        ),
        queue_depth=signal.queue_depth,
        low_alarm=read_low_alarm,
    )
    if suspend_scale_in:
        metrics.emit(SCALE_IN_SUSPENDED, 1, MetricUnit.COUNT)

    # Written before the start, so the suspension is in place when the desired count moves.
    narrow_capacity(
        autoscaling,
        resource_id=resource_id,
        configured_maximum=configured_maximum,
        ceiling=signal.capacity_ceiling,
        scale_in_suspended=suspend_scale_in,
    )

    drained = False
    started = 0
    if signal.queue_depth == 0:
        drained = drain_to_zero(ecs, cluster, service, desired=signal.desired_tasks)
    elif start_from_zero(ecs, cluster, service, count):
        started = count
        metrics.emit(STARTED_FROM_ZERO, started, MetricUnit.COUNT)

    result = {
        "queueDepth": signal.queue_depth,
        "runningTasks": signal.running_tasks,
        "backlogPerTask": round(signal.backlog_per_task, 3),
        "poolDeclaredConcurrency": signal.pool_concurrency,
        "poolClaimableConcurrency": signal.claimable_concurrency,
        "poolExhausted": signal.pool_exhausted,
        "maxCapacity": maximum,
        "drainedToZero": drained,
        "startedFromZero": started,
        "scaleInSuspended": suspend_scale_in,
    }
    logger.info("fleet signal %s", result)
    return result
