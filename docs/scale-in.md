# Scale-in and the item in progress

Part of the [sample README](../README.md).

Several things stop tasks while a batch runs: target tracking scaling in, the scaling function lowering
the maximum when a budget runs out, the drain to zero, and a deployment. ECS chooses which tasks to stop
without regard to which subscription they hold or whether they are in the middle of a run. A run stopped
part-way has already spent its credits, and its message is redelivered after the visibility timeout, so
the item is paid for twice.

So each worker uses
[task scale-in protection](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection.html)
for the length of one item. For every item, in this order:

1. The `/usage` reading and the budget check, unchanged. A worker at its ceiling stops here, unprotected.
2. Protection on: `PUT $ECS_AGENT_URI/task-protection/v1/state` with
   `{"ProtectionEnabled": true, "ExpiresInMinutes": 20}`.
3. Receive.
4. Run, then store the result and delete the message, or hand the message back.
5. Protection off, `{"ProtectionEnabled": false}`, however steps 3 and 4 ended: an empty receive, a
   failed or timed-out run, an exception, or SIGTERM.

The expiry is `ceil(workflow timeout / 60) + 5` minutes, which is 20 minutes for the 900 s timeout. The
item itself is held differently: the queue's visibility timeout is a 3-minute window that the worker
renews every minute while the run is in progress (`runner/visibility.py`), up to 1.5 times the workflow
timeout. A worker that hangs while protected therefore lets its item go after at most about 22.5 minutes,
and becomes eligible for scale-in when its protection expires.

What ECS does with protected tasks:

- **Target tracking and a lower maximum.** Application Auto Scaling lowers the desired count, and ECS
  stops only unprotected tasks, which are the ones between items. Every task that holds an item finishes
  and acknowledges it. A task on an exhausted subscription then reaches its budget stop before it
  protects itself again, and exits. Other tasks are stopped in their gap between items, or keep working
  until the count fits. A task stopped in its gap holds no item, and costs only a later replacement task
  start. If ECS refuses a task's next request with `DEPLOYMENT_BLOCKED`, which the docs say means the
  service has more protected tasks than its desired count (the state a lower maximum creates), that item
  runs unprotected and can be stopped mid-run, as every item could before this change.
  `TaskProtectionFailed` with `Reason=DeploymentBlocked` counts it.
- **Drain to zero.** The scaling function sets the service to zero only when nothing is visible and
  nothing is in flight, so no task holds an item. A task can be protected during its last, empty long
  poll (up to 20 s); ECS waits for that, and the worker exits on the empty queue anyway.
- **Deployments.** ECS does not stop a protected task, so a rolling deployment waits for each task's
  current item (a run is cut off at 900 s) or for its protection to expire. A worker protects itself
  again before each item, so ECS can only catch it in the gap between items: from turning protection
  off, through the next `/usage` reading (5-9 s), until it turns protection on again. How quickly ECS
  acts on a gap that short is not documented. A deployment in the middle of a long batch can therefore
  wait until tasks exit on an empty queue or at their budget stop. CloudFormation fails a stack update
  and rolls it back after 3 hours, so deploy between batches. If ECS answers a worker's request with
  `DEPLOYMENT_BLOCKED` (the service has more protected tasks than its desired count), that item runs
  unprotected, as every item did before this change. If ECS answers `TASK_STOPPING_OR_STOPPED`, the
  task is already being stopped: the worker takes no further item and ends its drain, so nothing is
  received only to be killed mid-run (measured on the M2 benchmark before this rule: 12 items lost
  mid-run in one level, each waiting out the visibility timeout; `bench/README.md`, M4).
- **SIGTERM.** The worker's handler is unchanged: it stops the heartbeat, returns the lease, and exits
  with 143. With protection, scale-in and deployments send it between items, so the task exits holding no
  item. `StopTask`, Fargate task retirement and crashes are not covered by protection and can still stop
  a task mid-item. Protection is turned off on the way out where the process still can, the lease is
  returned, the CLI is killed, and the item is handed back at once, so another task picks it up
  immediately; the credits the interrupted run spent are lost. If the hand-back cannot be made (the
  process is killed outright), the item reappears once its visibility window lapses: within 3 minutes,
  because the
  dead worker no longer renews it (measured before this window, with a fixed 22.5-minute timeout: crash
  recovery took 1,618 s against about 410 s for a clean batch; `bench/README.md`, M4).

Protection fails open. Without `ECS_AGENT_URI` (`runner.local_batch`, or the container under plain
Docker) nothing is called. A request that fails, times out after 6 s, or is refused logs one warning and
emits `TaskProtectionFailed` in the pool namespace, with `Action` (`Enable` or `Disable`) and `Reason`
(`Unreachable`, `HttpError`, `Refused`, `DeploymentBlocked`, `TaskStopping` or `BadResponse`) as
properties, and the item runs unprotected, except for `TaskStopping`, where the worker stops instead. The agent's
[endpoint](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection-endpoint.html)
reports a refusal from ECS with status 200, so the worker counts a request as done only when the answer
confirms the state it asked for. Alarm on `TaskProtectionFailed` above zero if you want to know when a
run was exposed.

The task role is granted `ecs:UpdateTaskProtection` on `task/<cluster>/*`, which is the call the agent
makes for the worker. `ecs:GetTaskProtection`, which the docs also list, is only for reading the state
back, and the worker never does that.

Verified on AWS with the default `private` egress mode (no internet gateway, NAT or firewall):

- **The agent's call reaches ECS from the task subnets** without an `ecs` interface endpoint. Every
  sample taken during a run read `protectionEnabled: true`, and no `TaskProtectionFailed` was emitted.
- **`ecs:UpdateTaskProtection` alone is enough.** The docs also list `ecs:GetTaskProtection` for the task
  role, but the agent's `PUT` handler calls only the update, and no request was denied.
- **The maximum dropping while a run is in flight.** A pre-item reading crossed the ceiling, the scaler
  lowered the maximum to 0, and ECS reported "unable to scale in due to (reason 1 tasks under protection)".
  The run in flight finished and stored its result, protection went off, and SIGTERM arrived about 5 s
  later, between items. This rests on one run that ended about 14 s after the maximum dropped.

Not verified on AWS:

- **Whether ECS returns `DEPLOYMENT_BLOCKED` while autoscaling lowers the count**, and not only during a
  deployment. It did not occur in the check above.
- **The `inspection` and `open` egress modes.** The call goes to the ECS agent and then to ECS, so it is
  not expected to depend on the mode. If a warning names `RequestCanceled` or `TaskProtectionFailed` shows
  `Reason=Unreachable`, add `ecs` to the interface endpoints in `infra/lib/network-stack.ts`, at about
  $14.60 a month for two zones.
