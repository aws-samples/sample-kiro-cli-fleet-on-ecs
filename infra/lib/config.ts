/**
 * Fleet parameters, read once from context so no stack reaches for a raw value.
 *
 * Two of these carry more weight than they look. The maximum task count is bounded by a
 * new account's Fargate quota, because a sample that needs a quota increase before it can
 * be deployed is a sample nobody runs. And the target architecture has to match what the
 * worker image was built for; a mismatch produces an emulated fleet, which would change
 * every timing the image is later used to measure.
 */

import { Construct } from 'constructs';
import { Duration, Stack } from 'aws-cdk-lib';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as logs from 'aws-cdk-lib/aws-logs';

export interface FleetConfig {
  /** Upper bound on concurrent tasks. Low by default; see the class comment. */
  readonly maxTasks: number;

  /**
   * One task's size, in Fargate's units.
   *
   * These are not tuned against a measurement, and saying so is more useful than a
   * confident number: nothing here has profiled a run's processor use. What the default
   * is chosen against is two things that are known.
   *
   * Memory has a floor. The CLI's chat binary is 829 MB, and a cgroup is charged for the
   * file-backed pages of a binary it executes, so a task at 1 GB spends its life
   * reclaiming pages it is about to need again. 2 GB is the first size that does not.
   *
   * Processor size decides how large the default fleet can be, because the quota is
   * counted in vCPUs rather than tasks — see `DEFAULT_FARGATE_ONDEMAND_VCPU_QUOTA`. Half
   * a vCPU is what lets the default fleet be ten tasks instead of six.
   *
   * Before changing either, read `MemoryUtilization` and `CpuUtilization` from a real
   * batch. The dashboard carries both.
   */
  readonly taskCpu: number;
  readonly taskMemoryMiB: number;

  /**
   * Which image the task definition runs.
   *
   * `latest` for a hand deployment. The pipeline passes the commit it built, so a
   * deployed task definition names the image built from the same commit rather than
   * whatever `latest` happens to point at by the time a task starts.
   */
  readonly imageTag: string;

  /**
   * Per-task processor and memory metrics from the container agent.
   *
   * On by default despite being billed, because the numbers above are unmeasured and this
   * is what measures them. Turn it off once the task size is settled.
   */
  readonly containerInsights: boolean;

  /**
   * Queue items per running task that autoscaling aims to hold.
   *
   * The signal is a ratio rather than raw depth because depth alone says nothing about
   * whether the fleet is keeping up: a thousand items behind two tasks and behind two
   * hundred are the same number and completely different situations.
   *
   * Target tracking alone cannot start a small batch. With the fleet at zero the metric is
   * `backlog / max(running + pending, 1)` = the whole backlog, and target tracking only scales
   * out when the metric exceeds the target. So a batch of `targetBacklogPerTask` items or fewer would
   * never start: the metric sits at or below the target and there are already no tasks to
   * remove. It shipped at 10, which stopped the README's three-file example; at 2, two items
   * sat queued for twelve minutes on a deployed fleet.
   *
   * The scaling function therefore takes an idle fleet off zero itself, setting the desired
   * count to `ceil(visible / targetBacklogPerTask)` within the fleet's ceiling, and it reads
   * this value from its environment so both writers aim at the same number. Target tracking
   * then handles the batch once a task is up.
   *
   * Two suits the workload: a run takes minutes, so holding ten items behind one task means
   * ten times the wait for the last of them.
   */
  readonly targetBacklogPerTask: number;

  /**
   * Credits held back below each subscription's budget, as the string the worker parses.
   *
   * One value with two readers. The workers refuse a claim, and stop draining, once
   * `creditsUsed >= creditBudget - margin`. The scaling function counts a subscription's
   * concurrency toward the fleet's ceiling only below that same line, so it does not start tasks
   * a claim would refuse. Both receive it as `CREDIT_MARGIN` from here, and both parse it with
   * `parse_credit_margin` in `worker/leaseclient/claim.py`. If the two disagreed, a scaler with
   * the smaller margin would start tasks that exit without work for that band of credits.
   *
   * The default is the worker's `DEFAULT_CREDIT_MARGIN`, which a test pins. See the README's
   * Credits section for how to size it from `RunCredits`.
   */
  readonly creditMargin: string;

  /**
   * Worst case for one workflow run. The queue's visibility timeout is not derived from
   * this: the worker renews a short window while a run is in progress, so a slow item is
   * kept however long it takes (see `VISIBILITY_WINDOW_SECONDS` in `queue-stack.ts`).
   */
  readonly workflowTimeout: Duration;

  readonly architecture: ecs.CpuArchitecture;
  readonly architectureName: 'arm64' | 'x86_64';

  /** Workspace storage is scratch. Unbounded scratch is a bill that grows quietly. */
  readonly workspaceExpiry: Duration;

  /**
   * How long log streams are kept.
   *
   * CloudWatch accepts only a fixed set of values, so this is validated rather than cast. A
   * cast would compile, deploy, and then reject the number at the API, which is the most
   * expensive place to learn that 45 is not a retention period.
   */
  readonly logRetention: logs.RetentionDays;
  readonly logRetentionDays: number;

  /**
   * The public endpoints with no PrivateLink equivalent.
   *
   * Authentication and MCP configuration reach these, so egress cannot be closed entirely.
   * Named here so the egress path can be constrained to them rather than left open.
   */
  readonly kiroEgressDomains: string[];

  /**
   * How the one path that has to leave the VPC is constrained.
   *
   * Three options rather than a switch, because the two that do something differ in strength
   * and in price by a factor of several hundred, and picking between them is a real decision
   * rather than an on/off.
   *
   *   'private'     No route to the internet at all. No NAT gateway, no firewall, nothing to
   *                 allowlist. Every destination this workload needs has a VPC endpoint,
   *                 including the Kiro service — the `q` endpoint serves private DNS for
   *                 `management`, `runtime` and `telemetry` under `kiro.dev`, which is how
   *                 inference reaches the model without leaving the VPC. A subnet with no
   *                 default route is a stronger control than any allowlist and costs nothing.
   *
   *   'inspection'  A NAT gateway, with traffic routed through AWS Network Firewall and matched
   *                 against the named domains on the TLS server name. For a workflow that needs
   *                 something off the internet — cloning a public repository, say. $0.395 an
   *                 hour per firewall endpoint, about $288 a month, accruing at zero tasks.
   *
   *   'open'        A NAT gateway and port 443. Not destination control at all.
   *
   * The default is 'private', and it took a deployment to establish that it works. Three
   * earlier conclusions about this went the other way, each inferred from a capture taken on a
   * laptop — where there are no VPC endpoints, so of course traffic went to public addresses. A
   * capture says which hosts are called; it cannot say how they route somewhere else.
   *
   * DNS Firewall was tried as a middle option and dropped. It evaluates the private DNS of the
   * interface endpoints too, so an allowlist naming the two Kiro domains stopped `sqs`,
   * `secretsmanager`, `logs`, `sts`, `ecr` and `q` from resolving at all. Making it work
   * requires `*.amazonaws.com`, which permits an attacker's own bucket — so the cheap control
   * bought a much wider allowlist, and with 'private' there is nothing to allowlist.
   */
  readonly egressControl: 'private' | 'inspection' | 'open';

  /**
   * Which workflow in the image a task runs, relative to the image's workflow directory.
   *
   * A path rather than a copy of the definition: the workflow is built into the image so
   * that the prompt that ran can be tied to a commit, and naming it here would not change
   * which prompts exist.
   */
  readonly workflowPath: string;

  /**
   * Where Kiro writes its daily per-user activity report, if you have enabled it.
   *
   * Optional, and absent by default: the report needs a Kiro Enterprise profile and a bucket
   * the operator creates in the profile's account and Region, so a sample that required one
   * would not deploy for anyone else. Without it the workers' `/usage` readings are the only
   * automated source of `creditsUsed`. With it, a daily function raises `creditsUsed` to the
   * report's month-to-date figure where that is higher.
   *
   * Set `fleet:activityReportBucket` and `fleet:activityReportPrefix` (the prefix you gave
   * Kiro, without the `AWSLogs/...` part). `fleet:activityReportRegion` defaults to the
   * stack's Region.
   */
  readonly activityReport?: ActivityReportConfig;
}

export interface ActivityReportConfig {
  readonly bucketName: string;
  /** The prefix entered in the Kiro console, without slashes at either end. */
  readonly prefix: string;
  /** The Kiro profile's Region, which is the bucket's Region. */
  readonly region: string;
}

export function fleetConfig(scope: Construct): FleetConfig {
  const stack = Stack.of(scope);
  const context = <T>(key: string, fallback: T): T =>
    (stack.node.tryGetContext(key) as T) ?? fallback;

  const architectureName = context<'arm64' | 'x86_64'>('fleet:targetArchitecture', 'arm64');
  const retentionDays = Number(context('fleet:logRetentionDays', 30));

  return {
    maxTasks: Number(context('fleet:maxTasks', 10)),
    taskCpu: Number(context('fleet:taskCpu', 512)),
    taskMemoryMiB: Number(context('fleet:taskMemoryMiB', 2048)),
    imageTag: context('fleet:imageTag', 'latest'),
    // `??` rather than `||`, so an explicit `false` in context is honoured.
    containerInsights: context<boolean>('fleet:containerInsights', true),
    targetBacklogPerTask: Number(context('fleet:targetBacklogPerTask', 2)),
    creditMargin: asCreditMargin(context<string | number>('fleet:creditMargin', '1')),
    workflowTimeout: Duration.seconds(Number(context('fleet:workflowTimeoutSeconds', 900))),
    architecture:
      architectureName === 'arm64' ? ecs.CpuArchitecture.ARM64 : ecs.CpuArchitecture.X86_64,
    architectureName,
    workspaceExpiry: Duration.days(Number(context('fleet:workspaceExpiryDays', 7))),
    logRetentionDays: retentionDays,
    logRetention: asRetention(retentionDays),
    kiroEgressDomains: context<string[]>('fleet:kiroEgressDomains', []),
    egressControl: context<'private' | 'inspection' | 'open'>(
      'fleet:egressControl',
      'private',
    ),
    workflowPath: context('fleet:workflowPath', 'examples/repo-file-summary'),
    activityReport: activityReportConfig(
      context<string | undefined>('fleet:activityReportBucket', undefined),
      context<string | undefined>('fleet:activityReportPrefix', undefined),
      context<string>('fleet:activityReportRegion', stack.region),
    ),
  };
}

function activityReportConfig(
  bucketName: string | undefined,
  prefix: string | undefined,
  region: string,
): ActivityReportConfig | undefined {
  if (!bucketName) {
    return undefined;
  }
  const trimmed = (prefix ?? '').replace(/^\/+|\/+$/g, '');
  if (!trimmed) {
    // Kiro refuses to write the report to the bucket root, so a bucket without a prefix is a
    // configuration mistake rather than a layout to support.
    throw new Error(
      'fleet:activityReportBucket is set but fleet:activityReportPrefix is not. Use the ' +
        'prefix entered in the Kiro console for user activity reports.',
    );
  }
  return { bucketName, prefix: trimmed, region };
}

/**
 * What `parse_credit_margin` accepts, checked at synthesis.
 *
 * The worker would exit with a configuration error and the scaling function would fail every
 * minute, so a bad value is cheaper to refuse here. Plain decimals only: not negative, at most
 * two places, because `/usage` reports credits to 0.01.
 */
function asCreditMargin(raw: string | number): string {
  const value = String(raw).trim();
  if (!/^\d+(\.\d{1,2})?$/.test(value)) {
    throw new Error(
      `fleet:creditMargin is ${JSON.stringify(raw)}. Use a non-negative number with at most ` +
        'two decimal places, such as 1 or 0.25.',
    );
  }
  return value;
}

function asRetention(days: number): logs.RetentionDays {
  const permitted = Object.values(logs.RetentionDays).filter(
    (value): value is logs.RetentionDays => typeof value === 'number',
  );
  if (!permitted.includes(days as logs.RetentionDays)) {
    throw new Error(
      `fleet:logRetentionDays is ${days}, which CloudWatch does not accept. ` +
        `Choose one of: ${permitted.sort((a, b) => a - b).join(', ')}.`,
    );
  }
  return days as logs.RetentionDays;
}

/** Where the image keeps its workflow definitions. Set by `worker/Dockerfile`. */
export const IMAGE_WORKFLOWS_DIR = '/opt/workflows';

/**
 * The signal autoscaling reads, and who publishes it.
 *
 * Separate from the pool's namespace because the two have different authors and different
 * lifetimes: the pool metrics come out of every task as embedded metric format, and these
 * come from one scheduled function that can see the whole fleet. A task cannot compute
 * backlog per task, because it does not know how many tasks are running.
 */
export const FLEET_METRICS = {
  namespace: 'KiroFleet/Fleet',
  /**
   * Queue depth over the tasks that are up or on their way, with
   * `max(runningTasks + pendingTasks, 1)` as the denominator.
   */
  backlogPerTask: 'BacklogPerTask',
  /** Sum of `maxConcurrency` over enabled subscriptions, whatever their budgets. */
  poolDeclaredConcurrency: 'PoolDeclaredConcurrency',
  /**
   * The same sum over subscriptions a claim could be granted on: enabled and below
   * `creditBudget - CREDIT_MARGIN`. The pool's own ceiling, and what the fleet maximum is held at.
   */
  poolClaimableConcurrency: 'PoolClaimableConcurrency',
  /** Work is waiting and every enabled subscription is at its credit ceiling. */
  poolExhausted: 'PoolExhausted',
  /** One count per stopped task, dimensioned by `Reason` (see `TASK_STOP_REASONS`). */
  taskStopped: 'TaskStopped',
} as const;

/**
 * Where the worker's own metrics land.
 *
 * Duplicated from `DEFAULT_NAMESPACE` in `worker/leaseclient/metrics.py`; the agreement
 * test asserts the two match, so a dashboard cannot end up pointed at a namespace nothing
 * writes to.
 */
export const POOL_METRICS_NAMESPACE = 'KiroFleet/SubscriptionPool';

/**
 * The dimension values the dashboard draws a series for.
 *
 * Copied from the enums in `worker/runner/workflow.py` and
 * `worker/leaseclient/metrics.py`, and the agreement test asserts they match. A series naming
 * a value the worker never emits is a flat line, which on a dashboard is indistinguishable
 * from a condition that is not happening — so a renamed verdict would quietly stop being
 * visible rather than break anything.
 */
export const RUN_VERDICTS = [
  'Succeeded',
  'CredentialRejected',
  'StartupFailed',
  'Throttled',
  'NoOutput',
  'WroteOutsideOutput',
  'Failed',
  'TimedOut',
] as const;

/**
 * Why a claim ended. What separates a saturated pool from workers contending.
 *
 * `BudgetExhausted` counts claims refused because the recorded `creditsUsed` had reached
 * `creditBudget - CREDIT_MARGIN`. Workers record `creditsUsed` from `/usage` before every work
 * item, so it rises as the pool is used; a worker that reaches the ceiling mid-drain stops and
 * emits the separate `BudgetExhausted` metric instead.
 */
export const CLAIM_OUTCOMES = [
  'Success',
  'NoConcurrency',
  'BudgetExhausted',
  'Saturated',
] as const;

/**
 * Why a task stopped. Copied from `StopReason` in `worker/taskstopped/handler.py`; the agreement
 * test asserts they match.
 *
 * `Exited` and `ScaledIn` are the normal ends. `Crashed`, `Killed` and `FailedToStart` are the
 * ones a task cannot report itself, which is why the count is taken from the ECS event.
 */
export const TASK_STOP_REASONS = [
  'Exited',
  'ScaledIn',
  'Killed',
  'Crashed',
  'FailedToStart',
  'Other',
] as const;

/** How capacity came back. `DriftCorrection` rising is a bug, not a workload. */
export const RECOVERY_MECHANISMS = ['Expiry', 'DriftCorrection'] as const;

/**
 * What one `/usage` reading achieved. Copied from `UsageReadingOutcome` in
 * `worker/leaseclient/metrics.py`; the agreement test asserts they match.
 *
 * `ReadFailed` is the one to watch. Readings fail open, so the fleet keeps working on the last
 * recorded figure, and a steady stream of failures means the budget is being enforced on stale
 * data.
 */
export const USAGE_READING_OUTCOMES = [
  'Recorded',
  'Unchanged',
  'WindowAdopted',
  'Stale',
  'ReadFailed',
  'WriteFailed',
] as const;

/**
 * What a brand new account is allowed to run on Fargate, in vCPUs.
 *
 * Read from the API rather than assumed, and the reading is the point:
 *
 *   aws service-quotas get-aws-default-service-quota \
 *     --service-code fargate --quota-code L-3032A538      ->  6
 *   aws service-quotas get-service-quota \
 *     --service-code fargate --quota-code L-3032A538      ->  4000   (this account)
 *
 * The same quota in a used account reads in the thousands, because increases accumulate
 * and are never given back. So an author checking their own account concludes there is no
 * constraint, and ships a sample that fails on the reader's first deployment with a quota
 * error rather than anything to do with this pattern.
 *
 * The quota counts vCPUs, not tasks, which is why the fleet ceiling and the task size are
 * one decision and not two: ten tasks at half a vCPU fit, and ten at one vCPU do not.
 */
export const DEFAULT_FARGATE_ONDEMAND_VCPU_QUOTA = 6;

/** vCPUs the fleet would hold at its ceiling. */
export function peakVcpu(config: FleetConfig): number {
  return (config.maxTasks * config.taskCpu) / 1024;
}

/**
 * Names and attributes shared with the worker.
 *
 * Duplicated from `worker/leaseclient/schema.py` because the two languages cannot share a
 * definition. A test asserts they agree, so a rename that touches only one side fails a
 * build instead of a deployment.
 */
export const POOL_SCHEMA = {
  subscriptions: {
    partitionKey: 'subscriptionId',
    stateAttribute: 'state',
    byStateIndex: 'byState',
  },
  leases: {
    partitionKey: 'leaseId',
    subscriptionAttribute: 'subscriptionId',
    bySubscriptionIndex: 'bySubscription',
    ttlAttribute: 'expiresAt',
  },
} as const;

/**
 * The pipeline's parameters, read from context at the app level.
 *
 * Separate from `fleetConfig` because the pipeline is deployed on its own, and because its
 * inputs are an operator's — a connection and a repository — rather than the fleet's. Returns
 * `undefined` when no connection and repository are given, which is the signal to not build a
 * pipeline at all: the fleet synthesizes and deploys by hand without one, and only an operator
 * who has authorised a connection opts in.
 */
export interface PipelineConfig {
  readonly connectionArn: string;
  readonly repo: string;
  readonly branch: string;
  readonly buildSpecPath: string;
}

export function pipelineConfig(scope: Construct): PipelineConfig | undefined {
  const connectionArn = scope.node.tryGetContext('fleet:pipelineConnectionArn') as
    | string
    | undefined;
  const repo = scope.node.tryGetContext('fleet:pipelineRepo') as string | undefined;
  // Both are required and neither has a sensible default: a connection ARN is account-specific,
  // and a repository guessed wrong would track someone else's code.
  if (!connectionArn || !repo) {
    return undefined;
  }
  return {
    connectionArn,
    repo,
    branch: (scope.node.tryGetContext('fleet:pipelineBranch') as string) ?? 'main',
    buildSpecPath:
      (scope.node.tryGetContext('fleet:pipelineBuildSpec') as string) ?? 'buildspec.yml',
  };
}
