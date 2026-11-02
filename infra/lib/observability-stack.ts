/**
 * The two things that need a person, and nothing else yet.
 *
 * Alarms and dashboards answer different questions, and conflating them is how an operator ends
 * up with twenty alarms nobody reads. A dashboard is for looking at a batch on purpose. An alarm
 * is for a condition that will not resolve itself and that nobody is looking at — and there are
 * exactly two of those here.
 *
 * Work in the dead-letter queue has stopped being retried, so it stays lost until someone acts.
 * And a subscription disabled by a worker shrinks the pool permanently, because nothing
 * re-enables one: the fleet keeps running, slower, with a ceiling nobody chose.
 *
 * Everything else about a run — claim outcomes, utilisation, backlog, verdicts — is published and
 * is not alarmed on, because none of it is actionable at three in the morning. The dashboard that
 * presents it is deliberately not here; its layering is an open decision, and shipping a guess at
 * it would be harder to correct than its absence.
 */

import { Duration, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as actions from 'aws-cdk-lib/aws-cloudwatch-actions';
import * as sns from 'aws-cdk-lib/aws-sns';
import {
  CLAIM_OUTCOMES,
  FLEET_METRICS,
  POOL_METRICS_NAMESPACE,
  RECOVERY_MECHANISMS,
  RUN_VERDICTS,
  TASK_STOP_REASONS,
  USAGE_READING_OUTCOMES,
  fleetConfig,
} from './config';
import { ComputeStack } from './compute-stack';
import { DataStack } from './data-stack';
import { QueueStack } from './queue-stack';

/**
 * One series per dimension value, which is the only way a distribution is readable.
 *
 * A single series summed over every verdict says how many runs there were, which nobody needs:
 * the whole diagnostic value is in the mix. These helpers exist so that adding a verdict is one
 * edit in `config.ts` rather than a widget rewrite.
 */
function byDimension(
  namespace: string,
  metricName: string,
  dimension: string,
  value: string,
  period: Duration,
): cloudwatch.Metric {
  return new cloudwatch.Metric({
    namespace,
    metricName,
    dimensionsMap: { [dimension]: value },
    period,
    statistic: cloudwatch.Stats.SUM,
    label: value,
  });
}

const runOutcome = (verdict: string, period: Duration) =>
  byDimension(POOL_METRICS_NAMESPACE, 'RunOutcome', 'Verdict', verdict, period);

const claimOutcome = (outcome: string, period: Duration) =>
  byDimension(POOL_METRICS_NAMESPACE, 'ClaimOutcome', 'Outcome', outcome, period);

const recovered = (mechanism: string, period: Duration) =>
  byDimension(POOL_METRICS_NAMESPACE, 'LeasesRecovered', 'Mechanism', mechanism, period);

const taskStopped = (reason: string, period: Duration) =>
  byDimension(FLEET_METRICS.namespace, FLEET_METRICS.taskStopped, 'Reason', reason, period);

export interface ObservabilityStackProps extends StackProps {
  readonly queue: QueueStack;
  readonly compute: ComputeStack;
  readonly data: DataStack;
}

export class ObservabilityStack extends Stack {
  readonly alarmTopic: sns.Topic;
  readonly deadLetterAlarm: cloudwatch.Alarm;
  readonly subscriptionDisabledAlarm: cloudwatch.Alarm;
  readonly dashboard: cloudwatch.Dashboard;

  constructor(scope: Construct, id: string, props: ObservabilityStackProps) {
    super(scope, id, props);
    const config = fleetConfig(this);

    // Created without a subscription on purpose. A topic with an address baked into a template
    // either carries someone else's address or a placeholder that silently swallows every
    // notification; an empty topic is visibly unfinished, which is the better failure.
    this.alarmTopic = new sns.Topic(this, 'FleetAlarms', {
      displayName: 'Kiro fleet: conditions that need a person',
      // Publishers must use TLS. CloudWatch alarms already do; this refuses anything else.
      enforceSSL: true,
    });

    this.deadLetterAlarm = this.alarmOnAbandonedWork(props);
    this.subscriptionDisabledAlarm = this.alarmOnShrinkingPool(config.logRetentionDays);
    this.dashboard = this.createDashboard(props);
  }

  /**
   * One view, in the three layers the post describes.
   *
   * It is a CloudWatch dashboard rather than a custom interface, and the reason is what each
   * would cost to be wrong. A dashboard is a few lines of configuration that an operator can
   * edit when the question changes; a web application is a thing to host, authenticate, and
   * keep alive next to a fleet whose argument is that it costs nothing when idle.
   *
   * Building this was blocked for a while on what looked like a question about presentation and
   * turned out to be a question about data: two of the three layers had none. Transcripts were
   * written to a task's scratch and never left it, and run verdicts went to a log line and no
   * metric. Both had to exist before there was anything to lay out.
   */
  private createDashboard(props: ObservabilityStackProps): cloudwatch.Dashboard {
    const period = Duration.minutes(1);
    const dashboard = new cloudwatch.Dashboard(this, 'Fleet', {
      dashboardName: `${this.stackName}-fleet`,
      defaultInterval: Duration.hours(3),
    });

    const queueDepth = props.queue.workQueue.metricApproximateNumberOfMessagesVisible({
      period,
      label: 'Waiting',
    });
    const inFlight = props.queue.workQueue.metricApproximateNumberOfMessagesNotVisible({
      period,
      label: 'In flight',
    });

    dashboard.addWidgets(
      new cloudwatch.TextWidget({
        markdown: [
          `# ${props.compute.cluster.clusterName}`,
          '',
          '**Progress** is the queue draining. **Outcomes** say whether the runs did what they',
          'were asked. **Pool** says whether the fleet can grow.',
          '',
          `Per-run transcripts: \`s3://${props.data.resultsBucket.bucketName}/results/<item>/transcript/\``,
        ].join('\n'),
        width: 24,
        height: 4,
      }),
    );

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Progress — work waiting and in flight',
        left: [queueDepth, inFlight],
        // Stacked, so the top line is total outstanding work. Side by side it is easy to read a
        // drop in one as progress when the other rose by the same amount.
        stacked: true,
        width: 12,
        height: 6,
      }),
      new cloudwatch.GraphWidget({
        title: 'Progress — fleet size against its signal',
        left: [
          new cloudwatch.Metric({
            namespace: 'ECS/ContainerInsights',
            metricName: 'RunningTaskCount',
            dimensionsMap: {
              ClusterName: props.compute.cluster.clusterName,
              ServiceName: props.compute.service.serviceName,
            },
            period,
            statistic: cloudwatch.Stats.MAXIMUM,
            label: 'Running tasks',
          }),
        ],
        // The signal on its own axis: it is a ratio and the task count is a count, and sharing
        // an axis makes whichever is larger hide the other.
        right: [
          new cloudwatch.Metric({
            namespace: FLEET_METRICS.namespace,
            metricName: FLEET_METRICS.backlogPerTask,
            period,
            statistic: cloudwatch.Stats.MAXIMUM,
            label: 'Backlog per task (drives scaling)',
          }),
        ],
        width: 12,
        height: 6,
      }),
    );

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Progress — how tasks stopped',
        // Reason by reason, because a crash and a scale-in both lower the task count and look
        // the same on the graph above. Exited and ScaledIn are the normal ends; Crashed, Killed
        // and FailedToStart are the stops a task cannot report itself, counted from the ECS
        // event instead.
        left: TASK_STOP_REASONS.map((reason) => taskStopped(reason, period)),
        stacked: true,
        width: 24,
        height: 6,
      }),
    );

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Outcomes — what the runs did',
        // Verdict by verdict rather than a success count, because the mix is the diagnosis:
        // NoOutput sends you to the prompt and Throttled sends you to the pool, and a single
        // failure count sends you nowhere.
        left: RUN_VERDICTS.map((verdict) => runOutcome(verdict, period)),
        stacked: true,
        width: 12,
        height: 6,
      }),
      new cloudwatch.GraphWidget({
        title: 'Outcomes — why a claim ended',
        // Requirement in its own right: this is what separates a saturated pool from workers
        // contending for the same subscription. Both look like "tasks are not getting keys".
        left: CLAIM_OUTCOMES.map((outcome) => claimOutcome(outcome, period)),
        stacked: true,
        width: 12,
        height: 6,
      }),
    );

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Pool — how much of it is in use',
        left: [
          new cloudwatch.Metric({
            namespace: POOL_METRICS_NAMESPACE,
            metricName: 'SubscriptionUtilization',
            period,
            statistic: cloudwatch.Stats.MAXIMUM,
            label: 'Utilisation (%)',
          }),
        ],
        // Both, because the gap between them is budget: declared counts every enabled
        // subscription, claimable only those below `creditBudget - CREDIT_MARGIN`, and the fleet
        // is held at the claimable figure.
        right: [
          new cloudwatch.Metric({
            namespace: FLEET_METRICS.namespace,
            metricName: FLEET_METRICS.poolClaimableConcurrency,
            period,
            statistic: cloudwatch.Stats.MAXIMUM,
            label: 'Claimable concurrency (the fleet ceiling)',
          }),
          new cloudwatch.Metric({
            namespace: FLEET_METRICS.namespace,
            metricName: FLEET_METRICS.poolDeclaredConcurrency,
            period,
            statistic: cloudwatch.Stats.MAXIMUM,
            label: 'Declared concurrency (enabled, any budget)',
          }),
        ],
        width: 12,
        height: 6,
      }),
      new cloudwatch.GraphWidget({
        title: 'Pool — capacity that had to be recovered',
        // Expiry is the normal path for a task that was stopped. Drift correction is not: it
        // means a counter was wrong, and a rising line here is a bug rather than a workload.
        left: RECOVERY_MECHANISMS.map((mechanism) => recovered(mechanism, period)),
        stacked: true,
        width: 12,
        height: 6,
      }),
    );

    this.addCreditsRow(dashboard);

    return dashboard;
  }

  /**
   * Credits: the recorded figure against the budget, whether readings are working, and what one
   * run costs.
   *
   * The first two are searches, because subscription ids are data rather than configuration
   * and are not known when the stack is synthesized. The per-run figure is drawn on its own:
   * it is what sizes `CREDIT_MARGIN`, and it is never added to `creditsUsed`.
   */
  private addCreditsRow(dashboard: cloudwatch.Dashboard): void {
    // Five minutes: readings arrive once per work item, and a one-minute period would draw
    // most of the line as gaps.
    const period = Duration.minutes(5);
    const perSubscription = (metricName: string, label: string) =>
      new cloudwatch.MathExpression({
        expression:
          `SEARCH('{${POOL_METRICS_NAMESPACE},SubscriptionId} ` +
          `MetricName="${metricName}"', 'Maximum', ${period.toSeconds()})`,
        label,
        period,
      });

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Credits — used against budget, per subscription',
        // Recorded from `/usage` before every work item and raised by the daily report. A line
        // reaching its budget less the margin is a subscription that has stopped being handed out.
        left: [
          perSubscription('SubscriptionCreditsUsed', 'Used'),
          perSubscription('SubscriptionCreditBudget', 'Budget'),
        ],
        // Published by the scaling function on minutes when work is waiting and every enabled
        // subscription is past that line. No task runs then, so nothing else reports it: the
        // queue just does not move until a budget is raised or a window resets.
        right: [
          new cloudwatch.Metric({
            namespace: FLEET_METRICS.namespace,
            metricName: FLEET_METRICS.poolExhausted,
            period,
            statistic: cloudwatch.Stats.SUM,
            label: 'PoolExhausted (work waiting, no budget left)',
          }),
        ],
        width: 8,
        height: 6,
      }),
      new cloudwatch.GraphWidget({
        title: 'Credits — usage readings',
        // Readings fail open, so `ReadFailed` rising means the budget is being enforced on an
        // old figure. `BudgetExhausted` is a worker stopping mid-drain at the ceiling.
        left: USAGE_READING_OUTCOMES.map((outcome) =>
          byDimension(POOL_METRICS_NAMESPACE, 'UsageReading', 'Outcome', outcome, period),
        ),
        right: [
          new cloudwatch.Metric({
            namespace: POOL_METRICS_NAMESPACE,
            metricName: 'BudgetExhausted',
            period,
            statistic: cloudwatch.Stats.SUM,
            label: 'BudgetExhausted (worker stopped at the ceiling)',
          }),
        ],
        stacked: false,
        width: 8,
        height: 6,
      }),
      new cloudwatch.GraphWidget({
        title: 'Credits — per run (observability only, sets the margin)',
        // The `▸ Credits:` figure a run prints. Primary agent only, two decimal places.
        left: [
          new cloudwatch.Metric({
            namespace: POOL_METRICS_NAMESPACE,
            metricName: 'RunCredits',
            period,
            statistic: cloudwatch.Stats.p(95),
            label: 'RunCredits p95',
          }),
          new cloudwatch.Metric({
            namespace: POOL_METRICS_NAMESPACE,
            metricName: 'RunCredits',
            period,
            statistic: cloudwatch.Stats.MAXIMUM,
            label: 'RunCredits max',
          }),
        ],
        // The CLI does not print the figure on every run: one of two identical runs printed
        // none. Shown beside the figures so a p95 is read against how many runs it is missing.
        // Split by verdict: only succeeded runs say something about the output format, because
        // a run that timed out, was throttled or crashed never prints a summary at all.
        right: [
          byDimension(
            POOL_METRICS_NAMESPACE,
            'RunCreditsMissing',
            'Verdict',
            'Succeeded',
            period,
          ).with({ label: 'RunCreditsMissing, succeeded runs' }),
          new cloudwatch.MathExpression({
            expression:
              `SUM(SEARCH('{${POOL_METRICS_NAMESPACE},Verdict} ` +
              `MetricName="RunCreditsMissing"', 'Sum', ${period.toSeconds()}))`,
            label: 'RunCreditsMissing, all verdicts',
            period,
          }),
        ],
        width: 8,
        height: 6,
      }),
    );
  }

  /**
   * Anything in the dead-letter queue at all.
   *
   * A threshold above zero would be a decision that some quantity of silently lost work is
   * acceptable, and there is no such quantity: an item here has failed its deliveries and will
   * not be retried again, so it stays lost until someone looks.
   *
   * `missingData: notBreaching` because an empty queue publishes nothing rather than zero, and
   * treating absence as a breach would alarm on a healthy idle fleet — which is its normal state.
   */
  private alarmOnAbandonedWork(props: ObservabilityStackProps): cloudwatch.Alarm {
    const alarm = new cloudwatch.Alarm(this, 'WorkAbandoned', {
      alarmName: `${this.stackName}-work-abandoned`,
      alarmDescription:
        'Items reached the dead-letter queue. They have stopped being retried and stay lost ' +
        'until someone looks at them.',
      metric: props.queue.deadLetterQueue.metricApproximateNumberOfMessagesVisible({
        period: Duration.minutes(5),
        statistic: cloudwatch.Stats.MAXIMUM,
      }),
      threshold: 0,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      evaluationPeriods: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    alarm.addAlarmAction(new actions.SnsAction(this.alarmTopic));
    return alarm;
  }

  /**
   * A worker took a subscription out of rotation.
   *
   * The pool does not recover from this by itself — nothing re-enables a subscription — so the
   * fleet carries on with a lower ceiling than anyone chose, and the only symptom is that
   * batches take longer. That is precisely the shape of problem an alarm is for: it will not
   * resolve, and it does not look like a failure.
   *
   * The metric arrives as embedded metric format from a task's log stream, so it exists only
   * after it has happened at least once. A metric that may not exist yet is the usual reason an
   * alarm sits in INSUFFICIENT_DATA; here that state is correct and means no subscription has
   * been disabled.
   */
  private alarmOnShrinkingPool(retentionDays: number): cloudwatch.Alarm {
    const alarm = new cloudwatch.Alarm(this, 'SubscriptionDisabled', {
      alarmName: `${this.stackName}-subscription-disabled`,
      alarmDescription:
        'A worker disabled a subscription after its credential was rejected. Nothing ' +
        're-enables one, so the pool stays smaller until an operator fixes the key. Query ' +
        `SubscriptionId in the worker log group to find which (retained ${retentionDays} days).`,
      metric: new cloudwatch.Metric({
        namespace: POOL_METRICS_NAMESPACE,
        metricName: 'SubscriptionDisabled',
        period: Duration.minutes(5),
        statistic: cloudwatch.Stats.SUM,
      }),
      threshold: 0,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      evaluationPeriods: 1,
      // Not `notBreaching`: a pool that has never lost a subscription publishes nothing, and
      // that is the state this alarm should report rather than treat as healthy-by-assumption.
      treatMissingData: cloudwatch.TreatMissingData.MISSING,
    });

    alarm.addAlarmAction(new actions.SnsAction(this.alarmTopic));
    return alarm;
  }
}
