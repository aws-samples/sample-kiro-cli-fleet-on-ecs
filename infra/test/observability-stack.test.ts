/**
 * Two alarms, and the reasons the thresholds are where they are.
 *
 * An alarm's settings decide whether it fires on a healthy fleet, and both failure directions are
 * silent. Too eager and it fires every time the fleet sits idle, which it does most of the time,
 * and then nobody reads it. Too lax and it never fires at all, which looks identical to nothing
 * going wrong.
 */

import { Match, Template } from 'aws-cdk-lib/assertions';
import {
  CLAIM_OUTCOMES,
  FLEET_METRICS,
  POOL_METRICS_NAMESPACE,
  RECOVERY_MECHANISMS,
  RUN_VERDICTS,
  TASK_STOP_REASONS,
  USAGE_READING_OUTCOMES,
} from '../lib/config';
import { fleetStacks } from './helpers';

let template: Template;

function alarmNamed(fragment: string): any {
  const found = Object.values(template.findResources('AWS::CloudWatch::Alarm')).find((alarm) =>
    String(alarm.Properties.AlarmName ?? '').includes(fragment),
  );
  expect(found).toBeDefined();
  return found!.Properties;
}

beforeAll(() => {
  template = Template.fromStack(fleetStacks().observability);
});

describe('only conditions that need a person are alarmed', () => {
  test('there are two alarms, not a wall of them', () => {
    // Claim outcomes, utilisation, backlog and verdicts are all published and none are alarmed:
    // nothing about them is actionable at three in the morning, and an operator with twenty
    // alarms reads none of them.
    template.resourceCountIs('AWS::CloudWatch::Alarm', 2);
  });

  test('both notify somewhere', () => {
    // An alarm with no action changes a console colour. That is not a notification.
    for (const name of ['work-abandoned', 'subscription-disabled']) {
      expect(alarmNamed(name).AlarmActions).toHaveLength(1);
    }
  });

  test('the topic ships without a subscriber', () => {
    // Deliberate. An address in a template is either someone else's or a placeholder that
    // swallows every notification silently; an empty topic is visibly unfinished.
    template.resourceCountIs('AWS::SNS::Topic', 1);
    template.resourceCountIs('AWS::SNS::Subscription', 0);
  });
});

describe('work that stopped being retried', () => {
  test('any item at all breaches', () => {
    // A threshold above zero would assert that some amount of silently lost work is fine. An
    // item in the dead-letter queue has exhausted its deliveries and stays lost until someone
    // looks, so there is no such amount.
    const alarm = alarmNamed('work-abandoned');

    expect(alarm.Threshold).toBe(0);
    expect(alarm.ComparisonOperator).toBe('GreaterThanThreshold');
  });

  test('it watches the dead-letter queue and not the work queue', () => {
    // Pointed at the work queue it would fire on every seeded batch, which is the normal case.
    const alarm = alarmNamed('work-abandoned');

    expect(JSON.stringify(alarm.Dimensions)).toContain('DeadLetterQueue');
    expect(JSON.stringify(alarm.Dimensions)).not.toContain('WorkQueue');
  });

  test('an idle fleet does not breach it', () => {
    // An empty queue publishes nothing rather than zero, so treating missing data as a breach
    // would alarm continuously on a fleet in its normal state — and an alarm that is always on
    // is an alarm nobody reads.
    expect(alarmNamed('work-abandoned').TreatMissingData).toBe('notBreaching');
  });
});

describe('a pool that shrank', () => {
  test('it watches the metric a worker emits when it disables one', () => {
    // The condition is invisible otherwise: the fleet keeps running with a lower ceiling than
    // anyone chose, and the only symptom is that batches take longer.
    const alarm = alarmNamed('subscription-disabled');

    expect(alarm.Namespace).toBe(POOL_METRICS_NAMESPACE);
    expect(alarm.MetricName).toBe('SubscriptionDisabled');
  });

  test('one occurrence is enough', () => {
    const alarm = alarmNamed('subscription-disabled');

    expect(alarm.Threshold).toBe(0);
    expect(alarm.EvaluationPeriods).toBe(1);
  });

  test('never having happened is reported rather than assumed healthy', () => {
    // The metric only exists after the first occurrence, so this alarm sits in
    // INSUFFICIENT_DATA until then. That is the honest state: `notBreaching` here would claim
    // a pool is intact on the strength of no data at all.
    expect(alarmNamed('subscription-disabled').TreatMissingData).toBe('missing');
  });

  test('the description says how to find which subscription', () => {
    // The metric carries no subscription dimension on purpose — one alarm should fire whichever
    // it was — so the description has to point at where the identifier is.
    expect(alarmNamed('subscription-disabled').AlarmDescription).toMatch(/SubscriptionId/);
  });
});

describe('agreement with the worker', () => {
  test('the alarm and the worker name the same namespace', () => {
    // Nothing at deploy time checks this. An alarm on a namespace nothing writes to stays in
    // INSUFFICIENT_DATA forever and looks exactly like a pool that has never lost a key.
    const alarm = alarmNamed('subscription-disabled');

    expect(alarm.Namespace).toBe(POOL_METRICS_NAMESPACE);
    expect(alarm.Namespace).toBe('KiroFleet/SubscriptionPool');
  });

  test('log retention is configured, so the identifier is findable for a bounded time', () => {
    // Requirement is retention rather than indefinite, and the alarm points an operator at the
    // logs — a pointer to logs that expired is worse than no pointer.
    template.hasResourceProperties('AWS::CloudWatch::Alarm', {
      AlarmDescription: Match.stringLikeRegexp('retained \\d+ days'),
    });
  });
});


describe('one dashboard, in the three layers the post describes', () => {
  /** The dashboard body, as the JSON CloudWatch stores. */
  function widgets(): any[] {
    const dashboard = Object.values(template.findResources('AWS::CloudWatch::Dashboard'))[0];
    const body = dashboard.Properties.DashboardBody;
    // The body is a Fn::Join of literals and resource references. The titles and metric names
    // are in the literal parts, which is all these assertions need.
    const literal = JSON.stringify(body);
    return [literal];
  }

  function bodyText(): string {
    return widgets()[0];
  }

  test('there is exactly one dashboard', () => {
    // Three dashboards for three layers would be three places to look during an incident,
    // which defeats the point of describing them as one view.
    template.resourceCountIs('AWS::CloudWatch::Dashboard', 1);
  });

  test('it shows progress, outcomes, and the pool', () => {
    const body = bodyText();

    expect(body).toContain('Progress');
    expect(body).toContain('Outcomes');
    expect(body).toContain('Pool');
  });

  test('run outcomes are drawn verdict by verdict', () => {
    // A success count cannot distinguish a prompt problem from a pool problem. The mix can.
    const body = bodyText();

    for (const verdict of RUN_VERDICTS) {
      expect(body).toContain(verdict);
    }
  });

  test('the claim outcome distribution is on it', () => {
    // Its own requirement: saturation and contention both present as "tasks are not getting
    // keys", and only this distribution separates them.
    const body = bodyText();

    for (const outcome of CLAIM_OUTCOMES) {
      expect(body).toContain(outcome);
    }
  });

  test('recovered capacity distinguishes expiry from drift', () => {
    // Expiry is the normal path for a stopped task. Drift correction means a counter was
    // wrong, so a rising line there is a bug rather than a workload.
    const body = bodyText();

    for (const mechanism of RECOVERY_MECHANISMS) {
      expect(body).toContain(mechanism);
    }
  });

  test('how tasks stopped is drawn reason by reason', () => {
    // A worker cannot report its own crash, so this is the one place a crash shows up as
    // itself rather than as capacity the reconciler had to recover.
    const dashboard = Object.values(template.findResources('AWS::CloudWatch::Dashboard'))[0];
    // The body is a Fn::Join of literals and references; a placeholder for each reference
    // leaves valid JSON whose metric arrays can be read as they are.
    const parts: unknown[] = dashboard.Properties.DashboardBody['Fn::Join'][1];
    const body = JSON.parse(parts.map((part) => (typeof part === 'string' ? part : 'ref')).join(''));
    const series: unknown[][] = body.widgets.flatMap((w: any) => w.properties.metrics ?? []);
    const drawn = series
      .filter((m) => m[0] === FLEET_METRICS.namespace && m[1] === FLEET_METRICS.taskStopped)
      .map((m) => [m[2], m[3]]);

    expect(drawn).toEqual(TASK_STOP_REASONS.map((reason) => ['Reason', reason]));
  });

  test('it says where the per-run transcripts are', () => {
    // Layer two is an artifact in object storage rather than a time series, so the only way it
    // appears in one view is a pointer to it. Without this the dashboard silently covers two
    // of the three layers the post promises.
    expect(bodyText()).toContain('transcript');
  });

  test('the scaling signal is plotted against the fleet it drives', () => {
    // The pair is the diagnosis when scaling looks wrong: a backlog that is falling while the
    // task count is flat is a policy problem, and both flat is a signal problem.
    const body = bodyText();

    expect(body).toContain('RunningTaskCount');
    expect(body).toContain(FLEET_METRICS.backlogPerTask);
  });

  test('credits used are drawn against the budget, per subscription', () => {
    // Subscription ids are data, so the series come from a search rather than a list.
    const body = bodyText();

    expect(body).toContain('SubscriptionCreditsUsed');
    expect(body).toContain('SubscriptionCreditBudget');
    expect(body).toContain('SEARCH(');
    expect(body).toContain('SubscriptionId');
  });

  test('an exhausted pool with work waiting is drawn on the credits-against-budget widget', () => {
    // No task runs while the pool is exhausted, so the scaling function is the only thing that
    // can say why the queue is not moving. Drawn with the per-subscription figures that explain
    // it.
    const body = bodyText();
    const widgetStart = body.indexOf('Credits — used against budget');
    const nextWidget = body.indexOf('Credits — usage readings');
    const exhausted = body.indexOf(FLEET_METRICS.poolExhausted);

    expect(widgetStart).toBeGreaterThan(-1);
    expect(exhausted).toBeGreaterThan(widgetStart);
    expect(exhausted).toBeLessThan(nextWidget);
  });

  test('the fleet ceiling is drawn as claimable concurrency beside the declared one', () => {
    const body = bodyText();

    expect(body).toContain(FLEET_METRICS.poolClaimableConcurrency);
    expect(body).toContain(FLEET_METRICS.poolDeclaredConcurrency);
  });

  test('failed usage readings are visible', () => {
    // Readings fail open, so this line is the only sign the budget runs on an old figure.
    const body = bodyText();

    for (const outcome of USAGE_READING_OUTCOMES) {
      expect(body).toContain(outcome);
    }
    expect(body).toContain('ReadFailed');
    expect(body).toContain('BudgetExhausted');
  });

  test('the per-run figure is shown as observability only', () => {
    const body = bodyText();

    expect(body).toContain('RunCredits');
    expect(body).toContain('observability only');
  });

  test('runs without a credit figure are drawn beside the figures', () => {
    // The worker emits RunCreditsMissing when a run printed no `Credits:` line. Without it on
    // the dashboard, a RunCredits p95 looks complete when some runs are absent from it.
    expect(bodyText()).toContain('RunCreditsMissing');
  });

  test('runs without a credit figure are split by verdict', () => {
    // The worker dimensions RunCreditsMissing by Verdict. A series without the dimension
    // would draw nothing, and one summed over verdicts would mix failed runs, which never
    // print a summary, with succeeded runs in the plain output format.
    const body = bodyText();

    expect(body).toMatch(/RunCreditsMissing\\",\\"Verdict\\",\\"Succeeded\\"/);
    expect(body).toContain('RunCreditsMissing, succeeded runs');
    expect(body).toContain('SubscriptionPool,Verdict} MetricName=\\\\\\"RunCreditsMissing\\\\\\"');
  });

  test('credits do not add an alarm', () => {
    // A subscription reaching its budget is the guardrail working, not a page.
    template.resourceCountIs('AWS::CloudWatch::Alarm', 2);
  });

  test('it is not a custom web application', () => {
    // Deliberate. A dashboard is configuration an operator can edit when the question changes;
    // a hosted interface is a thing to authenticate and keep alive beside a fleet whose whole
    // argument is that it costs nothing when idle.
    template.resourceCountIs('AWS::ApiGateway::RestApi', 0);
    template.resourceCountIs('AWS::CloudFront::Distribution', 0);
  });
});
