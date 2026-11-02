/**
 * Three functions, three roles, and the reason they are not one function.
 *
 * A single handler switching on the event shape would be less code and one role carrying the
 * union of three jobs: the seeder able to disable a subscription, the reconciler able to enqueue
 * work. Nothing would fail, and the blast radius of any one bug would be all three. So most of
 * what is asserted here is what each role *cannot* do.
 */

import { Match, Template } from 'aws-cdk-lib/assertions';
import { FLEET_METRICS } from '../lib/config';
import { fleetStacks } from './helpers';

let template: Template;

/** The three functions, by the handler they run, ignoring CDK's own log-retention helper. */
function functionsByHandler(
  t: Template = template,
): Record<string, { logicalId: string; properties: any }> {
  const found: Record<string, { logicalId: string; properties: any }> = {};
  for (const [logicalId, resource] of Object.entries(t.findResources('AWS::Lambda::Function'))) {
    const handler = String(resource.Properties.Handler ?? '');
    if (handler.endsWith('.handler.handler')) {
      found[handler.split('.')[0]] = { logicalId, properties: resource.Properties };
    }
  }
  return found;
}

/** Statements attached to a function's role. */
function statementsFor(name: string, t: Template = template): any[] {
  const roleRef = JSON.stringify(functionsByHandler(t)[name].properties.Role);
  const match = roleRef.match(/"([A-Za-z0-9]+ServiceRole[A-Za-z0-9]*)"/);

  return Object.values(t.findResources('AWS::IAM::Policy'))
    .filter((policy) =>
      match ? JSON.stringify(policy.Properties.Roles).includes(match[1]) : false,
    )
    .flatMap((policy) => policy.Properties.PolicyDocument.Statement);
}

/** Actions a function's role is permitted, flattened. */
function actionsFor(name: string, t: Template = template): string[] {
  return statementsFor(name, t).flatMap((statement: any) =>
    typeof statement.Action === 'string' ? [statement.Action] : statement.Action,
  );
}

/**
 * Actions a function's role is permitted on one table, sorted.
 *
 * Matched on the table's exported ARN, excluding its stream ARN: the event source mapping
 * grants stream reads on the leases table, which are not table actions.
 */
function tableActionsFor(
  name: string,
  table: 'SubscriptionsTable' | 'LeasesTable',
  t: Template = template,
): string[] {
  return statementsFor(name, t)
    .filter((statement: any) => {
      const resource = JSON.stringify(statement.Resource);
      return resource.includes(table) && !resource.includes('StreamArn');
    })
    .flatMap((statement: any) =>
      typeof statement.Action === 'string' ? [statement.Action] : statement.Action,
    )
    .sort();
}

beforeAll(() => {
  template = Template.fromStack(fleetStacks().functions);
});

describe('there are four functions because there are four jobs', () => {
  test('each job has its own function', () => {
    const functions = functionsByHandler();

    expect(Object.keys(functions).sort()).toEqual([
      'reconciler',
      'scaling',
      'seeding',
      'taskstopped',
    ]);
  });

  test('each runs the worker’s own module rather than a copy of its logic', () => {
    // A second implementation of reconciliation would drift from the first, and the drift
    // would appear as capacity that leaks on one path and not the other.
    const functions = functionsByHandler();

    expect(functions.reconciler.properties.Handler).toBe('reconciler.handler.handler');
    expect(functions.seeding.properties.Handler).toBe('seeding.handler.handler');
    expect(functions.scaling.properties.Handler).toBe('scaling.handler.handler');
    expect(functions.taskstopped.properties.Handler).toBe('taskstopped.handler.handler');
  });

  test('they match the fleet’s architecture', () => {
    // Not a performance point. A function built for the wrong architecture fails to start,
    // and the parameter exists so that is one decision rather than four.
    for (const entry of Object.values(functionsByHandler())) {
      expect(entry.properties.Architectures).toEqual(['arm64']);
    }
  });

  test('each has its own role', () => {
    const roles = Object.values(functionsByHandler()).map((f) => JSON.stringify(f.properties.Role));

    expect(new Set(roles).size).toBe(4);
  });
});

describe('no role carries another’s permissions', () => {
  test('the seeder can create work and not consume it', () => {
    // A seeder that could receive would be able to drain the batch it just created.
    const actions = actionsFor('seeding');

    expect(actions.some((a) => a.startsWith('sqs:SendMessage'))).toBe(true);
    expect(actions).not.toContain('sqs:ReceiveMessage');
    expect(actions).not.toContain('sqs:DeleteMessage');
  });

  test('the seeder cannot touch the subscription pool', () => {
    // It has no reason to know the pool exists.
    const actions = actionsFor('seeding');

    expect(actions.some((a) => a.startsWith('dynamodb:'))).toBe(false);
    expect(actions).not.toContain('secretsmanager:GetSecretValue');
  });

  test('the reconciler cannot enqueue work', () => {
    // It does hold sqs:SendMessage, for the stream's failure destination, so the action alone
    // says nothing — the question is which queue. Asserted on the resource, because the first
    // version of this test asserted the action and would have failed a correct stack.
    const statements = statementsFor('reconciler').filter((s: any) =>
      JSON.stringify(s.Action).includes('sqs:SendMessage'),
    );

    for (const statement of statements) {
      const resource = JSON.stringify(statement.Resource);
      expect(resource).toContain('DeadLetterQueue');
      expect(resource).not.toContain('WorkQueue');
    }
  });

  test('the reconciler can delete a lease but never create or extend one', () => {
    // It deletes leases that are past expiry and not yet removed by TTL (#15); the delete is
    // conditional on the expiry, in code. It must never put or update a lease, which would
    // invent or prolong capacity, and must never delete from the subscriptions table.
    const actions = actionsFor('reconciler');

    expect(actions).not.toContain('dynamodb:PutItem');
    expect(tableActionsFor('reconciler', 'SubscriptionsTable')).not.toContain(
      'dynamodb:DeleteItem',
    );
    expect(tableActionsFor('reconciler', 'LeasesTable')).not.toContain('dynamodb:UpdateItem');
  });

  test('the reconciler can scan the pool it sweeps', () => {
    // The scheduled pass walks every subscription through `Registry.describe_all`, which is a
    // Scan. Without it every pass fails with an access denial before correcting anything,
    // and nothing else surfaces that: explicit releases keep the counters right until the
    // first task dies without one. It also reads a record before resetting a credit window
    // and updates counters, and it never queries the pool.
    expect(tableActionsFor('reconciler', 'SubscriptionsTable')).toEqual([
      'dynamodb:GetItem',
      'dynamodb:Scan',
      'dynamodb:UpdateItem',
    ]);
  });

  test('the reconciler reads and reaps leases and nothing more', () => {
    // `Leases.reap_expired` queries the by-subscription index, reads each lease's expiry, and
    // deletes the expired ones. Nothing on the reconciler's path scans the leases table or
    // writes a lease.
    expect(tableActionsFor('reconciler', 'LeasesTable')).toEqual([
      'dynamodb:DeleteItem',
      'dynamodb:GetItem',
      'dynamodb:Query',
    ]);
  });

  test('the signal function only reads', () => {
    // It decides a ceiling. It has no business changing the pool or the queue.
    const actions = actionsFor('scaling');

    expect(actions).toContain('sqs:GetQueueAttributes');
    expect(actions).toContain('dynamodb:Query');
    expect(actions).not.toContain('dynamodb:UpdateItem');
    expect(actions.some((a) => a.startsWith('sqs:Send'))).toBe(false);
    expect(actions.some((a) => a.startsWith('sqs:Receive'))).toBe(false);
  });

  test('the signal function reads the pool through the state index and nothing more', () => {
    // The budget test needs `creditsUsed` and `creditBudget`, and the state index projects every
    // attribute, so the one Query that sums concurrency brings them. No GetItem, no Scan.
    expect(tableActionsFor('scaling', 'SubscriptionsTable')).toEqual(['dynamodb:Query']);
    expect(tableActionsFor('scaling', 'LeasesTable')).toEqual([]);
  });

  test('the signal function holds the ceiling and reads only what scale-in suspension needs', () => {
    // The maximum is written every run, not compared. The reads are for the scale-in
    // suspension around a start from zero: whether the last run left it on, and the state of
    // the scale-in alarm that decides when it comes off. Nothing in CloudWatch is written.
    const actions = actionsFor('scaling');

    expect(actions).toContain('application-autoscaling:RegisterScalableTarget');
    expect(actions).toContain('application-autoscaling:DescribeScalableTargets');
    expect(actions).toContain('cloudwatch:DescribeAlarms');
    expect(actions).not.toContain('cloudwatch:SetAlarmState');
    expect(actions).not.toContain('cloudwatch:PutMetricAlarm');
    expect(actions).not.toContain('application-autoscaling:PutScalingPolicy');
  });

  test('the signal function may set its own service’s count and nothing else in ECS', () => {
    // It writes the desired count at both ends of a batch: zero when the queue drains, and off
    // zero when work reaches an idle fleet. Both need DescribeServices to read the service and
    // UpdateService on that one service, and no other ECS action.
    const ecs = statementsFor('scaling').filter((s: any) =>
      JSON.stringify(s.Action).includes('ecs:'),
    );
    const actions = ecs
      .flatMap((s: any) => (typeof s.Action === 'string' ? [s.Action] : s.Action))
      .sort();

    expect(actions).toEqual(['ecs:DescribeServices', 'ecs:UpdateService']);

    const update = ecs.find((s: any) => s.Action === 'ecs:UpdateService');
    expect(JSON.stringify(update.Resource)).toContain('WorkerService');
    expect(update.Resource).not.toBe('*');

    const describe = ecs.find((s: any) => s.Action === 'ecs:DescribeServices');
    expect(Object.keys(describe.Condition)).toEqual(['ArnEquals']);
    expect(JSON.stringify(describe.Condition.ArnEquals['ecs:cluster'])).toContain('Cluster');
  });

  test('no role permits everything on everything', () => {
    for (const name of ['seeding', 'reconciler', 'scaling']) {
      expect(actionsFor(name)).not.toContain('*');
    }
  });
});

describe('capacity comes back on both triggers', () => {
  test('the lease stream feeds the reconciler', () => {
    // The fast path: a TTL deletion arrives as a removal event and the counter is corrected
    // within seconds.
    template.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      FunctionName: Match.anyValue(),
      BisectBatchOnFunctionError: true,
    });
  });

  test('a failing record cannot block every later recovery', () => {
    // Without a destination and bisection, one poisonous record stalls the shard and every
    // subsequent expiry waits behind it — which looks like a pool that slowly loses capacity.
    template.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      DestinationConfig: Match.objectLike({ OnFailure: Match.anyValue() }),
      MaximumRetryAttempts: 3,
    });
  });

  test('a schedule sweeps the pool as well', () => {
    // This is the guarantee, not a backstop: DynamoDB gives no upper bound on when a TTL
    // deletion happens, and a stream record can be missed. The stream only makes it prompt.
    template.hasResourceProperties('AWS::Events::Rule', {
      ScheduleExpression: 'rate(5 minutes)',
    });
  });
});

describe('the signal that starts the first task', () => {
  test('it is published every minute', () => {
    // Target tracking reads the latest datapoint, so a longer period means a fleet reacting
    // to a backlog that is already stale — scaling out after the burst and in before the work
    // is finished.
    template.hasResourceProperties('AWS::Events::Rule', {
      ScheduleExpression: 'rate(1 minute)',
    });
  });

  test('it publishes to the namespace the scaling policy reads', () => {
    // The policy in ComputeStack and the publisher here have to agree on a namespace, and
    // nothing at deploy time checks that they do.
    const scaling = functionsByHandler().scaling.properties;
    const variables = scaling.Environment.Variables;

    expect(variables.METRICS_NAMESPACE).toBe(FLEET_METRICS.namespace);
  });

  test('it knows the service whose ceiling it holds', () => {
    const variables = functionsByHandler().scaling.properties.Environment.Variables;

    for (const required of ['CLUSTER_NAME', 'SERVICE_NAME', 'MAX_TASKS', 'WORK_QUEUE_URL']) {
      expect(variables).toHaveProperty(required);
    }
  });

  test('it starts an idle fleet with the same target the scaling policy holds', () => {
    // If the two differed, a start from zero could ask for more tasks than target tracking
    // wants and the policy would pull the count back, or fewer and it would push it up.
    const variables = functionsByHandler().scaling.properties.Environment.Variables;
    const policy = Object.values(
      Template.fromStack(fleetStacks().compute).findResources(
        'AWS::ApplicationAutoScaling::ScalingPolicy',
      ),
    )[0] as any;
    const target =
      policy.Properties.TargetTrackingScalingPolicyConfiguration.TargetValue;

    expect(Number(variables.TARGET_BACKLOG_PER_TASK)).toBe(target);
  });
});

describe('the scaler and the workers draw the credit line in the same place', () => {
  /** `CREDIT_MARGIN` as the scaling function and the worker container each receive it. */
  function margins(overrides: Record<string, unknown> = {}): { scaler: string; worker: string } {
    const stacks = fleetStacks(overrides);
    const scaler = functionsByHandler(Template.fromStack(stacks.functions)).scaling.properties
      .Environment.Variables.CREDIT_MARGIN;
    const taskDefinition = Object.values(
      Template.fromStack(stacks.compute).findResources('AWS::ECS::TaskDefinition'),
    )[0] as any;
    const worker = taskDefinition.Properties.ContainerDefinitions[0].Environment.find(
      (entry: any) => entry.Name === 'CREDIT_MARGIN',
    )?.Value;
    return { scaler, worker };
  }

  test('both receive the same value', () => {
    // A scaler with a smaller margin than the workers would count subscriptions a claim
    // refuses, and start tasks that exit without work for that band of credits.
    const { scaler, worker } = margins();

    expect(scaler).toBeDefined();
    expect(scaler).toBe(worker);
    expect(scaler).toBe('1');
  });

  test('a configured margin reaches both', () => {
    expect(margins({ 'fleet:creditMargin': '0.25' })).toEqual({ scaler: '0.25', worker: '0.25' });
    // Context from the command line arrives as a number; it is passed on as the same string.
    expect(margins({ 'fleet:creditMargin': 2 })).toEqual({ scaler: '2', worker: '2' });
  });

  test.each(['-1', '0.001', 'abc', '1e3'])(
    'a margin the worker would refuse (%s) is refused at synthesis',
    (value) => {
      expect(() => fleetStacks({ 'fleet:creditMargin': value })).toThrow(/fleet:creditMargin/);
    },
  );
});

describe('seeding does not depend on finishing in one invocation', () => {
  test('it may invoke itself, and nothing else', () => {
    // The continuation. Scoped to its own ARN, because a function permitted to invoke
    // anything is a wider grant than continuing requires.
    const invoke = statementsFor('seeding').filter((s: any) =>
      JSON.stringify(s.Action).includes('lambda:InvokeFunction'),
    );

    expect(invoke).toHaveLength(1);
    const resource = JSON.stringify(invoke[0].Resource);
    // Its own name, and a Lambda ARN separates the name with a colon. The slash form that
    // `formatArn` produces by default matches nothing, which is how the first version of this
    // grant was wrong while looking right.
    expect(resource).toContain(':function:');
    expect(resource).toContain('-seeding');
  });

  test('it knows where the manifest lives without being told each time', () => {
    const variables = functionsByHandler().seeding.properties.Environment.Variables;

    expect(variables).toHaveProperty('REFERENCE_BUCKET');
    expect(variables).toHaveProperty('WORK_QUEUE_URL');
  });
});


describe('the activity report function exists only when the report is configured', () => {
  const REPORT_CONTEXT = {
    'fleet:activityReportBucket': 'example-report-bucket',
    'fleet:activityReportPrefix': '/kiro-activity/',
  };
  const REPORT_PREFIX = 'kiro-activity/AWSLogs/123456789012/KiroLogs/user_report/us-east-1/';
  let withReport: Template;

  beforeAll(() => {
    withReport = Template.fromStack(fleetStacks(REPORT_CONTEXT).functions);
  });

  test('without the bucket in context there are still four functions', () => {
    // The report needs a Kiro Enterprise profile and a bucket the operator creates. A sample
    // that required one would not deploy for anyone else.
    expect(Object.keys(functionsByHandler()).sort()).toEqual([
      'reconciler',
      'scaling',
      'seeding',
      'taskstopped',
    ]);
  });

  test('with it, a fifth function runs the worker module, with its own role', () => {
    const functions = functionsByHandler(withReport);

    expect(Object.keys(functions).sort()).toEqual([
      'reconciler',
      'scaling',
      'seeding',
      'taskstopped',
      'usagereport',
    ]);
    expect(functions.usagereport.properties.Handler).toBe('usagereport.handler.handler');
    const roles = Object.values(functions).map((f) => JSON.stringify(f.properties.Role));
    expect(new Set(roles).size).toBe(5);
  });

  test('a bucket without a prefix is refused at synthesis', () => {
    // Kiro will not write the report to a bucket root, so the layout cannot exist.
    expect(() =>
      fleetStacks({ 'fleet:activityReportBucket': 'example-report-bucket' }),
    ).toThrow(/activityReportPrefix/);
  });

  test('it scans the pool and raises one attribute, nothing more', () => {
    // A scan to find mapped subscriptions; the conditional raise is an UpdateItem. No GetItem,
    // no PutItem, no DeleteItem, no Query.
    expect(tableActionsFor('usagereport', 'SubscriptionsTable', withReport)).toEqual([
      'dynamodb:Scan',
      'dynamodb:UpdateItem',
    ]);
  });

  test('it cannot touch leases, keys or the queue', () => {
    expect(tableActionsFor('usagereport', 'LeasesTable', withReport)).toEqual([]);
    const actions = actionsFor('usagereport', withReport);
    expect(actions.some((a) => a.startsWith('secretsmanager:'))).toBe(false);
    expect(actions.some((a) => a.startsWith('sqs:'))).toBe(false);
    expect(actions.some((a) => a.startsWith('lambda:'))).toBe(false);
    expect(actions).not.toContain('*');
  });

  test('it reads the report prefix and cannot write or delete in the bucket', () => {
    const s3Statements = statementsFor('usagereport', withReport).filter((statement: any) =>
      JSON.stringify(statement.Action).includes('s3:'),
    );
    const s3Actions = s3Statements
      .flatMap((statement: any) =>
        typeof statement.Action === 'string' ? [statement.Action] : statement.Action,
      )
      .sort();

    expect(s3Actions).toEqual(['s3:GetObject', 's3:ListBucket']);

    const list = s3Statements.find((s: any) => s.Action === 's3:ListBucket');
    expect(list.Condition).toEqual({ StringLike: { 's3:prefix': [`${REPORT_PREFIX}*`] } });
    expect(JSON.stringify(list.Resource)).toContain('example-report-bucket');

    const get = s3Statements.find((s: any) => s.Action === 's3:GetObject');
    expect(JSON.stringify(get.Resource)).toContain(`example-report-bucket/${REPORT_PREFIX}*`);
  });

  test('it runs daily after the report is written', () => {
    // Kiro writes the files at 02:00 UTC; ninety minutes later they are there.
    withReport.hasResourceProperties('AWS::Events::Rule', {
      ScheduleExpression: 'cron(30 3 * * ? *)',
    });
  });

  test('it knows where the report and the pool are', () => {
    const variables = functionsByHandler(withReport).usagereport.properties.Environment.Variables;

    expect(variables.REPORT_BUCKET).toBe('example-report-bucket');
    expect(variables.REPORT_PREFIX).toBe(REPORT_PREFIX);
    expect(variables.REPORT_REGION).toBe('us-east-1');
    expect(variables).toHaveProperty('SUBSCRIPTIONS_TABLE');
    expect(variables).not.toHaveProperty('LEASES_TABLE');
  });

  test('the other three roles are unchanged by it', () => {
    for (const name of ['seeding', 'reconciler', 'scaling']) {
      expect(actionsFor(name, withReport).sort()).toEqual(actionsFor(name).sort());
    }
  });
});

describe('how each task stopped is counted from outside the task', () => {
  test('a rule sends this service’s stopped tasks, and nothing else, to the function', () => {
    const rules = Object.values(template.findResources('AWS::Events::Rule')).filter(
      (rule) => rule.Properties.EventPattern !== undefined,
    );

    expect(rules).toHaveLength(1);
    const pattern = rules[0].Properties.EventPattern;
    expect(pattern.source).toEqual(['aws.ecs']);
    expect(pattern['detail-type']).toEqual(['ECS Task State Change']);
    expect(pattern.detail.lastStatus).toEqual(['STOPPED']);
    // Scoped to the cluster and to the worker service, so a task started by hand (the
    // benchmark, a one-off run) is not counted as a fleet task stopping.
    expect(pattern.detail.clusterArn).toHaveLength(1);
    expect(pattern.detail.group).toHaveLength(1);
    expect(JSON.stringify(pattern.detail.group)).toContain('service:');

    const target = JSON.stringify(rules[0].Properties.Targets);
    expect(target).toContain(functionsByHandler().taskstopped.logicalId);
  });

  test('it publishes to the fleet namespace the dashboard reads', () => {
    const env = functionsByHandler().taskstopped.properties.Environment.Variables;

    expect(env.METRICS_NAMESPACE).toBe(FLEET_METRICS.namespace);
  });

  test('it needs no permissions beyond its logs', () => {
    // The count goes to stdout as embedded metric format, so the role carries nothing:
    // a function that only reads an event has no reason to call anything.
    expect(actionsFor('taskstopped')).toEqual([]);
  });
});
