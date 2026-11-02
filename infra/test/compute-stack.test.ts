/**
 * The properties of the fleet that are expensive to get wrong.
 *
 * Every assertion here stands for a failure that a deployment would not report. A fleet with a
 * minimum of one costs money forever and looks fine. A task definition targeting the wrong
 * architecture runs under emulation and looks fine, while changing every timing this image is
 * used to measure. A fleet sized past the account quota looks fine until the first batch.
 */

import { Annotations, Match, Template } from 'aws-cdk-lib/assertions';
import {
  DEFAULT_FARGATE_ONDEMAND_VCPU_QUOTA,
  FLEET_METRICS,
  POOL_METRICS_NAMESPACE,
} from '../lib/config';
import { deployedContext, fleetStacks } from './helpers';

let template: Template;
let scalableTarget: Record<string, any>;
let taskDefinition: Record<string, any>;
let service: Record<string, any>;

/** The one container in the task definition. */
function container(): Record<string, any> {
  return taskDefinition.Properties.ContainerDefinitions[0];
}

/** Statements attached to a role, found by the role it names. */
function statementsFor(roleLogicalId: string): any[] {
  const policies = template.findResources('AWS::IAM::Policy');
  const attached = Object.values(policies).filter((policy) =>
    JSON.stringify(policy.Properties.Roles).includes(roleLogicalId),
  );
  return attached.flatMap((policy) => policy.Properties.PolicyDocument.Statement);
}

/** Every action a role is permitted, flattened. */
function actionsFor(roleLogicalId: string): string[] {
  return statementsFor(roleLogicalId).flatMap((statement) =>
    typeof statement.Action === 'string' ? [statement.Action] : statement.Action,
  );
}

function logicalIdOf(type: string, match: (properties: any) => boolean): string {
  const found = Object.entries(template.findResources(type)).find(([, resource]) =>
    match(resource.Properties),
  );
  expect(found).toBeDefined();
  return found![0];
}

beforeAll(() => {
  template = Template.fromStack(fleetStacks().compute);
  scalableTarget = Object.values(
    template.findResources('AWS::ApplicationAutoScaling::ScalableTarget'),
  )[0];
  taskDefinition = Object.values(template.findResources('AWS::ECS::TaskDefinition'))[0];
  service = Object.values(template.findResources('AWS::ECS::Service'))[0];
});

describe('the fleet costs nothing when idle', () => {
  test('minimum capacity is zero', () => {
    // The entire cost argument for this pattern. One task held at all times is a standing
    // charge for a fleet whose whole point is that it is usually empty.
    expect(scalableTarget.Properties.MinCapacity).toBe(0);
  });

  test('a deployment prepares the fleet rather than starting a batch', () => {
    // Also what makes the first deployment work at all: the image tag does not exist until the
    // pipeline has pushed to the repository this stack creates.
    expect(service.Properties.DesiredCount).toBe(0);
  });

  test('the deployment circuit breaker is off', () => {
    // A task that exits promptly is normal here — an empty queue, a saturated pool, and a
    // finished batch all end that way. With the breaker on, a healthy idle fleet reads as a
    // failed deployment and rolls itself back.
    const configuration = service.Properties.DeploymentConfiguration ?? {};
    expect(configuration.DeploymentCircuitBreaker).toBeUndefined();
  });
});

describe('the fleet fits an account nobody has prepared', () => {
  test('the shipped default is within a new account Fargate quota', () => {
    // The quota counts vCPUs, not tasks, and defaults to 6. An author who checks their own
    // account reads thousands, because increases accumulate, and ships a sample that fails on
    // the reader's first deployment with an error naming nothing about this pattern.
    const context = deployedContext();
    const vcpu =
      (Number(context['fleet:maxTasks']) * Number(context['fleet:taskCpu'])) / 1024;

    expect(vcpu).toBeLessThanOrEqual(DEFAULT_FARGATE_ONDEMAND_VCPU_QUOTA);
  });

  test('maximum capacity is the configured ceiling, not an open one', () => {
    expect(scalableTarget.Properties.MaxCapacity).toBe(
      Number(deployedContext()['fleet:maxTasks']),
    );
  });

  test('a fleet sized past the default quota says so', () => {
    // A warning and not an error: an account that has asked for more should not be blocked by
    // a default. But it has to be said, or the reader meets it as a quota error instead.
    const oversized = fleetStacks({ 'fleet:maxTasks': 64, 'fleet:taskCpu': 1024 });

    Annotations.fromStack(oversized.compute).hasWarning(
      '*',
      Match.stringLikeRegexp('64 Fargate vCPUs'),
    );
  });

  test('memory stays above what the CLI binary forces', () => {
    // The chat binary is 829 MB and a cgroup is charged for the file-backed pages of a binary
    // it runs, so a task under 2 GB spends its life reclaiming pages it is about to need.
    expect(Number(taskDefinition.Properties.Memory)).toBeGreaterThanOrEqual(2048);
  });
});

describe('the fleet grows on the signal the function publishes', () => {
  test('scaling tracks backlog per task, not queue depth', () => {
    // Depth alone cannot say whether the fleet is keeping up: a thousand items behind two
    // tasks and behind two hundred are the same number and different situations.
    template.hasResourceProperties('AWS::ApplicationAutoScaling::ScalingPolicy', {
      PolicyType: 'TargetTrackingScaling',
      TargetTrackingScalingPolicyConfiguration: Match.objectLike({
        CustomizedMetricSpecification: Match.objectLike({
          Namespace: FLEET_METRICS.namespace,
          MetricName: FLEET_METRICS.backlogPerTask,
        }),
      }),
    });
  });

  test('the metric it tracks is the one the worker namespace does not carry', () => {
    // A task cannot compute backlog per task; it does not know how many tasks are running.
    // Pointing the policy at the pool's namespace would produce a fleet that never scales.
    expect(FLEET_METRICS.namespace).not.toBe(POOL_METRICS_NAMESPACE);
    expect(container().Environment).toEqual(
      expect.arrayContaining([
        { Name: 'METRICS_NAMESPACE', Value: POOL_METRICS_NAMESPACE },
      ]),
    );
  });

  test('scale-in is slower than scale-out', () => {
    // Stopping a task gives up a warm image and a held subscription, so it is worth being sure
    // the backlog has gone. Starting one costs a container start.
    const policy = Object.values(
      template.findResources('AWS::ApplicationAutoScaling::ScalingPolicy'),
    )[0].Properties.TargetTrackingScalingPolicyConfiguration;

    expect(policy.ScaleInCooldown).toBeGreaterThan(policy.ScaleOutCooldown);
  });
});

describe('the task definition matches the image', () => {
  test('architecture comes from the parameter', () => {
    // A mismatch does not fail. It runs under emulation, which silently changes every timing
    // this image is used to measure.
    expect(taskDefinition.Properties.RuntimePlatform).toEqual({
      CpuArchitecture: 'ARM64',
      OperatingSystemFamily: 'LINUX',
    });
  });

  test('the parameter is honoured, and not defaulted past', () => {
    const intel = Template.fromStack(
      fleetStacks({ 'fleet:targetArchitecture': 'x86_64' }).compute,
    );

    intel.hasResourceProperties('AWS::ECS::TaskDefinition', {
      RuntimePlatform: Match.objectLike({ CpuArchitecture: 'X86_64' }),
    });
  });

  test('the workflow the image carries is named, not guessed', () => {
    expect(container().Environment).toEqual(
      expect.arrayContaining([
        { Name: 'WORKFLOW_DIR', Value: '/opt/workflows/examples/repo-file-summary' },
      ]),
    );
  });

  test('every variable the entrypoint requires is present', () => {
    // The entrypoint refuses to start without these rather than guessing a table name and
    // operating against the wrong pool. Absent one, a task exits 78 on every start.
    const names = container().Environment.map((entry: any) => entry.Name);

    for (const required of [
      'SUBSCRIPTIONS_TABLE',
      'LEASES_TABLE',
      'WORK_QUEUE_URL',
      'RESULTS_BUCKET',
      'WORKFLOW_DIR',
    ]) {
      expect(names).toContain(required);
    }
  });

  test('no key is injected at task start', () => {
    // Nothing at task start knows which subscription this task will hold — the allocator
    // decides that later — so the container resolves the secret itself. The 8192-character cap
    // on container overrides would make passing keys in unworkable at pool scale anyway.
    expect(container().Secrets).toBeUndefined();
  });

  test('a stopping task gets as long as Fargate allows', () => {
    // 120 seconds is a ceiling, not a preference, and it is why the worker stops between work
    // items rather than draining the one it is on: a fifteen-minute run does not fit.
    expect(container().StopTimeout).toBe(120);
  });
});

describe('the two roles are separate, and each is narrow', () => {
  let executionRole: string;
  let taskRole: string;

  beforeAll(() => {
    executionRole = logicalIdOf('AWS::IAM::Role', (properties) =>
      String(properties.Description ?? '').startsWith('ECS agent'),
    );
    taskRole = logicalIdOf('AWS::IAM::Role', (properties) =>
      String(properties.Description ?? '').startsWith('Worker'),
    );
  });

  test('they are two roles, and the task definition uses both', () => {
    expect(executionRole).not.toBe(taskRole);
    expect(JSON.stringify(taskDefinition.Properties.ExecutionRoleArn)).toContain(
      executionRole,
    );
    expect(JSON.stringify(taskDefinition.Properties.TaskRoleArn)).toContain(taskRole);
  });

  test('neither carries a managed policy', () => {
    // `AmazonECSTaskExecutionRolePolicy` permits pulling any image in the account and writing
    // to any log group. Attaching it would make the split cosmetic.
    for (const role of [executionRole, taskRole]) {
      const properties = template.findResources('AWS::IAM::Role')[role].Properties;
      expect(properties.ManagedPolicyArns ?? []).toEqual([]);
    }
  });

  test('neither permits everything on everything', () => {
    for (const role of [executionRole, taskRole]) {
      for (const statement of statementsFor(role)) {
        const actions =
          typeof statement.Action === 'string' ? [statement.Action] : statement.Action;
        const wildcardAction = actions.includes('*');
        const wildcardResource =
          statement.Resource === '*' ||
          (Array.isArray(statement.Resource) && statement.Resource.includes('*'));

        expect(wildcardAction && wildcardResource).toBe(false);
      }
    }
  });

  test('the only wildcard resource is the one action that has no resource', () => {
    // `ecr:GetAuthorizationToken` has nothing to name. Everything else is scoped, and this
    // assertion is what keeps that true as grants are added.
    const wildcardStatements = statementsFor(executionRole).filter(
      (statement) => statement.Resource === '*',
    );

    expect(wildcardStatements).toHaveLength(1);
    expect(wildcardStatements[0].Action).toBe('ecr:GetAuthorizationToken');
  });

  test('the execution role cannot read a subscription key', () => {
    // The split is only real if this holds. ECS never resolves the secret; the task does,
    // under its own role, once it knows which subscription it holds.
    expect(actionsFor(executionRole)).not.toContain('secretsmanager:GetSecretValue');
  });

  test('the task role cannot pull images or create log groups', () => {
    const actions = actionsFor(taskRole);

    expect(actions.some((action) => action.startsWith('ecr:'))).toBe(false);
    expect(actions).not.toContain('logs:CreateLogGroup');
  });

  test('the task role can take work but not create it', () => {
    // A worker that could enqueue could extend its own batch. Seeding is a separate function
    // with a separate role.
    const actions = actionsFor(taskRole);

    expect(actions).toContain('sqs:ReceiveMessage');
    expect(actions).toContain('sqs:DeleteMessage');
    expect(actions).not.toContain('sqs:SendMessage');
  });

  test('the task role cannot remove a subscription or a result', () => {
    // Both are destructive in a way the workload never needs. `grantWrite` on a bucket would
    // have included `s3:DeleteObject*`, which is why the S3 grants are spelled out.
    const actions = actionsFor(taskRole);

    expect(actions.some((action) => action.startsWith('s3:DeleteObject'))).toBe(false);

    const registry = statementsFor(taskRole).find((statement) =>
      JSON.stringify(statement.Resource).includes('SubscriptionsTable'),
    );
    expect(registry).toBeDefined();
    expect(registry.Action).not.toContain('dynamodb:DeleteItem');
  });

  test('the task role reaches the pool with exactly the actions the worker calls', () => {
    // GetItem for the budget check before each work item, UpdateItem for the reservation,
    // release and the conditional `/usage` write, Query for the candidate index. Recording
    // credits needed nothing new; this pins it so a later grant is a visible decision.
    const actions = statementsFor(taskRole)
      .filter((statement) => JSON.stringify(statement.Resource).includes('SubscriptionsTable'))
      .flatMap((statement) =>
        typeof statement.Action === 'string' ? [statement.Action] : statement.Action,
      )
      .sort();

    expect(actions).toEqual(['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:UpdateItem']);
  });

  test('the pool keys are reachable by prefix, so a subscription is not a deployment', () => {
    const secretStatement = statementsFor(taskRole).find((statement) =>
      JSON.stringify(statement.Action).includes('secretsmanager'),
    );

    expect(secretStatement).toBeDefined();
    expect(JSON.stringify(secretStatement.Resource)).toContain('/subscription/*');
  });

  test('the task role can update scale-in protection on this cluster’s tasks, and nothing else in ECS', () => {
    // The ECS agent's PUT endpoint calls only `UpdateTaskProtection`, with the task role. The
    // worker never reads the state back, so `GetTaskProtection` is not granted. A task id is
    // unknown before the task starts, so this cluster's tasks are as narrow as the action allows.
    const ecsStatements = statementsFor(taskRole).filter((statement) =>
      (typeof statement.Action === 'string' ? [statement.Action] : statement.Action).some(
        (action: string) => action.startsWith('ecs:'),
      ),
    );

    expect(ecsStatements).toHaveLength(1);
    expect(ecsStatements[0].Action).toBe('ecs:UpdateTaskProtection');
    expect(actionsFor(taskRole)).not.toContain('ecs:GetTaskProtection');

    const cluster = logicalIdOf('AWS::ECS::Cluster', () => true);
    expect(ecsStatements[0].Resource).toEqual({
      'Fn::Join': [
        '',
        [
          'arn:',
          { Ref: 'AWS::Partition' },
          ':ecs:us-east-1:123456789012:task/',
          { Ref: cluster },
          '/*',
        ],
      ],
    });
  });

  test('the workspace bucket is granted to nobody', () => {
    // DataStack provisions it and nothing in the worker uses it: the workspace is the task's
    // own ephemeral disk. An unused grant is still a grant.
    expect(JSON.stringify(statementsFor(taskRole))).not.toContain('WorkspaceBucket');
  });
});

describe('exposure', () => {
  test('tasks have no public address', () => {
    expect(service.Properties.NetworkConfiguration.AwsvpcConfiguration.AssignPublicIp).toBe(
      'DISABLED',
    );
  });

  test('a shell into a task holding a live key is not available', () => {
    expect(service.Properties.EnableExecuteCommand).toBe(false);
  });

  test('logs expire', () => {
    template.hasResourceProperties('AWS::Logs::LogGroup', {
      RetentionInDays: Match.anyValue(),
    });
  });

  test('images are scanned and do not accumulate', () => {
    template.hasResourceProperties('AWS::ECR::Repository', {
      ImageScanningConfiguration: { ScanOnPush: true },
      LifecyclePolicy: Match.objectLike({ LifecyclePolicyText: Match.anyValue() }),
    });
  });
});
