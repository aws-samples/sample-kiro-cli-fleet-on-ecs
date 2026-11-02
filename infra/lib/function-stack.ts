/**
 * The three things that have to happen when no task is running.
 *
 * Each exists because the fleet scales to zero, which means there are long stretches with
 * nothing to run any of this. Seeding starts a batch; reconciliation recovers capacity the
 * tasks could not return themselves; the scaling signal is what causes the first task to start
 * at all, so it cannot live in a task.
 *
 * A fourth counts how each task stopped. A task that crashes or is killed cannot report its own
 * end, so the count is taken from the ECS event that every stop produces.
 *
 * A fifth, optional function reads Kiro's daily activity report into the pool's credit figures.
 * It exists only when the report has been enabled and its bucket named in context.
 *
 * The code is the worker's, unchanged. These are the same modules the container runs, packaged
 * as functions — not a reimplementation of them. A second copy of the reconciliation logic
 * would drift from the first, and the drift would show up as capacity that leaks on one path
 * and not the other.
 */

import { ArnFormat, Duration, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { SqsDlq, DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as path from 'path';
import { ComputeStack } from './compute-stack';
import { DataStack } from './data-stack';
import { QueueStack } from './queue-stack';
import { ActivityReportConfig, FLEET_METRICS, FleetConfig, fleetConfig } from './config';

export interface FunctionStackProps extends StackProps {
  readonly data: DataStack;
  readonly queue: QueueStack;
  readonly compute: ComputeStack;
}

/**
 * How often the fleet signal is published.
 *
 * One minute, which is the shortest a schedule allows and also what target tracking wants: the
 * policy evaluates against the most recent datapoint, so a five-minute period would mean a
 * fleet reacting to a backlog five minutes stale — scaling out after the burst and in before
 * the work is done.
 */
const SIGNAL_PERIOD = Duration.minutes(1);

/**
 * How often reconciliation sweeps the pool.
 *
 * The stream handles the prompt case. This is the one that guarantees the result, because
 * DynamoDB gives no upper bound on when a TTL deletion happens and a stream record can be
 * missed. Five minutes bounds how long stranded capacity can stay stranded.
 */
const RECONCILE_PERIOD = Duration.minutes(5);

/** The worker package, as a Lambda asset. */
const WORKER_ROOT = path.join(__dirname, '../../worker');

export class FunctionStack extends Stack {
  readonly seeding: lambda.Function;
  readonly reconciler: lambda.Function;
  readonly scalingSignal: lambda.Function;
  readonly taskStopped: lambda.Function;
  /** Only when `fleet:activityReportBucket` is set. */
  readonly usageReport?: lambda.Function;

  constructor(scope: Construct, id: string, props: FunctionStackProps) {
    super(scope, id, props);
    const config = fleetConfig(this);

    this.seeding = this.createSeeding(config, props);
    this.reconciler = this.createReconciler(config, props);
    this.scalingSignal = this.createScalingSignal(config, props);
    this.taskStopped = this.createTaskStopped(config, props);
    if (config.activityReport) {
      this.usageReport = this.createUsageReport(config, config.activityReport, props);
    }
  }

  /**
   * Shared shape, deliberately not shared permissions.
   *
   * Each function gets its own role, which is the point of having three of them rather than one
   * that switches on an event shape. A single role would carry the union: the seeder could
   * disable a subscription, the reconciler could enqueue work. Neither has any reason to.
   */
  private base(
    id: string,
    handler: string,
    config: FleetConfig,
    description: string,
    timeout: Duration,
    functionName?: string,
  ): lambda.Function {
    return new lambda.Function(this, id, {
      description,
      functionName,
      runtime: lambda.Runtime.PYTHON_3_14,
      architecture:
        config.architectureName === 'arm64' ? lambda.Architecture.ARM_64 : lambda.Architecture.X86_64,
      handler,
      // The worker's own source. `boto3` comes with the runtime, so there is nothing to
      // install and therefore no build step that could install something different from what
      // the tests ran against.
      code: lambda.Code.fromAsset(WORKER_ROOT, {
        exclude: [
          'tests',
          '.venv',
          '**/__pycache__',
          '*.pyc',
          'Dockerfile',
          'entrypoint.sh',
          'README.md',
          'uv.lock',
          'pyproject.toml',
        ],
      }),
      timeout,
      memorySize: 512,
      logRetention: config.logRetention,
      environment: { LOG_LEVEL: 'INFO' },
    });
  }

  /**
   * Reads a manifest and enqueues one message per item.
   *
   * Timeout is generous rather than long, because the function does not rely on it: a pass
   * sends a bounded number of items and continues in a fresh invocation. That is what keeps a
   * manifest of several thousand items from being one invocation against a wall — and a
   * timeout half way through seeding leaves a batch nobody can describe.
   */
  private createSeeding(config: FleetConfig, props: FunctionStackProps): lambda.Function {
    // Named explicitly, which is what lets the continuation grant below exist at all. A
    // policy referencing `seeding.functionArn` makes the function depend on its own role
    // policy and the policy depend on the function — CloudFormation rejects the cycle. With a
    // literal name the ARN is formatted rather than referenced. The stack name is in it so two
    // deployments can share an account.
    const functionName = `${this.stackName}-seeding`;
    const seeding = this.base(
      'Seeding',
      'seeding.handler.handler',
      config,
      'Reads a manifest and enqueues one work item per entry, a page per invocation',
      Duration.minutes(5),
      functionName,
    );

    seeding.addEnvironment('WORK_QUEUE_URL', props.queue.workQueue.queueUrl);
    seeding.addEnvironment('REFERENCE_BUCKET', props.data.referenceBucket.bucketName);

    // Send only. It has no reason to receive or delete, and a seeder that could do either
    // would be able to consume the batch it just created.
    props.queue.workQueue.grant(seeding, 'sqs:SendMessage', 'sqs:SendMessageBatch');
    // The manifest and the inputs it lists.
    props.data.referenceBucket.grantRead(seeding);
    // Continuing itself. Scoped to its own ARN: a function that may invoke anything is a
    // broader grant than the continuation needs.
    seeding.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['lambda:InvokeFunction'],
        resources: [
          // COLON_RESOURCE_NAME because a Lambda ARN is `function:name`. The default slash
          // form produces `function/name`, which matches nothing: the grant would look right
          // and the continuation would fail, stopping a long manifest half way through.
          this.formatArn({
            service: 'lambda',
            resource: 'function',
            resourceName: functionName,
            arnFormat: ArnFormat.COLON_RESOURCE_NAME,
          }),
        ],
      }),
    );

    return seeding;
  }

  /**
   * Returns capacity that expiry released without decrementing.
   *
   * Both triggers, because they guarantee different things. The lease stream makes recovery
   * prompt; the schedule makes it certain. Treating the stream as the mechanism would leave a
   * counter wrong indefinitely on a missed record, and treating the schedule as a backstop
   * misreads which one is load-bearing.
   */
  private createReconciler(config: FleetConfig, props: FunctionStackProps): lambda.Function {
    const reconciler = this.base(
      'Reconciler',
      'reconciler.handler.handler',
      config,
      'Decrements counters for leases that expired, and corrects drift on a schedule',
      Duration.minutes(2),
    );

    reconciler.addEnvironment('SUBSCRIPTIONS_TABLE', props.data.subscriptionsTable.tableName);
    reconciler.addEnvironment('LEASES_TABLE', props.data.leasesTable.tableName);

    // The scheduled pass scans every subscription (`Registry.describe_all`), reads one back
    // before resetting its credit window, and corrects counters, so it reads and writes the
    // registry. It has no reason to create or remove a subscription, and never queries one.
    props.data.subscriptionsTable.grant(
      reconciler,
      'dynamodb:GetItem',
      'dynamodb:UpdateItem',
      'dynamodb:Scan',
    );
    // Lists each subscription's leases through the by-subscription index, reads each back for
    // its expiry (the index projects keys only), and deletes the ones that have expired
    // (`Leases.reap_expired`). Without the delete, a lease that expired but that the
    // time-to-live service has not removed yet keeps its slot held, and TTL gives no bound on
    // when it acts. The delete is conditional on the lease being past expiry, so it can remove
    // nothing TTL would not. No Put or Update: it cannot create a lease or extend one, and no
    // Scan, because nothing reads the whole table.
    props.data.leasesTable.grant(
      reconciler,
      'dynamodb:Query',
      'dynamodb:GetItem',
      'dynamodb:DeleteItem',
    );

    reconciler.addEventSource(
      new DynamoEventSource(props.data.leasesTable, {
        startingPosition: lambda.StartingPosition.TRIM_HORIZON,
        batchSize: 25,
        // So one poisonous record cannot hold up every later recovery behind it.
        retryAttempts: 3,
        bisectBatchOnError: true,
        onFailure: new SqsDlq(props.queue.deadLetterQueue),
        // Expiry is not urgent to the second, and batching keeps invocations down on a table
        // that is quiet most of the time.
        maxBatchingWindow: Duration.seconds(30),
      }),
    );

    new events.Rule(this, 'ReconcileSchedule', {
      description: 'The guarantee that capacity returns, as opposed to the stream, which is speed',
      schedule: events.Schedule.rate(RECONCILE_PERIOD),
      targets: [new targets.LambdaFunction(reconciler)],
    });

    return reconciler;
  }

  /**
   * Publishes the signal autoscaling follows, and narrows the fleet's ceiling to the pool.
   *
   * The only one of the three that cannot be anything but a function. Backlog per task needs
   * the number of running tasks, which no task can see, and it has to be published when the
   * fleet is empty — that is the moment the number is supposed to cause a task to start.
   */
  private createScalingSignal(
    config: FleetConfig,
    props: FunctionStackProps,
  ): lambda.Function {
    const signal = this.base(
      'ScalingSignal',
      'scaling.handler.handler',
      config,
      'Publishes backlog per task, and holds the fleet ceiling at the pool concurrency',
      Duration.seconds(30),
    );

    signal.addEnvironment('WORK_QUEUE_URL', props.queue.workQueue.queueUrl);
    signal.addEnvironment('SUBSCRIPTIONS_TABLE', props.data.subscriptionsTable.tableName);
    signal.addEnvironment('CLUSTER_NAME', props.compute.cluster.clusterName);
    signal.addEnvironment('SERVICE_NAME', props.compute.service.serviceName);
    signal.addEnvironment('MAX_TASKS', String(config.maxTasks));
    // The same target the scaling policy holds, so a start from zero asks for the count target
    // tracking would settle on and the two never pull the desired count in opposite directions.
    signal.addEnvironment('TARGET_BACKLOG_PER_TASK', String(config.targetBacklogPerTask));
    // The workers' margin, from the same config value the task definition carries, so the
    // function counts a subscription as claimable exactly when a claim would be granted.
    signal.addEnvironment('CREDIT_MARGIN', config.creditMargin);
    signal.addEnvironment('METRICS_NAMESPACE', FLEET_METRICS.namespace);

    // Depth only. It never touches a work item.
    props.queue.workQueue.grant(signal, 'sqs:GetQueueAttributes');
    // Reads the pool through the state index to sum the concurrency of subscriptions that can
    // be claimed. The index projects every attribute, so the budget figures come with the same
    // Query and nothing else is read. Read only — it decides a ceiling, not the pool's contents.
    props.data.subscriptionsTable.grant(signal, 'dynamodb:Query');

    signal.addToRolePolicy(
      new iam.PolicyStatement({
        // How many tasks are running, which is the denominator — and what the service is set
        // to and has pending, which the drain and start decisions need.
        actions: ['ecs:DescribeServices'],
        resources: ['*'],
        conditions: { ArnEquals: { 'ecs:cluster': props.compute.cluster.clusterArn } },
      }),
    );
    signal.addToRolePolicy(
      new iam.PolicyStatement({
        // Setting the service to zero when the batch is finished. Target tracking cannot do
        // this promptly: its scale-in alarm is created with fifteen evaluation periods, so a
        // drained queue takes about fifteen minutes — during which a worker that finds an
        // empty queue exits, the service replaces it, and the fleet churns through task
        // starts paying for nothing. Measured on a deployed fleet before this was added.
        //
        // And taking an idle service off zero when work arrives. Target tracking scales out
        // only above its target, and at zero tasks the metric is the whole backlog, so a batch
        // of `targetBacklogPerTask` items or fewer never started. Also measured.
        //
        // Scoped to this one service. A function that could scale any service is a wider
        // grant than these two writes require.
        actions: ['ecs:UpdateService'],
        resources: [props.compute.service.serviceArn],
      }),
    );
    signal.addToRolePolicy(
      new iam.PolicyStatement({
        // Holding the maximum at the pool's concurrency, and suspending scale-in while a start
        // from zero is exposed to the scale-in alarm left over from the idle period. The
        // maximum is written every run rather than compared first; `DescribeScalableTargets`
        // reads back only whether the previous run left scale-in suspended.
        actions: [
          'application-autoscaling:RegisterScalableTarget',
          'application-autoscaling:DescribeScalableTargets',
        ],
        resources: ['*'],
      }),
    );
    signal.addToRolePolicy(
      new iam.PolicyStatement({
        // The state of target tracking's scale-in alarm, which decides when the suspension
        // is lifted. Read only. Looked up by the name prefix Application Auto Scaling gives
        // it, which is why it cannot be scoped to one alarm ARN known at deploy time.
        actions: ['cloudwatch:DescribeAlarms'],
        resources: ['*'],
      }),
    );

    new events.Rule(this, 'SignalSchedule', {
      description: 'Publishes the fleet signal; this is what starts the first task',
      schedule: events.Schedule.rate(SIGNAL_PERIOD),
      targets: [new targets.LambdaFunction(signal)],
    });

    return signal;
  }

  /**
   * Counts every stopped worker task by why it stopped.
   *
   * From outside the task, because the stops worth counting are the ones a task cannot report:
   * a crash or a SIGKILL ends the process before it can publish anything. ECS emits an `ECS Task
   * State Change` event for every stop, and the rule passes only this service's tasks, so a task
   * started by hand in the same cluster is not counted as the fleet losing one.
   *
   * No permissions beyond its logs: the count is written to stdout as embedded metric format.
   */
  private createTaskStopped(config: FleetConfig, props: FunctionStackProps): lambda.Function {
    const counter = this.base(
      'TaskStopped',
      'taskstopped.handler.handler',
      config,
      'Counts each stopped worker task by why it stopped',
      Duration.seconds(30),
    );

    counter.addEnvironment('METRICS_NAMESPACE', FLEET_METRICS.namespace);

    new events.Rule(this, 'TaskStoppedEvents', {
      description: 'Every stop of a worker task, including the ones the task cannot report',
      eventPattern: {
        source: ['aws.ecs'],
        detailType: ['ECS Task State Change'],
        detail: {
          clusterArn: [props.compute.cluster.clusterArn],
          group: [`service:${props.compute.service.serviceName}`],
          lastStatus: ['STOPPED'],
        },
      },
      targets: [new targets.LambdaFunction(counter)],
    });

    return counter;
  }

  /**
   * Raises `creditsUsed` from Kiro's daily activity report, the backstop for `/usage` readings.
   *
   * Runs at 03:30 UTC, ninety minutes after Kiro writes the day's files at 02:00. Each pass
   * recomputes whole windows from every file, so a file that arrives late is counted the next
   * day.
   *
   * Reads the report and the pool, and raises one attribute. It cannot touch leases, keys, or
   * the queue, and it cannot write or delete anything in the report bucket. A bucket encrypted
   * with a customer managed KMS key also needs `kms:Decrypt` on that key, which is not granted
   * here because the key is not known to this stack.
   */
  private createUsageReport(
    config: FleetConfig,
    report: ActivityReportConfig,
    props: FunctionStackProps,
  ): lambda.Function {
    const ingester = this.base(
      'UsageReport',
      'usagereport.handler.handler',
      config,
      'Raises creditsUsed from the daily Kiro activity report where it is higher',
      Duration.minutes(5),
    );

    // Kiro's layout below the prefix the operator chose.
    const reportPrefix =
      `${report.prefix}/AWSLogs/${this.account}/KiroLogs/user_report/${report.region}/`;

    ingester.addEnvironment('SUBSCRIPTIONS_TABLE', props.data.subscriptionsTable.tableName);
    ingester.addEnvironment('REPORT_BUCKET', report.bucketName);
    ingester.addEnvironment('REPORT_PREFIX', reportPrefix);
    ingester.addEnvironment('REPORT_REGION', report.region);

    // A scan to find mapped subscriptions and their windows, and the conditional raise.
    props.data.subscriptionsTable.grant(ingester, 'dynamodb:Scan', 'dynamodb:UpdateItem');

    const bucket = s3.Bucket.fromBucketName(this, 'ActivityReportBucket', report.bucketName);
    ingester.addToRolePolicy(
      new iam.PolicyStatement({
        // Listing, limited to the report's own prefix.
        actions: ['s3:ListBucket'],
        resources: [bucket.bucketArn],
        conditions: { StringLike: { 's3:prefix': [`${reportPrefix}*`] } },
      }),
    );
    ingester.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['s3:GetObject'],
        resources: [bucket.arnForObjects(`${reportPrefix}*`)],
      }),
    );

    new events.Rule(this, 'UsageReportSchedule', {
      description: 'Reads the daily Kiro activity report after it is written at 02:00 UTC',
      schedule: events.Schedule.cron({ minute: '30', hour: '3' }),
      targets: [new targets.LambdaFunction(ingester)],
    });

    return ingester;
  }
}
