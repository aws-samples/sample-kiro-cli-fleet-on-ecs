/**
 * The fleet: a cluster, one task definition, and a service that is empty until there is work.
 *
 * Three things here are less obvious than they look, and each is a property a reader would
 * otherwise have to discover by deploying.
 *
 * **The service starts at zero, and every deployment returns it to zero.** That is deliberate
 * twice over. A deployment should prepare a fleet rather than run a batch nobody asked for, and
 * on the very first deployment the image tag does not exist yet — the repository is created by
 * this stack and filled by the pipeline afterwards — so a service that started a task would fail
 * on an image it cannot pull. The cost is that deploying in the middle of a batch scales the
 * fleet in. That is a pause and not a loss: a task protects itself from scale-in while it holds
 * a work item, so the deployment waits for each task's current item (or for its protection to
 * expire, 20 minutes with the default timeout) and stops tasks between items. An item whose
 * protection request ECS refuses (for example with `DEPLOYMENT_BLOCKED`) runs unprotected and can
 * be stopped mid-run; the worker counts it as `TaskProtectionFailed`. The items not yet taken stay
 * in the queue, and autoscaling starts the fleet back up within a minute.
 *
 * **The deployment circuit breaker is off.** Its job is to roll back when tasks keep failing to
 * stay up, and here a task that exits promptly is the normal case: an empty queue, a saturated
 * pool, and a finished batch all end with exit 0 and no replacement needed. With the breaker on,
 * the first deployment into an idle fleet would roll itself back.
 *
 * **The two roles are built by hand.** The generated pair is fine, but the execution role would
 * carry a managed policy that permits pulling any image in the account and writing to any log
 * group, and the point of splitting the roles is lost if one of them is broad. So each is
 * constructed with no managed policy and granted the named resources it touches.
 */

import {
  Annotations,
  ArnFormat,
  CfnOutput,
  Duration,
  RemovalPolicy,
  Stack,
  StackProps,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import { DataStack } from './data-stack';
import { NetworkStack } from './network-stack';
import { QueueStack } from './queue-stack';
import {
  DEFAULT_FARGATE_ONDEMAND_VCPU_QUOTA,
  FLEET_METRICS,
  FleetConfig,
  IMAGE_WORKFLOWS_DIR,
  POOL_METRICS_NAMESPACE,
  fleetConfig,
  peakVcpu,
} from './config';

export interface ComputeStackProps extends StackProps {
  readonly network: NetworkStack;
  readonly data: DataStack;
  readonly queue: QueueStack;
}

/**
 * The longest Fargate will wait between SIGTERM and SIGKILL.
 *
 * A hard ceiling, not a preference. A fifteen-minute run cannot finish in two minutes, which is
 * why a task protects itself from scale-in for the length of a work item instead of relying on
 * the stop timeout: scale-in and deployments then stop it between items, when it holds nothing.
 * SIGTERM during an item now comes only from causes protection does not cover (`StopTask`, task
 * retirement, an item whose protection request ECS refused); the worker returns its lease and
 * exits, and the item reappears after the visibility timeout.
 */
const FARGATE_MAX_STOP_TIMEOUT = Duration.seconds(120);

/** Images kept per repository. Enough to roll back a few deployments, not enough to accumulate. */
const IMAGES_RETAINED = 10;

export class ComputeStack extends Stack {
  readonly cluster: ecs.Cluster;
  readonly service: ecs.FargateService;
  readonly taskDefinition: ecs.FargateTaskDefinition;
  readonly repository: ecr.Repository;
  readonly logGroup: logs.LogGroup;

  /**
   * The scalable target's identifier, for the function that narrows its ceiling.
   *
   * Exposed as a string because the second bound on capacity is applied at runtime, from
   * FunctionStack — see `addAutoscaling`.
   */
  readonly scalableTargetResourceId: string;

  constructor(scope: Construct, id: string, props: ComputeStackProps) {
    super(scope, id, props);
    const config = fleetConfig(this);

    this.repository = this.createRepository();
    this.logGroup = new logs.LogGroup(this, 'WorkerLogs', {
      logGroupName: `/aws/ecs/${this.stackName}/worker`,
      retention: config.logRetention,
      // Logs of a finished batch are reproducible by running it again; results are not, and
      // those are retained in DataStack.
      removalPolicy: RemovalPolicy.DESTROY,
    });

    this.cluster = new ecs.Cluster(this, 'Cluster', {
      vpc: props.network.vpc,
      containerInsights: config.containerInsights,
      // Capacity providers are deliberately not enabled. They would be the mechanism for mixing
      // Fargate Spot into the fleet, which this sample does not do — and associating one has a
      // cost at teardown that is easy to miss: a cluster whose tasks ran under a capacity
      // provider refuses to delete for a while afterwards with "the specified capacity provider
      // is in use", which is how the first attempted teardown failed. Without a strategy the
      // service uses the plain Fargate launch type and the cluster deletes immediately.
    });

    this.taskDefinition = this.createTaskDefinition(config, props);
    this.service = this.createService(props);
    this.scalableTargetResourceId = `service/${this.cluster.clusterName}/${this.service.serviceName}`;
    this.addAutoscaling(config);
    this.publishOutputs(config);
  }

  private createRepository(): ecr.Repository {
    return new ecr.Repository(this, 'WorkerImage', {
      // Deterministic, because the pipeline pushes to it by name without a CloudFormation
      // reference. A reference would make the pipeline depend on this stack, and this stack is
      // deployed by the pipeline.
      repositoryName: `${this.stackName.toLowerCase()}-worker`,
      imageScanOnPush: true,
      // Tags move, because `latest` has to. What pins a measurement is the commit tag the
      // pipeline also applies, not this setting.
      imageTagMutability: ecr.TagMutability.MUTABLE,
      lifecycleRules: [{ maxImageCount: IMAGES_RETAINED }],
      // An image is rebuildable from a commit, so keeping the repository after a teardown
      // would leave storage charges behind for something already in git.
      removalPolicy: RemovalPolicy.DESTROY,
      emptyOnDelete: true,
    });
  }

  private createTaskDefinition(
    config: FleetConfig,
    props: ComputeStackProps,
  ): ecs.FargateTaskDefinition {
    const executionRole = this.createExecutionRole();
    const taskRole = this.createTaskRole(props);

    const taskDefinition = new ecs.FargateTaskDefinition(this, 'WorkerTask', {
      family: `${this.stackName}-worker`,
      cpu: config.taskCpu,
      memoryLimitMiB: config.taskMemoryMiB,
      executionRole,
      taskRole,
      runtimePlatform: {
        // From the parameter, not a literal. A mismatch against the image does not fail: it
        // runs under emulation, which would silently change every timing measured from this
        // image. The Dockerfile takes the same value.
        cpuArchitecture: config.architecture,
        operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
      },
      // Ephemeral storage left at the Fargate default of 20 GiB. The image is 1.6 GB on disk
      // and the workspace holds one work item's files, so the default has room; raising it is
      // billed per GiB-hour above 20.
    });

    taskDefinition.addContainer('Worker', {
      image: ecs.ContainerImage.fromEcrRepository(this.repository, config.imageTag),
      // The only container, so its failure is the task's failure.
      essential: true,
      logging: ecs.LogDrivers.awsLogs({
        logGroup: this.logGroup,
        streamPrefix: 'worker',
      }),
      // As long as Fargate permits. See the constant.
      stopTimeout: FARGATE_MAX_STOP_TIMEOUT,
      // Nothing in the image needs to write outside the paths the Dockerfile creates, and a
      // read-only root is the cheapest way to keep that true — except that the CLI writes its
      // own configuration under HOME, so the filesystem cannot be read-only without a volume
      // for it. Left writable rather than half-done; the container runs as a non-root user.
      environment: this.workerEnvironment(config, props),
      // No `secrets`. The container reads the subscription key itself, after the allocator has
      // decided which one it holds — ECS-injected secrets are resolved at task start, when
      // nothing yet knows which subscription this task will get. That the total size of
      // container overrides is capped at 8192 characters makes passing keys in at run_task
      // unworkable at pool scale anyway.
    });

    return taskDefinition;
  }

  /**
   * Pulls the image and opens a log stream. Nothing else.
   *
   * Notably it cannot read the subscription secret, because ECS never resolves it — the task
   * does, under its own role, once it knows which subscription it holds.
   */
  private createExecutionRole(): iam.Role {
    const role = new iam.Role(this, 'ExecutionRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
      description: 'ECS agent: pull this image, write to this log group',
      // Deliberately no `AmazonECSTaskExecutionRolePolicy`. It permits pulling any image in
      // the account and writing to any log group, which would make the role split cosmetic.
    });

    // `ecr:GetAuthorizationToken` has no resource to name and is granted on `*` by necessity;
    // the layer reads that follow it are scoped to this repository. That is the one wildcard
    // resource in either role, and the test that forbids `*` on `*` still holds.
    this.repository.grantPull(role);
    this.logGroup.grantWrite(role);

    return role;
  }

  /**
   * What the workflow runs as: the queue, the pool, the two buckets it was given, and its own
   * scale-in protection.
   */
  private createTaskRole(props: ComputeStackProps): iam.Role {
    const role = new iam.Role(this, 'TaskRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
      description: 'Worker: claim a subscription, drain the queue, store results',
    });

    // Receive, finish, hand back, and report depth. No send: a worker that could enqueue could
    // extend its own batch, and seeding is the seeding function's job.
    props.queue.workQueue.grant(
      role,
      'sqs:ReceiveMessage',
      'sqs:DeleteMessage',
      'sqs:ChangeMessageVisibility',
      'sqs:GetQueueAttributes',
    );

    // The claim protocol: read candidates from the index, reserve, record a lease, heartbeat,
    // release. Named actions rather than `grantReadWriteData`, which would add `DeleteItem` on
    // the registry — a worker has no business removing a subscription.
    //
    // `Table.grant` covers `<table>/index/*` when the table has an index, so the candidate
    // query and the per-subscription lease count are both permitted without a second
    // statement. Worth checking rather than assuming, because the claim path is the one least
    // likely to be exercised before a real batch, and an access denial there looks like
    // saturation.
    props.data.subscriptionsTable.grant(
      role,
      'dynamodb:GetItem',
      'dynamodb:UpdateItem',
      'dynamodb:Query',
    );
    props.data.leasesTable.grant(
      role,
      'dynamodb:PutItem',
      'dynamodb:UpdateItem',
      'dynamodb:DeleteItem',
      'dynamodb:Query',
    );

    // The pool's keys, by prefix rather than one ARN, so registering a subscription does not
    // require a deployment. See DataStack for why a wildcard is unavoidable regardless.
    role.addToPolicy(
      new iam.PolicyStatement({
        actions: ['secretsmanager:GetSecretValue'],
        resources: [props.data.subscriptionSecretArnPattern],
      }),
    );

    // Read where inputs are, write where results go, and not the other way round. This is the
    // reason the two are separate buckets rather than two prefixes.
    //
    // Spelled out instead of `grantRead`/`grantWrite`, because those are wider than what the
    // worker calls. `grantWrite` includes `s3:DeleteObject*`, and a workload that can delete
    // results is one bug away from destroying the output of a batch that took hours. The
    // results bucket is versioned, so an overwrite is recoverable and a delete would not need
    // to be.
    role.addToPolicy(
      new iam.PolicyStatement({
        // `head_object` and `download_file` on declared inputs, `list_objects_v2` to expand a
        // prefix into keys.
        actions: ['s3:GetObject', 's3:ListBucket'],
        resources: [
          props.data.referenceBucket.bucketArn,
          props.data.referenceBucket.arnForObjects('*'),
        ],
      }),
    );
    role.addToPolicy(
      new iam.PolicyStatement({
        // `put_object` to store a result, and `list_objects_v2` to confirm afterwards that the
        // run produced what the workflow requires.
        actions: ['s3:PutObject', 's3:ListBucket'],
        resources: [
          props.data.resultsBucket.bucketArn,
          props.data.resultsBucket.arnForObjects('*'),
        ],
      }),
    );

    // Scale-in protection for the item in progress. The worker asks the ECS agent at
    // `$ECS_AGENT_URI/task-protection/v1/state`, and the agent calls `UpdateTaskProtection`
    // with this role. Only the update: the worker never reads the state back (the PUT's answer
    // already carries it), so `ecs:GetTaskProtection`, which the docs list for the GET call, is
    // left out. Scoped to this cluster's tasks, because a task's id is not known before it
    // starts. https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection.html
    // ("IAM permissions required for task scale-in protection").
    role.addToPolicy(
      new iam.PolicyStatement({
        actions: ['ecs:UpdateTaskProtection'],
        resources: [
          this.formatArn({
            service: 'ecs',
            resource: 'task',
            resourceName: `${this.cluster.clusterName}/*`,
            arnFormat: ArnFormat.SLASH_RESOURCE_NAME,
          }),
        ],
      }),
    );

    // Not granted: the workspace bucket DataStack also provisions. Nothing in the worker reads
    // or writes it — the workspace is the task's own ephemeral disk, and run transcripts stay
    // beside it. An unused grant is still a grant, so it is left out until a workflow needs
    // scratch that outlives its task.

    // No `cloudwatch:PutMetricData`. The worker emits embedded metric format on its log
    // stream, so metrics arrive through the log driver's credential and the workload needs no
    // CloudWatch permission of its own.

    return role;
  }

  private workerEnvironment(
    config: FleetConfig,
    props: ComputeStackProps,
  ): Record<string, string> {
    return {
      // Required by the entrypoint, which refuses to start without them rather than guessing a
      // table name and operating against the wrong pool.
      SUBSCRIPTIONS_TABLE: props.data.subscriptionsTable.tableName,
      LEASES_TABLE: props.data.leasesTable.tableName,
      WORK_QUEUE_URL: props.queue.workQueue.queueUrl,
      RESULTS_BUCKET: props.data.resultsBucket.bucketName,
      WORKFLOW_DIR: `${IMAGE_WORKFLOWS_DIR}/${config.workflowPath}`,

      REFERENCE_BUCKET: props.data.referenceBucket.bucketName,
      METRICS_NAMESPACE: POOL_METRICS_NAMESPACE,
      LOG_LEVEL: 'INFO',

      // The credit margin is set here, unlike the other tuning values, because it has a second
      // reader: the scaling function counts a subscription toward the fleet ceiling only below
      // `creditBudget - CREDIT_MARGIN`, and it gets the same value from the same config. Two
      // defaults, one per reader, could drift apart.
      CREDIT_MARGIN: config.creditMargin,

      // Same reasoning: the queue starts each item with this window and the worker renews it,
      // so both readers get it from one place.
      VISIBILITY_WINDOW_SECONDS: String(props.queue.visibilityWindow.toSeconds()),

      // Lease lifetime and retry delay are not set here on purpose. The worker already has
      // defaults for them, and a second copy in the task definition is a value that can disagree
      // with itself. What the infrastructure owns is names and locations.
    };
  }

  private createService(props: ComputeStackProps): ecs.FargateService {
    return new ecs.FargateService(this, 'WorkerService', {
      cluster: this.cluster,
      taskDefinition: this.taskDefinition,
      // Zero, and zero again on every deployment. See the class comment for both reasons and
      // for why interrupting a batch is a pause rather than a loss.
      desiredCount: 0,
      assignPublicIp: false,
      // From the network stack, not named here. There are two private tiers and only one is
      // the tasks'; the other is where the firewall's endpoint lives, and its default route
      // goes straight to the gateway.
      vpcSubnets: props.network.taskSubnets,
      securityGroups: [props.network.taskSecurityGroup],
      // A task that exits promptly is normal here, so the breaker would read a healthy idle
      // fleet as a failed deployment. See the class comment.
      circuitBreaker: undefined,
      // No load balancer, so there is no healthy count to preserve during a deployment, and
      // waiting for one would just make deployments slow.
      minHealthyPercent: 0,
      maxHealthyPercent: 100,
      // Off. It would open a shell into a task that is holding a live subscription key, and it
      // needs its own endpoints to work at all.
      enableExecuteCommand: false,
      propagateTags: ecs.PropagatedTagSource.TASK_DEFINITION,
      // Scale-in stops whichever unprotected tasks the scheduler picks, without regard to the
      // subscription they hold. A worker is protected from scale-in while it holds a work item,
      // so a task stopped this way holds none, unless ECS refused that item's protection
      // request (README, "Scale-in and the item in progress"). It receives SIGTERM and returns
      // its lease, so the capacity comes back immediately instead of waiting out the lease.
      //
      // No capacity provider strategy: see the cluster above for why, which is about teardown
      // rather than about scheduling.
    });
  }

  /**
   * Grows with the backlog, shrinks to nothing, and is bounded twice.
   *
   * The signal is backlog per task rather than queue depth, because depth alone cannot say
   * whether the fleet is keeping up: a thousand items behind two tasks and behind two hundred
   * are the same number and entirely different situations. A scheduled function publishes it
   * with `max(runningTasks + pendingTasks, 1)` as the denominator, so a fleet at zero still
   * produces a finite value and can start.
   *
   * The first bound is here: `maxTasks`, chosen so the default fleet fits a new account's
   * Fargate quota. The second is the concurrency of the subscriptions a claim could be granted
   * on (enabled, and below `creditBudget - CREDIT_MARGIN`), and it cannot be here, because it
   * is a number only the registry knows and it changes when an operator enables a subscription
   * or a budget runs out. A task beyond that point starts, fails to claim, and exits — it costs
   * money and does nothing. So the scaling function, which reads the registry anyway, narrows
   * this target's maximum at runtime. FunctionStack wires that up against
   * `scalableTargetResourceId`.
   */
  private addAutoscaling(config: FleetConfig): void {
    const peak = peakVcpu(config);
    if (peak > DEFAULT_FARGATE_ONDEMAND_VCPU_QUOTA) {
      // A warning rather than an error: an account that has asked for more should not be
      // blocked by a default. But it is said out loud, because the alternative is a reader
      // discovering it as a quota error that names nothing about this sample.
      Annotations.of(this).addWarning(
        `This fleet peaks at ${peak} Fargate vCPUs (${config.maxTasks} tasks x ` +
          `${config.taskCpu} CPU units), above the ${DEFAULT_FARGATE_ONDEMAND_VCPU_QUOTA} a ` +
          'new account is given. Request an increase for quota L-3032A538, or lower ' +
          'fleet:maxTasks or fleet:taskCpu.',
      );
    }

    const capacity = this.service.autoScaleTaskCount({
      // The cost argument in one line. Everything else here is about getting back up quickly.
      minCapacity: 0,
      maxCapacity: config.maxTasks,
    });

    capacity.scaleToTrackCustomMetric('BacklogTracking', {
      metric: new cloudwatch.Metric({
        namespace: FLEET_METRICS.namespace,
        metricName: FLEET_METRICS.backlogPerTask,
        period: Duration.minutes(1),
        statistic: cloudwatch.Stats.MAXIMUM,
      }),
      targetValue: config.targetBacklogPerTask,
      // Out quickly, in slowly. Starting a task costs a container start; stopping one gives up
      // a warm image and a held subscription, so it is worth being sure the backlog has really
      // gone before shrinking.
      scaleOutCooldown: Duration.minutes(1),
      scaleInCooldown: Duration.minutes(5),
    });
  }

  private publishOutputs(config: FleetConfig): void {
    new CfnOutput(this, 'ClusterName', { value: this.cluster.clusterName });
    new CfnOutput(this, 'ServiceName', { value: this.service.serviceName });
    new CfnOutput(this, 'WorkerImageUri', {
      value: `${this.repository.repositoryUri}:${config.imageTag}`,
      description: 'Push the worker image here; the pipeline tags it with the commit as well',
    });
    new CfnOutput(this, 'ScalableTargetResourceId', {
      value: this.scalableTargetResourceId,
      description: 'What the scaling function narrows when the pool is smaller than the fleet',
    });
    new CfnOutput(this, 'PeakFargateVcpu', {
      value: String(peakVcpu(config)),
      description: `vCPUs at maximum capacity. A new account is given ${DEFAULT_FARGATE_ONDEMAND_VCPU_QUOTA}.`,
    });
  }
}
