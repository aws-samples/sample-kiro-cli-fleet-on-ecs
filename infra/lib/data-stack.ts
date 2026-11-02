/**
 * The pool, the inputs, and the results.
 *
 * Separated from compute because this holds state. A task definition changes daily; a table
 * holding the record of which subscriptions exist and what limits they carry should not be in
 * the same deployment as something that changes daily.
 *
 * Retention is not uniform, and the asymmetry is the point. Losing the leases table costs a
 * lease lifetime of capacity, which reconciliation recovers. Losing the registry costs the
 * pool itself: which subscriptions exist, their limits, and any credit use written to them. Losing
 * results costs a benchmark run. So the registry and results are retained and recoverable,
 * and scratch is not.
 */

import { ArnFormat, CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { FleetConfig, POOL_SCHEMA, fleetConfig } from './config';

export class DataStack extends Stack {
  readonly subscriptionsTable: dynamodb.Table;
  readonly leasesTable: dynamodb.Table;
  readonly referenceBucket: s3.Bucket;
  readonly workspaceBucket: s3.Bucket;
  readonly resultsBucket: s3.Bucket;
  readonly subscriptionSecret: secretsmanager.ISecret;

  /** Where an operator creates the rest of the pool's keys. */
  readonly subscriptionSecretPrefix: string;

  /** What the task role is granted read on: the prefix, not one secret. */
  readonly subscriptionSecretArnPattern: string;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);
    const config: FleetConfig = fleetConfig(this);

    this.subscriptionsTable = this.createSubscriptionsTable();
    this.leasesTable = this.createLeasesTable();

    // Three areas rather than three prefixes in one bucket, so a task can be granted read
    // on inputs and write on results and nothing wider. Prefix-scoped policies work but are
    // easy to write slightly too broadly, and the mistake is invisible.
    //
    // Each of the three writes S3 server access logs to a fourth bucket, so who read or
    // wrote which object can be answered after the fact. The log bucket has no access
    // logging of its own (logging it would log the logging), uses SSE-S3 because log
    // delivery does not support KMS keys, and expires logs so it does not grow without
    // anyone deciding to. It is retained: an audit trail that goes with the stack is not one.
    const accessLogsBucket = new s3.Bucket(this, 'AccessLogsBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      removalPolicy: RemovalPolicy.RETAIN,
      lifecycleRules: [
        {
          id: 'expire-access-logs',
          enabled: true,
          expiration: Duration.days(90),
        },
      ],
    });

    this.referenceBucket = new s3.Bucket(this, 'ReferenceBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      serverAccessLogsBucket: accessLogsBucket,
      serverAccessLogsPrefix: 'reference/',
      removalPolicy: RemovalPolicy.RETAIN,
    });

    this.workspaceBucket = new s3.Bucket(this, 'WorkspaceBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      serverAccessLogsBucket: accessLogsBucket,
      serverAccessLogsPrefix: 'workspace/',
      // Scratch, so it goes when the stack does, and expires while the stack lives.
      // Without the lifecycle rule this is a bill that grows without anyone deciding to.
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [
        {
          id: 'expire-scratch',
          enabled: true,
          expiration: config.workspaceExpiry,
          abortIncompleteMultipartUploadAfter: Duration.days(1),
        },
      ],
    });

    this.resultsBucket = new s3.Bucket(this, 'ResultsBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      serverAccessLogsBucket: accessLogsBucket,
      serverAccessLogsPrefix: 'results/',
      // Versioned because result keys are deterministic: a redelivered work item overwrites
      // its previous result, which is what makes retrying safe, and versioning is what makes
      // that overwrite recoverable.
      versioned: true,
      removalPolicy: RemovalPolicy.RETAIN,
    });

    // A secret with no value at all, created through the low-level resource on purpose.
    //
    // Named under a prefix rather than left to a generated name, because a pool has more
    // than one subscription and the task role has to be able to read all of them. A grant
    // per secret would mean redeploying compute to add a subscription, which is the wrong
    // shape: registering a subscription is an operator action, not a deployment.
    //
    // So the grant is on `<prefix>*` and adding a subscription is creating a secret under
    // the prefix. The prefix carries the stack name, so two deployments in one account do
    // not share a namespace or each other's keys.
    //
    // The high-level construct generates a random string when given no value, which is not a
    // leak — it stays in Secrets Manager — but it is worse than nothing here. It makes an
    // unpopulated secret indistinguishable from a populated one, so an operator cannot tell
    // which subscriptions are ready, and a worker claiming one would authenticate with a
    // random string.
    //
    // With no version, resolution fails, and the allocator already does the right thing with
    // that: it releases the reservation and moves to the next candidate. An unpopulated
    // subscription is skipped rather than claimed.
    this.subscriptionSecretPrefix = `${this.stackName}/subscription/`;

    const secret = new secretsmanager.CfnSecret(this, 'SubscriptionKey', {
      name: `${this.subscriptionSecretPrefix}sub-01`,
      description:
        'Kiro subscription key. Written by an operator; the template never populates it.',
    });
    secret.applyRemovalPolicy(RemovalPolicy.RETAIN);
    this.subscriptionSecret = secretsmanager.Secret.fromSecretCompleteArn(
      this,
      'SubscriptionKeyRef',
      secret.ref,
    );

    // A wildcard is unavoidable here even for a single secret: Secrets Manager appends six
    // random characters to every ARN, so a policy naming an exact ARN can only be written
    // after the secret exists. The wildcard covers the suffix and the rest of the pool.
    this.subscriptionSecretArnPattern = this.formatArn({
      service: 'secretsmanager',
      resource: 'secret',
      resourceName: `${this.subscriptionSecretPrefix}*`,
      arnFormat: ArnFormat.COLON_RESOURCE_NAME,
    });

    this.publishOutputs();
  }

  private createSubscriptionsTable(): dynamodb.Table {
    const table = new dynamodb.Table(this, 'SubscriptionsTable', {
      partitionKey: {
        name: POOL_SCHEMA.subscriptions.partitionKey,
        type: dynamodb.AttributeType.STRING,
      },
      // On-demand: the claim path is bursty by nature, a few writes per task start against an
      // idle table the rest of the time. Provisioned capacity would have to be sized for the
      // burst and paid for between them.
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      // The only record of which subscriptions exist and what limits they carry.
      pointInTimeRecovery: true,
      removalPolicy: RemovalPolicy.RETAIN,
    });

    // Projects everything, so selecting a candidate needs no follow-up read of the base
    // table. The pool is tens to low hundreds of items, so a partition per state value is
    // acceptable and the alternative, scanning with a filter, reads disabled records only to
    // discard them.
    table.addGlobalSecondaryIndex({
      indexName: POOL_SCHEMA.subscriptions.byStateIndex,
      partitionKey: {
        name: POOL_SCHEMA.subscriptions.stateAttribute,
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: {
        name: POOL_SCHEMA.subscriptions.partitionKey,
        type: dynamodb.AttributeType.STRING,
      },
      projectionType: dynamodb.ProjectionType.ALL,
    });

    return table;
  }

  private createLeasesTable(): dynamodb.Table {
    const table = new dynamodb.Table(this, 'LeasesTable', {
      partitionKey: {
        name: POOL_SCHEMA.leases.partitionKey,
        type: dynamodb.AttributeType.STRING,
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      // Expiry is enforced here rather than by a process, because workers are stopped by
      // scale-in and by crashes and neither runs a release path.
      timeToLiveAttribute: POOL_SCHEMA.leases.ttlAttribute,
      // Old image included: when a lease expires, the handler needs the removed item's
      // subscription to know whose counter to decrement.
      stream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
      // Recoverable by reconciliation, so losing it costs a lease lifetime of capacity
      // rather than the pool.
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Keys only: reconciliation counts live leases per subscription and never reads their
    // attributes.
    table.addGlobalSecondaryIndex({
      indexName: POOL_SCHEMA.leases.bySubscriptionIndex,
      partitionKey: {
        name: POOL_SCHEMA.leases.subscriptionAttribute,
        type: dynamodb.AttributeType.STRING,
      },
      projectionType: dynamodb.ProjectionType.KEYS_ONLY,
    });

    return table;
  }

  private publishOutputs(): void {
    // Named outputs, because the worker reads these through its environment and an operator
    // has to put them there.
    new CfnOutput(this, 'SubscriptionsTableName', {
      value: this.subscriptionsTable.tableName,
      description: 'SUBSCRIPTIONS_TABLE for the worker',
    });
    new CfnOutput(this, 'LeasesTableName', {
      value: this.leasesTable.tableName,
      description: 'LEASES_TABLE for the worker',
    });
    new CfnOutput(this, 'ResultsBucketName', {
      value: this.resultsBucket.bucketName,
      description: 'RESULTS_BUCKET for the worker',
    });
    new CfnOutput(this, 'ReferenceBucketName', {
      value: this.referenceBucket.bucketName,
      description: 'REFERENCE_BUCKET for the worker',
    });
    new CfnOutput(this, 'SubscriptionSecretArn', {
      value: this.subscriptionSecret.secretArn,
      description: 'Write the subscription key here, then register it in the pool',
    });
    new CfnOutput(this, 'SubscriptionSecretPrefix', {
      value: this.subscriptionSecretPrefix,
      description: 'Create further subscription keys under this prefix; the task role can read them',
    });
  }
}
