/**
 * What the data plane must be, asserted on the synthesized template.
 *
 * These are not tests of CDK. They are tests of the decisions whose absence is invisible
 * until it matters: retention on the resource that holds the pool, a lifecycle rule on the
 * one that grows quietly, and the index and stream shape the allocator depends on.
 */

import { Match, Template } from 'aws-cdk-lib/assertions';
import { DataStack } from '../lib/data-stack';
import { testApp } from './helpers';
import { POOL_SCHEMA } from '../lib/config';

let template: Template;

beforeAll(() => {
  // Synthesized once: it is not instant, and nothing here mutates it.
  template = Template.fromStack(new DataStack(testApp(), 'TestData'));
});

describe('the pool tables match what the allocator needs', () => {
  test('subscriptions are partitioned by subscription', () => {
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      KeySchema: [
        { AttributeName: POOL_SCHEMA.subscriptions.partitionKey, KeyType: 'HASH' },
      ],
    });
  });

  test('the state index projects everything', () => {
    // So selecting a candidate needs no follow-up read of the base table. A KEYS_ONLY index
    // here would double the reads on the claim path.
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      GlobalSecondaryIndexes: Match.arrayWith([
        Match.objectLike({
          IndexName: POOL_SCHEMA.subscriptions.byStateIndex,
          Projection: { ProjectionType: 'ALL' },
        }),
      ]),
    });
  });

  test('leases expire without anything running', () => {
    // Workers are stopped by scale-in and by crashes, neither of which runs a release path,
    // so expiry has to be enforced by the datastore.
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      TimeToLiveSpecification: {
        AttributeName: POOL_SCHEMA.leases.ttlAttribute,
        Enabled: true,
      },
    });
  });

  test('the lease stream carries the removed image', () => {
    // When a lease expires the handler needs the vanished item's subscription to know whose
    // counter to decrement. NEW_IMAGE alone would be useless for a removal.
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      StreamSpecification: { StreamViewType: 'NEW_AND_OLD_IMAGES' },
    });
  });

  test('the lease index projects keys only', () => {
    // Reconciliation counts leases and never reads their attributes.
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      GlobalSecondaryIndexes: Match.arrayWith([
        Match.objectLike({
          IndexName: POOL_SCHEMA.leases.bySubscriptionIndex,
          Projection: { ProjectionType: 'KEYS_ONLY' },
        }),
      ]),
    });
  });
});

describe('what survives a mistake, and what does not', () => {
  test('the registry is retained and recoverable', () => {
    // It is the only record of which subscriptions exist and what limits they carry.
    template.hasResource('AWS::DynamoDB::Table', {
      Properties: Match.objectLike({
        PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: true },
      }),
      DeletionPolicy: 'Retain',
    });
  });

  test('results are retained and versioned', () => {
    // Result keys are deterministic so a retry overwrites; versioning is what makes that
    // overwrite recoverable.
    template.hasResource('AWS::S3::Bucket', {
      Properties: Match.objectLike({
        VersioningConfiguration: { Status: 'Enabled' },
      }),
      DeletionPolicy: 'Retain',
    });
  });

  test('scratch storage expires on its own', () => {
    // Without this it is a bill that grows without anyone deciding to.
    template.hasResourceProperties('AWS::S3::Bucket', {
      LifecycleConfiguration: {
        Rules: Match.arrayWith([
          Match.objectLike({ Status: 'Enabled', ExpirationInDays: Match.anyValue() }),
        ]),
      },
    });
  });

  test('exactly one bucket is removable, and it is the scratch one', () => {
    const buckets = template.findResources('AWS::S3::Bucket');
    const removable = Object.values(buckets).filter((b) => b.DeletionPolicy === 'Delete');

    expect(removable).toHaveLength(1);
    expect(removable[0].Properties.LifecycleConfiguration).toBeDefined();
  });
});

describe('separation and exposure', () => {
  test('inputs, scratch, and results are separate buckets, plus one for access logs', () => {
    // So a task can hold read on one and write on another. Prefixes in one bucket work and
    // are easy to scope slightly too broadly, and that mistake is invisible.
    template.resourceCountIs('AWS::S3::Bucket', 4);
  });

  test('every data bucket writes access logs to the log bucket', () => {
    const buckets = template.findResources('AWS::S3::Bucket');
    const logBucketIds = Object.keys(buckets).filter((id) => id.startsWith('AccessLogsBucket'));
    expect(logBucketIds).toHaveLength(1);

    const prefixes = Object.entries(buckets)
      .filter(([id]) => id !== logBucketIds[0])
      .map(([, b]) => {
        expect(b.Properties.LoggingConfiguration.DestinationBucketName).toEqual({
          Ref: logBucketIds[0],
        });
        return b.Properties.LoggingConfiguration.LogFilePrefix;
      });
    expect(prefixes.sort()).toEqual(['reference/', 'results/', 'workspace/']);
  });

  test('access logs are retained and expire', () => {
    const buckets = template.findResources('AWS::S3::Bucket');
    const [log] = Object.entries(buckets).filter(([id]) => id.startsWith('AccessLogsBucket'));
    expect(log[1].DeletionPolicy).toBe('Retain');
    expect(log[1].Properties.LifecycleConfiguration.Rules).toEqual(
      expect.arrayContaining([expect.objectContaining({ ExpirationInDays: 90 })]),
    );
  });

  test('no bucket is public', () => {
    const buckets = template.findResources('AWS::S3::Bucket');

    for (const bucket of Object.values(buckets)) {
      expect(bucket.Properties.PublicAccessBlockConfiguration).toEqual({
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      });
    }
  });

  test('every bucket requires TLS', () => {
    const policies = template.findResources('AWS::S3::BucketPolicy');
    expect(Object.keys(policies)).toHaveLength(4);

    for (const policy of Object.values(policies)) {
      const statements = policy.Properties.PolicyDocument.Statement;
      expect(statements).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            Effect: 'Deny',
            Condition: { Bool: { 'aws:SecureTransport': 'false' } },
          }),
        ]),
      );
    }
  });

  test('the template carries no secret value', () => {
    // A generated secret would appear in a stack event and in the account's template history.
    // The placeholder exists; an operator writes the value.
    const secrets = template.findResources('AWS::SecretsManager::Secret');
    expect(Object.keys(secrets)).toHaveLength(1);

    const properties = Object.values(secrets)[0].Properties;
    expect(properties.SecretString).toBeUndefined();
    expect(properties.GenerateSecretString).toBeUndefined();
  });

  test('the secret survives a teardown', () => {
    // Removing it would mean reissuing keys after every destroy.
    template.hasResource('AWS::SecretsManager::Secret', { DeletionPolicy: 'Retain' });
  });
});

describe('what an operator needs in order to use it', () => {
  test('every value the worker reads from its environment is an output', () => {
    const outputs = Object.keys(template.findOutputs('*'));

    expect(outputs).toEqual(
      expect.arrayContaining([
        'SubscriptionsTableName',
        'LeasesTableName',
        'ResultsBucketName',
        'ReferenceBucketName',
        'SubscriptionSecretArn',
      ]),
    );
  });
});
