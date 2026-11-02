/**
 * M1 on ECS: a throwaway stack that runs one benchmark stream per Fargate task.
 *
 * Kept apart from the fleet on purpose. M1 asks one question -- how many streams one
 * subscription serves at once -- and none of the fleet's machinery (pool, leases, queue,
 * scaling) is part of the answer. Sharing the fleet's stacks would put a benchmark's
 * lifecycle inside the product's, and the benchmark is deleted when it is done.
 *
 * Network: public subnets and a public IP per task, egress HTTPS only, no NAT. The fleet's
 * private, firewalled network answers a security question M1 does not ask, and a NAT gateway
 * would put every stream behind one address, which is not how a fleet of tasks reaches the
 * service.
 *
 * The subscription key: a secret created empty-ish (a generated placeholder) for the
 * operator to overwrite with `put-secret-value`, so the key never passes through the
 * template, the repository, or the deploying shell. ECS injects it at task start, which the
 * fleet cannot do (it does not know which key a task holds until it claims one) but M1 can,
 * because every task uses the same single key.
 */

import * as path from 'path';
import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecr_assets from 'aws-cdk-lib/aws-ecr-assets';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';

export interface BenchStackProps extends StackProps {
  /** Local tag of the worker image the stream image is built FROM. */
  readonly workerImage: string;
  /** Name of the secret the operator writes the one subscription key into. */
  readonly keySecretName?: string;
  /** Per stream. One vCPU so a stream never waits on its own host, which is the point. */
  readonly taskCpu?: number;
  readonly taskMemoryMiB?: number;
}

export class BenchStack extends Stack {
  constructor(scope: Construct, id: string, props: BenchStackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [{ name: 'public', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 22 }],
    });

    const securityGroup = new ec2.SecurityGroup(this, 'StreamSecurityGroup', {
      vpc,
      description: 'Benchmark streams: HTTPS out, nothing in',
      allowAllOutbound: false,
    });
    securityGroup.addEgressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(443), 'Kiro service and AWS APIs');

    const cluster = new ecs.Cluster(this, 'Cluster', { vpc });

    const logGroup = new logs.LogGroup(this, 'Streams', {
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const key = new secretsmanager.Secret(this, 'SubscriptionKey', {
      secretName: props.keySecretName ?? 'kiro-bench/key',
      description: 'One Kiro subscription key for M1. Overwrite with put-secret-value.',
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const image = new ecr_assets.DockerImageAsset(this, 'StreamImage', {
      directory: path.join(__dirname, '..', '..', 'bench'),
      file: 'Dockerfile',
      platform: ecr_assets.Platform.LINUX_ARM64,
      buildArgs: { WORKER_IMAGE: props.workerImage },
      // The base is a local tag, which the directory hash cannot see. Folding the tag into the
      // hash at least rebuilds when the operator points at a different worker build.
      extraHash: props.workerImage,
      exclude: ['.venv', 'results', 'tests', '**/__pycache__', '.pytest_cache', '.ruff_cache'],
    });

    const taskDefinition = new ecs.FargateTaskDefinition(this, 'StreamTask', {
      cpu: props.taskCpu ?? 1024,
      memoryLimitMiB: props.taskMemoryMiB ?? 2048,
      runtimePlatform: {
        cpuArchitecture: ecs.CpuArchitecture.ARM64,
        operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
      },
    });
    taskDefinition.addContainer('Stream', {
      image: ecs.ContainerImage.fromDockerImageAsset(image),
      essential: true,
      logging: ecs.LogDrivers.awsLogs({ logGroup, streamPrefix: 'stream' }),
      secrets: { KIRO_API_KEY: ecs.Secret.fromSecretsManager(key) },
      stopTimeout: Duration.seconds(30),
    });

    new CfnOutput(this, 'ClusterName', { value: cluster.clusterName });
    new CfnOutput(this, 'TaskDefinitionArn', { value: taskDefinition.taskDefinitionArn });
    new CfnOutput(this, 'LogGroupName', { value: logGroup.logGroupName });
    new CfnOutput(this, 'SubnetIds', {
      value: vpc.selectSubnets({ subnetType: ec2.SubnetType.PUBLIC }).subnetIds.join(','),
    });
    new CfnOutput(this, 'SecurityGroupId', { value: securityGroup.securityGroupId });
    new CfnOutput(this, 'KeySecretName', { value: key.secretName });
  }
}
