#!/usr/bin/env node
/**
 * The fleet, as stacks.
 *
 * Split by lifecycle rather than by service. What decides the boundaries is what a change to
 * one costs: state must survive a mistake, the network is slow to change and expensive to get
 * wrong, and compute is iterated on daily. One stack would put a task definition change in the
 * same deployment as a bucket.
 */

import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { ComputeStack } from '../lib/compute-stack';
import { DataStack } from '../lib/data-stack';
import { FunctionStack } from '../lib/function-stack';
import { NetworkStack } from '../lib/network-stack';
import { ObservabilityStack } from '../lib/observability-stack';
import { QueueStack } from '../lib/queue-stack';
import { Pipeline } from '../lib/pipeline-stack';
import { BenchStack } from '../lib/bench-stack';
import { pipelineConfig } from '../lib/config';
import { applyNagChecks } from '../lib/nag';

const app = new cdk.App();

// Region and account come from the environment, so the same app deploys anywhere without an
// account identifier appearing in the repository.
const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const prefix = app.node.tryGetContext('fleet:stackPrefix') ?? 'KiroFleet';

const data = new DataStack(app, `${prefix}Data`, {
  env,
  description: 'Subscription pool, shared inputs, and results (stateful; retained on destroy)',
});

const queue = new QueueStack(app, `${prefix}Queue`, {
  env,
  description: 'Work queue and dead-letter queue',
});

const network = new NetworkStack(app, `${prefix}Network`, {
  env,
  description:
    'Private subnets, PrivateLink for the high-volume paths, and a domain-restricted egress path',
});

const compute = new ComputeStack(app, `${prefix}Compute`, {
  env,
  description: 'Cluster, worker task definition, and a service that is empty until seeded',
  data,
  queue,
  network,
});

const observability = new ObservabilityStack(app, `${prefix}Observability`, {
  env,
  description: 'One dashboard in three layers, and alarms for what needs a person',
  queue,
  compute,
  data,
});

const functions = new FunctionStack(app, `${prefix}Functions`, {
  env,
  description: 'Seeding, capacity reconciliation, and the signal that starts the first task',
  data,
  queue,
  compute,
});

// cdk-nag on every synth of the fleet's stacks; see lib/nag.ts for each accepted finding.
applyNagChecks({ data, queue, network, compute, functions, observability });

// The deployment path, and the one stack the fleet does not deploy itself. It is built only when
// an operator has configured a connection and a repository (see `pipelineConfig`); without them
// the fleet still synthesizes and deploys by hand. The deploy stage is handed the application
// stacks explicitly and never this pipeline — see PipelineStack for why.
const pipeline = pipelineConfig(app);
if (pipeline) {
  const architectureName =
    (app.node.tryGetContext('fleet:targetArchitecture') as 'arm64' | 'x86_64') ?? 'arm64';
  new Pipeline(app, `${prefix}Pipeline`, {
    env,
    description: 'git commit -> CodeBuild (image to ECR, gates) -> cdk deploy (register task def)',
    connectionArn: pipeline.connectionArn,
    repo: pipeline.repo,
    branch: pipeline.branch,
    buildSpecPath: pipeline.buildSpecPath,
    architectureName,
    computeStackName: compute.stackName,
    appStackNames: [
      data.stackName,
      queue.stackName,
      network.stackName,
      compute.stackName,
      observability.stackName,
      functions.stackName,
    ],
  });
}

// The M1 benchmark stack, only when asked for (`-c fleet:benchWorkerImage=<local tag>`). It is
// independent of the fleet's stacks and is destroyed once the measurement is taken.
const benchWorkerImage = app.node.tryGetContext('fleet:benchWorkerImage') as string | undefined;
if (benchWorkerImage) {
  new BenchStack(app, `${prefix}Bench`, {
    env,
    description: 'M1 benchmark: one Kiro CLI stream per Fargate task (throwaway)',
    workerImage: benchWorkerImage,
  });
}

app.synth();
