/**
 * What the M1 bench stack promises: streams on separate public hosts, HTTPS out only, the key
 * injected from a secret the template never carries, and nothing left behind on destroy.
 */

import { Match, Template } from 'aws-cdk-lib/assertions';
import { App } from 'aws-cdk-lib';
import { BenchStack } from '../lib/bench-stack';

function benchTemplate(): Template {
  const app = new App();
  const stack = new BenchStack(app, 'KiroFleetBench', {
    env: { account: '123456789012', region: 'us-east-1' },
    workerImage: 'kiro-fleet-worker:bench',
  });
  return Template.fromStack(stack);
}

describe('the M1 bench stack', () => {
  const template = benchTemplate();

  test('has no NAT gateway, so every task reaches the service from its own address', () => {
    template.resourceCountIs('AWS::EC2::NatGateway', 0);
  });

  test('allows HTTPS out and nothing else', () => {
    template.hasResourceProperties('AWS::EC2::SecurityGroup', {
      SecurityGroupEgress: [Match.objectLike({ IpProtocol: 'tcp', FromPort: 443, ToPort: 443 })],
    });
  });

  test('runs one arm64 stream per task with its own vCPU', () => {
    template.hasResourceProperties('AWS::ECS::TaskDefinition', {
      Cpu: '1024',
      Memory: '2048',
      RuntimePlatform: { CpuArchitecture: 'ARM64', OperatingSystemFamily: 'LINUX' },
    });
  });

  test('injects the key from the secret by reference, never by value', () => {
    template.hasResourceProperties('AWS::ECS::TaskDefinition', {
      ContainerDefinitions: [
        Match.objectLike({
          Secrets: [Match.objectLike({ Name: 'KIRO_API_KEY' })],
          Environment: Match.absent(),
        }),
      ],
    });
  });

  test('deletes its secret and logs with the stack', () => {
    template.hasResource('AWS::SecretsManager::Secret', { DeletionPolicy: 'Delete' });
    template.hasResource('AWS::Logs::LogGroup', { DeletionPolicy: 'Delete' });
  });

  test('exports what the launcher reads', () => {
    for (const name of [
      'ClusterName',
      'TaskDefinitionArn',
      'LogGroupName',
      'SubnetIds',
      'SecurityGroupId',
    ]) {
      template.hasOutput(name, {});
    }
  });
});
