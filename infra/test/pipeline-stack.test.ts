/**
 * What the pipeline promises: three stages, a native build, scoped permissions, and a deploy that
 * pins the image and never deploys the pipeline itself.
 */

import { Match, Template } from 'aws-cdk-lib/assertions';
import { App } from 'aws-cdk-lib';
import { Pipeline, PipelineStackProps } from '../lib/pipeline-stack';

const CONNECTION_ARN =
  'arn:aws:codeconnections:us-east-1:123456789012:connection/11111111-2222-3333-4444-5555aaaabbbb';

const APP_STACKS = [
  'KiroFleetData',
  'KiroFleetQueue',
  'KiroFleetNetwork',
  'KiroFleetCompute',
  'KiroFleetObservability',
  'KiroFleetFunctions',
];

function pipelineTemplate(overrides: Partial<PipelineStackProps> = {}): Template {
  const app = new App();
  const stack = new Pipeline(app, 'KiroFleetPipeline', {
    env: { account: '123456789012', region: 'us-east-1' },
    connectionArn: CONNECTION_ARN,
    repo: 'aws-samples/sample-kiro-cli-fleet-on-ecs',
    branch: 'main',
    buildSpecPath: 'buildspec.yml',
    architectureName: 'arm64',
    computeStackName: 'KiroFleetCompute',
    appStackNames: APP_STACKS,
    ...overrides,
  });
  return Template.fromStack(stack);
}

describe('the shape of the pipeline', () => {
  test('a commit flows through source, build, then deploy', () => {
    const template = pipelineTemplate();
    const pipelines = template.findResources('AWS::CodePipeline::Pipeline');
    const stages = Object.values(pipelines)[0].Properties.Stages;

    expect(stages.map((s: { Name: string }) => s.Name)).toEqual(['Source', 'Build', 'Deploy']);
  });

  test('the source is the configured connection, triggered on push', () => {
    const template = pipelineTemplate();
    // The connection ARN appears in the source action's configuration; a pipeline that lost it
    // would fall back to polling or fail to find the repository.
    expect(JSON.stringify(template.toJSON())).toContain(CONNECTION_ARN);
  });

  test('a malformed repository is refused at synthesis, not at deploy', () => {
    expect(() => pipelineTemplate({ repo: 'no-slash' })).toThrow(/owner\/name/);
  });
});

describe('the build is native and can push one repository', () => {
  test('the worker image is built on arm64 compute, in privileged mode', () => {
    const template = pipelineTemplate();
    // Emulation would change every timing the image is later used to measure, so the build
    // container must match the target architecture; privileged mode is what lets it run docker.
    template.hasResourceProperties(
      'AWS::CodeBuild::Project',
      Match.objectLike({
        Environment: Match.objectLike({
          Type: 'ARM_CONTAINER',
          PrivilegedMode: true,
        }),
      }),
    );
  });

  test('x86 is a parameter, not a rewrite', () => {
    const template = pipelineTemplate({ architectureName: 'x86_64' });
    template.hasResourceProperties(
      'AWS::CodeBuild::Project',
      Match.objectLike({
        Environment: Match.objectLike({ Type: 'LINUX_CONTAINER' }),
      }),
    );
  });

  test('the build pushes by the name ComputeStack gives the repository', () => {
    const template = pipelineTemplate();
    // The build pushes by name because the pipeline must not reference the stack it deploys. The
    // name is derived the same way on both sides; this pins the agreement.
    expect(JSON.stringify(template.toJSON())).toContain('kirofleetcompute-worker');
  });

  test('registry access is scoped to that one repository', () => {
    const template = pipelineTemplate();
    const policies = template.findResources('AWS::IAM::Policy');
    const statements = Object.values(policies).flatMap(
      (p) => p.Properties.PolicyDocument.Statement,
    );

    // Only the auth token is granted on `*`, which has no resource to name. Everything that
    // writes layers is scoped to a repository ARN, so the house rule against `*` on `*` holds.
    const onEverything = statements.filter(
      (s: { Resource: unknown }) => s.Resource === '*',
    );
    for (const statement of onEverything) {
      expect(statement.Action).toBe('ecr:GetAuthorizationToken');
    }

    const pushers = statements.filter(
      (s: { Action: unknown }) =>
        Array.isArray(s.Action) && s.Action.includes('ecr:PutImage'),
    );
    expect(pushers).toHaveLength(1);
    expect(pushers[0].Resource).not.toBe('*');
  });
});

describe('deploying is not running, and never redeploys the pipeline', () => {
  test('the deploy pins the image to the built commit', () => {
    const template = pipelineTemplate();
    // The task definition should name the image from this commit, not whatever `latest` points at
    // by the time a task starts.
    expect(JSON.stringify(template.toJSON())).toContain('fleet:imageTag=$COMMIT_ID');
  });

  test('the deploy names the application stacks and not itself', () => {
    const template = pipelineTemplate();
    const body = JSON.stringify(template.toJSON());

    for (const name of APP_STACKS) {
      expect(body).toContain(name);
    }
    // Self-mutation is deliberately not a feature here: a change to the pipeline is a re-deploy,
    // not a side effect of a commit. The deploy command must not name this stack.
    expect(body).not.toContain('cdk deploy KiroFleetPipeline');
    expect(body).not.toMatch(/deploy[^"]*KiroFleetPipeline/);
  });

  test('deploy permission is the bootstrap roles, not a broad grant', () => {
    const template = pipelineTemplate();
    const policies = template.findResources('AWS::IAM::Policy');
    const statements = Object.values(policies).flatMap(
      (p) => p.Properties.PolicyDocument.Statement,
    );

    const assumeRole = statements.filter(
      (s: { Action: unknown }) => s.Action === 'sts:AssumeRole',
    );
    // CodePipeline wires its own action roles with AssumeRole; none of those, and not the
    // bootstrap grant either, may be on `*`.
    for (const statement of assumeRole) {
      expect(statement.Resource).not.toBe('*');
    }
    // Exactly one of them is the deploy grant, scoped to this account's CDK bootstrap roles,
    // where the actual deployment permission lives.
    const bootstrap = assumeRole.filter((s: { Resource: unknown }) =>
      JSON.stringify(s.Resource).includes(':role/cdk-'),
    );
    expect(bootstrap).toHaveLength(1);
  });
});
