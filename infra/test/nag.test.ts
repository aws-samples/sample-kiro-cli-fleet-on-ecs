import { Annotations, Match } from 'aws-cdk-lib/assertions';
import { Stack } from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import { applyNagChecks } from '../lib/nag';
import { fleetStacks } from './helpers';

/**
 * The fleet synthesizes with no open cdk-nag (AwsSolutions) finding.
 *
 * A finding is either fixed or suppressed with a reason in lib/nag.ts. This test is what makes
 * a new one fail CI instead of reaching a review.
 */
describe('cdk-nag', () => {
  const { app, data, queue, network, compute, functions, observability } = fleetStacks();
  applyNagChecks({ data, queue, network, compute, functions, observability });
  app.synth();

  const stacks: [string, Stack][] = Object.entries({ data, queue, network, compute, functions, observability });

  test.each(stacks)('%s has no unsuppressed errors', (_name, stack) => {
    const errors = Annotations.fromStack(stack).findError('*', Match.stringLikeRegexp('AwsSolutions-.*'));
    expect(errors.map((e) => `${e.id}: ${String(e.entry.data).split(': ')[0]}`)).toEqual([]);
  });

  test.each(stacks)('%s has no unsuppressed warnings', (_name, stack) => {
    const warnings = Annotations.fromStack(stack).findWarning('*', Match.stringLikeRegexp('AwsSolutions-.*'));
    expect(warnings.map((w) => `${w.id}: ${String(w.entry.data).split(': ')[0]}`)).toEqual([]);
  });

  test('a new unscoped wildcard grant is still reported', () => {
    const fresh = fleetStacks();
    fresh.compute.taskDefinition.taskRole.addToPrincipalPolicy(
      new iam.PolicyStatement({ actions: ['s3:GetObject'], resources: ['*'] }),
    );
    applyNagChecks(fresh);
    fresh.app.synth();
    const errors = Annotations.fromStack(fresh.compute).findError(
      '*',
      Match.stringLikeRegexp('AwsSolutions-IAM5\\[Resource::\\*\\]'),
    );
    expect(errors.length).toBeGreaterThan(0);
  });
});
