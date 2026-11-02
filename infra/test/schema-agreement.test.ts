/**
 * The infrastructure and the worker have to agree on names they cannot share.
 *
 * The table shape is declared twice, in `worker/leaseclient/schema.py` and in
 * `infra/lib/config.ts`, because TypeScript and Python cannot share a definition. A rename on
 * one side and not the other produces a table the allocator queries with the wrong index
 * name, which fails at runtime on a deployed fleet rather than in a build.
 *
 * This reads the Python source as text. Crude, and better than the alternative: the failure it
 * prevents is discovered at the worst possible time.
 */

import { readFileSync } from 'fs';
import { join } from 'path';
import { POOL_SCHEMA } from '../lib/config';

const workerSchema = readFileSync(
  join(__dirname, '../../worker/leaseclient/schema.py'),
  'utf8',
);

/** Matches `NAME: Final = "value"` in the worker's schema module. */
function pythonConstant(name: string): string | undefined {
  const match = workerSchema.match(new RegExp(`^${name}: Final = "([^"]+)"`, 'm'));
  return match?.[1];
}

describe('names shared with the worker', () => {
  test.each([
    ['SUBSCRIPTION_ID', POOL_SCHEMA.subscriptions.partitionKey],
    ['STATE', POOL_SCHEMA.subscriptions.stateAttribute],
    ['INDEX_BY_STATE', POOL_SCHEMA.subscriptions.byStateIndex],
    ['LEASE_ID', POOL_SCHEMA.leases.partitionKey],
    ['SUBSCRIPTION_ID', POOL_SCHEMA.leases.subscriptionAttribute],
    ['INDEX_BY_SUBSCRIPTION', POOL_SCHEMA.leases.bySubscriptionIndex],
    ['EXPIRES_AT', POOL_SCHEMA.leases.ttlAttribute],
  ])('%s agrees with the worker', (constant, expected) => {
    expect(pythonConstant(constant)).toBe(expected);
  });

  test('the worker schema is actually being read', () => {
    // Otherwise every assertion above compares undefined to undefined and passes.
    expect(workerSchema).toContain('ENV_SUBSCRIPTIONS_TABLE');
    expect(pythonConstant('SUBSCRIPTION_ID')).toBeDefined();
  });

  test('the environment variables the worker requires are the ones documented here', () => {
    // The stacks publish outputs an operator copies into a task definition. If the worker
    // renames a variable, the output names stop matching what it reads.
    expect(pythonConstant('ENV_SUBSCRIPTIONS_TABLE')).toBe('SUBSCRIPTIONS_TABLE');
    expect(pythonConstant('ENV_LEASES_TABLE')).toBe('LEASES_TABLE');
  });
});


/**
 * The dashboard draws one series per dimension value, and the values come from Python enums.
 *
 * This failure is quieter than a renamed table. A series naming a verdict the worker never
 * emits is a flat line, and a flat line on a dashboard looks exactly like a condition that is
 * not occurring — so a renamed verdict stops being visible instead of breaking anything, and
 * the first time anyone notices is when they are trying to diagnose a batch with it.
 */

import { readFileSync as read } from 'fs';
import { join as joinPath } from 'path';
import { App, Stack } from 'aws-cdk-lib';
import {
  CLAIM_OUTCOMES,
  FLEET_METRICS,
  RECOVERY_MECHANISMS,
  RUN_VERDICTS,
  TASK_STOP_REASONS,
  USAGE_READING_OUTCOMES,
  fleetConfig,
} from '../lib/config';
import { deployedContext } from './helpers';

/** Values of a Python `StrEnum`, read out of the class body. */
function pythonEnumValues(relativePath: string, className: string): string[] {
  const source = read(joinPath(__dirname, '../../worker', relativePath), 'utf8');
  const body = source.split(new RegExp(`^class ${className}\\(StrEnum\\):$`, 'm'))[1];
  if (body === undefined) {
    throw new Error(`${className} is not a StrEnum in ${relativePath}`);
  }
  // Up to the next top-level definition, so a later enum's members are not swept in.
  const declaration = body.split(/\n(?=\S)/)[0];
  return [...declaration.matchAll(/^\s+[A-Z_]+ = "([^"]+)"/gm)].map((match) => match[1]);
}

describe('dimension values the dashboard draws', () => {
  test('run verdicts match the worker’s Verdict enum', () => {
    const fromWorker = pythonEnumValues('runner/workflow.py', 'Verdict');

    expect(fromWorker.length).toBeGreaterThan(0);
    expect([...RUN_VERDICTS].sort()).toEqual(fromWorker.sort());
  });

  test('claim outcomes match the worker’s ClaimOutcome enum', () => {
    const fromWorker = pythonEnumValues('leaseclient/metrics.py', 'ClaimOutcome');

    expect(fromWorker.length).toBeGreaterThan(0);
    expect([...CLAIM_OUTCOMES].sort()).toEqual(fromWorker.sort());
  });

  test('recovery mechanisms match the worker’s RecoveryMechanism enum', () => {
    const fromWorker = pythonEnumValues('leaseclient/metrics.py', 'RecoveryMechanism');

    expect(fromWorker.length).toBeGreaterThan(0);
    expect([...RECOVERY_MECHANISMS].sort()).toEqual(fromWorker.sort());
  });

  test('usage reading outcomes match the worker’s UsageReadingOutcome enum', () => {
    const fromWorker = pythonEnumValues('leaseclient/metrics.py', 'UsageReadingOutcome');

    expect(fromWorker.length).toBeGreaterThan(0);
    expect([...USAGE_READING_OUTCOMES].sort()).toEqual(fromWorker.sort());
  });

  test('the scaling function publishes the names the dashboard and the policy read', () => {
    // A metric the dashboard names and nothing publishes is a flat line, which reads as a
    // condition that is not happening.
    const source = read(joinPath(__dirname, '../../worker/scaling/handler.py'), 'utf8');
    const constant = (name: string) =>
      source.match(new RegExp(`^${name}: Final = "([^"]+)"`, 'm'))?.[1];

    expect(constant('FLEET_NAMESPACE')).toBe(FLEET_METRICS.namespace);
    expect(constant('BACKLOG_PER_TASK')).toBe(FLEET_METRICS.backlogPerTask);
    expect(constant('POOL_DECLARED_CONCURRENCY')).toBe(FLEET_METRICS.poolDeclaredConcurrency);
    expect(constant('POOL_CLAIMABLE_CONCURRENCY')).toBe(FLEET_METRICS.poolClaimableConcurrency);
    expect(constant('POOL_EXHAUSTED')).toBe(FLEET_METRICS.poolExhausted);
  });

  test('task stop reasons match the worker’s StopReason enum', () => {
    const fromWorker = pythonEnumValues('taskstopped/handler.py', 'StopReason');

    expect(fromWorker.length).toBeGreaterThan(0);
    expect([...TASK_STOP_REASONS].sort()).toEqual(fromWorker.sort());
  });

  test('the task-stopped function publishes the metric the dashboard reads', () => {
    const source = read(joinPath(__dirname, '../../worker/taskstopped/handler.py'), 'utf8');

    expect(source.match(/^TASK_STOPPED: Final = "([^"]+)"/m)?.[1]).toBe(FLEET_METRICS.taskStopped);
    // The namespace is the scaling function's, imported rather than copied.
    expect(source).toContain('from scaling.handler import FLEET_NAMESPACE');
  });

  test('the enums are actually being read', () => {
    // Otherwise every assertion above compares an empty list to an empty list and passes,
    // which is the failure mode of reading source as text.
    expect(pythonEnumValues('runner/workflow.py', 'Verdict')).toContain('Succeeded');
    expect(() => pythonEnumValues('runner/workflow.py', 'NotAnEnum')).toThrow();
  });
});

/**
 * The credit margin has one default, the worker's.
 *
 * The stack passes `CREDIT_MARGIN` to the workers and to the scaling function. Its fallback and
 * the value in `cdk.json` must be the worker's own `DEFAULT_CREDIT_MARGIN`, or deploying the
 * stack would quietly change the margin the README documents.
 */
describe('the credit margin default', () => {
  const claimSource = read(joinPath(__dirname, '../../worker/leaseclient/claim.py'), 'utf8');
  const workerDefault = claimSource.match(
    /^DEFAULT_CREDIT_MARGIN: Final = Decimal\("([^"]+)"\)/m,
  )?.[1];

  test('the worker default is actually being read', () => {
    expect(workerDefault).toBeDefined();
  });

  test('the stack falls back to the worker default', () => {
    const stack = new Stack(new App(), 'NoContext');

    expect(fleetConfig(stack).creditMargin).toBe(workerDefault);
  });

  test('cdk.json ships the worker default', () => {
    expect(String(deployedContext()['fleet:creditMargin'])).toBe(workerDefault);
  });
});
