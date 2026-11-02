/**
 * Builds an app the way the CLI would.
 *
 * `new App()` in a test gets no context: the CDK CLI reads `cdk.json` and passes it in, and jest
 * does not. Left alone, every test would exercise the fallback values written into `config.ts`
 * rather than the ones actually deployed — so a bad default in `cdk.json` would ship with a green
 * suite behind it.
 *
 * Reading the same file the CLI reads keeps the two honest.
 */

import { readFileSync } from 'fs';
import { join } from 'path';
import { App, AppProps } from 'aws-cdk-lib';
import { ComputeStack } from '../lib/compute-stack';
import { FunctionStack } from '../lib/function-stack';
import { DataStack } from '../lib/data-stack';
import { NetworkStack } from '../lib/network-stack';
import { ObservabilityStack } from '../lib/observability-stack';
import { QueueStack } from '../lib/queue-stack';

const CDK_JSON = join(__dirname, '../cdk.json');

export function deployedContext(): Record<string, unknown> {
  const { context } = JSON.parse(readFileSync(CDK_JSON, 'utf8'));
  return context ?? {};
}

/** An app carrying the context a deployment would carry, with overrides for a specific case. */
export function testApp(overrides: Record<string, unknown> = {}, props: AppProps = {}): App {
  return new App({ ...props, context: { ...deployedContext(), ...overrides } });
}

/**
 * The whole fleet, because compute cannot be synthesized alone.
 *
 * ComputeStack takes the table, queue, bucket, and subnet it uses from the other stacks, so a
 * test of it is necessarily a test of the wiring too. That is the right trade: a compute stack
 * asserted against stub resources would pass while pointed at the wrong queue.
 *
 * An environment is set because several assertions here are about ARNs, and CDK produces
 * region-agnostic tokens for an account-agnostic stack that no policy assertion can read.
 */
export function fleetStacks(overrides: Record<string, unknown> = {}) {
  const app = testApp(overrides);
  // The documentation placeholder, and the only twelve-digit number the publication scan
  // allows. Any other value here would be indistinguishable from a real account number.
  const env = { account: '123456789012', region: 'us-east-1' };

  const data = new DataStack(app, 'TestData', { env });
  const queue = new QueueStack(app, 'TestQueue', { env });
  const network = new NetworkStack(app, 'TestNetwork', { env });
  const compute = new ComputeStack(app, 'TestCompute', { env, data, queue, network });
  const functions = new FunctionStack(app, 'TestFunctions', { env, data, queue, compute });
  const observability = new ObservabilityStack(app, 'TestObservability', { env, queue, compute, data });

  return { app, data, queue, network, compute, functions, observability };
}
