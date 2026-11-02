/**
 * The queue's two load-sensitive settings, and the thing that stops work being lost.
 */

import { Match, Template } from 'aws-cdk-lib/assertions';
import { QueueStack, VISIBILITY_WINDOW_SECONDS } from '../lib/queue-stack';
import { testApp } from './helpers';

const WORKFLOW_TIMEOUT_SECONDS = 900;

let template: Template;

beforeAll(() => {
  template = Template.fromStack(new QueueStack(testApp(), 'TestQueue'));
});

describe('work is not lost', () => {
  test('there is a dead-letter queue with a redrive policy', () => {
    // Without it a repeatedly failing item circulates forever behind a queue that never
    // empties, which hides the failure instead of surfacing it.
    template.hasResourceProperties('AWS::SQS::Queue', {
      RedrivePolicy: Match.objectLike({ maxReceiveCount: Match.anyValue() }),
    });
  });

  test('an item is set aside after a few attempts, not many', () => {
    const queues = template.findResources('AWS::SQS::Queue');
    const withRedrive = Object.values(queues).filter((q) => q.Properties.RedrivePolicy);

    expect(withRedrive).toHaveLength(1);
    expect(withRedrive[0].Properties.RedrivePolicy.maxReceiveCount).toBeLessThanOrEqual(5);
  });

  test('a batch survives a deployment', () => {
    // Reseeding means working out what already completed, so retention is long enough that
    // resuming is the easier option.
    const queues = template.findResources('AWS::SQS::Queue');

    for (const queue of Object.values(queues)) {
      expect(queue.Properties.MessageRetentionPeriod).toBeGreaterThanOrEqual(
        7 * 24 * 60 * 60,
      );
    }
  });
});

describe('ownership of a work item', () => {
  test('visibility is the short window the worker renews', () => {
    // The worker extends this window every third of it while a run is in progress, so a slow
    // run keeps its item. The window bounds how long an item waits after its task dies.
    const queues = template.findResources('AWS::SQS::Queue');
    const work = Object.values(queues).find((q) => q.Properties.RedrivePolicy);

    expect(work).toBeDefined();
    expect(work!.Properties.VisibilityTimeout).toBe(VISIBILITY_WINDOW_SECONDS);
    // Shorter than a run on purpose; recovery after a crash is one window, not one run.
    expect(VISIBILITY_WINDOW_SECONDS).toBeLessThan(WORKFLOW_TIMEOUT_SECONDS);
  });
});

describe('exposure', () => {
  test('both queues are encrypted at rest', () => {
    const queues = template.findResources('AWS::SQS::Queue');
    expect(Object.keys(queues)).toHaveLength(2);

    for (const queue of Object.values(queues)) {
      expect(queue.Properties.SqsManagedSseEnabled).toBe(true);
    }
  });

  test('both queues refuse plaintext transport', () => {
    const policies = template.findResources('AWS::SQS::QueuePolicy');
    expect(Object.keys(policies)).toHaveLength(2);

    for (const policy of Object.values(policies)) {
      expect(policy.Properties.PolicyDocument.Statement).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            Effect: 'Deny',
            Condition: { Bool: { 'aws:SecureTransport': 'false' } },
          }),
        ]),
      );
    }
  });
});
