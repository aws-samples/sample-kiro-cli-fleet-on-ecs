/**
 * The work queue.
 *
 * The queue is doing more than buffering. It is how a work item is owned: a received message
 * is invisible to other workers until it is deleted or its visibility lapses, which is why
 * the worker needs no lock of its own. And it is the scaling signal, since depth is what
 * autoscaling reads.
 *
 * Both of the settings here have a failure mode that only appears under load: a visibility
 * window that a live worker lets lapse, and a redrive count that hides a failing item.
 */

import { CfnOutput, Duration, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as sqs from 'aws-cdk-lib/aws-sqs';

/** How many deliveries before an item is set aside. */
const MAX_RECEIVE_COUNT = 3;

/**
 * How long a received item stays hidden without a fresh extension.
 *
 * Short on purpose. The worker renews it every third of a window for as long as the run is in
 * progress (`runner/visibility.py`), so a slow run is never handed to a second worker. What the
 * window decides is how long an item waits after its task dies mid-run: one window, rather
 * than the longest possible run. Sized to the run instead, that wait was most of the crash
 * recovery time in the measurements (22.5 minutes against a 7-minute batch).
 *
 * The worker reads the same value from its environment, so the window it renews and the one
 * the queue starts with cannot disagree.
 */
export const VISIBILITY_WINDOW_SECONDS = 180;

export class QueueStack extends Stack {
  readonly workQueue: sqs.Queue;
  readonly deadLetterQueue: sqs.Queue;
  readonly visibilityWindow: Duration = Duration.seconds(VISIBILITY_WINDOW_SECONDS);

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);


    this.deadLetterQueue = new sqs.Queue(this, 'DeadLetterQueue', {
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
      // Longest SQS allows. An item here needs a person to look at it, and people are slower
      // than the default four days.
      retentionPeriod: Duration.days(14),
    });

    this.workQueue = new sqs.Queue(this, 'WorkQueue', {
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
      visibilityTimeout: this.visibilityWindow,
      // Long enough that a batch interrupted by a deployment can be resumed rather than
      // reseeded, which matters because reseeding means working out what already completed.
      retentionPeriod: Duration.days(14),
      deadLetterQueue: {
        queue: this.deadLetterQueue,
        // An item that has failed three times is not going to succeed on the fourth, and
        // leaving it circulating hides the failure behind a queue that never empties.
        maxReceiveCount: MAX_RECEIVE_COUNT,
      },
    });

    new CfnOutput(this, 'WorkQueueUrl', {
      value: this.workQueue.queueUrl,
      description: 'WORK_QUEUE_URL for the worker',
    });
    new CfnOutput(this, 'DeadLetterQueueUrl', {
      value: this.deadLetterQueue.queueUrl,
      description: 'Items that failed repeatedly. Should be empty; alarmed if not.',
    });
  }
}
