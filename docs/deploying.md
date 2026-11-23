# Deploying and operating the fleet

Part of the [sample README](../README.md).

## Before you start

The pattern runs the Kiro CLI in headless mode, which authenticates from an API key rather than a browser
session — [setting `KIRO_API_KEY` is what makes a run headless][headless-blog]. Getting those keys has
preconditions that are easier to discover now than halfway through a deployment.

| You need | Notes |
|---|---|
| A paid Kiro subscription per pool member | API keys require a paid tier. The pool's whole purpose is to spread load across several, so a useful run needs more than one. |
| API key generation enabled for your organization | An administrator turns this on in the Kiro settings of the AWS console. Without it, the API Keys page will not issue a key. |
| One key per subscription, from [app.kiro.dev](https://app.kiro.dev) | The value is shown once, at creation. Keys begin `ksk_`. |
| An AWS account with CDK bootstrapped | Plus Node.js 20+, Python 3.13, [uv](https://docs.astral.sh/uv/), Docker, and the AWS CLI. |
| Fargate quota headroom for the fleet you want | The default allowance for a new account is **6 on-demand vCPUs**, counted in vCPUs rather than tasks. The shipped defaults fit inside it; see [Deploying it](#deploying-it). |

Tested with Kiro CLI 2.30.0 and Node.js 26.13.0 in the worker image (both pinned and checksum-verified in
`tools/build-worker-image.sh`), AWS CDK 2.274.0 (`aws-cdk-lib`) with CDK CLI 2.1146.1, Python 3.13, and
Node.js 22 in CI, deployed in `us-east-1` on `linux/arm64` tasks. Moving the Kiro CLI pin is worth re-checking: the
`/usage` reading and the `--trust-tools` names are what the worker depends on.

Two further constraints are worth knowing before you plan a region, though neither is verified here:

- The sample's egress allowlist (`fleet:kiroEgressDomains` in `infra/cdk.json`) names `us-east-1` Kiro
  endpoints. Deploying elsewhere means changing those names to match.
- The [`sample-kiro-flock`][flock] project reports that the Kiro portal validates an IAM Identity Center
  login only when the IDC instance is in the same region as the Kiro service, and that a mismatch fails
  without a clear error. If you authenticate through IDC, check this before assuming a region.

Read the [Kiro headless mode documentation][headless-docs] for the authoritative account of the flags and
the API key flow; the paragraph above is only what bears on deploying this sample.

[headless-blog]: https://kiro.dev/blog/introducing-headless-mode/
[headless-docs]: https://kiro.dev/docs/cli/headless/
[flock]: https://github.com/aws-samples/sample-kiro-flock

## Deploying it

These are the steps as run, in this order, against an account with nothing in it. The order matters in
two places and both are noted.

```bash
cd infra
npm ci
export CDK_DEFAULT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_DEFAULT_REGION=us-east-1

npx cdk bootstrap "aws://${CDK_DEFAULT_ACCOUNT}/${CDK_DEFAULT_REGION}"
npx cdk deploy --all --require-approval never
```

**If CDK reports `no credentials have been configured` while `aws sts get-caller-identity` works, sign in
again.** With IAM Identity Center profiles, the AWS CLI caches the role credentials it obtained and keeps
answering from that cache after the sign-in session has expired. CDK does not read that cache. Run
`aws sso login --profile <profile>` and retry.

**The first deployment happens before the image exists, and that is fine.** The repository is created by
`ComputeStack`, so there is nowhere to push to until it has been deployed once. A task definition naming a
tag that is not there yet is not an error: nothing validates the image until a task starts, and the
service's desired count is zero. This is the second reason that count is zero, the first being that a
deployment should prepare a fleet rather than run a batch nobody asked for.

Then build the image and push it to the repository the deployment created:

```bash
cd ..
./tools/build-worker-image.sh

REPO=$(aws cloudformation describe-stacks --stack-name KiroFleetCompute \
    --query "Stacks[0].Outputs[?OutputKey=='WorkerImageUri'].OutputValue" --output text)
aws ecr get-login-password | docker login --username AWS --password-stdin "${REPO%%/*}"
docker tag kiro-fleet-worker:latest "$REPO" && docker push "$REPO"
```

## Registering a subscription

A subscription is a key in Secrets Manager plus a row in the registry. The deployment creates one empty
secret under a prefix and the task role can read anything under it, so adding further subscriptions needs
no redeployment.

```bash
SECRET=$(aws cloudformation describe-stacks --stack-name KiroFleetData \
    --query "Stacks[0].Outputs[?OutputKey=='SubscriptionSecretArn'].OutputValue" --output text)
TABLE=$(aws cloudformation describe-stacks --stack-name KiroFleetData \
    --query "Stacks[0].Outputs[?OutputKey=='SubscriptionsTableName'].OutputValue" --output text)

# Typed at a prompt, so it stays out of shell history. Handed to the CLI as a file, so it
# stays out of the process list: an argument such as --secret-string "$KIRO_KEY" can be
# read by anyone who lists processes while the command runs. printf is a shell builtin,
# so writing the file starts no process that could see the value.
read -rs KIRO_KEY
KEY_FILE=$(umask 077; mktemp)
printf '%s' "$KIRO_KEY" > "$KEY_FILE"
unset KIRO_KEY
aws secretsmanager put-secret-value --secret-id "$SECRET" \
    --secret-string "file://$KEY_FILE" >/dev/null
rm -f "$KEY_FILE"

aws dynamodb put-item --table-name "$TABLE" --item "$(cat <<JSON
{"subscriptionId": {"S": "sub-01"},
 "state":          {"S": "ENABLED"},
 "secretRef":      {"S": "${SECRET}"},
 "maxConcurrency": {"N": "2"},
 "activeLeases":   {"N": "0"},
 "creditBudget":   {"N": "1000"},
 "creditsUsed":    {"N": "0"}}
JSON
)"
```

`maxConcurrency` is the operational cap you choose for that subscription, not a limit the service
publishes. Their sum, over the subscriptions that still have budget, is the fleet's real ceiling — the
scaling function holds the autoscaling maximum at it, because a task beyond that point starts, fails to
claim, exits, and is replaced while doing nothing.

## Seeding a batch

Put the inputs in the reference bucket and write a manifest beside them. The seeding function expands a
prefix into one work item per object, which is the shape fan-out work usually has:

```bash
REF=$(aws cloudformation describe-stacks --stack-name KiroFleetData \
    --query "Stacks[0].Outputs[?OutputKey=='ReferenceBucketName'].OutputValue" --output text)

aws s3 cp ./my-inputs "s3://${REF}/inputs/" --recursive
cat > manifest.json <<'JSON'
{ "inputPrefix": "inputs/",
  "prompt": "Read every file under ./input. For each one, write ./output/<the file name>.md ..." }
JSON
aws s3 cp manifest.json "s3://${REF}/manifest.json"

aws lambda invoke --function-name KiroFleetFunctions-seeding \
    --cli-binary-format raw-in-base64-out --payload '{}' /dev/stdout
```

Tasks start by themselves from there. Nothing needs to be told to start. The scaling function runs every
minute; when it finds visible messages and no task desired, running or pending, it sets the service to
`ceil(visible / targetBacklogPerTask)` tasks, at least one and at most the fleet's ceiling (`maxTasks`, or
the pool's summed `maxConcurrency` if lower). It logs the start and publishes `StartedFromZero` in the
`KiroFleet/Fleet` namespace. From there target tracking follows backlog per task as the batch runs, and
the same function sets the service back to zero when the queue, including in-flight messages, is empty.

Target tracking cannot do the first step by itself. At zero tasks its metric is the whole backlog, and it
scales out only when the metric is above `targetBacklogPerTask`, so a batch of one or two items (with the
default of 2) would never start. Larger batches would start only after the scale-out alarm has collected
its datapoints.

The start also suspends target tracking's scale-in. While the fleet idles the metric is 0, so the scale-in
alarm (fifteen datapoints below the target) sits in ALARM, and an alarm in ALARM invokes its policy every
minute. The start sets the desired count outside Application Auto Scaling, so no scale-in cooldown covers
it, and before this was added the stale alarm cut a just-started fleet within the minute: 25 tasks to 2,
measured after a quiet hour. The function sets `DynamicScalingInSuspended` on the scalable target in the
same run, before the desired count moves, and lifts it once the alarm has left ALARM (normally the next
minute) or the queue has drained. Scale-out is never suspended.

> **This has been run.** All six stacks, the image push, a subscription registered, the manifest seeded
> through the deployed function, the fleet leaving zero on its own, the batch completing, and the fleet
> returning to zero. Measured on a three-item batch:
>
> | | |
> |---|---|
> | seeded to fleet leaving zero | 149s with target tracking alone. Since the scaling function starts an idle fleet itself, a one-item batch reached a desired count above zero in 54s and a running task in 88s |
> | leaving zero to queue drained | 95s — including the image pull on a cold host |
> | drained to back at zero | 23s |
>
> The first number is the cold-start cost of scaling from nothing, and it is the price of not paying for
> an idle fleet. Three bugs were found by running this and none of them were visible in the templates or
> the tests; they are described in [Roadmap](architecture.md#roadmap).

## Watching it

Per-task processor and memory come from Container Insights, which is on by default precisely because the
task size is not tuned against a measurement. The worker emits its own metrics as embedded metric format
on its log stream, so the pool's behaviour — claim outcomes, utilisation, recovered leases — is in
CloudWatch without the workload holding any CloudWatch permission.

```bash
aws logs tail /aws/ecs/KiroFleetCompute/worker --follow
```

A single dashboard puts these in the post's three layers — `KiroFleetObservability-fleet`. Progress is
the queue draining against the fleet's size; outcomes are run verdicts and claim outcomes, verdict by
verdict, because a success count cannot tell a prompt problem from a pool problem; the pool panel shows
utilisation against the ceiling the subscriptions declare. Per-run transcripts are objects rather than a
time series, so the dashboard points at their prefix.

## Sizing

The sample is designed to be runnable with a handful of subscriptions and a low task concurrency cap.
Larger figures are parameters, not defaults, and the binding one is a quota rather than a preference.

A new account is allowed **6 on-demand Fargate vCPUs**, and the quota counts vCPUs rather than tasks, so
the fleet ceiling and the task size are one decision:

```bash
aws service-quotas get-aws-default-service-quota \
    --service-code fargate --quota-code L-3032A538      # 6, for a new account
aws service-quotas get-service-quota \
    --service-code fargate --quota-code L-3032A538      # what yours is now
```

Check the second command before raising `fleet:maxTasks`. The two readings diverge over time, because
increases accumulate and are never given back — which means an account that has been used for a while
reports no constraint at all, and is a misleading place to decide a default from. The shipped defaults
(`fleet:maxTasks: 10` at `fleet:taskCpu: 512`) come to 5 vCPUs and fit a new account; synthesizing a
larger fleet prints a warning naming the quota to raise.

## Removing it

```bash
cd infra
npx cdk destroy --all --force
```

That removes the compute, the network, the queue, and the functions. If you deployed the benchmark
stack (`-c fleet:benchWorkerImage=...`), `--all` does not see it without that context; remove it with
`npx cdk destroy KiroFleetBench -c fleet:benchWorkerImage=<the same tag> --force`.

Four things survive it, three of them on purpose.

| Survives | Why | To remove |
|---|---|---|
| Subscriptions table | The only record of which subscriptions exist, their limits, and their recorded `creditsUsed`. Losing it costs the pool, not a batch; the next readings rebuild `creditsUsed`. | `aws dynamodb delete-table --table-name <name>` |
| Reference bucket | Your inputs. | `aws s3 rb s3://<name> --force` |
| Results bucket | The output of runs that may have taken hours. | See below; versioning gets in the way. |
| Subscription secret | Holds a live key, so it goes to a recovery window rather than away. | `--force-delete-without-recovery` |

The asymmetry is deliberate: scratch is destroyed on teardown, and anything whose loss cannot be undone by
running the batch again is retained. The leases table is *not* retained — losing it costs a lease lifetime
of capacity, which reconciliation recovers.

### Three things that do not remove cleanly

Found by doing it. Each wastes an afternoon if you meet it without warning.

**The results bucket is versioned**, which is what makes a redelivered work item safe to overwrite. It also
means `s3 rb --force` leaves it non-empty and the delete fails, because `--force` removes objects and not
their versions:

```bash
aws s3api list-object-versions --bucket "$BUCKET" \
    --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' > versions.json
aws s3api delete-objects --bucket "$BUCKET" --delete file://versions.json
# Repeat for DeleteMarkers[], then:
aws s3 rb "s3://$BUCKET"
```

**The secret stays for its recovery window.** A plain delete schedules removal seven days out, and the key
is readable for all of them. If the key is live, revoke it at the source as well — deleting the secret does
not invalidate the credential it held:

```bash
aws secretsmanager delete-secret --secret-id "$SECRET" --force-delete-without-recovery
```

**With `fleet:egressControl: 'inspection'`, the firewall refuses to be deleted.** Delete protection is on
by design — a deployment that can remove what constrains egress is one mistake from an open fleet — and the
consequence is that teardown needs it turned off first. The default mode has no firewall and no NAT
gateway, so this does not arise:

```bash
aws network-firewall update-firewall-delete-protection \
    --firewall-name <stack>-egress --no-delete-protection
```

### Check nothing is still billing

Standing charges are the ones a teardown is for, and all three are easy to leave behind:

```bash
aws ec2 describe-nat-gateways --query 'length(NatGateways[?State!=`deleted`])'
aws ec2 describe-vpc-endpoints --query 'length(VpcEndpoints[?VpcEndpointType==`Interface`])'
aws network-firewall list-firewalls --query 'length(Firewalls)'
```
