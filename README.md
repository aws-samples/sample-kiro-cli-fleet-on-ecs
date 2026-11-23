# Running Kiro CLI workflows remotely at scale on Amazon ECS

Run the same headless Kiro CLI workflow across hundreds of independent inputs, on Amazon ECS with AWS
Fargate. Work arrives on an Amazon SQS queue, the fleet scales out to drain it and back to zero when it is
empty, and each task borrows a key from a pool of Kiro subscriptions so that no key is pushed past its
limits.

This is the reference implementation for the AWS blog post *Running Kiro CLI workflows remotely at scale
on Amazon ECS*.

```
manifest (S3) ──▶ seeding Lambda ──▶ SQS ──▶ Fargate tasks (0..N) ──▶ results (S3)
                                              │
                                              ├─ claim a key from the DynamoDB pool
                                              ├─ load it from Secrets Manager
                                              └─ run kiro-cli --no-interactive, check the output
```

## Quick start

### 1. Try it locally (about 5 minutes, no AWS account)

You need Docker and [uv](https://docs.astral.sh/uv/). A Kiro key is optional: without one, a built-in
stand-in replaces the CLI so you can watch the pool, the queue, and the result check work.

```bash
docker run -d --rm --name ddb-local -p 8000:8000 \
    amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -inMemory -sharedDb

cd worker
uv sync --frozen --extra dev
uv run --frozen python -m runner.local_batch --items 8 --subscriptions 3 --concurrency 2 --workers 5
```

The run ends with `PASS` when every item stored a result and all pool capacity came back. Set
`KIRO_API_KEY` to run the real Kiro CLI, and add `--workflow ../workflows/examples/repo-file-summary
--input-dir ./leaseclient` to run an example workflow.

### 2. Deploy to AWS

You need an AWS account with CDK bootstrapped, Node.js 20+, Python 3.13, uv, Docker, the AWS CLI, and at
least one paid Kiro subscription with an API key from [app.kiro.dev](https://app.kiro.dev) (see
[Before you start](docs/deploying.md#before-you-start)).

```bash
cd infra && npm ci
export CDK_DEFAULT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_DEFAULT_REGION=us-east-1
npx cdk bootstrap "aws://${CDK_DEFAULT_ACCOUNT}/${CDK_DEFAULT_REGION}"
npx cdk deploy --all --require-approval never

cd .. && ./tools/build-worker-image.sh
REPO=$(aws cloudformation describe-stacks --stack-name KiroFleetCompute \
    --query "Stacks[0].Outputs[?OutputKey=='WorkerImageUri'].OutputValue" --output text)
aws ecr get-login-password | docker login --username AWS --password-stdin "${REPO%%/*}"
docker tag kiro-fleet-worker:latest "$REPO" && docker push "$REPO"
```

The fleet starts at zero tasks, so nothing runs (or costs Fargate time) until you send work.

### 3. Register a subscription key

Store the key in AWS Secrets Manager and add the subscription to the pool. The full commands, which keep
the key out of shell history and the process list, are in
[Registering a subscription](docs/deploying.md#registering-a-subscription).

### 4. Run a batch

```bash
REF=$(aws cloudformation describe-stacks --stack-name KiroFleetData \
    --query "Stacks[0].Outputs[?OutputKey=='ReferenceBucketName'].OutputValue" --output text)

aws s3 cp ./my-inputs "s3://${REF}/inputs/" --recursive
jq -Rs '{inputPrefix: "inputs/", prompt: .}' workflows/examples/repo-file-summary/prompt.md > manifest.json
aws s3 cp manifest.json "s3://${REF}/manifest.json"

aws lambda invoke --function-name KiroFleetFunctions-seeding \
    --cli-binary-format raw-in-base64-out --payload '{}' /dev/stdout
```

Tasks start by themselves within a minute or two, one work item per input file, and the fleet returns to
zero when the queue is empty. Follow it with `aws logs tail /aws/ecs/KiroFleetCompute/worker --follow`
or the `KiroFleetObservability-fleet` dashboard. Each item's output and transcript land in the results
bucket (`ResultsBucketName` output of `KiroFleetData`) under `results/<item id>/`.

### 5. Remove it

```bash
cd infra && npx cdk destroy --all --force
```

The subscriptions table, both buckets, and the key secret are kept on purpose. See
[Removing it](docs/deploying.md#removing-it) to delete them and to check nothing is still billing.

## What it costs

- **Idle:** about $3.4 a day (~$102 a month) for the VPC interface endpoints in the default network
  mode. The fleet itself scales to zero. Remove the stacks when you are done.
- **Running:** Fargate per task-second, plus Kiro credits, which Kiro bills separately from AWS.

Details and the other network modes: [Security and cost](docs/security.md#what-it-costs-while-idle).

## Security in short

- Tasks have no internet route by default; AWS services and Kiro are reached through VPC endpoints.
- Keys live in Secrets Manager and are loaded at run time, never baked into the image.
- Each workflow trusts only the tools it lists. **Do not trust a shell tool for inputs you did not
  produce yourself**: the agent runs with the task role.
- This is a sample for one operator in their own account. Read
  [Before adapting this for production](docs/security.md#before-adapting-this-for-production-or-shared-use)
  first.

## Learn more

| To learn about | Read |
|---|---|
| Design choices, repository layout, what was verified on AWS | [docs/architecture.md](docs/architecture.md) |
| Prerequisites, deployment details, sizing, monitoring, teardown | [docs/deploying.md](docs/deploying.md) |
| How credit budgets are tracked and enforced | [docs/credits.md](docs/credits.md) |
| Why scale-in never stops a run part-way | [docs/scale-in.md](docs/scale-in.md) |
| Network modes, IAM, idle cost, production checklist | [docs/security.md](docs/security.md) |
| Tests, CI gates, and building the image | [docs/development.md](docs/development.md) |
| Writing your own workflow | [workflows/README.md](workflows/README.md) |
| The benchmark behind the post's figures | [bench/README.md](bench/README.md) |

Tested with Kiro CLI 2.30.0 (pinned in the image), AWS CDK 2.274.0, Python 3.13, and Node.js 26.13.0 in
the image, in `us-east-1` on `linux/arm64`.

## License

This sample is licensed under the MIT-0 License. See [LICENSE](LICENSE).
