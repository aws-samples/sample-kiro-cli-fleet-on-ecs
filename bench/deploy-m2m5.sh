#!/bin/bash
# Deploy the fleet for M2~M5 and push the bench image. Run from bench/.
# Creates billable resources: in the default `private` egress mode the network stack costs
# about $3.4 a day while it exists (7 interface endpoints in 2 AZs), so deploy right before
# measuring and remove the stacks when M5 is done.
#   1. cdk deploy --all with the bench workflow, 25 tasks, the 2.30.0 image tag
#   2. push kiro-fleet-worker:m2m5-2.30.0 (built by tools/build-worker-image.sh, whose
#      pinned default is 2.30.0 since the M2~M5 runs; they used the one-off
#      --cli-version 2.30.0 --cli-sha256 override) to the repository step 1 made
#   3. upload the shared corpus
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
PROFILE=${AWS_PROFILE:-default}
TAG=m2m5-2.30.0
export AWS_PROFILE=$PROFILE AWS_REGION=${AWS_REGION:-us-east-1}
CDK_DEFAULT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_DEFAULT_ACCOUNT
export CDK_DEFAULT_REGION=$AWS_REGION

cd "$HERE/../infra"
npx cdk deploy --all --require-approval never \
  -c fleet:workflowPath=bench/fanout -c fleet:maxTasks=25 -c fleet:imageTag="$TAG"

IMAGE=$(aws cloudformation describe-stacks --stack-name KiroFleetCompute \
  --query "Stacks[0].Outputs[?OutputKey=='WorkerImageUri'].OutputValue" --output text)
aws ecr get-login-password | docker login --username AWS --password-stdin "${IMAGE%%/*}"
docker tag "kiro-fleet-worker:$TAG" "$IMAGE"
docker push "$IMAGE"

cd "$HERE"
uv run --with boto3 python -m fleetbench.fleetload --aws-profile "$PROFILE" corpus
echo "DEPLOYED. Next: the operator registers keys with ./register-keys.sh"
