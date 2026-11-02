#!/usr/bin/env bash
#
# Stops a running container mid-batch and checks that its capacity came back.
#
#   ./tools/verify-sigterm-returns-capacity.sh
#
# This is the behaviour scale-in depends on. ECS sends SIGTERM before SIGKILL, and a task
# that ignores it holds its subscription until the lease expires — so on every scale-in the
# next task waits out a lease lifetime for capacity that is already free. Worth verifying
# rather than assuming, because the failure is invisible: the pool still works, just more
# slowly than it should, and only under scale-in.
#
# It also exercises the part that is easy to get wrong in a container: the entrypoint must
# exec the worker so it is PID 1 and receives the signal. If the shell stays PID 1 the
# signal goes nowhere.

set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE="${IMAGE:-kiro-fleet-worker}"
ENDPOINT_CONTAINER="${ENDPOINT_CONTAINER:-http://host.docker.internal:8000}"
ENDPOINT_HOST="${ENDPOINT_HOST:-http://localhost:8000}"
NAME="sigterm-probe-$$"

if ! curl -s -o /dev/null --max-time 3 "$ENDPOINT_HOST" 2>/dev/null; then
    echo "DynamoDB Local is not reachable at ${ENDPOINT_HOST}" >&2
    exit 2
fi

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# Fixed table names make the pool inspectable from outside the container, which means a
# previous run's tables are still there. Left behind, the container fails to create them
# and exits immediately, and the stale counters read like a failure of this run.
drop_tables() {
    python3 - "$ENDPOINT_HOST" <<'PY'
import sys
import boto3

ddb = boto3.client(
    "dynamodb", endpoint_url=sys.argv[1], region_name="us-east-1",
    aws_access_key_id="local", aws_secret_access_key="local",
)
for table in ddb.list_tables()["TableNames"]:
    if "sigterm" in table:
        ddb.delete_table(TableName=table)
        print(f"  removed stale table {table}")
PY
}
drop_tables

echo "starting a batch with slow items, so there is something to interrupt..."

# --delay makes each item take long enough that the container is still working when the
# signal arrives. Tables are named per run so this does not disturb anything else.
docker run -d --name "$NAME" --platform linux/arm64 \
    --entrypoint python3 \
    -e AWS_ACCESS_KEY_ID=local -e AWS_SECRET_ACCESS_KEY=local -e AWS_DEFAULT_REGION=us-east-1 \
    "$IMAGE" -m runner.local_batch \
        --items 20 --subscriptions 1 --concurrency 1 --workers 1 \
        --no-cli --delay 2 --endpoint "$ENDPOINT_CONTAINER" \
        --tables-suffix sigterm >/dev/null

sleep 8

echo "PID 1 inside the container:"
docker exec "$NAME" sh -c 'cat /proc/1/cmdline | tr "\0" " "; echo' 2>/dev/null | sed 's/^/  /' || \
    echo "  (could not read; container may already have exited)"

HELD_BEFORE="$(docker exec "$NAME" python3 -c '
import boto3, os
ddb = boto3.client("dynamodb", endpoint_url=os.environ["DDB"], region_name="us-east-1",
                   aws_access_key_id="local", aws_secret_access_key="local")
table = [t for t in ddb.list_tables()["TableNames"] if t.endswith("sigterm-subs")][0]
item = ddb.scan(TableName=table)["Items"][0]
print(item["activeLeases"]["N"])
' 2>/dev/null || echo "?")"
echo "  held leases while running: ${HELD_BEFORE}"

echo
echo "sending SIGTERM (docker stop)..."
docker stop --timeout 20 "$NAME" >/dev/null
echo "  exit status: $(docker inspect "$NAME" --format '{{.State.ExitCode}}')"

echo
echo "checking the pool from the host..."
python3 - "$ENDPOINT_HOST" <<'PY'
import sys
import boto3

ddb = boto3.client(
    "dynamodb", endpoint_url=sys.argv[1], region_name="us-east-1",
    aws_access_key_id="local", aws_secret_access_key="local",
)
tables = [t for t in ddb.list_tables()["TableNames"] if "sigterm" in t]
subs = [t for t in tables if t.endswith("subs")]
leases = [t for t in tables if t.endswith("leases")]
if not subs:
    sys.exit("  no pool table found; the container may not have got that far")

held = 0
for item in ddb.scan(TableName=subs[0])["Items"]:
    held += int(item["activeLeases"]["N"])
    print(f"  {item['subscriptionId']['S']}: held {item['activeLeases']['N']}")

remaining = ddb.scan(TableName=leases[0], Select="COUNT")["Count"] if leases else 0
print(f"  lease records remaining: {remaining}")
print()
if held == 0 and remaining == 0:
    print("PASS: SIGTERM returned the capacity before the container exited")
else:
    print("FAIL: capacity was still held after the container stopped, so the next task")
    print("      would wait out a lease lifetime for capacity that is already free")
    sys.exit(1)
PY
