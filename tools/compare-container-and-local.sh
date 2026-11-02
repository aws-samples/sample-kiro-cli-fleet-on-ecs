#!/usr/bin/env bash
#
# Runs the same batch twice, once locally and once in the container, and compares what
# came out.
#
#   ./tools/compare-container-and-local.sh
#
# This is the check that keeps local iteration meaningful. Everything is developed and
# tested on a laptop, and measurements are taken from a task; if the two diverge then the
# tests are about something other than what runs.
#
# Uses DynamoDB Local for the pool and the built-in stand-in workflow, so it needs no AWS
# account and no credential. What it compares is the part that could drift — allocation,
# the loop, the completion check, result keys — not the CLI, which is the same binary in
# both cases and is exercised separately.

set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE="${IMAGE:-kiro-fleet-worker}"
ITEMS="${ITEMS:-4}"
ENDPOINT_HOST="${ENDPOINT_HOST:-http://localhost:8000}"
# From inside a container, the host's DynamoDB Local is not on localhost.
ENDPOINT_CONTAINER="${ENDPOINT_CONTAINER:-http://host.docker.internal:8000}"

if ! curl -s -o /dev/null --max-time 3 "$ENDPOINT_HOST" 2>/dev/null; then
    cat >&2 <<MSG
DynamoDB Local is not reachable at ${ENDPOINT_HOST}. Start it with:

  docker run -d --rm --name ddb-local -p 8000:8000 \\
      amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -inMemory -sharedDb
MSG
    exit 2
fi

OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

echo "=== local ==="
( cd worker && DYNAMODB_LOCAL_ENDPOINT="$ENDPOINT_HOST" \
    uv run --frozen python -m runner.local_batch \
        --items "$ITEMS" --subscriptions 2 --concurrency 2 --workers 2 \
        --no-cli --keep 2>&1 ) | tee "$OUT/local.log" | grep -E '^(items |leases|capacity|PASS|FAIL)' || true

echo
echo "=== container ==="
docker run --rm --platform linux/arm64 \
    --entrypoint python3 \
    -e DYNAMODB_LOCAL_ENDPOINT="$ENDPOINT_CONTAINER" \
    -e AWS_ACCESS_KEY_ID=local -e AWS_SECRET_ACCESS_KEY=local -e AWS_DEFAULT_REGION=us-east-1 \
    -v "$OUT:/out" \
    "$IMAGE" -m runner.local_batch \
        --items "$ITEMS" --subscriptions 2 --concurrency 2 --workers 2 \
        --no-cli --endpoint "$ENDPOINT_CONTAINER" --results-dir /out/container 2>&1 \
    | tee "$OUT/container.log" | grep -E '^(items |leases|capacity|PASS|FAIL)' || true

echo
echo "=== comparison ==="

extract() { grep -E "^$2" "$1" | head -1 | sed 's/.*: *//' | tr -d ' '; }

for field in 'items completed' 'items with stored results' 'leases left behind'; do
    local_value="$(extract "$OUT/local.log" "$field")"
    container_value="$(extract "$OUT/container.log" "$field")"
    status="same"
    [ "$local_value" = "$container_value" ] || status="DIFFERENT"
    printf '  %-28s local=%-8s container=%-8s %s\n' \
        "$field" "${local_value:-?}" "${container_value:-?}" "$status"
done

if grep -q '^PASS' "$OUT/local.log" && grep -q '^PASS' "$OUT/container.log"; then
    echo
    echo "PASS: both runs drained the queue and returned all capacity"
else
    echo
    echo "FAIL: see the logs above"
    exit 1
fi
