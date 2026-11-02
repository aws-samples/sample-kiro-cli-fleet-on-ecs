#!/usr/bin/env bash
#
# Every gate that has to pass before this repository is deployed or published, in order.
#
# A script rather than a pipeline definition, and that is the whole design. The logic lives
# here once and a CI system calls it, so the same gates run on a laptop, in CodeBuild, in
# GitHub Actions, or in whatever a reader already has. Encoding the order inside a pipeline
# would mean the gates could only be run by deploying the pipeline — and a check nobody can
# run locally is a check that gets skipped.
#
#   ./tools/ci.sh                  everything, including the image
#   ./tools/ci.sh --skip-image     no Docker available, or only the fast gates are wanted
#   ./tools/ci.sh --arch x86_64    build for Intel instead of Graviton
#
# Order is cheapest first. The publication scan takes a few seconds, but it would run first
# even if it were slow: it is the only gate whose failure cannot be undone later, because a
# value committed once stays reachable in history even after it is removed.
#
set -euo pipefail

cd "$(dirname "$0")/.."

SKIP_IMAGE=0
ARCH=""
while [ $# -gt 0 ]; do
    case "$1" in
        --skip-image) SKIP_IMAGE=1; shift ;;
        --arch) ARCH="$2"; shift 2 ;;
        -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
        *) printf 'unknown option: %s\n' "$1" >&2; exit 2 ;;
    esac
done

STARTED=$(date +%s)
STAGE=0

stage() {
    STAGE=$((STAGE + 1))
    printf '\n\033[1m[%d] %s\033[0m\n' "$STAGE" "$1"
}

fail() {
    printf '\n\033[31mFAILED at stage %d: %s\033[0m\n' "$STAGE" "$1" >&2
    printf 'Nothing after this stage was run.\n' >&2
    exit 1
}

# --- 1. nothing unpublishable ------------------------------------------------

stage 'Publication scan'
./tools/scan-secrets.sh || fail 'a value that must not be published'

# --- 2. the worker -----------------------------------------------------------
#
# `--frozen` so the lockfile decides the versions. Without it a resolver runs and the suite
# can pass against dependencies the image will not contain.

stage 'Worker tests'
if ! command -v uv >/dev/null 2>&1; then
    fail 'uv is not installed; see https://docs.astral.sh/uv/getting-started/installation/'
fi
(cd worker && uv sync --frozen --extra dev >/dev/null && uv run --frozen pytest -q) \
    || fail 'worker tests'

# The contention tests skip without DynamoDB Local, and they are the ones that cover the
# claim protocol under real write races — an in-process fake does not reproduce those. A
# skip is not a pass, so it is reported rather than left in the noise.
if ! curl -fsS -o /dev/null --max-time 2 http://localhost:8000 2>/dev/null; then
    printf '\033[33m  note: contention tests were skipped. They need DynamoDB Local:\033[0m\n'
    printf '    docker run -d --rm -p 8000:8000 amazon/dynamodb-local:latest \\\n'
    printf '        -jar DynamoDBLocal.jar -inMemory -sharedDb\n'
fi

# --- 3. the infrastructure ---------------------------------------------------
#
# `npm ci` rather than `npm install`, for the same reason as `--frozen` above.

stage 'Infrastructure tests'
(cd infra && npm ci --silent && npx tsc --noEmit && npx jest) || fail 'infrastructure tests'

# --- 4. the image ------------------------------------------------------------
#
# Last because it is the slowest, and because everything above can fail without a Docker
# daemon being involved.

if [ "${SKIP_IMAGE}" = "1" ]; then
    stage 'Worker image (skipped)'
    printf '  --skip-image was given. The image guards did not run:\n'
    printf '    glibc floor for the target architecture, CLI checksum, MCP servers on PATH.\n'
else
    stage 'Worker image'
    if ! docker info >/dev/null 2>&1; then
        fail 'the Docker daemon is not reachable; pass --skip-image to run the other gates'
    fi
    # The build is a gate in its own right: it refuses a base image whose glibc is below what
    # the target architecture's CLI build needs, verifies the CLI against a pinned checksum,
    # and fails if a declared MCP server is not on PATH afterwards.
    if [ -n "${ARCH}" ]; then
        ./tools/build-worker-image.sh --arch "${ARCH}" || fail 'worker image'
    else
        ./tools/build-worker-image.sh || fail 'worker image'
    fi
fi

printf '\n\033[32mAll gates passed\033[0m in %ds.\n' "$(( $(date +%s) - STARTED ))"

if [ "${SKIP_IMAGE}" = "1" ]; then
    printf 'The image was not built, so this run does not clear a deployment.\n'
fi
