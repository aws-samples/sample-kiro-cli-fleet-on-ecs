#!/usr/bin/env bash
#
# Builds the worker image and reports what went into it.
#
#   ./tools/build-worker-image.sh                      arm64, the default
#   ./tools/build-worker-image.sh --arch x86_64        the other one
#   ./tools/build-worker-image.sh --tag mine:v1
#
# Computes the workflow checksum here rather than in the Dockerfile, because a checksum
# computed inside the build would cover whatever the build context happened to contain.
# Computed outside, it is a property of the repository.
#
# Reports image size on success. Size is a pull-at-scale problem rather than a Dockerfile
# problem — most of it is the CLI's own binaries — so the number is printed to make growth
# visible rather than to imply it can be optimised away here.

set -euo pipefail

cd "$(dirname "$0")/.."

ARCH="aarch64"
TAG="kiro-fleet-worker"
EXTRA=()

while [ $# -gt 0 ]; do
    case "$1" in
        --arch) ARCH="$2"; shift 2 ;;
        --tag)  TAG="$2";  shift 2 ;;
        --no-cache) EXTRA+=("--no-cache"); shift ;;
        --checksums) SHOW_CHECKSUMS_FOR="$2"; shift 2 ;;
        # A one-off build against another CLI version, without moving the pin below. Both
        # are required together: a version without its checksum is an unverified download.
        --cli-version) OVERRIDE_CLI_VERSION="$2"; shift 2 ;;
        --cli-sha256)  OVERRIDE_CLI_SHA256="$2";  shift 2 ;;
        -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

# The platform Docker builds for has to match the artifact the Dockerfile fetches.
case "$ARCH" in
    aarch64) PLATFORM="linux/arm64"; GLIBC_MINOR=39 ;;
    x86_64)  PLATFORM="linux/amd64"; GLIBC_MINOR=34 ;;
    *) echo "unsupported architecture: $ARCH (expected aarch64 or x86_64)" >&2; exit 2 ;;
esac

# The pinned version and its checksums, recorded together.
#
# Not resolved from the published manifest at build time, which is what this script did
# first and which defeats the point of pinning: that manifest describes the *latest*
# release, so pinning 2.21.4 while reading latest's checksum broke the moment 2.22.1
# shipped. There is also no way to look one up after the fact, because the versioned
# manifest path is not served. So a checksum is computed once, recorded here, and changed
# deliberately alongside the version.
#
# To move to a new version: ./tools/build-worker-image.sh --checksums <version>
#
# 2.30.0 is the version every M1-M5 measurement ran on. 2.21.4, the previous pin, stopped
# subagents on a permission prompt in non-interactive mode (bench M1e).
KIRO_CLI_VERSION="2.30.0"
KIRO_CLI_SHA256_aarch64="7a41094bdf39d792fadfb76616a2f3790b77d8908b905e73b72e3f2f76f761e5"
KIRO_CLI_SHA256_x86_64="2e913eb21c44f8bf70ee194fffbbc8dbd4c2ed586e3614e663d02247a8477777"

# Node.js for the MCP servers, the official release pinned the same way. Checksums are the
# node-v<version>-linux-<arch>.tar.gz lines of https://nodejs.org/dist/v<version>/SHASUMS256.txt.
# Debian's `nodejs` package is not used: it brings Debian's `node-undici`, which had
# Critical advisories with no fixed version in Debian 13.
NODE_VERSION="26.13.0"
NODE_SHA256_aarch64="439e9c438acdf6a6ffe47418da709153d66c4bed812e30a473faacb107dd2791"
NODE_SHA256_x86_64="5848553868b402d3fabb5b09270649977780a390f832155721025a08b5104455"

if [ -n "${SHOW_CHECKSUMS_FOR:-}" ]; then
    echo "checksums for Kiro CLI ${SHOW_CHECKSUMS_FOR}, to paste into this script:"
    echo
    for arch in aarch64 x86_64; do
        url="https://prod.download.cli.kiro.dev/stable/${SHOW_CHECKSUMS_FOR}/kirocli-${arch}-linux.zip"
        if ! sum="$(curl -fsSL "$url" | shasum -a 256 | cut -d' ' -f1)"; then
            echo "  could not download ${url}" >&2
            exit 1
        fi
        echo "KIRO_CLI_SHA256_${arch}=\"${sum}\""
    done
    echo
    echo "KIRO_CLI_VERSION=\"${SHOW_CHECKSUMS_FOR}\""
    echo
    echo "Verify against https://prod.download.cli.kiro.dev/stable/latest/manifest.json"
    echo "when the version being pinned is the current latest."
    exit 0
fi

case "$ARCH" in
    aarch64) KIRO_CLI_SHA256="$KIRO_CLI_SHA256_aarch64"; NODE_SHA256="$NODE_SHA256_aarch64" ;;
    x86_64)  KIRO_CLI_SHA256="$KIRO_CLI_SHA256_x86_64";  NODE_SHA256="$NODE_SHA256_x86_64" ;;
    *)       KIRO_CLI_SHA256=""; NODE_SHA256="" ;;
esac
[ -n "$KIRO_CLI_SHA256" ] || { echo "no recorded checksum for ${ARCH}" >&2; exit 1; }
[ -n "$NODE_SHA256" ] || { echo "no recorded Node.js checksum for ${ARCH}" >&2; exit 1; }

if [ -n "${OVERRIDE_CLI_VERSION:-}${OVERRIDE_CLI_SHA256:-}" ]; then
    if [ -z "${OVERRIDE_CLI_VERSION:-}" ] || [ -z "${OVERRIDE_CLI_SHA256:-}" ]; then
        echo "--cli-version and --cli-sha256 must be given together" >&2
        exit 2
    fi
    KIRO_CLI_VERSION="$OVERRIDE_CLI_VERSION"
    KIRO_CLI_SHA256="$OVERRIDE_CLI_SHA256"
fi

# Runtime dependency versions come from the lockfile, not from a list kept in the
# Dockerfile. Transcribing them by hand is how an image ends up carrying a different boto3
# than the tests ran against, which is the kind of difference nobody notices until a call
# behaves differently on a task than it did locally.
RUNTIME_DEPS="$(
    python3 - <<'PY'
import re
from pathlib import Path

lock = Path("worker/uv.lock").read_text()
# Everything boto3 needs at runtime. Installed with --no-deps, so the closure is listed
# explicitly rather than resolved again inside the build.
wanted = (
    "boto3", "botocore", "jmespath", "python-dateutil", "s3transfer", "six", "urllib3",
)
pins = []
for name in wanted:
    match = re.search(rf'name = "{re.escape(name)}"\nversion = "([^"]+)"', lock)
    if match is None:
        raise SystemExit(f"{name} is not in worker/uv.lock; update the wanted list")
    pins.append(f"{name}=={match.group(1)}")
print(" ".join(pins))
PY
)"

# Content, not timestamps: sorted paths and their hashes, hashed again.
WORKFLOWS_CHECKSUM="$(
    find workflows -type f \! -name '.DS_Store' \! -path '*/node_modules/*' -print0 \
        | sort -z \
        | xargs -0 shasum -a 256 \
        | shasum -a 256 \
        | cut -d' ' -f1
)"

cat <<SUMMARY

building ${TAG}
  platform            ${PLATFORM}   (arch ${ARCH})
  base glibc floor    2.${GLIBC_MINOR}
  kiro-cli            ${KIRO_CLI_VERSION}
  artifact sha256     ${KIRO_CLI_SHA256:0:16}...
  node                ${NODE_VERSION}   (sha256 ${NODE_SHA256:0:16}...)
  workflows checksum  ${WORKFLOWS_CHECKSUM:0:16}...
  runtime deps        $(echo "${RUNTIME_DEPS}" | tr ' ' '\n' | head -2 | tr '\n' ' ')...

SUMMARY

docker build \
    --platform "$PLATFORM" \
    --build-arg "TARGET_ARCH=${ARCH}" \
    --build-arg "REQUIRED_GLIBC_MINOR=${GLIBC_MINOR}" \
    --build-arg "KIRO_CLI_VERSION=${KIRO_CLI_VERSION}" \
    --build-arg "KIRO_CLI_SHA256=${KIRO_CLI_SHA256}" \
    --build-arg "NODE_VERSION=${NODE_VERSION}" \
    --build-arg "NODE_SHA256=${NODE_SHA256}" \
    --build-arg "WORKFLOWS_CHECKSUM=${WORKFLOWS_CHECKSUM}" \
    --build-arg "RUNTIME_DEPS=${RUNTIME_DEPS}" \
    -f worker/Dockerfile \
    -t "$TAG" \
    "${EXTRA[@]+"${EXTRA[@]}"}" \
    .

echo
echo "--- what was built ---"
# Three numbers get called "image size" and only one of them is what a task waits for.
#   pull      compressed layer total: what crosses the network, once per host
#   on disk   what the task's filesystem holds after unpacking
#   reported  what `docker images` prints, which counts differently again
# The pull figure is the one that matters at several hundred tasks, so it is first.
PULL_BYTES="$(docker image inspect "$TAG" --format '{{.Size}}')"
printf '  pull (compressed) %.2f GB\n' "$(echo "$PULL_BYTES" | awk '{print $1/1073741824}')"
printf '  on disk           %s\n' \
    "$(docker run --rm --platform "$PLATFORM" --entrypoint sh "$TAG" -c 'du -sh / 2>/dev/null | cut -f1')"
printf '  docker images     %s\n' "$(docker images "$TAG" --format '{{.Size}}')"
docker image inspect "$TAG" --format '  architecture    {{.Architecture}}'
docker image inspect "$TAG" --format '  user            {{.Config.User}}'
docker image inspect "$TAG" --format '  cli version     {{index .Config.Labels "dev.kiro.cli.version"}}'
docker image inspect "$TAG" --format '  target arch     {{index .Config.Labels "dev.kiro.target.arch"}}'
docker image inspect "$TAG" --format '  workflows       {{index .Config.Labels "dev.kiro.workflows.checksum"}}'

# The MCP servers came from a lockfile rather than from the registry at task start, so the
# versions are a property of the image and worth printing next to the CLI's.
echo "  mcp servers"
docker run --rm --entrypoint sh "$TAG" -c \
    'cd /opt/mcp-servers && node -e "
const lock = require(\"./package-lock.json\");
const direct = Object.keys(require(\"./package.json\").dependencies || {});
for (const name of direct) {
  const entry = lock.packages[\"node_modules/\" + name];
  console.log(\"    \" + name + \"  \" + (entry ? entry.version : \"MISSING\"));
}
console.log(\"    (\" + (Object.keys(lock.packages).length - 1) + \" packages pinned by integrity hash)\");
"' 2>/dev/null || echo "    (could not read the lockfile in the image)"

echo
echo "largest layers:"
docker history "$TAG" --format '{{.Size}}\t{{.CreatedBy}}' 2>/dev/null \
    | grep -vE '^\s*0B' \
    | head -5 \
    | sed 's/^/  /' \
    | cut -c1-110
