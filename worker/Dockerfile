# The worker image: one task runs one workflow at a time out of this.
#
# Build from the repository root, because the image carries both the worker and the
# workflow definitions:
#
#   docker build -f worker/Dockerfile --platform linux/arm64 -t kiro-fleet-worker .
#
# Everything measured about this pattern is measured from a task running this image, so
# it is pinned and its contents are verified during the build rather than trusted. An
# image that builds is an image whose CLI runs on the architecture it was built for.

# --- parameters --------------------------------------------------------------
#
# TARGET_ARCH is the one architecture knob. It selects the CLI artifact and must match
# the platform the image is built for. Changing architecture is a value change here plus
# the matching checksum, not an edit spread through the file.
#
# The CLI's aarch64 GNU build requires glibc 2.39, against 2.34 on x86_64 — a difference
# that appears in the installer, not the documentation. Amazon Linux 2023 ships 2.34 and
# therefore cannot run the aarch64 build at all, which is worth knowing because it is the
# obvious base image for anything on ECS. Debian 13 ships 2.41.

ARG TARGET_ARCH=aarch64
ARG REQUIRED_GLIBC_MINOR=39

# Pinned by digest, not by tag: a tag moves, and a measurement taken against a moving
# base cannot be defended later. This is the multi-architecture index digest, so it is
# correct for either value of TARGET_ARCH.
ARG BASE_IMAGE=debian:13-slim@sha256:89c1ea91e897f0fe0a3731595330bc45b8bdef4047464a07116f891a94054b31

ARG KIRO_CLI_VERSION=2.30.0
# SHA-256 of kirocli-aarch64-linux.zip for this version, as recorded in
# tools/build-worker-image.sh, which passes the right one for each architecture.
# Update both together.
ARG KIRO_CLI_SHA256=7a41094bdf39d792fadfb76616a2f3790b77d8908b905e73b72e3f2f76f761e5

# Node.js, the MCP server runtime, from the official release rather than Debian's package.
# Debian's `nodejs` pulls in its own `node-undici` package, which carried Critical
# advisories with no fixed version in Debian 13; the official build bundles a current
# undici and is pinned and verified the same way as the CLI. NODE_SHA256 is for
# node-v<version>-linux-arm64.tar.gz; tools/build-worker-image.sh passes the right one
# for each architecture. Update both together.
ARG NODE_VERSION=26.13.0
ARG NODE_SHA256=439e9c438acdf6a6ffe47418da709153d66c4bed812e30a473faacb107dd2791


# --- stage 1: fetch and verify -----------------------------------------------
#
# curl, unzip, a checksum tool, and npm are needed to assemble the image and have no
# business in a running task, so they stay here. npm in particular: a task that can
# install packages is a task that can change what it runs.

FROM ${BASE_IMAGE} AS fetch

ARG TARGET_ARCH
ARG REQUIRED_GLIBC_MINOR
ARG KIRO_CLI_VERSION
ARG KIRO_CLI_SHA256
ARG NODE_VERSION
ARG NODE_SHA256

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libatomic1 \
        unzip \
    && rm -rf /var/lib/apt/lists/*

# Node and npm for this stage only; the runtime stage takes the `node` binary alone.
RUN set -eux; \
    case "${TARGET_ARCH}" in \
        aarch64) node_arch=arm64 ;; \
        x86_64)  node_arch=x64 ;; \
        *) echo "unsupported TARGET_ARCH ${TARGET_ARCH}" >&2; exit 1 ;; \
    esac; \
    tarball="node-v${NODE_VERSION}-linux-${node_arch}.tar.gz"; \
    curl -fsSL -o "/tmp/${tarball}" "https://nodejs.org/dist/v${NODE_VERSION}/${tarball}"; \
    echo "${NODE_SHA256}  /tmp/${tarball}" | sha256sum -c -; \
    mkdir -p /opt/node; \
    tar -xzf "/tmp/${tarball}" -C /opt/node --strip-components=1 --no-same-owner; \
    rm -f "/tmp/${tarball}"; \
    /opt/node/bin/node --version
ENV PATH=/opt/node/bin:${PATH}

# Fail here rather than at task start. A base image whose glibc is below what this
# architecture's CLI build needs produces an image that installs fine and dies on the
# first run, which is the most expensive place to find out.
RUN set -eux; \
    minor="$(ldd --version | head -1 | sed -E 's/.* ([0-9]+)\.([0-9]+).*/\2/')"; \
    major="$(ldd --version | head -1 | sed -E 's/.* ([0-9]+)\.([0-9]+).*/\1/')"; \
    echo "base glibc ${major}.${minor}, ${TARGET_ARCH} requires 2.${REQUIRED_GLIBC_MINOR}"; \
    if [ "${major}" -ne 2 ] || [ "${minor}" -lt "${REQUIRED_GLIBC_MINOR}" ]; then \
        echo "REFUSING: glibc ${major}.${minor} is below 2.${REQUIRED_GLIBC_MINOR}," \
             "which the ${TARGET_ARCH} GNU build of Kiro CLI requires." >&2; \
        echo "Use a newer base image, or the -musl artifact variant." >&2; \
        exit 1; \
    fi

RUN set -eux; \
    url="https://prod.download.cli.kiro.dev/stable/${KIRO_CLI_VERSION}/kirocli-${TARGET_ARCH}-linux.zip"; \
    curl -fsSL -o /tmp/kirocli.zip "${url}"; \
    echo "${KIRO_CLI_SHA256}  /tmp/kirocli.zip" | sha256sum -c -; \
    unzip -q /tmp/kirocli.zip -d /tmp/extract; \
    chmod +x /tmp/extract/kirocli/install.sh; \
    KIRO_CLI_SKIP_SETUP=1 /tmp/extract/kirocli/install.sh; \
    rm -rf /tmp/kirocli.zip /tmp/extract


# The MCP servers the shipped workflows declare, installed here rather than fetched when a
# task runs.
#
# This was measured, not anticipated. The example workflow's `mcp.json` used `npx -y`, and
# a capture of one run showed `registry.npmjs.org` carrying 94% of all traffic — 5.6 MB per
# run, because `npx` downloads the server every time. Two things were wrong with that.
#
# It does not work in the network this sample deploys. Egress is restricted to named
# destinations and npm is not among them, so `--require-mcp-startup` would fail and every
# task with it. The fleet could not have run its own example.
#
# And it undid the pinning. The base image is pinned by digest and the CLI by checksum,
# while the agent's tooling arrived from whatever the registry served at task start. The
# lockfile alongside this pins 112 packages by integrity hash; `npm ci` refuses to deviate
# from it, so two builds of one commit install the same tree.
COPY workflows/mcp-servers/package.json workflows/mcp-servers/package-lock.json /opt/mcp-servers/
RUN cd /opt/mcp-servers \
    && npm ci --omit=dev --ignore-scripts --no-audit --no-fund \
    && npm cache clean --force

# --- stage 2: the runtime ----------------------------------------------------

FROM ${BASE_IMAGE}

ARG TARGET_ARCH
ARG KIRO_CLI_VERSION
ARG WORKFLOWS_CHECKSUM=unset

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    LANG=C.UTF-8 \
    WORKER_HOME=/opt/worker \
    WORKFLOWS_DIR=/opt/workflows \
    WORKSPACE_ROOT=/workspace

# The workflow's toolchain. Everything here was confirmed to run natively on arm64;
# adding to this list means confirming the addition too, because an emulated binary would
# quietly change every timing this image is used to measure.
#   git, ripgrep  - what a code-reading workflow uses
#   libatomic1    - a shared library the official Node.js build links against
#   python3       - the worker itself
#   ca-certificates - TLS to the Kiro service and to AWS
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        jq \
        libatomic1 \
        python3 \
        python3-pip \
        ripgrep \
    && rm -rf /var/lib/apt/lists/*

COPY --from=fetch /root/.local/bin/ /usr/local/bin/

# The MCP server runtime: the verified official `node` binary from the fetch stage, and
# nothing else from that release. Without npm: the servers are installed in the fetch
# stage from a lockfile, so a running task needs to execute them and has no reason to be
# able to install anything.
COPY --from=fetch /opt/node/bin/node /usr/local/bin/node
RUN node --version

# The MCP servers, and their entry points on PATH.
#
# On PATH so that one `mcp.json` works in the container and on a laptop: it names the
# command, not a path inside this image. A config that hardcoded /opt would have to differ
# between a local run and a task, and then the local run would stop being evidence about
# the task.
COPY --from=fetch /opt/mcp-servers /opt/mcp-servers
RUN set -eux; \
    for entry in /opt/mcp-servers/node_modules/.bin/*; do \
        [ -e "${entry}" ] || continue; \
        ln -sf "${entry}" "/usr/local/bin/$(basename "${entry}")"; \
    done; \
    # Fails the build if the server the shipped workflow declares is not actually here.
    # Without this the image builds, and the first task discovers it instead.
    command -v mcp-server-sequential-thinking >/dev/null \
        || { echo "REFUSING: mcp-server-sequential-thinking is not on PATH" >&2; exit 1; }

# A non-root user, and directories it owns. The workflow runs as this user, so a workflow
# that misbehaves cannot write outside what it was given.
RUN groupadd --system --gid 10001 worker \
    && useradd --system --uid 10001 --gid worker --home-dir /home/worker --create-home worker \
    && mkdir -p ${WORKSPACE_ROOT} ${WORKER_HOME} ${WORKFLOWS_DIR} \
    && chown -R 10001:10001 ${WORKSPACE_ROOT} ${WORKER_HOME} ${WORKFLOWS_DIR} /home/worker

# Runtime dependencies at the versions the lockfile resolved, so the image matches what
# the tests ran against. Pinned explicitly rather than installed by name: `pip install
# boto3` would resolve whatever is current at build time, which makes two builds of the
# same commit differ and quietly undoes the point of committing a lockfile.
#
# Passed in by tools/build-worker-image.sh, which reads worker/uv.lock. The default below
# is a fallback for a bare `docker build` and will drift; the script is the supported path
# precisely so these versions are never transcribed by hand.
ARG RUNTIME_DEPS="boto3==1.43.118 botocore==1.43.118 jmespath==1.1.0 python-dateutil==2.9.0.post0 s3transfer==0.19.3 six==1.17.0 urllib3==2.8.0"
RUN python3 -m pip install --break-system-packages --no-cache-dir --no-deps ${RUNTIME_DEPS}

# Copied for provenance: the image can be compared against the lockfile it was built from.
COPY worker/pyproject.toml worker/uv.lock ${WORKER_HOME}/

COPY --chown=10001:10001 worker/leaseclient ${WORKER_HOME}/leaseclient
COPY --chown=10001:10001 worker/runner ${WORKER_HOME}/runner
COPY --chown=10001:10001 worker/reconciler ${WORKER_HOME}/reconciler
COPY --chown=10001:10001 worker/entrypoint.sh ${WORKER_HOME}/entrypoint.sh

# Workflow definitions, carried as code. The checksum is passed in by the build script
# and recorded as a label, so a running task can be tied to the commit that produced it.
# Without it, "the prompt in the repository is the prompt that ran" is an assertion.
COPY --chown=10001:10001 workflows ${WORKFLOWS_DIR}

RUN chmod +x ${WORKER_HOME}/entrypoint.sh

LABEL org.opencontainers.image.title="kiro-fleet-worker" \
      org.opencontainers.image.description="Runs one headless Kiro CLI workflow per work item" \
      org.opencontainers.image.licenses="MIT-0" \
      dev.kiro.cli.version="${KIRO_CLI_VERSION}" \
      dev.kiro.target.arch="${TARGET_ARCH}" \
      dev.kiro.workflows.checksum="${WORKFLOWS_CHECKSUM}"

USER worker
WORKDIR ${WORKER_HOME}
ENV PYTHONPATH=${WORKER_HOME} \
    HOME=/home/worker

ENTRYPOINT ["/opt/worker/entrypoint.sh"]
