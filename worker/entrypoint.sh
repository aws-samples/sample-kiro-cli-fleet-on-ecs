#!/bin/sh
#
# Check the environment, then get out of the way.
#
# Shell is good at asserting that a container was started correctly and bad at
# everything the worker does, so it does the first and hands over. The handover is an
# exec, deliberately: the Python process becomes PID 1 and receives SIGTERM directly.
# Without it the shell is PID 1, the signal never reaches the worker, and capacity comes
# back only when the lease expires — which on scale-in means the next task waits out a
# lease lifetime for capacity that is already free.

set -eu

EXIT_CONFIG=78        # configuration missing: fail before touching any work item

fail_config() {
    echo "entrypoint: $1" >&2
    exit "${EXIT_CONFIG}"
}

# Required, with no defaults. A worker that guessed a table or queue name could operate
# against the wrong pool, which is worse than not starting.
for name in SUBSCRIPTIONS_TABLE LEASES_TABLE WORK_QUEUE_URL RESULTS_BUCKET WORKFLOW_DIR; do
    eval "value=\${${name}:-}"
    [ -n "${value}" ] || fail_config "${name} is not set. It comes from the task definition."
done

[ -d "${WORKFLOW_DIR}" ] || fail_config "WORKFLOW_DIR ${WORKFLOW_DIR} does not exist in this image"
[ -f "${WORKFLOW_DIR}/workflow.json" ] || fail_config "no workflow.json in ${WORKFLOW_DIR}"

WORKSPACE_ROOT="${WORKSPACE_ROOT:-/workspace}"
mkdir -p "${WORKSPACE_ROOT}" 2>/dev/null || true
[ -w "${WORKSPACE_ROOT}" ] || fail_config "workspace ${WORKSPACE_ROOT} is not writable by $(id -un)"

command -v kiro-cli >/dev/null 2>&1 || fail_config "kiro-cli is not on PATH in this image"

echo "entrypoint: $(kiro-cli --version 2>/dev/null || echo 'kiro-cli version unknown')," \
     "workflow $(basename "${WORKFLOW_DIR}"), workspace ${WORKSPACE_ROOT}, user $(id -un)" >&2

exec python3 -m runner.main "$@"
