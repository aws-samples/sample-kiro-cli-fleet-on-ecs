# The M1-on-ECS stream image: the worker image, plus the benchmark harness.
#
# Built FROM the worker image so a stream on ECS runs the exact CLI invocation the fleet
# runs -- run_workflow from the worker package, the same pinned CLI, the same clean HOME --
# with nothing added but the code that times it. Build the worker first:
#
#   ./tools/build-worker-image.sh --tag kiro-fleet-worker:bench
#
# then the BenchStack builds this file with WORKER_IMAGE pointing at that tag.

ARG WORKER_IMAGE=kiro-fleet-worker:bench
FROM ${WORKER_IMAGE}

COPY --chown=10001:10001 fleetbench /opt/bench/fleetbench

ENV PYTHONPATH=/opt/worker:/opt/bench
# Already the worker image's user; stated again because a scanner reads this file alone and
# cannot see the base image, and because the bench must never run as root even if the base
# changes.
USER 10001:10001

# One stream per task. The launcher passes the stream's arguments as the command.
ENTRYPOINT ["python3", "-m", "fleetbench.fleet", "stream"]
