#!/bin/bash
# M2~M5 on the deployed fleet. Run from bench/ with credentials for the fleet's
# account; every step goes through fleetbench.fleetload. Prerequisites, in order:
#   1. fleet deployed with -c fleet:workflowPath=bench/fanout -c fleet:maxTasks=25
#   2. five keys in Secrets Manager and five rows registered (fleetload register)
#   3. fleetload corpus run once
# SUBS lists the subscriptions in the order M2 adds them; put any key whose month-to-date
# credits are already high last, so it only joins the 5-key level.
# Usage: SUBS="sub-01 sub-02 sub-03 sub-04 sub-05" ./m2m5.sh [s0|m2|m4|m5|all]
set -u
read -r -a SUBS <<<"${SUBS:-sub-01 sub-02 sub-03 sub-04 sub-05}"
OUT=${OUT:-results/m2m5}
PROFILE=${AWS_PROFILE:-default}
PER_KEY=15   # 5 concurrent x 3 waves per key
FL=(uv run --with boto3 python -m fleetbench.fleetload --aws-profile "$PROFILE")

pool() { local IFS=,; "${FL[@]}" pool --enable "$*"; }
batch() { "${FL[@]}" run --tag "$1" --items "$2" --out "$OUT" "${@:3}"; echo "END $1 rc=$? $(date +%H:%M:%S)"; }

s0() { pool "${SUBS[0]}"; batch s0 5; }

m2() {
  for round in 1 2; do
    levels=(1 2 3 5); [ "$round" = 2 ] && levels=(5 3 2 1)
    for k in "${levels[@]}"; do
      pool "${SUBS[@]:0:$k}"
      batch "m2-r$round-k$k" $((PER_KEY * k))
    done
  done
}

m4() { pool "${SUBS[@]}"; batch m4 50 --chaos-fraction 0.2 --chaos-after 180; }

m5() { pool "${SUBS[@]}"; batch m5 75; }

case "${1:-all}" in
  s0) s0 ;; m2) m2 ;; m4) m4 ;; m5) m5 ;;
  all) s0 && m2 && m4 && m5 ;;
  *) echo "usage: $0 [s0|m2|m4|m5|all]" >&2; exit 2 ;;
esac
echo DONE
