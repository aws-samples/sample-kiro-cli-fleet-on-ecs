#!/bin/bash
# Resume M2~M5 after an interrupted m2m5.sh: runs the same batches in the same order and
# skips every tag whose <tag>-summary.json already exists in $OUT. Run from bench/.
# Usage: SUBS="sub-01 sub-02 sub-03 sub-04 sub-05" ./m2m5-resume.sh
set -u
read -r -a SUBS <<<"${SUBS:-sub-01 sub-02 sub-03 sub-04 sub-05}"
OUT=${OUT:-results/m2m5}
PROFILE=${AWS_PROFILE:-default}
PER_KEY=15
FL=(uv run --with boto3 python -m fleetbench.fleetload --aws-profile "$PROFILE")

pool() { local IFS=,; "${FL[@]}" pool --enable "$*"; }
step() {  # tag keys items [run options...]
  local tag=$1 keys=$2 items=$3; shift 3
  if [ -f "$OUT/$tag-summary.json" ]; then echo "SKIP $tag (summary exists)"; return 0; fi
  pool "${SUBS[@]:0:$keys}"
  "${FL[@]}" run --tag "$tag" --items "$items" --out "$OUT" "$@"
  echo "END $tag rc=$? $(date +%H:%M:%S)"
}

step s0 1 5
for k in 1 2 3 5; do step "m2-r1-k$k" "$k" $((PER_KEY * k)); done
for k in 5 3 2 1; do step "m2-r2-k$k" "$k" $((PER_KEY * k)); done
step m4 5 50 --chaos-fraction 0.2 --chaos-after 180
step m5 5 75
echo DONE
