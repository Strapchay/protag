#!/usr/bin/env bash
set -u

# Repeat scenario suites and record per-run pass/fail plus aggregate results.
# Usage:
#   RUNS=3 MODE=deterministic scripts/scenario-flake.sh
#   RUNS=3 MODE=deterministic RACE=1 scripts/scenario-flake.sh
#   RUNS=1 MODE=live scripts/scenario-flake.sh
#   RUNS=2 MODE=all scripts/scenario-flake.sh

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUNS="${RUNS:-3}"
MODE="${MODE:-deterministic}"
RACE="${RACE:-0}"
OUTPUT="${OUTPUT:-/tmp/opencode/scenario-flake-$(date +%Y%m%dT%H%M%S)}"
mkdir -p "$OUTPUT"
RESULTS="$OUTPUT/runs.jsonl"
: > "$RESULTS"

run_suite() {
  local mode="$1"
  local run="$2"
  local log="$OUTPUT/${mode}_run_${run}.log"
  local start end status code
  start=$(date +%s%3N)
  if [ "$mode" = "live" ]; then
    AION_LIVE_SCENARIO=1 \
    AION_KERNEL_BIN="$ROOT/bin/aion-kernel" \
    AION_KERNEL_CONFIG="$ROOT/configs/aion.yaml" \
    GOCACHE="${GOCACHE:-/tmp/aion-go-cache}" \
      go test -tags=scenario_live ./scenario -run '^TestLive' -timeout 25m -count=1 >"$log" 2>&1
  else
    local args=(-tags=scenario ./internal/orchestrator -run '^TestScenario' -count=1)
    if [ "$RACE" = "1" ]; then
      args=(-race "${args[@]}")
    fi
    GOCACHE="${GOCACHE:-/tmp/aion-go-cache}" go test "${args[@]}" >"$log" 2>&1
  fi
  code=$?
  end=$(date +%s%3N)
  if [ "$code" -eq 0 ]; then status="passed"; else status="failed"; fi
  printf '{"mode":"%s","run":%d,"status":"%s","exit_code":%d,"duration_ms":%d,"log":"%s"}\n' \
    "$mode" "$run" "$status" "$code" $((end - start)) "$log" >> "$RESULTS"
  printf '%-13s run %-3s %-7s %6sms  %s\n' "$mode" "$run" "$status" $((end - start)) "$log"
  return "$code"
}

failed=0
for mode in $(if [ "$MODE" = "all" ]; then echo "deterministic live"; else echo "$MODE"; fi); do
  for run in $(seq 1 "$RUNS"); do
    if ! run_suite "$mode" "$run"; then
      failed=1
    fi
  done
done

python3 - "$RESULTS" "$OUTPUT/summary.json" <<'PY'
import json,sys
rows=[json.loads(line) for line in open(sys.argv[1]) if line.strip()]
modes=sorted({r["mode"] for r in rows})
summary={"runs":len(rows),"passed":sum(1 for r in rows if r["status"]=="passed"),
         "failed":sum(1 for r in rows if r["status"]=="failed"),
         "pass_rate":(sum(1 for r in rows if r["status"]=="passed")/len(rows)) if rows else 0,
         "by_mode":{m:{"runs":sum(1 for r in rows if r["mode"]==m),
                        "passed":sum(1 for r in rows if r["mode"]==m and r["status"]=="passed"),
                        "failed":sum(1 for r in rows if r["mode"]==m and r["status"]=="failed")}
                    for m in modes}}
json.dump(summary,open(sys.argv[2],"w"),indent=2)
print(json.dumps(summary,indent=2))
PY

if [ "$failed" -ne 0 ]; then
  echo "flake run failed; see $OUTPUT" >&2
  exit 1
fi
echo "flake run passed; artifacts: $OUTPUT"
