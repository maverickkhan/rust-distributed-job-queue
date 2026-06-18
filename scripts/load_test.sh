#!/usr/bin/env bash
#
# Reproducible submission load test for the distributed job queue.
#
# Submits COUNT jobs to the API with CONCURRENCY parallel curl workers and
# reports submission wall-clock time and throughput. It then polls queue stats
# until the backlog drains and reports processing throughput.
#
# This script measures *your* environment. It does not ship canned numbers —
# run it and record what you observe. Latency percentiles are best measured
# with a dedicated tool (see README "Benchmarks"); this script focuses on
# throughput and is dependency-light (bash + curl).
#
# Usage:
#   API=http://localhost:8080 QUEUE=default COUNT=1000 CONCURRENCY=20 ./scripts/load_test.sh
set -euo pipefail

API="${API:-http://localhost:8080}"
QUEUE="${QUEUE:-default}"
COUNT="${COUNT:-1000}"
CONCURRENCY="${CONCURRENCY:-20}"
JOB_TYPE="${JOB_TYPE:-echo}"

echo "Target:       $API"
echo "Queue:        $QUEUE"
echo "Job type:     $JOB_TYPE"
echo "Jobs:         $COUNT"
echo "Concurrency:  $CONCURRENCY"
echo

# Ensure the queue exists.
curl -fsS -X POST "$API/api/v1/queues" \
  -H 'content-type: application/json' \
  -d "{\"name\":\"$QUEUE\"}" >/dev/null || true

submit_one() {
  curl -fsS -o /dev/null -X POST "$API/api/v1/jobs" \
    -H 'content-type: application/json' \
    -d "{\"queue\":\"$QUEUE\",\"job_type\":\"$JOB_TYPE\",\"payload\":{\"n\":$1}}"
}
export -f submit_one
export API QUEUE JOB_TYPE

echo "Submitting..."
start=$(date +%s.%N)
seq "$COUNT" | xargs -P "$CONCURRENCY" -I{} bash -c 'submit_one {}'
end=$(date +%s.%N)

elapsed=$(echo "$end - $start" | bc -l)
rps=$(echo "$COUNT / $elapsed" | bc -l)
printf "Submitted %d jobs in %.2fs  =>  %.0f submissions/sec\n" "$COUNT" "$elapsed" "$rps"

echo
echo "Draining (polling queue stats)..."
drain_start=$(date +%s.%N)
while true; do
  stats=$(curl -fsS "$API/api/v1/queues/$QUEUE/stats")
  pending=$(echo "$stats" | grep -oE '"queued":[0-9]+|"processing":[0-9]+|"retrying":[0-9]+|"scheduled":[0-9]+' | grep -oE '[0-9]+' | paste -sd+ - | bc)
  echo "  pending=$pending  $stats"
  if [ "${pending:-0}" -eq 0 ]; then break; fi
  sleep 1
done
drain_end=$(date +%s.%N)
drain_elapsed=$(echo "$drain_end - $drain_start" | bc -l)
prps=$(echo "$COUNT / $drain_elapsed" | bc -l)
printf "Processed ~%d jobs in %.2fs  =>  %.0f jobs/sec (end-to-end, includes submit overlap)\n" \
  "$COUNT" "$drain_elapsed" "$prps"
