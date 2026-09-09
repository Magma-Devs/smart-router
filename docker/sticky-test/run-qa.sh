#!/usr/bin/env bash
# Production-readiness scenarios for cross-pod sticky sessions.
#
#   docker/sticky-test/run-qa.sh
#
# Brings the stack up once and runs every scenario against it, including the ones that
# need the infrastructure disturbed (a replica restarted, the registry taken away and
# brought back). Exit 0 only if every scenario passes.
set -uo pipefail
cd "$(dirname "$0")"
COMPOSE=(docker compose -f docker-compose.falsegap.yml)
FAILED=0

run() { python3 qa_scenarios.py "$1" || FAILED=$((FAILED+1)); }

cleanup() {
  if [[ "${KEEP:-0}" != "1" ]]; then
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> starting the stack (3 router replicas, 5 upstreams, 1 registry)"
"${COMPOSE[@]}" up -d --build --wait >/dev/null 2>&1 || { echo "stack failed to start"; exit 2; }

for attempt in $(seq 1 60); do
  curl -fsS -m 5 -H 'Content-Type: application/json' -H 'Connection: close' \
    -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
    http://127.0.0.1:18081 >/dev/null 2>&1 && break
  [[ $attempt -eq 60 ]] && { echo "ingress never served a relay"; exit 2; }
  sleep 1
done
sleep 10   # settle: exclude startup verification traffic

echo
echo "=== NORMAL OPERATION ==="
run customer
run id-on-every-call
run concurrency
run select-provider

echo
echo "=== A REPLICA RESTARTS (rolling deploy, crash, eviction) ==="
"${COMPOSE[@]}" restart router-a >/dev/null 2>&1
sleep 12
run restarted-pod

echo
echo "=== THE REGISTRY GOES AWAY (cache-be down) ==="
"${COMPOSE[@]}" stop cache >/dev/null 2>&1
sleep 3
run cache-down

echo
echo "=== THE REGISTRY COMES BACK ==="
"${COMPOSE[@]}" start cache >/dev/null 2>&1
sleep 12
run cache-recovered

echo
if [[ $FAILED -eq 0 ]]; then
  echo "ALL SCENARIOS PASSED"
else
  echo "$FAILED SCENARIO(S) FAILED"
fi
exit $FAILED
