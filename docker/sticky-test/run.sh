#!/usr/bin/env bash
# Cross-pod sticky sessions, end to end: nginx -> 3 router replicas -> 4 upstreams,
# with one cache-be pod holding the fleet-wide claims.
#
#   docker/sticky-test/run.sh              # run it
#   KEEP=1 docker/sticky-test/run.sh       # leave the stack up afterwards
#
# Exit codes: 0 pass, 1 assertion failed, 2 the harness itself is not trustworthy
# (see the control phases in driver.py).
set -euo pipefail

cd "$(dirname "$0")"
COMPOSE=(docker compose -f docker-compose.sticky.yml)

cleanup() {
  if [[ "${KEEP:-0}" != "1" ]]; then
    echo "==> tearing down"
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  else
    echo "==> stack left running (KEEP=1). Ingress on :18080, router metrics on :7801-3"
  fi
}
trap cleanup EXIT

echo "==> building and starting the stack"
"${COMPOSE[@]}" up -d --build --wait

echo "==> waiting for the ingress to serve relays"
for attempt in $(seq 1 60); do
  if curl -fsS -m 5 -H 'Content-Type: application/json' -H 'Connection: close' \
       -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
       http://127.0.0.1:18080 >/dev/null 2>&1; then
    echo "    ready after ${attempt}s"
    break
  fi
  if [[ $attempt -eq 60 ]]; then
    echo "    ingress never served a relay; recent router logs:"
    "${COMPOSE[@]}" logs --tail 40 router-a
    exit 2
  fi
  sleep 1
done

# Settle before measuring, so startup verification traffic and the first
# provider-score samples do not land inside the window.
echo "==> settling"
sleep 10

echo "==> running assertions"
set +e
python3 driver.py "$@"
status=$?
set -e

if [[ $status -ne 0 ]]; then
  echo
  echo "==> sticky claim outcomes per replica (adopted > 0 is the cross-pod signal)"
  for port in 7801 7802 7803; do
    echo "--- router on :${port}"
    curl -fsS -m 5 "http://127.0.0.1:${port}/metrics" 2>/dev/null \
      | grep '^smartrouter_csm_sticky' || echo "    (no sticky metrics reported)"
  done
fi

exit $status
