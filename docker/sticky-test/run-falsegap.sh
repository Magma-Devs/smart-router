#!/usr/bin/env bash
# The customer's false-gap bug: reproduce it, then show affinity removing it.
#
#   docker/sticky-test/run-falsegap.sh
#   KEEP=1 docker/sticky-test/run-falsegap.sh
#
# Exit codes: 0 pass, 1 assertion failed, 2 the harness is not trustworthy.
set -euo pipefail

cd "$(dirname "$0")"
COMPOSE=(docker compose -f docker-compose.falsegap.yml)

cleanup() {
  if [[ "${KEEP:-0}" != "1" ]]; then
    echo "==> tearing down"
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  else
    echo "==> stack left running (KEEP=1). Ingress :18081, router metrics :7811-3"
  fi
}
trap cleanup EXIT

echo "==> building and starting the stack"
"${COMPOSE[@]}" up -d --build --wait

echo "==> waiting for the ingress to serve relays"
for attempt in $(seq 1 60); do
  if curl -fsS -m 5 -H 'Content-Type: application/json' -H 'Connection: close' \
       -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
       http://127.0.0.1:18081 >/dev/null 2>&1; then
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

echo "==> settling"
sleep 10

echo "==> running assertions"
set +e
python3 driver_falsegap.py "$@"
status=$?
set -e

if [[ $status -ne 0 ]]; then
  echo
  echo "==> sticky claim outcomes"
  for port in 7811 7812 7813; do
    echo "--- router :${port}"
    curl -fsS -m 5 "http://127.0.0.1:${port}/metrics" 2>/dev/null \
      | grep '^smartrouter_csm_sticky' || echo "    (none)"
  done
fi

exit $status
