#!/bin/bash
# Smart Router + lavap cache — UC-5 DEMO LANE
#
# The PRD's UC-5: an operator with NO RESP infrastructure. No redis, no
# resp-cache: block — just the router and the cache sidecar, exactly as
# deployments run today. Runs cold; nothing else has to be up first.
#
#   cache        127.0.0.1:${CACHE_PORT}   (lavap cache sidecar)
#   smartrouter  0.0.0.0:${ROUTER_PORT}    (metrics :${METRICS_PORT})
#
# USAGE
#   scripts/pre_setups/init_smartrouter_eth_lavap_cache.sh          # bring up + check
#   scripts/pre_setups/init_smartrouter_eth_lavap_cache.sh --stop   # tear down
#
# ENVIRONMENT: ETH_RPC_URL_1/2, ETH_WS_URL_1/2, ROUTER_PORT (3360),
# METRICS_PORT (7779), CACHE_PORT (20100), CACHE_METRICS_PORT (20200),
# HEALTH_INTERVAL (15s), SKIP_SMOKE=1
#
# OWNERSHIP SAFETY: never `killall`. Reclaims only recorded pids; refuses to
# start if its ports are held by anything else.

__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT=$(cd "${__dir}"/../.. && pwd)

LOGS_DIR="${PROJECT_ROOT}/debugging/logs"
mkdir -p "$LOGS_DIR"

ROUTER_PORT="${ROUTER_PORT:-3360}"
METRICS_PORT="${METRICS_PORT:-7779}"
CACHE_PORT="${CACHE_PORT:-20100}"
CACHE_METRICS_PORT="${CACHE_METRICS_PORT:-20200}"
HEALTH_INTERVAL="${HEALTH_INTERVAL:-15s}"

ROUTER_SCREEN="sr-lavap-cache"
CACHE_SCREEN="sr-lavap-cache-be"
CONFIG_REL="debugging/smartrouter_eth_lavap_cache.yml"
CONFIG_FILE="${PROJECT_ROOT}/${CONFIG_REL}"
PIDFILE="${PROJECT_ROOT}/debugging/.lavap-cache-router.pid"
CACHE_PIDFILE="${PROJECT_ROOT}/debugging/.lavap-cache-be.pid"
ROUTER_LOG="${LOGS_DIR}/SMARTROUTER_LAVAP_CACHE.log"
CACHE_LOG="${LOGS_DIR}/CACHE_LAVAP.log"

proc_fingerprint() { ps -p "$1" -o lstart= 2>/dev/null | tr -s ' '; }

# Screens are not enough: both processes run inside a `... | tee` pipeline and
# outlive `screen -X quit`, holding their ports against the next run.
reclaim_owned() {
    local pidfile="$1" what="$2"
    [[ -f "$pidfile" ]] || return 0
    local rec_pid rec_fp cur_fp
    rec_pid=$(cut -d'|' -f1 "$pidfile" 2>/dev/null)
    rec_fp=$(cut -d'|' -f2- "$pidfile" 2>/dev/null)
    [[ -n "$rec_pid" ]] || { rm -f "$pidfile"; return 0; }
    cur_fp=$(proc_fingerprint "$rec_pid")
    if [[ -z "$cur_fp" ]]; then rm -f "$pidfile"; return 0; fi
    if [[ "$cur_fp" != "$rec_fp" ]]; then
        echo "  stale pid file for the $what — leaving that process alone"
        rm -f "$pidfile"; return 0
    fi
    echo "  stopping this lane's previous $what (pid $rec_pid)"
    kill "$rec_pid" 2>/dev/null || true
    for _ in $(seq 1 20); do [[ -z "$(proc_fingerprint "$rec_pid")" ]] && break; sleep 0.25; done
    rm -f "$pidfile"
}

record_owned() {
    local pidfile="$1" port="$2" what="$3" pid=""
    for _ in $(seq 1 60); do
        pid=$(lsof -nP -iTCP:${port} -sTCP:LISTEN -t 2>/dev/null | head -1)
        [[ -n "$pid" ]] && break
        sleep 1
    done
    [[ -n "$pid" ]] || { echo "ERROR: the $what never bound :${port}"; return 1; }
    printf '%s|%s\n' "$pid" "$(proc_fingerprint "$pid")" > "$pidfile"
}


# Fallback ownership signal when the pid file is missing (interrupted run, crash
# between spawn and record): the port is held by a command line naming a file
# only this lane generates. Identity, not a name match, so it cannot reach
# another lane or checkout.
reclaim_by_identity() { # port, unique-substring, what
    local port="$1" needle="$2" what="$3" pid
    pid=$(lsof -nP -iTCP:${port} -sTCP:LISTEN -t 2>/dev/null | head -1)
    [[ -n "$pid" ]] || return 0
    ps -p "$pid" -o command= 2>/dev/null | grep -qF -- "$needle" || return 0
    echo "  stopping this lane's $what by identity (pid $pid, no pid file)"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
        lsof -nP -iTCP:${port} -sTCP:LISTEN -t >/dev/null 2>&1 || break
        sleep 0.25
    done
}

teardown() {
    echo "[Teardown] stopping this lane's resources (nothing else is signalled)"
    reclaim_owned "$PIDFILE" "router"
    reclaim_owned "$CACHE_PIDFILE" "cache sidecar"
    reclaim_by_identity "$ROUTER_PORT" "$CONFIG_REL" "router"
    reclaim_by_identity "$CACHE_PORT" "smartrouter cache 127.0.0.1:${CACHE_PORT}" "cache sidecar"
    screen -S "$ROUTER_SCREEN" -X quit 2>/dev/null || true
    screen -S "$CACHE_SCREEN" -X quit 2>/dev/null || true
    echo "[Teardown] done"
}

[[ "$1" == "--stop" || "$1" == "stop" ]] && { teardown; exit 0; }

command -v screen >/dev/null || { echo "ERROR: this lane needs screen"; exit 1; }

ETH_RPC_URL_1="${ETH_RPC_URL_1:-https://ethereum-rpc.publicnode.com}"
ETH_WS_URL_1="${ETH_WS_URL_1:-wss://ethereum-rpc.publicnode.com}"
ETH_RPC_URL_2="${ETH_RPC_URL_2:-https://eth.drpc.org}"
ETH_WS_URL_2="${ETH_WS_URL_2:-wss://eth.drpc.org}"

echo "============================================"
echo "UC-5 — no RESP infrastructure: router + lavap cache"
echo "============================================"

echo "[Setup] reclaiming this lane's previous run (if any)"
reclaim_owned "$PIDFILE" "router"
reclaim_owned "$CACHE_PIDFILE" "cache sidecar"
screen -S "$ROUTER_SCREEN" -X quit 2>/dev/null || true
screen -S "$CACHE_SCREEN" -X quit 2>/dev/null || true
sleep 1

BLOCKED=0
for port in "$ROUTER_PORT" "$METRICS_PORT" "$CACHE_PORT" "$CACHE_METRICS_PORT"; do
    if lsof -nP -iTCP:$port -sTCP:LISTEN >/dev/null 2>&1; then
        [[ "$BLOCKED" == "0" ]] && { echo ""; echo "ERROR: this lane's port(s) are held by process(es) it does not own."; echo "       Refusing to run. Nothing was signalled or removed."; }
        echo "  port $port:"; lsof -nP -iTCP:$port -sTCP:LISTEN | sed 's/^/    /'
        BLOCKED=1
    fi
done
[[ "$BLOCKED" == "1" ]] && { echo ""; echo "Free them (a RESP demo lane may be running: <lane> --stop), or move this one:"; echo "  ROUTER_PORT=3390 METRICS_PORT=7809 CACHE_PORT=20120 CACHE_METRICS_PORT=20220 $0"; exit 1; }

echo "[Setup] installing binaries"
make -C "$PROJECT_ROOT" install || { echo "ERROR: make install failed"; exit 1; }

echo "[Setup] starting the lavap cache sidecar on 127.0.0.1:${CACHE_PORT}"
screen -d -m -S "$CACHE_SCREEN" bash -c "source ~/.bashrc; smartrouter cache \
127.0.0.1:${CACHE_PORT} --metrics_address 0.0.0.0:${CACHE_METRICS_PORT} --log_level debug 2>&1 | tee $CACHE_LOG"
sleep 2
record_owned "$CACHE_PIDFILE" "$CACHE_PORT" "cache sidecar" || exit 1
echo "  cache sidecar: UP"

echo "[Setup] generating ${CONFIG_REL} (no resp-cache: block)"
cat > "$CONFIG_FILE" <<EOF
# GENERATED by scripts/pre_setups/init_smartrouter_eth_lavap_cache.sh.
# UC-5: no resp-cache: block anywhere — this is today's deployment shape.

metrics-listen-address: "0.0.0.0:${METRICS_PORT}"
cache-be: "127.0.0.1:${CACHE_PORT}"

endpoints:
  - listen-address: "0.0.0.0:${ROUTER_PORT}"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    network-address: "0.0.0.0:${ROUTER_PORT}"

direct-rpc:
  - name: "eth-rpc-1"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    node-urls:
      - url: "${ETH_RPC_URL_1}"
        skip-verifications:
          - chain-id
          - pruning
      - url: "${ETH_WS_URL_1}"

  - name: "eth-rpc-2"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    node-urls:
      - url: "${ETH_RPC_URL_2}"
        skip-verifications:
          - chain-id
          - pruning
      - url: "${ETH_WS_URL_2}"
EOF

echo "[Setup] starting the smart router (:${ROUTER_PORT})"
screen -d -m -S "$ROUTER_SCREEN" bash -c "cd $PROJECT_ROOT && source ~/.bashrc; smartrouter \
$CONFIG_REL \
--log-level debug \
--use-static-spec $PROJECT_ROOT/specs/ethereum.json \
--debug-relays \
--relays-health-interval ${HEALTH_INTERVAL} 2>&1 | tee $ROUTER_LOG"

ROUTER_OK=0
for _ in $(seq 1 45); do
    grep -q "cache service connected" "$ROUTER_LOG" 2>/dev/null && { ROUTER_OK=1; break; }
    grep -q "^Error:" "$ROUTER_LOG" 2>/dev/null && break
    sleep 1
done
[[ "$ROUTER_OK" == "1" ]] || { echo "ERROR: the router never connected to the cache — last log lines:"; tail -8 "$ROUTER_LOG" | sed 's/^/  /'; exit 1; }
record_owned "$PIDFILE" "$ROUTER_PORT" "router" || exit 1
echo "  router: UP"

if [[ "$SKIP_SMOKE" != "1" ]]; then
    echo ""
    echo "[Smoke] relay -> cache write -> cache hit"
    RELAY='{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1",false],"id":1}'
    note() { printf '  %-22s %s\n' "$1" "$2"; }
    PASS=1

    note "cache backend" "lavap cache (cache-be)"

    BODY=$(curl -sf -m 25 -X POST "http://127.0.0.1:${ROUTER_PORT}" -H 'Content-Type: application/json' -d "$RELAY" || true)
    [[ "$BODY" == *'"result"'* ]] && note "relay" "ok" || { note "relay" "failed"; PASS=0; }

    HITS=0
    for _ in $(seq 1 15); do
        curl -sf -m 25 -X POST "http://127.0.0.1:${ROUTER_PORT}" -H 'Content-Type: application/json' -d "$RELAY" >/dev/null 2>&1 || true
        HITS=$(curl -sf "http://127.0.0.1:${METRICS_PORT}/metrics" | awk '/^smartrouter_cache_success_total/ {s+=$NF} END {printf "%d", s}')
        [[ "$HITS" -gt 0 ]] && break
        sleep 1
    done
    [[ "$HITS" -gt 0 ]] && note "cache hits" "$HITS" || { note "cache hits" "0 (expected >0)"; PASS=0; }

    # Absent, not zero: the series register only when a RESP backend is built,
    # so this is what proves the new code path was never entered.
    SERIES=$(curl -sf "http://127.0.0.1:${METRICS_PORT}/metrics" | grep -c '^smartrouter_resp_cache')
    [[ "$SERIES" == "0" ]] && note "resp-cache metrics" "none (backend never built)" \
        || { note "resp-cache metrics" "$SERIES (expected none)"; PASS=0; }

    echo ""
    [[ "$PASS" == "1" ]] && echo "  UC-5 PASS — current behavior preserved; existing deployments need no changes." \
        || { echo "  UC-5 FAIL — see $ROUTER_LOG"; exit 1; }
fi

cat <<EOF

Router  http://127.0.0.1:${ROUTER_PORT}   metrics http://127.0.0.1:${METRICS_PORT}/metrics
Cache   127.0.0.1:${CACHE_PORT}
Config  ${CONFIG_REL}
Stop    $0 --stop
EOF
