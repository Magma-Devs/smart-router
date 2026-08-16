#!/bin/bash
# RESP cache backend integration lane (docs/RESP-CACHE.md).
#
# Runs the router against a REAL Valkey (docker) instead of the cache sidecar,
# using the CHECKED-IN example config so the lane validates the same fixture
# operators are handed:
#
#   valkey (docker, 127.0.0.1:63790, volatile-lru)
#   smartrouter (:3360) config/smartrouter_examples/smartrouter_eth_resp_cache.yml
#                       --resp-cache-addresses 127.0.0.1:63790
#
# That config declares PublicNode and dRPC, each with an HTTP *and* a WS leg,
# because the ETH1 spec requires a websocket leg per provider — an HTTP-only
# provider is excluded at verification and drags overall-health to 503. The
# lane therefore does NOT pass --skip-websocket-verification: bypassing the
# gate would mask exactly the fixture property this lane exists to prove.
#
# The example config points resp-cache at the compose service name
# ("valkey:6379"); the --resp-cache-addresses flag overrides it to this lane's
# host-published port, which also exercises flag-over-YAML precedence.
#
# OWNERSHIP SAFETY. This lane never signals a process merely because it is
# named "smartrouter", and never force-removes a container merely because the
# name matches. It reclaims only resources it can PROVE it created (pid file
# with a start-time fingerprint; container with this lane's label) and
# otherwise refuses to run, leaving foreign processes untouched.
#
# RUN_DEMO=1 additionally proves the flow end to end: router health, metrics
# health, a real relay, RESP keys created under the prefix, and a genuine
# cache hit.
#
# The router and valkey are LEFT RUNNING for interactive testing; stop
# instructions are printed at the end.
__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

LOGS_DIR=${__dir}/../../debugging/logs
mkdir -p "$LOGS_DIR"
LOGS_DIR=$(cd "$LOGS_DIR" && pwd)

PROJECT_ROOT=$(cd "${__dir}"/../.. && pwd)
# Reuse the checked-in example rather than generating a throwaway config.
CONFIG_REL="config/smartrouter_examples/smartrouter_eth_resp_cache.yml"
CONFIG_FILE="$PROJECT_ROOT/$CONFIG_REL"
# Ports are overridable so the lane can be moved off a busy host (the
# pre-flight refusal message points at these).
VALKEY_NAME="${VALKEY_NAME:-smartrouter-resp-valkey}"
VALKEY_PORT="${VALKEY_PORT:-63790}"
METRICS_PORT="${METRICS_PORT:-7779}"
KEY_PREFIX="sr"
SCREEN_NAME="smartrouter-resp"
LANE_LABEL="sr-lane=resp-cache-init"
PIDFILE="$PROJECT_ROOT/debugging/.resp-cache-lane.pid"

command -v docker >/dev/null || { echo "ERROR: this lane needs docker (for valkey)"; exit 1; }
[[ -f "$CONFIG_FILE" ]] || { echo "ERROR: missing $CONFIG_REL"; exit 1; }

# The router's listen port lives in the CONFIG, not in a flag, so the config is
# the single source of truth: we read the declared port rather than assuming it.
# A ROUTER_PORT override must therefore move the LISTENER too — otherwise the
# pre-flight would guard one port while the router bound another, which would
# defeat the whole ownership guarantee (it could collide with a foreign process
# on the real port without ever checking it). When the override differs we
# materialise a rewritten copy under the git-ignored debugging/ scratch dir and
# run that; the checked-in example is never modified.
# The key must be anchored: "metrics-listen-address" ENDS with "listen-address",
# so an unanchored match picks up the metrics port instead of the RPC port.
CONFIG_PORT=$(grep -E '^[[:space:]]*-?[[:space:]]*listen-address:' "$CONFIG_FILE" \
              | head -1 | grep -oE ':[0-9]+"' | tr -d ':"')
[[ -n "$CONFIG_PORT" ]] || { echo "ERROR: could not read listen-address from $CONFIG_REL"; exit 1; }
ROUTER_PORT="${ROUTER_PORT:-$CONFIG_PORT}"

if [[ "$ROUTER_PORT" != "$CONFIG_PORT" ]]; then
    mkdir -p "$PROJECT_ROOT/debugging"
    GEN_REL="debugging/.resp-cache-lane-config.yml"
    sed "s/:${CONFIG_PORT}\"/:${ROUTER_PORT}\"/g" "$CONFIG_FILE" > "$PROJECT_ROOT/$GEN_REL"
    CONFIG_REL="$GEN_REL"
    CONFIG_FILE="$PROJECT_ROOT/$GEN_REL"
    echo "[Setup] ROUTER_PORT override ${CONFIG_PORT} -> ${ROUTER_PORT}: running a rewritten copy ($GEN_REL)"
fi

# --- ownership helpers -------------------------------------------------------
# A pid alone is not proof: pids are recycled. We fingerprint the process start
# time when we record it and require both to match before signalling anything.
proc_fingerprint() { ps -p "$1" -o lstart= 2>/dev/null | tr -s ' '; }

# Reclaim ONLY a router this lane previously started.
reclaim_owned_router() {
    [[ -f "$PIDFILE" ]] || return 0
    local rec_pid rec_fp cur_fp
    rec_pid=$(cut -d'|' -f1 "$PIDFILE" 2>/dev/null)
    rec_fp=$(cut -d'|' -f2- "$PIDFILE" 2>/dev/null)
    [[ -n "$rec_pid" ]] || { rm -f "$PIDFILE"; return 0; }
    cur_fp=$(proc_fingerprint "$rec_pid")
    if [[ -z "$cur_fp" ]]; then
        rm -f "$PIDFILE"; return 0          # already gone
    fi
    if [[ "$cur_fp" != "$rec_fp" ]]; then
        # Pid was recycled into an unrelated process — do NOT touch it.
        echo "[Setup] stale pid file (pid $rec_pid now belongs to another process) — leaving it alone"
        rm -f "$PIDFILE"; return 0
    fi
    echo "[Setup] stopping this lane's previous router (pid $rec_pid)"
    kill "$rec_pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
        [[ -z "$(proc_fingerprint "$rec_pid")" ]] && break
        sleep 0.25
    done
    rm -f "$PIDFILE"
}

# Remove the valkey container ONLY if it carries this lane's label.
reclaim_owned_container() {
    docker inspect "$VALKEY_NAME" >/dev/null 2>&1 || return 0
    local lbl
    lbl=$(docker inspect --format '{{index .Config.Labels "sr-lane"}}' "$VALKEY_NAME" 2>/dev/null)
    if [[ "$lbl" == "resp-cache-init" ]]; then
        echo "[Setup] removing this lane's previous valkey container"
        docker rm -f "$VALKEY_NAME" >/dev/null 2>&1 || true
    else
        echo "ERROR: a container named '$VALKEY_NAME' exists but was not created by this lane"
        echo "       (label sr-lane='${lbl:-<none>}'). Refusing to remove it."
        exit 1
    fi
}

reclaim_owned_router
reclaim_owned_container
# The screen session is this lane's own name; quitting it is safe and does not
# signal anything by executable name.
screen -S "$SCREEN_NAME" -X quit 2>/dev/null || true
sleep 1

# --- pre-flight: refuse rather than evict -----------------------------------
# Anything still listening here is NOT ours. Report it and stop; never signal.
BLOCKED=0
for port in $ROUTER_PORT $METRICS_PORT $VALKEY_PORT; do
    if lsof -nP -iTCP:$port -sTCP:LISTEN >/dev/null 2>&1; then
        if [[ "$BLOCKED" == "0" ]]; then
            echo "ERROR: this lane's port(s) are held by process(es) it does not own."
            echo "       Refusing to run. Nothing was signalled or removed."
        fi
        echo "  port $port:"
        lsof -nP -iTCP:$port -sTCP:LISTEN | sed 's/^/    /'
        BLOCKED=1
    fi
done
if [[ "$BLOCKED" == "1" ]]; then
    echo ""
    echo "Stop the owning process yourself, or re-run with different ports:"
    echo "  ROUTER_PORT=... METRICS_PORT=... VALKEY_PORT=... $0"
    exit 1
fi

echo "============================================"
echo "Smart Router RESP Cache Backend Lane"
echo "============================================"

echo "[Setup] installing binaries"
make -C "$PROJECT_ROOT" install

echo "[Setup] starting valkey (docker, 127.0.0.1:${VALKEY_PORT}, volatile-lru)"
docker run -d --rm --name "$VALKEY_NAME" --label "$LANE_LABEL" \
  -p 127.0.0.1:${VALKEY_PORT}:6379 \
  valkey/valkey:7.2 valkey-server --maxmemory 256mb --maxmemory-policy volatile-lru >/dev/null
for _ in $(seq 1 20); do
    docker exec "$VALKEY_NAME" valkey-cli ping >/dev/null 2>&1 && break
    sleep 0.5
done
docker exec "$VALKEY_NAME" valkey-cli ping >/dev/null || { echo "ERROR: valkey did not come up"; exit 1; }
echo "  valkey: UP"

echo "[Setup] starting smart router (:${ROUTER_PORT}) with $CONFIG_REL"
echo "        --resp-cache-addresses 127.0.0.1:${VALKEY_PORT} (overrides the config's compose address)"
# NOTE: the config path must be RELATIVE to the project root — the router's
# config loader resolves the argument against its search paths.
# No --skip-websocket-verification: the example config supplies a WS leg per
# provider and this lane must prove that.
#
# --relays-health-interval: /metrics/overall-health starts FAIL-CLOSED (503) and
# only flips once RelaysMonitorAggregator runs its first sweep — and that
# aggregator acts on the first TICK, never immediately. At the 5m production
# default the endpoint is therefore legitimately 503 for five minutes after
# boot, which no reasonable lane can wait out. We shorten the cadence for the
# lane instead of sleeping blindly: readiness is then observed, not assumed.
HEALTH_INTERVAL="${HEALTH_INTERVAL:-15s}"
screen -d -m -S "$SCREEN_NAME" bash -c "cd $PROJECT_ROOT && source ~/.bashrc; smartrouter \
$CONFIG_REL \
--log-level debug \
--resp-cache-addresses \"127.0.0.1:${VALKEY_PORT}\" \
--use-static-spec $PROJECT_ROOT/specs/ethereum.json \
--relays-health-interval ${HEALTH_INTERVAL} \
--metrics-listen-address ':${METRICS_PORT}' 2>&1 | tee $LOGS_DIR/SMARTROUTER_RESP.log" && sleep 0.25

# The definitive startup marker is OUR router logging its backend selection —
# port probes alone can false-green against a foreign process.
ROUTER_OK=0
for _ in $(seq 1 45); do
    if grep -q "resp-cache backend configured" "$LOGS_DIR/SMARTROUTER_RESP.log" 2>/dev/null; then
        ROUTER_OK=1
        break
    fi
    if grep -q "^Error:" "$LOGS_DIR/SMARTROUTER_RESP.log" 2>/dev/null; then
        break
    fi
    sleep 1
done
if [[ "$ROUTER_OK" != "1" ]]; then
    echo "ERROR: router did not come up with the RESP backend — last log lines:"
    tail -5 "$LOGS_DIR/SMARTROUTER_RESP.log" 2>/dev/null | sed 's/^/  /'
    exit 1
fi

# Record ownership so a later run can reclaim exactly this process (and only
# this process). The pid is resolved via the port it holds — but the RPC
# listener binds AFTER the backend-selection log line above, so we must wait for
# the bind rather than sample immediately. Getting this wrong is not cosmetic:
# an unrecorded router cannot be told apart from a foreign one, so the next run
# refuses on its own leftover process and the lane needs manual cleanup.
ROUTER_PID=""
for _ in $(seq 1 60); do
    ROUTER_PID=$(lsof -nP -iTCP:${ROUTER_PORT} -sTCP:LISTEN -t 2>/dev/null | head -1)
    [[ -n "$ROUTER_PID" ]] && break
    sleep 1
done
if [[ -z "$ROUTER_PID" ]]; then
    echo "ERROR: the router configured its RESP backend but never bound :${ROUTER_PORT}."
    echo "       Refusing to continue without an ownership record — a later run could"
    echo "       not distinguish this process from a foreign one. Last log lines:"
    tail -5 "$LOGS_DIR/SMARTROUTER_RESP.log" 2>/dev/null | sed 's/^/  /'
    exit 1
fi
mkdir -p "$(dirname "$PIDFILE")"
printf '%s|%s\n' "$ROUTER_PID" "$(proc_fingerprint "$ROUTER_PID")" > "$PIDFILE"
echo "  router: UP with resp-cache backend (pid ${ROUTER_PID}, logs: $LOGS_DIR/SMARTROUTER_RESP.log)"

if [[ "$RUN_DEMO" == "1" ]]; then
    echo ""
    echo "[Demo] health -> relay -> RESP keys -> cache hit"
    RELAY='{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'
    relay_body() { curl -sf -m 10 -X POST "http://127.0.0.1:${ROUTER_PORT}" -H 'Content-Type: application/json' -d "$RELAY"; }
    hits() { curl -sf "http://127.0.0.1:${METRICS_PORT}/metrics" | awk '/^smartrouter_cache_success_total/ {sum += $NF} END {printf "%d", sum}'; }
    connected() { curl -sf "http://127.0.0.1:${METRICS_PORT}/metrics" | awk '/^smartrouter_resp_cache_connected/ {print $NF; exit}'; }

    DEMO_FAIL=0
    note() { printf '  %-28s %s\n' "$1" "$2"; }

    # 1. router health — the lane must not declare success against a router
    #    that is up but unhealthy. The RPC listener binds slightly AFTER the
    #    backend-selection log line we gate startup on, so a single probe here
    #    races the bind and reports 000. Poll to a bounded deadline instead of
    #    sleeping a guessed amount: the wait is observed, not assumed.
    LAVA_HEALTH=""
    for _ in $(seq 1 30); do
        LAVA_HEALTH=$(curl -s -o /dev/null -w '%{http_code}' -m 10 "http://127.0.0.1:${ROUTER_PORT}/lava/health")
        [[ "$LAVA_HEALTH" == "200" ]] && break
        sleep 1
    done
    [[ "$LAVA_HEALTH" == "200" ]] && note "router /lava/health" "200" \
        || { note "router /lava/health" "$LAVA_HEALTH (expected 200)"; DEMO_FAIL=1; }

    # 2. metrics health — overall-health is fail-closed until the aggregator's
    #    first sweep (cadence set by --relays-health-interval above), so poll
    #    rather than sampling once. The budget must exceed that interval.
    OVERALL=""
    for _ in $(seq 1 60); do
        OVERALL=$(curl -s -o /dev/null -w '%{http_code}' -m 10 "http://127.0.0.1:${METRICS_PORT}/metrics/overall-health")
        [[ "$OVERALL" == "200" ]] && break
        relay_body >/dev/null 2>&1 || true      # drive the health sweep
        sleep 2
    done
    [[ "$OVERALL" == "200" ]] && note "metrics /overall-health" "200" \
        || { note "metrics /overall-health" "$OVERALL (expected 200)"; DEMO_FAIL=1; }

    # 3. backend connectivity gauge
    [[ "$(connected)" == "1" ]] && note "resp_cache_connected" "1" \
        || { note "resp_cache_connected" "$(connected) (expected 1)"; DEMO_FAIL=1; }

    # 4. a real relay returns a real result
    BODY=$(relay_body || true)
    if [[ "$BODY" == *'"result":"0x'* ]]; then note "relay eth_blockNumber" "ok"
    else note "relay eth_blockNumber" "unexpected body: ${BODY:0:80}"; DEMO_FAIL=1; fi

    # 5. keys land in the backend, and 6. a genuine cache hit occurs
    DEMO_OK=0
    for _ in $(seq 1 20); do
        relay_body >/dev/null 2>&1 || true
        sleep 0.5
        relay_body >/dev/null 2>&1 || true
        sleep 0.5
        if [[ "$(hits)" -gt 0 ]]; then DEMO_OK=1; break; fi
    done
    STORED=$(docker exec "$VALKEY_NAME" valkey-cli --scan --pattern "${KEY_PREFIX}:*" | head -5)
    STORED_N=$(docker exec "$VALKEY_NAME" valkey-cli --scan --pattern "${KEY_PREFIX}:*" | wc -l | tr -d ' ')
    [[ -n "$STORED" ]] && note "RESP keys under '${KEY_PREFIX}:'" "$STORED_N" \
        || { note "RESP keys under '${KEY_PREFIX}:'" "none"; DEMO_FAIL=1; }
    [[ "$DEMO_OK" == "1" ]] && note "cache_success_total" "$(hits) (>0)" \
        || { note "cache_success_total" "0 (expected >0)"; DEMO_FAIL=1; }

    echo ""
    if [[ "$DEMO_FAIL" == "0" ]]; then
        echo "DEMO PASS — served from the RESP backend; sample keys:"
        echo "$STORED" | sed 's/^/    /'
    else
        echo "DEMO FAIL — see $LOGS_DIR/SMARTROUTER_RESP.log"
        exit 1
    fi
fi

echo ""
echo "============================================"
echo "RESP cache lane ready"
echo "============================================"
echo "Router:   http://127.0.0.1:${ROUTER_PORT}   (metrics: :${METRICS_PORT})"
echo "Valkey:   127.0.0.1:${VALKEY_PORT}          (docker: ${VALKEY_NAME})"
echo "Config:   ${CONFIG_REL} (checked in; RESP address overridden by flag)"
echo ""
echo "Try it:"
echo "  curl -s -X POST http://127.0.0.1:${ROUTER_PORT} -H 'Content-Type: application/json' \\"
echo "    -d '{\"jsonrpc\":\"2.0\",\"method\":\"eth_blockNumber\",\"params\":[],\"id\":1}'"
echo ""
echo "Inspect the backend:"
echo "  docker exec ${VALKEY_NAME} valkey-cli --scan --pattern '${KEY_PREFIX}:*'"
echo "  curl -s http://127.0.0.1:${METRICS_PORT}/metrics | grep smartrouter_resp_cache"
echo ""
echo "To stop (lane-owned resources only):"
echo "  screen -S ${SCREEN_NAME} -X quit; docker rm -f ${VALKEY_NAME}; rm -f ${PIDFILE}"
echo "============================================"
