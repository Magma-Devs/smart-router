#!/bin/bash
# Smart Router + Redis (RESP cache backend) — DEMO LANE
#
# Based on init_smartrouter_eth.sh, but the router caches into a Redis running
# in docker instead of the `smartrouter cache` sidecar. Built to be driven by a
# human in front of an audience: it brings the whole stack up, proves it works
# before anyone is watching, and then prints a per-use-case cheat sheet.
#
#   redis        docker, 127.0.0.1:${REDIS_PORT}      (volatile-lru, optional AUTH)
#   cache        127.0.0.1:20100                       (the sidecar — kept running
#                                                       so UC-5 rollback is real)
#   smartrouter  0.0.0.0:${ROUTER_PORT}                (metrics :${METRICS_PORT})
#
# Both `resp-cache:` and `cache-be:` are written into the config on purpose:
# that is the documented precedence + rollback story (docs/RESP-CACHE.md).
#
# USAGE
#   scripts/pre_setups/init_smartrouter_eth_redis_demo.sh          # bring it up
#   RESP_CACHE=off scripts/pre_setups/init_smartrouter_eth_redis_demo.sh
#                                                                  # UC-5 rollback
#   scripts/pre_setups/init_smartrouter_eth_redis_demo.sh --stop   # tear it down
#
# ENVIRONMENT (all optional — the defaults are public endpoints that work
# without an API key, so the demo runs out of the box)
#   ETH_RPC_URL_1/2/3, ETH_WS_URL_1/2/3   upstream endpoints
#   REDIS_PASSWORD                        run Redis with AUTH and point the
#                                         router at a password FILE (demos the
#                                         rotation-capable credential form)
#   REDIS_PORT (63790)  ROUTER_PORT (3360)  METRICS_PORT (7779)
#                                         REDIS_PORT must avoid the ports the
#                                         docker drills bind — see the reserved
#                                         list below; the lane refuses on them.
#   KEY_PREFIX (sr)     HEALTH_INTERVAL (15s)   SKIP_SMOKE=1
#   REDIS_IMAGE (redis:7.2-alpine)        e.g. valkey/valkey:7.2
#
# The generated config and the password file land in debugging/ — which is
# gitignored — because ETH_RPC_URL_* may embed an API key and REDIS_PASSWORD is
# a credential. Neither ever reaches a tracked file.
#
# OWNERSHIP SAFETY. Unlike init_smartrouter_eth.sh this lane never runs
# `killall smartrouter` or `killall screen`: other lanes (and other checkouts)
# routinely have routers, caches and screens running, and a demo must not take
# them down. It reclaims only what it can PROVE it started — a pid file carrying
# a process start-time fingerprint, screens under its own names, a container
# carrying its own label — and refuses to run if its ports are held by anything
# else, printing the override to use instead.

__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT=$(cd "${__dir}"/../.. && pwd)

LOGS_DIR="${PROJECT_ROOT}/debugging/logs"
mkdir -p "$LOGS_DIR"

REDIS_NAME="${REDIS_NAME:-smartrouter-demo-redis}"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7.2-alpine}"
REDIS_PORT="${REDIS_PORT:-63790}"
ROUTER_PORT="${ROUTER_PORT:-3360}"
METRICS_PORT="${METRICS_PORT:-7779}"
CACHE_PORT="${CACHE_PORT:-20100}"
CACHE_METRICS_PORT="${CACHE_METRICS_PORT:-20200}"
KEY_PREFIX="${KEY_PREFIX:-sr}"
HEALTH_INTERVAL="${HEALTH_INTERVAL:-15s}"
LANE_LABEL="sr-lane=redis-demo"
ROUTER_SCREEN="sr-redis-demo"
CACHE_SCREEN="sr-redis-demo-cache"
CONFIG_REL="debugging/smartrouter_eth_redis_demo.yml"
CONFIG_FILE="${PROJECT_ROOT}/${CONFIG_REL}"
PW_FILE="${PROJECT_ROOT}/debugging/.redis-demo.pw"
PIDFILE="${PROJECT_ROOT}/debugging/.redis-demo-router.pid"
CACHE_PIDFILE="${PROJECT_ROOT}/debugging/.redis-demo-cache.pid"
# Second router — same Redis, different ports. Used by --shared-state to show
# that a peer replica reads what this one cached.
PEER_PORT="${PEER_PORT:-3365}"
PEER_METRICS_PORT="${PEER_METRICS_PORT:-7785}"
PEER_SCREEN="sr-redis-demo-peer"
PEER_CONFIG_REL="debugging/smartrouter_eth_redis_demo_peer.yml"
PEER_PIDFILE="${PROJECT_ROOT}/debugging/.redis-demo-peer.pid"
PEER_LOG="${LOGS_DIR}/SMARTROUTER_REDIS_DEMO_PEER.log"
ROUTER_LOG="${LOGS_DIR}/SMARTROUTER_REDIS_DEMO.log"
CACHE_LOG="${LOGS_DIR}/CACHE_REDIS_DEMO.log"

# redis-cli invocation that works with or without AUTH.
rcli() {
    if [[ -n "$REDIS_PASSWORD" ]]; then
        docker exec "$REDIS_NAME" redis-cli -a "$REDIS_PASSWORD" --no-auth-warning "$@"
    else
        docker exec "$REDIS_NAME" redis-cli "$@"
    fi
}

# A pid alone is not proof — pids get recycled. Record the process start time
# alongside it and require both to match before signalling anything.
proc_fingerprint() { ps -p "$1" -o lstart= 2>/dev/null | tr -s ' '; }

# Stop ONLY a process this lane previously started and recorded.
#
# Both the router and the cache sidecar need this. Quitting the screen session
# is NOT sufficient for either: they run inside a `... | tee` pipeline, so the
# process outlives `screen -X quit` and would hold its port against the next
# run — which the port pre-flight would then (correctly) refuse on.
reclaim_owned() {
    local pidfile="$1" what="$2"
    [[ -f "$pidfile" ]] || return 0
    local rec_pid rec_fp cur_fp
    rec_pid=$(cut -d'|' -f1 "$pidfile" 2>/dev/null)
    rec_fp=$(cut -d'|' -f2- "$pidfile" 2>/dev/null)
    [[ -n "$rec_pid" ]] || { rm -f "$pidfile"; return 0; }
    cur_fp=$(proc_fingerprint "$rec_pid")
    if [[ -z "$cur_fp" ]]; then rm -f "$pidfile"; return 0; fi          # already gone
    if [[ "$cur_fp" != "$rec_fp" ]]; then
        echo "  stale pid file for the $what (pid $rec_pid now belongs to another process) — leaving it alone"
        rm -f "$pidfile"; return 0
    fi
    echo "  stopping this lane's previous $what (pid $rec_pid)"
    kill "$rec_pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
        [[ -z "$(proc_fingerprint "$rec_pid")" ]] && break
        sleep 0.25
    done
    rm -f "$pidfile"
}

# Resolve the pid holding $2 and record it with its start-time fingerprint, so
# the next run can reclaim exactly this process and nothing else.
record_owned() {
    local pidfile="$1" port="$2" what="$3" pid=""
    for _ in $(seq 1 60); do
        pid=$(lsof -nP -iTCP:${port} -sTCP:LISTEN -t 2>/dev/null | head -1)
        [[ -n "$pid" ]] && break
        sleep 1
    done
    if [[ -z "$pid" ]]; then
        echo "ERROR: the $what never bound :${port} — refusing to continue without an"
        echo "       ownership record, since a later run could not tell it from a foreign process."
        return 1
    fi
    printf '%s|%s\n' "$pid" "$(proc_fingerprint "$pid")" > "$pidfile"
    echo "  $what pid ${pid} recorded"
    return 0
}

# Remove the container only if this lane created it.
reclaim_owned_container() {
    docker inspect "$REDIS_NAME" >/dev/null 2>&1 || return 0
    if [[ "$(docker inspect --format '{{index .Config.Labels "sr-lane"}}' "$REDIS_NAME" 2>/dev/null)" == "redis-demo" ]]; then
        docker rm -f "$REDIS_NAME" >/dev/null 2>&1 || true
    else
        echo "  NOTE: a container named '$REDIS_NAME' exists but this lane did not create it — leaving it alone."
    fi
}


# Fallback ownership signal: the pid file is the primary record, but if it is
# missing or was never written (an interrupted run, a crash between spawn and
# record) the process would survive every teardown and hold the port forever.
#
# A process is still provably ours when the port we own is held by a command
# line naming a file only this lane generates. That is identity, not a name
# match on "smartrouter", so it cannot hit another lane or another checkout.
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
    reclaim_owned "$PEER_PIDFILE" "peer router"
    reclaim_owned "$CACHE_PIDFILE" "cache sidecar"
    # Anything left holding our ports that is provably ours (see above).
    reclaim_by_identity "$ROUTER_PORT" "$CONFIG_REL"      "router"
    reclaim_by_identity "$PEER_PORT"   "$PEER_CONFIG_REL" "peer router"
    reclaim_by_identity "$CACHE_PORT"  "smartrouter cache 127.0.0.1:${CACHE_PORT}" "cache sidecar"
    # Screens carry this lane's own names; quitting them signals nothing by
    # executable name.
    screen -S "$ROUTER_SCREEN" -X quit 2>/dev/null || true
    screen -S "$PEER_SCREEN" -X quit 2>/dev/null || true
    screen -S "$CACHE_SCREEN" -X quit 2>/dev/null || true
    reclaim_owned_container
    rm -f "$PW_FILE"
    echo "[Teardown] done"
}



relay_block() { # port, block -> body
    curl -s -m 25 -X POST "http://127.0.0.1:$1" -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"$2\",false],\"id\":1}"
}
provider_of() { # port, block -> the Lava-Provider-Address value
    curl -s -m 25 -D- -o /dev/null -X POST "http://127.0.0.1:$1" -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"$2\",false],\"id\":1}" \
        | grep -i '^lava-provider-address:' | awk '{print $2}' | tr -d '\r'
}
wait_healthy() { # port — the lane returns once the backend is selected, which is
                 # BEFORE the RPC listener is serving; poll rather than guess.
    for _ in $(seq 1 45); do
        [[ "$(curl -s -o /dev/null -w '%{http_code}' -m 10 "http://127.0.0.1:$1/lava/health")" == "200" ]] && return 0
        sleep 1
    done
    return 1
}
hits_on() { # metrics port
    curl -sf "http://127.0.0.1:$1/metrics" | awk '/^smartrouter_cache_success_total/ {s+=$NF} END {printf "%d", s}'
}

# Persistence: the cache lives OUTSIDE the router, so restarting the router (or
# the sidecar) does not cost the cached data. With the in-memory sidecar the
# same restart empties the cache and every entry has to be re-fetched from the
# upstream nodes.
persistence() {
    local block="0x8"
    echo "============================================"
    echo "Persistence — the cache survives a router restart"
    echo "============================================"
    note() { printf '    %-34s %s\n' "$1" "$2"; }

    echo
    echo "[1] cache block ${block} through this router"
    relay_block "$ROUTER_PORT" "$block" >/dev/null || { echo "  ERROR: relay failed"; return 1; }
    sleep 1
    local key; key=$(rcli --scan --pattern "${KEY_PREFIX}:rel:f:*" 2>/dev/null | head -1)
    [[ -n "$key" ]] || { echo "  ERROR: nothing was cached"; return 1; }
    note "entry in redis" "${key}"
    note "hits before restart" "$(hits_on "$METRICS_PORT")"

    echo
    echo "[2] restart the router (redis keeps running, untouched)"
    SKIP_SMOKE=1 "$0" >/dev/null 2>&1 || { echo "  ERROR: restart failed"; return 1; }
    wait_healthy "$ROUTER_PORT" || { echo "  ERROR: the router did not come back — see $ROUTER_LOG"; return 1; }
    # A fresh process starts its counters at zero, which is what makes the next
    # number meaningful.
    note "hits after restart (fresh process)" "$(hits_on "$METRICS_PORT")"

    echo
    echo "[3] the FIRST request after the restart"
    local who; who=$(provider_of "$ROUTER_PORT" "$block")
    if [[ "$who" == "Cached" ]]; then
        note "served by" "Cached — no upstream call"
        note "hits now" "$(hits_on "$METRICS_PORT")"
        echo
        echo "  PASS — a restarted router serves a warm cache; nothing was re-fetched."
        echo "  (The sidecar would have come back empty.)"
    else
        note "served by" "$who (expected Cached)"
        echo "  FAIL — the entry did not survive; see $ROUTER_LOG"
        return 1
    fi
    echo "============================================"
}

# Shared state: a second router against the same backend reads what the first
# one cached. With the per-process sidecar each replica keeps its own copy, so
# a request routed to a cold replica is a miss.
shared_state() {
    local block="0x9"
    echo "============================================"
    echo "Shared state — a second router reads what the first cached"
    echo "============================================"
    note() { printf '    %-34s %s\n' "$1" "$2"; }

    echo
    echo "[1] starting a second router on :${PEER_PORT} against the SAME redis"
    sed -e "s/0.0.0.0:${METRICS_PORT}/0.0.0.0:${PEER_METRICS_PORT}/" \
        -e "s/0.0.0.0:${ROUTER_PORT}/0.0.0.0:${PEER_PORT}/" \
        "$CONFIG_FILE" > "${PROJECT_ROOT}/${PEER_CONFIG_REL}"
    reclaim_owned "$PEER_PIDFILE" "peer router"
    screen -S "$PEER_SCREEN" -X quit 2>/dev/null || true
    screen -d -m -S "$PEER_SCREEN" bash -c "cd $PROJECT_ROOT && source ~/.bashrc; smartrouter \
$PEER_CONFIG_REL \
--log-level debug \
--use-static-spec $PROJECT_ROOT/specs/ethereum.json \
--debug-relays \
--relays-health-interval ${HEALTH_INTERVAL} 2>&1 | tee $PEER_LOG"
    local ok=0
    for _ in $(seq 1 45); do
        grep -q "resp-cache backend configured" "$PEER_LOG" 2>/dev/null && { ok=1; break; }
        sleep 1
    done
    [[ "$ok" == "1" ]] || { echo "  ERROR: the peer router never started — see $PEER_LOG"; return 1; }
    record_owned "$PEER_PIDFILE" "$PEER_PORT" "peer router" >/dev/null || return 1
    wait_healthy "$PEER_PORT" || { echo "  ERROR: the peer router never became healthy — see $PEER_LOG"; return 1; }
    note "peer router" "UP on :${PEER_PORT}"
    note "peer cache hits (fresh)" "$(hits_on "$PEER_METRICS_PORT")"

    echo
    echo "[2] cache block ${block} through router 1 (:${ROUTER_PORT}) only"
    relay_block "$ROUTER_PORT" "$block" >/dev/null || { echo "  ERROR: relay failed"; return 1; }
    sleep 1
    note "entries in redis" "$(rcli --scan --pattern "${KEY_PREFIX}:rel:f:*" 2>/dev/null | wc -l | tr -d ' ')"

    echo
    echo "[3] the FIRST request for ${block} on router 2 — which never saw it"
    local who; who=$(provider_of "$PEER_PORT" "$block")
    if [[ "$who" == "Cached" ]]; then
        note "served by" "Cached — router 2 made no upstream call"
        note "peer cache hits" "$(hits_on "$PEER_METRICS_PORT")"
        echo
        echo "  PASS — replicas share one cache; scaling out does not cost hit rate."
        echo "  (With the per-process sidecar this would have been a miss.)"
    else
        note "served by" "$who (expected Cached)"
        echo "  FAIL — see $PEER_LOG"
        return 1
    fi
    echo
    echo "  Router 2 stays up on :${PEER_PORT}; '$0 --stop' removes both."
    echo "============================================"
}


# Convenience teardown across every RESP demo lane. The older pre_setups scripts
# offer `killall smartrouter` for this, which is a sledgehammer: it also kills
# routers belonging to other lanes and other checkouts. This calls each lane's
# own --stop instead, so only recorded pids and labelled containers go.
stop_all_lanes() {
    local lanes=(
        init_smartrouter_eth_redis_demo.sh
        init_smartrouter_eth_redis_sentinel.sh
        init_smartrouter_eth_redis_multiregion.sh
        init_smartrouter_eth_lavap_cache.sh
    )
    for lane in "${lanes[@]}"; do
        local path="${__dir}/${lane}"
        [[ -x "$path" ]] || continue
        echo "--- ${lane}"
        if [[ "$lane" == "init_smartrouter_eth_redis_demo.sh" ]]; then
            teardown                  # this script; calling itself would recurse
        else
            "$path" --stop
        fi
    done
    echo
    echo "All RESP demo lanes stopped. Routers from other lanes or checkouts were not touched"
    echo "(use 'killall smartrouter' yourself if you really want every router on the machine)."
}

if [[ "$1" == "--stop" || "$1" == "stop" ]]; then
    teardown
    exit 0
fi
[[ "$1" == "--stop-all" ]] && { stop_all_lanes; exit 0; }
[[ "$1" == "--persistence" ]] && { persistence; exit $?; }
[[ "$1" == "--shared-state" ]] && { shared_state; exit $?; }

command -v docker >/dev/null || { echo "ERROR: this lane needs docker (for redis)"; exit 1; }
docker info >/dev/null 2>&1 || { echo "ERROR: docker is installed but not running — start Docker Desktop"; exit 1; }
command -v screen >/dev/null || { echo "ERROR: this lane needs screen"; exit 1; }

# Upstreams. Defaults are public and key-less so the demo works everywhere; each
# provider needs an HTTP *and* a WS leg (the ETH1 spec requires a websocket leg
# per provider — an HTTP-only provider is dropped at verification and drags
# /metrics/overall-health to 503).
ETH_RPC_URL_1="${ETH_RPC_URL_1:-https://ethereum-rpc.publicnode.com}"
ETH_WS_URL_1="${ETH_WS_URL_1:-wss://ethereum-rpc.publicnode.com}"
ETH_RPC_URL_2="${ETH_RPC_URL_2:-https://eth.drpc.org}"
ETH_WS_URL_2="${ETH_WS_URL_2:-wss://eth.drpc.org}"
ETH_RPC_URL_3="${ETH_RPC_URL_3:-}"   # optional third provider
ETH_WS_URL_3="${ETH_WS_URL_3:-}"

RESP_ENABLED=1
[[ "$RESP_CACHE" == "off" ]] && RESP_ENABLED=0

# The docker-backed acceptance drills bind FIXED ports, and the sentinel drill
# (UC-2) is run as a step of this very demo — while this lane's redis is still
# up. Parking the demo redis on one of those ports makes the drill die with
# "port is already allocated", which reads like a broken feature in front of an
# audience. Refuse up front instead.
#   sentinel  63791 63792 26380 26381 26382
#   cluster    7100  7101  7102 (+ bus 17100-17102)
#   tls       63794
for reserved in 63791 63792 63794 26380 26381 26382 7100 7101 7102; do
    if [[ "$REDIS_PORT" == "$reserved" ]]; then
        echo "ERROR: REDIS_PORT=${REDIS_PORT} is reserved by a docker acceptance drill"
        echo "       (sentinel 63791/63792/26380-26382, cluster 7100-7102, tls 63794)."
        echo "       The UC-2 failover drill is a step of this demo and would fail to bind."
        echo "       Use a clear port, e.g. REDIS_PORT=63800."
        exit 1
    fi
done

echo "============================================"
echo "Smart Router + Redis RESP Cache — DEMO LANE"
echo "============================================"
if [[ "$RESP_ENABLED" == "1" ]]; then
    echo "Cache backend: REDIS (resp-cache)   [UC-1..UC-4]"
else
    echo "Cache backend: SIDECAR (cache-be)   [UC-5 rollback — resp-cache block removed]"
fi
echo "Upstreams:     ${ETH_RPC_URL_1}"
echo "               ${ETH_RPC_URL_2}"
[[ -n "$ETH_RPC_URL_3" ]] && echo "               ${ETH_RPC_URL_3}"
echo ""

# --- reclaim this lane's own leftovers, then refuse on anything foreign ------
echo "[Setup] reclaiming this lane's previous run (if any)"
reclaim_owned "$PIDFILE" "router"
reclaim_owned "$CACHE_PIDFILE" "cache sidecar"
screen -S "$ROUTER_SCREEN" -X quit 2>/dev/null || true
screen -S "$CACHE_SCREEN" -X quit 2>/dev/null || true
sleep 1

# Anything still listening on these ports is NOT ours. Report and stop — never
# signal it. Redis is excluded here: a lane-owned container legitimately holds
# its port between runs, and it is identified by label below.
BLOCKED=0
for port in $ROUTER_PORT $METRICS_PORT $CACHE_PORT $CACHE_METRICS_PORT; do
    if lsof -nP -iTCP:$port -sTCP:LISTEN >/dev/null 2>&1; then
        if [[ "$BLOCKED" == "0" ]]; then
            echo ""
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
    echo "Stop the owning process yourself, or run the demo on free ports:"
    echo "  ROUTER_PORT=3370 METRICS_PORT=7789 CACHE_PORT=20110 CACHE_METRICS_PORT=20210 \\"
    echo "    REDIS_PORT=63800 $0"
    exit 1
fi

echo "[Setup] installing binaries"
make -C "$PROJECT_ROOT" install || { echo "ERROR: make install failed"; exit 1; }

# --- redis -------------------------------------------------------------------
# Reuse a running lane-owned redis so RESP_CACHE=off (the rollback demo) does
# not wipe the cache we just filled — the audience should see the SAME redis
# still holding its keys after the rollback.
# RESP_CACHE=off is the UC-5 scenario: no RESP infrastructure exists, so no
# redis is started. (Restart the lane normally to get it back.)
if [[ "$RESP_ENABLED" == "0" ]]; then
    echo "[Setup] resp-cache disabled — starting NO redis (UC-5: no RESP infrastructure)"
fi
REDIS_RUNNING=0
if [[ "$RESP_ENABLED" == "1" ]] && docker inspect "$REDIS_NAME" >/dev/null 2>&1; then
    if [[ "$(docker inspect --format '{{index .Config.Labels "sr-lane"}}' "$REDIS_NAME" 2>/dev/null)" == "redis-demo" ]]; then
        echo "[Setup] reusing this lane's running redis (${REDIS_NAME})"
        REDIS_RUNNING=1
    else
        echo "ERROR: a container named '$REDIS_NAME' exists but was not created by this lane."
        echo "       Refusing to remove it. Set REDIS_NAME=<other> and re-run."
        exit 1
    fi
fi

if [[ "$RESP_ENABLED" == "1" && "$REDIS_RUNNING" == "0" ]]; then
    echo "[Setup] starting redis (docker, 127.0.0.1:${REDIS_PORT}, ${REDIS_IMAGE}, volatile-lru)"
    REDIS_ARGS=(--maxmemory 256mb --maxmemory-policy volatile-lru)
    [[ -n "$REDIS_PASSWORD" ]] && REDIS_ARGS+=(--requirepass "$REDIS_PASSWORD")
    docker run -d --rm --name "$REDIS_NAME" --label "$LANE_LABEL" \
        -p 127.0.0.1:${REDIS_PORT}:6379 \
        "$REDIS_IMAGE" redis-server "${REDIS_ARGS[@]}" >/dev/null || {
        echo "ERROR: could not start redis — is port ${REDIS_PORT} free?"; exit 1; }

    for _ in $(seq 1 20); do
        rcli ping >/dev/null 2>&1 && break
        sleep 0.5
    done
    rcli ping >/dev/null 2>&1 || { echo "ERROR: redis did not come up"; exit 1; }
    echo "  redis: UP${REDIS_PASSWORD:+ (AUTH required)}"
fi

# The credential goes in a FILE, never inline in the config: that is the
# rotation-capable form the router polls (docs/RESP-CACHE.md).
if [[ -n "$REDIS_PASSWORD" ]]; then
    umask 077
    printf '%s' "$REDIS_PASSWORD" > "$PW_FILE"
    echo "  credential file: ${PW_FILE} (mode 600, gitignored)"
fi

# --- cache sidecar (the rollback path) ---------------------------------------
echo "[Setup] starting the cache sidecar on 127.0.0.1:${CACHE_PORT} (rollback path for UC-5)"
screen -d -m -S "$CACHE_SCREEN" bash -c "source ~/.bashrc; smartrouter cache \
127.0.0.1:${CACHE_PORT} --metrics_address 0.0.0.0:${CACHE_METRICS_PORT} --log_level debug 2>&1 | tee $CACHE_LOG"
sleep 2
record_owned "$CACHE_PIDFILE" "$CACHE_PORT" "cache sidecar" || exit 1

# --- config ------------------------------------------------------------------
echo "[Setup] generating ${CONFIG_REL}"
{
cat <<EOF
# GENERATED by scripts/pre_setups/init_smartrouter_eth_redis_demo.sh — do not edit.
# Demo config: the router caches into Redis (resp-cache) while keeping the
# cache sidecar (cache-be) configured as the rollback path. With both set the
# RESP backend wins and the router logs a warning naming the precedence.

metrics-listen-address: "0.0.0.0:${METRICS_PORT}"

# The rollback path. Never dialled while resp-cache is configured.
cache-be: "127.0.0.1:${CACHE_PORT}"
EOF

if [[ "$RESP_ENABLED" == "1" ]]; then
cat <<EOF

resp-cache:
  addresses: ["127.0.0.1:${REDIS_PORT}"]
  key-prefix: "${KEY_PREFIX}"
EOF
    if [[ -n "$REDIS_PASSWORD" ]]; then
cat <<EOF
  # File-based credential: polled, and changes are pushed to LIVE connections,
  # which re-authenticate in place — no restart, no dropped connections.
  password-file: "${PW_FILE}"
  credential-refresh-interval: 5s
EOF
    fi
else
cat <<EOF

# resp-cache block intentionally ABSENT (RESP_CACHE=off) — UC-5 rollback:
# the preserved cache-be above takes over on this start.
EOF
fi

cat <<EOF

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

if [[ -n "$ETH_RPC_URL_3" && -n "$ETH_WS_URL_3" ]]; then
cat <<EOF

  - name: "eth-rpc-3"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    node-urls:
      - url: "${ETH_RPC_URL_3}"
        skip-verifications:
          - chain-id
          - pruning
      - url: "${ETH_WS_URL_3}"
EOF
fi
} > "$CONFIG_FILE"

# --- router ------------------------------------------------------------------
# --relays-health-interval: /metrics/overall-health starts FAIL-CLOSED (503) and
# only flips once the health aggregator's first TICK fires. At the 5m production
# default a freshly booted demo shows 503 while happily serving relays, which
# reads as "broken" to an audience. Shorten the cadence instead of waiting.
echo "[Setup] starting the smart router (:${ROUTER_PORT})"
screen -d -m -S "$ROUTER_SCREEN" bash -c "cd $PROJECT_ROOT && source ~/.bashrc; smartrouter \
$CONFIG_REL \
--log-level debug \
--use-static-spec $PROJECT_ROOT/specs/ethereum.json \
--debug-relays \
--relays-health-interval ${HEALTH_INTERVAL} 2>&1 | tee $ROUTER_LOG"
sleep 0.5

# Gate on OUR router logging its backend selection — a port probe alone can
# false-green against some other process.
if [[ "$RESP_ENABLED" == "1" ]]; then
    MARKER="resp-cache backend configured"
else
    MARKER="cache service connected"
fi
ROUTER_OK=0
for _ in $(seq 1 45); do
    grep -q "$MARKER" "$ROUTER_LOG" 2>/dev/null && { ROUTER_OK=1; break; }
    grep -q "^Error:" "$ROUTER_LOG" 2>/dev/null && break
    sleep 1
done
if [[ "$ROUTER_OK" != "1" ]]; then
    echo "ERROR: the router did not report its cache backend ('${MARKER}') — last log lines:"
    tail -8 "$ROUTER_LOG" 2>/dev/null | sed 's/^/  /'
    exit 1
fi
echo "  router: UP — backend selected (\"${MARKER}\")"

# The RPC listener binds AFTER the backend-selection line above, so this waits
# for the bind rather than sampling immediately.
record_owned "$PIDFILE" "$ROUTER_PORT" "router" || {
    tail -8 "$ROUTER_LOG" 2>/dev/null | sed 's/^/  /'
    exit 1
}

# --- smoke check (before the audience is watching) ---------------------------
RELAY='{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'
relay_once() { curl -sf -m 10 -X POST "http://127.0.0.1:${ROUTER_PORT}" -H 'Content-Type: application/json' -d "$RELAY"; }
cache_hits() { curl -sf "http://127.0.0.1:${METRICS_PORT}/metrics" | awk '/^smartrouter_cache_success_total/ {s+=$NF} END {printf "%d", s}'; }
resp_connected() { curl -sf "http://127.0.0.1:${METRICS_PORT}/metrics" | awk '/^smartrouter_resp_cache_connected/ {print $NF; exit}'; }

if [[ "$SKIP_SMOKE" != "1" ]]; then
    echo ""
    echo "[Smoke] relay -> cache write -> cache hit"
    SMOKE_FAIL=0
    note() { printf '  %-30s %s\n' "$1" "$2"; }

    HEALTH=""
    for _ in $(seq 1 30); do
        HEALTH=$(curl -s -o /dev/null -w '%{http_code}' -m 10 "http://127.0.0.1:${ROUTER_PORT}/lava/health")
        [[ "$HEALTH" == "200" ]] && break
        sleep 1
    done
    [[ "$HEALTH" == "200" ]] && note "router /lava/health" "200" || { note "router /lava/health" "$HEALTH (want 200)"; SMOKE_FAIL=1; }

    BODY=$(relay_once || true)
    [[ "$BODY" == *'"result":"0x'* ]] && note "relay eth_blockNumber" "ok" \
        || { note "relay eth_blockNumber" "unexpected: ${BODY:0:70}"; SMOKE_FAIL=1; }

    HIT=0
    for _ in $(seq 1 20); do
        relay_once >/dev/null 2>&1 || true; sleep 0.5
        relay_once >/dev/null 2>&1 || true; sleep 0.5
        [[ "$(cache_hits)" -gt 0 ]] && { HIT=1; break; }
    done
    [[ "$HIT" == "1" ]] && note "cache hits (smartrouter_cache_success_total)" "$(cache_hits)" \
        || { note "cache hits" "0 (want >0)"; SMOKE_FAIL=1; }

    if [[ "$RESP_ENABLED" == "1" ]]; then
        [[ "$(resp_connected)" == "1" ]] && note "resp_cache_connected" "1" \
            || { note "resp_cache_connected" "$(resp_connected) (want 1)"; SMOKE_FAIL=1; }
        KEYS=$(rcli --scan --pattern "${KEY_PREFIX}:*" 2>/dev/null | wc -l | tr -d ' ')
        [[ "$KEYS" -gt 0 ]] && note "keys in redis under '${KEY_PREFIX}:'" "$KEYS" \
            || { note "keys in redis" "none (want >0)"; SMOKE_FAIL=1; }
    fi

    echo ""
    if [[ "$SMOKE_FAIL" == "0" ]]; then
        echo "  SMOKE PASS — the stack is demo-ready."
    else
        echo "  SMOKE FAIL — see $ROUTER_LOG"
        exit 1
    fi
fi

# --- cheat sheet -------------------------------------------------------------
RCLI_HINT="docker exec ${REDIS_NAME} redis-cli"
[[ -n "$REDIS_PASSWORD" ]] && RCLI_HINT="docker exec ${REDIS_NAME} redis-cli -a \$REDIS_PASSWORD --no-auth-warning"

# Re-runs must carry the same non-default settings, or they would target a
# different stack. Echo back exactly what this run used.
ENV_PREFIX=""
[[ "$ROUTER_PORT"        != "3360"                  ]] && ENV_PREFIX+="ROUTER_PORT=${ROUTER_PORT} "
[[ "$METRICS_PORT"       != "7779"                  ]] && ENV_PREFIX+="METRICS_PORT=${METRICS_PORT} "
[[ "$CACHE_PORT"         != "20100"                 ]] && ENV_PREFIX+="CACHE_PORT=${CACHE_PORT} "
[[ "$CACHE_METRICS_PORT" != "20200"                 ]] && ENV_PREFIX+="CACHE_METRICS_PORT=${CACHE_METRICS_PORT} "
[[ "$REDIS_PORT"         != "63790"                 ]] && ENV_PREFIX+="REDIS_PORT=${REDIS_PORT} "
[[ "$REDIS_NAME"         != "smartrouter-demo-redis" ]] && ENV_PREFIX+="REDIS_NAME=${REDIS_NAME} "
[[ -n "$REDIS_PASSWORD"                             ]] && ENV_PREFIX+="REDIS_PASSWORD=\$REDIS_PASSWORD "

cat <<EOF

============================================
DEMO CHEAT SHEET  (full script: docs/RESP-CACHE.md)
============================================
Router    http://127.0.0.1:${ROUTER_PORT}      metrics http://127.0.0.1:${METRICS_PORT}/metrics
Redis     127.0.0.1:${REDIS_PORT}              container ${REDIS_NAME}
Sidecar   127.0.0.1:${CACHE_PORT}              (rollback path, not in use$([[ "$RESP_ENABLED" == "0" ]] && echo " — ACTIVE NOW"))
Config    ${CONFIG_REL}
Log       tail -f ${ROUTER_LOG}

UC-1  Drop-in external cache — relay, then see the entry in Redis
  # Ask for a FINALIZED historical block: it gets the 1h TTL and stays visible.
  # A "latest" query (eth_blockNumber) is cached too, but with a sub-second TTL
  # — it expires before you can point at it.
  curl -s -X POST http://127.0.0.1:${ROUTER_PORT} -H 'Content-Type: application/json' \\
    -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1",false],"id":1}'
  ${RCLI_HINT} --scan --pattern '${KEY_PREFIX}:*'
  ${RCLI_HINT} ttl \$(${RCLI_HINT} --scan --pattern '${KEY_PREFIX}:rel:f:*' | head -1)
  # run the curl again — same answer, now served from Redis:
  curl -s http://127.0.0.1:${METRICS_PORT}/metrics | grep '^smartrouter_cache_success_total'

UC-2  High availability — sentinel failover drill (kills the primary mid-run)
  RESP_CACHE_TEST_SENTINEL_DOCKER=1 \\
    go test ./ecosystem/cache/redisstore -run TestSentinelFailover -v -timeout 5m

UC-3  Multi-region read/write split — see docs/RESP-CACHE.md; the routing proof is
  go test ./ecosystem/cache/redisstore -run TestReadWriteRouting -v

UC-4  Backend failure — relays keep serving, metrics flip
  # Reuse the FINALIZED request from UC-1 — the one cached in Redis right now.
  # Lava-Provider-Address shows where the answer came from.
  PROV="curl -s -D- -o /dev/null -X POST http://127.0.0.1:${ROUTER_PORT}"
  \$PROV -H 'Content-Type: application/json' \\
    -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1",false],"id":1}' \\
    | grep -i Lava-Provider-Address          # -> Cached
  docker pause ${REDIS_NAME}
  \$PROV -H 'Content-Type: application/json' \\
    -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1",false],"id":1}' \\
    | grep -i Lava-Provider-Address          # -> eth-rpc-N (same answer, no error)
  curl -s http://127.0.0.1:${METRICS_PORT}/metrics | grep smartrouter_resp_cache
  docker unpause ${REDIS_NAME}
  (use pause, not stop: the container runs with --rm)

Persistence   restart the router, cache stays warm
  ${ENV_PREFIX}$0 --persistence

Shared state  a second router reads what this one cached
  ${ENV_PREFIX}$0 --shared-state

UC-5  No RESP infrastructure — its own lane (stop this one first)
  $0 --stop
  scripts/pre_setups/init_smartrouter_eth_lavap_cache.sh

Stop everything
  ${ENV_PREFIX}$0 --stop        this lane
  $0 --stop-all                 every RESP demo lane
============================================
EOF
