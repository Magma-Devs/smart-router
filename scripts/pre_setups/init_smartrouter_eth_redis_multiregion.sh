#!/bin/bash
# Smart Router — UC-3 MULTI-REGION READ/WRITE SPLIT DEMO LANE
#
# Third of the RESP demo lanes (standalone -> sentinel -> multi-region). It
# models the PRD's multi-region picture with two "regions" on one laptop:
#
#   region A   redis  127.0.0.1:${A_PORT}   the write endpoint (holds the data)
#   region B   redis  127.0.0.1:${B_PORT}   a REPLICA of A — region B's local reader
#
#   router-A   :${ROUTER_A_PORT}   caches normally: reads and writes region A
#   router-B   :${ROUTER_B_PORT}   writes -> region A, reads -> region B
#                                  (resp-cache: addresses + read-addresses)
#
# Real deployments get the replication from the infrastructure (ElastiCache
# Global Datastore, MemoryDB Multi-Region); the router-side requirement the PRD
# actually asks for is only the read/write endpoint separation. Redis's own
# replication stands in for the infra here so the whole loop is visible.
#
# WHAT THE DEMO PROVES (--demo runs it end to end)
#   1. A relay through router-A writes an entry into region A.
#   2. The infrastructure replicates it to region B (poll until it lands).
#   3. The SAME relay through router-B is a cache HIT — it never touches a
#      blockchain node, and Lava-Cache-Backend names region B's replica, so the
#      read demonstrably came from the LOCAL endpoint.
#   4. A relay for a DIFFERENT block through router-B writes back to region A
#      (the write endpoint), not to its local replica.
#
# That is the whole point of UC-3: blockchain nodes live in one region, every
# region serves cached reads locally.
#
# USAGE
#   scripts/pre_setups/init_smartrouter_eth_redis_multiregion.sh          # bring up
#   scripts/pre_setups/init_smartrouter_eth_redis_multiregion.sh --demo   # the story
#   scripts/pre_setups/init_smartrouter_eth_redis_multiregion.sh --status
#   scripts/pre_setups/init_smartrouter_eth_redis_multiregion.sh --stop
#
# ENVIRONMENT
#   LAN_IP        override the auto-detected host address. Needed only for the
#                 replica->primary link: inside container B, 127.0.0.1 is B
#                 itself, so the replicaof target must be an address the
#                 container can route to. Both ROUTERS use plain 127.0.0.1.
#   REDIS_IMAGE (redis:7.2-alpine)   A_PORT (63821)  B_PORT (63822)
#   ROUTER_A_PORT (3380) METRICS_A_PORT (7801)
#   ROUTER_B_PORT (3381) METRICS_B_PORT (7802)
#   ETH_RPC_URL_1/2, ETH_WS_URL_1/2, KEY_PREFIX (sr), HEALTH_INTERVAL (15s),
#   SKIP_SMOKE=1
#
# OWNERSHIP SAFETY: never `killall`. Reclaims only recorded pids and labelled
# containers; refuses to start if its ports are held by anything else.

__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT=$(cd "${__dir}"/../.. && pwd)

LOGS_DIR="${PROJECT_ROOT}/debugging/logs"
mkdir -p "$LOGS_DIR"

REDIS_IMAGE="${REDIS_IMAGE:-redis:7.2-alpine}"
A_PORT="${A_PORT:-63821}"
B_PORT="${B_PORT:-63822}"
ROUTER_A_PORT="${ROUTER_A_PORT:-3380}"
METRICS_A_PORT="${METRICS_A_PORT:-7801}"
ROUTER_B_PORT="${ROUTER_B_PORT:-3381}"
METRICS_B_PORT="${METRICS_B_PORT:-7802}"
KEY_PREFIX="${KEY_PREFIX:-sr}"
HEALTH_INTERVAL="${HEALTH_INTERVAL:-15s}"

LANE_LABEL="sr-lane=multiregion-demo"
A_NAME="sr-demo-region-a"
B_NAME="sr-demo-region-b"
A_SCREEN="sr-mr-router-a"
B_SCREEN="sr-mr-router-b"
CONFIG_A_REL="debugging/smartrouter_eth_mr_region_a.yml"
CONFIG_B_REL="debugging/smartrouter_eth_mr_region_b.yml"
PID_A="${PROJECT_ROOT}/debugging/.mr-router-a.pid"
PID_B="${PROJECT_ROOT}/debugging/.mr-router-b.pid"
LOG_A="${LOGS_DIR}/SMARTROUTER_MR_REGION_A.log"
LOG_B="${LOGS_DIR}/SMARTROUTER_MR_REGION_B.log"

CLI_BIN="redis-cli"; [[ "$REDIS_IMAGE" == *valkey* ]] && CLI_BIN="valkey-cli"
SERVER_BIN="redis-server"; [[ "$REDIS_IMAGE" == *valkey* ]] && SERVER_BIN="valkey-server"

acli() { docker exec "$A_NAME" "$CLI_BIN" "$@"; }
bcli() { docker exec "$B_NAME" "$CLI_BIN" "$@"; }

detect_lan_ip() {
    local ip
    for iface in en0 en1 en2; do
        ip=$(ipconfig getifaddr "$iface" 2>/dev/null) && [[ -n "$ip" ]] && { echo "$ip"; return 0; }
    done
    ip=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7; exit}')
    [[ -n "$ip" ]] && { echo "$ip"; return 0; }
    return 1
}

proc_fingerprint() { ps -p "$1" -o lstart= 2>/dev/null | tr -s ' '; }

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
        echo "  stale pid file for $what (pid $rec_pid is now another process) — leaving it alone"
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
    [[ -n "$pid" ]] || { echo "ERROR: $what never bound :${port}"; return 1; }
    printf '%s|%s\n' "$pid" "$(proc_fingerprint "$pid")" > "$pidfile"
    echo "  $what pid ${pid} recorded"
}

rm_owned_container() {
    local name="$1"
    docker inspect "$name" >/dev/null 2>&1 || return 0
    if [[ "$(docker inspect --format '{{index .Config.Labels "sr-lane"}}' "$name" 2>/dev/null)" == "multiregion-demo" ]]; then
        docker rm -f "$name" >/dev/null 2>&1 || true
    else
        echo "  NOTE: container '$name' exists but this lane did not create it — leaving it alone."
    fi
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
    reclaim_owned "$PID_A" "router-A"
    reclaim_owned "$PID_B" "router-B"
    reclaim_by_identity "$ROUTER_A_PORT" "$CONFIG_A_REL" "router-A"
    reclaim_by_identity "$ROUTER_B_PORT" "$CONFIG_B_REL" "router-B"
    screen -S "$A_SCREEN" -X quit 2>/dev/null || true
    screen -S "$B_SCREEN" -X quit 2>/dev/null || true
    rm_owned_container "$A_NAME"
    rm_owned_container "$B_NAME"
    echo "[Teardown] done"
}

status() {
    echo "region A (write endpoint, 127.0.0.1:${A_PORT})"
    printf '  role: %s   keys: %s\n' \
        "$(acli info replication 2>/dev/null | awk -F: '/^role:/{print $2}' | tr -d '\r')" \
        "$(acli --scan --pattern "${KEY_PREFIX}:*" 2>/dev/null | wc -l | tr -d ' ')"
    echo "region B (local reader, 127.0.0.1:${B_PORT})"
    printf '  role: %s   keys: %s   link: %s\n' \
        "$(bcli info replication 2>/dev/null | awk -F: '/^role:/{print $2}' | tr -d '\r')" \
        "$(bcli --scan --pattern "${KEY_PREFIX}:*" 2>/dev/null | wc -l | tr -d ' ')" \
        "$(bcli info replication 2>/dev/null | awk -F: '/^master_link_status:/{print $2}' | tr -d '\r')"
}

# The narrated UC-3 story.
relay_to() { # port, block
    curl -s -m 25 -X POST "http://127.0.0.1:$1" -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"$2\",false],\"id\":1}"
}
headers_from() { # port, block
    curl -s -m 25 -D- -o /dev/null -X POST "http://127.0.0.1:$1" -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"$2\",false],\"id\":1}" \
        | grep -iE 'lava-provider-address|lava-cache-backend' | tr -d '\r'
}
# Count and list FINALIZED relay entries only. Non-finalized ("latest") entries
# carry a sub-second TTL, so they appear and vanish between two samples — a
# count that includes them can stay flat while a write really did land, which
# reads as a broken feature mid-demo. Finalized entries hold for an hour.
keys_on() { # cli-fn
    $1 --scan --pattern "${KEY_PREFIX}:rel:f:*" 2>/dev/null | wc -l | tr -d ' '
}
keylist_on() { # cli-fn
    $1 --scan --pattern "${KEY_PREFIX}:rel:f:*" 2>/dev/null | sort
}

demo() {
    local block_shared="0x5" block_b_only="0x6"
    echo "============================================"
    echo "UC-3 — one region caches, every region reads locally"
    echo "============================================"

    echo
    echo "[0] starting clean"
    acli flushdb >/dev/null 2>&1
    for _ in $(seq 1 20); do [[ "$(keys_on bcli)" == "0" ]] && break; sleep 0.5; done
    status

    echo
    echo "[1] relay block ${block_shared} through router-A (region A, where the nodes are)"
    relay_to "$ROUTER_A_PORT" "$block_shared" >/dev/null || { echo "  ERROR: relay failed"; return 1; }
    for _ in $(seq 1 20); do [[ "$(keys_on acli)" -gt 0 ]] && break; sleep 0.5; done
    echo "    entry now in region A:"
    keylist_on acli | sed 's/^/      /'

    echo
    echo "[2] the infrastructure replicates it to region B"
    local cached_key; cached_key=$(keylist_on acli | head -1)
    local replicated=0
    for _ in $(seq 1 40); do
        keylist_on bcli | grep -qF "$cached_key" && { replicated=1; break; }
        sleep 0.5
    done
    if [[ "$replicated" == "1" ]]; then
        echo "    the same entry is now in region B (replicated, not re-fetched):"
        echo "      ${cached_key}"
    else
        echo "    ERROR: nothing replicated within 20s — check: docker logs ${B_NAME}"; return 1
    fi

    echo
    echo "[3] the SAME request through router-B (region B) — served locally, no node call"
    headers_from "$ROUTER_B_PORT" "$block_shared" | sed 's/^/    /'
    echo "    ^ 'Cached' + region B's address = the read came from the LOCAL replica"

    echo
    echo "[4] a DIFFERENT block (${block_b_only}) through router-B — writes go to region A"
    local before_a; before_a=$(keylist_on acli)
    relay_to "$ROUTER_B_PORT" "$block_b_only" >/dev/null || { echo "  ERROR: relay failed"; return 1; }
    # Name the key that appeared rather than comparing counts: identity is the
    # claim being made ("this write went to region A"), and a count cannot make it.
    local new_key=""
    for _ in $(seq 1 40); do
        new_key=$(comm -13 <(echo "$before_a") <(keylist_on acli) | head -1)
        [[ -n "$new_key" ]] && break
        sleep 0.5
    done
    if [[ -z "$new_key" ]]; then
        echo "    ERROR: no new entry appeared in region A within 20s"; return 1
    fi
    echo "    new entry in region A (the WRITE endpoint):"
    echo "      ${new_key}"
    echo "    region B never received a write — it is a read-only replica and gets"
    echo "    the entry by replication:"
    for _ in $(seq 1 40); do
        keylist_on bcli | grep -qF "$new_key" && break
        sleep 0.5
    done
    if keylist_on bcli | grep -qF "$new_key"; then
        echo "      ${new_key}  (replicated to region B)"
    else
        echo "      ERROR: it never replicated to region B"; return 1
    fi

    echo
    echo "============================================"
    echo "Reads local, writes central, no cross-region node calls."
    echo "Replication lag is safe by construction: an entry that has not arrived"
    echo "yet is a plain cache miss, and block-freshness validation runs on every"
    echo "hit — a lagging replica can never serve data older than the client saw."
    echo "============================================"
}

case "$1" in
    --stop|stop)     teardown; exit 0 ;;
    --status|status) status;   exit 0 ;;
    --demo|demo)     demo;     exit $? ;;
esac

command -v docker >/dev/null || { echo "ERROR: this lane needs docker"; exit 1; }
docker info >/dev/null 2>&1 || { echo "ERROR: docker is installed but not running"; exit 1; }
command -v screen >/dev/null || { echo "ERROR: this lane needs screen"; exit 1; }

LAN_IP="${LAN_IP:-$(detect_lan_ip)}"
[[ -n "$LAN_IP" ]] || { echo "ERROR: could not detect a host LAN address (needed for the replica->primary link). Pass LAN_IP=..."; exit 1; }

ETH_RPC_URL_1="${ETH_RPC_URL_1:-https://ethereum-rpc.publicnode.com}"
ETH_WS_URL_1="${ETH_WS_URL_1:-wss://ethereum-rpc.publicnode.com}"
ETH_RPC_URL_2="${ETH_RPC_URL_2:-https://eth.drpc.org}"
ETH_WS_URL_2="${ETH_WS_URL_2:-wss://eth.drpc.org}"

echo "============================================"
echo "Smart Router — UC-3 MULTI-REGION DEMO LANE"
echo "============================================"
echo "region A  redis 127.0.0.1:${A_PORT}   router-A :${ROUTER_A_PORT}  (writes + reads A)"
echo "region B  redis 127.0.0.1:${B_PORT}   router-B :${ROUTER_B_PORT}  (writes A, reads B)"
echo ""

echo "[Setup] reclaiming this lane's previous run (if any)"
reclaim_owned "$PID_A" "router-A"
reclaim_owned "$PID_B" "router-B"
screen -S "$A_SCREEN" -X quit 2>/dev/null || true
screen -S "$B_SCREEN" -X quit 2>/dev/null || true
rm_owned_container "$A_NAME"
rm_owned_container "$B_NAME"
sleep 1

BLOCKED=0
for port in "$ROUTER_A_PORT" "$METRICS_A_PORT" "$ROUTER_B_PORT" "$METRICS_B_PORT" "$A_PORT" "$B_PORT"; do
    if lsof -nP -iTCP:$port -sTCP:LISTEN >/dev/null 2>&1; then
        [[ "$BLOCKED" == "0" ]] && { echo ""; echo "ERROR: this lane's port(s) are held by process(es) it does not own."; echo "       Refusing to run. Nothing was signalled or removed."; }
        echo "  port $port:"; lsof -nP -iTCP:$port -sTCP:LISTEN | sed 's/^/    /'
        BLOCKED=1
    fi
done
[[ "$BLOCKED" == "1" ]] && { echo ""; echo "Free them, or move this lane with ROUTER_A_PORT / ROUTER_B_PORT / A_PORT / B_PORT."; exit 1; }

echo "[Setup] installing binaries"
make -C "$PROJECT_ROOT" install || { echo "ERROR: make install failed"; exit 1; }

# --- the two "regions" -------------------------------------------------------
echo "[Setup] starting region A (write endpoint) on 127.0.0.1:${A_PORT}"
docker run -d --name "$A_NAME" --label "$LANE_LABEL" \
    -p 0.0.0.0:${A_PORT}:6379 "$REDIS_IMAGE" \
    "$SERVER_BIN" --maxmemory 256mb --maxmemory-policy volatile-lru >/dev/null \
    || { echo "ERROR: region A failed to start"; exit 1; }

# The replica must reach the primary from INSIDE its container, where 127.0.0.1
# means itself — hence the host's LAN address here (and only here; both routers
# use plain loopback).
echo "[Setup] starting region B (local reader, replica of A) on 127.0.0.1:${B_PORT}"
docker run -d --name "$B_NAME" --label "$LANE_LABEL" \
    -p 0.0.0.0:${B_PORT}:6379 "$REDIS_IMAGE" \
    "$SERVER_BIN" --maxmemory 256mb --maxmemory-policy volatile-lru \
    --replicaof "$LAN_IP" "$A_PORT" >/dev/null \
    || { echo "ERROR: region B failed to start"; exit 1; }

for _ in $(seq 1 30); do acli ping >/dev/null 2>&1 && break; sleep 0.5; done
echo "  waiting for the replication link"
LINKED=0
for _ in $(seq 1 40); do
    if bcli info replication 2>/dev/null | grep -q "master_link_status:up"; then LINKED=1; break; fi
    sleep 0.5
done
[[ "$LINKED" == "1" ]] && echo "  replication: UP (A -> B)" || { echo "ERROR: region B never linked to A — docker logs $B_NAME"; exit 1; }

# --- configs -----------------------------------------------------------------
write_config() { # $1 = file, $2 = metrics port, $3 = listen port, $4 = read-addresses line (may be empty)
    cat > "$1" <<EOF
# GENERATED by scripts/pre_setups/init_smartrouter_eth_redis_multiregion.sh.

metrics-listen-address: "0.0.0.0:${2}"

resp-cache:
  # Writes always go to the region that owns the data.
  addresses: ["127.0.0.1:${A_PORT}"]
${4}  key-prefix: "${KEY_PREFIX}"

endpoints:
  - listen-address: "0.0.0.0:${3}"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    network-address: "0.0.0.0:${3}"

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
}

echo "[Setup] generating both region configs"
write_config "${PROJECT_ROOT}/${CONFIG_A_REL}" "$METRICS_A_PORT" "$ROUTER_A_PORT" ""
# Region B is the whole point: reads are served by its LOCAL replica, writes
# still travel to region A.
write_config "${PROJECT_ROOT}/${CONFIG_B_REL}" "$METRICS_B_PORT" "$ROUTER_B_PORT" \
"  # Reads are served locally; this is the only line that differs from region A.
  read-addresses: [\"127.0.0.1:${B_PORT}\"]
"

# --- routers -----------------------------------------------------------------
start_router() { # $1 screen, $2 config, $3 log, $4 port, $5 label, $6 pidfile
    screen -d -m -S "$1" bash -c "cd $PROJECT_ROOT && source ~/.bashrc; smartrouter \
$2 \
--log-level debug \
--use-static-spec $PROJECT_ROOT/specs/ethereum.json \
--debug-relays \
--relays-health-interval ${HEALTH_INTERVAL} 2>&1 | tee $3"
    local ok=0
    for _ in $(seq 1 45); do
        grep -q "resp-cache backend configured" "$3" 2>/dev/null && { ok=1; break; }
        sleep 1
    done
    [[ "$ok" == "1" ]] || { echo "ERROR: $5 never reported its RESP backend:"; tail -6 "$3" | sed 's/^/  /'; return 1; }
    record_owned "$6" "$4" "$5" || return 1
    echo "  $5: UP"
}

echo "[Setup] starting router-A (region A)"
start_router "$A_SCREEN" "$CONFIG_A_REL" "$LOG_A" "$ROUTER_A_PORT" "router-A" "$PID_A" || exit 1
echo "[Setup] starting router-B (region B — reads local, writes to A)"
start_router "$B_SCREEN" "$CONFIG_B_REL" "$LOG_B" "$ROUTER_B_PORT" "router-B" "$PID_B" || exit 1

if [[ "$SKIP_SMOKE" != "1" ]]; then
    echo ""
    echo "[Smoke] both regions answer"
    ok=1
    for pair in "router-A:${ROUTER_A_PORT}" "router-B:${ROUTER_B_PORT}"; do
        name="${pair%%:*}"; port="${pair##*:}"
        code=""
        for _ in $(seq 1 30); do
            code=$(curl -s -o /dev/null -w '%{http_code}' -m 10 "http://127.0.0.1:${port}/lava/health")
            [[ "$code" == "200" ]] && break
            sleep 1
        done
        printf '  %-10s /lava/health %s\n' "$name" "$code"
        [[ "$code" == "200" ]] || ok=0
    done
    [[ "$ok" == "1" ]] && echo "  SMOKE PASS" || { echo "  SMOKE FAIL — see $LOG_A / $LOG_B"; exit 1; }
fi

cat <<EOF

============================================
UC-3 MULTI-REGION — CHEAT SHEET
============================================
router-A (region A)  http://127.0.0.1:${ROUTER_A_PORT}   metrics :${METRICS_A_PORT}
router-B (region B)  http://127.0.0.1:${ROUTER_B_PORT}   metrics :${METRICS_B_PORT}
region A redis       127.0.0.1:${A_PORT}   (${A_NAME})   <- writes from BOTH routers
region B redis       127.0.0.1:${B_PORT}   (${B_NAME})   <- router-B reads here
Configs              ${CONFIG_A_REL}
                     ${CONFIG_B_REL}   (differs by ONE line: read-addresses)

Run the whole story:
  $0 --demo

Or by hand:
  # 1. cache it in region A
  curl -s -X POST http://127.0.0.1:${ROUTER_A_PORT} -H 'Content-Type: application/json' \\
    -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x5",false],"id":1}' >/dev/null
  # 2. watch it replicate
  docker exec ${B_NAME} ${CLI_BIN} --scan --pattern '${KEY_PREFIX}:rel:*'
  # 3. the same request in region B is a local cache hit
  curl -s -D- -o /dev/null -X POST http://127.0.0.1:${ROUTER_B_PORT} -H 'Content-Type: application/json' \\
    -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x5",false],"id":1}' \\
    | grep -iE 'lava-provider-address|lava-cache-backend'
    #  Lava-Provider-Address: Cached
    #  Lava-Cache-Backend: 127.0.0.1:${B_PORT}   <- the LOCAL replica served it

  $0 --status
Stop everything
  $0 --stop
============================================
EOF
