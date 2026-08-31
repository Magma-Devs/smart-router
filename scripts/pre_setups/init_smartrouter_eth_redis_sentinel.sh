#!/bin/bash
# Smart Router + Redis/Valkey SENTINEL — UC-2 DEMO LANE
#
# Companion to init_smartrouter_eth_redis_demo.sh. That lane runs one standalone
# backend; this one stands up a replicated topology with automatic failover and
# points a real router at it, so UC-2 can be *shown* — kill the primary, watch
# the sentinels promote the replica, watch the router keep serving without a
# restart.
#
#   primary     docker, announces ${LAN_IP}:63811   (AUTH)
#   replica     docker, announces ${LAN_IP}:63812   (replicaof primary)
#   sentinel x3 docker, 127.0.0.1:26390-2           (own AUTH — the control
#                                                    plane authenticates
#                                                    independently)
#   smartrouter  0.0.0.0:${ROUTER_PORT}    (metrics :${METRICS_PORT})
#
# USAGE
#   scripts/pre_setups/init_smartrouter_eth_redis_sentinel.sh             # bring up
#   scripts/pre_setups/init_smartrouter_eth_redis_sentinel.sh --failover  # the demo
#   scripts/pre_setups/init_smartrouter_eth_redis_sentinel.sh --status    # who is master
#   scripts/pre_setups/init_smartrouter_eth_redis_sentinel.sh --stop      # tear down
#
# WHY THE DATA NODES ANNOUNCE THE HOST'S LAN ADDRESS
# The router reaches the SENTINELS at plain 127.0.0.1:<published port> — that
# half needs nothing special, and the generated config says exactly that.
#
# The catch is what sentinel hands BACK. It does not proxy: it replies with the
# master's address and the client dials that itself. So the data-node address
# has to be valid from inside the docker network (sentinels monitor it) AND from
# the host (the router dials it). Neither obvious choice is:
#   - 127.0.0.1             fine on the host; inside a container it means itself
#   - host.docker.internal  fine in containers; does not resolve on macOS hosts
#   - container names       fine in containers; do not resolve on the host
# The host's own LAN address satisfies both, so the data nodes announce
# ${LAN_IP}:<port> and the router follows promotions with no address rewriting.
# (TestSentinelFailover instead injects a dialer that rewrites announced hosts —
# production code has no such hook, which is why this lane exists.)
#
# Want none of this? Run the router in a container on ${NETWORK} too; then every
# address is a container name and the announce flags disappear. It costs a router
# image build, which is why this lane keeps the router on the host like its
# sibling lanes.
#
# ENVIRONMENT
#   LAN_IP            override the auto-detected host address (VPNs, multi-NIC)
#   REDIS_IMAGE       valkey/valkey:7.2 (default) or redis:7.2-alpine
#   ROUTER_PORT (3370)  METRICS_PORT (7790)   <- deliberately NOT the standalone
#                                            lane's 3360/7779, so both can run at once
#   PRIMARY_PORT (63811) REPLICA_PORT (63812) SENTINEL_PORTS (26390,26391,26392)
#   DATA_PASSWORD / SENTINEL_PASSWORD   credentials for the two planes
#   ETH_RPC_URL_1/2, ETH_WS_URL_1/2     upstreams
#   SKIP_SMOKE=1
#
# Ports are deliberately clear of the docker acceptance drills (sentinel
# 63791/63792/26380-26382, cluster 7100-7102, tls 63794) so this lane and those
# tests can coexist.
#
# OWNERSHIP SAFETY: never `killall`. Reclaims only what it recorded starting
# (pid + start-time fingerprint, its own labelled containers) and refuses to run
# if its ports are held by anything else.

__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT=$(cd "${__dir}"/../.. && pwd)

LOGS_DIR="${PROJECT_ROOT}/debugging/logs"
mkdir -p "$LOGS_DIR"

REDIS_IMAGE="${REDIS_IMAGE:-valkey/valkey:7.2}"
PRIMARY_PORT="${PRIMARY_PORT:-63811}"
REPLICA_PORT="${REPLICA_PORT:-63812}"
IFS=',' read -r -a SENTINEL_PORTS <<< "${SENTINEL_PORTS:-26390,26391,26392}"
ROUTER_PORT="${ROUTER_PORT:-3370}"
METRICS_PORT="${METRICS_PORT:-7790}"
MASTER_NAME="${MASTER_NAME:-mymaster}"
DATA_PASSWORD="${DATA_PASSWORD:-datapass}"
SENTINEL_PASSWORD="${SENTINEL_PASSWORD:-sentinelpass}"
KEY_PREFIX="${KEY_PREFIX:-sr}"
HEALTH_INTERVAL="${HEALTH_INTERVAL:-15s}"

NETWORK="sr-demo-sentinel-net"
LANE_LABEL="sr-lane=sentinel-demo"
PRIMARY_NAME="sr-demo-primary"
REPLICA_NAME="sr-demo-replica"
sentinel_name() { echo "sr-demo-sentinel-$1"; }
ROUTER_SCREEN="sr-sentinel-demo"
CONFIG_REL="debugging/smartrouter_eth_redis_sentinel.yml"
CONFIG_FILE="${PROJECT_ROOT}/${CONFIG_REL}"
PW_FILE="${PROJECT_ROOT}/debugging/.sentinel-demo-data.pw"
SPW_FILE="${PROJECT_ROOT}/debugging/.sentinel-demo-control.pw"
PIDFILE="${PROJECT_ROOT}/debugging/.sentinel-demo-router.pid"
ROUTER_LOG="${LOGS_DIR}/SMARTROUTER_SENTINEL_DEMO.log"

# The image decides the binary names; both engines ship the same trio.
case "$REDIS_IMAGE" in
    *valkey*) SERVER_BIN="valkey-server"; SENTINEL_BIN="valkey-sentinel"; CLI_BIN="valkey-cli" ;;
    *)        SERVER_BIN="redis-server";  SENTINEL_BIN="redis-sentinel";  CLI_BIN="redis-cli"  ;;
esac

detect_lan_ip() {
    local ip
    for iface in en0 en1 en2; do
        ip=$(ipconfig getifaddr "$iface" 2>/dev/null) && [[ -n "$ip" ]] && { echo "$ip"; return 0; }
    done
    # Linux fallback
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
        echo "  stale pid file for the $what (pid $rec_pid is now another process) — leaving it alone"
        rm -f "$pidfile"; return 0
    fi
    echo "  stopping this lane's previous $what (pid $rec_pid)"
    kill "$rec_pid" 2>/dev/null || true
    for _ in $(seq 1 20); do [[ -z "$(proc_fingerprint "$rec_pid")" ]] && break; sleep 0.25; done
    rm -f "$pidfile"
}

# Remove a container only when it carries this lane's label.
rm_owned_container() {
    local name="$1"
    docker inspect "$name" >/dev/null 2>&1 || return 0
    if [[ "$(docker inspect --format '{{index .Config.Labels "sr-lane"}}' "$name" 2>/dev/null)" == "sentinel-demo" ]]; then
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
    reclaim_owned "$PIDFILE" "router"
    reclaim_by_identity "$ROUTER_PORT" "$CONFIG_REL" "router"
    screen -S "$ROUTER_SCREEN" -X quit 2>/dev/null || true
    rm_owned_container "$PRIMARY_NAME"
    rm_owned_container "$REPLICA_NAME"
    for i in "${!SENTINEL_PORTS[@]}"; do rm_owned_container "$(sentinel_name "$i")"; done
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
    rm -f "$PW_FILE" "$SPW_FILE"
    echo "[Teardown] done"
}

# Ask a sentinel who the current master is.
sentinel_master() {
    docker exec "$(sentinel_name 0)" "$CLI_BIN" -p "${SENTINEL_PORTS[0]}" -a "$SENTINEL_PASSWORD" --no-auth-warning \
        sentinel get-master-addr-by-name "$MASTER_NAME" 2>/dev/null | tr '\n' ':' | sed 's/:$//'
}

status() {
    local master; master=$(sentinel_master)
    echo "sentinel reports master: ${master:-<unavailable>}"

    # The announced host is baked into the containers at setup. If the laptop
    # has since moved networks its LAN address changed, every announced address
    # became unroutable, and the router can no longer follow the topology —
    # a failure that otherwise shows up mid-demo as an unreachable backend.
    local announced current
    announced="${master%%:*}"
    current="${LAN_IP:-$(detect_lan_ip 2>/dev/null)}"
    if [[ -n "$announced" && -n "$current" && "$announced" != "$current" && "$announced" != "127.0.0.1" ]]; then
        echo "  WARNING: this topology announces ${announced}, but this host is now ${current}."
        echo "           You have changed networks since the lane was started — re-run it:"
        echo "             $0"
    fi
    for name in "$PRIMARY_NAME" "$REPLICA_NAME"; do
        local role
        role=$(docker exec "$name" "$CLI_BIN" -p "$([[ $name == "$PRIMARY_NAME" ]] && echo "$PRIMARY_PORT" || echo "$REPLICA_PORT")" \
                 -a "$DATA_PASSWORD" --no-auth-warning info replication 2>/dev/null | awk -F: '/^role:/{print $2}' | tr -d '\r')
        printf '  %-18s %s\n' "$name" "${role:-<down>}"
    done
}

# The live UC-2 demo: kill the primary, watch the promotion, prove the router
# never stopped serving.
failover() {
    local relay='{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1",false],"id":1}'
    local before; before=$(sentinel_master)
    echo "master before:  ${before}"
    echo "relay before:   $(curl -s -m 15 -X POST "http://127.0.0.1:${ROUTER_PORT}" -H 'Content-Type: application/json' -d "$relay" | head -c 60)..."
    echo
    echo "[Failover] stopping the primary (${PRIMARY_NAME})"
    docker stop "$PRIMARY_NAME" >/dev/null

    echo "[Failover] waiting for the sentinels to promote the replica"
    local after=""
    for _ in $(seq 1 60); do
        after=$(sentinel_master)
        [[ -n "$after" && "$after" != "$before" ]] && break
        sleep 1
    done
    if [[ -z "$after" || "$after" == "$before" ]]; then
        echo "  ERROR: no promotion observed within 60s — inspect: docker logs $(sentinel_name 0)"
        return 1
    fi
    echo "  promoted: ${before}  ->  ${after}"
    echo
    echo "[Failover] the router was never restarted; relaying again"
    local body; body=$(curl -s -m 20 -X POST "http://127.0.0.1:${ROUTER_PORT}" -H 'Content-Type: application/json' -d "$relay")
    if [[ "$body" == *'"result"'* ]]; then
        echo "  relay after:    $(echo "$body" | head -c 60)..."
    else
        echo "  ERROR: relay failed after failover: ${body:0:120}"; return 1
    fi
    echo "  writes now land on the promoted node:"
    docker exec "$REPLICA_NAME" "$CLI_BIN" -p "$REPLICA_PORT" -a "$DATA_PASSWORD" --no-auth-warning \
        --scan --pattern "${KEY_PREFIX}:*" 2>/dev/null | head -5 | sed 's/^/    /'
    echo
    echo "  backend connectivity gauge:"
    curl -s "http://127.0.0.1:${METRICS_PORT}/metrics" | grep '^smartrouter_resp_cache_connected' | sed 's/^/    /'
    echo
    echo "Bring the old primary back (it rejoins as a REPLICA of the new master):"
    echo "  $0 --recover"
}

# Restart the node that was killed and prove the sentinels demote it to a
# replica of the current master — the half of failover people forget to check.
recover() {
    echo "[Recover] starting ${PRIMARY_NAME} again"
    docker start "$PRIMARY_NAME" >/dev/null 2>&1 || { echo "  ERROR: could not start ${PRIMARY_NAME} — re-run the lane"; return 1; }
    echo "[Recover] waiting for the sentinels to reconfigure it"
    local role=""
    for _ in $(seq 1 60); do
        role=$(docker exec "$PRIMARY_NAME" "$CLI_BIN" -p "$PRIMARY_PORT" -a "$DATA_PASSWORD" --no-auth-warning \
                 info replication 2>/dev/null | awk -F: '/^role:/{print $2}' | tr -d '\r')
        [[ "$role" == "slave" ]] && break
        sleep 1
    done
    if [[ "$role" == "slave" ]]; then
        echo "  ${PRIMARY_NAME} rejoined as a replica — no split brain, no manual step"
    else
        echo "  WARNING: role is '${role:-<unknown>}' after 60s; check: docker logs $(sentinel_name 0)"
    fi
    status
}

case "$1" in
    --stop|stop)         teardown; exit 0 ;;
    --status|status)     status;   exit 0 ;;
    --failover|failover) failover; exit $? ;;
    --recover|recover)   recover;  exit $? ;;
esac

command -v docker >/dev/null || { echo "ERROR: this lane needs docker"; exit 1; }
docker info >/dev/null 2>&1 || { echo "ERROR: docker is installed but not running"; exit 1; }
command -v screen >/dev/null || { echo "ERROR: this lane needs screen"; exit 1; }

LAN_IP="${LAN_IP:-$(detect_lan_ip)}"
if [[ -z "$LAN_IP" ]]; then
    echo "ERROR: could not detect a host LAN address, and sentinel needs one it can announce."
    echo "       Every node must advertise an address reachable BOTH from the host (the"
    echo "       router) and from inside docker (the sentinels). Pass it explicitly:"
    echo "         LAN_IP=192.168.1.20 $0"
    exit 1
fi

ETH_RPC_URL_1="${ETH_RPC_URL_1:-https://ethereum-rpc.publicnode.com}"
ETH_WS_URL_1="${ETH_WS_URL_1:-wss://ethereum-rpc.publicnode.com}"
ETH_RPC_URL_2="${ETH_RPC_URL_2:-https://eth.drpc.org}"
ETH_WS_URL_2="${ETH_WS_URL_2:-wss://eth.drpc.org}"

echo "============================================"
echo "Smart Router + Sentinel — UC-2 DEMO LANE"
echo "============================================"
echo "Announce address: ${LAN_IP}  (reachable from the host AND from containers)"
echo "Topology:         primary :${PRIMARY_PORT}  replica :${REPLICA_PORT}  sentinels :${SENTINEL_PORTS[*]}"
echo "Engine:           ${REDIS_IMAGE}"
echo ""

echo "[Setup] reclaiming this lane's previous run (if any)"
reclaim_owned "$PIDFILE" "router"
screen -S "$ROUTER_SCREEN" -X quit 2>/dev/null || true
rm_owned_container "$PRIMARY_NAME"
rm_owned_container "$REPLICA_NAME"
for i in "${!SENTINEL_PORTS[@]}"; do rm_owned_container "$(sentinel_name "$i")"; done
docker network rm "$NETWORK" >/dev/null 2>&1 || true
sleep 1

BLOCKED=0
for port in "$ROUTER_PORT" "$METRICS_PORT" "$PRIMARY_PORT" "$REPLICA_PORT" "${SENTINEL_PORTS[@]}"; do
    if lsof -nP -iTCP:$port -sTCP:LISTEN >/dev/null 2>&1; then
        if [[ "$BLOCKED" == "0" ]]; then
            echo ""
            echo "ERROR: this lane's port(s) are held by process(es) it does not own."
            echo "       Refusing to run. Nothing was signalled or removed."
        fi
        echo "  port $port:"; lsof -nP -iTCP:$port -sTCP:LISTEN | sed 's/^/    /'
        BLOCKED=1
    fi
done
[[ "$BLOCKED" == "1" ]] && { echo ""; echo "Free them, or move this lane: ROUTER_PORT=3372 METRICS_PORT=7792 $0"; exit 1; }

echo "[Setup] installing binaries"
make -C "$PROJECT_ROOT" install || { echo "ERROR: make install failed"; exit 1; }

docker network create "$NETWORK" >/dev/null 2>&1 || true

# --- data plane --------------------------------------------------------------
# Each node listens on its own port and ANNOUNCES ${LAN_IP}:<that port>, so the
# address sentinel hands the router is dialable from the host.
# NOTE: the data nodes deliberately run WITHOUT --rm. The demo kills the primary
# with `docker stop`, and a --rm container is deleted on stop — you could never
# bring it back to show it rejoining as a replica of the new master. Teardown
# removes them explicitly (docker rm -f handles stopped containers).
echo "[Setup] starting the primary (${LAN_IP}:${PRIMARY_PORT})"
docker run -d --name "$PRIMARY_NAME" --label "$LANE_LABEL" --net "$NETWORK" \
    -p 0.0.0.0:${PRIMARY_PORT}:${PRIMARY_PORT} \
    "$REDIS_IMAGE" "$SERVER_BIN" --port ${PRIMARY_PORT} \
        --requirepass "$DATA_PASSWORD" --masterauth "$DATA_PASSWORD" \
        --replica-announce-ip "$LAN_IP" --replica-announce-port ${PRIMARY_PORT} >/dev/null \
    || { echo "ERROR: primary failed to start"; exit 1; }

echo "[Setup] starting the replica (${LAN_IP}:${REPLICA_PORT})"
docker run -d --name "$REPLICA_NAME" --label "$LANE_LABEL" --net "$NETWORK" \
    -p 0.0.0.0:${REPLICA_PORT}:${REPLICA_PORT} \
    "$REDIS_IMAGE" "$SERVER_BIN" --port ${REPLICA_PORT} \
        --requirepass "$DATA_PASSWORD" --masterauth "$DATA_PASSWORD" \
        --replicaof "$LAN_IP" ${PRIMARY_PORT} \
        --replica-announce-ip "$LAN_IP" --replica-announce-port ${REPLICA_PORT} >/dev/null \
    || { echo "ERROR: replica failed to start"; exit 1; }

for _ in $(seq 1 30); do
    docker exec "$PRIMARY_NAME" "$CLI_BIN" -p ${PRIMARY_PORT} -a "$DATA_PASSWORD" --no-auth-warning ping >/dev/null 2>&1 && break
    sleep 0.5
done
echo "  waiting for replication to sync"
SYNCED=0
for _ in $(seq 1 40); do
    if docker exec "$PRIMARY_NAME" "$CLI_BIN" -p ${PRIMARY_PORT} -a "$DATA_PASSWORD" --no-auth-warning info replication 2>/dev/null | grep -q "connected_slaves:1"; then
        SYNCED=1; break
    fi
    sleep 0.5
done
[[ "$SYNCED" == "1" ]] && echo "  replication: OK" || { echo "ERROR: the replica never attached — docker logs $REPLICA_NAME"; exit 1; }

# --- control plane -----------------------------------------------------------
# The sentinels authenticate INDEPENDENTLY of the data nodes (requirepass here is
# the sentinel's own password; auth-pass is how it reaches the data nodes). A
# hardened deployment that forgets the former fails discovery before any data
# node is contacted — this lane models that shape on purpose.
echo "[Setup] starting ${#SENTINEL_PORTS[@]} sentinels"
for i in "${!SENTINEL_PORTS[@]}"; do
    port="${SENTINEL_PORTS[$i]}"
    docker run -d --rm --name "$(sentinel_name "$i")" --label "$LANE_LABEL" --net "$NETWORK" \
        -p 0.0.0.0:${port}:${port} \
        "$REDIS_IMAGE" sh -c "cat > /tmp/sentinel.conf <<CONF
port ${port}
requirepass ${SENTINEL_PASSWORD}
sentinel announce-ip ${LAN_IP}
sentinel announce-port ${port}
sentinel monitor ${MASTER_NAME} ${LAN_IP} ${PRIMARY_PORT} 2
sentinel auth-pass ${MASTER_NAME} ${DATA_PASSWORD}
sentinel down-after-milliseconds ${MASTER_NAME} 5000
sentinel failover-timeout ${MASTER_NAME} 10000
sentinel parallel-syncs ${MASTER_NAME} 1
CONF
exec ${SENTINEL_BIN} /tmp/sentinel.conf" >/dev/null \
        || { echo "ERROR: sentinel $i failed to start"; exit 1; }
done

echo "  waiting for the quorum to agree on the master"
MASTER=""
for _ in $(seq 1 40); do
    MASTER=$(sentinel_master)
    [[ "$MASTER" == "${LAN_IP}:${PRIMARY_PORT}" ]] && break
    sleep 1
done
[[ -n "$MASTER" ]] || { echo "ERROR: sentinels never reported a master — docker logs $(sentinel_name 0)"; exit 1; }
echo "  sentinels agree: master = ${MASTER}"

# --- credentials (file form: the rotation-capable one) -----------------------
umask 077
printf '%s' "$DATA_PASSWORD"     > "$PW_FILE"
printf '%s' "$SENTINEL_PASSWORD" > "$SPW_FILE"

# --- router config -----------------------------------------------------------
echo "[Setup] generating ${CONFIG_REL}"
# The router reaches the SENTINELS at their published ports, so plain loopback
# is correct here — no LAN address needed for this half. The LAN address is only
# required for what the sentinels ANNOUNCE (the data-node addresses they hand
# back), which must be valid from inside docker as well as on the host.
SENTINEL_ADDRS=""
for port in "${SENTINEL_PORTS[@]}"; do
    [[ -n "$SENTINEL_ADDRS" ]] && SENTINEL_ADDRS+=", "
    SENTINEL_ADDRS+="\"127.0.0.1:${port}\""
done

cat > "$CONFIG_FILE" <<EOF
# GENERATED by scripts/pre_setups/init_smartrouter_eth_redis_sentinel.sh.
# UC-2: the router talks to the SENTINELS, never to a data node directly. It
# discovers the primary through them and follows promotions transparently.

metrics-listen-address: "0.0.0.0:${METRICS_PORT}"

resp-cache:
  topology: sentinel
  # Sentinel addresses — NOT data nodes. The client asks these where the
  # master is and re-asks after every failover.
  addresses: [${SENTINEL_ADDRS}]
  master-name: "${MASTER_NAME}"
  key-prefix: "${KEY_PREFIX}"
  # Two INDEPENDENT credential domains. The first authenticates against the
  # data nodes, the second against the sentinels themselves — supplying only
  # the first is the classic misconfiguration and fails at discovery.
  password-file: "${PW_FILE}"
  sentinel-password-file: "${SPW_FILE}"

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

# --- router ------------------------------------------------------------------
echo "[Setup] starting the smart router (:${ROUTER_PORT})"
screen -d -m -S "$ROUTER_SCREEN" bash -c "cd $PROJECT_ROOT && source ~/.bashrc; smartrouter \
$CONFIG_REL \
--log-level debug \
--use-static-spec $PROJECT_ROOT/specs/ethereum.json \
--debug-relays \
--relays-health-interval ${HEALTH_INTERVAL} 2>&1 | tee $ROUTER_LOG"
sleep 0.5

ROUTER_OK=0
for _ in $(seq 1 45); do
    grep -q "resp-cache backend configured" "$ROUTER_LOG" 2>/dev/null && { ROUTER_OK=1; break; }
    grep -q "^Error:" "$ROUTER_LOG" 2>/dev/null && break
    sleep 1
done
if [[ "$ROUTER_OK" != "1" ]]; then
    echo "ERROR: the router never reported its RESP backend — last log lines:"
    tail -8 "$ROUTER_LOG" 2>/dev/null | sed 's/^/  /'
    exit 1
fi

ROUTER_PID=""
for _ in $(seq 1 60); do
    ROUTER_PID=$(lsof -nP -iTCP:${ROUTER_PORT} -sTCP:LISTEN -t 2>/dev/null | head -1)
    [[ -n "$ROUTER_PID" ]] && break
    sleep 1
done
[[ -n "$ROUTER_PID" ]] || { echo "ERROR: the router never bound :${ROUTER_PORT}"; tail -8 "$ROUTER_LOG" | sed 's/^/  /'; exit 1; }
printf '%s|%s\n' "$ROUTER_PID" "$(proc_fingerprint "$ROUTER_PID")" > "$PIDFILE"
echo "  router: UP (pid ${ROUTER_PID}) against the sentinel topology"

# --- smoke -------------------------------------------------------------------
if [[ "$SKIP_SMOKE" != "1" ]]; then
    echo ""
    echo "[Smoke] relay -> entry written through the sentinel-discovered primary"
    note() { printf '  %-32s %s\n' "$1" "$2"; }
    SMOKE_FAIL=0
    RELAY='{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1",false],"id":1}'

    HEALTH=""
    for _ in $(seq 1 30); do
        HEALTH=$(curl -s -o /dev/null -w '%{http_code}' -m 10 "http://127.0.0.1:${ROUTER_PORT}/lava/health")
        [[ "$HEALTH" == "200" ]] && break
        sleep 1
    done
    [[ "$HEALTH" == "200" ]] && note "router /lava/health" "200" || { note "router /lava/health" "$HEALTH (want 200)"; SMOKE_FAIL=1; }

    BODY=$(curl -sf -m 20 -X POST "http://127.0.0.1:${ROUTER_PORT}" -H 'Content-Type: application/json' -d "$RELAY" || true)
    [[ "$BODY" == *'"result"'* ]] && note "relay eth_getBlockByNumber" "ok" \
        || { note "relay" "unexpected: ${BODY:0:60}"; SMOKE_FAIL=1; }

    CONNECTED=$(curl -sf "http://127.0.0.1:${METRICS_PORT}/metrics" | awk '/^smartrouter_resp_cache_connected/ {print $NF; exit}')
    [[ "$CONNECTED" == "1" ]] && note "resp_cache_connected" "1" || { note "resp_cache_connected" "$CONNECTED (want 1)"; SMOKE_FAIL=1; }

    KEYS=0
    for _ in $(seq 1 20); do
        curl -sf -m 20 -X POST "http://127.0.0.1:${ROUTER_PORT}" -H 'Content-Type: application/json' -d "$RELAY" >/dev/null 2>&1 || true
        KEYS=$(docker exec "$PRIMARY_NAME" "$CLI_BIN" -p ${PRIMARY_PORT} -a "$DATA_PASSWORD" --no-auth-warning --scan --pattern "${KEY_PREFIX}:*" 2>/dev/null | wc -l | tr -d ' ')
        [[ "$KEYS" -gt 0 ]] && break
        sleep 1
    done
    [[ "$KEYS" -gt 0 ]] && note "keys on the primary" "$KEYS" || { note "keys on the primary" "none"; SMOKE_FAIL=1; }

    # Poll rather than sample once: replication is asynchronous, so a single
    # immediate read races it and would report a scary "none" that resolves a
    # second later — mid-demo that reads as a broken replica.
    REPLICATED=0
    for _ in $(seq 1 20); do
        REPLICATED=$(docker exec "$REPLICA_NAME" "$CLI_BIN" -p ${REPLICA_PORT} -a "$DATA_PASSWORD" --no-auth-warning --scan --pattern "${KEY_PREFIX}:*" 2>/dev/null | wc -l | tr -d ' ')
        [[ "$REPLICATED" -gt 0 ]] && break
        sleep 0.5
    done
    [[ "$REPLICATED" -gt 0 ]] && note "keys replicated to the replica" "$REPLICATED" \
        || { note "keys on the replica" "none after 10s — check: docker logs $REPLICA_NAME"; SMOKE_FAIL=1; }

    echo ""
    [[ "$SMOKE_FAIL" == "0" ]] && echo "  SMOKE PASS — sentinel topology is demo-ready." \
        || { echo "  SMOKE FAIL — see $ROUTER_LOG"; exit 1; }
fi

cat <<EOF

============================================
UC-2 SENTINEL DEMO — CHEAT SHEET
============================================
Router     http://127.0.0.1:${ROUTER_PORT}    metrics http://127.0.0.1:${METRICS_PORT}/metrics
Primary    ${LAN_IP}:${PRIMARY_PORT}   (${PRIMARY_NAME})
Replica    ${LAN_IP}:${REPLICA_PORT}   (${REPLICA_NAME})
Sentinels  ${LAN_IP}:$(IFS=,; echo "${SENTINEL_PORTS[*]}")
Config     ${CONFIG_REL}
Log        tail -f ${ROUTER_LOG}

1. Show the topology — the router holds NO data-node address, only sentinels
     grep -A6 'resp-cache' ${CONFIG_REL}
     $0 --status

2. Show data flowing through the sentinel-discovered primary
     docker exec ${PRIMARY_NAME} ${CLI_BIN} -p ${PRIMARY_PORT} -a ${DATA_PASSWORD} --no-auth-warning --scan --pattern '${KEY_PREFIX}:*'

3. THE DEMO — kill the primary, watch the promotion, keep serving
     $0 --failover

4. Bring the old primary back (it rejoins as a REPLICA of the new master)
     $0 --recover

Stop everything
  $0 --stop
============================================
EOF
