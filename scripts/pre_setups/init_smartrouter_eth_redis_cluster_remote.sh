#!/bin/bash
# Smart Router -> REMOTE Redis OSS 7.4 CLUSTER, 3 masters (the cache-test lab) — AUTH
#
# Redis OSS sibling of init_smartrouter_eth_valkey_cluster_remote.sh — same
# lane shape, fourth lab backend. It points a locally built router at the
# shared Redis OSS CLUSTER, so the resp-cache cluster topology is exercised
# against the classic engine: the client uses the configured addresses only as
# DISCOVERY SEEDS, learns the slot map, and talks to whichever master owns
# each key's hash slot.
#
#   redis cluster    <seed host>:7001-7003   (a remote Redis OSS 7.4, 3 masters,
#                    slots 0-5460 / 5461-10922 / 10923-16383; plain TCP + AUTH,
#                    no TLS. The nodes ANNOUNCE their public addresses — a bare
#                    GET without -c really answers "MOVED 14438
#                    <node>:7003" — which is what makes an internet
#                    client possible: discovery and redirects hand back
#                    addresses the router must be able to dial.)
#   smartrouter      0.0.0.0:${ROUTER_PORT}   (metrics :${METRICS_PORT})
#
# EVERYTHING talks straight to the remote cluster: the router dials it for
# caching, and the script's own checks (pre-flight, per-node key scans,
# --flush) use a local redis-cli/valkey-cli when one is installed or speak
# inline RESP over bash's /dev/tcp otherwise — no docker, no local backend.
# (For a TLS cluster, borrow the openssl s_client transport from the
# standalone lanes; this lab is deliberately plain TCP.)
#
# The password surface has NO flag form (flags carry only addresses and
# topology), so the lane GENERATES a config with a full resp-cache: block under
# git-ignored debugging/ — the credential never lands in the repo tree.
#
# CLUSTER SPECIFICS WORTH KNOWING
#   - Keys spray across the three masters by hash slot, so "did writes land"
#     is a PER-NODE question: the smoke scans every master and shows the
#     split. A handful of smoke keys may well land on only one or two masters
#     — that is hashing, not a failure; the assertion is total > 0.
#   - Lava-Cache-Backend names the node the client LAST dialed, so on a hit
#     expect ONE OF the three masters, not always :7001.
#   - SCAN is per-node, and multi-key DEL would CROSSSLOT — --flush therefore
#     scans each master and deletes its keys one by one on that same node.
#
# THE READ BUDGET AND THE WAN (why this lane passes --cache-timeout)
#   Cache READS run inside the per-relay --cache-timeout budget (default 50ms,
#   sized for a same-zone backend); a warm GET costs one round trip to the
#   owning master, so a budget below the RTT times out on EVERY lookup while
#   the asynchronous writes (5s budget) still land. The lane measures the RTT
#   and passes a budget sized to it (2*RTT + 150ms; CACHE_TIMEOUT overrides).
#   A hit still costs ~1 RTT — co-locate router and cluster in production and
#   keep the 50ms default.
#
# USAGE
#   scripts/pre_setups/init_smartrouter_eth_redis_cluster_remote.sh            # bring up + smoke
#   scripts/pre_setups/init_smartrouter_eth_redis_cluster_remote.sh --status   # cluster + router state
#   scripts/pre_setups/init_smartrouter_eth_redis_cluster_remote.sh --flush    # delete ONLY this lane's keys
#   scripts/pre_setups/init_smartrouter_eth_redis_cluster_remote.sh --stop     # stop the router
#
# ENVIRONMENT
#   REDIS_CLUSTER_SEEDS     comma-separated discovery seeds
#                           (host:7001,host:7002,host:7003)
#   REDIS_CLUSTER_PASSWORD  REQUIRED — the cache-test lab credential (never
#                           baked into the script; export it before running)
#   REDIS_CLUSTER_USERNAME  ACL user (default: none -> AUTH default user)
#   REDIS_CLUSTER_DIAL_TIMEOUT (1s) resp-cache dial-timeout (WAN-sized)
#   CACHE_TIMEOUT       per-relay cache lookup budget passed to the router as
#                       --cache-timeout (default: computed from measured RTT)
#   ROUTER_PORT (3378)  METRICS_PORT (7798)   <- clear of the sibling lanes
#                       (3377/7797 valkey-cluster, 3375-6/7795-6 the remote
#                       standalones, 3360/7779 resp, 3370/7790 sentinel,
#                       3380-1/7801-2 multiregion)
#   KEY_PREFIX (sr-$USER) per-user prefix so testers on the SHARED backend
#                       never scan or flush each other's entries
#   ETH_RPC_URL_1/2, ETH_WS_URL_1/2   upstreams     HEALTH_INTERVAL (15s)
#   SKIP_SMOKE=1        bring up only
#
# OWNERSHIP SAFETY: never `killall`. Reclaims only the router it recorded
# starting (pid + start-time fingerprint, or a command line naming this lane's
# generated config) and refuses to run if its ports are held by anything else.
# The lab cluster is shared infrastructure: this lane only ever touches keys
# under its own prefix, and only on --flush.

__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT=$(cd "${__dir}"/../.. && pwd)

LOGS_DIR="${PROJECT_ROOT}/debugging/logs"
mkdir -p "$LOGS_DIR"

REDIS_CLUSTER_SEEDS="${REDIS_CLUSTER_SEEDS:-}"
REDIS_CLUSTER_PASSWORD="${REDIS_CLUSTER_PASSWORD:-}"
REDIS_CLUSTER_USERNAME="${REDIS_CLUSTER_USERNAME:-}"
REDIS_CLUSTER_DIAL_TIMEOUT="${REDIS_CLUSTER_DIAL_TIMEOUT:-1s}"
CACHE_TIMEOUT="${CACHE_TIMEOUT:-}"   # empty -> sized from the measured RTT below
ROUTER_PORT="${ROUTER_PORT:-3378}"
METRICS_PORT="${METRICS_PORT:-7798}"
HEALTH_INTERVAL="${HEALTH_INTERVAL:-15s}"

IFS=',' read -r -a NODES <<< "$REDIS_CLUSTER_SEEDS"
SEED0="${NODES[0]}"

# Per-user prefix, restricted to the router's allowed charset [A-Za-z0-9._-].
KEY_PREFIX="${KEY_PREFIX:-sr-$(id -un 2>/dev/null)}"
KEY_PREFIX=$(printf '%s' "$KEY_PREFIX" | tr -cd 'A-Za-z0-9._-')
[[ -n "$KEY_PREFIX" ]] || KEY_PREFIX="sr-lab"

ROUTER_SCREEN="sr-redis-cluster-remote"
CONFIG_REL="debugging/smartrouter_eth_redis_cluster_remote.yml"
CONFIG_FILE="${PROJECT_ROOT}/${CONFIG_REL}"
PIDFILE="${PROJECT_ROOT}/debugging/.redis-cluster-remote-router.pid"
ROUTER_LOG="${LOGS_DIR}/SMARTROUTER_REDIS_CLUSTER_REMOTE.log"

# --- backend probe: a local CLI when installed, raw RESP over /dev/tcp else --
# Every probe goes STRAIGHT to a cluster node — no docker, no sidecars. Auth
# for a local CLI travels via environment, not -a, so the password never shows
# in ps; both names are exported because the two CLIs each read their own.
export REDISCLI_AUTH="$REDIS_CLUSTER_PASSWORD" VALKEYCLI_AUTH="$REDIS_CLUSTER_PASSWORD"
CLI_BIN=""
for c in redis-cli valkey-cli; do
    command -v "$c" >/dev/null 2>&1 && { CLI_BIN="$c"; break; }
done
CLI_USER_ARGS=()
[[ -n "$REDIS_CLUSTER_USERNAME" ]] && CLI_USER_ARGS=(--user "$REDIS_CLUSTER_USERNAME")
AUTH_CRED="${REDIS_CLUSTER_USERNAME:+${REDIS_CLUSTER_USERNAME} }${REDIS_CLUSTER_PASSWORD}"

resp_cmd() { # $1 = node host:port, $2 = ONE inline RESP command; raw reply stream
    local host="${1%%:*}" port="${1##*:}"
    if [[ -n "$CLI_BIN" ]]; then
        # shellcheck disable=SC2086 — the inline command is intentionally split
        "$CLI_BIN" -h "$host" -p "$port" "${CLI_USER_ARGS[@]}" $2
        return
    fi
    # This script is bash (shebang), so /dev/tcp is available even though the
    # surrounding interactive shell may be zsh. QUIT makes the node close the
    # connection, ending cat; a watchdog bounds a black-holed node.
    local pid wd
    ( exec 3<>"/dev/tcp/${host}/${port}" 2>/dev/null || exit 1
      printf 'AUTH %s\r\n%s\r\nQUIT\r\n' "$AUTH_CRED" "$2" >&3
      cat <&3 ) 2>/dev/null &
    pid=$!
    ( sleep 10; kill "$pid" 2>/dev/null ) >/dev/null 2>&1 &
    wd=$!
    wait "$pid" 2>/dev/null; local rc=$?
    kill "$wd" 2>/dev/null; wait "$wd" 2>/dev/null
    return $rc
}

node_keys() { # $1 = host:port -> this lane's keys living on THAT node (SCAN is per-node)
    if [[ -n "$CLI_BIN" ]]; then
        "$CLI_BIN" -h "${1%%:*}" -p "${1##*:}" "${CLI_USER_ARGS[@]}" --scan --pattern "${KEY_PREFIX}:*" 2>/dev/null
    else
        # One SCAN pass; COUNT 1000 covers the small lab keyspace, and every
        # caller polls, so a rare partial batch self-heals on the next try.
        resp_cmd "$1" "SCAN 0 MATCH ${KEY_PREFIX}:* COUNT 1000" | tr -d '\r' | grep "^${KEY_PREFIX}:"
    fi
}
backend_keys() { local n; for n in "${NODES[@]}"; do node_keys "$n"; done; }

# --- ownership helpers (house pattern: pid + start-time fingerprint) ---------
proc_fingerprint() { ps -p "$1" -o lstart= 2>/dev/null | tr -s ' '; }

reclaim_owned() {
    [[ -f "$PIDFILE" ]] || return 0
    local rec_pid rec_fp cur_fp
    rec_pid=$(cut -d'|' -f1 "$PIDFILE" 2>/dev/null)
    rec_fp=$(cut -d'|' -f2- "$PIDFILE" 2>/dev/null)
    [[ -n "$rec_pid" ]] || { rm -f "$PIDFILE"; return 0; }
    cur_fp=$(proc_fingerprint "$rec_pid")
    if [[ -z "$cur_fp" ]]; then rm -f "$PIDFILE"; return 0; fi
    if [[ "$cur_fp" != "$rec_fp" ]]; then
        echo "  stale pid file (pid $rec_pid is now another process) — leaving it alone"
        rm -f "$PIDFILE"; return 0
    fi
    echo "  stopping this lane's previous router (pid $rec_pid)"
    kill "$rec_pid" 2>/dev/null || true
    for _ in $(seq 1 20); do [[ -z "$(proc_fingerprint "$rec_pid")" ]] && break; sleep 0.25; done
    rm -f "$PIDFILE"
}

# Fallback when the pid file is missing (crash between spawn and record): the
# port holder's command line names a file only this lane generates — identity,
# not a name match.
reclaim_by_identity() {
    local pid
    pid=$(lsof -nP -iTCP:${ROUTER_PORT} -sTCP:LISTEN -t 2>/dev/null | head -1)
    [[ -n "$pid" ]] || return 0
    ps -p "$pid" -o command= 2>/dev/null | grep -qF -- "$CONFIG_REL" || return 0
    echo "  stopping this lane's router by identity (pid $pid, no pid file)"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
        lsof -nP -iTCP:${ROUTER_PORT} -sTCP:LISTEN -t >/dev/null 2>&1 || break
        sleep 0.25
    done
}

# One TCP connect = one network round trip; three samples against the first
# seed, keep the smallest positive. This decides the --cache-timeout below.
sample_rtt_ms() {
    local best="" t ms
    for _ in 1 2 3; do
        t=$(curl -s -o /dev/null -w '%{time_connect}' --connect-timeout 5 -m 2 \
              "telnet://${SEED0}" </dev/null 2>/dev/null)
        ms=$(awk -v t="${t:-0}" 'BEGIN{printf "%d", t*1000}')
        [[ "$ms" -gt 0 ]] && { [[ -z "$best" || "$ms" -lt "$best" ]] && best="$ms"; }
    done
    echo "$best"
}

backend_version() { # prints "redis 7.4.11 (cluster)" style, empty on failure
    # Judged by OUTPUT, not exit code (transport helpers exit non-zero on
    # peer-close races).
    local info v m
    info=$(resp_cmd "$SEED0" "INFO server" 2>/dev/null)
    [[ -n "$info" ]] || return 0
    v=$(echo "$info" | awk -F: '/^valkey_version/{print $2}' | tr -d '\r')
    [[ -n "$v" ]] && v="valkey $v" || v="redis $(echo "$info" | awk -F: '/^redis_version/{print $2}' | tr -d '\r')"
    # Valkey with extended-redis-compatibility off renames redis_mode to
    # server_mode — accept either (this lane GATES on the mode).
    m=$(echo "$info" | awk -F: '/^(redis|server)_mode/{print $2; exit}' | tr -d '\r')
    echo "${v}${m:+ (${m})}"
}

cluster_state() { resp_cmd "$SEED0" "CLUSTER INFO" 2>/dev/null | tr -d '\r' | awk -F: '/^cluster_state/{print $2}'; }

show_masters() { # indented "host:port slots" lines from the live topology
    resp_cmd "$SEED0" "CLUSTER NODES" 2>/dev/null | tr -d '\r' \
        | awk '/master/ {sub(/@.*/, "", $2); print "    " $2, $9}'
}

teardown() {
    echo "[Teardown] stopping this lane's router (nothing else is signalled)"
    reclaim_owned
    reclaim_by_identity
    screen -S "$ROUTER_SCREEN" -X quit 2>/dev/null || true
    echo "[Teardown] done. The lab cluster is untouched; this lane's keys expire"
    echo "           by TTL, or delete them now: $0 --flush"
    echo "           The generated config keeps the credential: rm -f ${CONFIG_REL}"
}

status() {
    echo "backend  ${REDIS_CLUSTER_SEEDS}"
    echo "         $(backend_version)  cluster_state:$(cluster_state)  ping: $(resp_cmd "$SEED0" PING | tr -d '\r' | grep -m1 -E '^\+?PONG$' || echo FAILED)"
    local n split=""
    for n in "${NODES[@]}"; do split+=":${n##*:}=$(node_keys "$n" | wc -l | tr -d ' ') "; done
    echo "keys     ${split}under '${KEY_PREFIX}:' (per master — SCAN is per-node)"
    local rtt; rtt=$(sample_rtt_ms)
    echo "rtt      ${rtt:-<unmeasurable>} ms  (the lane sizes --cache-timeout to this; production default 50ms)"
    if [[ -f "$PIDFILE" ]]; then
        local pid; pid=$(cut -d'|' -f1 "$PIDFILE")
        [[ -n "$(proc_fingerprint "$pid")" ]] && echo "router   UP (pid $pid) http://127.0.0.1:${ROUTER_PORT}" \
                                              || echo "router   recorded pid $pid is gone (crashed?) — re-run the lane"
    else
        echo "router   not recorded as running"
    fi
    curl -s -m 5 "http://127.0.0.1:${METRICS_PORT}/metrics" 2>/dev/null \
        | grep -E '^smartrouter_resp_cache_(connected|connection_errors_total|failed_total)' | sed 's/^/metric   /'
}

flush() {
    # SCAN is per-node and multi-key DEL would CROSSSLOT, so: scan each master
    # and delete ITS keys one by one on that same node — a single-key DEL on
    # the node that returned it can neither CROSSSLOT nor MOVED.
    local n k total=0
    for n in "${NODES[@]}"; do
        while IFS= read -r k; do
            [[ -n "$k" ]] || continue
            resp_cmd "$n" "DEL $k" >/dev/null
            total=$((total + 1))
        done < <(node_keys "$n")
    done
    [[ "$total" -gt 0 ]] && echo "deleted $total key(s) under '${KEY_PREFIX}:' across the masters (only this lane's prefix)" \
                         || echo "no keys under '${KEY_PREFIX}:' — nothing to delete"
}

# The remote seeds and credential are required and never hard-coded (secret
# scanners; shared infra). Every verb except --stop needs them to reach it.
if [[ "$1" != "--stop" && "$1" != "stop" ]] && { [[ -z "$REDIS_CLUSTER_SEEDS" || -z "$REDIS_CLUSTER_PASSWORD" ]]; }; then
    echo "ERROR: set REDIS_CLUSTER_SEEDS and REDIS_CLUSTER_PASSWORD (remote cluster seeds + credential), e.g.:"
    echo "  REDIS_CLUSTER_SEEDS=<host>:7001,<host>:7002,<host>:7003 REDIS_CLUSTER_PASSWORD='...' $0 ${1:-}"
    exit 1
fi

case "$1" in
    --stop|stop)     teardown; exit 0 ;;
    --status|status) status;   exit 0 ;;
    --flush|flush)   flush;    exit $? ;;
esac

command -v screen >/dev/null || { echo "ERROR: this lane needs screen"; exit 1; }
command -v curl   >/dev/null || { echo "ERROR: this lane needs curl"; exit 1; }

ETH_RPC_URL_1="${ETH_RPC_URL_1:-https://ethereum-rpc.publicnode.com}"
ETH_WS_URL_1="${ETH_WS_URL_1:-wss://ethereum-rpc.publicnode.com}"
ETH_RPC_URL_2="${ETH_RPC_URL_2:-https://eth.drpc.org}"
ETH_WS_URL_2="${ETH_WS_URL_2:-wss://eth.drpc.org}"

echo "============================================"
echo "Smart Router -> REMOTE Redis OSS CLUSTER, 3 masters (AUTH)"
echo "============================================"
echo "Seeds:    ${REDIS_CLUSTER_SEEDS}"
echo "Prefix:   ${KEY_PREFIX}:   (per-user isolation on the shared lab backend)"
echo ""

# --- pre-flight: prove the CLUSTER before spending a build on it -------------
# The router does NOT dial at construction and degrades per-operation, so a bad
# seed/password would still log "resp-cache backend configured" and the lane
# would look green while silently missing forever. Three hard gates: an
# authenticated PING, mode=cluster (someone pointing this lane at a standalone
# gets one clear line, not a baffling cluster-client failure), and
# cluster_state:ok (full slot coverage — a partial cluster serves errors).
echo "[Setup] pre-flight: authenticated ping + topology via ${CLI_BIN:-/dev/tcp}"
PROBE_OUT=$(resp_cmd "$SEED0" PING 2>&1 | tr -d '\r')
if ! echo "$PROBE_OUT" | grep -qE '^\+?PONG$'; then
    echo "ERROR: ${SEED0} did not answer an authenticated PING:"
    echo "       $(echo "${PROBE_OUT:-<no reply>}" | head -2 | tr '\n' ' ')"
    echo "       Check network/VPN, REDIS_CLUSTER_PASSWORD, or REDIS_CLUSTER_SEEDS."
    exit 1
fi
BACKEND_DESC=$(backend_version)
if [[ "$BACKEND_DESC" != *"(cluster)"* ]]; then
    echo "ERROR: ${SEED0} reports '${BACKEND_DESC:-<unknown>}' — not a cluster node."
    echo "       For a standalone backend use init_smartrouter_eth_redis_oss_remote.sh."
    exit 1
fi
STATE=$(cluster_state)
if [[ "$STATE" != "ok" ]]; then
    echo "ERROR: cluster_state is '${STATE:-<none>}' (want ok) — the cluster is not serving all slots."
    exit 1
fi
echo "  backend: ${BACKEND_DESC} — PONG, cluster_state:ok"
echo "  masters (discovered live; the config below carries only SEEDS):"
show_masters

RTT_MS=$(sample_rtt_ms)
if [[ -z "$CACHE_TIMEOUT" ]]; then
    # The lookup budget must exceed the backend RTT or every read times out —
    # writes would still land and the lane would look "up" while never serving
    # a single hit (the failure mode this lane exists to catch).
    if [[ -n "$RTT_MS" ]]; then CACHE_TIMEOUT="$(( 2 * RTT_MS + 150 ))ms"; else CACHE_TIMEOUT="750ms"; fi
fi
echo "  rtt: ~${RTT_MS:-?}ms -> --cache-timeout ${CACHE_TIMEOUT}"
echo "       (the production default, 50ms, suits a same-zone backend; a hit here"
echo "       still costs ~1 RTT — co-locate router and cluster in production)"
echo ""

echo "[Setup] reclaiming this lane's previous run (if any)"
reclaim_owned
reclaim_by_identity
screen -S "$ROUTER_SCREEN" -X quit 2>/dev/null || true
sleep 1

# Anything still on this lane's ports is NOT ours: refuse, never evict.
BLOCKED=0
for port in "$ROUTER_PORT" "$METRICS_PORT"; do
    if lsof -nP -iTCP:$port -sTCP:LISTEN >/dev/null 2>&1; then
        [[ "$BLOCKED" == "0" ]] && { echo "ERROR: this lane's port(s) are held by process(es) it does not own."; echo "       Refusing to run. Nothing was signalled."; }
        echo "  port $port:"; lsof -nP -iTCP:$port -sTCP:LISTEN | sed 's/^/    /'
        BLOCKED=1
    fi
done
[[ "$BLOCKED" == "1" ]] && { echo ""; echo "Free them, or move this lane: ROUTER_PORT=3379 METRICS_PORT=7799 $0"; exit 1; }

echo "[Setup] installing binaries"
# env -u GOFLAGS: when the calling shell exports GOFLAGS (any value), make
# auto-re-exports its own GOFLAGS := ... -ldflags '...' to the child go
# install, and go's $GOFLAGS env parser cannot read the quoted ldflags —
# "go: parsing $GOFLAGS: unknown flag -w". Scrubbing it keeps the Makefile's
# flags on argv only, where the quoting is legal.
env -u GOFLAGS make -C "$PROJECT_ROOT" install || { echo "ERROR: make install failed"; exit 1; }

# --- router config -----------------------------------------------------------
# Generated (not checked in) because it embeds the credential; debugging/ is
# git-ignored and the file is created 0600.
echo "[Setup] generating ${CONFIG_REL}"
umask 077
USERNAME_LINE=""
[[ -n "$REDIS_CLUSTER_USERNAME" ]] && USERNAME_LINE="  username: \"${REDIS_CLUSTER_USERNAME}\"
"
SEED_ADDRS=""
for n in "${NODES[@]}"; do
    [[ -n "$SEED_ADDRS" ]] && SEED_ADDRS+=", "
    SEED_ADDRS+="\"${n}\""
done

cat > "$CONFIG_FILE" <<EOF
# GENERATED by scripts/pre_setups/init_smartrouter_eth_redis_cluster_remote.sh.
# Contains a credential (the shared cache-test lab password) — this file lives
# in git-ignored debugging/ and must never be committed.

metrics-listen-address: "0.0.0.0:${METRICS_PORT}"

resp-cache:
  topology: cluster
  # DISCOVERY SEEDS, not the node list: the client asks these for the slot
  # map and then dials whatever the nodes ANNOUNCE. This lab announces its
  # public addresses (a bare GET really answers MOVED <slot> <public-addr>),
  # which is why an internet client works; a cluster that announces private
  # IPs is unreachable from outside no matter what is written here.
  # (db: is rejected under cluster — one logical database.)
  addresses: [${SEED_ADDRS}]
${USERNAME_LINE}  password: "${REDIS_CLUSTER_PASSWORD}"
  key-prefix: "${KEY_PREFIX}"
  # The repo default (500ms) is LAN-sized; a WAN dial pays 2-3 round trips,
  # and a too-small value makes the connected gauge flap.
  dial-timeout: "${REDIS_CLUSTER_DIAL_TIMEOUT}"
  # No tls: block — this lab speaks plain TCP + AUTH (see the header).

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
# If the router logs its backend line but the first cache op then fails oddly,
# suspect the go-redis streaming-credentials init for this client type before
# anything else — the failover client historically accepted the provider but
# never initialized its re-auth manager (see redisstore/config.go). Standalone
# and the Valkey cluster use the same provider and are proven by the sibling
# lanes.
echo "[Setup] starting the smart router (:${ROUTER_PORT}) against the cluster"
screen -d -m -S "$ROUTER_SCREEN" bash -c "cd $PROJECT_ROOT && source ~/.bashrc; smartrouter \
$CONFIG_REL \
--log-level debug \
--use-static-spec $PROJECT_ROOT/specs/ethereum.json \
--debug-relays \
--cache-timeout ${CACHE_TIMEOUT} \
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
echo "  router: UP (pid ${ROUTER_PID}) with the remote cluster backend"

# --- smoke -------------------------------------------------------------------
if [[ "$SKIP_SMOKE" != "1" ]]; then
    echo ""
    echo "[Smoke] router health -> relay -> writes across the masters -> cache hit"
    note() { printf '  %-34s %s\n' "$1" "$2"; }
    SMOKE_FAIL=0
    # LATEST-shaped relay to drive traffic; a fixed long-finalized block for a
    # deterministic key (same one on every run and every router).
    RELAY_TIP='{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'
    RELAY_FIXED='{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x112a880",false],"id":1}'
    relay() { curl -sf -m 20 -X POST "http://127.0.0.1:${ROUTER_PORT}" -H 'Content-Type: application/json' -d "$1"; }
    hits() { curl -sf "http://127.0.0.1:${METRICS_PORT}/metrics" | awk '/^smartrouter_cache_success_total/ {sum += $NF} END {printf "%d", sum}'; }

    HEALTH=""
    for _ in $(seq 1 30); do
        HEALTH=$(curl -s -o /dev/null -w '%{http_code}' -m 10 "http://127.0.0.1:${ROUTER_PORT}/lava/health")
        [[ "$HEALTH" == "200" ]] && break
        sleep 1
    done
    [[ "$HEALTH" == "200" ]] && note "router /lava/health" "200" || { note "router /lava/health" "$HEALTH (want 200)"; SMOKE_FAIL=1; }

    # The gauge is driven by a periodic probe (3s budget — WAN-tolerant); give
    # it a couple of cycles rather than sampling once.
    CONNECTED=""
    for _ in $(seq 1 30); do
        CONNECTED=$(curl -sf "http://127.0.0.1:${METRICS_PORT}/metrics" | awk '/^smartrouter_resp_cache_connected/ {print $NF; exit}')
        [[ "$CONNECTED" == "1" ]] && break
        sleep 1
    done
    [[ "$CONNECTED" == "1" ]] && note "resp_cache_connected" "1" || { note "resp_cache_connected" "${CONNECTED:-<none>} (want 1)"; SMOKE_FAIL=1; }

    BODY=$(relay "$RELAY_FIXED" || true)
    [[ "$BODY" == *'"result"'* ]] && note "relay eth_getBlockByNumber" "ok" \
        || { note "relay" "unexpected: ${BODY:0:60}"; SMOKE_FAIL=1; }

    # Writes are asynchronous and spray across the masters by hash slot —
    # poll the TOTAL while driving more traffic, then show the split. A small
    # sample may land on one or two masters only; that is hashing, not a bug.
    KEYS=0
    for _ in $(seq 1 20); do
        relay "$RELAY_TIP" >/dev/null 2>&1 || true
        KEYS=$(backend_keys | wc -l | tr -d ' ')
        [[ "$KEYS" -gt 0 ]] && break
        sleep 1
    done
    if [[ "$KEYS" -gt 0 ]]; then
        SPLIT=""
        for n in "${NODES[@]}"; do SPLIT+=":${n##*:}=$(node_keys "$n" | wc -l | tr -d ' ') "; done
        note "keys across the masters" "$KEYS total (${SPLIT%% })"
    else
        note "keys across the masters" "none (writes should land from anywhere)"; SMOKE_FAIL=1
    fi

    HIT_OK=0
    for _ in $(seq 1 20); do
        relay "$RELAY_FIXED" >/dev/null 2>&1 || true
        sleep 0.5
        [[ "$(hits)" -gt 0 ]] && { HIT_OK=1; break; }
    done
    if [[ "$HIT_OK" == "1" ]]; then
        note "cache_success_total" "$(hits) (>0)"
        HDR=$(curl -s -D - -o /dev/null -m 20 -X POST "http://127.0.0.1:${ROUTER_PORT}" -H 'Content-Type: application/json' -d "$RELAY_FIXED" | grep -i '^Lava-Cache-Backend' | tr -d '\r')
        [[ -n "$HDR" ]] && note "hit served by" "${HDR#*: } (one of the masters — last dialed)"
    else
        # connected=1 + keys present + zero hits means lookups still time out:
        # compare --cache-timeout against the RTT and watch the get-timeout
        # counter (smartrouter_resp_cache_failed_total) before suspecting auth.
        note "cache_success_total" "0 (expected >0 with --cache-timeout ${CACHE_TIMEOUT} at ~${RTT_MS:-?}ms RTT)"
        SMOKE_FAIL=1
    fi

    echo ""
    [[ "$SMOKE_FAIL" == "0" ]] && echo "  SMOKE PASS — the router is spraying entries across the remote Redis cluster." \
        || { echo "  SMOKE FAIL — see $ROUTER_LOG"; exit 1; }
fi

# The 'rc' helper the samples below rely on — a FUNCTION taking a node
# host:port first (SCAN/TTL are per-node in a cluster), then the command.
# Named 'rc' so it coexists with the sibling lanes' vk/rd/vc in one shell.
# The raw variant wraps /dev/tcp in `bash -c` so it also works when pasted
# into zsh (macOS default), where /dev/tcp does not exist.
if [[ -n "$CLI_BIN" ]]; then
    RC_SETUP="export REDISCLI_AUTH='${REDIS_CLUSTER_PASSWORD}' VALKEYCLI_AUTH=\"\$REDISCLI_AUTH\"
  rc() { local hp=\"\$1\"; shift; ${CLI_BIN} -h \"\${hp%%:*}\" -p \"\${hp##*:}\"${REDIS_CLUSTER_USERNAME:+ --user ${REDIS_CLUSTER_USERNAME}} \"\$@\"; }"
else
    RC_SETUP="rc() { local hp=\"\$1\"; shift; bash -c 'exec 3<>\"/dev/tcp/\$0/\$1\"; printf \"AUTH ${AUTH_CRED}\\r\\n%s\\r\\nQUIT\\r\\n\" \"\$2\" >&3; cat <&3' \"\${hp%%:*}\" \"\${hp##*:}\" \"\$*\" 2>/dev/null; }"
fi

cat <<EOF

============================================
REMOTE REDIS OSS CLUSTER LANE — CHEAT SHEET
============================================
Router    http://127.0.0.1:${ROUTER_PORT}    metrics http://127.0.0.1:${METRICS_PORT}/metrics
Cluster   ${REDIS_CLUSTER_SEEDS}  (AUTH, plain TCP)
Config    ${CONFIG_REL}   (holds the credential — git-ignored)
Log       tail -f ${ROUTER_LOG}

One-time in your shell (paste as-is), then use 'rc <node> <COMMAND>':
  ${RC_SETUP}

--- PROVE THE CLUSTER CACHE IS WORKING -----

1. See the topology the router discovers (masters + slot ranges):
     rc ${SEED0} CLUSTER NODES | awk '/master/ {print \$2, \$9}'

2. Warm the cache — relay a fixed, long-finalized block (deterministic key):
     curl -s -X POST http://127.0.0.1:${ROUTER_PORT} -H 'Content-Type: application/json' \\
       -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x112a880",false],"id":1}' | head -c 120; echo

3. Find where entries landed — SCAN is PER NODE, so ask each master; the
   split across them is the cluster working:
     for n in ${NODES[*]##*:}; do echo ":\$n"; rc ${SEED0%%:*}:\$n SCAN 0 MATCH '${KEY_PREFIX}:*' COUNT 1000; done
     rc <node-that-has-it> TTL '<paste a key>'

4. Repeat the SAME relay and look at the headers — a hit is served as
   "Cached"; Lava-Cache-Backend names the LAST-DIALED node, so expect ONE OF
   the masters (not always :${SEED0##*:}):
     curl -s -D - -o /dev/null -X POST http://127.0.0.1:${ROUTER_PORT} -H 'Content-Type: application/json' \\
       -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x112a880",false],"id":1}' \\
       | grep -iE 'lava-(provider-address|cache-backend)'
     # (hits work even far from the cluster — the lane raised the lookup
     #  budget with --cache-timeout ${CACHE_TIMEOUT}; the production default is 50ms)

5. The counters move — success/requests per tier, plus backend health:
     curl -s http://127.0.0.1:${METRICS_PORT}/metrics | grep -E '^smartrouter_cache_(success|requests)_total'
     curl -s http://127.0.0.1:${METRICS_PORT}/metrics | grep '^smartrouter_resp_cache'

6. Watch the router decide, live:
     tail -f ${ROUTER_LOG} | grep -iE 'cache (lookup|hit|miss)|resp-cache'

--------------------------------------------
Status (cluster state, per-master keys, RTT):      $0 --status
Clean up this lane's keys (never anyone else's):   $0 --flush
Stop the router:                                   $0 --stop
============================================
EOF
