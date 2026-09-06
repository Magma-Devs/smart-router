#!/bin/bash
# Smart Router -> REMOTE Valkey 8.1 STANDALONE (the cache-test lab) — TLS + AUTH
#
# Companion to init_smartrouter_eth_resp_cache.sh (local docker valkey). This
# lane points a locally built router at the shared LAB backend instead, so the
# resp-cache path is exercised against a real remote standalone Valkey with the
# production-shaped surface: TLS on the wire, password AUTH, DNS endpoint.
#
#   valkey       $VALKEY_HOST:$VALKEY_PORT   (a remote Valkey 8.1
#                standalone — ElastiCache-style TLS endpoint behind an AWS NLB
#                in us-east-1; the cert does not verify against public roots
#                -> verification is skipped, same as redis-cli --insecure)
#   smartrouter  0.0.0.0:${ROUTER_PORT}   (metrics :${METRICS_PORT})
#
# EVERYTHING talks straight to that remote endpoint: the router dials it for
# caching, and the script's own checks (pre-flight ping, key scans, --flush)
# use a local valkey-cli/redis-cli when one is installed or plain openssl
# s_client speaking RESP otherwise. No docker, no local backend, no sidecars.
#
# The password + TLS surface has NO flag form (flags carry only addresses and
# topology), so the lane GENERATES a config with a full resp-cache: block under
# git-ignored debugging/ — the credential never lands in the repo tree.
#
# THE READ BUDGET AND THE WAN (why this lane passes --cache-timeout)
#   - Cache WRITES are asynchronous with a 5s budget (common.CacheWriteTimeout):
#     they land from anywhere. The smoke proves them by scanning the lane's
#     key prefix on the lab backend.
#   - Cache READS run inside the per-relay --cache-timeout budget (default
#     50ms, sized for a same-zone backend). A warm-connection GET costs one
#     network round trip, so a budget below the backend RTT turns EVERY lookup
#     into a timeout — smartrouter_resp_cache_failed_total{kind="timeout",
#     op="get"} climbs while writes still land, and relays are answered by the
#     upstream (Lava-Provider-Address: eth-rpc-N, never Cached).
#   - The lane therefore measures the RTT and passes a --cache-timeout sized
#     to it (2*RTT + 150ms headroom; CACHE_TIMEOUT overrides), so cache HITS
#     work from anywhere. The trade is honest: a hit saves the upstream call
#     but still costs ~1 RTT, and a miss now waits up to the budget before
#     falling through. Production should co-locate router and backend and
#     keep the 50ms default.
#
# USAGE
#   scripts/pre_setups/init_smartrouter_eth_valkey_remote.sh            # bring up + smoke
#   scripts/pre_setups/init_smartrouter_eth_valkey_remote.sh --status   # backend + router state
#   scripts/pre_setups/init_smartrouter_eth_valkey_remote.sh --flush    # delete ONLY this lane's keys
#   scripts/pre_setups/init_smartrouter_eth_valkey_remote.sh --stop     # stop the router
#
# ENVIRONMENT
#   VALKEY_HOST         REQUIRED — the remote Valkey host   VALKEY_PORT (6379)
#   VALKEY_PASSWORD     REQUIRED — the cache-test lab credential (never baked
#                       into the script; export it before running)
#   VALKEY_USERNAME     ACL user (default: none -> AUTH default user)
#   VALKEY_CA_FILE      PEM to VERIFY the server against; unset -> skip
#                       verification (the lab cert does not verify)
#   VALKEY_DIAL_TIMEOUT (1s) resp-cache dial-timeout. The repo default (500ms)
#                       is LAN-sized; a WAN dial pays TCP + TLS = 2-3 RTTs.
#   CACHE_TIMEOUT       per-relay cache lookup budget passed to the router as
#                       --cache-timeout (default: computed from measured RTT)
#   ROUTER_PORT (3375)  METRICS_PORT (7795)   <- clear of the sibling lanes
#                       (3360/7779 resp, 3370/7790 sentinel, 3380-1/7801-2 multiregion)
#   KEY_PREFIX (sr-$USER) per-user prefix so testers on the SHARED backend
#                       never scan or flush each other's entries
#   ETH_RPC_URL_1/2, ETH_WS_URL_1/2   upstreams     HEALTH_INTERVAL (15s)
#   SKIP_SMOKE=1        bring up only
#
# OWNERSHIP SAFETY: never `killall`. Reclaims only the router it recorded
# starting (pid + start-time fingerprint, or a command line naming this lane's
# generated config) and refuses to run if its ports are held by anything else.
# The lab backend is shared infrastructure: this lane only ever touches keys
# under its own prefix, and only on --flush.

__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT=$(cd "${__dir}"/../.. && pwd)

LOGS_DIR="${PROJECT_ROOT}/debugging/logs"
mkdir -p "$LOGS_DIR"

VALKEY_HOST="${VALKEY_HOST:-}"
VALKEY_PORT="${VALKEY_PORT:-6379}"
VALKEY_PASSWORD="${VALKEY_PASSWORD:-}"
VALKEY_USERNAME="${VALKEY_USERNAME:-}"
VALKEY_CA_FILE="${VALKEY_CA_FILE:-}"
VALKEY_DIAL_TIMEOUT="${VALKEY_DIAL_TIMEOUT:-1s}"
CACHE_TIMEOUT="${CACHE_TIMEOUT:-}"   # empty -> sized from the measured RTT below
ROUTER_PORT="${ROUTER_PORT:-3375}"
METRICS_PORT="${METRICS_PORT:-7795}"
HEALTH_INTERVAL="${HEALTH_INTERVAL:-15s}"

# Per-user prefix, restricted to the router's allowed charset [A-Za-z0-9._-].
KEY_PREFIX="${KEY_PREFIX:-sr-$(id -un 2>/dev/null)}"
KEY_PREFIX=$(printf '%s' "$KEY_PREFIX" | tr -cd 'A-Za-z0-9._-')
[[ -n "$KEY_PREFIX" ]] || KEY_PREFIX="sr-lab"

ROUTER_SCREEN="sr-valkey-remote"
CONFIG_REL="debugging/smartrouter_eth_valkey_remote.yml"
CONFIG_FILE="${PROJECT_ROOT}/${CONFIG_REL}"
PIDFILE="${PROJECT_ROOT}/debugging/.valkey-remote-router.pid"
ROUTER_LOG="${LOGS_DIR}/SMARTROUTER_VALKEY_REMOTE.log"

# --- backend probe: a local CLI when installed, raw RESP-over-TLS otherwise --
# Every probe goes STRAIGHT to the remote endpoint — no docker, no sidecars.
# Auth for a local valkey-cli/redis-cli travels via environment, not -a, so
# the password never shows in ps; both names are exported because the two CLIs
# each read their own variable.
export REDISCLI_AUTH="$VALKEY_PASSWORD" VALKEYCLI_AUTH="$VALKEY_PASSWORD"
CLI_BIN=""
for c in valkey-cli redis-cli; do
    command -v "$c" >/dev/null 2>&1 || continue
    "$c" --help 2>&1 | grep -q -- '--tls' && { CLI_BIN="$c"; break; }
done
CLI_ARGS=(--tls -h "$VALKEY_HOST" -p "$VALKEY_PORT")
if [[ -n "$VALKEY_CA_FILE" ]]; then CLI_ARGS+=(--cacert "$VALKEY_CA_FILE"); else CLI_ARGS+=(--insecure); fi
[[ -n "$VALKEY_USERNAME" ]] && CLI_ARGS+=(--user "$VALKEY_USERNAME")
rcli() { "$CLI_BIN" "${CLI_ARGS[@]}" "$@"; }

# Without a CLI the script speaks inline RESP over TLS itself through openssl
# s_client (present on macOS and Linux). -servername: ElastiCache-style
# endpoints route on SNI. QUIT makes the server close the stream, which ends
# s_client; a watchdog bounds a black-holed connection.
OPENSSL_TRUST=()
[[ -n "$VALKEY_CA_FILE" ]] && OPENSSL_TRUST=(-CAfile "$VALKEY_CA_FILE" -verify_return_error)
resp_cmd() { # $1 = ONE inline RESP command; prints the raw reply stream
    if [[ -n "$CLI_BIN" ]]; then
        # shellcheck disable=SC2086 — the inline command is intentionally split
        rcli $1
        return
    fi
    local pid wd
    printf 'AUTH %s\r\n%s\r\nQUIT\r\n' \
        "${VALKEY_USERNAME:+${VALKEY_USERNAME} }${VALKEY_PASSWORD}" "$1" \
        | openssl s_client -connect "${VALKEY_HOST}:${VALKEY_PORT}" \
              -servername "$VALKEY_HOST" "${OPENSSL_TRUST[@]}" -quiet -ign_eof 2>/dev/null &
    pid=$!
    ( sleep 10; kill "$pid" 2>/dev/null ) >/dev/null 2>&1 &
    wd=$!
    wait "$pid" 2>/dev/null; local rc=$?
    kill "$wd" 2>/dev/null; wait "$wd" 2>/dev/null
    return $rc
}
backend_keys() { # this lane's keys on the backend, one per line
    if [[ -n "$CLI_BIN" ]]; then
        rcli --scan --pattern "${KEY_PREFIX}:*" 2>/dev/null
    else
        # One SCAN pass; COUNT 1000 covers the small lab keyspace, and every
        # caller polls, so a rare partial batch self-heals on the next try.
        resp_cmd "SCAN 0 MATCH ${KEY_PREFIX}:* COUNT 1000" | tr -d '\r' | grep "^${KEY_PREFIX}:"
    fi
}
HAVE_PROBE=""
{ [[ -n "$CLI_BIN" ]] || command -v openssl >/dev/null 2>&1; } && HAVE_PROBE=1

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

# One TCP connect = one network round trip; three samples, keep the smallest
# positive. This is what decides whether cache HITS are physically possible
# inside the router's 50ms read budget.
sample_rtt_ms() {
    local best="" t ms
    for _ in 1 2 3; do
        t=$(curl -s -o /dev/null -w '%{time_connect}' --connect-timeout 5 -m 2 \
              "telnet://${VALKEY_HOST}:${VALKEY_PORT}" </dev/null 2>/dev/null)
        ms=$(awk -v t="${t:-0}" 'BEGIN{printf "%d", t*1000}')
        [[ "$ms" -gt 0 ]] && { [[ -z "$best" || "$ms" -lt "$best" ]] && best="$ms"; }
    done
    echo "$best"
}

backend_version() { # prints "valkey 8.1.0 (standalone)" style, empty on failure
    # Judged by OUTPUT, not exit code: s_client exits non-zero when the peer
    # TCP-closes without a TLS close_notify, as NLB-fronted endpoints do.
    local info v m
    info=$(resp_cmd "INFO server" 2>/dev/null)
    [[ -n "$info" ]] || return 0
    v=$(echo "$info" | awk -F: '/^valkey_version/{print $2}' | tr -d '\r')
    [[ -n "$v" ]] && v="valkey $v" || v="redis $(echo "$info" | awk -F: '/^redis_version/{print $2}' | tr -d '\r')"
    # Valkey with extended-redis-compatibility off renames redis_mode to
    # server_mode — accept either.
    m=$(echo "$info" | awk -F: '/^(redis|server)_mode/{print $2; exit}' | tr -d '\r')
    echo "${v}${m:+ (${m})}"
}

teardown() {
    echo "[Teardown] stopping this lane's router (nothing else is signalled)"
    reclaim_owned
    reclaim_by_identity
    screen -S "$ROUTER_SCREEN" -X quit 2>/dev/null || true
    echo "[Teardown] done. The lab backend is untouched; this lane's keys expire"
    echo "           by TTL, or delete them now: $0 --flush"
    echo "           The generated config keeps the credential: rm -f ${CONFIG_REL}"
}

status() {
    if [[ -n "$HAVE_PROBE" ]]; then
        echo "backend  ${VALKEY_HOST}:${VALKEY_PORT}  $(backend_version)  ping: $(resp_cmd PING | tr -d '\r' | grep -m1 -E '^\+?PONG$' || echo FAILED)"
        echo "keys     $(backend_keys | wc -l | tr -d ' ') under '${KEY_PREFIX}:'"
    else
        echo "backend  ${VALKEY_HOST}:${VALKEY_PORT}  (no valkey-cli/redis-cli and no openssl — cannot probe)"
    fi
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
    [[ -n "$HAVE_PROBE" ]] || { echo "ERROR: --flush needs valkey-cli/redis-cli or openssl"; exit 1; }
    local keys n
    keys=$(backend_keys)
    n=$(printf '%s' "$keys" | grep -c .)
    [[ "$n" -gt 0 ]] || { echo "no keys under '${KEY_PREFIX}:' — nothing to delete"; return 0; }
    echo "deleting $n key(s) under '${KEY_PREFIX}:' on ${VALKEY_HOST} (only this lane's prefix)"
    # One inline DEL carries them all — key names never contain spaces.
    resp_cmd "DEL $(echo "$keys" | tr '\n' ' ')" >/dev/null
    echo "done"
}

# The remote endpoint and credential are required and never hard-coded (secret
# scanners; shared infra). Every verb except --stop needs them to reach it.
if [[ "$1" != "--stop" && "$1" != "stop" ]] && { [[ -z "$VALKEY_HOST" || -z "$VALKEY_PASSWORD" ]]; }; then
    echo "ERROR: set VALKEY_HOST and VALKEY_PASSWORD (the remote Valkey endpoint + credential), e.g.:"
    echo "  VALKEY_HOST=<host> VALKEY_PASSWORD='...' $0 ${1:-}"
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
echo "Smart Router -> REMOTE Valkey standalone (TLS + AUTH)"
echo "============================================"
echo "Backend:  ${VALKEY_HOST}:${VALKEY_PORT}"
echo "Prefix:   ${KEY_PREFIX}:   (per-user isolation on the shared lab backend)"
echo ""

# --- pre-flight: prove the backend before spending a build on it -------------
# The router does NOT dial at construction and degrades per-operation, so a bad
# host/password would still log "resp-cache backend configured" and the lane
# would look green while silently missing forever. When a TLS-capable CLI is
# available, an explicit authenticated PING is therefore a HARD gate.
if [[ -n "$HAVE_PROBE" ]]; then
    echo "[Setup] pre-flight: authenticated TLS ping straight to the endpoint (via ${CLI_BIN:-openssl})"
    PROBE_OUT=$(resp_cmd PING 2>&1 | tr -d '\r')
    if ! echo "$PROBE_OUT" | grep -qE '^\+?PONG$'; then
        echo "ERROR: ${VALKEY_HOST}:${VALKEY_PORT} did not answer an authenticated PING:"
        echo "       $(echo "${PROBE_OUT:-<no reply>}" | head -2 | tr '\n' ' ')"
        echo "       Check network/VPN, VALKEY_PASSWORD, or point the lane elsewhere (VALKEY_HOST=...)."
        exit 1
    fi
    echo "  backend: $(backend_version) — PONG"
else
    echo "  WARNING: no valkey-cli/redis-cli and no openssl on PATH — skipping the"
    echo "           backend pre-flight and key-scan checks. The router's own gauge"
    echo "           (smartrouter_resp_cache_connected) becomes the only connectivity signal."
fi

RTT_MS=$(sample_rtt_ms)
if [[ -z "$CACHE_TIMEOUT" ]]; then
    # The lookup budget must exceed the backend RTT or every read times out —
    # writes would still land and the lane would look "up" while never serving
    # a single hit (the failure mode this lane exists to catch).
    if [[ -n "$RTT_MS" ]]; then CACHE_TIMEOUT="$(( 2 * RTT_MS + 150 ))ms"; else CACHE_TIMEOUT="750ms"; fi
fi
echo "  rtt: ~${RTT_MS:-?}ms -> --cache-timeout ${CACHE_TIMEOUT}"
echo "       (the production default, 50ms, suits a same-zone backend; a hit here"
echo "       still costs ~1 RTT — co-locate router and backend in production)"
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
[[ "$BLOCKED" == "1" ]] && { echo ""; echo "Free them, or move this lane: ROUTER_PORT=3376 METRICS_PORT=7796 $0"; exit 1; }

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
[[ -n "$VALKEY_USERNAME" ]] && USERNAME_LINE="  username: \"${VALKEY_USERNAME}\"
"
if [[ -n "$VALKEY_CA_FILE" ]]; then
    TLS_TRUST_LINES="    # Verify the server against the operator-supplied bundle.
    ca-file: \"${VALKEY_CA_FILE}\""
else
    TLS_TRUST_LINES="    # The lab endpoint's certificate does not verify against public roots
    # (redis-cli needs --insecure for the same reason). Supply VALKEY_CA_FILE
    # to verify instead. server-name / cert-file / key-file (mTLS) are also
    # available — docs/RESP-CACHE.md.
    insecure-skip-verify: true"
fi

cat > "$CONFIG_FILE" <<EOF
# GENERATED by scripts/pre_setups/init_smartrouter_eth_valkey_remote.sh.
# Contains a credential (the shared cache-test lab password) — this file lives
# in git-ignored debugging/ and must never be committed.

metrics-listen-address: "0.0.0.0:${METRICS_PORT}"

resp-cache:
  topology: standalone
  addresses: ["${VALKEY_HOST}:${VALKEY_PORT}"]
${USERNAME_LINE}  password: "${VALKEY_PASSWORD}"
  key-prefix: "${KEY_PREFIX}"
  # The repo default (500ms) is LAN-sized; a WAN dial pays TCP + TLS handshake,
  # 2-3 round trips, and a too-small value makes the connected gauge flap.
  dial-timeout: "${VALKEY_DIAL_TIMEOUT}"
  tls:
    enabled: true
${TLS_TRUST_LINES}

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
echo "[Setup] starting the smart router (:${ROUTER_PORT}) against ${VALKEY_HOST}"
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
echo "  router: UP (pid ${ROUTER_PID}) with the remote RESP backend"

# --- smoke -------------------------------------------------------------------
if [[ "$SKIP_SMOKE" != "1" ]]; then
    echo ""
    echo "[Smoke] router health -> relay -> writes on the lab backend -> cache hit"
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

    # Writes are asynchronous — poll while driving more traffic.
    if [[ -n "$HAVE_PROBE" ]]; then
        KEYS=0
        for _ in $(seq 1 20); do
            relay "$RELAY_TIP" >/dev/null 2>&1 || true
            KEYS=$(backend_keys | wc -l | tr -d ' ')
            [[ "$KEYS" -gt 0 ]] && break
            sleep 1
        done
        [[ "$KEYS" -gt 0 ]] && note "keys on the lab backend" "$KEYS under '${KEY_PREFIX}:'" \
            || { note "keys on the lab backend" "none (writes should land from anywhere)"; SMOKE_FAIL=1; }
    else
        note "keys on the lab backend" "skipped (no CLI and no openssl)"
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
        [[ -n "$HDR" ]] && note "hit served by" "${HDR#*: }"
    else
        # connected=1 + keys present + zero hits means lookups still time out:
        # compare --cache-timeout against the RTT and watch the get-timeout
        # counter (smartrouter_resp_cache_failed_total) before suspecting auth.
        note "cache_success_total" "0 (expected >0 with --cache-timeout ${CACHE_TIMEOUT} at ~${RTT_MS:-?}ms RTT)"
        SMOKE_FAIL=1
    fi

    echo ""
    [[ "$SMOKE_FAIL" == "0" ]] && echo "  SMOKE PASS — the router is talking TLS+AUTH to the remote Valkey." \
        || { echo "  SMOKE FAIL — see $ROUTER_LOG"; exit 1; }
fi

# The 'vk' helper the samples below rely on: an alias when a local CLI exists,
# otherwise a tiny function speaking RESP over TLS via openssl — either way it
# talks straight to the remote endpoint.
if [[ -n "$CLI_BIN" ]]; then
    VK_SETUP="export REDISCLI_AUTH='${VALKEY_PASSWORD}' VALKEYCLI_AUTH=\"\$REDISCLI_AUTH\"
  alias vk='${CLI_BIN} ${CLI_ARGS[*]}'"
else
    VK_SETUP="vk() { printf 'AUTH ${VALKEY_PASSWORD}\\r\\n%s\\r\\nQUIT\\r\\n' \"\$*\" | openssl s_client -connect ${VALKEY_HOST}:${VALKEY_PORT} -servername ${VALKEY_HOST} -quiet -ign_eof 2>/dev/null; }"
fi

cat <<EOF

============================================
REMOTE VALKEY LANE — CHEAT SHEET
============================================
Router    http://127.0.0.1:${ROUTER_PORT}    metrics http://127.0.0.1:${METRICS_PORT}/metrics
Backend   ${VALKEY_HOST}:${VALKEY_PORT}  (TLS + AUTH, standalone)
Config    ${CONFIG_REL}   (holds the credential — git-ignored)
Log       tail -f ${ROUTER_LOG}

One-time in your shell (paste as-is), then use 'vk <COMMAND>':
  ${VK_SETUP}

--- PROVE THE CACHE IS WORKING -------------

1. Warm it — relay a fixed, long-finalized block (deterministic cache key,
   same key on every run):
     curl -s -X POST http://127.0.0.1:${ROUTER_PORT} -H 'Content-Type: application/json' \\
       -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x112a880",false],"id":1}' | head -c 120; echo

2. The entry lands on the lab backend (write is async — give it a second),
   with a real TTL from the router's policy:
     vk SCAN 0 MATCH '${KEY_PREFIX}:*' COUNT 1000
     vk TTL '<paste one key from the scan>'

3. Repeat the SAME relay and look at the headers — a cache hit is served as
   "Cached" and Lava-Cache-Backend names the node that served it:
     curl -s -D - -o /dev/null -X POST http://127.0.0.1:${ROUTER_PORT} -H 'Content-Type: application/json' \\
       -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x112a880",false],"id":1}' \\
       | grep -iE 'lava-(provider-address|cache-backend)'
     # expect:  Lava-Provider-Address: Cached  +  Lava-Cache-Backend: ${VALKEY_HOST}:${VALKEY_PORT}
     # (works even far from the backend — the lane raised the router's lookup
     #  budget with --cache-timeout ${CACHE_TIMEOUT}; the production default is 50ms)

4. The counters move — success/requests per tier, plus backend health:
     curl -s http://127.0.0.1:${METRICS_PORT}/metrics | grep -E '^smartrouter_cache_(success|requests)_total'
     curl -s http://127.0.0.1:${METRICS_PORT}/metrics | grep '^smartrouter_resp_cache'

5. Watch the router decide, live:
     tail -f ${ROUTER_LOG} | grep -iE 'cache (lookup|hit|miss)|resp-cache'

--------------------------------------------
Status (backend ping, RTT, keys, gauges):          $0 --status
Clean up this lane's keys (never anyone else's):   $0 --flush
Stop the router:                                   $0 --stop
============================================
EOF
