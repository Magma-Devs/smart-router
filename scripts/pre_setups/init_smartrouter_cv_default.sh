#!/bin/bash
# Cross-Validation — DEFAULT LANE (PRD UC-7: no cross-validation configuration)
#
# The other lane (init_smartrouter_cv_demo.sh) shows what the feature does. This
# one shows what a deployment that never adopts it does: the config below has NO
# `cross-validation:` block, NO `group-label:` on any provider, and no threshold
# of any kind. That is today's shape, and it must keep behaving exactly as it did
# before per-method policies existed — the caller's request headers remain the
# only way to ask for cross-validation.
#
# It is a separate lane rather than a flag on the other one because the thing
# under test IS the absence of configuration: a router loaded with policies can
# never demonstrate it, whatever request you send.
#
# USAGE
#   scripts/pre_setups/init_smartrouter_cv_default.sh          # bring it up + check
#   scripts/pre_setups/init_smartrouter_cv_default.sh --check  # re-run the checks
#   scripts/pre_setups/init_smartrouter_cv_default.sh --status
#   scripts/pre_setups/init_smartrouter_cv_default.sh --stop
#
# ENVIRONMENT
#   SIM_DIR         provider_simulator checkout (searched for by default)
#   ROUTER_PORT (3399)  METRICS_PORT (7793)   SKIP_CHECK=1
#
# OWNERSHIP SAFETY: same contract as the other lanes — no `killall`, reclaims only
# a process it recorded starting, refuses to run on ports it does not own.

__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT=$(cd "${__dir}"/../.. && pwd)
source "$__dir"/../useful_commands.sh

LOGS_DIR="${PROJECT_ROOT}/debugging/logs"
mkdir -p "$LOGS_DIR"

ROUTER_PORT="${ROUTER_PORT:-3399}"
METRICS_PORT="${METRICS_PORT:-7793}"
ROUTER_SCREEN="sr-cv-default"
CONFIG_REL="debugging/smartrouter_cv_default.yml"
CONFIG_FILE="${PROJECT_ROOT}/${CONFIG_REL}"
PIDFILE="${PROJECT_ROOT}/debugging/.cv-default-router.pid"
ROUTER_LOG="${LOGS_DIR}/SMARTROUTER_CV_DEFAULT.log"
SIM_LOG="${LOGS_DIR}/CV_DEFAULT_SIM.log"
SPEC_FILE="${PROJECT_ROOT}/specs/ethereum.json"

SIM_CONTROL="127.0.0.1:19000"
SIM_PORTS="18545 18546 18547"
BLOCK="0x1000000"
METHOD="eth_getBalance"       # the method the OTHER lane mandates a policy on

HDR=""

# --- ownership (see init_smartrouter_cv_demo.sh for the rationale) ------------
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

record_owned() {
    local pidfile="$1" port="$2" what="$3" pid=""
    for _ in $(seq 1 60); do
        pid=$(lsof -nP -iTCP:${port} -sTCP:LISTEN -t 2>/dev/null | head -1)
        [[ -n "$pid" ]] && break
        sleep 1
    done
    [[ -n "$pid" ]] || { echo "ERROR: the $what never bound :${port}"; return 1; }
    printf '%s|%s\n' "$pid" "$(proc_fingerprint "$pid")" > "$pidfile"
    echo "  $what pid ${pid} recorded"
}

reclaim_by_identity() {
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
    screen -S "$ROUTER_SCREEN" -X quit >/dev/null 2>&1 || true
    echo "  the provider_simulator is shared infrastructure — left running"
    echo "[Teardown] done"
}

sim_up() { curl -s --max-time 2 "http://$SIM_CONTROL/health" >/dev/null 2>&1; }
sim_reset() { curl -s -X POST "http://$SIM_CONTROL/reset/all" >/dev/null 2>&1; }

PASS=0; FAIL=0
pass() { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }
check() { if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1 (got '$2', want '$3')"; fi; }
note() { printf '    %-38s %s\n' "$1" "$2"; }

call() {
    local method="$1" params="$2"; shift 2
    curl -s -m 40 -D "$HDR" -X POST "http://127.0.0.1:${ROUTER_PORT}" \
        -H 'Content-Type: application/json' "$@" \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"${method}\",\"params\":${params},\"id\":1}"
}
addr() { printf '0x%040x' "$1"; }
hdr() { grep -i "^lava-cross-validation-$1:" "$HDR" | head -1 | tr -d '\r' | cut -d' ' -f2-; }
cv_present() { grep -qi '^lava-cross-validation-' "$HDR"; }
count_csv() { printf '%s' "$1" | tr ',' '\n' | grep -c '[^[:space:]]' | tr -d ' '; }
router_up() { curl -s -o /dev/null -m 5 "http://127.0.0.1:${ROUTER_PORT}/lava/health" 2>/dev/null; }

write_config() {
    {
        cat <<EOF
# GENERATED by scripts/pre_setups/init_smartrouter_cv_default.sh — do not hand-edit.
#
# UC-7: the pre-feature shape. Note what is NOT here — no cross-validation:
# block, no group-label: on any provider, no thresholds. Everything below is
# config that existed before per-method policies did.
endpoints:
  - listen-address: "0.0.0.0:${ROUTER_PORT}"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    network-address: "0.0.0.0:${ROUTER_PORT}"

direct-rpc:
EOF
        local i=0 port
        for port in $SIM_PORTS; do
            i=$((i + 1))
            cat <<EOF
  - name: "sim-$i"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    node-urls:
      - url: "http://127.0.0.1:$port"
        timeout: 10s
        skip-verifications: [chain-id, pruning]
EOF
        done
    } > "$CONFIG_FILE"
}

checks() {
    echo ""
    echo "============================================"
    echo "UC-7 — no cross-validation configuration"
    echo "============================================"
    sim_reset

    echo ""
    echo "[1] the startup log carries no policy layout at all"
    if grep -q "cross-validation per-method policies loaded" "$ROUTER_LOG" 2>/dev/null; then
        fail "a policy-layout line was logged — this config has no policies"
    else
        pass "absent, not 'policies=0' — the resolver was never populated"
    fi

    echo ""
    echo "[2] '$METHOD' — the method the demo lane MANDATES — is plain here"
    call "$METHOD" "[\"$(addr 71)\",\"$BLOCK\"]" >/dev/null
    if cv_present; then
        fail "cross-validation happened with no policy and no caller headers"
        grep -i '^lava-cross-validation-' "$HDR" | tr -d '\r' | sed 's/^/        /'
    else
        pass "one provider, one relay — nothing changed for existing callers"
    fi

    echo ""
    echo "[3] a caller that DOES send the headers still gets cross-validation"
    call "$METHOD" "[\"$(addr 72)\",\"$BLOCK\"]" \
        -H 'lava-cross-validation-max-participants: 3' \
        -H 'lava-cross-validation-agreement-threshold: 2' >/dev/null
    note "status" "$(hdr status)"
    note "all-providers" "$(hdr all-providers)"
    note "agreeing-providers" "$(hdr agreeing-providers)"
    check "the header-driven path is untouched" "$(hdr status)" "success"
    check "the caller's fan-out was honoured" "$(count_csv "$(hdr all-providers)")" "3"

    echo ""
    echo "[4] providers with no group-label fold into the implicit 'default' group"
    echo "    — so a header-driven request never fails for lack of diversity"
    check "no diversity failure reason" "$(hdr failure-reason)" ""

    echo ""
    echo "[5] the degenerate caller request the docs warn about"
    echo "    (max-participants 1 + agreement-threshold 1: a 'quorum' of one)"
    call "$METHOD" "[\"$(addr 73)\",\"$BLOCK\"]" \
        -H 'lava-cross-validation-max-participants: 1' \
        -H 'lava-cross-validation-agreement-threshold: 1' >/dev/null
    note "status" "$(hdr status)"
    note "all-providers" "$(hdr all-providers)"
    note "agreeing-providers" "$(hdr agreeing-providers)"
    check "it reports success" "$(hdr status)" "success"
    check "having compared nothing — one provider queried" "$(count_csv "$(hdr all-providers)")" "1"
    echo "    A client that needs real corroboration must read the provider lists,"
    echo "    not 'status' alone. An operator policy is what forecloses this."

    echo ""
    echo "[6] the cross-validation counters only move for requests that asked for it"
    curl -s -m 10 "http://127.0.0.1:${METRICS_PORT}/metrics" 2>/dev/null \
        | grep '^smartrouter_cross_validation_requests_total' | sed 's/^/        /'
    if [[ "$(curl -s -m 10 "http://127.0.0.1:${METRICS_PORT}/metrics" 2>/dev/null \
             | grep -c '^smartrouter_cross_validation_mismatch_total')" == "0" ]]; then
        pass "no mismatch series — nothing was cross-validated by policy"
    else
        fail "a mismatch series exists on a router with no policies"
    fi

    echo ""
    if [[ "$FAIL" -eq 0 ]]; then
        echo "  UC-7 PASS — existing deployments and existing callers need zero changes."
    else
        echo "  UC-7 FAIL — $FAIL check(s) did not hold. Router left running for inspection."
        echo "  logs: $ROUTER_LOG"
    fi
}

bring_up() {
    echo "============================================"
    echo "Cross-Validation — DEFAULT LANE (UC-7)"
    echo "============================================"
    echo "  router     0.0.0.0:${ROUTER_PORT}   (ETH1 jsonrpc)"
    echo "  metrics    127.0.0.1:${METRICS_PORT}"
    echo "  upstreams  provider_simulator eth-sim 1-3, NO group labels"
    echo "  policies   none — this is the whole point"
    echo "============================================"
    echo ""

    for tool in curl lsof screen; do
        command_exists "$tool" || { echo "ERROR: '$tool' is required."; exit 1; }
    done
    [[ -f "$SPEC_FILE" ]] || { echo "ERROR: spec not found: $SPEC_FILE"; exit 1; }

    echo "[Setup] reclaiming this lane's previous run (if any)"
    reclaim_owned "$PIDFILE" "router"
    reclaim_by_identity "$ROUTER_PORT" "$CONFIG_REL" "router"
    screen -S "$ROUTER_SCREEN" -X quit >/dev/null 2>&1 || true
    sleep 1

    local blocked=0 port
    for port in $ROUTER_PORT $METRICS_PORT; do
        if lsof -nP -iTCP:$port -sTCP:LISTEN >/dev/null 2>&1; then
            if [[ "$blocked" == "0" ]]; then
                echo ""
                echo "ERROR: this lane's port(s) are held by process(es) it does not own."
                echo "       Refusing to run. Nothing was signalled or removed."
            fi
            echo "  port $port:"
            lsof -nP -iTCP:$port -sTCP:LISTEN | sed 's/^/    /'
            blocked=1
        fi
    done
    if [[ "$blocked" == "1" ]]; then
        echo ""
        echo "Run the lane on free ports instead:"
        echo "  ROUTER_PORT=3499 METRICS_PORT=7893 $0"
        exit 1
    fi

    echo "[Setup] installing binaries"
    make -C "$PROJECT_ROOT" install || { echo "ERROR: make install failed"; exit 1; }

    if [[ -z "$SIM_DIR" ]]; then
        local candidate
        for candidate in "$PROJECT_ROOT/.." "$PROJECT_ROOT/../.." "$PROJECT_ROOT/../../.."; do
            if [[ -f "$candidate/provider_simulator/run.py" ]]; then
                SIM_DIR=$(cd "$candidate/provider_simulator" && pwd); break
            fi
        done
        SIM_DIR="${SIM_DIR:-$(cd "$PROJECT_ROOT/.." && pwd)/provider_simulator}"
    fi
    echo ""
    if sim_up; then
        echo "[Setup] provider_simulator already running on $SIM_CONTROL (reusing it)"
    else
        echo "[Setup] starting provider_simulator from $SIM_DIR"
        [[ -f "$SIM_DIR/run.py" ]] || { echo "ERROR: $SIM_DIR/run.py not found. Set SIM_DIR=..."; exit 1; }
        local sim_py; sim_py="$(command -v python3.12 || command -v python3)"
        # Redirect the SUBSHELL too: a backgrounded child that inherits this
        # script's stdout holds the write end of the pipe, and `lane | tee` then
        # never sees EOF (see the same note in init_smartrouter_cv_demo.sh).
        ( cd "$SIM_DIR" && nohup "$sim_py" -u run.py > "$SIM_LOG" 2>&1 & ) >/dev/null 2>&1 </dev/null
        for _ in $(seq 1 30); do sim_up && break; sleep 1; done
        sim_up || { echo "ERROR: the simulator did not become ready. See $SIM_LOG"; exit 1; }
        echo "  simulator up (control $SIM_CONTROL, eth-sim on $SIM_PORTS)"
    fi
    sim_reset

    echo ""
    echo "[Setup] generating ${CONFIG_REL}"
    write_config
    echo "  $(wc -c < "$CONFIG_FILE" | tr -d ' ') bytes, 3 providers, no cross-validation block"

    echo ""
    echo "[Setup] starting the Smart Router (log -> $ROUTER_LOG)"
    screen -d -m -S "$ROUTER_SCREEN" bash -c "cd \"$PROJECT_ROOT\" && source ~/.bashrc; smartrouter \
$CONFIG_REL \
--log-level debug \
--use-static-spec \"$SPEC_FILE\" \
--metrics-listen-address ':$METRICS_PORT' \
--skip-websocket-verification 2>&1 | tee \"$ROUTER_LOG\"" && sleep 0.25

    echo "[Setup] waiting for the router ..."
    local ready=0
    for _ in $(seq 1 60); do
        if [[ "$(curl -s -o /dev/null -w '%{http_code}' -m 5 "http://127.0.0.1:${ROUTER_PORT}/lava/health")" == "200" ]]; then
            ready=1; break
        fi
        screen -list 2>/dev/null | grep -q "$ROUTER_SCREEN" || break
        sleep 1
    done
    [[ "$ready" == "1" ]] || {
        echo "ERROR: the router never became healthy. Tail of $ROUTER_LOG:"
        tail -n 30 "$ROUTER_LOG" 2>/dev/null | sed 's/^/    /'
        exit 1; }
    record_owned "$PIDFILE" "$ROUTER_PORT" "router" || exit 1
    echo "  router ready"
}

cheat_sheet() {
    cat <<EOF

============================================
Router is running — manual commands
============================================

Plain relay — no cross-validation, no headers on the way back:
  curl -si -X POST http://127.0.0.1:${ROUTER_PORT} -H 'Content-Type: application/json' \\
    -d '{"jsonrpc":"2.0","method":"${METHOD}","params":["0x00000000000000000000000000000000000000ff","${BLOCK}"],"id":1}' \\
    | grep -i '^lava-'

The same request, with the caller opting in as it always could:
  curl -si -X POST http://127.0.0.1:${ROUTER_PORT} -H 'Content-Type: application/json' \\
    -H 'lava-cross-validation-max-participants: 3' \\
    -H 'lava-cross-validation-agreement-threshold: 2' \\
    -d '{"jsonrpc":"2.0","method":"${METHOD}","params":["0x00000000000000000000000000000000000000fe","${BLOCK}"],"id":1}' \\
    | grep -i '^lava-cross-validation'

Diff this lane's config against the demo lane's to see exactly what the feature adds:
  diff debugging/smartrouter_cv_default.yml debugging/smartrouter_cv_demo.yml

Re-run the checks / teardown:
  $0 --check
  $0 --stop
============================================
EOF
}

HDR=$(mktemp)
trap 'rm -f "$HDR"' EXIT

case "${1:-}" in
    --stop)   teardown; exit 0 ;;
    --status)
        echo "router  :${ROUTER_PORT}   $(router_up && echo UP || echo DOWN)"
        echo "sim     ${SIM_CONTROL}  $(sim_up && echo UP || echo DOWN)"
        [[ -f "$PIDFILE" ]] && echo "owned router pid: $(cut -d'|' -f1 "$PIDFILE")"
        exit 0 ;;
    --check)
        router_up || { echo "ERROR: no router on :${ROUTER_PORT}. Bring the lane up first: $0"; exit 1; }
        checks ;;
    "")
        bring_up
        [[ "$SKIP_CHECK" == "1" ]] || checks
        cheat_sheet ;;
    *)
        echo "usage: $0 [--check|--status|--stop]"; exit 2 ;;
esac

[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1
