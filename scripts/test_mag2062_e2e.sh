#!/usr/bin/env bash
#
# test_mag2062_e2e.sh — end-to-end check for the MAG-2062 cross-validation fix.
#
# MAG-2062: two providers returning the SAME semantic result but with their JSON
# envelope keys in a different order (e.g. {jsonrpc,id,result} vs {id,jsonrpc,result})
# were placed in separate quorum buckets, so cross-validation failed with HTTP 500
# even though the responses were semantically identical. The fix hashes a canonical
# form (sorted keys) so reordered envelopes land in the same bucket.
#
# This script reproduces that exact condition against a LIVE router + the sibling
# provider_simulator:
#   1. (optionally) launches the provider_simulator
#   2. pins two providers to the same result with DIFFERENT key order via /scenario
#   3. sends a request through the router and asserts CV SUCCEEDS  (the fix)
#   4. NEGATIVE CONTROL: pins a genuinely different result and asserts CV FAILS,
#      proving the header reflects real agreement and not a constant "success".
#
# It deliberately does NOT build or launch the router: the router config is
# environment-specific and the cross-validation config wiring may live on a
# different branch. Point it at a running, cross-validation-enabled router.
#
# Usage:
#   ROUTER_URL=http://127.0.0.1:3340 bash scripts/test_mag2062_e2e.sh --start-sim
#   ROUTER_URL=https://eth-sim.example.com bash scripts/test_mag2062_e2e.sh
#
# Environment variables:
#   ROUTER_URL    (required) base URL the router listens on for RPC requests.
#   CONTROL_URL   simulator control endpoint for /scenario. Default http://127.0.0.1:19000
#   SIM_DIR       path to the provider_simulator checkout (for --start-sim).
#                 Default: ../provider_simulator relative to this repo.
#   METHOD        JSON-RPC method to exercise. Default eth_chainId.
#                 IMPORTANT: must be a BLOCK-INDEPENDENT method. Block-height
#                 methods like eth_blockNumber feed the router's consistency
#                 tracker, so overriding them makes providers look stale and the
#                 consistency filter drops them before cross-validation runs
#                 ("insufficient sessions"). eth_chainId mirrors the ticket's
#                 own block-independent method (Solana getGenesisHash).
#   RESULT        the shared "result" value both providers return. Default "0x1"
#   ALT_RESULT    the divergent value for the negative control. Default "0x89"
#   CV_HEADER     response header carrying CV status. Default lava-cross-validation-status
#   ROUTER_PORT   port the --start-router router listens on. Default 3340
#   SPEC_DIR      spec directory for --use-static-spec. Default <repo>/specs
#
# Flags:
#   --start-sim      launch provider_simulator (python run.py) and tear it down on exit.
#   --start-router   build a router from THIS branch + a 2-provider sim config and
#                    launch it on ROUTER_PORT (sets ROUTER_URL automatically), torn
#                    down on exit. Use this to verify the fix without a prebuilt router.
#
# Exit status: 0 if both the positive and negative assertions pass, 1 otherwise.

set -euo pipefail

# ── configuration ─────────────────────────────────────────────────────────────
ROUTER_URL="${ROUTER_URL:-}"
CONTROL_URL="${CONTROL_URL:-http://127.0.0.1:19000}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SIM_DIR="${SIM_DIR:-$SCRIPT_DIR/../../provider_simulator}"
SPEC_DIR="${SPEC_DIR:-$REPO_ROOT/specs}"
ROUTER_PORT="${ROUTER_PORT:-3340}"
METHOD="${METHOD:-eth_chainId}"
RESULT="${RESULT:-0x1}"
ALT_RESULT="${ALT_RESULT:-0x89}"
CV_HEADER="${CV_HEADER:-lava-cross-validation-status}"
# Request directive headers that ACTIVATE cross-validation (both required).
MAX_PARTICIPANTS_HEADER="${MAX_PARTICIPANTS_HEADER:-lava-cross-validation-max-participants}"
AGREEMENT_THRESHOLD_HEADER="${AGREEMENT_THRESHOLD_HEADER:-lava-cross-validation-agreement-threshold}"
MAX_PARTICIPANTS="${MAX_PARTICIPANTS:-2}"
AGREEMENT_THRESHOLD="${AGREEMENT_THRESHOLD:-2}"
START_SIM=false
START_ROUTER=false

for arg in "$@"; do
    case "$arg" in
        --start-sim)    START_SIM=true ;;
        --start-router) START_ROUTER=true ;;
        -h|--help)      sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown argument: $arg" >&2; exit 2 ;;
    esac
done

# --start-router owns the router URL; otherwise the caller must supply one.
if [[ "$START_ROUTER" == true ]]; then
    ROUTER_URL="http://127.0.0.1:$ROUTER_PORT"
fi

if [[ -z "$ROUTER_URL" ]]; then
    echo "ERROR: ROUTER_URL is required (the router's RPC listen address)," >&2
    echo "       or pass --start-router to build & launch one from this branch." >&2
    echo "       e.g. ROUTER_URL=http://127.0.0.1:3340 bash $0" >&2
    echo "       e.g. bash $0 --start-sim --start-router" >&2
    exit 2
fi

# ── pretty output ─────────────────────────────────────────────────────────────
if [[ -t 1 ]]; then GREEN=$'\e[32m'; RED=$'\e[31m'; BOLD=$'\e[1m'; DIM=$'\e[2m'; RST=$'\e[0m'
else GREEN=; RED=; BOLD=; DIM=; RST=; fi

header() { printf '\n%s══ %s ══%s\n' "$BOLD" "$1" "$RST"; }
info()   { printf '%s   %s%s\n' "$DIM" "$1" "$RST"; }
pass()   { printf '%s ✓ PASS%s  %s\n' "$GREEN" "$RST" "$1"; }
fail()   { printf '%s ✗ FAIL%s  %s\n' "$RED" "$RST" "$1"; }

# ── process lifecycle (optional) ──────────────────────────────────────────────
SIM_PID=""
ROUTER_PID=""
ROUTER_CFG=""
ROUTER_BIN=""
ROUTER_LOG=""
# shellcheck disable=SC2329  # invoked indirectly via `trap cleanup EXIT`
cleanup() {
    if [[ -n "$ROUTER_PID" ]] && kill -0 "$ROUTER_PID" 2>/dev/null; then
        info "stopping router (pid $ROUTER_PID)"
        kill "$ROUTER_PID" 2>/dev/null || true
        wait "$ROUTER_PID" 2>/dev/null || true
    fi
    [[ -n "$ROUTER_CFG" ]] && rm -f "$ROUTER_CFG"
    [[ -n "$ROUTER_BIN" ]] && rm -f "$ROUTER_BIN"
    if [[ -n "$SIM_PID" ]] && kill -0 "$SIM_PID" 2>/dev/null; then
        info "stopping provider_simulator (pid $SIM_PID)"
        kill "$SIM_PID" 2>/dev/null || true
        wait "$SIM_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

wait_for_control() {
    info "waiting for simulator control plane at $CONTROL_URL ..."
    for _ in $(seq 1 30); do
        if curl -fsS "$CONTROL_URL/scenario" >/dev/null 2>&1; then
            info "control plane is up"
            return 0
        fi
        sleep 0.5
    done
    fail "simulator control plane never became reachable at $CONTROL_URL"
    exit 1
}

if [[ "$START_SIM" == true ]]; then
    header "Launching provider_simulator"
    # Reuse an already-running simulator rather than spawning one that would
    # crash on EADDRINUSE (the simulator binds fixed ports 18545+/19000) and
    # leave us asserting against the wrong process.
    if curl -fsS "$CONTROL_URL/scenario" >/dev/null 2>&1; then
        info "a simulator is already listening on $CONTROL_URL — reusing it (not launching another)"
    else
        if [[ ! -f "$SIM_DIR/run.py" ]]; then
            fail "provider_simulator not found at $SIM_DIR (set SIM_DIR=...)"
            exit 1
        fi
        info "starting: python run.py  (cwd: $SIM_DIR)"
        ( cd "$SIM_DIR" && exec python3 run.py ) &
        SIM_PID=$!
        # If the freshly-spawned process dies immediately (e.g. port clash), fail loudly.
        sleep 1
        if ! kill -0 "$SIM_PID" 2>/dev/null; then
            SIM_PID=""
            fail "provider_simulator exited immediately after launch (port already in use?)"
            exit 1
        fi
        wait_for_control
    fi
else
    info "assuming provider_simulator is already running; control: $CONTROL_URL"
fi

# ── router lifecycle (optional) ───────────────────────────────────────────────
# Builds a router from the CURRENT branch (so it contains the fix) and launches
# it with a minimal 2-provider direct-rpc config wired to the simulator's
# JSON-RPC listeners (18545/18546). Two providers + threshold 2 keeps the test
# deterministic: both are always queried, so divergence can't be masked by the
# optimizer picking an unconfigured third provider.
if [[ "$START_ROUTER" == true ]]; then
    header "Building & launching router from this branch"

    ROUTER_BIN="$(mktemp -t smartrouter_mag2062.XXXXXX)"
    info "go build ./cmd/smartrouter  ->  $ROUTER_BIN"
    ( cd "$REPO_ROOT" && go build -o "$ROUTER_BIN" ./cmd/smartrouter ) \
        || { fail "router build failed"; exit 1; }

    # The router resolves its config arg as a viper config NAME searched in
    # [cwd, cwd/config, ~/.lava] — not an arbitrary path. Write it into the repo
    # root (a search path) and pass the basename; remove it on exit.
    ROUTER_CFG="$REPO_ROOT/.mag2062_sim_cv.yml"
    cat > "$ROUTER_CFG" <<YAML
endpoints:
  - chain-id: ETH1
    api-interface: jsonrpc
    network-address: 127.0.0.1:$ROUTER_PORT

direct-rpc:
  - name: sim1
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: http://127.0.0.1:18545
        skip-verifications: [chain-id, pruning]
  - name: sim2
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: http://127.0.0.1:18546
        skip-verifications: [chain-id, pruning]
YAML

    ROUTER_LOG="$(mktemp -t smartrouter_mag2062_log.XXXXXX)"
    info "starting router on :$ROUTER_PORT (log: $ROUTER_LOG)"
    ( cd "$REPO_ROOT" && exec "$ROUTER_BIN" "$(basename "$ROUTER_CFG")" \
        --use-static-spec "$SPEC_DIR" \
        --skip-websocket-verification --log-level warn ) > "$ROUTER_LOG" 2>&1 &
    ROUTER_PID=$!

    info "waiting for router at $ROUTER_URL ..."
    router_up=false
    for _ in $(seq 1 60); do
        if ! kill -0 "$ROUTER_PID" 2>/dev/null; then
            fail "router exited during startup — last log lines:"; tail -15 "$ROUTER_LOG" >&2
            ROUTER_PID=""; exit 1
        fi
        if curl -fsS -o /dev/null -X POST "$ROUTER_URL" \
            -H 'Content-Type: application/json' \
            -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$METHOD\",\"params\":[]}" 2>/dev/null; then
            router_up=true; break
        fi
        sleep 0.5
    done
    if [[ "$router_up" != true ]]; then
        fail "router did not become reachable at $ROUTER_URL — last log lines:"; tail -15 "$ROUTER_LOG" >&2
        exit 1
    fi
    info "router is up"
fi

# ── helpers ───────────────────────────────────────────────────────────────────

# Configure providers 1 & 2 via the simulator's /scenario body-override.
# json.dumps in the simulator preserves the key order we send here, which is
# exactly what lets us reproduce the byte-different-but-semantically-equal case.
configure() {
    local body_p1="$1" body_p2="$2"
    curl -fsS "$CONTROL_URL/scenario" -H 'Content-Type: application/json' -d @- >/dev/null <<JSON
{
  "providers": {
    "1": {"mode": "success", "responses": {"$METHOD": {"body": $body_p1}}},
    "2": {"mode": "success", "responses": {"$METHOD": {"body": $body_p2}}}
  }
}
JSON
}

# Send one request through the router; echo "<http_status> <cv_status>".
# cv_status is the value of the CV header, or "<absent>" if the router didn't emit it.
#
# Cross-validation only activates when BOTH directive headers are present on the
# request (see GetCrossValidationParameters in chainlib/protocol_message.go):
# max-participants and agreement-threshold, each >= 1, with threshold <= participants.
# Without them the router runs an ordinary relay and emits no CV status header —
# which presents identically to "CV disabled". Send both here.
probe() {
    local resp http_status cv_status
    resp="$(curl -sS -D - -o /dev/null \
        -X POST "$ROUTER_URL" \
        -H 'Content-Type: application/json' \
        -H "$MAX_PARTICIPANTS_HEADER: $MAX_PARTICIPANTS" \
        -H "$AGREEMENT_THRESHOLD_HEADER: $AGREEMENT_THRESHOLD" \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$METHOD\",\"params\":[]}")"
    http_status="$(printf '%s' "$resp" | awk 'NR==1{print $2}')"
    cv_status="$(printf '%s' "$resp" \
        | tr -d '\r' \
        | awk -F': ' -v h="$CV_HEADER" 'tolower($1)==tolower(h){print $2}')"
    printf '%s %s' "${http_status:-000}" "${cv_status:-<absent>}"
}

FAILURES=0

# ── 1. POSITIVE: same result, different key order → CV must SUCCEED ────────────
header "Test 1 — reordered envelopes must AGREE (the MAG-2062 fix)"
configure \
    "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"$RESULT\"}" \
    "{\"id\":1,\"jsonrpc\":\"2.0\",\"result\":\"$RESULT\"}"
info "provider 1: {jsonrpc,id,result} = $RESULT"
info "provider 2: {id,jsonrpc,result} = $RESULT   (same value, reordered keys)"
read -r http cv <<<"$(probe)"
info "router responded: HTTP $http   $CV_HEADER=$cv"
if [[ "$cv" == "<absent>" ]]; then
    fail "router did not emit '$CV_HEADER' — is cross-validation enabled on this router/config?"
    FAILURES=$((FAILURES + 1))
elif [[ "$http" == "200" && "$cv" == "success" ]]; then
    pass "semantically-identical responses agreed despite different key order"
else
    fail "expected HTTP 200 + $CV_HEADER=success, got HTTP $http + $cv (pre-fix symptom: 500/failed)"
    FAILURES=$((FAILURES + 1))
fi

# ── 2. NEGATIVE CONTROL: different result → CV must FAIL ───────────────────────
header "Test 2 — genuinely different results must DISAGREE (negative control)"
configure \
    "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"$RESULT\"}" \
    "{\"id\":1,\"jsonrpc\":\"2.0\",\"result\":\"$ALT_RESULT\"}"
info "provider 1: result = $RESULT"
info "provider 2: result = $ALT_RESULT   (genuinely divergent)"
read -r http cv <<<"$(probe)"
info "router responded: HTTP $http   $CV_HEADER=$cv"
if [[ "$cv" == "<absent>" ]]; then
    fail "router did not emit '$CV_HEADER' — is cross-validation enabled on this router/config?"
    FAILURES=$((FAILURES + 1))
elif [[ "$cv" == "failed" ]]; then
    pass "divergent responses correctly failed agreement (header is not a constant)"
else
    fail "expected $CV_HEADER=failed for divergent results, got HTTP $http + $cv"
    FAILURES=$((FAILURES + 1))
fi

# ── verdict ───────────────────────────────────────────────────────────────────
header "Result"
if [[ "$FAILURES" -eq 0 ]]; then
    pass "MAG-2062 verified end-to-end (agreement on reorder, failure on divergence)"
    exit 0
else
    fail "$FAILURES check(s) failed"
    exit 1
fi
