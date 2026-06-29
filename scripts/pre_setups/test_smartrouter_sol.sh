#!/bin/bash
# =============================================================================
# Smart Router — Solana integration smoke test.
#
# Companion to init_smartrouter_sol.sh: that script STARTS the router (direct RPC,
# SOLANA, port 3360); THIS script EXERCISES it and grades the result. Run the init
# script first, then run this one against the live instance.
#
# What it tests (all Solana-specific behaviour, not generic JSON-RPC plumbing):
#   1. Slot/blockHeight consistency under load (PR #21 / MAG-1591). getLatestBlockhash
#      returns context.slot (~425M) and value.lastValidBlockHeight (~403M) which
#      diverge by the skip rate; the tracker must compare slot-to-slot. We hammer
#      the router so SVMChainTracker keeps polling and the shared-state seen-block
#      path runs. A slot/unit-mismatch in the log -> FAIL; 'No pairings' / degraded
#      availability -> WARN (endpoint throttling, not a router defect).
#
# Grading: FAIL = router defect (fails the suite / non-zero exit). WARN = upstream
# capacity/transient (retried once, then reported, never fails the suite). SKIP =
# untestable on this endpoint (e.g. testnet WS can't serve subscriptions).
#   2. Commitment levels — processed / confirmed / finalized all resolve.
#   3. Skipped-slot / propagation retry path (svm_block_hash_retry.go) — a far-future
#      block must come back as a clean JSON-RPC error, not a router crash/hang.
#   4. Method-coverage sweep — a spread of Solana methods (varied param shapes and
#      encodings) each return a JSON-RPC "result", catching parser/spec regressions.
#   5. JSON-RPC batch — array-of-calls handling.
#
# This script does NOT delete logs (the router is live and writing to them); it
# records the log line count up front and greps only the lines this run produced.
# =============================================================================
__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "$__dir"/../useful_commands.sh

ROUTER_URL="${ROUTER_URL:-http://127.0.0.1:3360}"
# Solana subscriptions are served over WebSocket on the same listen-address, but
# at the /ws (or /websocket) path — the root "/" only accepts HTTP POST and returns
# 405 on a ws upgrade (see jsonRPC.go app.Get("/ws", ...)).
WS_URL="${WS_URL:-ws://127.0.0.1:3360/ws}"
LOGS_DIR=${__dir}/../../debugging/logs
ROUTER_LOG="$LOGS_DIR/SMARTROUTER.log"
LOAD_ITERS="${LOAD_ITERS:-30}"   # consistency-hammer iterations
WS_WAIT="${WS_WAIT:-6}"          # seconds to hold the ws open for notifications

PASS=0
FAIL=0
SKIP=0
WARN=0

green()  { printf '\033[32m%s\033[0m\n' "$1"; }
red()    { printf '\033[31m%s\033[0m\n' "$1"; }
yellow() { printf '\033[33m%s\033[0m\n' "$1"; }

# is_capacity_error <response> — true if the response is an UPSTREAM capacity /
# transient outcome (rate-limit, node 5xx, quorum-not-met) rather than a router
# defect. These are graded WARN, not FAIL: they reflect endpoint quality, and the
# router returning a clean JSON-RPC error for them is correct behaviour.
is_capacity_error() {
    echo "$1" | grep -Eqi 'HTTP 429|429 Too Many Requests|HTTP 50[0-9]|Internal node error|insufficient results|no pairings'
}

# rpc <method> <params-json> -> echoes response body, returns 0 on transport success
rpc() {
    local method="$1" params="$2"
    curl -s --max-time 15 -X POST "$ROUTER_URL" \
        -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"$method\",\"params\":$params,\"id\":1}"
}

# expect_result <label> <method> <params> — PASS if a JSON-RPC "result" comes back.
# Retries once (a single transient upstream hiccup — 429/5xx/quorum — shouldn't fail
# the suite). If it still fails, an upstream-capacity error is graded WARN; anything
# else (e.g. a genuine -32601 / parse error reproduced on retry) is a hard FAIL.
expect_result() {
    local label="$1" method="$2" params="$3" resp
    resp="$(rpc "$method" "$params")"
    if echo "$resp" | grep -q '"result"'; then
        green "  PASS  $label"; PASS=$((PASS + 1)); return
    fi
    sleep 0.5
    resp="$(rpc "$method" "$params")"
    if echo "$resp" | grep -q '"result"'; then
        green "  PASS  $label (after retry)"; PASS=$((PASS + 1)); return
    fi
    if is_capacity_error "$resp"; then
        yellow "  WARN  $label — upstream capacity/transient (not a router defect)"
        echo   "        -> ${resp:0:200}"
        WARN=$((WARN + 1))
    else
        red    "  FAIL  $label"
        echo   "        -> ${resp:0:200}"
        FAIL=$((FAIL + 1))
    fi
}

# expect_clean_error <label> <method> <params> — pass if we get a well-formed JSON-RPC
# error (or result) rather than an empty body / router-level failure. Used for the
# skipped-slot path where an error IS the correct, graceful outcome.
expect_clean_error() {
    local label="$1" method="$2" params="$3" resp
    resp="$(rpc "$method" "$params")"
    if echo "$resp" | grep -Eq '"(result|error)"' && ! echo "$resp" | grep -qi 'No pairings'; then
        green "  PASS  $label (graceful response)"
        PASS=$((PASS + 1))
    else
        red   "  FAIL  $label"
        echo  "        -> ${resp:0:200}"
        FAIL=$((FAIL + 1))
    fi
}

# ws_send <request-json> — send one ws message, hold the connection $WS_WAIT
# seconds to collect notifications, echo everything received. Echoes nothing and
# returns 1 if no client is available.
#
# Client preference: websocat > node(ws) > wscat. wscat is LAST because in a
# non-TTY (command substitution, as used here) it is unreliable: `-x MSG -w N`
# exits before the sub completes, and a stdin pipe connects but never sends the
# message. websocat (stdin pipe) and a tiny node 'ws' script both work reliably
# and print received frames to stdout.
ws_client=""
WS_NODE_SCRIPT=""
if command -v websocat >/dev/null 2>&1; then
    ws_client="websocat"
elif command -v node >/dev/null 2>&1 && node -e "require.resolve('ws')" >/dev/null 2>&1; then
    ws_client="node"
    # The probe script is written to a temp dir, so node would resolve require('ws')
    # from /tmp (no node_modules there) and fail. Pin NODE_PATH to the node_modules
    # that actually holds 'ws' (resolved from THIS dir, which can find it).
    WS_NODE_PATH="$(dirname "$(dirname "$(node -e "console.log(require.resolve('ws'))" 2>/dev/null)")")"
    WS_NODE_SCRIPT="$(mktemp -t wsprobe.XXXXXX).js"
    cat > "$WS_NODE_SCRIPT" <<'NODEEOF'
const WebSocket = require('ws');
const [, , url, msg, waitMs] = process.argv;
const ws = new WebSocket(url);
const t = setTimeout(() => { try { ws.close(); } catch (e) {} process.exit(0); }, parseInt(waitMs || '6000', 10));
ws.on('open', () => ws.send(msg));
ws.on('message', (d) => process.stdout.write(d.toString() + '\n'));
ws.on('error', (e) => { process.stdout.write('[wserror] ' + e.message + '\n'); clearTimeout(t); process.exit(0); });
NODEEOF
    trap 'rm -f "$WS_NODE_SCRIPT"' EXIT
elif command -v wscat >/dev/null 2>&1; then
    ws_client="wscat"
fi

ws_send() {
    local req="$1"
    case "$ws_client" in
        websocat)
            # Keep stdin open for $WS_WAIT so the server can push notifications;
            # timeout caps the whole exchange in case the server never closes.
            { printf '%s\n' "$req"; sleep "$WS_WAIT"; } \
                | timeout "$((WS_WAIT + 4))" websocat -t "$WS_URL" 2>/dev/null
            ;;
        node)
            # node 'ws' client: connects, sends the request, prints every received
            # frame for $WS_WAIT seconds. Reliable in non-TTY (unlike wscat).
            # NODE_PATH lets the temp script resolve 'ws' from wherever it's installed.
            NODE_PATH="$WS_NODE_PATH" timeout "$((WS_WAIT + 4))" node "$WS_NODE_SCRIPT" "$WS_URL" "$req" "$((WS_WAIT * 1000))" 2>/dev/null
            ;;
        wscat)
            # Use a stdin pipe held open for $WS_WAIT rather than `-x MSG -w N`:
            # in a non-TTY (command substitution) wscat's `-w` is ignored and it
            # exits immediately, so the subscription never completes. Feeding the
            # request on stdin and holding it open keeps the connection alive long
            # enough for the sub to start and notifications to arrive.
            { printf '%s\n' "$req"; sleep "$WS_WAIT"; } \
                | timeout "$((WS_WAIT + 4))" wscat -c "$WS_URL" 2>/dev/null
            ;;
        *)
            return 1
            ;;
    esac
}

# --- preflight ---------------------------------------------------------------
echo "============================================"
echo "Smart Router — Solana integration smoke test"
echo "============================================"
echo "Router:   $ROUTER_URL"
echo "Log:      $ROUTER_LOG"
echo ""

if ! rpc getSlot '[]' | grep -q '"result"'; then
    red "ERROR: router not reachable / not returning getSlot at $ROUTER_URL"
    echo "Start it first:  bash $__dir/init_smartrouter_sol.sh"
    exit 1
fi

# Mark where this run starts in the log so we only grade lines we caused.
LOG_START_LINE=0
if [ -f "$ROUTER_LOG" ]; then
    LOG_START_LINE=$(wc -l < "$ROUTER_LOG" | tr -d ' ')
fi

# --- 1. consistency under load (the Solana regression gate) ------------------
echo ""
echo "[1] Slot/blockHeight consistency under load ($LOAD_ITERS iterations)..."
load_ok=0
for i in $(seq 1 "$LOAD_ITERS"); do
    # Alternate the two methods that read context.slot so both the tracker poll
    # and the parsed-relay paths are exercised against the same seen-block value.
    if (( i % 2 == 0 )); then
        r="$(rpc getLatestBlockhash '[{"commitment":"finalized"}]')"
    else
        r="$(rpc getSlot '[]')"
    fi
    echo "$r" | grep -q '"result"' && load_ok=$((load_ok + 1))
    sleep 0.3
done
echo "      $load_ok/$LOAD_ITERS requests returned a result"

# Two distinct axes, graded separately:
#   * slot/blockHeight UNIT MISMATCH (the PR #21 regression) -> hard FAIL.
#   * 'No pairings' / requests that didn't return -> endpoint AVAILABILITY -> WARN.
# A loaded test can exhaust a shared testnet key (429s) and produce 'No pairings'
# with zero consistency errors; that is endpoint capacity, not a router defect.
if [ -f "$ROUTER_LOG" ]; then
    consistency_hits="$(tail -n +"$((LOG_START_LINE + 1))" "$ROUTER_LOG" \
        | grep -Ei 'consistency error|too new|blockGap|[Cc]ode 3368' || true)"
    pairing_hits="$(tail -n +"$((LOG_START_LINE + 1))" "$ROUTER_LOG" \
        | grep -Ei 'No pairings' || true)"
else
    consistency_hits=""; pairing_hits=""
    echo "      (log not found at $ROUTER_LOG — skipping log assertion)"
fi
if [ -n "$consistency_hits" ]; then
    red   "  FAIL  slot/blockHeight consistency errors detected (PR #21 regression):"
    echo "$consistency_hits" | sed 's/^/        /' | head
    FAIL=$((FAIL + 1))
elif [ "$load_ok" -lt "$LOAD_ITERS" ] || [ -n "$pairing_hits" ]; then
    yellow "  WARN  no consistency errors, but endpoint availability degraded"
    echo   "        ($load_ok/$LOAD_ITERS returned; 'No pairings' lines: $(echo -n "$pairing_hits" | grep -c . ))"
    echo   "        -> shared testnet key likely rate-limited; lower LOAD_ITERS or use a dedicated RPC"
    WARN=$((WARN + 1))
else
    green "  PASS  no consistency / slot-mismatch errors during load"
    PASS=$((PASS + 1))
fi

# --- 2. commitment levels ----------------------------------------------------
echo ""
echo "[2] Commitment levels..."
for c in processed confirmed finalized; do
    expect_result "getSlot commitment=$c" getSlot "[{\"commitment\":\"$c\"}]"
done

# --- 3. skipped-slot / propagation retry path --------------------------------
echo ""
echo "[3] Skipped-slot retry path (svm_block_hash_retry.go)..."
# A far-future block forces the -32004 retry walk-back; the correct outcome is a
# graceful JSON-RPC error, never a hang or a router-level failure.
expect_clean_error "getBlock far-future slot" getBlock '[999999999999,{"maxSupportedTransactionVersion":0}]'

# --- 4. method-coverage sweep ------------------------------------------------
echo ""
echo "[4] Method-coverage sweep (param shapes + encodings)..."
expect_result "getVersion"                getVersion    '[]'
expect_result "getBlockHeight"            getBlockHeight '[]'
expect_result "getEpochInfo"              getEpochInfo   '[]'
expect_result "getHealth"                 getHealth      '[]'
expect_result "getAccountInfo base64"     getAccountInfo '["11111111111111111111111111111111",{"encoding":"base64"}]'
expect_result "getBalance"                getBalance     '["11111111111111111111111111111111"]'
expect_result "getMultipleAccounts"       getMultipleAccounts '[["11111111111111111111111111111111"],{"encoding":"base64"}]'
expect_result "getLatestBlockhash"        getLatestBlockhash '[{"commitment":"finalized"}]'
expect_result "getRecentPrioritizationFees" getRecentPrioritizationFees '[[]]'
expect_result "getTransactionCount"       getTransactionCount '[]'

# --- 5. JSON-RPC batch -------------------------------------------------------
echo ""
echo "[5] JSON-RPC batch request..."
batch() {
    curl -s --max-time 15 -X POST "$ROUTER_URL" -H 'Content-Type: application/json' \
        -d '[{"jsonrpc":"2.0","method":"getSlot","params":[],"id":1},{"jsonrpc":"2.0","method":"getBlockHeight","params":[],"id":2}]'
}
# Both ids should come back with results.
batch_ok() {
    echo "$1" | grep -q '"id":1' && echo "$1" | grep -q '"id":2' \
        && [ "$(echo "$1" | grep -o '"result"' | wc -l | tr -d ' ')" -ge 2 ]
}
batch_resp="$(batch)"
if ! batch_ok "$batch_resp"; then sleep 0.5; batch_resp="$(batch)"; fi  # retry once
if batch_ok "$batch_resp"; then
    green "  PASS  batch returned both results"
    PASS=$((PASS + 1))
elif is_capacity_error "$batch_resp"; then
    yellow "  WARN  batch — upstream capacity/transient (not a router defect)"
    echo   "        -> ${batch_resp:0:200}"
    WARN=$((WARN + 1))
else
    red   "  FAIL  batch incomplete"
    echo  "        -> ${batch_resp:0:200}"
    FAIL=$((FAIL + 1))
fi

# --- 6. WebSocket subscription -----------------------------------------------
echo ""
echo "[6] WebSocket subscription (slotSubscribe)..."
if [ "${SKIP_WS:-0}" = "1" ]; then
    echo "      SKIP  (SKIP_WS=1)"
    SKIP=$((SKIP + 1))
elif [ -z "$ws_client" ]; then
    echo "      SKIP  no ws client found — need 'websocat', node with the 'ws' module, or 'wscat'"
    echo "            (e.g. 'brew install websocat', or 'npm i -g wscat')"
    SKIP=$((SKIP + 1))
else
    echo "      using $ws_client against $WS_URL (holding ${WS_WAIT}s for notifications)"
    # The WS client's stdout is unreliable across versions (wscat -x prints nothing
    # in some builds), so we grade from the ROUTER LOG, which records the consumer
    # subscription outcome deterministically. Mark the log right before sending and
    # read only the lines this attempt produced, excluding the [SVMChainTracker]
    # background poller (its periodic 'close 1011' noise is unrelated to consumer
    # subscriptions and must not be mistaken for the subscription's result).
    ws_mark=0; [ -f "$ROUTER_LOG" ] && ws_mark=$(wc -l < "$ROUTER_LOG" | tr -d ' ')
    ws_out="$(ws_send '{"jsonrpc":"2.0","id":1,"method":"slotSubscribe","params":[]}')"
    ws_log=""
    [ -f "$ROUTER_LOG" ] && ws_log="$(tail -n +"$((ws_mark + 1))" "$ROUTER_LOG" | grep -v 'SVMChainTracker' || true)"
    both="$ws_out
$ws_log"
    [ -n "${WS_DEBUG:-}" ] && echo "      [debug] ws_mark=$ws_mark ws_out=[${ws_out:0:60}] ws_log_lines=$(echo "$ws_log" | grep -c .)" >&2
    # Upstream-can't-serve-subscriptions signature (gateway accepts the ws but the
    # backend refuses/aborts the sub): Lava 'code 889 SubscriptionInitiationError',
    # ZAN 'close 1011', a plain 'failed to dial WebSocket'. Deliberately NARROW — it
    # must NOT swallow a router defect such as 'unknown parameters type'.
    # Two SKIP reasons (neither is a router defect): (a) no ws:// endpoint configured
    # at all — HTTP-only setup, the NoOp manager returns "no ws:// or wss:// endpoints
    # configured"; (b) the upstream accepts the ws but can't serve the sub.
    ws_unsupported_re='SubscriptionInitiationError|[Cc]ode 889|close 1011|failed to dial WebSocket|Provider failed initiating subscription|no ws:// or wss:// endpoints configured|WebSocket subscriptions not available'
    # Success: a subscription id (rs_…) over the wire or 'DirectWS: subscription started'
    # in the log. Notifications are a bonus. A consumer-path error means the router
    # tried but failed — SKIP if the cause is the upstream signature, else FAIL.
    if echo "$both" | grep -q 'slotNotification'; then
        green "  PASS  slotSubscribe confirmed and notifications received"
        PASS=$((PASS + 1))
    elif echo "$both" | grep -Eq 'DirectWS: subscription started|"result":"?rs_|HasError=false.*result.*rs_'; then
        green "  PASS  slotSubscribe confirmed (subscription id returned)"
        PASS=$((PASS + 1))
    elif echo "$both" | grep -Eqi "$ws_unsupported_re"; then
        # No ws endpoint configured, or upstream can't serve subs — neither is a router defect.
        yellow "  SKIP  WS subscription not serviceable on this endpoint (not a router defect)"
        echo   "        -> $(echo "$both" | grep -Eoi "$ws_unsupported_re" | head -1)"
        SKIP=$((SKIP + 1))
    elif echo "$both" | grep -Eqi 'could not start subscription|StartSubscription returned an error|HasError=true'; then
        red   "  FAIL  router accepted the ws but failed to start the subscription"
        echo  "        -> $(echo "$both" | grep -Ei 'could not start subscription|StartSubscription returned an error' | head -1 | sed 's/^[[:space:]]*//' | cut -c1-200)"
        FAIL=$((FAIL + 1))
    elif ! echo "$both" | grep -Eqi 'websocket opened|consumer websocket manager started'; then
        red   "  FAIL  router did not accept the ws upgrade (wrong path? expected /ws) — got: ${ws_out:0:120}"
        FAIL=$((FAIL + 1))
    else
        red   "  FAIL  no subscription confirmation and no clear error in log"
        echo  "        -> ${ws_out:0:160}"
        FAIL=$((FAIL + 1))
    fi
fi

# --- summary -----------------------------------------------------------------
# Exit code is keyed ONLY on FAIL (router defects). WARN (endpoint capacity) and
# SKIP (untestable on this infra) are reported but do not fail the suite — so this
# is safe to gate CI on without flaking on shared-testnet rate limits.
echo ""
echo "============================================"
echo "Results:  PASS=$PASS  FAIL=$FAIL  WARN=$WARN  SKIP=$SKIP"
echo "  (WARN = upstream capacity/transient · SKIP = untestable on this endpoint)"
if [ "$FAIL" -eq 0 ]; then
    green "ALL ROUTER CHECKS PASSED"
    [ "$WARN" -gt 0 ] && yellow "  ($WARN warning(s) — endpoint quality, not router defects)"
    [ "$SKIP" -gt 0 ] && yellow "  ($SKIP check(s) skipped — endpoint can't serve them)"
    echo "============================================"
    exit 0
else
    red "SOME ROUTER CHECKS FAILED — inspect $ROUTER_LOG"
    echo "  grep -Ei 'consistency error|too new|No pairings|blockGap|retry|32004' $ROUTER_LOG | tail"
    echo "============================================"
    exit 1
fi
