#!/bin/bash
# =============================================================================
# UC-4 (Quorum Mismatch -> Metric for Alerting) + UC-5 (Quorum Failure ->
# Structured Signal to Client) + UC-6 (Outlier Excluded from Result Set) test
# harness — smart-router driven against the provider_simulator (a fault-
# injectable mock backend).
#
# These three are facets of ONE primitive — "make N providers diverge":
#   * Scenario A (1 dissenter, quorum HOLDS): the operator gets an alerting metric
#     (UC-4) AND the outlier is excluded from the client result + logged (UC-6).
#   * Scenario B (all diverge, quorum LOST): the client gets a structured failure
#     signal distinguishable from a generic upstream error (UC-5).
#
# UC-1/UC-2 work BECAUSE providers agree (shared upstream -> byte-identical ->
# quorum forms). UC-4 needs the OPPOSITE: a provider that DISAGREES. A shared
# upstream can never emit a mismatch metric, so this harness drives the
# provider_simulator and injects a divergent-but-SUCCESSFUL response on one
# provider via the simulator's per-method body override (POST /scenario).
#
# The PRD (UC-4): "A response set does not reach quorum, OR reaches quorum with
# one or more providers dissenting -> Router emits a metric tagged with chain,
# method, provider group, dissenting providers, and a finality marker."
#
# Two scenarios, one per trigger:
#   A) QUORUM REACHED + 1 DISSENTER -> the headline alerting metric:
#        cross_validation_mismatch_total{group,finality}  (+ provider_disagreements_total)
#        Requires a SUCCESSFUL content outlier on a DETERMINISTIC method ('block')
#        after a quorum formed — exactly what the body-override produces.
#   B) NO QUORUM (3 distinct successful values, threshold 2) -> cross_validation_failed_total.
#
# Why the body-override and not corruption_mode: the simulator's tendermintrpc
# SUCCESS path ignores corruption_mode (it is gated to eth/btc/ln), and the
# corruption primitives produce parse/node errors anyway — which fall OUTSIDE the
# mismatch metric's gate (quorum-reached + deterministic + successful-outlier).
# Overriding only responses.block.body keeps the response a valid HTTP-200 success
# with divergent content, and keeps 'status' clean so the provider still passes
# startup verification (it dissents ONLY on the measured 'block' call).
# =============================================================================
__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "$__dir"/../useful_commands.sh

LOGS_DIR=${__dir}/../../debugging/logs
mkdir -p "$LOGS_DIR"
LOGS_DIR=$(cd "$LOGS_DIR" && pwd)
rm "$LOGS_DIR"/*.log 2>/dev/null || true

PROJECT_ROOT=$(cd "${__dir}/../.." && pwd)
LOG_FILE="$LOGS_DIR/SMARTROUTER_UC4.log"
SIM_LOG="$LOGS_DIR/UC4_SIM.log"

# The provider_simulator (sibling repo). Override with SIM_DIR=... if it lives elsewhere.
SIM_DIR="${SIM_DIR:-$(cd "$PROJECT_ROOT/.." && pwd)/provider_simulator}"
SIM_CONTROL="127.0.0.1:19000"
# Tendermint-RPC simulator providers (constants.TM_PORTS): ids 1/2/3.
SIM_TM_1=18554
SIM_TM_2=18555
SIM_TM_3=18556

# Smart-router ports (UC-4 owns its own, distinct from UC-1/UC-2).
TM_PORT=3392
METRICS_PORT=7798

# -----------------------------------------------------------------------------
# THE POLICY UNDER TEST. A plain cross-validation policy on a deterministic
# method — NO min-groups (UC-4 is mismatch-on-dissent, orthogonal to UC-2
# diversity). Group labels ARE set so mismatch_total{group=...} is meaningful.
# -----------------------------------------------------------------------------
CV_CHAIN="LAVA"
CV_API="tendermintrpc"
CV_METHOD="block"          # deterministic consensus state -> content outliers are real, not node-local
CV_THRESHOLD=2             # 2 identical responses form the quorum
MAX_PART=3                 # fan out to all 3 providers
DISSENTER="sim-3"          # the provider we make disagree (group g-b)
DISSENTER_GROUP="g-b"      # mismatch_total{group} we expect for scenario A
HEIGHT=4000000             # << sim head (5,000,000) -> a settled, FINALIZED height
OTHER_METHOD="health"      # a method with NO cross-validation policy — used by the UC-5
                           # "distinguishable from a generic upstream error" contrast.

# Divergent block bodies are distinguished by these 8-hex block_id.hash prefixes.
SALT_A="DEADBEEF"          # scenario A: sim-3's lone dissent
SALT_B2="FEEDFACE"         # scenario B: sim-2 variant
SALT_B3="0BADC0DE"         # scenario B: sim-3 variant (distinct from sim-2 -> no value reaches quorum)

print_policy_banner() {
	echo "+------------------------------------------------------------+"
	echo "| UC-4 — Quorum Mismatch -> Metric for Alerting              |"
	echo "+------------------------------------------------------------+"
	printf "|  backend:             %-36s|\n" "provider_simulator (tendermintrpc)"
	printf "|  chain / api:         %-36s|\n" "$CV_CHAIN / $CV_API"
	printf "|  method / quorum:     %-36s|\n" "$CV_METHOD: $CV_THRESHOLD of $MAX_PART (no min-groups)"
	printf "|  finalized height:    %-36s|\n" "$HEIGHT (sim head 5,000,000)"
	printf "|  A) quorum+dissent -> %-36s|\n" "mismatch_total{group,finality}"
	printf "|  B) no quorum      -> %-36s|\n" "failed_total"
	echo "+------------------------------------------------------------+"
}

echo "============================================"
echo "UC-4 (Mismatch Metric) + UC-5 (Structured Failure Signal) + UC-6 (Outlier Excluded) — provider_simulator"
echo "============================================"
print_policy_banner
echo ""

for tool in jq python3 curl; do
	command_exists "$tool" || { echo "✗ ERROR: '$tool' is required."; exit 1; }
done

# Pick a Python for the simulator (README: requires 3.12; the TM side is stdlib-only).
SIM_PY="$(command -v python3.12 || command -v python3)"

# -----------------------------------------------------------------------------
# Simulator control helpers
# -----------------------------------------------------------------------------
sim_up() { curl -s --max-time 2 "http://$SIM_CONTROL/health" >/dev/null 2>&1; }

# clean_block <port> -> the simulator's clean block@HEIGHT result (JSON-RPC envelope).
clean_block() {
	curl -s -X POST "http://127.0.0.1:$1/" -H 'Content-Type: application/json' \
		-d "{\"jsonrpc\":\"2.0\",\"method\":\"block\",\"params\":{\"height\":\"$HEIGHT\"},\"id\":1}"
}

# set_block_override <provider_id> <hex8-prefix> : make that provider return a
# valid-but-divergent block@HEIGHT (mutates block_id.hash, preserves the height so
# it still parses as a successful block on the router side).
set_block_override() {
	local pid="$1" salt="$2" scenario
	scenario=$(clean_block "$SIM_TM_1" | SALT="$salt" PID="$pid" "$SIM_PY" -c "
import sys, json, os
r = json.load(sys.stdin)['result']
r['block_id']['hash'] = os.environ['SALT'] + r['block_id']['hash'][8:]
print(json.dumps({'providers': {os.environ['PID']: {'chain_family': 'tendermintrpc',
    'responses': {'block': {'status': 200, 'body': r}}}}}))
")
	curl -s -X POST "http://$SIM_CONTROL/scenario" -H 'Content-Type: application/json' -d "$scenario" >/dev/null
}

# set_latency <provider_id> <ms> : delay that provider's responses (merges into its
# scenario config). Used to order responses around the quorum early-exit (Scenario A).
set_latency() {
	curl -s -X POST "http://$SIM_CONTROL/scenario" -H 'Content-Type: application/json' \
		-d "{\"providers\":{\"$1\":{\"chain_family\":\"tendermintrpc\",\"latency_ms\":$2}}}" >/dev/null
}

sim_reset() { curl -s -X POST "http://$SIM_CONTROL/reset/all" >/dev/null 2>&1; }

# --- Tear down only THIS script's previous router (NOT the simulator, which is
# shared and may be serving other work). The router is re-created and LEFT RUNNING.
screen -S smartrouter -X quit >/dev/null 2>&1 || true
killall smartrouter 2>/dev/null || true
sleep 1
screen -wipe >/dev/null 2>&1 || true

echo "[Setup] installing binaries"
make install

# --- Bring up the simulator (only if it is not already running) ---------------
echo ""
if sim_up; then
	echo "[Setup] provider_simulator already running on $SIM_CONTROL (reusing it)"
else
	echo "[Setup] starting provider_simulator from $SIM_DIR"
	[ -f "$SIM_DIR/run.py" ] || { echo "✗ ERROR: $SIM_DIR/run.py not found. Set SIM_DIR=... to the provider_simulator checkout."; exit 1; }
	( cd "$SIM_DIR" && nohup "$SIM_PY" -u run.py > "$SIM_LOG" 2>&1 & )
	for _ in $(seq 1 30); do sim_up && break; sleep 1; done
	sim_up || { echo "✗ ERROR: simulator did not become ready. See $SIM_LOG"; tail -n 20 "$SIM_LOG" 2>/dev/null | sed 's/^/    /'; exit 1; }
	echo "✓ simulator up (control: $SIM_CONTROL, tendermintrpc: $SIM_TM_1/$SIM_TM_2/$SIM_TM_3)"
fi
# Start every scenario from a clean slate so providers AGREE during router startup
# verification (a divergence injected before boot would only touch 'block' anyway,
# but a clean slate keeps the QoS baseline identical across all three).
sim_reset
echo "✓ simulator scenarios reset (all providers clean)"

# --- Specs + config -----------------------------------------------------------
SPECS_DIR="$PROJECT_ROOT/specs/tendermint.json,$PROJECT_ROOT/specs/ibc.json,$PROJECT_ROOT/specs/cosmossdk.json,$PROJECT_ROOT/specs/lava.json"
IFS=',' read -r -a SPEC_FILES <<< "$SPECS_DIR"
for spec_file in "${SPEC_FILES[@]}"; do
	[ -f "$spec_file" ] || { echo "✗ ERROR: Spec file not found: $spec_file"; exit 1; }
done

CONFIG_FILE="$PROJECT_ROOT/config/smartrouter_examples/smartrouter_uc4_sim.yml"
CONFIG_REL="config/smartrouter_examples/smartrouter_uc4_sim.yml"
echo ""
echo "[Setup] generating UC-4 config: $CONFIG_FILE"
cat > "$CONFIG_FILE" <<EOF
# Smart Router — UC-4 (Quorum Mismatch metrics) test config.
# Generated by: scripts/pre_setups/test_uc4_smartrouter_sim.sh  (do not hand-edit)
#
# 3 tendermintrpc providers backed by the provider_simulator (ports $SIM_TM_1/$SIM_TM_2/$SIM_TM_3).
# Group labels (g-a x2, g-b x1) so cross_validation_mismatch_total{group=...} is meaningful.
# The simulator reports chain-id 'lava-sim-tm', so chain-id verification is skipped; the
# LAVA tendermintrpc spec is subscription-capable, so the router is started with
# --skip-websocket-verification (these providers are http-only).
endpoints:
  - network-address: "0.0.0.0:$TM_PORT"
    chain-id: "LAVA"
    api-interface: "tendermintrpc"

direct-rpc:
  - name: "sim-1"
    chain-id: "LAVA"
    api-interface: "tendermintrpc"
    group-label: "g-a"
    node-urls:
      - url: "http://127.0.0.1:$SIM_TM_1"
        timeout: 10s
        skip-verifications: [chain-id, pruning, tx-indexing]
  - name: "sim-2"
    chain-id: "LAVA"
    api-interface: "tendermintrpc"
    group-label: "g-a"
    node-urls:
      - url: "http://127.0.0.1:$SIM_TM_2"
        timeout: 10s
        skip-verifications: [chain-id, pruning, tx-indexing]
  - name: "sim-3"
    chain-id: "LAVA"
    api-interface: "tendermintrpc"
    group-label: "g-b"
    node-urls:
      - url: "http://127.0.0.1:$SIM_TM_3"
        timeout: 10s
        skip-verifications: [chain-id, pruning, tx-indexing]

# Plain cross-validation on '$CV_METHOD' — quorum of $CV_THRESHOLD, NO min-groups.
cross-validation:
  policies:
    - chain-id: $CV_CHAIN
      api-interface: $CV_API
      method: $CV_METHOD
      enabled: true
      agreement-threshold: $CV_THRESHOLD
      max-participants: $MAX_PART
EOF
echo "✓ config written ($(wc -c < "$CONFIG_FILE") bytes)"

# --- Start the router ---------------------------------------------------------
echo ""
echo "[Setup] starting Smart Router (debug log -> $LOG_FILE)"
screen -d -m -S smartrouter bash -c "cd \"$PROJECT_ROOT\" && source ~/.bashrc; smartrouter \
$CONFIG_REL \
--geolocation 1 \
--log-level debug \
--use-static-spec \"$SPECS_DIR\" \
--metrics-listen-address ':$METRICS_PORT' \
--skip-websocket-verification \
--min-relay-timeout 5s 2>&1 | tee \"$LOG_FILE\"" && sleep 0.25

echo ""
echo "[Setup] waiting for the router to become ready ..."
ready=0
for _ in $(seq 1 30); do
	if curl -sS -o /dev/null "http://127.0.0.1:$TM_PORT/status" 2>/dev/null; then ready=1; break; fi
	if ! screen -list | grep -q "smartrouter"; then
		echo "✗ ERROR: smart router screen exited during startup."
		tail -n 25 "$LOG_FILE" 2>/dev/null | sed 's/^/    /'
		exit 1
	fi
	sleep 1
done
[ "$ready" -eq 1 ] || { echo "✗ ERROR: router did not become ready in time. See $LOG_FILE"; exit 1; }
echo "✓ router is answering on :$TM_PORT"

# =============================================================================
# UC-4 CHECKS
# =============================================================================
PASS=0
FAIL=0
HDR=$(mktemp)
BODY=$(mktemp)   # client response body — Scenario A (UC-6) checks the client got the CONSENSUS value
pass() { echo "  ✅ PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  ❌ FAIL: $1"; FAIL=$((FAIL + 1)); }
cv_status()         { grep -i '^lava-cross-validation-status:'           "$HDR" | tr -d '\r' | awk '{print $2}'; }
cv_failure_reason() { grep -i '^lava-cross-validation-failure-reason:'   "$HDR" | tr -d '\r' | awk '{print $2}'; }
cv_agreeing()       { grep -i '^lava-cross-validation-agreeing-providers:'    "$HDR" | tr -d '\r' | cut -d' ' -f2-; }
cv_disagreeing()    { grep -i '^lava-cross-validation-disagreeing-providers:' "$HDR" | tr -d '\r' | cut -d' ' -f2-; }

# metric_value <name-fragment> <label-grep> : the value of the first matching metric line, or "" .
metric_value() {
	curl -sS "http://127.0.0.1:$METRICS_PORT/metrics" 2>/dev/null \
		| grep "^smartrouter_cross_validation_$1" | grep "$2" | awk '{print $NF}' | head -n1
}
ge1() { [[ "$1" =~ ^[0-9]+(\.[0-9]+)?$ ]] && awk "BEGIN{exit !($1 >= 1)}"; }

fire_block() { curl -sS -D "$HDR" -o "$BODY" -X POST "http://127.0.0.1:$TM_PORT/" \
	-d "{\"jsonrpc\":\"2.0\",\"method\":\"$CV_METHOD\",\"params\":{\"height\":\"$HEIGHT\"},\"id\":1}"; }

echo ""
echo "============================================"
echo "Running UC-4 + UC-5 + UC-6 checks"
echo "============================================"

# --- Scenario A: quorum reached + 1 dissenter -> mismatch_total ---------------
echo ""
echo "[A] QUORUM + DISSENT: $DISSENTER returns a divergent (but successful) '$CV_METHOD'"
echo "    sim-1 & sim-2 agree -> quorum; $DISSENTER ($DISSENTER_GROUP) is the content outlier"
sim_reset
# Cross-validation EARLY-EXITS once the agreement threshold is met. If both honest
# providers answered first, the quorum would form and the router would exit before
# the dissenter's response was collected — so the dissent would never be observed and
# the mismatch metric would not fire (a real race, not a test bug). Slow the two
# honest providers so the fast dissenter is ALWAYS collected before the quorum
# completes — modelling the case the metric targets: the outlier responded in time.
set_latency "1" 500
set_latency "2" 500
set_block_override "3" "$SALT_A"     # sim-3 = the fast dissenter (group g-b); sim-1/sim-2 agree (slow)
fire_block
a_status=$(cv_status); a_agree=$(cv_agreeing); a_disagree=$(cv_disagreeing)
echo "      status:                ${a_status:-<absent>}"
echo "      agreeing-providers:    ${a_agree:-<absent>}"
echo "      disagreeing-providers: ${a_disagree:-<absent>}"
if [ "$a_status" = "success" ]; then
	pass "quorum still REACHED despite the dissenter (status=success)"
else
	fail "expected status=success (quorum among sim-1/sim-2); got '${a_status:-<absent>}'"
fi
if echo "$a_disagree" | grep -q "$DISSENTER"; then
	pass "disagreeing-providers header names the dissenter ($DISSENTER)"
else
	fail "disagreeing-providers header did not name $DISSENTER"
fi
# THE UC-4 HEADLINE: a bounded group+finality-labeled mismatch metric fired.
mismatch_line=$(curl -sS "http://127.0.0.1:$METRICS_PORT/metrics" 2>/dev/null \
	| grep '^smartrouter_cross_validation_mismatch_total' | grep "group=\"$DISSENTER_GROUP\"" | grep "method=\"$CV_METHOD\"")
echo "      ${mismatch_line:-<mismatch metric not found>}"
mismatch_val=$(echo "$mismatch_line" | awk '{print $NF}' | head -n1)
if ge1 "$mismatch_val"; then
	pass "cross_validation_mismatch_total{group=\"$DISSENTER_GROUP\"} fired ($mismatch_val)"
else
	fail "cross_validation_mismatch_total{group=\"$DISSENTER_GROUP\",method=\"$CV_METHOD\"} not >= 1"
fi
# Finality is a SECONDARY assertion: report the label, do not gate on it (it is
# 'finalized' here only because the chaintracker has the sim head; 'unknown' would
# still satisfy UC-4's "a metric is emitted").
a_finality=$(echo "$mismatch_line" | sed -n 's/.*finality="\([a-z_]*\)".*/\1/p')
echo "      finality label on the mismatch metric: ${a_finality:-<none>}"
if [ "$a_finality" = "finalized" ]; then
	pass "finality marker present and = 'finalized' (the high-signal alert case)"
elif [ -n "$a_finality" ]; then
	pass "finality marker present (= '$a_finality'); UC-4 only requires a finality label"
else
	fail "no finality label on the mismatch metric"
fi
# Per-provider dissent counter names the dissenting provider address.
dis_val=$(metric_value "provider_disagreements_total" "provider_address=\"$DISSENTER\"")
echo "      provider_disagreements_total{provider_address=\"$DISSENTER\"}: ${dis_val:-<absent>}"
if ge1 "$dis_val"; then
	pass "provider_disagreements_total identifies the dissenter ($DISSENTER)"
else
	fail "provider_disagreements_total{provider_address=\"$DISSENTER\"} not >= 1"
fi

# --- UC-6 (Outlier Excluded from Result Set): the SAME fan-out also proves the
# outlier is excluded from the RESULT (not just flagged) and that the exclusion is
# observable in the log. The outlier was made divergent via block_id.hash prefix
# $SALT_A, so the client must receive the HONEST consensus hash, never $SALT_A.
echo ""
echo "  [UC-6] outlier '$DISSENTER' must be EXCLUDED from the result + the exclusion recorded in the log"
client_hash=$("$SIM_PY" -c "import sys,json;print(json.load(open('$BODY'))['result']['block_id']['hash'])" 2>/dev/null)
honest_hash=$(clean_block "$SIM_TM_1" | "$SIM_PY" -c "import sys,json;print(json.load(sys.stdin)['result']['block_id']['hash'])" 2>/dev/null)
echo "      client block_id.hash:  ${client_hash:0:16}...   (honest: ${honest_hash:0:16}..., outlier prefix: $SALT_A)"
if [ -n "$client_hash" ] && [ "$client_hash" = "$honest_hash" ] && [ "${client_hash:0:8}" != "$SALT_A" ]; then
	pass "client received the CONSENSUS value — outlier excluded from the result set"
else
	fail "client did not receive the consensus value (got '${client_hash:0:16}...', expected honest '${honest_hash:0:16}...')"
fi
# Exclusion is observable in the log (UC-6 accepts 'metric OR log' — we have both).
# Tiny grace so the tee'd INFO line is flushed before we grep.
sleep 1
if grep -q "cross-validation outlier detected.*provider=$DISSENTER" "$LOG_FILE"; then
	pass "exclusion recorded in the log ('cross-validation outlier detected' names $DISSENTER)"
	grep "cross-validation outlier detected.*provider=$DISSENTER" "$LOG_FILE" | tail -n1 \
		| grep -o 'consensusHashHex=[^ ]*\|outlierHashHex=[^ ]*\|finality=[^ ]*' | tr '\n' ' ' | sed 's/^/        /'; echo ""
else
	fail "no 'cross-validation outlier detected' log line naming $DISSENTER in $LOG_FILE"
fi

# --- Scenario B: no quorum -> failed_total ------------------------------------
echo ""
echo "[B] NO QUORUM: 3 distinct successful '$CV_METHOD' values (threshold $CV_THRESHOLD) -> failure metric"
echo "    sim-1 clean, sim-2 and $DISSENTER each diverge differently -> no value reaches the quorum"
sim_reset
set_block_override "2" "$SALT_B2"
set_block_override "3" "$SALT_B3"    # sim-1 stays clean -> three distinct hashes
failed_before=$(metric_value "failed_total" "method=\"$CV_METHOD\""); failed_before=${failed_before:-0}
fire_block
b_status=$(cv_status); b_reason=$(cv_failure_reason)
failed_after=$(metric_value "failed_total" "method=\"$CV_METHOD\""); failed_after=${failed_after:-0}
echo "      status:         ${b_status:-<absent>}"
echo "      failure-reason: ${b_reason:-<absent>}"
echo "      failed_total{method=\"$CV_METHOD\"}: $failed_before -> $failed_after"
if [ "$b_status" = "failed" ]; then
	pass "no quorum -> status=failed (3 distinct values, threshold $CV_THRESHOLD)"
else
	fail "expected status=failed; got '${b_status:-<absent>}'"
fi
if awk "BEGIN{exit !($failed_after > $failed_before)}"; then
	pass "cross_validation_failed_total incremented ($failed_before -> $failed_after)"
else
	fail "cross_validation_failed_total did not increment (stayed $failed_after)"
fi

# --- UC-5 (Structured Signal to Client): the SAME quorum failure must reach the
# client as a STRUCTURED, actionable signal — headers + body — DISTINGUISHABLE
# from a generic upstream error. ($HDR/$BODY still hold Scenario B's failure.)
echo ""
echo "  [UC-5] the quorum failure must be a structured client signal, distinct from a generic upstream error"
http_code=$(head -n1 "$HDR" | tr -d '\r' | awk '{print $2}')
echo "      HTTP status: ${http_code:-<none>}   CV-status: $(cv_status)   failure-reason: $(cv_failure_reason)"
# 1) Non-200 HTTP status — the failure is distinguishable at the transport layer.
if [ -n "$http_code" ] && [ "$http_code" != "200" ]; then
	pass "quorum failure carries a non-200 HTTP status ($http_code)"
else
	fail "expected a non-200 HTTP status on quorum failure; got '${http_code:-<none>}'"
fi
# 2) failure-reason is a known MACHINE-READABLE enum — enough for a client routing
#    decision (structural reasons -> fall back; transient -> maybe retry).
b_reason=$(cv_failure_reason)
case "$b_reason" in
	no-agreement|diversity-unmet|insufficient-responses|insufficient-capacity|insufficient-groups|group-quorum-unmet)
		pass "failure-reason is a recognized cross-validation enum ($b_reason)" ;;
	*) fail "failure-reason '${b_reason:-<absent>}' is not a recognized cross-validation enum" ;;
esac
# 3) The error BODY also carries the structured cross-validation detail (in-band
#    signal that survives header-stripping proxies).
if grep -qi "cross-validation" "$BODY"; then
	pass "error body carries the structured cross-validation failure detail"
else
	fail "error body did not mention cross-validation"; head -c 200 "$BODY" 2>/dev/null | sed 's/^/        /'
fi
# 4) DISTINGUISHABLE from a generic upstream error: a NON-cross-validated method
#    ('$OTHER_METHOD') — even with all providers erroring — must carry NO
#    lava-cross-validation-* headers, so a client can tell the two apart by the
#    presence of the cross-validation channel alone.
curl -s -X POST "http://$SIM_CONTROL/scenario" -H 'Content-Type: application/json' \
	-d '{"providers":{"1":{"chain_family":"tendermintrpc","mode":"error"},"2":{"chain_family":"tendermintrpc","mode":"error"},"3":{"chain_family":"tendermintrpc","mode":"error"}}}' >/dev/null
curl -sS -D "$HDR" -o "$BODY" -X POST "http://127.0.0.1:$TM_PORT/" \
	-d "{\"jsonrpc\":\"2.0\",\"method\":\"$OTHER_METHOD\",\"params\":[],\"id\":1}"
if grep -qi '^lava-cross-validation' "$HDR"; then
	fail "a generic upstream error on '$OTHER_METHOD' wrongly carried cross-validation headers:"
	grep -i '^lava-cross-validation' "$HDR" | tr -d '\r' | sed 's/^/        /'
else
	pass "a generic upstream error ('$OTHER_METHOD') carries NO cross-validation headers — the quorum failure is distinguishable"
fi

rm -f "$HDR" "$BODY"

# Leave the router in the Scenario-A state (single dissenter, honest providers slowed
# so the dissent is reliably observed) so a manual 'block' curl reproduces the headline
# mismatch deterministically. The simulator stays running.
sim_reset
set_latency "1" 500
set_latency "2" 500
set_block_override "3" "$SALT_A"

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo "============================================"
echo "UC-4 + UC-5 + UC-6 result: $PASS passed, $FAIL failed   (alerting metric + outlier-excluded + structured failure signal)"
echo "============================================"
echo "Tendermint listener: http://127.0.0.1:$TM_PORT   (metrics: $METRICS_PORT)"
echo "Config:              $CONFIG_FILE"
echo "Router log:          $LOG_FILE"
echo "Simulator:           control $SIM_CONTROL, tendermintrpc $SIM_TM_1/$SIM_TM_2/$SIM_TM_3 (log: $SIM_LOG)"
echo ""
echo "🔬 Reproduce by hand (router is left in the single-dissenter state, height $HEIGHT):"
echo "  # quorum reached + sim-3 dissents -> mismatch metric:"
echo "  curl -i -X POST http://127.0.0.1:$TM_PORT/ -d '{\"jsonrpc\":\"2.0\",\"method\":\"block\",\"params\":{\"height\":\"$HEIGHT\"},\"id\":1}'"
echo "  curl -s http://127.0.0.1:$METRICS_PORT/metrics | grep -E 'cross_validation_(mismatch|provider_disagreements|failed)_total'"
echo ""
echo "  # reset the simulator to all-clean (no dissent):"
echo "  curl -s -X POST http://$SIM_CONTROL/reset/all"
echo ""
echo "📊 Logs:   tail -f $LOG_FILE"
echo "🟢 Router LEFT RUNNING (screen 'smartrouter', port $TM_PORT). Simulator also left running."
echo "✋ Stop when done:  screen -S smartrouter -X quit ;  pkill -f 'run.py'"
echo "============================================"

[ "$FAIL" -eq 0 ] && exit 0 || exit 1
