#!/bin/bash
# =============================================================================
# UC-1 (Per-Method Validation Policy) test harness — Lava via PublicNode.
#
# Goal (kept deliberately simple):
#   * ONE method is ALWAYS cross-validated by operator policy — no caller flags:
#         {"jsonrpc":"2.0","method":"block","params":{"height":"<H>"},"id":1}
#     fans out to $CV_MAXPART providers and needs $CV_THRESHOLD identical
#     responses (a "$CV_THRESHOLD of $CV_MAXPART" quorum).
#   * EVERY OTHER method is NOT cross-validated — unless the caller explicitly
#     opts in with the lava-cross-validation-* request headers.
#
# Why 'block' (and a fixed height): a block is CONSENSUS state — byte-identical
# on every node — so the fan-out responses agree and the 2-of-3 quorum forms
# deterministically. (By contrast 'status' carries node-local fields, so it can
# disagree across a load-balanced upstream.) We resolve the current latest height
# once, then query that now-settled height across all providers.
# =============================================================================
__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "$__dir"/../useful_commands.sh

LOGS_DIR=${__dir}/../../testutil/debugging/logs
mkdir -p "$LOGS_DIR"
LOGS_DIR=$(cd "$LOGS_DIR" && pwd)
rm "$LOGS_DIR"/*.log 2>/dev/null || true

PROJECT_ROOT=$(cd "${__dir}/../.." && pwd)
LOG_FILE="$LOGS_DIR/SMARTROUTER_LAVA_UC1.log"

# Ports (this script kills any prior router first). UC-1 is tendermintrpc-only.
TM_PORT=3362
CACHE_ADDR="127.0.0.1:20100"
METRICS_PORT=7779

# -----------------------------------------------------------------------------
# THE POLICY UNDER TEST. Cross-validation is enforced on exactly ONE method,
# defined here and used everywhere below — config policy, request payload, and
# metric label — so there is one obvious place that says "which method?".
# -----------------------------------------------------------------------------
CV_CHAIN="LAVA"
CV_API="tendermintrpc"
CV_METHOD="block"          # <<<< cross-validation is ENFORCED on this method (deterministic)
CV_THRESHOLD=2             # need this many identical responses ("2 of 3")
CV_MAXPART=3              # fan out to this many providers
OTHER_METHOD="health"     # a method with NO policy (control); also used to show
                          # header-driven (opt-in) cross-validation still works.

# Non-policy JSON-RPC payload (the policy payload needs a height, built post-startup).
OTHER_PAYLOAD="{\"jsonrpc\":\"2.0\",\"method\":\"$OTHER_METHOD\",\"params\":[],\"id\":1}"

print_policy_banner() {
	echo "+------------------------------------------------------------+"
	echo "| ENFORCED CROSS-VALIDATION POLICY (UC-1)                     |"
	echo "+------------------------------------------------------------+"
	printf "|  method:              %-36s|\n" "$CV_METHOD   <== always cross-validated"
	printf "|  chain / api:         %-36s|\n" "$CV_CHAIN / $CV_API"
	printf "|  quorum:              %-36s|\n" "$CV_THRESHOLD of $CV_MAXPART identical responses"
	printf "|  provider groups:     %-36s|\n" "1 (single 'default' group — no diversity)"
	printf "|  every other method:  %-36s|\n" "plain (CV only via request headers)"
	echo "+------------------------------------------------------------+"
}

echo "============================================"
echo "UC-1 — Per-Method Validation Policy (Lava via PublicNode)"
echo "============================================"
print_policy_banner
echo ""

if ! command_exists jq; then
	echo "✗ ERROR: 'jq' is required (used to resolve the latest block height). Install jq and retry."
	exit 1
fi

# --- Tear down only THIS script's previous run --------------------------------
# Quit just the smartrouter/cache screen sessions this script owns (NOT `killall
# screen`, which would nuke unrelated sessions). The router is then re-created
# below and LEFT RUNNING in the background after this script exits — it is not
# stopped at the end (use the "Stop" command in the summary to stop it yourself).
screen -S smartrouter -X quit >/dev/null 2>&1 || true
screen -S cache -X quit >/dev/null 2>&1 || true
# Free the listener ports by stopping a prior smartrouter/cache from this script
# (needed so the new router can bind). Other processes are left untouched.
killall smartrouter 2>/dev/null || true
sleep 1
screen -wipe >/dev/null 2>&1 || true
sleep 1

echo "[Setup] installing binaries"
make install

# --- Cache service ------------------------------------------------------------
echo ""
echo "[Setup] starting cache service ($CACHE_ADDR)"
screen -d -m -S cache bash -c "source ~/.bashrc; smartrouter cache \
$CACHE_ADDR --metrics_address 0.0.0.0:20200 --log_level debug 2>&1 | tee \"$LOGS_DIR/CACHE.log\"" && sleep 0.25
sleep 2

# --- Upstreams + specs --------------------------------------------------------
# Official Lava MAINNET Tendermint-RPC endpoints (lava.build). NOTE: PublicNode (the
# init script's generated upstream) returns HTTP 403 "unsupported platform" to
# non-browser clients, so it cannot be used for an automated test — these official
# endpoints are reachable and report network=lava-mainnet-1 (matches the LAVA spec).
# Override with TENDERMINTRPC_URL=... / TENDERMINTRPC_WS_URL=... to point elsewhere.
LAVA_TENDERMINTRPC_LOCAL="${TENDERMINTRPC_URL:-https://lava.tendermintrpc.lava.build:443}"
LAVA_TENDERMINTRPC_WS_LOCAL="${TENDERMINTRPC_WS_URL:-wss://lava.tendermintrpc.lava.build/websocket}"

SPECS_DIR="$PROJECT_ROOT/specs/tendermint.json,$PROJECT_ROOT/specs/ibc.json,$PROJECT_ROOT/specs/cosmossdk.json,$PROJECT_ROOT/specs/lava.json"
IFS=',' read -r -a SPEC_FILES <<< "$SPECS_DIR"
for spec_file in "${SPEC_FILES[@]}"; do
	[ -f "$spec_file" ] || { echo "✗ ERROR: Spec file not found: $spec_file"; exit 1; }
done

# --- Generate a DEDICATED UC-1 config (never clobbers smartrouter_lava.yml) ----
# UC-1 ASSUMPTION: a SINGLE provider group. The 3 Tendermint-RPC providers carry
# NO group-label, so they all fold into the implicit "default" group, and the
# policy sets NO min-groups (defaults to 1) — a plain "$CV_THRESHOLD of $CV_MAXPART"
# COUNT quorum with no diversity requirement. (Group diversity is UC-2.)
CONFIG_FILE="$PROJECT_ROOT/config/smartrouter_examples/smartrouter_lava_uc1.yml"
# The router resolves the config arg via viper.SetConfigName + AddConfigPath(".") from the working
# directory (PROJECT_ROOT), NOT as a filesystem path — so it must be passed RELATIVE to PROJECT_ROOT,
# exactly like init_smartrouter_lava.sh. An absolute path is treated as a config name and not found.
CONFIG_REL="config/smartrouter_examples/smartrouter_lava_uc1.yml"
echo ""
echo "[Setup] generating UC-1 config: $CONFIG_FILE"
cat > "$CONFIG_FILE" <<EOF
# Smart Router — UC-1 (Per-Method Validation Policy) test config.
# Generated by: scripts/pre_setups/test_uc1_smartrouter_lava.sh  (do not hand-edit)

# UC-1 is tendermintrpc-only: a single listener with 3 providers. (We deliberately
# do NOT declare rest/grpc listeners — the router exits if any declared endpoint has
# no provider that passes startup verification, and we only need tendermintrpc here.)
endpoints:
  - network-address: "0.0.0.0:$TM_PORT"
    chain-id: "LAVA"
    api-interface: "tendermintrpc"

direct-rpc:
  # 3 Tendermint-RPC providers (same upstream) so the policy fans out to $CV_MAXPART.
  # Mirrors init_smartrouter_lava.sh exactly (http + ws node-urls, skip-verifications:
  # [pruning]) — but pointed at the official mainnet endpoint instead of the blocked
  # PublicNode one. They share an upstream, so a fixed-height block is identical across
  # the fan-out and the 2-of-3 quorum forms deterministically.
  - name: "lava-tm-1"
    chain-id: "LAVA"
    api-interface: "tendermintrpc"
    node-urls:
      - url: "$LAVA_TENDERMINTRPC_LOCAL"
        timeout: 10s
        skip-verifications: [pruning]
      - url: "$LAVA_TENDERMINTRPC_WS_LOCAL"
        skip-verifications: [pruning]

  - name: "lava-tm-2"
    chain-id: "LAVA"
    api-interface: "tendermintrpc"
    node-urls:
      - url: "$LAVA_TENDERMINTRPC_LOCAL"
        timeout: 10s
        skip-verifications: [pruning]
      - url: "$LAVA_TENDERMINTRPC_WS_LOCAL"
        skip-verifications: [pruning]

  - name: "lava-tm-3"
    chain-id: "LAVA"
    api-interface: "tendermintrpc"
    node-urls:
      - url: "$LAVA_TENDERMINTRPC_LOCAL"
        timeout: 10s
        skip-verifications: [pruning]
      - url: "$LAVA_TENDERMINTRPC_WS_LOCAL"
        skip-verifications: [pruning]

# UC-1: '$CV_METHOD' is MANDATED (enabled: true) -> always cross-validated, no
# caller headers needed. Every other method is absent from this list, so it is
# only cross-validated when the caller sends the lava-cross-validation-* headers.
cross-validation:
  policies:
    - chain-id: $CV_CHAIN
      api-interface: $CV_API
      method: $CV_METHOD
      enabled: true
      agreement-threshold: $CV_THRESHOLD
      max-participants: $CV_MAXPART
EOF

echo "✓ config written ($(wc -c < "$CONFIG_FILE") bytes). The cross-validation: block:"
sed -n '/^cross-validation:/,$p' "$CONFIG_FILE" | sed 's/^/    /'

# --- Start the router ---------------------------------------------------------
echo ""
echo "[Setup] starting Smart Router (trace log -> $LOG_FILE)"
screen -d -m -S smartrouter bash -c "cd \"$PROJECT_ROOT\" && source ~/.bashrc; smartrouter \
$CONFIG_REL \
--geolocation 1 \
--log-level trace \
--cache-be \"$CACHE_ADDR\" \
--use-static-spec \"$SPECS_DIR\" \
--metrics-listen-address ':$METRICS_PORT' \
--maximum-streams-per-connection 10 \
--min-relay-timeout 5s 2>&1 | tee \"$LOG_FILE\"" && sleep 0.25

# --- Wait for the Tendermint listener to answer (probe the non-policy method) --
echo ""
echo "[Setup] waiting for the router to become ready ..."
ready=0
for _ in $(seq 1 30); do
	if curl -sS -o /dev/null "http://127.0.0.1:$TM_PORT/$OTHER_METHOD" 2>/dev/null; then
		ready=1
		break
	fi
	if ! screen -list | grep -q "smartrouter"; then
		echo "✗ ERROR: smart router screen exited during startup."
		echo "  Last lines of $LOG_FILE:"
		tail -n 25 "$LOG_FILE" 2>/dev/null | sed 's/^/    /'
		exit 1
	fi
	sleep 1
done
[ "$ready" -eq 1 ] || { echo "✗ ERROR: router did not become ready in time. See $LOG_FILE"; exit 1; }
echo "✓ router is answering on :$TM_PORT"

# Resolve the latest block height, then back off a few blocks so the chosen
# height is settled on every backend — its block is immutable and identical
# everywhere, which is what makes the cross-validation quorum deterministic.
LATEST=$(curl -sS "http://127.0.0.1:$TM_PORT/status" | jq -r '.result.sync_info.latest_block_height' 2>/dev/null)
if ! [[ "$LATEST" =~ ^[0-9]+$ ]] || [ "$LATEST" -le 5 ]; then
	echo "✗ ERROR: could not resolve a usable block height (got '$LATEST')."
	exit 1
fi
HEIGHT=$((LATEST - 5))
echo "✓ latest block height: $LATEST -> cross-validating settled height: $HEIGHT"

# Now that we have a height, build the policy-method payload.
CV_PAYLOAD="{\"jsonrpc\":\"2.0\",\"method\":\"$CV_METHOD\",\"params\":{\"height\":\"$HEIGHT\"},\"id\":1}"

# =============================================================================
# UC-1 CHECKS
# =============================================================================
PASS=0
FAIL=0
HDR=$(mktemp)
pass() { echo "  ✅ PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  ❌ FAIL: $1"; FAIL=$((FAIL + 1)); }
cv_status() { grep -i '^lava-cross-validation-status:' "$HDR" | tr -d '\r' | awk '{print $2}'; }
cv_headers_present() { grep -qi '^lava-cross-validation-' "$HDR"; }

echo ""
echo "============================================"
echo "Running UC-1 checks"
echo "============================================"
print_policy_banner

# --- Check A: the policy method is ALWAYS cross-validated (no caller headers) --
echo ""
echo "[A] '$CV_METHOD' (height $HEIGHT) must be cross-validated by POLICY (no caller headers)"
echo "    POST $CV_PAYLOAD"
curl -sS -D "$HDR" -o /dev/null -X POST "http://127.0.0.1:$TM_PORT/" -d "$CV_PAYLOAD"
status=$(cv_status)
agreeing=$(grep -i '^lava-cross-validation-agreeing-providers:' "$HDR" | tr -d '\r' | cut -d' ' -f2-)
all_providers=$(grep -i '^lava-cross-validation-all-providers:' "$HDR" | tr -d '\r' | cut -d' ' -f2-)
echo "      lava-cross-validation-status:            ${status:-<absent>}"
echo "      lava-cross-validation-all-providers:     ${all_providers:-<absent>}"
echo "      lava-cross-validation-agreeing-providers:${agreeing:-<absent>}"
if [ -n "$status" ]; then
	pass "policy mandated cross-validation on '$CV_METHOD' (status header present)"
else
	fail "no cross-validation header — policy did NOT take effect on '$CV_METHOD'"
fi
if [ "$status" = "success" ]; then
	pass "quorum reached ($CV_THRESHOLD of $CV_MAXPART, status=success)"
else
	fail "quorum NOT reached (status='${status:-<absent>}') — check upstream availability / log"
fi
if [ -n "$agreeing" ]; then
	pass "agreeing-providers header lists the quorum members"
else
	fail "agreeing-providers header empty/absent"
fi

# --- Check B: another method is NOT cross-validated without caller headers -----
echo ""
echo "[B] '$OTHER_METHOD' must NOT be cross-validated without caller headers (default)"
echo "    POST $OTHER_PAYLOAD"
curl -sS -D "$HDR" -o /dev/null -X POST "http://127.0.0.1:$TM_PORT/" -d "$OTHER_PAYLOAD"
if cv_headers_present; then
	fail "unexpected cross-validation header on non-policy method '$OTHER_METHOD':"
	grep -i '^lava-cross-validation-' "$HDR" | tr -d '\r' | sed 's/^/        /'
else
	pass "no cross-validation on '$OTHER_METHOD' (policy is scoped to '$CV_METHOD')"
fi

# --- Check C: that same method IS cross-validated WITH caller headers (opt-in) -
echo ""
echo "[C] '$OTHER_METHOD' must be cross-validated WHEN the caller sends the headers"
echo "    POST $OTHER_PAYLOAD  + lava-cross-validation-max-participants/agreement-threshold"
curl -sS -D "$HDR" -o /dev/null -X POST "http://127.0.0.1:$TM_PORT/" \
	-H "lava-cross-validation-max-participants: $CV_MAXPART" \
	-H "lava-cross-validation-agreement-threshold: $CV_THRESHOLD" \
	-d "$OTHER_PAYLOAD"
if [ -n "$(cv_status)" ]; then
	pass "header-driven cross-validation works on '$OTHER_METHOD' (status=$(cv_status))"
else
	fail "caller headers did NOT trigger cross-validation on '$OTHER_METHOD'"
fi

# --- Check D: the CV request metric incremented for method=$CV_METHOD ---------
echo ""
echo "[D] cross_validation_requests_total{method=\"$CV_METHOD\"} must be >= 1"
metric_line=$(curl -sS "http://127.0.0.1:$METRICS_PORT/metrics" 2>/dev/null \
	| grep '^lava_rpcsmartrouter_cross_validation_requests_total' | grep "method=\"$CV_METHOD\"")
metric_val=$(echo "$metric_line" | awk '{print $NF}' | head -n1)
echo "      ${metric_line:-<metric not found>}"
if [[ "$metric_val" =~ ^[0-9]+(\.[0-9]+)?$ ]] && awk "BEGIN{exit !($metric_val >= 1)}"; then
	pass "requests_total counted the cross-validated '$CV_METHOD' request ($metric_val)"
else
	fail "cross_validation_requests_total{method=\"$CV_METHOD\"} not >= 1"
fi

# --- Check E: the log shows the policy loaded and resolved ---------------------
echo ""
echo "[E] router log must show the policy loaded + cross-validation resolved by policy"
if grep -q "cross-validation per-method policies loaded" "$LOG_FILE"; then
	pass "startup log: policies loaded"
else
	fail "startup log missing the policies-loaded line"
fi
if grep -q "CrossValidation mode enabled (policy-resolved)" "$LOG_FILE"; then
	pass "request log: CrossValidation resolved by policy"
else
	fail "request log missing the policy-resolved cross-validation line"
fi

rm -f "$HDR"

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo "============================================"
echo "UC-1 result: $PASS passed, $FAIL failed   (cross-validation enforced on '$CV_METHOD')"
echo "============================================"
echo "Tendermint listener: http://127.0.0.1:$TM_PORT   (metrics: $METRICS_PORT)"
echo "Config:              $CONFIG_FILE"
echo "Log:                 $LOG_FILE"
echo ""
echo "🔬 Reproduce by hand (settled height $HEIGHT):"
echo "  # ALWAYS cross-validated by policy (no flags):"
echo "  curl -i -X POST http://127.0.0.1:$TM_PORT/ -d '$CV_PAYLOAD'"
echo "  #   ... or the URI form of the same block query:"
echo "  curl -i \"http://127.0.0.1:$TM_PORT/$CV_METHOD?height=$HEIGHT\""
echo ""
echo "  # NOT cross-validated (different method, no flags):"
echo "  curl -i -X POST http://127.0.0.1:$TM_PORT/ -d '$OTHER_PAYLOAD'"
echo ""
echo "  # cross-validated only because of the explicit caller headers:"
echo "  curl -i -X POST http://127.0.0.1:$TM_PORT/ \\"
echo "    -H 'lava-cross-validation-max-participants: $CV_MAXPART' \\"
echo "    -H 'lava-cross-validation-agreement-threshold: $CV_THRESHOLD' \\"
echo "    -d '$OTHER_PAYLOAD'"
echo ""
echo "  # the CV counters:"
echo "  curl -s http://127.0.0.1:$METRICS_PORT/metrics | grep cross_validation"
echo ""
echo "📊 Logs:   tail -f $LOG_FILE"
echo "🟢 Router LEFT RUNNING in the background (screen session 'smartrouter', port $TM_PORT)."
echo "   Attach:  screen -r smartrouter      Re-test: ./scripts/pre_setups/uc1_curls.sh"
echo "✋ Stop when done:  screen -S smartrouter -X quit; screen -S cache -X quit"
echo "============================================"

[ "$FAIL" -eq 0 ] && exit 0 || exit 1
