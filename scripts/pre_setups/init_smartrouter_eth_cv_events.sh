#!/bin/bash
# =============================================================================
# Boot a smart router with --debug-address so GET /debug/cross-validation-events
# (MAG-2772) can be exercised by hand, then prove it end to end.
#
# Shaped like init_smartrouter_eth.sh — same screens, same generated-config
# style, same "leave it running and print the commands" ending — with two
# deliberate differences:
#
#   1. --debug-address. That flag is what installs the cross-validation event
#      recorder (see rpcsmartrouter.Start, next to utils.EnableDebugLogBuffer).
#      Without it the endpoint is not registered at all, and a router that IS
#      serving it but never recorded answers 503 rather than an empty 200.
#
#   2. The upstreams are the provider_simulator, not public Ethereum RPC. The
#      feature under test only produces rows when providers DISAGREE, and real
#      endpoints agree on finalized state — a shared-truth fleet can never
#      manufacture a dissent. The simulator's per-method body override
#      (POST /scenario) makes one provider return a valid-but-divergent
#      eth_getBalance, which is exactly a "successful content outlier on a
#      deterministic method after quorum": the one input the mismatch surface
#      admits. If all you want is a debug-enabled router against real upstreams,
#      add --debug-address to init_smartrouter_eth.sh instead — you just will
#      not be able to make anything dissent.
#
# The three providers carry GROUP LABELS (tier-1 x2, external x1). That is not
# decoration: whether a chart's group-label actually reaches
# ProviderInfo.ProviderGroup on a live relay is the one thing the router-side Go
# tests cannot prove — they inject the group directly — so the smoke below
# asserts the label arrives on the row.
#
# Both recording paths are demonstrated, deterministically, using the
# simulator's latency knob to decide who wins the race to quorum:
#
#   reply-time  the dissenter answers FIRST, so its response is in hand when the
#               reply is built -> it lands in lava-cross-validation-disagreeing-providers.
#   straggler   the dissenter is delayed past the quorum early-exit, so it is
#               PENDING at reply time and the async watcher resolves it after.
#
# Usage:
#   scripts/pre_setups/init_smartrouter_eth_cv_events.sh     # boot + smoke, leaves router up
#   SKIP_SMOKE=1 scripts/pre_setups/init_smartrouter_eth_cv_events.sh
#   SIM_DIR=/path/to/provider_simulator scripts/pre_setups/init_smartrouter_eth_cv_events.sh
# =============================================================================
__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "$__dir"/../useful_commands.sh

LOGS_DIR=${__dir}/../../debugging/logs
mkdir -p "$LOGS_DIR"
LOGS_DIR=$(cd "$LOGS_DIR" && pwd)

PROJECT_ROOT=$(cd "${__dir}/../.." && pwd)
LOG_FILE="$LOGS_DIR/SMARTROUTER_CV_EVENTS.log"
SIM_LOG="$LOGS_DIR/CV_EVENTS_SIM.log"
rm -f "$LOG_FILE" "$SIM_LOG" 2>/dev/null || true

# --- Ports. Deliberately distinct from init_smartrouter_eth.sh (3360/7779) and
# --- test_uc4_smartrouter_sim.sh (3392/7798) so this can run alongside them.
ETH_PORT=3371          # router JSON-RPC listener
METRICS_PORT=7797      # prometheus (NOT scraped by this script — that is the point)
DEBUG_PORT=6767        # --debug-address: where /debug/cross-validation-events lives

# --- provider_simulator: the eth-sim pool, pids 1/2/3 (see its topology.py).
# Search upward for the sibling checkout rather than assuming one layout: a
# working copy nested under a per-branch directory sits two levels down from the
# workspace root, not one. SIM_DIR=... overrides the search entirely.
if [[ -z "$SIM_DIR" ]]; then
	for candidate in "$PROJECT_ROOT/.." "$PROJECT_ROOT/../.." "$PROJECT_ROOT/../../.."; do
		if [ -f "$candidate/provider_simulator/run.py" ]; then
			SIM_DIR=$(cd "$candidate/provider_simulator" && pwd)
			break
		fi
	done
	SIM_DIR="${SIM_DIR:-$(cd "$PROJECT_ROOT/.." && pwd)/provider_simulator}"
fi
SIM_CONTROL="127.0.0.1:19000"
SIM_ETH_1=18545
SIM_ETH_2=18546
SIM_ETH_3=18547
DISSENTER="eth-sim:3"          # the provider we make disagree
DISSENTER_GROUP="external"     # its group-label -> expected ProviderGroup on the row

# --- The policy under test: a plain 2-of-3 quorum on a deterministic method.
CV_METHOD="eth_getBalance"
CV_THRESHOLD=2
MAX_PART=3
# A concrete block well below the simulator's head (0x1312D00 = 20,000,000), so the
# request is FINALIZED and the row's Finality reads "finalized" rather than "unknown".
CV_BLOCK="0x1000000"
DISSENT_BALANCE="0xdeadbeef"   # the divergent-but-valid result the outlier returns

echo "============================================"
echo "MAG-2772 — GET /debug/cross-validation-events"
echo "============================================"
echo "  router listener : 127.0.0.1:$ETH_PORT      (ETH1 jsonrpc)"
echo "  debug endpoint  : 127.0.0.1:$DEBUG_PORT     (--debug-address)"
echo "  metrics         : 127.0.0.1:$METRICS_PORT     (present, deliberately unused)"
echo "  upstreams       : provider_simulator eth-sim 1/2/3 ($SIM_ETH_1/$SIM_ETH_2/$SIM_ETH_3)"
echo "  policy          : $CV_METHOD, $CV_THRESHOLD of $MAX_PART, groups tier-1 x2 + $DISSENTER_GROUP x1"
echo "============================================"
echo ""

for tool in jq curl python3; do
	command_exists "$tool" || { echo "ERROR: '$tool' is required."; exit 1; }
done
SIM_PY="$(command -v python3.12 || command -v python3)"

# -----------------------------------------------------------------------------
# Simulator control helpers. Provider keys are "pool:pid" — the control API
# rejects the older bare-pid + chain_family shape with a 400.
# -----------------------------------------------------------------------------
sim_up() { curl -s --max-time 2 "http://$SIM_CONTROL/health" >/dev/null 2>&1; }
sim_reset() { curl -s -X POST "http://$SIM_CONTROL/reset/all" >/dev/null 2>&1; }

# sim_scenario <json> : POST a scenario fragment, failing loudly on a rejected key.
sim_scenario() {
	local response
	response=$(curl -s -w '\n%{http_code}' -X POST "http://$SIM_CONTROL/scenario" \
		-H 'Content-Type: application/json' -d "$1")
	if [[ "$(printf '%s' "$response" | tail -n1)" != "200" ]]; then
		echo "  ! simulator rejected the scenario: $(printf '%s' "$response" | sed '$d')"
		return 1
	fi
}

# set_fleet <sim1_ms> <sim2_ms> <sim3_ms> <sim3_dissents:yes|no> : state the WHOLE
# fleet every time rather than mutating one provider, so no scenario inherits the
# previous one's latency or override.
#
# Latency is how this script picks the recording path deterministically. All three
# upstreams are local and answer in microseconds, so who wins the race to quorum is
# otherwise chance — and that choice is exactly what decides whether a dissent is
# seen before the reply (reply-time) or after it (straggler).
set_fleet() {
	local l1=$1 l2=$2 l3=$3 dissents=$4 sim3_responses='{}'
	if [[ "$dissents" == "yes" ]]; then
		sim3_responses="{\"$CV_METHOD\":{\"result\":\"$DISSENT_BALANCE\"}}"
	fi
	sim_scenario "{\"providers\":{
		\"eth-sim:1\":{\"latency_ms\":$l1,\"responses\":{}},
		\"eth-sim:2\":{\"latency_ms\":$l2,\"responses\":{}},
		\"$DISSENTER\":{\"latency_ms\":$l3,\"responses\":$sim3_responses}}}"
}

# -----------------------------------------------------------------------------
# Debug-endpoint helpers — the surface under test.
# -----------------------------------------------------------------------------
events() { curl -s "http://127.0.0.1:$DEBUG_PORT/debug/cross-validation-events$1"; }
events_clear() { curl -s -X POST "http://127.0.0.1:$DEBUG_PORT/debug/cross-validation-events/clear"; }

# cv_call <nonce> : one cross-validated eth_getBalance, echoing the request's lava-guid.
# The address varies per call so no response can be served from cache — a cache hit
# would skip the fan-out entirely and there would be nothing to disagree about.
cv_call() {
	local nonce=$1 addr headers
	addr=$(printf '0x%040x' "$nonce")
	headers=$(curl -s -D - -o /dev/null -X POST "http://127.0.0.1:$ETH_PORT" \
		-H 'Content-Type: application/json' \
		-d "{\"jsonrpc\":\"2.0\",\"method\":\"$CV_METHOD\",\"params\":[\"$addr\",\"$CV_BLOCK\"],\"id\":$nonce}")
	# grep -i, not awk's IGNORECASE: that is a gawk extension and macOS ships BWK awk,
	# where it silently sets an unused variable and the match never fires.
	printf '%s' "$headers" | grep -i '^lava-guid:' | tr -d '\r' | awk '{print $2}'
}

# cv_headers <nonce> : the cross-validation headers for one call, for the manual view.
cv_headers() {
	local nonce=$1 addr
	addr=$(printf '0x%040x' "$nonce")
	curl -s -D - -o /dev/null -X POST "http://127.0.0.1:$ETH_PORT" \
		-H 'Content-Type: application/json' \
		-d "{\"jsonrpc\":\"2.0\",\"method\":\"$CV_METHOD\",\"params\":[\"$addr\",\"$CV_BLOCK\"],\"id\":$nonce}" \
		| grep -i '^lava-' | sed 's/\r//' | sed 's/^/    /'
}

# -----------------------------------------------------------------------------
# Tear down only THIS script's router. The simulator is shared with other
# harnesses and may be serving them, so it is reused rather than restarted.
# -----------------------------------------------------------------------------
screen -S smartrouter-cvevents -X quit >/dev/null 2>&1 || true
killall smartrouter 2>/dev/null || true
sleep 1
screen -wipe >/dev/null 2>&1 || true

echo "[Setup] installing binaries"
make install || { echo "ERROR: make install failed"; exit 1; }

echo ""
if sim_up; then
	echo "[Setup] provider_simulator already running on $SIM_CONTROL (reusing it)"
else
	echo "[Setup] starting provider_simulator from $SIM_DIR"
	[ -f "$SIM_DIR/run.py" ] || { echo "ERROR: $SIM_DIR/run.py not found. Set SIM_DIR=... to the provider_simulator checkout."; exit 1; }
	( cd "$SIM_DIR" && nohup "$SIM_PY" -u run.py > "$SIM_LOG" 2>&1 & )
	for _ in $(seq 1 30); do sim_up && break; sleep 1; done
	sim_up || { echo "ERROR: simulator did not become ready. See $SIM_LOG"; tail -n 20 "$SIM_LOG" 2>/dev/null | sed 's/^/    /'; exit 1; }
	echo "  simulator up (control: $SIM_CONTROL, eth: $SIM_ETH_1/$SIM_ETH_2/$SIM_ETH_3)"
fi
# Clean slate so all three providers AGREE during router startup verification.
sim_reset
echo "  simulator scenarios reset (all providers clean)"

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
SPECS_DIR="$PROJECT_ROOT/specs/ethereum.json"
[ -f "$SPECS_DIR" ] || { echo "ERROR: spec not found: $SPECS_DIR"; exit 1; }

CONFIG_FILE="$PROJECT_ROOT/config/smartrouter_examples/smartrouter_eth_cv_events.yml"
CONFIG_REL="config/smartrouter_examples/smartrouter_eth_cv_events.yml"
echo ""
echo "[Setup] generating config: $CONFIG_FILE"
cat > "$CONFIG_FILE" <<EOF
# Smart Router — MAG-2772 /debug/cross-validation-events manual test config.
# Generated by: scripts/pre_setups/init_smartrouter_eth_cv_events.sh (do not hand-edit)
#
# Three ETH1 jsonrpc providers backed by the provider_simulator's eth-sim pool.
# GROUP LABELS are the point: tier-1 x2 + $DISSENTER_GROUP x1, so a dissent row can be
# checked for the label actually reaching ProviderInfo.ProviderGroup on a live relay.
# The simulator is http-only here, so the router runs with --skip-websocket-verification.
endpoints:
  - listen-address: "0.0.0.0:$ETH_PORT"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    network-address: "0.0.0.0:$ETH_PORT"

direct-rpc:
  - name: "sim-1"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    group-label: "tier-1"
    node-urls:
      - url: "http://127.0.0.1:$SIM_ETH_1"
        timeout: 10s
        skip-verifications: [chain-id, pruning]
  - name: "sim-2"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    group-label: "tier-1"
    node-urls:
      - url: "http://127.0.0.1:$SIM_ETH_2"
        timeout: 10s
        skip-verifications: [chain-id, pruning]
  - name: "sim-3"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    group-label: "$DISSENTER_GROUP"
    node-urls:
      - url: "http://127.0.0.1:$SIM_ETH_3"
        timeout: 10s
        skip-verifications: [chain-id, pruning]

# Plain cross-validation on a deterministic method — quorum of $CV_THRESHOLD, no min-groups.
# A dissent here is a SUCCESSFUL content outlier after quorum, the only input the
# mismatch surface (and therefore the event recorder) admits.
cross-validation:
  policies:
    - chain-id: ETH1
      api-interface: jsonrpc
      method: $CV_METHOD
      enabled: true
      agreement-threshold: $CV_THRESHOLD
      max-participants: $MAX_PART
EOF
echo "  config written ($(wc -c < "$CONFIG_FILE") bytes)"

# -----------------------------------------------------------------------------
# Start the router — WITH --debug-address, which is what installs the recorder.
# No --cache-be: a cache hit would short-circuit the fan-out and there would be
# no second opinion to disagree with.
# -----------------------------------------------------------------------------
echo ""
echo "[Setup] starting Smart Router (log -> $LOG_FILE)"
screen -d -m -S smartrouter-cvevents bash -c "cd \"$PROJECT_ROOT\" && source ~/.bashrc; smartrouter \
$CONFIG_REL \
--log-level debug \
--use-static-spec \"$SPECS_DIR\" \
--metrics-listen-address ':$METRICS_PORT' \
--debug-address '127.0.0.1:$DEBUG_PORT' \
--skip-websocket-verification 2>&1 | tee \"$LOG_FILE\"" && sleep 0.25

echo "[Setup] waiting for the router and the debug listener ..."
ready=0
for _ in $(seq 1 40); do
	# The debug listener answering 200 is the real readiness signal here: it proves
	# both that --debug-address took effect and that the recorder is installed (an
	# uninstalled recorder answers 503, never an empty 200).
	if [[ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$DEBUG_PORT/debug/cross-validation-events" 2>/dev/null)" == "200" ]]; then
		ready=1
		break
	fi
	if ! screen -list | grep -q "smartrouter-cvevents"; then
		echo "ERROR: the router screen exited during startup."
		tail -n 30 "$LOG_FILE" 2>/dev/null | sed 's/^/    /'
		exit 1
	fi
	sleep 1
done
if [ "$ready" -ne 1 ]; then
	echo "ERROR: /debug/cross-validation-events did not answer 200 in time."
	echo "  A 503 here means the router is up but the recorder was never installed."
	echo "  Anything else means the debug listener never came up. See $LOG_FILE"
	exit 1
fi
echo "  router ready; the recorder is live (endpoint answers 200)"

# Give the chain tracker a moment to learn the head, so Finality resolves to
# "finalized" instead of "unknown" (an unknown head is a valid answer, just a
# less interesting demo).
sleep 5

# -----------------------------------------------------------------------------
# Smoke: prove the feature end to end, then leave everything running.
# -----------------------------------------------------------------------------
FAILURES=0
check() { # check <description> <actual> <expected>
	if [[ "$2" == "$3" ]]; then
		printf '  PASS  %s\n' "$1"
	else
		printf '  FAIL  %s (got %q, want %q)\n' "$1" "$2" "$3"
		FAILURES=$((FAILURES + 1))
	fi
}

if [[ "$SKIP_SMOKE" == "1" ]]; then
	echo ""
	echo "[Smoke] skipped (SKIP_SMOKE=1)"
else
	# await_row <guid> <jq-filter-args> : poll until exactly one row matches, then echo
	# it. The straggler path resolves on a watcher goroutine after the reply has already
	# been returned, so a read taken right after the curl is racing it.
	await_row() {
		local guid=$1 extra=$2 row='{}'
		for _ in $(seq 1 25); do
			if [[ "$(events "?request_id=$guid$extra" | jq 'length')" == "1" ]]; then
				row=$(events "?request_id=$guid$extra" | jq '.[0]')
				break
			fi
			sleep 1
		done
		printf '%s' "$row"
	}

	echo ""
	echo "============================================"
	echo "SMOKE 1/3 — nobody dissents: the agreeing-straggler positive control"
	echo "============================================"
	# sim-3 late but HONEST: quorum closes on sim-1 + sim-2, the reply ships, and sim-3
	# is pending. The watcher resolves it as agreed — a row that exists precisely so a
	# test asserting "no dissent happened" has something to anchor on instead of reading
	# an empty array and hoping the request ran at all.
	set_fleet 0 0 800 no
	events_clear >/dev/null
	GUID_OK=$(cv_call 1)
	echo "  lava-guid: ${GUID_OK:-<none>}"
	check "the guid is returned so a test can correlate" "$([[ -n "$GUID_OK" ]] && echo yes || echo no)" "yes"
	ROW_OK=$(await_row "$GUID_OK" "")
	echo "$ROW_OK" | jq . | sed 's/^/    /'
	check "the agreeing straggler is recorded" "$(echo "$ROW_OK" | jq -r '.Outcome // ""')" "agreed"
	check "it came from the async path"        "$(echo "$ROW_OK" | jq -r '.Source // ""')" "straggler"
	check "an agreement never moves the counter" "$(echo "$ROW_OK" | jq -r '.MismatchCounted | tostring')" "false"
	check "its hash IS the consensus hash" \
		"$(echo "$ROW_OK" | jq -r 'if (.OutlierHash != "" and .OutlierHash == .ConsensusHash) then "yes" else "no" end')" "yes"
	check "no dissent was recorded for this request" \
		"$(events "?request_id=$GUID_OK&outcome=disagreed" | jq 'length')" "0"

	echo ""
	echo "============================================"
	echo "SMOKE 2/3 — reply-time dissent (outlier answers before the quorum closes)"
	echo "============================================"
	# The dissenter answers FIRST and the two honest providers are held back, so its
	# response is already in hand when quorum forms — the reply-time path, and the
	# provider appears in lava-cross-validation-disagreeing-providers.
	set_fleet 400 400 0 yes
	events_clear >/dev/null
	GUID_RT=$(cv_call 2)
	echo "  lava-guid: ${GUID_RT:-<none>}"
	ROW_RT=$(await_row "$GUID_RT" "")
	echo "$ROW_RT" | jq . | sed 's/^/    /'
	check "Source is reply-time"       "$(echo "$ROW_RT" | jq -r '.Source // ""')" "reply-time"
	check "Outcome is disagreed"       "$(echo "$ROW_RT" | jq -r '.Outcome // ""')" "disagreed"
	check "the dissenting provider is named" "$(echo "$ROW_RT" | jq -r '.ProviderAddress // ""')" "sim-3"
	check "ProviderGroup is the chart's label" "$(echo "$ROW_RT" | jq -r '.ProviderGroup // ""')" "$DISSENTER_GROUP"
	check "the counter moved for it"   "$(echo "$ROW_RT" | jq -r '.MismatchCounted | tostring')" "true"
	check "the row is self-describing" "$(echo "$ROW_RT" | jq -r '.ChainID + "/" + .ApiInterface')" "ETH1/jsonrpc"
	check "the outlier hash differs from consensus" \
		"$(echo "$ROW_RT" | jq -r 'if (.ConsensusHash != "" and .OutlierHash != "" and .ConsensusHash != .OutlierHash) then "yes" else "no" end')" "yes"
	check "the request is finalized, not unknown" "$(echo "$ROW_RT" | jq -r '.Finality // ""')" "finalized"

	echo ""
	echo "============================================"
	echo "SMOKE 3/3 — straggler dissent (same outlier, delayed past the early-exit)"
	echo "============================================"
	# 1500 ms comfortably outlasts the two local honest responses, so quorum closes and
	# the reply ships while the dissenter is still in flight. It is PENDING at reply
	# time — the reply-time path could never see it — and only the async watcher can
	# classify it.
	set_fleet 0 0 1500 yes
	events_clear >/dev/null
	GUID_ST=$(cv_call 3)
	echo "  lava-guid: ${GUID_ST:-<none>}"
	echo "  waiting for the straggler watcher ..."
	ROW_ST=$(await_row "$GUID_ST" "")
	echo "$ROW_ST" | jq . | sed 's/^/    /'
	check "Source is straggler"      "$(echo "$ROW_ST" | jq -r '.Source // ""')" "straggler"
	check "Outcome is disagreed"     "$(echo "$ROW_ST" | jq -r '.Outcome // ""')" "disagreed"
	check "ProviderGroup survives the async path" "$(echo "$ROW_ST" | jq -r '.ProviderGroup // ""')" "$DISSENTER_GROUP"
	check "the late dissent reached the mismatch surface" "$(echo "$ROW_ST" | jq -r '.MismatchCounted | tostring')" "true"
	check "the delay after the reply was measured" \
		"$(echo "$ROW_ST" | jq -r 'if (.DelayMs // 0) > 0 then "yes" else "no" end')" "yes"

	# Leave the fleet honest and prompt for manual poking; recorded rows are kept.
	set_fleet 0 0 0 no
	echo ""
	echo "  fleet returned to agreement (recorded rows are kept)"

	echo ""
	echo "============================================"
	if [ "$FAILURES" -eq 0 ]; then
		echo "SMOKE PASSED — both recording paths verified against a live router"
	else
		echo "SMOKE FAILED — $FAILURES check(s) did not hold. Router left running for inspection."
		echo "  logs: $LOG_FILE"
	fi
	echo "============================================"
fi

# -----------------------------------------------------------------------------
# Manual cheat sheet
# -----------------------------------------------------------------------------
cat <<EOF

============================================
Router is running — manual commands
============================================

Read every recorded event:
  curl -s http://127.0.0.1:$DEBUG_PORT/debug/cross-validation-events | jq

Filter (all optional, ANDed) — request_id is the Lava-Guid response header value,
NOT /debug/logs' request_id (that one is the caller's X-Request-Id):
  curl -s "http://127.0.0.1:$DEBUG_PORT/debug/cross-validation-events?request_id=<lava-guid>" | jq
  curl -s "http://127.0.0.1:$DEBUG_PORT/debug/cross-validation-events?chain_id=ETH1" | jq
  curl -s "http://127.0.0.1:$DEBUG_PORT/debug/cross-validation-events?outcome=disagreed" | jq
  curl -s "http://127.0.0.1:$DEBUG_PORT/debug/cross-validation-events?limit=5" | jq

See the bounded-ring accounting (evictions are reported, never silent):
  curl -si http://127.0.0.1:$DEBUG_PORT/debug/cross-validation-events | grep -i x-cross-validation

Clear between scenarios:
  curl -s -X POST http://127.0.0.1:$DEBUG_PORT/debug/cross-validation-events/clear | jq

Send a cross-validated call and keep its guid (the address must vary — a cache hit
would skip the fan-out):
  curl -s -D - -o /dev/null -X POST http://127.0.0.1:$ETH_PORT \\
    -H 'Content-Type: application/json' \\
    -d '{"jsonrpc":"2.0","method":"$CV_METHOD","params":["0x000000000000000000000000000000000000beef","$CV_BLOCK"],"id":1}' \\
    | grep -i '^lava-'

Make sim-3 dissent (reply-time row), or delay it past the quorum (straggler row):
  curl -s -X POST http://$SIM_CONTROL/scenario -H 'Content-Type: application/json' \\
    -d '{"providers":{"$DISSENTER":{"latency_ms":0,"responses":{"$CV_METHOD":{"result":"$DISSENT_BALANCE"}}}}}'
  curl -s -X POST http://$SIM_CONTROL/scenario -H 'Content-Type: application/json' \\
    -d '{"providers":{"$DISSENTER":{"latency_ms":1500,"responses":{"$CV_METHOD":{"result":"$DISSENT_BALANCE"}}}}}'

Stop dissenting:
  curl -s -X POST http://$SIM_CONTROL/scenario -H 'Content-Type: application/json' \\
    -d '{"providers":{"$DISSENTER":{"latency_ms":0,"responses":{}}}}'

The positive control (an agreeing straggler still records a row) — delay a provider
that is NOT dissenting, so it resolves late but in agreement:
  curl -s -X POST http://$SIM_CONTROL/scenario -H 'Content-Type: application/json' \\
    -d '{"providers":{"eth-sim:2":{"latency_ms":1500}}}'
  ... then send a call and read: outcome=agreed, MismatchCounted=false.

Cross-check the same dissent on the surfaces this endpoint replaces:
  curl -s http://127.0.0.1:$METRICS_PORT/metrics | grep cross_validation
  curl -s "http://127.0.0.1:$DEBUG_PORT/debug/logs?limit=200" | jq -r '.lines[].message' | grep cross-validation

Logs / teardown:
  tail -f $LOG_FILE | grep -i cross-validation
  screen -S smartrouter-cvevents -X quit ; killall smartrouter ; screen -wipe
============================================
EOF

exit $(( FAILURES > 0 ? 1 : 0 ))
