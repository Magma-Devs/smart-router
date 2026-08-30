#!/bin/bash
# =============================================================================
# ChainTracker / ChainState live-scenario harness — smart-router driven against
# the provider_simulator (a fault-injectable mock backend).
#
# WHAT THIS COVERS
# The Topic C tip stack: the self-healing per-chain ChainState tip (T3, PR #233),
# the block-monotonic per-endpoint endpointtip store (T4), and the shared-state
# chain-level tip (T10, PR #228). Unit tests pin each layer in isolation; this
# harness exercises how they COMPOSE — which is where the real latencies and
# failure modes live. Two examples the unit suites structurally cannot show:
#
#   * A chain-wide downward move is gated TWICE — first by the per-endpoint store's
#     staleness backstop, only then by ChainState's own TTL. The end-to-end heal
#     time (~130 s on ETH1) is a property of the pair, not of either layer.
#   * A pod can silently degrade below the min-2 consensus rule, which switches the
#     consensus-anchored anti-lie guard off entirely. No single-layer test sees it.
#
# WHY THE SIMULATOR AND NOT REAL NODES
# Every scenario needs an endpoint to LIE about its block height, or to lag by an
# exact number of blocks, on demand. Real upstreams cannot be asked to do that. The
# simulator gives both primitives per-provider:
#     lie high :  {"providers":{"eth-sim:3":{"responses":{"eth_blockNumber":{"result":"0x..."}}}}}
#     lag      :  {"providers":{"eth-sim:3":{"blocks_behind":N}}}
#
# DERIVED CONSTANTS (ETH1, average_block_time = 13000 ms)
# Nothing here is a flat constant — every threshold is derived from block time, so
# the numbers below are ETH1-specific and change per chain:
#     TTL / staleness window = max(10 x 13 s, 2 s)          = 130 s
#     outlier threshold      = clamp(1200 s / 13 s, 32, 512) = 92 blocks
#     poll cadence           = 13 s / 2                      = 6.5 s
#     consensus bucket width = 5 blocks   (compile-time, widened 2->5 in a61aa6e)
#     probe loop cadence     = 5 s
#
# RUNTIME
#   --fast   skips the two scenarios that must wait real time (~3 min saved).
#   Full run is ~10-11 min. The two long poles are the 130 s staleness window in
#   scenario 5 and scenario 6's cold-start recovery, which must outwait the 3 min
#   retryFailedStaticProviders ticker before the restored endpoints rejoin the pairing.
#   NOTE: /debug/chain-state-time-warp CANNOT accelerate that window — the
#   endpointtip store measures staleness as a delta between OBSERVATION STAMPS,
#   not against a clock, so the warp never reaches it. Use a fast chain (Solana,
#   400 ms -> ~4 s window) if you need that scenario to be quick.
#
# KNOWN-ISSUE CHECKS
# Scenario 7 asserts CURRENT (incorrect) behaviour and is tallied as KNOWN, not FAIL,
# so this script stays green in CI. When it flips to "FIXED", the underlying bug has
# been fixed and the check should be promoted to a hard assert.
# Scenario 6 was one of these (MAG-2622) and has been promoted: it is now a hard assert.
# =============================================================================
set -uo pipefail
__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "$__dir"/../useful_commands.sh

PROJECT_ROOT=$(cd "${__dir}/../.." && pwd)
# debugging/ is gitignored — every artifact this script produces stays there.
WORK_DIR="$PROJECT_ROOT/debugging"
LOGS_DIR="$WORK_DIR/logs"
mkdir -p "$LOGS_DIR"

LOG_FILE="$LOGS_DIR/CHAINSTATE_ROUTER.log"
SIM_LOG="$LOGS_DIR/CHAINSTATE_SIM.log"
CONFIG_FILE="$WORK_DIR/chainstate_sim.yml"
# The router resolves its config argument against a fixed set of search roots
# (repo root, repo/config, ~/.smart-router), so an absolute path outside those roots is
# reported as "not found". Pass it repo-relative and run with cwd = repo root.
CONFIG_REL="debugging/chainstate_sim.yml"
# Built into the working tree rather than installed to GOPATH/bin: the harness must
# test THIS checkout, not whatever binary happens to be on PATH, and it must not
# clobber the developer's installed smartrouter.
BIN="$WORK_DIR/testbin/smartrouter"

SIM_DIR="${SIM_DIR:-$(cd "$PROJECT_ROOT/.." && pwd)/provider_simulator}"
SIM_CONTROL="127.0.0.1:19000"
# ETH JSON-RPC primary pool (constants.ETH_PRIMARY_PORTS). Provider keys are
# "pool:pid" — the bare-pid form is rejected by current simulator builds.
SIM_1=18545; SIM_2=18546; SIM_3=18547
P1="eth-sim:1"; P2="eth-sim:2"; P3="eth-sim:3"

# Router ports (distinct from UC-1/UC-2/UC-4 so this can run alongside them).
LISTEN_PORT=3395
METRICS_PORT=7799
DEBUG_PORT=9999

# ETH1-derived constants asserted below (see header).
TTL_SECONDS=130
OUTLIER_THRESHOLD=92
POLL_MS=6500
SIM_HEAD=20000000

FAST=0
[[ "${1:-}" == "--fast" ]] && FAST=1

PASS=0; FAIL=0; KNOWN=0
pass()  { echo "  ✅ PASS:  $1"; PASS=$((PASS + 1)); }
fail()  { echo "  ❌ FAIL:  $1"; FAIL=$((FAIL + 1)); }
known() { echo "  ⚠️  KNOWN: $1"; KNOWN=$((KNOWN + 1)); }
fixed() { echo "  🎉 FIXED: $1"; PASS=$((PASS + 1)); }

for tool in jq python3 curl; do
	command_exists "$tool" || { echo "✗ ERROR: '$tool' is required."; exit 1; }
done
SIM_PY="$(command -v python3.12 || command -v python3)"

# -----------------------------------------------------------------------------
# Simulator + router control helpers
# -----------------------------------------------------------------------------
sim_up()    { curl -s --max-time 2 "http://$SIM_CONTROL/health" >/dev/null 2>&1; }
sim_reset() { curl -s -X POST "http://$SIM_CONTROL/reset/all" >/dev/null 2>&1; }
sim_post()  { curl -s -X POST "http://$SIM_CONTROL/scenario" -H 'Content-Type: application/json' -d "$1" >/dev/null; }

# lie_high <provider-key> <decimal-block> : pin that provider's eth_blockNumber.
lie_high()  { sim_post "{\"providers\":{\"$1\":{\"responses\":{\"eth_blockNumber\":{\"result\":\"$(printf '0x%x' "$2")\"}}}}}"; }
# lag <provider-key> <blocks> : that provider reports head-N.
lag()       { sim_post "{\"providers\":{\"$1\":{\"blocks_behind\":$2}}}"; }
set_mode()  { sim_post "{\"providers\":{\"$1\":{\"mode\":\"$2\"}}}"; }

# cs <field> : one field of the (single-chain) /debug/chain-state row.
cs() { curl -s "http://127.0.0.1:$DEBUG_PORT/debug/chain-state" | jq -r ".[0].$1"; }
# ep <port> <field> : one field of the /debug/endpoint-state row for that URL.
ep() { curl -s "http://127.0.0.1:$DEBUG_PORT/debug/endpoint-state" \
	| jq -r --arg u "http://127.0.0.1:$1" '.[] | select(.NetworkAddress==$u) | .'"$2"; }
# metric <full-metric-name> : the scalar value, or "" when absent.
metric() { curl -s "http://127.0.0.1:$METRICS_PORT/metrics" | grep "^$1{" | awk '{print $NF}' | head -n1; }
# metric_int <full-metric-name> : same, normalised out of Prometheus 2e+07 notation.
metric_int() { local v; v=$(metric "$1"); [ -z "$v" ] && { echo ""; return; }; python3 -c "print(int(float('$v')))"; }

warp()      { curl -s -X POST "http://127.0.0.1:$DEBUG_PORT/debug/chain-state-time-warp" \
	-H 'Content-Type: application/json' -d "{\"offset_seconds\":$1}" >/dev/null; }
reset_all() { curl -s -X POST "http://127.0.0.1:$DEBUG_PORT/debug/reset-all" >/dev/null; }
relay()     { curl -s --max-time 8 -X POST "http://127.0.0.1:$LISTEN_PORT" -H 'Content-Type: application/json' \
	-d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'; }

# sim_calls : total upstream calls the eth-sim pool has served (probe-silence proof).
sim_calls() { curl -s "http://$SIM_CONTROL/stats" | jq '[.providers | to_entries[] | select(.key|startswith("eth-sim:")) | .value.total_calls] | add // 0'; }

# stop_router : kill every smartrouter built into this repo's debugging/testbin.
#
# Matches on the "debugging/testbin/smartrouter" SUFFIX, not on "$BIN" (the absolute path):
# pkill -f matches the literal command line, so a router someone launched by RELATIVE path
# ("./debugging/testbin/smartrouter ...") survives an absolute-path pattern and keeps running
# alongside the harness's own. Two routers polling the same simulator pool DOUBLE the upstream
# call volume, which fails scenario 3 (probe silence) with a number that looks like a probe-loop
# regression, and lets later scenarios read a router that never cold-started. The suffix pattern
# catches both spellings; the post-check below catches anything else still holding our ports.
stop_router() {
	pkill -f "debugging/testbin/smartrouter" 2>/dev/null
	sleep 2
	local stray
	stray=$(pgrep -f "debugging/testbin/smartrouter" | tr '\n' ' ')
	if [ -n "${stray// /}" ]; then
		echo "  ⚠️  stray smartrouter still running after stop (pids: $stray) — killing hard"
		pkill -9 -f "debugging/testbin/smartrouter" 2>/dev/null
		sleep 1
	fi
	# Belt and braces: whatever still holds the debug port would make the next router's
	# readiness probe answer from the WRONG process, silently invalidating every assertion.
	local holder
	holder=$(lsof -ti:"$DEBUG_PORT" 2>/dev/null | tr '\n' ' ')
	if [ -n "${holder// /}" ]; then
		echo "  ⚠️  port $DEBUG_PORT still held by pid(s) $holder — killing so assertions read OUR router"
		kill -9 $holder 2>/dev/null
		sleep 1
	fi
}

# start_router [log-suffix] : boot the router and block until it answers.
# A restart is the ONLY way to get a genuine cold start: the endpointtip store is
# process-global and /debug/reset-all does not clear it (see scenario 7).
start_router() {
	local suffix="${1:-}"
	[ -n "$suffix" ] && mv "$LOG_FILE" "$LOGS_DIR/CHAINSTATE_ROUTER_$suffix.log" 2>/dev/null
	( cd "$PROJECT_ROOT" && nohup "$BIN" "$CONFIG_REL" \
		--log-level debug \
		--use-static-spec "$PROJECT_ROOT/specs/ethereum.json" \
		--metrics-listen-address ":$METRICS_PORT" \
		--debug-address ":$DEBUG_PORT" \
		--skip-websocket-verification \
		--min-relay-timeout 5s > "$LOG_FILE" 2>&1 & )
	for _ in $(seq 1 40); do
		curl -s --max-time 2 -o /dev/null "http://127.0.0.1:$DEBUG_PORT/debug/chain-state" 2>/dev/null && return 0
		sleep 1
	done
	echo "✗ ERROR: router did not become ready. See $LOG_FILE"; tail -n 25 "$LOG_FILE" | sed 's/^/    /'; exit 1
}

# await_baseline <timeout-s> : block until a consensus baseline exists.
await_baseline() {
	for _ in $(seq 1 "$1"); do [ "$(cs HasBaseline)" = "true" ] && return 0; sleep 1; done
	return 1
}
# await_tip <block> <timeout-s> : block until the observed tip equals <block>.
await_tip() {
	for _ in $(seq 1 "$2"); do [ "$(cs ObservedTip)" = "$1" ] && return 0; sleep 1; done
	return 1
}

# await_epoch_runway <seconds> : block until the next epoch boundary is at least
# <seconds> away.
#
# Epoch numbers are WALL-CLOCK derived, not uptime derived — common/EpochTimer computes
# floor((now - 2024-01-01T00:00:00Z) / 15m), and that reference instant is an exact
# multiple of 900s, so boundaries land on :00/:15/:30/:45 no matter when the router
# started. Any scenario that deliberately holds an endpoint DOWN across a boundary gets
# an unrelated epoch re-verify: applyReverification sees the 503, DEMOTES the provider
# out of the pairing, and cleanupStaleTrackers drops its ChainTracker. The endpoint then
# vanishes from /debug/endpoint-state entirely (that handler iterates the live pairing),
# so ep() returns "" and any numeric comparison on it dies with
# "integer expression expected" — a confusing way to learn the run simply started at an
# awkward minute. Demotion is correct behaviour; it just isn't what those scenarios are
# measuring, so they wait it out instead.
await_epoch_runway() {
	local need="$1" left
	left=$(( 900 - ($(date +%s) % 900) ))
	if [ "$left" -lt "$need" ]; then
		echo "      epoch boundary in ${left}s but this scenario needs ${need}s of runway — waiting it out..."
		sleep $(( left + 3 ))
	fi
}

echo "==================================================================="
echo "ChainTracker / ChainState live scenarios — provider_simulator"
echo "==================================================================="
echo "  chain ETH1 (average_block_time 13000ms) -> TTL ${TTL_SECONDS}s | outlier ${OUTLIER_THRESHOLD} blocks | poll ${POLL_MS}ms"
[ "$FAST" -eq 1 ] && echo "  --fast: skipping the two real-time-bound scenarios (5, 8)"
echo ""

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
echo "[Setup] building smartrouter from the working tree -> $BIN"
mkdir -p "$WORK_DIR/testbin"
( cd "$PROJECT_ROOT" && go build -o "$BIN" ./cmd/smartrouter ) || { echo "✗ ERROR: build failed"; exit 1; }

if sim_up; then
	echo "[Setup] provider_simulator already running on $SIM_CONTROL (reusing it)"
else
	echo "[Setup] starting provider_simulator from $SIM_DIR"
	[ -f "$SIM_DIR/run.py" ] || { echo "✗ ERROR: $SIM_DIR/run.py not found. Set SIM_DIR=... to the checkout."; exit 1; }
	( cd "$SIM_DIR" && nohup "$SIM_PY" -u run.py > "$SIM_LOG" 2>&1 & )
	for _ in $(seq 1 30); do sim_up && break; sleep 1; done
	sim_up || { echo "✗ ERROR: simulator did not start. See $SIM_LOG"; exit 1; }
fi
sim_reset
echo "✓ simulator ready (eth-sim pool on $SIM_1/$SIM_2/$SIM_3, head $SIM_HEAD)"

cat > "$CONFIG_FILE" <<EOF
# Smart Router — ChainTracker/ChainState scenario config.
# Generated by scripts/pre_setups/test_chaintracker_chainstate_sim.sh (do not hand-edit).
#
# THREE endpoints is deliberate and load-bearing: chainstate's consensus needs a
# strict majority with a minimum of 2 agreeing URLs, so 3 is the smallest pool that
# can outvote a single liar. Drop to 2 and no baseline can ever form, which switches
# the consensus-anchored anti-lie guard off — the condition scenario 6 exploits.
endpoints:
  - network-address: "0.0.0.0:$LISTEN_PORT"
    chain-id: "ETH1"
    api-interface: "jsonrpc"

direct-rpc:
$(for i in 1 2 3; do
	port=$(eval echo \$SIM_$i)
	cat <<PROVIDER
  - name: "sim-$i"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    node-urls:
      - url: "http://127.0.0.1:$port"
        timeout: 10s
        skip-verifications: [chain-id, pruning, tx-indexing]
PROVIDER
done)
EOF
echo "✓ config written ($CONFIG_FILE)"

stop_router
echo "[Setup] starting router"
start_router
echo "✓ router ready (listener :$LISTEN_PORT, metrics :$METRICS_PORT, debug :$DEBUG_PORT)"

# =============================================================================
# 1 — Baseline: consensus forms, and every derived constant is what we think
# =============================================================================
echo ""
echo "[1] BASELINE — 3 agreeing endpoints form a strict-majority consensus"
await_baseline 60 || true
tip=$(cs ObservedTip); base=$(cs ConsensusBaseline); poll=$(ep $SIM_1 PollIntervalMs)
echo "      tip=$tip baseline=$base hasBaseline=$(cs HasBaseline) pollMs=$poll"
[ "$tip" = "$SIM_HEAD" ] && pass "observed tip is the simulator head ($SIM_HEAD)" \
	|| fail "observed tip $tip != $SIM_HEAD"
[ "$(cs HasBaseline)" = "true" ] && pass "consensus baseline formed (min-2 + >50% satisfied)" \
	|| fail "no consensus baseline formed from 3 agreeing endpoints"
# Pins the derived cadence: a regression in the block-time derivation shows up here
# before it shows up as a subtle freshness bug.
[ "$poll" = "$POLL_MS" ] && pass "poll cadence is average_block_time/2 (${POLL_MS}ms)" \
	|| fail "poll cadence $poll != $POLL_MS"

# =============================================================================
# 2 — The T3 headline: a lying-high endpoint cannot move the router-wide tip
# =============================================================================
echo ""
echo "[2] ANTI-LIE GUARD — one endpoint claims head+1,000,000 (>> outlier threshold $OUTLIER_THRESHOLD)"
LIE=$((SIM_HEAD + 1000000))
lie_high "$P3" "$LIE"
sleep 22
tip=$(cs ObservedTip); base=$(cs ConsensusBaseline)
router_metric=$(metric_int smartrouter_latest_block)
ep3_metric=$(curl -s "http://127.0.0.1:$METRICS_PORT/metrics" | grep '^rpc_endpoint_latest_block' | grep 'sim-3' | awk '{print $NF}' | head -n1)
ep3_metric=$(python3 -c "print(int(float('${ep3_metric:-0}')))")
echo "      tip=$tip baseline=$base | smartrouter_latest_block=$router_metric | rpc_endpoint_latest_block{sim-3}=$ep3_metric"
[ "$tip" = "$SIM_HEAD" ] && pass "chain tip rejected the lie (still $SIM_HEAD)" \
	|| fail "chain tip moved to $tip — the outlier guard did not hold"
[ "$base" = "$SIM_HEAD" ] && pass "consensus baseline unmoved — the outlier did not define it" \
	|| fail "baseline moved to $base"
# The scope split T3 designed: the router-wide gauge follows the GUARDED tip, while
# the per-endpoint gauge honestly reports that endpoint's own (lying) view.
[ "$router_metric" = "$SIM_HEAD" ] && pass "smartrouter_latest_block follows the GUARDED tip, not the raw harvest" \
	|| fail "smartrouter_latest_block=$router_metric leaked the unguarded value"
[ "$ep3_metric" = "$LIE" ] && pass "rpc_endpoint_latest_block{sim-3} reports the endpoint's OWN view ($LIE)" \
	|| fail "per-endpoint metric $ep3_metric did not record sim-3's own value"
sim_reset

# =============================================================================
# 3 — The prober is a read-only decision plane: zero upstream calls
# =============================================================================
echo ""
echo "[3] PROBE SILENCE — the probe loop (5s) must make NO upstream calls"
sleep 8
before=$(sim_calls); sleep 26; after=$(sim_calls); delta=$((after - before))
# A poll CYCLE is two calls (eth_blockNumber + eth_getBlockByNumber), so 26s at a
# 6.5s cadence across 3 endpoints is ~24. If the prober also hit the wire we would
# see roughly a further 5s-cadence worth (~15 more) on top.
echo "      upstream calls over 26s across 3 endpoints: $delta (poll-only ~24, poll+probe ~39)"
if [ "$delta" -le 30 ]; then
	pass "call volume matches the poll cadence alone — probe loop is in-memory only"
else
	fail "call volume $delta is consistent with the probe loop hitting the wire"
fi
# Stronger than counting: prove every call lands on a poll boundary as a method PAIR.
pattern=$(curl -s "http://$SIM_CONTROL/history" | python3 -c "
import sys,json,collections
h=[e for e in json.load(sys.stdin)['history'] if e['pool']=='eth-sim'][-30:]
print(','.join(sorted(collections.Counter(e['method'] for e in h))))")
[ "$pattern" = "eth_blockNumber,eth_getBlockByNumber" ] \
	&& pass "only poll methods on the wire ($pattern) — no probe-originated traffic" \
	|| fail "unexpected methods on the wire: $pattern"

# =============================================================================
# 4 — TTL freshness gate, driven by the ChainState-only debug warp
# =============================================================================
echo ""
echo "[4] TTL GATE — warp +$((TTL_SECONDS + 70))s must make the gated getters report not-found"
raw_before=$(cs ObservedTip)
warp $((TTL_SECONDS + 70))
tf=$(cs TipFresh); bf=$(cs BaselineFresh); raw_after=$(cs ObservedTip)
echo "      TipFresh=$tf BaselineFresh=$bf | raw ObservedTip preserved: $raw_before -> $raw_after"
[ "$tf" = "false" ] && [ "$bf" = "false" ] && pass "TipFresh and BaselineFresh both false past TTL" \
	|| fail "expected both false past TTL; got TipFresh=$tf BaselineFresh=$bf"
# The raw value must survive: a stale tip reports (0,false) to GATED readers, but
# GetLatestBlockAllowStale (archive routing) still needs the last-known head.
[ "$raw_after" = "$raw_before" ] && pass "raw tip preserved under warp (allow-stale readers keep a head)" \
	|| fail "raw tip changed under warp: $raw_before -> $raw_after"
warp 0
sleep 1
# Clearing the warp must restore freshness IMMEDIATELY. If stored timestamps had been
# written against the warped clock they would now be future-dated and read as
# artificially fresh or stale — this is the MAG-2307 "store real / compare warped" fix.
[ "$(cs TipFresh)" = "true" ] && pass "freshness restored on warp reset — no future-dated timestamps stored" \
	|| fail "tip still stale after warp reset — a warped timestamp was persisted"

# =============================================================================
# 5 — The T3 headline the retired bootstrap atomic could never do: heal DOWN
# =============================================================================
if [ "$FAST" -eq 1 ]; then
	echo ""; echo "[5] DOWNWARD HEAL — skipped (--fast)"
else
	echo ""
	echo "[5] DOWNWARD HEAL — all endpoints drop 200 blocks; tip must follow DOWN"
	echo "    Gated twice: the per-endpoint store rejects a lower block until its own"
	echo "    stamp goes stale (${TTL_SECONDS}s), and only then can ChainState re-adopt."
	DROPPED=$((SIM_HEAD - 200))
	lag "$P1" 200; lag "$P2" 200; lag "$P3" 200
	echo "      endpoints now report $DROPPED; waiting out the ${TTL_SECONDS}s staleness window..."
	if await_tip "$DROPPED" 200; then
		pass "tip healed DOWN to $DROPPED (the retired bootstrap atomic could not)"
	else
		fail "tip did not heal down within 200s (currently $(cs ObservedTip))"
	fi
	sim_reset
	# Let the endpoints climb back before the cold-start scenarios.
	await_tip "$SIM_HEAD" 200 >/dev/null 2>&1 || true
fi

# =============================================================================
# 6 — MAG-2622 regression: an endpoint down at boot must still end up polling
# =============================================================================
# Was a KNOWN check; promoted to a hard assert when MAG-2622 was fixed
# (initializeChainTrackers became a reconcile LOOP instead of a one-shot startup pass).
#
# Recovery here is two BOUNDED stages, and the wait below has to cover both:
#   1. re-admission to the pairing  — retryFailedStaticProviders, a 3m ticker. An
#      endpoint down at boot fails startup verification and is held out of the pairing
#      entirely, which is why the boot log reads "failed=0 success=1": skipped, not failed.
#   2. tracker registration         — the reconcile loop, chainTrackerReconcileInterval (15s).
# Stage 2 was the unbounded one: before the fix the endpoint was re-admitted, looked
# healthy in ValidAddresses, and still never polled for the life of the process.
COLD_RECOVERY_TIMEOUT=260 # 3m re-admission + 15s reconcile + margin
echo ""
echo "[6] MAG-2622 — an endpoint DOWN at boot must acquire a ChainTracker once it returns"
echo "    Take 2 of 3 endpoints down, cold-start the router, then restore them."
echo "    Required: they start polling, a baseline forms, and the poisoned tip is corrected."
set_mode "$P1" down; set_mode "$P2" down
COLD_LIE=$((SIM_HEAD + 5000000))
lie_high "$P3" "$COLD_LIE"
stop_router
start_router "run_coldstart"
sleep 25
cold_tip=$(cs ObservedTip)
echo "      after cold start with only the liar reachable: tip=$cold_tip hasBaseline=$(cs HasBaseline)"
# The cold-start hole itself is DOCUMENTED and accepted (SetLatestBlock cannot guard
# the very first observation — no reference exists). It is only a problem when the
# self-heal never arrives, which is what the rest of this scenario now asserts.
[ "$cold_tip" = "$COLD_LIE" ] && pass "cold-start lie accepted, as SetLatestBlock documents (no reference yet)" \
	|| echo "      (cold-start lie not reproduced this run — tip=$cold_tip; timing-dependent)"

set_mode "$P1" success; set_mode "$P2" success
echo "      restored sim-1/sim-2; waiting up to ${COLD_RECOVERY_TIMEOUT}s for re-admission + tracker reconcile..."
recovered=0
for i in $(seq 1 "$COLD_RECOVERY_TIMEOUT"); do
	p1_poll=$(ep $SIM_1 PollIntervalMs)
	[ -n "$p1_poll" ] && [ "$p1_poll" != "0" ] && [ "$(ep $SIM_1 LatestBlock)" != "0" ] && { recovered=$i; break; }
	sleep 1
done
p1_poll=$(ep $SIM_1 PollIntervalMs); p1_block=$(ep $SIM_1 LatestBlock)
echo "      sim-1: PollIntervalMs=$p1_poll LatestBlock=$p1_block | hasBaseline=$(cs HasBaseline) tip=$(cs ObservedTip)"
if [ "$recovered" -gt 0 ]; then
	pass "sim-1 acquired a tracker after boot and polls at ${p1_poll}ms holding block $p1_block (t+${recovered}s)"
	# With 2 of 3 honest endpoints polling again the min-2 rule is satisfied, so the
	# consensus-anchored guard comes back on and Recompute can snap the poisoned tip down.
	await_baseline 60 && pass "baseline re-formed — the consensus anti-lie guard is back on" \
		|| fail "sim-1 polls but no baseline formed within 60s"
else
	fail "sim-1 never started polling (PollIntervalMs=$p1_poll) despite being reachable again — MAG-2622 regression"
	echo "         -> a nominal 3-endpoint pod is really a 1-endpoint pod: no baseline can form,"
	echo "            the anti-lie guard is OFF, and the cold-start lie is permanent."
	echo "            Check PollIntervalMs in /debug/endpoint-state before trusting HasBaseline."
fi

# =============================================================================
# 7 — KNOWN ISSUE: /debug/reset-all does not clear the endpointtip store
# =============================================================================
echo ""
echo "[7] KNOWN ISSUE — /debug/reset-all clears ChainState but not the endpointtip store"
echo "    Endpoints are held DOWN so they CANNOT re-report; anything that comes back"
echo "    after the reset must have come from data that survived it."
sim_reset
stop_router; start_router "run_resetall"
await_baseline 60 || true
pre_tip=$(cs ObservedTip)
set_mode "$P1" down; set_mode "$P2" down; set_mode "$P3" down
sleep 3
reset_all
echo "      immediately after reset-all: tip=$(cs ObservedTip) initialized=$(cs Initialized)"
sleep 20
post_tip=$(cs ObservedTip); post_base=$(cs ConsensusBaseline)
echo "      t+20s (all endpoints still DOWN): tip=$post_tip baseline=$post_base"
if [ "$post_base" = "$pre_tip" ] && [ "$pre_tip" != "0" ]; then
	known "baseline re-formed at the pre-reset block $pre_tip from surviving endpointtip data"
	echo "         -> reset-all reports 'cleared:[...,chain-state,...]' but the per-endpoint"
	echo "            tips that FEED it survive for up to ${TTL_SECONDS}s. Use a process restart"
	echo "            for a genuine cold start, not reset-all."
else
	fixed "reset-all now clears the endpointtip store too (baseline stayed cleared)"
fi
sim_reset

# =============================================================================
# 8 — Contrast: an endpoint down AFTER startup recovers correctly
# =============================================================================
if [ "$FAST" -eq 1 ]; then
	echo ""; echo "[8] BACKOFF RECOVERY — skipped (--fast)"
else
	echo ""
	echo "[8] BACKOFF RECOVERY — the contrast that isolates scenario 6's trigger"
	echo "    Same endpoint, same outage, but the tracker already EXISTS. Must recover."
	stop_router; start_router "run_backoff"
	await_baseline 60 || true
	# This scenario needs sim-2 to stay PAIRED for its whole outage — the point is that an
	# EXISTING tracker backs off and recovers, which cannot be observed once the endpoint has
	# left the pairing (no /debug/endpoint-state row at all).
	#
	# Belt and braces since the MAG-2445 fix: demotion now needs the failure to persist across
	# reverifyDemoteThreshold=2 consecutive epoch ticks, and this window (~130s) cannot span two
	# boundaries 900s apart — so one boundary mid-outage is now survivable. The guard stays
	# because it costs nothing and keeps the scenario honest if that threshold is ever tuned
	# back to 1.
	await_epoch_runway 150
	set_mode "$P2" down
	sleep 40
	down_poll=$(ep $SIM_2 PollIntervalMs); down_fails=$(ep $SIM_2 ConsecutivePollFailures)
	echo "      during outage: PollIntervalMs=$down_poll (backed off from $POLL_MS) fails=$down_fails"
	if [ -z "$down_poll" ]; then
		fail "sim-2 left the pairing during the outage (no endpoint-state row) — expected it to stay paired and back off"
	elif [ "$down_poll" -gt "$POLL_MS" ]; then
		pass "poll interval backed off exponentially during the outage"
	else
		fail "expected backoff above ${POLL_MS}ms; got $down_poll"
	fi
	set_mode "$P2" success
	echo "      restored; waiting for the poll cadence to return (BACKOFF_MAX_TIME is 1m)..."
	recovered=0
	for _ in $(seq 1 90); do
		[ "$(ep $SIM_2 PollIntervalMs)" = "$POLL_MS" ] && [ "$(ep $SIM_2 LatestBlock)" != "0" ] && { recovered=1; break; }
		sleep 1
	done
	if [ "$recovered" -eq 1 ]; then
		pass "an endpoint down AFTER startup fully recovers — isolates 'down at init' as the trigger"
	else
		fail "endpoint did not recover: PollIntervalMs=$(ep $SIM_2 PollIntervalMs) block=$(ep $SIM_2 LatestBlock)"
	fi
fi

# -----------------------------------------------------------------------------
# Leave the environment healthy and RUNNING for manual poking (repo convention).
# -----------------------------------------------------------------------------
sim_reset
stop_router; start_router "run_final"
await_baseline 60 >/dev/null 2>&1 || true

echo ""
echo "==================================================================="
echo "Result: $PASS passed, $FAIL failed, $KNOWN known-issue"
echo "==================================================================="
echo "  Router:    http://127.0.0.1:$LISTEN_PORT   metrics :$METRICS_PORT   debug :$DEBUG_PORT"
echo "  Simulator: control $SIM_CONTROL, eth-sim on $SIM_1/$SIM_2/$SIM_3"
echo "  Config:    $CONFIG_FILE"
echo "  Logs:      $LOG_FILE  (per-scenario restarts saved as *_run*.log)"
echo ""
echo "🔬 Poke it by hand:"
echo "  curl -s http://127.0.0.1:$DEBUG_PORT/debug/chain-state    | jq"
echo "  curl -s http://127.0.0.1:$DEBUG_PORT/debug/endpoint-state | jq"
echo "  curl -s http://127.0.0.1:$METRICS_PORT/metrics | grep -E 'latest_block'"
echo "  # make sim-3 lie:"
echo "  curl -s -X POST http://$SIM_CONTROL/scenario -H 'Content-Type: application/json' \\"
echo "    -d '{\"providers\":{\"$P3\":{\"responses\":{\"eth_blockNumber\":{\"result\":\"0x2000000\"}}}}}'"
echo "  curl -s -X POST http://$SIM_CONTROL/reset/all"
echo ""
echo "🟢 Router and simulator LEFT RUNNING."
echo "✋ Stop when done:  pkill -f '$BIN' ; pkill -f 'run.py'"
echo "==================================================================="

# Known issues are reported, not failed — this stays green until a real regression.
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
