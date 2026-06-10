#!/bin/bash
# =============================================================================
# UC-2 (Group-Diversity Validation Policy) test harness — Lava via lava.build.
#
# UC-1 proved a per-method COUNT quorum ("$CV_THRESHOLD of N identical responses",
# a single implicit 'default' group, no diversity). UC-2 adds the missing axis:
# the agreeing quorum must also span at least $MIN_GROUPS *distinct provider groups*.
#
# Goal (kept deliberately simple):
#   * Providers carry a `group-label` so the fleet has 2 groups of 3 endpoints each:
#         group-a = {lava-tm-a1, lava-tm-a2, lava-tm-a3}
#         group-b = {lava-tm-b1, lava-tm-b2, lava-tm-b3}
#   * ONE method ('$CV_METHOD') is ALWAYS cross-validated by operator policy with
#     min-groups: $MIN_GROUPS — so a quorum that all came from ONE group is REJECTED
#     even though it met the count; the agreeing providers must straddle both groups.
#   * A DELIBERATELY-IMPOSSIBLE policy (min-groups: $BAD_MIN_GROUPS over only
#     $CONFIGURED_GROUPS configured groups) makes the router REFUSE TO START — the
#     group-diversity capacity fail-fast — proving the requirement is structural,
#     not cosmetic. (Run in a THROWAWAY instance; the kept router stays satisfiable.)
#
# Why 'block' (and a fixed height): a block is CONSENSUS state — byte-identical on
# every node — so the cross-group responses agree and the quorum forms determin-
# istically. We resolve the latest height once, then query that settled height.
#
# Why this is a real UC-2 test and not UC-1 with extra labels: group-a alone has
# $MAX_PART providers (== max-participants), so a group-BLIND fan-out could fill ALL
# $MAX_PART slots from group-a, span only 1 group, and the post-filter group guard
# would return diversity-unmet. So Check A's "agreeing set spans >= $MIN_GROUPS groups"
# only passes BECAUSE the group-aware fan-out enforced diversity — it breaks the moment
# group enforcement breaks. The 3rd provider per group gives the quorum failover slack
# (fan out to $MAX_PART for a threshold of $CV_THRESHOLD) without weakening that property.
# =============================================================================
__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "$__dir"/../useful_commands.sh

LOGS_DIR=${__dir}/../../debugging/logs
mkdir -p "$LOGS_DIR"
LOGS_DIR=$(cd "$LOGS_DIR" && pwd)
rm "$LOGS_DIR"/*.log 2>/dev/null || true

PROJECT_ROOT=$(cd "${__dir}/../.." && pwd)
LOG_FILE="$LOGS_DIR/SMARTROUTER_LAVA_UC2.log"
NEG1_LOG_FILE="$LOGS_DIR/SMARTROUTER_LAVA_UC2_NEG_DIVERSITY.log"   # throwaway: min-groups capacity
NEG2_LOG_FILE="$LOGS_DIR/SMARTROUTER_LAVA_UC2_NEG_PERGROUP.log"    # throwaway: per-group capacity

# Ports (UC-2 uses its OWN ports, distinct from UC-1's). NOTE: teardown below reuses
# the shared 'smartrouter'/'cache' screen-session names + killall, so starting UC-2
# REPLACES any router this machine is running (same as UC-1). tendermintrpc-only.
TM_PORT=3372
NEG_TM_PORT=3373          # throwaway negative #1 (diversity capacity) binds here
NEG_TM_PORT2=3374         # throwaway negative #2 (per-group capacity) binds here
CACHE_ADDR="127.0.0.1:20110"
METRICS_PORT=7789

# -----------------------------------------------------------------------------
# THE POLICY UNDER TEST. Defined once and reused everywhere (config, payload,
# metric label, group accounting) so there is one obvious place that says "what?".
# -----------------------------------------------------------------------------
CV_CHAIN="LAVA"
CV_API="tendermintrpc"
# --- Tier 1: BASELINE min-groups diversity (UC-2 core, dev-plan item 1.2) -----
# One agreeing quorum that must SPAN >= MIN_GROUPS distinct groups.
CV_METHOD="block"          # <<<< cross-validated on this method, WITH group diversity
CV_THRESHOLD=2             # need this many identical responses
MAX_PART=3                 # fan out to this many providers (== group size -> see header note)
MIN_GROUPS=2               # <<<< the UC-2 axis: quorum must span this many distinct groups
CONFIGURED_GROUPS=2        # group-a + group-b
BAD_MIN_GROUPS=3           # > CONFIGURED_GROUPS -> the router must REFUSE TO START (negative test)
# --- Tier 2: STRONGER variant — per-group-quorum (UC-2 variant, dev-plan 2.3) -
# Each of MIN_GROUPS groups must reach its OWN internal AGREEMENT_THRESHOLD quorum,
# then the per-group winners must agree. Capacity rule (enforced at config parse):
# max-participants >= min-groups * agreement-threshold, AND each group needs >= threshold
# providers. On a SHARED upstream every group always agrees internally, so success is
# header-identical to Tier 1 — the deterministic, feature-specific proof of the stronger
# gate is therefore its parse-time capacity FAIL-FAST (the negative below).
PG_METHOD="commit"         # <<<< per-group-quorum is ENFORCED on this method (also consensus state)
PG_THRESHOLD=2             # each group must reach this internal quorum
PG_MIN_GROUPS=2            # this many groups must each reach it
PG_MAX_PART=4              # >= PG_MIN_GROUPS * PG_THRESHOLD (2*2) -> satisfiable
BAD_PG_MAX_PART=3          # < PG_MIN_GROUPS * PG_THRESHOLD (2*2=4) -> the router must REFUSE TO START
OTHER_METHOD="health"      # a method with NO policy (control)

# Two groups, THREE providers each. The script maps an agreeing-provider NAME back to
# its group (the response has no dedicated group header; provider address == configured
# name, see direct_rpc_relay.go). NB: a plain `case` (not an associative array) so this
# runs on macOS's stock bash 3.2 — `declare -A` is a bash 4+ feature and silently no-ops
# there, which would map every provider to "default" and wrongly fail the diversity check.
group_of_provider() {
	case "$1" in
		lava-tm-a1|lava-tm-a2|lava-tm-a3) echo "group-a" ;;
		lava-tm-b1|lava-tm-b2|lava-tm-b3) echo "group-b" ;;
		*) echo "default" ;;
	esac
}

OTHER_PAYLOAD="{\"jsonrpc\":\"2.0\",\"method\":\"$OTHER_METHOD\",\"params\":[],\"id\":1}"

print_policy_banner() {
	echo "+------------------------------------------------------------+"
	echo "| ENFORCED CROSS-VALIDATION POLICY (UC-2 — group diversity)   |"
	echo "+------------------------------------------------------------+"
	printf "|  chain / api:         %-36s|\n" "$CV_CHAIN / $CV_API"
	printf "|  provider groups:     %-36s|\n" "$CONFIGURED_GROUPS (group-a, group-b — 3 providers each)"
	printf "|  T1 diversity:        %-36s|\n" "$CV_METHOD: $CV_THRESHOLD agree, span >= $MIN_GROUPS groups"
	printf "|  T2 per-group-quorum: %-36s|\n" "$PG_METHOD: each of $PG_MIN_GROUPS groups reaches $PG_THRESHOLD"
	printf "|  every other method:  %-36s|\n" "plain (CV only via request headers)"
	echo "+------------------------------------------------------------+"
}

echo "============================================"
echo "UC-2 — Group-Diversity Validation Policy (Lava via lava.build)"
echo "============================================"
print_policy_banner
echo ""

if ! command_exists jq; then
	echo "✗ ERROR: 'jq' is required (used to resolve the latest block height). Install jq and retry."
	exit 1
fi

# --- Tear down only THIS script's previous run --------------------------------
# Quit just the smartrouter/cache screen sessions this script owns (NOT `killall
# screen`). The kept router is re-created below and LEFT RUNNING after this script
# exits (use the "Stop" command in the summary to stop it yourself).
screen -S smartrouter -X quit >/dev/null 2>&1 || true
screen -S cache -X quit >/dev/null 2>&1 || true
killall smartrouter 2>/dev/null || true
sleep 1
screen -wipe >/dev/null 2>&1 || true
sleep 1

echo "[Setup] installing binaries"
make install

# --- Upstreams + specs --------------------------------------------------------
# Official Lava MAINNET Tendermint-RPC endpoints (lava.build); PublicNode returns
# HTTP 403 to non-browser clients, so it cannot back an automated test. All four
# providers share this upstream, so a fixed-height block is byte-identical across
# the cross-GROUP fan-out and the quorum forms deterministically.
# Override with TENDERMINTRPC_URL=... / TENDERMINTRPC_WS_URL=... to point elsewhere.
LAVA_TENDERMINTRPC_LOCAL="${TENDERMINTRPC_URL:-https://lava.tendermintrpc.lava.build:443}"
LAVA_TENDERMINTRPC_WS_LOCAL="${TENDERMINTRPC_WS_URL:-wss://lava.tendermintrpc.lava.build/websocket}"

SPECS_DIR="$PROJECT_ROOT/specs/tendermint.json,$PROJECT_ROOT/specs/ibc.json,$PROJECT_ROOT/specs/cosmossdk.json,$PROJECT_ROOT/specs/lava.json"
IFS=',' read -r -a SPEC_FILES <<< "$SPECS_DIR"
for spec_file in "${SPEC_FILES[@]}"; do
	[ -f "$spec_file" ] || { echo "✗ ERROR: Spec file not found: $spec_file"; exit 1; }
done

# emit_provider <name> <group-label> -> one direct-rpc provider block (http + ws).
# The `group-label` key is the UC-2 addition over UC-1 (deserializes into
# RPCStaticProviderEndpoint.GroupLabel -> ProviderInfo.ProviderGroup).
emit_provider() {
	cat <<EOF
  - name: "$1"
    chain-id: "LAVA"
    api-interface: "tendermintrpc"
    group-label: "$2"
    node-urls:
      - url: "$LAVA_TENDERMINTRPC_LOCAL"
        timeout: 10s
        skip-verifications: [pruning]
      - url: "$LAVA_TENDERMINTRPC_WS_LOCAL"
        skip-verifications: [pruning]
EOF
}

# write_neg_config <file> <port> -> writes a full throwaway negative config: same 6-provider
# 2-group fleet as the kept config, with the (intentionally unsatisfiable) `cross-validation:`
# policy block read from STDIN. emit_provider reads its own heredoc, not stdin, so the policy
# block passed to this function survives intact for the trailing `cat`.
write_neg_config() {
	local file="$1" port="$2"
	{
		cat <<EOF
# Smart Router — UC-2 NEGATIVE config (intentionally UNSATISFIABLE).
# Generated by: scripts/pre_setups/test_uc2_smartrouter_lava.sh  (do not hand-edit)
endpoints:
  - network-address: "0.0.0.0:$port"
    chain-id: "LAVA"
    api-interface: "tendermintrpc"

direct-rpc:
EOF
		emit_provider "lava-tm-a1" "group-a"
		emit_provider "lava-tm-a2" "group-a"
		emit_provider "lava-tm-a3" "group-a"
		emit_provider "lava-tm-b1" "group-b"
		emit_provider "lava-tm-b2" "group-b"
		emit_provider "lava-tm-b3" "group-b"
		echo ""
		cat   # the `cross-validation:` policy block, from this function's stdin
	} > "$file"
}

# --- Generate the KEPT UC-2 config (satisfiable: min-groups <= configured groups) ---
CONFIG_FILE="$PROJECT_ROOT/config/smartrouter_examples/smartrouter_lava_uc2.yml"
# Passed RELATIVE to PROJECT_ROOT (viper SetConfigName + AddConfigPath("."), not a path).
CONFIG_REL="config/smartrouter_examples/smartrouter_lava_uc2.yml"
echo ""
echo "[Setup] generating UC-2 config: $CONFIG_FILE"
{
	cat <<EOF
# Smart Router — UC-2 (Group-Diversity Validation Policy) test config.
# Generated by: scripts/pre_setups/test_uc2_smartrouter_lava.sh  (do not hand-edit)

# UC-2 is tendermintrpc-only: a single listener, 6 providers across 2 groups.
endpoints:
  - network-address: "0.0.0.0:$TM_PORT"
    chain-id: "LAVA"
    api-interface: "tendermintrpc"

direct-rpc:
  # group-a (3 providers) + group-b (3 providers). Same upstream => identical block
  # across groups => the cross-GROUP quorum is deterministic. Each group is the size of
  # max-participants ($MAX_PART), so a group-BLIND fan-out could fill all $MAX_PART slots
  # from ONE group and be rejected (diversity-unmet) — which is exactly what makes the
  # group-aware selection observable. The 3rd member gives the threshold-$CV_THRESHOLD
  # quorum failover slack (one provider can drop without losing the quorum).
EOF
	emit_provider "lava-tm-a1" "group-a"
	emit_provider "lava-tm-a2" "group-a"
	emit_provider "lava-tm-a3" "group-a"
	emit_provider "lava-tm-b1" "group-b"
	emit_provider "lava-tm-b2" "group-b"
	emit_provider "lava-tm-b3" "group-b"
	cat <<EOF

# UC-2 TWO TIERS of the same group-diversity use case:
#  * Tier 1 ('$CV_METHOD'): plain min-groups diversity — ONE quorum spanning >= $MIN_GROUPS
#    groups. A count-quorum from a single group is rejected (reason: diversity-unmet).
#  * Tier 2 ('$PG_METHOD', per-group-quorum: true): the STRONGER variant — EACH of
#    $PG_MIN_GROUPS groups must reach its own internal $PG_THRESHOLD quorum, then the per-group
#    winners must agree (reason on failure: group-quorum-unmet). Needs max-participants
#    ($PG_MAX_PART) >= min-groups * threshold ($PG_MIN_GROUPS*$PG_THRESHOLD) and each group >= threshold.
# Every other method is absent, so it is only cross-validated via caller headers.
cross-validation:
  policies:
    - chain-id: $CV_CHAIN
      api-interface: $CV_API
      method: $CV_METHOD
      enabled: true
      agreement-threshold: $CV_THRESHOLD
      max-participants: $MAX_PART
      min-groups: $MIN_GROUPS
    - chain-id: $CV_CHAIN
      api-interface: $CV_API
      method: $PG_METHOD
      enabled: true
      per-group-quorum: true
      agreement-threshold: $PG_THRESHOLD
      max-participants: $PG_MAX_PART
      min-groups: $PG_MIN_GROUPS
EOF
} > "$CONFIG_FILE"

echo "✓ config written ($(wc -c < "$CONFIG_FILE") bytes). The cross-validation: block:"
sed -n '/^cross-validation:/,$p' "$CONFIG_FILE" | sed 's/^/    /'

# --- Generate the TWO throwaway negative configs (one per UC-2 tier) -----------
# Same 6-provider/2-group fleet as the kept config; only the policy is unsatisfiable.
# NEG1 (diversity): min-groups ($BAD_MIN_GROUPS) > configured groups ($CONFIGURED_GROUPS).
# NEG2 (per-group): max-participants ($BAD_PG_MAX_PART) < min-groups*threshold ($PG_MIN_GROUPS*$PG_THRESHOLD).
# Both must make the router REFUSE TO START. Distinct ports so they never collide.
NEG1_CONFIG_FILE="$PROJECT_ROOT/config/smartrouter_examples/smartrouter_lava_uc2_neg_diversity.yml"
NEG1_CONFIG_REL="config/smartrouter_examples/smartrouter_lava_uc2_neg_diversity.yml"
NEG2_CONFIG_FILE="$PROJECT_ROOT/config/smartrouter_examples/smartrouter_lava_uc2_neg_pergroup.yml"
NEG2_CONFIG_REL="config/smartrouter_examples/smartrouter_lava_uc2_neg_pergroup.yml"

write_neg_config "$NEG1_CONFIG_FILE" "$NEG_TM_PORT" <<EOF
cross-validation:
  policies:
    - chain-id: $CV_CHAIN
      api-interface: $CV_API
      method: $CV_METHOD
      enabled: true
      agreement-threshold: $CV_THRESHOLD
      max-participants: $BAD_MIN_GROUPS
      min-groups: $BAD_MIN_GROUPS
EOF

write_neg_config "$NEG2_CONFIG_FILE" "$NEG_TM_PORT2" <<EOF
cross-validation:
  policies:
    - chain-id: $CV_CHAIN
      api-interface: $CV_API
      method: $PG_METHOD
      enabled: true
      per-group-quorum: true
      agreement-threshold: $PG_THRESHOLD
      max-participants: $BAD_PG_MAX_PART
      min-groups: $PG_MIN_GROUPS
EOF

# --- Cache service ------------------------------------------------------------
echo ""
echo "[Setup] starting cache service ($CACHE_ADDR)"
screen -d -m -S cache bash -c "source ~/.bashrc; smartrouter cache \
$CACHE_ADDR --metrics_address 0.0.0.0:20210 --log_level debug 2>&1 | tee \"$LOGS_DIR/CACHE.log\"" && sleep 0.25
sleep 2

# =============================================================================
# CHECK 0 (negative, runs FIRST): each unsatisfiable policy must REFUSE TO START.
# A throwaway foreground instance per tier; asserts on the SPECIFIC capacity error so a
# port clash / bad upstream cannot masquerade as a passing capacity test. This is the
# DETERMINISTIC, feature-specific proof of each tier's gate (success paths on a shared
# upstream are header-identical between the two tiers, so only the negatives distinguish).
# =============================================================================
PASS=0
FAIL=0
pass() { echo "  ✅ PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  ❌ FAIL: $1"; FAIL=$((FAIL + 1)); }

# assert_refuses_to_start <config-rel> <log-file> <port> <error-regex> <banner>
# Runs a time-boxed foreground router and asserts (a) the specific error is logged and
# (b) nothing is serving on its port. A satisfiable router would instead run until the
# timeout kills it (rc 124) and answer on its port — both would fail this check.
assert_refuses_to_start() {
	local cfg="$1" log="$2" port="$3" rx="$4" banner="$5" rc
	echo ""
	echo "============================================"
	echo "$banner"
	echo "============================================"
	echo "    (throwaway instance on :$port — expected to exit, not to serve traffic)"
	( cd "$PROJECT_ROOT" && source ~/.bashrc; timeout 60 smartrouter \
		"$cfg" \
		--geolocation 1 \
		--log-level debug \
		--use-static-spec "$SPECS_DIR" \
		--min-relay-timeout 5s ) > "$log" 2>&1
	rc=$?
	killall smartrouter 2>/dev/null || true   # belt-and-suspenders: ensure the throwaway is gone
	if grep -qiE "$rx" "$log"; then
		pass "router refused to start on the capacity check (specific error present)"
		grep -iE "$rx" "$log" | head -n1 | sed 's/^/        /'
	else
		fail "expected capacity error /$rx/ in $log (rc=$rc); last lines:"
		tail -n 15 "$log" 2>/dev/null | sed 's/^/        /'
	fi
	if curl -sS -o /dev/null --max-time 2 "http://127.0.0.1:$port/$OTHER_METHOD" 2>/dev/null; then
		fail "the unsatisfiable instance is STILL SERVING on :$port — it should have refused to start"
	else
		pass "the unsatisfiable instance is not serving (refused to start, as required)"
	fi
}

# [0a] Tier-1 diversity capacity: min-groups exceeds the number of configured groups.
assert_refuses_to_start "$NEG1_CONFIG_REL" "$NEG1_LOG_FILE" "$NEG_TM_PORT" \
	"min-groups policy cannot be satisfied|configured provider groups are fewer than required|insufficient-groups" \
	"[0a] NEGATIVE (diversity): min-groups=$BAD_MIN_GROUPS over $CONFIGURED_GROUPS groups must REFUSE TO START"

# [0b] Tier-2 per-group capacity: max-participants < min-groups * agreement-threshold.
assert_refuses_to_start "$NEG2_CONFIG_REL" "$NEG2_LOG_FILE" "$NEG_TM_PORT2" \
	"per-group-quorum needs max-participants|maxParticipants >= minGroups|per-group-quorum policy cannot be satisfied" \
	"[0b] NEGATIVE (per-group): max-participants=$BAD_PG_MAX_PART < min-groups*threshold=$((PG_MIN_GROUPS * PG_THRESHOLD)) must REFUSE TO START"

# =============================================================================
# Bring up the KEPT (satisfiable) router for the positive checks.
# =============================================================================
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

# Resolve a settled height (latest-5): its block is immutable and identical on every
# backend, which is what makes the cross-GROUP quorum deterministic.
LATEST=$(curl -sS "http://127.0.0.1:$TM_PORT/status" | jq -r '.result.sync_info.latest_block_height' 2>/dev/null)
if ! [[ "$LATEST" =~ ^[0-9]+$ ]] || [ "$LATEST" -le 5 ]; then
	echo "✗ ERROR: could not resolve a usable block height (got '$LATEST')."
	exit 1
fi
HEIGHT=$((LATEST - 5))
echo "✓ latest block height: $LATEST -> cross-validating settled height: $HEIGHT"
CV_PAYLOAD="{\"jsonrpc\":\"2.0\",\"method\":\"$CV_METHOD\",\"params\":{\"height\":\"$HEIGHT\"},\"id\":1}"
# Tier-2 payload: 'commit' at the same settled height is also consensus state (identical
# across groups), so each group reaches its internal quorum deterministically.
PG_PAYLOAD="{\"jsonrpc\":\"2.0\",\"method\":\"$PG_METHOD\",\"params\":{\"height\":\"$HEIGHT\"},\"id\":1}"

# =============================================================================
# UC-2 POSITIVE CHECKS
# =============================================================================
HDR=$(mktemp)
cv_status() { grep -i '^lava-cross-validation-status:' "$HDR" | tr -d '\r' | awk '{print $2}'; }
cv_failure_reason() { grep -i '^lava-cross-validation-failure-reason:' "$HDR" | tr -d '\r' | awk '{print $2}'; }
cv_headers_present() { grep -qi '^lava-cross-validation-' "$HDR"; }
cv_agreeing() { grep -i '^lava-cross-validation-agreeing-providers:' "$HDR" | tr -d '\r' | cut -d' ' -f2-; }

# distinct_groups_of "<comma,sep,names>" -> number of distinct groups they belong to
distinct_groups_of() {
	local csv="$1" name groups
	groups=""
	IFS=',' read -r -a names <<< "$csv"
	for name in "${names[@]}"; do
		name="$(echo "$name" | tr -d '[:space:]')"
		[ -z "$name" ] && continue
		groups+="$(group_of_provider "$name")"$'\n'
	done
	echo "$groups" | grep -v '^$' | sort -u | wc -l | tr -d '[:space:]'
}

echo ""
echo "============================================"
echo "Running UC-2 positive checks"
echo "============================================"
print_policy_banner

# --- Check A: the policy method is cross-validated AND the quorum spans >= min-groups ---
echo ""
echo "[A] '$CV_METHOD' (height $HEIGHT): cross-validated by POLICY, quorum must span >= $MIN_GROUPS groups"
echo "    POST $CV_PAYLOAD"
curl -sS -D "$HDR" -o /dev/null -X POST "http://127.0.0.1:$TM_PORT/" -d "$CV_PAYLOAD"
status=$(cv_status)
agreeing=$(cv_agreeing)
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
	pass "quorum reached (status=success)"
else
	fail "quorum NOT reached (status='${status:-<absent>}', reason='$(cv_failure_reason)') — check upstream/log"
fi
# THE UC-2 ASSERTION: the agreeing providers straddle >= MIN_GROUPS distinct groups.
# This is the check that breaks if group enforcement breaks (max-participants == min-groups,
# so a group-blind selection could span only 1 group).
ngroups=$(distinct_groups_of "$agreeing")
echo "      distinct groups among agreeing providers: ${ngroups:-0} (need >= $MIN_GROUPS)"
if [[ "$ngroups" =~ ^[0-9]+$ ]] && [ "$ngroups" -ge "$MIN_GROUPS" ]; then
	pass "agreeing quorum spans >= $MIN_GROUPS distinct groups (group diversity enforced)"
else
	fail "agreeing quorum spans only ${ngroups:-0} group(s) — group diversity NOT enforced"
fi

# --- Check B: a non-policy method is NOT cross-validated by default -------------
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

# --- Check C: the CV request metric incremented for method=$CV_METHOD ----------
echo ""
echo "[C] cross_validation_requests_total{method=\"$CV_METHOD\"} must be >= 1"
metric_line=$(curl -sS "http://127.0.0.1:$METRICS_PORT/metrics" 2>/dev/null \
	| grep '^lava_rpcsmartrouter_cross_validation_requests_total' | grep "method=\"$CV_METHOD\"")
metric_val=$(echo "$metric_line" | awk '{print $NF}' | head -n1)
echo "      ${metric_line:-<metric not found>}"
if [[ "$metric_val" =~ ^[0-9]+(\.[0-9]+)?$ ]] && awk "BEGIN{exit !($metric_val >= 1)}"; then
	pass "requests_total counted the cross-validated '$CV_METHOD' request ($metric_val)"
else
	fail "cross_validation_requests_total{method=\"$CV_METHOD\"} not >= 1"
fi

# --- Check D: the startup log shows the group layout with >= min-groups groups ---
echo ""
echo "[D] router log must show policies loaded with distinctGroups >= $MIN_GROUPS"
if grep -q "cross-validation per-method policies loaded" "$LOG_FILE"; then
	pass "startup log: policies loaded"
	layout=$(grep "cross-validation per-method policies loaded" "$LOG_FILE" | head -n1)
	# NB: parse the `key=N` ATTRIBUTES, not the message word "policies" in "...policies loaded".
	# Anchor on the literal `policies=` / `distinctGroups=` token — no \b word boundary, which
	# BSD/macOS sed does not support (it would silently match nothing).
	echo "      $(echo "$layout" | grep -o 'policies=[0-9][0-9]*' | head -n1) $(echo "$layout" | grep -o 'distinctGroups=[0-9][0-9]*' | head -n1) $(echo "$layout" | grep -o 'groupSizes[^ ]*' | head -n1)"
	dg=$(echo "$layout" | sed -n 's/.*distinctGroups=\([0-9][0-9]*\).*/\1/p' | head -n1)
	if [[ "$dg" =~ ^[0-9]+$ ]] && [ "$dg" -ge "$MIN_GROUPS" ]; then
		pass "startup log reports $dg distinct groups (>= min-groups $MIN_GROUPS)"
	else
		fail "startup log distinctGroups='${dg:-?}' is not >= $MIN_GROUPS"
	fi
	# Both tiers must have loaded: the diversity policy ('$CV_METHOD') + the per-group
	# policy ('$PG_METHOD'). The per-group one only survives load if its capacity check
	# passed (max-participants >= min-groups*threshold AND each group >= threshold).
	np=$(echo "$layout" | sed -n 's/.*policies=\([0-9][0-9]*\).*/\1/p' | head -n1)
	if [[ "$np" =~ ^[0-9]+$ ]] && [ "$np" -ge 2 ]; then
		pass "startup log reports $np policies loaded (diversity + per-group tiers)"
	else
		fail "startup log policies='${np:-?}' is not >= 2 (expected both tiers)"
	fi
else
	fail "startup log missing the policies-loaded line"
fi
if grep -q "CrossValidation mode enabled (policy-resolved)" "$LOG_FILE"; then
	pass "request log: CrossValidation resolved by policy"
else
	fail "request log missing the policy-resolved cross-validation line"
fi

# --- Check E: the STRONGER tier ('$PG_METHOD', per-group-quorum) serves successfully --
# Each of the $PG_MIN_GROUPS groups reaches its own internal $PG_THRESHOLD quorum, then the
# per-group winners agree. On a shared upstream the success response is header-identical to
# Tier-1 diversity (both: status=success, quorum spans $PG_MIN_GROUPS groups) — the per-group
# GATE itself is proven deterministically by [0b] above. Here we confirm the stronger policy
# actually carries live traffic rather than only rejecting bad configs.
echo ""
echo "[E] per-group-quorum tier: '$PG_METHOD' (height $HEIGHT) must succeed spanning >= $PG_MIN_GROUPS groups"
echo "    POST $PG_PAYLOAD"
curl -sS -D "$HDR" -o /dev/null -X POST "http://127.0.0.1:$TM_PORT/" -d "$PG_PAYLOAD"
pg_status=$(cv_status)
pg_agreeing=$(cv_agreeing)
echo "      lava-cross-validation-status:            ${pg_status:-<absent>}"
echo "      lava-cross-validation-agreeing-providers:${pg_agreeing:-<absent>}"
if [ "$pg_status" = "success" ]; then
	pass "per-group-quorum policy reached consensus on '$PG_METHOD' (status=success)"
else
	fail "per-group-quorum on '$PG_METHOD' did not succeed (status='${pg_status:-<absent>}', reason='$(cv_failure_reason)')"
fi
pg_ngroups=$(distinct_groups_of "$pg_agreeing")
echo "      distinct groups among agreeing providers: ${pg_ngroups:-0} (need >= $PG_MIN_GROUPS)"
if [[ "$pg_ngroups" =~ ^[0-9]+$ ]] && [ "$pg_ngroups" -ge "$PG_MIN_GROUPS" ]; then
	pass "per-group winners span >= $PG_MIN_GROUPS distinct groups"
else
	fail "per-group quorum spans only ${pg_ngroups:-0} group(s) — expected >= $PG_MIN_GROUPS"
fi
pg_metric=$(curl -sS "http://127.0.0.1:$METRICS_PORT/metrics" 2>/dev/null \
	| grep '^lava_rpcsmartrouter_cross_validation_requests_total' | grep "method=\"$PG_METHOD\"" | awk '{print $NF}' | head -n1)
if [[ "$pg_metric" =~ ^[0-9]+(\.[0-9]+)?$ ]] && awk "BEGIN{exit !($pg_metric >= 1)}"; then
	pass "requests_total counted the per-group '$PG_METHOD' request ($pg_metric)"
else
	fail "cross_validation_requests_total{method=\"$PG_METHOD\"} not >= 1"
fi

rm -f "$HDR"

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo "============================================"
echo "UC-2 result: $PASS passed, $FAIL failed   (T1 diversity on '$CV_METHOD' + T2 per-group-quorum on '$PG_METHOD')"
echo "============================================"
echo "Tendermint listener: http://127.0.0.1:$TM_PORT   (metrics: $METRICS_PORT)"
echo "Config (kept):       $CONFIG_FILE"
echo "Config (neg, diversity): $NEG1_CONFIG_FILE"
echo "Config (neg, per-group): $NEG2_CONFIG_FILE"
echo "Log:                 $LOG_FILE"
echo "                     (neg logs: $NEG1_LOG_FILE , $NEG2_LOG_FILE)"
echo ""
echo "🔬 Reproduce by hand (settled height $HEIGHT):"
echo "  # T1 — diversity: ONE quorum that must span both groups (agreeing-providers cross-group):"
echo "  curl -i -X POST http://127.0.0.1:$TM_PORT/ -d '$CV_PAYLOAD'"
echo "  #   ... or the URI form: curl -i \"http://127.0.0.1:$TM_PORT/$CV_METHOD?height=$HEIGHT\""
echo ""
echo "  # T2 — per-group-quorum: EACH group reaches its own quorum, then winners agree:"
echo "  curl -i -X POST http://127.0.0.1:$TM_PORT/ -d '$PG_PAYLOAD'"
echo ""
echo "  # NOT cross-validated (different method, no flags):"
echo "  curl -i -X POST http://127.0.0.1:$TM_PORT/ -d '$OTHER_PAYLOAD'"
echo ""
echo "  # the CV counters (note the per-group 'group' label on mismatch metrics):"
echo "  curl -s http://127.0.0.1:$METRICS_PORT/metrics | grep cross_validation"
echo ""
echo "  # the two capacity fail-fasts (each MUST refuse to start):"
echo "  smartrouter $NEG1_CONFIG_REL --geolocation 1 --use-static-spec '$SPECS_DIR'   # diversity"
echo "  smartrouter $NEG2_CONFIG_REL --geolocation 1 --use-static-spec '$SPECS_DIR'   # per-group"
echo ""
echo "📊 Logs:   tail -f $LOG_FILE"
echo "🟢 Router LEFT RUNNING in the background (screen session 'smartrouter', port $TM_PORT)."
echo "   Attach:  screen -r smartrouter"
echo "✋ Stop when done:  screen -S smartrouter -X quit; screen -S cache -X quit"
echo "============================================"

[ "$FAIL" -eq 0 ] && exit 0 || exit 1
