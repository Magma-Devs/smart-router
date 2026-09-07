#!/bin/bash
# Cross-Validation — DEMO LANE (PRD "Cross Validation Enhancements")
#
# One router, one fleet, one config that carries every shipped use case of the
# PRD, plus a sub-demo per use case that drives it and checks itself. The flag
# names track the PRD's own use-case numbers, so they match docs/CROSS-VALIDATION.md
# rather than renumbering to close the gap:
#
#   --uc1   Per-method validation policy   (mandate / no-policy / caller opt-in /
#                                           forbid-caller-cv / floor+cap precedence)
#   --uc2   Provider-group diversity       (min-groups, per-group quorum, and the
#                                           two startup capacity fail-fasts)
#   --uc4   Quorum mismatch -> metric      (reply-time dissent and straggler dissent,
#                                           with the group + finality labels)
#   --uc5   Quorum failure -> structured   (quorum-time vs structural reasons, and the
#           signal to the client            contrast with a real upstream error)
#   --uc6   Outlier excluded from result   (outvoted under a tolerant policy, fatal
#                                           under a unanimous one)
#   --all   bring the stack up fresh, smoke it, then run every sub-demo in order
#
# UC-7 (a deployment with no policies at all) is its own lane, because the whole
# point of it is a config that does NOT have this one's `cross-validation:` block:
#   scripts/pre_setups/init_smartrouter_cv_default.sh
#
# WHY THE SIMULATOR AND NOT REAL ENDPOINTS. Half of these use cases only exist
# when providers DISAGREE, and real endpoints agree on finalized state — a
# shared-truth fleet can never manufacture a dissent. The sibling
# provider_simulator's per-method body override (POST /scenario) makes a chosen
# provider return a valid-but-divergent result, which is exactly "a successful
# content outlier on a deterministic method": the one input the mismatch surface
# admits. Latency knobs decide who wins the race to quorum, so the reply-time and
# straggler paths are selected deterministically rather than by luck.
#
# THE SIMULATOR IS SHARED. It is reused when already up, never torn down, and
# never reset globally — this lane states the six providers it owns explicitly
# instead. Even so, the two cross-validation lanes drive overlapping provider
# ids, so run them one at a time.
#
# USAGE
#   scripts/pre_setups/init_smartrouter_cv_demo.sh            # bring it up + smoke
#   scripts/pre_setups/init_smartrouter_cv_demo.sh --all      # fresh stack, then every use case
#   scripts/pre_setups/init_smartrouter_cv_demo.sh --uc2      # one use case (stack must be up)
#   scripts/pre_setups/init_smartrouter_cv_demo.sh --status
#   scripts/pre_setups/init_smartrouter_cv_demo.sh --stop
#
# ENVIRONMENT (all optional)
#   SIM_DIR         provider_simulator checkout (searched for by default)
#   ROUTER_PORT (3396)  METRICS_PORT (7794)  DEBUG_PORT (6796)  NEG_PORT (3398)
#   SKIP_SMOKE=1    boot without the self-check
#
# OWNERSHIP SAFETY. This lane never runs `killall smartrouter` or `killall
# screen`: other lanes and other checkouts routinely have routers running. It
# reclaims only what it can prove it started — a pid file carrying a process
# start-time fingerprint, a screen under a name derived from THIS checkout, or a
# process whose command line names this checkout's ABSOLUTE config path — and
# refuses to run if its ports are held by anything else, printing the override to
# use instead. The shared helpers live in _cv_lane_lib.sh so that contract has
# one implementation rather than two that can drift.

__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT=$(cd "${__dir}"/../.. && pwd)
source "$__dir"/../useful_commands.sh

LOGS_DIR="${PROJECT_ROOT}/debugging/logs"
mkdir -p "$LOGS_DIR"

ROUTER_PORT="${ROUTER_PORT:-3396}"
METRICS_PORT="${METRICS_PORT:-7794}"
DEBUG_PORT="${DEBUG_PORT:-6796}"
NEG_PORT="${NEG_PORT:-3398}"

source "$__dir"/_cv_lane_lib.sh

# Screen names and pid files are global to the user, so they carry a per-checkout
# tag: without it a second checkout's lane reclaims the first one's session.
LANE="$(lane_tag)"
ROUTER_SCREEN="sr-cv-demo-${LANE}"
CONFIG_REL="debugging/smartrouter_cv_demo.yml"
CONFIG_FILE="${PROJECT_ROOT}/${CONFIG_REL}"
NEG_DIVERSITY_REL="debugging/smartrouter_cv_demo_neg_diversity.yml"
NEG_PERGROUP_REL="debugging/smartrouter_cv_demo_neg_pergroup.yml"
NEG_DIVERSITY_FILE="${PROJECT_ROOT}/${NEG_DIVERSITY_REL}"
NEG_PERGROUP_FILE="${PROJECT_ROOT}/${NEG_PERGROUP_REL}"
PIDFILE="${PROJECT_ROOT}/debugging/.cv-demo-router.pid"
ROUTER_LOG="${LOGS_DIR}/SMARTROUTER_CV_DEMO.log"
SIM_LOG="${LOGS_DIR}/CV_DEMO_SIM.log"
NEG_LOG="${LOGS_DIR}/SMARTROUTER_CV_DEMO_NEG.log"
SPEC_FILE="${PROJECT_ROOT}/specs/ethereum.json"

# --- the fleet: provider_simulator eth-sim, 6 providers in 3 groups -----------
# Ports come from the simulator's constants.py (ETH_PRIMARY_PORTS 18545-18547,
# ETH_BACKUP_PORTS 18560-18562); the control API keys providers as "pool:pid".
SIM_PORTS="18545 18546 18547 18560 18561 18562"     # sim-1 .. sim-6, positionally
GROUP_A="tier-1"      # sim-1, sim-2
GROUP_B="external"    # sim-3, sim-4
GROUP_C="archive"     # sim-5, sim-6

# A block far below the simulator's static head (0x1312D00 = 20,000,000), so every
# request below is for FINALIZED state: that is what makes a divergence a real
# mismatch rather than propagation lag, and what makes the metric's finality label
# read "finalized" instead of "unknown".
BLOCK="0x1000000"

# The methods each use case owns. Kept distinct so one demo's injected dissent can
# never leak into another's quorum.
M_MANDATE="eth_getBalance"          # UC-1 mandate, UC-4 mismatch, UC-5 no-agreement, UC-6 outvoted
M_CLAMP="eth_getCode"               # UC-1 floor + cap precedence
M_FORBID="eth_gasPrice"             # UC-1 forbid-caller-cv
M_DIVERSITY="eth_getTransactionCount"  # UC-2 min-groups
M_PERGROUP="eth_call"               # UC-2 per-group quorum
M_STRICT="eth_getStorageAt"         # UC-6 unanimous policy
M_NOPOLICY="eth_getBlockByNumber"   # no policy at all — the caller-driven control
M_UPSTREAM_ERR="eth_getTransactionByHash"  # UC-5 step 3 — forced to a real node error

HDR=""   # set in main(); the header dump every check reads

teardown() {
    echo "[Teardown] stopping this lane's resources (nothing else is signalled)"
    reclaim_owned "$PIDFILE" "router"
    reclaim_by_identity "$ROUTER_PORT" "$CONFIG_FILE" "router"
    # An interrupted --uc2 can leave a throwaway negative router holding NEG_PORT.
    reclaim_by_identity "$NEG_PORT" "$NEG_DIVERSITY_FILE" "negative router"
    reclaim_by_identity "$NEG_PORT" "$NEG_PERGROUP_FILE" "negative router"
    screen -S "$ROUTER_SCREEN" -X quit >/dev/null 2>&1 || true
    echo "  the provider_simulator is shared infrastructure — left running"
    echo "[Teardown] done"
}

# =============================================================================
# Fleet control
# =============================================================================

# fleet_clean : every provider of THIS lane honest and fast.
#
# It states all six providers rather than calling POST /reset/all, which is
# unscoped and would wipe every other harness's fixtures on the shared simulator.
# Stating the whole fleet each time also means no scenario can inherit the
# previous one's latency or body override. sim_scenario dies on a rejection, so
# a failed injection can never be mistaken for a router defect downstream.
fleet_clean() {
    sim_scenario '{"providers":{
        "eth-sim:1":{"latency_ms":0,"responses":{}},
        "eth-sim:2":{"latency_ms":0,"responses":{}},
        "eth-sim:3":{"latency_ms":0,"responses":{}},
        "eth-sim:4":{"latency_ms":0,"responses":{}},
        "eth-sim:5":{"latency_ms":0,"responses":{}},
        "eth-sim:6":{"latency_ms":0,"responses":{}}}}'
}

# dissent <pid> <method> <value> [latency_ms] : make sim-<pid> return a
# valid-but-divergent result for <method>. The response stays an HTTP 200 JSON-RPC
# success — a content outlier, not a node error, which is the only shape the
# mismatch surface counts.
dissent() {
    local pid="$1" method="$2" value="$3" latency="${4:-0}"
    sim_scenario "{\"providers\":{\"eth-sim:$pid\":{\"latency_ms\":$latency,
        \"responses\":{\"$method\":{\"result\":\"$value\"}}}}}"
}

# node_error <pid> <method> : make sim-<pid> answer <method> with a real JSON-RPC
# error, so a genuine upstream failure can be contrasted with a quorum failure.
node_error() {
    sim_scenario "{\"providers\":{\"eth-sim:$1\":{\"latency_ms\":0,
        \"responses\":{\"$2\":{\"error_stub\":\"revert\"}}}}}"
}

# slow <pid> <ms> : delay a provider without changing what it answers. Latency is
# how this lane picks the recording path deterministically — all six upstreams are
# local and answer in microseconds, so who wins the race to quorum is otherwise
# chance, and that race is exactly what decides reply-time vs straggler.
slow() { sim_scenario "{\"providers\":{\"eth-sim:$1\":{\"latency_ms\":$2}}}"; }

# =============================================================================
# Lane-specific helpers
# =============================================================================

# group_of <provider-name> : the group-label this lane wrote into the config. The
# response headers name providers, not groups, so the mapping is re-stated here.
# A `case`, not an associative array: macOS ships bash 3.2, where `declare -A` is
# a silent no-op that would map every provider to "default".
group_of() {
    case "$1" in
        sim-1|sim-2) echo "$GROUP_A" ;;
        sim-3|sim-4) echo "$GROUP_B" ;;
        sim-5|sim-6) echo "$GROUP_C" ;;
        *) echo "default" ;;
    esac
}

distinct_groups() { # <csv of provider names> -> count of distinct groups
    # printf '%s\n' matters: without the trailing newline `read` drops the last
    # field, which would silently under-count the diversity this lane asserts on.
    local csv="$1" name
    printf '%s\n' "$csv" | tr ',' '\n' | while read -r name; do
        name=$(printf '%s' "$name" | tr -d '[:space:]')
        [[ -z "$name" ]] && continue
        group_of "$name"
    done | sort -u | grep -c '[^[:space:]]' | tr -d ' '
}

# log_mark / log_since : read only the lines THIS sub-demo produced.
#
# Greping the whole accumulated router log asserts on something an earlier
# sub-demo (or the smoke) already guaranteed, so the check passes even when the
# requests it is about produced nothing.
log_mark() { wc -l < "$ROUTER_LOG" 2>/dev/null | tr -d ' ' || echo 0; }
log_since() { tail -n "+$(( ${1:-0} + 1 ))" "$ROUTER_LOG" 2>/dev/null; }

require_up() {
    router_healthy && return 0
    echo "ERROR: no router answering 200 on :${ROUTER_PORT}/lava/health."
    echo "       Bring the lane up first:  $0"
    exit 1
}

# =============================================================================
# Config generation
# =============================================================================
emit_provider() { # <name> <group> <port>
    cat <<EOF
  - name: "$1"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    group-label: "$2"
    node-urls:
      - url: "http://127.0.0.1:$3"
        timeout: 10s
        skip-verifications: [chain-id, pruning]
EOF
}

emit_fleet() { # <listen-port>
    cat <<EOF
# GENERATED by scripts/pre_setups/init_smartrouter_cv_demo.sh — do not hand-edit.
#
# Six provider_simulator upstreams in THREE groups. The group labels are the
# deployment-shaped half of the feature: which providers count as independent
# corroboration is an operator trust decision, so it lives here next to the
# provider entries, not in a spec file.
endpoints:
  - listen-address: "0.0.0.0:$1"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    network-address: "0.0.0.0:$1"

direct-rpc:
EOF
    local i=0 port
    for port in $SIM_PORTS; do
        i=$((i + 1))
        emit_provider "sim-$i" "$(group_of "sim-$i")" "$port"
    done
}

emit_config() {
    emit_fleet "$ROUTER_PORT"
    cat <<EOF

# One policy per use case. A method absent from this list is untouched: it is
# cross-validated only when the caller sends the lava-cross-validation-* headers,
# which is the pre-feature behaviour (UC-7).
cross-validation:
  policies:
    # UC-1 mandate. Cross-validation happens with no caller headers at all.
    # max-participants is the whole fleet so a designated dissenter is always in
    # the participant set — that is what makes UC-4/UC-5/UC-6 deterministic.
    - chain-id: ETH1
      api-interface: jsonrpc
      method: $M_MANDATE
      enabled: true
      agreement-threshold: 2
      max-participants: 6

    # UC-1 precedence. floor = operator minimum a caller may exceed; cap = operator
    # maximum that clamps a caller who asks for more. floor == cap pins the knob.
    - chain-id: ETH1
      api-interface: jsonrpc
      method: $M_CLAMP
      enabled: true
      agreement-threshold: { floor: 2, cap: 3 }
      max-participants: { floor: 3, cap: 3 }

    # UC-1 disable. The caller's CV headers are ignored and the method routes by
    # its normal category. Mutually exclusive with enabled:.
    - chain-id: ETH1
      api-interface: jsonrpc
      method: $M_FORBID
      forbid-caller-cv: true

    # UC-2 diversity. ONE quorum that must span at least 2 distinct groups.
    - chain-id: ETH1
      api-interface: jsonrpc
      method: $M_DIVERSITY
      enabled: true
      agreement-threshold: 2
      max-participants: 6
      min-groups: 2

    # UC-2 stronger. EACH of 2 groups must independently reach its own quorum of
    # 2, and the per-group winners must then agree. Needs
    # max-participants >= min-groups * agreement-threshold.
    - chain-id: ETH1
      api-interface: jsonrpc
      method: $M_PERGROUP
      enabled: true
      per-group-quorum: true
      agreement-threshold: 2
      min-groups: 2
      max-participants: 4

    # UC-5 contrast. A mandated method used to show that a genuine upstream error
    # is reported as an error, without a cross-validation failure-reason.
    - chain-id: ETH1
      api-interface: jsonrpc
      method: $M_UPSTREAM_ERR
      enabled: true
      agreement-threshold: 2
      max-participants: 6

    # UC-6 contrast. Unanimity: with agreement-threshold == max-participants a
    # single outlier drops the agreeing count below the threshold, so the request
    # fails instead of returning a quorum that excluded it.
    - chain-id: ETH1
      api-interface: jsonrpc
      method: $M_STRICT
      enabled: true
      agreement-threshold: 6
      max-participants: 6
EOF
}

# The two negative configs are never served — they exist to be REFUSED at startup.
emit_neg_diversity() {
    emit_fleet "$NEG_PORT"
    cat <<EOF

# UNSATISFIABLE ON PURPOSE: min-groups 4 over a fleet with 3 groups. No request
# could ever satisfy it, so the router must refuse to start rather than fail
# every request at runtime.
cross-validation:
  policies:
    - chain-id: ETH1
      api-interface: jsonrpc
      method: $M_DIVERSITY
      enabled: true
      agreement-threshold: 2
      max-participants: 6
      min-groups: 4
EOF
}

emit_neg_pergroup() {
    emit_fleet "$NEG_PORT"
    cat <<EOF

# UNSATISFIABLE ON PURPOSE: per-group quorum needs
# max-participants >= min-groups * agreement-threshold (2 * 2 = 4), and this asks
# for 3. Caught by the config preflight, before any provider is dialled.
cross-validation:
  policies:
    - chain-id: ETH1
      api-interface: jsonrpc
      method: $M_PERGROUP
      enabled: true
      per-group-quorum: true
      agreement-threshold: 2
      min-groups: 2
      max-participants: 3
EOF
}

write_config() { write_config_atomic "$CONFIG_FILE" emit_config; }
write_negative_configs() {
    write_config_atomic "$NEG_DIVERSITY_FILE" emit_neg_diversity
    write_config_atomic "$NEG_PERGROUP_FILE" emit_neg_pergroup
}

# =============================================================================
# UC-1 — Per-method validation policy
# =============================================================================
uc1() {
    echo ""
    echo "============================================"
    echo "UC-1 — Per-method validation policy"
    echo "============================================"
    fleet_clean
    local mark; mark=$(log_mark)

    echo ""
    echo "[1] '$M_MANDATE' has a policy with enabled: true — no caller headers sent"
    call "$M_MANDATE" "[\"$(addr 11)\",\"$BLOCK\"]" >/dev/null
    note "status" "$(hdr status)"
    note "all-providers" "$(hdr all-providers)"
    note "agreeing-providers" "$(hdr agreeing-providers)"
    check "the operator policy cross-validated it unasked" "$(hdr status)" "success"
    check "the fan-out is the policy's max-participants" "$(count_csv "$(hdr all-providers)")" "6"

    echo ""
    echo "[2] '$M_NOPOLICY' has NO policy and the caller sent no headers"
    call "$M_NOPOLICY" "[\"$BLOCK\",false]" >/dev/null
    if cv_present; then
        fail "unexpected cross-validation on a method with no policy"
        grep -i '^lava-cross-validation-' "$HDR" | tr -d '\r' | sed 's/^/        /'
    else
        pass "not cross-validated — a policy is scoped to its own method"
    fi

    echo ""
    echo "[3] the same method, with the caller's headers — the pre-feature path"
    call "$M_NOPOLICY" "[\"$BLOCK\",false]" \
        -H 'lava-cross-validation-max-participants: 3' \
        -H 'lava-cross-validation-agreement-threshold: 2' >/dev/null
    note "status" "$(hdr status)"
    check "header-driven cross-validation still works" "$(hdr status)" "success"
    check "the caller's max-participants was honoured" "$(count_csv "$(hdr all-providers)")" "3"

    echo ""
    echo "[4] '$M_FORBID' carries forbid-caller-cv — the same headers are IGNORED"
    call "$M_FORBID" "[]" \
        -H 'lava-cross-validation-max-participants: 3' \
        -H 'lava-cross-validation-agreement-threshold: 2' >/dev/null
    if cv_present; then
        fail "forbid-caller-cv did not suppress the caller's headers"
        grep -i '^lava-cross-validation-' "$HDR" | tr -d '\r' | sed 's/^/        /'
    else
        pass "cross-validation is off for this method whatever the caller asks"
    fi

    echo ""
    echo "[5] precedence on '$M_CLAMP' — floor 2/cap 3 threshold, max-participants pinned to 3"
    echo "    a caller asking for MORE than the cap is clamped down to it:"
    call "$M_CLAMP" "[\"$(addr 12)\",\"$BLOCK\"]" \
        -H 'lava-cross-validation-max-participants: 6' \
        -H 'lava-cross-validation-agreement-threshold: 6' >/dev/null
    note "requested" "max-participants 6, threshold 6"
    note "all-providers" "$(hdr all-providers)"
    check "the cap bounded the caller's fan-out" "$(count_csv "$(hdr all-providers)")" "3"
    check "and it still reached quorum" "$(hdr status)" "success"

    echo "    a caller asking for LESS than the floor gets the floor:"
    call "$M_CLAMP" "[\"$(addr 13)\",\"$BLOCK\"]" \
        -H 'lava-cross-validation-max-participants: 1' \
        -H 'lava-cross-validation-agreement-threshold: 1' >/dev/null
    note "requested" "max-participants 1, threshold 1"
    note "all-providers" "$(hdr all-providers)"
    check "the floor held against a weakening caller" "$(count_csv "$(hdr all-providers)")" "3"

    echo ""
    echo "[6] policy resolution is logged at request time"
    # Only the lines THIS sub-demo produced: the smoke already issued a mandated
    # call, so greping the whole log would pass even if none of the five requests
    # above reached the resolver.
    if log_since "$mark" | grep -q "CrossValidation mode enabled (policy-resolved)"; then
        pass "the router logged which path resolved the parameters, for these requests"
    else
        fail "no policy-resolution line for this sub-demo's requests in $ROUTER_LOG (needs --log-level debug)"
    fi
}

# =============================================================================
# UC-2 — Provider-group diversity quorum
# =============================================================================
uc2() {
    echo ""
    echo "============================================"
    echo "UC-2 — Provider-group diversity quorum"
    echo "============================================"
    fleet_clean

    echo ""
    echo "[1] '$M_DIVERSITY' requires the quorum to span >= 2 of the 3 groups"
    call "$M_DIVERSITY" "[\"$(addr 21)\",\"$BLOCK\"]" >/dev/null
    local agreeing; agreeing=$(hdr agreeing-providers)
    note "status" "$(hdr status)"
    note "agreeing-providers" "$agreeing"
    note "distinct groups among them" "$(distinct_groups "$agreeing")"
    check "quorum reached" "$(hdr status)" "success"
    if [[ "$(distinct_groups "$agreeing")" -ge 2 ]]; then
        pass "the agreeing set straddles at least 2 groups"
    else
        fail "the agreeing set spans $(distinct_groups "$agreeing") group(s) — diversity was not enforced"
    fi

    echo ""
    echo "[2] a count quorum that all came from ONE group must be REJECTED"
    echo "    (every provider outside '$GROUP_A' answers a DISTINCT wrong value, so"
    echo "     the only pair that agrees is sim-1 + sim-2 — one group)"
    fleet_clean
    dissent 3 "$M_DIVERSITY" "0x30"
    dissent 4 "$M_DIVERSITY" "0x40"
    dissent 5 "$M_DIVERSITY" "0x50"
    dissent 6 "$M_DIVERSITY" "0x60"
    call "$M_DIVERSITY" "[\"$(addr 22)\",\"$BLOCK\"]" >/dev/null
    note "status" "$(hdr status)"
    note "failure-reason" "$(hdr failure-reason)"
    check "the count quorum did not pass as success" "$(hdr status)" "failed"
    check "and the reason names diversity, not disagreement" "$(hdr failure-reason)" "diversity-unmet"

    echo ""
    echo "[3] the stronger variant: '$M_PERGROUP' needs EACH of 2 groups to reach its own quorum"
    fleet_clean
    call "$M_PERGROUP" "[{\"to\":\"$(addr 23)\",\"data\":\"0x\"},\"$BLOCK\"]" >/dev/null
    local pg_agreeing; pg_agreeing=$(hdr agreeing-providers)
    note "status" "$(hdr status)"
    note "agreeing-providers" "$pg_agreeing"
    note "distinct groups among them" "$(distinct_groups "$pg_agreeing")"
    check "per-group quorum reached" "$(hdr status)" "success"

    echo ""
    echo "[4] break ONE provider in EVERY group — no group can reach its own quorum"
    fleet_clean
    dissent 2 "$M_PERGROUP" "0x02"
    dissent 4 "$M_PERGROUP" "0x04"
    dissent 6 "$M_PERGROUP" "0x06"
    call "$M_PERGROUP" "[{\"to\":\"$(addr 24)\",\"data\":\"0x\"},\"$BLOCK\"]" >/dev/null
    note "status" "$(hdr status)"
    note "failure-reason" "$(hdr failure-reason)"
    check "the request failed" "$(hdr status)" "failed"
    check "with the per-group reason" "$(hdr failure-reason)" "group-quorum-unmet"

    fleet_clean

    echo ""
    echo "[5] a policy the fleet can NEVER satisfy is refused at STARTUP, not per request"
    # The patterns are the router's own error strings, anchored. A loose pattern
    # such as `distinct.*group` also matches the HEALTHY startup line
    # ("... policies loaded ... distinctGroups=3") and every served relay, so a
    # router whose guard regressed would still be reported as having refused.
    assert_refuses_to_start "$NEG_DIVERSITY_REL" \
        "cross-validation min-groups policy cannot be satisfied" \
        "min-groups: 4 over a 3-group fleet"
    assert_refuses_to_start "$NEG_PERGROUP_REL" \
        "per-group-quorum needs max-participants" \
        "per-group-quorum with max-participants 3 < 2 * 2"
}

# assert_refuses_to_start <config-rel> <error-regex> <what>
# A time-boxed foreground router. A satisfiable config would instead run until the
# bound expires (rc 124) and answer on its port — both of which fail this check,
# so a port clash or a dead upstream cannot masquerade as a passing capacity test.
assert_refuses_to_start() {
    local cfg="$1" rx="$2" what="$3" rc
    echo "    trying to start: $what"
    run_bounded 90 "$NEG_LOG" "$PROJECT_ROOT" \
        smartrouter "$cfg" \
            --log-level debug \
            --use-static-spec "$SPEC_FILE" \
            --skip-websocket-verification
    rc=$?
    if [[ "$rc" == "124" ]]; then
        fail "the unsatisfiable config KEPT RUNNING until the bound expired — $what"
    elif grep -qE "$rx" "$NEG_LOG"; then
        pass "refused to start — $what"
        grep -E "$rx" "$NEG_LOG" | head -n1 | cut -c1-160 | sed 's/^/        /'
    else
        fail "expected the router's own capacity error /$rx/ (rc=$rc); tail of $NEG_LOG:"
        tail -n 8 "$NEG_LOG" 2>/dev/null | cut -c1-160 | sed 's/^/        /'
    fi
    if curl -sS -o /dev/null --max-time 2 "http://127.0.0.1:${NEG_PORT}/lava/health" 2>/dev/null; then
        fail "the unsatisfiable instance is STILL SERVING on :${NEG_PORT}"
    fi
}

# =============================================================================
# UC-4 — Quorum mismatch -> metric for alerting
# =============================================================================
uc4() {
    echo ""
    echo "============================================"
    echo "UC-4 — Quorum mismatch -> metric for alerting"
    echo "============================================"
    local before after fin_before fin_after straggler_before straggler_after snap

    echo ""
    echo "[1] reply-time dissent: sim-6 ($GROUP_C) answers FIRST and answers wrong"
    echo "    (the honest five are held back, so the outlier is in hand when quorum forms)"
    fleet_clean
    local p
    for p in 1 2 3 4 5; do slow "$p" 400; done
    dissent 6 "$M_MANDATE" "0xdeadbeef" 0
    # One scrape, two readings — and both are deltas: a cumulative counter is
    # already non-zero on any re-run, so an absolute assertion cannot fail.
    snap=$(metrics_scrape)
    before=$(metric_in "$snap" cross_validation_mismatch_total "group=\"${GROUP_C}\"")
    fin_before=$(metric_in "$snap" cross_validation_mismatch_total 'finality="finalized"')
    call "$M_MANDATE" "[\"$(addr 41)\",\"$BLOCK\"]" >/dev/null
    snap=$(metrics_scrape)
    after=$(metric_in "$snap" cross_validation_mismatch_total "group=\"${GROUP_C}\"")
    fin_after=$(metric_in "$snap" cross_validation_mismatch_total 'finality="finalized"')
    note "status" "$(hdr status)"
    note "disagreeing-providers" "$(hdr disagreeing-providers)"
    note "mismatch_total{group=$GROUP_C}" "${before} -> ${after}"
    note "mismatch_total{finality=finalized}" "${fin_before} -> ${fin_after}"
    check "the quorum still formed around the honest answer" "$(hdr status)" "success"
    check "the dissenter is named in the response headers" "$(hdr disagreeing-providers)" "sim-6"
    check_gt "the alerting counter moved for the outlier's group" "$after" "$before"
    check_gt "THIS request was labelled finality=finalized" "$fin_after" "$fin_before"
    printf '%s' "$snap" | grep '^smartrouter_cross_validation_mismatch_total' | sed 's/^/        /'

    echo ""
    echo "[2] straggler dissent: the SAME outlier, delayed past the quorum early-exit"
    echo "    (it is 'pending' when the reply ships; only the async watcher can classify it)"
    fleet_clean
    dissent 6 "$M_MANDATE" "0xdeadbeef" 2000
    straggler_before=$(metric cross_validation_straggler_total 'outcome="disagreed"')
    call "$M_MANDATE" "[\"$(addr 42)\",\"$BLOCK\"]" >/dev/null
    note "status" "$(hdr status)"
    note "pending-providers" "$(hdr pending-providers)"
    note "disagreeing-providers (at reply time)" "$(hdr disagreeing-providers)"
    check "the reply did not wait for it" "$(hdr status)" "success"
    if printf '%s' "$(hdr pending-providers)" | grep -q 'sim-6'; then
        pass "the straggler is reported as pending, not silently dropped"
    else
        fail "sim-6 was not listed as pending (got '$(hdr pending-providers)')"
    fi
    echo "    waiting for the straggler watcher ..."
    for _ in $(seq 1 25); do
        straggler_after=$(metric cross_validation_straggler_total 'outcome="disagreed"')
        [[ "$straggler_after" -gt "$straggler_before" ]] && break
        sleep 1
    done
    note "straggler_total{outcome=disagreed}" "${straggler_before} -> ${straggler_after}"
    check_gt "the late dissent still reached the alerting surface" "$straggler_after" "$straggler_before"

    fleet_clean
}

# =============================================================================
# UC-5 — Quorum failure -> structured signal to the client
# =============================================================================
uc5() {
    echo ""
    echo "============================================"
    echo "UC-5 — Quorum failure -> structured signal to the client"
    echo "============================================"
    local body na_before na_after cap_before cap_after snap p

    snap=$(metrics_scrape)
    na_before=$(metric_in "$snap" cross_validation_failures_total 'reason="no-agreement"')
    cap_before=$(metric_in "$snap" cross_validation_failures_total 'reason="insufficient-capacity"')

    echo ""
    echo "[1] QUORUM-TIME failure: all six answer differently, so nothing reaches 2"
    fleet_clean
    for p in 1 2 3 4 5 6; do dissent "$p" "$M_MANDATE" "0x${p}${p}"; done
    call "$M_MANDATE" "[\"$(addr 51)\",\"$BLOCK\"]" >/dev/null
    note "status" "$(hdr status)"
    note "failure-reason" "$(hdr failure-reason)"
    note "all-providers" "$(hdr all-providers)"
    check "the client is told this is a quorum failure" "$(hdr status)" "failed"
    check "with a reason from the closed enum" "$(hdr failure-reason)" "no-agreement"
    if [[ -n "$(hdr all-providers)" ]]; then
        pass "the provider lists let the client pick a different set to retry"
    else
        fail "no provider lists on a quorum-time failure"
    fi

    echo ""
    echo "[2] STRUCTURAL failure: a caller asks for more providers than exist"
    echo "    (a retry against this router can never help — the client should fall back)"
    fleet_clean
    call "$M_NOPOLICY" "[\"$BLOCK\",false]" \
        -H 'lava-cross-validation-max-participants: 99' \
        -H 'lava-cross-validation-agreement-threshold: 99' >/dev/null
    note "status" "$(hdr status)"
    note "failure-reason" "$(hdr failure-reason)"
    check "still a structured failure" "$(hdr status)" "failed"
    check "and a structural reason, not a quorum one" "$(hdr failure-reason)" "insufficient-capacity"
    if [[ -z "$(hdr all-providers)" ]]; then
        pass "no provider lists — nothing was queried, and the headers say so"
    else
        fail "provider lists present on a request-time fail-fast: '$(hdr all-providers)'"
    fi

    echo ""
    echo "[3] the contrast: a REAL upstream error on a cross-validated method"
    echo "    (every provider returns a JSON-RPC error, so this is an upstream failure,"
    echo "     not a disagreement — the client must be able to tell them apart)"
    fleet_clean
    for p in 1 2 3 4 5 6; do node_error "$p" "$M_UPSTREAM_ERR"; done
    body=$(call "$M_UPSTREAM_ERR" "[\"0x$(printf '%064x' 52)\"]")
    local up_status up_reason
    up_status=$(hdr status); up_reason=$(hdr failure-reason)
    note "body" "$(printf '%s' "$body" | head -c 120)"
    note "status" "${up_status:-<absent>}"
    note "failure-reason" "${up_reason:-<absent>}"
    if printf '%s' "$body" | grep -q '"error"'; then
        pass "the upstream error reaches the client as an error"
    else
        fail "expected a JSON-RPC error body, got: $(printf '%s' "$body" | head -c 120)"
    fi
    # The discriminator: an upstream failure never carries a quorum-time reason.
    # (It may still be reported as a cross-validation failure, but with
    # insufficient-responses — nobody answered successfully — which is a
    # different and honest statement than "they disagreed".)
    case "$up_reason" in
        no-agreement|diversity-unmet|group-quorum-unmet)
            fail "an all-error upstream was reported as a disagreement: $up_reason" ;;
        *)
            pass "not reported as a disagreement (reason: ${up_reason:-none})" ;;
    esac
    fleet_clean

    echo ""
    echo "[4] both failures are broken out by reason for alerting"
    snap=$(metrics_scrape)
    na_after=$(metric_in "$snap" cross_validation_failures_total 'reason="no-agreement"')
    cap_after=$(metric_in "$snap" cross_validation_failures_total 'reason="insufficient-capacity"')
    printf '%s' "$snap" | grep '^smartrouter_cross_validation_failures_total' | sed 's/^/        /'
    note "failures_total{no-agreement}" "${na_before} -> ${na_after}"
    note "failures_total{insufficient-capacity}" "${cap_before} -> ${cap_after}"
    check_gt "THIS run recorded the quorum-time reason" "$na_after" "$na_before"
    check_gt "THIS run recorded the structural reason" "$cap_after" "$cap_before"

    echo ""
    echo "    The router did NOT retry either request with a different provider set —"
    echo "    that decision is deliberately the client's (PRD: no bespoke retry in the router)."
}

# =============================================================================
# UC-6 — Outlier provider excluded from the result set
# =============================================================================
uc6() {
    echo ""
    echo "============================================"
    echo "UC-6 — Outlier provider excluded from the result set"
    echo "============================================"
    local body result p

    echo ""
    echo "[1] tolerant policy ('$M_MANDATE', 2 of 6): one provider diverges"
    # The outlier answers FIRST and the honest five are held back, so its response
    # is in hand when the quorum forms. Without that ordering the early-exit ships
    # the reply while the outlier is still in flight, and it lands in
    # pending-providers — a straggler, which is UC-4's second scenario, not this one.
    fleet_clean
    for p in 1 2 3 4 6; do slow "$p" 400; done
    dissent 5 "$M_MANDATE" "0xbadbadbad" 0
    body=$(call "$M_MANDATE" "[\"$(addr 61)\",\"$BLOCK\"]")
    # jq, not sed: the upstream emits `"result": "0x0"` with a space after the
    # colon, which a naive `"result":"..."` pattern silently misses.
    result=$(printf '%s' "$body" | jq -r '.result // empty' 2>/dev/null)
    note "status" "$(hdr status)"
    note "disagreeing-providers" "$(hdr disagreeing-providers)"
    note "result returned to the client" "${result:-<none>}"
    check "the request still succeeded" "$(hdr status)" "success"
    if [[ "$result" != "0xbadbadbad" && -n "$result" ]]; then
        pass "the outlier's value never became the answer"
    else
        fail "the client received the outlier's value"
    fi
    if printf '%s' "$(hdr disagreeing-providers)" | grep -q 'sim-5'; then
        pass "the exclusion is observable — the dissenter is named"
    else
        fail "the outlier was dropped silently (disagreeing-providers: '$(hdr disagreeing-providers)')"
    fi

    echo ""
    echo "[2] unanimous policy ('$M_STRICT', 6 of 6): the SAME single divergence"
    fleet_clean
    dissent 5 "$M_STRICT" "0xbadbadbad"
    call "$M_STRICT" "[\"$(addr 62)\",\"0x0\",\"$BLOCK\"]" >/dev/null
    note "status" "$(hdr status)"
    note "failure-reason" "$(hdr failure-reason)"
    check "the request failed instead" "$(hdr status)" "failed"
    check "the largest bucket (5) never reached the threshold (6)" "$(hdr failure-reason)" "no-agreement"

    echo ""
    echo "    Exclusion is minority-loses, not a detect-then-filter pass: the outlier"
    echo "    forms its own bucket of one and is outvoted only while the agreeing"
    echo "    providers still meet the threshold. Under unanimity there is no slack,"
    echo "    so the same divergence correctly fails the request."
    fleet_clean
}

# =============================================================================
# Smoke — prove the stack before anyone drives it
# =============================================================================
smoke() {
    echo ""
    echo "[Smoke] policy load -> fan-out -> quorum"
    fleet_clean
    local layout
    layout=$(grep "cross-validation per-method policies loaded" "$ROUTER_LOG" 2>/dev/null | head -n1)
    note "router /lava/health" "$(curl -s -o /dev/null -w '%{http_code}' -m 5 "http://127.0.0.1:${ROUTER_PORT}/lava/health")"
    note "policies loaded" "$(printf '%s' "$layout" | grep -o 'policies=[0-9]*' | head -1)"
    note "distinct groups" "$(printf '%s' "$layout" | grep -o 'distinctGroups=[0-9]*' | head -1)"
    call "$M_MANDATE" "[\"$(addr 1)\",\"$BLOCK\"]" >/dev/null
    note "mandated cross-validation" "$(hdr status)"
    note "agreeing providers" "$(hdr agreeing-providers)"
    check "the policy block loaded" "$(printf '%s' "$layout" | grep -o 'policies=[0-9]*' | head -1)" "policies=7"
    check "all three groups were resolved" "$(printf '%s' "$layout" | grep -o 'distinctGroups=[0-9]*' | head -1)" "distinctGroups=3"
    check "a mandated request reaches quorum" "$(hdr status)" "success"
    echo ""
    if [[ "$FAIL" -eq 0 ]]; then
        echo "  SMOKE PASS — the stack is demo-ready."
    else
        echo "  SMOKE FAIL — $FAIL check(s) did not hold. Router left running for inspection."
        echo "  logs: $ROUTER_LOG"
    fi
}

# =============================================================================
# Bring-up
# =============================================================================
bring_up() {
    echo "============================================"
    echo "Cross-Validation — DEMO LANE"
    echo "============================================"
    echo "  router     0.0.0.0:${ROUTER_PORT}   (ETH1 jsonrpc)"
    echo "  metrics    127.0.0.1:${METRICS_PORT}"
    echo "  debug      127.0.0.1:${DEBUG_PORT}   (/debug/cross-validation-events)"
    echo "  upstreams  provider_simulator eth-sim 1-6"
    echo "  groups     ${GROUP_A} {sim-1,sim-2} · ${GROUP_B} {sim-3,sim-4} · ${GROUP_C} {sim-5,sim-6}"
    echo "============================================"
    echo ""

    # `timeout` is in this list because it is NOT present on a stock macOS — the
    # platform this lane targets — and its absence would otherwise surface as two
    # FAILs against the startup fail-fast rather than as a missing dependency.
    # (run_bounded replaces it, so this is a belt-and-braces check for anything
    # else in pre_setups that still shells out to it.)
    for tool in jq curl python3 lsof screen; do
        command_exists "$tool" || die "'$tool' is required."
    done
    [[ -f "$SPEC_FILE" ]] || die "spec not found: $SPEC_FILE"

    echo "[Setup] reclaiming this lane's previous run (if any)"
    reclaim_owned "$PIDFILE" "router"
    reclaim_by_identity "$ROUTER_PORT" "$CONFIG_FILE" "router"
    screen -S "$ROUTER_SCREEN" -X quit >/dev/null 2>&1 || true
    sleep 1

    require_free_ports \
        "ROUTER_PORT=3496 METRICS_PORT=7894 DEBUG_PORT=6896 NEG_PORT=3498 $0" \
        "$ROUTER_PORT" "$METRICS_PORT" "$DEBUG_PORT" "$NEG_PORT"

    echo "[Setup] installing binaries"
    make -C "$PROJECT_ROOT" install || die "make install failed"

    echo ""
    sim_ensure "$SIM_LOG"
    fleet_clean
    echo "  this lane's six providers set honest and fast (the simulator is not reset globally)"

    echo ""
    echo "[Setup] generating ${CONFIG_REL} (+ two deliberately unsatisfiable ones)"
    write_config
    write_negative_configs
    echo "  $(wc -c < "$CONFIG_FILE" | tr -d ' ') bytes, 6 providers / 3 groups / 7 policies"

    # No cache: a cache hit would short-circuit the fan-out and there would be no
    # second opinion to compare against. --debug-address installs the
    # cross-validation event recorder that /debug/cross-validation-events serves.
    echo ""
    echo "[Setup] starting the Smart Router (log -> $ROUTER_LOG)"
    screen -d -m -S "$ROUTER_SCREEN" bash -c "cd \"$PROJECT_ROOT\" && source ~/.bashrc; smartrouter \
$CONFIG_REL \
--log-level debug \
--use-static-spec \"$SPEC_FILE\" \
--metrics-listen-address ':$METRICS_PORT' \
--debug-address '127.0.0.1:$DEBUG_PORT' \
--skip-websocket-verification 2>&1 | tee \"$ROUTER_LOG\"" && sleep 0.25

    echo "[Setup] waiting for the router ..."
    wait_healthy "$ROUTER_PORT" "$ROUTER_SCREEN" || {
        echo "Tail of $ROUTER_LOG:"
        tail -n 30 "$ROUTER_LOG" 2>/dev/null | sed 's/^/    /'
        die "the router never answered 200 on /lava/health"
    }
    record_owned "$PIDFILE" "$ROUTER_PORT" "router" || exit 1

    # /lava/health going 200 is not provider readiness — the listener starts while
    # validation is still running, so a request issued now can see a partial fleet.
    echo "[Setup] waiting for all six providers to become selectable ..."
    wait_providers "$ROUTER_PORT" 6 "$M_NOPOLICY" \
        || echo "  WARNING: the full fleet was not selectable in time; checks may fail on capacity"
    # Let the chain tracker learn the head, so a request for $BLOCK resolves to
    # "finalized" rather than "unknown" on the mismatch metric.
    sleep 5
    echo "  router ready"
}

cheat_sheet() {
    cat <<EOF

============================================
Router is running — manual commands
============================================

A mandated cross-validation (no caller headers needed):
  curl -si -X POST http://127.0.0.1:${ROUTER_PORT} -H 'Content-Type: application/json' \\
    -d '{"jsonrpc":"2.0","method":"${M_MANDATE}","params":["0x00000000000000000000000000000000000000ff","${BLOCK}"],"id":1}' \\
    | grep -i '^lava-cross-validation'

The group-diversity method, and the per-group one:
  ... "method":"${M_DIVERSITY}"   (one quorum spanning >= 2 groups)
  ... "method":"${M_PERGROUP}"    (each of 2 groups reaches its own quorum)

Make a provider diverge / delay it (pid 1-6 = sim-1..sim-6):
  curl -s -X POST http://${SIM_CONTROL}/scenario -H 'Content-Type: application/json' \\
    -d '{"providers":{"eth-sim:6":{"latency_ms":0,"responses":{"${M_MANDATE}":{"result":"0xdeadbeef"}}}}}'

  # ... and back to honest. Do NOT use POST /reset/all: it is unscoped and would
  # wipe every other harness's fixtures on the shared simulator.
  curl -s -X POST http://${SIM_CONTROL}/scenario -H 'Content-Type: application/json' \\
    -d '{"providers":{"eth-sim:6":{"latency_ms":0,"responses":{}}}}'

The alerting surface:
  curl -s http://127.0.0.1:${METRICS_PORT}/metrics | grep cross_validation
  curl -s http://127.0.0.1:${DEBUG_PORT}/debug/cross-validation-events | jq
  curl -s "http://127.0.0.1:${DEBUG_PORT}/debug/cross-validation-events?outcome=disagreed" | jq

Sub-demos (the stack stays up between them; --all restarts it first, then runs all):
  $0 --uc1     per-method policy
  $0 --uc2     group diversity + the startup fail-fasts
  $0 --uc4     mismatch metric
  $0 --uc5     structured quorum-failure signal
  $0 --uc6     outlier exclusion
  scripts/pre_setups/init_smartrouter_cv_default.sh    # UC-7, its own lane

Logs / teardown:
  tail -f $ROUTER_LOG | grep -i cross-validation
  $0 --stop
============================================
EOF
}

status() {
    echo "router  :${ROUTER_PORT}   $(router_healthy && echo UP || echo DOWN)"
    echo "metrics :${METRICS_PORT}  $(curl -s -o /dev/null -m 3 "http://127.0.0.1:${METRICS_PORT}/metrics" && echo UP || echo DOWN)"
    echo "debug   :${DEBUG_PORT}   $(curl -s -o /dev/null -m 3 "http://127.0.0.1:${DEBUG_PORT}/debug/cross-validation-events" && echo UP || echo DOWN)"
    echo "sim     ${SIM_CONTROL}  $(sim_up && echo UP || echo DOWN)"
    [[ -f "$PIDFILE" ]] && echo "owned router pid: $(cut -d'|' -f1 "$PIDFILE")"
    curl -s -m 10 "http://127.0.0.1:${METRICS_PORT}/metrics" 2>/dev/null \
        | grep '^smartrouter_cross_validation' | sed 's/^/  /'
    return 0
}

# =============================================================================
# main
# =============================================================================
HDR=$(mktemp)
trap 'rm -f "$HDR"' EXIT

case "${1:-}" in
    --stop)   teardown; exit 0 ;;
    --status) status; exit 0 ;;
    --uc1)    require_up; uc1 ;;
    --uc2)    require_up; write_negative_configs; uc2 ;;
    --uc4)    require_up; uc4 ;;
    --uc5)    require_up; uc5 ;;
    --uc6)    require_up; uc6 ;;
    --all)
        # Always brought up fresh. Reusing a running router would mean driving
        # whatever config it happens to hold — an older revision of this script,
        # or a hand-edited debugging/ file — and reporting the resulting
        # mismatches as cross-validation defects.
        bring_up
        [[ "$SKIP_SMOKE" == "1" ]] || smoke
        uc1; uc2; uc4; uc5; uc6
        ;;
    "")
        bring_up
        [[ "$SKIP_SMOKE" == "1" ]] || smoke
        cheat_sheet
        ;;
    *)
        echo "usage: $0 [--all|--uc1|--uc2|--uc4|--uc5|--uc6|--status|--stop]"
        exit 2 ;;
esac

if [[ "${1:-}" != "" ]]; then
    echo ""
    echo "============================================"
    echo "$PASS passed, $FAIL failed"
    echo "============================================"
fi
[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1
