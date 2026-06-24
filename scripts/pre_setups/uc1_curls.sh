#!/bin/bash
# =============================================================================
# UC-1 curl scenarios — exercise the per-method cross-validation policy by hand
# against a running smart router (see test_uc1_smartrouter_lava.sh for bring-up).
#
# The cross-validation verdict rides in the RESPONSE HEADERS (smartrouter-cross-validation-*),
# so every call uses `curl -i` and greps for them.
#
# Usage:
#   ./uc1_curls.sh [scenario] [height]
#     scenario : 1|2|3|4|5|6|all   (default: all)
#     height   : block height to query in scenario 1/5 (default: latest-5, auto)
#
#   Scenarios:
#     1  policy method 'block'        -> ALWAYS cross-validated (no caller flags)
#     2  non-policy method 'health'   -> NOT cross-validated (default behavior)
#     3  'health' + caller headers    -> cross-validated (opt-in / "use the flags")
#     4  request more providers than exist -> structured failure (insufficient-capacity)
#     5  full response (headers + body) for the policy method
#     6  cross-validation metrics
#
# Env overrides: TM_PORT (3362), METRICS_PORT (7779), CV_METHOD (block),
#                OTHER_METHOD (health), CV_MAXPART (3), CV_THRESHOLD (2), HOST (127.0.0.1)
# =============================================================================
set -u

HOST=${HOST:-127.0.0.1}
TM_PORT=${TM_PORT:-3362}
METRICS_PORT=${METRICS_PORT:-7779}
CV_METHOD=${CV_METHOD:-block}
OTHER_METHOD=${OTHER_METHOD:-health}
CV_MAXPART=${CV_MAXPART:-3}
CV_THRESHOLD=${CV_THRESHOLD:-2}

BASE="http://$HOST:$TM_PORT"
METRICS="http://$HOST:$METRICS_PORT/metrics"
SCENARIO=${1:-all}
HEIGHT_ARG=${2:-}

hr() { echo "------------------------------------------------------------"; }
note() { echo "    $*"; }

# show <url-or-"POST"> ... runs a curl, prints the command, then the cross-validation
# response headers (or a clear "none" line). Pass curl args after the first marker.
cv_grep() { grep -i '^smartrouter-cross-validation' || echo "    (no cross-validation headers)"; }

resolve_height() {
	if [ -n "$HEIGHT_ARG" ]; then echo "$HEIGHT_ARG"; return; fi
	local latest
	latest=$(curl -s "$BASE/status" | jq -r '.result.sync_info.latest_block_height' 2>/dev/null)
	if [[ "$latest" =~ ^[0-9]+$ ]] && [ "$latest" -gt 5 ]; then
		echo $((latest - 5))
	else
		echo ""   # caller handles empty
	fi
}

scenario_1() {
	hr; echo "[1] POLICY method '$CV_METHOD' -> ALWAYS cross-validated (NO caller flags)"
	local h; h=$(resolve_height)
	if [ -z "$h" ]; then echo "    ✗ could not resolve a block height (is the router up? is jq installed?)"; return; fi
	local payload="{\"jsonrpc\":\"2.0\",\"method\":\"$CV_METHOD\",\"params\":{\"height\":\"$h\"},\"id\":1}"
	note "height=$h"
	note "curl -is -X POST $BASE/ -d '$payload'"
	echo "    expect: status=success, all-providers + agreeing-providers listed"
	curl -is -X POST "$BASE/" -H 'Content-Type: application/json' -d "$payload" | cv_grep | sed 's/^/    /'
}

scenario_2() {
	hr; echo "[2] NON-POLICY method '$OTHER_METHOD' -> NOT cross-validated (default)"
	local payload="{\"jsonrpc\":\"2.0\",\"method\":\"$OTHER_METHOD\",\"params\":[],\"id\":1}"
	note "curl -is -X POST $BASE/ -d '$payload'"
	echo "    expect: (no cross-validation headers)"
	curl -is -X POST "$BASE/" -H 'Content-Type: application/json' -d "$payload" | cv_grep | sed 's/^/    /'
}

scenario_3() {
	hr; echo "[3] '$OTHER_METHOD' + caller headers -> cross-validated (opt-in / 'use the flags')"
	local payload="{\"jsonrpc\":\"2.0\",\"method\":\"$OTHER_METHOD\",\"params\":[],\"id\":1}"
	note "curl -is -X POST $BASE/ \\"
	note "  -H 'smartrouter-cross-validation-max-participants: $CV_MAXPART' \\"
	note "  -H 'smartrouter-cross-validation-agreement-threshold: $CV_THRESHOLD' -d '$payload'"
	echo "    expect: status=success (header-driven CV on a non-policy method)"
	curl -is -X POST "$BASE/" -H 'Content-Type: application/json' \
		-H "smartrouter-cross-validation-max-participants: $CV_MAXPART" \
		-H "smartrouter-cross-validation-agreement-threshold: $CV_THRESHOLD" \
		-d "$payload" | cv_grep | sed 's/^/    /'
}

scenario_4() {
	hr; echo "[4] request more providers than exist -> structured FAILURE"
	local over=$((CV_MAXPART + 2))
	local payload="{\"jsonrpc\":\"2.0\",\"method\":\"$OTHER_METHOD\",\"params\":[],\"id\":1}"
	note "curl -is -X POST $BASE/ -H 'smartrouter-cross-validation-max-participants: $over' \\"
	note "  -H 'smartrouter-cross-validation-agreement-threshold: $CV_THRESHOLD' -d '$payload'"
	echo "    expect: status=failed, failure-reason=insufficient-capacity ($over > available providers)"
	curl -is -X POST "$BASE/" -H 'Content-Type: application/json' \
		-H "smartrouter-cross-validation-max-participants: $over" \
		-H "smartrouter-cross-validation-agreement-threshold: $CV_THRESHOLD" \
		-d "$payload" | cv_grep | sed 's/^/    /'
}

scenario_5() {
	hr; echo "[5] full response (headers + body) for the policy method"
	local h; h=$(resolve_height)
	if [ -z "$h" ]; then echo "    ✗ could not resolve a block height"; return; fi
	note "curl -i \"$BASE/$CV_METHOD?height=$h\""
	curl -is "$BASE/$CV_METHOD?height=$h" | head -40 | sed 's/^/    /'
}

scenario_6() {
	hr; echo "[6] cross-validation metrics"
	note "curl -s $METRICS | grep cross_validation"
	curl -s "$METRICS" 2>/dev/null | grep cross_validation | sed 's/^/    /' \
		|| echo "    (metrics endpoint not reachable on :$METRICS_PORT)"
}

if ! command -v jq >/dev/null 2>&1; then
	echo "WARNING: jq not found — scenarios 1 and 5 need it to resolve a block height (pass one as arg 2)."
fi

case "$SCENARIO" in
	1) scenario_1 ;;
	2) scenario_2 ;;
	3) scenario_3 ;;
	4) scenario_4 ;;
	5) scenario_5 ;;
	6) scenario_6 ;;
	all) scenario_1; scenario_2; scenario_3; scenario_4; scenario_5; scenario_6; hr ;;
	*) echo "unknown scenario '$SCENARIO' (use 1..6 or 'all')"; exit 2 ;;
esac
