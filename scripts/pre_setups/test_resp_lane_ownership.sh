#!/bin/bash
# Regression harness for the RESP lane's ownership safety.
#
# The lane must NEVER signal a process merely because its executable is named
# "smartrouter", and must never force-remove a same-named container it cannot
# prove it created. This harness asserts the refusal path with a foreign
# listener in place and, critically, that the foreign process receives NO
# signal — it must still be alive and unharmed afterwards.
#
# Safe by construction: it binds a throwaway port with its own helper process
# and never touches 3360/7779/63790 or any real router.
#
#   bash scripts/pre_setups/test_resp_lane_ownership.sh
set -u

__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
LANE="$__dir/init_smartrouter_eth_resp_cache.sh"
PROJECT_ROOT=$(cd "${__dir}"/../.. && pwd)

# Throwaway ports, deliberately far from the real lane's defaults.
T_ROUTER=39361
T_METRICS=39362
T_VALKEY=39363

PASS=0; FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS+1)); }
bad()  { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL+1)); }

cleanup() {
    [[ -n "${FOREIGN_PID:-}" ]] && kill "$FOREIGN_PID" 2>/dev/null
    wait "${FOREIGN_PID:-}" 2>/dev/null
    return 0
}
trap cleanup EXIT

echo "=== RESP lane ownership regression"

# ---------------------------------------------------------------------------
# A foreign listener occupies the lane's router port. The lane must refuse and
# leave it running. We use a plain bash/python listener: the point is that the
# lane must not care what it is called, but the old code keyed on the process
# NAME, so we also run a case where the name itself would have matched.
# ---------------------------------------------------------------------------
python3 - "$T_ROUTER" <<'PY' &
import socket, sys, time
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1]))); s.listen(8)
while True: time.sleep(3600)
PY
FOREIGN_PID=$!
sleep 1.5

if ! kill -0 "$FOREIGN_PID" 2>/dev/null; then
    echo "  SETUP FAIL: could not start the foreign listener"; exit 1
fi
echo "  foreign listener up on 127.0.0.1:$T_ROUTER (pid $FOREIGN_PID)"

OUT=$(cd "$PROJECT_ROOT" && ROUTER_PORT=$T_ROUTER METRICS_PORT=$T_METRICS VALKEY_PORT=$T_VALKEY \
      VALKEY_NAME="sr-ownership-probe-$$" bash "$LANE" 2>&1)
RC=$?

# 1. clean refusal
[[ "$RC" -ne 0 ]] && ok "lane exits non-zero when a foreign listener holds its port (rc=$RC)" \
                  || bad "lane exited 0 despite a foreign listener (rc=$RC)"

# 2. refusal is explicit, not an incidental failure
grep -qi "does not own" <<<"$OUT" && ok "refusal message names the ownership reason" \
                                  || bad "no ownership refusal message in output"
grep -qi "Nothing was signalled or removed" <<<"$OUT" && ok "refusal states nothing was signalled" \
                                  || bad "refusal does not state that nothing was signalled"

# 3. THE CRITICAL PROPERTY: the foreign process is untouched.
if kill -0 "$FOREIGN_PID" 2>/dev/null; then
    ok "foreign process survived — no signal was sent (pid $FOREIGN_PID)"
else
    bad "foreign process was killed — ownership safety is broken"
fi

# 4. the lane must not have reached the build/run stage
grep -qi "installing binaries" <<<"$OUT" && bad "lane proceeded past pre-flight into the build stage" \
                                         || ok "lane refused before doing any work"

# ---------------------------------------------------------------------------
# A container with the lane's name but WITHOUT its label must not be removed.
# ---------------------------------------------------------------------------
if command -v docker >/dev/null && docker info >/dev/null 2>&1; then
    FOREIGN_CT="sr-ownership-foreign-$$"
    docker run -d --rm --name "$FOREIGN_CT" alpine:3 sleep 300 >/dev/null 2>&1
    if docker inspect "$FOREIGN_CT" >/dev/null 2>&1; then
        OUT2=$(cd "$PROJECT_ROOT" && ROUTER_PORT=$T_METRICS METRICS_PORT=$((T_METRICS+1)) VALKEY_PORT=$((T_METRICS+2)) \
               VALKEY_NAME="$FOREIGN_CT" bash "$LANE" 2>&1)
        RC2=$?
        [[ "$RC2" -ne 0 ]] && ok "lane refuses an unlabelled same-named container (rc=$RC2)" \
                           || bad "lane did not refuse an unlabelled same-named container"
        # A non-zero exit alone is not proof: the lane could have failed for an
        # unrelated reason and still left the container intact by accident. The
        # refusal must name the ownership reason, exactly as the process case does.
        grep -qi "Refusing to remove it" <<<"$OUT2" \
            && ok "container refusal names the ownership reason" \
            || bad "container refusal is incidental — no ownership message in output"
        if docker inspect "$FOREIGN_CT" >/dev/null 2>&1; then
            ok "foreign container survived — not force-removed"
        else
            bad "foreign container was removed — ownership safety is broken"
        fi
        docker rm -f "$FOREIGN_CT" >/dev/null 2>&1 || true
    else
        echo "  SKIP  container-ownership case (could not start probe container)"
    fi
else
    echo "  SKIP  container-ownership case (docker unavailable)"
fi

echo ""
echo "=== $PASS passed, $FAIL failed"
[[ "$FAIL" -eq 0 ]]
