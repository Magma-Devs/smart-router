#!/bin/bash
# Shared helpers for the cross-validation lanes.
#
#   init_smartrouter_cv_demo.sh      the policies + groups lane
#   init_smartrouter_cv_default.sh   the no-configuration lane (UC-7)
#
# The two lanes differ only in the config they generate and the checks they run.
# Everything else — process ownership, the port pre-flight, request and metric
# helpers, the simulator control plane — is identical, so it lives here once.
# That is not only tidiness: an ownership bug fixed in one copy and not the other
# is a bug that still kills someone's router, and this file is what makes
# "fixed once" true.
#
# Sourced, never executed. The sourcing lane must set, before sourcing:
#   PROJECT_ROOT   the checkout root
#   ROUTER_PORT    the lane's RPC listener
#   METRICS_PORT   the lane's prometheus port
# and may set SIM_CONTROL (defaults to the standard simulator control address).

SIM_CONTROL="${SIM_CONTROL:-127.0.0.1:19000}"

# =============================================================================
# Failing loudly
# =============================================================================

# die <message...> — a harness that cannot trust its own setup must stop, not
# carry on and report the consequences as findings about the router.
die() {
    printf '\nERROR: %s\n' "$*" >&2
    exit 1
}

# =============================================================================
# Process ownership
# =============================================================================

# A pid alone is not proof — pids get recycled. Record the process start time
# alongside it and require both to match before signalling anything.
proc_fingerprint() { ps -p "$1" -o lstart= 2>/dev/null | tr -s ' '; }

# lane_tag — a stable, per-checkout suffix.
#
# Screen session names and pid files are global to the user, so a bare name like
# "sr-cv-demo" is shared by every checkout of this repo on the machine. Deriving
# the suffix from the checkout path keeps a second checkout's lane from
# reclaiming the first one's session (see reclaim_by_identity for the same
# reasoning applied to the config path).
lane_tag() { printf '%s' "$PROJECT_ROOT" | cksum | awk '{print $1}'; }

# reclaim_owned <pidfile> <what> — stop ONLY a process this lane recorded
# starting. Quitting the screen session is not sufficient: the router runs
# inside a `... | tee` pipeline, so it outlives `screen -X quit` and would hold
# its port against the next run.
reclaim_owned() {
    local pidfile="$1" what="$2"
    [[ -f "$pidfile" ]] || return 0
    local rec_pid rec_fp cur_fp
    rec_pid=$(cut -d'|' -f1 "$pidfile" 2>/dev/null)
    rec_fp=$(cut -d'|' -f2- "$pidfile" 2>/dev/null)
    [[ -n "$rec_pid" ]] || { rm -f "$pidfile"; return 0; }
    cur_fp=$(proc_fingerprint "$rec_pid")
    if [[ -z "$cur_fp" ]]; then rm -f "$pidfile"; return 0; fi          # already gone
    if [[ "$cur_fp" != "$rec_fp" ]]; then
        echo "  stale pid file for the $what (pid $rec_pid now belongs to another process) — leaving it alone"
        rm -f "$pidfile"; return 0
    fi
    echo "  stopping this lane's previous $what (pid $rec_pid)"
    kill "$rec_pid" 2>/dev/null || true
    local dead=0 _
    for _ in $(seq 1 20); do
        if [[ -z "$(proc_fingerprint "$rec_pid")" ]]; then dead=1; break; fi
        sleep 0.25
    done
    # Only forget the record once the process is actually gone. Removing it while
    # the process still lives would strand it: the next run has no record to
    # reclaim by, and the port pre-flight would (correctly) refuse to continue.
    if [[ "$dead" == "1" ]]; then
        rm -f "$pidfile"
    else
        echo "  WARNING: the $what (pid $rec_pid) did not exit; keeping its ownership record"
    fi
}

# record_owned <pidfile> <port> <what> — resolve the pid holding <port> and
# record it with its start-time fingerprint, so the next run can reclaim exactly
# this process and nothing else.
record_owned() {
    local pidfile="$1" port="$2" what="$3" pid="" _
    for _ in $(seq 1 60); do
        pid=$(lsof -nP -iTCP:"${port}" -sTCP:LISTEN -t 2>/dev/null | head -1)
        [[ -n "$pid" ]] && break
        sleep 1
    done
    if [[ -z "$pid" ]]; then
        echo "ERROR: the $what never bound :${port} — refusing to continue without an"
        echo "       ownership record, since a later run could not tell it from a foreign process."
        return 1
    fi
    printf '%s|%s\n' "$pid" "$(proc_fingerprint "$pid")" > "$pidfile"
    echo "  $what pid ${pid} recorded"
    return 0
}

# reclaim_by_identity <port> <absolute-needle> <what> — fallback ownership signal
# for an interrupted run that never wrote a pid file.
#
# The needle MUST be absolute. A relative config path such as
# "debugging/smartrouter_cv_demo.yml" is byte-identical in every checkout of this
# repo, so matching on it would let a lane run from checkout B kill checkout A's
# router — the exact opposite of the safety this function exists to provide. An
# absolute path is unique to one checkout, so a match really is proof of ours.
reclaim_by_identity() {
    local port="$1" needle="$2" what="$3" pid _
    case "$needle" in
        /*) : ;;
        *) die "reclaim_by_identity needs an ABSOLUTE needle (got '$needle'); a relative path matches every checkout" ;;
    esac
    pid=$(lsof -nP -iTCP:"${port}" -sTCP:LISTEN -t 2>/dev/null | head -1)
    [[ -n "$pid" ]] || return 0
    ps -p "$pid" -o command= 2>/dev/null | grep -qF -- "$needle" || return 0
    echo "  stopping this lane's $what by identity (pid $pid, no pid file)"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
        lsof -nP -iTCP:"${port}" -sTCP:LISTEN -t >/dev/null 2>&1 || break
        sleep 0.25
    done
}

# require_free_ports <override-hint> <port...> — refuse to run when a port this
# lane needs is held by something it does not own. Never signals anything.
require_free_ports() {
    local hint="$1"; shift
    local blocked=0 port
    for port in "$@"; do
        if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
            if [[ "$blocked" == "0" ]]; then
                echo ""
                echo "ERROR: this lane's port(s) are held by process(es) it does not own."
                echo "       Refusing to run. Nothing was signalled or removed."
            fi
            echo "  port $port:"
            lsof -nP -iTCP:"$port" -sTCP:LISTEN | sed 's/^/    /'
            blocked=1
        fi
    done
    if [[ "$blocked" == "1" ]]; then
        echo ""
        echo "Stop the owning process yourself, or run the lane on free ports:"
        echo "  $hint"
        exit 1
    fi
}

# =============================================================================
# Bounded execution — a portable `timeout`
# =============================================================================

# run_bounded <seconds> <logfile> <workdir> <cmd...> : run a command from
# <workdir> with its output in <logfile>, killed after <seconds>. Returns the
# command's status, or 124 if it had to be killed.
#
# coreutils `timeout` is NOT present on a stock macOS, which these lanes target,
# and a missing binary there turns a working startup fail-fast into two reported
# FAILs. `env -C` is no better — it is absent on older macOS — hence the explicit
# workdir argument. The `exec` matters: it replaces the subshell with the
# command, so $! is the command's own pid and `kill` reaches it rather than a
# shell that has already forked away from it.
run_bounded() {
    local secs="$1" log="$2" workdir="$3"; shift 3
    ( cd "$workdir" && exec "$@" ) > "$log" 2>&1 &
    local pid=$! waited=0 rc
    while kill -0 "$pid" 2>/dev/null; do
        if [[ "$waited" -ge "$secs" ]]; then
            kill "$pid" 2>/dev/null || true
            sleep 2
            kill -9 "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null
            return 124
        fi
        sleep 1
        waited=$((waited + 1))
    done
    wait "$pid"; rc=$?
    return $rc
}

# =============================================================================
# Assertions
# =============================================================================
PASS=0
FAIL=0
pass() { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }
check() { # check <description> <actual> <expected>
    if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1 (got '$2', want '$3')"; fi
}
# check_gt <description> <actual> <floor> — for deltas, where the assertion is
# "this run moved it", not "it has ever been non-zero".
check_gt() {
    if [[ "${2:-0}" -gt "${3:-0}" ]]; then pass "$1"; else fail "$1 (got '${2:-0}', want > ${3:-0})"; fi
}
note() { printf '    %-38s %s\n' "$1" "$2"; }

# =============================================================================
# Requests
# =============================================================================

# addr <n> : a distinct account per call, so no two checks can share a cached
# response or an upstream-side memoisation.
addr() { printf '0x%040x' "$1"; }

# call <method> <params-json> [curl args...] : one request through the router.
# Headers land in $HDR; the body is echoed. $HDR and $ROUTER_PORT come from the
# sourcing lane.
call() {
    local method="$1" params="$2"; shift 2
    curl -s -m 40 -D "$HDR" -X POST "http://127.0.0.1:${ROUTER_PORT}" \
        -H 'Content-Type: application/json' "$@" \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"${method}\",\"params\":${params},\"id\":1}"
}

hdr() { grep -i "^lava-cross-validation-$1:" "$HDR" | head -1 | tr -d '\r' | cut -d' ' -f2-; }
cv_present() { grep -qi '^lava-cross-validation-' "$HDR"; }
count_csv() { printf '%s' "$1" | tr ',' '\n' | grep -c '[^[:space:]]' | tr -d ' '; }

# router_healthy [port] : strictly 200, not merely "curl connected".
#
# `curl -o /dev/null` exits 0 on a 503, and /lava/health answers 503 while the
# router is up but not serving — so a lax probe accepts exactly the state that
# makes every downstream check fail with an empty header dump.
router_healthy() {
    local port="${1:-$ROUTER_PORT}"
    [[ "$(curl -s -o /dev/null -w '%{http_code}' -m 5 "http://127.0.0.1:${port}/lava/health" 2>/dev/null)" == "200" ]]
}

# wait_healthy <port> [screen-name] : poll until the listener answers 200. Gives
# up early if the named screen session has gone, so a router that died during
# startup is reported at once rather than after the full timeout.
wait_healthy() {
    local port="$1" screen_name="${2:-}" _
    for _ in $(seq 1 60); do
        router_healthy "$port" && return 0
        if [[ -n "$screen_name" ]] && ! screen -list 2>/dev/null | grep -q "$screen_name"; then
            return 1
        fi
        sleep 1
    done
    return 1
}

# wait_providers <port> <expected> <method> : poll until a header-driven
# cross-validated call sees <expected> providers in lava-cross-validation-all-providers.
#
# A 200 from /lava/health is NOT a provider-readiness signal — the listener
# starts in a goroutine while provider validation is still running, so a request
# issued straight after it can see a partial fleet and fail
# insufficient-capacity. This polls the thing that actually matters. It warms on
# a method the caller does not assert on, so the per-method counters the checks
# read stay clean.
wait_providers() {
    local port="$1" expected="$2" method="$3" n=0 i
    for i in $(seq 1 45); do
        curl -s -m 20 -D "$HDR" -o /dev/null -X POST "http://127.0.0.1:${port}" \
            -H 'Content-Type: application/json' \
            -H "lava-cross-validation-max-participants: ${expected}" \
            -H "lava-cross-validation-agreement-threshold: 2" \
            -d "{\"jsonrpc\":\"2.0\",\"method\":\"${method}\",\"params\":[\"$(addr $((900 + i)))\",\"latest\"],\"id\":1}" \
            2>/dev/null || true
        n=$(count_csv "$(hdr all-providers)")
        [[ "${n:-0}" -ge "$expected" ]] && return 0
        sleep 1
    done
    return 1
}

# =============================================================================
# Metrics
# =============================================================================

# metrics_scrape : the whole /metrics body, once. Checks that need several
# series from the same instant should scrape once and use metric_in.
metrics_scrape() { curl -s -m 10 "http://127.0.0.1:${METRICS_PORT}/metrics" 2>/dev/null; }

# metric_in <blob> <substring...> : sum every line of <blob> matching ALL the
# given substrings. Absent series read 0, so a delta is always computable.
metric_in() {
    local out="$1"; shift
    local pat
    for pat in "$@"; do out=$(printf '%s' "$out" | grep -F -- "$pat"); done
    printf '%s' "$out" | awk '{s+=$NF} END {printf "%d", s+0}'
}

# metric <substring...> : the same, scraped live. Convenience for one-off reads.
metric() { metric_in "$(metrics_scrape)" "$@"; }

# =============================================================================
# provider_simulator control plane
# =============================================================================
sim_up() { curl -s --max-time 2 "http://$SIM_CONTROL/health" >/dev/null 2>&1; }

# sim_scenario <json> : POST a scenario fragment. A rejected scenario is FATAL.
#
# The simulator 400s on an unknown provider key or field and leaves the previous
# scenario fully intact, so a silently-dropped injection means the next check
# runs against the last use case's overrides — and reports the resulting
# mismatch as a router defect. There is no outcome where continuing is useful.
sim_scenario() {
    local response body code
    response=$(curl -s -w '\n%{http_code}' -X POST "http://$SIM_CONTROL/scenario" \
        -H 'Content-Type: application/json' -d "$1" 2>/dev/null)
    code=$(printf '%s' "$response" | tail -n1)
    body=$(printf '%s' "$response" | sed '$d')
    [[ "$code" == "200" ]] && return 0
    die "the simulator rejected a scenario (HTTP ${code:-none}): ${body:-no response}
       Injection failed, so any check run after this point would be measuring the
       previous scenario's overrides and blaming the router for them."
}

# sim_find_dir : locate the sibling provider_simulator checkout.
#
# Searches upward rather than assuming one layout: a working copy nested under a
# per-branch directory sits two levels below the workspace root, not one.
# SIM_DIR=... overrides the search entirely.
sim_find_dir() {
    [[ -n "$SIM_DIR" ]] && return 0
    local candidate
    for candidate in "$PROJECT_ROOT/.." "$PROJECT_ROOT/../.." "$PROJECT_ROOT/../../.."; do
        if [[ -f "$candidate/provider_simulator/run.py" ]]; then
            SIM_DIR=$(cd "$candidate/provider_simulator" && pwd)
            return 0
        fi
    done
    SIM_DIR="$(cd "$PROJECT_ROOT/.." && pwd)/provider_simulator"
}

# sim_ensure <logfile> : reuse a running simulator, or start one.
#
# The simulator is shared infrastructure — other harnesses drive it too — so it
# is never torn down, and never reset globally. Each lane states the providers it
# owns explicitly instead (see the lane's fleet_clean): POST /reset/all is
# unscoped and would wipe every other harness's fixtures along with its own.
sim_ensure() {
    local sim_log="$1"
    sim_find_dir
    if sim_up; then
        echo "[Setup] provider_simulator already running on $SIM_CONTROL (reusing it)"
        return 0
    fi
    echo "[Setup] starting provider_simulator from $SIM_DIR"
    [[ -f "$SIM_DIR/run.py" ]] || die "$SIM_DIR/run.py not found. Set SIM_DIR=... to the provider_simulator checkout."
    local sim_py _; sim_py="$(command -v python3.12 || command -v python3)"
    # Redirecting the SUBSHELL too, not just the simulator: a backgrounded child
    # that inherits this script's stdout holds the write end of the pipe, so
    # `lane | tee` never sees EOF and appears to hang after the lane has finished.
    ( cd "$SIM_DIR" && nohup "$sim_py" -u run.py > "$sim_log" 2>&1 & ) >/dev/null 2>&1 </dev/null
    for _ in $(seq 1 30); do sim_up && break; sleep 1; done
    sim_up || {
        tail -n 20 "$sim_log" 2>/dev/null | sed 's/^/    /'
        die "the simulator did not become ready. See $sim_log"
    }
    echo "  simulator up (control $SIM_CONTROL)"
}

# =============================================================================
# Config writing
# =============================================================================

# write_config_atomic <destination> <emitter-function> [args...] : run the
# emitter, capture its output, and install it only if every write succeeded.
#
# A bare `{ ... } > "$file"` discards its status, so a root-owned target or a
# full disk leaves the PREVIOUS config in place while the lane reports the
# numbers it believes it just wrote — and the router boots the stale file.
write_config_atomic() {
    local dest="$1"; shift
    local tmp="${dest}.tmp.$$"
    if ! "$@" > "$tmp" 2>/dev/null; then
        rm -f "$tmp"
        die "could not generate $dest (the emitter failed)"
    fi
    [[ -s "$tmp" ]] || { rm -f "$tmp"; die "generated an empty $dest"; }
    mv -f "$tmp" "$dest" || { rm -f "$tmp"; die "could not write $dest (permissions? disk full?)"; }
}
