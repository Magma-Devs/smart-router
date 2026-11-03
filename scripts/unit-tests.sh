#!/usr/bin/env bash
# Runs the router module's unit tests the way the unit-tests check does (.github/workflows/unit-tests.yml).
#
# Every package goes through gotestsum, and protocol/rpcsmartrouter is split into SHARDS processes
# that run beside the rest. Its ~1,000 tests run one after another and mostly wait on timers, so as
# one process it takes about 4 minutes while every other package finishes in under 1.5. The shards
# are cut from `go test -list`, so a new test lands in one of them without anyone listing it.
#
# A failed test is rerun on its own, at most twice, and fails the run only if every run fails. A few
# timing-based tests fail now and then on a busy machine, and a check that goes red on those teaches
# people to rerun it until it passes. gotestsum writes each test that needed a rerun to
# $REPORT_DIR/reruns-<process>.txt, which the workflow turns into warnings. It does not rerun after a
# compile error, a panic (an expired -timeout is a panic), a data race, or more than 10 failures.
#
# Environment:
#   SHARDS           rpcsmartrouter processes (default 4); 1 runs every package in one process
#   PACKAGE_TIMEOUT  go test -timeout, per test binary (default 15m)
#   RACE             set to -race to run under the race detector (needs CGO_ENABLED=1)
#   REPORT_DIR       where the rerun reports go (default $RUNNER_TEMP, else a new temp dir)
#
# Needs gotestsum on PATH. Each output line starts with the [process] it came from.
set -euo pipefail

sharded=github.com/magma-Devs/smart-router/protocol/rpcsmartrouter

# run_part NAME PACKAGES [RUN]: one gotestsum process over PACKAGES, limited to the tests RUN matches.
run_part() {
	local name=$1 packages=$2 run=${3:-}
	local args=(-count=1 "-timeout=${PACKAGE_TIMEOUT:-15m}")
	if [ -n "${RACE:-}" ]; then args+=("$RACE"); fi
	if [ -n "$run" ]; then args+=("-run=$run"); fi
	gotestsum \
		--format pkgname-and-test-fails \
		--format-hide-empty-pkg \
		--hide-summary=skipped \
		--rerun-fails=2 \
		--rerun-fails-max-failures=10 \
		--rerun-fails-abort-on-data-race \
		--rerun-fails-report="$report_dir/reruns-$name.txt" \
		--packages="$packages" \
		-- "${args[@]}" 2>&1 | awk -v prefix="[$name] " '{ print prefix $0; fflush() }'
}

main() {
	local shards=${SHARDS:-4}
	report_dir=${REPORT_DIR:-${RUNNER_TEMP:-$(mktemp -d)}}

	# One process for everything: what the race leg uses. Under -race the tests are CPU-bound, so
	# more processes on the same runner only slow each other down.
	if [ "$shards" -le 1 ]; then
		run_part all ./...
		return
	fi

	local others tests
	others=$(go list ./... | grep -vxF "$sharded" | tr '\n' ' ')
	# The shards are cut from this list, so a list that cannot be read (a test that does not
	# compile, say) fails the run here instead of running nothing.
	tests=$(go test -list '.*' "$sharded" | grep -E '^(Test|Fuzz|Example)')

	local -a pids=() names=()
	run_part others "$others" &
	pids+=("$!") names+=(others)
	local i regex
	for ((i = 0; i < shards; i++)); do
		regex="^($(awk -v n="$shards" -v i="$i" 'NR % n == i' <<<"$tests" | paste -sd '|' -))\$"
		run_part "rpcsmartrouter-$((i + 1))" "$sharded" "$regex" &
		pids+=("$!") names+=("rpcsmartrouter-$((i + 1))")
	done

	local status=0 k
	for k in "${!pids[@]}"; do
		if ! wait "${pids[$k]}"; then
			echo "::error::unit tests failed in ${names[$k]}"
			status=1
		fi
	done
	return "$status"
}

main "$@"
exit "$?"
