#!/usr/bin/env bash
# Runs the unit tests of every Go module with coverage, then checks the share
# of the statements changed since <base-ref> that those tests run.
#
#   scripts/patch-coverage.sh <base-ref>
#
# The unit-coverage CI job runs it on a pull request's merge commit with
# HEAD^1, which is main. `make patch-coverage` runs it on the working tree
# against its merge base with origin/main, uncommitted changes included. An
# untracked file is not in the diff: `git add -N` it first.
#
# PATCH_COVERAGE_MIN (default 80) is the percentage of the changed statements
# a test must run. With GITHUB_REPOSITORY, PR_NUMBER and GH_TOKEN set (as in
# CI), a reviewer's no-unit-coverage label on the pull request lets a shortfall
# pass. docs/PR-GATES.md describes the check. Written for bash 3.2, the shell
# macOS ships.
set -euo pipefail

base="${1:?usage: scripts/patch-coverage.sh <base-ref>}"
min="${PATCH_COVERAGE_MIN:-80}"
root="$(git rev-parse --show-toplevel)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cd "$root"

git -c core.quotePath=false diff --no-color --no-ext-diff --unified=0 --find-renames \
  --src-prefix=a/ --dst-prefix=b/ "$base" -- '*.go' > "$work/change.diff"

patchcover() {
  go -C tools/prgate run ./cmd/patchcover -root "$root" -diff "$work/change.diff" "$@"
}

# The tests this change adds or edits. They must pass on their first run.
changed_tests="$(patchcover -changed-tests)"

# Every tracked go.mod is a module to test. One under testdata is a fixture.
modules=()
while IFS= read -r gomod; do
  modules+=("$(dirname "$gomod")")
done < <(git ls-files -- 'go.mod' '*/go.mod' | grep -v '/testdata/')

args=(-min "$min")
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  args+=(-summary "$GITHUB_STEP_SUMMARY")
fi
if [ -n "${GITHUB_REPOSITORY:-}" ] && [ -n "${PR_NUMBER:-}" ]; then
  args+=(-repo "$GITHUB_REPOSITORY" -pr "$PR_NUMBER")
fi

# failures prints "<package><TAB><Test1|Test2>" for every package a `go test`
# log marks FAIL, with the top-level tests that failed in it. go test prints a
# package's output in one block, its FAIL line last. A package that failed
# without naming a test (a build error, a timeout, a panic outside a test)
# gets an empty list.
failures() {
  awk -F'\t' '
    /^--- FAIL: / { split($0, f, " "); names = names (names == "" ? "" : "|") f[3]; next }
    /^ok[ \t]/ { names = ""; next }
    /^FAIL\t/ { pkg = $2; sub(/ .*/, "", pkg); print pkg "\t" names; names = "" }
  ' "$1"
}

# retry reruns, up to twice, the tests that failed in module $1 (log $2,
# profiles named after $3). A test that then passes is flaky: a warning, not a
# failure. Some tests on main lose a timing race under load, and a full-suite
# run on a 4-core runner is load. Each retry records coverage too: a test that
# panics writes none, so its first run may have left lines looking unrun. A
# package that failed without naming a test, and a test this change adds or
# edits, get no retry.
flaky=()
retries=0
retry() {
  local dir="$1" pkg names attempt passed name profile
  while IFS=$'\t' read -r pkg names; do
    if [ -z "$names" ]; then
      echo "::error title=Unit tests failed::$pkg failed without a failing test to retry: a build error, a timeout or a panic. See the log."
      return 1
    fi
    for name in ${names//|/ }; do
      if printf '%s\n' "$changed_tests" | grep -qxF "$name"; then
        echo "::error title=Unit tests failed::$name in $pkg failed, and this change adds or edits it: it must pass on its first run."
        return 1
      fi
    done
    passed=""
    for attempt in 1 2; do
      echo "Retry $attempt of $names in $pkg"
      retries=$((retries + 1))
      profile="$work/$3.retry$retries.cover"
      if (cd "$dir" && go test "$pkg" -count=1 -timeout 20m -run "^(${names})\$" \
        -covermode=set -coverpkg=./... -coverprofile="$profile" </dev/null); then
        passed="$attempt"
        args+=(-module "$dir=$profile")
        break
      fi
    done
    if [ -z "$passed" ]; then
      echo "::error title=Unit tests failed::$names in $pkg failed three times."
      return 1
    fi
    echo "::warning title=Flaky test::$names in $pkg failed, then passed on retry $passed."
    flaky+=("\`${names//|/\`, \`}\` in \`$pkg\`")
  done < <(failures "$2")
}

failed=0
for dir in "${modules[@]}"; do
  name="$(printf '%s' "$dir" | tr '/.' '__')"
  profile="$work/$name.cover"
  echo "::group::go test ./... in $dir"
  # -coverpkg=./... counts a test anywhere in the module for code anywhere in
  # it. A failing package still writes its coverage, unless a test panicked.
  if ! (cd "$dir" && go test ./... -count=1 -timeout 20m -covermode=set -coverpkg=./... -coverprofile="$profile") 2>&1 | tee "$work/$name.log"; then
    retry "$dir" "$work/$name.log" "$name" || failed=1
  fi
  echo "::endgroup::"
  args+=(-module "$dir=$profile")
done
if [ "$failed" -ne 0 ]; then
  echo "::error title=Unit tests failed::A unit test failed (see the log above), so the coverage of the change was not measured."
  exit 1
fi
if [ "${#flaky[@]}" -gt 0 ] && [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "### Flaky tests"
    echo
    echo "Failed, then passed on a retry. Each one is a bug in the test or in the code it runs:"
    echo
    printf -- '- %s\n' "${flaky[@]}"
    echo
  } >> "$GITHUB_STEP_SUMMARY"
fi

patchcover "${args[@]}"
