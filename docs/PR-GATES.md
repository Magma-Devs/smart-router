# Pull request gates

Two checks guard what reaches `main`, besides the build: unit tests must run
the code a pull request adds, and a change to the router must have an e2e test
in [smart-router-automation](https://github.com/Magma-Devs/smart-router-automation).
The code of both lives in `tools/prgate`.

A check blocks a merge only while the ruleset "Require PR Gate on main" lists
it as a required status check. Both catch mistakes. Neither stops someone with
write access who sets out to get around it, for instance by editing the
workflow in their own pull request.

## `unit-coverage`: unit tests run the changed statements

The `unit-coverage` job ([`unit-coverage.yml`](../.github/workflows/unit-coverage.yml))
runs on the pull request's merge commit:

1. It runs `go test ./...` in every Go module of the repository (every tracked
   `go.mod`: the router, `tools/wizard`, `tools/prgate`) with
   `-coverpkg=./...`, so a test anywhere in a module counts for code anywhere in
   that module.
2. It finds the statements the pull request adds or edits. A statement counts
   when a line of it changed; for an `if`, `for`, `switch` or `select`, a line
   of its header. Comments, blank lines and declarations hold no statement.
3. It counts the statements a test ran. A line holding two statements, such as
   `if err != nil { return err }`, counts two, each covered or not on its own.

Tests (`_test.go`) and files the go tool ignores (`testdata/`, directories
whose names start with `.` or `_`) are not measured. Neither is generated code:
a file whose `// Code generated ... DO NOT EDIT.` line was there before the
change, or a new file with that line and a generator's name (`.pb.go`,
`_mock.go`, `mock_*.go`, `_string.go`).

A file the tests do not compile counts in full, none of it covered, unless it
builds only for another `GOOS` or `GOARCH` (`ipc_windows.go`,
`//go:build darwin`). A tag such as `netgo` or `cgo`, which the release sets and
the tests do not, does not hide code from the check.

The job fails when a test fails, or when tests run less than **80%** of the
changed statements. Its summary lists every file with the lines no test runs,
and the first ten show as annotations on the diff. When a gap is right (flag
wiring, a branch no unit test can reach), a reviewer other than the author adds
the `no-unit-coverage` label, after the latest commit: a later push voids it.
The tests must still pass.

A test that fails runs again, up to twice, with coverage. When a retry passes,
the job warns that the test is flaky, names it in the summary, and goes on.
Some tests lose a timing race when the whole suite shares a few cores, and
they would otherwise block pull requests at random. A test the pull request
adds or edits gets no retry, and neither does a package that does not build.

Run the same check before pushing:

```bash
make patch-coverage                          # against the merge base with origin/main
PATCH_COVERAGE_MIN=90 make patch-coverage    # another threshold, for one run
```

It measures the working tree, uncommitted changes included. A new file counts
once git tracks it (`git add -N <file>`).

## `e2e-test/linked`: an automation test names the ticket

The `e2e-test/linked` commit status
([`e2e-test-gate.yml`](../.github/workflows/e2e-test-gate.yml)) applies to a
pull request that adds code to the router: lines other than blanks and line
comments in any `.go` file other than tests, `testdata/`, `tools/`, and
generated mocks and protobuf code. Any other pull request passes with "No
router Go code changed".

The Jira ticket links the two repositories. The gate reads the tickets on the
pull request's `Jira ticket: MAG-123` lines, and nothing else: not the title,
not the branch name.

It passes when a test on smart-router-automation's `main` names one of those
tickets. A test is a `test_*.py` or `*_test.py` file under `tests/` that names
the ticket: in an xfail or `expose` marker, an allure tag, or its docstring.
Tests of the automation framework do not count: files under `tests/unit/` or
`tests/infrastructure/unit/`, or marked `pytest.mark.unit`. The automation
repository's verify-bug-fix skill finds the tests of a ticket by the same rule.

The test must be merged. A test for a bug the pull request fixes merges first,
with an xfail marker that names the ticket, and verify-bug-fix removes the
marker once the fix lands. When the e2e test tracks a ticket of its own (QA
often files one), it names the router change's ticket too.

A change no e2e test can see (a refactor, a log line, a speed-up with no other
effect) gets the `no-e2e-test` label. It counts only when someone other than the
pull request's author adds it, after the latest commit: a later push voids it.
A pull request from a fork passes only that way. The gate never reads
smart-router-automation on a fork's behalf.

The status updates on every push, description edit and label change. A sweep
checks the open pull requests against `main` that are not green every hour, so
a test merged later turns the status green without a new push. To check one at
once, run the "E2E test gate" workflow from the Actions tab with the pull
request's number.

This repository is public and smart-router-automation is not. The gate's
statuses and job summaries show ticket keys and counts, never automation paths
or test names. It clones smart-router-automation's `tests/` with the
`SMART_ROUTER_AUTOMATION_READ_TOKEN` secret, which needs read access to its
contents and nothing more.
