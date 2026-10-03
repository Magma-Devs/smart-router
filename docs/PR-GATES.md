# Pull request gates

Besides the build, one check guards what reaches `main`: unit tests must run
the code a pull request adds. Its code lives in `tools/prgate`.

The check blocks a merge only while the ruleset "Require PR Gate on main" lists
it as a required status check. It catches mistakes. It does not stop someone
with write access who sets out to get around it, for instance by editing the
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
