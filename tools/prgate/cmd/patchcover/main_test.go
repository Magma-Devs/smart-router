package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/tools/prgate/internal/fakegh"
)

const mod = "github.com/example/repo"

// fixture writes a one-module repository where a test ran the if of A and its
// final return, not the if's body, plus a diff adding all three statements.
func fixture(t *testing.T) (root, diff, profile string) {
	t.Helper()
	root = t.TempDir()
	mustWrite(t, filepath.Join(root, "go.mod"), "module "+mod+"\n")
	mustWrite(t, filepath.Join(root, "pkg", "a.go"), "package pkg\n\nfunc A(b bool) int {\n\tif b {\n\t\treturn 1\n\t}\n\treturn 2\n}\n")
	mustWrite(t, filepath.Join(root, "pkg", "a_test.go"), "package pkg\n\nimport \"testing\"\n\nfunc TestA(t *testing.T) {\n\tA(false)\n}\n")
	diff = filepath.Join(root, "change.diff")
	mustWrite(t, diff, "diff --git a/pkg/a.go b/pkg/a.go\n--- a/pkg/a.go\n+++ b/pkg/a.go\n@@ -3,0 +4,2 @@\n+\tif b {\n+\t\treturn 1\n@@ -4,0 +7 @@\n+\treturn 2\n"+
		"diff --git a/pkg/a_test.go b/pkg/a_test.go\n--- a/pkg/a_test.go\n+++ b/pkg/a_test.go\n@@ -5,0 +6 @@\n+\tA(false)\n")
	profile = filepath.Join(root, "cover.out")
	mustWrite(t, profile, "mode: set\n"+mod+"/pkg/a.go:3.20,4.7 1 1\n"+mod+"/pkg/a.go:4.7,6.3 1 0\n"+mod+"/pkg/a.go:7.2,7.10 1 1\n")
	return root, diff, profile
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func noEnv(string) string { return "" }

func TestRunPassAndFail(t *testing.T) {
	ctx := context.Background()
	root, diff, profile := fixture(t)
	base := []string{"-root", root, "-diff", diff, "-module", ".=" + profile}

	var out, errOut strings.Builder
	if code := run(ctx, append(base, "-min", "60"), noEnv, &out, &errOut); code != 0 {
		t.Fatalf("2 of 3 statements at 60%%: exit %d, stderr %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "**2 of 3** changed statements") || !strings.Contains(out.String(), "file=pkg/a.go,line=5,endLine=5") {
		t.Errorf("stdout lacks the summary or the annotation:\n%s", out.String())
	}

	out.Reset()
	summary := filepath.Join(root, "summary.md")
	if code := run(ctx, append(base, "-min", "80", "-summary", summary), noEnv, &out, &errOut); code != 1 {
		t.Fatalf("2 of 3 statements at 80%%: exit %d, want 1", code)
	}
	if !strings.Contains(out.String(), "::error title=Unit test coverage::") {
		t.Errorf("a failing run must print an error annotation:\n%s", out.String())
	}
	if written, err := os.ReadFile(summary); err != nil || !strings.Contains(string(written), "**fail**") {
		t.Errorf("summary file: %q, %v", written, err)
	}
}

func TestRunChangedTests(t *testing.T) {
	root, diff, _ := fixture(t)
	var out, errOut strings.Builder
	if code := run(context.Background(), []string{"-root", root, "-diff", diff, "-changed-tests"}, noEnv, &out, &errOut); code != 0 || out.String() != "TestA\n" {
		t.Fatalf("exit %d, stdout %q, stderr %s", code, out.String(), errOut.String())
	}
	if code := run(context.Background(), []string{"-root", t.TempDir(), "-diff", diff, "-changed-tests"}, noEnv, &out, &errOut); code != 2 {
		t.Fatalf("a test file that is not there: exit %d, want 2", code)
	}
}

func TestRunWaiver(t *testing.T) {
	ctx := context.Background()
	root, diff, profile := fixture(t)
	api := fakegh.New()
	defer api.Close()
	committed := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)
	pr := map[string]any{
		"number": 1, "state": "open", "user": map[string]any{"login": "alice", "type": "User"},
		"head": map[string]any{"sha": "abc"}, "labels": []any{map[string]any{"name": "no-unit-coverage"}},
	}
	api.JSON("GET /repos/o/r/pulls/1", pr)
	api.JSON("GET /repos/o/r/commits/abc", map[string]any{"commit": map[string]any{"committer": map[string]any{"date": committed}}})
	api.JSON("GET /repos/o/r/issues/1/events", []any{map[string]any{
		"event": "labeled", "actor": map[string]any{"login": "bob", "type": "User"},
		"label": map[string]any{"name": "no-unit-coverage"}, "created_at": committed.Add(time.Minute),
	}})
	args := []string{"-root", root, "-diff", diff, "-module", ".=" + profile, "-min", "80", "-repo", "o/r", "-pr", "1", "-api", api.URL}
	token := func(k string) string {
		if k == "GH_TOKEN" {
			return "t"
		}
		return ""
	}

	var out, errOut strings.Builder
	if code := run(ctx, args, token, &out, &errOut); code != 0 || !strings.Contains(out.String(), "Waived by @bob with the no-unit-coverage label") {
		t.Fatalf("a reviewer's waiver: exit %d\n%s", code, out.String())
	}

	pr["labels"] = []any{}
	api.JSON("GET /repos/o/r/pulls/1", pr)
	out.Reset()
	if code := run(ctx, args, token, &out, &errOut); code != 1 || !strings.Contains(out.String(), "a reviewer other than the author adds the no-unit-coverage label") {
		t.Fatalf("no waiver: exit %d\n%s", code, out.String())
	}

	pr["labels"] = []any{map[string]any{"name": "no-unit-coverage"}}
	api.JSON("GET /repos/o/r/pulls/1", pr)
	api.Fail("GET /repos/o/r/issues/1/events", 403)
	out.Reset()
	if code := run(ctx, args, token, &out, &errOut); code != 1 || !strings.Contains(out.String(), "could not be read") {
		t.Fatalf("an unreadable waiver: exit %d\n%s", code, out.String())
	}

	api.Fail("GET /repos/o/r/pulls/1", 403)
	out.Reset()
	if code := run(ctx, args, token, &out, &errOut); code != 1 || !strings.Contains(out.String(), "could not be read") {
		t.Fatalf("an unreadable pull request: exit %d\n%s", code, out.String())
	}

	api.Unfail("GET /repos/o/r/pulls/1")
	api.Unfail("GET /repos/o/r/issues/1/events")
	api.JSON("GET /repos/o/r/issues/1/events", []any{map[string]any{
		"event": "labeled", "actor": map[string]any{"login": "alice", "type": "User"},
		"label": map[string]any{"name": "no-unit-coverage"}, "created_at": committed.Add(time.Minute),
	}})
	out.Reset()
	if code := run(ctx, args, token, &out, &errOut); code != 1 || !strings.Contains(out.String(), "the author added it") {
		t.Fatalf("the author's own waiver: exit %d\n%s", code, out.String())
	}
}

func TestRunUsageErrors(t *testing.T) {
	root, diff, profile := fixture(t)
	for name, args := range map[string][]string{
		"no diff":          {"-module", ".=" + profile},
		"no module":        {"-diff", diff},
		"min out of range": {"-diff", diff, "-module", ".=" + profile, "-min", "101"},
		"unknown flag":     {"-nope"},
		"bad module flag":  {"-root", root, "-diff", diff, "-module", "." + profile},
		"missing go.mod":   {"-root", root, "-diff", diff, "-module", "tools=" + profile},
		"missing profile":  {"-root", root, "-diff", diff, "-module", ".=" + profile + ".gone"},
		"missing diff":     {"-root", root, "-diff", diff + ".gone", "-module", ".=" + profile},
		"bad summary path": {"-root", root, "-diff", diff, "-module", ".=" + profile, "-summary", root},
	} {
		var out, errOut strings.Builder
		if code := run(context.Background(), args, noEnv, &out, &errOut); code != 2 {
			t.Errorf("%s: exit %d, want 2", name, code)
		}
	}
}
