package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/tools/prgate/internal/fakegh"
)

// world serves router pull request 1, which adds code under protocol/ and
// whose description is body, and an automation checkout whose one test names
// MAG-100.
func world(t *testing.T, body string) (api *fakegh.Server, dir string) {
	t.Helper()
	api = fakegh.New()
	t.Cleanup(api.Close)
	repo := map[string]any{"full_name": "o/r"}
	pr := map[string]any{
		"number": 1, "state": "open", "title": "fix: x", "body": body,
		"user": map[string]any{"login": "alice", "type": "User"},
		"head": map[string]any{"ref": "b", "sha": "abc", "repo": repo},
		"base": map[string]any{"ref": "main", "repo": repo},
	}
	api.JSON("GET /repos/o/r/pulls/1", pr)
	api.JSON("GET /repos/o/r/pulls", []any{pr})
	api.JSON("GET /repos/o/r/pulls/1/files", []any{map[string]any{"filename": "protocol/a.go", "status": "modified", "additions": 1}})

	dir = t.TempDir()
	writeFile(t, filepath.Join(dir, "tests", "sim", "test_a.py"), `@pytest.mark.expose(bug="MAG-100")`)
	return api, dir
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

var tokens = env(map[string]string{"GH_TOKEN": "t"})

func args(api *fakegh.Server, dir string, extra ...string) []string {
	return append([]string{"-repo", "o/r", "-automation-dir", dir, "-api", api.URL, "-target-url", "https://run"}, extra...)
}

func TestRunOnePullRequest(t *testing.T) {
	ctx := context.Background()

	api, dir := world(t, "Jira ticket: MAG-100")
	var out, errOut strings.Builder
	if code := run(ctx, args(api, dir, "-pr", "1"), tokens, &out, &errOut); code != 0 {
		t.Fatalf("exit %d\nstdout %s\nstderr %s", code, out.String(), errOut.String())
	}
	if posts := api.Posts(); len(posts) != 1 || posts[0].State != "success" || posts[0].TargetURL != "https://run" {
		t.Fatalf("posts = %+v", posts)
	}
	if !strings.Contains(out.String(), "### #1 fix: x") {
		t.Errorf("the report goes to stdout without -summary:\n%s", out.String())
	}

	api, dir = world(t, "Jira ticket: MAG-7")
	out.Reset()
	summary := filepath.Join(t.TempDir(), "summary.md")
	if code := run(ctx, args(api, dir, "-pr", "1", "-summary", summary), tokens, &out, &errOut); code != 1 {
		t.Fatalf("a failing verdict: exit %d, want 1", code)
	}
	if !strings.Contains(out.String(), "::error title=E2E test gate::No smart-router-automation test on main names MAG-7") {
		t.Errorf("stdout:\n%s", out.String())
	}
	if written, err := os.ReadFile(summary); err != nil || !strings.Contains(string(written), "To pass, do one of these") {
		t.Errorf("summary file: %q, %v", written, err)
	}
}

func TestRunClones(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	ctx := context.Background()
	api, _ := world(t, "Jira ticket: MAG-100")
	// A repository at <root>/o/auto.git, as -git-url and -automation-repo name it.
	root := t.TempDir()
	src := filepath.Join(root, "o", "auto.git")
	writeFile(t, filepath.Join(src, "tests", "sim", "test_a.py"), `@pytest.mark.expose(bug="MAG-100")`)
	for _, a := range [][]string{
		{"init", "--quiet", "--initial-branch=main"},
		{"add", "."},
		{"-c", "user.name=t", "-c", "user.email=t@t", "commit", "--quiet", "-m", "init"},
	} {
		if out, err := exec.Command("git", append([]string{"-C", src}, a...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", a, err, out)
		}
	}
	base := []string{"-repo", "o/r", "-automation-repo", "o/auto", "-git-url", "file://" + root, "-api", api.URL, "-pr", "1"}

	var out, errOut strings.Builder
	if code := run(ctx, base, tokens, &out, &errOut); code != 1 || !strings.Contains(out.String(), "AUTOMATION_TOKEN is not set") {
		t.Fatalf("no automation token: exit %d, stdout %s", code, out.String())
	}

	out.Reset()
	withToken := env(map[string]string{"GH_TOKEN": "t", "AUTOMATION_TOKEN": "a"})
	if code := run(ctx, base, withToken, &out, &errOut); code != 0 {
		t.Fatalf("exit %d\nstdout %s\nstderr %s", code, out.String(), errOut.String())
	}
}

func TestRunClosedAndBroken(t *testing.T) {
	ctx := context.Background()
	api, dir := world(t, "Jira ticket: MAG-100")
	api.JSON("GET /repos/o/r/pulls/1", map[string]any{"number": 1, "state": "closed"})
	var out, errOut strings.Builder
	if code := run(ctx, args(api, dir, "-pr", "1"), tokens, &out, &errOut); code != 0 || !strings.Contains(out.String(), "is not open") {
		t.Fatalf("a closed pull request: exit %d, stdout %s", code, out.String())
	}

	out.Reset()
	if code := run(ctx, args(api, dir, "-pr", "2"), tokens, &out, &errOut); code != 1 || !strings.Contains(out.String(), "::error title=E2E test gate::read #2") {
		t.Fatalf("an unreadable pull request: exit %d, stdout %s", code, out.String())
	}
}

func TestRunSweep(t *testing.T) {
	ctx := context.Background()
	api, dir := world(t, "Jira ticket: MAG-100")
	var out, errOut strings.Builder
	if code := run(ctx, args(api, dir, "-sweep", "-dry-run"), tokens, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	if len(api.Posts()) != 0 {
		t.Fatal("a dry run sets no status")
	}

	api.Fail("GET /repos/o/r/pulls", 403) // 403: no retry, so no back-off wait
	if code := run(ctx, args(api, dir, "-sweep"), tokens, &out, &errOut); code != 1 {
		t.Fatalf("an unreadable list: exit %d, want 1", code)
	}
}

func TestRunUsage(t *testing.T) {
	ctx := context.Background()
	api, dir := world(t, "")
	for name, tc := range map[string]struct {
		args   []string
		getenv func(string) string
	}{
		"unknown flag":    {[]string{"-nope"}, tokens},
		"no target":       {args(api, dir), tokens},
		"pr and sweep":    {args(api, dir, "-pr", "1", "-sweep"), tokens},
		"no repo":         {[]string{"-automation-dir", dir, "-pr", "1"}, tokens},
		"no automation":   {[]string{"-repo", "o/r", "-pr", "1"}, tokens},
		"no token":        {args(api, dir, "-pr", "1"), env(nil)},
		"bad summary dir": {args(api, dir, "-pr", "1", "-summary", dir), tokens},
	} {
		var out, errOut strings.Builder
		if code := run(ctx, tc.args, tc.getenv, &out, &errOut); code != 2 {
			t.Errorf("%s: exit %d, want 2", name, code)
		}
	}
}

func TestEscapeData(t *testing.T) {
	if got := escapeData("a%b\r\nc"); got != "a%25b%0D%0Ac" {
		t.Errorf("escapeData = %q", got)
	}
}
