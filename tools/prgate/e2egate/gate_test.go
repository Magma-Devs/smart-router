package e2egate

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/tools/prgate/github"
	"github.com/magma-Devs/smart-router/tools/prgate/internal/fakegh"
)

const routerRepo = "o/router"

// committed is when every test pull request's head commit was made.
var committed = time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)

type harness struct {
	t      *testing.T
	api    *fakegh.Server
	g      *Gate
	out    *strings.Builder
	clones int // how many times the gate asked for the automation checkout
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	api := fakegh.New()
	t.Cleanup(api.Close)
	h := &harness{t: t, api: api, out: &strings.Builder{}}
	dir := automationTree(t)
	h.g = &Gate{
		Router:      testClient(api.URL),
		Repo:        routerRepo,
		Context:     "e2e-test/linked",
		WaiverLabel: "no-e2e-test",
		TargetURL:   "https://run",
		Summary:     h.out,
		Tests: func(context.Context) (string, error) {
			h.clones++
			return dir, nil
		},
	}
	return h
}

func testClient(url string) *github.Client {
	c := github.NewClient(url, "secret")
	c.Wait = func(int) time.Duration { return 0 }
	return c
}

// pr registers an open router pull request, from the router repository
// itself, that adds code to protocol/a.go.
func (h *harness) pr(number int, body string, labels ...string) github.PullRequest {
	pr := github.PullRequest{
		Number: number, State: "open", Title: fmt.Sprintf("fix: change %d", number), Body: body,
		User:      github.User{Login: "alice", Type: "User"},
		Head:      github.Ref{Ref: fmt.Sprintf("branch-%d", number), SHA: fmt.Sprintf("sha%d", number), Repo: &github.Repo{FullName: routerRepo}},
		Base:      github.Ref{Ref: "main", Repo: &github.Repo{FullName: routerRepo}},
		UpdatedAt: committed,
	}
	for _, l := range labels {
		pr.Labels = append(pr.Labels, github.Label{Name: l})
	}
	h.api.JSON(fmt.Sprintf("GET /repos/%s/pulls/%d", routerRepo, number), pr)
	h.api.JSON(fmt.Sprintf("GET /repos/%s/commits/%s", routerRepo, pr.Head.SHA), map[string]any{"commit": map[string]any{"committer": map[string]any{"date": committed}}})
	h.files(number, github.PRFile{Filename: "protocol/a.go", Status: "modified", Additions: 2})
	return pr
}

func (h *harness) files(number int, files ...github.PRFile) {
	h.api.JSON(fmt.Sprintf("GET /repos/%s/pulls/%d/files", routerRepo, number), files)
}

// labelled registers who added the waiver label to a pull request, and when.
func (h *harness) labelled(number int, login string, at time.Time) {
	h.api.JSON(fmt.Sprintf("GET /repos/%s/issues/%d/events", routerRepo, number), []github.IssueEvent{
		{Event: "labeled", Actor: github.User{Login: login, Type: "User"}, Label: &github.Label{Name: "no-e2e-test"}, CreatedAt: at},
	})
}

func (h *harness) check(pr github.PullRequest) Verdict {
	h.t.Helper()
	v, err := h.g.Check(context.Background(), pr)
	if err != nil {
		h.t.Fatalf("Check: %v", err)
	}
	return v
}

func TestCheckNoRouterCode(t *testing.T) {
	h := newHarness(t)
	pr := h.pr(1, "")
	h.files(1, github.PRFile{Filename: "docs/X.md", Status: "modified", Additions: 4}, github.PRFile{Filename: "protocol/a_test.go", Status: "modified", Additions: 4})
	if v := h.check(pr); v.State != "success" || v.Description != "No router Go code changed" {
		t.Fatalf("verdict = %+v", v)
	}
	if h.clones != 0 {
		t.Error("a pull request without router code must not need the automation checkout")
	}
}

func TestCheckTestOnMain(t *testing.T) {
	h := newHarness(t)
	v := h.check(h.pr(1, "Jira ticket: MAG-100\nJira ticket: MAG-404"))
	if v.State != "success" || v.Description != "MAG-100: 1 test file on automation main" {
		t.Fatalf("verdict = %+v", v)
	}
	h.g.report(github.PullRequest{Number: 1, Title: "t"}, v)
	// smart-router is public and smart-router-automation is not: no path or
	// test name of the automation repository may reach the summary.
	if strings.Contains(h.out.String(), "tests/") || strings.Contains(h.out.String(), "test_failover") {
		t.Fatalf("the summary names a private automation test:\n%s", h.out.String())
	}
}

func TestCheckNoTicket(t *testing.T) {
	h := newHarness(t)
	v := h.check(h.pr(1, "Jira ticket:\nE2E test: MAG-100"))
	if v.State != "failure" || !strings.Contains(v.Description, "Jira ticket: MAG-123") || len(v.Fix) != 2 {
		t.Fatalf("verdict = %+v", v)
	}
	if h.clones != 0 {
		t.Error("a pull request without a ticket must not need the automation checkout")
	}
}

func TestCheckNoTest(t *testing.T) {
	h := newHarness(t)
	v := h.check(h.pr(1, "Jira ticket: MAG-404"))
	if v.State != "failure" || v.Description != "No smart-router-automation test on main names MAG-404" || len(v.Fix) != 2 {
		t.Fatalf("verdict = %+v", v)
	}
	h.g.report(github.PullRequest{Number: 1, Title: "t"}, v)
	if !strings.Contains(h.out.String(), "To pass, do one of these:\n\n1. Merge a test") {
		t.Errorf("summary lacks the fix list:\n%s", h.out.String())
	}
}

func TestCheckWaiver(t *testing.T) {
	h := newHarness(t)
	h.labelled(1, "bob", committed.Add(time.Minute))
	if v := h.check(h.pr(1, "", "no-e2e-test")); v.State != "success" || v.Description != "Waived by @bob with the no-e2e-test label" {
		t.Fatalf("verdict = %+v", v)
	}

	h.labelled(2, "alice", committed.Add(time.Minute))
	v := h.check(h.pr(2, "Jira ticket: MAG-404", "no-e2e-test"))
	if v.State != "failure" || !strings.HasSuffix(v.Description, "; the no-e2e-test label does not count") {
		t.Fatalf("an author's own waiver must not count: %+v", v)
	}
	if !strings.Contains(strings.Join(v.Notes, "\n"), "the author added it") {
		t.Errorf("notes must say why the label does not count: %v", v.Notes)
	}

	h.labelled(3, "bob", committed.Add(-time.Minute))
	v = h.check(h.pr(3, "Jira ticket: MAG-100", "no-e2e-test"))
	if v.State != "success" || !strings.Contains(strings.Join(v.Notes, "\n"), "predates the latest commit") {
		t.Fatalf("a stale waiver does not count, but a test still passes the gate: %+v", v)
	}
	if h.clones != 1 {
		t.Errorf("the automation checkout was asked for %d times, want 1", h.clones)
	}
}

func TestCheckFork(t *testing.T) {
	h := newHarness(t)
	pr := h.pr(1, "Jira ticket: MAG-100")
	pr.Head.Repo = &github.Repo{FullName: "stranger/router"}
	v := h.check(pr)
	if v.State != "failure" || !strings.HasPrefix(v.Description, "From a fork") {
		t.Fatalf("verdict = %+v", v)
	}
	if h.clones != 0 {
		t.Error("the automation repository must never be read for a fork's pull request")
	}

	h.labelled(1, "bob", committed.Add(time.Minute))
	pr.Labels = []github.Label{{Name: "no-e2e-test"}}
	if v := h.check(pr); v.State != "success" {
		t.Fatalf("a reviewer's waiver passes a fork's pull request: %+v", v)
	}
}

func TestCheckErrors(t *testing.T) {
	ctx := context.Background()

	h := newHarness(t)
	h.api.Fail("GET /repos/"+routerRepo+"/pulls/1/files", http.StatusNotFound)
	if _, err := h.g.Check(ctx, github.PullRequest{Number: 1}); err == nil {
		t.Error("unreadable pull request files must be an error")
	}

	h = newHarness(t)
	pr := h.pr(1, "", "no-e2e-test")
	h.api.Fail("GET /repos/"+routerRepo+"/issues/1/events", http.StatusInternalServerError)
	if _, err := h.g.Check(ctx, pr); err == nil {
		t.Error("unreadable label events must be an error")
	}

	h = newHarness(t)
	h.g.Tests = func(context.Context) (string, error) { return "", errors.New("token expired") }
	if _, err := h.g.Check(ctx, h.pr(1, "Jira ticket: MAG-1")); err == nil || !strings.Contains(err.Error(), "token expired") {
		t.Errorf("a failed checkout: err = %v", err)
	}

	h = newHarness(t)
	missing := t.TempDir() + "/missing"
	h.g.Tests = func(context.Context) (string, error) { return missing, nil }
	if _, err := h.g.Check(ctx, h.pr(1, "Jira ticket: MAG-1")); err == nil {
		t.Error("a checkout without tests/ must be an error")
	}

	h = newHarness(t)
	empty := t.TempDir()
	mustWrite(t, empty+"/tests/test_a.py", "def test_a(): pass")
	h.g.Tests = func(context.Context) (string, error) { return empty, nil }
	if _, err := h.g.Check(ctx, h.pr(1, "Jira ticket: MAG-1")); err == nil || !strings.Contains(err.Error(), "checkout is incomplete") {
		t.Errorf("a checkout naming no ticket at all: err = %v", err)
	}
}

func TestRunSetsStatusOnce(t *testing.T) {
	h := newHarness(t)
	h.pr(1, "Jira ticket: MAG-100")
	ctx := context.Background()

	v, err := h.g.Run(ctx, 1)
	if err != nil || v.State != "success" {
		t.Fatalf("Run = %+v, %v", v, err)
	}
	if _, err := h.g.Run(ctx, 1); err != nil {
		t.Fatal(err)
	}
	posts := h.api.Posts()
	want := fakegh.Status{State: "success", Context: "e2e-test/linked", Description: "MAG-100: 1 test file on automation main", TargetURL: "https://run"}
	if len(posts) != 1 || posts[0] != want {
		t.Fatalf("an unchanged verdict must not post again: %+v", posts)
	}
	if !strings.Contains(h.out.String(), "### #1 fix: change 1\n\n**success**") {
		t.Errorf("summary:\n%s", h.out.String())
	}
}

func TestRunClosedAndDryRun(t *testing.T) {
	h := newHarness(t)
	pr := h.pr(1, "Jira ticket: MAG-404")
	pr.State = "closed"
	h.api.JSON("GET /repos/"+routerRepo+"/pulls/1", pr)
	ctx := context.Background()

	if v, err := h.g.Run(ctx, 1); err != nil || v.State != "" {
		t.Fatalf("a closed pull request: %+v, %v", v, err)
	}
	h.g.DryRun = true
	if v, err := h.g.Run(ctx, 1); err != nil || v.State != "failure" {
		t.Fatalf("a dry run checks a closed pull request too: %+v, %v", v, err)
	}
	if posts := h.api.Posts(); len(posts) != 0 {
		t.Fatalf("no status may be set: %+v", posts)
	}
}

func TestRunErrors(t *testing.T) {
	ctx := context.Background()

	h := newHarness(t)
	if _, err := h.g.Run(ctx, 404); err == nil {
		t.Error("an unreadable pull request must be an error")
	}

	h = newHarness(t)
	h.pr(1, "Jira ticket: MAG-404")
	h.api.Fail("GET /repos/"+routerRepo+"/pulls/1/files", http.StatusInternalServerError)
	v, err := h.g.Run(ctx, 1)
	if err == nil || v.State != "error" {
		t.Fatalf("Run = %+v, %v", v, err)
	}
	if posts := h.api.Posts(); len(posts) != 1 || posts[0].State != "error" || !strings.HasPrefix(posts[0].Description, "The e2e test gate could not decide") {
		t.Fatalf("an undecided pull request must get an error status: %+v", posts)
	}

	h = newHarness(t)
	h.pr(1, "Jira ticket: MAG-100")
	h.api.Fail("POST /repos/"+routerRepo+"/statuses/sha1", http.StatusUnprocessableEntity)
	if _, err := h.g.Run(ctx, 1); err == nil || !strings.Contains(err.Error(), "set the status") {
		t.Errorf("a refused status: err = %v", err)
	}
}

func TestSweep(t *testing.T) {
	h := newHarness(t)
	ok := h.pr(1, "Jira ticket: MAG-100")
	broken := h.pr(2, "Jira ticket: MAG-404")
	missing := h.pr(3, "Jira ticket: MAG-404")
	green := h.pr(4, "Jira ticket: MAG-404")
	h.api.JSON("GET /repos/"+routerRepo+"/pulls", []github.PullRequest{ok, broken, missing, green})
	h.api.Fail("GET /repos/"+routerRepo+"/pulls/2/files", http.StatusInternalServerError)
	if err := h.g.Router.CreateStatus(context.Background(), routerRepo, "sha4", github.Status{State: "success", Context: "e2e-test/linked", Description: "earlier"}); err != nil {
		t.Fatal(err)
	}

	err := h.g.Sweep(context.Background())
	if err == nil || !strings.Contains(err.Error(), "#2:") {
		t.Fatalf("Sweep must report the pull request it could not decide, got %v", err)
	}
	posts := h.api.Posts()[1:] // after the one this test set up
	if len(posts) != 2 || posts[0].State != "success" || posts[1].State != "failure" {
		t.Fatalf("#1 and #3 get their status; #2 keeps its own; #4 is green already: %+v", posts)
	}

	h.api.Fail("GET /repos/"+routerRepo+"/pulls", http.StatusBadGateway)
	if err := h.g.Sweep(context.Background()); err == nil {
		t.Error("an unreadable list of pull requests must be an error")
	}
}

func TestSweepLeavesAChangedPullRequest(t *testing.T) {
	h := newHarness(t)
	listed := h.pr(1, "Jira ticket: MAG-404")
	h.api.JSON("GET /repos/"+routerRepo+"/pulls", []github.PullRequest{listed})
	changed := listed
	changed.UpdatedAt = committed.Add(time.Minute) // a label landed during the check
	h.api.JSON("GET /repos/"+routerRepo+"/pulls/1", changed)

	if err := h.g.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if posts := h.api.Posts(); len(posts) != 0 {
		t.Fatalf("a pull request that changed during the check must keep its status: %+v", posts)
	}
	if !strings.Contains(h.out.String(), "changed while it was checked") {
		t.Errorf("summary:\n%s", h.out.String())
	}
}

func TestSweepErrors(t *testing.T) {
	h := newHarness(t)
	h.api.JSON("GET /repos/"+routerRepo+"/pulls", []github.PullRequest{h.pr(1, "Jira ticket: MAG-100")})
	h.api.Fail("GET /repos/"+routerRepo+"/commits/sha1/status", http.StatusInternalServerError)
	if err := h.g.Sweep(context.Background()); err == nil || !strings.Contains(err.Error(), "read its statuses") {
		t.Errorf("unreadable statuses: err = %v", err)
	}

	h = newHarness(t)
	h.api.JSON("GET /repos/"+routerRepo+"/pulls", []github.PullRequest{h.pr(1, "Jira ticket: MAG-100")})
	h.api.Fail("GET /repos/"+routerRepo+"/pulls/1", http.StatusInternalServerError)
	if err := h.g.Sweep(context.Background()); err == nil || !strings.Contains(err.Error(), "read it again") {
		t.Errorf("a pull request that cannot be read again: err = %v", err)
	}

	h = newHarness(t)
	h.api.JSON("GET /repos/"+routerRepo+"/pulls", []github.PullRequest{h.pr(1, "Jira ticket: MAG-100")})
	h.api.Fail("POST /repos/"+routerRepo+"/statuses/sha1", http.StatusUnprocessableEntity)
	if err := h.g.Sweep(context.Background()); err == nil || !strings.Contains(err.Error(), "set the status of #1") {
		t.Errorf("a refused status: err = %v", err)
	}
}

func TestHelpers(t *testing.T) {
	long := strings.Repeat("é", 150)
	if got := clip(long); len([]rune(got)) != maxDescription || !strings.HasSuffix(got, "…") {
		t.Errorf("clip kept %d runes", len([]rune(got)))
	}
	if clip("short") != "short" {
		t.Error("clip changed a short description")
	}
	if orList([]string{"A"}) != "A" || orList([]string{"A", "B", "C"}) != "A, B or C" {
		t.Error("orList")
	}
	if plural(1, "test file") != "1 test file" || plural(2, "test file") != "2 test files" {
		t.Error("plural")
	}
	if got := codeList([]string{"a", "b", "c"}, 2); got != "`a`, `b` and 1 more" {
		t.Errorf("codeList = %q", got)
	}
	if got := namedKeys([]Match{{Keys: []string{"B", "A"}}, {Keys: []string{"A"}}}); strings.Join(got, ",") != "A,B" {
		t.Errorf("namedKeys = %v", got)
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(path[:strings.LastIndex(path, "/")], 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
