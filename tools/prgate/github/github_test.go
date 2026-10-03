package github

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(url string) *Client {
	c := NewClient(url, "secret")
	c.Wait = func(int) time.Duration { return 0 }
	return c
}

func TestClientRetriesServerErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("X-GitHub-Api-Version") == "" {
			t.Errorf("missing auth or API version header: %v", r.Header)
		}
		switch calls.Add(1) {
		case 1:
			w.WriteHeader(http.StatusBadGateway)
		case 2:
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			_, _ = io.WriteString(w, `{"number":7,"state":"open","body":null,"head":{"repo":{"full_name":"o/r"}},"base":{"repo":{"full_name":"o/r"}}}`)
		}
	}))
	defer srv.Close()

	pr, err := testClient(srv.URL).PullRequest(context.Background(), "o/r", 7)
	if err != nil || pr.Number != 7 || pr.State != "open" || pr.Body != "" || pr.FromFork() {
		t.Fatalf("PullRequest = %+v, %v", pr, err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", calls.Load())
	}
}

func TestFromFork(t *testing.T) {
	repo := func(name string) *Repo { return &Repo{FullName: name} }
	for _, tc := range []struct {
		head, base *Repo
		fork       bool
	}{
		{repo("o/r"), repo("O/R"), false},
		{repo("x/r"), repo("o/r"), true},
		{nil, repo("o/r"), true}, // the fork was deleted
	} {
		pr := PullRequest{Head: Ref{Repo: tc.head}, Base: Ref{Repo: tc.base}}
		if pr.FromFork() != tc.fork {
			t.Errorf("head %v base %v: FromFork = %v", tc.head, tc.base, pr.FromFork())
		}
	}
}

func TestClientGivesUpAfterThreeAttempts(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, strings.Repeat("x", 300))
	}))
	defer srv.Close()

	_, err := testClient(srv.URL).PullRequest(context.Background(), "o/r", 1)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusServiceUnavailable || len(apiErr.Message) != 200 {
		t.Fatalf("err = %v", err)
	}
	if calls.Load() != attempts {
		t.Fatalf("calls = %d, want %d", calls.Load(), attempts)
	}
}

func TestClientDoesNotRetryClientErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"message":"Resource not accessible by integration"}`)
	}))
	defer srv.Close()

	_, err := testClient(srv.URL).OpenPullRequests(context.Background(), "o/r", "main")
	if err == nil || !strings.Contains(err.Error(), "HTTP 403: Resource not accessible by integration") {
		t.Fatalf("err = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestClientNetworkErrorAndCancel(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // every request now fails to connect

	if _, err := testClient(url).PullRequest(context.Background(), "o/r", 1); err == nil {
		t.Fatal("a closed server must fail")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := testClient(url)
	c.Wait = func(int) time.Duration { return time.Hour }
	if _, err := c.PullRequest(ctx, "o/r", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled context must stop the retries, got %v", err)
	}
}

func TestGetAllFollowsLinks(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("per_page") != "100" {
			t.Errorf("per_page = %q", r.URL.Query().Get("per_page"))
		}
		switch r.URL.Query().Get("page") {
		case "":
			w.Header().Set("Link", fmt.Sprintf(`<%s/repos/o/r/pulls/1/files?per_page=100&page=2>; rel="next", <%s/x>; rel="last"`, srv.URL, srv.URL))
			_, _ = io.WriteString(w, `[{"filename":"a.go","status":"added","additions":1,"patch":"@@ -0,0 +1 @@\n+x"}]`)
		case "2":
			_, _ = io.WriteString(w, `[{"filename":"b.go","status":"removed"}]`)
		}
	}))
	defer srv.Close()

	files, err := testClient(srv.URL).PullRequestFiles(context.Background(), "o/r", 1)
	if err != nil || len(files) != 2 || files[0].Patch == "" || files[1].Status != "removed" {
		t.Fatalf("files = %+v, %v", files, err)
	}
}

func TestGetAllPageLimitAndForeignHost(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next := srv.URL + r.URL.Path + "?page=again"
		if strings.HasSuffix(r.URL.Path, "/events") {
			next = "https://elsewhere.example/steal"
		}
		w.Header().Set("Link", "<"+next+`>; rel="next"`)
		_, _ = io.WriteString(w, `[]`)
	}))
	defer srv.Close()
	c := testClient(srv.URL)

	if _, err := c.OpenPullRequests(context.Background(), "o/r", "main"); err == nil || !strings.Contains(err.Error(), "more than 10 pages") {
		t.Fatalf("an endless list must fail, got %v", err)
	}
	if _, err := c.IssueEvents(context.Background(), "o/r", 1); err == nil || !strings.Contains(err.Error(), "refusing to send the token") {
		t.Fatalf("a link to another host must not be followed, got %v", err)
	}
}

func TestGetAllBadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"not":"a list"}`)
	}))
	defer srv.Close()
	if _, err := testClient(srv.URL).PullRequestFiles(context.Background(), "o/r", 1); err == nil {
		t.Fatal("an object where a list belongs must fail")
	}
}

func TestCommitDateAndStatuses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/o/r/commits/abc":
			_, _ = io.WriteString(w, `{"commit":{"committer":{"date":"2026-10-03T10:00:00Z"}}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/repos/o/r/commits/abc/status":
			_, _ = io.WriteString(w, `{"state":"pending","statuses":[{"state":"failure","context":"e2e-test/linked","description":"d"}]}`)
		case r.Method == http.MethodPost:
			body, _ := io.ReadAll(r.Body)
			if r.URL.Path != "/repos/o/r/statuses/abc" || r.Header.Get("Content-Type") != "application/json" ||
				string(body) != `{"state":"success","context":"c","description":"ok","target_url":"u"}` {
				t.Errorf("status post: %s %s", r.URL.Path, body)
			}
			w.WriteHeader(http.StatusCreated)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := testClient(srv.URL)
	ctx := context.Background()

	if when, err := c.CommitDate(ctx, "o/r", "abc"); err != nil || !when.Equal(time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("CommitDate = %v, %v", when, err)
	}
	statuses, err := c.CommitStatuses(ctx, "o/r", "abc")
	if err != nil || len(statuses) != 1 || statuses[0].State != "failure" {
		t.Fatalf("CommitStatuses = %+v, %v", statuses, err)
	}
	if err := c.CreateStatus(ctx, "o/r", "abc", Status{State: "success", Context: "c", Description: "ok", TargetURL: "u"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CommitDate(ctx, "o/r", "missing"); err == nil {
		t.Fatal("an unknown commit must fail")
	}
}

func TestClientErrorsBeforeSending(t *testing.T) {
	c := testClient("http://127.0.0.1:1")
	if _, _, err := c.do(context.Background(), http.MethodPost, "/x", func() {}); err == nil {
		t.Error("a body that cannot be encoded must fail")
	}
	if _, _, err := c.do(context.Background(), "BAD METHOD", "/x", nil); err == nil {
		t.Error("an invalid method must fail")
	}
	if _, err := c.CommitStatuses(context.Background(), "o/r", "x"); err == nil {
		t.Error("an unreachable API must fail")
	}
}
