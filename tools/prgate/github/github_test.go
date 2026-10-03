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
			_, _ = io.WriteString(w, `{"number":7,"user":{"login":"alice","type":"User"},"head":{"sha":"abc"},"labels":[{"name":"No-Unit-Coverage"}]}`)
		}
	}))
	defer srv.Close()

	pr, err := testClient(srv.URL).PullRequest(context.Background(), "o/r", 7)
	if err != nil || pr.Number != 7 || pr.User.Login != "alice" || pr.Head.SHA != "abc" || !pr.HasLabel("no-unit-coverage") || pr.HasLabel("other") {
		t.Fatalf("PullRequest = %+v, %v", pr, err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", calls.Load())
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

	_, err := testClient(srv.URL).IssueEvents(context.Background(), "o/r", 1)
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
			w.Header().Set("Link", fmt.Sprintf(`<%s/repos/o/r/issues/1/events?per_page=100&page=2>; rel="next", <%s/x>; rel="last"`, srv.URL, srv.URL))
			_, _ = io.WriteString(w, `[{"event":"labeled","actor":{"login":"bob"},"label":{"name":"a"}}]`)
		case "2":
			_, _ = io.WriteString(w, `[{"event":"unlabeled","actor":{"login":"bob"},"label":{"name":"a"}}]`)
		}
	}))
	defer srv.Close()

	events, err := testClient(srv.URL).IssueEvents(context.Background(), "o/r", 1)
	if err != nil || len(events) != 2 || events[0].Event != "labeled" || events[1].Event != "unlabeled" {
		t.Fatalf("events = %+v, %v", events, err)
	}
}

func TestGetAllPageLimitAndForeignHost(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next := srv.URL + r.URL.Path + "?page=again"
		if r.URL.Path == "/repos/o/r/issues/2/events" {
			next = "https://elsewhere.example/steal"
		}
		w.Header().Set("Link", "<"+next+`>; rel="next"`)
		_, _ = io.WriteString(w, `[]`)
	}))
	defer srv.Close()
	c := testClient(srv.URL)

	if _, err := c.IssueEvents(context.Background(), "o/r", 1); err == nil || !strings.Contains(err.Error(), "more than 30 pages") {
		t.Fatalf("an endless list must fail, got %v", err)
	}
	if _, err := c.IssueEvents(context.Background(), "o/r", 2); err == nil || !strings.Contains(err.Error(), "refusing to send the token") {
		t.Fatalf("a link to another host must not be followed, got %v", err)
	}
}

func TestGetAllBadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"not":"a list"}`)
	}))
	defer srv.Close()
	if _, err := testClient(srv.URL).IssueEvents(context.Background(), "o/r", 1); err == nil {
		t.Fatal("an object where a list belongs must fail")
	}
}

func TestCommitDate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/repos/o/r/commits/abc" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, `{"commit":{"committer":{"date":"2026-10-03T10:00:00Z"}}}`)
	}))
	defer srv.Close()
	c := testClient(srv.URL)
	ctx := context.Background()

	if when, err := c.CommitDate(ctx, "o/r", "abc"); err != nil || !when.Equal(time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("CommitDate = %v, %v", when, err)
	}
	if _, err := c.CommitDate(ctx, "o/r", "missing"); err == nil {
		t.Fatal("an unknown commit must fail")
	}
}

func TestClientErrorsBeforeSending(t *testing.T) {
	c := testClient("http://127.0.0.1:1")
	c.BaseURL = "http://bad host"
	if _, _, err := c.get(context.Background(), "/x"); err == nil {
		t.Error("a URL that does not parse must fail")
	}
	if _, err := testClient("http://127.0.0.1:1").CommitDate(context.Background(), "o/r", "x"); err == nil {
		t.Error("an unreachable API must fail")
	}
}
