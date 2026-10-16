// Package github is the small GitHub REST client the pull request gates use,
// and the rule for a reviewer's waiver label.
package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// PullRequest is the part of a GitHub pull request the gates read.
type PullRequest struct {
	Number    int       `json:"number"`
	State     string    `json:"state"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	User      User      `json:"user"`
	Head      Ref       `json:"head"`
	Base      Ref       `json:"base"`
	Labels    []Label   `json:"labels"`
	UpdatedAt time.Time `json:"updated_at"`
}

// FromFork reports whether the pull request's head lives outside the base
// repository, or in a fork since deleted.
func (pr PullRequest) FromFork() bool {
	return pr.Head.Repo == nil || pr.Base.Repo == nil || !strings.EqualFold(pr.Head.Repo.FullName, pr.Base.Repo.FullName)
}

// HasLabel reports whether the pull request carries a label.
func (pr PullRequest) HasLabel(name string) bool {
	for _, l := range pr.Labels {
		if strings.EqualFold(l.Name, name) {
			return true
		}
	}
	return false
}

// User is a GitHub account. Type is "User" or "Bot".
type User struct {
	Login string `json:"login"`
	Type  string `json:"type"`
}

// Ref is the head or base of a pull request.
type Ref struct {
	Ref  string `json:"ref"`
	SHA  string `json:"sha"`
	Repo *Repo  `json:"repo"`
}

// Repo names a repository.
type Repo struct {
	FullName string `json:"full_name"`
}

// Label is a pull request label.
type Label struct {
	Name string `json:"name"`
}

// PRFile is a file a pull request changes. GitHub leaves Patch empty when the
// file's diff is too large to show.
type PRFile struct {
	Filename  string `json:"filename"`
	Status    string `json:"status"`
	Additions int    `json:"additions"`
	Patch     string `json:"patch"`
}

// IssueEvent is an entry of a pull request's event timeline.
type IssueEvent struct {
	Event     string    `json:"event"`
	Actor     User      `json:"actor"`
	Label     *Label    `json:"label"`
	CreatedAt time.Time `json:"created_at"`
}

// Status is a commit status.
type Status struct {
	State       string `json:"state"`
	Context     string `json:"context"`
	Description string `json:"description"`
	TargetURL   string `json:"target_url,omitempty"`
}

// Client is a small GitHub REST client: the few calls the gates make.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
	// Wait is how long a retry waits after attempt n (0-based) failed.
	Wait func(n int) time.Duration
}

// NewClient returns a client for the GitHub API at baseURL.
func NewClient(baseURL, token string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Token:   token,
		HTTP:    &http.Client{Timeout: 30 * time.Second},
		Wait:    func(n int) time.Duration { return time.Duration(n+1) * 2 * time.Second },
	}
}

// APIError is a 4xx answer: retrying it cannot help.
type APIError struct {
	Method, URL string
	Status      int
	Message     string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d: %s", e.Method, e.URL, e.Status, e.Message)
}

const (
	acceptJSON = "application/vnd.github+json"
	attempts   = 3
)

// do sends one request, retrying a network error, a 5xx and a 429. target is
// a path under BaseURL, or an absolute URL on BaseURL's host (a Link header).
func (c *Client) do(ctx context.Context, method, target string, body any) ([]byte, http.Header, error) {
	full := target
	if strings.HasPrefix(target, "/") {
		full = c.BaseURL + target
	} else if !c.sameHost(target) {
		return nil, nil, fmt.Errorf("refusing to send the token to %s", target)
	}
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return nil, nil, err
		}
	}

	var lastErr error
	for n := 0; n < attempts; n++ {
		if n > 0 {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(c.Wait(n - 1)):
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, full, bytes.NewReader(payload))
		if err != nil {
			return nil, nil, err
		}
		req.Header.Set("Accept", acceptJSON)
		req.Header.Set("Authorization", "Bearer "+c.Token)
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		resp.Body.Close()
		switch {
		case err != nil:
			lastErr = err
		case resp.StatusCode < 300:
			return data, resp.Header, nil
		case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests:
			lastErr = &APIError{Method: method, URL: full, Status: resp.StatusCode, Message: apiMessage(data)}
		default:
			return nil, nil, &APIError{Method: method, URL: full, Status: resp.StatusCode, Message: apiMessage(data)}
		}
	}
	return nil, nil, lastErr
}

func (c *Client) sameHost(target string) bool {
	base, err1 := url.Parse(c.BaseURL)
	u, err2 := url.Parse(target)
	return err1 == nil && err2 == nil && u.Scheme == base.Scheme && u.Host == base.Host
}

func apiMessage(data []byte) string {
	var e struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(data, &e) == nil && e.Message != "" {
		return e.Message
	}
	if len(data) > 200 {
		data = data[:200]
	}
	return strings.TrimSpace(string(data))
}

var nextLink = regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)

// getAll reads every page of a list endpoint, at most maxPages of 100 items.
// More is an error rather than a silent cut: the caller may need the last
// items (the newest label event).
func getAll[T any](ctx context.Context, c *Client, target string, maxPages int) ([]T, error) {
	sep := "?"
	if strings.Contains(target, "?") {
		sep = "&"
	}
	next := target + sep + "per_page=100"
	var all []T
	for page := 0; next != ""; page++ {
		if page == maxPages {
			return nil, fmt.Errorf("%s: more than %d pages", target, maxPages)
		}
		data, header, err := c.do(ctx, http.MethodGet, next, nil)
		if err != nil {
			return nil, err
		}
		var items []T
		if err := json.Unmarshal(data, &items); err != nil {
			return nil, fmt.Errorf("%s: %w", target, err)
		}
		all = append(all, items...)
		next = ""
		if m := nextLink.FindStringSubmatch(header.Get("Link")); m != nil {
			next = m[1]
		}
	}
	return all, nil
}

func getOne[T any](ctx context.Context, c *Client, target string) (T, error) {
	var v T
	data, _, err := c.do(ctx, http.MethodGet, target, nil)
	if err != nil {
		return v, err
	}
	err = json.Unmarshal(data, &v)
	return v, err
}

// PullRequest reads one pull request.
func (c *Client) PullRequest(ctx context.Context, repo string, number int) (PullRequest, error) {
	return getOne[PullRequest](ctx, c, fmt.Sprintf("/repos/%s/pulls/%d", repo, number))
}

// OpenPullRequests lists a repository's open pull requests against base.
func (c *Client) OpenPullRequests(ctx context.Context, repo, base string) ([]PullRequest, error) {
	return getAll[PullRequest](ctx, c, fmt.Sprintf("/repos/%s/pulls?state=open&base=%s", repo, url.QueryEscape(base)), 10)
}

// PullRequestFiles lists the files a pull request changes (GitHub stops at 3000).
func (c *Client) PullRequestFiles(ctx context.Context, repo string, number int) ([]PRFile, error) {
	return getAll[PRFile](ctx, c, fmt.Sprintf("/repos/%s/pulls/%d/files", repo, number), 30)
}

// IssueEvents lists a pull request's timeline events, oldest first.
func (c *Client) IssueEvents(ctx context.Context, repo string, number int) ([]IssueEvent, error) {
	return getAll[IssueEvent](ctx, c, fmt.Sprintf("/repos/%s/issues/%d/events", repo, number), 30)
}

// CommitDate returns when a commit was committed.
func (c *Client) CommitDate(ctx context.Context, repo, sha string) (time.Time, error) {
	commit, err := getOne[struct {
		Commit struct {
			Committer struct {
				Date time.Time `json:"date"`
			} `json:"committer"`
		} `json:"commit"`
	}](ctx, c, fmt.Sprintf("/repos/%s/commits/%s", repo, sha))
	return commit.Commit.Committer.Date, err
}

// CommitStatuses returns the latest status of each context on a commit.
func (c *Client) CommitStatuses(ctx context.Context, repo, sha string) ([]Status, error) {
	combined, err := getOne[struct {
		Statuses []Status `json:"statuses"`
	}](ctx, c, fmt.Sprintf("/repos/%s/commits/%s/status?per_page=100", repo, sha))
	return combined.Statuses, err
}

// CreateStatus sets a commit status.
func (c *Client) CreateStatus(ctx context.Context, repo, sha string, s Status) error {
	_, _, err := c.do(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/statuses/%s", repo, sha), s)
	return err
}
