package github

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/tools/prgate/internal/fakegh"
)

func TestWaiver(t *testing.T) {
	at := func(min int) time.Time { return time.Date(2026, 10, 3, 10, min, 0, 0, time.UTC) }
	committed := at(10)
	labelled := PullRequest{User: User{Login: "alice", Type: "User"}, Labels: []Label{{Name: "No-E2E-Test"}}}
	ev := func(event, login, typ, label string, min int) IssueEvent {
		return IssueEvent{Event: event, Actor: User{Login: login, Type: typ}, Label: &Label{Name: label}, CreatedAt: at(min)}
	}

	for name, tc := range map[string]struct {
		pr     PullRequest
		events []IssueEvent
		want   WaiverCheck
	}{
		"no label": {pr: PullRequest{User: labelled.User}},
		"a reviewer after the commit": {
			pr: labelled, events: []IssueEvent{ev("labeled", "bob", "User", "no-e2e-test", 11)},
			want: WaiverCheck{Present: true, Valid: true, By: "bob"},
		},
		"a reviewer before the commit": {
			pr: labelled, events: []IssueEvent{ev("labeled", "bob", "User", "no-e2e-test", 9)},
			want: WaiverCheck{Present: true, By: "bob", Why: "it predates the latest commit, so a reviewer adds it again after reading the new code"},
		},
		"the author, last": {
			pr: labelled, events: []IssueEvent{
				ev("labeled", "bob", "User", "no-e2e-test", 11),
				ev("unlabeled", "bob", "User", "no-e2e-test", 12),
				ev("labeled", "Alice", "User", "no-e2e-test", 13),
			},
			want: WaiverCheck{Present: true, By: "Alice", Why: "the author added it, and only a reviewer can"},
		},
		"a bot": {
			pr: labelled, events: []IssueEvent{ev("labeled", "helper[bot]", "Bot", "no-e2e-test", 11)},
			want: WaiverCheck{Present: true, By: "helper[bot]", Why: "a bot added it"},
		},
		"no matching event": {
			pr:     labelled,
			events: []IssueEvent{ev("labeled", "bob", "User", "other", 11), {Event: "labeled", Actor: User{Login: "bob"}}},
			want:   WaiverCheck{Present: true, Why: "no label event says who added it"},
		},
	} {
		if got := Waiver(tc.pr, tc.events, "no-e2e-test", committed); got != tc.want {
			t.Errorf("%s:\n got %+v\nwant %+v", name, got, tc.want)
		}
	}
}

func TestCheckWaiver(t *testing.T) {
	api := fakegh.New()
	defer api.Close()
	c := testClient(api.URL)
	ctx := context.Background()
	pr := PullRequest{Number: 1, User: User{Login: "alice"}, Head: Ref{SHA: "abc"}, Labels: []Label{{Name: "no-e2e-test"}}}

	if w, err := c.CheckWaiver(ctx, "o/r", PullRequest{Number: 1}, "no-e2e-test"); err != nil || w.Present {
		t.Fatalf("no label must not call the API: %+v, %v", w, err)
	}

	api.Fail("GET /repos/o/r/issues/1/events", http.StatusForbidden)
	if _, err := c.CheckWaiver(ctx, "o/r", pr, "no-e2e-test"); err == nil {
		t.Fatal("unreadable events must be an error")
	}

	api.JSON("GET /repos/o/r/issues/1/events", []IssueEvent{{Event: "labeled", Actor: User{Login: "bob"}, Label: &Label{Name: "no-e2e-test"}, CreatedAt: time.Now()}})
	api.Unfail("GET /repos/o/r/issues/1/events")
	if _, err := c.CheckWaiver(ctx, "o/r", pr, "no-e2e-test"); err == nil {
		t.Fatal("an unreadable head commit must be an error")
	}

	api.JSON("GET /repos/o/r/commits/abc", map[string]any{"commit": map[string]any{"committer": map[string]any{"date": time.Now().Add(-time.Hour)}}})
	if w, err := c.CheckWaiver(ctx, "o/r", pr, "no-e2e-test"); err != nil || !w.Valid || w.By != "bob" {
		t.Fatalf("CheckWaiver = %+v, %v", w, err)
	}
}
