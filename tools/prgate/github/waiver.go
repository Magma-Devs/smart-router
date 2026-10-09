package github

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// WaiverCheck is what a waiver label says about a pull request.
type WaiverCheck struct {
	Present bool   // the pull request carries the label
	Valid   bool   // the label waives the check
	By      string // who added it last
	Why     string // why it does not count, when present but not valid
}

// Waiver reads a waiver label. It counts only when a person other than the
// author added it after the head commit was made: a reviewer waives the code
// they read, not commits pushed afterwards. events are the pull request's
// issue events, oldest first. The commit date comes from the commit, so this
// guards against mistakes, not against an author who forges dates.
func Waiver(pr PullRequest, events []IssueEvent, label string, headCommitted time.Time) WaiverCheck {
	if !pr.HasLabel(label) {
		return WaiverCheck{}
	}
	var last *IssueEvent
	for i := range events {
		if e := &events[i]; e.Event == "labeled" && e.Label != nil && strings.EqualFold(e.Label.Name, label) {
			last = e
		}
	}
	w := WaiverCheck{Present: true}
	switch {
	case last == nil:
		w.Why = "no label event says who added it"
	case last.Actor.Type == "Bot":
		w.By, w.Why = last.Actor.Login, "a bot added it"
	case strings.EqualFold(last.Actor.Login, pr.User.Login):
		w.By, w.Why = last.Actor.Login, "the author added it, and only a reviewer can"
	case !last.CreatedAt.After(headCommitted):
		w.By, w.Why = last.Actor.Login, "it predates the latest commit, so a reviewer adds it again after reading the new code"
	default:
		w.By, w.Valid = last.Actor.Login, true
	}
	return w
}

// CheckWaiver applies Waiver to a pull request. It calls the API only when the
// pull request carries the label.
func (c *Client) CheckWaiver(ctx context.Context, repo string, pr PullRequest, label string) (WaiverCheck, error) {
	if !pr.HasLabel(label) {
		return WaiverCheck{}, nil
	}
	events, err := c.IssueEvents(ctx, repo, pr.Number)
	if err != nil {
		return WaiverCheck{}, fmt.Errorf("read the label events of #%d: %w", pr.Number, err)
	}
	committed, err := c.CommitDate(ctx, repo, pr.Head.SHA)
	if err != nil {
		return WaiverCheck{}, fmt.Errorf("read the head commit of #%d: %w", pr.Number, err)
	}
	return Waiver(pr, events, label, committed), nil
}
