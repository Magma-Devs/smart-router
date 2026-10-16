package e2egate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/magma-Devs/smart-router/tools/prgate/github"
)

// Gate checks the pull requests of Repo for an e2e test in smart-router-automation.
//
// Everything it publishes (statuses, the job summary) is public: smart-router
// is a public repository and smart-router-automation is not. So it publishes
// ticket keys and counts, never automation paths or test names.
type Gate struct {
	Router      *github.Client // reads Repo and sets its commit statuses
	Repo        string
	Context     string // the commit status the gate sets
	WaiverLabel string
	TargetURL   string // where the status links: the workflow run
	DryRun      bool   // check and report, but set no status
	Summary     io.Writer
	// Tests returns a checkout of smart-router-automation's main branch, of
	// which tests/ is searched. The gate calls it at most once, and only for
	// a pull request that needs the search.
	Tests func(ctx context.Context) (string, error)

	testsDir  string
	testsErr  error
	testsDone bool
}

// Verdict is the gate's answer for one pull request.
type Verdict struct {
	State       string   // success, failure or error
	Description string   // the status line
	Notes       []string // what the gate found, for the job summary
	Fix         []string // what the author can do, when the gate fails
}

// GitHub rejects a status description longer than this.
const maxDescription = 140

// Check decides one pull request. An error means the gate could not decide.
func (g *Gate) Check(ctx context.Context, pr github.PullRequest) (Verdict, error) {
	files, err := g.Router.PullRequestFiles(ctx, g.Repo, pr.Number)
	if err != nil {
		return Verdict{}, fmt.Errorf("list the files of #%d: %w", pr.Number, err)
	}
	changed := RouterChanges(files)
	if len(changed) == 0 {
		return Verdict{State: "success", Description: "No router Go code changed"}, nil
	}
	notes := []string{"Router Go files with added code: " + codeList(changed, 10)}

	waiver, err := g.Router.CheckWaiver(ctx, g.Repo, pr, g.WaiverLabel)
	if err != nil {
		return Verdict{}, err
	}
	if waiver.Valid {
		return Verdict{
			State:       "success",
			Description: clip(fmt.Sprintf("Waived by @%s with the %s label", waiver.By, g.WaiverLabel)),
			Notes:       notes,
		}, nil
	}
	labelFix := fmt.Sprintf("If no e2e test can see the change (a refactor, a log line), a reviewer other than the author adds the `%s` label.", g.WaiverLabel)
	if waiver.Present {
		notes = append(notes, fmt.Sprintf("The `%s` label does not count: %s.", g.WaiverLabel, waiver.Why))
	}

	if pr.FromFork() {
		return Verdict{
			State:       "failure",
			Description: clip("From a fork: a reviewer decides on the e2e test, with the " + g.WaiverLabel + " label"),
			Notes:       append(notes, "A pull request from a fork never gets the automation repository searched on its behalf."),
			Fix:         []string{fmt.Sprintf("A reviewer adds the `%s` label once smart-router-automation has a test for this change, or when it needs none.", g.WaiverLabel)},
		}, nil
	}

	keys := TicketKeys(pr)
	if len(keys) == 0 {
		return Verdict{
			State:       "failure",
			Description: "No MAG ticket named: add a line 'Jira ticket: MAG-123' to the description",
			Notes:       notes,
			Fix:         []string{"Add a line `Jira ticket: MAG-123`, with this change's ticket, to the pull request's description.", labelFix},
		}, nil
	}
	notes = append(notes, "Tickets: "+strings.Join(keys, ", "))

	dir, err := g.automationTests(ctx)
	if err != nil {
		return Verdict{}, fmt.Errorf("check out the smart-router-automation tests: %w", err)
	}
	matches, control, err := SearchTree(dir, keys)
	if err != nil {
		return Verdict{}, fmt.Errorf("search the smart-router-automation tests: %w", err)
	}
	if control == 0 {
		return Verdict{}, errors.New("no smart-router-automation test names any MAG ticket: the checkout is incomplete")
	}
	if len(matches) > 0 {
		named := namedKeys(matches)
		notes = append(notes, fmt.Sprintf("%s on smart-router-automation main name %s.", plural(len(matches), "test file"), strings.Join(named, ", ")))
		return Verdict{
			State:       "success",
			Description: clip(fmt.Sprintf("%s: %s on automation main", strings.Join(named, ", "), plural(len(matches), "test file"))),
			Notes:       notes,
		}, nil
	}

	desc := "No smart-router-automation test on main names " + orList(keys)
	if waiver.Present {
		desc += "; the " + g.WaiverLabel + " label does not count"
	}
	return Verdict{
		State:       "failure",
		Description: clip(desc),
		Notes:       notes,
		Fix: []string{
			fmt.Sprintf("Merge a test to smart-router-automation's main that names %s: in its xfail or expose marker, an allure tag, or its docstring. "+
				"A test for an unfixed bug merges with an xfail marker naming the ticket. A test that tracks a ticket of its own names this one too.", orList(keys)),
			labelFix,
		},
	}, nil
}

func (g *Gate) automationTests(ctx context.Context) (string, error) {
	if !g.testsDone {
		g.testsDir, g.testsErr = g.Tests(ctx)
		g.testsDone = true
	}
	return g.testsDir, g.testsErr
}

// namedKeys returns the keys the matches name, sorted and without repeats.
func namedKeys(matches []Match) []string {
	seen := map[string]bool{}
	for _, m := range matches {
		for _, k := range m.Keys {
			seen[k] = true
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Run checks one pull request and sets its status. A pull request that is not
// open gets no status, except in a dry run. When the gate cannot decide, the
// status is "error" and Run returns the reason.
func (g *Gate) Run(ctx context.Context, number int) (Verdict, error) {
	pr, err := g.Router.PullRequest(ctx, g.Repo, number)
	if err != nil {
		return Verdict{}, fmt.Errorf("read #%d: %w", number, err)
	}
	if pr.State != "open" && !g.DryRun {
		fmt.Fprintf(g.Summary, "#%d is %s: nothing to check.\n\n", number, pr.State)
		return Verdict{}, nil
	}
	v, checkErr := g.Check(ctx, pr)
	if checkErr != nil {
		v = Verdict{State: "error", Description: clip("The e2e test gate could not decide: " + checkErr.Error())}
	}
	g.report(pr, v)
	if err := g.publish(ctx, pr.Head.SHA, v, nil); err != nil {
		return v, errors.Join(checkErr, fmt.Errorf("set the status of #%d: %w", number, err))
	}
	return v, checkErr
}

// Sweep checks the open pull requests against main whose status is not
// already a success: a test merged to smart-router-automation after a pull
// request's last event turns its status green here. A pull request that
// changes while it is checked is left to the run its change starts, so the
// sweep never overwrites a newer status with an older answer. A pull request
// the gate cannot decide keeps its status; the errors return once every one
// was tried.
func (g *Gate) Sweep(ctx context.Context) error {
	prs, err := g.Router.OpenPullRequests(ctx, g.Repo, "main")
	if err != nil {
		return fmt.Errorf("list the open pull requests: %w", err)
	}
	var errs []error
	for _, pr := range prs {
		current, err := g.Router.CommitStatuses(ctx, g.Repo, pr.Head.SHA)
		if err != nil {
			errs = append(errs, fmt.Errorf("#%d: read its statuses: %w", pr.Number, err))
			continue
		}
		if st, ok := find(current, g.Context); ok && st.State == "success" {
			continue
		}
		v, err := g.Check(ctx, pr)
		if err != nil {
			errs = append(errs, fmt.Errorf("#%d: %w", pr.Number, err))
			continue
		}
		after, err := g.Router.PullRequest(ctx, g.Repo, pr.Number)
		if err != nil {
			errs = append(errs, fmt.Errorf("#%d: read it again: %w", pr.Number, err))
			continue
		}
		if after.Head.SHA != pr.Head.SHA || !after.UpdatedAt.Equal(pr.UpdatedAt) {
			fmt.Fprintf(g.Summary, "#%d changed while it was checked: the run its change started sets the status.\n\n", pr.Number)
			continue
		}
		g.report(pr, v)
		if err := g.publish(ctx, pr.Head.SHA, v, current); err != nil {
			errs = append(errs, fmt.Errorf("set the status of #%d: %w", pr.Number, err))
		}
	}
	return errors.Join(errs...)
}

func find(statuses []github.Status, context string) (github.Status, bool) {
	for _, s := range statuses {
		if s.Context == context {
			return s, true
		}
	}
	return github.Status{}, false
}

// publish sets the status unless the commit already has the same one. current
// holds the commit's statuses when the caller has read them already. GitHub
// keeps at most 1000 statuses per commit and context, and an hourly sweep
// would otherwise pile them up.
func (g *Gate) publish(ctx context.Context, sha string, v Verdict, current []github.Status) error {
	if g.DryRun {
		return nil
	}
	if current == nil {
		current, _ = g.Router.CommitStatuses(ctx, g.Repo, sha) // on an error, post anyway
	}
	if st, ok := find(current, g.Context); ok && st.State == v.State && st.Description == v.Description {
		return nil
	}
	return g.Router.CreateStatus(ctx, g.Repo, sha, github.Status{
		State:       v.State,
		Context:     g.Context,
		Description: v.Description,
		TargetURL:   g.TargetURL,
	})
}

// report writes one pull request's verdict to the job summary.
func (g *Gate) report(pr github.PullRequest, v Verdict) {
	fmt.Fprintf(g.Summary, "### #%d %s\n\n**%s**: %s\n\n", pr.Number, pr.Title, v.State, v.Description)
	for _, n := range v.Notes {
		fmt.Fprintf(g.Summary, "- %s\n", n)
	}
	if len(v.Fix) > 0 {
		fmt.Fprintf(g.Summary, "\nTo pass, do one of these:\n\n")
		for i, f := range v.Fix {
			fmt.Fprintf(g.Summary, "%d. %s\n", i+1, f)
		}
	}
	fmt.Fprintln(g.Summary)
}

// clip shortens a status description to the length GitHub accepts.
func clip(s string) string {
	if utf8.RuneCountInString(s) <= maxDescription {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxDescription-1]) + "…"
}

func orList(keys []string) string {
	if len(keys) == 1 {
		return keys[0]
	}
	return strings.Join(keys[:len(keys)-1], ", ") + " or " + keys[len(keys)-1]
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

func codeList(items []string, max int) string {
	shown := items
	if len(items) > max {
		shown = items[:max]
	}
	quoted := make([]string, len(shown))
	for i, s := range shown {
		quoted[i] = "`" + s + "`"
	}
	out := strings.Join(quoted, ", ")
	if len(items) > max {
		out += fmt.Sprintf(" and %d more", len(items)-max)
	}
	return out
}
