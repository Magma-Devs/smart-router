// Command e2egate sets the e2e-test/linked commit status on smart-router pull
// requests: success when smart-router-automation's main branch has a test that
// names the pull request's ticket, failure when it has none.
// docs/PR-GATES.md describes the rule.
//
//	e2egate -repo Magma-Devs/smart-router \
//	    -automation-repo Magma-Devs/smart-router-automation -pr 476
//
// GH_TOKEN reads -repo and sets its statuses. AUTOMATION_TOKEN clones the
// tests of -automation-repo, and only when a pull request needs them;
// -automation-dir names a checkout to use instead. -sweep checks every open
// pull request against main instead of one. -dry-run sets no status.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/magma-Devs/smart-router/tools/prgate/e2egate"
	"github.com/magma-Devs/smart-router/tools/prgate/github"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("e2egate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	repo := fs.String("repo", "", "owner/name of the repository whose pull requests are checked")
	automationRepo := fs.String("automation-repo", "", "owner/name of the repository holding the e2e tests")
	automationDir := fs.String("automation-dir", "", "existing checkout of the e2e repository's main branch (default: clone it when needed)")
	gitURL := fs.String("git-url", "https://github.com", "where -automation-repo is cloned from")
	number := fs.Int("pr", 0, "pull request to check")
	sweep := fs.Bool("sweep", false, "check every open pull request against main")
	statusContext := fs.String("context", "e2e-test/linked", "commit status context to set")
	waiverLabel := fs.String("waiver-label", "no-e2e-test", "label a reviewer adds when a change needs no e2e test")
	targetURL := fs.String("target-url", "", "link the status points to")
	dryRun := fs.Bool("dry-run", false, "check and report, but set no status")
	api := fs.String("api", "https://api.github.com", "GitHub API base URL")
	summary := fs.String("summary", getenv("GITHUB_STEP_SUMMARY"), "file to append the Markdown report to (default stdout)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *repo == "" || (*automationRepo == "" && *automationDir == "") || (*number > 0) == *sweep {
		fmt.Fprintln(stderr, "e2egate: need -repo, -automation-repo or -automation-dir, and either -pr N or -sweep")
		return 2
	}
	token := getenv("GH_TOKEN")
	if token == "" {
		fmt.Fprintln(stderr, "e2egate: GH_TOKEN is not set")
		return 2
	}

	report := stdout
	if *summary != "" {
		f, err := os.OpenFile(*summary, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Fprintf(stderr, "e2egate: %v\n", err)
			return 2
		}
		defer f.Close()
		report = f
	}

	var cloned string
	defer func() {
		if cloned != "" {
			os.RemoveAll(cloned)
		}
	}()
	tests := func(ctx context.Context) (string, error) {
		if *automationDir != "" {
			return *automationDir, nil
		}
		automationToken := getenv("AUTOMATION_TOKEN")
		if automationToken == "" {
			return "", errors.New("AUTOMATION_TOKEN is not set")
		}
		parent, err := os.MkdirTemp("", "e2egate-")
		if err != nil {
			return "", err
		}
		cloned = parent
		dir := filepath.Join(parent, "automation")
		url := strings.TrimRight(*gitURL, "/") + "/" + *automationRepo + ".git"
		return dir, e2egate.CloneTests(ctx, url, automationToken, dir)
	}

	router := github.NewClient(*api, token)
	g := &e2egate.Gate{
		Router:      router,
		Repo:        *repo,
		Context:     *statusContext,
		WaiverLabel: *waiverLabel,
		TargetURL:   *targetURL,
		DryRun:      *dryRun,
		Summary:     report,
		Tests:       tests,
	}

	if *sweep {
		if err := g.Sweep(ctx); err != nil {
			fmt.Fprintf(stderr, "e2egate: %v\n", err)
			return 1
		}
		return 0
	}

	v, err := g.Run(ctx, *number)
	if err != nil {
		fmt.Fprintf(stdout, "::error title=E2E test gate::%s\n", escapeData(err.Error()))
		return 1
	}
	if v.State == "" {
		fmt.Fprintf(stdout, "e2egate: #%d is not open: no status set\n", *number)
		return 0
	}
	fmt.Fprintf(stdout, "e2egate: #%d: %s: %s\n", *number, v.State, v.Description)
	if v.State != "success" {
		fmt.Fprintf(stdout, "::error title=E2E test gate::%s\n", escapeData(v.Description))
		return 1
	}
	return 0
}

// escapeData escapes a workflow command message the way GitHub's toolkit does,
// so a newline or a percent sign in an error cannot break the command.
func escapeData(s string) string {
	return strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A").Replace(s)
}
