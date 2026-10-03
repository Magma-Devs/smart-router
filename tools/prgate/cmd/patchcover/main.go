// Command patchcover fails when the unit tests run too little of a change.
//
//	patchcover -root . -diff change.diff -min 80 \
//	    -module .=root.cover -module tools/wizard=wizard.cover
//
// It reads the lines the diff adds and one coverage profile per Go module,
// writes the result as Markdown (to -summary, or stdout) and as GitHub
// annotations on stdout, and exits 1 when unit tests run less than -min
// percent of the statements the change adds or edits. With -repo and -pr (and
// GH_TOKEN) a reviewer's -waiver-label on the pull request lets a shortfall
// pass. With -changed-tests it prints the tests the change adds or edits, one
// per line, and measures nothing. scripts/patch-coverage.sh builds its inputs.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/magma-Devs/smart-router/tools/prgate/github"
	"github.com/magma-Devs/smart-router/tools/prgate/patchcover"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

// moduleFlags collects repeated -module dir=profile flags.
type moduleFlags []string

func (m *moduleFlags) String() string     { return strings.Join(*m, ",") }
func (m *moduleFlags) Set(v string) error { *m = append(*m, v); return nil }

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("patchcover", flag.ContinueOnError)
	fs.SetOutput(stderr)
	root := fs.String("root", ".", "repository checkout the diff was taken against")
	diffPath := fs.String("diff", "", "unified diff of the change, taken with --unified=0")
	minPercent := fs.Int("min", 80, "share, in percent, of the changed statements a test must run")
	summary := fs.String("summary", "", "file to append the Markdown result to (default stdout)")
	changedTests := fs.Bool("changed-tests", false, "print the Test and Fuzz functions the change adds or edits, and stop")
	repo := fs.String("repo", "", "owner/name of the repository, to read the waiver label")
	number := fs.Int("pr", 0, "pull request whose waiver label may let a shortfall pass")
	waiverLabel := fs.String("waiver-label", "no-unit-coverage", "label a reviewer adds to accept a shortfall")
	api := fs.String("api", "https://api.github.com", "GitHub API base URL")
	var modules moduleFlags
	fs.Var(&modules, "module", "dir=profile: a Go module's directory, relative to -root, and a coverage profile of it (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *diffPath == "" || (len(modules) == 0 && !*changedTests) || *minPercent < 0 || *minPercent > 100 {
		fmt.Fprintln(stderr, "patchcover: need -diff, at least one -module (or -changed-tests), and -min between 0 and 100")
		return 2
	}

	diff, err := readDiff(*diffPath)
	if err != nil {
		fmt.Fprintf(stderr, "patchcover: %v\n", err)
		return 2
	}
	if *changedTests {
		names, err := patchcover.ChangedTests(diff, *root)
		if err != nil {
			fmt.Fprintf(stderr, "patchcover: %v\n", err)
			return 2
		}
		for _, n := range names {
			fmt.Fprintln(stdout, n)
		}
		return 0
	}

	res, err := measure(*root, diff, modules)
	if err != nil {
		fmt.Fprintf(stderr, "patchcover: %v\n", err)
		return 2
	}

	out := stdout
	if *summary != "" {
		f, err := os.OpenFile(*summary, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Fprintf(stderr, "patchcover: %v\n", err)
			return 2
		}
		defer f.Close()
		out = f
	}
	res.WriteMarkdown(out, *minPercent)
	res.WriteAnnotations(stdout, 10)

	line := fmt.Sprintf("Unit tests run %d of %d changed statements (%.1f%%), threshold %d%%", res.Covered, res.Coverable, res.Percent(), *minPercent)
	if res.Pass(*minPercent) {
		fmt.Fprintf(stdout, "patchcover: %s.\n", line)
		return 0
	}
	if *repo != "" && *number > 0 {
		waived, note := waiver(ctx, *api, getenv("GH_TOKEN"), *repo, *number, *waiverLabel)
		fmt.Fprintf(out, "\n%s\n", note)
		if waived {
			fmt.Fprintf(stdout, "::warning title=Unit test coverage::%s. %s\n", line, note)
			return 0
		}
	}
	fmt.Fprintf(stdout, "::error title=Unit test coverage::%s.\n", line)
	return 1
}

// waiver reads the pull request's waiver label. Anything it cannot read counts
// as no waiver.
func waiver(ctx context.Context, api, token, repo string, number int, label string) (bool, string) {
	client := github.NewClient(api, token)
	pr, err := client.PullRequest(ctx, repo, number)
	if err != nil {
		return false, fmt.Sprintf("The %s label could not be read: %v.", label, err)
	}
	w, err := client.CheckWaiver(ctx, repo, pr, label)
	switch {
	case err != nil:
		return false, fmt.Sprintf("The %s label could not be read: %v.", label, err)
	case w.Valid:
		return true, fmt.Sprintf("Waived by @%s with the %s label.", w.By, label)
	case w.Present:
		return false, fmt.Sprintf("The %s label does not count: %s.", label, w.Why)
	default:
		return false, fmt.Sprintf("When the gap is right, a reviewer other than the author adds the %s label.", label)
	}
}

func readDiff(path string) (map[string]*patchcover.FileDiff, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return patchcover.ParseDiff(f)
}

func measure(root string, diff map[string]*patchcover.FileDiff, modules []string) (patchcover.Result, error) {
	cov := patchcover.NewCoverage()
	dirs := make([]string, 0, len(modules))
	for _, m := range modules {
		dir, profile, ok := strings.Cut(m, "=")
		if !ok || dir == "" || profile == "" {
			return patchcover.Result{}, fmt.Errorf("-module %q: want dir=profile", m)
		}
		modPath, err := patchcover.ModulePath(filepath.Join(root, dir))
		if err != nil {
			return patchcover.Result{}, err
		}
		if err := addProfile(cov, profile, modPath, dir); err != nil {
			return patchcover.Result{}, fmt.Errorf("%s: %w", profile, err)
		}
		dirs = append(dirs, dir)
	}
	return patchcover.Measure(diff, cov, root, dirs)
}

func addProfile(cov *patchcover.Coverage, profile, modPath, dir string) error {
	f, err := os.Open(profile)
	if err != nil {
		return err
	}
	defer f.Close()
	return cov.AddProfile(f, modPath, dir)
}
