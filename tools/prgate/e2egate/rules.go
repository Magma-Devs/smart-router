// Package e2egate decides whether a smart-router pull request has an e2e test
// in smart-router-automation, and publishes the answer as a commit status.
//
// The Jira ticket links the two repositories. A router pull request names its
// ticket on a "Jira ticket: MAG-123" line. An automation test names the ticket
// it tracks: in an xfail or expose marker, an allure tag, or its docstring.
// The automation repository's verify-bug-fix skill finds the tests of a ticket
// by the same rule (.claude/skills/verify-bug-fix/scripts/select_tests.py).
package e2egate

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/magma-Devs/smart-router/tools/prgate/github"
)

var (
	jiraTicketLine = regexp.MustCompile(`(?im)^[ \t]*jira[ \t]+ticket[ \t]*:(.*)$`)
	ticketKey      = regexp.MustCompile(`(?i)\bMAG-([0-9]+)\b`)
	anyTicket      = regexp.MustCompile(`MAG-[0-9]`)
	unitMarker     = regexp.MustCompile(`\bpytest\.mark\.unit\b`)
)

// TicketKeys returns the MAG tickets on a pull request's "Jira ticket:" lines,
// sorted. Only those lines count: the title and the branch carry no ticket of
// their own, and a line naming any other ticket would let an author pick one
// that happens to have tests.
func TicketKeys(pr github.PullRequest) []string {
	seen := map[string]bool{}
	for _, m := range jiraTicketLine.FindAllStringSubmatch(pr.Body, -1) {
		for _, k := range ticketKey.FindAllStringSubmatch(m[1], -1) {
			seen["MAG-"+k[1]] = true
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// RouterChanges returns the router Go files a pull request adds code to: Go
// sources other than tests, testdata, tools/, and generated mocks and protobuf
// code. A file the pull request only deletes from, only renames, or only adds
// line comments to adds no code.
func RouterChanges(files []github.PRFile) []string {
	var out []string
	for _, f := range files {
		name := f.Filename
		base := path.Base(name)
		switch {
		case f.Status == "removed", f.Additions == 0:
		case !strings.HasSuffix(name, ".go"), strings.HasSuffix(name, "_test.go"):
		case strings.HasPrefix(name, "tools/"), strings.HasPrefix(name, "testdata/"), strings.Contains(name, "/testdata/"):
		case strings.HasSuffix(base, ".pb.go"), strings.HasSuffix(base, "_mock.go"), strings.HasPrefix(base, "mock_"):
		case commentsOnly(f.Patch):
		default:
			out = append(out, name)
		}
	}
	return out
}

// commentsOnly reports whether every line a patch adds is blank or a line
// comment. An empty patch (GitHub omits a large one) is not: it may hold code.
func commentsOnly(patch string) bool {
	if patch == "" {
		return false
	}
	for _, line := range strings.Split(patch, "\n") {
		added, ok := strings.CutPrefix(line, "+")
		if !ok {
			continue
		}
		if t := strings.TrimSpace(added); t != "" && !strings.HasPrefix(t, "//") {
			return false
		}
	}
	return true
}

// IsTestFile reports whether a smart-router-automation path is a pytest module
// under tests/: test_*.py or *_test.py.
func IsTestFile(p string) bool {
	base := path.Base(p)
	return strings.HasPrefix(p, "tests/") && strings.HasSuffix(base, ".py") &&
		(strings.HasPrefix(base, "test_") || strings.HasSuffix(base, "_test.py"))
}

// IsUnitTest reports whether an automation test file tests the framework rather
// than the router: it sits in a unit directory or uses the unit marker.
// select_tests.py leaves the same tests out.
func IsUnitTest(p string, src []byte) bool {
	return strings.HasPrefix(p, "tests/unit/") || strings.HasPrefix(p, "tests/infrastructure/unit/") || unitMarker.Match(src)
}

// Names returns the keys a file names. MAG-12 must not match MAG-123, so a key
// counts only when no digit follows it.
func Names(src []byte, keys []string) []string {
	var named []string
	for _, k := range keys {
		if regexp.MustCompile(regexp.QuoteMeta(k) + `([^0-9]|$)`).Match(src) {
			named = append(named, k)
		}
	}
	return named
}

// Match is an automation test file that names one or more of the keys.
type Match struct {
	Path string
	Keys []string
}

// SearchTree returns the automation test files under dir/tests that name any
// of the keys, unit tests left out. control counts the test files that name
// any MAG ticket at all. Zero means the search is broken (a wrong directory,
// an empty checkout), not that the tickets lack tests.
func SearchTree(dir string, keys []string) (matches []Match, control int, err error) {
	err = filepath.WalkDir(filepath.Join(dir, "tests"), func(full string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, full)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() || !IsTestFile(rel) {
			return nil
		}
		src, err := os.ReadFile(full)
		if err != nil {
			return err
		}
		if !anyTicket.Match(src) {
			return nil
		}
		control++
		if IsUnitTest(rel, src) {
			return nil
		}
		if named := Names(src, keys); len(named) > 0 {
			matches = append(matches, Match{Path: rel, Keys: named})
		}
		return nil
	})
	return matches, control, err
}
