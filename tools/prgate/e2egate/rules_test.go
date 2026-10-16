package e2egate

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/magma-Devs/smart-router/tools/prgate/github"
)

func TestTicketKeys(t *testing.T) {
	// The body below is the pull request template with its lines filled in.
	body := "# Description\n\nCloses: #XXXX\n\n" +
		"<!-- Keep the PR title descriptive. Add exactly one line using the format\n" +
		"`Jira ticket: MAG-123`. The pull request cannot be merged unless the ticket\n" +
		"exists in Jira. -->\n\n" +
		"Jira ticket: MAG-4032\r\n" +
		"  JIRA TICKET: mag-12\n" +
		"E2E test: MAG-3729\n" +
		"See also MAG-1, which no ticket line names.\n"
	pr := github.PullRequest{Body: body, Title: "fix(x): thing (MAG-77)", Head: github.Ref{Ref: "mag-78"}}
	// The title, the branch, and an E2E line name no ticket for the gate: an
	// author could point them at any ticket that happens to have tests.
	want := []string{"MAG-12", "MAG-4032"}
	if got := TicketKeys(pr); !reflect.DeepEqual(got, want) {
		t.Fatalf("TicketKeys = %v, want %v", got, want)
	}
	if got := TicketKeys(github.PullRequest{Body: "Jira ticket:\nJira ticket: MAG-40321x"}); len(got) != 0 {
		t.Fatalf("an empty or glued key is no key, got %v", got)
	}
}

func TestRouterChanges(t *testing.T) {
	files := []github.PRFile{
		{Filename: "protocol/a.go", Status: "modified", Additions: 3},
		{Filename: "protocol/new.go", Status: "added", Additions: 10},
		{Filename: "protocol/gone.go", Status: "removed"},
		{Filename: "protocol/trimmed.go", Status: "modified", Additions: 0},
		{Filename: "protocol/moved.go", Status: "renamed", Additions: 0},
		{Filename: "protocol/a_test.go", Status: "modified", Additions: 5},
		{Filename: "protocol/testdata/fixture.go", Status: "added", Additions: 5},
		{Filename: "testdata/x.go", Status: "added", Additions: 5},
		{Filename: "tools/wizard/main.go", Status: "modified", Additions: 5},
		{Filename: "docs/X.md", Status: "modified", Additions: 5},
		{Filename: "protocol/chainlib/chainlib_mock.go", Status: "modified", Additions: 5},
		{Filename: "protocol/chainlib/mock_websocket.go", Status: "modified", Additions: 5},
		{Filename: "protocol/grpcproxy/testproto/service.pb.go", Status: "modified", Additions: 5},
		{Filename: "protocol/doc.go", Status: "modified", Additions: 2, Patch: "@@ -1,2 +1,4 @@\n // Package protocol\n+// says more now.\n+\n-// old\n package protocol"},
		{Filename: "protocol/code.go", Status: "modified", Additions: 2, Patch: "@@ -1 +1,2 @@\n+// a note\n+\tx := 1 // and code"},
	}
	want := []string{"protocol/a.go", "protocol/new.go", "protocol/code.go"}
	if got := RouterChanges(files); !reflect.DeepEqual(got, want) {
		t.Fatalf("RouterChanges = %v, want %v", got, want)
	}
}

func TestIsTestFile(t *testing.T) {
	for p, want := range map[string]bool{
		"tests/simulator/test_retry.py": true,
		"tests/x/retry_test.py":         true,
		"tests/simulator/helpers.py":    false,
		"tests/simulator/conftest.py":   false,
		"src/test_x.py":                 false,
		"tests/test_x.pyc":              false,
	} {
		if got := IsTestFile(p); got != want {
			t.Errorf("IsTestFile(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestIsUnitTest(t *testing.T) {
	for _, tc := range []struct {
		path, src string
		want      bool
	}{
		{"tests/infrastructure/unit/test_a.py", "", true},
		{"tests/unit/test_a.py", "", true},
		{"tests/resp/test_a.py", "pytestmark = [\n    pytest.mark.unit,\n]\n", true},
		{"tests/resp/test_a.py", "pytestmark = [pytest.mark.unit_like]\n", false},
		{"tests/simulator/test_a.py", "pytestmark = [pytest.mark.simulator]\n", false},
	} {
		if got := IsUnitTest(tc.path, []byte(tc.src)); got != tc.want {
			t.Errorf("IsUnitTest(%q, %q) = %v, want %v", tc.path, tc.src, got, tc.want)
		}
	}
}

func TestNames(t *testing.T) {
	src := []byte(`@pytest.mark.expose(bug="MAG-1672")` + "\n# MAG-16720 is another ticket\nMAG-9")
	if got := Names(src, []string{"MAG-1672", "MAG-1672 ", "MAG-167", "MAG-9", "MAG-16"}); !reflect.DeepEqual(got, []string{"MAG-1672", "MAG-9"}) {
		t.Fatalf("Names = %v", got)
	}
}

func automationTree(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for rel, content := range map[string]string{
		"tests/simulator/test_failover.py":              `@pytest.mark.xfail(reason="MAG-100: retry")`,
		"tests/simulator/test_other.py":                 `"""Refs MAG-200."""`,
		"tests/infrastructure/unit/test_skill.py":       `assert key == "MAG-100"`,
		"tests/resp/test_unit_marked.py":                "pytestmark = [pytest.mark.unit]\n# MAG-300",
		"tests/simulator/helpers.py":                    "# MAG-300 workaround",
		"tests/real_nodes/test_plain.py":                "def test_x(): pass",
		"tests/infrastructure/binary_startup/test_b.py": "# MAG-300 starts the real binary",
	} {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestSearchTree(t *testing.T) {
	dir := automationTree(t)
	matches, control, err := SearchTree(dir, []string{"MAG-100", "MAG-300"})
	if err != nil {
		t.Fatal(err)
	}
	want := []Match{
		{Path: "tests/infrastructure/binary_startup/test_b.py", Keys: []string{"MAG-300"}},
		{Path: "tests/simulator/test_failover.py", Keys: []string{"MAG-100"}},
	}
	if !reflect.DeepEqual(matches, want) {
		t.Errorf("matches:\n got %+v\nwant %+v", matches, want)
	}
	// Five test files name a ticket, unit tests included; helpers.py is no test.
	if control != 5 {
		t.Errorf("control = %d, want 5", control)
	}

	if _, _, err := SearchTree(t.TempDir(), []string{"MAG-1"}); err == nil {
		t.Error("a checkout without tests/ must be an error, not zero matches")
	}
}

// The pull request template must not name a ticket on its own: every pull
// request opened from it would carry that ticket.
func TestTemplateNamesNoTicket(t *testing.T) {
	body, err := os.ReadFile("../../../.github/pull_request_template.md")
	if err != nil {
		t.Skipf("no pull request template here: %v", err)
	}
	if keys := TicketKeys(github.PullRequest{Body: string(body)}); len(keys) != 0 {
		t.Fatalf("the template names %v", keys)
	}
}
