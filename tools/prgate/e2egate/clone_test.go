package e2egate

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// origin creates a git repository with tests/ and another directory.
func origin(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	dir := t.TempDir()
	mustWrite(t, dir+"/tests/sim/test_a.py", `@pytest.mark.expose(bug="MAG-7")`)
	mustWrite(t, dir+"/src/big.py", "x = 1")
	for _, args := range [][]string{
		{"init", "--quiet", "--initial-branch=main"},
		{"add", "."},
		{"-c", "user.name=t", "-c", "user.email=t@t", "commit", "--quiet", "-m", "init"},
	} {
		if out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	return dir
}

func TestCloneTests(t *testing.T) {
	src := origin(t)
	dir := filepath.Join(t.TempDir(), "automation")
	// A token rides an HTTP header, which a file:// clone never sends.
	if err := CloneTests(context.Background(), "file://"+src, "a-token", dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "tests/sim/test_a.py")); err != nil {
		t.Errorf("tests/ was not checked out: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "src/big.py")); !os.IsNotExist(err) {
		t.Errorf("a directory other than tests/ was checked out: %v", err)
	}
	matches, control, err := SearchTree(dir, []string{"MAG-7"})
	if err != nil || control != 1 || len(matches) != 1 {
		t.Fatalf("SearchTree on the clone = %v, %d, %v", matches, control, err)
	}
}

func TestCloneTestsFails(t *testing.T) {
	origin(t) // skips without git
	err := CloneTests(context.Background(), "file:///nowhere/at/all", "secret-token", filepath.Join(t.TempDir(), "x"))
	if err == nil {
		t.Fatal("a missing repository must fail")
	}
	if strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("the error leaks the token: %v", err)
	}
}
