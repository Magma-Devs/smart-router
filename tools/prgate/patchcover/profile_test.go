package patchcover

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const mod = "github.com/example/repo"

func coverage(t *testing.T, profile, modulePath, dir string) *Coverage {
	t.Helper()
	c := NewCoverage()
	if err := c.AddProfile(strings.NewReader(profile), modulePath, dir); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestAddProfileMergesCopies(t *testing.T) {
	// -coverpkg makes every test binary report every block; one copy ran.
	c := coverage(t, "mode: set\n"+
		mod+"/pkg/a.go:3.10,5.2 2 0\n"+
		mod+"/pkg/a.go:3.10,5.2 2 1\n"+
		mod+"/pkg/a.go:3.10,5.2 2 0\n"+
		mod+"/pkg/a.go:7.10,9.2 1 0\n", mod, ".")

	for _, tc := range []struct {
		line, col          int
		coverable, covered bool
	}{
		{2, 5, false, false},
		{3, 9, false, false}, // before the block starts on its line
		{3, 10, true, true},
		{4, 1, true, true},
		{5, 2, true, true},
		{5, 3, false, false}, // after the block ends on its line
		{8, 3, true, false},
		{10, 1, false, false},
	} {
		coverable, covered := c.At("pkg/a.go", tc.line, tc.col)
		if coverable != tc.coverable || covered != tc.covered {
			t.Errorf("%d.%d: got coverable=%v covered=%v, want %v %v", tc.line, tc.col, coverable, covered, tc.coverable, tc.covered)
		}
	}
}

func TestAtTouchingBlocks(t *testing.T) {
	// The block before an if ends at its brace, where the body's block starts.
	c := coverage(t, "mode: set\n"+
		mod+"/a.go:3.10,5.11 1 1\n"+
		mod+"/a.go:5.11,7.3 1 0\n", mod, ".")
	if coverable, covered := c.At("a.go", 5, 2); !coverable || !covered {
		t.Fatalf("5.2 lies in the block that ran: %v %v", coverable, covered)
	}
	if coverable, covered := c.At("a.go", 6, 3); !coverable || covered {
		t.Fatalf("6.3 lies in the body that did not run: %v %v", coverable, covered)
	}
}

func TestAddProfileNestedModule(t *testing.T) {
	c := coverage(t, "mode: set\n"+mod+"/tools/w/x.go:1.1,2.2 1 1\n", mod+"/tools/w", "tools/w")
	if _, covered := c.At("tools/w/x.go", 1, 5); !covered {
		t.Fatal("block of a nested module not mapped under its directory")
	}
}

func TestAddProfileErrors(t *testing.T) {
	for name, profile := range map[string]string{
		"no mode line":   mod + "/a.go:1.1,2.2 1 1\n",
		"empty":          "",
		"malformed":      "mode: set\n" + mod + "/a.go:1.1,2.2 x 1\n",
		"outside module": "mode: set\ngithub.com/other/repo/a.go:1.1,2.2 1 1\n",
		"no block":       "mode: set\n\n",
	} {
		if err := NewCoverage().AddProfile(strings.NewReader(profile), mod, "."); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestModulePath(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "go.mod", "// comment\nmodule \"example.com/q\"\n\ngo 1.26\n")
	if got, err := ModulePath(dir); err != nil || got != "example.com/q" {
		t.Fatalf("ModulePath = %q, %v", got, err)
	}
	write(t, dir, "go.mod", "go 1.26\n")
	if _, err := ModulePath(dir); err == nil {
		t.Fatal("go.mod without a module line: want an error")
	}
	if _, err := ModulePath(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("missing go.mod: want an error")
	}
}

func write(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
