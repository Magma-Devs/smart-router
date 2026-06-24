package laststate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// withConfigDir points XDG at a temp dir for the duration of the test so we
// never touch the real ~/.config. os.UserConfigDir honours $XDG_CONFIG_HOME on
// Linux, so this fully isolates Save/Load/Path.
func withConfigDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	return dir
}

func sampleRecord() Record {
	return Record{
		GeneratedAt: "2026-06-24T12:00:00Z",
		RepoRoot:    "/home/bob/projects/smart-router",
		ConfigPath:  "config/local/smartrouter_custom.yml",
		RenderStep:  "SR_SECRETS=… smartrouter render …",
		UpCommand:   "SR_CONFIG=… docker compose up --build",
		DownCommand: "docker compose down",
		ScriptPath:  "config/local/run.sh",
		Dashboard:   true,
	}
}

func TestPathUsesXDGConfigHome(t *testing.T) {
	base := withConfigDir(t)
	got, err := Path()
	if err != nil {
		t.Fatalf("Path: %v", err)
	}
	want := filepath.Join(base, "smartrouter-wizard", "last-run.json")
	if got != want {
		t.Fatalf("Path = %q, want %q", got, want)
	}
}

func TestSaveThenLoadRoundTrips(t *testing.T) {
	withConfigDir(t)
	rec := sampleRecord()
	if err := Save(rec); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got == nil {
		t.Fatal("Load returned nil after Save")
	}
	if *got != rec {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", *got, rec)
	}
}

func TestSaveCreatesDirAndTrailingNewline(t *testing.T) {
	base := withConfigDir(t)
	// The smartrouter-wizard subdir must not exist yet — Save should MkdirAll it.
	if _, err := os.Stat(filepath.Join(base, "smartrouter-wizard")); !os.IsNotExist(err) {
		t.Fatalf("precondition: dir should not exist yet (err=%v)", err)
	}
	if err := Save(sampleRecord()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	p, _ := Path()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !strings.HasSuffix(string(b), "}\n") {
		t.Fatalf("expected pretty JSON ending in a newline, got tail %q", tail(string(b)))
	}
}

func TestLoadMissingReturnsNilNil(t *testing.T) {
	withConfigDir(t) // fresh temp config dir, nothing written
	got, err := Load()
	if err != nil {
		t.Fatalf("Load on missing record should not error, got %v", err)
	}
	if got != nil {
		t.Fatalf("Load on missing record should be nil, got %+v", *got)
	}
}

func TestLoadCorruptJSONErrors(t *testing.T) {
	withConfigDir(t)
	p, _ := Path()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(p, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("seed corrupt file: %v", err)
	}
	if _, err := Load(); err == nil {
		t.Fatal("expected an error loading corrupt JSON, got nil")
	}
}

func TestSaveOverwritesPrevious(t *testing.T) {
	withConfigDir(t)
	if err := Save(sampleRecord()); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	second := sampleRecord()
	second.UpCommand = "docker compose -f a -f b up --build"
	second.Dashboard = false
	if err := Save(second); err != nil {
		t.Fatalf("second Save: %v", err)
	}
	got, err := Load()
	if err != nil || got == nil {
		t.Fatalf("Load after overwrite: rec=%v err=%v", got, err)
	}
	if got.UpCommand != second.UpCommand || got.Dashboard {
		t.Fatalf("overwrite not reflected: %+v", *got)
	}
}

func tail(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[len(s)-12:]
}
