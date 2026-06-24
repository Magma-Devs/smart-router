// Package laststate persists the most recently generated run plan so a later
// `wizard --last` invocation can reprint it without re-walking the whole wizard.
//
// The record lives in the user's config dir (XDG: $XDG_CONFIG_HOME, else
// ~/.config) under smartrouter-wizard/last-run.json — NOT in the repo, so the
// generated configs the wizard writes into config/local stay the only repo
// artifacts (and those are gitignored separately). One global "most recent"
// record, overwritten on each finish, readable from any working directory.
package laststate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Record is the persisted snapshot of a finished wizard run. It holds the exact
// command strings (already rendered) plus enough context to reprint the panel.
type Record struct {
	// GeneratedAt is an RFC3339 timestamp stamped by the caller (the flow can't
	// read the clock — Date.now is unavailable there — so main passes it in).
	GeneratedAt string `json:"generatedAt"`
	RepoRoot    string `json:"repoRoot"`
	ConfigPath  string `json:"configPath"` // repo-relative rendered config
	RenderStep  string `json:"renderStep"`
	UpCommand   string `json:"upCommand"`
	DownCommand string `json:"downCommand"`
	ScriptPath  string `json:"scriptPath,omitempty"` // repo-relative run.sh, if written
	Dashboard   bool   `json:"dashboard"`
}

// dir returns the directory the record lives in: $XDG_CONFIG_HOME/smartrouter-wizard
// or ~/.config/smartrouter-wizard. os.UserConfigDir already implements that XDG
// fallback, so we defer to it.
func dir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "smartrouter-wizard"), nil
}

// Path is the absolute path to the last-run record (for display / tests).
func Path() (string, error) {
	d, err := dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(d, "last-run.json"), nil
}

// Save writes rec as the most-recent run, creating the config dir if needed.
// Best-effort by contract of the caller: a failure to persist must never break
// the run itself, so callers typically ignore the error (or log it softly).
func Save(rec Record) error {
	d, err := dir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(d, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	p := filepath.Join(d, "last-run.json")
	return os.WriteFile(p, append(b, '\n'), 0o644)
}

// Load reads the most-recent run record. It returns (nil, nil) when no record
// exists yet (a fresh machine that never finished the wizard) so the caller can
// print a friendly "nothing yet" message rather than treating it as an error.
func Load() (*Record, error) {
	p, err := Path()
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec Record
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, fmt.Errorf("last-run record is corrupt (%s): %w", p, err)
	}
	return &rec, nil
}
