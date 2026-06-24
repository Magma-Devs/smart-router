package flow

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/magma-Devs/smart-router/tools/wizard/internal/emit"
)

// SaveDir is the absolute directory configs are written to (config/local by
// default), shown to the user so they see the full path.
func (s *State) SaveDir() string {
	if s.ConfigDir != "" {
		return s.ConfigDir
	}
	return filepath.Join(s.RepoRoot, "config", "local")
}

// buildConfig assembles the emit.Config from current state.
func (s *State) buildConfig() *emit.Config {
	ls := make([]emit.Listener, 0, len(s.Listeners))
	for _, l := range s.Listeners {
		ls = append(ls, emit.Listener{ChainID: l.Chain.Index, Iface: l.Iface, Port: l.Port})
	}
	metrics := s.MetricsAddr()
	if s.Metrics == "disabled" {
		metrics = "disabled"
	}
	return &emit.Config{
		Metrics:   metrics,
		Cache:     s.Cache,
		Listeners: ls,
		Primary:   s.Primary,
		Backup:    s.Backup,
	}
}

// writeProbeConfig writes a throwaway config under config/local/ (so viper
// resolves it) and returns its repo-relative path + a cleanup func.
func (s *State) writeProbeConfig(cfg *emit.Config) (relPath string, cleanup func(), err error) {
	dir := filepath.Join(s.RepoRoot, "config", "local")
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return "", func() {}, err
	}
	name := fmt.Sprintf("_probe_%d.yml", os.Getpid())
	full := filepath.Join(dir, name)
	if err = os.WriteFile(full, []byte(cfg.YAML()), 0o644); err != nil {
		return "", func() {}, err
	}
	rel := filepath.ToSlash(filepath.Join("config", "local", name))
	return rel, func() { _ = os.Remove(full) }, nil
}

// Save renders the template + .env + final config under config/local/, runs the
// lint, and (when secrets are filled) the full health validation. Returns the
// repo-relative rendered path.
func (s *State) Save(name string) (relRendered string, problems []string, err error) {
	dir := s.ConfigDir
	if dir == "" {
		dir = filepath.Join(s.RepoRoot, "config", "local")
	}
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return "", nil, err
	}
	s.ensureGitignore(dir)

	cfg := s.buildConfig()
	tpl := filepath.Join(dir, name+".template.yml")
	env := filepath.Join(dir, ".env")
	out := filepath.Join(dir, name+".yml")

	if err = os.WriteFile(tpl, []byte(cfg.YAML()), 0o644); err != nil {
		return "", nil, err
	}
	envMap := map[string]string{}
	if vars := cfg.EnvVars(); len(vars) > 0 {
		if _, statErr := os.Stat(env); statErr != nil {
			_ = os.WriteFile(env, []byte(cfg.EnvTemplate()), 0o600)
		}
		if m, e := emit.LoadEnv(env); e == nil {
			envMap = m
		}
	}
	rendered := emit.Render(cfg.YAML(), envMap)
	if err = os.WriteFile(out, []byte(rendered), 0o644); err != nil {
		return "", nil, err
	}

	s.TemplatePath, s.EnvPath, s.RenderedPath = tpl, env, out
	rel := filepath.ToSlash(out)
	if r, e := filepath.Rel(s.RepoRoot, out); e == nil {
		rel = filepath.ToSlash(r)
	}
	return rel, cfg.Lint(rendered), nil
}

func (s *State) ensureGitignore(dir string) {
	rel, err := filepath.Rel(s.RepoRoot, dir)
	if err != nil || rel == "" || rel[0] == '.' && len(rel) > 1 && rel[1] == '.' {
		return // outside repo
	}
	gi := filepath.Join(s.RepoRoot, ".gitignore")
	entry := filepath.ToSlash(rel) + "/"
	if b, e := os.ReadFile(gi); e == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if line == entry {
				return
			}
		}
	}
	f, e := os.OpenFile(gi, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if e != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(entry + "\n")
}
