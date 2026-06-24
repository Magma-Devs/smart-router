// Package flow orchestrates the wizard's screens and holds the run state that
// the steps fill in: chains → interfaces → endpoints → backups → cache →
// dashboard → save → run → smoke.
package flow

import (
	"github.com/magma-Devs/smart-router/tools/wizard/internal/catalog"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/emit"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/health"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/icons"
)

// Listener pairs a chosen (chain, interface) with its assigned local port.
type Listener struct {
	Chain catalog.Chain
	Iface string
	Port  int
}

// State is the accumulating wizard result.
type State struct {
	RepoRoot string
	Source   string // catalog source (gh/raw/local), for display
	SpecDir  string // local specs/ dir (for catalog local fallback + health)
	Icons    *icons.Renderer
	Prober   *health.Prober

	Listeners []Listener
	Primary   []emit.Upstream
	Backup    []emit.Upstream

	Cache     bool
	Dashboard bool
	Metrics   string

	ConfigName string
	ConfigDir  string

	// outputs computed at save time
	TemplatePath string
	RenderedPath string
	EnvPath      string
}

const firstPort = 3360

// AssignListeners turns chosen chains + their interfaces into ordered listeners
// with sequential ports starting at 3360.
func (s *State) AssignListeners(sel map[string][]string, chains []catalog.Chain) {
	byIndex := map[string]catalog.Chain{}
	for _, c := range chains {
		byIndex[c.Index] = c
	}
	port := firstPort
	s.Listeners = s.Listeners[:0]
	for _, c := range chains { // stable catalog order
		ifs, ok := sel[c.Index]
		if !ok {
			continue
		}
		for _, iface := range ifs {
			s.Listeners = append(s.Listeners, Listener{Chain: byIndex[c.Index], Iface: iface, Port: port})
			port++
		}
	}
}

// MetricsAddr returns the configured metrics listen address (default on).
func (s *State) MetricsAddr() string {
	if s.Metrics == "" {
		return "0.0.0.0:7779"
	}
	return s.Metrics
}
