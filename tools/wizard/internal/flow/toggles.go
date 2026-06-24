package flow

import (
	"fmt"

	"github.com/charmbracelet/huh"

	"github.com/magma-Devs/smart-router/tools/wizard/internal/ui"
)

// Backups optionally collects a backup-direct-rpc tier. Esc → Back.
func (s *State) Backups() Nav {
	fmt.Println(ui.Section(3, "Backup RPC endpoints"))
	fmt.Println(ui.Panel.Width(72).Render(
		ui.Accent.Render("What backups are for") + "\n" +
			"A second tier, consulted only when the primary pool is exhausted.\n" +
			"They must support the same addons as the primary tier for addon-\n" +
			"specific requests to fail over to them."))

	add := false
	if nav := runForm(huh.NewGroup(
		huh.NewConfirm().Title("Add backup endpoints?").Value(&add),
	)); nav != Next {
		return nav
	}
	if !add {
		return Next
	}
	return s.CollectEndpoints("backup")
}

// CacheStep toggles the response cache. Esc → Back.
func (s *State) CacheStep() Nav {
	fmt.Println(ui.Section(4, "Cache"))
	fmt.Println(ui.Panel.Width(72).Render(
		ui.Accent.Render("Response cache") + "\n" +
			"Runs the smartrouter 'cache' sidecar; immutable RPC responses are\n" +
			"served from it. Adds cache-be: + the cache overlay."))

	cache := s.Cache
	if nav := runForm(huh.NewGroup(
		huh.NewConfirm().Title("Enable the response cache?").Value(&cache),
	)); nav != Next {
		return nav
	}
	s.Cache = cache
	return Next
}

// DashboardStep toggles the observability dashboard. Esc → Back.
func (s *State) DashboardStep() Nav {
	fmt.Println(ui.Section(5, "Dashboard"))
	fmt.Println(ui.Panel.Width(72).Render(
		ui.Accent.Render("Observability dashboard") + "\n" +
			"Prometheus + smart-router-dashboard (UI :3000, API :8000, Prom :9090).\n" +
			"Needs router metrics ON. Login admin / password."))

	dash := s.Dashboard
	if nav := runForm(huh.NewGroup(
		huh.NewConfirm().Title("Enable the observability dashboard?").Value(&dash),
	)); nav != Next {
		return nav
	}
	s.Dashboard = dash
	if s.Dashboard && s.Metrics == "disabled" {
		s.Metrics = "0.0.0.0:7779"
	}
	return Next
}
