package ui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Breadcrumb renders the compact logo plus a step trail with the current step
// highlighted, e.g.  "SMART ROUTER · config wizard
//                     chains › endpoints › ▸ backups · cache › save".
// It's drawn at the top of each step (after clearing the screen) so the user
// keeps their place when navigating back and forth.
func Breadcrumb(steps []string, current int) string {
	var parts []string
	for i, s := range steps {
		switch {
		case i == current:
			parts = append(parts, lipgloss.NewStyle().Foreground(Brand).Bold(true).Render("▸ "+s))
		case i < current:
			parts = append(parts, OK.Render(s))
		default:
			parts = append(parts, Hint.Render(s))
		}
	}
	sep := Hint.Render(" › ")
	trail := strings.Join(parts, sep)
	return "  " + LogoCompact() + "\n  " + trail + "\n"
}
