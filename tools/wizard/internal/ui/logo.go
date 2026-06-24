package ui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// logoLines is the SMART ROUTER wordmark (same glyphs as the bash wizard /
// smart-router-standalone), rendered with a top-to-bottom ember gradient.
var logoLines = []string{
	`███████╗███╗   ███╗ █████╗ ██████╗ ████████╗    ██████╗  ██████╗ ██╗   ██╗████████╗███████╗██████╗`,
	`██╔════╝████╗ ████║██╔══██╗██╔══██╗╚══██╔══╝    ██╔══██╗██╔═══██╗██║   ██║╚══██╔══╝██╔════╝██╔══██╗`,
	`███████╗██╔████╔██║███████║██████╔╝   ██║       ██████╔╝██║   ██║██║   ██║   ██║   █████╗  ██████╔╝`,
	`╚════██║██║╚██╔╝██║██╔══██║██╔══██╗   ██║       ██╔══██╗██║   ██║██║   ██║   ██║   ██╔══╝  ██╔══██╗`,
	`███████║██║ ╚═╝ ██║██║  ██║██║  ██║   ██║       ██║  ██║╚██████╔╝╚██████╔╝   ██║   ███████╗██║  ██║`,
	`╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝       ╚═╝  ╚═╝ ╚═════╝  ╚═════╝    ╚═╝   ╚══════╝╚═╝  ╚═╝`,
}

// gradient runs warm-red → amber down the six logo rows.
var gradient = []lipgloss.Color{Brand, Ember1, Ember1, Ember2, Ember2, Ember3}

// Logo returns the colored wordmark plus a subtitle, centered to width.
func Logo(width int) string {
	var b strings.Builder
	for i, line := range logoLines {
		c := gradient[i%len(gradient)]
		b.WriteString(lipgloss.NewStyle().Foreground(c).Render(line))
		b.WriteByte('\n')
	}
	sub := lipgloss.NewStyle().Foreground(Muted).Italic(true).
		Render("local config wizard")
	tagline := lipgloss.NewStyle().Foreground(Faint).
		Render("spec-driven · health-verified · docker-ready")

	logo := strings.TrimRight(b.String(), "\n")
	block := lipgloss.JoinVertical(lipgloss.Center, logo, "", sub, tagline)
	if width > 0 {
		return lipgloss.PlaceHorizontal(width, lipgloss.Center, block)
	}
	return block
}

// Banner wraps the logo in breathing room for the splash screen.
func Banner(width int) string {
	return "\n" + Logo(width) + "\n"
}

// LogoCompact is a single-line gradient wordmark for use as a persistent header
// inside the TUI (where the full 6-row banner would eat too much height).
func LogoCompact() string {
	word := "SMART ROUTER"
	var b strings.Builder
	cols := []lipgloss.Color{Brand, Ember1, Ember2, Ember3}
	for i, r := range word {
		c := cols[i%len(cols)]
		b.WriteString(lipgloss.NewStyle().Foreground(c).Bold(true).Render(string(r)))
	}
	tag := lipgloss.NewStyle().Foreground(Faint).Render("  · config wizard")
	return b.String() + tag
}
