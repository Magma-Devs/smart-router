package ui

import (
	"github.com/charmbracelet/lipgloss"

	"github.com/magma-Devs/smart-router/tools/wizard/internal/catalog"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/classify"
)

// familyColor maps a family to its accent color.
func familyColor(f classify.Family) lipgloss.Color {
	switch f {
	case classify.EVM:
		return Brand
	case classify.Cosmos:
		return Ember1
	case classify.BTC:
		return Ember2
	case classify.OtherL1:
		return Ember3
	default:
		return Muted
	}
}

// FamilyTag renders a chain's family glyph in its accent color, e.g. "◆".
func FamilyTag(c catalog.Chain) string {
	return FamilyTagFor(classify.FamilyOf(c))
}

// FamilyTagFor renders a known family's glyph in its accent color.
func FamilyTagFor(f classify.Family) string {
	return lipgloss.NewStyle().Foreground(familyColor(f)).Bold(true).Render(f.Icon())
}
