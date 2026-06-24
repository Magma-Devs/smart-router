package ui

import (
	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss"
)

// HuhTheme is the brand-matched theme applied to every huh form so selects,
// inputs, and confirms share the wizard's palette.
func HuhTheme() *huh.Theme {
	t := huh.ThemeBase()

	t.Focused.Title = t.Focused.Title.Foreground(Brand).Bold(true)
	t.Focused.Description = t.Focused.Description.Foreground(Muted)
	t.Focused.Base = t.Focused.Base.BorderForeground(Brand)
	t.Focused.SelectSelector = t.Focused.SelectSelector.Foreground(Brand)
	t.Focused.SelectedOption = t.Focused.SelectedOption.Foreground(Ember2)
	t.Focused.MultiSelectSelector = t.Focused.MultiSelectSelector.Foreground(Brand)
	t.Focused.SelectedPrefix = lipgloss.NewStyle().Foreground(Good).SetString(Check+" ")
	t.Focused.UnselectedPrefix = lipgloss.NewStyle().Foreground(Faint).SetString("• ")
	t.Focused.FocusedButton = t.Focused.FocusedButton.Background(Brand).Foreground(PanelBg).Bold(true)
	t.Focused.BlurredButton = t.Focused.BlurredButton.Foreground(Muted)
	t.Focused.TextInput.Cursor = t.Focused.TextInput.Cursor.Foreground(Brand)
	t.Focused.TextInput.Prompt = t.Focused.TextInput.Prompt.Foreground(Ember2)

	t.Blurred = t.Focused
	t.Blurred.Title = t.Blurred.Title.Foreground(Muted)
	t.Blurred.Base = t.Blurred.Base.BorderForeground(Faint)

	return t
}
