package flow

import (
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/huh"

	"github.com/magma-Devs/smart-router/tools/wizard/internal/ui"
)

// Nav is a step's outcome in the wizard's step machine.
type Nav int

const (
	Next   Nav = iota // advance to the next step
	Back              // return to the previous step (redo)
	Cancel            // abort the wizard
)

// runForm runs a huh form and maps the result to a Nav. Esc (and Ctrl-C) on any
// step aborts the form; the step machine treats that as Back — so the user
// returns to the previous step from anywhere, with no extra "continue?" prompt.
// At the first step, Back is interpreted as quit by the caller.
func runForm(groups ...*huh.Group) Nav { return RunForm(groups...) }

// RunForm runs a huh form with Esc/Ctrl-C mapped to abort, returning Back on
// abort and Next on a clean submit. Exported so the top-level save screen
// (package main) shares the same back semantics.
func RunForm(groups ...*huh.Group) Nav {
	km := huh.NewDefaultKeyMap()
	km.Quit = key.NewBinding(key.WithKeys("esc", "ctrl+c"), key.WithHelp("esc", "back"))

	form := huh.NewForm(groups...).WithTheme(ui.HuhTheme()).WithKeyMap(km)
	_ = form.Run()
	if form.State == huh.StateAborted {
		return Back
	}
	return Next
}
