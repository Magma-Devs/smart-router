package flow

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/huh"

	"github.com/magma-Devs/smart-router/tools/wizard/internal/catalog"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/ui"
)

// SelectInterfaces asks, for each chosen chain that exposes more than one
// interface, which interface(s) to expose. Single-interface chains are taken
// as-is. Returns chainIndex → []iface. Cancelling returns (nil, false).
func SelectInterfaces(chosen []catalog.Chain) (map[string][]string, bool) {
	ui.Clear() // fresh screen — the picker's alt-screen has just closed
	sel := map[string][]string{}
	ptrs := map[string]*[]string{}
	var groups []*huh.Group

	for i := range chosen {
		c := chosen[i]
		if len(c.Interfaces) <= 1 {
			sel[c.Index] = append([]string{}, c.Interfaces...)
			continue
		}
		dflt := append([]string{}, c.Interfaces...) // default: all selected
		ptrs[c.Index] = &dflt
		opts := make([]huh.Option[string], 0, len(c.Interfaces))
		for _, iface := range c.Interfaces {
			opts = append(opts, huh.NewOption(iface, iface).Selected(true))
		}
		groups = append(groups, huh.NewGroup(
			huh.NewMultiSelect[string]().
				Title(fmt.Sprintf("%s  %s — %s", ui.FamilyTag(c), c.Index, c.Name)).
				Description(addonDesc(c)).
				Options(opts...).
				Validate(nonEmpty).
				Value(ptrs[c.Index]),
		))
	}

	if len(groups) == 0 {
		return sel, true // every chain single-interface
	}

	if RunForm(groups...) == Back {
		return nil, false // Esc → back to the chain picker
	}
	for idx, p := range ptrs {
		sel[idx] = *p
	}
	return sel, true
}

func addonDesc(c catalog.Chain) string {
	if len(c.Addons) == 0 {
		return "no addons"
	}
	return "addons: " + strings.Join(c.Addons, ", ")
}

func nonEmpty(v []string) error {
	if len(v) == 0 {
		return fmt.Errorf("pick at least one interface")
	}
	return nil
}
