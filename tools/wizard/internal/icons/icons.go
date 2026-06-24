// Package icons renders per-chain icons in the terminal.
//
// Source of truth is the docs site's published SVGs
// (http://docs.magmadevs.com/assets/chains/<slug>.svg) — referenced by URL and
// cached locally at runtime; NEVER vendored into the repo. The spec-file →
// docs-slug map comes from the docs' chains-data.js (also fetched, with a
// filename-derived fallback).
//
// Rendering: terminals that speak the Kitty graphics protocol (Kitty, Ghostty,
// recent WezTerm) or iTerm2's inline-image protocol show the real rasterized
// SVG; everywhere else falls back to a family glyph. Detection is conservative
// — when unsure, glyphs.
package icons

import (
	"os"
	"strings"
)

// Mode is the resolved rendering capability of the current terminal.
type Mode int

const (
	ModeGlyph Mode = iota // unicode fallback (always works)
	ModeKitty             // kitty graphics protocol (per-row via placeholders)
	ModeITerm             // iTerm2 inline images
	ModeSixel             // sixel (side-panel preview; can't go per-row in a TUI)
)

// Detect resolves the terminal's image capability. It honors an explicit
// override (WIZARD_ICONS=glyph|kitty|iterm), then checks env signals, then —
// when inconclusive — actively QUERIES the terminal for Kitty graphics support.
// The active probe is what makes detection env-independent (works even when no
// identifying env var is set).
func Detect() Mode {
	switch strings.ToLower(os.Getenv("WIZARD_ICONS")) {
	case "glyph", "off", "none":
		return ModeGlyph
	case "kitty":
		return ModeKitty
	case "iterm":
		return ModeITerm
	case "sixel":
		return ModeSixel
	}
	if os.Getenv("NO_COLOR") != "" {
		return ModeGlyph
	}

	// Strong env signals first (no round-trip needed).
	term := os.Getenv("TERM")
	switch {
	case os.Getenv("KITTY_WINDOW_ID") != "",
		os.Getenv("GHOSTTY_RESOURCES_DIR") != "", os.Getenv("GHOSTTY_BIN_DIR") != "",
		strings.Contains(term, "kitty"), strings.Contains(term, "ghostty"):
		return ModeKitty
	}
	switch os.Getenv("TERM_PROGRAM") {
	case "WezTerm":
		return ModeKitty // WezTerm speaks the kitty protocol
	case "iTerm.app":
		return ModeITerm
	case "ghostty":
		return ModeKitty
	}
	if os.Getenv("WEZTERM_PANE") != "" || os.Getenv("WEZTERM_EXECUTABLE") != "" {
		return ModeKitty
	}
	if os.Getenv("ITERM_SESSION_ID") != "" || os.Getenv("LC_TERMINAL") == "iTerm2" {
		return ModeITerm
	}

	// Windows Terminal advertises itself; it supports Sixel (recent builds).
	if os.Getenv("WT_SESSION") != "" {
		return ModeSixel
	}

	// Inconclusive — ask the terminal directly. One round-trip gets us both:
	// the Kitty graphics reply AND the DA1 attributes (sixel = capability "4").
	kitty, sixel := queryGraphics()
	switch {
	case kitty:
		return ModeKitty
	case sixel:
		return ModeSixel
	}
	return ModeGlyph
}

// FamilyGlyph is the fallback glyph for a family name (all single-width).
func FamilyGlyph(family string) string {
	switch family {
	case "EVM":
		return "◆"
	case "Cosmos":
		return "◇"
	case "BTC":
		return "₿"
	case "Other-L1":
		return "◈"
	default:
		return "●"
	}
}
