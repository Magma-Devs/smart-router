// Package ui holds the wizard's visual language: the brand palette, the ASCII
// logo, reusable lipgloss styles (headers, panels, badges, key hints), and a
// matching huh form theme so every screen looks like one product.
package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Brand palette — anchored on Lava Connect brand red #FF3900, with a warm
// ember gradient for the logo and muted neutrals for body text.
var (
	Brand   = lipgloss.Color("#FF3900") // primary brand red
	Ember1  = lipgloss.Color("#FF6A00")
	Ember2  = lipgloss.Color("#FF9100")
	Ember3  = lipgloss.Color("#FFB347")
	Ink     = lipgloss.Color("#F5F5F4") // near-white body
	Muted   = lipgloss.Color("#A1A1AA") // secondary text
	Faint   = lipgloss.Color("#52525B") // hints, separators
	Good    = lipgloss.Color("#22C55E")
	Warn    = lipgloss.Color("#FBBF24")
	Bad     = lipgloss.Color("#EF4444")
	SurfBg  = lipgloss.Color("#1C1917")
	PanelBg = lipgloss.Color("#0C0A09")
)

// Reusable styles.
var (
	Title = lipgloss.NewStyle().Bold(true).Foreground(Brand)

	Subtle = lipgloss.NewStyle().Foreground(Muted)

	Hint = lipgloss.NewStyle().Foreground(Faint)

	// SectionHeader — "▎ 1 · Supported chains" rule above each step.
	SectionBar = lipgloss.NewStyle().Foreground(Brand).Bold(true)

	OK   = lipgloss.NewStyle().Foreground(Good)
	Wn   = lipgloss.NewStyle().Foreground(Warn)
	Er   = lipgloss.NewStyle().Foreground(Bad)
	Accent = lipgloss.NewStyle().Foreground(Ember1).Bold(true)

	// Comment — orange (not bold) for the "# …" annotation lines above commands.
	Comment = lipgloss.NewStyle().Foreground(Ember2)

	// Panel — a rounded, brand-bordered explanatory box. Generous horizontal
	// padding (more on the right) so copy never crowds the border.
	Panel = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(Brand).
		Padding(1, 4, 1, 3). // top, right, bottom, left
		Foreground(Ink)

	// Badge — small inverse chip, e.g. a family tag.
	Badge = lipgloss.NewStyle().
		Foreground(PanelBg).
		Background(Ember2).
		Bold(true).
		Padding(0, 1)
)

// Clear resets the terminal to a clean top-of-screen state (used between
// screens so stale output never lingers).
func Clear() { fmt.Print("\x1b[2J\x1b[3J\x1b[H") }

// RuleTop draws the TOP of an open box with an inline label, e.g.
//   ┌── run command ───────────────────────────────┐
// It's a box (corners + top border) but has NO vertical side walls on the
// content lines below — so copy-pasting any command line grabs no box chars.
func RuleTop(label string, width int) string {
	bar := lipgloss.NewStyle().Foreground(Brand)
	lead := "┌── "
	tail := " "
	used := lipgloss.Width(lead) + lipgloss.Width(label) + lipgloss.Width(tail) + 1 // +1 for ┐
	pad := width - used
	if pad < 0 {
		pad = 0
	}
	return bar.Render(lead) + Title.Render(label) + bar.Render(tail+strings.Repeat("─", pad)+"┐")
}

// RuleBottom draws the BOTTOM of the open box (corners + bottom border).
func RuleBottom(width int) string {
	bar := lipgloss.NewStyle().Foreground(Brand)
	pad := width - 2
	if pad < 0 {
		pad = 0
	}
	return bar.Render("└" + strings.Repeat("─", pad) + "┘")
}

// Section renders a numbered step header like "  ▎ 2 · RPC endpoints".
func Section(n int, label string) string {
	bar := SectionBar.Render("▎")
	num := lipgloss.NewStyle().Foreground(Ember2).Bold(true).Render(itoa(n))
	return "\n" + bar + " " + num + " " + Hint.Render("·") + " " + Title.Render(label)
}

// Subsection renders an INDENTED sub-step header beneath a Section, with a dotted
// number like "2.1" — e.g. "    ▎ 2.1 · websocket". It signals that the step
// belongs to (is nested under) the preceding Section. The number is a string so
// callers control the dotted form (2.1, 2.2, …); the bar is dimmed relative to a
// top-level Section so the hierarchy reads at a glance.
func Subsection(num, label string) string {
	bar := lipgloss.NewStyle().Foreground(Ember2).Render("▎")
	n := lipgloss.NewStyle().Foreground(Ember3).Bold(true).Render(num)
	return "\n  " + bar + " " + n + " " + Hint.Render("·") + " " + Subtle.Render(label)
}

// Alert renders a loud, attention-grabbing red callout box (for failures the
// user must not miss, e.g. a non-verifying endpoint).
func Alert(title, body string) string {
	const w = 72
	inner := lipgloss.NewStyle().
		Border(lipgloss.ThickBorder()).
		BorderForeground(Bad).
		Background(lipgloss.Color("#2A0A0A")).
		Foreground(Ink).
		Padding(0, 3).
		Width(w).
		Bold(true)
	head := lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFFFF")).Background(Bad).Bold(true).
		Padding(0, 1).Render(" " + Cross + " " + title + " ")
	if body == "" {
		return inner.Render(head)
	}
	// hard-wrap the body so a long error never blows the box wide
	body = lipgloss.NewStyle().Width(w - 6).Render(body)
	return inner.Render(head + "\n" + lipgloss.NewStyle().Foreground(Bad).Render(body))
}

// Prompt renders an attention prompt prefix — a brand chevron + bold question,
// so interactive questions stand out from status lines.
func Prompt(q string) string {
	chev := lipgloss.NewStyle().Foreground(Brand).Bold(true).Render("❯ ")
	return chev + lipgloss.NewStyle().Foreground(Ink).Bold(true).Render(q)
}

// KeyHint renders a footer like "↑/↓ move · / filter · space toggle · enter ok".
func KeyHint(pairs ...[2]string) string {
	var parts []string
	keyS := lipgloss.NewStyle().Foreground(Ember2).Bold(true)
	for _, p := range pairs {
		parts = append(parts, keyS.Render(p[0])+" "+Hint.Render(p[1]))
	}
	return Hint.Render(strings.Join(parts, "  ·  "))
}

// Glyphs. We use the TEXT-presentation check/cross (U+2713 / U+2717), NOT the
// heavy emoji forms (U+2714 ✔ / U+2718 ✘) — the heavy ones default to emoji
// presentation in many terminals, which colors them itself (ignoring our ANSI
// green/red) and renders them double-width. The text forms respect color and
// stay single-width.
const (
	Check = "✓" // U+2713 CHECK MARK (text)
	Cross = "✗" // U+2717 BALLOT X (text)
	Dot   = "•"
	Arrow = "→"
)

// Tick renders a green "✔" then a SPACE (outside the style, so terminals don't
// trim styled trailing whitespace) then msg.
func Tick(msg string) string { return OK.Render(Check) + " " + msg }

// XMark renders a red "✘ " followed by msg.
func XMark(msg string) string { return Er.Render(Cross) + " " + msg }

// Mark renders the green "✔ " marker (space outside the style).
func Mark() string { return OK.Render(Check) + " " }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}
