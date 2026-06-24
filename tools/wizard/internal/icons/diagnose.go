package icons

import (
	"fmt"
	"io"
	"os"
)

// Diagnose prints what terminal we're in and whether real raster chain logos
// are achievable here (Kitty graphics protocol), plus the env signals used.
func Diagnose(w io.Writer) {
	fmt.Fprintln(w, "smart-router wizard — terminal icon diagnostic")
	fmt.Fprintln(w, "----------------------------------------------")

	envs := []string{
		"TERM", "TERM_PROGRAM", "TERM_PROGRAM_VERSION",
		"KITTY_WINDOW_ID", "GHOSTTY_RESOURCES_DIR", "GHOSTTY_BIN_DIR",
		"WEZTERM_PANE", "WEZTERM_EXECUTABLE",
		"ITERM_SESSION_ID", "LC_TERMINAL", "COLORTERM", "WIZARD_ICONS", "NO_COLOR",
	}
	fmt.Fprintln(w, "\nenvironment:")
	for _, e := range envs {
		if v := os.Getenv(e); v != "" {
			fmt.Fprintf(w, "  %-22s = %s\n", e, v)
		}
	}

	mode := Detect()
	modeName := map[Mode]string{
		ModeGlyph: "glyph / per-chain marks (no inline images)",
		ModeKitty: "kitty graphics",
		ModeITerm: "iterm inline images",
		ModeSixel: "sixel",
	}[mode]
	fmt.Fprintf(w, "\ndetected mode: %s\n", modeName)

	kitty, sixel := queryGraphics()
	fmt.Fprintf(w, "active kitty-graphics query: %v\n", kitty)
	fmt.Fprintf(w, "active sixel (DA1) query:    %v\n", sixel)

	fmt.Fprintln(w, "\nverdict:")
	switch {
	case mode == ModeKitty || kitty:
		fmt.Fprintln(w, "  ✓ Kitty graphics supported — real chain-logo images render in the picker.")
	case mode == ModeSixel || sixel:
		fmt.Fprintln(w, "  ✓ Sixel supported — real chain-logo images render (Windows Terminal / xterm).")
	default:
		fmt.Fprintln(w, "  • No inline-image protocol detected (Kitty / Sixel / iTerm).")
		fmt.Fprintln(w, "    The wizard uses distinct per-chain colored marks (Ξ ◤ ▲ ₿ ◇ ◎ …).")
		fmt.Fprintln(w, "    For real logos, run in WezTerm or a recent Windows Terminal — NOT the")
		fmt.Fprintln(w, "    VS Code terminal (xterm.js has no image support), and not via tmux/ssh.")
	}
}
