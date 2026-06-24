package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/magma-Devs/smart-router/tools/wizard/internal/catalog"
	"github.com/magma-Devs/smart-router/tools/wizard/internal/classify"
)

// visible lightens a hex color until it clears a brightness floor, so no icon
// renders near-invisible against the dark terminal background.
func visible(c lipgloss.Color) lipgloss.Color {
	var r, g, b int
	if _, err := fmt.Sscanf(string(c), "#%02x%02x%02x", &r, &g, &b); err != nil {
		return c
	}
	// perceived luminance; floor at ~120/255
	lum := (r*299 + g*587 + b*114) / 1000
	const floor = 120
	if lum >= floor {
		return c
	}
	// scale up toward white, preserving hue
	boost := func(v int) int {
		v = v + (floor - lum) + 40
		if v > 255 {
			v = 255
		}
		return v
	}
	return lipgloss.Color(fmt.Sprintf("#%02X%02X%02X", boost(r), boost(g), boost(b)))
}

// Per-chain marks: when a terminal can't render raster logos (no Kitty/iTerm
// graphics), a single family glyph repeated across 40+ EVM chains reads as a
// placeholder. A distinct Unicode mark + brand color per chain gives real
// visual identity that works in ANY terminal. Matched by name keyword (most
// specific first); unmatched chains fall back to the family glyph.
type chainMark struct {
	key    string // lowercase substring matched against the chain name
	symbol string
	color  lipgloss.Color
}

// All symbols are restricted to RELIABLY SINGLE-WIDTH glyphs (Latin letters,
// currency, and U+25xx geometric shapes). Emoji-presentation chars (atom ⚛,
// the U+2600–27BF & U+2B00 dingbat/symbol ranges) render double-width in many
// terminals and break column alignment — they're banned here.
var chainMarks = []chainMark{
	{"ethereum", "Ξ", lipgloss.Color("#8A92B2")},
	{"arbitrum", "◤", lipgloss.Color("#28A0F0")},
	{"optimism", "◓", lipgloss.Color("#FF0420")},
	{"base", "◖", lipgloss.Color("#0052FF")},
	{"blast", "◇", lipgloss.Color("#FCFC03")},
	{"scroll", "▤", lipgloss.Color("#FFEEDA")},
	{"mantle", "◧", lipgloss.Color("#65B3AE")},
	{"polygon", "○", lipgloss.Color("#8247E5")},
	{"avalanche", "▲", lipgloss.Color("#E84142")},
	{"avax", "▲", lipgloss.Color("#E84142")},
	{"bsc", "◆", lipgloss.Color("#F0B90B")},
	{"fantom", "◍", lipgloss.Color("#1969FF")},
	{"celo", "○", lipgloss.Color("#FCFF52")},
	{"berachain", "◔", lipgloss.Color("#A86A3D")},
	{"bera", "◔", lipgloss.Color("#A86A3D")},
	{"fuse", "ϟ", lipgloss.Color("#B4F9BC")},
	{"evmos", "≋", lipgloss.Color("#ED4E33")},
	{"canto", "◐", lipgloss.Color("#06FC99")},
	{"bitcoin cash", "Ƀ", lipgloss.Color("#0AC18E")},
	{"bitcoin", "₿", lipgloss.Color("#F7931A")},
	{"litecoin", "Ł", lipgloss.Color("#BFBBBB")},
	{"dogecoin", "Ð", lipgloss.Color("#C2A633")},
	{"cosmos", "◉", lipgloss.Color("#6C7BFF")},
	{"osmosis", "◑", lipgloss.Color("#A24DFF")},
	{"celestia", "◒", lipgloss.Color("#9B5BF9")},
	{"axelar", "◈", lipgloss.Color("#E8E8E8")},
	{"juno", "◉", lipgloss.Color("#F0827D")},
	{"agoric", "◬", lipgloss.Color("#E04D63")},
	{"secret", "▦", lipgloss.Color("#9AA0A6")},
	{"namada", "◴", lipgloss.Color("#FFFF66")},
	{"lava", "▰", Brand},
	{"solana", "◎", lipgloss.Color("#14F195")},
	{"aptos", "◌", lipgloss.Color("#E8E8E8")},
	{"sui", "◐", lipgloss.Color("#4DA2FF")},
	{"near", "Ⓝ", lipgloss.Color("#E8E8E8")},
	{"starknet", "◭", lipgloss.Color("#EC796B")},
	{"stellar", "▵", lipgloss.Color("#FDDA24")},
	{"ripple", "◅", lipgloss.Color("#7C8B96")},
	{"cardano", "₳", lipgloss.Color("#5C7CFF")},
	{"tezos", "ꜩ", lipgloss.Color("#2C7DF7")},
	{"ton", "◈", lipgloss.Color("#0098EA")},
	{"tron", "◣", lipgloss.Color("#EC0928")},
	{"filecoin", "◈", lipgloss.Color("#0090FF")},
	{"hedera", "ℏ", lipgloss.Color("#E8E8E8")},
	{"iota", "◆", lipgloss.Color("#25DAC5")},
	{"casper", "◍", lipgloss.Color("#FF4D5E")},
	{"fuel", "◣", lipgloss.Color("#00F58C")},
	{"beacon", "◆", lipgloss.Color("#8AA0EE")},
}

// derivedShapes — RELIABLY single-width glyphs only (U+25xx geometric range),
// deterministically assigned per chain so unknown chains get distinct marks
// without breaking alignment.
var derivedShapes = []string{
	"●", "◆", "■", "▲", "▼", "◢", "◣", "◤", "◥", "◧", "◨", "◩",
	"◪", "◫", "◬", "◭", "◮", "◰", "◱", "◲", "◳", "◔", "◕", "◵",
	"◶", "◷", "◸", "◹", "◺", "◌", "◍", "◉", "◎", "◐", "◑", "▰",
}

// fnv32 hashes a string deterministically (no Math/rand, which is unavailable).
func fnv32(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// ChainMark renders a per-chain colored symbol (trailing space included). Known
// chains get their brand mark; unknown ones get a deterministic distinct shape
// (family-tinted) so the list never shows the same glyph repeated down rows.
func ChainMark(c catalog.Chain) string {
	name := strings.ToLower(c.Name)
	for _, m := range chainMarks {
		if strings.Contains(name, m.key) {
			return lipgloss.NewStyle().Foreground(visible(m.color)).Bold(true).Render(m.symbol) + " "
		}
	}
	// Deterministic fallback: distinct shape per chain, tinted by family.
	// Hash on the chain's display name with network words removed, so a chain's
	// mainnet and testnet variants share the same icon (Worldchain Mainnet ==
	// Worldchain Sepolia Testnet) while distinct chains stay distinct.
	shape := derivedShapes[fnv32(baseName(c.Name))%uint32(len(derivedShapes))]
	color := visible(familyColor(classify.FamilyOf(c)))
	return lipgloss.NewStyle().Foreground(color).Bold(true).Render(shape) + " "
}

// baseName lowercases a chain's display name and removes network/variant words
// ("mainnet", "testnet", "sepolia", …) so variants of one chain collapse to the
// same key. Far more reliable than guessing index suffixes.
func baseName(name string) string {
	drop := map[string]bool{
		"mainnet": true, "testnet": true, "devnet": true, "sepolia": true,
		"holesky": true, "goerli": true, "preprod": true, "preview": true,
		"arabica": true, "mocha": true, "artio": true, "bartio": true,
		"nova": true, "shadownet": true, "net": true, "main": true, "test": true,
	}
	var keep []string
	for _, w := range strings.Fields(strings.ToLower(name)) {
		if !drop[w] {
			keep = append(keep, w)
		}
	}
	if len(keep) == 0 {
		return strings.ToLower(name)
	}
	return strings.Join(keep, " ")
}
