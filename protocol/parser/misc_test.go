package parser

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

// CapStringLen keeps a string of up to 250 bytes whole and cuts a longer one to its first 150 and
// last 100 bytes around "...Truncated...". Its cuts land on rune boundaries, as FormatLongString's
// do: a rune the cut would split is left out, so a logged reply stays valid UTF-8.
func TestCapStringLen(t *testing.T) {
	const marker = "...Truncated..."
	a, b, c := strings.Repeat("a", 150), strings.Repeat("b", 50), strings.Repeat("c", 100)
	for _, tc := range []struct {
		name, in, want string
	}{
		{"short", "abc", "abc"},
		{"250 bytes", a + strings.Repeat("b", 100), a + strings.Repeat("b", 100)},
		{"251 bytes", a + "x" + c, a + marker + c},
		{"ascii", a + b + c, a + marker + c},
		{"3-byte rune across the head cut", a[:149] + "€" + b + c, a[:149] + marker + c},
		{"3-byte rune across the tail cut", a + b + "€" + c[:98], a + marker + c[:98]},
		{"4-byte rune across the head cut", a[:148] + "𝄞" + b + c, a[:148] + marker + c},
		{"4-byte rune across the tail cut", a + b + "𝄞" + c[:97], a + marker + c[:97]},
	} {
		got := CapStringLen(tc.in)
		require.Equal(t, tc.want, got, tc.name)
		require.True(t, utf8.ValidString(got), tc.name)
	}

	// Every length, against runes of every width at every offset.
	runes := strings.Repeat("aé€𝄞", 80)
	for n := 1; n <= len(runes); n++ {
		in := runes[:n]
		if !utf8.ValidString(in) {
			continue
		}
		got := CapStringLen(in)
		require.True(t, utf8.ValidString(got), "length %d: %q", n, got)
	}
}
