package parser

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/utils"
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

// A string the cap cuts is redacted before the cut (MAG-5698). The log layer replaces a
// registered value whole, so a value the cut had split no longer matched it, and the part the
// cut kept was written. A registered value that straddles either cut, or sits whole in the part
// the cut drops, leaves no fragment behind.
func TestCapStringLen_RedactsBeforeItCuts(t *testing.T) {
	const registered = "MAG5698CapValue0123456789abcdefghij" // 35 bytes
	utils.RegisterSecrets(registered)
	for _, tc := range []struct {
		name, in string
		// marked: the mark lands in a part the cut keeps. A value the cut drops whole leaves none.
		marked bool
	}{
		{"across the head cut", strings.Repeat("a", 140) + registered + strings.Repeat("b", 120), true},
		{"across the tail cut", strings.Repeat("a", 160) + registered + strings.Repeat("b", 90), true},
		{"in the part the cut drops", strings.Repeat("a", 155) + registered + strings.Repeat("b", 150), false},
		{"first, before a long tail", registered + strings.Repeat("b", 250), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Greater(t, len(tc.in), 250, "a string the cap cuts")
			got := CapStringLen(tc.in)
			require.Equal(t, tc.marked, strings.Contains(got, utils.RedactedMark), got)
			for i := 0; i+8 <= len(registered); i++ {
				require.NotContains(t, got, registered[i:i+8], "a fragment of the value survived the cut: %q", got)
			}
		})
	}
}
