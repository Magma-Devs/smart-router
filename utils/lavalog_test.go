package utils_test

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

var TestError = errors.New("error for tests")

func TestErrorTypeChecks(t *testing.T) {
	err := TestError
	newErr := utils.LavaFormatError("testing 123", err, utils.Attribute{"attribute", "test"})
	require.True(t, errors.Is(newErr, TestError))
}

// A log line carries a long request body as its head and tail around a marker. The cuts must not
// split a rune: a logged body that is not valid UTF-8 has its stray bytes rewritten by the JSON
// writer, so the line no longer shows what the client sent.
func TestFormatLongString_CutsOnRuneBoundaries(t *testing.T) {
	const marker = "...truncated..."
	a, b, c := strings.Repeat("a", 134), strings.Repeat("b", 100), strings.Repeat("c", 66)
	for i, tc := range []struct {
		name, msg string
		max       int
		want      string
	}{
		{"no budget keeps it whole", a + b + c, 0, a + b + c},
		{"within the budget", a + c, 200, a + c},
		{"ascii keeps two thirds and a third", a + b + c, 200, a + marker + c},
		// 200 bytes: the head is 134 bytes and the tail 66.
		{"3-byte rune across the head cut", a[:133] + "€" + b + c, 200, a[:133] + marker + c},
		{"3-byte rune across the tail cut", a + b + "€" + c[:64], 200, a + marker + c[:64]},
		{"4-byte rune whose last byte the head cut takes", a[:131] + "𝄞" + b + c, 200, a[:131] + marker + c},
		{"4-byte rune whose first byte the tail cut leaves", a + b + "𝄞" + c[:63], 200, a + marker + c[:63]},
	} {
		got := utils.FormatLongString(tc.msg, tc.max)
		require.Equal(t, tc.want, got, "%s, tc #%d", tc.name, i)
		require.True(t, utf8.ValidString(got), "%s, tc #%d", tc.name, i)
	}

	// Every budget, against runes of every width at every offset.
	runes := strings.Repeat("aé€𝄞", 40)
	for max := 1; max <= 300; max++ {
		got := utils.FormatLongString(runes, max)
		require.True(t, utf8.ValidString(got), "max %d: %q", max, got)
		head, tail, found := strings.Cut(got, marker)
		if !found {
			require.Equal(t, runes, got, "max %d", max)
			continue
		}
		require.True(t, strings.HasPrefix(runes, head), "max %d", max)
		require.True(t, strings.HasSuffix(runes, tail), "max %d", max)
		require.LessOrEqual(t, len(head)+len(tail), max, "max %d", max)
		require.Greater(t, len(head)+len(tail), max-2*(utf8.UTFMax-1)-1, "max %d: a cut moved past one rune", max)
	}

	// Bytes that are not UTF-8 (a gRPC body is protobuf) are cut where they were: the search for a
	// rune boundary stops after one rune's length instead of eating into the kept parts.
	binary := "a" + strings.Repeat("\x80", 299)
	require.Equal(t, binary[:134]+marker+binary[234:], utils.FormatLongString(binary, 200))
}
