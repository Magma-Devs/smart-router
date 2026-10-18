package utils

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A log reader turns a GUID into a time window by shifting it, so the top 32 bits must be the
// second it was minted.
func TestGenerateUniqueIdentifier_DecodesToItsSecond(t *testing.T) {
	before := time.Now().Unix()
	guid := GenerateUniqueIdentifier()
	after := time.Now().Unix()

	minted := int64(guid >> 32)
	require.GreaterOrEqual(t, minted, before)
	require.LessOrEqual(t, minted, after)

	// Still a decimal uint64 on the wire, the shape every log line and header already carries.
	parsed, err := strconv.ParseUint(strconv.FormatUint(guid, 10), 10, 64)
	require.NoError(t, err)
	require.Equal(t, guid, parsed)
}

// GUIDs minted in the same second differ only in the low 32 bits, so those bits must be random.
func TestGenerateUniqueIdentifier_LowBitsVary(t *testing.T) {
	const draws = 1000
	seen := make(map[uint32]struct{}, draws)
	for range draws {
		seen[uint32(GenerateUniqueIdentifier())] = struct{}{}
	}
	// 1000 draws from 2^32 values collide about once in 8,600 runs; a constant or a counter
	// stuck on a few values cannot get near this bar.
	require.Greater(t, len(seen), draws-10)
}
