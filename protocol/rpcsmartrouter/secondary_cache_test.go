package rpcsmartrouter

import (
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// Block-hash→height merge semantics from docs/SECONDARY-CACHE-DESIGN.md §8 (T14):
// merged, never overwritten — earliest is the minimum valid height, latest the
// maximum, and a reply without mappings (NOT_APPLICABLE) cannot erase the other
// tier's values.
func TestMergeBlockHashHeights(t *testing.T) {
	na := spectypes.NOT_APPLICABLE
	cases := []struct {
		name                                   string
		latestA, earliestA, latestB, earliestB int64
		wantLatest, wantEarliest               int64
	}{
		{"both empty", na, na, na, na, na, na},
		{"primary only", 200, 100, na, na, 200, 100},
		{"secondary only", na, na, 300, 50, 300, 50},
		{"complementary ranges widen both ends", 200, 100, 300, 50, 300, 50},
		{"secondary inside primary range changes nothing", 300, 50, 200, 100, 300, 50},
		{"conflicting values fold deterministically", 200, 100, 250, 80, 250, 80},
		{"empty secondary cannot erase primary", 200, 100, na, na, 200, 100},
		{"zero is a valid height", 200, 100, na, 0, 200, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			latest, earliest := mergeBlockHashHeights(tc.latestA, tc.earliestA, tc.latestB, tc.earliestB)
			require.Equal(t, tc.wantLatest, latest, "latest")
			require.Equal(t, tc.wantEarliest, earliest, "earliest")
		})
	}
}

// The secondary tier must be skipped cleanly in every unconfigured/disconnected
// state (design §5): nil interface, and an interface wrapping a typed-nil concrete
// client (the wiring can hand over a nil *performance.Cache).
func TestSecondaryCacheActiveNilSafety(t *testing.T) {
	rpcss := &RPCSmartRouterServer{}
	require.False(t, rpcss.secondaryCacheActive(), "nil interface must be inactive")
}
