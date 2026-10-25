package chainlib

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-3988: GetRelayTimeout's result becomes the relay state machine's time.NewTicker interval,
// which panics on a non-positive value — in a goroutine with no recover, so the process exits. A
// non-positive override must fall back to the computed window. A positive one is used as sent when
// it lies within the caller bound (MAG-3600): even below the --min-relay-timeout floor, but never
// below common.MinCallerRelayTimeout.
func TestGetRelayTimeout_Override(t *testing.T) {
	floor := common.MinimumTimePerRelayDelay
	t.Cleanup(func() { common.MinimumTimePerRelayDelay = floor })
	common.MinimumTimePerRelayDelay = 2 * time.Second

	computed := 2 * time.Second // 10 CU x 100ms = 1s, raised to the 2s floor
	cases := []struct {
		name     string
		override time.Duration
		want     time.Duration
	}{
		{"negative falls back to the computed window", -time.Second, computed},
		{"most negative falls back too", time.Duration(-1 << 63), computed},
		{"unset uses the computed window", 0, computed},
		{"positive above the floor is verbatim", 12 * time.Second, 12 * time.Second},
		{"positive below the floor is verbatim", 500 * time.Millisecond, 500 * time.Millisecond},
		// MAG-3600 changes PR 452's rule: below the caller floor is raised to it, not used verbatim.
		{"positive below the caller floor is raised to it", 250 * time.Millisecond, common.MinCallerRelayTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := &baseChainMessageContainer{
				api:           &spectypes.Api{Name: "net_version", ComputeUnits: 10},
				apiCollection: &spectypes.ApiCollection{},
			}
			msg.TimeoutOverride(tc.override)

			got := GetRelayTimeout(msg, time.Second)
			require.Equal(t, tc.want, got)
			require.Positive(t, got, "a non-positive window panics time.NewTicker")
		})
	}
}
