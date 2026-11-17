package rpcsmartrouter

import (
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/stretchr/testify/require"
)

// TestStrategyFlag_RoundTripsEveryValue is MAG-1881 item 5.2.1: the ticket's 2026-05 audit found
// --strategy tested for only two of its then three values. The flag has since grown to seven
// (cost, privacy, accuracy and distributed were added with no matching test), so this
// parametrizes every name strategyFlag.Set currently accepts rather than re-pinning a stale list.
//
// The flag names a Strategy by its position in strategyNames, so each position is pinned against
// the optimizer's own name for that value (the flag spells sync-freshness with a dash, the
// optimizer with an underscore): with the list reordered, --strategy cost would select another
// strategy. The value one past the list must have no name: a Strategy appended after the last one
// would otherwise be unreachable from the flag.
func TestStrategyFlag_RoundTripsEveryValue(t *testing.T) {
	require.Empty(t, provideroptimizer.Strategy(len(strategyNames)).String(),
		"a provideroptimizer.Strategy added without a name in strategyNames is unreachable from the --strategy flag")

	for i, name := range strategyNames {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, strings.ReplaceAll(name, "-", "_"), provideroptimizer.Strategy(i).String(),
				"strategyNames[%d] must name the provideroptimizer.Strategy with that value", i)

			var v strategyValue
			require.NoError(t, v.Set(name))
			require.Equal(t, provideroptimizer.Strategy(i), v.Strategy)
			require.Equal(t, name, v.String(), "String must echo back what Set accepted")

			// Case-insensitive, matching cobra's usual convention for enum-shaped string flags.
			var upper strategyValue
			require.NoError(t, upper.Set(strings.ToUpper(name)))
			require.Equal(t, provideroptimizer.Strategy(i), upper.Strategy)
		})
	}
}

func TestStrategyFlag_RejectsUnknownValue(t *testing.T) {
	var v strategyValue
	err := v.Set("not-a-real-strategy")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not-a-real-strategy")
}
