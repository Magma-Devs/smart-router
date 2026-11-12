package provideroptimizer

import (
	"testing"
	"time"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-2786: KnownBelowBlock is the check that keeps a request for a block off a provider known not
// to hold it. It only ever answers from an observed block, so it never turns away a provider it
// knows nothing about.
func TestKnownBelowBlock(t *testing.T) {
	const (
		observed = 1000
		provider = "provider"
	)
	po := setupProviderOptimizer(1)

	require.False(t, po.KnownBelowBlock(provider, observed+1), "a provider never observed is not known to be behind")

	// A relay sample, read straight back with no wait: the floor is written before the cache is, so
	// a sample recorded a moment ago already counts.
	po.AppendRelayData(provider, time.Millisecond, 1, observed)

	for _, tc := range []struct {
		name  string
		block int64
		below bool
	}{
		{name: "a block above the one observed", block: observed + 1, below: true},
		{name: "the block observed", block: observed},
		{name: "a block below the one observed", block: observed - 1},
		{name: "genesis", block: 0},
		{name: "latest", block: spectypes.LATEST_BLOCK},
		{name: "earliest", block: spectypes.EARLIEST_BLOCK},
		{name: "pending", block: spectypes.PENDING_BLOCK},
		{name: "safe", block: spectypes.SAFE_BLOCK},
		{name: "finalized", block: spectypes.FINALIZED_BLOCK},
		{name: "not applicable", block: spectypes.NOT_APPLICABLE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.below, po.KnownBelowBlock(provider, tc.block))
		})
	}

	t.Run("the floor never moves back", func(t *testing.T) {
		// A later, lower observation leaves the provider where it was seen highest, as every
		// reader of SyncBlock sees it.
		po.AppendSyncData(provider, observed-1, SyncReference{})
		require.False(t, po.KnownBelowBlock(provider, observed))
	})
}
