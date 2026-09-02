package endpointstate

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// TestCollectionAddonsIsSortedAndStable covers the input the head poll and the
// fork check hand to the collection-scoped directive lookup (MAG-3296).
//
// Endpoint.Addons is a map, so ranging it directly would order differently per
// call. Where a node declares two add-ons that both carry GET_BLOCKNUM, that
// would let consecutive polls of the SAME endpoint read the head from different
// collections — which surfaces as an endpoint whose tip jumps rather than as a
// configuration problem anyone could see.
func TestCollectionAddonsIsSortedAndStable(t *testing.T) {
	poller := &EndpointPoller{
		endpoint: &lavasession.Endpoint{
			Addons: map[string]struct{}{
				"evm":     {},
				"archive": {},
				"txpool":  {},
			},
		},
	}

	want := []string{"archive", "evm", "txpool"}
	for i := 0; i < 200; i++ {
		require.Equal(t, want, poller.collectionAddons())
	}
}

func TestCollectionAddonsEmptyCases(t *testing.T) {
	t.Run("no addons", func(t *testing.T) {
		poller := &EndpointPoller{endpoint: &lavasession.Endpoint{}}
		require.Nil(t, poller.collectionAddons(),
			"an empty set must resolve to the base collection, not to no collection")
	})

	t.Run("nil endpoint", func(t *testing.T) {
		poller := &EndpointPoller{}
		require.Nil(t, poller.collectionAddons())
	})
}
