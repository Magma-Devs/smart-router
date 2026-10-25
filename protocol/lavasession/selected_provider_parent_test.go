package lavasession

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// splitPairingList builds the pairing the router builds since MAG-4219: one entity per url, named
// "<provider>/<index>", carrying the configured provider's name as Parent.
func splitPairingList(entities map[string]string) map[uint64]*ConsumerSessionsWithProvider {
	names := make([]string, 0, len(entities))
	for name := range entities {
		names = append(names, name)
	}
	list := createNamedPairingList(names...)
	for _, cswp := range list {
		cswp.Parent = entities[cswp.PublicLavaAddress]
	}
	return list
}

// MAG-4219. A customer pins with the configured provider name. The router now serves that
// provider through entities named "<provider>/<index>", so the header must keep meaning "any url
// of this provider", while a header naming an entity pins that url alone.
func TestSelectedProvider_ParentNameSelectsAmongItsURLs(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, splitPairingList(map[string]string{
		"chainstack/0": "chainstack",
		"chainstack/1": "chainstack",
		"tatum/0":      "tatum",
	}), nil))
	time.Sleep(5 * time.Millisecond) // let probes finish
	ctx := context.Background()

	t.Run("the configured name resolves to one of its urls", func(t *testing.T) {
		for range 20 {
			got, err := csm.getValidProviderAddresses(ctx, 1, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "chainstack")
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.True(t, strings.HasPrefix(got[0], "chainstack/"), "got %q", got[0])
		}
	})

	t.Run("the configured name, case-folded", func(t *testing.T) {
		got, err := csm.getValidProviderAddresses(ctx, 1, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "CHAINSTACK")
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.True(t, strings.HasPrefix(got[0], "chainstack/"), "got %q", got[0])
	})

	t.Run("an entity name pins that url alone", func(t *testing.T) {
		got, err := csm.getValidProviderAddresses(ctx, 1, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "chainstack/1")
		require.NoError(t, err)
		require.Equal(t, []string{"chainstack/1"}, got)
	})

	t.Run("a sibling already set aside in this request is not handed back", func(t *testing.T) {
		ignored := map[string]struct{}{"chainstack/0": {}}
		for range 20 {
			got, err := csm.getValidProviderAddresses(ctx, 1, ignored, 10, 100, "", nil, common.NO_STATE, "", "chainstack")
			require.NoError(t, err)
			require.Equal(t, []string{"chainstack/1"}, got)
		}
	})

	t.Run("every url set aside ends the request instead of spinning", func(t *testing.T) {
		ignored := map[string]struct{}{"chainstack/0": {}, "chainstack/1": {}}
		_, err := csm.getValidProviderAddresses(ctx, 1, ignored, 10, 100, "", nil, common.NO_STATE, "", "chainstack")
		require.Error(t, err)
		require.True(t, errors.Is(err, PairingListEmptyError), "got %v", err)
	})

	t.Run("a name that is neither a provider nor an entity is unavailable", func(t *testing.T) {
		_, err := csm.getValidProviderAddresses(ctx, 1, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "alchemy")
		require.Error(t, err)
		require.True(t, errors.Is(err, SelectedProviderUnavailableError), "got %v", err)
	})

	t.Run("the pin never leaves the family", func(t *testing.T) {
		// tatum is the only other provider; a parent pin on chainstack must not reach it even
		// when one sibling is set aside.
		ignored := map[string]struct{}{"chainstack/1": {}}
		got, err := csm.getValidProviderAddresses(ctx, 1, ignored, 10, 100, "", nil, common.NO_STATE, "", "chainstack")
		require.NoError(t, err)
		require.Equal(t, []string{"chainstack/0"}, got)
	})
}

// Two configured providers whose names differ only in case, and a header matching neither
// exactly: the collision is reported, as it is for entity names (MAG-3881 rule).
func TestSelectedProvider_ParentCaseTwinsAreAmbiguous(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, splitPairingList(map[string]string{
		"chainstack/0": "chainstack",
		"Chainstack/0": "Chainstack",
	}), nil))
	time.Sleep(5 * time.Millisecond)
	ctx := context.Background()

	_, err := csm.getValidProviderAddresses(ctx, 1, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "CHAINSTACK")
	require.Error(t, err)
	require.True(t, errors.Is(err, SelectedProviderUnavailableError), "got %v", err)

	got, err := csm.getValidProviderAddresses(ctx, 1, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "Chainstack")
	require.NoError(t, err)
	require.Equal(t, []string{"Chainstack/0"}, got, "an exact parent match wins over the fold")
}

func TestAddressesWithParent(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, splitPairingList(map[string]string{
		"chainstack/0": "chainstack",
		"chainstack/1": "chainstack",
		"legacy":       "", // a record not built by the split never matches a parent
	}), nil))
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	addresses := []string{"chainstack/0", "chainstack/1", "legacy"}

	siblings, ambiguous := csm.addressesWithParent("chainstack", addresses)
	require.ElementsMatch(t, []string{"chainstack/0", "chainstack/1"}, siblings)
	require.Empty(t, ambiguous)

	siblings, ambiguous = csm.addressesWithParent("legacy", addresses)
	require.Empty(t, siblings)
	require.Empty(t, ambiguous)

	// Only the addresses offered are considered: a blocked sibling is not in validAddresses and
	// must not come back through the parent.
	siblings, _ = csm.addressesWithParent("chainstack", []string{"chainstack/1"})
	require.Equal(t, []string{"chainstack/1"}, siblings)
}
