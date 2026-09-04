package redisstore

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/stretchr/testify/require"
)

func TestSticky_FirstWriterWins(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	won, err := store.SetStickyIfAbsent(ctx, "sticky:ETH1:jsonrpc:id", core.StickyPin{Provider: "node-a", Epoch: 3}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "node-a", won.Provider)
	require.EqualValues(t, 3, won.Epoch)

	// The losing claimant is handed the winner, which is what lets it adopt instead of split.
	lost, err := store.SetStickyIfAbsent(ctx, "sticky:ETH1:jsonrpc:id", core.StickyPin{Provider: "node-b", Epoch: 4}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "node-a", lost.Provider)
	require.EqualValues(t, 3, lost.Epoch)
}

func TestSticky_RoundTripAndMiss(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	_, found, err := store.GetSticky(ctx, "sticky:ETH1:jsonrpc:absent")
	require.NoError(t, err, "an unclaimed session is a miss, not an error")
	require.False(t, found)

	_, err = store.SetStickyIfAbsent(ctx, "sticky:ETH1:jsonrpc:id", core.StickyPin{Provider: "node-a", Epoch: 9}, time.Minute)
	require.NoError(t, err)

	pin, found, err := store.GetSticky(ctx, "sticky:ETH1:jsonrpc:id")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "node-a", pin.Provider)
	require.EqualValues(t, 9, pin.Epoch)
}

// A provider name may contain any character a config allows, including the separators a
// delimiter-joined encoding would use.
func TestSticky_ProviderNameWithSeparators(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	name := `node:a|b"c,{}`

	_, err := store.SetStickyIfAbsent(ctx, "sticky:k", core.StickyPin{Provider: name, Epoch: 1}, time.Minute)
	require.NoError(t, err)

	pin, found, err := store.GetSticky(ctx, "sticky:k")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, name, pin.Provider)
}

func TestSticky_ClaimExpires(t *testing.T) {
	store, mr := newTestStore(t)
	ctx := context.Background()

	_, err := store.SetStickyIfAbsent(ctx, "sticky:k", core.StickyPin{Provider: "node-a", Epoch: 1}, time.Minute)
	require.NoError(t, err)

	mr.FastForward(2 * time.Minute)

	_, found, err := store.GetSticky(ctx, "sticky:k")
	require.NoError(t, err)
	require.False(t, found)

	won, err := store.SetStickyIfAbsent(ctx, "sticky:k", core.StickyPin{Provider: "node-b", Epoch: 2}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "node-b", won.Provider, "an expired claim must not block a new one")
}

// Corruption is surfaced rather than read as a miss. Reporting "unclaimed" for data we could
// not read would invent a free claim and split the session; failing the request is safer.
func TestSticky_CorruptValueIsAnErrorNotAMiss(t *testing.T) {
	store, mr := newTestStore(t)
	require.NoError(t, mr.Set("sr:sticky:k", "not-json"))

	_, found, err := store.GetSticky(context.Background(), "sticky:k")
	require.Error(t, err)
	require.False(t, found)
}

func TestSticky_PurgeDropsClaims(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	_, err := store.SetStickyIfAbsent(ctx, "sticky:k", core.StickyPin{Provider: "node-a", Epoch: 1}, time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.Purge(ctx))

	_, found, err := store.GetSticky(ctx, "sticky:k")
	require.NoError(t, err)
	require.False(t, found, "/debug/reset-all must genuinely drop claims")
}
