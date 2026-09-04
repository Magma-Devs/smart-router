package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEngineSticky_ClampsTTL(t *testing.T) {
	for _, tc := range []struct {
		name     string
		asked    time.Duration
		expected time.Duration
	}{
		{"zero is floored", 0, MinStickyTTL},
		{"negative is floored", -time.Hour, MinStickyTTL},
		{"in range is kept", 30 * time.Minute, 30 * time.Minute},
		{"excessive is capped", 24 * time.Hour, MaxStickyTTL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			engine := &Engine{Store: store}
			_, err := engine.SetStickyIfAbsent(context.Background(), "ETH1", "jsonrpc", "id", StickyPin{Provider: "node-a"}, tc.asked)
			require.NoError(t, err)
			require.Equal(t, tc.expected, store.stickyTTLs[StickyKey("ETH1", "jsonrpc", "id")])
		})
	}
}

// The default epoch window is 15 minutes and a pin must outlive two of them, so a ceiling
// shorter than that would expire pins the epoch rule still accepts — the silent split this
// feature removes. This pins the relationship rather than the constant.
func TestEngineSticky_CeilingOutlivesTwoEpochs(t *testing.T) {
	require.GreaterOrEqual(t, MaxStickyTTL, 2*15*time.Minute)
}

func TestEngineSticky_KeysAreScopedPerChainAndInterface(t *testing.T) {
	store := newFakeStore()
	engine := &Engine{Store: store}
	ctx := context.Background()

	_, err := engine.SetStickyIfAbsent(ctx, "ETH1", "jsonrpc", "same-id", StickyPin{Provider: "node-a", Epoch: 1}, time.Minute)
	require.NoError(t, err)

	// A session manager is scoped to one chain AND one api interface, so an upstream name only
	// means anything inside that scope. Sharing a key across scopes would hand a router a name
	// its pairing does not contain.
	_, found, err := engine.GetSticky(ctx, "ETH1", "rest", "same-id")
	require.NoError(t, err)
	require.False(t, found)

	_, found, err = engine.GetSticky(ctx, "POLYGON1", "jsonrpc", "same-id")
	require.NoError(t, err)
	require.False(t, found)
}

// A failed lookup must never be reported as "no claim exists". That would invent a free claim
// for the caller and split the session — the exact failure cross-pod stickiness prevents.
func TestEngineSticky_StoreErrorIsNotAMiss(t *testing.T) {
	store := newFakeStore()
	store.stickyErr = errors.New("backend unreachable")
	engine := &Engine{Store: store}

	_, found, err := engine.GetSticky(context.Background(), "ETH1", "jsonrpc", "id")
	require.Error(t, err)
	require.False(t, found)

	_, err = engine.SetStickyIfAbsent(context.Background(), "ETH1", "jsonrpc", "id", StickyPin{Provider: "node-a"}, time.Minute)
	require.Error(t, err)
}

func TestEngineSticky_EmptyIdIsRejectedOnWrite(t *testing.T) {
	engine := &Engine{Store: newFakeStore()}
	_, err := engine.SetStickyIfAbsent(context.Background(), "ETH1", "jsonrpc", "", StickyPin{Provider: "node-a"}, time.Minute)
	require.ErrorIs(t, err, ErrEmptyStickyId)

	_, found, err := engine.GetSticky(context.Background(), "ETH1", "jsonrpc", "")
	require.NoError(t, err)
	require.False(t, found)
}
