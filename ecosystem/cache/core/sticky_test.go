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
			_, err := engine.SetStickyIfAbsent(context.Background(), "ETH1", "jsonrpc", "base", "id", StickyPin{Provider: "node-a"}, tc.asked)
			require.NoError(t, err)
			require.Equal(t, tc.expected, store.stickyTTLs[StickyKey("ETH1", "jsonrpc", "base", "id")])
		})
	}
}

// Readers honour a claim for two router epochs, so the ceiling must clear two epochs at every
// epoch length an operator can actually configure — not merely at the default.
//
// The previous version compared MaxStickyTTL against a hardcoded 2*15m. That is two constants,
// so it passed at every setting and could never catch the case it was written for:
// --epoch-duration is operator-settable and its help text suggests 1h, where readers trusted a
// claim for two hours while the store dropped it after one.
func TestEngineSticky_CeilingClearsTwoEpochsAtEverySupportedEpochLength(t *testing.T) {
	for _, epoch := range []time.Duration{15 * time.Minute, 30 * time.Minute, time.Hour} {
		require.GreaterOrEqualf(t, MaxStickyTTL, 2*epoch,
			"a %s epoch makes readers trust a claim for %s, which the ceiling must cover", epoch, 2*epoch)
	}
}

func TestEngineSticky_KeysAreScopedPerChainAndInterface(t *testing.T) {
	store := newFakeStore()
	engine := &Engine{Store: store}
	ctx := context.Background()

	_, err := engine.SetStickyIfAbsent(ctx, "ETH1", "jsonrpc", "base", "same-id", StickyPin{Provider: "node-a", Epoch: 1}, time.Minute)
	require.NoError(t, err)

	// A session manager is scoped to one chain AND one api interface, so an upstream name only
	// means anything inside that scope. Sharing a key across scopes would hand a router a name
	// its pairing does not contain.
	_, found, err := engine.GetSticky(ctx, "ETH1", "rest", "base", "same-id")
	require.NoError(t, err)
	require.False(t, found)

	_, found, err = engine.GetSticky(ctx, "POLYGON1", "jsonrpc", "base", "same-id")
	require.NoError(t, err)
	require.False(t, found)
}

// A failed lookup must never be reported as "no claim exists". That would invent a free claim
// for the caller and split the session — the exact failure cross-pod stickiness prevents.
func TestEngineSticky_StoreErrorIsNotAMiss(t *testing.T) {
	store := newFakeStore()
	store.stickyErr = errors.New("backend unreachable")
	engine := &Engine{Store: store}

	_, found, err := engine.GetSticky(context.Background(), "ETH1", "jsonrpc", "base", "id")
	require.Error(t, err)
	require.False(t, found)

	_, err = engine.SetStickyIfAbsent(context.Background(), "ETH1", "jsonrpc", "base", "id", StickyPin{Provider: "node-a"}, time.Minute)
	require.Error(t, err)
}

func TestEngineSticky_EmptyIdIsRejectedOnWrite(t *testing.T) {
	engine := &Engine{Store: newFakeStore()}
	_, err := engine.SetStickyIfAbsent(context.Background(), "ETH1", "jsonrpc", "base", "", StickyPin{Provider: "node-a"}, time.Minute)
	require.ErrorIs(t, err, ErrEmptyStickyId)

	_, found, err := engine.GetSticky(context.Background(), "ETH1", "jsonrpc", "base", "")
	require.NoError(t, err)
	require.False(t, found)
}
