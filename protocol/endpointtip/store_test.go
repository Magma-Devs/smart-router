package endpointtip

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStore_Set_TimeMonotonicGuard pins the relocated guard: a write older than the
// stored observation is dropped wholesale (no field regresses), a newer write applies,
// and an equal timestamp applies (last-writer-wins). This is the exact semantics the
// observation record enforced before consolidation — it must not become block-monotonic.
func TestStore_Set_TimeMonotonicGuard(t *testing.T) {
	s := NewStore()
	k := Key("ETH1", "jsonrpc", "https://a.example:443")
	base := time.Unix(1_000, 0)

	require.True(t, s.Set(k, Tip{Block: 100, ObservedAt: base, Source: SourcePoll}))
	require.Equal(t, int64(100), s.Block(k))

	// Older observation is dropped wholesale, even though its block is higher.
	require.False(t, s.Set(k, Tip{Block: 999, ObservedAt: base.Add(-time.Second), Source: SourceRelay}))
	got, ok := s.Get(k)
	require.True(t, ok)
	require.Equal(t, int64(100), got.Block, "stale write must not move the block")
	require.Equal(t, SourcePoll, got.Source, "stale write must not move the source")

	// Newer observation applies even with a LOWER block (guard is time-, not block-monotonic).
	require.True(t, s.Set(k, Tip{Block: 50, ObservedAt: base.Add(time.Second), Source: SourceRelay}))
	require.Equal(t, int64(50), s.Block(k))

	// Equal timestamp applies (last-writer-wins).
	require.True(t, s.Set(k, Tip{Block: 77, ObservedAt: base.Add(time.Second), Source: SourcePoll}))
	require.Equal(t, int64(77), s.Block(k))
}

// TestStore_Set_RejectsNonPositive guards that a zero/negative block is never stored,
// matching the upstream "block <= 0" rejection in the observation writers.
func TestStore_Set_RejectsNonPositive(t *testing.T) {
	s := NewStore()
	k := Key("ETH1", "jsonrpc", "u")
	require.False(t, s.Set(k, Tip{Block: 0, ObservedAt: time.Unix(1, 0)}))
	require.False(t, s.Set(k, Tip{Block: -5, ObservedAt: time.Unix(1, 0)}))
	require.Equal(t, int64(0), s.Block(k))
	_, ok := s.Get(k)
	require.False(t, ok)
}

// TestStore_Key_NoCrossChainCollision is the reason the key is composite: the same url
// string under two different chains must address two independent tips.
func TestStore_Key_NoCrossChainCollision(t *testing.T) {
	s := NewStore()
	url := "https://shared.example:443"
	kEth := Key("ETH1", "jsonrpc", url)
	kLava := Key("LAVA", "jsonrpc", url)
	require.NotEqual(t, kEth, kLava)

	now := time.Unix(2_000, 0)
	s.Set(kEth, Tip{Block: 111, ObservedAt: now, Source: SourcePoll})
	s.Set(kLava, Tip{Block: 222, ObservedAt: now, Source: SourcePoll})
	require.Equal(t, int64(111), s.Block(kEth))
	require.Equal(t, int64(222), s.Block(kLava))
}

// TestStore_RemoveAndReset covers the lifecycle cleanup (Remove on tracker removal) and
// test isolation (Reset).
func TestStore_RemoveAndReset(t *testing.T) {
	s := NewStore()
	k := Key("ETH1", "jsonrpc", "u")
	s.Set(k, Tip{Block: 5, ObservedAt: time.Unix(1, 0), Source: SourcePoll})
	require.Equal(t, int64(5), s.Block(k))

	s.Remove(k)
	_, ok := s.Get(k)
	require.False(t, ok)
	require.Equal(t, int64(0), s.Block(k))

	s.Set(k, Tip{Block: 9, ObservedAt: time.Unix(2, 0), Source: SourcePoll})
	s.Reset()
	require.Equal(t, int64(0), s.Block(k), "Reset clears the whole store")
}

// TestDefault_Singleton verifies Default() always returns the same instance — the
// property that makes the tip live in one place across both importing layers.
func TestDefault_Singleton(t *testing.T) {
	require.Same(t, Default(), Default())
}
