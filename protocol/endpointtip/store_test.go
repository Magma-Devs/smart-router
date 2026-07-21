package endpointtip

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStore_Set_BlockMonotonic pins the T4/C-D rule: the tip orders by BLOCK, not by
// arrival time. A higher block always wins; an equal block refreshes freshness; a
// newer-but-LOWER straggler is rejected while the stored tip is fresh (the F2 bug). This
// is the inverse of the old time-monotonic guard.
func TestStore_Set_BlockMonotonic(t *testing.T) {
	s := NewStore()
	k := Key("ETH1", "jsonrpc", "https://a.example:443")
	base := time.Unix(1_000, 0)
	const ttl = 100 * time.Second

	// First observation stores.
	require.True(t, s.Set(k, Tip{Block: 100, ObservedAt: base, Source: SourcePoll}, ttl))
	require.Equal(t, int64(100), s.Block(k))

	// Higher block wins — even carrying an EARLIER arrival stamp (block, not time, is the
	// comparator now; the inverse of the old guard).
	require.True(t, s.Set(k, Tip{Block: 200, ObservedAt: base.Add(-time.Second), Source: SourceRelay}, ttl))
	require.Equal(t, int64(200), s.Block(k))

	// The F2 straggler: a LOWER block observed only slightly later is rejected (< ttl since
	// the stored tip's observation, so the stored tip is still fresh).
	require.False(t, s.Set(k, Tip{Block: 198, ObservedAt: base.Add(2 * time.Second), Source: SourceRelay}, ttl))
	got, ok := s.Get(k)
	require.True(t, ok)
	require.Equal(t, int64(200), got.Block, "a newer-but-lower straggler must not regress the tip")
	require.Equal(t, SourceRelay, got.Source, "rejected write must not move the source")
	require.Equal(t, base.Add(-time.Second), got.ObservedAt, "rejected write must not refresh the stamp")
}

// TestStore_Set_EqualBlockRefreshesFreshness pins that an equal block re-proves liveness:
// it advances the freshness stamp (so a healthy endpoint sitting at a stable head keeps
// re-confirming and never goes stale) without moving the block, and never regresses it.
func TestStore_Set_EqualBlockRefreshesFreshness(t *testing.T) {
	s := NewStore()
	k := Key("ETH1", "jsonrpc", "u")
	base := time.Unix(1_000, 0)
	const ttl = 10 * time.Second

	require.True(t, s.Set(k, Tip{Block: 100, ObservedAt: base, Source: SourcePoll}, ttl))

	// Re-report the SAME block 9s later: the stamp advances to base+9s.
	require.True(t, s.Set(k, Tip{Block: 100, ObservedAt: base.Add(9 * time.Second), Source: SourcePoll}, ttl))
	got, _ := s.Get(k)
	require.Equal(t, base.Add(9*time.Second), got.ObservedAt, "equal-block confirmation advances the stamp")

	// A lower block observed 8s after the REFRESH (base+17s) is only 8s < ttl past the
	// refreshed stamp → still fresh → rejected. Without the refresh it would be 17s > ttl.
	require.False(t, s.Set(k, Tip{Block: 99, ObservedAt: base.Add(17 * time.Second), Source: SourcePoll}, ttl))
	require.Equal(t, int64(100), s.Block(k), "the equal-block refresh kept the tip fresh")

	// A delayed equal block must NOT drag the stamp backward.
	require.True(t, s.Set(k, Tip{Block: 100, ObservedAt: base, Source: SourceRelay}, ttl))
	got, _ = s.Get(k)
	require.Equal(t, base.Add(9*time.Second), got.ObservedAt, "stamp only advances, never regresses")
}

// TestStore_Set_StaleBackstopAcceptsReorg is the reorg half of C-D: while the stored tip
// is fresh a lower block is rejected; once no higher block has re-confirmed it for longer
// than the horizon, the same lower block is accepted (the endpoint genuinely fell back).
// No fork signal is involved — the observation-time gap is the sole downward mechanism
// (D-FORK resolved). Staleness is measured in observation time, so this is deterministic.
func TestStore_Set_StaleBackstopAcceptsReorg(t *testing.T) {
	s := NewStore()
	k := Key("ETH1", "jsonrpc", "u")
	base := time.Unix(1_000, 0)
	const ttl = 100 * time.Second

	require.True(t, s.Set(k, Tip{Block: 1001, ObservedAt: base, Source: SourcePoll}, ttl))

	// Reorg to 1000 observed 30s later → within the horizon → rejected (stale-high, bounded).
	require.False(t, s.Set(k, Tip{Block: 1000, ObservedAt: base.Add(30 * time.Second), Source: SourcePoll}, ttl))
	require.Equal(t, int64(1001), s.Block(k))

	// The endpoint keeps reporting 1000; 101s after the (never-refreshed) stored stamp the
	// gap exceeds the horizon → accept the downward move. Self-heals to the true head.
	require.True(t, s.Set(k, Tip{Block: 1000, ObservedAt: base.Add(101 * time.Second), Source: SourcePoll}, ttl))
	require.Equal(t, int64(1000), s.Block(k), "stale stored tip → accept the endpoint's true head")
}

// TestStore_Set_ZeroStaleAfterDisablesDownward: staleAfter<=0 is pure up-only (never
// accepts a lower block). Used by never-stale test seeds, which Remove-then-Set anyway.
func TestStore_Set_ZeroStaleAfterDisablesDownward(t *testing.T) {
	s := NewStore()
	k := Key("ETH1", "jsonrpc", "u")
	base := time.Unix(1_000, 0)

	require.True(t, s.Set(k, Tip{Block: 100, ObservedAt: base, Source: SourcePoll}, 0))
	// Even a far-later lower block is rejected when staleness is disabled.
	require.False(t, s.Set(k, Tip{Block: 50, ObservedAt: base.Add(time.Hour), Source: SourcePoll}, 0))
	require.Equal(t, int64(100), s.Block(k))
}

// TestStore_Set_RejectsNonPositive guards that a zero/negative block is never stored.
func TestStore_Set_RejectsNonPositive(t *testing.T) {
	s := NewStore()
	k := Key("ETH1", "jsonrpc", "u")
	require.False(t, s.Set(k, Tip{Block: 0, ObservedAt: time.Unix(1, 0)}, time.Minute))
	require.False(t, s.Set(k, Tip{Block: -5, ObservedAt: time.Unix(1, 0)}, time.Minute))
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
	s.Set(kEth, Tip{Block: 111, ObservedAt: now, Source: SourcePoll}, time.Minute)
	s.Set(kLava, Tip{Block: 222, ObservedAt: now, Source: SourcePoll}, time.Minute)
	require.Equal(t, int64(111), s.Block(kEth))
	require.Equal(t, int64(222), s.Block(kLava))
}

// TestStore_RemoveAndReset covers the lifecycle cleanup (Remove on tracker removal) and
// test isolation (Reset).
func TestStore_RemoveAndReset(t *testing.T) {
	s := NewStore()
	k := Key("ETH1", "jsonrpc", "u")
	s.Set(k, Tip{Block: 5, ObservedAt: time.Unix(1, 0), Source: SourcePoll}, time.Minute)
	require.Equal(t, int64(5), s.Block(k))

	s.Remove(k)
	_, ok := s.Get(k)
	require.False(t, ok)
	require.Equal(t, int64(0), s.Block(k))

	s.Set(k, Tip{Block: 9, ObservedAt: time.Unix(2, 0), Source: SourcePoll}, time.Minute)
	s.Reset()
	require.Equal(t, int64(0), s.Block(k), "Reset clears the whole store")
}

// TestDefault_Singleton verifies Default() always returns the same instance — the
// property that makes the tip live in one place across both importing layers.
func TestDefault_Singleton(t *testing.T) {
	require.Same(t, Default(), Default())
}
