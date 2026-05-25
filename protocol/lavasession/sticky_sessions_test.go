package lavasession

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStickySessionStore_Clear backs the /debug/reset-all promise that every
// sticky-session affinity is dropped on demand, independent of epoch.
// DeleteOldSessions only purges sessions older than the given epoch — Clear is
// the unconditional variant tests need.
func TestStickySessionStore_Clear(t *testing.T) {
	s := NewStickySessionStore()
	s.Set("a", &StickySession{Provider: "p1", Epoch: 5})
	s.Set("b", &StickySession{Provider: "p2", Epoch: 999})

	s.Clear()

	_, okA := s.Get("a")
	_, okB := s.Get("b")
	require.False(t, okA, "Clear must drop all sticky sessions")
	require.False(t, okB, "Clear must drop all sticky sessions regardless of epoch")
}

func TestStickySessionStore_ClearEmpty(t *testing.T) {
	s := NewStickySessionStore()
	require.NotPanics(t, func() { s.Clear() })
}
