package lavaprotocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRelayRetriesManager_Reset is the core guarantee /debug/reset-all relies
// on: after Reset() the manager must report no banned hashes, regardless of
// how recently they were added. The Ristretto 6h TTL otherwise leaks failure
// memory across black-box test runs.
func TestRelayRetriesManager_Reset(t *testing.T) {
	rrm := NewRelayRetriesManager()
	rrm.AddHashToCache("hash-a")
	rrm.AddHashToCache("hash-b")

	require.True(t, rrm.CheckHashInCache("hash-a"))
	require.True(t, rrm.CheckHashInCache("hash-b"))

	rrm.Reset()

	require.False(t, rrm.CheckHashInCache("hash-a"), "Reset must drop previously-banned hash")
	require.False(t, rrm.CheckHashInCache("hash-b"), "Reset must drop previously-banned hash")
}

// TestRelayRetriesManager_ResetEmpty verifies Reset on a fresh manager is a
// no-op, not a panic.
func TestRelayRetriesManager_ResetEmpty(t *testing.T) {
	rrm := NewRelayRetriesManager()
	require.NotPanics(t, func() { rrm.Reset() })
}
