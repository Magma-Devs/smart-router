package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSharedStateTipExpiration locks the shared-state tip TTL contract (T10): a pod's published
// tip lives for SharedStateTipBlockMultiplier * averageBlockTime, floored at ExpirationNonFinalized
// so a chain with an unknown block time still gets a sane, non-zero TTL instead of never expiring.
func TestSharedStateTipExpiration(t *testing.T) {
	cs := &CacheServer{ExpirationNonFinalized: 500 * time.Millisecond}

	// Normal chain: 12s block → 120s TTL (well above the floor, far below the ~1h finalized TTL).
	require.Equal(t, 120*time.Second, cs.SharedStateTipExpiration(12*time.Second))

	// Fast chain: 400ms block → 4s (still above the floor).
	require.Equal(t, 4*time.Second, cs.SharedStateTipExpiration(400*time.Millisecond))

	// Unknown block time (spec omits average_block_time): 0*10 = 0 → floored so it is not eternal.
	require.Equal(t, cs.ExpirationNonFinalized, cs.SharedStateTipExpiration(0))
}

// TestSharedStateTip_RoundTripAndKeyIsolation is the T10 fix: a tip written under one shared-state
// id is readable only under the SAME id. The historical bug was that the write used a chain-level
// id and the read used a per-user id, so the read never resolved the write. This test proves the
// round-trip works when the ids match and misses when they do not, and that writes are max-merged.
func TestSharedStateTip_RoundTripAndKeyIsolation(t *testing.T) {
	cs := &CacheServer{
		finalizedCache:         newRistrettoForTest(t),
		ExpirationNonFinalized: 500 * time.Millisecond,
	}
	srv := &RelayerCacheServer{CacheServer: cs}

	const chainID = "ETH1"
	const writeID = "ETH1jsonrpc" // == listenEndpoint.Key(): the chain-level id both sides now use

	srv.setSeenBlockOnSharedStateMode(chainID, writeID, 1000, time.Minute)
	cs.finalizedCache.Wait()

	// Aligned id → round-trips.
	require.Equal(t, int64(1000), srv.getSeenBlockForSharedStateMode(chainID, writeID))

	// Mismatched (old per-user) id → miss, returns 0. This is the pre-T10 breakage, now asserted
	// to stay dead only for a genuinely different id.
	require.Equal(t, int64(0), srv.getSeenBlockForSharedStateMode(chainID, "mydapp__1.2.3.4"))

	// Max-merge: a lower write is ignored, a higher write wins.
	srv.setSeenBlockOnSharedStateMode(chainID, writeID, 900, time.Minute)
	cs.finalizedCache.Wait()
	require.Equal(t, int64(1000), srv.getSeenBlockForSharedStateMode(chainID, writeID), "lower write must not lower the shared tip")

	srv.setSeenBlockOnSharedStateMode(chainID, writeID, 1100, time.Minute)
	cs.finalizedCache.Wait()
	require.Equal(t, int64(1100), srv.getSeenBlockForSharedStateMode(chainID, writeID), "higher write must advance the shared tip")
}
