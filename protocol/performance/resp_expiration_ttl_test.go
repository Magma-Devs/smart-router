package performance_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// The expiration block's lifetimes must reach the key the store writes, and a
// lifetime that would round to zero must never get that far: a zero TTL is a
// plain SET to the store, a key that never expires (Codex review of #405). Both
// halves go through the real loader and backend selection.
func TestExpirationBlockReachesTheStoredTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	_, err := performance.SelectCacheBackend(context.Background(), viperFromYAML(t, fmt.Sprintf(`
resp-cache:
  addresses: [%q]
  expiration:
    finalized: 1ns
    finalized-multiplier: 0.5
`, mr.Addr())))
	require.ErrorContains(t, err, "expiration.finalized", "a lifetime that rounds to zero is refused before any key can be written without an expiry")

	backend := selectBackend(t, fmt.Sprintf(`
resp-cache:
  addresses: [%q]
  expiration:
    finalized: 2s
    finalized-multiplier: 0.5
`, mr.Addr()))
	require.NoError(t, backend.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
		RequestHash:      []byte("ttl"),
		ChainId:          "ETH1",
		RequestedBlock:   100,
		SeenBlock:        100,
		Finalized:        true,
		AverageBlockTime: int64(12 * time.Second),
		Response:         &pairingtypes.RelayReply{Data: []byte(`x`), LatestBlock: 100},
	}))
	var relayKeys []string
	for _, key := range mr.Keys() {
		if strings.Contains(key, "rel:f:") {
			relayKeys = append(relayKeys, key)
		}
	}
	require.Len(t, relayKeys, 1, "one finalized entry was written")
	require.InDelta(t, time.Second.Seconds(), mr.TTL(relayKeys[0]).Seconds(), 0.1,
		"the stored TTL is the configured lifetime after its multiplier, not zero")
}
