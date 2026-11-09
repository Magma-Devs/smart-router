package holdoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// URLReadyAt reads both keys a url's hold-off is recorded under — the url-only one (the chain
// tracker, WS and gRPC subscriptions) and the relay path's (provider, url) one — and answers with
// the later. The chain tracker's poll and the probe loop both ask it (MAG-4387).
func TestURLReadyAt_ReadsBothKeys(t *testing.T) {
	const provider, url = "vendor", "https://node.example/v1"
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }

	t.Run("neither key: free", func(t *testing.T) {
		r := NewRegistryWithClock(clock)
		require.True(t, r.URLReadyAt(provider, url).IsZero())
	})
	t.Run("url-only key", func(t *testing.T) {
		r := NewRegistryWithClock(clock)
		r.RecordRateLimit(url, url, 0)
		require.Equal(t, now.Add(InitialHoldoff), r.URLReadyAt(provider, url))
	})
	t.Run("relay path's key", func(t *testing.T) {
		r := NewRegistryWithClock(clock)
		r.RecordRateLimit(provider, url, 0)
		require.Equal(t, now.Add(InitialHoldoff), r.URLReadyAt(provider, url))
	})
	t.Run("both keys: the later wins", func(t *testing.T) {
		r := NewRegistryWithClock(clock)
		r.RecordRateLimit(url, url, 0)
		r.RecordRateLimit(provider, url, 5*time.Minute)
		require.Equal(t, now.Add(5*time.Minute), r.URLReadyAt(provider, url))
	})
	t.Run("no provider, or the url as provider, reads the url-only key alone", func(t *testing.T) {
		r := NewRegistryWithClock(clock)
		r.RecordRateLimit(provider, url, 0)
		require.True(t, r.URLReadyAt("", url).IsZero())
		require.True(t, r.URLReadyAt(url, url).IsZero())
	})
	t.Run("an answer under the provider's name frees both", func(t *testing.T) {
		r := NewRegistryWithClock(clock)
		r.RecordRateLimit(url, url, 0)
		r.RecordRateLimit(provider, url, 0)
		r.RecordAnswer(provider, url)
		require.True(t, r.URLReadyAt(provider, url).IsZero())
	})
}
