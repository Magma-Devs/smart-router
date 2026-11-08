package performance_test

import (
	"net"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/stretchr/testify/require"
)

// A router running a RESP primary and a RESP secondary has two stores logging the
// same kinds of line; the secondary's say which tier they are about (MAG-4778).
func TestSecondaryRespCacheLogsUnderItsOwnName(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddress := listener.Addr().String()
	require.NoError(t, listener.Close())

	cfg := redisstore.Config{Addresses: []string{deadAddress}, KeyPrefix: "zone-a"}
	var reader *performance.RespCache
	logs := captureLog(t, func() {
		cfg.Name = "secondary-resp-cache"
		reader, err = performance.OpenSecondaryRespCache(cfg, performance.DefaultSecondaryCacheTimeout)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return reader.DebugCacheState().Reachable != nil }, 10*time.Second, 50*time.Millisecond)
	})
	t.Cleanup(func() { _ = reader.Close() })
	require.Contains(t, logs, "secondary-resp-cache backend unavailable at startup")
	require.NotContains(t, logs, `"resp-cache backend`, "no line reads as the primary's")
}
