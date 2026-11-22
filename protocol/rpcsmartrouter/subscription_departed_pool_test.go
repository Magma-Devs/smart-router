package rpcsmartrouter

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// onlyWSPool returns the manager's single upstream pool and the connection it holds.
func (h *releaseHarness) onlyWSPool() (*UpstreamWSPool, *UpstreamWSConnection) {
	h.t.Helper()
	h.manager.lock.RLock()
	defer h.manager.lock.RUnlock()
	require.Len(h.t, h.manager.upstreamPools, 1)
	for _, pool := range h.manager.upstreamPools {
		pool.lock.RLock()
		defer pool.lock.RUnlock()
		require.Len(h.t, pool.connections, 1)
		return pool, pool.connections[0]
	}
	return nil, nil
}

// TestWS_ADepartedURLKeepsItsLiveSubscriptionThenClosesItsSocket: the URL a subscription runs
// on leaves both tiers. The subscription keeps running on its socket, as setEndpoints promises,
// and when it ends the socket closes. The pool's scale-down used to stop at minConnections, so
// the socket to an upstream the router no longer served stayed open for good (MAG-4924).
func TestWS_ADepartedURLKeepsItsLiveSubscriptionThenClosesItsSocket(t *testing.T) {
	h := newReleaseHarness(t)
	a := h.subscribe("ws-a")
	t.Cleanup(a.disconnect)
	pool, conn := h.onlyWSPool()

	require.True(t, h.manager.SetEndpoints(nil, nil), "the URL leaves both tiers")

	require.Never(t, func() bool { return !conn.IsHealthy() }, 200*time.Millisecond, 10*time.Millisecond,
		"the live subscription's socket must survive its URL leaving the tiers")
	select {
	case _, ok := <-a.replies:
		require.True(t, ok, "the subscription must still be delivering")
	case <-time.After(5 * time.Second):
		t.Fatal("no push reached the client after its URL left the tiers")
	}

	h.unsubscribe(a)
	require.Eventually(t, func() bool { return pool.ConnectionCount() == 0 && !conn.IsHealthy() },
		5*time.Second, 10*time.Millisecond, "the departed URL's last socket must close with its last subscription")
}

// TestWS_AnIdlePoolClosesItsSocketWhenItsURLDeparts covers the pool that is already idle when
// its URL leaves: no release will ever come for it, so SetEndpoints itself scales it down. A
// URL that comes back is served again, on a socket dialed for it.
func TestWS_AnIdlePoolClosesItsSocketWhenItsURLDeparts(t *testing.T) {
	h := newReleaseHarness(t)
	a := h.subscribe("ws-a")
	h.unsubscribe(a)
	a.disconnect()
	pool, conn := h.onlyWSPool()
	require.Never(t, func() bool { return pool.ConnectionCount() == 0 }, 100*time.Millisecond, 10*time.Millisecond,
		"while its URL is served, an idle pool keeps minConnections")

	served := []*common.NodeUrl{{Url: h.srv.URL()}}
	require.True(t, h.manager.SetEndpoints(nil, nil))
	require.Eventually(t, func() bool { return pool.ConnectionCount() == 0 && !conn.IsHealthy() },
		5*time.Second, 10*time.Millisecond, "an idle pool's socket must close when its URL leaves the tiers")

	require.True(t, h.manager.SetEndpoints(served, nil), "the URL comes back")
	b := h.subscribe("ws-b")
	t.Cleanup(b.disconnect)
	require.Equal(t, 1, pool.ConnectionCount(), "the returning URL's pool dials again")
}

// TestGRPCSetEndpoints_AnIdlePoolClosesItsConnectionWhenItsURLDeparts is the gRPC manager's
// wiring of the same floor: the stream pool shares the WebSocket pool's scale-down.
func TestGRPCSetEndpoints_AnIdlePoolClosesItsConnectionWhenItsURLDeparts(t *testing.T) {
	endpoint := &common.NodeUrl{Url: "grpc://localhost:9090"}
	manager := NewDirectGRPCSubscriptionManager(getTestMetricsManager(), "ETH", "grpc",
		[]*common.NodeUrl{endpoint}, nil, nil, nil)
	t.Cleanup(manager.Stop)
	pool, err := manager.getOrCreatePool(endpoint)
	require.NoError(t, err)
	conn := &UpstreamGRPCStreamConnection{
		endpoint:     endpoint.Url,
		sanitizedURL: endpoint.Url,
		createdAt:    time.Now(),
	}
	conn.healthy.Store(true)
	pool.lock.Lock()
	pool.connections = []*UpstreamGRPCStreamConnection{conn}
	pool.lock.Unlock()

	pool.maybeScaleDown()
	require.Equal(t, 1, pool.ConnectionCount(), "while its URL is served, an idle pool keeps minConnections")

	require.True(t, manager.SetEndpoints(nil, nil))
	require.Eventually(t, func() bool { return pool.ConnectionCount() == 0 && conn.closed.Load() },
		5*time.Second, 10*time.Millisecond, "an idle pool's connection must close when its URL leaves the tiers")
}
