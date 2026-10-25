package rpcsmartrouter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRestoreOnTheSameConnection_KeepsOneCount: the pool hands the restore the same
// connection object it lost the socket on. The restore counts the subscription on it again,
// so it must also uncount the old one, or the connection counts one subscription forever and
// the pool can never scale it down.
func TestRestoreOnTheSameConnection_KeepsOneCount(t *testing.T) {
	h := newReleaseHarness(t)
	sub := h.subscribe("ws-1")
	t.Cleanup(sub.disconnect)
	h.manager.lock.RLock()
	var activeSub *directActiveSubscription
	for _, s := range h.manager.activeSubscriptions {
		activeSub = s
	}
	oldID := activeSub.upstreamID
	h.manager.lock.RUnlock()
	require.Equal(t, int32(1), h.pooledSubscriptionCount())

	h.srv.dropConnections()
	require.Eventually(t, func() bool {
		h.manager.lock.RLock()
		defer h.manager.lock.RUnlock()
		return activeSub.upstreamID != oldID
	}, 10*time.Second, 10*time.Millisecond, "the restore commits")
	// The restore counted the subscription on the connection before it committed; the old
	// connection is uncounted just after.
	require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 1 },
		5*time.Second, 10*time.Millisecond, "one live subscription, one count")

	sub.disconnect()
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond,
		"a pooled connection still counts a subscription, so the pool can never scale it down")
}
