package rpcsmartrouter

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRestoreAfterTeardown_LeavesNoUpstreamSubscription is MAG-2547: an upstream socket
// drops, the router re-subscribes, and the subscription's last client disconnects while
// that restore is still waiting on the node. The teardown runs first and releases the
// old upstream subscription; the restore must then notice its subscription is gone and
// end the one it just created — on the node, locally, and in the connection's count —
// rather than committing it onto an entry nothing holds any more. Before the fix the
// restored upstream subscription stayed live on the node with nobody reading it, the
// connection kept counting it so the pool could never scale it down, and the teardown
// goroutine read the subscription's upstream fields off the lock while the restore
// rewrote them under it.
func TestRestoreAfterTeardown_LeavesNoUpstreamSubscription(t *testing.T) {
	h := newReleaseHarness(t)

	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce sync.Once
	h.manager.beforeRestoreCommit = func() {
		reachedOnce.Do(func() { close(reached) })
		<-release
	}

	sub := h.subscribe("ws-1")
	require.Equal(t, 1, h.srv.liveSubscriptions())

	// The upstream socket drops. The router re-subscribes on a fresh connection and is
	// held just before it commits the result.
	h.srv.dropConnections()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the restore never re-subscribed on the node")
	}
	require.Equal(t, 2, h.upstreamCalls("eth_subscribe"), "the restore subscribed again on the node")

	// The last client leaves while the restore is held, so the subscription is torn down
	// underneath it.
	sub.disconnect()
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 },
		5*time.Second, 10*time.Millisecond, "the last client leaving tears the subscription down")

	close(release)

	require.Eventually(t, func() bool { return h.srv.liveSubscriptions() == 0 },
		5*time.Second, 10*time.Millisecond,
		"the restored upstream subscription was left running on the node with nobody reading it")
	require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 0 },
		5*time.Second, 10*time.Millisecond,
		"a pooled connection still counts a subscription, so the pool can never scale it down")
	require.Equal(t, int64(0), h.manager.totalSubscriptions.Load())
	require.Equal(t, 0, h.activeSubscriptionCount())
	require.Eventually(t, func() bool { return goroutinesIn("listenForUpstreamMessages") == 0 },
		5*time.Second, 10*time.Millisecond, "no upstream listener may outlive the subscription")
}
