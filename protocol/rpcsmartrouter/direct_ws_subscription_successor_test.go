package rpcsmartrouter

import (
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// onlySubscription returns the one registered subscription and its upstream id.
func onlySubscription(t *testing.T, h *releaseHarness) (*directActiveSubscription, string) {
	t.Helper()
	h.manager.lock.RLock()
	defer h.manager.lock.RUnlock()
	require.Len(t, h.manager.activeSubscriptions, 1)
	for _, sub := range h.manager.activeSubscriptions {
		return sub, sub.upstreamID
	}
	return nil, ""
}

func (ms *mockSubscriptionServer) has(upstreamID string) bool {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	_, ok := ms.subscriptions[upstreamID]
	return ok
}

// TestListenerEndingLate_LeavesASuccessorAlone: a torn-down subscription's listener ends
// only after the teardown goroutine's node call returns, which a slow node can hold for
// upstreamUnsubscribeTimeout. A new client can register a fresh subscription under the same
// params in that window, and the old listener's deferred cleanup must leave it alone.
func TestListenerEndingLate_LeavesASuccessorAlone(t *testing.T) {
	cfg := DefaultWebsocketConfig()
	cfg.UpstreamPoolSubscriptionsPerConn = 1 // the successor is dialed its own connection
	cfg.UpstreamPoolMaxConnections = 4
	srv := newMockSubscriptionServer()
	srv.messageInterval = 20 * time.Millisecond
	t.Cleanup(srv.Close)
	manager := NewDirectWSSubscriptionManager(getTestMetricsManager(), "jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{{Url: srv.URL()}}, nil, nil, cfg)
	h := &releaseHarness{t: t, srv: srv, manager: manager}

	a := h.subscribe("ws-a")
	t.Cleanup(a.disconnect)
	aSub, aID := onlySubscription(t, h)
	h.manager.lock.RLock()
	aConn := aSub.upstreamConnection
	h.manager.lock.RUnlock()

	// Hold A's teardown goroutine at its node call: GetClient takes the connection's lock.
	aConn.lock.Lock()
	releaseNodeCall := sync.OnceFunc(func() { aConn.lock.Unlock() })
	t.Cleanup(releaseNodeCall)

	a.disconnect()
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond)

	b := h.subscribe("ws-b")
	t.Cleanup(b.disconnect)
	bSub, bID := onlySubscription(t, h)
	require.NotSame(t, aSub, bSub)

	releaseNodeCall() // A's listener ends now and runs its deferred cleanup
	require.Eventually(t, func() bool { return !srv.has(aID) }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return goroutinesIn("listenForUpstreamMessages") <= 1 },
		5*time.Second, 10*time.Millisecond)
	time.Sleep(300 * time.Millisecond)

	require.Equal(t, 1, h.activeSubscriptionCount(), "the old listener's cleanup removed the successor")
	current, _ := onlySubscription(t, h)
	require.Same(t, bSub, current, "the old listener's cleanup removed the successor")
	require.Equal(t, int64(1), h.manager.totalSubscriptions.Load())
	require.True(t, srv.has(bID), "the old listener's cleanup unsubscribed the successor on the node")
	require.True(t, h.attached(b.clientKey))
}

// TestRestoreAfterTeardown_LeavesASuccessorAlone: the restore is held at its commit point,
// the last client leaves, and a new client registers a fresh subscription under the same
// params. The restore must yield without touching the successor: no second listener, no
// count, and the successor keeps its stream.
func TestRestoreAfterTeardown_LeavesASuccessorAlone(t *testing.T) {
	h := newReleaseHarness(t)
	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce sync.Once
	h.manager.beforeRestoreCommit = func() {
		reachedOnce.Do(func() { close(reached) })
		<-release
	}
	releaseRestore := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseRestore)

	a := h.subscribe("ws-a")
	t.Cleanup(a.disconnect)
	aSub, _ := onlySubscription(t, h)
	h.srv.dropConnections()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the restore never re-subscribed on the node")
	}
	a.disconnect()
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond)

	b := h.subscribe("ws-b")
	t.Cleanup(b.disconnect)
	bSub, bID := onlySubscription(t, h)
	require.NotSame(t, aSub, bSub)

	releaseRestore()
	require.Eventually(t, func() bool { return goroutinesIn("handleUpstreamDisconnect") == 0 },
		10*time.Second, 10*time.Millisecond)
	time.Sleep(300 * time.Millisecond)

	require.Equal(t, 1, h.activeSubscriptionCount(), "the abandoned restore removed the successor")
	current, _ := onlySubscription(t, h)
	require.Same(t, bSub, current)
	require.Equal(t, int64(1), h.manager.totalSubscriptions.Load())
	require.True(t, h.srv.has(bID))
	require.Equal(t, 1, goroutinesIn("listenForUpstreamMessages"), "only the successor's listener runs")
	require.Equal(t, int32(1), h.pooledSubscriptionCount())
	require.True(t, h.attached(b.clientKey))
}
