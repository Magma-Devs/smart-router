package rpcsmartrouter

import (
	"encoding/json"
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

// TestUnsubscribeDuringRestore_TellsTheNodeTheRestoredID: the last client's eth_unsubscribe
// is waiting on the node when the restore reaches its commit point. Unsubscribe told the node
// the id it read before its call, and the restore has re-opened the subscription under a new
// id: that one must be ended on the node too, or it stays live with nobody reading it
// (MAG-2547). Unsubscribe retires the entry before its call (MAG-4345), so the restore finds
// nothing to commit onto and discards the subscription it re-opened; the test asserts the
// outcome, not the path: neither id is left on the node.
//
// The restore runs on a connection that stays healthy, because rpcclient gave up on the
// subscription's queue. That is the only restore the unsubscribe can race on the node: after a
// dropped socket the connection is retired, so there is no node left to call (MAG-4242).
func TestUnsubscribeDuringRestore_TellsTheNodeTheRestoredID(t *testing.T) {
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
	nodeHas := func(id string) bool {
		h.srv.lock.RLock()
		defer h.srv.lock.RUnlock()
		_, ok := h.srv.subscriptions[id]
		return ok
	}
	// nodeSubscriptionOtherThan is the id the node minted for the restore, read while the
	// node still holds both.
	nodeSubscriptionOtherThan := func(id string) string {
		h.srv.lock.RLock()
		defer h.srv.lock.RUnlock()
		for other := range h.srv.subscriptions {
			if other != id {
				return other
			}
		}
		return ""
	}

	sub := h.subscribe("ws-1")
	t.Cleanup(sub.disconnect)
	// The client reads, as a live one does; a client that stopped would be dropped as slow.
	countReplies(sub)
	activeSub := h.subscriptionOf(sub.clientKey)
	require.NotNil(t, activeSub)
	conn, oldID := h.upstreamOf(activeSub)

	overflowSubscription(t, h, activeSub)
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the restore never re-subscribed on the node")
	}
	require.True(t, conn.IsHealthy(), "the restore's connection must still answer the unsubscribe")
	restoredID := nodeSubscriptionOtherThan(oldID)
	require.NotEmpty(t, restoredID, "the node holds the restored subscription beside the old one")

	// The node reads and applies the unsubscribe but cannot answer yet.
	h.srv.writeMu.Lock()
	unlockNode := sync.OnceFunc(func() { h.srv.writeMu.Unlock() })
	t.Cleanup(unlockNode)
	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "eth_unsubscribe", "params": []interface{}{sub.routerID}, "id": 2,
	})
	require.NoError(t, err)
	msg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_unsubscribe", params: []interface{}{sub.routerID}},
		rawData:               body,
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := h.manager.Unsubscribe(sub.ctx, msg, "dapp", "10.0.0.1", sub.uid, nil)
		errCh <- err
	}()
	require.Eventually(t, func() bool { return h.upstreamCalls("eth_unsubscribe") == 1 },
		5*time.Second, 5*time.Millisecond)

	// The restore reaches its commit point while Unsubscribe waits on the node.
	require.Equal(t, 0, h.activeSubscriptionCount(), "the entry left the map before the node was called")
	releaseRestore()

	unlockNode()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("Unsubscribe never returned")
	}
	require.Eventually(t, func() bool { return !nodeHas(restoredID) }, 5*time.Second, 10*time.Millisecond,
		"the restored upstream subscription was left running on the node after its only client unsubscribed")
	require.False(t, nodeHas(oldID), "the node dropped the old subscription on the client's unsubscribe")
	require.Eventually(t, func() bool { return goroutinesIn("handleUpstreamDisconnect") == 0 }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(0), h.manager.totalSubscriptions.Load())
	require.Equal(t, 0, h.activeSubscriptionCount())
	require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond,
		"a pooled connection still counts a subscription")
	require.Eventually(t, func() bool { return goroutinesIn("listenForUpstreamMessages") == 0 }, 5*time.Second, 10*time.Millisecond)
}
