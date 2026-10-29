package rpcsmartrouter

import (
	"errors"
	"testing"
	"time"

	rpcclient "github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOverflowRestore_TellsTheNodeToDropTheOldSubscription is MAG-4396's leak.
//
// When a subscription's forward queue overflows, rpcclient ends it with
// ErrSubscriptionQueueOverflow on a connection that stays up, and the restore re-subscribes on
// that same live connection. Nothing told the node to drop the old subscription:
// ClientSubscription.run discards the unsubscribe-the-server flag forward returns, so rpcclient
// sends nothing, and handleUpstreamDisconnect used the old id only for logging and for dropping
// its id mappings. The node kept publishing the old subscription on the same socket, rpcclient
// read those frames and dropped them, and a subscription that overflows repeatedly left one
// more of them behind each time.
func TestOverflowRestore_TellsTheNodeToDropTheOldSubscription(t *testing.T) {
	h := newReleaseHarness(t)
	sub := h.subscribe("ws-1")
	t.Cleanup(sub.disconnect)
	received := countReplies(sub)
	activeSub := h.subscriptionOf(sub.clientKey)
	require.NotNil(t, activeSub)
	conn, oldID := h.upstreamOf(activeSub)
	require.True(t, h.srv.has(oldID))

	overflowSubscription(t, h, activeSub)

	require.Eventually(t, func() bool {
		restoredConn, restoredID := h.upstreamOf(activeSub)
		return restoredConn == conn && restoredID != oldID
	}, 10*time.Second, 10*time.Millisecond, "the restore must re-subscribe on the same live connection")
	_, restoredID := h.upstreamOf(activeSub)

	if !assert.Eventually(t, func() bool { return !h.srv.has(oldID) }, 10*time.Second, 10*time.Millisecond) {
		t.Fatalf("the node still publishes the subscription the restore replaced (%s); it saw %v",
			oldID, h.srv.ObservedMethods())
	}
	require.Equal(t, 1, h.srv.liveSubscriptions(), "the node holds the restored subscription and nothing else")
	require.True(t, h.srv.has(restoredID))
	require.Equal(t, 1, h.upstreamCalls("eth_unsubscribe"), "exactly one goodbye, for the subscription that was replaced")

	// The client is unaffected: it keeps receiving on the restored subscription.
	requireStillReceiving(t, received, "the client stopped receiving after the restore")
	require.True(t, h.attached(sub.clientKey))
	require.Equal(t, int64(1), h.manager.totalSubscriptions.Load())
	require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 1 }, 5*time.Second, 10*time.Millisecond,
		"one live subscription, one count")
}

// TestRestoreAfterADroppedSocket_DoesNotCallADeadNode is the other half of the same call: when
// the restore follows a lost socket, the node lost the subscription with it, so there is nobody
// to tell. teardownUpstreamSubscription skips a dead connection, which is what makes one call
// right on both restore paths — without it, telling the node here would make rpcclient dial
// again just to say goodbye, into the outage the pool is still reconnecting from (MAG-4242).
func TestRestoreAfterADroppedSocket_DoesNotCallADeadNode(t *testing.T) {
	h := newReleaseHarness(t)
	sub := h.subscribe("ws-1")
	t.Cleanup(sub.disconnect)
	countReplies(sub)
	activeSub := h.subscriptionOf(sub.clientKey)
	require.NotNil(t, activeSub)
	_, oldID := h.upstreamOf(activeSub)

	h.srv.dropConnections()
	require.Eventually(t, func() bool {
		_, restoredID := h.upstreamOf(activeSub)
		return restoredID != oldID
	}, 10*time.Second, 10*time.Millisecond, "the restore commits")

	time.Sleep(500 * time.Millisecond) // room for a wrong call to land
	require.Equal(t, 0, h.upstreamCalls("eth_unsubscribe"),
		"the node lost the subscription with the socket; there is nobody to tell")
	require.Equal(t, 1, h.activeSubscriptionCount())
	require.True(t, h.attached(sub.clientKey))
}

// endSubscriptionWith drives the real listener's error branch with err, through the
// upstreamErrSource seam listenForUpstreamMessages takes for exactly this. A client that
// redialed cannot be produced from the node side: rpcclient mints ErrClientReconnected
// internally, when a write fails while the old read loop is still running, and a call whose
// deadline has already passed — the MAG-4037 trigger — fails the redial on the same expired
// deadline, so no reconnect lands.
func endSubscriptionWith(t *testing.T, h *releaseHarness, activeSub *directActiveSubscription, err error) {
	t.Helper()
	errCh := make(chan error, 1)
	errCh <- err
	close(errCh)
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.manager.listenForUpstreamMessages(activeSub.ctx, activeSub.hashedParams, activeSub,
			fakeUpstreamErrSource{ch: errCh}, activeSub.messagesChan)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the listener did not take the error branch")
	}
}

// TestClientRedial_KeepsTheConnectionAndItsSiblingSubscriptions is MAG-4396's decision.
//
// PR #519 made any subscription error except a queue overflow or a node error retire the pooled
// connection, so the pool prunes it and closes it. ErrClientReconnected is one of those errors,
// and it is the one case where the client has already recovered: rpcclient replaced the socket
// itself and holds a working one. Retiring the connection there closes that client, so the pool
// dials again for nothing and every OTHER subscription sharing the connection is ended too —
// including one a restore has just re-created on the new socket.
//
// The decision recorded here: ErrClientReconnected leaves the connection alone. A real socket
// failure still retires it, which TestUpstreamError_RetiresTheSharedConnectionOnlyOnASocketFailure
// pins.
func TestClientRedial_KeepsTheConnectionAndItsSiblingSubscriptions(t *testing.T) {
	h := newReleaseHarness(t)
	first := h.subscribe("ws-first")
	t.Cleanup(first.disconnect)
	second := h.subscribeToLogs("ws-second")
	t.Cleanup(second.disconnect)
	firstSub, secondSub := h.subscriptionOf(first.clientKey), h.subscriptionOf(second.clientKey)
	require.NotNil(t, firstSub)
	require.NotNil(t, secondSub)
	conn, _ := h.upstreamOf(firstSub)
	secondConn, _ := h.upstreamOf(secondSub)
	require.Same(t, conn, secondConn, "both subscriptions share one pooled connection")
	countReplies(first)
	secondReceived := countReplies(second)
	requireStillReceiving(t, secondReceived, "the second subscription receives before the redial")

	endSubscriptionWith(t, h, firstSub, rpcclient.ErrClientReconnected)

	time.Sleep(300 * time.Millisecond) // room for a wrong retirement to land
	require.True(t, conn.IsHealthy(), "a client that redialed on its own is not a dead socket")
	require.True(t, h.poolHolds(conn), "the pool dropped a connection whose client had already recovered")
	require.Same(t, conn, mustConnOf(t, h, secondSub), "the sibling subscription was moved off its connection")
	requireStillReceiving(t, secondReceived, "the sibling subscription on the same connection was ended too")
	require.True(t, h.attached(second.clientKey))
}

// mustConnOf reads the connection a subscription currently lives on.
func mustConnOf(t *testing.T, h *releaseHarness, activeSub *directActiveSubscription) *UpstreamWSConnection {
	t.Helper()
	conn, _ := h.upstreamOf(activeSub)
	require.NotNil(t, conn)
	return conn
}

// TestRetireConnectionOnUpstreamError_LeavesAClientThatRedialed pins the classification on its
// own, so a change at the call site cannot pass by never reaching it.
func TestRetireConnectionOnUpstreamError_LeavesAClientThatRedialed(t *testing.T) {
	h := newReleaseHarness(t)
	sub := h.subscribe("ws-1")
	t.Cleanup(sub.disconnect)
	activeSub := h.subscriptionOf(sub.clientKey)
	require.NotNil(t, activeSub)
	conn, _ := h.upstreamOf(activeSub)

	for _, err := range []error{rpcclient.ErrClientReconnected, rpcclient.ErrSubscriptionQueueOverflow} {
		h.manager.retireConnectionOnUpstreamError(activeSub, err)
		require.True(t, conn.IsHealthy(), "%v is not a dead socket", err)
	}

	h.manager.retireConnectionOnUpstreamError(activeSub, errors.New("read: connection reset by peer"))
	require.False(t, conn.IsHealthy(), "a socket failure must still retire the connection")
}
