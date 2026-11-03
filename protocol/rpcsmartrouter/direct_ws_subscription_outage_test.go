package rpcsmartrouter

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestRestoreAfterUpstreamOutage_ReconnectsWithBackoff is MAG-4242 item 6. The upstream socket
// drops and the node stays down for a moment. Before the fix the pool never learned the
// connection had died: ReconnectWithBackoff found it "healthy" and returned at once,
// GetConnection handed the dead socket back, the re-subscribe got rpcclient's single dial,
// that dial failed, and the subscription was cleaned up with every client released — the
// backoff ladder never ran. Now the listener retires the connection, the pool reconnects with
// backoff until the node is back, and the subscription is restored with its client attached.
func TestRestoreAfterUpstreamOutage_ReconnectsWithBackoff(t *testing.T) {
	h := newReleaseHarness(t)
	sub := h.subscribe("ws-1")
	require.Equal(t, 1, h.srv.liveSubscriptions())

	// The client keeps reading throughout, as a live client does; a client that stopped
	// reading would be dropped as a slow consumer, which is a different test.
	var pushes atomic.Int64
	var channelClosed atomic.Bool
	go func() {
		defer channelClosed.Store(true)
		for range sub.replies {
			pushes.Add(1)
		}
	}()

	// The node goes down: every socket drops and new dials are refused.
	h.srv.setRefusing(true)
	h.srv.dropConnections()
	require.Eventually(t, func() bool { return h.srv.refusedDials.Load() >= 1 },
		5*time.Second, 10*time.Millisecond, "the router must try to reconnect while the node is down")
	require.True(t, h.attached(sub.clientKey), "the client stays attached while the pool retries")

	// The node comes back. The next attempt of the backoff ladder lands and re-subscribes.
	// (The mock node, like a real one, forgets the subscription that died with the socket, so
	// its live count cannot tell the restore apart: the second subscribe call is the signal.)
	h.srv.setRefusing(false)
	require.Eventually(t, func() bool { return h.upstreamCalls("eth_subscribe") == 2 },
		10*time.Second, 10*time.Millisecond, "the subscription must be restored on the node once it is back")
	require.GreaterOrEqual(t, h.srv.liveSubscriptions(), 1)
	require.Equal(t, 1, h.activeSubscriptionCount())
	require.True(t, h.attached(sub.clientKey))
	require.Equal(t, int64(1), h.manager.totalSubscriptions.Load())

	// And the client keeps receiving from the restored subscription, on the same channel.
	before := pushes.Load()
	require.Eventually(t, func() bool { return pushes.Load() > before },
		5*time.Second, 10*time.Millisecond, "no push reached the client after the restore")
	require.False(t, channelClosed.Load(), "the client's channel must stay open across the outage")
	require.Equal(t, 0, h.upstreamCalls("eth_unsubscribe"), "nothing was unsubscribed: the subscription survived")
}

// TestUpstreamWSPool_RetiresAConnectionMarkedDead: once a listener marks a pooled connection
// dead, the pool neither hands it out again nor keeps it — it is closed and replaced.
func TestUpstreamWSPool_RetiresAConnectionMarkedDead(t *testing.T) {
	srv := newMockSubscriptionServer()
	t.Cleanup(srv.Close)
	pool := NewUpstreamWSPool(&common.NodeUrl{Url: srv.URL()})
	t.Cleanup(func() { _ = pool.Close() })
	ctx := context.Background()

	dead, err := pool.GetConnection(ctx)
	require.NoError(t, err)
	dead.MarkUnhealthy(errors.New("read: connection reset by peer"))

	replacement, err := pool.GetConnection(ctx)
	require.NoError(t, err)
	require.NotSame(t, dead, replacement, "a connection marked dead must not be handed out again")
	require.True(t, replacement.IsHealthy())
	require.Equal(t, 1, pool.ConnectionCount(), "the dead connection is dropped, not kept beside its replacement")
	require.True(t, dead.closed.Load(), "the dead connection is closed when it is dropped")
}

// TestTeardownUpstreamSubscription_SkipsADeadConnection: the node lost the subscription with the
// socket, so there is nobody to tell; the unsubscribe would only make rpcclient dial again to
// say goodbye. A live connection is still told.
func TestTeardownUpstreamSubscription_SkipsADeadConnection(t *testing.T) {
	h := newReleaseHarness(t)
	h.subscribe("ws-1")

	h.manager.lock.RLock()
	require.Len(t, h.manager.activeSubscriptions, 1)
	var active *directActiveSubscription
	for _, sub := range h.manager.activeSubscriptions {
		active = sub
	}
	teardown := teardownOfLocked(active)
	conn := teardown.conn
	h.manager.lock.RUnlock()

	h.manager.teardownUpstreamSubscription(teardown)
	require.Eventually(t, func() bool { return h.upstreamCalls("eth_unsubscribe") == 1 },
		5*time.Second, 10*time.Millisecond, "a live connection is told")

	conn.MarkUnhealthy(errors.New("read: connection reset by peer"))
	h.manager.teardownUpstreamSubscription(teardown)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, h.upstreamCalls("eth_unsubscribe"), "a dead connection is not")
}

// TestUnsubscribeDuringOutage_ReleasesTheClient: the node is down and the pool is retrying with
// backoff, so the subscription lives on to be restored, on a connection the listener marked dead
// but the pool has not closed yet. The client's eth_unsubscribe must release it locally. Calling
// the node made rpcclient redial into the outage: the unsubscribe failed, the client stayed
// attached, and the restore later resumed the pushes it had asked to stop.
func TestUnsubscribeDuringOutage_ReleasesTheClient(t *testing.T) {
	h := newReleaseHarness(t)
	sub := h.subscribe("ws-1")
	countReplies(sub)
	conn, _ := h.upstreamOf(h.subscriptionOf(sub.clientKey))

	h.srv.setRefusing(true)
	h.srv.dropConnections()
	require.Eventually(t, func() bool { return h.srv.refusedDials.Load() >= 1 },
		5*time.Second, 5*time.Millisecond, "the pool retries while the node is down")
	require.False(t, conn.IsHealthy(), "the listener marked the connection dead")
	require.NotNil(t, conn.GetClient(), "the pool closes the connection only once a reconnect lands")

	h.unsubscribe(sub)
	require.False(t, h.attached(sub.clientKey), "the client asked to leave")
	require.Equal(t, 0, h.activeSubscriptionCount())
	require.Equal(t, int64(0), h.manager.totalSubscriptions.Load())

	// The node comes back: nothing is restored for a subscription nobody holds.
	h.srv.setRefusing(false)
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, 1, h.upstreamCalls("eth_subscribe"), "a subscription the client left was restored")
}
