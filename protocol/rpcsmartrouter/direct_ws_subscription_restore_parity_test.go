package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestRestoreBudget_GivesUpOnceTheBudgetIsSpent is MAG-4278 item 11's restore bound.
//
// Each WS restore spawns a fresh listener, and that listener's own failure spawns the next
// restore, so a node that accepts a subscribe and ends it immediately was re-subscribed for
// the life of the process: one round trip per cycle, forever, on every subscription it does
// this to. Nothing counted the cycles. The gRPC manager has bounded this since MAG-4177.
//
// The count is set to the budget and the subscription's connection is then dropped, which is
// the transport failure that restores. The router must give the subscription up instead: the
// client's channel closes, the node is asked for no second subscribe, and nothing is left in
// the maps, the counters or the pool.
func TestRestoreBudget_GivesUpOnceTheBudgetIsSpent(t *testing.T) {
	h := newReleaseHarness(t)
	sub := h.subscribe("ws-1")
	t.Cleanup(sub.disconnect)
	countReplies(sub)
	activeSub := h.subscriptionOf(sub.clientKey)
	require.NotNil(t, activeSub)
	require.Zero(t, activeSub.restoresWithoutProgress.Load())

	// Five restores have already failed to hold, and none of them stayed up for
	// restoreStableAfter, so nothing refills the budget.
	activeSub.restoresWithoutProgress.Store(maxRestoresWithoutProgress)
	require.Zero(t, activeSub.lastRestoredAt.Load())

	h.srv.dropConnections()

	require.True(t, channelClosed(sub.replies, 20*time.Second),
		"a subscription out of restores must be given up, not restored again")
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, h.upstreamCalls("eth_subscribe"), "the node was asked to re-subscribe past the budget")
	require.False(t, h.attached(sub.clientKey))
	require.Equal(t, int64(0), h.manager.totalSubscriptions.Load())
	require.Eventually(t, func() bool { return goroutinesIn("handleUpstreamDisconnect") == 0 },
		10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return goroutinesIn("listenForUpstreamMessages") == 0 },
		10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 0 }, 10*time.Second, 10*time.Millisecond)
}

// TestRestoreBudget_RefillsAfterASubscriptionStaysUp: a subscription that is restored and then
// stays up has made progress, so the next failure starts from a full budget. Without the
// refill a long-lived subscription on a node that drops a connection now and then — a load
// balancer's age limit — would spend its five restores over days and then be given up on an
// ordinary reconnect.
func TestRestoreBudget_RefillsAfterASubscriptionStaysUp(t *testing.T) {
	h := newReleaseHarness(t)
	sub := h.subscribe("ws-1")
	t.Cleanup(sub.disconnect)
	countReplies(sub)
	activeSub := h.subscriptionOf(sub.clientKey)
	require.NotNil(t, activeSub)

	// Spend the budget down to its last restore, then present a subscription that committed
	// longer ago than restoreStableAfter: it was up all that time, which is progress.
	activeSub.restoresWithoutProgress.Store(maxRestoresWithoutProgress)
	activeSub.lastRestoredAt.Store(time.Now().Add(-2 * restoreStableAfter).UnixNano())

	h.srv.dropConnections()
	require.Eventually(t, func() bool {
		_, upstreamID := h.upstreamOf(activeSub)
		return upstreamID != "" && h.upstreamCalls("eth_subscribe") == 2
	}, 10*time.Second, 10*time.Millisecond, "the restore must run on a refilled budget")
	require.Equal(t, 1, h.activeSubscriptionCount())
	require.True(t, h.attached(sub.clientKey))
	require.Equal(t, int32(1), activeSub.restoresWithoutProgress.Load(),
		"progress resets the count, and this restore is the first against it")
}

// TestSubscribeReservesItsPoolSlot is MAG-4278 item 11's reserve-on-pick.
//
// The manager used to pick a connection from the pool and count its subscription on it only
// after the node had answered the subscribe. In that window the connection carries no counted
// subscription, and maybeScaleDown closes a connection past minConnections that carries none —
// so the subscription being created on it died on a socket the pool had closed. The pool now
// counts the slot under the same lock as the pick.
func TestSubscribeReservesItsPoolSlot(t *testing.T) {
	srv := newMockSubscriptionServer()
	srv.messageInterval = time.Hour
	t.Cleanup(srv.Close)
	pool := NewUpstreamWSPool(&common.NodeUrl{Url: srv.URL()})
	t.Cleanup(func() { _ = pool.Close() })

	reserved, err := pool.ReserveConnectionForSubscription(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(1), reserved.SubscriptionCount(),
		"the slot is counted when the connection is handed out, not after the node answers")

	// A second subscription takes its own connection, and the first one's slot keeps the
	// sweep off it.
	pool.minConnections = 0
	pool.perConn = 1
	second, err := pool.ReserveConnectionForSubscription(context.Background())
	require.NoError(t, err)
	require.NotSame(t, reserved, second)
	pool.NotifySubscriptionRemoved(second) // the second subscription's subscribe fails
	require.Eventually(t, func() bool { return pool.ConnectionCount() == 1 }, 5*time.Second, 10*time.Millisecond,
		"the released connection is swept")
	require.True(t, reserved.IsHealthy(), "the reserved connection was swept out from under its subscription")
	require.Equal(t, int32(1), reserved.SubscriptionCount())

	pool.NotifySubscriptionRemoved(reserved)
	require.Equal(t, int32(0), reserved.SubscriptionCount())
}

// TestSubscribeReleasesItsPoolSlotWhenTheSubscribeFails: the reservation is made before the
// node is asked, so every failure after it has to give the slot back or the connection carries
// a subscription that does not exist and the pool can never scale it down.
func TestSubscribeReleasesItsPoolSlotWhenTheSubscribeFails(t *testing.T) {
	srv := newMockSubscriptionServer()
	srv.messageInterval = time.Hour
	t.Cleanup(srv.Close)
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(), "jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{{Url: srv.URL()}}, nil, nil, nil,
	)
	t.Cleanup(manager.Close)
	h := &releaseHarness{t: t, srv: srv, manager: manager}

	// The node answers the subscribe with a JSON-RPC error, which createUpstreamSubscription
	// reports as a failed subscribe (MAG-2685).
	srv.rejectSubscribes.Store(true)

	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "eth_subscribe", "params": []interface{}{"newHeads"}, "id": 1,
	})
	require.NoError(t, err)
	msg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_subscribe", params: []interface{}{"newHeads"}},
		rawData:               body,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	_, _, err = manager.StartSubscription(ctx, msg, "dapp", "10.0.0.1", "ws-1", nil)
	require.Error(t, err)

	require.Equal(t, int32(0), h.pooledSubscriptionCount(),
		"a failed subscribe left its reserved slot counted on the connection")
	require.Equal(t, 0, h.activeSubscriptionCount())
	require.Equal(t, int64(0), manager.totalSubscriptions.Load())
}
