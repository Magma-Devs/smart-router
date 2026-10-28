package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// unsubscribeMessage is the client's eth_unsubscribe for its router id.
func unsubscribeMessage(t *testing.T, routerID string) *mockWSProtocolMessageWithRawData {
	t.Helper()
	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "eth_unsubscribe", "params": []interface{}{routerID}, "id": 2,
	})
	require.NoError(t, err)
	return &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_unsubscribe", params: []interface{}{routerID}},
		rawData:               body,
	}
}

// subscribeOutcome is what StartSubscription handed a client that subscribed off the test
// goroutine.
type subscribeOutcome struct {
	reply   *pairingtypes.RelayReply
	replies <-chan *pairingtypes.RelayReply
	err     error
}

// TestWS_JoinerDuringLastClientUnsubscribeGetsItsOwnSubscription: a client arrives for the same
// params while the last client's eth_unsubscribe is waiting on the node.
//
// Unsubscribe read "last client" in its first critical section and called the node off the
// lock, and the entry stayed in the active map meanwhile. So the arriving client joined it, a
// local step with no node call, and the local cleanup afterwards saw the joiner's mapping and
// tore nothing down. The node had already dropped the subscription: the joiner got no pushes
// and its channel was never closed, so it waited until it disconnected (MAG-4345, repro by
// Anna Rayer). The entry now leaves the map before the node is called, so the arriving client
// creates a subscription of its own on the node.
func TestWS_JoinerDuringLastClientUnsubscribeGetsItsOwnSubscription(t *testing.T) {
	h := newReleaseHarness(t)
	a := h.subscribe("ws-a")
	t.Cleanup(a.disconnect)
	activeSub, upstreamID := onlySubscription(t, h)
	hashedParams := activeSub.hashedParams
	countReplies(a)

	// The node reads and applies the unsubscribe but cannot answer yet.
	h.srv.writeMu.Lock()
	unlockNode := sync.OnceFunc(func() { h.srv.writeMu.Unlock() })
	t.Cleanup(unlockNode)

	errCh := make(chan error, 1)
	go func() {
		_, err := h.manager.Unsubscribe(a.ctx, unsubscribeMessage(t, a.routerID), "dapp", "10.0.0.1", a.uid, nil)
		errCh <- err
	}()
	require.Eventually(t, func() bool { return h.upstreamCalls("eth_unsubscribe") == 1 }, 5*time.Second, 5*time.Millisecond)
	require.False(t, h.srv.has(upstreamID), "the node already dropped the subscription")
	require.Equal(t, 0, h.activeSubscriptionCount(),
		"a subscription the router has told the node to drop must take no new clients")

	// b arrives while the node has not answered a. It must not join the dying entry, so it
	// needs the node, which is still stalled: it subscribes in the background.
	bCtx, bDisconnect := context.WithCancel(context.Background())
	t.Cleanup(bDisconnect)
	bKey := h.manager.CreateWebSocketConnectionUniqueKey("dapp", "10.0.0.1", "ws-b")
	bCh := make(chan subscribeOutcome, 1)
	go func() {
		body, _ := json.Marshal(map[string]interface{}{
			"jsonrpc": "2.0", "method": "eth_subscribe", "params": []interface{}{"newHeads"}, "id": 1,
		})
		msg := &mockWSProtocolMessageWithRawData{
			mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_subscribe", params: []interface{}{"newHeads"}},
			rawData:               body,
		}
		reply, replies, err := h.manager.StartSubscription(bCtx, msg, "dapp", "10.0.0.1", "ws-b", nil)
		bCh <- subscribeOutcome{reply: reply, replies: replies, err: err}
	}()
	require.Eventually(t, func() bool {
		h.manager.lock.RLock()
		defer h.manager.lock.RUnlock()
		_, creating := h.manager.pendingSubscriptions[hashedParams]
		return creating
	}, 5*time.Second, 5*time.Millisecond, "the arriving client must create a subscription of its own, not join the dying one")
	require.False(t, h.attached(bKey), "b must not be attached to the subscription the node dropped")

	unlockNode()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("Unsubscribe never returned")
	}
	var b subscribeOutcome
	select {
	case b = <-bCh:
	case <-time.After(15 * time.Second):
		t.Fatal("b's subscribe never returned")
	}
	require.NoError(t, b.err)
	require.NotNil(t, b.reply)
	require.NotNil(t, b.replies)
	require.Equal(t, 2, h.upstreamCalls("eth_subscribe"), "b subscribed on the node itself")
	require.Equal(t, 1, h.activeSubscriptionCount())
	require.True(t, h.attached(bKey))
	require.Equal(t, int64(1), h.manager.totalSubscriptions.Load())
	bSub, bUpstreamID := onlySubscription(t, h)
	require.NotSame(t, activeSub, bSub, "b holds a subscription of its own, not the one the node dropped")
	require.NotEqual(t, upstreamID, bUpstreamID)
	require.True(t, h.srv.has(bUpstreamID), "b's subscription is live on the node")

	select {
	case reply, ok := <-b.replies:
		require.True(t, ok, "b's channel must stay open")
		require.NotNil(t, reply, "b receives the node's pushes")
	case <-time.After(5 * time.Second):
		t.Fatalf("b is attached to a subscription the node does not publish (pushes every %s)", h.srv.messageInterval)
	}

	// Both clients gone: nothing is left on the node, in the pool or in the counters.
	bDisconnect()
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return h.srv.liveSubscriptions() == 0 }, 5*time.Second, 10*time.Millisecond,
		"the node still publishes a subscription nobody reads")
	require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond,
		"a pooled connection still counts a subscription")
	require.Equal(t, int64(0), h.manager.totalSubscriptions.Load())
	require.Eventually(t, func() bool { return goroutinesIn("listenForUpstreamMessages") == 0 }, 5*time.Second, 10*time.Millisecond)
}

// TestWS_LastClientUnsubscribeFailingUpstreamStillEndsTheSubscription: the node applies the
// last client's unsubscribe but never answers it, as some gateways do, and the call runs out
// its deadline. The client asked to leave and the router had already retired the subscription
// before the call, so it ends locally all the same: keeping the client on a subscription the
// node has stopped publishing would park it on exactly the dead one MAG-4345 is about. The
// failure still reaches the client with its reply. Before the fix the call's error returned
// early and left the client attached to the dead subscription.
func TestWS_LastClientUnsubscribeFailingUpstreamStillEndsTheSubscription(t *testing.T) {
	h := newReleaseHarness(t)
	a := h.subscribe("ws-a")
	t.Cleanup(a.disconnect)
	_, upstreamID := onlySubscription(t, h)
	countReplies(a)

	h.srv.unsubscribeUnanswered.Store(true)
	callCtx, cancel := context.WithTimeout(a.ctx, 300*time.Millisecond)
	defer cancel()
	_, err := h.manager.Unsubscribe(callCtx, unsubscribeMessage(t, a.routerID), "dapp", "10.0.0.1", a.uid, nil)
	require.Error(t, err, "the failed node call reaches the client")
	require.NotErrorIs(t, err, common.SubscriptionNotFoundError, "the subscription was the client's to end")
	require.Equal(t, 1, h.upstreamCalls("eth_unsubscribe"))
	require.False(t, h.srv.has(upstreamID), "the node applied the unsubscribe it did not answer")

	require.False(t, h.attached(a.clientKey), "the client asked to leave and left")
	require.Equal(t, 0, h.activeSubscriptionCount(), "the subscription ended locally all the same")
	require.Equal(t, int64(0), h.manager.totalSubscriptions.Load())
	require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return goroutinesIn("listenForUpstreamMessages") == 0 }, 5*time.Second, 10*time.Millisecond)
	_, err = h.manager.Unsubscribe(a.ctx, unsubscribeMessage(t, a.routerID), "dapp", "10.0.0.1", a.uid, nil)
	require.ErrorIs(t, err, common.SubscriptionNotFoundError, "a second unsubscribe finds nothing to end")
}
