package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// releaseHarness drives one DirectWSSubscriptionManager against the mock node with
// several clients, each on its own connection context, the way the ingress does.
type releaseHarness struct {
	t       *testing.T
	srv     *mockSubscriptionServer
	manager *DirectWSSubscriptionManager
}

func newReleaseHarness(t *testing.T) *releaseHarness {
	t.Helper()
	srv := newMockSubscriptionServer()
	srv.messageInterval = 20 * time.Millisecond
	t.Cleanup(srv.Close)

	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{{Url: srv.URL()}},
		nil, nil, nil,
	)
	return &releaseHarness{t: t, srv: srv, manager: manager}
}

// subscription is what one client got back from subscribe.
type subscription struct {
	ctx        context.Context
	disconnect context.CancelFunc
	replies    <-chan *pairingtypes.RelayReply
	clientKey  string
	routerID   string
	uid        string
}

// subscribe opens a subscription for a client named uid on its own connection context.
func (h *releaseHarness) subscribe(uid string) subscription {
	h.t.Helper()
	ctx, disconnect := context.WithCancel(context.Background())
	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_subscribe",
		"params":  []interface{}{"newHeads"},
		"id":      1,
	})
	require.NoError(h.t, err)
	msg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_subscribe", params: []interface{}{"newHeads"}},
		rawData:               body,
	}
	reply, replies, err := h.manager.StartSubscription(ctx, msg, "dapp", "10.0.0.1", uid, nil)
	require.NoError(h.t, err)
	require.NotNil(h.t, reply)
	require.NotNil(h.t, replies, "every subscriber, joiner or creator, gets a reply channel")
	var resp struct {
		Result string `json:"result"`
	}
	require.NoError(h.t, json.Unmarshal(reply.Data, &resp))
	require.NotEmpty(h.t, resp.Result, "the subscribe reply carries the router id")
	return subscription{
		ctx:        ctx,
		disconnect: disconnect,
		replies:    replies,
		clientKey:  h.manager.CreateWebSocketConnectionUniqueKey("dapp", "10.0.0.1", uid),
		routerID:   resp.Result,
		uid:        uid,
	}
}

// unsubscribe sends the client's explicit eth_unsubscribe for its router id.
func (h *releaseHarness) unsubscribe(sub subscription) {
	h.t.Helper()
	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_unsubscribe",
		"params":  []interface{}{sub.routerID},
		"id":      2,
	})
	require.NoError(h.t, err)
	msg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_unsubscribe", params: []interface{}{sub.routerID}},
		rawData:               body,
	}
	_, err = h.manager.Unsubscribe(sub.ctx, msg, "dapp", "10.0.0.1", sub.uid, nil)
	require.NoError(h.t, err)
}

// connectionClosed is what the ingress does when a client's socket ends.
func (h *releaseHarness) connectionClosed(sub subscription) {
	h.t.Helper()
	require.NoError(h.t, h.manager.UnsubscribeAll(sub.ctx, "dapp", "10.0.0.1", sub.uid, nil))
}

func (h *releaseHarness) connectedClientCount() int {
	h.manager.lock.RLock()
	defer h.manager.lock.RUnlock()
	return len(h.manager.connectedClients)
}

func (h *releaseHarness) upstreamCalls(method string) int {
	n := 0
	for _, m := range h.srv.ObservedMethods() {
		if m == method {
			n++
		}
	}
	return n
}

// attached reports whether clientKey is still counted on the (single) active subscription.
func (h *releaseHarness) attached(clientKey string) bool {
	h.manager.lock.RLock()
	defer h.manager.lock.RUnlock()
	for _, sub := range h.manager.activeSubscriptions {
		if _, ok := sub.connectedClients[clientKey]; ok {
			return true
		}
	}
	return false
}

func (h *releaseHarness) activeSubscriptionCount() int {
	h.manager.lock.RLock()
	defer h.manager.lock.RUnlock()
	return len(h.manager.activeSubscriptions)
}

// channelClosed drains pushes until the channel closes, or reports false on timeout.
func channelClosed(replies <-chan *pairingtypes.RelayReply, within time.Duration) bool {
	deadline := time.After(within)
	for {
		select {
		case _, ok := <-replies:
			if !ok {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// goroutinesIn counts live goroutines whose stack mentions fn.
func goroutinesIn(fn string) int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), fn)
}

// TestJoinedClientDisconnect_IsReleased is the regression for MAG-3722's largest ws leak.
// The second subscriber to a query joins the first one's upstream subscription, and
// before this fix nothing watched a joiner's connection: when it closed its socket the
// reply channel stayed open (parking the ingress forwarder goroutine for good), the
// joiner kept counting as connected, and the upstream subscription outlived every real
// subscriber.
func TestJoinedClientDisconnect_IsReleased(t *testing.T) {
	h := newReleaseHarness(t)

	a := h.subscribe("ws-a")
	b := h.subscribe("ws-b")
	defer a.disconnect()
	defer b.disconnect()

	require.Equal(t, 1, h.upstreamCalls("eth_subscribe"), "the second client must join, not resubscribe")
	require.True(t, h.attached(a.clientKey))
	require.True(t, h.attached(b.clientKey))
	require.NotEqual(t, a.routerID, b.routerID, "each client gets its own id on the shared subscription")

	// The joiner closes its socket without unsubscribing.
	b.disconnect()

	require.Eventually(t, func() bool { return !h.attached(b.clientKey) }, 5*time.Second, 20*time.Millisecond,
		"a joined client must be released when its connection ends")
	require.True(t, channelClosed(b.replies, 5*time.Second),
		"the joiner's reply channel must close so the ingress forwarder goroutine can end")
	require.True(t, h.attached(a.clientKey), "the creator is unaffected")
	require.Equal(t, 0, h.upstreamCalls("eth_unsubscribe"), "the subscription still has a subscriber")

	// The creator leaves too: last one out tells the node.
	a.disconnect()
	require.True(t, channelClosed(a.replies, 5*time.Second), "the creator's reply channel must close on disconnect")
	require.Eventually(t, func() bool { return h.upstreamCalls("eth_unsubscribe") == 1 }, 15*time.Second, 50*time.Millisecond,
		"the last client leaving must tell the node to stop pushing")
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return h.connectedClientCount() == 0 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, 0, h.manager.stickyStore.Len(), "no sticky endpoint survives its client")
	require.Equal(t, 0, h.manager.rateLimiter.ClientCount(), "no rate limiter survives its client")
}

// TestCreatorDisconnect_ClosesItsReplyChannel covers the other half of the same leak: the
// creating client had a disconnect watcher, but it removed the client without closing
// the reply channel, so the ingress forwarder goroutine parked forever anyway.
func TestCreatorDisconnect_ClosesItsReplyChannel(t *testing.T) {
	h := newReleaseHarness(t)

	a := h.subscribe("ws-a")
	b := h.subscribe("ws-b")
	defer b.disconnect()

	a.disconnect()

	require.True(t, channelClosed(a.replies, 5*time.Second), "the creator's reply channel must close on disconnect")
	require.Eventually(t, func() bool { return !h.attached(a.clientKey) }, 5*time.Second, 20*time.Millisecond)
	require.True(t, h.attached(b.clientKey), "the joiner keeps the subscription alive")
	require.Equal(t, 1, h.activeSubscriptionCount())
	require.Equal(t, 0, h.upstreamCalls("eth_unsubscribe"))

	// The joiner still receives pushes after the creator left.
	select {
	case reply, ok := <-b.replies:
		require.True(t, ok, "the joiner's channel must stay open")
		require.NotNil(t, reply)
	case <-time.After(5 * time.Second):
		t.Fatal("the joiner must keep receiving pushes after the creator disconnects")
	}
}

// TestUnsubscribeAll_ReleasesWatchersAndTellsNode covers the path the ingress now takes
// when a connection ends. Two things were missing: an explicit release left the
// client's disconnect watcher parked until the socket closed, and the last client
// leaving through UnsubscribeAll never told the node.
func TestUnsubscribeAll_ReleasesWatchersAndTellsNode(t *testing.T) {
	h := newReleaseHarness(t)

	a := h.subscribe("ws-a")
	b := h.subscribe("ws-b")
	defer a.disconnect()
	defer b.disconnect()

	require.Eventually(t, func() bool { return goroutinesIn("handleClientDisconnect") == 2 }, 5*time.Second, 20*time.Millisecond,
		"one disconnect watcher per attached client")

	h.connectionClosed(b)
	require.False(t, h.attached(b.clientKey))
	require.True(t, channelClosed(b.replies, 5*time.Second))
	require.Eventually(t, func() bool { return goroutinesIn("handleClientDisconnect") == 1 }, 5*time.Second, 20*time.Millisecond,
		"an explicit release must wake the client's watcher instead of leaving it parked until the socket closes")
	require.True(t, h.attached(a.clientKey))
	require.Equal(t, 0, h.upstreamCalls("eth_unsubscribe"))

	h.connectionClosed(a)
	require.True(t, channelClosed(a.replies, 5*time.Second))
	require.Eventually(t, func() bool { return h.upstreamCalls("eth_unsubscribe") == 1 }, 15*time.Second, 50*time.Millisecond,
		"the last client leaving through UnsubscribeAll must tell the node to stop pushing")
	require.Eventually(t, func() bool { return goroutinesIn("handleClientDisconnect") == 0 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, 0, h.activeSubscriptionCount())
	require.Equal(t, 0, h.connectedClientCount())
	require.Equal(t, 0, h.manager.stickyStore.Len())
	require.Equal(t, 0, h.manager.rateLimiter.ClientCount())
}

// TestUnsubscribeThenDisconnect_LeavesNothingBehind covers the commonest well-behaved
// client: subscribe, eth_unsubscribe, close the socket. The explicit unsubscribe empties
// the client's subscription set, and the connection-close release then found nothing to
// do and returned before handing back the client's sticky endpoint and rate limiters,
// so a well-behaved client leaked those per connection (MAG-3722, review finding).
func TestUnsubscribeThenDisconnect_LeavesNothingBehind(t *testing.T) {
	h := newReleaseHarness(t)

	a := h.subscribe("ws-a")
	defer a.disconnect()
	require.Equal(t, 1, h.manager.stickyStore.Len(), "the creator pins the endpoint it subscribed on")

	h.unsubscribe(a)
	require.True(t, channelClosed(a.replies, 5*time.Second), "an explicit unsubscribe closes the reply channel")
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return goroutinesIn("handleClientDisconnect") == 0 }, 5*time.Second, 20*time.Millisecond,
		"the disconnect watcher must not stay parked once the client unsubscribed")
	require.Equal(t, 1, h.upstreamCalls("eth_unsubscribe"))
	require.Equal(t, 0, h.connectedClientCount())
	require.Equal(t, 1, h.manager.stickyStore.Len(), "the sticky endpoint outlives an unsubscribe while the socket is open")

	// The socket closes. The ingress calls UnsubscribeAll for a client that has nothing
	// left to unsubscribe, and that must still hand back its per-client state.
	h.connectionClosed(a)
	require.Equal(t, 0, h.manager.stickyStore.Len(), "closing the socket releases the sticky endpoint")
	require.Equal(t, 0, h.manager.rateLimiter.ClientCount(), "closing the socket releases the rate limiters")
	require.Equal(t, 0, h.connectedClientCount())
	require.Equal(t, 1, h.upstreamCalls("eth_unsubscribe"), "nothing is unsubscribed twice")
}
