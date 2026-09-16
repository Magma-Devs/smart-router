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

// subscribe opens a subscription for a client named uid on its own connection context.
func (h *releaseHarness) subscribe(uid string) (ctx context.Context, disconnect context.CancelFunc, replies <-chan *pairingtypes.RelayReply, clientKey string) {
	h.t.Helper()
	ctx, disconnect = context.WithCancel(context.Background())
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
	return ctx, disconnect, replies, h.manager.CreateWebSocketConnectionUniqueKey("dapp", "10.0.0.1", uid)
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

	_, disconnectA, repliesA, keyA := h.subscribe("ws-a")
	_, disconnectB, repliesB, keyB := h.subscribe("ws-b")
	defer disconnectA()
	defer disconnectB()

	require.Equal(t, 1, h.upstreamCalls("eth_subscribe"), "the second client must join, not resubscribe")
	require.True(t, h.attached(keyA))
	require.True(t, h.attached(keyB))
	require.Equal(t, 2, h.manager.idMapper.ClientCount())

	// The joiner closes its socket without unsubscribing.
	disconnectB()

	require.Eventually(t, func() bool { return !h.attached(keyB) }, 5*time.Second, 20*time.Millisecond,
		"a joined client must be released when its connection ends")
	require.True(t, channelClosed(repliesB, 5*time.Second),
		"the joiner's reply channel must close so the ingress forwarder goroutine can end")
	require.Eventually(t, func() bool { return h.manager.idMapper.ClientCount() == 1 }, 5*time.Second, 20*time.Millisecond,
		"the joiner's id counter must go with it")
	require.True(t, h.attached(keyA), "the creator is unaffected")
	require.Equal(t, 0, h.upstreamCalls("eth_unsubscribe"), "the subscription still has a subscriber")

	// The creator leaves too: last one out tells the node.
	disconnectA()
	require.True(t, channelClosed(repliesA, 5*time.Second), "the creator's reply channel must close on disconnect")
	require.Eventually(t, func() bool { return h.upstreamCalls("eth_unsubscribe") == 1 }, 15*time.Second, 50*time.Millisecond,
		"the last client leaving must tell the node to stop pushing")
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return h.manager.idMapper.ClientCount() == 0 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, 0, h.manager.rateLimiter.ClientCount(), "no rate limiter survives its client")
}

// TestCreatorDisconnect_ClosesItsReplyChannel covers the other half of the same leak: the
// creating client had a disconnect watcher, but it removed the client without closing
// the reply channel, so the ingress forwarder goroutine parked forever anyway.
func TestCreatorDisconnect_ClosesItsReplyChannel(t *testing.T) {
	h := newReleaseHarness(t)

	_, disconnectA, repliesA, keyA := h.subscribe("ws-a")
	_, disconnectB, repliesB, keyB := h.subscribe("ws-b")
	defer disconnectB()

	disconnectA()

	require.True(t, channelClosed(repliesA, 5*time.Second), "the creator's reply channel must close on disconnect")
	require.Eventually(t, func() bool { return !h.attached(keyA) }, 5*time.Second, 20*time.Millisecond)
	require.True(t, h.attached(keyB), "the joiner keeps the subscription alive")
	require.Equal(t, 1, h.activeSubscriptionCount())
	require.Equal(t, 0, h.upstreamCalls("eth_unsubscribe"))

	// The joiner still receives pushes after the creator left.
	select {
	case reply, ok := <-repliesB:
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

	ctxA, disconnectA, repliesA, keyA := h.subscribe("ws-a")
	ctxB, disconnectB, repliesB, keyB := h.subscribe("ws-b")
	defer disconnectA()
	defer disconnectB()

	require.Eventually(t, func() bool { return goroutinesIn("handleClientDisconnect") == 2 }, 5*time.Second, 20*time.Millisecond,
		"one disconnect watcher per attached client")

	require.NoError(t, h.manager.UnsubscribeAll(ctxB, "dapp", "10.0.0.1", "ws-b", nil))
	require.False(t, h.attached(keyB))
	require.True(t, channelClosed(repliesB, 5*time.Second))
	require.Eventually(t, func() bool { return goroutinesIn("handleClientDisconnect") == 1 }, 5*time.Second, 20*time.Millisecond,
		"an explicit release must wake the client's watcher instead of leaving it parked until the socket closes")
	require.True(t, h.attached(keyA))
	require.Equal(t, 0, h.upstreamCalls("eth_unsubscribe"))

	require.NoError(t, h.manager.UnsubscribeAll(ctxA, "dapp", "10.0.0.1", "ws-a", nil))
	require.True(t, channelClosed(repliesA, 5*time.Second))
	require.Eventually(t, func() bool { return h.upstreamCalls("eth_unsubscribe") == 1 }, 15*time.Second, 50*time.Millisecond,
		"the last client leaving through UnsubscribeAll must tell the node to stop pushing")
	require.Eventually(t, func() bool { return goroutinesIn("handleClientDisconnect") == 0 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, 0, h.activeSubscriptionCount())
	require.Equal(t, 0, h.manager.idMapper.ClientCount())
	require.Equal(t, 0, h.manager.rateLimiter.ClientCount())
}
