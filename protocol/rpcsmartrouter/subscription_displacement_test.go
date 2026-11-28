package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOutOfRotationWhileOneIsLeft is the question a join asks before it attaches a client to
// a live subscription (MAG-5405), over both managers, since both read the same helper.
//
// The last two rows are the ones that keep a working router working: with nothing in rotation
// left, joining stays allowed, which is the same last chance selectEndpoint gives a new
// subscription; and with no ranking wired at all — every unit test that does not set one, and
// any build where the session manager has not handed one over yet — nothing is ever displaced.
func TestOutOfRotationWhileOneIsLeft(t *testing.T) {
	const inRotation, headStalled, switchedOff = inRotationRank, 1, 2

	scenarios := []struct {
		name string
		// ranks is the router's ranking of each endpoint, by name. Absent means no ranking is
		// wired at all.
		ranks map[string]int
		// url is the endpoint the live subscription is on.
		url  string
		want bool
	}{
		{name: "no ranking wired: nothing is out of rotation", url: "primary-1"},
		{name: "in rotation", ranks: map[string]int{"primary-1": inRotation, "primary-2": inRotation}, url: "primary-1"},
		{
			name:  "head-stalled while a primary is in rotation",
			ranks: map[string]int{"primary-1": headStalled, "primary-2": inRotation},
			url:   "primary-1", want: true,
		},
		{
			name:  "switched off while a primary is in rotation",
			ranks: map[string]int{"primary-1": switchedOff, "primary-2": inRotation},
			url:   "primary-1", want: true,
		},
		{
			name:  "out of rotation while only a backup is in rotation",
			ranks: map[string]int{"primary-1": switchedOff, "primary-2": switchedOff, "backup-1": inRotation},
			url:   "primary-1", want: true,
		},
		{
			name:  "out of rotation and nothing in rotation is left",
			ranks: map[string]int{"primary-1": switchedOff, "primary-2": headStalled, "backup-1": switchedOff},
			url:   "primary-1",
		},
		{
			name:  "head-stalled while only a switched-off url is left",
			ranks: map[string]int{"primary-1": headStalled, "primary-2": switchedOff, "backup-1": switchedOff},
			url:   "primary-1",
		},
		{
			name:  "a url that is not in the tiers at all is judged by its own rank",
			ranks: map[string]int{"departed": switchedOff, "primary-1": inRotation},
			url:   "departed", want: true,
		},
		{
			// The session manager ranks a url it holds no endpoint for in rotation
			// (URLLendRank), and that is what a url whose provider left the pairing reads.
			name:  "a url that left the tiers is out of rotation, whatever it ranks",
			ranks: map[string]int{"primary-1": inRotation},
			url:   "departed", want: true,
		},
		{
			name:  "a url that left the tiers while nothing in rotation is left",
			ranks: map[string]int{"primary-1": switchedOff, "primary-2": switchedOff, "backup-1": switchedOff},
			url:   "departed",
		},
	}

	for _, manager := range subscriptionManagerCases() {
		for _, scenario := range scenarios {
			t.Run(manager.name+"/"+scenario.name, func(t *testing.T) {
				primary := []*common.NodeUrl{{Url: manager.url("primary-1")}, {Url: manager.url("primary-2")}}
				backup := []*common.NodeUrl{{Url: manager.url("backup-1")}}
				cascade := manager.build(primary, backup, nil)
				if scenario.ranks != nil {
					ranks := map[string]int{}
					for name, rank := range scenario.ranks {
						ranks[manager.url(name)] = rank
					}
					cascade.setURLLendRank(func(url string) int { return ranks[url] })
				}

				assert.Equal(t, scenario.want, cascade.outOfRotationWhileOneIsLeft(manager.url(scenario.url)))
			})
		}
	}
}

// TestDisplacedSubscriptionKeyCannotBeAParamsKey pins what makes displacement safe: the key a
// subscription takes when the params key is held must be one no client can ever arrive with,
// or a client would find a subscription the manager has decided it may not join.
//
// Asserted against each manager's own hashing rather than against a written-down shape, so a
// change to either one fails here instead of silently handing arriving clients a displaced
// subscription.
func TestDisplacedSubscriptionKeyCannotBeAParamsKey(t *testing.T) {
	displaced := displacedSubscriptionKey(1, "the-params-key")

	t.Run("websocket keys are a fixed-width digest", func(t *testing.T) {
		short := rpcclient.CreateHashFromParams([]byte("eth_subscribe:[\"newHeads\"]"))
		long := rpcclient.CreateHashFromParams([]byte(strings.Repeat("eth_subscribe:[\"logs\"]", 500)))
		require.Len(t, long, len(short), "a params key is a digest, so every one is the same width")
		require.Greater(t, len(displaced), len(short), "a displaced key must not be that width")
	})

	t.Run("grpc keys hold hex digits alone", func(t *testing.T) {
		manager := NewDirectGRPCSubscriptionManager(nil, "SUI", spectypes.APIInterfaceGrpc, nil, nil, nil, nil)
		paramsKey := manager.hashSubscriptionParams(streamingMethodPath, []byte("{}"))
		require.NotEmpty(t, paramsKey)
		for _, b := range []byte(paramsKey) {
			require.True(t, strings.IndexByte("0123456789abcdefABCDEF", b) >= 0,
				"a params key is hex, so a key holding anything else cannot be one")
		}
		require.Contains(t, displaced, displacedSubscriptionKeyPrefix)
		require.False(t, isHex(displaced), "a displaced key must hold something that is not a hex digit")
	})

	t.Run("each displacement takes a key of its own", func(t *testing.T) {
		require.NotEqual(t, displaced, displacedSubscriptionKey(2, "the-params-key"))
	})
}

func isHex(s string) bool {
	for _, b := range []byte(s) {
		if strings.IndexByte("0123456789abcdefABCDEF", b) < 0 {
			return false
		}
	}
	return true
}

// --- WebSocket ---------------------------------------------------------------

// displacementHarness is a manager over two nodes, so a subscription can be left on one while
// the router takes that one out of rotation.
type displacementHarness struct {
	*releaseHarness
	first  *mockSubscriptionServer
	second *mockSubscriptionServer
	// ranks is swapped whole rather than written in place: selection reads the ranking off
	// the manager's lock, so a map mutated under the test's feet would be a data race.
	ranks atomic.Pointer[map[string]int]
}

func newDisplacementHarness(t *testing.T) *displacementHarness {
	t.Helper()

	node := func(subscriptionID string) *mockSubscriptionServer {
		srv := newMockSubscriptionServer()
		srv.messageInterval = 20 * time.Millisecond
		// Distinct ids per node: the id mapper is keyed by upstream id, and two live
		// subscriptions answering "0xfff…" would share its entries.
		srv.subscriptionID = subscriptionID
		t.Cleanup(srv.Close)
		return srv
	}
	first, second := node("0x"+strings.Repeat("a", 32)), node("0x"+strings.Repeat("b", 32))

	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{{Url: first.URL()}, {Url: second.URL()}},
		nil, nil, nil,
	)
	t.Cleanup(manager.Close)

	h := &displacementHarness{
		releaseHarness: &releaseHarness{t: t, srv: first, manager: manager},
		first:          first,
		second:         second,
	}
	h.ranks.Store(&map[string]int{})
	manager.SetURLLendRank(func(url string) int { return (*h.ranks.Load())[url] })
	return h
}

// takeOutOfRotation ranks a node's url the way the router ranks one it has switched off after
// repeated node errors.
func (h *displacementHarness) takeOutOfRotation(nodes ...*mockSubscriptionServer) {
	ranks := map[string]int{}
	for url, rank := range *h.ranks.Load() {
		ranks[url] = rank
	}
	for _, node := range nodes {
		ranks[node.URL()] = 2
	}
	h.ranks.Store(&ranks)
}

// subscribeCalls counts the eth_subscribe frames one node was sent.
func subscribeCalls(node *mockSubscriptionServer) int {
	n := 0
	for _, method := range node.ObservedMethods() {
		if method == "eth_subscribe" {
			n++
		}
	}
	return n
}

// urlOfSubscriptionFor returns the url the subscription a client is on is open on.
func (h *displacementHarness) urlOfSubscriptionFor(clientKey string) string {
	h.manager.lock.RLock()
	defer h.manager.lock.RUnlock()
	for _, sub := range h.manager.activeSubscriptions {
		if _, ok := sub.connectedClients[clientKey]; ok {
			return upstreamURLOf(sub)
		}
	}
	return ""
}

func (h *displacementHarness) activeSubscriptionCountLocked() int {
	h.manager.lock.RLock()
	defer h.manager.lock.RUnlock()
	return len(h.manager.activeSubscriptions)
}

func (h *displacementHarness) joinableSubscriptionCount() int {
	h.manager.lock.RLock()
	defer h.manager.lock.RUnlock()
	return len(h.manager.joinableSubscriptions)
}

// requirePush fails unless the client is still being served.
func requirePush(t *testing.T, replies <-chan *pairingtypes.RelayReply, who string) {
	t.Helper()
	select {
	case reply, open := <-replies:
		require.True(t, open, who+"'s channel closed instead of delivering a push")
		require.NotNil(t, reply)
	case <-time.After(10 * time.Second):
		t.Fatal(who + " stopped receiving pushes")
	}
}

// TestWSJoin_NewClientDoesNotJoinASubscriptionOnAUrlOutOfRotation is MAG-5405 on the
// WebSocket side.
//
// A client already holds newHeads on the first node when the router takes that node out of
// rotation. The arriving client must not be attached to it — it would get that node's frozen
// heads — and the node in rotation must get the eth_subscribe. The first client stays where it
// is: moving a live subscription is separate work.
func TestWSJoin_NewClientDoesNotJoinASubscriptionOnAUrlOutOfRotation(t *testing.T) {
	h := newDisplacementHarness(t)

	first := h.subscribe("ws-first")
	t.Cleanup(first.disconnect)
	require.Equal(t, 1, subscribeCalls(h.first), "the first client's subscription opens on the first node")
	require.Equal(t, 0, subscribeCalls(h.second))
	require.Equal(t, h.first.URL(), h.urlOfSubscriptionFor(first.clientKey))

	h.takeOutOfRotation(h.first)

	second := h.subscribe("ws-second")
	t.Cleanup(second.disconnect)

	require.Equal(t, 1, subscribeCalls(h.second), "the arriving client's subscription opens on the node in rotation")
	require.Equal(t, 1, subscribeCalls(h.first), "and the node out of rotation is asked nothing more")
	require.Equal(t, h.second.URL(), h.urlOfSubscriptionFor(second.clientKey))
	require.Equal(t, h.first.URL(), h.urlOfSubscriptionFor(first.clientKey),
		"the client already on the subscription stays on it")

	require.Equal(t, 2, h.activeSubscriptionCountLocked(), "two upstream subscriptions, one per node")
	require.Equal(t, int64(2), h.manager.totalSubscriptions.Load())
	require.Equal(t, 1, h.joinableSubscriptionCount(),
		"only the subscription in rotation is the one a further client joins")

	// Both are still served, each by its own node.
	requirePush(t, first.replies, "the client on the node out of rotation")
	requirePush(t, second.replies, "the arriving client")

	// A third client joins the one in rotation rather than opening a third subscription.
	third := h.subscribe("ws-third")
	t.Cleanup(third.disconnect)
	require.Equal(t, 1, subscribeCalls(h.second), "a further client joins the subscription in rotation")
	require.Equal(t, 2, h.activeSubscriptionCountLocked())
	require.Equal(t, h.second.URL(), h.urlOfSubscriptionFor(third.clientKey))
}

// TestWSJoin_NewClientDoesNotJoinASubscriptionOnAUrlThatLeftTheTiers: the first node's
// provider leaves the live pairing (a failed re-verification), so its url leaves both tiers and
// the session manager, which holds no endpoint for it any more, ranks it in rotation. The
// subscription on it keeps its client, and the arriving client opens one on the node that is
// still in the tiers, as a new subscription would.
func TestWSJoin_NewClientDoesNotJoinASubscriptionOnAUrlThatLeftTheTiers(t *testing.T) {
	h := newDisplacementHarness(t)

	first := h.subscribe("ws-first")
	t.Cleanup(first.disconnect)
	require.Equal(t, 1, subscribeCalls(h.first))

	h.manager.SetEndpoints([]*common.NodeUrl{{Url: h.second.URL()}}, nil)

	second := h.subscribe("ws-second")
	t.Cleanup(second.disconnect)

	require.Equal(t, 1, subscribeCalls(h.second), "the arriving client's subscription opens on the node still in the tiers")
	require.Equal(t, 1, subscribeCalls(h.first))
	require.Equal(t, h.second.URL(), h.urlOfSubscriptionFor(second.clientKey))
	require.Equal(t, h.first.URL(), h.urlOfSubscriptionFor(first.clientKey), "the client already on it stays")
	requirePush(t, first.replies, "the client on the url that left the tiers")
	requirePush(t, second.replies, "the arriving client")
}

// TestWSJoin_StillJoinsWhenNoUrlIsInRotation keeps the last chance: with every url out of
// rotation there is nowhere better to put a new subscription, so joining the live one — a node
// behind the chain, or one that kept failing — beats refusing the client.
func TestWSJoin_StillJoinsWhenNoUrlIsInRotation(t *testing.T) {
	h := newDisplacementHarness(t)

	first := h.subscribe("ws-first")
	t.Cleanup(first.disconnect)
	h.takeOutOfRotation(h.first, h.second)

	second := h.subscribe("ws-second")
	t.Cleanup(second.disconnect)

	require.Equal(t, 1, subscribeCalls(h.first), "the arriving client joined rather than subscribing again")
	require.Equal(t, 0, subscribeCalls(h.second))
	require.Equal(t, 1, h.activeSubscriptionCountLocked())
	require.Equal(t, int64(1), h.manager.totalSubscriptions.Load())
	require.Equal(t, h.first.URL(), h.urlOfSubscriptionFor(second.clientKey))
	requirePush(t, second.replies, "the client that joined the subscription out of rotation")
}

// TestWSJoin_ClientHoldingTheSubscriptionIsNotDisplaced: a client re-sending a subscribe it
// already holds gets its own reply back, even on a url out of rotation. It is already on that
// subscription, so there is nothing to keep it off, and opening a second one for it would
// leave it with two ids for one feed.
func TestWSJoin_ClientHoldingTheSubscriptionIsNotDisplaced(t *testing.T) {
	h := newDisplacementHarness(t)

	first := h.subscribe("ws-first")
	t.Cleanup(first.disconnect)
	h.takeOutOfRotation(h.first)

	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "method": "eth_subscribe", "params": []any{"newHeads"}, "id": 7,
	})
	require.NoError(t, err)
	reply, replies, err := h.manager.StartSubscription(first.ctx, &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_subscribe", params: []any{"newHeads"}},
		rawData:               body,
	}, "dapp", "10.0.0.1", "ws-first", nil)
	require.NoError(t, err)
	require.Nil(t, replies, "a client already on the subscription is not attached a second time")
	var answer struct {
		Result string `json:"result"`
	}
	require.NoError(t, json.Unmarshal(reply.Data, &answer))
	require.Equal(t, first.routerID, answer.Result, "the client gets the id it already holds")
	require.Equal(t, 1, subscribeCalls(h.first))
	require.Equal(t, 0, subscribeCalls(h.second), "no second subscription is opened for it")
	require.Equal(t, 1, h.activeSubscriptionCountLocked())
	require.Equal(t, 1, h.joinableSubscriptionCount(), "and the subscription is not displaced")
}

// TestWSJoin_DisplacedSubscriptionIsStillReleasedByItsOwnClients is the regression that
// matters most about the mechanism rather than the behaviour.
//
// A displaced subscription keeps its key in activeSubscriptions, because its listener, its
// client watchers and its deferred cleanup all carry that key. Were it re-keyed instead, each
// of them would be left looking at the subscription that took the key — the one the arriving
// client created — so the displaced subscription's clients would never be released, its node
// would never be told to stop publishing, and the count would drift.
func TestWSJoin_DisplacedSubscriptionIsStillReleasedByItsOwnClients(t *testing.T) {
	h := newDisplacementHarness(t)

	first := h.subscribe("ws-first")
	t.Cleanup(first.disconnect)
	h.takeOutOfRotation(h.first)
	second := h.subscribe("ws-second")
	t.Cleanup(second.disconnect)
	require.Equal(t, 2, h.activeSubscriptionCountLocked())

	// The displaced subscription's client hangs up: its own node is told, and only its own
	// subscription goes.
	h.connectionClosed(first)

	require.Eventually(t, func() bool { return h.activeSubscriptionCountLocked() == 1 },
		5*time.Second, 10*time.Millisecond, "the displaced subscription was never released")
	require.Equal(t, int64(1), h.manager.totalSubscriptions.Load(), "the count follows the subscriptions")
	require.Eventually(t, func() bool { return subscribeCalls(h.first) == 1 && h.first.liveSubscriptions() == 0 },
		5*time.Second, 10*time.Millisecond, "the node out of rotation was never told to stop publishing")
	require.False(t, h.attached(first.clientKey))

	// The arriving client is untouched by any of it.
	require.True(t, h.attached(second.clientKey))
	require.Equal(t, h.second.URL(), h.urlOfSubscriptionFor(second.clientKey))
	requirePush(t, second.replies, "the arriving client")

	// And it can still leave through its own explicit unsubscribe.
	h.unsubscribe(second)
	require.Eventually(t, func() bool { return h.activeSubscriptionCountLocked() == 0 },
		5*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(0), h.manager.totalSubscriptions.Load())
	require.Equal(t, 0, h.joinableSubscriptionCount(), "nothing is left for a client to join")
}

// TestWSJoin_DisplacedSubscriptionAnswersItsOwnClientsUnsubscribe: the client on the displaced
// subscription unsubscribes explicitly, by the router id it was given. The scan that finds its
// subscription runs over activeSubscriptions, which a displaced subscription is still in.
func TestWSJoin_DisplacedSubscriptionAnswersItsOwnClientsUnsubscribe(t *testing.T) {
	h := newDisplacementHarness(t)

	first := h.subscribe("ws-first")
	t.Cleanup(first.disconnect)
	h.takeOutOfRotation(h.first)
	second := h.subscribe("ws-second")
	t.Cleanup(second.disconnect)

	h.unsubscribe(first)

	require.Eventually(t, func() bool { return h.first.liveSubscriptions() == 0 },
		5*time.Second, 10*time.Millisecond, "the node was never told to drop the displaced subscription")
	require.Equal(t, 1, h.activeSubscriptionCountLocked())
	require.False(t, h.attached(first.clientKey))
	require.True(t, h.attached(second.clientKey))
}

// --- gRPC --------------------------------------------------------------------

// newGRPCManagerAgainstUpstreams points a manager at several fake upstreams, seeding each
// endpoint's descriptor cache as newManagerAgainstUpstream does for one, so no subscribe here
// ever calls reflection.
func newGRPCManagerAgainstUpstreams(t *testing.T, addrs ...string) (*DirectGRPCSubscriptionManager, []*common.NodeUrl) {
	t.Helper()

	endpoints := make([]*common.NodeUrl, 0, len(addrs))
	for _, addr := range addrs {
		endpoints = append(endpoints, &common.NodeUrl{
			Url:        "grpc://" + addr,
			GrpcConfig: common.GrpcConfig{AllowInsecure: true},
		})
	}

	manager := NewDirectGRPCSubscriptionManager(nil, "SUI", spectypes.APIInterfaceGrpc,
		endpoints, nil, nil, nil)
	for _, endpoint := range endpoints {
		pool, err := manager.getOrCreatePool(endpoint)
		require.NoError(t, err)
		conn, err := pool.take(context.Background(), false)
		require.NoError(t, err)
		conn.descriptors.cache.Store("grpc.reflection.v1.ServerReflection.ServerReflectionInfo", streamingMethodDescriptor(t))
	}
	return manager, endpoints
}

// TestGRPCJoin_NewClientDoesNotJoinAStreamOnAUrlOutOfRotation is MAG-5405 on the gRPC side.
// Sharing is on by default, so the arriving client would otherwise be handed the stream on the
// url the router has stopped sending relays to.
func TestGRPCJoin_NewClientDoesNotJoinAStreamOnAUrlOutOfRotation(t *testing.T) {
	firstNode, secondNode := startFakeStreamingUpstream(t), startFakeStreamingUpstream(t)
	manager, endpoints := newGRPCManagerAgainstUpstreams(t, firstNode.addr, secondNode.addr)
	defer manager.Stop()

	ctx := context.Background()
	message := newGrpcSubscriptionMessage(t)

	_, firstReplies, err := manager.StartSubscription(ctx, message, "dapp", "1.1.1.1", "stream-1", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, firstNode)

	ranks := map[string]int{endpoints[0].Url: 2}
	manager.SetURLLendRank(func(url string) int { return ranks[url] })

	_, secondReplies, err := manager.StartSubscription(ctx, message, "dapp", "2.2.2.2", "stream-2", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, secondNode)

	manager.lock.RLock()
	active, joinable := len(manager.activeSubscriptions), len(manager.joinableSubscriptions)
	manager.lock.RUnlock()
	require.Equal(t, 2, active, "the arriving client got a stream of its own on the url in rotation")
	require.Equal(t, 1, joinable, "only the stream in rotation is the one a further client joins")
	require.Equal(t, int64(2), manager.totalSubscriptions.Load())

	// Each client is fed by its own node, so neither was moved onto the other's stream.
	firstNode.messages <- marshalStreamPayload(t, "from-the-url-out-of-rotation")
	require.Equal(t, "from-the-url-out-of-rotation", awaitStreamPayload(t, firstReplies))
	secondNode.messages <- marshalStreamPayload(t, "from-the-url-in-rotation")
	require.Equal(t, "from-the-url-in-rotation", awaitStreamPayload(t, secondReplies))
}

// TestGRPCJoin_StillJoinsWhenNoUrlIsInRotation is the gRPC half of the last chance: with every
// url out of rotation, sharing the live stream beats refusing the client.
func TestGRPCJoin_StillJoinsWhenNoUrlIsInRotation(t *testing.T) {
	firstNode, secondNode := startFakeStreamingUpstream(t), startFakeStreamingUpstream(t)
	manager, endpoints := newGRPCManagerAgainstUpstreams(t, firstNode.addr, secondNode.addr)
	defer manager.Stop()

	ctx := context.Background()
	message := newGrpcSubscriptionMessage(t)

	_, _, err := manager.StartSubscription(ctx, message, "dapp", "1.1.1.1", "stream-1", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, firstNode)

	ranks := map[string]int{endpoints[0].Url: 2, endpoints[1].Url: 2}
	manager.SetURLLendRank(func(url string) int { return ranks[url] })

	_, secondReplies, err := manager.StartSubscription(ctx, message, "dapp", "2.2.2.2", "stream-2", nil)
	require.NoError(t, err)

	manager.lock.RLock()
	active := len(manager.activeSubscriptions)
	manager.lock.RUnlock()
	require.Equal(t, 1, active, "the arriving client joined the live stream")
	require.Equal(t, int64(1), manager.totalSubscriptions.Load())

	select {
	case <-secondNode.streamsOpen:
		t.Fatal("the url in rotation was asked for a stream, though none was in rotation")
	case <-time.After(300 * time.Millisecond):
	}

	firstNode.messages <- marshalStreamPayload(t, "shared")
	require.Equal(t, "shared", awaitStreamPayload(t, secondReplies))
}

// TestGRPCJoin_DisplacedStreamIsStillReleasedByItsOwnClient is the gRPC counterpart of the
// WebSocket release regression: a displaced stream keeps its activeSubscriptions key, so the
// listener and the per-client tracking it was registered with still find it.
func TestGRPCJoin_DisplacedStreamIsStillReleasedByItsOwnClient(t *testing.T) {
	firstNode, secondNode := startFakeStreamingUpstream(t), startFakeStreamingUpstream(t)
	manager, endpoints := newGRPCManagerAgainstUpstreams(t, firstNode.addr, secondNode.addr)
	defer manager.Stop()

	ctx := context.Background()
	message := newGrpcSubscriptionMessage(t)
	firstKey := manager.ClientKey("dapp", "1.1.1.1", "stream-1")

	_, _, err := manager.StartSubscription(ctx, message, "dapp", "1.1.1.1", "stream-1", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, firstNode)

	ranks := map[string]int{endpoints[0].Url: 2}
	manager.SetURLLendRank(func(url string) int { return ranks[url] })

	_, _, err = manager.StartSubscription(ctx, message, "dapp", "2.2.2.2", "stream-2", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, secondNode)

	// The displaced stream's only client goes, the way the listener releases one.
	require.NoError(t, manager.UnsubscribeAll(ctx, firstKey))

	require.Eventually(t, func() bool {
		manager.lock.RLock()
		defer manager.lock.RUnlock()
		return len(manager.activeSubscriptions) == 1
	}, 5*time.Second, 10*time.Millisecond, "the displaced stream was never released")
	require.Equal(t, int64(1), manager.totalSubscriptions.Load())
	require.Equal(t, 0, manager.GetClientSubscriptionCount(firstKey),
		"the client must be untracked, or its limit ratchets on a dead stream")
	select {
	case <-firstNode.streamEnded:
	case <-time.After(10 * time.Second):
		t.Fatal("the upstream stream on the url out of rotation was never closed")
	}
}

// TestGRPCJoin_WaiterDoesNotJoinADisplacedStream covers the second lookup in
// StartSubscription: a client that found another client already creating a subscription for
// these params waits for it and then looks again.
//
// That second look must read the same index the first one did. A displaced subscription still
// holds the params key in activeSubscriptions — by design, so its own listener and tracking
// keep finding it — so a lookup there would hand this client the very subscription the
// creator was displaced from, on the url out of rotation.
func TestGRPCJoin_WaiterDoesNotJoinADisplacedStream(t *testing.T) {
	firstNode, secondNode := startFakeStreamingUpstream(t), startFakeStreamingUpstream(t)
	manager, endpoints := newGRPCManagerAgainstUpstreams(t, firstNode.addr, secondNode.addr)
	defer manager.Stop()

	ctx := context.Background()
	message := newGrpcSubscriptionMessage(t)

	_, _, err := manager.StartSubscription(ctx, message, "dapp", "1.1.1.1", "stream-1", nil)
	require.NoError(t, err)
	requireUpstreamStreamOpened(t, firstNode)

	ranks := map[string]int{endpoints[0].Url: 2}
	manager.SetURLLendRank(func(url string) int { return ranks[url] })

	// Another client is creating a subscription for these params right now, which is what
	// sends the arriving client down the waiting path.
	paramsKey := onlyRegisteredSubscriptionKey(t, manager)
	manager.lock.Lock()
	manager.pendingSubscriptions[paramsKey] = &pendingSubscriptionsBroadcastManager{}
	manager.lock.Unlock()

	waiterDone := make(chan error, 1)
	go func() {
		_, _, waitErr := manager.StartSubscription(ctx, message, "dapp", "2.2.2.2", "stream-2", nil)
		waiterDone <- waitErr
	}()

	// It displaces the subscription on the url out of rotation on its way in, then waits.
	require.Eventually(t, func() bool {
		manager.lock.RLock()
		defer manager.lock.RUnlock()
		return len(manager.joinableSubscriptions) == 0
	}, 5*time.Second, 10*time.Millisecond, "the arriving client never displaced the subscription")

	// The creator reports success, so the waiter looks again.
	manager.settlePendingSubscription(paramsKey, true)

	select {
	case waitErr := <-waiterDone:
		require.NoError(t, waitErr)
	case <-time.After(10 * time.Second):
		t.Fatal("the waiting client never returned")
	}

	requireUpstreamStreamOpened(t, secondNode)
	manager.lock.RLock()
	active := len(manager.activeSubscriptions)
	manager.lock.RUnlock()
	require.Equal(t, 2, active, "the waiter opened a stream of its own on the url in rotation")
}
