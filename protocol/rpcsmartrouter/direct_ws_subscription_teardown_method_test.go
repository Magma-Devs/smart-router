package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unsubscribeFramesSeen returns the method and first param of every frame the node was asked
// for whose method is one of methods. The param is the raw JSON, because its shape is half of
// what MAG-4278 item 1 got wrong: Solana numbers its subscriptions and rejects the decimal
// string the router canonicalises to internally.
func (ms *mockSubscriptionServer) requestsFor(methods ...string) [][2]string {
	wanted := make(map[string]struct{}, len(methods))
	for _, method := range methods {
		wanted[method] = struct{}{}
	}
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	var out [][2]string
	for _, frame := range ms.observedFrames {
		var req struct {
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(frame, &req); err != nil {
			continue
		}
		if _, ok := wanted[req.Method]; !ok {
			continue
		}
		first := ""
		if len(req.Params) > 0 {
			first = string(req.Params[0])
		}
		out = append(out, [2]string{req.Method, first})
	}
	return out
}

// holdRestoreAtCommit holds the manager's next restore at its commit point.
func holdRestoreAtCommit(manager *DirectWSSubscriptionManager) (reached chan struct{}, release func()) {
	reached = make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	manager.beforeRestoreCommit = func() {
		once.Do(func() { close(reached) })
		<-gate
	}
	return reached, sync.OnceFunc(func() { close(gate) })
}

// TestWS_TeardownOnSolanaSendsTheSpecUnsubscribeMethod is MAG-4278 item 1, from the repro Anna
// Rayer attached to the ticket.
//
// A Solana subscription's connection drops, the restore re-opens it, and its only client
// disconnects while the restore waits at its commit point. Both router-initiated goodbyes go
// through teardownUpstreamSubscription: the disconnect's teardown of the old subscription, and
// the restore's discard of the one it just re-opened. Each derived its method from the
// subscribe method by string surgery, whose fallback is the literal "unsubscribe" — a method
// Solana does not have — and named the subscription with the decimal string the router
// canonicalises to rather than the number Solana issued. So the node rejected both calls and
// kept publishing into the pooled connection: the MAG-3366 leak shape.
//
// It asserts what the node was asked for, because that is the only place the defect is
// visible. Asserting the absence of "unsubscribe" alone would also pass for a router that
// sends nothing at all.
func TestWS_TeardownOnSolanaSendsTheSpecUnsubscribeMethod(t *testing.T) {
	srv := newSolanaMockServer()
	t.Cleanup(srv.Close)
	manager := newSolanaManager(&common.NodeUrl{Url: srv.URL()})
	t.Cleanup(manager.Close)
	h := &releaseHarness{t: t, srv: srv, manager: manager}
	reached, release := holdRestoreAtCommit(manager)
	t.Cleanup(release)

	ctx, disconnect := context.WithCancel(context.Background())
	t.Cleanup(disconnect)
	_, replies, err := manager.StartSubscription(ctx, solanaSubscribeMessage(t, 1), "dapp", "10.0.0.1", "ws-sol", nil)
	require.NoError(t, err)
	require.NotNil(t, replies)
	go func() {
		for range replies {
		}
	}()
	originalID := soleLiveSubscriptionOf(t, srv)

	// The socket drops; the restore re-subscribes and is held before it commits.
	srv.dropConnections()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the restore never re-subscribed on the node")
	}
	require.Equal(t, 2, h.upstreamCalls("accountSubscribe"), "the restore subscribed again")
	// The id the node minted for the restore is the one the discard has to name.
	restoredID := liveSubscriptionOtherThan(t, srv, originalID)
	require.NotEqual(t, originalID, restoredID)

	// The only client leaves while the restore is held, so the subscription is torn down
	// under it and the restore has nothing to commit onto: it discards what it re-opened.
	disconnect()
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond)
	release()
	require.Eventually(t, func() bool { return goroutinesIn("handleUpstreamDisconnect") == 0 }, 15*time.Second, 10*time.Millisecond)

	// One call, for the subscription the restore re-opened. The dropped socket took the
	// original subscription with it, so there is nobody left to tell about that one, and the
	// teardown skips a dead connection (MAG-4242).
	if !assert.Eventually(t, func() bool { return len(srv.requestsFor("accountUnsubscribe")) == 1 },
		10*time.Second, 10*time.Millisecond) {
		// Printed after the wait, not before it: the methods the node saw are the evidence.
		t.Fatalf("the node must be told accountUnsubscribe for the subscription the restore re-opened; it saw %v",
			srv.ObservedMethods())
	}
	require.NotContains(t, srv.ObservedMethods(), "unsubscribe",
		"the router sent a method Solana does not have")
	require.Equal(t, [2]string{"accountUnsubscribe", restoredID}, srv.requestsFor("accountUnsubscribe")[0],
		"Solana numbers its subscriptions and rejects the quoted id the router canonicalises to")
	require.Eventually(t, func() bool { return srv.liveSubscriptions() == 0 }, 10*time.Second, 10*time.Millisecond,
		"the node still publishes a subscription nobody reads")
	require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(0), manager.totalSubscriptions.Load())
}

// TestWS_ClientDisconnectOnSubstrateSendsTheSpecUnsubscribeMethod covers the plainest and
// commonest shape of the same defect: one Substrate client closes its socket without
// unsubscribing. The subscribe method carries no "subscribe" token the unsubscribe name can be
// reached through, so it is the namespace pairing that finds chainHead_v1_unfollow — and the
// string derivation sent "unsubscribe".
func TestWS_ClientDisconnectOnSubstrateSendsTheSpecUnsubscribeMethod(t *testing.T) {
	srv := newMockSubscriptionServer()
	srv.messageInterval = 20 * time.Millisecond
	srv.notificationMethod = "chainHead_v1_followEvent"
	t.Cleanup(srv.Close)
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(), "jsonrpc", "ACA", "jsonrpc",
		[]*common.NodeUrl{{Url: srv.URL()}}, nil, nil, nil,
	)
	t.Cleanup(manager.Close)
	h := &releaseHarness{t: t, srv: srv, manager: manager}

	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "chainHead_v1_follow", "params": []interface{}{true}, "id": 1,
	})
	require.NoError(t, err)
	msg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{
			method: "chainHead_v1_follow",
			params: []interface{}{true},
			parseDirectives: subscriptionDirectives(
				[2]string{"chainHead_v1_follow", "chainHead_v1_unfollow"},
				[2]string{"chain_subscribeNewHeads", "chain_unsubscribeNewHeads"},
				[2]string{"state_subscribeStorage", "state_unsubscribeStorage"},
			),
		},
		rawData: body,
	}
	ctx, disconnect := context.WithCancel(context.Background())
	t.Cleanup(disconnect)
	_, replies, err := manager.StartSubscription(ctx, msg, "dapp", "10.0.0.1", "ws-aca", nil)
	require.NoError(t, err)
	go func() {
		for range replies {
		}
	}()
	require.Equal(t, 1, srv.liveSubscriptions())

	disconnect() // the client closes its socket, as a real one does
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond)
	if !assert.Eventually(t, func() bool { return len(srv.requestsFor("chainHead_v1_unfollow")) == 1 },
		10*time.Second, 10*time.Millisecond) {
		t.Fatalf("the node must be told the spec's unfollow; it saw %v", srv.ObservedMethods())
	}
	require.NotContains(t, srv.ObservedMethods(), "unsubscribe")
	require.Eventually(t, func() bool { return srv.liveSubscriptions() == 0 }, 10*time.Second, 10*time.Millisecond,
		"the node still publishes a subscription nobody reads")
}

// TestWS_TeardownTellsTheNodeNothingWhenTheSpecDeclaresNoUnsubscribe: the FVM spec declares
// Filecoin.ChainNotify with no unsubscribe at all, and two Enjin archive subscribes share a
// namespace with one unclaimed unsubscribe between them. For those the router sends nothing
// rather than a method the spec says the node does not answer to: the call cannot stop the node
// publishing, and the warning logged when the subscription was created is what gets the pair
// added to the spec.
func TestWS_TeardownTellsTheNodeNothingWhenTheSpecDeclaresNoUnsubscribe(t *testing.T) {
	srv := newMockSubscriptionServer()
	srv.messageInterval = 20 * time.Millisecond
	srv.notificationMethod = "Filecoin.ChainNotifyEvent"
	t.Cleanup(srv.Close)
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(), "jsonrpc", "FVM", "jsonrpc",
		[]*common.NodeUrl{{Url: srv.URL()}}, nil, nil, nil,
	)
	t.Cleanup(manager.Close)
	h := &releaseHarness{t: t, srv: srv, manager: manager}

	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "Filecoin.ChainNotify", "params": []interface{}{}, "id": 1,
	})
	require.NoError(t, err)
	msg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{
			method: "Filecoin.ChainNotify",
			params: []interface{}{},
			parseDirectives: subscriptionDirectives(
				[2]string{"Filecoin.EthSubscribe", "Filecoin.EthUnsubscribe"},
				[2]string{"Filecoin.ChainNotify", ""},
			),
		},
		rawData: body,
	}
	ctx, disconnect := context.WithCancel(context.Background())
	t.Cleanup(disconnect)
	_, replies, err := manager.StartSubscription(ctx, msg, "dapp", "10.0.0.1", "ws-fvm", nil)
	require.NoError(t, err)
	go func() {
		for range replies {
		}
	}()

	disconnect()
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(500 * time.Millisecond) // room for a wrong call to land

	require.Equal(t, []string{"Filecoin.ChainNotify"}, srv.ObservedMethods(),
		"the node must be told nothing it cannot answer")
	require.Empty(t, srv.requestsFor("Filecoin.EthUnsubscribe"),
		"the other pair's unsubscribe is not this subscription's")
	require.Equal(t, int64(0), manager.totalSubscriptions.Load())
	require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 0 }, 5*time.Second, 10*time.Millisecond)
}

// TestWS_ExplicitUnsubscribeStillSendsTheClientsMethod guards the path that was always right:
// the client names the method, and the router sends that rather than anything it resolved from
// the spec. It also pins the id shape the two paths now share.
func TestWS_ExplicitUnsubscribeStillSendsTheClientsMethod(t *testing.T) {
	srv := newSolanaMockServer()
	t.Cleanup(srv.Close)
	manager := newSolanaManager(&common.NodeUrl{Url: srv.URL()})
	t.Cleanup(manager.Close)
	h := &releaseHarness{t: t, srv: srv, manager: manager}

	ctx, disconnect := context.WithCancel(context.Background())
	t.Cleanup(disconnect)
	reply, replies, err := manager.StartSubscription(ctx, solanaSubscribeMessage(t, 1), "dapp", "10.0.0.1", "ws-sol", nil)
	require.NoError(t, err)
	go func() {
		for range replies {
		}
	}()
	routerID := numericSubscriptionID(t, reply.Data)

	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "accountUnsubscribe", "params": []interface{}{routerID}, "id": 2,
	})
	require.NoError(t, err)
	unsubscribe := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{
			method:          "accountUnsubscribe",
			params:          []interface{}{routerID},
			parseDirectives: solanaSubscriptionDirectives(),
		},
		rawData: body,
	}
	_, err = manager.Unsubscribe(ctx, unsubscribe, "dapp", "10.0.0.1", "ws-sol", nil)
	require.NoError(t, err)

	requests := srv.requestsFor("accountUnsubscribe")
	require.Len(t, requests, 1)
	require.NotContains(t, requests[0][1], `"`, "Solana rejects a quoted id")
	require.Equal(t, 0, h.activeSubscriptionCount())
	require.Eventually(t, func() bool { return srv.liveSubscriptions() == 0 }, 5*time.Second, 10*time.Millisecond)
}

// soleLiveSubscriptionOf returns the id of the one subscription the node holds.
func soleLiveSubscriptionOf(t *testing.T, srv *mockSubscriptionServer) string {
	t.Helper()
	srv.lock.RLock()
	defer srv.lock.RUnlock()
	require.Len(t, srv.subscriptions, 1, "exactly one subscription is live on the node")
	for id := range srv.subscriptions {
		return id
	}
	return ""
}

// liveSubscriptionOtherThan returns the one live subscription id that is not other, which is
// how a test names the subscription a restore re-opened while the node still holds both. The
// id is the key the node tracks it under, which is the decimal form of the number it put on
// the wire — so comparing an unsubscribe's raw param to it also checks the param is unquoted.
func liveSubscriptionOtherThan(t *testing.T, srv *mockSubscriptionServer, other string) string {
	t.Helper()
	srv.lock.RLock()
	defer srv.lock.RUnlock()
	var found string
	for id := range srv.subscriptions {
		if id == other {
			continue
		}
		require.Empty(t, found, "more than one subscription beside %s is live", other)
		found = id
	}
	require.NotEmpty(t, found, "no subscription beside %s is live", other)
	return found
}
