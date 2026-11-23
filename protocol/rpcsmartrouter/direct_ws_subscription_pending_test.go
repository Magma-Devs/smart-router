package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDirectWS_AClientWaitingOnAPendingSubscriptionLeavesWithItsContext: a client whose
// subscription another client is creating waits for that creator, and its own context ending
// ends the wait. The WebSocket manager waited on the creator's outcome alone, so a client that
// left stayed parked for as long as the creator's dial and the node's answer took; the gRPC
// manager's wait already ended with the context (MAG-4924). The creator settles afterwards
// without blocking on the client that left.
func TestDirectWS_AClientWaitingOnAPendingSubscriptionLeavesWithItsContext(t *testing.T) {
	manager := newCounterTestManager(t)
	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "eth_subscribe", "params": []interface{}{"newHeads"}, "id": 1,
	})
	require.NoError(t, err)
	msg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_subscribe", params: []interface{}{"newHeads"}},
		rawData:               body,
	}
	hashedParams, _, err := manager.getHashedParams(msg)
	require.NoError(t, err)

	// Another client is creating the same subscription.
	_, waits := manager.checkAndAddPendingSubscription(hashedParams)
	require.False(t, waits, "the first caller is the creator")

	ctx, leave := context.WithCancel(context.Background())
	defer leave()
	outcome := make(chan subscribeOutcome, 1)
	go func() {
		reply, replies, err := manager.StartSubscription(ctx, msg, "dapp", "10.0.0.2", "ws-b", nil)
		outcome <- subscribeOutcome{reply: reply, replies: replies, err: err}
	}()
	require.Eventually(t, func() bool {
		manager.lock.RLock()
		defer manager.lock.RUnlock()
		pending := manager.pendingSubscriptions[hashedParams]
		return pending != nil && len(pending.broadcastChannelList) == 1
	}, 5*time.Second, 5*time.Millisecond, "the client must be waiting on the creator")

	leave()
	select {
	case got := <-outcome:
		require.ErrorIs(t, got.err, context.Canceled)
		require.Nil(t, got.reply)
		require.Nil(t, got.replies)
	case <-time.After(5 * time.Second):
		t.Fatal("the client stayed parked on the creator after its context ended")
	}

	settled := make(chan struct{})
	go func() {
		manager.settlePendingSubscription(hashedParams, false)
		close(settled)
	}()
	receiveWithin(t, settled, "the creator to settle past the client that left")

	manager.lock.RLock()
	defer manager.lock.RUnlock()
	require.Empty(t, manager.connectedClients, "the client that left is attached to nothing")
	require.Empty(t, manager.pendingSubscriptions)
}
