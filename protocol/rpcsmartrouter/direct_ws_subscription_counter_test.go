package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// newCounterTestManager builds a manager with one primary endpoint and no optimizer.
func newCounterTestManager(t *testing.T) *DirectWSSubscriptionManager {
	t.Helper()
	return NewDirectWSSubscriptionManager(
		getTestMetricsManager(), "jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{{Url: "wss://test.example.com"}}, nil, nil, nil,
	)
}

// storeCreatedSubscription does what the creator does at the end of StartSubscription:
// stores the subscription with its first client attached, and counts it.
func storeCreatedSubscription(t *testing.T, manager *DirectWSSubscriptionManager, ctx context.Context, cancel context.CancelFunc, hashedParams, clientKey string) *directActiveSubscription {
	t.Helper()
	const upstreamID = "0xupstream"
	routerID := manager.idMapper.GenerateRouterID(clientKey)
	manager.idMapper.RegisterMapping(routerID, upstreamID)
	firstReply, err := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": routerID})
	require.NoError(t, err)
	sender := common.NewSafeChannelSender(ctx, make(chan *pairingtypes.RelayReply, 10))
	activeSub := &directActiveSubscription{
		upstreamID:       upstreamID,
		clientRouterIDs:  make(map[string]string),
		connectedClients: make(map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]),
		hashedParams:     hashedParams,
		firstReply:       &pairingtypes.RelayReply{Data: firstReply},
		ctx:              ctx,
		cancel:           cancel,
	}
	activeSub.attachClientLocked(clientKey, routerID, sender)
	manager.lock.Lock()
	manager.activeSubscriptions[hashedParams] = activeSub
	manager.connectedClients[clientKey] = map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{hashedParams: sender}
	manager.lock.Unlock()
	manager.totalSubscriptions.Add(1)
	return activeSub
}

// TestDirectWS_CleanupOfCancelledSubscriptionDecrementsTotal is the check MAG-2546 asked
// for: the cleanup ticker's cancelled-context branch deletes the subscription, and the
// global counter must come back down with it. That branch once deleted without
// decrementing, so under client churn the counter drifted up until the global limit
// refused every new eth_subscribe (reject mode) or warned forever (warn mode).
func TestDirectWS_CleanupOfCancelledSubscriptionDecrementsTotal(t *testing.T) {
	manager := newCounterTestManager(t)
	clientKey := manager.CreateWebSocketConnectionUniqueKey("dapp", "10.0.0.1", "ws-1")
	ctx, cancel := context.WithCancel(context.Background())
	storeCreatedSubscription(t, manager, ctx, cancel, "params-hash", clientKey)
	require.Equal(t, int64(1), manager.totalSubscriptions.Load())

	cancel() // the subscription's context ends; the next cleanup tick reaps it
	manager.performCleanup()

	require.Equal(t, int64(0), manager.totalSubscriptions.Load(),
		"cleanup deleted the subscription but left the counter high")
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	require.Empty(t, manager.activeSubscriptions)
	require.Empty(t, manager.connectedClients)
}

// TestDirectWS_PendingCheckJoinsASubscriptionStoredSinceTheActiveCheck pins the second
// way the counter drifted. StartSubscription looks for an active subscription and
// registers a pending one under two separate lock acquisitions. A creator that stored
// its subscription and cleared its pending entry in between left the second caller
// seeing neither, so it became a second creator: a second upstream subscription for the
// same params, the first map entry overwritten and orphaned (its listener goroutine and
// node-side subscription living on unowned), and the counter incremented twice for an
// entry that is dropped once. The pending check must notice the subscription is already
// active and send the caller down the join path instead.
func TestDirectWS_PendingCheckJoinsASubscriptionStoredSinceTheActiveCheck(t *testing.T) {
	manager := newCounterTestManager(t)
	const hashedParams = "params-hash"
	creatorKey := manager.CreateWebSocketConnectionUniqueKey("dapp", "10.0.0.1", "ws-1")
	joinerKey := manager.CreateWebSocketConnectionUniqueKey("dapp", "10.0.0.2", "ws-2")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	joinerSender := common.NewSafeChannelSender(ctx, make(chan *pairingtypes.RelayReply, 10))

	// The joiner's active check runs first and misses: nothing is stored yet.
	reply, joined := manager.checkForActiveSubscriptionAndConnect(ctx, hashedParams, joinerKey, joinerSender, json.RawMessage("2"))
	require.Nil(t, reply)
	require.False(t, joined)

	// The creator finishes in the gap: it stores its subscription, counts it, and clears
	// its pending entry, so from the joiner's point of view there is neither a pending
	// nor (as of its check) an active subscription.
	activeSub := storeCreatedSubscription(t, manager, ctx, cancel, hashedParams, creatorKey)
	manager.successPendingSubscription(hashedParams)

	// The joiner's pending check must hand it a settled success, not make it a creator.
	pendingChan, found := manager.checkAndAddPendingSubscription(hashedParams)
	require.True(t, found,
		"a subscription that became active since the active check must be joined, not created again")
	require.NotNil(t, pendingChan)
	select {
	case ok := <-pendingChan:
		require.True(t, ok)
	default:
		t.Fatal("the pending channel for an already-active subscription must be settled")
	}
	manager.lock.RLock()
	_, pendingRegistered := manager.pendingSubscriptions[hashedParams]
	manager.lock.RUnlock()
	require.False(t, pendingRegistered,
		"no pending entry may be registered for a subscription that is already active")

	// Which is the join path StartSubscription takes on a true broadcast.
	reply, joined = manager.checkForActiveSubscriptionAndConnect(ctx, hashedParams, joinerKey, joinerSender, json.RawMessage("2"))
	require.True(t, joined)
	require.NotNil(t, reply)

	require.Equal(t, int64(1), manager.totalSubscriptions.Load(),
		"one subscription shared by two clients counts once")
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	require.Len(t, manager.activeSubscriptions, 1)
	require.Same(t, activeSub, manager.activeSubscriptions[hashedParams],
		"the creator's entry must not be overwritten")
	require.Len(t, activeSub.connectedClients, 2)
}
