package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	rpcInterfaceMessages "github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	rpcclient "github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSubscriptionServer creates a mock WebSocket server for testing subscriptions
type mockSubscriptionServer struct {
	server          *httptest.Server
	upgrader        websocket.Upgrader
	subscriptions   map[string]chan struct{} // subscription ID -> close channel
	observedMethods []string                 // every JSON-RPC method the server was actually asked for
	lock            sync.RWMutex
	writeMu         sync.Mutex // serializes conn.WriteMessage — gorilla requires a single concurrent writer
	messageInterval time.Duration

	// notificationMethod is the JSON-RPC method the push frames carry, and subscriptionID
	// is the id handed back on subscribe. Both default to the EVM shape. Substrate names
	// its frames after the payload and its ids are not hex, and a harness that can only
	// speak EVM is exactly why MAG-3345 went unnoticed: every existing test here asserted
	// against the one shape the router happened to handle.
	notificationMethod string
	subscriptionID     string

	// numericIDs makes the server number its subscriptions instead of naming them, which
	// is what Solana does and what no test here could express before MAG-3359.
	// firstNumericID is the id the first such subscription gets.
	numericIDs     bool
	firstNumericID int

	// subscriptionSeq makes successive ids distinct, so a test can tell a restored
	// subscription from the one it replaced.
	subscriptionSeq int
}

// safeWriteMessage serializes WS writes: handleWS (response) and the spawned sendSubscriptionMessages
// goroutine both write the same conn, and gorilla/websocket forbids concurrent writers (data race
// under -race). All writes go through this.
func (ms *mockSubscriptionServer) safeWriteMessage(conn *websocket.Conn, messageType int, data []byte) error {
	ms.writeMu.Lock()
	defer ms.writeMu.Unlock()
	return conn.WriteMessage(messageType, data)
}

func newMockSubscriptionServer() *mockSubscriptionServer {
	ms := &mockSubscriptionServer{
		upgrader:           websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
		subscriptions:      make(map[string]chan struct{}),
		messageInterval:    100 * time.Millisecond,
		notificationMethod: "eth_subscription",
		subscriptionID:     "0x" + strings.Repeat("f", 32),
		firstNumericID:     23784,
	}

	ms.server = httptest.NewServer(http.HandlerFunc(ms.handleWS))
	return ms
}

func (ms *mockSubscriptionServer) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := ms.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			return
		}

		// Parse JSON-RPC request
		var req struct {
			ID     interface{} `json:"id"`
			Method string      `json:"method"`
			Params interface{} `json:"params"`
		}
		if err := json.Unmarshal(message, &req); err != nil {
			continue
		}

		ms.recordMethod(req.Method)

		// Classify by whether the caller handed us a LIVE subscription id, never by
		// the method name. This harness used to gate on "_subscribe"/"_unsubscribe"
		// suffixes, which is the same assumption that produced MAG-3297: it could
		// not serve a chain_subscribeNewHeads / chain_unsubscribeNewHeads pair at
		// all, so no test here could have caught the bug.
		//
		// Everything gets a reply. Dropping an unrecognised method would make a
		// regression hang out the router's 10s CallContext timeout instead of
		// failing fast.
		if subID, ok := ms.firstSubscriptionParam(req.Params); ok && ms.hasSubscription(subID) {
			ms.closeSubscription(subID)
			response := map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  true,
			}
			respBytes, _ := json.Marshal(response)
			ms.safeWriteMessage(conn, websocket.TextMessage, respBytes)
			continue
		}

		subID, wireID := ms.createSubscription()
		response := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  wireID,
		}
		respBytes, _ := json.Marshal(response)
		ms.safeWriteMessage(conn, websocket.TextMessage, respBytes)
		go ms.sendSubscriptionMessages(conn, subID, wireID)
	}
}

// firstSubscriptionParam returns params[0] rendered the way createSubscription keys
// subscriptions: a string verbatim, a number in decimal.
//
// Accepting the number matters. Solana numbers its subscriptions, so a string-only check
// reads accountUnsubscribe(23784) as a fresh subscribe and answers with a new id instead of
// tearing the old one down — which is the same "the harness can only speak EVM" blindness
// that hid MAG-3345.
func (ms *mockSubscriptionServer) firstSubscriptionParam(params interface{}) (string, bool) {
	list, ok := params.([]interface{})
	if !ok || len(list) == 0 {
		return "", false
	}
	switch v := list[0].(type) {
	case string:
		// A node that numbers its subscriptions does not answer to a quoted id. Being
		// strict here is what makes the teardown param's shape observable: a lenient mock
		// accepts the router's canonical decimal string and hides the bug.
		if ms.numericIDs {
			return "", false
		}
		return v, true
	case float64:
		return strconv.FormatInt(int64(v), 10), true
	}
	return "", false
}

func (ms *mockSubscriptionServer) recordMethod(method string) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	ms.observedMethods = append(ms.observedMethods, method)
}

// ObservedMethods returns the JSON-RPC methods the upstream was actually asked
// for, which is the thing MAG-3297 got wrong and the only place a regression at
// the CallContext call site is visible.
func (ms *mockSubscriptionServer) ObservedMethods() []string {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	return append([]string(nil), ms.observedMethods...)
}

func (ms *mockSubscriptionServer) hasSubscription(subID string) bool {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	_, ok := ms.subscriptions[subID]
	return ok
}

// createSubscription mints the next subscription id: the key the server tracks it under,
// and the JSON value it puts on the wire. The first non-numeric id is subscriptionID
// unchanged, so tests written against the old fixed-id harness keep their expectations.
func (ms *mockSubscriptionServer) createSubscription() (key string, wire any) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	ms.subscriptionSeq++
	if ms.numericIDs {
		id := ms.firstNumericID + ms.subscriptionSeq - 1
		key = strconv.Itoa(id)
		ms.subscriptions[key] = make(chan struct{})
		return key, id
	}

	key = ms.subscriptionID
	if ms.subscriptionSeq > 1 {
		key = fmt.Sprintf("%s-%d", ms.subscriptionID, ms.subscriptionSeq)
	}
	ms.subscriptions[key] = make(chan struct{})
	return key, key
}

func (ms *mockSubscriptionServer) closeSubscription(subID string) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	if ch, exists := ms.subscriptions[subID]; exists {
		close(ch)
		delete(ms.subscriptions, subID)
	}
}

func (ms *mockSubscriptionServer) sendSubscriptionMessages(conn *websocket.Conn, subID string, wireID any) {
	ms.lock.RLock()
	closeCh, exists := ms.subscriptions[subID]
	ms.lock.RUnlock()
	if !exists {
		return
	}

	counter := 0
	ticker := time.NewTicker(ms.messageInterval)
	defer ticker.Stop()

	for {
		select {
		case <-closeCh:
			return
		case <-ticker.C:
			counter++
			// Send subscription notification
			notification := map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  ms.notificationMethod,
				"params": map[string]interface{}{
					"subscription": wireID,
					"result": map[string]interface{}{
						"number": counter,
					},
				},
			}
			msgBytes, _ := json.Marshal(notification)
			if err := ms.safeWriteMessage(conn, websocket.TextMessage, msgBytes); err != nil {
				return
			}
		}
	}
}

func (ms *mockSubscriptionServer) Close() {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	for _, ch := range ms.subscriptions {
		close(ch)
	}
	ms.subscriptions = make(map[string]chan struct{})
	ms.server.Close()
}

func (ms *mockSubscriptionServer) URL() string {
	return "ws" + strings.TrimPrefix(ms.server.URL, "http")
}

// mockWSProtocolMessage implements chainlib.ProtocolMessage for WebSocket subscription tests
type mockWSProtocolMessage struct {
	method string
	params interface{}
}

func (m *mockWSProtocolMessage) GetApi() *spectypes.Api {
	return &spectypes.Api{Name: m.method}
}

func (m *mockWSProtocolMessage) GetApiCollection() *spectypes.ApiCollection {
	return &spectypes.ApiCollection{
		CollectionData: spectypes.CollectionData{
			ApiInterface: "jsonrpc",
		},
	}
}

func (m *mockWSProtocolMessage) GetRPCMessage() rpcInterfaceMessages.GenericMessage {
	return &mockWSGenericMessage{method: m.method, params: m.params}
}

func (m *mockWSProtocolMessage) RelayPrivateData() *pairingtypes.RelayPrivateData {
	data, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  m.method,
		"params":  m.params,
	})
	return &pairingtypes.RelayPrivateData{Data: data}
}

// Implement remaining chainlib.ProtocolMessage methods (stubs)
func (m *mockWSProtocolMessage) SubscriptionIdExtractor(reply *rpcclient.JsonrpcMessage) string {
	return ""
}
func (m *mockWSProtocolMessage) RequestedBlock() (latest int64, earliest int64) { return 0, 0 }
func (m *mockWSProtocolMessage) UpdateLatestBlockInMessage(latestBlock int64, modifyContent bool) bool {
	return false
}
func (m *mockWSProtocolMessage) AppendHeader(metadata []pairingtypes.Metadata) {}
func (m *mockWSProtocolMessage) GetExtensions() []*spectypes.Extension         { return nil }
func (m *mockWSProtocolMessage) OverrideExtensions(extensionNames []string, extensionParser *extensionslib.ExtensionParser) {
}
func (m *mockWSProtocolMessage) DisableErrorHandling()                          {}
func (m *mockWSProtocolMessage) TimeoutOverride(...time.Duration) time.Duration { return 0 }
func (m *mockWSProtocolMessage) GetForceCacheRefresh() bool                     { return false }

func (m *mockWSProtocolMessage) SetForceCacheRefresh(force bool) bool { return false }

func (m *mockWSProtocolMessage) GetRawRequestHash() ([]byte, error)                  { return nil, nil }
func (m *mockWSProtocolMessage) GetRequestedBlocksHashes() []string                  { return nil }
func (m *mockWSProtocolMessage) UpdateEarliestInMessage(incomingEarliest int64) bool { return false }
func (m *mockWSProtocolMessage) SetExtension(extension *spectypes.Extension)         {}
func (m *mockWSProtocolMessage) GetUsedDefaultValue() bool                           { return false }
func (m *mockWSProtocolMessage) GetParseDirective() *spectypes.ParseDirective        { return nil }
func (m *mockWSProtocolMessage) IsBatch() bool                                       { return false }

func (m *mockWSProtocolMessage) CheckResponseError(data []byte, httpStatusCode int) (bool, string) {
	return false, ""
}
func (m *mockWSProtocolMessage) GetDirectiveHeaders() map[string]string { return nil }
func (m *mockWSProtocolMessage) HashCacheRequest(chainId string) ([]byte, func([]byte) []byte, error) {
	return nil, nil, nil
}
func (m *mockWSProtocolMessage) GetBlockedProviders() []string { return nil }
func (m *mockWSProtocolMessage) GetUserData() common.UserData  { return common.UserData{} }
func (m *mockWSProtocolMessage) IsDefaultApi() bool            { return false }
func (m *mockWSProtocolMessage) UpdateEarliestAndValidateExtensionRules(extensionParser *extensionslib.ExtensionParser, earliestBlockHashRequested int64, addon string, seenBlock int64) bool {
	return false
}

func (m *mockWSProtocolMessage) GetCrossValidationParameters() (common.CrossValidationParams, bool, error) {
	return common.CrossValidationParams{}, false, nil
}

type mockWSGenericMessage struct {
	method string
	params interface{}
}

func (m *mockWSGenericMessage) GetHeaders() []pairingtypes.Metadata { return nil }
func (m *mockWSGenericMessage) DisableErrorHandling()               {}
func (m *mockWSGenericMessage) GetParams() interface{}              { return m.params }
func (m *mockWSGenericMessage) GetMethod() string                   { return m.method }
func (m *mockWSGenericMessage) GetResult() json.RawMessage          { return nil }
func (m *mockWSGenericMessage) GetID() json.RawMessage              { return []byte("1") }

// getTestMetricsManager returns a no-op metrics sink for the subscription managers.
// The smart router's real metrics owner is SmartRouterMetricsManager; these tests only
// need something that satisfies ConsumerMetricsManagerInf.
func getTestMetricsManager() metrics.ConsumerMetricsManagerInf {
	return metrics.NoOpConsumerMetrics{}
}

// TestNewDirectWSSubscriptionManager tests the constructor
func TestNewDirectWSSubscriptionManager(t *testing.T) {
	nodeUrl := &common.NodeUrl{Url: "wss://test.example.com"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}
	metricsManager := getTestMetricsManager()

	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil, // No optimizer for basic test
		nil, // Use default WebSocket config
	)

	require.NotNil(t, manager)
	assert.NotNil(t, manager.connectedClients)
	assert.NotNil(t, manager.activeSubscriptions)
	assert.NotNil(t, manager.pendingSubscriptions)
	assert.NotNil(t, manager.upstreamPools)
	assert.NotNil(t, manager.idMapper)
	assert.Equal(t, "ETH", manager.chainID)
	assert.Len(t, manager.wsEndpoints, 1)
	assert.Equal(t, "wss://test.example.com", manager.wsEndpoints[0].Url)
}

// TestCreateWebSocketConnectionUniqueKey tests the client key generation
func TestCreateWebSocketConnectionUniqueKey(t *testing.T) {
	manager := &DirectWSSubscriptionManager{}

	key := manager.CreateWebSocketConnectionUniqueKey("dapp1", "192.168.1.1", "ws-uid-123")
	assert.Equal(t, "dapp1:192.168.1.1:ws-uid-123", key)
}

// TestGetOrCreatePool tests pool creation and reuse
func TestGetOrCreatePool(t *testing.T) {
	nodeUrl := &common.NodeUrl{Url: "wss://test.example.com"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}
	metricsManager := getTestMetricsManager()

	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		nil, // Use default WebSocket config
	)

	// First call should create a new pool
	pool1 := manager.GetOrCreatePool(nodeUrl)
	require.NotNil(t, pool1)

	// Second call should return the same pool
	pool2 := manager.GetOrCreatePool(nodeUrl)
	assert.Same(t, pool1, pool2)

	// Different URL should create different pool
	nodeUrl2 := &common.NodeUrl{Url: "wss://other.example.com"}
	pool3 := manager.GetOrCreatePool(nodeUrl2)
	assert.NotSame(t, pool1, pool3)
}

// TestDirectWSSubscriptionManager_ImplementsInterface verifies interface compliance
func TestDirectWSSubscriptionManager_ImplementsInterface(t *testing.T) {
	// Compile-time check that DirectWSSubscriptionManager implements WSSubscriptionManager
	var _ chainlib.WSSubscriptionManager = (*DirectWSSubscriptionManager)(nil)
}

// TestDirectWSSubscriptionManager_UnsubscribeAll_NoSubscriptions tests unsubscribe all with no active subscriptions
func TestDirectWSSubscriptionManager_UnsubscribeAll_NoSubscriptions(t *testing.T) {
	nodeUrl := &common.NodeUrl{Url: "wss://test.example.com"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}
	metricsManager := getTestMetricsManager()

	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		nil, // Use default WebSocket config
	)

	ctx := context.Background()
	err := manager.UnsubscribeAll(ctx, "dapp1", "192.168.1.1", "ws-uid-123", nil)

	// Should not error when no subscriptions exist
	assert.NoError(t, err)
}

// TestDirectWSSubscriptionManager_Unsubscribe_NoSubscriptions tests unsubscribe with no active subscriptions
func TestDirectWSSubscriptionManager_Unsubscribe_NoSubscriptions(t *testing.T) {
	nodeUrl := &common.NodeUrl{Url: "wss://test.example.com"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}
	metricsManager := getTestMetricsManager()

	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		nil, // Use default WebSocket config
	)

	ctx := context.Background()
	protocolMessage := &mockWSProtocolMessage{
		method: "eth_unsubscribe",
		params: []interface{}{"0x123"},
	}

	_, err := manager.Unsubscribe(ctx, protocolMessage, "dapp1", "192.168.1.1", "ws-uid-123", nil)

	// Should return subscription not found error
	assert.Error(t, err)
	assert.Equal(t, common.SubscriptionNotFoundError, err)
}

// TestDirectWSSubscriptionManager_StartSubscription_ConnectionFailure tests handling connection failures
func TestDirectWSSubscriptionManager_StartSubscription_ConnectionFailure(t *testing.T) {
	// Use an invalid WebSocket URL that will fail to connect
	nodeUrl := &common.NodeUrl{Url: "wss://invalid.nonexistent.example.com:9999"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}
	metricsManager := getTestMetricsManager()

	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		nil, // Use default WebSocket config
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	protocolMessage := &mockWSProtocolMessage{
		method: "eth_subscribe",
		params: []interface{}{"newHeads"},
	}

	reply, repliesChan, err := manager.StartSubscription(ctx, protocolMessage, "dapp1", "192.168.1.1", "ws-uid-123", nil)

	// Should fail with connection error
	assert.Error(t, err)
	assert.Nil(t, reply)
	assert.Nil(t, repliesChan)
	assert.Contains(t, err.Error(), "failed to get WebSocket connection")
}

// TestDeduplicationMultiClientUniqueRouterIDs verifies that when multiple clients subscribe
// to the same subscription parameters, each client gets their own unique router ID.
// This is critical for proper unsubscribe behavior - when one client unsubscribes,
// the others should remain connected and continue receiving messages.
//
// This test addresses the bug where a single router ID was shared across all clients,
// causing the first client's unsubscribe to tear down the shared upstream subscription.
func TestDeduplicationMultiClientUniqueRouterIDs(t *testing.T) {
	// Test that the ID mapper correctly handles multiple router IDs for one upstream ID
	mapper := NewSubscriptionIDMapper()

	// Simulate three clients subscribing to the same feed
	client1Key := "dapp1:192.168.1.1:ws-1"
	client2Key := "dapp1:192.168.1.2:ws-2"
	client3Key := "dapp1:192.168.1.3:ws-3"

	// Each client should get a unique router ID
	routerID1 := mapper.GenerateRouterID(client1Key)
	routerID2 := mapper.GenerateRouterID(client2Key)
	routerID3 := mapper.GenerateRouterID(client3Key)

	// Verify all router IDs are unique
	assert.NotEqual(t, routerID1, routerID2, "Router IDs for different clients must be unique")
	assert.NotEqual(t, routerID2, routerID3, "Router IDs for different clients must be unique")
	assert.NotEqual(t, routerID1, routerID3, "Router IDs for different clients must be unique")

	// All three router IDs map to the same upstream ID (the shared subscription)
	upstreamID := "0xupstreamABC123"
	mapper.RegisterMapping(routerID1, upstreamID)
	mapper.RegisterMapping(routerID2, upstreamID)
	mapper.RegisterMapping(routerID3, upstreamID)

	// Verify all mappings are correct
	gotUpstream1, found1 := mapper.GetUpstreamID(routerID1)
	gotUpstream2, found2 := mapper.GetUpstreamID(routerID2)
	gotUpstream3, found3 := mapper.GetUpstreamID(routerID3)
	assert.True(t, found1 && found2 && found3)
	assert.Equal(t, upstreamID, gotUpstream1)
	assert.Equal(t, upstreamID, gotUpstream2)
	assert.Equal(t, upstreamID, gotUpstream3)

	// Verify GetRouterIDs returns all three
	routerIDs := mapper.GetRouterIDs(upstreamID)
	assert.Len(t, routerIDs, 3)
	assert.Contains(t, routerIDs, routerID1)
	assert.Contains(t, routerIDs, routerID2)
	assert.Contains(t, routerIDs, routerID3)

	// Client 2 unsubscribes - should NOT report lastClient
	removedUpstream, lastClient := mapper.RemoveMapping(routerID2)
	assert.Equal(t, upstreamID, removedUpstream)
	assert.False(t, lastClient, "Client 2 should NOT be the last client")

	// Client 1 and 3 should still have valid mappings
	gotUpstream1, found1 = mapper.GetUpstreamID(routerID1)
	gotUpstream3, found3 = mapper.GetUpstreamID(routerID3)
	assert.True(t, found1, "Client 1's mapping should still exist")
	assert.True(t, found3, "Client 3's mapping should still exist")
	assert.Equal(t, upstreamID, gotUpstream1)
	assert.Equal(t, upstreamID, gotUpstream3)

	// Client 2's mapping should be gone
	_, found2 = mapper.GetUpstreamID(routerID2)
	assert.False(t, found2, "Client 2's mapping should be removed")

	// Remaining router IDs
	routerIDs = mapper.GetRouterIDs(upstreamID)
	assert.Len(t, routerIDs, 2)
	assert.NotContains(t, routerIDs, routerID2)

	// Client 3 unsubscribes - still NOT the last client
	removedUpstream, lastClient = mapper.RemoveMapping(routerID3)
	assert.Equal(t, upstreamID, removedUpstream)
	assert.False(t, lastClient, "Client 3 should NOT be the last client")

	// Only client 1 remains
	routerIDs = mapper.GetRouterIDs(upstreamID)
	assert.Len(t, routerIDs, 1)
	assert.Equal(t, routerID1, routerIDs[0])

	// Client 1 unsubscribes - should be the LAST client
	removedUpstream, lastClient = mapper.RemoveMapping(routerID1)
	assert.Equal(t, upstreamID, removedUpstream)
	assert.True(t, lastClient, "Client 1 SHOULD be the last client")

	// No more mappings
	routerIDs = mapper.GetRouterIDs(upstreamID)
	assert.Len(t, routerIDs, 0)
}

// TestActiveSubscriptionTracksPerClientRouterIDs verifies that directActiveSubscription
// correctly tracks per-client router IDs through the clientRouterIDs map.
func TestActiveSubscriptionTracksPerClientRouterIDs(t *testing.T) {
	nodeUrl := &common.NodeUrl{Url: "wss://test.example.com"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}
	metricsManager := getTestMetricsManager()

	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		nil,
	)

	// Simulate creating an active subscription
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	activeSub := &directActiveSubscription{
		upstreamID:       "0xupstreamXYZ",
		clientRouterIDs:  make(map[string]string),
		connectedClients: make(map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]),
		hashedParams:     "test-params-hash",
		ctx:              ctx,
		cancel:           cancel,
	}

	// Add three clients
	client1Key := manager.CreateWebSocketConnectionUniqueKey("dapp1", "192.168.1.1", "ws-1")
	client2Key := manager.CreateWebSocketConnectionUniqueKey("dapp1", "192.168.1.2", "ws-2")
	client3Key := manager.CreateWebSocketConnectionUniqueKey("dapp1", "192.168.1.3", "ws-3")

	// Generate unique router IDs for each client
	routerID1 := manager.idMapper.GenerateRouterID(client1Key)
	routerID2 := manager.idMapper.GenerateRouterID(client2Key)
	routerID3 := manager.idMapper.GenerateRouterID(client3Key)

	// Store in active subscription
	activeSub.clientRouterIDs[client1Key] = routerID1
	activeSub.clientRouterIDs[client2Key] = routerID2
	activeSub.clientRouterIDs[client3Key] = routerID3

	// Verify each client has a unique router ID
	assert.Len(t, activeSub.clientRouterIDs, 3)
	assert.Equal(t, routerID1, activeSub.clientRouterIDs[client1Key])
	assert.Equal(t, routerID2, activeSub.clientRouterIDs[client2Key])
	assert.Equal(t, routerID3, activeSub.clientRouterIDs[client3Key])

	// Verify router IDs are all different
	assert.NotEqual(t, routerID1, routerID2)
	assert.NotEqual(t, routerID2, routerID3)
	assert.NotEqual(t, routerID1, routerID3)

	// Simulate client 2 disconnecting
	delete(activeSub.clientRouterIDs, client2Key)

	// Client 1 and 3 should still have their router IDs
	assert.Len(t, activeSub.clientRouterIDs, 2)
	assert.Equal(t, routerID1, activeSub.clientRouterIDs[client1Key])
	assert.Equal(t, routerID3, activeSub.clientRouterIDs[client3Key])
}

// TestUnsubscribeRateLimiting verifies that unsubscribe requests are rate limited.
// This prevents clients from spamming unsubscribe requests which could cause
// unnecessary load on the subscription manager.
func TestUnsubscribeRateLimiting(t *testing.T) {
	// Create a config with very low unsubscribe limit for testing
	config := &WebsocketConfig{
		MaxSubscriptionsPerClient:       25,
		PerClientLimitEnforcement:       "warn",
		MaxTotalSubscriptions:           5000,
		TotalLimitEnforcement:           "warn",
		SubscriptionSharingEnabled:      true,
		SubscriptionsPerMinutePerClient: 10,
		UnsubscribesPerMinutePerClient:  2, // Very low limit for testing
		MaxMessageSize:                  1048576,
		CleanupInterval:                 time.Minute,
	}

	nodeUrl := &common.NodeUrl{Url: "wss://test.example.com"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}
	metricsManager := getTestMetricsManager()

	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		config,
	)

	ctx := context.Background()

	// First two unsubscribes should succeed (within burst limit)
	for i := 0; i < 2; i++ {
		protocolMessage := &mockWSProtocolMessage{
			method: "eth_unsubscribe",
			params: []interface{}{fmt.Sprintf("0x%d", i)},
		}

		_, err := manager.Unsubscribe(ctx, protocolMessage, "dapp1", "192.168.1.1", "ws-uid-123", nil)
		// These will return "subscription not found" but shouldn't be rate limited
		assert.Equal(t, common.SubscriptionNotFoundError, err,
			"Unsubscribe %d should return subscription not found (not rate limited)", i+1)
	}

	// Third unsubscribe should be rate limited
	protocolMessage := &mockWSProtocolMessage{
		method: "eth_unsubscribe",
		params: []interface{}{"0x999"},
	}

	_, err := manager.Unsubscribe(ctx, protocolMessage, "dapp1", "192.168.1.1", "ws-uid-123", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsubscribe rate limit exceeded",
		"Third unsubscribe should be rate limited")
}

// TestUnsubscribeAllNotRateLimited verifies that UnsubscribeAll is NOT rate limited
// since it's a cleanup operation that should always succeed.
func TestUnsubscribeAllNotRateLimited(t *testing.T) {
	// Create a config with very low unsubscribe limit
	config := &WebsocketConfig{
		MaxSubscriptionsPerClient:       25,
		PerClientLimitEnforcement:       "warn",
		MaxTotalSubscriptions:           5000,
		TotalLimitEnforcement:           "warn",
		SubscriptionSharingEnabled:      true,
		SubscriptionsPerMinutePerClient: 10,
		UnsubscribesPerMinutePerClient:  1, // Extremely low limit
		MaxMessageSize:                  1048576,
		CleanupInterval:                 time.Minute,
	}

	nodeUrl := &common.NodeUrl{Url: "wss://test.example.com"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}
	metricsManager := getTestMetricsManager()

	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		config,
	)

	ctx := context.Background()

	// Multiple UnsubscribeAll calls should all succeed (not rate limited)
	for i := 0; i < 5; i++ {
		err := manager.UnsubscribeAll(ctx, "dapp1", "192.168.1.1", fmt.Sprintf("ws-uid-%d", i), nil)
		assert.NoError(t, err, "UnsubscribeAll %d should not be rate limited", i+1)
	}
}

// =============================================================================
// Multi-Client Flow Integration Tests
// =============================================================================
// These tests exercise the full multi-client subscription flows to ensure:
// 1. Multiple clients can share a subscription with unique router IDs
// 2. Clients can unsubscribe independently without affecting others
// 3. Reconnection properly updates connection bookkeeping
// =============================================================================

// TestMultiClientJoinExistingSubscription tests that when multiple clients subscribe
// to the same parameters, each gets a unique router ID and can operate independently.
// This is the core deduplication flow test.
func TestMultiClientJoinExistingSubscription(t *testing.T) {
	metricsManager := getTestMetricsManager()
	nodeUrl := &common.NodeUrl{Url: "wss://test.example.com"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}

	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		nil,
	)

	ctx := context.Background()
	hashedParams := "test-subscription-params-hash"

	// Manually set up an active subscription (simulating first client)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	upstreamID := "0xupstream123"
	client1Key := manager.CreateWebSocketConnectionUniqueKey("dapp1", "192.168.1.1", "ws-1")
	client1RouterID := manager.idMapper.GenerateRouterID(client1Key)
	manager.idMapper.RegisterMapping(client1RouterID, upstreamID)

	// Create reply channel for client 1
	client1ReplyChan := make(chan *pairingtypes.RelayReply, 10)
	client1Sender := common.NewSafeChannelSender(subCtx, client1ReplyChan)

	// Create first reply data
	firstReplyData, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"result":  client1RouterID,
	})

	activeSub := &directActiveSubscription{
		upstreamID:       upstreamID,
		clientRouterIDs:  make(map[string]string),
		connectedClients: make(map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]),
		hashedParams:     hashedParams,
		firstReply:       &pairingtypes.RelayReply{Data: firstReplyData},
		ctx:              subCtx,
		cancel:           cancel,
		messagesChan:     make(chan *rpcclient.JsonrpcMessage, 100),
	}
	activeSub.connectedClients[client1Key] = client1Sender
	activeSub.clientRouterIDs[client1Key] = client1RouterID

	// Store in manager
	manager.lock.Lock()
	manager.activeSubscriptions[hashedParams] = activeSub
	manager.connectedClients[client1Key] = make(map[string]*common.SafeChannelSender[*pairingtypes.RelayReply])
	manager.connectedClients[client1Key][hashedParams] = client1Sender
	manager.lock.Unlock()

	// Now client 2 joins the existing subscription
	client2Key := manager.CreateWebSocketConnectionUniqueKey("dapp2", "192.168.1.2", "ws-2")
	client2ReplyChan := make(chan *pairingtypes.RelayReply, 10)
	client2Sender := common.NewSafeChannelSender(subCtx, client2ReplyChan)

	// Call checkForActiveSubscriptionAndConnect for client 2
	// Use a different request ID to verify proper ID handling
	client2RequestID := json.RawMessage("2")
	reply, joined := manager.checkForActiveSubscriptionAndConnect(subCtx, hashedParams, client2Key, client2Sender, client2RequestID)

	// Verify client 2 joined successfully
	assert.True(t, joined, "Client 2 should have joined existing subscription")
	assert.NotNil(t, reply, "Client 2 should receive a reply")

	// Verify client 2 got a unique router ID
	manager.lock.RLock()
	client2RouterID := activeSub.clientRouterIDs[client2Key]
	manager.lock.RUnlock()

	assert.NotEmpty(t, client2RouterID, "Client 2 should have a router ID")
	assert.NotEqual(t, client1RouterID, client2RouterID,
		"Client 2's router ID should be different from Client 1's")

	// Verify the reply contains client 2's unique router ID and correct request ID
	var replyData map[string]interface{}
	err := json.Unmarshal(reply.Data, &replyData)
	require.NoError(t, err)
	assert.Equal(t, client2RouterID, replyData["result"],
		"Reply should contain Client 2's unique router ID")
	// Verify the response ID matches client 2's request ID (JSON-RPC compliance)
	assert.Equal(t, float64(2), replyData["id"],
		"Reply should contain Client 2's request ID (2), not a hard-coded value")

	// Verify both clients are tracked
	manager.lock.RLock()
	assert.Len(t, activeSub.connectedClients, 2, "Should have 2 connected clients")
	assert.Len(t, activeSub.clientRouterIDs, 2, "Should have 2 client router IDs")
	manager.lock.RUnlock()

	// Verify ID mapper has both mappings
	upstream1, found1 := manager.idMapper.GetUpstreamID(client1RouterID)
	upstream2, found2 := manager.idMapper.GetUpstreamID(client2RouterID)
	assert.True(t, found1, "Client 1's router ID should be in mapper")
	assert.True(t, found2, "Client 2's router ID should be in mapper")
	assert.Equal(t, upstreamID, upstream1, "Client 1's mapping should point to upstream")
	assert.Equal(t, upstreamID, upstream2, "Client 2's mapping should point to upstream")
}

// TestMultiClientIndependentUnsubscribe verifies that when one client unsubscribes
// from a shared subscription, other clients remain connected and functional.
// This is the critical test for the deduplication bug fix.
func TestMultiClientIndependentUnsubscribe(t *testing.T) {
	metricsManager := getTestMetricsManager()
	nodeUrl := &common.NodeUrl{Url: "wss://test.example.com"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}

	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		nil,
	)

	ctx := context.Background()
	hashedParams := "shared-subscription-hash"
	upstreamID := "0xupstreamShared"

	// Set up 3 clients sharing the same subscription
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	client1Key := manager.CreateWebSocketConnectionUniqueKey("dapp1", "10.0.0.1", "ws-1")
	client2Key := manager.CreateWebSocketConnectionUniqueKey("dapp2", "10.0.0.2", "ws-2")
	client3Key := manager.CreateWebSocketConnectionUniqueKey("dapp3", "10.0.0.3", "ws-3")

	// Generate unique router IDs for each client
	client1RouterID := manager.idMapper.GenerateRouterID(client1Key)
	client2RouterID := manager.idMapper.GenerateRouterID(client2Key)
	client3RouterID := manager.idMapper.GenerateRouterID(client3Key)

	// Register all mappings to the same upstream
	manager.idMapper.RegisterMapping(client1RouterID, upstreamID)
	manager.idMapper.RegisterMapping(client2RouterID, upstreamID)
	manager.idMapper.RegisterMapping(client3RouterID, upstreamID)

	// Create channels for each client
	client1Chan := make(chan *pairingtypes.RelayReply, 10)
	client2Chan := make(chan *pairingtypes.RelayReply, 10)
	client3Chan := make(chan *pairingtypes.RelayReply, 10)

	client1Sender := common.NewSafeChannelSender(subCtx, client1Chan)
	client2Sender := common.NewSafeChannelSender(subCtx, client2Chan)
	client3Sender := common.NewSafeChannelSender(subCtx, client3Chan)

	// Create active subscription with all 3 clients
	activeSub := &directActiveSubscription{
		upstreamID:       upstreamID,
		clientRouterIDs:  make(map[string]string),
		connectedClients: make(map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]),
		hashedParams:     hashedParams,
		ctx:              subCtx,
		cancel:           cancel,
		messagesChan:     make(chan *rpcclient.JsonrpcMessage, 100),
	}
	activeSub.connectedClients[client1Key] = client1Sender
	activeSub.connectedClients[client2Key] = client2Sender
	activeSub.connectedClients[client3Key] = client3Sender
	activeSub.clientRouterIDs[client1Key] = client1RouterID
	activeSub.clientRouterIDs[client2Key] = client2RouterID
	activeSub.clientRouterIDs[client3Key] = client3RouterID

	// Store in manager
	manager.lock.Lock()
	manager.activeSubscriptions[hashedParams] = activeSub
	manager.connectedClients[client1Key] = map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{hashedParams: client1Sender}
	manager.connectedClients[client2Key] = map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{hashedParams: client2Sender}
	manager.connectedClients[client3Key] = map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{hashedParams: client3Sender}
	manager.lock.Unlock()

	// Verify initial state
	assert.Equal(t, 3, len(activeSub.connectedClients), "Should start with 3 clients")

	// CLIENT 2 UNSUBSCRIBES
	// This simulates: eth_unsubscribe(client2RouterID)
	protocolMessage := &mockWSProtocolMessage{
		method: "eth_unsubscribe",
		params: []interface{}{client2RouterID},
	}
	_, err := manager.Unsubscribe(ctx, protocolMessage, "dapp2", "10.0.0.2", "ws-2", nil)
	assert.NoError(t, err, "Client 2 unsubscribe should succeed")

	// Verify client 2 was removed but clients 1 and 3 remain
	manager.lock.RLock()
	assert.Equal(t, 2, len(activeSub.connectedClients),
		"Should have 2 clients after Client 2 unsubscribed")
	_, client1Exists := activeSub.connectedClients[client1Key]
	_, client2Exists := activeSub.connectedClients[client2Key]
	_, client3Exists := activeSub.connectedClients[client3Key]
	manager.lock.RUnlock()

	assert.True(t, client1Exists, "Client 1 should still be connected")
	assert.False(t, client2Exists, "Client 2 should be disconnected")
	assert.True(t, client3Exists, "Client 3 should still be connected")

	// Verify ID mapper state
	_, found1 := manager.idMapper.GetUpstreamID(client1RouterID)
	_, found2 := manager.idMapper.GetUpstreamID(client2RouterID)
	_, found3 := manager.idMapper.GetUpstreamID(client3RouterID)

	assert.True(t, found1, "Client 1's router ID should still be in mapper")
	assert.False(t, found2, "Client 2's router ID should be removed from mapper")
	assert.True(t, found3, "Client 3's router ID should still be in mapper")

	// Verify the subscription is still active (not torn down)
	manager.lock.RLock()
	_, subExists := manager.activeSubscriptions[hashedParams]
	manager.lock.RUnlock()
	assert.True(t, subExists, "Subscription should still be active after one client unsubscribed")

	// CLIENT 3 UNSUBSCRIBES
	protocolMessage3 := &mockWSProtocolMessage{
		method: "eth_unsubscribe",
		params: []interface{}{client3RouterID},
	}
	_, err = manager.Unsubscribe(ctx, protocolMessage3, "dapp3", "10.0.0.3", "ws-3", nil)
	assert.NoError(t, err, "Client 3 unsubscribe should succeed")

	// Verify only client 1 remains
	manager.lock.RLock()
	assert.Equal(t, 1, len(activeSub.connectedClients),
		"Should have 1 client after Client 3 unsubscribed")
	manager.lock.RUnlock()

	// Subscription should STILL be active (client 1 is still connected)
	manager.lock.RLock()
	_, subExists = manager.activeSubscriptions[hashedParams]
	manager.lock.RUnlock()
	assert.True(t, subExists, "Subscription should still be active with 1 client")
}

// TestRouteMessageToClientsPerClientID verifies that when routing messages to clients,
// each client receives the message with their own unique subscription ID.
func TestRouteMessageToClientsPerClientID(t *testing.T) {
	metricsManager := getTestMetricsManager()
	nodeUrl := &common.NodeUrl{Url: "wss://test.example.com"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}

	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		nil,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hashedParams := "route-test-hash"
	upstreamID := "0xupstreamRoute"

	// Set up 2 clients
	client1Key := manager.CreateWebSocketConnectionUniqueKey("dapp1", "10.0.0.1", "ws-1")
	client2Key := manager.CreateWebSocketConnectionUniqueKey("dapp2", "10.0.0.2", "ws-2")

	client1RouterID := "rs_client1_00001"
	client2RouterID := "rs_client2_00001"

	// Create buffered channels to receive routed messages
	client1Chan := make(chan *pairingtypes.RelayReply, 10)
	client2Chan := make(chan *pairingtypes.RelayReply, 10)

	client1Sender := common.NewSafeChannelSender(ctx, client1Chan)
	client2Sender := common.NewSafeChannelSender(ctx, client2Chan)

	// Create active subscription
	activeSub := &directActiveSubscription{
		upstreamID:       upstreamID,
		clientRouterIDs:  make(map[string]string),
		connectedClients: make(map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]),
		hashedParams:     hashedParams,
		ctx:              ctx,
		cancel:           cancel,
	}
	activeSub.connectedClients[client1Key] = client1Sender
	activeSub.connectedClients[client2Key] = client2Sender
	activeSub.clientRouterIDs[client1Key] = client1RouterID
	activeSub.clientRouterIDs[client2Key] = client2RouterID

	// Create an upstream message (eth_subscription notification)
	upstreamMsg := &rpcclient.JsonrpcMessage{
		Method: "eth_subscription",
		Params: json.RawMessage(`{"subscription":"0xupstreamRoute","result":{"blockNumber":"0x123"}}`),
	}

	// Route the message
	manager.routeMessageToClients(hashedParams, activeSub, upstreamMsg)

	// Give a moment for async sends
	time.Sleep(50 * time.Millisecond)

	// Verify client 1 received message with their router ID
	select {
	case reply1 := <-client1Chan:
		var msg1 map[string]interface{}
		err := json.Unmarshal(reply1.Data, &msg1)
		require.NoError(t, err)
		params1, ok := msg1["params"].(map[string]interface{})
		require.True(t, ok, "params should be a map")
		assert.Equal(t, client1RouterID, params1["subscription"],
			"Client 1 should receive message with their router ID")
	default:
		require.Fail(t, "Client 1 should have received a message")
	}

	// Verify client 2 received message with their router ID
	select {
	case reply2 := <-client2Chan:
		var msg2 map[string]interface{}
		err := json.Unmarshal(reply2.Data, &msg2)
		require.NoError(t, err)
		params2, ok := msg2["params"].(map[string]interface{})
		require.True(t, ok, "params should be a map")
		assert.Equal(t, client2RouterID, params2["subscription"],
			"Client 2 should receive message with their router ID")
	default:
		require.Fail(t, "Client 2 should have received a message")
	}
}

// TestReconnectionUpdatesConnectionBookkeeping verifies that after an upstream
// reconnection, the connection bookkeeping is properly updated.
func TestReconnectionUpdatesConnectionBookkeeping(t *testing.T) {
	metricsManager := getTestMetricsManager()
	nodeUrl := &common.NodeUrl{Url: "wss://test.example.com"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}

	config := DefaultWebsocketConfig()
	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		config,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hashedParams := "reconnect-test-hash"
	oldUpstreamID := "0xoldUpstream"
	newUpstreamID := "0xnewUpstream"

	// Set up clients
	client1Key := manager.CreateWebSocketConnectionUniqueKey("dapp1", "10.0.0.1", "ws-1")
	client2Key := manager.CreateWebSocketConnectionUniqueKey("dapp2", "10.0.0.2", "ws-2")

	client1RouterID := manager.idMapper.GenerateRouterID(client1Key)
	client2RouterID := manager.idMapper.GenerateRouterID(client2Key)

	// Register initial mappings
	manager.idMapper.RegisterMapping(client1RouterID, oldUpstreamID)
	manager.idMapper.RegisterMapping(client2RouterID, oldUpstreamID)

	// Create mock connections
	oldConn := &UpstreamWSConnection{}
	newConn := &UpstreamWSConnection{}

	// Create channels
	client1Chan := make(chan *pairingtypes.RelayReply, 10)
	client2Chan := make(chan *pairingtypes.RelayReply, 10)

	client1Sender := common.NewSafeChannelSender(ctx, client1Chan)
	client2Sender := common.NewSafeChannelSender(ctx, client2Chan)

	// Create active subscription with old connection
	activeSub := &directActiveSubscription{
		upstreamID:         oldUpstreamID,
		upstreamConnection: oldConn,
		clientRouterIDs:    make(map[string]string),
		connectedClients:   make(map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]),
		hashedParams:       hashedParams,
		ctx:                ctx,
		cancel:             cancel,
	}
	activeSub.connectedClients[client1Key] = client1Sender
	activeSub.connectedClients[client2Key] = client2Sender
	activeSub.clientRouterIDs[client1Key] = client1RouterID
	activeSub.clientRouterIDs[client2Key] = client2RouterID

	// Verify initial state
	assert.Equal(t, oldConn, activeSub.upstreamConnection, "Should start with old connection")

	// Simulate what handleUpstreamDisconnect does after getting a new connection:
	// 1. Increment new connection's subscription count
	newConn.IncrementSubscriptions()

	// 2. Update the active subscription
	manager.lock.Lock()
	oldConnection := activeSub.upstreamConnection
	activeSub.upstreamID = newUpstreamID
	activeSub.upstreamConnection = newConn
	manager.lock.Unlock()

	// 3. Update ID mappings
	manager.idMapper.RemoveAllForUpstream(oldUpstreamID)
	manager.idMapper.RegisterMapping(client1RouterID, newUpstreamID)
	manager.idMapper.RegisterMapping(client2RouterID, newUpstreamID)

	// Verify connection was updated
	assert.Equal(t, newConn, activeSub.upstreamConnection, "Should have new connection")
	assert.NotEqual(t, oldConnection, activeSub.upstreamConnection, "Connection should have changed")

	// Verify new connection has subscription count
	assert.Equal(t, int32(1), newConn.subscriptionCount.Load(),
		"New connection should have subscription count incremented")

	// Verify ID mappings were updated
	upstream1, found1 := manager.idMapper.GetUpstreamID(client1RouterID)
	upstream2, found2 := manager.idMapper.GetUpstreamID(client2RouterID)

	assert.True(t, found1, "Client 1's router ID should be in mapper")
	assert.True(t, found2, "Client 2's router ID should be in mapper")
	assert.Equal(t, newUpstreamID, upstream1, "Client 1 should map to NEW upstream")
	assert.Equal(t, newUpstreamID, upstream2, "Client 2 should map to NEW upstream")

	// Verify old upstream has no mappings
	oldRouterIDs := manager.idMapper.GetRouterIDs(oldUpstreamID)
	assert.Empty(t, oldRouterIDs, "Old upstream should have no router ID mappings")

	// Verify new upstream has both mappings
	newRouterIDs := manager.idMapper.GetRouterIDs(newUpstreamID)
	assert.Len(t, newRouterIDs, 2, "New upstream should have 2 router ID mappings")
	assert.Contains(t, newRouterIDs, client1RouterID)
	assert.Contains(t, newRouterIDs, client2RouterID)
}

// TestUnsubscribeRouterIDOwnershipValidation verifies that clients can only unsubscribe
// using their own router IDs and not IDs belonging to other clients.
// This is a critical security test to prevent one client from disrupting another's subscriptions.
func TestUnsubscribeRouterIDOwnershipValidation(t *testing.T) {
	// Create a config with high limits to avoid rate limiting
	config := &WebsocketConfig{
		MaxSubscriptionsPerClient:       25,
		PerClientLimitEnforcement:       "warn",
		MaxTotalSubscriptions:           5000,
		TotalLimitEnforcement:           "warn",
		SubscriptionSharingEnabled:      true,
		SubscriptionsPerMinutePerClient: 60, // High limit to avoid rate limiting
		UnsubscribesPerMinutePerClient:  60,
		MaxMessageSize:                  1048576,
		CleanupInterval:                 time.Minute,
	}

	nodeUrl := &common.NodeUrl{Url: "wss://test.example.com"}
	wsEndpoints := []*common.NodeUrl{nodeUrl}
	metricsManager := getTestMetricsManager()

	manager := NewDirectWSSubscriptionManager(
		metricsManager,
		"jsonrpc",
		"ETH",
		"jsonrpc",
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		config,
	)

	// Create two different clients
	client1Key := manager.CreateWebSocketConnectionUniqueKey("dapp1", "192.168.1.1", "ws-1")
	client2Key := manager.CreateWebSocketConnectionUniqueKey("dapp2", "192.168.1.2", "ws-2")

	// Generate router IDs for both clients
	client1RouterID := manager.idMapper.GenerateRouterID(client1Key)
	client2RouterID := manager.idMapper.GenerateRouterID(client2Key)
	upstreamID := "0x12345"

	// Register mappings for both clients to the same upstream
	manager.idMapper.RegisterMapping(client1RouterID, upstreamID)
	manager.idMapper.RegisterMapping(client2RouterID, upstreamID)

	// Create an active subscription with both clients
	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client1ReplyChan := make(chan *pairingtypes.RelayReply, 10)
	client2ReplyChan := make(chan *pairingtypes.RelayReply, 10)
	client1Sender := common.NewSafeChannelSender(subCtx, client1ReplyChan)
	client2Sender := common.NewSafeChannelSender(subCtx, client2ReplyChan)

	hashedParams := "test_params_hash"
	activeSub := &directActiveSubscription{
		connectedClients: map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{
			client1Key: client1Sender,
			client2Key: client2Sender,
		},
		clientRouterIDs: map[string]string{
			client1Key: client1RouterID,
			client2Key: client2RouterID,
		},
		hashedParams: hashedParams,
	}

	manager.lock.Lock()
	manager.activeSubscriptions[hashedParams] = activeSub
	manager.lock.Unlock()

	ctx := context.Background()

	// Test 1: Client 2 tries to unsubscribe using Client 1's router ID (should fail)
	// Create a protocol message with client 1's router ID
	protocolMessage1 := &mockWSProtocolMessage{
		method: "eth_unsubscribe",
		params: []interface{}{client1RouterID},
	}

	// Client 2 attempts to unsubscribe client 1's subscription (should be rejected)
	_, err := manager.Unsubscribe(ctx, protocolMessage1, "dapp2", "192.168.1.2", "ws-2", nil)
	assert.Equal(t, common.SubscriptionNotFoundError, err,
		"Client 2 should NOT be able to unsubscribe using Client 1's router ID")

	// Verify client 1's subscription is still intact
	manager.lock.RLock()
	_, client1StillConnected := activeSub.connectedClients[client1Key]
	client1RouterStillExists := activeSub.clientRouterIDs[client1Key] == client1RouterID
	manager.lock.RUnlock()

	assert.True(t, client1StillConnected, "Client 1 should still be connected")
	assert.True(t, client1RouterStillExists, "Client 1's router ID should still exist")

	// Verify ID mapper still has client 1's mapping
	_, found := manager.idMapper.GetUpstreamID(client1RouterID)
	assert.True(t, found, "Client 1's ID mapping should still exist")

	// Test 2: Client 1 tries to unsubscribe using their own router ID (should succeed)
	protocolMessage2 := &mockWSProtocolMessage{
		method: "eth_unsubscribe",
		params: []interface{}{client1RouterID},
	}

	_, err = manager.Unsubscribe(ctx, protocolMessage2, "dapp1", "192.168.1.1", "ws-1", nil)
	assert.NoError(t, err, "Client 1 should be able to unsubscribe using their own router ID")

	// Verify client 1 is now disconnected
	manager.lock.RLock()
	_, client1StillConnected = activeSub.connectedClients[client1Key]
	_, client1RouterStillExists = activeSub.clientRouterIDs[client1Key]
	manager.lock.RUnlock()

	assert.False(t, client1StillConnected, "Client 1 should be disconnected after unsubscribe")
	assert.False(t, client1RouterStillExists, "Client 1's router ID should be removed")

	// Verify client 2 is still connected (independent unsubscribe)
	manager.lock.RLock()
	_, client2StillConnected := activeSub.connectedClients[client2Key]
	client2RouterStillExists := activeSub.clientRouterIDs[client2Key] == client2RouterID
	manager.lock.RUnlock()

	assert.True(t, client2StillConnected, "Client 2 should still be connected")
	assert.True(t, client2RouterStillExists, "Client 2's router ID should still exist")
}

// ==================== Tendermint RPC Subscription Tests ====================

// TestExtractTendermintSubscriptionID tests extraction of subscription ID from Tendermint params
func TestExtractTendermintSubscriptionID(t *testing.T) {
	tests := []struct {
		name     string
		params   []byte
		expected string
	}{
		{
			name:     "valid query param",
			params:   []byte(`{"query":"tm.event='NewBlock'"}`),
			expected: "tm.event='NewBlock'",
		},
		{
			name:     "valid query with spaces",
			params:   []byte(`{"query": "tm.event = 'NewBlock'"}`),
			expected: "tm.event = 'NewBlock'",
		},
		{
			name:     "empty params",
			params:   nil,
			expected: "",
		},
		{
			name:     "missing query field",
			params:   []byte(`{"foo":"bar"}`),
			expected: "",
		},
		{
			name:     "invalid json",
			params:   []byte(`{invalid}`),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractTendermintSubscriptionID(tt.params)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestGetSubscriptionID tests the protocol-aware subscription ID extraction
func TestGetSubscriptionID(t *testing.T) {
	// Test EVM (eth_subscribe) - subscription ID from response result
	t.Run("EVM subscription ID from result", func(t *testing.T) {
		msg := &rpcclient.JsonrpcMessage{
			Result: json.RawMessage(`"0x1a2b3c4d"`),
		}
		result := getSubscriptionID("jsonrpc", msg, nil)
		assert.Equal(t, "0x1a2b3c4d", result)
	})

	// Test Tendermint - subscription ID from request params (query)
	t.Run("Tendermint subscription ID from params query", func(t *testing.T) {
		params := []byte(`{"query":"tm.event='NewBlock'"}`)
		result := getSubscriptionID("tendermintrpc", nil, params)
		assert.Equal(t, "tm.event='NewBlock'", result)
	})

	// Test Tendermint with nil response - should still get ID from params
	t.Run("Tendermint with nil response", func(t *testing.T) {
		params := []byte(`{"query":"tm.event='Tx'"}`)
		msg := &rpcclient.JsonrpcMessage{
			Result: json.RawMessage(`{}`), // Empty result (typical for Tendermint)
		}
		result := getSubscriptionID("tendermintrpc", msg, params)
		assert.Equal(t, "tm.event='Tx'", result)
	})
}

// TestRewriteSubscriptionID_Tendermint tests that Tendermint notifications are passed through
func TestRewriteSubscriptionID_Tendermint(t *testing.T) {
	// Tendermint notification format
	tendermintMsg := &rpcclient.JsonrpcMessage{
		Result: json.RawMessage(`{"query":"tm.event='NewBlock'","data":{"block":{"height":"12345"}}}`),
	}

	// Should pass through unchanged (Tendermint doesn't need ID rewriting)
	result, err := rewriteSubscriptionID(tendermintMsg, "router-id-123", false)
	require.NoError(t, err)

	var parsed map[string]json.RawMessage
	err = json.Unmarshal(result, &parsed)
	require.NoError(t, err)

	// Verify the result contains the original query (not rewritten)
	var resultData struct {
		Query string `json:"query"`
	}
	err = json.Unmarshal(parsed["result"], &resultData)
	require.NoError(t, err)
	assert.Equal(t, "tm.event='NewBlock'", resultData.Query)
}

// TestRewriteSubscriptionID_EVM tests that EVM notifications are properly rewritten
func TestRewriteSubscriptionID_EVM(t *testing.T) {
	// EVM notification format
	evmMsg := &rpcclient.JsonrpcMessage{
		Method: "eth_subscription",
		Params: json.RawMessage(`{"subscription":"0xoriginal","result":{"blockNumber":"0x1234"}}`),
	}

	result, err := rewriteSubscriptionID(evmMsg, "0xrouter123", false)
	require.NoError(t, err)

	var parsed struct {
		Method string `json:"method"`
		Params struct {
			Subscription string          `json:"subscription"`
			Result       json.RawMessage `json:"result"`
		} `json:"params"`
	}
	err = json.Unmarshal(result, &parsed)
	require.NoError(t, err)

	assert.Equal(t, "eth_subscription", parsed.Method)
	assert.Equal(t, "0xrouter123", parsed.Params.Subscription) // Should be rewritten
}

// TestRewriteSubscriptionID_Substrate is the unit half of MAG-3345. Substrate uses the
// same params envelope as EVM but names the frame after the payload, so matching on
// method == "eth_subscription" passed these through untouched — the client received the
// upstream id instead of the router id it was issued at subscribe time.
func TestRewriteSubscriptionID_Substrate(t *testing.T) {
	substrateMsg := &rpcclient.JsonrpcMessage{
		Method: "chain_newHead",
		Params: json.RawMessage(`{"subscription":"Ck1rTHhOa1hxTGV3","result":{"number":"0x1234"},"extra":"keepme"}`),
	}

	result, err := rewriteSubscriptionID(substrateMsg, "rs_router_1", false)
	require.NoError(t, err)

	var parsed struct {
		Method string `json:"method"`
		Params struct {
			Subscription string          `json:"subscription"`
			Result       json.RawMessage `json:"result"`
			Extra        json.RawMessage `json:"extra"`
		} `json:"params"`
	}
	require.NoError(t, json.Unmarshal(result, &parsed))

	assert.Equal(t, "chain_newHead", parsed.Method,
		"the frame's own method must survive; the client dispatches on it")
	assert.Equal(t, "rs_router_1", parsed.Params.Subscription,
		"subscription must be the router id, not the upstream id")
	assert.JSONEq(t, `{"number":"0x1234"}`, string(parsed.Params.Result),
		"the payload must be relayed unchanged")
	assert.JSONEq(t, `"keepme"`, string(parsed.Params.Extra),
		"a sibling field must survive: preserving them is the whole reason the rewrite "+
			"rebuilds the envelope from a map instead of a fixed {subscription, result} "+
			"struct, and without this assertion a revert to the struct passes every test")
}

// TestRewriteSubscriptionID_SolanaNumeric supersedes MAG-3345's
// TestRewriteSubscriptionID_SolanaUntouched, which asserted that a Solana frame passed
// through carrying the upstream id. That was the correct boundary while router ids were
// always strings — a string id is one a Solana client cannot hand back to
// accountUnsubscribe. Now that router ids follow the chain's own shape, the frame must be
// rewritten like any other, with the id staying a JSON number (MAG-3359).
func TestRewriteSubscriptionID_SolanaNumeric(t *testing.T) {
	solanaMsg := &rpcclient.JsonrpcMessage{
		Method: "accountNotification",
		Params: json.RawMessage(`{"subscription":23784,"result":{"context":{"slot":5208469}}}`),
	}

	result, err := rewriteSubscriptionID(solanaMsg, "1000001", true)
	require.NoError(t, err)

	var parsed struct {
		Method string `json:"method"`
		Params struct {
			Subscription json.RawMessage `json:"subscription"`
			Result       json.RawMessage `json:"result"`
		} `json:"params"`
	}
	require.NoError(t, json.Unmarshal(result, &parsed))

	assert.Equal(t, "accountNotification", parsed.Method)
	assert.JSONEq(t, `1000001`, string(parsed.Params.Subscription),
		"the router id must replace the upstream id")
	assert.Equal(t, "1000001", string(parsed.Params.Subscription),
		"and must stay a JSON number — a quoted id is one accountUnsubscribe cannot use")
	assert.JSONEq(t, `{"context":{"slot":5208469}}`, string(parsed.Params.Result))
}

// TestRewriteSubscriptionID_UnusableIDIsAnError covers an envelope that names its
// subscription in a shape no router id can stand in for.
//
// Passing it through would be worse than failing: every client sharing the subscription
// would receive the same UPSTREAM id, silently defeating the per-client indirection that
// unsubscribe depends on. routeMessageToClients logs the error and drops the frame for that
// client, which is what the pre-MAG-3345 code did for a params blob it could not parse.
func TestRewriteSubscriptionID_UnusableIDIsAnError(t *testing.T) {
	msg := &rpcclient.JsonrpcMessage{
		Method: "oddNotification",
		Params: json.RawMessage(`{"subscription":{"nested":true},"result":{}}`),
	}

	_, err := rewriteSubscriptionID(msg, "1000001", false)
	require.Error(t, err, "an object id must not be silently passed through")
	assert.Contains(t, err.Error(), "neither a string nor a number")
}

// TestRewriteSubscriptionID_NonEnvelopePassesThrough keeps that error off frames that are
// not subscription envelopes at all — those still fall through to the other shapes.
func TestRewriteSubscriptionID_NonEnvelopePassesThrough(t *testing.T) {
	msg := &rpcclient.JsonrpcMessage{
		Method: "someNotification",
		Params: json.RawMessage(`{"height":"12345"}`),
	}

	result, err := rewriteSubscriptionID(msg, "1000001", false)
	require.NoError(t, err, "params that name no subscription are not this function's business")
	assert.Contains(t, string(result), `"height":"12345"`)
}

// TestExtractSubscriptionID_Numeric covers the upstream id arriving as a number. Returning
// "" here — which is what a string-only decode did — left the router with no mapping to
// translate back to on unsubscribe.
func TestExtractSubscriptionID_Numeric(t *testing.T) {
	assert.Equal(t, "23784", extractSubscriptionID(&rpcclient.JsonrpcMessage{
		Result: json.RawMessage(`23784`),
	}), "a numbered subscription must canonicalise to its decimal string")

	assert.Equal(t, "0xabc", extractSubscriptionID(&rpcclient.JsonrpcMessage{
		Result: json.RawMessage(`"0xabc"`),
	}), "a named subscription is unchanged")

	assert.Equal(t, "", extractSubscriptionID(&rpcclient.JsonrpcMessage{
		Result: json.RawMessage(`{"query":"tm.event='NewBlock'"}`),
	}), "an object result names no subscription")
}

// TestSubscriptionIDValue_ShapesForTheWire pins the one place the string/number distinction
// is allowed to exist: the wire. Everything inside the router is the decimal string.
func TestSubscriptionIDValue_ShapesForTheWire(t *testing.T) {
	assert.Equal(t, int64(1000001), subscriptionIDValue("1000001", true))
	assert.Equal(t, "1000001", subscriptionIDValue("1000001", false))
	assert.Equal(t, "rs_abc123_00001", subscriptionIDValue("rs_abc123_00001", true),
		"an id that is not a number must not be mangled into one")
}

// TestDirectWSSubscriptionManager_TendermintAPIInterface tests manager with Tendermint API interface
func TestDirectWSSubscriptionManager_TendermintAPIInterface(t *testing.T) {
	// Use nil for metricsManager to avoid Prometheus registration conflicts in tests
	wsEndpoints := []*common.NodeUrl{{Url: "ws://localhost:26657/websocket"}}

	manager := NewDirectWSSubscriptionManager(
		nil, // No metrics manager for this test
		"",
		"COSMOSHUB",
		"tendermintrpc", // Tendermint API interface
		wsEndpoints,
		nil, // wsBackupEndpoints — none for primary-only test
		nil,
		nil,
	)

	assert.Equal(t, "tendermintrpc", manager.apiInterface)
	assert.Equal(t, "COSMOSHUB", manager.chainID)
}

// TestUnsubscribeParamsExtraction_Tendermint tests extraction of subscription ID from Tendermint unsubscribe params
func TestUnsubscribeParamsExtraction_Tendermint(t *testing.T) {
	// Tendermint unsubscribe format: {"query": "tm.event='NewBlock'"}
	params := []byte(`{"query":"tm.event='NewBlock'"}`)

	// Parse params and extract query
	var paramsMap map[string]any
	err := json.Unmarshal(params, &paramsMap)
	require.NoError(t, err)

	query, ok := paramsMap["query"].(string)
	require.True(t, ok)
	assert.Equal(t, "tm.event='NewBlock'", query)
}

// TestUnsubscribeParamsExtraction_EVM tests extraction of subscription ID from EVM unsubscribe params
func TestUnsubscribeParamsExtraction_EVM(t *testing.T) {
	// EVM unsubscribe format: ["0x1a2b3c4d"]
	params := []byte(`["0x1a2b3c4d"]`)

	// Parse params and extract subscription ID
	var paramsArray []string
	err := json.Unmarshal(params, &paramsArray)
	require.NoError(t, err)
	require.Len(t, paramsArray, 1)
	assert.Equal(t, "0x1a2b3c4d", paramsArray[0])
}

// TestCreateSubscriptionReply_Tendermint verifies that Tendermint subscription responses
// preserve the original {"result":{"query":"..."}} format instead of using router IDs
func TestCreateSubscriptionReply_Tendermint(t *testing.T) {
	// Simulated Tendermint subscribe response from upstream
	originalMsg := &rpcclient.JsonrpcMessage{
		Version: "2.0",
		ID:      json.RawMessage(`1`),
		Result:  json.RawMessage(`{"query":"tm.event='NewBlock'"}`),
	}

	// For Tendermint, should return original result format (not router ID)
	replyData, err := createSubscriptionReply("router-id-ignored", json.RawMessage(`1`), originalMsg, "tendermintrpc", false)
	require.NoError(t, err)

	// Parse the response
	var response struct {
		Jsonrpc string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
	}
	err = json.Unmarshal(replyData, &response)
	require.NoError(t, err)

	// Verify the result contains the query object, NOT a router ID
	var result struct {
		Query string `json:"query"`
	}
	err = json.Unmarshal(response.Result, &result)
	require.NoError(t, err)
	assert.Equal(t, "tm.event='NewBlock'", result.Query, "Tendermint response should preserve query object")
}

// TestCreateSubscriptionReply_Tendermint_PreservesUpstreamFields guards against
// the Tendermint reply becoming lossy. The pre-PR code returned json.Marshal(originalMsg)
// verbatim; the post-PR fix must still preserve every top-level field other than id,
// otherwise a non-nil Error envelope (or any future field a Tendermint client depends on)
// silently disappears.
func TestCreateSubscriptionReply_Tendermint_PreservesUpstreamFields(t *testing.T) {
	originalMsg := &rpcclient.JsonrpcMessage{
		Version: "2.0",
		ID:      json.RawMessage(`1`),
		Result:  json.RawMessage(`{"query":"tm.event='NewBlock'"}`),
		// Error is the realistic "extra field" most likely to appear on a
		// subscription reply; if dropped, the client never sees that the
		// upstream subscribe actually failed.
		Error: &rpcclient.JsonError{
			Code:    -32000,
			Message: "upstream subscription rejected",
		},
	}

	replyData, err := createSubscriptionReply("ignored", json.RawMessage(`"abc"`), originalMsg, "tendermintrpc", false)
	require.NoError(t, err)

	var resp map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(replyData, &resp))

	// id must be the caller's, not the upstream's.
	assert.JSONEq(t, `"abc"`, string(resp["id"]))
	// All other fields the upstream sent must survive — this is the regression guard.
	require.Contains(t, resp, "result", "result must be preserved")
	require.Contains(t, resp, "error", "error envelope must be preserved when upstream returned one")
	assert.JSONEq(t, `{"query":"tm.event='NewBlock'"}`, string(resp["result"]))
	var errObj struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(resp["error"], &errObj))
	assert.Equal(t, -32000, errObj.Code)
	assert.Equal(t, "upstream subscription rejected", errObj.Message)
}

// TestCreateSubscriptionReply_EVM verifies that EVM subscription responses use router IDs
func TestCreateSubscriptionReply_EVM(t *testing.T) {
	// Simulated EVM eth_subscribe response from upstream
	originalMsg := &rpcclient.JsonrpcMessage{
		Version: "2.0",
		ID:      json.RawMessage(`1`),
		Result:  json.RawMessage(`"0xupstream123"`),
	}

	routerID := "0xrouter456"

	// For EVM, should replace upstream ID with router ID
	replyData, err := createSubscriptionReply(routerID, json.RawMessage(`1`), originalMsg, "jsonrpc", false)
	require.NoError(t, err)

	// Parse the response
	var response struct {
		Jsonrpc string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  string          `json:"result"`
	}
	err = json.Unmarshal(replyData, &response)
	require.NoError(t, err)

	// Verify the result is the router ID
	assert.Equal(t, routerID, response.Result, "EVM response should use router ID")
}

// TestCreateSubscriptionReplyFromRouterID_Tendermint verifies that joining clients
// receive the correct Tendermint response format with their request ID
func TestCreateSubscriptionReplyFromRouterID_Tendermint(t *testing.T) {
	// Original result from the first subscription
	originalResult := json.RawMessage(`{"query":"tm.event='NewBlock'"}`)

	// Client's request ID (different from original)
	clientRequestID := json.RawMessage(`42`)

	// For Tendermint, should return query format with client's request ID
	replyData, err := createSubscriptionReplyFromRouterID("router-id-ignored", clientRequestID, "tendermintrpc", originalResult, false)
	require.NoError(t, err)

	// Parse the response
	var response struct {
		Jsonrpc string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
	}
	err = json.Unmarshal(replyData, &response)
	require.NoError(t, err)

	// Verify the ID is the client's request ID
	assert.Equal(t, "42", string(response.ID), "Response should use client's request ID")

	// Verify the result contains the query object
	var result struct {
		Query string `json:"query"`
	}
	err = json.Unmarshal(response.Result, &result)
	require.NoError(t, err)
	assert.Equal(t, "tm.event='NewBlock'", result.Query, "Joining client should receive query format")
}

// TestCreateSubscriptionReplyFromRouterID_EVM verifies that joining clients
// receive router IDs for EVM subscriptions
func TestCreateSubscriptionReplyFromRouterID_EVM(t *testing.T) {
	clientRequestID := json.RawMessage(`99`)
	clientRouterID := "0xclient-router-id"

	// For EVM, should return router ID (originalResult not used)
	replyData, err := createSubscriptionReplyFromRouterID(clientRouterID, clientRequestID, "jsonrpc", nil, false)
	require.NoError(t, err)

	// Parse the response
	var response struct {
		Jsonrpc string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  string          `json:"result"`
	}
	err = json.Unmarshal(replyData, &response)
	require.NoError(t, err)

	// Verify the ID and result
	assert.Equal(t, "99", string(response.ID), "Response should use client's request ID")
	assert.Equal(t, clientRouterID, response.Result, "EVM joining client should receive router ID")
}

// TestTendermintSubscriptionEndToEnd tests the full subscription flow for Tendermint
// to ensure responses and notifications have the correct format
func TestTendermintSubscriptionEndToEnd(t *testing.T) {
	t.Run("subscribe response preserves query format", func(t *testing.T) {
		// Simulated upstream response
		upstreamResponse := &rpcclient.JsonrpcMessage{
			Version: "2.0",
			ID:      json.RawMessage(`1`),
			Result:  json.RawMessage(`{"query":"tm.event='Tx'"}`),
		}

		// Create reply for Tendermint
		reply, err := createSubscriptionReply("any-router-id", json.RawMessage(`1`), upstreamResponse, "tendermintrpc", false)
		require.NoError(t, err)

		// Client should see the query format, not a router ID
		assert.Contains(t, string(reply), `"query":"tm.event='Tx'"`)
		assert.NotContains(t, string(reply), "any-router-id")
	})

	t.Run("notification passthrough for Tendermint", func(t *testing.T) {
		// Simulated Tendermint notification
		notification := &rpcclient.JsonrpcMessage{
			Version: "2.0",
			ID:      nil,
			Result:  json.RawMessage(`{"query":"tm.event='NewBlock'","data":{"type":"tendermint/event/NewBlock","value":{}}}`),
		}

		// Rewrite should pass through unchanged for Tendermint
		rewritten, err := rewriteSubscriptionID(notification, "some-router-id", false)
		require.NoError(t, err)

		// Should still contain the query and data
		assert.Contains(t, string(rewritten), `"query":"tm.event='NewBlock'"`)
		assert.Contains(t, string(rewritten), `"data"`)
		// Should NOT have the router ID injected
		assert.NotContains(t, string(rewritten), "some-router-id")
	})
}

// fakeSubscriptionOptimizer is a minimal WebSocketEndpointOptimizer for cascade tests.
// Used by both DirectWSSubscriptionManager and DirectGRPCSubscriptionManager test suites
// (the interface is shared across both subscription managers despite its name).
// It returns a configurable address from ChooseUpstream; relay-data callbacks are no-ops.
type fakeSubscriptionOptimizer struct {
	chooseFn func(allAddresses []string, ignored map[string]struct{}) []string
}

func (f *fakeSubscriptionOptimizer) ChooseUpstream(_ context.Context, allAddresses []string, ignored map[string]struct{}, _ uint64, _ int64) []string {
	if f.chooseFn != nil {
		return f.chooseFn(allAddresses, ignored)
	}
	return nil
}

func (f *fakeSubscriptionOptimizer) AppendRelayData(_ string, _ time.Duration, _, _ uint64) {}
func (f *fakeSubscriptionOptimizer) AppendRelayFailure(_ string)                            {}

func TestSelectEndpoint_WS_PrimaryOnly_NilBackup(t *testing.T) {
	primary := []*common.NodeUrl{
		{Url: "wss://primary-1.example.com"},
		{Url: "wss://primary-2.example.com"},
	}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc",
		"ETH",
		"jsonrpc",
		primary,
		nil, // wsBackupEndpoints — none
		nil, // optimizer
		nil, // config
	)

	ep, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.NoError(t, err)
	require.NotNil(t, ep)
	// First-available branch with no optimizer and no ignored set returns first endpoint.
	assert.Equal(t, "wss://primary-1.example.com", ep.Url)
}

func TestSelectEndpoint_WS_PrimaryOnly_EmptyBackup(t *testing.T) {
	primary := []*common.NodeUrl{{Url: "wss://primary-1.example.com"}}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc",
		"ETH",
		"jsonrpc",
		primary,
		[]*common.NodeUrl{}, // empty backup, regression guard for nil-vs-empty equivalence
		nil,
		nil,
	)

	ep, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.NoError(t, err)
	assert.Equal(t, "wss://primary-1.example.com", ep.Url)
}

func TestSelectEndpoint_WS_PrimaryExhausted_FallsBackToBackup(t *testing.T) {
	primary := []*common.NodeUrl{
		{Url: "wss://primary-1.example.com"},
		{Url: "wss://primary-2.example.com"},
	}
	backup := []*common.NodeUrl{
		{Url: "wss://backup-1.example.com"},
	}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc",
		"ETH",
		"jsonrpc",
		primary,
		backup,
		nil, // optimizer nil — first-available branch
		nil,
	)

	ignored := map[string]struct{}{
		"wss://primary-1.example.com": {},
		"wss://primary-2.example.com": {},
	}
	ep, err := manager.selectEndpoint(context.Background(), "client-1", ignored)
	require.NoError(t, err)
	require.NotNil(t, ep)
	assert.Equal(t, "wss://backup-1.example.com", ep.Url)
}

func TestSelectEndpoint_WS_PrimaryEmpty_BackupOnly(t *testing.T) {
	backup := []*common.NodeUrl{
		{Url: "wss://backup-1.example.com"},
		{Url: "wss://backup-2.example.com"},
	}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc",
		"ETH",
		"jsonrpc",
		nil, // primary empty
		backup,
		nil,
		nil,
	)

	ep, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.NoError(t, err)
	require.NotNil(t, ep)
	assert.Equal(t, "wss://backup-1.example.com", ep.Url)
}

func TestSelectEndpoint_WS_BothExhausted_ReturnsError(t *testing.T) {
	primary := []*common.NodeUrl{{Url: "wss://primary-1.example.com"}}
	backup := []*common.NodeUrl{{Url: "wss://backup-1.example.com"}}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc",
		"ETH",
		"jsonrpc",
		primary,
		backup,
		nil,
		nil,
	)

	ignored := map[string]struct{}{
		"wss://primary-1.example.com": {},
		"wss://backup-1.example.com":  {},
	}
	_, err := manager.selectEndpoint(context.Background(), "client-1", ignored)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary and backup both exhausted")
}

func TestSelectEndpoint_WS_BothEmpty_ReturnsError(t *testing.T) {
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc",
		"ETH",
		"jsonrpc",
		nil,
		nil,
		nil,
		nil,
	)
	_, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.Error(t, err)
}

func TestSelectEndpoint_WS_StickyOnPrimary_Returns(t *testing.T) {
	primary := []*common.NodeUrl{{Url: "wss://primary-1.example.com"}}
	backup := []*common.NodeUrl{{Url: "wss://backup-1.example.com"}}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc",
		"ETH",
		"jsonrpc",
		primary,
		backup,
		nil,
		nil,
	)

	// Manually set a sticky entry pointing at the primary.
	manager.stickyStore.Set("client-1", &lavasession.StickySession{Provider: "wss://primary-1.example.com"})

	ep, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.NoError(t, err)
	assert.Equal(t, "wss://primary-1.example.com", ep.Url)
}

func TestSelectEndpoint_WS_StickyOnBackup_Returns(t *testing.T) {
	primary := []*common.NodeUrl{{Url: "wss://primary-1.example.com"}}
	backup := []*common.NodeUrl{{Url: "wss://backup-1.example.com"}}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc",
		"ETH",
		"jsonrpc",
		primary,
		backup,
		nil,
		nil,
	)

	manager.stickyStore.Set("client-1", &lavasession.StickySession{Provider: "wss://backup-1.example.com"})

	ep, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.NoError(t, err)
	// Sticky-on-backup must resolve directly via endpointsByURL (which spans both tiers).
	assert.Equal(t, "wss://backup-1.example.com", ep.Url)
}

func TestSelectEndpoint_WS_StickyIgnored_FallsThroughCascade(t *testing.T) {
	primary := []*common.NodeUrl{{Url: "wss://primary-1.example.com"}}
	backup := []*common.NodeUrl{{Url: "wss://backup-1.example.com"}}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc",
		"ETH",
		"jsonrpc",
		primary,
		backup,
		nil,
		nil,
	)

	manager.stickyStore.Set("client-1", &lavasession.StickySession{Provider: "wss://primary-1.example.com"})

	ignored := map[string]struct{}{
		"wss://primary-1.example.com": {},
	}
	ep, err := manager.selectEndpoint(context.Background(), "client-1", ignored)
	require.NoError(t, err)
	assert.Equal(t, "wss://backup-1.example.com", ep.Url)
}

func TestSelectEndpoint_WS_OptimizerOverPrimary_NotConsultedForBackup(t *testing.T) {
	primary := []*common.NodeUrl{
		{Url: "wss://primary-1.example.com"},
		{Url: "wss://primary-2.example.com"},
	}
	backup := []*common.NodeUrl{{Url: "wss://backup-1.example.com"}}

	calls := 0
	opt := &fakeSubscriptionOptimizer{
		chooseFn: func(addresses []string, _ map[string]struct{}) []string {
			calls++
			// Always return the second primary URL — backup must never reach the optimizer.
			return []string{"wss://primary-2.example.com"}
		},
	}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc",
		"ETH",
		"jsonrpc",
		primary,
		backup,
		opt,
		nil,
	)

	ep, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.NoError(t, err)
	assert.Equal(t, "wss://primary-2.example.com", ep.Url)
	// Optimizer must run exactly once (against primary tier). If the cascade
	// erroneously fell into the backup tier, the optimizer would be invoked twice.
	assert.Equal(t, 1, calls)
}

func TestSelectEndpoint_WS_OptimizerOverBackup(t *testing.T) {
	primary := []*common.NodeUrl{
		{Url: "wss://primary-1.example.com"},
		{Url: "wss://primary-2.example.com"},
	}
	backup := []*common.NodeUrl{
		{Url: "wss://backup-1.example.com"},
		{Url: "wss://backup-2.example.com"},
	}

	calls := 0
	opt := &fakeSubscriptionOptimizer{
		chooseFn: func(addresses []string, ignored map[string]struct{}) []string {
			calls++
			// When invoked over the backup tier, return backup-2.
			// When invoked over the primary tier (with both primaries ignored),
			// return nothing so the cascade falls through.
			for _, addr := range addresses {
				if addr == "wss://backup-2.example.com" {
					return []string{"wss://backup-2.example.com"}
				}
			}
			return nil
		},
	}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc",
		"ETH",
		"jsonrpc",
		primary,
		backup,
		opt,
		nil,
	)

	ignored := map[string]struct{}{
		"wss://primary-1.example.com": {},
		"wss://primary-2.example.com": {},
	}
	ep, err := manager.selectEndpoint(context.Background(), "client-1", ignored)
	require.NoError(t, err)
	assert.Equal(t, "wss://backup-2.example.com", ep.Url)
	// Optimizer must be consulted for both tiers: once over primary (returns nil
	// → cascade falls through), then once over backup (returns backup-2).
	assert.Equal(t, 2, calls)
}

func TestSelectEndpoint_WS_EndpointsByURL_IncludesBothTiers(t *testing.T) {
	primary := []*common.NodeUrl{{Url: "wss://primary-1.example.com"}}
	backup := []*common.NodeUrl{{Url: "wss://backup-1.example.com"}}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc",
		"ETH",
		"jsonrpc",
		primary,
		backup,
		nil,
		nil,
	)

	require.Len(t, manager.endpointsByURL, 2)
	assert.NotNil(t, manager.endpointsByURL["wss://primary-1.example.com"])
	assert.NotNil(t, manager.endpointsByURL["wss://backup-1.example.com"])
}

// ==================== MAG-1824 regression coverage ====================

// mockWSProtocolMessageWithRawData is a ProtocolMessage variant that lets a test inject
// raw RelayPrivateData bytes, so we can construct unsubscribe requests with non-numeric
// ids (e.g. string UUIDs) — the scenario the bug targets.
type mockWSProtocolMessageWithRawData struct {
	mockWSProtocolMessage
	rawData []byte
}

func (m *mockWSProtocolMessageWithRawData) RelayPrivateData() *pairingtypes.RelayPrivateData {
	return &pairingtypes.RelayPrivateData{Data: m.rawData}
}

// TestCreateSubscriptionReply_EchoesStringRequestID is the direct regression for MAG-1824's
// first symptom: the response id must be the caller's verbatim string, not the upstream's
// internal counter.
func TestCreateSubscriptionReply_EchoesStringRequestID(t *testing.T) {
	t.Run("EVM with string UUID id", func(t *testing.T) {
		// Upstream returned its own id (typically the rpcclient's nextID() = 1) — this
		// must NOT leak to the response.
		upstream := &rpcclient.JsonrpcMessage{
			Version: "2.0",
			ID:      json.RawMessage(`1`),
			Result:  json.RawMessage(`"0xabc"`),
		}

		clientID := json.RawMessage(`"client-uuid-1"`)
		replyData, err := createSubscriptionReply("rs_abc123_00001", clientID, upstream, "jsonrpc", false)
		require.NoError(t, err)

		var resp map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(replyData, &resp))

		assert.JSONEq(t, `"client-uuid-1"`, string(resp["id"]),
			"id must echo caller's string verbatim")
		assert.JSONEq(t, `"rs_abc123_00001"`, string(resp["result"]),
			"result must be router id, not upstream hex")
	})

	t.Run("EVM with numeric id", func(t *testing.T) {
		upstream := &rpcclient.JsonrpcMessage{
			Version: "2.0",
			ID:      json.RawMessage(`99`), // upstream counter — should not appear
			Result:  json.RawMessage(`"0xabc"`),
		}

		replyData, err := createSubscriptionReply("rs_xxx_00001", json.RawMessage(`42`), upstream, "jsonrpc", false)
		require.NoError(t, err)

		var resp map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(replyData, &resp))

		assert.JSONEq(t, `42`, string(resp["id"]))
	})

	t.Run("Tendermint preserves caller id and query result", func(t *testing.T) {
		upstream := &rpcclient.JsonrpcMessage{
			Version: "2.0",
			ID:      json.RawMessage(`7`),
			Result:  json.RawMessage(`{"query":"tm.event='NewBlock'"}`),
		}

		replyData, err := createSubscriptionReply("ignored", json.RawMessage(`"abc"`), upstream, "tendermintrpc", false)
		require.NoError(t, err)

		var resp map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(replyData, &resp))

		assert.JSONEq(t, `"abc"`, string(resp["id"]),
			"Tendermint must also echo caller's id, not the upstream's")
		assert.JSONEq(t, `{"query":"tm.event='NewBlock'"}`, string(resp["result"]),
			"query format must be preserved")
	})
}

// TestUnsubscribe_AcceptsUpstreamIDFallback is the regression for MAG-1824's second symptom:
// the unsubscribe lookup must succeed even when the client sends back the upstream hex id
// (rather than the router-issued rs_xxx id). Ownership is still gated on the caller having
// an entry in clientRouterIDs for that subscription, so this remains safe.
func TestUnsubscribe_AcceptsUpstreamIDFallback(t *testing.T) {
	config := &WebsocketConfig{
		MaxSubscriptionsPerClient:       25,
		PerClientLimitEnforcement:       "warn",
		MaxTotalSubscriptions:           5000,
		TotalLimitEnforcement:           "warn",
		SubscriptionSharingEnabled:      true,
		SubscriptionsPerMinutePerClient: 60,
		UnsubscribesPerMinutePerClient:  60,
		MaxMessageSize:                  1048576,
		CleanupInterval:                 time.Minute,
	}

	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{{Url: "wss://test.example.com"}},
		nil, nil, config,
	)

	clientKey := manager.CreateWebSocketConnectionUniqueKey("dapp1", "192.168.1.1", "ws-1")
	routerID := manager.idMapper.GenerateRouterID(clientKey)
	upstreamID := "0xupstream-hex-deadbeef"
	manager.idMapper.RegisterMapping(routerID, upstreamID)

	// Second co-tenant on the same shared subscription — keeps len(connectedClients) > 1
	// so the unsubscribe stays on the "remove this client only" branch and doesn't try to
	// tear down the (nil) upstreamSubscription.
	keeperKey := manager.CreateWebSocketConnectionUniqueKey("dappKeeper", "10.0.0.1", "ws-keeper")
	keeperRouterID := manager.idMapper.GenerateRouterID(keeperKey)
	manager.idMapper.RegisterMapping(keeperRouterID, upstreamID)

	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hashedParams := "test-params-hash"
	activeSub := &directActiveSubscription{
		upstreamID:       upstreamID,
		hashedParams:     hashedParams,
		connectedClients: map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{},
		clientRouterIDs:  map[string]string{clientKey: routerID, keeperKey: keeperRouterID},
		ctx:              subCtx,
		cancel:           cancel,
	}
	replyChan := make(chan *pairingtypes.RelayReply, 1)
	keeperReplyChan := make(chan *pairingtypes.RelayReply, 1)
	activeSub.connectedClients[clientKey] = common.NewSafeChannelSender(subCtx, replyChan)
	activeSub.connectedClients[keeperKey] = common.NewSafeChannelSender(subCtx, keeperReplyChan)

	manager.lock.Lock()
	manager.activeSubscriptions[hashedParams] = activeSub
	manager.lock.Unlock()

	// Client sends back the UPSTREAM id, not the router id — historically this would have
	// produced "subscription not found".
	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_unsubscribe",
		"params":  []interface{}{upstreamID},
		"id":      "client-uuid-1",
	})
	require.NoError(t, err)

	pm := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{
			method: "eth_unsubscribe",
			params: []interface{}{upstreamID},
		},
		rawData: body,
	}

	_, err = manager.Unsubscribe(context.Background(), pm, "dapp1", "192.168.1.1", "ws-1", nil)
	assert.NoError(t, err, "unsubscribe should accept the upstream id as a fallback")

	// And confirm the canonical (router-id) path still works for a freshly registered sub.
	clientKey2 := manager.CreateWebSocketConnectionUniqueKey("dapp2", "192.168.2.2", "ws-2")
	routerID2 := manager.idMapper.GenerateRouterID(clientKey2)
	manager.idMapper.RegisterMapping(routerID2, "0xupstream-2")
	keeperKey2 := manager.CreateWebSocketConnectionUniqueKey("dappKeeper2", "10.0.0.2", "ws-keeper2")
	keeperRouterID2 := manager.idMapper.GenerateRouterID(keeperKey2)
	manager.idMapper.RegisterMapping(keeperRouterID2, "0xupstream-2")

	hashedParams2 := "params-2"
	subCtx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	activeSub2 := &directActiveSubscription{
		upstreamID:   "0xupstream-2",
		hashedParams: hashedParams2,
		connectedClients: map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{
			clientKey2: common.NewSafeChannelSender(subCtx2, make(chan *pairingtypes.RelayReply, 1)),
			keeperKey2: common.NewSafeChannelSender(subCtx2, make(chan *pairingtypes.RelayReply, 1)),
		},
		clientRouterIDs: map[string]string{clientKey2: routerID2, keeperKey2: keeperRouterID2},
		ctx:             subCtx2,
		cancel:          cancel2,
	}
	manager.lock.Lock()
	manager.activeSubscriptions[hashedParams2] = activeSub2
	manager.lock.Unlock()

	body2, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_unsubscribe",
		"params":  []interface{}{routerID2},
		"id":      2,
	})
	require.NoError(t, err)
	pm2 := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_unsubscribe", params: []interface{}{routerID2}},
		rawData:               body2,
	}
	_, err = manager.Unsubscribe(context.Background(), pm2, "dapp2", "192.168.2.2", "ws-2", nil)
	assert.NoError(t, err, "router-id unsubscribe path must still work")
}

// TestEndToEnd_SubscribeUnsubscribeRoundTrip is the closest in-process analogue of the
// MAG-1824 smoke test (tests/release_smoke/test_ws_subscribe_smoke.py): drive the manager
// against a real upstream WebSocket (mockSubscriptionServer) and verify the full lifecycle.
//
// Acceptance criteria covered:
//  1. Subscribe response id echoes the caller's string verbatim (no `id:1` substitution).
//  2. Unsubscribe of the just-issued subscription succeeds (no SubscriptionNotFoundError).
//  3. Notifications arrive while the subscription is live (no regression).
func TestEndToEnd_SubscribeUnsubscribeRoundTrip(t *testing.T) {
	mockSrv := newMockSubscriptionServer()
	mockSrv.messageInterval = 20 * time.Millisecond // get a notification quickly
	defer mockSrv.Close()

	nodeUrl := &common.NodeUrl{Url: mockSrv.URL()}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{nodeUrl},
		nil, nil, nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Subscribe with a STRING id like the bug report's "client-uuid-1".
	subscribeBody, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_subscribe",
		"params":  []interface{}{"newHeads"},
		"id":      "client-uuid-1",
	})
	require.NoError(t, err)

	subscribeMsg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{
			method: "eth_subscribe",
			params: []interface{}{"newHeads"},
		},
		rawData: subscribeBody,
	}

	const dappID, ip, wsUID = "dapp-e2e", "192.168.99.99", "ws-e2e"
	reply, repliesChan, err := manager.StartSubscription(ctx, subscribeMsg, dappID, ip, wsUID, nil)
	require.NoError(t, err, "subscribe must succeed")
	require.NotNil(t, reply, "subscribe must return a reply")
	require.NotNil(t, repliesChan, "subscribe must return a notifications channel")

	// Verify id is echoed verbatim — this is Bug #1's regression in the e2e shape.
	var subResp map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(reply.Data, &subResp))
	assert.JSONEq(t, `"client-uuid-1"`, string(subResp["id"]),
		"subscribe response id must be the caller's string, not the upstream's counter")

	// Capture the subscription id the router exposed to the client (router id, NOT upstream).
	var routerSubID string
	require.NoError(t, json.Unmarshal(subResp["result"], &routerSubID))
	assert.NotEmpty(t, routerSubID)
	assert.True(t, strings.HasPrefix(routerSubID, "rs_"),
		"router id should be rs_-prefixed (got %q)", routerSubID)

	// 2. At least one notification must arrive on the live subscription.
	select {
	case notif := <-repliesChan:
		require.NotNil(t, notif, "first notification")
		assert.Contains(t, string(notif.Data), `"eth_subscription"`,
			"notification should be an eth_subscription event")
		assert.Contains(t, string(notif.Data), routerSubID,
			"notification subscription field should be the router id (rewritten from upstream)")
	case <-time.After(2 * time.Second):
		t.Fatal("expected at least one notification within 2s")
	}

	// 3. Unsubscribe using the just-issued router id — the bug's primary symptom is that
	// this returns SubscriptionNotFoundError. After the fix it must succeed and the node's
	// response (with the caller's id and result:true) must be returned.
	unsubBody, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_unsubscribe",
		"params":  []interface{}{routerSubID},
		"id":      "client-uuid-2",
	})
	require.NoError(t, err)

	unsubMsg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{
			method: "eth_unsubscribe",
			params: []interface{}{routerSubID},
		},
		rawData: unsubBody,
	}

	nodeResp, err := manager.Unsubscribe(ctx, unsubMsg, dappID, ip, wsUID, nil)
	require.NoError(t, err, "unsubscribe of just-issued subscription id must NOT return subscription-not-found")
	require.NotNil(t, nodeResp, "expected node response bytes from upstream")

	// The mock upstream returns {"jsonrpc":"2.0","id":<caller's id>,"result":true}.
	// Per the ticket's acceptance criteria, the caller's id must round-trip end-to-end.
	var unsubResp map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(nodeResp, &unsubResp))
	assert.JSONEq(t, `"client-uuid-2"`, string(unsubResp["id"]),
		"unsubscribe response id must echo caller's string id")
	assert.JSONEq(t, `true`, string(unsubResp["result"]),
		`unsubscribe response result must be "true"`)

	// Belt-and-suspenders: confirm the router translated router-id → upstream-id when
	// it called the upstream. If the router had forwarded `rs_xxx_NNNNN` to the upstream,
	// the mock's closeSubscription would not have matched any entry and the map would
	// still hold the upstream id. This guards against a regression where the router
	// leaks router ids to upstream nodes.
	mockSrv.lock.RLock()
	leftoverSubs := len(mockSrv.subscriptions)
	mockSrv.lock.RUnlock()
	assert.Equal(t, 0, leftoverSubs,
		"mock upstream should see its own subscription id on eth_unsubscribe, not the router id")
}

// TestListenForUpstreamMessages_ReconnectSkipsCleanup is the regression for the cleanup
// race in listenForUpstreamMessages. With the bug, when upstreamSub.Err() fires and we
// hand off to handleUpstreamDisconnect, the deferred dwsm.cleanupSubscription(hashedParams)
// in the returning goroutine deletes the activeSubscription that the reconnect path is
// (or will be) restoring. After the fix, the deferred cleanup is skipped when
// reconnectInFlight is set.
//
// We drive the listener directly with a synthetic *ClientSubscription whose Err() is a
// pre-loaded channel, so we don't need a real WS server. The assertion is on the post-
// condition: activeSubscriptions[hp] survives the listener's return, AND the client is
// still present in connectedClients (i.e. cleanupSubscription did NOT close their channel).
func TestListenForUpstreamMessages_ReconnectSkipsCleanup(t *testing.T) {
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{{Url: "wss://test.example.com"}},
		nil, nil, nil,
	)

	clientKey := manager.CreateWebSocketConnectionUniqueKey("d", "1.1.1.1", "ws")
	routerID := manager.idMapper.GenerateRouterID(clientKey)
	upstreamID := "0xreconnect-survives"
	manager.idMapper.RegisterMapping(routerID, upstreamID)

	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const hp = "hp-reconnect"
	activeSub := &directActiveSubscription{
		upstreamID:   upstreamID,
		hashedParams: hp,
		connectedClients: map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{
			clientKey: common.NewSafeChannelSender(subCtx, make(chan *pairingtypes.RelayReply, 1)),
		},
		clientRouterIDs: map[string]string{clientKey: routerID},
		ctx:             subCtx,
		cancel:          cancel,
		closeSubChan:    make(chan struct{}),
		messagesChan:    make(chan *rpcclient.JsonrpcMessage, 1),
	}
	// Pre-set restoring=true so the spawned handleUpstreamDisconnect goroutine no-ops
	// instead of dereferencing the nil upstreamPool. The thing under test is the OLD
	// listener's deferred cleanup branch, not the reconnect logic itself.
	activeSub.restoring.Store(true)

	manager.lock.Lock()
	manager.activeSubscriptions[hp] = activeSub
	manager.lock.Unlock()

	// Drive the listener via a local fake satisfying the upstreamErrSource interface.
	// A pre-loaded, closed channel triggers the Err() branch immediately so the listener
	// returns deterministically — no need for a real rpcclient.ClientSubscription.
	errCh := make(chan error, 1)
	errCh <- fmt.Errorf("simulated upstream error")
	close(errCh)
	upstreamSub := fakeUpstreamErrSource{ch: errCh}

	// Run the listener synchronously to observe its return; the listener returns as soon
	// as it drains the Err() channel and kicks off handleUpstreamDisconnect.
	done := make(chan struct{})
	go func() {
		manager.listenForUpstreamMessages(subCtx, hp, activeSub, upstreamSub)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("listenForUpstreamMessages did not return within 2s")
	}

	// The crucial invariant: the deferred cleanup in the returning listener must NOT have
	// fired (reconnect handoff suppressed it). Without the fix, this assertion fails:
	// cleanupSubscription would have deleted activeSubscriptions[hp] and closed every
	// SafeChannelSender. We rely on the entry being present right after return; the
	// background handleUpstreamDisconnect goroutine may eventually call cleanup itself,
	// but only after async reconnect attempts complete.
	manager.lock.RLock()
	_, stillPresent := manager.activeSubscriptions[hp]
	manager.lock.RUnlock()
	assert.True(t, stillPresent,
		"deferred cleanup must be skipped when reconnect is in flight (race regression)")
}

// fakeUpstreamErrSource satisfies the upstreamErrSource interface that
// listenForUpstreamMessages takes. Defined in the test file so we don't have to expose any
// test-only constructor from the rpcclient package.
type fakeUpstreamErrSource struct{ ch chan error }

func (f fakeUpstreamErrSource) Err() <-chan error { return f.ch }

// TestListenForUpstreamMessages_NilUpstreamSubDoesNotPanic is the regression for MAG-2685.
//
// rpcclient.Client.Subscribe returns (nil subscription, response, NIL error) when the upstream
// answers a subscribe with a JSON-RPC error object — e.g. author_submitAndWatchExtrinsic with an
// invalid extrinsic. createUpstreamSubscription only checked the error, so the nil subscription
// reached listenForUpstreamMessages, which parks it in an upstreamErrSource interface. That is a
// NON-nil interface wrapping a nil pointer, so no `== nil` check catches it, and the select's
// upstreamSub.Err() dereferenced the nil receiver: the router panicked and the container exited
// with code 2. Remote-triggerable by any client sending a subscribe the upstream rejects.
//
// createUpstreamSubscription now rejects a nil subscription, and Err() tolerates a nil receiver.
// This test drives the second layer directly: the listener must survive a typed-nil subscription
// and exit cleanly through its context rather than panicking.
func TestListenForUpstreamMessages_NilUpstreamSubDoesNotPanic(t *testing.T) {
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{{Url: "wss://test.example.com"}},
		nil, nil, nil,
	)

	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const hp = "hp-nil-upstream-sub"
	activeSub := &directActiveSubscription{
		upstreamID:       "0xnil-sub",
		hashedParams:     hp,
		connectedClients: map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{},
		clientRouterIDs:  map[string]string{},
		ctx:              subCtx,
		cancel:           cancel,
		closeSubChan:     make(chan struct{}),
		messagesChan:     make(chan *rpcclient.JsonrpcMessage, 1),
	}

	manager.lock.Lock()
	manager.activeSubscriptions[hp] = activeSub
	manager.lock.Unlock()

	// A typed nil, exactly as createUpstreamSubscription used to hand back.
	var nilSub *rpcclient.ClientSubscription //nolint:staticcheck // SA4023: the typed nil is the subject of the test
	var upstreamSub upstreamErrSource = nilSub
	// Plain `== nil` is the check a caller would naturally reach for, and it does NOT catch
	// this — the interface carries a type, so it compares non-nil even though the pointer
	// inside is nil. (testify's NotNil disagrees: it unwraps via reflection. The Go-level
	// comparison below is the semantics that actually let the nil through to Err().)
	require.False(t, upstreamSub == nil, //nolint:staticcheck // SA4023: the never-true comparison IS the assertion
		"a typed nil yields an interface that `== nil` reports as non-nil — precisely why a nil guard cannot catch this")

	// A panic here crashes the test binary, which is the assertion. Before the fix this
	// panicked immediately on upstreamSub.Err().
	done := make(chan struct{})
	go func() {
		defer close(done)
		manager.listenForUpstreamMessages(subCtx, hp, activeSub, upstreamSub)
	}()

	// Err() on a nil receiver yields a nil channel, so that select case never fires and the
	// listener parks on its other cases. Cancelling the context must still unwind it.
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("listenForUpstreamMessages did not return within 2s after context cancel")
	}
}

// TestUnsubscribe_StillRejectsForeignSubscription confirms the upstream-id fallback does NOT
// open a hole that lets one client unsubscribe another client's shared subscription.
func TestUnsubscribe_StillRejectsForeignSubscription(t *testing.T) {
	config := &WebsocketConfig{
		MaxSubscriptionsPerClient:       25,
		PerClientLimitEnforcement:       "warn",
		MaxTotalSubscriptions:           5000,
		TotalLimitEnforcement:           "warn",
		SubscriptionSharingEnabled:      true,
		SubscriptionsPerMinutePerClient: 60,
		UnsubscribesPerMinutePerClient:  60,
		MaxMessageSize:                  1048576,
		CleanupInterval:                 time.Minute,
	}

	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{{Url: "wss://test.example.com"}},
		nil, nil, config,
	)

	owner := manager.CreateWebSocketConnectionUniqueKey("dapp1", "192.168.1.1", "ws-owner")
	ownerRouterID := manager.idMapper.GenerateRouterID(owner)
	upstreamID := "0xshared-sub"
	manager.idMapper.RegisterMapping(ownerRouterID, upstreamID)

	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	activeSub := &directActiveSubscription{
		upstreamID:       upstreamID,
		hashedParams:     "hp",
		connectedClients: map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{owner: common.NewSafeChannelSender(subCtx, make(chan *pairingtypes.RelayReply, 1))},
		clientRouterIDs:  map[string]string{owner: ownerRouterID},
		ctx:              subCtx,
		cancel:           cancel,
	}
	manager.lock.Lock()
	manager.activeSubscriptions["hp"] = activeSub
	manager.lock.Unlock()

	// A second client (different dappID/ip/wsUID) tries to unsubscribe using the upstream id.
	// The fallback must still require clientRouterIDs[attacker] to exist — which it does not.
	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "eth_unsubscribe", "params": []interface{}{upstreamID}, "id": 5,
	})
	pm := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_unsubscribe", params: []interface{}{upstreamID}},
		rawData:               body,
	}
	_, err := manager.Unsubscribe(context.Background(), pm, "evil", "10.0.0.1", "ws-evil", nil)
	assert.Equal(t, common.SubscriptionNotFoundError, err,
		"unrelated client must not be able to unsubscribe via upstream-id fallback")
}

// substrateUnsubscribePairs is the full SUBSCRIBE/UNSUBSCRIBE set declared by the
// ASTAR spec's base collection, transcribed from lava-specs `astar.json` (already
// merged on that repo's main); Acala (`aca.json`) and Shiden (`sdn.json`) carry
// the same shape. The specs live in lava-specs, not under `specs/` here, so this
// table cannot be validated in-repo — hence naming the file it came from.
//
// It is documentation of WHY no string rule could have worked, not coverage
// breadth: every row drives the same one-line pass-through. The three rows that
// carry the argument are chainHead_v1_follow, author_submitAndWatchExtrinsic and
// transactionWatch_v1_submitAndWatch, which do not contain "subscribe" at all.
var substrateUnsubscribePairs = []struct {
	subscribe   string
	unsubscribe string
}{
	{"author_submitAndWatchExtrinsic", "author_unwatchExtrinsic"},
	{"chainHead_v1_follow", "chainHead_v1_unfollow"},
	{"chain_subscribeAllHeads", "chain_unsubscribeAllHeads"},
	{"chain_subscribeFinalisedHeads", "chain_unsubscribeFinalisedHeads"},
	{"chain_subscribeFinalizedHeads", "chain_unsubscribeFinalizedHeads"},
	{"chain_subscribeNewHead", "chain_unsubscribeNewHead"},
	{"chain_subscribeNewHeads", "chain_unsubscribeNewHeads"},
	{"chain_subscribeRuntimeVersion", "chain_unsubscribeRuntimeVersion"},
	{"state_subscribeRuntimeVersion", "state_unsubscribeRuntimeVersion"},
	{"state_subscribeStorage", "state_unsubscribeStorage"},
	{"subscribe_newHead", "unsubscribe_newHead"},
	{"transactionWatch_v1_submitAndWatch", "transactionWatch_v1_unwatch"},
}

// TestResolveUnsubscribeMethod_UsesClientMethodName asserts the method the router
// sends UPSTREAM, which is the thing MAG-3297 got wrong. Asserting only that an
// unsubscribe does not error would pass against a node that happens to accept the
// generic "unsubscribe".
func TestResolveUnsubscribeMethod_UsesClientMethodName(t *testing.T) {
	for _, pair := range substrateUnsubscribePairs {
		t.Run(pair.unsubscribe, func(t *testing.T) {
			// The client calls the spec's unsubscribe method; the api resolved from
			// the spec by name is what lands on the protocol message.
			pm := &mockWSProtocolMessage{method: pair.unsubscribe}

			got := resolveUnsubscribeMethod(pm, pair.subscribe)
			assert.Equal(t, pair.unsubscribe, got,
				"router must call upstream with the method the client invoked")

			// MAG-3297 regression guard: deriving from the subscribe method cannot
			// produce this name. If someone reinstates derivation here, the assertion
			// above stops holding and this one says why.
			assert.NotEqual(t, pair.unsubscribe, getUnsubscribeMethod(pair.subscribe),
				"derivation is expected to be wrong for Substrate — it is why this fix exists")
		})
	}
}

// TestResolveUnsubscribeMethod_EthAndTendermintUnchanged pins the two shapes that
// already worked, so the fix is not a behaviour change for them. eth_unsubscribe
// working while every Substrate pair failed is what localised this bug.
func TestResolveUnsubscribeMethod_EthAndTendermintUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name        string
		subscribe   string
		unsubscribe string
	}{
		{"evm", "eth_subscribe", "eth_unsubscribe"},
		{"tendermint", "subscribe", "unsubscribe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pm := &mockWSProtocolMessage{method: tc.unsubscribe}
			assert.Equal(t, tc.unsubscribe, resolveUnsubscribeMethod(pm, tc.subscribe))
			// These are the cases the old derivation hardcoded, so it agrees.
			assert.Equal(t, tc.unsubscribe, getUnsubscribeMethod(tc.subscribe))
		})
	}
}

// mockWSProtocolMessageNoApi is a message that resolved to no spec api — the only
// case the derivation fallback is still reachable from.
type mockWSProtocolMessageNoApi struct {
	mockWSProtocolMessage
}

func (m *mockWSProtocolMessageNoApi) GetApi() *spectypes.Api { return nil }

func TestResolveUnsubscribeMethod_FallsBackWithoutApi(t *testing.T) {
	t.Run("nil api", func(t *testing.T) {
		pm := &mockWSProtocolMessageNoApi{mockWSProtocolMessage{method: "chain_unsubscribeNewHeads"}}
		assert.Equal(t, "eth_unsubscribe", resolveUnsubscribeMethod(pm, "eth_subscribe"),
			"with no api to read, fall back to derivation rather than sending an empty method")
	})

	t.Run("empty api name", func(t *testing.T) {
		pm := &mockWSProtocolMessage{method: ""}
		assert.Equal(t, "eth_unsubscribe", resolveUnsubscribeMethod(pm, "eth_subscribe"))
	})

	t.Run("nil message", func(t *testing.T) {
		assert.Equal(t, "eth_unsubscribe", resolveUnsubscribeMethod(nil, "eth_subscribe"))
	})

	// The one row here whose input the parser can actually hand you. A method the
	// spec does not declare still gets an api, synthesised as "Default-<method>"
	// (base_chain_parser.go defaultApiContainer). Sending that upstream would be a
	// method no node implements, so it must not be preferred over the fallback.
	//
	// Unreachable from today's only caller — a "Default-" name matches no
	// directive's ApiName, so the message is never classified UNSUBSCRIBE — but
	// the guard belongs to this function, not to its caller's invariant.
	t.Run("synthesised default api name", func(t *testing.T) {
		pm := &mockWSProtocolMessage{method: chainlib.DefaultApiName + "chain_unsubscribeNewHeads"}
		assert.Equal(t, "eth_unsubscribe", resolveUnsubscribeMethod(pm, "eth_subscribe"),
			"a Default-prefixed name is not a wire method")
	})
}

// TestEndToEnd_UnsubscribeSendsTheSpecMethodUpstream is the MAG-3297 regression
// test at the level the bug lived: what the ROUTER SENDS UPSTREAM.
//
// The unit tests above pin resolveUnsubscribeMethod's return value, which does
// not exercise the CallContext call site — reverting that one line back to
// getUnsubscribeMethod would leave them all green. This one drives a real
// WebSocket round trip and asserts the method the upstream was actually asked
// for, which is the only place that regression is visible.
//
// It uses a Substrate pair on purpose. Before the fix the router derived the
// teardown method from the subscribe method by string surgery, and
// chain_subscribeNewHeads matches none of its rules, so it sent the literal
// "unsubscribe" — which is what this asserts is absent.
func TestEndToEnd_UnsubscribeSendsTheSpecMethodUpstream(t *testing.T) {
	mockSrv := newMockSubscriptionServer()
	mockSrv.messageInterval = 20 * time.Millisecond
	defer mockSrv.Close()

	nodeUrl := &common.NodeUrl{Url: mockSrv.URL()}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc", "SDN", "jsonrpc",
		[]*common.NodeUrl{nodeUrl},
		nil, nil, nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const dappID, ip, wsUID = "dapp-sdn", "192.168.99.98", "ws-sdn"
	subscribeBody, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "chain_subscribeNewHeads", "params": []interface{}{}, "id": 1,
	})
	require.NoError(t, err)
	subscribeMsg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "chain_subscribeNewHeads", params: []interface{}{}},
		rawData:               subscribeBody,
	}

	reply, _, err := manager.StartSubscription(ctx, subscribeMsg, dappID, ip, wsUID, nil)
	require.NoError(t, err, "the subscribe half always worked, including before the fix")
	require.NotNil(t, reply)

	var subResp map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(reply.Data, &subResp))
	var routerSubID string
	require.NoError(t, json.Unmarshal(subResp["result"], &routerSubID))
	require.NotEmpty(t, routerSubID)

	unsubscribeBody, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "chain_unsubscribeNewHeads", "params": []interface{}{routerSubID}, "id": 2,
	})
	require.NoError(t, err)
	unsubscribeMsg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{
			method: "chain_unsubscribeNewHeads",
			params: []interface{}{routerSubID},
		},
		rawData: unsubscribeBody,
	}

	_, err = manager.Unsubscribe(ctx, unsubscribeMsg, dappID, ip, wsUID, nil)
	require.NoError(t, err)

	observed := mockSrv.ObservedMethods()
	require.Contains(t, observed, "chain_subscribeNewHeads")
	require.Contains(t, observed, "chain_unsubscribeNewHeads",
		"the router must tear down with the spec's own unsubscribe method; observed=%v", observed)
	require.NotContains(t, observed, "unsubscribe",
		"the derivation fallback sent this literal method, which no Substrate node implements; observed=%v", observed)
}

// TestEndToEnd_SubstratePushFrameReachesClient is the regression for MAG-3345: no Substrate
// subscription had ever relayed a push frame end-to-end. The subscribe call itself always
// succeeded, which is what made the gap silent — nothing errored, frames simply never came.
//
// It runs against a real WebSocket, so the whole delivery path is live: the rpcclient
// dispatcher that decides whether a frame is a subscription push at all, and the manager's
// per-client id rewrite. Two independent defects sat on that path, and the assertions below
// separate them:
//
//   - rpcclient's handleImmediate only recognised pushes whose method ended in
//     "_subscription"/"Notification" (or carried a Tendermint query). chain_newHead matches
//     none of those, so the frame was never delivered to the subscription — worse, it fell
//     through to the call path and the router answered the node with "invalid request".
//     Nothing arrives on repliesChan at all.
//   - rewriteSubscriptionID keyed on method == "eth_subscription", so even once delivered,
//     a Substrate frame reached the client carrying the UPSTREAM id rather than the router
//     id the client was handed at subscribe time.
//
// Either bug alone leaves the subscription unusable, so both are asserted.
func TestEndToEnd_SubstratePushFrameReachesClient(t *testing.T) {
	mockSrv := newMockSubscriptionServer()
	mockSrv.messageInterval = 20 * time.Millisecond
	// A Substrate node names the frame after the payload, and its ids are opaque
	// base58-ish strings — not hex, so nothing here can pass by matching an "0x" prefix.
	mockSrv.notificationMethod = "chain_newHead"
	mockSrv.subscriptionID = "Ck1rTHhOa1hxTGV3"
	defer mockSrv.Close()

	nodeUrl := &common.NodeUrl{Url: mockSrv.URL()}
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc", "AVN", "jsonrpc",
		[]*common.NodeUrl{nodeUrl},
		nil, nil, nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const dappID, ip, wsUID = "dapp-avn", "192.168.99.97", "ws-avn"
	subscribeBody, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "chain_subscribeNewHeads", "params": []interface{}{}, "id": 1,
	})
	require.NoError(t, err)
	subscribeMsg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "chain_subscribeNewHeads", params: []interface{}{}},
		rawData:               subscribeBody,
	}

	reply, repliesChan, err := manager.StartSubscription(ctx, subscribeMsg, dappID, ip, wsUID, nil)
	require.NoError(t, err, "the subscribe half always worked, including before the fix")
	require.NotNil(t, reply)
	require.NotNil(t, repliesChan)

	var subResp map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(reply.Data, &subResp))
	var routerSubID string
	require.NoError(t, json.Unmarshal(subResp["result"], &routerSubID))
	require.NotEmpty(t, routerSubID)
	require.NotEqual(t, mockSrv.subscriptionID, routerSubID,
		"the router must issue its own id, otherwise the rewrite assertion below proves nothing")

	select {
	case notif := <-repliesChan:
		require.NotNil(t, notif, "a Substrate subscription must deliver push frames")

		var frame struct {
			Method string `json:"method"`
			Params struct {
				Subscription string          `json:"subscription"`
				Result       json.RawMessage `json:"result"`
			} `json:"params"`
		}
		require.NoError(t, json.Unmarshal(notif.Data, &frame))

		assert.Equal(t, "chain_newHead", frame.Method,
			"the frame must keep the method the node sent — a Substrate client dispatches on it")
		assert.Equal(t, routerSubID, frame.Params.Subscription,
			"the push must carry the router id issued at subscribe time, not the upstream id %q",
			mockSrv.subscriptionID)
		assert.NotEmpty(t, frame.Params.Result, "the payload must be relayed")
	case <-time.After(5 * time.Second):
		t.Fatal("no push frame reached the client within 5s — the subscription is dead end-to-end")
	}
}

// --- MAG-3359: chains that number their subscriptions -----------------------------------

// TestUnsubscribeParamsExtraction_SolanaNumeric covers the client sending its id back as a
// JSON number. A string-only type assertion rejected it outright, so the unsubscribe failed
// before it ever reached the ownership check.
func TestUnsubscribeParamsExtraction_SolanaNumeric(t *testing.T) {
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc", "SOLANA", "jsonrpc",
		[]*common.NodeUrl{{Url: "ws://localhost:8900"}},
		nil, nil, nil,
	)

	numeric := &mockWSProtocolMessage{
		method: "accountUnsubscribe",
		params: []interface{}{int64(1000001)},
	}
	id, err := manager.extractSubscriptionIDFromUnsubscribe(numeric)
	require.NoError(t, err)
	assert.Equal(t, "1000001", id, "a numbered id must canonicalise to its decimal string")

	named := &mockWSProtocolMessage{
		method: "eth_unsubscribe",
		params: []interface{}{"0x1a2b3c4d"},
	}
	id, err = manager.extractSubscriptionIDFromUnsubscribe(named)
	require.NoError(t, err)
	assert.Equal(t, "0x1a2b3c4d", id, "a named id is unchanged")
}

// TestCreateSubscriptionReply_SolanaNumeric pins the reply shape. Solana's collection is
// jsonrpc, so it lands in the same branch as EVM — the number/string decision has to come
// from the node's own response, not from the api interface.
func TestCreateSubscriptionReply_SolanaNumeric(t *testing.T) {
	upstream := &rpcclient.JsonrpcMessage{
		ID:     json.RawMessage(`7`),
		Result: json.RawMessage(`23784`),
	}

	reply, err := createSubscriptionReply("1000001", json.RawMessage(`7`), upstream, "jsonrpc", true)
	require.NoError(t, err)

	var parsed struct {
		ID     json.RawMessage `json:"id"`
		Result json.RawMessage `json:"result"`
	}
	require.NoError(t, json.Unmarshal(reply, &parsed))
	assert.JSONEq(t, `7`, string(parsed.ID))
	assert.Equal(t, "1000001", string(parsed.Result),
		"result must be the router id as a JSON number, not the upstream id and not a string")
}

// solanaSubscribeMessage builds the accountSubscribe the tests below drive the manager with.
func solanaSubscribeMessage(t *testing.T, requestID int) *mockWSProtocolMessageWithRawData {
	t.Helper()
	const account = "Vote111111111111111111111111111111111111111"
	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "accountSubscribe",
		"params": []interface{}{account}, "id": requestID,
	})
	require.NoError(t, err)
	return &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{
			method: "accountSubscribe",
			params: []interface{}{account},
		},
		rawData: body,
	}
}

// newSolanaMockServer returns a mock upstream that behaves like a Solana node: it numbers
// its subscriptions and names its push frames after the payload.
func newSolanaMockServer() *mockSubscriptionServer {
	ms := newMockSubscriptionServer()
	ms.messageInterval = 20 * time.Millisecond
	ms.notificationMethod = "accountNotification"
	ms.numericIDs = true
	return ms
}

func newSolanaManager(nodeUrl *common.NodeUrl) *DirectWSSubscriptionManager {
	return NewDirectWSSubscriptionManager(
		getTestMetricsManager(),
		"jsonrpc", "SOLANA", "jsonrpc",
		[]*common.NodeUrl{nodeUrl},
		nil, nil, nil,
	)
}

// numericSubscriptionID reads the id out of a subscribe reply, failing the test unless it is
// an unquoted JSON number. The quoting is the whole point: a Solana client stores whatever
// `result` gave it and hands that straight back to accountUnsubscribe.
func numericSubscriptionID(t *testing.T, replyData []byte) int64 {
	t.Helper()
	var resp struct {
		Result json.RawMessage `json:"result"`
	}
	require.NoError(t, json.Unmarshal(replyData, &resp))
	var id int64
	require.NoError(t, json.Unmarshal(resp.Result, &id),
		"result must be a JSON number, got %s", resp.Result)
	return id
}

// TestEndToEnd_SolanaNumericSubscriptionRoundTrip is the regression for MAG-3359. It runs
// over a real WebSocket against a node that numbers its subscriptions, so every site the
// ticket enumerated is live at once: the dispatcher that decides a frame is a push at all,
// the upstream-id extraction, the reply shape, the per-frame rewrite, and both ends of
// unsubscribe.
//
// Before the fix nothing arrived on repliesChan at all, exactly as for Substrate — the
// dispatcher never recognised accountNotification, so the router answered the node with an
// "invalid request" for every push instead of delivering it.
func TestEndToEnd_SolanaNumericSubscriptionRoundTrip(t *testing.T) {
	mockSrv := newSolanaMockServer()
	defer mockSrv.Close()

	manager := newSolanaManager(&common.NodeUrl{Url: mockSrv.URL()})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const dappID, ip, wsUID = "dapp-sol", "192.168.99.96", "ws-sol"
	reply, repliesChan, err := manager.StartSubscription(ctx, solanaSubscribeMessage(t, 1), dappID, ip, wsUID, nil)
	require.NoError(t, err, "the subscribe half always worked")
	require.NotNil(t, reply)
	require.NotNil(t, repliesChan)

	routerSubID := numericSubscriptionID(t, reply.Data)
	require.NotEqual(t, int64(23784), routerSubID,
		"the router must issue its own id; passing the upstream one through is what breaks on reconnect")

	select {
	case notif := <-repliesChan:
		require.NotNil(t, notif, "a Solana subscription must deliver push frames")
		var frame struct {
			Method string `json:"method"`
			Params struct {
				Subscription json.RawMessage `json:"subscription"`
				Result       json.RawMessage `json:"result"`
			} `json:"params"`
		}
		require.NoError(t, json.Unmarshal(notif.Data, &frame))
		assert.Equal(t, "accountNotification", frame.Method)
		assert.Equal(t, strconv.FormatInt(routerSubID, 10), string(frame.Params.Subscription),
			"the push must carry the router id, still unquoted")
		assert.NotEmpty(t, frame.Params.Result, "the payload must be relayed")
	case <-time.After(5 * time.Second):
		t.Fatal("no push frame reached the client within 5s — the subscription is dead end-to-end")
	}

	unsubBody, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": "accountUnsubscribe",
		"params": []interface{}{routerSubID}, "id": 2,
	})
	require.NoError(t, err)
	unsubMsg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{
			method: "accountUnsubscribe",
			params: []interface{}{routerSubID},
		},
		rawData: unsubBody,
	}

	nodeResp, err := manager.Unsubscribe(ctx, unsubMsg, dappID, ip, wsUID, nil)
	require.NoError(t, err, "unsubscribe with the issued numeric id must not be rejected")
	require.NotNil(t, nodeResp)

	observed := mockSrv.ObservedMethods()
	require.Contains(t, observed, "accountSubscribe")
	require.Contains(t, observed, "accountUnsubscribe")

	mockSrv.lock.RLock()
	leftover := len(mockSrv.subscriptions)
	mockSrv.lock.RUnlock()
	assert.Equal(t, 0, leftover,
		"the node must have been sent its own integer id; the router id would have matched nothing there")
}

// TestEndToEnd_SolanaJoiningClientGetsItsOwnNumericID covers the dedup path. A second client
// on the same params joins the one upstream subscription and is issued its own id — which
// has to be a number too, since the id-shape decision lives on the subscription rather than
// being re-derived from a node response the joining client never sees.
func TestEndToEnd_SolanaJoiningClientGetsItsOwnNumericID(t *testing.T) {
	mockSrv := newSolanaMockServer()
	defer mockSrv.Close()

	manager := newSolanaManager(&common.NodeUrl{Url: mockSrv.URL()})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	firstReply, firstChan, err := manager.StartSubscription(ctx, solanaSubscribeMessage(t, 1), "dapp-a", "10.0.0.1", "ws-a", nil)
	require.NoError(t, err)
	require.NotNil(t, firstChan)
	firstID := numericSubscriptionID(t, firstReply.Data)

	secondReply, secondChan, err := manager.StartSubscription(ctx, solanaSubscribeMessage(t, 2), "dapp-b", "10.0.0.2", "ws-b", nil)
	require.NoError(t, err)
	require.NotNil(t, secondChan, "the second client must join, not be turned away")
	secondID := numericSubscriptionID(t, secondReply.Data)

	assert.NotEqual(t, firstID, secondID, "each client gets its own id")

	manager.lock.RLock()
	activeCount := len(manager.activeSubscriptions)
	manager.lock.RUnlock()
	require.Equal(t, 1, activeCount,
		"identical params must share one upstream subscription")

	for name, ch := range map[string]<-chan *pairingtypes.RelayReply{"first": firstChan, "second": secondChan} {
		select {
		case notif := <-ch:
			require.NotNil(t, notif)
			assert.Contains(t, string(notif.Data), `"accountNotification"`,
				"%s client should receive the push", name)
		case <-time.After(5 * time.Second):
			t.Fatalf("%s client received no push frame within 5s", name)
		}
	}
}

// TestSolanaRouterIDSurvivesUpstreamReconnect is the assertion that encodes why the router
// issues its own numeric id rather than relaying the node's.
//
// Passing the upstream id through would have been a far smaller change, and it is what the
// ticket floated as the cheap option. This test is why it was rejected:
// handleUpstreamDisconnect re-subscribes and the node answers with a DIFFERENT id, so a
// client holding the upstream one would silently be left with an id that names nothing —
// pushes tagged with an id it never saw, and an unsubscribe that cannot be matched.
//
// Scope: this calls handleUpstreamDisconnect directly rather than provoking a real upstream
// error, so it exercises the restore path and NOT listenForUpstreamMessages' cleanup-ownership
// handoff (which is covered by TestListenForUpstreamMessages_ReconnectSkipsCleanup). One
// consequence is that the original listener goroutine is still selecting when the restored one
// starts; both fan out to the same clients, so the assertions below are unaffected.
func TestSolanaRouterIDSurvivesUpstreamReconnect(t *testing.T) {
	mockSrv := newSolanaMockServer()
	defer mockSrv.Close()

	manager := newSolanaManager(&common.NodeUrl{Url: mockSrv.URL()})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const dappID, ip, wsUID = "dapp-sol", "192.168.99.95", "ws-sol"
	reply, repliesChan, err := manager.StartSubscription(ctx, solanaSubscribeMessage(t, 1), dappID, ip, wsUID, nil)
	require.NoError(t, err)
	routerSubID := numericSubscriptionID(t, reply.Data)

	clientKey := manager.CreateWebSocketConnectionUniqueKey(dappID, ip, wsUID)

	manager.lock.RLock()
	require.Len(t, manager.activeSubscriptions, 1)
	var hashedParams string
	var activeSub *directActiveSubscription
	for hp, sub := range manager.activeSubscriptions {
		hashedParams, activeSub = hp, sub
	}
	upstreamBefore := activeSub.upstreamID
	manager.lock.RUnlock()

	require.Equal(t, "23784", upstreamBefore, "the first upstream subscription")

	// Drain whatever the original subscription already delivered, so the frame asserted on
	// below is unambiguously one the restored subscription produced.
	drain := time.After(200 * time.Millisecond)
drainLoop:
	for {
		select {
		case <-repliesChan:
		case <-drain:
			break drainLoop
		}
	}

	manager.handleUpstreamDisconnect(ctx, hashedParams, activeSub)

	manager.lock.RLock()
	upstreamAfter := activeSub.upstreamID
	routerIDAfter := activeSub.clientRouterIDs[clientKey]
	manager.lock.RUnlock()

	require.NotEqual(t, upstreamBefore, upstreamAfter,
		"the node must have issued a new id, or this test proves nothing")
	assert.Equal(t, strconv.FormatInt(routerSubID, 10), routerIDAfter,
		"the client's id must be unchanged across the reconnect — that is what the indirection buys")

	select {
	case notif := <-repliesChan:
		require.NotNil(t, notif)
		var frame struct {
			Params struct {
				Subscription json.RawMessage `json:"subscription"`
			} `json:"params"`
		}
		require.NoError(t, json.Unmarshal(notif.Data, &frame))
		assert.Equal(t, strconv.FormatInt(routerSubID, 10), string(frame.Params.Subscription),
			"post-reconnect frames must still carry the id the client holds, not upstream %q", upstreamAfter)
	case <-time.After(5 * time.Second):
		t.Fatal("no push frame after the reconnect — the subscription was not restored")
	}
}

// TestSubscriptionIDsAreNumeric is the guard on the one decision that picks a client's id
// type. It is separated out because the shape decision and the id canonicalisation are two
// different functions, and a case covered on only one of them is how `null` slipped through:
// json.Unmarshal decodes null into an int64 with a NIL error, so a plain decode reports it as
// a number while CanonicalSubscriptionID correctly reports it as naming nothing.
func TestSubscriptionIDsAreNumeric(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want bool
	}{
		{"solana number", `23784`, true},
		{"zero", `0`, true},
		{"negative", `-5`, true},
		{"leading whitespace", "  23784", true},
		{"ethereum hex string", `"0x9ce59a13"`, false},
		{"substrate opaque string", `"Ck1rTHhOa1hxTGV3"`, false},
		{"null is not a number", `null`, false},
		{"tendermint query object", `{"query":"tm.event='NewBlock'"}`, false},
		{"array", `[1]`, false},
		{"absent", ``, false},
		{"true", `true`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, subscriptionIDsAreNumeric(json.RawMessage(tc.raw)),
				"subscriptionIDsAreNumeric(%s)", tc.raw)
		})
	}
}

// TestSubscriptionIDsAreNumeric_AgreesWithCanonicalisation pins the two halves together. A
// value this reports as numeric must be one CanonicalSubscriptionID also accepts, or the
// router would issue an id in a shape whose upstream counterpart it cannot key on.
func TestSubscriptionIDsAreNumeric_AgreesWithCanonicalisation(t *testing.T) {
	for _, raw := range []string{`23784`, `0`, `null`, `""`, `"0xabc"`, `{}`, `[1]`, `1.5`} {
		if subscriptionIDsAreNumeric(json.RawMessage(raw)) {
			_, named := rpcclient.CanonicalSubscriptionID(json.RawMessage(raw))
			assert.True(t, named,
				"%s reads as numeric but names no subscription — the two halves disagree", raw)
		}
	}
}

// TestGenerateNumericRouterID covers the generator whose output the client is handed
// verbatim: ids must be numbers, must be distinct, and must sit above the range a node
// plausibly issues so they cannot be mistaken for an upstream id in the unsubscribe lookup.
func TestGenerateNumericRouterID(t *testing.T) {
	mapper := NewSubscriptionIDMapper()

	seen := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		id := mapper.GenerateNumericRouterID()

		parsed, err := strconv.ParseInt(id, 10, 64)
		require.NoError(t, err, "a numeric router id must parse as a number, got %q", id)
		assert.GreaterOrEqual(t, parsed, int64(numericRouterIDBase),
			"ids must sit above the base, or they can collide with a node's own ids")
		assert.Less(t, parsed, int64(1)<<53,
			"ids must stay below 2^53 or a JavaScript client cannot round-trip them")

		_, dup := seen[id]
		require.False(t, dup, "ids must be distinct, %q repeated at i #%d", id, i)
		seen[id] = struct{}{}
	}
}
