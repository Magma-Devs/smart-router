package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirectRPCRelaySender_SendDirectRelay(t *testing.T) {
	// Create mock JSON-RPC server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request format
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		// Return mock JSON-RPC response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1234"}`))
	}))
	defer mockServer.Close()

	// Create direct RPC connection
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: mockServer.URL}

	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)
	require.NotNil(t, directConn)

	// Create DirectRPCRelaySender with endpoint name
	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "test-endpoint",
	}

	// Create mock chain message
	chainMessage := createMockChainMessage(t, `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)

	// Send relay
	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)

	// Verify results
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.NotNil(t, result.Reply)
	assert.NotNil(t, result.Reply.Data)
	assert.True(t, result.Finalized)
	assert.Equal(t, 200, result.StatusCode)
	// Provider address should be the sanitized endpoint name (not full URL with potential API keys)
	assert.Equal(t, "test-endpoint", result.ProviderInfo.ProviderAddress)

	// Verify response data
	assert.Contains(t, string(result.Reply.Data), "0x1234")
}

func TestDirectRPCRelaySender_SendDirectRelay_Timeout(t *testing.T) {
	// Create slow mock server that exceeds timeout
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second) // Sleep longer than timeout
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1234"}`))
	}))
	defer mockServer.Close()

	// Create direct RPC connection
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: mockServer.URL}

	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	// Create sender
	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "test-timeout-endpoint",
	}

	// Create mock chain message
	chainMessage := createMockChainMessage(t, `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)

	// Send relay with short timeout
	result, err := sender.SendDirectRelay(ctx, chainMessage, 100*time.Millisecond)

	// Should timeout — error preserves original message, classification in metadata
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "deadline exceeded")
}

func TestDirectRPCRelaySender_SendDirectRelay_ServerError(t *testing.T) {
	// Create mock server that returns error
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"service unavailable"}`))
	}))
	defer mockServer.Close()

	// Create direct RPC connection
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: mockServer.URL}

	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	// Create sender
	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "test-error-endpoint",
	}

	// Create mock chain message
	chainMessage := createMockChainMessage(t, `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)

	// Send relay
	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)

	// Should return error for 5xx status codes — error preserves original HTTP status
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "503")
}

// TestDirectRPCRelaySender_HTTPStatusPrefixReachesClassifier_MAG1666 is the
// call-site regression test for the MAG-1666 fix. The classifier-level test
// (TestClassifyError_GenericJsonRPCHTTPStatusMappings) only proves the matcher
// fires on an already-prefixed string; it can't catch a regression where the
// call site at direct_rpc_relay.go::sendJSONRPCRelay stops prepending
// "HTTP <status>: " to the classifier message.
//
// The discriminator: when the upstream returns HTTP 404/413 with a JSON-RPC
// body whose code is generic (-1, not in the registry), the only signal that
// can route classification to a non-retryable LavaError is the HTTP status
// digits in the message. A bare/inert message (no substring matcher hits)
// classifies to LavaErrorUnknown (retryable), so IsNonRetryable flips between
// true (prefix present) and false (prefix removed).
func TestDirectRPCRelaySender_HTTPStatusPrefixReachesClassifier_MAG1666(t *testing.T) {
	// The bare error message returned by CheckResponseError. Must be inert —
	// no substring like "endpoint not found" / "method not allowed" / "request
	// too large" or any other classifier matcher token, or the regression
	// would be masked by a different matcher firing.
	const bareErrorMessage = "upstream returned error"

	// Discriminator assertion: without the call site's prefix, this exact
	// (transport, errorCode, message) combination must classify to
	// LavaErrorUnknown. If a future matcher swallows it, the test must fail
	// loudly here rather than producing a confusing failure below.
	require.Equal(t,
		common.LavaErrorUnknown,
		common.ClassifyError(nil, common.ChainFamilyEVM, common.TransportJsonRPC, -1, bareErrorMessage),
		"bare message must classify to UNKNOWN without the HTTP <status>: prefix — "+
			"otherwise this test cannot prove the prefix is what flipped the verdict")

	tests := []struct {
		name       string
		statusCode int
	}{
		{name: "HTTP 404 → NODE_ENDPOINT_NOT_FOUND (non-retryable)", statusCode: 404},
		{name: "HTTP 413 → USER_REQUEST_TOO_LARGE (non-retryable)", statusCode: 413},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock upstream returns the chosen HTTP status with a JSON-RPC
			// body whose error.code is generic (-1) so ExtractJSONRPCErrorCode
			// resolves to -1 and the HTTP status digits become the only
			// signal the classifier has.
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.statusCode)
				w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"` + bareErrorMessage + `"}}`))
			}))
			defer mockServer.Close()

			ctx := context.Background()
			nodeUrl := common.NodeUrl{Url: mockServer.URL}

			directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
			require.NoError(t, err)

			sender := &DirectRPCRelaySender{
				directConnection: directConn,
				endpointName:     "test-http-status-prefix",
				chainFamily:      common.ChainFamilyEVM,
			}

			chainMessage := &mockChainMessage{
				requestData: []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`),
				// Drive the call site into its hasError branch with the inert
				// message; this is what the classifier sees before prefixing.
				checkResponseError: func(_ []byte, _ int) (bool, string) {
					return true, bareErrorMessage
				},
			}

			result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tt.statusCode, result.StatusCode)
			assert.True(t, result.IsNodeError, "hasError branch must have fired")
			assert.True(t, result.IsNonRetryable,
				"HTTP %d with generic body code should classify as non-retryable; "+
					"removing the HTTP <status>: prefix at direct_rpc_relay.go::sendJSONRPCRelay would regress this",
				tt.statusCode)
		})
	}
}

// TestDirectRPCRelaySender_HTTPStatusPrefixSkippedOn2xx pins the guard at
// direct_rpc_relay.go::sendJSONRPCRelay: only non-2xx responses get the
// "HTTP <status>: " prefix. An always-prefix change would silently re-route
// 2xx JSON-RPC node errors through HTTP matchers and break the registry
// contract for body-code classification.
func TestDirectRPCRelaySender_HTTPStatusPrefixSkippedOn2xx(t *testing.T) {
	const bareErrorMessage = "upstream returned error"

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"` + bareErrorMessage + `"}}`))
	}))
	defer mockServer.Close()

	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: mockServer.URL}

	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "test-http-status-prefix-2xx",
		chainFamily:      common.ChainFamilyEVM,
	}

	chainMessage := &mockChainMessage{
		requestData: []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`),
		checkResponseError: func(_ []byte, _ int) (bool, string) {
			return true, bareErrorMessage
		},
	}

	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 200, result.StatusCode)
	assert.True(t, result.IsNodeError)
	// HTTP 200 + generic body code (-1) + inert message → no matcher fires →
	// LavaErrorUnknown → IsNonRetryable=false. An "always-prefix" regression
	// would inject "HTTP 200: ..." into the classifier message, which is
	// harmless today but documents that 2xx responses must not be reclassified
	// through HTTP-status matchers.
	assert.False(t, result.IsNonRetryable,
		"2xx responses must not be classified via HTTP-status matchers")
}

func TestDirectRPCRelaySender_SendDirectRelay_BatchRequest(t *testing.T) {
	// Create mock JSON-RPC server that handles batch requests
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request format
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		// Read and verify incoming request is a valid batch (JSON array)
		body := make([]byte, r.ContentLength)
		r.Body.Read(body)

		// Verify it's a JSON array (batch request starts with '[')
		assert.Equal(t, byte('['), body[0], "Batch request should start with '['")

		// Return mock batch JSON-RPC response (array of results)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[
			{"jsonrpc":"2.0","id":1,"result":"0x12a7b5c"},
			{"jsonrpc":"2.0","id":2,"result":"0x2fe3f504c5cf346076d"}
		]`))
	}))
	defer mockServer.Close()

	// Create direct RPC connection
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: mockServer.URL}

	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)
	require.NotNil(t, directConn)

	// Batch request JSON (array of two requests)
	batchRequestJSON := []byte(`[
		{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1},
		{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","latest"],"id":2}
	]`)

	// Create DirectRPCRelaySender with original request data (critical for batch support)
	sender := &DirectRPCRelaySender{
		directConnection:    directConn,
		endpointName:        "test-batch-endpoint",
		originalRequestData: batchRequestJSON, // This preserves the batch request
	}

	// Create mock batch chain message
	chainMessage := createMockBatchChainMessage(t, batchRequestJSON)

	// Send relay
	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)

	// Verify results
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.NotNil(t, result.Reply)
	assert.NotNil(t, result.Reply.Data)
	assert.True(t, result.Finalized)
	assert.Equal(t, 200, result.StatusCode)

	// Verify response data is a batch response (contains both results)
	responseStr := string(result.Reply.Data)
	assert.Contains(t, responseStr, "0x12a7b5c", "Should contain eth_blockNumber result")
	assert.Contains(t, responseStr, "0x2fe3f504c5cf346076d", "Should contain eth_getBalance result")
	assert.Equal(t, byte('['), result.Reply.Data[0], "Batch response should start with '['")
}

func TestDirectRPCSession_IsDirectRPC(t *testing.T) {
	// This test verifies that IsDirectRPC() correctly identifies direct RPC sessions

	// Create mock JSON-RPC server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xabc"}`))
	}))
	defer mockServer.Close()

	// Create direct RPC connection
	ctx := context.Background()
	nodeUrl := common.NodeUrl{Url: mockServer.URL}

	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	// Create parent ConsumerSessionsWithProvider with endpoint
	cswp := &lavasession.ConsumerSessionsWithProvider{
		PublicLavaAddress: "test-direct-endpoint",
		PairingEpoch:      100,
		Endpoints: []*lavasession.Endpoint{
			{
				NetworkAddress:    mockServer.URL,
				Enabled:           true,
				DirectConnections: []lavasession.DirectRPCConnection{directConn},
			},
		},
	}

	// Create DirectRPCSessionConnection (smart router session)
	session := &lavasession.SingleConsumerSession{
		Parent: cswp,
		Connection: &lavasession.DirectRPCSessionConnection{
			DirectConnection: directConn,
			EndpointAddress:  mockServer.URL,
		},
	}

	// Verify session is recognized as direct RPC
	assert.True(t, session.IsDirectRPC())

	// Verify GetDirectConnection works
	conn, ok := session.GetDirectConnection()
	assert.True(t, ok)
	assert.Equal(t, directConn, conn)

	// NOTE: Full relayInnerDirect test would require:
	// - Mock RPCSmartRouterServer with chainParser, metrics, etc.
	// - This is covered by the end-to-end tests
}

// createMockChainMessage creates a mock ChainMessage for testing
// This is a simplified mock - real implementation would use chainlib.CreateChainLibMocks
func createMockChainMessage(t *testing.T, requestData string) chainlib.ChainMessage {
	t.Helper()

	// For now, return a basic mock that implements the minimal interface
	// In real integration tests, use chainlib.CreateChainLibMocks
	return &mockChainMessage{
		requestData: []byte(requestData),
	}
}

// createMockBatchChainMessage creates a mock ChainMessage for batch requests
func createMockBatchChainMessage(t *testing.T, requestData []byte) chainlib.ChainMessage {
	t.Helper()

	// Return a mock chain message with batch API name
	return &mockChainMessage{
		requestData: requestData,
		api: &spectypes.Api{
			Name: "eth_blockNumber&eth_getBalance", // Batch request combined API name
		},
	}
}

type mockChainMessage struct {
	requestData []byte
	api         *spectypes.Api
	// checkResponseError, when non-nil, overrides the default (false, "") so
	// tests can drive the call site's hasError branch with a custom message.
	checkResponseError func(data []byte, httpStatusCode int) (bool, string)
}

func (m *mockChainMessage) GetRPCMessage() rpcInterfaceMessages.GenericMessage {
	return &mockGenericMessage{data: m.requestData}
}

func (m *mockChainMessage) GetApi() *spectypes.Api {
	if m.api == nil {
		return &spectypes.Api{Name: "eth_blockNumber"}
	}
	return m.api
}

func (m *mockChainMessage) CheckResponseError(data []byte, httpStatusCode int) (bool, string) {
	if m.checkResponseError != nil {
		return m.checkResponseError(data, httpStatusCode)
	}
	return false, ""
}

func (m *mockChainMessage) GetApiCollection() *spectypes.ApiCollection {
	return &spectypes.ApiCollection{
		CollectionData: spectypes.CollectionData{
			ApiInterface: "jsonrpc",
		},
	}
}

// Implement remaining ChainMessage interface methods (stubs for testing)
func (m *mockChainMessage) SubscriptionIdExtractor(reply *rpcclient.JsonrpcMessage) string { return "" }
func (m *mockChainMessage) RequestedBlock() (latest int64, earliest int64)                 { return 0, 0 }
func (m *mockChainMessage) UpdateLatestBlockInMessage(latestBlock int64, modifyContent bool) bool {
	return false
}
func (m *mockChainMessage) AppendHeader(metadata []pairingtypes.Metadata) {}
func (m *mockChainMessage) GetExtensions() []*spectypes.Extension         { return nil }
func (m *mockChainMessage) OverrideExtensions(extensionNames []string, extensionParser *extensionslib.ExtensionParser) {
}
func (m *mockChainMessage) DisableErrorHandling()                               {}
func (m *mockChainMessage) TimeoutOverride(...time.Duration) time.Duration      { return 0 }
func (m *mockChainMessage) GetForceCacheRefresh() bool                          { return false }
func (m *mockChainMessage) SetForceCacheRefresh(force bool) bool                { return false }
func (m *mockChainMessage) GetRawRequestHash() ([]byte, error)                  { return m.requestData, nil }
func (m *mockChainMessage) GetRequestedBlocksHashes() []string                  { return nil }
func (m *mockChainMessage) UpdateEarliestInMessage(incomingEarliest int64) bool { return false }
func (m *mockChainMessage) SetExtension(extension *spectypes.Extension)         {}
func (m *mockChainMessage) GetUsedDefaultValue() bool                           { return false }
func (m *mockChainMessage) GetParseDirective() *spectypes.ParseDirective        { return nil }
func (m *mockChainMessage) IsBatch() bool                                       { return false }

type mockGenericMessage struct {
	data []byte
}

func (m *mockGenericMessage) GetHeaders() []pairingtypes.Metadata {
	return []pairingtypes.Metadata{}
}

func (m *mockGenericMessage) DisableErrorHandling() {}

func (m *mockGenericMessage) GetParams() interface{} {
	return nil
}

// ==================== Block Extraction Tests ====================

// TestExtractLatestBlockFromEVMResponse tests EVM-specific block extraction
func TestExtractBlockHeightFromEVMResponse(t *testing.T) {
	tests := []struct {
		name         string
		responseData []byte
		method       string
		expected     int64
	}{
		{
			name:         "eth_blockNumber - hex string",
			responseData: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x12a7b5c"}`),
			method:       "eth_blockNumber",
			expected:     19561308, // 0x12a7b5c in decimal
		},
		{
			name:         "eth_getBlockByNumber - block object",
			responseData: []byte(`{"jsonrpc":"2.0","id":1,"result":{"number":"0x100","hash":"0xabc"}}`),
			method:       "eth_getBlockByNumber",
			expected:     256, // 0x100 in decimal
		},
		{
			name:         "eth_getBlockByHash - block object",
			responseData: []byte(`{"jsonrpc":"2.0","id":1,"result":{"number":"0xff","hash":"0xdef"}}`),
			method:       "eth_getBlockByHash",
			expected:     255, // 0xff in decimal
		},
		{
			name:         "eth_getTransactionReceipt - receipt object",
			responseData: []byte(`{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x200","transactionHash":"0x123"}}`),
			method:       "eth_getTransactionReceipt",
			expected:     512, // 0x200 in decimal
		},
		{
			name:         "eth_getLogs - logs array",
			responseData: []byte(`{"jsonrpc":"2.0","id":1,"result":[{"blockNumber":"0x300","logIndex":"0x0"}]}`),
			method:       "eth_getLogs",
			expected:     768, // 0x300 in decimal
		},
		{
			name:         "unknown method - returns 0",
			responseData: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x12345"}`),
			method:       "eth_call",
			expected:     0,
		},
		{
			name:         "invalid JSON - returns 0",
			responseData: []byte(`not json`),
			method:       "eth_blockNumber",
			expected:     0,
		},
		{
			name:         "null result - returns 0",
			responseData: []byte(`{"jsonrpc":"2.0","id":1,"result":null}`),
			method:       "eth_getBlockByNumber",
			expected:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractBlockHeightFromEVMResponse(tt.responseData, tt.method)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractBlockHeightFromJSONResponse_Tendermint tests Tendermint-specific block extraction
// Note: This tests the fallback behavior when parse directive is nil
func TestExtractBlockHeightFromJSONResponse_WithoutParseDirective(t *testing.T) {
	// Create a mock chain message without parse directive (simulating Tendermint without spec)
	mockMsg := &mockChainMessage{
		api: &spectypes.Api{Name: "status"},
	}

	// Tendermint status response - without spec-driven parsing, returns 0 (fallback)
	// This is expected behavior - Tendermint methods need spec parsing to extract blocks
	responseData := []byte(`{"jsonrpc":"2.0","id":1,"result":{"sync_info":{"latest_block_height":"12345"}}}`)
	result := extractBlockHeightFromJSONResponse(responseData, mockMsg)

	// Without parse directive, Tendermint methods will return 0 (needs spec for proper parsing)
	// This test verifies the fallback behavior doesn't crash
	assert.Equal(t, int64(0), result, "Without parse directive, Tendermint should fallback gracefully")
}

// TestExtractBlockHeightFromJSONResponse_EVMFallback tests EVM fallback when no parse directive
func TestExtractBlockHeightFromJSONResponse_EVMFallback(t *testing.T) {
	// Create mock chain message without parse directive but with EVM method
	mockMsg := &mockChainMessage{
		api: &spectypes.Api{Name: "eth_blockNumber"},
	}

	// EVM eth_blockNumber response
	responseData := []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1000"}`)
	result := extractBlockHeightFromJSONResponse(responseData, mockMsg)

	// Should fallback to EVM-specific parsing
	assert.Equal(t, int64(4096), result, "EVM methods should work via fallback parsing")
}

// ==================== joinURLPath Tests ====================

func TestJoinURLPath(t *testing.T) {
	tests := []struct {
		name        string
		base        string
		path        string
		wantURL     string
		wantErr     bool
		description string
	}{
		{
			name:        "absolute path preserves base path (gateway)",
			base:        "https://g.w.lavanet.xyz:443/gateway/lava/rest/YOUR_LAVA_GATEWAY_KEY",
			path:        "/cosmos/bank/v1beta1/balances/lava@1ax3pa3eg0k5wnmuj8v3k2yd87a4ka5g9ymwdu3",
			wantURL:     "https://g.w.lavanet.xyz:443/gateway/lava/rest/YOUR_LAVA_GATEWAY_KEY/cosmos/bank/v1beta1/balances/lava@1ax3pa3eg0k5wnmuj8v3k2yd87a4ka5g9ymwdu3",
			wantErr:     false,
			description: "base path must not be replaced by ResolveReference",
		},
		{
			name:        "base with no path and absolute path",
			base:        "https://api.celestia.pops.one",
			path:        "/cosmos/bank/v1beta1/balances/someaddr",
			wantURL:     "https://api.celestia.pops.one/cosmos/bank/v1beta1/balances/someaddr",
			wantErr:     false,
			description: "host-only base gets request path appended",
		},
		{
			name:        "base with trailing slash",
			base:        "https://example.com/api/",
			path:        "/v1/query",
			wantURL:     "https://example.com/api/v1/query",
			wantErr:     false,
			description: "single slash between base path and request path",
		},
		{
			name:        "path with query string",
			base:        "https://gateway.example.com/rest/key",
			path:        "/cosmos/bank/v1beta1/balances/addr?height=100",
			wantURL:     "https://gateway.example.com/rest/key/cosmos/bank/v1beta1/balances/addr?height=100",
			wantErr:     false,
			description: "query string preserved",
		},
		{
			name:        "relative path uses ResolveReference",
			base:        "https://example.com/base/",
			path:        "sub/foo",
			wantURL:     "https://example.com/base/sub/foo",
			wantErr:     false,
			description: "relative path resolved against base",
		},
		{
			name:        "invalid base URL",
			base:        "://invalid",
			path:        "/path",
			wantURL:     "",
			wantErr:     true,
			description: "invalid base returns error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := joinURLPath(tt.base, tt.path)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantURL, got, tt.description)
		})
	}
}
