package rpcsmartrouter

import (
	"errors"
	"net"
	"syscall"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyDirectRPCError_Nil(t *testing.T) {
	routerErr, wrappedErr := classifyDirectRPCError(nil, -1, common.TransportJsonRPC)
	assert.Equal(t, common.RouterErrorUnknown, routerErr)
	assert.Nil(t, wrappedErr)
}

func TestClassifyDirectRPCError_ConnectionRefused(t *testing.T) {
	errno := syscall.ECONNREFUSED
	err := &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: &net.OpError{
			Op:  "dial",
			Net: "tcp",
			Err: &errno,
		},
	}

	routerErr, _ := classifyDirectRPCError(err, -1, common.TransportJsonRPC)
	require.NotNil(t, routerErr)
	assert.Equal(t, common.RouterErrorConnectionRefused, routerErr)
	assert.True(t, routerErr.Retryable)
	assert.Equal(t, common.CategoryInternal, routerErr.Category)
}

func TestClassifyDirectRPCError_Timeout(t *testing.T) {
	err := &mockNetError{timeout: true}

	routerErr, _ := classifyDirectRPCError(err, -1, common.TransportJsonRPC)
	require.NotNil(t, routerErr)
	assert.Equal(t, common.RouterErrorConnectionTimeout, routerErr)
	assert.True(t, routerErr.Retryable)
	assert.Equal(t, common.CategoryInternal, routerErr.Category)
}

func TestClassifyDirectRPCError_HTTPStatusCodes(t *testing.T) {
	tests := []struct {
		name          string
		errorMsg      string
		expectedError *common.RouterError
	}{
		{
			name:          "HTTP 429 rate limit",
			errorMsg:      "HTTP status 429: Too Many Requests",
			expectedError: common.RouterErrorNodeRateLimited,
		},
		{
			name:          "HTTP 503 service unavailable",
			errorMsg:      "HTTP status 503: Service Unavailable",
			expectedError: common.RouterErrorNodeServiceUnavailable,
		},
		{
			name:          "HTTP 500 internal server error",
			errorMsg:      "HTTP status 500: Internal Server Error",
			expectedError: common.RouterErrorNodeInternalError,
		},
		{
			name:          "HTTP 502 bad gateway",
			errorMsg:      "HTTP status 502: Bad Gateway",
			expectedError: common.RouterErrorNodeBadGateway,
		},
		{
			name:          "HTTP 504 gateway timeout",
			errorMsg:      "HTTP status 504: Gateway Timeout",
			expectedError: common.RouterErrorNodeGatewayTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errorMsg)
			routerErr, _ := classifyDirectRPCError(err, -1, common.TransportJsonRPC)
			require.NotNil(t, routerErr)
			assert.Equal(t, tt.expectedError, routerErr)
			assert.Equal(t, common.CategoryExternal, routerErr.Category)
			assert.True(t, routerErr.Retryable)
		})
	}
}

func TestClassifyDirectRPCError_UnknownError(t *testing.T) {
	err := errors.New("your mom died")
	routerErr, _ := classifyDirectRPCError(err, -1, common.TransportJsonRPC)
	require.NotNil(t, routerErr)
	assert.Equal(t, common.RouterErrorUnknown, routerErr)
	// Unknown errors are external — they're pass-throughs from the node
	assert.Equal(t, common.CategoryExternal, routerErr.Category)
}

func TestClassifyDirectRPCError_RateLimitByMessage(t *testing.T) {
	err := errors.New("rate limit exceeded for this endpoint")
	routerErr, _ := classifyDirectRPCError(err, -1, common.TransportJsonRPC)
	require.NotNil(t, routerErr)
	assert.Equal(t, common.RouterErrorNodeRateLimited, routerErr)
}

func TestClassifyDirectRPCError_InternalVsExternal(t *testing.T) {
	// Connection errors are internal (Lava protocol layer)
	timeoutErr := &mockNetError{timeout: true}
	routerErr, _ := classifyDirectRPCError(timeoutErr, -1, common.TransportJsonRPC)
	assert.True(t, common.IsInternal(routerErr.Code))

	// HTTP status errors are external (node/chain layer)
	httpErr := errors.New("HTTP status 503: Service Unavailable")
	routerErr, _ = classifyDirectRPCError(httpErr, -1, common.TransportJsonRPC)
	assert.True(t, common.IsExternal(routerErr.Code))
}

func TestClassifyError_MatchOrdering(t *testing.T) {
	// "rate limit" message should match before HTTP 429 status
	err := "rate limit 429"
	result := common.ClassifyError(nil, common.ChainFamilyEVM, common.TransportJsonRPC, 0, err)
	assert.Equal(t, common.RouterErrorNodeRateLimited, result)
}

func TestClassifyDirectRPCError_UnsupportedMethod(t *testing.T) {
	// "method not found" has SubCategoryUnsupportedMethod (zero retries, cached response)
	err := errors.New("method not found")
	routerErr, _ := classifyDirectRPCError(err, -1, common.TransportJsonRPC)
	require.NotNil(t, routerErr)
	assert.Equal(t, common.RouterErrorNodeMethodNotFound, routerErr)
	assert.True(t, routerErr.SubCategory.IsUnsupportedMethod())

	// "method not supported" is retryable — another provider may support it — no SubCategory
	err = errors.New("the method is method not supported on this node")
	routerErr, _ = classifyDirectRPCError(err, -1, common.TransportJsonRPC)
	require.NotNil(t, routerErr)
	assert.Equal(t, common.RouterErrorNodeMethodNotSupported, routerErr)
	assert.False(t, routerErr.SubCategory.IsUnsupportedMethod())
	assert.True(t, routerErr.Retryable)
}

func TestClassifyDirectRPCError_JSONRPCBodyExtraction(t *testing.T) {
	// GIVEN an HTTP error whose body contains a Solana-specific JSON-RPC error code
	// WHEN classifyDirectRPCError processes it with ChainFamilySolana
	// THEN the code is extracted from the body and Tier 2 classification fires
	jsonBody := `{"jsonrpc":"2.0","error":{"code":-32009,"message":"Slot 123 was skipped, or missing in long-term storage"},"id":1}`
	httpErr := rpcclient.HTTPError{StatusCode: 200, Status: "200 OK", Body: []byte(jsonBody)}

	routerErr, _ := classifyDirectRPCError(httpErr, common.ChainFamilySolana, common.TransportJsonRPC)
	require.NotNil(t, routerErr)
	assert.Equal(t, common.RouterErrorChainSolanaMissingLongTerm, routerErr, "Solana -32009 should be extracted from HTTP body and classified via Tier 2")
}

func TestClassifyError_UnsupportedMethodByCode(t *testing.T) {
	// JSON-RPC -32601 should classify as unsupported method
	result := common.ClassifyError(nil, common.ChainFamilyEVM, common.TransportJsonRPC, -32601, "some error")
	assert.Equal(t, common.RouterErrorNodeMethodNotFound, result)
	assert.True(t, result.SubCategory.IsUnsupportedMethod())
}

func TestDetectConnectionError_NotRefused(t *testing.T) {
	// ETIMEDOUT is a timeout, not a connection refused
	otherErr := &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: syscall.ETIMEDOUT,
	}
	assert.NotEqual(t, common.RouterErrorConnectionRefused, common.DetectConnectionError(otherErr))

	regularErr := errors.New("regular error")
	assert.Nil(t, common.DetectConnectionError(regularErr))
}

func TestDetectConnectionError_Timeout(t *testing.T) {
	timeoutErr := &mockNetError{timeout: true}
	assert.Equal(t, common.RouterErrorConnectionTimeout, common.DetectConnectionError(timeoutErr))

	nonTimeoutErr := &mockNetError{timeout: false}
	assert.Nil(t, common.DetectConnectionError(nonTimeoutErr))

	regularErr := errors.New("regular error")
	assert.Nil(t, common.DetectConnectionError(regularErr))
}

func TestExtractRouterError_FromWrappedError(t *testing.T) {
	origErr := errors.New("nonce too low")
	_, wrappedErr := classifyDirectRPCError(origErr, -1, common.TransportJsonRPC)
	require.NotNil(t, wrappedErr)

	routerErr := extractRouterError(wrappedErr)
	require.NotNil(t, routerErr)
	assert.Equal(t, common.RouterErrorChainNonceTooLow, routerErr)
}

func TestExtractRouterError_FromPlainError(t *testing.T) {
	plainErr := errors.New("plain error")
	routerErr := extractRouterError(plainErr)
	assert.Nil(t, routerErr)
}

func TestWrappedError_ErrorsIs(t *testing.T) {
	// RouterWrappedError.Is() enables errors.Is matching against the *RouterError sentinel.
	// This was broken with the old classifiedError whose Unwrap() returned Original,
	// not the RouterError, so errors.Is(err, RouterErrorSomething) never worked.
	origErr := errors.New("nonce too low")
	_, wrappedErr := classifyDirectRPCError(origErr, -1, common.TransportJsonRPC)
	require.NotNil(t, wrappedErr)

	assert.True(t, errors.Is(wrappedErr, common.RouterErrorChainNonceTooLow), "errors.Is should match the RouterError sentinel")
}

// mockNetError implements net.Error for testing
type mockNetError struct {
	timeout   bool
	temporary bool
}

func (e *mockNetError) Error() string   { return "mock net error" }
func (e *mockNetError) Timeout() bool   { return e.timeout }
func (e *mockNetError) Temporary() bool { return e.temporary }

// ---------------------------------------------------------------------------
// Endpoint health classification tests (GIVEN–WHEN–THEN)
// ---------------------------------------------------------------------------

func TestClassifyEndpointHealth_InternalError(t *testing.T) {
	// GIVEN a CategoryInternal error (transport timeout, connection refused, DNS failure)
	// WHEN the relay fails
	// THEN the endpoint is marked unhealthy AND backoff is requested
	internalErrors := []*common.RouterError{
		common.RouterErrorConnectionTimeout,
		common.RouterErrorConnectionRefused,
		common.RouterErrorDNSFailure,
		common.RouterErrorConnectionReset,
		common.RouterErrorContextDeadline,
	}
	for _, le := range internalErrors {
		unhealthy, backoff := classifyEndpointHealth(le, false)
		assert.True(t, unhealthy, "%s should mark unhealthy", le.Name)
		assert.True(t, backoff, "%s should request backoff", le.Name)
	}
}

func TestClassifyEndpointHealth_ExternalRetryable(t *testing.T) {
	// GIVEN a CategoryExternal + Retryable error (5xx, node syncing)
	// WHEN the relay fails
	// THEN backoff is requested AND endpoint is marked unhealthy
	retryableErrors := []*common.RouterError{
		common.RouterErrorNodeInternalError,
		common.RouterErrorNodeServiceUnavailable,
		common.RouterErrorNodeBadGateway,
		common.RouterErrorNodeGatewayTimeout,
		common.RouterErrorNodeSyncing,
	}
	for _, le := range retryableErrors {
		unhealthy, backoff := classifyEndpointHealth(le, false)
		assert.True(t, unhealthy, "%s should mark unhealthy", le.Name)
		assert.True(t, backoff, "%s should request backoff", le.Name)
	}
}

func TestClassifyEndpointHealth_RateLimited(t *testing.T) {
	// GIVEN a rate-limited error (CategoryExternal + Retryable but rate-limited)
	// WHEN the relay fails
	// THEN backoff is requested but endpoint is NOT marked unhealthy (it's healthy, just busy)
	unhealthy, backoff := classifyEndpointHealth(common.RouterErrorNodeRateLimited, false)
	assert.False(t, unhealthy, "rate-limited should NOT mark unhealthy")
	assert.True(t, backoff, "rate-limited should request backoff")
}

func TestClassifyEndpointHealth_ExternalNonRetryable(t *testing.T) {
	// GIVEN a CategoryExternal + non-retryable error (4xx, unsupported method, nonce too low)
	// WHEN the relay fails
	// THEN neither mark unhealthy nor backoff (error is the user's or permanent)
	nonRetryableErrors := []*common.RouterError{
		common.RouterErrorNodeMethodNotFound,
		common.RouterErrorNodeEndpointNotFound,
		common.RouterErrorChainNonceTooLow,
		common.RouterErrorChainExecutionReverted,
		common.RouterErrorUserInvalidParams,
	}
	for _, le := range nonRetryableErrors {
		unhealthy, backoff := classifyEndpointHealth(le, false)
		assert.False(t, unhealthy, "%s should NOT mark unhealthy", le.Name)
		assert.False(t, backoff, "%s should NOT request backoff", le.Name)
	}
}

func TestClassifyEndpointHealth_UnsupportedMethodNeverPoisonsHealth(t *testing.T) {
	// GIVEN any unsupported-method classification — the method is absent from the node's API
	// surface, OR present but disabled on this specific node (NODE_METHOD_NOT_SUPPORTED, which is
	// Retryable and previously fell into the unhealthy arm)
	// WHEN the relay fails
	// THEN the endpoint is neither marked unhealthy nor backed off: a per-method capability gap is
	// not an endpoint fault, and it must not feed the MAG-2550 disable/re-enable flap.
	unsupported := []*common.RouterError{
		common.RouterErrorNodeMethodNotFound,
		common.RouterErrorNodeMethodNotSupported, // Retryable=true — the regression this test pins
		common.RouterErrorNodeUnimplemented,
		common.RouterErrorNodeEndpointNotFound,
		common.RouterErrorNodeMethodNotAllowed,
	}
	for _, le := range unsupported {
		unhealthy, backoff := classifyEndpointHealth(le, false)
		assert.False(t, unhealthy, "%s should NOT mark unhealthy (capability gap, not endpoint fault)", le.Name)
		assert.False(t, backoff, "%s should NOT request backoff (endpoint is neither broken nor busy)", le.Name)
	}
}

func TestClassifyEndpointHealth_Nil(t *testing.T) {
	// GIVEN a nil classification
	// WHEN health is evaluated
	// THEN neither mark unhealthy nor backoff
	unhealthy, backoff := classifyEndpointHealth(nil, false)
	assert.False(t, unhealthy)
	assert.False(t, backoff)
}

func TestClassifyEndpointHealth_ClientCancellationCarvesOut(t *testing.T) {
	// GIVEN any classification, including CategoryInternal errors that would
	// normally mark the endpoint unhealthy
	// WHEN the caller flags the failure as a client cancellation (relay race
	// loser or client disconnect)
	// THEN the endpoint MUST NOT be marked unhealthy and MUST NOT back off —
	// the provider is not at fault.
	cases := []*common.RouterError{
		common.RouterErrorContextCanceled,   // internal, !retryable
		common.RouterErrorContextDeadline,   // internal, retryable
		common.RouterErrorConnectionTimeout, // internal, retryable
		common.RouterErrorNodeInternalError, // external, retryable
		common.RouterErrorNodeRateLimited,   // external, retryable, rate-limited
		common.RouterErrorChainNonceTooLow,  // external, non-retryable
	}
	for _, le := range cases {
		unhealthy, backoff := classifyEndpointHealth(le, true)
		assert.False(t, unhealthy, "%s with client-cancellation must NOT mark unhealthy", le.Name)
		assert.False(t, backoff, "%s with client-cancellation must NOT back off", le.Name)
	}
}
