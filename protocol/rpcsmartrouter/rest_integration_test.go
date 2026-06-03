package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRESTRelay_GET_PathParameters(t *testing.T) {
	ctx := context.Background()
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx,
		"LAVA",
		spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, "/cosmos/base/tendermint/v1beta1/blocks/17", r.URL.Path)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"block":{"header":{"height":"17","chain_id":"cosmoshub-4"}}}`))
		}),
		nil,
		"../../",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, endpoint)
	defer closeServer()

	chainMessage, err := chainParser.ParseMsg(
		"/cosmos/base/tendermint/v1beta1/blocks/17",
		nil,
		http.MethodGet,
		nil,
		extensionslib.ExtensionInfo{LatestBlock: 0},
	)
	require.NoError(t, err)

	nodeUrl := endpoint.NodeUrls[0]
	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "test-cosmos-lcd",
	}

	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Reply)

	assert.Equal(t, http.StatusOK, result.StatusCode)
	assert.Contains(t, string(result.Reply.Data), "cosmoshub-4")
	assert.False(t, result.IsNodeError)
}

func TestRESTRelay_GET_QueryParameters(t *testing.T) {
	ctx := context.Background()
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx,
		"LAVA",
		spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, "/cosmos/tx/v1beta1/txs", r.URL.Path)
			q := r.URL.Query()
			assert.Equal(t, "cosmos1...", q.Get("sender"))
			assert.Equal(t, "10", q.Get("limit"))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"txs":[],"pagination":{"total":"0"}}`))
		}),
		nil,
		"../../",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, endpoint)
	defer closeServer()

	chainMessage, err := chainParser.ParseMsg(
		"/cosmos/tx/v1beta1/txs?sender=cosmos1...&limit=10",
		nil,
		http.MethodGet,
		nil,
		extensionslib.ExtensionInfo{LatestBlock: 0},
	)
	require.NoError(t, err)

	nodeUrl := endpoint.NodeUrls[0]
	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "test-cosmos-lcd",
	}

	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, http.StatusOK, result.StatusCode)
	assert.Contains(t, string(result.Reply.Data), "pagination")
}

func TestRESTRelay_POST_JSONBody(t *testing.T) {
	ctx := context.Background()
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx,
		"LAVA",
		spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodPost, r.Method)
			assert.Equal(t, "/cosmos/tx/v1beta1/simulate", r.URL.Path)
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			assert.NotEqual(t, int64(0), r.ContentLength)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"gas_info":{"gas_used":"12345"}}`))
		}),
		nil,
		"../../",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, endpoint)
	defer closeServer()

	chainMessage, err := chainParser.ParseMsg(
		"/cosmos/tx/v1beta1/simulate",
		[]byte(`{"tx_bytes":"base64encodedtx"}`),
		http.MethodPost,
		nil,
		extensionslib.ExtensionInfo{LatestBlock: 0},
	)
	require.NoError(t, err)

	nodeUrl := endpoint.NodeUrls[0]
	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "test-cosmos-lcd",
	}

	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, http.StatusOK, result.StatusCode)
	assert.Contains(t, string(result.Reply.Data), "gas_used")
}

func TestRESTRelay_404_NotFound(t *testing.T) {
	ctx := context.Background()
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx,
		"LAVA",
		spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":5,"message":"block not found"}`))
		}),
		nil,
		"../../",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, endpoint)
	defer closeServer()

	chainMessage, err := chainParser.ParseMsg(
		"/cosmos/base/tendermint/v1beta1/blocks/999999999",
		nil,
		http.MethodGet,
		nil,
		extensionslib.ExtensionInfo{LatestBlock: 0},
	)
	require.NoError(t, err)

	nodeUrl := endpoint.NodeUrls[0]
	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "test-cosmos-lcd",
	}

	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, http.StatusNotFound, result.StatusCode)
	assert.False(t, result.IsNodeError)
	assert.Contains(t, string(result.Reply.Data), "block not found")
}

func TestRESTRelay_429_RateLimit(t *testing.T) {
	ctx := context.Background()
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx,
		"LAVA",
		spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}`))
		}),
		nil,
		"../../",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, endpoint)
	defer closeServer()

	chainMessage, err := chainParser.ParseMsg(
		"/cosmos/base/tendermint/v1beta1/blocks/latest",
		nil,
		http.MethodGet,
		nil,
		extensionslib.ExtensionInfo{LatestBlock: 0},
	)
	require.NoError(t, err)

	nodeUrl := endpoint.NodeUrls[0]
	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "test-cosmos-lcd",
	}

	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, http.StatusTooManyRequests, result.StatusCode)
	assert.False(t, result.IsNodeError)
}

func TestRESTRelay_503_ServiceUnavailable(t *testing.T) {
	ctx := context.Background()
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx,
		"LAVA",
		spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"service temporarily unavailable"}`))
		}),
		nil,
		"../../",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, endpoint)
	defer closeServer()

	chainMessage, err := chainParser.ParseMsg(
		"/cosmos/base/tendermint/v1beta1/blocks/latest",
		nil,
		http.MethodGet,
		nil,
		extensionslib.ExtensionInfo{LatestBlock: 0},
	)
	require.NoError(t, err)

	nodeUrl := endpoint.NodeUrls[0]
	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "test-cosmos-lcd",
	}

	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, http.StatusServiceUnavailable, result.StatusCode)
	assert.True(t, result.IsNodeError)
}

func TestRESTRelay_ResponseHeaders(t *testing.T) {
	ctx := context.Background()
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx,
		"LAVA",
		spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Custom-Header", "test-value")
			w.Header().Set("X-Block-Height", "12345")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":"success"}`))
		}),
		nil,
		"../../",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, endpoint)
	defer closeServer()

	chainMessage, err := chainParser.ParseMsg(
		"/cosmos/base/tendermint/v1beta1/blocks/latest",
		nil,
		http.MethodGet,
		nil,
		extensionslib.ExtensionInfo{LatestBlock: 0},
	)
	require.NoError(t, err)

	nodeUrl := endpoint.NodeUrls[0]
	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	sender := &DirectRPCRelaySender{
		directConnection: directConn,
		endpointName:     "test-rest-endpoint",
	}

	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Reply)

	found := false
	for _, md := range result.Reply.Metadata {
		if md.Name == "X-Custom-Header" && md.Value == "test-value" {
			found = true
			break
		}
	}
	assert.True(t, found, fmt.Sprintf("expected X-Custom-Header in metadata, got: %+v", result.Reply.Metadata))
}

// TestRESTRelay_501_NotImplemented_relayInnerDirect reproduces MAG-1576: a
// Cosmos-based node returns HTTP 501 ("not implemented"), and relayInnerDirect()
// mis-routes it to the PROTOCOL-error path instead of treating it as a NodeError.
//
// Why the bug lives in relayInnerDirect (not the REST sender): sendRESTRelay
// already classifies 5xx as IsNodeError=true and returns a NIL Go error — see
// TestRESTRelay_503_ServiceUnavailable. But relayInnerDirect's blanket
//
//	if statusCode >= 500 || statusCode == 429 { ... return fmt.Errorf("HTTP %d", statusCode) }
//
// converts that node-error result into a synthetic transport error, which the
// caller files via setErrorResponse on the protocol-error path. For 501 this is
// wrong: 501 should be NODE_UNIMPLEMENTED (Retryable:false), so as a protocol
// error it gets RETRIED instead of failing fast and the node's body is lost.
//
// Regression guard: asserts that relayInnerDirect does NOT convert a REST 501
// into a transport error. Before the fix this failed with fmt.Errorf("HTTP 501");
// the fix excludes 501 from the `statusCode >= 500` transport-error branch so the
// NodeError the sender produced is preserved and returned to the client.
func TestRESTRelay_501_NotImplemented_relayInnerDirect(t *testing.T) {
	ctx := context.Background()
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx,
		"LAVA",
		spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Cosmos gRPC-gateway "not implemented" surfaces as HTTP 501.
			w.WriteHeader(http.StatusNotImplemented)
			_, _ = w.Write([]byte(`{"code":12,"message":"Not Implemented"}`))
		}),
		nil,
		"../../",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, endpoint)
	defer closeServer()

	chainMessage, err := chainParser.ParseMsg(
		"/cosmos/base/tendermint/v1beta1/blocks/latest",
		nil,
		http.MethodGet,
		nil,
		extensionslib.ExtensionInfo{LatestBlock: 0},
	)
	require.NoError(t, err)

	nodeUrl := endpoint.NodeUrls[0]
	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)

	// Minimal smart-router direct session. Endpoint is deliberately left nil so
	// relayInnerDirect's MarkUnhealthy/metrics blocks (guarded by
	// `targetEndpoint != nil`) are skipped — smartRouterEndpointMetrics is never
	// dereferenced, keeping the harness self-contained.
	cswp := &lavasession.ConsumerSessionsWithProvider{PublicLavaAddress: "test-cosmos-lcd"}
	session := &lavasession.SingleConsumerSession{
		Parent: cswp,
		Connection: &lavasession.DirectRPCSessionConnection{
			DirectConnection: directConn,
			EndpointAddress:  nodeUrl.Url,
		},
	}

	rpcss := &RPCSmartRouterServer{
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: "rest"},
	}

	relayResult := &common.RelayResult{}
	_, relayErr, _ := rpcss.relayInnerDirect(ctx, session, relayResult, 5*time.Second, chainMessage, nil, nil)

	// DESIRED (post-fix): a REST 501 "not implemented" is a NodeError, so
	// relayInnerDirect must NOT convert it into a Go error (which routes it to
	// setErrorResponse / the protocol-error path). This fails today with
	// relayErr = "HTTP 501" — that failure IS the reproduction.
	require.NoError(t, relayErr,
		"BUG: relayInnerDirect converts a REST 501 (which sendRESTRelay tagged IsNodeError=true) into a transport error, routing it to the protocol-error path instead of treating it as a NodeError")
	assert.Equal(t, http.StatusNotImplemented, relayResult.StatusCode)
	assert.True(t, relayResult.IsNodeError, "REST 501 should be classified as a node error")
	// End-to-end: with the classifier mapping 501→NODE_UNIMPLEMENTED, the node
	// error must be non-retryable so the policy stops instead of retrying an
	// unsupported method. This ties the routing fix (Part 1) to the classifier
	// fix (Part 2).
	assert.True(t, relayResult.IsNonRetryable, "REST 501 should be a non-retryable node error")
}
