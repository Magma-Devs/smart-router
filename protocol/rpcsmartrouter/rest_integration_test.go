package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
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
	// A node error, classified by the registry's 404 row: "not here" — retryable on another
	// node (a lagging node or a gateway may say it while another node has the data), data-scope
	// so the endpoint is not scored, body returned to the caller unchanged.
	assert.True(t, result.IsNodeError)
	assert.False(t, result.IsNonRetryable)
	assert.True(t, result.IsDataScope)
	assert.False(t, result.IsNodeAtFault)
	assert.False(t, shouldFailSessionForResult(nil, result), "a 404 is not scored against the endpoint")
	assert.Contains(t, string(result.Reply.Data), "block not found")
}

// sendRESTThroughMockUpstream drives the real REST sender against a mock upstream that answers
// every request with the given status and body, for a POST to a stateful path.
func sendRESTThroughMockUpstream(t *testing.T, status int, body string) *common.RelayResult {
	t.Helper()
	ctx := context.Background()
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx,
		"LAVA",
		spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			if body != "" {
				_, _ = w.Write([]byte(body))
			}
		}),
		nil,
		"../../",
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(closeServer)

	chainMessage, err := chainParser.ParseMsg("/cosmos/tx/v1beta1/txs", []byte("data"), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)

	directConn, err := lavasession.NewDirectRPCConnection(ctx, endpoint.NodeUrls[0], 5, "")
	require.NoError(t, err)
	sender := &DirectRPCRelaySender{directConnection: directConn, endpointName: "test-cosmos-lcd"}

	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)
	return result
}

// TestRESTRelay_404_EmptyBody_IsANodeError pins the sender's side for the dfns incident: an empty
// 404 from a gateway is a node error (a broadcast keeps waiting for its sibling), classified by
// the registry's 404 row as data-scope: retried elsewhere on a read, not the endpoint's fault.
func TestRESTRelay_404_EmptyBody_IsANodeError(t *testing.T) {
	result := sendRESTThroughMockUpstream(t, http.StatusNotFound, "")

	assert.Equal(t, http.StatusNotFound, result.StatusCode)
	assert.True(t, result.IsNodeError)
	assert.False(t, result.IsNonRetryable)
	assert.True(t, result.IsDataScope)
	assert.False(t, result.IsNodeAtFault)
	assert.Empty(t, result.Reply.Data)
}

// TestRESTRelay_405_IsANodeError: whatever its body, a 405 is a node error, classified by the
// registry's 405 row.
func TestRESTRelay_405_IsANodeError(t *testing.T) {
	result := sendRESTThroughMockUpstream(t, http.StatusMethodNotAllowed, `{"message":"method not allowed"}`)

	assert.Equal(t, http.StatusMethodNotAllowed, result.StatusCode)
	assert.True(t, result.IsNodeError)
	assert.True(t, result.IsNonRetryable)
	assert.False(t, result.IsNodeAtFault)
}

// TestRESTRelay_404_ProblemDocument_IsANodeError: a Horizon problem document is the chain's answer
// to the caller — and, like every other non-2xx, a node error the router classifies. The body
// travels unchanged; a write keeps waiting, a read is tried on another node first.
func TestRESTRelay_404_ProblemDocument_IsANodeError(t *testing.T) {
	result := sendRESTThroughMockUpstream(t, http.StatusNotFound, `{"type":"https://stellar.org/horizon-errors/not_found","title":"Resource Missing","status":404}`)

	assert.Equal(t, http.StatusNotFound, result.StatusCode)
	assert.True(t, result.IsNodeError)
	assert.False(t, result.IsNonRetryable)
	assert.True(t, result.IsDataScope)
	assert.False(t, result.IsNodeAtFault)
	assert.Contains(t, string(result.Reply.Data), "Resource Missing")
}

// TestRESTRelay_400_RejectedWrite_IsANodeError: Horizon reports a transaction the chain rejected
// as a 400 problem document with result codes. Node error, non-retryable (the registry's new 400
// row), not the endpoint's fault — and on a broadcast the sibling is still waited for.
func TestRESTRelay_400_RejectedWrite_IsANodeError(t *testing.T) {
	result := sendRESTThroughMockUpstream(t, http.StatusBadRequest, `{"type":"https://stellar.org/horizon-errors/transaction_failed","status":400,"extras":{"result_codes":{"transaction":"tx_bad_seq"}}}`)

	assert.Equal(t, http.StatusBadRequest, result.StatusCode)
	assert.True(t, result.IsNodeError)
	assert.True(t, result.IsNonRetryable)
	assert.False(t, result.IsNodeAtFault)
	assert.False(t, shouldFailSessionForResult(nil, result), "a rejected request must not score the endpoint")
	assert.Contains(t, string(result.Reply.Data), "tx_bad_seq")
}

// TestRESTRelay_409_Duplicate_IsANodeErrorNotScored: Horizon answers a transaction it already has
// with 409 {"tx_status":"DUPLICATE"}. A node error (so on a broadcast the sibling's 201 wins),
// classified as "already known": non-retryable, never the endpoint's fault, not scored.
func TestRESTRelay_409_Duplicate_IsANodeErrorNotScored(t *testing.T) {
	result := sendRESTThroughMockUpstream(t, http.StatusConflict, `{"tx_status":"DUPLICATE","hash":"f39835424ff755846b5982836812ee08f29df926fb87bf48be2048231a99dc24"}`)

	assert.Equal(t, http.StatusConflict, result.StatusCode)
	assert.True(t, result.IsNodeError)
	assert.True(t, result.IsNonRetryable)
	assert.False(t, result.IsNodeAtFault)
	assert.False(t, shouldFailSessionForResult(nil, result), "a duplicate is not the endpoint's fault")
	assert.Contains(t, string(result.Reply.Data), "DUPLICATE")
}

// TestRESTRelay_403_AccessDenied_IsRetriedButNotBlamed: a 403 on REST is the endpoint refusing the
// router (a WAF, a plan, an operator's filter). Another provider can serve the request, so it is
// retried — and the refusing endpoint keeps its health, because it answered truthfully about its
// own configuration rather than failing.
//
// Blaming it was the bug: IsNodeAtFault takes the MarkUnhealthy arm, which walks
// Endpoint.ConnectionRefusals, and that counter disables the URL for EVERY path at
// MaxConsecutiveConnectionAttempts (--bench-after, default 50) and is reset only by a 2xx. A vendor
// gating one path behind a plan took out the whole endpoint; a wrong or expired shared credential
// answering 401 on every path took out every endpoint using it, with nothing ever resetting the
// counter. REST gets no replayable probe evidence either (recordRelayProbeEvidence is JSON-RPC
// only), so recovery was poll-only.
func TestRESTRelay_403_AccessDenied_IsRetriedButNotBlamed(t *testing.T) {
	result := sendRESTThroughMockUpstream(t, http.StatusForbidden, `<!DOCTYPE html><html>blocked</html>`)

	assert.Equal(t, http.StatusForbidden, result.StatusCode)
	assert.True(t, result.IsNodeError)
	assert.False(t, result.IsNonRetryable, "another provider can serve it")
	assert.True(t, result.IsNodeCapability, "it is a property of the endpoint's configuration")
	assert.False(t, result.IsNodeAtFault, "so the endpoint is not marked unhealthy")
	assert.False(t, shouldFailSessionForResult(nil, result),
		"and not demoted through the availability signal either — one answer, one verdict")
}

// TestRESTRelay_401_Unauthorized_IsRetriedButNotBlamed is the same rule on the status that makes it
// load-bearing: a 401 from a wrong or expired credential fails every path, so unlike a 403 on one
// gated path there is never a 2xx in between to reset the refusal counter.
func TestRESTRelay_401_Unauthorized_IsRetriedButNotBlamed(t *testing.T) {
	result := sendRESTThroughMockUpstream(t, http.StatusUnauthorized, `{"message":"invalid api key"}`)

	assert.Equal(t, http.StatusUnauthorized, result.StatusCode)
	assert.True(t, result.IsNodeError)
	assert.False(t, result.IsNonRetryable)
	assert.False(t, result.IsNodeAtFault)
	assert.False(t, shouldFailSessionForResult(nil, result))
}

// TestRESTRelay_500_CosmosPrunedHeight_IsNotBlamed: Cosmos answers a pruned height with a 500, and
// it is the node telling the truth about what it holds. Through the sender it must stay a node
// error the client receives unchanged, retried on an archive node, and not counted against the
// endpoint that answered honestly.
func TestRESTRelay_500_CosmosPrunedHeight_IsNotBlamed(t *testing.T) {
	body := `{"code":2,"message":"height 1 is not available, lowest height is 25280088","details":[]}`
	result := sendRESTThroughMockUpstream(t, http.StatusInternalServerError, body)

	assert.Equal(t, http.StatusInternalServerError, result.StatusCode)
	assert.True(t, result.IsNodeError)
	assert.False(t, result.IsNonRetryable, "an archive node may hold it")
	assert.True(t, result.IsDataScope)
	assert.False(t, result.IsNodeAtFault, "the node answered truthfully about what it holds")
}

// TestRESTRelay_500_Opaque_StaysBlamed is the other side of that line: a 500 the router cannot read
// as the caller's answer is still the endpoint's fault. Each 500 carve-out is gated on a probed
// body precisely so this case does not move.
func TestRESTRelay_500_Opaque_StaysBlamed(t *testing.T) {
	result := sendRESTThroughMockUpstream(t, http.StatusInternalServerError, `{"message":"database connection lost"}`)

	assert.True(t, result.IsNodeError)
	assert.True(t, result.IsNodeAtFault, "an unexplained 500 is still the endpoint's problem")
}

// TestRESTRelay_400_BeyondHead_IsRetriedElsewhere: a lagging sidecar answers "the block you asked
// for is beyond my head" with a 400. A synced node has the block: retry, do not blame.
func TestRESTRelay_400_BeyondHead_IsRetriedElsewhere(t *testing.T) {
	result := sendRESTThroughMockUpstream(t, http.StatusBadRequest, `{"code":400,"message":"Specified block number is larger than the current largest block. The largest known block number is 21144972."}`)

	assert.True(t, result.IsNodeError)
	assert.False(t, result.IsNonRetryable)
	assert.True(t, result.IsDataScope)
	assert.False(t, result.IsNodeAtFault)
	assert.False(t, shouldFailSessionForResult(nil, result))
}

// TestRESTRelay_410_Pruned_IsRetriedElsewhere: Aptos and Horizon answer 410 for data the node no
// longer holds. The registry's new 410 row makes it data-scope: retryable on an archive node, and
// not the endpoint's fault.
func TestRESTRelay_410_Pruned_IsRetriedElsewhere(t *testing.T) {
	result := sendRESTThroughMockUpstream(t, http.StatusGone, `{"error_code":"version_pruned","message":"Ledger version(1) has been pruned"}`)

	assert.Equal(t, http.StatusGone, result.StatusCode)
	assert.True(t, result.IsNodeError)
	assert.False(t, result.IsNonRetryable)
	assert.True(t, result.IsDataScope)
	assert.False(t, result.IsNodeAtFault)
	assert.False(t, shouldFailSessionForResult(nil, result), "a pruned answer is truthful, not a fault")
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
	_, relayErr, _ := rpcss.relayInnerDirect(ctx, session, relayResult, 5*time.Second, 5*time.Second, chainMessage, nil, nil, nil, nil)

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

// relayInnerDirectREST drives relayInnerDirect — the arm that turns a REST 5xx into a transport
// error — against a mock upstream answering with the given status and body. Endpoint is left nil,
// as in the 501 test above, so the MarkUnhealthy blocks are skipped; the fault verdict is read from
// the result's flags instead.
func relayInnerDirectREST(t *testing.T, status int, body string) (*common.RelayResult, error) {
	t.Helper()
	ctx := context.Background()
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx,
		"LAVA",
		spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			if body != "" {
				_, _ = w.Write([]byte(body))
			}
		}),
		nil,
		"../../",
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(closeServer)

	chainMessage, err := chainParser.ParseMsg("/cosmos/tx/v1beta1/txs", []byte("data"), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)

	nodeUrl := endpoint.NodeUrls[0]
	directConn, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)
	session := &lavasession.SingleConsumerSession{
		Parent: &lavasession.ConsumerSessionsWithProvider{PublicLavaAddress: "test-rest"},
		Connection: &lavasession.DirectRPCSessionConnection{
			DirectConnection: directConn,
			EndpointAddress:  nodeUrl.Url,
		},
	}
	rpcss := &RPCSmartRouterServer{
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: "rest"},
	}
	relayResult := &common.RelayResult{}
	_, relayErr, _ := rpcss.relayInnerDirect(ctx, session, relayResult, 5*time.Second, 5*time.Second, chainMessage, nil, nil, nil)
	return relayResult, relayErr
}

// TestRESTRelay_503_TryAgainLater_KeepsTheNodesReply is the ticket's case: Horizon answers a submit
// with 503 {"tx_status":"TRY_AGAIN_LATER"} — nothing was submitted, a retry is safe. The reply must
// stay a node error carrying Horizon's status and body, instead of becoming a transport error that
// drops the body. The endpoint is still at fault (the registry's 503 row) and still scored.
func TestRESTRelay_503_TryAgainLater_KeepsTheNodesReply(t *testing.T) {
	body := `{"tx_status":"TRY_AGAIN_LATER","hash":"0000000000000000000000000000000000000000000000000000000000000007"}`
	relayResult, relayErr := relayInnerDirectREST(t, http.StatusServiceUnavailable, body)

	require.NoError(t, relayErr, "a 5xx with the node's JSON reply must not become a transport error")
	assert.Equal(t, http.StatusServiceUnavailable, relayResult.StatusCode)
	assert.True(t, relayResult.IsNodeError)
	assert.Equal(t, body, string(relayResult.Reply.Data), "the client must be able to read TRY_AGAIN_LATER")
	assert.True(t, relayResult.IsNodeAtFault, "blame unchanged: the endpoint is still at fault")
	assert.False(t, relayResult.IsNonRetryable, "a read is still retried on another node")
	assert.True(t, shouldFailSessionForResult(relayErr, relayResult), "and still scored")
}

// TestRESTRelay_500_TonRejection_KeepsTheNodesReply: toncenter rejects a sendBoc with a JSON 500
// (MAG-3974, seen live on dfns TON testnet). The client must get toncenter's reason, not the router's
// "insufficient results" 500, or it cannot tell that the message will never be accepted.
func TestRESTRelay_500_TonRejection_KeepsTheNodesReply(t *testing.T) {
	body := `{"ok":false,"error":"duplicate message","code":500}`
	relayResult, relayErr := relayInnerDirectREST(t, http.StatusInternalServerError, body)

	require.NoError(t, relayErr, "a 5xx with the node's JSON reply must not become a transport error")
	assert.Equal(t, http.StatusInternalServerError, relayResult.StatusCode)
	assert.True(t, relayResult.IsNodeError)
	assert.Equal(t, body, string(relayResult.Reply.Data), "the client must be able to read toncenter's reason")
}

// TestRESTRelay_5xx_TransportPathUnchanged: the shapes that must keep the old path — an empty 500, a
// proxy's HTML 502, and a JSON 502 or 504 (a gateway that may already have forwarded the request:
// the write may still apply, so it must stay "may have reached the node").
func TestRESTRelay_5xx_TransportPathUnchanged(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"empty 500", http.StatusInternalServerError, ""},
		{"HTML 502", http.StatusBadGateway, `<html><body><h1>502 Bad Gateway</h1></body></html>`},
		{"JSON 504 (Horizon timeout)", http.StatusGatewayTimeout, `{"type":"https://stellar.org/horizon-errors/timeout","status":504}`},
		{"JSON 502 (a gateway that may have forwarded the request)", http.StatusBadGateway, `{"message":"bad gateway"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, relayErr := relayInnerDirectREST(t, tc.status, tc.body)
			require.Error(t, relayErr, "still a transport error")
			assert.Contains(t, relayErr.Error(), strconv.Itoa(tc.status))
		})
	}
}
