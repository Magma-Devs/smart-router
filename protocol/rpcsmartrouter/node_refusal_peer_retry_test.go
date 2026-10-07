package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// refusalPeerAnswer is what the peer that serves the request answers.
const refusalPeerAnswer = `{"jsonrpc":"2.0","id":1,"result":"0x10"}`

// refusalReply is one upstream answer: an HTTP status and a body.
type refusalReply struct {
	status int
	body   string
}

// TestNodeRefusal_AskedOfAPeerBeforeFailingTheCaller_MAG2771 sends requests through
// SendParsedRelay to two local upstreams. One refuses, the other serves.
//
// MAG-2771: a refusal is terminal only if it is a fact about the chain. A NODE_* refusal is a
// claim the node makes about itself — its method set, its routes, its plan's limits, the
// credentials it was given — so it is provider-local until a peer confirms it, and the request
// must reach the peer before the caller is failed. CHAIN_* and USER_* answers are the same from
// every node, so the controls must still end on the first provider.
//
// The refusing upstream is the only primary and the serving one the backup, so every request is
// asked of the refuser first. Each case sends several requests, so a refusal that changed how the
// next request is routed would show.
func TestNodeRefusal_AskedOfAPeerBeforeFailingTheCaller_MAG2771(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	cases := []struct {
		name    string
		refusal refusalReply
		// code is the registry entry the refusal classifies as. Asserted, so a matcher change
		// cannot quietly turn a case into a test of some other code.
		code uint32
		// peerAsked is the rule under test: true for a claim about the node, false for a fact.
		peerAsked bool
	}{
		{
			// The MAG-2735 shape: a provider with the method off for its plan answers -32601.
			name:      "2001 NODE_METHOD_NOT_FOUND, -32601",
			refusal:   refusalReply{http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found: eth_getBalance"}}`},
			code:      common.LavaErrorNodeMethodNotFound.Code,
			peerAsked: true,
		},
		{
			name:      "2008 NODE_UNIMPLEMENTED, HTTP 501",
			refusal:   refusalReply{http.StatusNotImplemented, `{}`},
			code:      common.LavaErrorNodeUnimplemented.Code,
			peerAsked: true,
		},
		{
			name:      "2009 NODE_ENDPOINT_NOT_FOUND, HTTP 404",
			refusal:   refusalReply{http.StatusNotFound, `{}`},
			code:      common.LavaErrorNodeEndpointNotFound.Code,
			peerAsked: true,
		},
		{
			name:      "2010 NODE_METHOD_NOT_ALLOWED, HTTP 405",
			refusal:   refusalReply{http.StatusMethodNotAllowed, `{}`},
			code:      common.LavaErrorNodeMethodNotAllowed.Code,
			peerAsked: true,
		},
		{
			name:      "2011 NODE_LIMIT_EXCEEDED, -32005",
			refusal:   refusalReply{http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"limit exceeded"}}`},
			code:      common.LavaErrorNodeLimitExceeded.Code,
			peerAsked: true,
		},
		{
			name:      "2016 NODE_UNAUTHORIZED, HTTP 401",
			refusal:   refusalReply{http.StatusUnauthorized, `{}`},
			code:      common.LavaErrorNodeUnauthorized.Code,
			peerAsked: true,
		},
		{
			// Positive control: retryable before MAG-2771, so this harness must show it failing
			// over on any build. A case above that fails while this one passes is the rule, not
			// the harness.
			name:      "control: 2002 NODE_METHOD_NOT_SUPPORTED, -32004",
			refusal:   refusalReply{http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32004,"message":"Method not supported"}}`},
			code:      common.LavaErrorNodeMethodNotSupported.Code,
			peerAsked: true,
		},
		{
			name:      "control: CHAIN_EXECUTION_REVERTED stays terminal",
			refusal:   refusalReply{http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}`},
			code:      common.LavaErrorChainExecutionReverted.Code,
			peerAsked: false,
		},
		{
			name:      "control: USER_INVALID_PARAMS stays terminal",
			refusal:   refusalReply{http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid argument 0: hex string has length 2, want 40 for common.Address"}}`},
			code:      common.LavaErrorUserInvalidParams.Code,
			peerAsked: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.code, classifyLikeTheJSONRPCSender(t, chainParser, tc.refusal).Code,
				"the refusal must classify as the code this case is about")

			hits, server := newRefusalTestServer(t, chainParser, logs, tc.refusal, refusalReply{http.StatusOK, refusalPeerAnswer})

			refusedFirst := 0
			for i := 0; i < 24; i++ {
				before := loadRefusalHits(hits)
				result, sendErr := sendRefusalTestRequest(t, ctx, chainParser, server)
				after := loadRefusalHits(hits)
				refuserAsked, peerAsked := after[0]-before[0], after[1]-before[1]

				if refuserAsked == 0 {
					// The peer was picked first and answered: nothing to retry. Not expected with
					// the refuser as the only primary, but not wrong either.
					require.NoError(t, sendErr)
					require.Equal(t, int64(1), peerAsked)
					continue
				}
				refusedFirst++
				require.Equal(t, int64(1), refuserAsked, "the refusing upstream is asked once")

				if tc.peerAsked {
					require.NoError(t, sendErr, "the peer serves the request, so the caller must get its answer")
					require.Equal(t, int64(1), peerAsked, "the refusal must send the request to the peer")
					require.Contains(t, string(result.Reply.Data), `"result":"0x10"`, "the caller gets the peer's answer")
					require.Equal(t, "1", refusalReplyHeader(result, common.RETRY_COUNT_HEADER_NAME), "one retry: the hop to the peer")
					continue
				}
				require.Zero(t, peerAsked, "a fact about the chain or the request must not be asked of a peer")
				if sendErr == nil {
					require.NotContains(t, string(result.Reply.Data), `"result"`, "the caller gets the first answer, not a peer's")
				}
			}
			t.Logf("the refusing upstream was asked first in %d of 24 requests", refusedFirst)
			require.Positive(t, refusedFirst, "the refusing upstream was never asked first, so this case tested nothing")
		})
	}
}

// TestNodeRefusal_EveryProviderRefuses_TheCallerGetsTheRefusal_MAG2771 is the cost side of the
// rule: when no provider serves the method, the router pays one extra hop and the caller still
// gets the node's own refusal, not a router error.
func TestNodeRefusal_EveryProviderRefuses_TheCallerGetsTheRefusal_MAG2771(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	refusal := refusalReply{http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found: eth_getBalance"}}`}
	hits, server := newRefusalTestServer(t, chainParser, logs, refusal, refusal)

	result, sendErr := sendRefusalTestRequest(t, ctx, chainParser, server)
	require.Equal(t, [2]int64{1, 1}, loadRefusalHits(hits), "each provider is asked once")
	require.NoError(t, sendErr, "a node's refusal is relayed to the caller, as it is from one provider")
	require.NotNil(t, result.Reply)
	require.Contains(t, string(result.Reply.Data), "-32601", "the caller gets the node's own refusal")
	require.Equal(t, "1", refusalReplyHeader(result, common.RETRY_COUNT_HEADER_NAME))
}

// classifyLikeTheJSONRPCSender classifies a reply from the same inputs sendJSONRPCRelay builds: the
// body's JSON-RPC code when it has one, otherwise the HTTP status, and the status prefixed to the
// message when it is not 2xx.
func classifyLikeTheJSONRPCSender(t *testing.T, chainParser chainlib.ChainParser, reply refusalReply) *common.LavaError {
	t.Helper()
	chainMessage := parseRefusalTestRequest(t, chainParser)
	hasError, message := chainMessage.CheckResponseError([]byte(reply.body), reply.status)
	require.True(t, hasError, "the reply must read as a node error")
	errorCode := reply.status
	if code := common.ExtractJSONRPCErrorCode([]byte(reply.body)); code != 0 {
		errorCode = code
	}
	if reply.status < 200 || reply.status >= 300 {
		message = fmt.Sprintf("HTTP %d: %s", reply.status, message)
	}
	return common.ClassifyError(nil, common.GetChainFamilyOrDefault("ETH1"), common.TransportJsonRPC, errorCode, message)
}

// newRefusalTestServer builds a router with two local upstreams, each answering every request with
// its reply and counting it. "refuser" is the only primary and "peer" the backup, so every request
// is asked of the refuser first and a retry has exactly one other provider to go to. Weighted
// selection between two primaries made which one is asked first unpredictable, and a case where
// the refuser is never asked first tests nothing.
func newRefusalTestServer(t *testing.T, chainParser chainlib.ChainParser, logs *metrics.RPCConsumerLogs, refuser, peer refusalReply) (*[2]int64, *RPCSmartRouterServer) {
	t.Helper()
	ctx := context.Background()
	hits := &[2]int64{}
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "ETH1")
	csm := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())

	tiers := []map[uint64]*lavasession.ConsumerSessionsWithProvider{{}, {}}
	for i, upstreamCfg := range []struct {
		name  string
		reply refusalReply
	}{{"refuser", refuser}, {"peer", peer}} {
		counter, reply := &hits[i], upstreamCfg.reply
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			atomic.AddInt64(counter, 1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(reply.status)
			_, _ = w.Write([]byte(reply.body))
		}))
		t.Cleanup(upstream.Close)
		connection, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "")
		require.NoError(t, err)
		endpoint := &lavasession.Endpoint{NetworkAddress: upstream.URL, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{connection}}
		provider := lavasession.NewConsumerSessionWithProvider(upstreamCfg.name, []*lavasession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		tiers[i][uint64(i)] = provider
	}
	require.NoError(t, csm.UpdateAllProviders(1, tiers[0], tiers[1]))

	return hits, &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        chainParser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}
}

func parseRefusalTestRequest(t *testing.T, chainParser chainlib.ChainParser) chainlib.ChainMessage {
	t.Helper()
	body := []byte(`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","latest"],"id":1}`)
	chainMessage, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	return chainMessage
}

func sendRefusalTestRequest(t *testing.T, ctx context.Context, chainParser chainlib.ChainParser, server *RPCSmartRouterServer) (*common.RelayResult, error) {
	t.Helper()
	chainMessage := parseRefusalTestRequest(t, chainParser)
	body := []byte(`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","latest"],"id":1}`)
	relayData := lavaprotocol.NewRelayData(ctx, http.MethodPost, "", body, 0, spectypes.LATEST_BLOCK, spectypes.APIInterfaceJsonRPC,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "dapp", "1.2.3.4")

	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return server.SendParsedRelay(callCtx, nil, protocolMessage)
}

func loadRefusalHits(hits *[2]int64) [2]int64 {
	return [2]int64{atomic.LoadInt64(&hits[0]), atomic.LoadInt64(&hits[1])}
}

// refusalReplyHeader returns a reply metadata value by name, matched case-insensitively as HTTP
// headers are.
func refusalReplyHeader(result *common.RelayResult, name string) string {
	if result == nil || result.Reply == nil {
		return ""
	}
	for _, metadata := range result.Reply.Metadata {
		if strings.EqualFold(metadata.Name, name) {
			return metadata.Value
		}
	}
	return ""
}
