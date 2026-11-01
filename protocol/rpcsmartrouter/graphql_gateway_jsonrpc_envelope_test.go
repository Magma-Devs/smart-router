package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
)

// gatewayWrongPathReply is QuickNode's Sui GraphQL gateway answering a request sent to a path that is
// not its GraphQL service (MAG-4379): a JSON-RPC error envelope under a 400.
const gatewayWrongPathReply = `{"jsonrpc":"2.0","id":null,"error":{"code":-32604,"message":"this request method is not supported"}}`

// gatewayPeerAnswer is the provider that serves the query.
const gatewayPeerAnswer = `{"data":{"chainIdentifier":"35834a8a"}}`

const gatewayTestQuery = `{"query":"{ chainIdentifier }"}`

func newGatewayTestGraphQLParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	chainParser, err := chainlib.NewGraphQLChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(spectypes.Spec{
		Index:            "SUI",
		Enabled:          true,
		AverageBlockTime: 400,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGraphQL, Type: http.MethodPost},
			Apis: []*spectypes.Api{{
				Name:         "chainIdentifier",
				Enabled:      true,
				ComputeUnits: 10,
				BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
			}},
		}},
	})
	return chainParser
}

// TestGraphQLDirectRelay_GatewayJSONRPCEnvelope (MAG-4379) drives replies through the direct sender,
// the path every live relay takes. A JSON-RPC envelope under a status the registry files as the
// caller's is the gateway refusing the route: retryable, not the endpoint's fault. Every other
// status keeps its own verdict, and the node's own GraphQL answer under a 400 stays the caller's.
func TestGraphQLDirectRelay_GatewayJSONRPCEnvelope(t *testing.T) {
	ctx := context.Background()
	chainParser := newGatewayTestGraphQLParser(t)

	tests := []struct {
		name            string
		status          int
		reply           string
		wantNodeError   bool
		wantNonRetrying bool
		wantRateLimited bool
		wantAtFault     bool
		wantCapability  bool
		wantDataScope   bool
	}{
		{
			name:           "QuickNode's wrong-path 400 is the gateway refusing the route",
			status:         http.StatusBadRequest,
			reply:          gatewayWrongPathReply,
			wantNodeError:  true,
			wantCapability: true,
		},
		{
			name:           "a JSON-RPC envelope under a 422 is the gateway's too",
			status:         http.StatusUnprocessableEntity,
			reply:          gatewayWrongPathReply,
			wantNodeError:  true,
			wantCapability: true,
		},
		{
			name:            "a GraphQL validation error under a 400 is still the caller's",
			status:          http.StatusBadRequest,
			reply:           `{"errors":[{"message":"Cannot query field \"nope\" on type \"Query\".","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`,
			wantNodeError:   true,
			wantNonRetrying: true,
		},
		{
			// The hold-off keys on the 429; the envelope must not take it away.
			name:            "a JSON-RPC envelope under a 429 is still a rate limit",
			status:          http.StatusTooManyRequests,
			reply:           `{"jsonrpc":"2.0","id":null,"error":{"code":-32007,"message":"50/second request limit reached"}}`,
			wantRateLimited: true,
		},
		{
			name:           "a JSON-RPC envelope under a 403 keeps the access-denied row",
			status:         http.StatusForbidden,
			reply:          gatewayWrongPathReply,
			wantNodeError:  true,
			wantCapability: true,
		},
		{
			name:          "a JSON-RPC envelope under a 404 keeps the not-held row",
			status:        http.StatusNotFound,
			reply:         gatewayWrongPathReply,
			wantNodeError: true,
			wantDataScope: true,
		},
		{
			name:          "a JSON-RPC envelope under a 500 is still the endpoint's fault",
			status:        http.StatusInternalServerError,
			reply:         gatewayWrongPathReply,
			wantNodeError: true,
			wantAtFault:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.reply))
			}))
			defer server.Close()

			directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: server.URL + "/graphql"}, 1, "")
			require.NoError(t, err)
			sender := &DirectRPCRelaySender{directConnection: directConn, endpointName: "graphql-upstream"}

			chainMessage, err := chainParser.ParseMsg("", []byte(gatewayTestQuery), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)

			result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
			require.NoError(t, err)
			require.Equal(t, test.status, result.StatusCode)
			require.Equal(t, test.reply, string(result.Reply.Data), "the reply is passed through as the upstream wrote it")
			require.Equal(t, test.wantNodeError, result.IsNodeError, "IsNodeError")
			require.Equal(t, test.wantNonRetrying, result.IsNonRetryable, "IsNonRetryable")
			require.Equal(t, test.wantRateLimited, result.IsRateLimited, "IsRateLimited")
			require.Equal(t, test.wantAtFault, result.IsNodeAtFault, "IsNodeAtFault")
			require.Equal(t, test.wantCapability, result.IsNodeCapability, "IsNodeCapability")
			require.Equal(t, test.wantDataScope, result.IsDataScope, "IsDataScope")
		})
	}
}

// TestGraphQLRelay_GatewayJSONRPCEnvelopeIsAskedOfThePeer (MAG-4379) sends reads through
// SendParsedRelay to two local upstreams. The first is the only primary and answers QuickNode's
// wrong-path 400; the second, the backup, serves the query. The read must reach the second and the
// caller must get its answer. Before the fix the 400 was the caller's mistake: the relay stopped on
// the first provider and returned the gateway's JSON-RPC body. The control, the node's own GraphQL
// validation error under a 400, still ends the request on the first provider.
func TestGraphQLRelay_GatewayJSONRPCEnvelopeIsAskedOfThePeer(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := newGatewayTestGraphQLParser(t)

	cases := []struct {
		name      string
		refusal   refusalReply
		peerAsked bool
	}{
		{
			name:      "QuickNode's wrong-path 400 goes to the peer",
			refusal:   refusalReply{http.StatusBadRequest, gatewayWrongPathReply},
			peerAsked: true,
		},
		{
			name:      "control: a GraphQL validation error under a 400 stays terminal",
			refusal:   refusalReply{http.StatusBadRequest, `{"errors":[{"message":"Cannot query field \"nope\" on type \"Query\".","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`},
			peerAsked: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hits, server := newGatewayTestServer(t, chainParser, logs, tc.refusal, refusalReply{http.StatusOK, gatewayPeerAnswer})

			refusedFirst := 0
			for i := 0; i < 12; i++ {
				before := loadRefusalHits(hits)
				result, sendErr := sendGatewayTestRequest(t, ctx, chainParser, server)
				after := loadRefusalHits(hits)
				refuserAsked, peerAsked := after[0]-before[0], after[1]-before[1]
				if refuserAsked == 0 {
					require.NoError(t, sendErr)
					require.Equal(t, int64(1), peerAsked)
					continue
				}
				refusedFirst++
				require.Equal(t, int64(1), refuserAsked, "the first upstream is asked once")
				if tc.peerAsked {
					require.NoError(t, sendErr, "the peer serves the query, so the caller gets its answer")
					require.Equal(t, int64(1), peerAsked, "the gateway's refusal must send the read to the peer")
					require.NotNil(t, result.Reply)
					require.JSONEq(t, gatewayPeerAnswer, string(result.Reply.Data), "the caller gets the peer's answer, not the gateway's envelope")
					require.Equal(t, "1", refusalReplyHeader(result, common.RETRY_COUNT_HEADER_NAME), "one retry: the hop to the peer")
					continue
				}
				require.Zero(t, peerAsked, "the node's own answer about the query must not be asked of a peer")
				require.NoError(t, sendErr, "a node's answer is relayed to the caller")
				require.NotNil(t, result.Reply)
				require.JSONEq(t, tc.refusal.body, string(result.Reply.Data), "the caller gets the first answer")
			}
			require.Positive(t, refusedFirst, "the first upstream was never asked first, so this case tested nothing")
		})
	}
}

// newGatewayTestServer is newRefusalTestServer for the graphql interface: two local upstreams, the
// first the only primary and the second the backup, each answering every request with its reply.
func newGatewayTestServer(t *testing.T, chainParser chainlib.ChainParser, logs *metrics.RPCConsumerLogs, refuser, peer refusalReply) (*[2]int64, *RPCSmartRouterServer) {
	t.Helper()
	ctx := context.Background()
	hits := &[2]int64{}
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGraphQL}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "SUI")
	csm := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())

	tiers := []map[uint64]*lavasession.ConsumerSessionsWithProvider{{}, {}}
	for i, upstreamCfg := range []struct {
		name  string
		reply refusalReply
	}{{"gateway", refuser}, {"peer", peer}} {
		counter, reply := &hits[i], upstreamCfg.reply
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			atomic.AddInt64(counter, 1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(reply.status)
			_, _ = w.Write([]byte(reply.body))
		}))
		t.Cleanup(upstream.Close)
		connection, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL + "/graphql"}, 5, "")
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

func sendGatewayTestRequest(t *testing.T, ctx context.Context, chainParser chainlib.ChainParser, server *RPCSmartRouterServer) (*common.RelayResult, error) {
	t.Helper()
	body := []byte(gatewayTestQuery)
	chainMessage, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := lavaprotocol.NewRelayData(ctx, http.MethodPost, "", body, 0, spectypes.LATEST_BLOCK, spectypes.APIInterfaceGraphQL,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "dapp", "1.2.3.4")

	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return server.SendParsedRelay(callCtx, nil, protocolMessage)
}
