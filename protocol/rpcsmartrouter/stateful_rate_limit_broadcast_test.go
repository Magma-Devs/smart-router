package rpcsmartrouter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// writeUpstream is one upstream of a two-provider write broadcast: what it answers and how long it
// takes to answer. firstStatus and firstBody, when set, are what it answers its first request
// with instead.
type writeUpstream struct {
	name        string
	status      int
	body        string
	delay       time.Duration
	firstStatus int
	firstBody   string
}

// writeInterfaceCase is one api interface's write: the request that is stateful on it, and the parser
// that says so.
type writeInterfaceCase struct {
	name         string
	chainID      string
	apiInterface string
	parser       func(t *testing.T) chainlib.ChainParser
	method       string
	path         string
	body         string
	// nodeURLPath is appended to the upstream's address: GraphQL posts to the endpoint's one URL.
	nodeURLPath string
	success     string
}

func eth1WriteParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	return chainParser
}

func lavaRESTWriteParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "LAVA", spectypes.APIInterfaceRest, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	return chainParser
}

// suiGraphQLWriteParser is a Sui-shaped GraphQL spec with one read and the stateful mutation Sui
// submits transactions with.
func suiGraphQLWriteParser(t *testing.T) chainlib.ChainParser {
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
			Apis: []*spectypes.Api{
				{
					Name:         "chainIdentifier",
					Enabled:      true,
					ComputeUnits: 10,
					BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
				},
				{
					Name:         "executeTransaction",
					Enabled:      true,
					ComputeUnits: 100,
					Category:     spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS},
					BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
				},
			},
		}},
	})
	return chainParser
}

var writeInterfaceCases = []writeInterfaceCase{
	{
		name: "jsonrpc", chainID: "ETH1", apiInterface: spectypes.APIInterfaceJsonRPC, parser: eth1WriteParser,
		method: http.MethodPost, body: `{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x01"],"id":1}`,
		success: `{"jsonrpc":"2.0","id":1,"result":"0xabc"}`,
	},
	{
		name: "rest", chainID: "LAVA", apiInterface: spectypes.APIInterfaceRest, parser: lavaRESTWriteParser,
		method: http.MethodPost, path: "/cosmos/tx/v1beta1/txs", body: `{"tx_bytes":"dHg=","mode":"BROADCAST_MODE_SYNC"}`,
		success: `{"tx_response":{"code":0,"txhash":"ABC"}}`,
	},
	{
		name: "graphql", chainID: "SUI", apiInterface: spectypes.APIInterfaceGraphQL, parser: suiGraphQLWriteParser,
		method: http.MethodPost, nodeURLPath: "/graphql",
		body:    `{"query":"mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } }"}`,
		success: `{"data":{"executeTransaction":{"digest":"D1"}}}`,
	},
}

// newTwoPrimaryWriteServer builds a router whose two primaries are each a local upstream answering
// every request with its own reply, after its own delay. It returns how many requests each
// upstream received, in the order the upstreams were given.
//
// Both are primaries on purpose: a stateful request is broadcast to every primary, so both receive
// the write and neither order nor weighting decides who is asked.
func newTwoPrimaryWriteServer(t *testing.T, tc writeInterfaceCase, chainParser chainlib.ChainParser, upstreams [2]writeUpstream) (*[2]int64, *RPCSmartRouterServer) {
	t.Helper()
	ctx := context.Background()
	hits := &[2]int64{}
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: tc.chainID, ApiInterface: tc.apiInterface}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, tc.chainID)
	csm := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())

	primaries := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
	for i, upstreamCfg := range upstreams {
		counter, reply := &hits[i], upstreamCfg
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.ReadAll(r.Body)
			first := atomic.AddInt64(counter, 1) == 1
			if reply.delay > 0 {
				select {
				case <-time.After(reply.delay):
				case <-r.Context().Done():
					return
				}
			}
			status, body := reply.status, reply.body
			if first && reply.firstStatus != 0 {
				status, body = reply.firstStatus, reply.firstBody
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
		t.Cleanup(upstream.Close)
		// The relay path records a 429 on the process-wide registry, keyed on the provider and the
		// url it dialed. Clear it, or this upstream's hold-off outlives the test.
		name, url := upstreamCfg.name, upstream.URL+tc.nodeURLPath
		t.Cleanup(func() { holdoff.Shared.RecordAnswer(name, url) })

		connection, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: url}, 5, "")
		require.NoError(t, err)
		endpoint := &lavasession.Endpoint{NetworkAddress: url, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{connection}}
		provider := lavasession.NewConsumerSessionWithProvider(upstreamCfg.name, []*lavasession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		primaries[uint64(i)] = provider
	}
	require.NoError(t, csm.UpdateAllProviders(1, primaries, nil))

	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	return hits, &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        chainParser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}
}

func sendWrite(t *testing.T, tc writeInterfaceCase, chainParser chainlib.ChainParser, server *RPCSmartRouterServer) (*common.RelayResult, error) {
	t.Helper()
	ctx := context.Background()
	chainMessage, err := chainParser.ParseMsg(tc.path, []byte(tc.body), tc.method, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := lavaprotocol.NewRelayData(ctx, tc.method, tc.path, []byte(tc.body), 0, spectypes.NOT_APPLICABLE, tc.apiInterface,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "dapp", "1.2.3.4")
	require.Equal(t, uint32(common.CONSISTENCY_SELECT_ALL_PROVIDERS), chainlib.GetStateful(protocolMessage),
		"setup: the request must be a write on this interface, or this measures nothing")

	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return server.SendParsedRelay(callCtx, nil, protocolMessage)
}

// TestStatefulBroadcastWaitsThroughARateLimit pins what makes a 429 harmless on a write: the 429 is
// not the answer, so the broadcast keeps waiting for the sibling that is still executing the
// transaction, and returns that sibling's reply. Every interface turns the upstream's 429 into a
// failed attempt rather than a result (relayInnerDirect), and an in-body RESOURCE_EXHAUSTED is a
// node error, so neither can win the broadcast. When every provider answers 429 the client gets 503.
//
// This is the premise of sending a write to a provider that is held off after a 429 (MAG-4388): if
// a still-limited provider's instant 429 could end the broadcast, broadcasting to it would hand
// the client that 429 while the healthy provider lands the transaction.
//
// An in-body RESOURCE_EXHAUSTED from every provider is not pinned here: today the client gets the
// node's own GraphQL error document with HTTP 200 (the first node error is returned with no
// error), which MAG-4374 changes.
func TestStatefulBroadcastWaitsThroughARateLimit(t *testing.T) {
	rand.InitRandomSeed()
	httpRateLimit := writeUpstream{status: http.StatusTooManyRequests, body: `{"code":-32007,"message":"50/second request limit reached"}`}
	for _, tc := range writeInterfaceCases {
		limits := []struct {
			name          string
			reply         writeUpstream
			pinAllLimited bool
		}{{"an HTTP 429", httpRateLimit, true}}
		if tc.apiInterface == spectypes.APIInterfaceGraphQL {
			// Sui's own rate limit arrives inside a 200.
			limits = append(limits, struct {
				name          string
				reply         writeUpstream
				pinAllLimited bool
			}{"RESOURCE_EXHAUSTED inside a 200", writeUpstream{
				status: http.StatusOK,
				body:   `{"data":null,"errors":[{"message":"slow down","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`,
			}, false})
		}
		for _, limit := range limits {
			t.Run(tc.name+"/"+limit.name+" then a success", func(t *testing.T) {
				chainParser := tc.parser(t)
				limited := limit.reply
				limited.name = "limited-" + t.Name()
				healthy := writeUpstream{name: "healthy-" + t.Name(), status: http.StatusOK, body: tc.success, delay: 150 * time.Millisecond}
				hits, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{limited, healthy})

				result, err := sendWrite(t, tc, chainParser, server)
				require.NoError(t, err, "the healthy provider executed the write, so the write succeeded")
				require.NotNil(t, result)
				require.Equal(t, healthy.name, result.ProviderInfo.ProviderAddress,
					"the instant rate limit ended the broadcast: the client got the 429 while the sibling landed the transaction")
				require.Equal(t, http.StatusOK, result.StatusCode)
				require.JSONEq(t, tc.success, string(result.Reply.Data))
				require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]), "the limited provider received the write once")
				require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]), "the healthy provider received the write once")
			})
			if !limit.pinAllLimited {
				continue
			}
			t.Run(tc.name+"/"+limit.name+" from both", func(t *testing.T) {
				chainParser := tc.parser(t)
				first, second := limit.reply, limit.reply
				first.name = "limited-a-" + t.Name()
				second.name = "limited-b-" + t.Name()
				hits, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{first, second})

				result, err := sendWrite(t, tc, chainParser, server)
				require.Error(t, err, "no provider executed the write")
				require.NotNil(t, result)
				require.Equal(t, http.StatusServiceUnavailable, result.StatusCode,
					"every provider refused for rate: the chain is temporarily unservable, and 503 says so")
				require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]))
				require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]))
			})
		}
	}
}

// TestStatefulBroadcastReachesAProviderHeldOffAfterARateLimit is MAG-4388 end to end. A provider
// answers one write with a 429, so the router holds it off. The next write must still be broadcast
// to it: the hold-off steers reads, and taking a provider out of the write broadcast single-homed
// every write for as long as the hold-off lasted (measured on Fuel Sepolia: five writes in a row to
// one provider after one 429).
func TestStatefulBroadcastReachesAProviderHeldOffAfterARateLimit(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range writeInterfaceCases {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			limitedOnce := writeUpstream{
				name: "limited-once-" + t.Name(), status: http.StatusOK, body: tc.success,
				firstStatus: http.StatusTooManyRequests, firstBody: `{"code":-32007,"message":"50/second request limit reached"}`,
			}
			healthy := writeUpstream{name: "healthy-" + t.Name(), status: http.StatusOK, body: tc.success, delay: 50 * time.Millisecond}
			hits, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{limitedOnce, healthy})

			_, err := sendWrite(t, tc, chainParser, server)
			require.NoError(t, err, "setup: the healthy provider executed the first write")
			held := slices.ContainsFunc(server.sessionManager.ProviderRoutingSnapshot().HeldOff, func(p lavasession.HeldOffProviderInfo) bool { return p.Address == limitedOnce.name })
			require.True(t, held, "setup: the 429 must have held the provider off, or this measures nothing")

			_, err = sendWrite(t, tc, chainParser, server)
			require.NoError(t, err)
			require.Equal(t, int64(2), atomic.LoadInt64(&hits[1]), "the healthy provider received both writes")
			require.Equal(t, int64(2), atomic.LoadInt64(&hits[0]),
				"the second write was not broadcast to the held-off provider: the hold-off single-homed it")
		})
	}
}
