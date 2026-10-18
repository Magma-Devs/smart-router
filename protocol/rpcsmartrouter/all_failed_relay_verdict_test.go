package rpcsmartrouter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
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

// verdictRecorder keeps the two request-level verdicts a relay reports: the retry incident's
// success flag and the request's own Success, as the listener hands it to SetRelayMetrics.
type verdictRecorder struct {
	metrics.NoOpConsumerMetrics
	mu       sync.Mutex
	retries  []bool
	requests []bool
}

func (r *verdictRecorder) RecordIncidentRetry(_, _, _ string, _ uint64, success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retries = append(r.retries, success)
}

func (r *verdictRecorder) SetRelayMetrics(relayMetric *metrics.RelayMetrics, _ error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, relayMetric.Success)
}

func (r *verdictRecorder) snapshot() (retries, requests []bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bool(nil), r.retries...), append([]bool(nil), r.requests...)
}

// verdictInterface is one api interface with a read that is retried on a node error and, where
// the case needs one, a write.
type verdictInterface struct {
	name         string
	chainID      string
	apiInterface string
	parser       func(t *testing.T) chainlib.ChainParser
	readPath     string
	readMethod   string
	readBody     string
	writeBody    string
	writePath    string
	// nodeURLPath is appended to the upstream's address: GraphQL posts to the endpoint's one URL.
	nodeURLPath string
}

func verdictETH1Parser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	return chainParser
}

func verdictLAVARESTParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "LAVA", spectypes.APIInterfaceRest, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	return chainParser
}

// verdictSuiGraphQLParser is a Sui-shaped GraphQL spec: one read and the stateful mutation Sui
// submits transactions with.
func verdictSuiGraphQLParser(t *testing.T) chainlib.ChainParser {
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

var (
	verdictJSONRPC = verdictInterface{
		name: "jsonrpc", chainID: "ETH1", apiInterface: spectypes.APIInterfaceJsonRPC, parser: verdictETH1Parser,
		readMethod: http.MethodPost,
		readBody:   `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","latest"],"id":1}`,
		writeBody:  `{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x01"],"id":1}`,
	}
	verdictREST = verdictInterface{
		name: "rest", chainID: "LAVA", apiInterface: spectypes.APIInterfaceRest, parser: verdictLAVARESTParser,
		readMethod: http.MethodGet, readPath: "/cosmos/base/tendermint/v1beta1/blocks/17",
	}
	verdictGraphQL = verdictInterface{
		name: "graphql", chainID: "SUI", apiInterface: spectypes.APIInterfaceGraphQL, parser: verdictSuiGraphQLParser,
		readMethod: http.MethodPost, readBody: `{"query":"{ chainIdentifier }"}`,
		writeBody:   `{"query":"mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } }"}`,
		nodeURLPath: "/graphql",
	}
)

// newVerdictServer builds a router whose two primaries are local upstreams that each answer every
// request with the same status and body. A read that fails on one is retried on the other, and a
// write is broadcast to both.
func newVerdictServer(t *testing.T, iface verdictInterface, chainParser chainlib.ChainParser, recorder *verdictRecorder, status int, body string) (*[2]int64, *RPCSmartRouterServer, *metrics.RPCConsumerLogs) {
	t.Helper()
	ctx := context.Background()
	hits := &[2]int64{}
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: iface.chainID, ApiInterface: iface.apiInterface}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, iface.chainID)
	csm := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())

	primaries := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
	for i := range hits {
		counter := &hits[i]
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.ReadAll(r.Body)
			atomic.AddInt64(counter, 1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
		t.Cleanup(upstream.Close)
		name, url := "upstream-"+string(rune('a'+i))+"-"+t.Name(), upstream.URL+iface.nodeURLPath
		// A 429 holds the provider off in the process-wide registry; clear it with the test.
		t.Cleanup(func() { holdoff.Shared.RecordAnswer(name, url) })

		connection, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: url}, 5, "")
		require.NoError(t, err)
		endpoint := &lavasession.Endpoint{NetworkAddress: url, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{connection}}
		provider := lavasession.NewConsumerSessionWithProvider(name, []*lavasession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		primaries[uint64(i)] = provider
	}
	require.NoError(t, csm.UpdateAllProviders(1, primaries, nil))

	logs, err := metrics.NewRPCConsumerLogs(recorder, nil, nil)
	require.NoError(t, err)
	return hits, &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        chainParser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}, logs
}

// sendVerdictRequest sends one request through SendParsedRelay, then hands its analytics to the
// listener-side metric hook the way every HTTP listener does after replying.
func sendVerdictRequest(t *testing.T, iface verdictInterface, chainParser chainlib.ChainParser, server *RPCSmartRouterServer, logs *metrics.RPCConsumerLogs, write bool) (*common.RelayResult, error) {
	t.Helper()
	ctx := context.Background()
	method, path, body := iface.readMethod, iface.readPath, iface.readBody
	requestedBlock := spectypes.LATEST_BLOCK
	if write {
		method, path, body = http.MethodPost, iface.writePath, iface.writeBody
		requestedBlock = spectypes.NOT_APPLICABLE
	}
	chainMessage, err := chainParser.ParseMsg(path, []byte(body), method, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := lavaprotocol.NewRelayData(ctx, method, path, []byte(body), 0, requestedBlock, iface.apiInterface,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "dapp", "1.2.3.4")
	if write {
		require.Equal(t, uint32(common.CONSISTENCY_SELECT_ALL_PROVIDERS), chainlib.GetStateful(protocolMessage),
			"setup: the request must be a write on this interface, or this measures nothing")
	}

	analytics := metrics.NewRelayAnalytics("dapp", iface.chainID, iface.apiInterface)
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result, err := server.SendParsedRelay(callCtx, analytics, protocolMessage)
	logs.AddMetricForHttp(analytics, err, nil)
	return result, err
}

// TestRelayEveryProviderFailedIsNotASuccess is MAG-4374's verdict half. When no attempt succeeded
// and a node error is what the client receives, the router still hands over the node's own answer
// — but the request did not succeed, so the retry incident must record a failed retry and the
// request a failed one. Both used to read "err == nil", which is true on that path, so a relay the
// customer received as an error was counted as "recovered by retry" and as a customer success.
func TestRelayEveryProviderFailedIsNotASuccess(t *testing.T) {
	rand.InitRandomSeed()
	cases := []struct {
		iface  verdictInterface
		status int
		body   string
	}{
		{verdictJSONRPC, http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`},
		{verdictREST, http.StatusNotFound, `{"code":5,"message":"block not found","details":[]}`},
		{verdictGraphQL, http.StatusOK, `{"data":null,"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.iface.name, func(t *testing.T) {
			chainParser := tc.iface.parser(t)
			recorder := &verdictRecorder{}
			hits, server, logs := newVerdictServer(t, tc.iface, chainParser, recorder, tc.status, tc.body)

			result, err := sendVerdictRequest(t, tc.iface, chainParser, server, logs, false)
			require.Equal(t, [2]int64{1, 1}, loadRefusalHits(hits), "setup: the read must be retried on the other provider")
			require.NoError(t, err, "the client still gets the node's own answer")
			require.NotNil(t, result)
			require.Equal(t, tc.status, result.StatusCode)
			require.JSONEq(t, tc.body, string(result.Reply.Data), "the client still gets the node's own answer")

			retries, requests := recorder.snapshot()
			require.Equal(t, []bool{false}, retries, "every provider failed: the retry did not recover the request")
			require.Equal(t, []bool{false}, requests, "the request ended on a node error: it is not a customer success")
		})
	}
}

// TestRelayEveryProviderAnsweredAnEmpty2xx: a 2xx with nothing in it is not an answer the client
// can read. When it is all the router got, the client must not receive it as a 200 with no body;
// it gets a 502 and an error body.
//
// REST is not a case: a REST 2xx is a success whatever its body (CheckResponseError reads only a
// Cosmos tx_response code in it), so an empty one never reaches the node-error branch.
func TestRelayEveryProviderAnsweredAnEmpty2xx(t *testing.T) {
	rand.InitRandomSeed()
	for _, iface := range []verdictInterface{verdictJSONRPC, verdictGraphQL} {
		for _, write := range []bool{false, true} {
			name := iface.name + "/read"
			if write {
				name = iface.name + "/write"
			}
			t.Run(name, func(t *testing.T) {
				chainParser := iface.parser(t)
				recorder := &verdictRecorder{}
				hits, server, logs := newVerdictServer(t, iface, chainParser, recorder, http.StatusOK, "")

				result, err := sendVerdictRequest(t, iface, chainParser, server, logs, write)
				require.Equal(t, [2]int64{1, 1}, loadRefusalHits(hits), "setup: both providers must be asked")
				require.Error(t, err, "an empty 2xx from every provider was handed to the client as the answer")
				require.NotNil(t, result)
				require.Equal(t, http.StatusBadGateway, result.StatusCode)

				_, requests := recorder.snapshot()
				require.Equal(t, []bool{false}, requests)
			})
		}
	}
}

// TestRelayEveryProviderRateLimitedInBodyIs503: every provider refusing for rate is a chain that is
// temporarily unservable, and the client is told so with a 503 — whether the refusal came as an
// HTTP 429 (already the case) or inside a 200 (Sui's RESOURCE_EXHAUSTED, a JSON-RPC error whose
// code is 429). The in-body shape used to end on the node's error with a 200.
func TestRelayEveryProviderRateLimitedInBodyIs503(t *testing.T) {
	rand.InitRandomSeed()
	cases := []struct {
		iface  verdictInterface
		status int
		body   string
	}{
		{verdictJSONRPC, http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":429,"message":"Too Many Requests"}}`},
		{verdictGraphQL, http.StatusOK, `{"data":null,"errors":[{"message":"slow down","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`},
		// The HTTP shape, unchanged: a 429 is a failed attempt, so it always reached the 503.
		{verdictJSONRPC, http.StatusTooManyRequests, `{"code":-32007,"message":"50/second request limit reached"}`},
	}
	for _, tc := range cases {
		for _, write := range []bool{false, true} {
			name := tc.iface.name + "/" + http.StatusText(tc.status) + "/read"
			if write {
				name = tc.iface.name + "/" + http.StatusText(tc.status) + "/write"
			}
			t.Run(name, func(t *testing.T) {
				chainParser := tc.iface.parser(t)
				recorder := &verdictRecorder{}
				hits, server, logs := newVerdictServer(t, tc.iface, chainParser, recorder, tc.status, tc.body)

				result, err := sendVerdictRequest(t, tc.iface, chainParser, server, logs, write)
				require.Positive(t, atomic.LoadInt64(&hits[0]), "setup: both providers must be asked")
				require.Positive(t, atomic.LoadInt64(&hits[1]), "setup: both providers must be asked")
				require.Error(t, err, "every provider refused for rate")
				require.NotNil(t, result)
				require.Equal(t, http.StatusServiceUnavailable, result.StatusCode,
					"every provider refused for rate: the chain is temporarily unservable, and 503 says so")
				require.NotErrorIs(t, err, errUnknownWriteOutcome, "every provider answered: the write's outcome is known")

				_, requests := recorder.snapshot()
				require.Equal(t, []bool{false}, requests)
			})
		}
	}
}

// TestWriteEveryProviderRejectedReturnsTheRejection: a write every node rejects (a bad signature)
// ends on that rejection, which the client receives as the node wrote it — that does not change.
// It is a failed request all the same.
func TestWriteEveryProviderRejectedReturnsTheRejection(t *testing.T) {
	rand.InitRandomSeed()
	cases := []struct {
		iface  verdictInterface
		status int
		body   string
	}{
		{verdictGraphQL, http.StatusOK, `{"data":null,"errors":[{"message":"Invalid user signature"}]}`},
		{verdictJSONRPC, http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"invalid sender"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.iface.name, func(t *testing.T) {
			chainParser := tc.iface.parser(t)
			recorder := &verdictRecorder{}
			hits, server, logs := newVerdictServer(t, tc.iface, chainParser, recorder, tc.status, tc.body)

			result, err := sendVerdictRequest(t, tc.iface, chainParser, server, logs, true)
			require.Equal(t, [2]int64{1, 1}, loadRefusalHits(hits), "a write is broadcast to both providers")
			require.NoError(t, err, "the client gets the node's rejection")
			require.NotNil(t, result)
			require.Equal(t, tc.status, result.StatusCode)
			require.JSONEq(t, tc.body, string(result.Reply.Data))

			_, requests := recorder.snapshot()
			require.Equal(t, []bool{false}, requests, "a rejected write is not a customer success")
		})
	}
}

// TestRelaySuccessIsStillASuccess is the control: a served request keeps both verdicts.
func TestRelaySuccessIsStillASuccess(t *testing.T) {
	rand.InitRandomSeed()
	chainParser := verdictJSONRPC.parser(t)
	recorder := &verdictRecorder{}
	_, server, logs := newVerdictServer(t, verdictJSONRPC, chainParser, recorder, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`)

	result, err := sendVerdictRequest(t, verdictJSONRPC, chainParser, server, logs, false)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, result.StatusCode)
	_, requests := recorder.snapshot()
	require.Equal(t, []bool{true}, requests)
}
