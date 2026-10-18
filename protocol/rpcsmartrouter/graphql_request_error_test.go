package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// graphQLRequestErrorHealth reports the router healthy, so the listener serves every request.
type graphQLRequestErrorHealth struct{}

func (graphQLRequestErrorHealth) IsHealthy() bool { return true }

// graphQLRequestMetricsRecorder keeps the method each request was recorded under in the
// per-request metrics, the family that counts customer failures.
type graphQLRequestMetricsRecorder struct {
	metrics.NoOpConsumerMetrics
	mu       sync.Mutex
	recorded []string
}

func (r *graphQLRequestMetricsRecorder) SetRelayMetrics(relayMetric *metrics.RelayMetrics, _ error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recorded = append(r.recorded, relayMetric.ApiMethod)
}

func (r *graphQLRequestMetricsRecorder) methods() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.recorded...)
}

// graphQLRequestErrorParser is a Sui-shaped GraphQL spec: two reads and the mutation Sui submits
// transactions with.
func graphQLRequestErrorParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	chainParser, err := chainlib.NewGraphQLChainParser()
	require.NoError(t, err)
	emptyBlockParsing := spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY}
	chainParser.SetSpec(spectypes.Spec{
		Index:            "SUI",
		Enabled:          true,
		AverageBlockTime: 400,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGraphQL, Type: http.MethodPost},
			Apis: []*spectypes.Api{
				{Name: "chainIdentifier", Enabled: true, ComputeUnits: 10, BlockParsing: emptyBlockParsing},
				{Name: "checkpoint", Enabled: true, ComputeUnits: 20, BlockParsing: emptyBlockParsing},
				{
					Name:         "executeTransaction",
					Enabled:      true,
					ComputeUnits: 100,
					Category:     spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS},
					BlockParsing: emptyBlockParsing,
				},
			},
		}},
	})
	return chainParser
}

// startGraphQLRequestErrorRouter serves a GraphQL listener in front of the router's own relay
// path (SendRelay → ParseRelay → ParseMsg → SendParsedRelay) and one upstream. It returns the
// listener's base URL, how many requests the upstream received, and the per-request metrics.
func startGraphQLRequestErrorRouter(t *testing.T) (string, *int64, *graphQLRequestMetricsRecorder) {
	t.Helper()
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	hits := new(int64)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		atomic.AddInt64(hits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"chainIdentifier":"4btiuiMPvEENsttpZC7CZ53DruC3MAgfznDbASZ7DR6S"}}`))
	}))
	t.Cleanup(upstream.Close)

	chainParser := graphQLRequestErrorParser(t)
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGraphQL}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "SUI")
	csm := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())
	connection, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL + "/graphql"}, 5, "")
	require.NoError(t, err)
	endpoint := &lavasession.Endpoint{NetworkAddress: upstream.URL + "/graphql", Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{connection}}
	provider := lavasession.NewConsumerSessionWithProvider("sui-upstream", []*lavasession.Endpoint{endpoint}, 100000, 1, 1)
	provider.StaticProvider = true
	require.NoError(t, csm.UpdateAllProviders(1, map[uint64]*lavasession.ConsumerSessionsWithProvider{0: provider}, nil))

	recorder := &graphQLRequestMetricsRecorder{}
	logs, err := metrics.NewRPCConsumerLogs(recorder, nil, nil)
	require.NoError(t, err)
	server := &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        chainParser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}

	listener := chainlib.NewGraphQLChainListener(ctx, &lavasession.RPCEndpoint{
		NetworkAddress:  "127.0.0.1:0",
		ChainID:         "SUI",
		ApiInterface:    spectypes.APIInterfaceGraphQL,
		HealthCheckPath: common.DEFAULT_HEALTH_PATH,
	}, server, graphQLRequestErrorHealth{}, logs)
	go listener.Serve(ctx, common.ConsumerCmdFlags{})
	addr := ""
	for deadline := time.Now().Add(3 * time.Second); addr == "" && time.Now().Before(deadline); {
		if addr = listener.GetListeningAddress(); addr == "" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	require.NotEmpty(t, addr, "listener never reported a listening address")
	return "http://" + addr, hits, recorder
}

// graphQLErrorReply is the body a GraphQL server answers a failed request with.
type graphQLErrorReply struct {
	Data   json.RawMessage `json:"data"`
	Errors []struct {
		Message   string `json:"message"`
		Locations []struct {
			Line   int `json:"line"`
			Column int `json:"column"`
		} `json:"locations"`
		Extensions map[string]any `json:"extensions"`
	} `json:"errors"`
}

// TestGraphQLRequestErrorsAreAnsweredTheWayANodeAnswersThem (MAG-4351): a request whose GraphQL
// cannot be read never reaches an upstream, because the router has to parse the document to know
// which api it is. The router used to answer it with HTTP 500 and its own {"error":…} body, so a
// GraphQL client (the Sui TypeScript SDK reads errors[]) saw a transport failure instead of the
// mistake and where it is, and a client retrying 5xx resent a request that can never succeed.
//
// Each case is answered the way a Sui GraphQL node answers it, probed on graphql.mainnet.sui.io and
// mainnet.sui.rpcpool.com on 2026-10-18: a document the server cannot read or run is a GraphQL error
// inside a 200, and a body that is not a GraphQL request at all is a 400. The body is a GraphQL
// response either way, the request reaches no upstream and is not retried.
func TestGraphQLRequestErrorsAreAnsweredTheWayANodeAnswersThem(t *testing.T) {
	baseURL, upstreamHits, recorder := startGraphQLRequestErrorRouter(t)

	post := func(t *testing.T, contentType, body string) *http.Response {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, baseURL+"/graphql", strings.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", contentType)
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		return response
	}

	// The harness relays: a well-formed query reaches the upstream and comes back as it answered.
	response := post(t, "application/json", `{"query":"{ chainIdentifier }"}`)
	controlBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode, string(controlBody))
	require.JSONEq(t, `{"data":{"chainIdentifier":"4btiuiMPvEENsttpZC7CZ53DruC3MAgfznDbASZ7DR6S"}}`, string(controlBody))
	require.Equal(t, int64(1), atomic.LoadInt64(upstreamHits))
	require.Equal(t, []string{"chainIdentifier"}, recorder.methods(), "a relayed request is recorded under its method")

	type location struct{ line, column int }
	cases := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
		// wantCode is errors[0].extensions.code; "" means the error carries no extensions, as
		// Sui's own operation-selection and subscription errors do.
		wantCode    string
		wantMessage string
		wantAt      *location
	}{
		// What the server cannot read as a GraphQL document: Sui answers GRAPHQL_PARSE_FAILED, with
		// the position of the first token it could not read, inside a 200.
		{name: "a syntax error", body: `{"query":"{ checkpoint { sequenceNumber }"}`, wantStatus: http.StatusOK, wantCode: "GRAPHQL_PARSE_FAILED", wantMessage: "Expected", wantAt: &location{1, 32}},
		{name: "not graphql at all", body: `{"query":"not graphql at all"}`, wantStatus: http.StatusOK, wantCode: "GRAPHQL_PARSE_FAILED", wantMessage: `Unexpected Name "not"`, wantAt: &location{1, 1}},
		{name: "an empty query", body: `{"query":""}`, wantStatus: http.StatusOK, wantCode: "GRAPHQL_PARSE_FAILED", wantMessage: "no GraphQL document", wantAt: &location{1, 1}},
		{name: "a request with no query member", body: `{"variables":{}}`, wantStatus: http.StatusOK, wantCode: "GRAPHQL_PARSE_FAILED", wantMessage: "no GraphQL document", wantAt: &location{1, 1}},
		// A document that reads but cannot run as sent: Sui's own words, no code.
		{name: "two operations and no operationName", body: `{"query":"query A { chainIdentifier } query B { chainIdentifier }"}`, wantStatus: http.StatusOK, wantMessage: "Operation name required in request."},
		{name: "an unknown operationName", body: `{"query":"query A { chainIdentifier }","operationName":"Z"}`, wantStatus: http.StatusOK, wantMessage: `Unknown operation named "Z"`},
		{name: "a subscription", body: `{"query":"subscription { checkpoints { cursor } }"}`, wantStatus: http.StatusOK, wantMessage: "Subscriptions are not supported on this transport."},
		// Spreads that cannot be resolved fail validation, at the spread.
		{name: "an undefined fragment", body: `{"query":"{ ...Missing }"}`, wantStatus: http.StatusOK, wantCode: "GRAPHQL_VALIDATION_FAILED", wantMessage: `Unknown fragment "Missing"`, wantAt: &location{1, 3}},
		{name: "a cyclic fragment", body: `{"query":"{ ...A } fragment A on Query { ...B } fragment B on Query { ...A }"}`, wantStatus: http.StatusOK, wantCode: "GRAPHQL_VALIDATION_FAILED", wantMessage: "within itself"},
		// A body that is not a GraphQL-over-HTTP request: Sui answers 400.
		{name: "invalid JSON", body: `{"query": `, wantStatus: http.StatusBadRequest, wantCode: "BAD_REQUEST", wantMessage: "not a JSON GraphQL request"},
		{name: "an empty body", body: ``, wantStatus: http.StatusBadRequest, wantCode: "BAD_REQUEST", wantMessage: "not a JSON GraphQL request"},
		{name: "a batch array", body: `[{"query":"{ chainIdentifier }"}]`, wantStatus: http.StatusBadRequest, wantCode: "BAD_REQUEST", wantMessage: "batched GraphQL requests are not supported"},
		{name: "a raw document sent as application/graphql", contentType: "application/graphql", body: `{ chainIdentifier }`, wantStatus: http.StatusBadRequest, wantCode: "BAD_REQUEST", wantMessage: "not a JSON GraphQL request"},
		{name: "variables that are not an object", body: `{"query":"{ chainIdentifier }","variables":"x"}`, wantStatus: http.StatusBadRequest, wantCode: "BAD_REQUEST", wantMessage: "not a JSON GraphQL request"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			contentType := tc.contentType
			if contentType == "" {
				contentType = "application/json"
			}
			before := atomic.LoadInt64(upstreamHits)
			response := post(t, contentType, tc.body)
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())

			require.Equal(t, tc.wantStatus, response.StatusCode, "body: %s", body)
			require.Contains(t, response.Header.Get("Content-Type"), "application/json")
			require.Empty(t, response.Header.Get(common.RETRY_COUNT_HEADER_NAME), "a request that cannot be read is not retried")
			require.NotEmpty(t, response.Header.Get(common.GUID_HEADER_NAME), "the reply names its request, as a relayed one does")
			require.Equal(t, before, atomic.LoadInt64(upstreamHits), "a request the router cannot read must not reach an upstream")

			var reply graphQLErrorReply
			require.NoError(t, json.Unmarshal(body, &reply), "not a GraphQL response: %s", body)
			require.JSONEq(t, "null", string(reply.Data), "body: %s", body)
			require.Len(t, reply.Errors, 1, "body: %s", body)
			require.Contains(t, reply.Errors[0].Message, tc.wantMessage)
			if tc.wantCode == "" {
				require.Nil(t, reply.Errors[0].Extensions, "body: %s", body)
			} else {
				require.Equal(t, tc.wantCode, reply.Errors[0].Extensions["code"], "body: %s", body)
			}
			if tc.wantAt != nil {
				require.Len(t, reply.Errors[0].Locations, 1, "body: %s", body)
				require.Equal(t, tc.wantAt.line, reply.Errors[0].Locations[0].Line)
				require.Equal(t, tc.wantAt.column, reply.Errors[0].Locations[0].Column)
			}
		})
	}

	// The per-request family counts a customer failure only for a request recorded under a method
	// (TestSetRelayMetrics_SkipsWhatIsNotARequest). A request the router cannot read never gets one,
	// so none of the cases above is a failure there.
	recorded := recorder.methods()
	require.Len(t, recorded, 1+len(cases))
	for _, method := range recorded[1:] {
		require.Empty(t, method, "a request the router cannot read was recorded under a method")
	}
}
