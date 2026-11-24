package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relayprotocol"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-4481 end to end: a request goes through a real listener, the real router and the real
// direct sender to an httptest upstream, and the test asserts what the client reads. The router is
// a proxy: when the upstream answered, its status and body come back as is on every interface;
// when it did not, the router answers 502, or 504 for a timeout, in the listener's own error shape.

// failedReplyInterface is one listener under test and a request it serves.
type failedReplyInterface struct {
	name         string
	chainID      string
	apiInterface string
	parser       func(t *testing.T) chainlib.ChainParser
	method       string
	path         string
	body         string
	// jsonRpcShape: the router's own error is a JSON-RPC error object; otherwise {"error": "..."}.
	jsonRpcShape bool
	// graphQLShape: the router's own error is a GraphQL error with the code NO_PROVIDER_ANSWER, its
	// guid equal to Router-Guid, and a sentence that names no provider (MAG-5119).
	graphQLShape bool
	// headers are request headers sent on top of the ones every request here sends.
	headers map[string]string
	// upstreamTimeout is the node-url timeout of every upstream, which lengthens each attempt's
	// own budget past the request's (NodeUrl.LowerContextTimeoutWithDuration).
	upstreamTimeout time.Duration
	// staleUpstreams: every upstream reports a head far behind the chain tip, so the consistency
	// gate turns it away.
	staleUpstreams bool
}

func failedReplyInterfaces() []failedReplyInterface {
	mockParser := func(chainID, apiInterface string) func(t *testing.T) chainlib.ChainParser {
		return func(t *testing.T) chainlib.ChainParser {
			t.Helper()
			parser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), chainID, apiInterface,
				http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }), nil, "../../", nil)
			require.NoError(t, err)
			if closeServer != nil {
				t.Cleanup(closeServer)
			}
			return parser
		}
	}
	return []failedReplyInterface{
		{
			name: "jsonrpc", chainID: "ETH1", apiInterface: spectypes.APIInterfaceJsonRPC, parser: mockParser("ETH1", spectypes.APIInterfaceJsonRPC),
			method: http.MethodPost, path: "/", body: `{"jsonrpc":"2.0","id":77,"method":"eth_blockNumber","params":[]}`, jsonRpcShape: true,
		},
		{
			name: "rest", chainID: "COSMOSHUB", apiInterface: spectypes.APIInterfaceRest, parser: mockParser("COSMOSHUB", spectypes.APIInterfaceRest),
			method: http.MethodGet, path: "/cosmos/base/tendermint/v1beta1/blocks/17",
		},
		{
			name: "tendermintrpc", chainID: "COSMOSHUB", apiInterface: spectypes.APIInterfaceTendermintRPC, parser: mockParser("COSMOSHUB", spectypes.APIInterfaceTendermintRPC),
			method: http.MethodPost, path: "/", body: `{"jsonrpc":"2.0","id":77,"method":"status","params":[]}`, jsonRpcShape: true,
		},
		{
			name: "graphql", chainID: "SUI", apiInterface: spectypes.APIInterfaceGraphQL, parser: newGatewayTestGraphQLParser,
			method: http.MethodPost, path: "/", body: gatewayTestQuery, graphQLShape: true,
		},
	}
}

// failedReplyClientRead is what the client read.
type failedReplyClientRead struct {
	status      int
	body        string
	contentType string
	guid        string
	nodeError   string // router-identified-node-error
	header      http.Header
}

// failedReplyRelayTimeout is the attempt window every request here asks for in router-relay-timeout.
const failedReplyRelayTimeout = 300 * time.Millisecond

// startFailedReplyRouter serves iface through a real listener and router whose providers are
// upstreamURLs, named upstream-1, upstream-2, … in that order, and returns a function that sends
// the interface's request and reads the reply.
func startFailedReplyRouter(t *testing.T, iface failedReplyInterface, upstreamURLs ...string) func() failedReplyClientRead {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	parser := iface.parser(t)
	providers := map[uint64]*routersession.ConsumerSessionsWithProvider{}
	for i, upstreamURL := range upstreamURLs {
		connection, err := routersession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstreamURL, Timeout: iface.upstreamTimeout}, 5, iface.apiInterface)
		require.NoError(t, err)
		t.Cleanup(func() { _ = connection.Close() })
		provider := routersession.NewConsumerSessionWithProvider(fmt.Sprintf("upstream-%d", i+1), []*routersession.Endpoint{{
			NetworkAddress: upstreamURL, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection},
		}}, 100000, 1, 1)
		provider.StaticProvider = true
		providers[uint64(i)] = provider
	}
	sessionManager, rpcEndpoint := createTestSessionManager(iface.chainID, iface.apiInterface)
	rpcEndpoint.NetworkAddress = "127.0.0.1:0"
	rpcEndpoint.HealthCheckPath = common.DEFAULT_HEALTH_PATH
	require.NoError(t, sessionManager.UpdateAllProviders(1, providers, nil))
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	server := &RPCSmartRouterServer{
		chainParser: parser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
		rpcSmartRouterLogs: logs,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
	}
	if iface.staleUpstreams {
		server.chainState = seedChainTip(200)
		for _, upstreamURL := range upstreamURLs {
			seedEndpointTip(iface.chainID, iface.apiInterface, upstreamURL, 50)
			key := endpointtip.Key(iface.chainID, iface.apiInterface, upstreamURL)
			t.Cleanup(func() { endpointtip.Default().Remove(key) })
		}
	}
	listener, err := chainlib.NewChainListener(ctx, rpcEndpoint, server, nil, logs, parser, nil, NewNoOpWSSubscriptionManager(iface.chainID, iface.apiInterface))
	require.NoError(t, err)
	go listener.Serve(ctx, common.ConsumerCmdFlags{})
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = listener.Shutdown(shutdownCtx)
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, time.Millisecond)
	address := listener.GetListeningAddress()

	return func() failedReplyClientRead {
		var body io.Reader
		if iface.body != "" {
			body = bytes.NewBufferString(iface.body)
		}
		request, err := http.NewRequestWithContext(ctx, iface.method, "http://"+address+iface.path, body)
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(common.RELAY_TIMEOUT_HEADER_NAME, failedReplyRelayTimeout.String())
		for name, value := range iface.headers {
			request.Header.Set(name, value)
		}
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		read, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		return failedReplyClientRead{
			status:      response.StatusCode,
			body:        string(read),
			contentType: response.Header.Get("Content-Type"),
			guid:        response.Header.Get(common.GUID_HEADER_NAME),
			nodeError:   response.Header.Get(common.IDENTIFIED_NODE_ERROR_HEADER),
			header:      response.Header,
		}
	}
}

// answeringUpstream answers every request with status, body and Content-Type.
func answeringUpstream(t *testing.T, status int, body, contentType string) string {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(upstream.Close)
	return upstream.URL
}

// refusingUpstream is an address nothing listens on.
func refusingUpstream(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return "http://" + address
}

// hangingUpstream accepts every request and never answers. arrivals, when not nil, counts the
// requests it received.
func hangingUpstream(t *testing.T, arrivals *atomic.Int32) string {
	t.Helper()
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if arrivals != nil {
			arrivals.Add(1)
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); upstream.Close() })
	return upstream.URL
}

// routerErrorMessage reads the sentence from the router's own error body in the listener's shape.
func routerErrorMessage(t *testing.T, iface failedReplyInterface, body string) string {
	t.Helper()
	if iface.graphQLShape {
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(body), &fields), body)
		require.NotContains(t, fields, "data", "the router's own GraphQL reply has no data key")
		var reply struct {
			Errors []struct {
				Message    string            `json:"message"`
				Extensions map[string]string `json:"extensions"`
			} `json:"errors"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &reply), body)
		require.Len(t, reply.Errors, 1, body)
		require.Equal(t, "NO_PROVIDER_ANSWER", reply.Errors[0].Extensions["code"], body)
		require.NotEmpty(t, reply.Errors[0].Extensions["guid"], body)
		return reply.Errors[0].Message
	}
	if iface.jsonRpcShape {
		var envelope struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Error   struct {
				Code    int            `json:"code"`
				Message string         `json:"message"`
				Data    map[string]any `json:"data"`
			} `json:"error"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &envelope), body)
		require.Equal(t, "2.0", envelope.JSONRPC)
		require.JSONEq(t, `77`, string(envelope.ID), "the caller's id")
		require.Equal(t, -32000, envelope.Error.Code)
		require.NotContains(t, envelope.Error.Data, "router_detail")
		return envelope.Error.Message
	}
	var restError struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &restError), body)
	return restError.Error
}

func TestFailedRelayReply_EndToEnd_MAG4481(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	originalTimeout := common.DefaultTimeout
	common.DefaultTimeout = 300 * time.Millisecond
	t.Cleanup(func() { common.DefaultTimeout = originalTimeout })

	for _, iface := range failedReplyInterfaces() {
		t.Run(iface.name, func(t *testing.T) {
			t.Run("node answer goes back as is", func(t *testing.T) {
				const page = "<html><body>503 Service Temporarily Unavailable</body></html>"
				read := startFailedReplyRouter(t, iface, answeringUpstream(t, http.StatusServiceUnavailable, page, "text/html"))()
				require.Equal(t, http.StatusServiceUnavailable, read.status)
				require.Equal(t, page, read.body, "no envelope, no added fields")
				require.Equal(t, "text/html", read.contentType, "labelled as the upstream labelled it")
				require.NotEmpty(t, read.guid, "the GUID stays in the reply header")
			})

			t.Run("empty body goes back empty", func(t *testing.T) {
				read := startFailedReplyRouter(t, iface, answeringUpstream(t, http.StatusServiceUnavailable, "", ""))()
				require.Equal(t, http.StatusServiceUnavailable, read.status)
				require.Empty(t, read.body)
			})

			t.Run("cloudflare 520 goes back as is", func(t *testing.T) {
				read := startFailedReplyRouter(t, iface, answeringUpstream(t, 520, "error code: 520", "text/plain; charset=UTF-8"))()
				require.Equal(t, 520, read.status)
				require.Equal(t, "error code: 520", read.body)
			})

			t.Run("429 goes back as 429", func(t *testing.T) {
				const quota = `{"code":429,"message":"daily quota exceeded"}`
				read := startFailedReplyRouter(t, iface, answeringUpstream(t, http.StatusTooManyRequests, quota, "application/json"))()
				require.Equal(t, http.StatusTooManyRequests, read.status, "not a router-made 503")
				require.Equal(t, quota, read.body)
				require.Empty(t, read.nodeError, "the upstream refused for rate; no node answered (MAG-3030)")
			})

			// The shape a vendor's rate limiter answers with, and the simulator since MAG-3030 was
			// filed: an HTTP 429 whose body is prose, which the JSON-RPC body check reads as a
			// malformed node reply. The relay still fails on the status, and the client was told a
			// node had answered with an error (MAG-3030).
			t.Run("429 with a prose body is not a node error", func(t *testing.T) {
				const refusal = "Too Many Requests"
				read := startFailedReplyRouter(t, iface, answeringUpstream(t, http.StatusTooManyRequests, refusal, "text/plain; charset=utf-8"))()
				require.Equal(t, http.StatusTooManyRequests, read.status)
				require.Equal(t, refusal, read.body)
				require.Empty(t, read.nodeError, "the upstream refused for rate; no node answered")
			})

			t.Run("refused connection is the router's 502", func(t *testing.T) {
				read := startFailedReplyRouter(t, iface, refusingUpstream(t))()
				require.Equal(t, http.StatusBadGateway, read.status)
				message := routerErrorMessage(t, iface, read.body)
				if iface.graphQLShape {
					// The router's own sentence names no provider, address or transport error: the
					// cause stays in its log line (MAG-5119).
					require.Equal(t, "no provider answered the request", message)
					require.NotContains(t, read.body, "connection refused")
					require.NotContains(t, read.body, "127.0.0.1")
					require.Contains(t, read.body, `"guid":"`+read.guid+`"`, "Router-Guid equals extensions.guid")
					return
				}
				require.True(t, strings.HasPrefix(message, "no answer from upstream: "), message)
				require.Contains(t, message, "connection refused")
			})

			t.Run("hung upstream is the router's 504", func(t *testing.T) {
				read := startFailedReplyRouter(t, iface, hangingUpstream(t, nil))()
				require.Equal(t, http.StatusGatewayTimeout, read.status, read.body)
				message := routerErrorMessage(t, iface, read.body)
				if iface.graphQLShape {
					require.True(t, strings.HasPrefix(message, "no provider answered within "), message)
					require.Contains(t, read.body, `"guid":"`+read.guid+`"`, "Router-Guid equals extensions.guid")
				} else {
					require.True(t, strings.HasPrefix(message, "no answer from upstream within "), message)
				}
				require.NotContains(t, message, "context deadline exceeded")
				// MAG-3980: the router's own 504 carries the request's headers, as a reply an
				// upstream produced does: the GUID, and the upstream it asked. One attempt went out,
				// so there is no retry to report.
				require.NotEmpty(t, read.guid, "the router's 504 carries no Router-Guid")
				require.Equal(t, "upstream-1", read.header.Get(common.PROVIDER_ADDRESS_HEADER_NAME))
				require.Empty(t, read.header.Get(common.RETRY_COUNT_HEADER_NAME))
			})

			t.Run("masked mode", func(t *testing.T) {
				previous := metrics.ReturnMaskedErrors
				metrics.ReturnMaskedErrors = "true"
				t.Cleanup(func() { metrics.ReturnMaskedErrors = previous })

				// The node's answer is not the router's text: masked mode leaves it alone.
				read := startFailedReplyRouter(t, iface, answeringUpstream(t, http.StatusServiceUnavailable, "busy", "text/plain"))()
				require.Equal(t, http.StatusServiceUnavailable, read.status)
				require.Equal(t, "busy", read.body)

				// The router's own no-answer reply is masked in status and body.
				read = startFailedReplyRouter(t, iface, refusingUpstream(t))()
				require.Equal(t, http.StatusInternalServerError, read.status)
				require.NotContains(t, read.body, "connection refused")
				require.NotContains(t, read.body, "no answer from upstream")
				require.NotEmpty(t, read.guid)
				require.Contains(t, read.body, read.guid, "the masked reply names only the GUID")
			})
		})
	}
}

// MAG-3980: a request whose budget runs out while every upstream is quiet is answered by the
// router itself, and that reply carries the request's headers like any other failed reply. It used
// to return before they were built, so the client had no Router-Guid to quote for the failure that
// is hardest to diagnose, and no Router-Retries or Router-Provider-Address to say what was tried.
func TestFailedRelayReply_BudgetExpiryKeepsTheRequestHeaders_MAG3980(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	// Two windows: the ticker fires once inside the budget and hedges to the other upstream, with a
	// window to spare. Its next tick finds both upstreams busy and sends nothing.
	originalTimeout := common.DefaultTimeout
	common.DefaultTimeout = 2 * failedReplyRelayTimeout
	t.Cleanup(func() { common.DefaultTimeout = originalTimeout })

	jsonRPC := failedReplyInterfaces()[0]
	require.Equal(t, "jsonrpc", jsonRPC.name)

	var arrivals atomic.Int32
	read := startFailedReplyRouter(t, jsonRPC, hangingUpstream(t, &arrivals), hangingUpstream(t, &arrivals))()
	require.Equal(t, http.StatusGatewayTimeout, read.status, read.body)
	require.EqualValues(t, 2, arrivals.Load(), "setup: the ticker must have hedged to the other upstream, or this measures a single attempt")

	require.NotEmpty(t, read.guid, "the reply carries no Router-Guid")
	require.Contains(t, read.body, read.guid, "the header names the request the body names")
	require.Equal(t, "1", read.header.Get(common.RETRY_COUNT_HEADER_NAME), "two attempts went out")
	require.ElementsMatch(t, []string{"upstream-1", "upstream-2"},
		strings.Split(read.header.Get(common.PROVIDER_ADDRESS_HEADER_NAME), ","),
		"both upstreams were asked; nothing answered, so nothing follows them")
	require.Equal(t, "true", read.header.Get(common.HEDGE_TRIGGERED_HEADER))
}

// MAG-5076: a request the router ends before asking any provider, a pin on a provider it cannot use
// or a router with no provider at all, still carries the request's headers: Router-Guid, the guid the
// body names, router-user-request-type, and Router-Relay-Timeout-Applied for the router-relay-timeout
// every request here sends. It used to carry none, so the guid was only in the body. No attempt went
// out, so it names no provider and no retry. The status and the body are unchanged.
func TestFailedRelayReply_ARequestRefusedBeforeDispatchKeepsTheRequestHeaders_MAG5076(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	// upstream-9 is in no pool. The router keeps no list of configured names here, so the pin is
	// not the caller's mistake (MAG-3800): it is the provider-unavailable 500.
	cases := []struct {
		name      string
		headers   map[string]string
		upstreams int
	}{
		{"pin", map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "upstream-9"}, 1},
		{"strict pin", map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "upstream-9", common.SELECT_PROVIDER_STRICT_HEADER_NAME: "true"}, 1},
		{"no provider", nil, 0},
	}
	for _, iface := range failedReplyInterfaces() {
		for _, tc := range cases {
			t.Run(iface.name+"/"+tc.name, func(t *testing.T) {
				iface.headers = tc.headers
				var arrivals atomic.Int32
				upstreams := make([]string, tc.upstreams)
				for i := range upstreams {
					upstreams[i] = hangingUpstream(t, &arrivals)
				}
				read := startFailedReplyRouter(t, iface, upstreams...)()
				require.Equal(t, http.StatusInternalServerError, read.status, read.body)
				require.Zero(t, arrivals.Load(), "setup: the request must end before any provider is asked")

				require.NotEmpty(t, read.guid, "the reply carries no Router-Guid")
				require.Contains(t, read.body, read.guid, "the header names the request the body names")
				require.NotEmpty(t, read.header.Get(common.USER_REQUEST_TYPE), "the reply carries no router-user-request-type")
				require.Equal(t, failedReplyRelayTimeout.String(), read.header.Get(common.RELAY_TIMEOUT_APPLIED_HEADER_NAME))
				require.Empty(t, read.header.Get(common.PROVIDER_ADDRESS_HEADER_NAME), "no provider was asked")
				require.Empty(t, read.header.Get(common.RETRY_COUNT_HEADER_NAME), "no attempt went out")
			})
		}
	}
}

// MAG-5076: a strict pin on an upstream too far behind the chain tip is turned away by the
// consistency gate, after selection picked it and before anything is sent. The refusal carries the
// request's headers as the other refusals do. A write had the upstream on record as a target, but
// the upstream never received it, so router-fast-tx-participants must not name it.
func TestFailedRelayReply_AStrictPinTheConsistencyGateTurnsAwayKeepsTheRequestHeaders_MAG5076(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	iface := failedReplyInterfaces()[0]
	require.Equal(t, "jsonrpc", iface.name)
	iface.staleUpstreams = true
	iface.headers = map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "upstream-1", common.SELECT_PROVIDER_STRICT_HEADER_NAME: "true"}
	for _, tc := range []struct {
		name   string
		body   string
		method string
		write  bool
	}{
		{"read", iface.body, "eth_blockNumber", false},
		{"write", `{"jsonrpc":"2.0","id":77,"method":"eth_sendRawTransaction","params":["0xf86c"]}`, "eth_sendRawTransaction", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			iface.body = tc.body
			var arrivals atomic.Int32
			read := startFailedReplyRouter(t, iface, hangingUpstream(t, &arrivals))()
			if tc.write {
				// A write sent nowhere gets the router's not-sent reply, HTTP 200 (MAG-5232).
				require.Equal(t, http.StatusOK, read.status, read.body)
				require.Contains(t, read.body, `"router_error":"PROVIDER_UNAVAILABLE"`, "setup: the refused pin must be what ended the write")
				require.Equal(t, writeOutcomeNotSent, read.header.Get(common.WRITE_OUTCOME_HEADER_NAME))
			} else {
				require.Equal(t, http.StatusInternalServerError, read.status, read.body)
				require.Contains(t, read.body, "too far behind the chain tip", "setup: the consistency gate must be what refused the pin")
			}
			require.Zero(t, arrivals.Load(), "setup: the pinned upstream must not be asked")

			require.NotEmpty(t, read.guid, "the reply carries no Router-Guid")
			require.Contains(t, read.body, read.guid)
			require.Equal(t, tc.method, read.header.Get(common.USER_REQUEST_TYPE))
			require.Empty(t, read.header.Values(common.PROVIDER_ADDRESS_HEADER_NAME), "no provider was asked")
			require.Empty(t, read.header.Values(common.RETRY_COUNT_HEADER_NAME), "no attempt went out")
			require.Empty(t, read.header.Values(common.STATEFUL_ALL_PROVIDERS_HEADER_NAME), "no node received the request")
			if tc.write {
				require.Equal(t, "true", read.header.Get(common.STATEFUL_API_HEADER), "setup: the request must be a write")
			}
		})
	}
}

// MAG-5076 leaves a cross-validated request out. One that ends before any attempt goes out ran no
// round, so its reply carries no cross-validation verdict and the router writes no compliance record
// for it. The header builder would have filed it as a failed round.
func TestSendParsedRelay_ACrossValidatedRequestThatEndsBeforeDispatchHasNoVerdict_MAG5076(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	records := observeCrossValidationRecords(t)
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC,
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }), nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)

	for _, tc := range []struct {
		name    string
		callCtx func() (context.Context, context.CancelFunc)
		err     error
	}{
		{"the caller left", func() (context.Context, context.CancelFunc) {
			callCtx, cancel := context.WithCancel(ctx)
			cancel()
			return callCtx, cancel
		}, context.Canceled},
		{"the deadline passed", func() (context.Context, context.CancelFunc) {
			return context.WithDeadline(ctx, time.Now().Add(-time.Second))
		}, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hits, server := newPinOverrideServer(t, chainParser, logs, false)
			body := []byte(`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","latest"],"id":1}`)
			chainMessage, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			relayData := relayprotocol.NewRelayData(ctx, http.MethodPost, "", body, 0, spectypes.LATEST_BLOCK, spectypes.APIInterfaceJsonRPC,
				chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
			protocolMessage := chainlib.NewProtocolMessage(chainMessage, map[string]string{
				common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
				common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
			}, relayData, "dapp", "1.2.3.4")

			callCtx, cancel := tc.callCtx()
			defer cancel()
			result, err := server.SendParsedRelay(callCtx, nil, protocolMessage)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, []int64{0, 0, 0}, loadPinOverrideHits(hits), "setup: no upstream may be asked")
			for _, metadata := range result.GetReply().GetMetadata() {
				require.NotEqual(t, common.CROSS_VALIDATION_STATUS_HEADER_NAME, metadata.Name, "a round that never ran has no verdict")
			}
			require.Empty(t, records(), "a round that never ran has no compliance record")
		})
	}
}

// MAG-3980: a cross-validated request whose budget runs out while every upstream is quiet fails as
// one with too few answers does. Its reply names the reason, insufficient-responses, as every
// other cross-validation failure does, and every upstream asked is still pending.
func TestFailedRelayReply_BudgetExpiryOfACrossValidatedRequestNamesItsReason_MAG3980(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	originalTimeout := common.DefaultTimeout
	common.DefaultTimeout = 2 * failedReplyRelayTimeout
	t.Cleanup(func() { common.DefaultTimeout = originalTimeout })

	jsonRPC := failedReplyInterfaces()[0]
	require.Equal(t, "jsonrpc", jsonRPC.name)
	// Three upstreams for a threshold of two: the 429 cases above leave upstream-1 held off for a
	// while, and selection leaves a held-off upstream out while another is ready.
	jsonRPC.headers = map[string]string{
		common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
		common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
	}
	// A cross-validated attempt is not cut off with the request (MAG-2187), and its own budget is the
	// request's, so it could give up just before the request does and be recorded as an error. Its
	// upstreams' node-url timeout keeps every attempt out past the request's budget: nothing is back.
	jsonRPC.upstreamTimeout = 5 * failedReplyRelayTimeout

	read := startFailedReplyRouter(t, jsonRPC, hangingUpstream(t, nil), hangingUpstream(t, nil), hangingUpstream(t, nil))()
	require.Equal(t, http.StatusGatewayTimeout, read.status, read.body)
	require.NotEmpty(t, read.guid, "the reply carries no Router-Guid")
	require.Equal(t, "failed", read.header.Get(common.CROSS_VALIDATION_STATUS_HEADER_NAME))
	queried := read.header.Get(common.CROSS_VALIDATION_ALL_PROVIDERS_HEADER_NAME)
	require.NotEmpty(t, queried)
	require.Equal(t, queried, read.header.Get(common.CROSS_VALIDATION_PENDING_PROVIDERS_HEADER),
		"every upstream asked is still out")
	require.Equal(t, common.CrossValidationReasonInsufficientResponses, read.header.Get(common.CROSS_VALIDATION_FAILURE_REASON_HEADER),
		"a failed cross-validation names its reason")
}
