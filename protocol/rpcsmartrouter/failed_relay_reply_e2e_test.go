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
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
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
	// headers are request headers sent on top of the ones every request here sends.
	headers map[string]string
	// upstreamTimeout is the node-url timeout of every upstream, which lengthens each attempt's
	// own budget past the request's (NodeUrl.LowerContextTimeoutWithDuration).
	upstreamTimeout time.Duration
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
			name: "rest", chainID: "LAVA", apiInterface: spectypes.APIInterfaceRest, parser: mockParser("LAVA", spectypes.APIInterfaceRest),
			method: http.MethodGet, path: "/cosmos/base/tendermint/v1beta1/blocks/17",
		},
		{
			name: "tendermintrpc", chainID: "LAVA", apiInterface: spectypes.APIInterfaceTendermintRPC, parser: mockParser("LAVA", spectypes.APIInterfaceTendermintRPC),
			method: http.MethodPost, path: "/", body: `{"jsonrpc":"2.0","id":77,"method":"status","params":[]}`, jsonRpcShape: true,
		},
		{
			name: "graphql", chainID: "SUI", apiInterface: spectypes.APIInterfaceGraphQL, parser: newGatewayTestGraphQLParser,
			method: http.MethodPost, path: "/", body: gatewayTestQuery,
		},
	}
}

// failedReplyClientRead is what the client read.
type failedReplyClientRead struct {
	status      int
	body        string
	contentType string
	guid        string
	nodeError   string // lava-identified-node-error
	header      http.Header
}

// failedReplyRelayTimeout is the attempt window every request here asks for in lava-relay-timeout.
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
				require.True(t, strings.HasPrefix(message, "no answer from upstream: "), message)
				require.Contains(t, message, "connection refused")
			})

			t.Run("hung upstream is the router's 504", func(t *testing.T) {
				read := startFailedReplyRouter(t, iface, hangingUpstream(t, nil))()
				require.Equal(t, http.StatusGatewayTimeout, read.status, read.body)
				message := routerErrorMessage(t, iface, read.body)
				require.True(t, strings.HasPrefix(message, "no answer from upstream within "), message)
				require.NotContains(t, message, "context deadline exceeded")
				// MAG-3980: the router's own 504 carries the request's headers, as a reply an
				// upstream produced does: the GUID, and the upstream it asked. One attempt went out,
				// so there is no retry to report.
				require.NotEmpty(t, read.guid, "the router's 504 carries no Lava-Guid")
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
// to return before they were built, so the client had no Lava-Guid to quote for the failure that
// is hardest to diagnose, and no Lava-Retries or Lava-Provider-Address to say what was tried.
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

	require.NotEmpty(t, read.guid, "the reply carries no Lava-Guid")
	require.Contains(t, read.body, read.guid, "the header names the request the body names")
	require.Equal(t, "1", read.header.Get(common.RETRY_COUNT_HEADER_NAME), "two attempts went out")
	require.ElementsMatch(t, []string{"upstream-1", "upstream-2"},
		strings.Split(read.header.Get(common.PROVIDER_ADDRESS_HEADER_NAME), ","),
		"both upstreams were asked; nothing answered, so nothing follows them")
	require.Equal(t, "true", read.header.Get(common.HEDGE_TRIGGERED_HEADER))
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
	require.NotEmpty(t, read.guid, "the reply carries no Lava-Guid")
	require.Equal(t, "failed", read.header.Get(common.CROSS_VALIDATION_STATUS_HEADER_NAME))
	queried := read.header.Get(common.CROSS_VALIDATION_ALL_PROVIDERS_HEADER_NAME)
	require.NotEmpty(t, queried)
	require.Equal(t, queried, read.header.Get(common.CROSS_VALIDATION_PENDING_PROVIDERS_HEADER),
		"every upstream asked is still out")
	require.Equal(t, common.CrossValidationReasonInsufficientResponses, read.header.Get(common.CROSS_VALIDATION_FAILURE_REASON_HEADER),
		"a failed cross-validation names its reason")
}
