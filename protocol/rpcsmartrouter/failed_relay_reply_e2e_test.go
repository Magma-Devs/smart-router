package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
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
}

// startFailedReplyRouter serves iface through a real listener and router whose only provider is
// upstreamURL, and returns a function that sends the interface's request and reads the reply.
func startFailedReplyRouter(t *testing.T, iface failedReplyInterface, upstreamURL string) func() failedReplyClientRead {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	parser := iface.parser(t)
	nodeURL := common.NodeUrl{Url: upstreamURL}
	connection, err := lavasession.NewDirectRPCConnection(ctx, nodeURL, 5, iface.apiInterface)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	provider := lavasession.NewConsumerSessionWithProvider("upstream", []*lavasession.Endpoint{{
		NetworkAddress: upstreamURL, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{connection},
	}}, 100000, 1, 1)
	provider.StaticProvider = true
	sessionManager, rpcEndpoint := createTestSessionManager(iface.chainID, iface.apiInterface)
	rpcEndpoint.NetworkAddress = "127.0.0.1:0"
	rpcEndpoint.HealthCheckPath = common.DEFAULT_HEALTH_PATH
	require.NoError(t, sessionManager.UpdateAllProviders(1, map[uint64]*lavasession.ConsumerSessionsWithProvider{0: provider}, nil))
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
		request.Header.Set(common.RELAY_TIMEOUT_HEADER_NAME, "300ms")
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

// hangingUpstream accepts every request and never answers.
func hangingUpstream(t *testing.T) string {
	t.Helper()
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
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
			})

			t.Run("refused connection is the router's 502", func(t *testing.T) {
				read := startFailedReplyRouter(t, iface, refusingUpstream(t))()
				require.Equal(t, http.StatusBadGateway, read.status)
				message := routerErrorMessage(t, iface, read.body)
				require.True(t, strings.HasPrefix(message, "no answer from upstream: "), message)
				require.Contains(t, message, "connection refused")
			})

			t.Run("hung upstream is the router's 504", func(t *testing.T) {
				read := startFailedReplyRouter(t, iface, hangingUpstream(t))()
				require.Equal(t, http.StatusGatewayTimeout, read.status, read.body)
				message := routerErrorMessage(t, iface, read.body)
				require.True(t, strings.HasPrefix(message, "no answer from upstream within "), message)
				require.NotContains(t, message, "context deadline exceeded")
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
