package chainlib

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

var updateListenerGolden = flag.Bool("update-listener-golden", false, "rewrite testdata/listener_handlers.golden.json")

// scriptedRelay answers every relay with one fixed result and error, and records the arguments
// the listener handed it.
type scriptedRelay struct {
	result *common.RelayResult
	err    error

	mu    sync.Mutex
	calls []string
}

func (r *scriptedRelay) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fmt.Sprintf("url=%q req=%q connectionType=%q", url, req, connectionType))
	return r.result, r.err
}

func (r *scriptedRelay) ParseRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, metadata []pairingtypes.Metadata) (ProtocolMessage, error) {
	return nil, errors.New("not used")
}

func (r *scriptedRelay) SendParsedRelay(ctx context.Context, analytics *metrics.RelayMetrics, protocolMessage ProtocolMessage) (*common.RelayResult, error) {
	return nil, errors.New("not used")
}

// guidPattern matches the per-request GUID a masked error carries; it is random, so the golden
// file holds a placeholder.
var guidPattern = regexp.MustCompile(`\d{12,}`)

// TestListenerHandlers_Golden pins what every HTTP listener handler answers for each relay outcome:
// status, response headers and body, and the url, body and connection type it hands SendRelay. The
// handlers share one skeleton with per-interface differences, and this file is the record of those
// differences. Regenerate with -update-listener-golden only for a change meant to be client-visible.
func TestListenerHandlers_Golden(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	nodeMetadata := []pairingtypes.Metadata{{Name: "X-Node-Header", Value: "node"}}
	upstreamBody := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"upstream said no"}}`)
	outcomes := []struct {
		name   string
		result *common.RelayResult
		err    error
	}{
		{"ok", &common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte(`{"result":"0x1"}`), Metadata: nodeMetadata}}, nil},
		{"ok status 201", &common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte(`{"result":"0x1"}`), Metadata: nodeMetadata}, StatusCode: http.StatusCreated}, nil},
		{"error", &common.RelayResult{Reply: &pairingtypes.RelayReply{Metadata: nodeMetadata}}, errors.New("all providers failed")},
		{"error status 503", &common.RelayResult{Reply: &pairingtypes.RelayReply{Metadata: nodeMetadata}, StatusCode: http.StatusServiceUnavailable}, errors.New("all providers failed")},
		{"error nil result", nil, errors.New("all providers failed")},
		{"error with upstream answer", &common.RelayResult{
			Reply:      &pairingtypes.RelayReply{Metadata: nodeMetadata},
			StatusCode: http.StatusBadGateway,
			ClientFacing: &common.ClientFacingError{
				UpstreamStatus: http.StatusBadGateway, UpstreamStatusText: "502 Bad Gateway", UpstreamBody: upstreamBody,
				RouterErrorCode: 1, RouterErrorName: "TEST",
			},
		}, errors.New("upstream failed")},
		{"api not supported", &common.RelayResult{Reply: &pairingtypes.RelayReply{Metadata: nodeMetadata}}, fmt.Errorf("wrapped: %w", common.APINotSupportedError)},
		{"batch too large", nil, ErrBatchRequestSizeExceeded},
		{"batch refused", nil, rpcInterfaceMessages.ErrJsonrpcBatchRefused},
		{"json syntax error", nil, &json.SyntaxError{}},
	}
	jsonBody := `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`
	requests := []struct {
		name, apiInterface, method, path, body string
	}{
		{"jsonrpc POST", spectypes.APIInterfaceJsonRPC, http.MethodPost, "/sub/path?q=1", jsonBody},
		{"jsonrpc GET", spectypes.APIInterfaceJsonRPC, http.MethodGet, "/", ""},
		{"tendermintrpc POST", spectypes.APIInterfaceTendermintRPC, http.MethodPost, "/sub/path?q=1", jsonBody},
		{"tendermintrpc GET", spectypes.APIInterfaceTendermintRPC, http.MethodGet, "/status?height=7", ""},
		{"tendermintrpc GET with body", spectypes.APIInterfaceTendermintRPC, http.MethodGet, "/status", jsonBody},
		{"rest POST", spectypes.APIInterfaceRest, http.MethodPost, "/cosmos/tx/v1beta1/simulate?x=1", `{"tx":1}`},
		{"rest GET", spectypes.APIInterfaceRest, http.MethodGet, "/cosmos/base/v1beta1/blocks/latest?x=1", ""},
		{"rest PUT", spectypes.APIInterfaceRest, http.MethodPut, "/v1/thing", `{"put":1}`},
		{"rest DELETE", spectypes.APIInterfaceRest, http.MethodDelete, "/v1/thing", `{"ignored":1}`},
		{"graphql POST", spectypes.APIInterfaceGraphQL, http.MethodPost, "/graphql", `{"query":"{ chainIdentifier }"}`},
		{"graphql GET", spectypes.APIInterfaceGraphQL, http.MethodGet, "/graphql?query=x", ""},
	}

	got := map[string]string{}
	for _, outcome := range outcomes {
		relay := &scriptedRelay{result: outcome.result, err: outcome.err}
		addrs := map[string]string{}
		ctx, cancel := context.WithCancel(context.Background())
		for _, apiInterface := range []string{spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC, spectypes.APIInterfaceRest, spectypes.APIInterfaceGraphQL} {
			addrs[apiInterface] = serveGoldenListener(t, ctx, apiInterface, relay)
		}
		for _, req := range requests {
			relay.mu.Lock()
			relay.calls = nil
			relay.mu.Unlock()
			var body io.Reader
			if req.body != "" {
				body = strings.NewReader(req.body)
			}
			httpReq, err := http.NewRequestWithContext(ctx, req.method, "http://"+addrs[req.apiInterface]+req.path, body)
			require.NoError(t, err)
			if req.body != "" {
				httpReq.Header.Set("Content-Type", "application/json")
			}
			resp, err := http.DefaultClient.Do(httpReq)
			require.NoError(t, err)
			respBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			var headerLines []string
			for name, values := range resp.Header {
				if name == "Date" || name == "Content-Length" {
					continue
				}
				headerLines = append(headerLines, name+": "+strings.Join(values, ", "))
			}
			sort.Strings(headerLines)
			relay.mu.Lock()
			calls := append([]string(nil), relay.calls...)
			relay.mu.Unlock()
			got[req.name+" / "+outcome.name] = fmt.Sprintf("status %d\nheaders %s\nbody %s\nrelay %s",
				resp.StatusCode, strings.Join(headerLines, " | "), guidPattern.ReplaceAllString(string(respBody), "<GUID>"), strings.Join(calls, " ; "))
		}
		cancel()
	}

	goldenPath := filepath.Join("testdata", "listener_handlers.golden.json")
	if *updateListenerGolden {
		encoded, err := json.MarshalIndent(got, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(goldenPath, append(encoded, '\n'), 0o600))
	}
	raw, err := os.ReadFile(goldenPath)
	require.NoError(t, err)
	want := map[string]string{}
	require.NoError(t, json.Unmarshal(raw, &want))
	require.Equal(t, want, got)
}

func serveGoldenListener(t *testing.T, ctx context.Context, apiInterface string, relay RelaySender) string {
	t.Helper()
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	endpoint := &lavasession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "LAV1", ApiInterface: apiInterface, HealthCheckPath: common.DEFAULT_HEALTH_PATH}
	var listener ChainListener
	switch apiInterface {
	case spectypes.APIInterfaceJsonRPC:
		listener = NewJrpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, nil, nil)
	case spectypes.APIInterfaceTendermintRPC:
		listener = NewTendermintRpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, nil, nil)
	case spectypes.APIInterfaceRest:
		listener = NewRestChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger)
	case spectypes.APIInterfaceGraphQL:
		listener = NewGraphQLChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger)
	}
	go listener.Serve(ctx, common.ConsumerCmdFlags{})
	addr := ""
	for deadline := time.Now().Add(3 * time.Second); addr == "" && time.Now().Before(deadline); {
		if addr = listener.GetListeningAddress(); addr == "" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	require.NotEmpty(t, addr, "listener never reported a listening address")
	return addr
}
