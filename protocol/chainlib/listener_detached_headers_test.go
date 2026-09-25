package chainlib

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// headerKeepingRelay is a RelaySender that keeps the metadata of every relay it is handed, the way
// the router keeps a pinned provider name past the request: as a session-map key and a metric label.
type headerKeepingRelay struct {
	mu   sync.Mutex
	kept [][]pairingtypes.Metadata
}

func (r *headerKeepingRelay) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kept = append(r.kept, metadataValues)
	return &common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)}, StatusCode: http.StatusOK}, nil
}

func (r *headerKeepingRelay) ParseRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, metadata []pairingtypes.Metadata) (ProtocolMessage, error) {
	return nil, errors.New("not used")
}

func (r *headerKeepingRelay) SendParsedRelay(ctx context.Context, analytics *metrics.RelayMetrics, protocolMessage ProtocolMessage) (*common.RelayResult, error) {
	return nil, errors.New("not used")
}

func (r *headerKeepingRelay) CancelSubscriptionContext(subscriptionKey string) {}

// keptValues reads, now, the value each relay was handed for header name.
func (r *headerKeepingRelay) keptValues(name string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	values := make([]string, len(r.kept))
	for i, metadata := range r.kept {
		for _, md := range metadata {
			if strings.EqualFold(md.Name, name) {
				values[i] = md.Value
			}
		}
	}
	return values
}

// TestListeners_KeepNoHeaderPastTheRequest sends two requests over one keep-alive connection to each
// HTTP handler that reads request headers. The second request pins a different provider whose name has
// the same length, and fasthttp writes it over the first request's value in place. The value the first
// relay was handed must still read as it was sent, because the router keeps it past the request as a
// session-map key and a metric label (MAG-3881). Each case is one of the five places a listener takes
// the request's headers.
func TestListeners_KeepNoHeaderPastTheRequest(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	jsonBody := `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`
	newJsonRPC := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewJrpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, nil, nil)
	}
	newRest := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewRestChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger)
	}
	newTendermint := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewTendermintRpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, nil, nil)
	}
	for _, tc := range []struct {
		name, apiInterface, method, path, body string
		newListener                            func(context.Context, *lavasession.RPCEndpoint, RelaySender, *metrics.RPCConsumerLogs) ChainListener
	}{
		{"jsonrpc POST", spectypes.APIInterfaceJsonRPC, http.MethodPost, "/", jsonBody, newJsonRPC},
		{"rest POST", spectypes.APIInterfaceRest, http.MethodPost, "/cosmos/tx/v1beta1/simulate", "{}", newRest},
		{"rest GET", spectypes.APIInterfaceRest, http.MethodGet, "/cosmos/base/tendermint/v1beta1/blocks/latest", "", newRest},
		{"tendermintrpc POST", spectypes.APIInterfaceTendermintRPC, http.MethodPost, "/", jsonBody, newTendermint},
		{"tendermintrpc GET", spectypes.APIInterfaceTendermintRPC, http.MethodGet, "/status", "", newTendermint},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
			require.NoError(t, err)
			relay := &headerKeepingRelay{}
			listener := tc.newListener(ctx, &lavasession.RPCEndpoint{
				NetworkAddress:  "127.0.0.1:0",
				ChainID:         "LAV1",
				ApiInterface:    tc.apiInterface,
				HealthCheckPath: common.DEFAULT_HEALTH_PATH,
			}, relay, logger)
			go listener.Serve(ctx, common.ConsumerCmdFlags{})
			addr := ""
			for deadline := time.Now().Add(3 * time.Second); addr == "" && time.Now().Before(deadline); {
				if addr = listener.GetListeningAddress(); addr == "" {
					time.Sleep(20 * time.Millisecond)
				}
			}
			require.NotEmpty(t, addr, "listener never reported a listening address")

			client := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1}}
			defer client.CloseIdleConnections()
			var reused []bool
			trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = append(reused, info.Reused) }}
			for _, pinned := range []string{"ethprimaryprovider2", "tests.simulator.sim"} {
				var body io.Reader
				if tc.body != "" {
					body = strings.NewReader(tc.body)
				}
				req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), tc.method, "http://"+addr+tc.path, body)
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set(common.SELECT_PROVIDER_HEADER_NAME, pinned)
				resp, err := client.Do(req)
				require.NoError(t, err)
				_, _ = io.Copy(io.Discard, resp.Body)
				require.NoError(t, resp.Body.Close())
				require.Equal(t, http.StatusOK, resp.StatusCode)
			}
			require.Equal(t, []bool{false, true}, reused, "both requests on one connection, or the second cannot reach the first one's buffer")

			require.Equal(t, []string{"ethprimaryprovider2", "tests.simulator.sim"}, relay.keptValues(common.SELECT_PROVIDER_HEADER_NAME),
				"what the first relay was handed changed under it")
		})
	}
}
