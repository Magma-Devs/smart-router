package chainlib

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

type listenerCustomerRecorder struct {
	metrics.NoOpConsumerMetrics
	mu       sync.Mutex
	received int
	terminal []metrics.CustomerTerminal
}

func (r *listenerCustomerRecorder) RecordCustomerReceived(string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.received++
}

func (r *listenerCustomerRecorder) RecordCustomerOutcome(t metrics.CustomerTerminal) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.terminal = append(r.terminal, t)
}

func (r *listenerCustomerRecorder) snapshot() (int, []metrics.CustomerTerminal) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.received, append([]metrics.CustomerTerminal(nil), r.terminal...)
}

type listenerCustomerRelay struct{ headerKeepingRelay }

func (r *listenerCustomerRelay) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, data *metrics.RelayMetrics, md []pairingtypes.Metadata) (*common.RelayResult, error) {
	if data.Customer == nil {
		return nil, errors.New("customer receipt was missing before parse")
	}
	data.ApiMethod = "normalized_test_method"
	switch {
	case strings.Contains(req, "parse-failure"):
		return nil, errors.New("malformed request")
	case strings.Contains(req, "node-failure"):
		return &common.RelayResult{StatusCode: 200, Reply: &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"node failed"}}`)}, IsNodeError: true, CustomerError: common.LavaErrorNodeServerError}, nil
	case strings.Contains(req, "cache-primary"):
		return &common.RelayResult{StatusCode: 200, Reply: &pairingtypes.RelayReply{Data: []byte(`{"result":1}`)}, CacheLookup: common.CacheLookupReport{ServedTier: common.CacheTierPrimary}}, nil
	case strings.Contains(req, "cache-secondary"):
		return &common.RelayResult{StatusCode: 200, Reply: &pairingtypes.RelayReply{Data: []byte(`{"result":1}`)}, CacheLookup: common.CacheLookupReport{ServedTier: common.CacheTierSecondary}}, nil
	default:
		return &common.RelayResult{StatusCode: 200, Reply: &pairingtypes.RelayReply{Data: []byte(`{"result":1}`)}, ProviderInfo: common.ProviderInfo{ProviderAddress: "test-provider"}}, nil
	}
}

func TestCustomerHTTPListenersCountBeforeParseAndExcludeHealth(t *testing.T) {
	for _, tc := range []struct{ name, api, method, path string }{
		{"jsonrpc", "jsonrpc", http.MethodPost, "/"}, {"rest POST", "rest", http.MethodPost, "/test"}, {"rest GET", "rest", http.MethodGet, "/test"}, {"tendermint POST", "tendermintrpc", http.MethodPost, "/"}, {"tendermint GET", "tendermintrpc", http.MethodGet, "/status"}, {"graphql", "graphql", http.MethodPost, "/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			recorder := &listenerCustomerRecorder{}
			logger, err := metrics.NewRPCConsumerLogs(recorder, nil, nil)
			require.NoError(t, err)
			endpoint := &lavasession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "ETH1", ApiInterface: tc.api, HealthCheckPath: common.DEFAULT_HEALTH_PATH}
			relay := &listenerCustomerRelay{}
			var listener ChainListener
			switch tc.api {
			case "jsonrpc":
				listener = NewJrpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, nil, nil)
			case "rest":
				listener = NewRestChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger)
			case "tendermintrpc":
				listener = NewTendermintRpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, nil, nil)
			case "graphql":
				listener = NewGraphQLChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger)
			}
			go listener.Serve(ctx, common.ConsumerCmdFlags{})
			require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, 10*time.Millisecond)
			client := &http.Client{Timeout: 3 * time.Second}
			defer client.CloseIdleConnections()
			send := func(path, body string) {
				req, err := http.NewRequest(tc.method, "http://"+listener.GetListeningAddress()+path, strings.NewReader(body))
				require.NoError(t, err)
				req.Header.Set("X-Request-Id", "client-owned-id")
				req.Header.Set("Content-Type", "application/json")
				response, err := client.Do(req)
				require.NoError(t, err)
				_, err = io.Copy(io.Discard, response.Body)
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
			}
			cases := []struct{ body, outcome, tier string }{{"ok", "success", "none"}, {"parse-failure", "failure", "none"}, {"node-failure", "failure", "none"}, {"cache-primary", "success", "primary"}, {"cache-secondary", "success", "secondary"}}
			if tc.method == http.MethodGet {
				cases = cases[:1]
			}
			for _, test := range cases {
				send(tc.path, test.body)
			}
			received, terminal := recorder.snapshot()
			require.Equal(t, len(cases), received)
			require.Len(t, terminal, len(cases))
			seen := map[string]bool{}
			for i, test := range cases {
				require.Equal(t, test.outcome, terminal[i].Outcome)
				require.Equal(t, test.tier, terminal[i].CacheTier)
				require.Equal(t, "client-owned-id", terminal[i].RequestID)
				require.False(t, seen[terminal[i].GUID])
				seen[terminal[i].GUID] = true
			}
			healthResponse, err := client.Get("http://" + listener.GetListeningAddress() + common.DEFAULT_HEALTH_PATH)
			require.NoError(t, err)
			_, _ = io.Copy(io.Discard, healthResponse.Body)
			_ = healthResponse.Body.Close()
			after, _ := recorder.snapshot()
			require.Equal(t, received, after, "readiness probe must never be a customer request")
		})
	}
}
