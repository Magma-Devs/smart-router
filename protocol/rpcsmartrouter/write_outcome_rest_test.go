package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Keep a real cancelled HTTP attempt from reporting its result until the client
// has received its response. This deterministically exercises SendParsedRelay's
// no-results branch; no response or stop reason is fabricated by the test.
type heldCancelledRESTConnection struct {
	lavasession.HTTPDirectRPCDoer
	cancelled chan struct{}
	release   <-chan struct{}
}

func (c *heldCancelledRESTConnection) DoHTTPRequest(ctx context.Context, params lavasession.HTTPRequestParams) (*lavasession.HTTPDirectRPCResponse, error) {
	response, err := c.HTTPDirectRPCDoer.DoHTTPRequest(ctx, params)
	if ctx.Err() != nil {
		close(c.cancelled)
		<-c.release
	}
	return response, err
}

// Exercise the REST listener, parser, session selection, dispatcher, and write verdict.
// The REST sender returns gateway failures with a nil Go error, but relayInnerDirect
// converts them to protocol errors before the results manager sees them. Calling the
// sender and results manager directly skips the production step this test must cover.
func TestRESTListener_WriteOutcome(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range []struct {
		name        string
		method      string
		status      int
		body        string
		wantUnclear bool
		hang        bool
	}{
		{"write bad gateway", http.MethodPost, http.StatusBadGateway, `{"message":"bad gateway"}`, true, false},
		{"write gateway timeout", http.MethodPost, http.StatusGatewayTimeout, `{"message":"gateway timeout"}`, true, false},
		{"write hangs until budget expires", http.MethodPost, 0, "", true, true},
		{"write node rejection", http.MethodPost, http.StatusBadRequest, `{"code":3,"message":"invalid transaction"}`, false, false},
		{"write success", http.MethodPost, http.StatusOK, `{"tx_response":{"code":0,"txhash":"ABC"}}`, false, false},
		{"read bad gateway", http.MethodGet, http.StatusBadGateway, `{"message":"bad gateway"}`, false, false},
		{"read gateway timeout", http.MethodGet, http.StatusGatewayTimeout, `{"message":"gateway timeout"}`, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if tc.hang {
				originalTimeout := common.DefaultTimeout
				common.DefaultTimeout = 250 * time.Millisecond // stateful processing budget: 6 x 250 ms
				t.Cleanup(func() { common.DefaultTimeout = originalTimeout })
			}
			path := "/cosmos/tx/v1beta1/txs"
			data := []byte(`{"tx_bytes":"dHg=","mode":"BROADCAST_MODE_SYNC"}`)
			if tc.method == http.MethodGet {
				path = "/cosmos/base/tendermint/v1beta1/blocks/17"
				data = nil
			}
			var requests atomic.Int32
			parser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
				ctx, "LAVA", spectypes.APIInterfaceRest, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					assert.Equal(t, tc.method, r.Method)
					assert.Equal(t, path, r.URL.Path)
					body, readErr := io.ReadAll(r.Body)
					assert.NoError(t, readErr)
					assert.Equal(t, string(data), string(body))
					if tc.hang {
						<-r.Context().Done()
						return
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				}), nil, "../../", nil)
			require.NoError(t, err)
			defer closeServer()
			chainMessage, err := parser.ParseMsg(path, data, tc.method, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			relayData := lavaprotocol.NewRelayData(ctx, tc.method, path, data, 0, spectypes.NOT_APPLICABLE,
				spectypes.APIInterfaceRest, nil, "", nil)
			msg := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "test", "127.0.0.1")
			if tc.method == http.MethodPost {
				require.Equal(t, uint32(common.CONSISTENCY_SELECT_ALL_PROVIDERS), chainlib.GetStateful(msg))
			} else {
				require.Equal(t, uint32(common.NO_STATE), chainlib.GetStateful(msg))
			}

			nodeURL := endpoint.NodeUrls[0]
			if tc.hang {
				// The attempt's own deadline must be later than the request budget,
				// so cancellation takes the new budgetExpired health path.
				nodeURL.Timeout = 2 * time.Second
			}
			conn, err := lavasession.NewDirectRPCConnection(ctx, nodeURL, 5, spectypes.APIInterfaceRest)
			require.NoError(t, err)
			defer conn.Close()
			var held *heldCancelledRESTConnection
			var releaseAttempt func()
			if tc.hang {
				release := make(chan struct{})
				releaseAttempt = sync.OnceFunc(func() { close(release) })
				defer releaseAttempt()
				held = &heldCancelledRESTConnection{
					HTTPDirectRPCDoer: conn.(lavasession.HTTPDirectRPCDoer),
					cancelled:         make(chan struct{}), release: release,
				}
				conn = held
			}
			providerEndpoint := &lavasession.Endpoint{
				NetworkAddress: endpoint.NodeUrls[0].Url, Enabled: true,
				DirectConnections: []lavasession.DirectRPCConnection{conn},
			}
			if tc.hang {
				providerEndpoint.ConnectionRefusals = lavasession.MaxConsecutiveConnectionAttempts - 1
			}
			provider := lavasession.NewConsumerSessionWithProvider("rest-upstream", []*lavasession.Endpoint{providerEndpoint}, 100000, 1, 1)
			provider.StaticProvider = true
			sessionManager, rpcEndpoint := createTestSessionManager("LAVA", spectypes.APIInterfaceRest)
			rpcEndpoint.NetworkAddress = "127.0.0.1:0"
			require.NoError(t, sessionManager.UpdateAllProviders(1,
				map[uint64]*lavasession.ConsumerSessionsWithProvider{0: provider}, nil))
			logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
			require.NoError(t, err)
			server := &RPCSmartRouterServer{
				chainParser: parser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
				rpcSmartRouterLogs: logs,
				consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
			}
			if tc.hang {
				server.smartRouterEndpointMetrics = metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
				server.smartRouterEndpointMetrics.SetEndpointOverallHealth("LAVA", "rest", "rest-upstream", true)
			}
			listener := chainlib.NewRestChainListener(ctx, rpcEndpoint, server, nil, logs)
			listenerDone := make(chan struct{})
			go func() {
				defer close(listenerDone)
				listener.Serve(ctx, common.ConsumerCmdFlags{})
			}()
			t.Cleanup(func() {
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
				defer shutdownCancel()
				assert.NoError(t, listener.Shutdown(shutdownCtx))
				select {
				case <-listenerDone:
				case <-shutdownCtx.Done():
					t.Error("REST listener did not stop")
				}
			})
			require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, time.Second, time.Millisecond)
			req, err := http.NewRequestWithContext(ctx, tc.method, "http://"+listener.GetListeningAddress()+path, bytes.NewReader(data))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			if tc.hang {
				req.Header.Set(common.RELAY_TIMEOUT_HEADER_NAME, "200ms")
			}
			response, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Positive(t, requests.Load(), "the request must actually reach the upstream")
			if tc.wantUnclear {
				require.Equal(t, http.StatusInternalServerError, response.StatusCode)
				// REST keeps the GUID envelope as a JSON string inside its error field.
				var restError struct {
					Error string `json:"error"`
				}
				require.NoError(t, json.Unmarshal(body, &restError))
				var envelope struct {
					ErrorGUID string `json:"Error_GUID"`
					Error     string `json:"Error"`
				}
				require.NoError(t, json.Unmarshal([]byte(restError.Error), &envelope))
				require.NotEmpty(t, envelope.ErrorGUID)
				require.Contains(t, envelope.Error, "transaction status unclear")
				require.Contains(t, envelope.Error, "may already have been submitted")
				require.Contains(t, envelope.Error, "verify on-chain before resubmitting")
				require.NotContains(t, envelope.Error, "failed")
			} else if tc.method == http.MethodGet {
				require.Equal(t, http.StatusInternalServerError, response.StatusCode)
				require.Contains(t, string(body), fmt.Sprintf("HTTP %d", tc.status))
				require.NotContains(t, string(body), "transaction status unclear")
			} else {
				require.Equal(t, tc.status, response.StatusCode)
				require.JSONEq(t, tc.body, string(body))
			}
			if tc.hang {
				select {
				case <-held.cancelled:
				case <-ctx.Done():
					t.Fatal("request budget did not cancel the upstream attempt")
				}
				require.True(t, providerEndpoint.HealthSnapshot().Enabled,
					"the held attempt has not reported a health outcome yet")
				releaseAttempt()
				require.Eventually(t, func() bool { return !providerEndpoint.HealthSnapshot().Enabled }, time.Second, time.Millisecond,
					"a budget-expired cancellation must disable the unhealthy URL at the refusal threshold")
				require.NotZero(t, providerEndpoint.HealthSnapshot().DisabledAt)
				require.Eventually(t, func() bool {
					value, found := gatherHealthGauge(t, "LAVA", "rest", "rest-upstream")
					return found && value == 0
				}, time.Second, time.Millisecond, "endpoint health metric must reflect the hang")
			}
		})
	}
}
