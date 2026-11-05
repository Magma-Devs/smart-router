package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type customerHTTPUsageSink struct {
	metrics.NoopUsageSink
	events chan metrics.RelayUsageEvent
}

func (s *customerHTTPUsageSink) Emit(event metrics.RelayUsageEvent) { s.events <- event }

func customerCounter(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var count float64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "spec" && label.GetValue() == "ETH1" {
					count += metric.GetCounter().GetValue()
				}
			}
		}
	}
	return count
}

// Real HTTP listeners and real upstreams must publish the same immutable verdict
// in Prometheus, the one relay_end and the optional usage event.
func TestCustomerTelemetryHTTPReconciles(t *testing.T) {
	rand.InitRandomSeed()
	read := captureRelayEnds(t)
	utils.SetGlobalLoggingLevel("warn")
	t.Cleanup(func() { utils.SetGlobalLoggingLevel("debug") })
	manager := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	receivedBefore := customerCounter(t, "smartrouter_customer_requests_received_total")
	outcomesBefore := customerCounter(t, "smartrouter_customer_request_outcomes_total")
	cases := []struct {
		name, body, reply, outcome, recovery     string
		primaryUnavailable, retry, providerFault bool
	}{
		{name: "success", body: relayEndRead, reply: relayEndBalance, outcome: "success", recovery: "none"},
		{name: "client domain error", body: relayEndRead, reply: relayEndReverted, outcome: "failure", recovery: "none"},
		{name: "provider fault", body: relayEndRead, reply: relayEndNodeDown, outcome: "failure", recovery: "none", providerFault: true},
		{name: "parse error", body: `{"jsonrpc":`, reply: relayEndBalance, outcome: "failure", recovery: "none"},
		{name: "retry", body: relayEndRead, reply: relayEndBalance, outcome: "success", recovery: "retry", retry: true},
		{name: "unavailable primary", body: relayEndRead, reply: relayEndBalance, outcome: "success", recovery: "reroute", primaryUnavailable: true},
		{name: "partial batch", body: `[` + relayEndRead + `,` + relayEndRead + `]`, reply: `[` + relayEndBalance + `,` + relayEndReverted + `]`, outcome: "failure", recovery: "none"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			primary := []relayEndNode{{"node-a", relayEndReply(200, tc.reply, 0)}}
			var backup []relayEndNode
			if tc.retry {
				primary = []relayEndNode{{"node-a", nil}}
				backup = []relayEndNode{{"node-b", relayEndReply(200, tc.reply, 0)}}
			}
			if tc.primaryUnavailable {
				primary = nil
				backup = []relayEndNode{{"node-b", relayEndReply(200, tc.reply, 0)}}
			}
			server, _ := newRelayEndServerWithBackups(t, "ETH1", "jsonrpc", ethJsonRPCParser(t), primary, backup)
			if tc.primaryUnavailable {
				server.sessionManager.SetCustomerPrimaryBaseConfigured(true)
			}
			sink := &customerHTTPUsageSink{events: make(chan metrics.RelayUsageEvent, 2)}
			logs, err := metrics.NewRPCConsumerLogs(manager, sink, nil)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			listener := chainlib.NewJrpcChainListener(ctx, &lavasession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "ETH1", ApiInterface: "jsonrpc", HealthCheckPath: common.DEFAULT_HEALTH_PATH}, server, nil, logs, nil, nil)
			go listener.Serve(ctx, common.ConsumerCmdFlags{})
			t.Cleanup(func() {
				shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require.NoError(t, listener.Shutdown(shutdown))
			})
			require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, time.Second, time.Millisecond)
			seen := len(read())
			req, err := http.NewRequest(http.MethodPost, "http://"+listener.GetListeningAddress()+"/", bytes.NewBufferString(tc.body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Request-Id", "customer-http-reconcile")
			response, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
			require.NoError(t, err)
			_, err = io.Copy(io.Discard, response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			events := newRelayEndsSince(read, seen)
			require.Len(t, events, 1)
			var event struct {
				Customer metrics.CustomerTerminal `json:"customer_request"`
			}
			require.NoError(t, json.Unmarshal([]byte(events[0]), &event))
			require.True(t, event.Customer.Terminal)
			require.Equal(t, tc.outcome, event.Customer.Outcome)
			require.Equal(t, tc.recovery, event.Customer.Recovery)
			require.Equal(t, tc.providerFault, event.Customer.ProviderFault)
			require.Equal(t, "customer-http-reconcile", event.Customer.RequestID)
			if tc.name == "partial batch" {
				require.True(t, event.Customer.PartialBatch)
			}
			select {
			case usage := <-sink.events:
				require.NotNil(t, usage.Customer)
				require.Equal(t, event.Customer, *usage.Customer)
			case <-time.After(time.Second):
				t.Fatal("usage export missing")
			}
			t.Logf("customer_terminal: %s", events[0])
		})
	}
	require.Equal(t, float64(len(cases)), customerCounter(t, "smartrouter_customer_requests_received_total")-receivedBefore)
	require.Equal(t, float64(len(cases)), customerCounter(t, "smartrouter_customer_request_outcomes_total")-outcomesBefore)
}
