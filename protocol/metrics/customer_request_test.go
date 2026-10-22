package metrics

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	gojson "github.com/goccy/go-json"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type customerRecorder struct {
	NoOpConsumerMetrics
	mu       sync.Mutex
	received int
	outcomes []CustomerTerminal
}

func (r *customerRecorder) RecordCustomerReceived(string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.received++
}

func (r *customerRecorder) RecordCustomerOutcome(t CustomerTerminal) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes = append(r.outcomes, t)
}

type customerSink struct {
	NoopUsageSink
	mu     sync.Mutex
	events []RelayUsageEvent
}

func (s *customerSink) Emit(e RelayUsageEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func customerFixture(t *testing.T) (*RPCConsumerLogs, *customerRecorder, *customerSink, *RelayMetrics, context.Context) {
	t.Helper()
	recorder, sink := &customerRecorder{}, &customerSink{}
	logger, err := NewRPCConsumerLogs(recorder, sink, nil)
	require.NoError(t, err)
	data := NewRelayAnalytics("tenant", "ETH1", "jsonrpc")
	data.ApiMethod = "eth_blockNumber"
	ctx := utils.WithRequestId(utils.WithUniqueIdentifier(context.Background(), 42), "external-42")
	ctx = logger.BeginCustomerRequest(ctx, data)
	return logger, recorder, sink, data, ctx
}

func providerResult(provider string) *common.RelayResult {
	return &common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: provider}, Reply: &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)}, StatusCode: 200}
}

func TestCustomerOutcomeDecisionMatrix(t *testing.T) {
	for _, tc := range []struct {
		name, provider, tier, recovery, outcome string
		attempts                                []CustomerAttempt
		rerouteProvider                         string
		alternatives                            []string
		nodeError                               bool
		err                                     error
	}{
		{name: "ordinary selection", provider: "B", recovery: "none", outcome: "success"},
		{name: "failed A successful B", provider: "B", attempts: []CustomerAttempt{{Provider: "A", Outcome: "failure"}, {Provider: "B", Outcome: "success"}}, recovery: "retry", outcome: "success"},
		{name: "A unavailable before first attempt", provider: "B", rerouteProvider: "A", alternatives: []string{"B"}, recovery: "reroute", outcome: "success"},
		{name: "both count once", provider: "B", attempts: []CustomerAttempt{{Provider: "A", Outcome: "failure"}}, rerouteProvider: "A", alternatives: []string{"B"}, recovery: "both", outcome: "success"},
		{name: "healthy parallel results", provider: "B", attempts: []CustomerAttempt{{Provider: "A", Outcome: "success"}, {Provider: "B", Outcome: "success"}}, recovery: "none", outcome: "success"},
		{name: "pending loser", provider: "B", attempts: []CustomerAttempt{{Provider: "A", Outcome: "pending"}}, recovery: "none", outcome: "success"},
		{name: "cancelled hedge loser", provider: "B", attempts: []CustomerAttempt{{Provider: "A", Outcome: "cancelled"}}, recovery: "none", outcome: "success"},
		{name: "retry same provider", provider: "B", attempts: []CustomerAttempt{{Provider: "B", Outcome: "failure"}}, recovery: "none", outcome: "success"},
		{name: "unrelated reroute decision", provider: "B", rerouteProvider: "A", alternatives: []string{"C"}, recovery: "none", outcome: "success"},
		{name: "intended provider actually served", provider: "A", rerouteProvider: "A", alternatives: []string{"A"}, recovery: "none", outcome: "success"},
		{name: "all providers failed", provider: "B", attempts: []CustomerAttempt{{Provider: "A", Outcome: "failure"}}, rerouteProvider: "A", alternatives: []string{"B"}, nodeError: true, recovery: "none", outcome: "failure"},
		{name: "no provider", err: errors.New("no providers"), recovery: "none", outcome: "failure"},
		{name: "deadline", err: context.DeadlineExceeded, recovery: "none", outcome: "failure"},
		{name: "cancelled request", err: context.Canceled, recovery: "none", outcome: "cancelled"},
		{name: "primary cache", tier: common.CacheTierPrimary, recovery: "none", outcome: "success"},
		{name: "secondary cache", tier: common.CacheTierSecondary, recovery: "none", outcome: "success"},
		{name: "cached node error", tier: common.CacheTierPrimary, nodeError: true, recovery: "none", outcome: "failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, recorder, sink, data, ctx := customerFixture(t)
			result := providerResult(tc.provider)
			result.IsNodeError = tc.nodeError
			result.CacheLookup.ServedTier = tc.tier
			if tc.nodeError {
				result.CustomerError = common.LavaErrorNodeServerError
			}
			data.CaptureCustomerResult(result, "test-stop", tc.attempts, nil, false)
			MarkCustomerReroute(ctx, tc.rerouteProvider, "test-reason", tc.alternatives)
			logger.AddMetricForHttp(data, tc.err, nil)
			require.Equal(t, 1, recorder.received)
			require.Len(t, recorder.outcomes, 1)
			require.Len(t, sink.events, 1)
			final := recorder.outcomes[0]
			require.Equal(t, tc.outcome, final.Outcome)
			require.Equal(t, tc.recovery, final.Recovery)
			require.Equal(t, "42", final.GUID)
			require.Equal(t, "external-42", final.RequestID)
			require.GreaterOrEqual(t, final.CompletedAtNs, final.StartedAtNs)
			require.Equal(t, final, *sink.events[0].Customer)
			require.Equal(t, tc.outcome == "success", sink.events[0].Success)
			require.Equal(t, tc.tier != "", sink.events[0].CacheHit)
			if tc.tier != "" {
				require.Equal(t, "Cached", sink.events[0].ProviderAddress)
			}
			if tc.outcome == "success" {
				require.Equal(t, "none", final.ErrorName)
			} else {
				require.NotEqual(t, "none", final.ErrorName)
			}
		})
	}
}

func TestCustomerExactlyOnceAndLateResultsAreImmutable(t *testing.T) {
	logger, recorder, sink, data, ctx := customerFixture(t)
	attempts := []CustomerAttempt{{Provider: "A", Outcome: "failure"}}
	alternatives := []string{"B"}
	data.CaptureCustomerResult(providerResult("B"), "winner", attempts, nil, false)
	MarkCustomerReroute(ctx, "A", "blocked", alternatives)
	attempts[0].Provider = "changed"
	alternatives[0] = "changed"
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); logger.AddMetricForWebSocket(data, nil) }()
	}
	wg.Wait()
	require.Len(t, recorder.outcomes, 1)
	require.Len(t, sink.events, 1)
	frozen := data.Customer.Snapshot()
	require.Equal(t, "both", frozen.Recovery)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data.CaptureCustomerResult(providerResult("late"), "late", nil, nil, false)
			MarkCustomerReroute(ctx, "late", "late", []string{"late"})
			_ = data.Customer.Snapshot()
		}()
	}
	wg.Wait()
	require.Equal(t, frozen, data.Customer.Snapshot())
	copied := data.Customer.Snapshot()
	copied.Attempts[0].Provider = "changed"
	copied.Reroutes[0].Alternatives[0] = "changed"
	require.Equal(t, frozen, data.Customer.Snapshot())
}

func TestCustomerPartialBatchDoesNotAlterRoutingResult(t *testing.T) {
	for _, body := range []struct {
		name, body, outcome string
		partial             bool
	}{
		{"success", `[{"id":1,"result":null},{"id":2,"result":3}]`, "success", false},
		{"error null", `[{"id":1,"result":1,"error":null}]`, "success", false},
		{"empty error beside result", `[{"id":1,"result":1,"error":{"code":0,"message":""}}]`, "success", false},
		{"missing result", `[{"id":1,"result":3},{"id":2}]`, "failure", true},
		{"malformed batch", `[{"id":1,"result":3},`, "failure", true},
		{"empty error without result", `[{"id":1,"error":{}}]`, "failure", false},
		{"partial", `[{"id":1,"result":3},{"id":2,"error":{"code":-32602,"message":"invalid params secret"}}]`, "failure", true},
		{"all failed", `[{"id":1,"error":{"code":-32602,"message":"invalid params"}}]`, "failure", false},
	} {
		t.Run(body.name, func(t *testing.T) {
			logger, recorder, _, data, _ := customerFixture(t)
			result := providerResult("B")
			result.Reply.Data = []byte(body.body)
			logger.ObserveCustomerReply(data, result)
			require.False(t, result.IsNodeError, "telemetry must not change retry classification")
			require.Nil(t, result.CustomerError, "telemetry only owns its snapshot")
			logger.AddMetricForHttp(data, nil, nil)
			require.Equal(t, body.outcome, recorder.outcomes[0].Outcome)
			require.Equal(t, body.partial, recorder.outcomes[0].PartialBatch)
			require.NotContains(t, recorder.outcomes[0].ErrorMessage, "secret")
		})
	}
}

func TestCustomerInternalAndNotificationsExcludedButSameMethodCustomerIncluded(t *testing.T) {
	recorder, sink := &customerRecorder{}, &customerSink{}
	logger, err := NewRPCConsumerLogs(recorder, sink, nil)
	require.NoError(t, err)
	internal := NewRelayAnalytics("tenant", "ETH1", "jsonrpc")
	internal.ApiMethod = "eth_blockNumber"
	logger.AddMetricForHttp(internal, nil, nil)
	require.Zero(t, recorder.received)
	require.Empty(t, recorder.outcomes)
	customer := NewRelayAnalytics("tenant", "ETH1", "jsonrpc")
	customer.ApiMethod = internal.ApiMethod
	logger.BeginCustomerRequest(context.Background(), customer)
	logger.AddMetricForWebSocket(customer, nil)
	notification := *customer
	notification.Customer = nil
	notification.RequestKind = "notification"
	logger.AddMetricForWebSocket(&notification, nil)
	require.Equal(t, 1, recorder.received)
	require.Len(t, recorder.outcomes, 1)
	require.Equal(t, "notification", sink.events[2].RequestKind)
	require.Nil(t, sink.events[2].Customer)
}

func TestCustomerUnknownWriteRemainsFailureWithoutRecovery(t *testing.T) {
	logger, recorder, _, data, ctx := customerFixture(t)
	result := providerResult("A")
	result.IsNodeError = true
	result.CustomerError = common.LavaErrorNodeServerError
	data.CaptureCustomerResult(result, "Stateful", []CustomerAttempt{{Provider: "B", Outcome: "failure"}}, nil, true)
	logger.ObserveCustomerReply(data, result)
	MarkCustomerReroute(ctx, "B", "blocked", []string{"A"})
	logger.AddMetricForHttp(data, errors.New("unclear write"), nil)
	require.Equal(t, "failure", recorder.outcomes[0].Outcome)
	require.Equal(t, "none", recorder.outcomes[0].Recovery)
	require.Equal(t, "unknown", recorder.outcomes[0].WriteOutcome)
	require.Equal(t, common.LavaErrorUnknown.Name, recorder.outcomes[0].ErrorName)
}

func TestCustomerPrometheusReconcilesByChainWithoutLegacyCounterChanges(t *testing.T) {
	manager := &SmartRouterMetricsManager{
		customerRequestsReceived: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_received"}, []string{"spec", "apiInterface"}),
		customerRequestOutcomes:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_outcomes"}, []string{"spec", "apiInterface", "outcome", "recovery", "cache_hit", "error_name"}),
	}
	sink := &customerSink{}
	logger, err := NewRPCConsumerLogs(manager, sink, nil)
	require.NoError(t, err)
	for _, chain := range []string{"ETH1", "SOLANA"} {
		for i := 0; i < 80; i++ {
			data := NewRelayAnalytics("tenant", chain, "jsonrpc")
			ctx := logger.BeginCustomerRequest(context.Background(), data)
			result := providerResult("B")
			attempts := []CustomerAttempt{{Provider: "A", Outcome: "failure"}}
			var terminalErr error
			switch i % 4 {
			case 0:
				terminalErr = fmt.Errorf("unclassified detail-%d", i)
			case 1:
				terminalErr = context.Canceled
			case 2:
				result.CacheLookup.ServedTier = common.CacheTierPrimary
			case 3:
				MarkCustomerReroute(ctx, "A", "blocked", []string{"B"})
			}
			data.CaptureCustomerResult(result, "done", attempts, nil, false)
			logger.AddMetricForWebSocket(data, terminalErr)
			logger.AddMetricForWebSocket(data, nil) // duplicate finalization attempt
		}
		require.Equal(t, float64(80), testutil.ToFloat64(manager.customerRequestsReceived.WithLabelValues(chain, "jsonrpc")))
	}
	require.Len(t, sink.events, 160)
	for _, chain := range []string{"ETH1", "SOLANA"} {
		total, successes, recovered := 0, 0, 0
		for _, event := range sink.events {
			if event.ChainID != chain {
				continue
			}
			total++
			if event.Success {
				successes++
			}
			if event.Customer.Recovery != "none" {
				recovered++
			}
		}
		require.Equal(t, 80, total)
		require.Equal(t, 40, successes)
		require.Equal(t, 20, recovered)
		for _, tuple := range []struct{ outcome, recovery, hit, name string }{
			{"failure", "none", "false", common.LavaErrorUnknown.Name},
			{"cancelled", "none", "false", common.LavaErrorContextCanceled.Name},
			{"success", "none", "true", "none"},
			{"success", "both", "false", "none"},
		} {
			require.Equal(t, float64(20), testutil.ToFloat64(manager.customerRequestOutcomes.WithLabelValues(chain, "jsonrpc", tuple.outcome, tuple.recovery, tuple.hit, tuple.name)))
		}
	}
}

func TestCustomerMetricsRegistrationAndBoundedLabels(t *testing.T) {
	manager := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	require.NotNil(t, manager)
	logger, err := NewRPCConsumerLogs(manager, nil, nil)
	require.NoError(t, err)
	data := NewRelayAnalytics("tenant-not-a-label", "MAG4276-REGISTRATION", "jsonrpc")
	logger.BeginCustomerRequest(context.Background(), data)
	logger.AddMetricForWebSocket(data, nil)
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	found := map[string]bool{}
	for _, family := range families {
		name := family.GetName()
		if name != "smartrouter_customer_requests_received_total" && name != "smartrouter_customer_request_outcomes_total" && name != "smartrouter_customer_telemetry_info" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if name == "smartrouter_customer_telemetry_info" {
				require.Equal(t, map[string]string{"schema_version": "1"}, labels)
				require.Equal(t, float64(1), metric.GetGauge().GetValue())
				found[name] = true
				continue
			}
			if labels["spec"] != "MAG4276-REGISTRATION" {
				continue
			}
			want := map[string]string{"spec": "MAG4276-REGISTRATION", "apiInterface": "jsonrpc"}
			if name == "smartrouter_customer_request_outcomes_total" {
				want["outcome"] = "success"
				want["recovery"] = "none"
				want["cache_hit"] = "false"
				want["error_name"] = "none"
			}
			require.Equal(t, want, labels)
			found[name] = true
		}
	}
	require.Len(t, found, 3)
}

func TestCustomerTypedParseFailureUsesExistingRegistry(t *testing.T) {
	logger, recorder, _, data, _ := customerFixture(t)
	var parsed interface{}
	err := gojson.Unmarshal([]byte("{malformed"), &parsed)
	require.Error(t, err)
	logger.AddMetricForHttp(data, fmt.Errorf("parse wrapper: %w", err), nil)
	require.Equal(t, common.LavaErrorUserParseError.Name, recorder.outcomes[0].ErrorName)
	require.False(t, recorder.outcomes[0].Retryable)
}

// MAG-4081 landed while MAG-4276 was being implemented. Its parsed-request
// counters and the new all-request outcomes share one completion without doubling.
func TestCustomerExistingParsedCountersShareFinalVerdict(t *testing.T) {
	manager := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	logger, err := NewRPCConsumerLogs(manager, nil, nil)
	require.NoError(t, err)
	chain, customer, method := "MAG4276-COMPAT", "tenant-4276", "eth_blockNumber"
	oldTotal := manager.customerRequestsTotal.WithLabelValues(chain, "jsonrpc", customer, method)
	oldFailure := manager.customerRequestsFailed.WithLabelValues(chain, "jsonrpc", customer, method)
	beforeTotal, beforeFailure := testutil.ToFloat64(oldTotal), testutil.ToFloat64(oldFailure)
	for _, failure := range []bool{false, true} {
		data := NewRelayAnalytics(customer, chain, "jsonrpc")
		data.ApiMethod = method
		logger.BeginCustomerRequest(context.Background(), data)
		result := providerResult("provider")
		if failure {
			result.IsNodeError = true
			result.CustomerError = common.LavaErrorNodeServerError
		}
		logger.ObserveCustomerReply(data, result)
		logger.AddMetricForHttp(data, nil, nil)
		logger.AddMetricForHttp(data, nil, nil)
	}
	// A parse failure is new report coverage, not a second parsed method request.
	unparsed := NewRelayAnalytics(customer, chain, "jsonrpc")
	logger.BeginCustomerRequest(context.Background(), unparsed)
	logger.AddMetricForHttp(unparsed, errors.New("unparsed"), nil)
	require.Equal(t, beforeTotal+2, testutil.ToFloat64(oldTotal))
	require.Equal(t, beforeFailure+1, testutil.ToFloat64(oldFailure))
	require.Equal(t, float64(3), testutil.ToFloat64(manager.customerRequestsReceived.WithLabelValues(chain, "jsonrpc")))
	notification := NewRelayAnalytics(customer, chain, "jsonrpc")
	notification.RequestKind = "notification"
	notification.ApiMethod = SubscriptionDeliveryMethod
	logger.AddMetricForWebSocket(notification, nil)
	require.Equal(t, beforeTotal+2, testutil.ToFloat64(oldTotal))
}
