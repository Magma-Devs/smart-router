package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/stretchr/testify/require"
)

func TestCustomerDecisionSnapshotEmptyProcessorAndUnknownWrite(t *testing.T) {
	for _, tc := range []struct {
		name       string
		dispatched int
		err        error
		unknown    bool
	}{
		{name: "empty deadline", err: context.DeadlineExceeded},
		{name: "pending unknown write", dispatched: 1, err: errUnknownWriteOutcome, unknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
			require.NoError(t, err)
			data := metrics.NewRelayAnalytics("tenant", "ETH1", "jsonrpc")
			logger.BeginCustomerRequest(context.Background(), data)
			msg := writeMessage(common.CONSISTENCY_SELECT_ALL_PROVIDERS)
			processor := newWriteOutcomeProcessor(t, msg, tc.dispatched)
			processor.SetStopReason(relaycore.StopReasonProcessingTimeout)
			beforeS, beforeN, beforeP := processor.GetResultsData()
			require.NotPanics(t, func() { captureCustomerDecision(data, msg, processor, nil, tc.err) })
			afterS, afterN, afterP := processor.GetResultsData()
			require.Equal(t, beforeS, afterS)
			require.Equal(t, beforeN, afterN)
			require.Equal(t, beforeP, afterP)
			logger.AddMetricForWebSocket(data, tc.err)
			final := data.Customer.Snapshot()
			require.Equal(t, "failure", final.Outcome)
			require.Equal(t, "none", final.Recovery)
			require.Equal(t, "eth_sendRawTransaction", final.APIMethod)
			if tc.unknown {
				require.Equal(t, "unknown", final.WriteOutcome)
				require.Len(t, final.Attempts, 1)
				require.Equal(t, "pending", final.Attempts[0].Outcome)
			}
		})
	}
}

func TestCustomerDecisionReadsObservedFailuresWithoutChangingVoteCounts(t *testing.T) {
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	data := metrics.NewRelayAnalytics("tenant", "ETH1", "jsonrpc")
	logger.BeginCustomerRequest(context.Background(), data)
	msg := writeMessage(common.NO_STATE)
	processor := newWriteOutcomeProcessor(t, msg, 1)
	processor.SetResponse(&relaycore.RelayResponse{RelayResult: common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "provider0"}}, Err: errors.New("connection refused")})
	drain(t, processor)
	_, _, before := processor.GetResultsData()
	require.Len(t, before, 1)
	captureCustomerDecision(data, msg, processor, nil, errors.New("final relay error"))
	_, _, after := processor.GetResultsData()
	require.Equal(t, before, after)
	logger.AddMetricForWebSocket(data, errors.New("final relay error"))
	require.Equal(t, "failure", data.Customer.Snapshot().Outcome)
	require.Equal(t, common.LavaErrorConnectionRefused.Name, data.Customer.Snapshot().ErrorName)
}

func TestCustomerSubscriptionUnavailableConfiguredPrimaryAndBackupOnly(t *testing.T) {
	for _, api := range []string{"jsonrpc", "grpc"} {
		for _, configured := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/configured=%t", api, configured), func(t *testing.T) {
				logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
				require.NoError(t, err)
				data := metrics.NewRelayAnalytics("tenant", "ETH1", api)
				ctx := logger.BeginCustomerRequest(context.Background(), data)
				backupURL := "wss://backup.example.com"
				if api == "grpc" {
					backupURL = "grpc://backup.example.com:9090"
				}
				backup := []*common.NodeUrl{{Url: backupURL}}
				var endpoint *common.NodeUrl
				names := map[string]string{backupURL: "backup-provider"}
				if api == "jsonrpc" {
					manager := NewDirectWSSubscriptionManager(nil, "jsonrpc", "ETH1", api, nil, backup, nil, nil)
					manager.SetEndpointNames(names)
					manager.SetCustomerPrimaryConfigured(configured)
					endpoint, err = manager.selectEndpoint(ctx, "client", nil)
				} else {
					manager := NewDirectGRPCSubscriptionManager(nil, "ETH1", api, nil, backup, nil, nil)
					manager.SetEndpointNames(names)
					manager.SetCustomerPrimaryConfigured(configured)
					endpoint, err = manager.selectEndpoint(ctx, "client", nil)
				}
				require.NoError(t, err)
				require.Equal(t, backupURL, endpoint.Url)
				logger.AddMetricForWebSocket(data, nil)
				final := data.Customer.Snapshot()
				require.Equal(t, "backup-provider", final.Provider, "the provider is the configured name relay_end writes, not the url")
				want := "none"
				if configured {
					want = "reroute"
				}
				require.Equal(t, want, final.Recovery)
			})
		}
	}
}
