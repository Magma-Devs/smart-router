package rpcsmartrouter

import (
	"context"
	"errors"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
)

// Observe only the processor's recorded results at the final decision. Pending
// attempts stay pending; starting a hedge is never itself evidence of recovery.
func captureCustomerDecision(data *metrics.RelayMetrics, message chainlib.ProtocolMessage, processor *relaycore.RelayProcessor, result *common.RelayResult, err error) {
	if data == nil || data.Customer == nil {
		return
	}
	if api := message.GetApi(); api != nil {
		data.ApiMethod, data.ComputeUnits = api.Name, api.ComputeUnits
	}
	var attempts []metrics.CustomerAttempt
	var classified *common.LavaError
	stopReason := ""
	if processor != nil {
		stopReason = processor.GetStopReason()
		successes, nodeErrors, protocolErrors := processor.GetResultsData()
		observed := map[string]bool{}
		for _, success := range successes {
			provider := success.GetProvider()
			observed[provider] = true
			attempts = append(attempts, metrics.CustomerAttempt{Provider: provider, Outcome: "success"})
		}
		for _, failure := range nodeErrors {
			provider := failure.GetProvider()
			observed[provider] = true
			name := common.LavaErrorUnknown.Name
			if failure.CustomerError != nil {
				name = failure.CustomerError.Name
			}
			attempts = append(attempts, metrics.CustomerAttempt{Provider: provider, Outcome: "failure", ErrorName: name})
		}
		for _, failure := range protocolErrors {
			provider := failure.ProviderInfo.ProviderAddress
			observed[provider] = true
			outcome, name := "failure", common.LavaErrorUnknown.Name
			if errors.Is(failure.Err, context.Canceled) {
				outcome = "cancelled"
			}
			if failure.LavaError != nil {
				name = failure.LavaError.Name
			}
			attempts = append(attempts, metrics.CustomerAttempt{Provider: provider, Outcome: outcome, ErrorName: name})
		}
		for _, provider := range processor.GetUsedProviders().DispatchedProviders() {
			if !observed[provider] {
				attempts = append(attempts, metrics.CustomerAttempt{Provider: provider, Outcome: "pending"})
			}
		}
		// Read already-observed identities only. GetBest*ErrorMessageForUser
		// mutates vote counts and is unsafe on an empty list; telemetry must
		// never re-run the processor's decision.
		if err != nil {
			if len(nodeErrors) > 0 {
				classified = nodeErrors[0].CustomerError
			}
			if classified == nil && len(protocolErrors) > 0 {
				classified = protocolErrors[0].LavaError
			}
		}
	}
	data.CaptureCustomerResult(result, stopReason, attempts, classified, errors.Is(err, errUnknownWriteOutcome))
}
