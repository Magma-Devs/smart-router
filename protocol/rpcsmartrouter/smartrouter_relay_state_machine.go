package rpcsmartrouter

import (
	"context"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relaypolicy"
	"github.com/magma-Devs/smart-router/utils"
)

// Using interfaces from relaycore
type (
	RelayStateMachine          = relaycore.RelayStateMachine
	ResultsCheckerInf          = relaycore.ResultsCheckerInf
	RelayStateSendInstructions = relaycore.RelayStateSendInstructions
)

// SmartRouterRelayStateMachine is an alias for the unified state machine type.
// Kept for backward compatibility with existing tests.
type SmartRouterRelayStateMachine = relaycore.UnifiedRelayStateMachine

// SmartRouterRelaySender is kept as a type alias for the unified interface
// so that existing test mocks continue to compile.
type SmartRouterRelaySender = relaycore.RelaySenderInf

// SmartRouterStateMachineConfig returns the StateMachineConfig for SmartRouter mode
func SmartRouterStateMachineConfig() relaycore.StateMachineConfig {
	return relaycore.StateMachineConfig{
		EnableCircuitBreaker:    true,
		CircuitBreakerThreshold: 2,
		EnableTimeoutPriority:   true,
		MaxRetries:              MaximumNumberOfTickerRelayRetries,
		SendRelayAttempts:       SendRelayAttempts,
	}
}

// SmartRouterPolicyConfig returns the PolicyConfig for SmartRouter mode
func SmartRouterPolicyConfig() relaypolicy.PolicyConfig {
	return relaypolicy.PolicyConfig{
		MaxRetries:              MaximumNumberOfTickerRelayRetries,
		RelayRetryLimit:         relaycore.RelayRetryLimit,
		DisableBatchRetry:       relaycore.DisableBatchRequestRetry,
		EnableCircuitBreaker:    true,
		CircuitBreakerThreshold: 2,
		SendRelayAttempts:       SendRelayAttempts,
	}
}

// NewSmartRouterRelayStateMachine creates a SmartRouter-mode unified state machine with no per-method
// policy resolver (selection is purely header-driven). Kept for tests and callers that do not wire a
// policy resolver.
func NewSmartRouterRelayStateMachine(
	ctx context.Context,
	usedProviders *lavasession.UsedProviders,
	relaySender SmartRouterRelaySender,
	protocolMessage chainlib.ProtocolMessage,
	analytics *metrics.RelayMetrics,
	debugRelays bool,
) (RelayStateMachine, error) {
	return NewSmartRouterRelayStateMachineWithPolicy(ctx, usedProviders, relaySender, protocolMessage, analytics, debugRelays, nil, "", "")
}

// NewSmartRouterRelayStateMachineWithPolicy is the production constructor: it consults the per-method
// cross-validation policy resolver (which lives in this package — relaycore cannot import it) and, when a
// policy applies, injects the resolved params as an override so the unified state machine selects
// CrossValidation regardless of the method's stateful category. resolver may be nil / empty, in which
// case behavior is identical to the header-driven path.
func NewSmartRouterRelayStateMachineWithPolicy(
	ctx context.Context,
	usedProviders *lavasession.UsedProviders,
	relaySender SmartRouterRelaySender,
	protocolMessage chainlib.ProtocolMessage,
	analytics *metrics.RelayMetrics,
	debugRelays bool,
	resolver *CrossValidationPolicyResolver,
	chainID string,
	apiInterface string,
) (RelayStateMachine, error) {
	var cvOverride *common.CrossValidationParams
	var forbidCallerCV bool
	if resolver.HasPolicies() {
		method := protocolMessage.GetApi().GetName()
		// A forbid-caller-cv policy disables CV for the method: skip the header read entirely (so invalid CV
		// headers do not even error) and signal the state machine to ignore caller headers. This must be
		// checked before Resolve, since Resolve returns applies=false for a forbid policy — which on its own
		// would just let the machine fall back to the caller's headers.
		forbidCallerCV = resolver.ForbidsCallerCV(chainID, apiInterface, method)
		if !forbidCallerCV {
			caller, callerPresent, err := protocolMessage.GetCrossValidationParameters()
			if callerPresent && err != nil {
				return nil, utils.LavaFormatError("invalid cross-validation headers", err, utils.LogAttr("GUID", ctx))
			}
			if eff, applies := resolver.Resolve(chainID, apiInterface, method, caller, callerPresent); applies {
				cvOverride = &eff
				if debugRelays {
					utils.LavaFormatDebug("[CrossValidation] per-method policy resolved",
						utils.LogAttr("chainID", chainID),
						utils.LogAttr("apiInterface", apiInterface),
						utils.LogAttr("method", method),
						utils.LogAttr("maxParticipants", eff.MaxParticipants),
						utils.LogAttr("agreementThreshold", eff.AgreementThreshold),
						utils.LogAttr("minGroups", eff.MinGroups),
						utils.LogAttr("callerHeadersPresent", callerPresent),
						utils.LogAttr("GUID", ctx))
				}
			}
		} else if debugRelays {
			utils.LavaFormatDebug("[CrossValidation] per-method policy forbids caller-driven cross-validation",
				utils.LogAttr("chainID", chainID),
				utils.LogAttr("apiInterface", apiInterface),
				utils.LogAttr("method", method),
				utils.LogAttr("GUID", ctx))
		}
	}

	policy := relaypolicy.NewPolicy(SmartRouterPolicyConfig())
	return relaycore.NewUnifiedRelayStateMachine(
		ctx,
		usedProviders,
		relaySender,
		protocolMessage,
		analytics,
		debugRelays,
		SmartRouterStateMachineConfig(),
		policy,
		cvOverride,
		forbidCallerCV,
	)
}
