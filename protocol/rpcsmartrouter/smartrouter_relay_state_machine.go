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
//
// It reads the caller's cross-validation headers here as well, ahead of the state machine, so a policy
// can be resolved against what the caller asked for — and therefore skips that read for a method that
// routes as a write, whose headers are ignored either way. Without the skip this constructor failed such
// a request outright on a header it could not parse (MAG-3603).
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
		// routesAsWrite, not isWrite: the stateful category is what decides routing, and four of
		// the fifteen methods carrying it are not chain writes at all (see the state machine's
		// own note). What matters here is how the method routes.
		routesAsWrite := chainlib.GetStateful(protocolMessage) == common.CONSISTENCY_SELECT_ALL_PROVIDERS
		// A write's cross-validation headers are not read here, and this is the half of MAG-3603
		// that the state machine cannot fix on its own. Resolve returns (callerParams,
		// callerPresent) for a method that has no policy of its own, so with ANY policy
		// configured — for any method at all — a write's own headers came back as a resolved
		// override, and cvOverride is the machine's FIRST branch, ahead of the write branch by
		// design (Finding A). Routing the write as a write in the machine was therefore inert on
		// every router that had a cross-validation policy.
		//
		// It also stopped the request outright when a value would not parse, before the machine
		// could ignore the headers at all.
		//
		// So the startup guard is NOT what keeps a policy override off a write, which an earlier
		// version of this comment claimed: validateCrossValidationStartup only rejects an ENABLED
		// policy whose own method is stateful, and says nothing about a write with no policy
		// borrowing the caller's headers. This skip is what keeps it off.
		if !forbidCallerCV && !routesAsWrite {
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
			// Two reasons reach here now, and naming the wrong one sends an operator looking for
			// a policy nobody wrote: before the write skip above, this branch was reachable only
			// when a forbid policy existed.
			reason := "forbidden by per-method policy"
			if routesAsWrite {
				reason = "method routes as a write"
			}
			utils.LavaFormatDebug("[CrossValidation] caller cross-validation headers not consulted",
				utils.LogAttr("reason", reason),
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
