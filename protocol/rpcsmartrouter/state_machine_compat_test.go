package rpcsmartrouter

import (
	"context"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
)

// Test-only aliases and the policy-less constructor. Production builds the state machine through
// NewSmartRouterRelayStateMachineWithPolicy; these names exist so the existing tests keep compiling
// without carrying the aliases in the binary.
type (
	ResultsCheckerInf            = relaycore.ResultsCheckerInf
	RelayStateSendInstructions   = relaycore.RelayStateSendInstructions
	SmartRouterRelayStateMachine = relaycore.UnifiedRelayStateMachine
)

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
