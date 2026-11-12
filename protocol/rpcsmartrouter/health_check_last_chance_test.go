package rpcsmartrouter

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// disableEveryEndpoint switches every endpoint of the fleet off the way the relay path does: by
// failures, up to the threshold. With nothing enabled, a request can only be served through the
// last chance, which lends it one disabled endpoint (MAG-4232).
func disableEveryEndpoint(t *testing.T, fleet *healthRelayFleet) {
	t.Helper()
	for _, endpoint := range fleet.endpoints {
		for i := uint64(0); i < routersession.MaxConsecutiveConnectionAttempts; i++ {
			endpoint.MarkUnhealthy(routersession.EndpointDisableNodeError)
		}
		require.False(t, endpoint.IsEnabled(), "premise: the endpoint is switched off")
	}
}

func enabledEndpoints(fleet *healthRelayFleet) int {
	n := 0
	for _, endpoint := range fleet.endpoints {
		if endpoint.IsEnabled() {
			n++
		}
	}
	return n
}

// TestHealthCheck_ALentURLStaysOffWhenTheHealthCheckSucceeds is MAG-4790.
//
// When every URL of a chain is switched off, the last chance lends one to a request that nothing
// else can serve. The router's own health check sends its latest-block request through the same
// path as a client, so it is lent one too — and when the node answered it, the success reached
// ResetHealth and switched the URL back on. On eth-failover-solo-sim-router that undid, 0.35 s
// later, a URL the MAG-2550 prober was keeping off because eth_getBalance still hung on it.
//
// A health check's answer is not the evidence that brings a URL back. ResetHealth is for "a
// successful relay [that] proves the endpoint healthy for REAL traffic (not just cheap polls)",
// and a lent success gets the full reset because it is "a real customer request that the URL
// answered" (MAG-4232, PR #504). The health check is neither: it is the router's own poll.
//
// So the health check may still be lent a URL — readiness reports whether the router reaches a
// node, and it does — but its success leaves the URL off. The URL comes back the ways it is meant
// to: a real request succeeding on it, or the prober's replay of the request that failed.
func TestHealthCheck_ALentURLStaysOffWhenTheHealthCheckSucceeds(t *testing.T) {
	fleet := newHealthRelayFleetAnswering(t, nil, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`)
	disableEveryEndpoint(t, fleet)

	verdict, _ := fleet.server.sendCraftedRelays(1, false)

	require.Positive(t, fleet.served.Load(),
		"premise: the node was asked, which with every URL off means the health check was lent one")
	require.Equal(t, metrics.ProbeHealthy, verdict,
		"readiness still reports the node reachable: the lend answered")
	require.Zero(t, enabledEndpoints(fleet),
		"the health check's own success must not switch a lent URL back on")
}

// sendTheLatestBlockRequestUnmarked sends the health check's own request through the health
// check's own path, one try of sendRelayWithRetries, but on a UsedProviders that is not marked as
// the health check's. Nothing else differs, so it isolates who is asking as the only cause.
func sendTheLatestBlockRequestUnmarked(t *testing.T, fleet *healthRelayFleet) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	message := fleet.craftedMessage(t)

	usedProviders := routersession.NewUsedProviders(nil)
	usedProviders.SetChainID(fleet.server.listenEndpoint.ChainID)
	stateMachine, err := fleet.server.internalRelayStateMachine(ctx, usedProviders, message)
	require.NoError(t, err)
	relayProcessor := relaycore.NewRelayProcessor(ctx, stateMachine.GetCrossValidationParams(),
		fleet.server.rpcSmartRouterLogs, fleet.server, stateMachine)

	err = fleet.server.sendRelayToEndpoint(ctx, 1, relaycore.GetEmptyRelayState(message), relayProcessor, nil, nil)
	if errors.Is(err, routersession.PairingListEmptyError) {
		relayProcessor.GetUsedProviders().ClearUnwanted()
		err = fleet.server.sendRelayToEndpoint(ctx, 1, relaycore.GetEmptyRelayState(message), relayProcessor, nil, nil)
	}
	require.NoError(t, err)
	require.NoError(t, relayProcessor.WaitForResults(ctx))
}

// TestHealthCheck_ALentURLComesBackForAnyoneElse is the control for the test above, and it holds
// on main as well. The same request, through the same path, lent the same switched-off URL —
// asked by anything other than the health check — still switches the URL back on, because a real
// request that a URL answers is the strongest evidence of recovery (PR #504). Only the health
// check is excluded, so the rule MAG-4232 decided is untouched.
func TestHealthCheck_ALentURLComesBackForAnyoneElse(t *testing.T) {
	fleet := newHealthRelayFleetAnswering(t, nil, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`)
	disableEveryEndpoint(t, fleet)

	sendTheLatestBlockRequestUnmarked(t, fleet)

	require.Positive(t, fleet.served.Load(), "premise: the node was asked through the lend")
	require.Equal(t, 1, enabledEndpoints(fleet),
		"a lent success that is not the health check's switches the one lent URL back on")
}

// TestHealthCheck_ClearsTheFailuresOfAnEndpointThatIsOn pins the half of the rule that did NOT
// change. On an endpoint that is still on, the health check's success resets its failure count,
// as any success does. The health check's own failures count against an endpoint, so its successes
// must clear them too: an idle chain gets no client traffic to do it, and its endpoints would
// otherwise collect health-check blips until one was switched off for nothing.
//
// Read by behaviour rather than by the counter: every endpoint is one failure short of the
// threshold, and after the health check one more failure must leave the endpoint it reached on.
func TestHealthCheck_ClearsTheFailuresOfAnEndpointThatIsOn(t *testing.T) {
	fleet := newHealthRelayFleetAnswering(t, nil, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`)
	for _, endpoint := range fleet.endpoints {
		for i := uint64(0); i < routersession.MaxConsecutiveConnectionAttempts-1; i++ {
			endpoint.MarkUnhealthy(routersession.EndpointDisableNodeError)
		}
		require.True(t, endpoint.IsEnabled(), "premise: one failure short of the threshold, still on")
	}

	verdict, _ := fleet.server.sendCraftedRelays(1, false)
	require.Equal(t, metrics.ProbeHealthy, verdict)

	stillOn := 0
	for _, endpoint := range fleet.endpoints {
		endpoint.MarkUnhealthy(routersession.EndpointDisableNodeError)
		if endpoint.IsEnabled() {
			stillOn++
		}
	}
	require.Positive(t, stillOn,
		"the health check's success cleared the failures of the endpoint it reached, so one more does not switch it off")
}

// Review of #631, 631-2. A lent health check that FAILS must not rewrite the replay evidence of the
// switched-off endpoint it was lent. The evidence is the request that switched the endpoint off, and
// the prober replays it to decide whether the endpoint is back (MAG-2550). If the health check's
// own latest-block poll replaced it, the prober would replay the poll, which the node answers, and
// switch the endpoint on: the MAG-4790 outcome through the prober instead of the health check.
func TestHealthCheck_AFailedLendLeavesTheReplayEvidenceAlone(t *testing.T) {
	fleet := newHealthRelayFleetServing(t, nil, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	disableEveryEndpoint(t, fleet)
	const failingMethod = "eth_getBalance"
	payload := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xa1","latest"]}`)
	for _, endpoint := range fleet.endpoints {
		endpoint.RecordFailingRelay(failingMethod, payload, 5*time.Second)
		require.Equal(t, failingMethod, endpoint.HealthSnapshot().RelayProbeMethod, "premise: the endpoint carries the evidence of what switched it off")
	}

	_, _ = fleet.server.sendCraftedRelays(1, false)

	require.Positive(t, fleet.served.Load(), "premise: the health check was lent an endpoint and the node was asked")
	for _, endpoint := range fleet.endpoints {
		require.Equal(t, failingMethod, endpoint.HealthSnapshot().RelayProbeMethod,
			"the health check's own failed poll must not replace the request the prober replays")
	}
	require.Zero(t, enabledEndpoints(fleet))
}
