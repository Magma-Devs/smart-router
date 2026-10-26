package rpcsmartrouter

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-3746 is a WIRING bug: one call site in sendRelayWithRetries built the health check's state
// machine with the per-method cross-validation resolver. The sibling file's cases prove
// internalRelayStateMachine's contract one frame below that call, so restoring the old constructor
// at the call site leaves them green — the review's finding. These drive the real health path,
// sendCraftedRelays → craftRelay → sendRelayWithRetries → sendRelayToEndpoint, against a live
// upstream, so the call site itself is what they read.

// healthRelayFleet is one endpoint's worth of router: a real chain parser, a session manager over
// three providers that all answer from one upstream, and the served-request counter. Three
// providers across two groups so the report's policy (max-participants 3, min-groups 2) is
// satisfiable by the fleet — the health check still takes ONE session out of it, which is the
// whole bug.
type healthRelayFleet struct {
	server   *RPCSmartRouterServer
	served   *atomic.Int32
	resolver *CrossValidationPolicyResolver
}

// newHealthRelayFleet wires the fleet under the given policy, written for the latest-block method.
func newHealthRelayFleet(t *testing.T, policy CrossValidationPolicy) *healthRelayFleet {
	t.Helper()
	return newHealthRelayFleetAnswering(t, &policy, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`)
}

// newHealthRelayFleetAnswering is newHealthRelayFleet with the upstream's reply chosen. A nil
// policy configures none.
func newHealthRelayFleetAnswering(t *testing.T, policy *CrossValidationPolicy, reply string) *healthRelayFleet {
	t.Helper()
	return newHealthRelayFleetServing(t, policy, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(reply))
	})
}

// newHealthRelayFleetServing is newHealthRelayFleetAnswering with the upstream's handler chosen.
// served counts every request the handler receives.
func newHealthRelayFleetServing(t *testing.T, policy *CrossValidationPolicy, handler http.HandlerFunc) *healthRelayFleet {
	t.Helper()
	rand.InitRandomSeed()
	ctx := t.Context()

	var served atomic.Int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		handler(w, r)
	})
	chainParser, _, _, closeServer, providerEndpoint, err := chainlib.CreateChainLibMocks(
		ctx, "ETH1", spectypes.APIInterfaceJsonRPC, upstream, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)

	// Three providers, one upstream. Each holds its own connection, so session selection has a real
	// choice to make and the relay it picks reaches a real node.
	pairingList := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
	for i, group := range []string{"group-a", "group-a", "group-b"} {
		conn, connErr := lavasession.NewDirectRPCConnection(ctx, providerEndpoint.NodeUrls[0], 5, spectypes.APIInterfaceJsonRPC)
		require.NoError(t, connErr)
		t.Cleanup(func() { _ = conn.Close() })
		provider := lavasession.NewConsumerSessionWithProvider(
			"upstream-"+string(rune('a'+i)),
			[]*lavasession.Endpoint{{
				NetworkAddress:    providerEndpoint.NodeUrls[0].Url,
				Enabled:           true,
				DirectConnections: []lavasession.DirectRPCConnection{conn},
			}},
			100000, 1, 1)
		provider.StaticProvider = true
		provider.GroupLabel = group
		pairingList[uint64(i)] = provider
	}
	sessionManager, rpcEndpoint := createTestSessionManager("ETH1", spectypes.APIInterfaceJsonRPC)
	rpcEndpoint.NetworkAddress = "127.0.0.1:0"
	require.NoError(t, sessionManager.UpdateAllProviders(1, pairingList, nil))

	var config CrossValidationConfig
	if policy != nil {
		config.Policies = []CrossValidationPolicyEntry{{
			ChainID: "ETH1", ApiInterface: "jsonrpc", Method: "eth_blockNumber",
			CrossValidationPolicy: *policy,
		}}
	}
	resolver, err := NewCrossValidationPolicyResolver(config)
	require.NoError(t, err)

	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	return &healthRelayFleet{
		served:   &served,
		resolver: resolver,
		server: &RPCSmartRouterServer{
			chainParser:             chainParser,
			sessionManager:          sessionManager,
			listenEndpoint:          rpcEndpoint,
			rpcSmartRouterLogs:      logs,
			crossValidationResolver: resolver,
		},
	}
}

// craftedMessage is the message the health path builds for itself, through craftRelay — so the
// premise "the crafted relay resolves to the method the policy names" is read off production code
// rather than restated by the test.
func (f *healthRelayFleet) craftedMessage(t *testing.T) chainlib.ProtocolMessage {
	t.Helper()
	ok, relay, chainMessage, err := f.server.craftRelay(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "eth_blockNumber", chainMessage.GetApi().Name,
		"premise: the crafted health relay must resolve to the method the policy names, or this proves nothing")
	return chainlib.NewProtocolMessage(chainMessage, nil, relay, initRelaysDappId, initRelaysSmartRouterIp)
}

// thresholdOnlyPolicy varies the agreement threshold and nothing else: no min-groups floor, so a
// refusal under it can only be the threshold's. The report's own policy (theReportsPolicy, in the
// sibling file) also carries min-groups 2, which refuses a one-session check on its own account.
func thresholdOnlyPolicy(threshold int) CrossValidationPolicy {
	return CrossValidationPolicy{
		Enabled:            true,
		MaxParticipants:    Bound{Floor: new(3), Cap: new(3)},
		AgreementThreshold: Bound{Floor: new(threshold), Cap: new(3)},
	}
}

// TestSendCraftedRelays_IgnoresCrossValidationPolicy drives the real health path under the report's
// policy at every threshold an operator could write. Restore the pre-fix constructor at
// rpcsmartrouter_server.go's sendRelayWithRetries —
//
//	NewSmartRouterRelayStateMachineWithPolicy(ctx, usedProviders, rpcss, protocolMessage, nil,
//	    rpcss.debugRelays, rpcss.crossValidationResolver, chainID, apiInterface)
//
// — and every case fails, which is MAG-3746 verbatim.
func TestSendCraftedRelays_IgnoresCrossValidationPolicy(t *testing.T) {
	for _, threshold := range []int{1, 2, 3} {
		t.Run(thresholdName(threshold), func(t *testing.T) {
			fleet := newHealthRelayFleet(t, theReportsPolicy(threshold))

			// Premise: the policy is live and really would bind this method for a CLIENT request,
			// with the operator's own threshold. Without this the case below would pass equally
			// against a resolver that had quietly stopped working.
			clientSM, err := NewSmartRouterRelayStateMachineWithPolicy(t.Context(), lavasession.NewUsedProviders(nil),
				fleet.server, fleet.craftedMessage(t), nil, false, fleet.resolver, "ETH1", "jsonrpc")
			require.NoError(t, err)
			require.Equal(t, relaycore.CrossValidation, clientSM.GetSelection())
			require.Equal(t, threshold, clientSM.GetCrossValidationParams().AgreementThreshold)

			success, err := fleet.server.sendCraftedRelays(1, false)
			require.NoError(t, err)
			require.True(t, success,
				"the health check must pass under a policy on its own method — threshold %d", threshold)
			require.Positive(t, fleet.served.Load(),
				"and it must pass by actually reaching a node, not by being skipped")
		})
	}
}

// TestHealthRelayUnderThePreFixWiring pins what the pre-fix wiring did, so the numbers in
// docs/CROSS-VALIDATION.md are an artifact rather than an assertion. It reconstructs that wiring
// from the same three production calls sendRelayWithRetries used to make — the state machine built
// WITH the resolver, a processor over it, and one dispatch of one endpoint — so the refusal it reads
// is the real one, not a restatement.
//
// Two dimensions, kept apart. With the threshold as the only knob, 1 passed (one session is not
// fewer than one) and 2 and 3 could not be met. The report's own policy also carries min-groups 2,
// and that floor refuses a one-session check by itself, threshold 1 included — so on the reported
// configuration no threshold was a workaround.
func TestHealthRelayUnderThePreFixWiring(t *testing.T) {
	for _, tc := range []struct {
		name     string
		policy   CrossValidationPolicy
		refusal  string // empty: the relay must go through
		received int32  // requests the upstream must have seen afterwards
	}{
		{name: "threshold 1, no min-groups: served", policy: thresholdOnlyPolicy(1), received: 1},
		{name: "threshold 2, no min-groups: refused on the threshold", policy: thresholdOnlyPolicy(2), refusal: "insufficient sessions for cross-validation consensus"},
		{name: "threshold 3, no min-groups: refused on the threshold", policy: thresholdOnlyPolicy(3), refusal: "insufficient sessions for cross-validation consensus"},
		{name: "the report's policy at threshold 1: refused on min-groups", policy: theReportsPolicy(1), refusal: "insufficient provider groups for cross-validation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fleet := newHealthRelayFleet(t, tc.policy)
			ctx := t.Context()
			message := fleet.craftedMessage(t)

			usedProviders := lavasession.NewUsedProviders(nil)
			usedProviders.SetChainID("ETH1")
			// The pre-fix constructor, verbatim.
			stateMachine, err := NewSmartRouterRelayStateMachineWithPolicy(ctx, usedProviders, fleet.server,
				message, nil, false, fleet.resolver, "ETH1", "jsonrpc")
			require.NoError(t, err)
			require.Equal(t, relaycore.CrossValidation, stateMachine.GetSelection(),
				"premise: the pre-fix wiring is what put a health check into cross-validation at all")

			relayProcessor := relaycore.NewRelayProcessor(ctx, stateMachine.GetCrossValidationParams(),
				fleet.server.rpcSmartRouterLogs, fleet.server, stateMachine)
			// numOfEndpoints 1 is the health path's own fan-out: a health check asks one provider.
			err = fleet.server.sendRelayToEndpoint(ctx, 1, relaycore.GetEmptyRelayState(message), relayProcessor, nil, nil)

			if tc.refusal == "" {
				require.NoError(t, err, "one session is not fewer than one, so threshold 1 alone was never the bug")
				require.NoError(t, relayProcessor.WaitForResults(ctx))
				require.Equal(t, tc.received, fleet.served.Load(), "and the one node was asked")
				return
			}
			require.Error(t, err, "one session cannot satisfy this policy")
			require.ErrorIs(t, err, lavasession.PairingListEmptyError)
			require.Contains(t, err.Error(), tc.refusal, "the refusal an operator saw on every health check")
			require.Zero(t, fleet.served.Load(),
				"refused before anything was sent — which is why no node was ever asked whether it was healthy")
		})
	}
}

// TestSendCraftedRelays_ErrorAnswerIsNotHealthy is MAG-4617. ProcessingResult returns a node
// error with err == nil when nothing succeeded, and the health check took any reply as a pass. In
// production the reply was a gRPC call that hit the try's own deadline, which the gRPC transport
// reports as a node error: sui-testnet stayed Ready for seven minutes without egress, its health
// check "succeeding" with latestBlock 0. Every upstream here answers the latest-block call with an
// error, so the check must fail however many tries it gets.
func TestSendCraftedRelays_ErrorAnswerIsNotHealthy(t *testing.T) {
	fleet := newHealthRelayFleetAnswering(t, nil,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`)

	success, _ := fleet.server.sendCraftedRelays(2, false)
	require.Positive(t, fleet.served.Load(), "premise: the upstreams were asked")
	require.False(t, success, "an error answer is not a healthy upstream")
}

// The same fleet answering with a result passes, so the case above fails on the answer alone.
func TestSendCraftedRelays_ResultAnswerIsHealthy(t *testing.T) {
	fleet := newHealthRelayFleetAnswering(t, nil, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`)

	success, err := fleet.server.sendCraftedRelays(2, false)
	require.NoError(t, err)
	require.True(t, success)
}

// TestSendCraftedRelays_AStalledTryDoesNotAnswerTheNext: an attempt cut off at its try's deadline
// still delivers its response, late. Were that response read by the next try, the next try would
// take it as its own answer and cut off its own attempt microseconds after sending it, passing the
// same late response down to the try after, so one stall would fail every try of the check. The
// upstream here stalls on its first request and then answers each request with a result after
// 50 ms: slower than reading a response already waiting, far faster than the try deadline. The
// check must pass on its second try.
func TestSendCraftedRelays_AStalledTryDoesNotAnswerTheNext(t *testing.T) {
	var requests atomic.Int32
	release := make(chan struct{})
	fleet := newHealthRelayFleetServing(t, nil, func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			// Stalled. The router's deadline cuts the request off on its side; the test server is
			// not told, so the handler waits for the test to end.
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`))
	})
	t.Cleanup(func() { close(release) }) // runs before the fleet's cleanups close the server

	start := time.Now()
	success, err := fleet.server.sendCraftedRelays(MaxRelayRetries, false)
	require.GreaterOrEqual(t, time.Since(start), internalRelayTryTimeout, "premise: the first try stalled to its deadline")
	require.GreaterOrEqual(t, fleet.served.Load(), int32(2), "premise: a later try reached the upstream")
	require.NoError(t, err)
	require.True(t, success, "the try after the stall got a result from the upstream")
}

func thresholdName(threshold int) string {
	return "agreement threshold " + string(rune('0'+threshold))
}
