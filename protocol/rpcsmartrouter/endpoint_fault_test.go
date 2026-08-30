package rpcsmartrouter

import (
	"errors"
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// jsonrpcNodeError builds the relay result an upstream produces when it answers HTTP 200 with a
// JSON-RPC error in the body — the shape that used to freeze the endpoint counter.
//
// It goes through ApplyNodeErrorClassification rather than setting flags by hand, so the fault
// verdict under test is the one the registry actually produces.
func jsonrpcNodeError(code int, message string) *common.RelayResult {
	rr := &common.RelayResult{StatusCode: 200, IsNodeError: true}
	rr.ApplyNodeErrorClassification(common.ChainFamilyEVM, common.TransportJsonRPC, code, message)
	return rr
}

// The endpoint counter has three outcomes, and this is the table for which answer produces which
// (FAILOVER-TASKS section 2, decision 4).
//
//	the endpoint was at fault        -> +1        blame
//	the relay demonstrably succeeded -> reset     proof
//	anything else                    -> untouched
//
// Before this change there were only two outcomes and a whole class of answers reached NEITHER: a
// node error inside an HTTP 200 has err == nil and status 200, so no increment rule was reachable
// while the reset rule returned false. The counter froze — which is why an upstream answering every
// single request with an error was never disabled.
func TestEndpointFault_AnswerShapes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		result     *common.RelayResult
		blame      bool
		provesGood bool
		why        string
	}{
		{
			name:   "internal error in a 200 body",
			result: jsonrpcNodeError(-32603, "internal error"),
			blame:  true,
			why:    "THE freeze case: the node is telling us it is broken and this used to move nothing",
		},
		{
			name:   "method disabled on this node",
			result: jsonrpcNodeError(-32004, "method not supported"),
			why:    "a full node is not broken for being a full node; the retry reaches the archive tier",
		},
		{
			name:   "nonce too low",
			result: jsonrpcNodeError(-32000, "nonce too low"),
			why:    "the caller's fault — a customer retry loop must not walk the fleet out of rotation",
		},
		{
			name:   "transaction not found",
			result: jsonrpcNodeError(-32000, "transaction not found"),
			why:    "every endpoint says this for a pending tx; blaming all of them is the amplification bug",
		},
		{
			name:   "rate limited",
			result: jsonrpcNodeError(-32005, "rate limit exceeded"),
			why:    "healthy but busy; the hold-off registry owns it",
		},
		{
			name:   "generic server error (-32000)",
			result: jsonrpcNodeError(-32000, "flux capacitor desynchronised"),
			blame:  true,
			why:    "-32000 IS recognised, as NODE_SERVER_ERROR — the node saying it broke, whatever the prose",
		},
		{
			name:   "unrecognised error body",
			result: jsonrpcNodeError(-39999, "flux capacitor desynchronised"),
			why:    "no code or message matcher hits, so UNKNOWN_ERROR: not fault, and no reset either",
		},
		{
			name:       "clean 200",
			result:     &common.RelayResult{StatusCode: 200},
			provesGood: true,
			why:        "the path virtually every relay takes",
		},
		{
			name:       "clean gRPC OK",
			result:     &common.RelayResult{StatusCode: 0},
			provesGood: true,
			why:        "gRPC statuses are 0-16 and never reach the 2xx range",
		},
		{
			name:   "unrecognised 403",
			result: &common.RelayResult{StatusCode: 403},
			why:    "used to count as a SUCCESS and reset the counter to zero",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.blame, tc.result.IsNodeAtFault, "blame verdict: %s", tc.why)
			require.Equal(t, tc.provesGood, relayProvesEndpointHealthy(tc.result), "health proof: %s", tc.why)
			require.False(t, tc.blame && tc.provesGood, "an answer can never both blame and certify")
		})
	}
}

// A relay that is blamed must not also be a relay the endpoint counter treats as recovery, and vice
// versa. The two gates are no longer negations of each other, so nothing structurally prevents an
// answer falling into both — this is the check that would catch it.
func TestEndpointFault_BlameAndProofAreDisjoint(t *testing.T) {
	for _, code := range []int{-32603, -32601, -32602, -32000, -32001, -32004, -32005, -39999} {
		rr := jsonrpcNodeError(code, "")
		require.False(t, rr.IsNodeAtFault && relayProvesEndpointHealthy(rr),
			"JSON-RPC %d both blames and certifies the endpoint", code)
	}
}

// The verdict must actually reach the counter that disables a URL.
//
// The table above pins which answers blame; this pins that a blaming answer moves
// Endpoint.ConnectionRefusals and crosses the threshold, and that a blameless one leaves it alone.
// Without this the fault axis could be perfectly correct and wired to nothing.
func TestEndpointFault_CounterMovesOnlyOnFault(t *testing.T) {
	endpointWith := func(refusals uint64) *lavasession.Endpoint {
		e := &lavasession.Endpoint{NetworkAddress: "http://fault-test", Enabled: true}
		for i := uint64(0); i < refusals; i++ {
			e.MarkUnhealthy()
		}
		return e
	}

	atFault := jsonrpcNodeError(-32603, "internal error")
	require.True(t, atFault.IsNodeAtFault, "precondition: the freeze case blames")

	// One short of the threshold, the blaming answer is what disables it.
	e := endpointWith(uint64(lavasession.MaxConsecutiveConnectionAttempts) - 1)
	require.True(t, e.Enabled, "precondition: still enabled one failure short")
	e.MarkUnhealthy() // the relay path's call, gated on IsNodeAtFault
	require.False(t, e.Enabled,
		"a node error inside a 200 must be able to disable the endpoint — this is the whole freeze fix")

	// A blameless answer never reaches MarkUnhealthy, so an endpoint sitting one short stays up
	// however many of them arrive.
	blameless := jsonrpcNodeError(-32000, "transaction not found")
	require.False(t, blameless.IsNodeAtFault, "precondition: not-found does not blame")

	survivor := endpointWith(uint64(lavasession.MaxConsecutiveConnectionAttempts) - 1)
	for i := 0; i < 100; i++ {
		if blameless.IsNodeAtFault { // never true; mirrors the relay path's gate
			survivor.MarkUnhealthy()
		}
	}
	require.True(t, survivor.Enabled,
		"a customer polling for a pending transaction must never disable the endpoints it asks")
}

// "Unrecognised" means two different things, and they get opposite verdicts. This pins the split,
// because collapsing it is an easy and expensive mistake in either direction.
//
//	an unrecognised ANSWER    -> not fault. The node replied; we cannot interpret it, and absence
//	                             of information is not evidence of a broken endpoint (decision 4).
//	an unrecognised FAILURE   -> fault. The relay produced no answer at all. An EOF or a novel dial
//	                             error is the endpoint failing us, named or not.
//
// Getting this backwards the first time is what TestGenuineFaults_StillPenalised caught: because
// LavaErrorUnknown is CategoryExternal by construction, excusing it wholesale silently stopped
// penalising every uncatalogued transport failure.
func TestEndpointFault_UnrecognisedAnswerAndUnrecognisedFailureDiffer(t *testing.T) {
	answer := jsonrpcNodeError(-39999, "flux capacitor desynchronised")
	require.False(t, answer.IsNodeAtFault,
		"an answer we cannot interpret is not evidence the endpoint is broken")
	require.False(t, relayProvesEndpointHealthy(answer),
		"nor is it evidence the endpoint is working — the counter is left alone")

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"eof", fmt.Errorf("http request failed: %w", errors.New("EOF"))},
		{"novel transport error", errors.New("some totally novel transport thing")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			classified := extractLavaError(classifyAndWrap(tc.err, common.ChainFamily(-1), common.TransportJsonRPC))
			require.Equal(t, common.LavaErrorUnknown, classified, "precondition: genuinely unclassified")

			atFault, backoff := classifyEndpointHealth(classified, false)
			require.True(t, atFault,
				"a failure that produced no answer must still blame the endpoint, catalogued or not")
			require.True(t, backoff)
		})
	}
}
