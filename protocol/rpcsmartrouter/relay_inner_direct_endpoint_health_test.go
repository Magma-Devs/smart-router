package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// These tests drive relayInnerDirect against a real HTTP upstream and assert on the endpoint
// counter it actually moves. Everything else covering FAILOVER-TASKS section 2 asserts on the
// classification (IsNodeAtFault) or calls Endpoint.MarkUnhealthy directly, which leaves the wiring
// between the two untested: with the arms at relayInnerDirect deleted outright, the rest of the
// suite stays green. That is the gap this file closes, so keep these end-to-end rather than
// refactoring them down to the gates.
//
// The upstream shapes below are the ones section 2 is about — an error delivered inside an HTTP
// 200, which carries no Go error and no failing status, so before the fix it moved the counter in
// neither direction.

// benchAfter pins the disable threshold for one test and restores it. Low, so a case can cross the
// threshold in a readable number of relays.
func benchAfter(t *testing.T, value uint64) {
	t.Helper()
	original := lavasession.MaxConsecutiveConnectionAttempts
	t.Cleanup(func() { lavasession.MaxConsecutiveConnectionAttempts = original })
	lavasession.MaxConsecutiveConnectionAttempts = value
}

// directRelayHarness wires one endpoint to one upstream through the real relay path.
type directRelayHarness struct {
	rpcss    *RPCSmartRouterServer
	session  *lavasession.SingleConsumerSession
	endpoint *lavasession.Endpoint
}

// relay runs one relay and reports the endpoint's refusal count and enabled bit afterwards.
// hasNodeError drives the JSON-RPC sender's CheckResponseError branch, which is what makes a 200
// body carrying {"error":...} classify as a node error at all.
func (h *directRelayHarness) relay(t *testing.T, hasNodeError bool, message string) (refusals uint64, enabled bool) {
	t.Helper()
	msg := &mockChainMessage{
		requestData: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`),
	}
	if hasNodeError {
		msg.checkResponseError = func([]byte, int) (bool, string) { return true, message }
	}
	relayResult := &common.RelayResult{}
	_, _, _ = h.rpcss.relayInnerDirect(
		context.Background(), h.session, relayResult, 5*time.Second, msg, msg.requestData, nil,
	)
	// Read directly: the relay above ran synchronously on this goroutine, so there is no
	// concurrent writer. IsEnabled takes the lock; ConnectionRefusals has no exported accessor.
	return h.endpoint.ConnectionRefusals, h.endpoint.IsEnabled()
}

func newDirectRelayHarness(t *testing.T, status int, body string) *directRelayHarness {
	t.Helper()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(upstream.Close)

	directConn, err := lavasession.NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: upstream.URL}, 5, "")
	require.NoError(t, err)

	endpoint := &lavasession.Endpoint{
		NetworkAddress:    upstream.URL,
		Enabled:           true,
		DirectConnections: []lavasession.DirectRPCConnection{directConn},
	}
	cswp := &lavasession.ConsumerSessionsWithProvider{
		PublicLavaAddress: "upstream-under-test",
		PairingEpoch:      100,
		Endpoints:         []*lavasession.Endpoint{endpoint},
	}

	return &directRelayHarness{
		// A nil metrics manager is safe: every setter on it is nil-guarded, and the counter this
		// asserts on lives on the Endpoint, not in the metric.
		rpcss: &RPCSmartRouterServer{
			listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		},
		session: &lavasession.SingleConsumerSession{
			Parent: cswp,
			Connection: &lavasession.DirectRPCSessionConnection{
				DirectConnection: directConn,
				EndpointAddress:  upstream.URL,
				Endpoint:         endpoint,
			},
		},
		endpoint: endpoint,
	}
}

// THE freeze case, end to end. An upstream answering every request with an error inside an HTTP 200
// must climb the counter and be taken out — the behaviour section 2 exists to deliver.
//
// Delete the `if result.IsNodeAtFault` arm in relayInnerDirect and this is the test that fails.
func TestRelayInnerDirect_NodeErrorInA200DisablesTheEndpoint(t *testing.T) {
	benchAfter(t, 3)
	h := newDirectRelayHarness(t, http.StatusOK,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`)

	for want := uint64(1); want <= 2; want++ {
		refusals, enabled := h.relay(t, true, "internal error")
		require.Equal(t, want, refusals, "a node error inside a 200 must raise the counter")
		require.True(t, enabled, "still under the threshold")
	}

	refusals, enabled := h.relay(t, true, "internal error")
	require.Equal(t, uint64(3), refusals)
	require.False(t, enabled,
		"the third consecutive node error must disable the endpoint — this is the whole freeze fix")
}

// A clean answer is the only thing that clears the count.
func TestRelayInnerDirect_CleanAnswerResetsTheCounter(t *testing.T) {
	benchAfter(t, 5)
	h := newDirectRelayHarness(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0xabc"}`)

	h.endpoint.MarkUnhealthy()
	h.endpoint.MarkUnhealthy()

	refusals, enabled := h.relay(t, false, "")
	require.Zero(t, refusals, "a clean 2xx is positive proof the endpoint serves")
	require.True(t, enabled)
}

// The third outcome, and the one a two-way gate cannot express.
//
// "Transaction not found" is the endpoint answering honestly about data it does not hold. It must
// neither climb the counter nor clear it: an endpoint two failures into its budget that then
// answers one of these is still two failures in.
//
// This is the case that fails if relayProvesEndpointHealthy is ever widened back into the negation
// of the failure gate — under that rule a blameless answer counted as a success and reset to zero.
func TestRelayInnerDirect_BlamelessAnswerNeitherBlamesNorCertifies(t *testing.T) {
	benchAfter(t, 5)
	h := newDirectRelayHarness(t, http.StatusOK,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"transaction not found"}}`)

	h.endpoint.MarkUnhealthy()
	h.endpoint.MarkUnhealthy()

	for i := 0; i < 3; i++ {
		refusals, enabled := h.relay(t, true, "transaction not found")
		require.Equal(t, uint64(2), refusals,
			"a customer polling for a pending transaction must neither disable the endpoints it "+
				"asks nor wipe their real failure history")
		require.True(t, enabled)
	}
}

// The hole in the other direction. Under the old reset rule an unrecognised 4xx was not a failure,
// so it was a success, so it reset the counter — an endpoint one failure short of its budget that
// answered a strange 403 looked perfectly healthy again.
func TestRelayInnerDirect_UnrecognisedStatusDoesNotCertifyHealth(t *testing.T) {
	benchAfter(t, 5)
	h := newDirectRelayHarness(t, http.StatusForbidden, `{"error":"go away"}`)

	h.endpoint.MarkUnhealthy()
	h.endpoint.MarkUnhealthy()

	refusals, _ := h.relay(t, false, "")
	require.Equal(t, uint64(2), refusals,
		"an answer we cannot interpret is not proof the endpoint recovered")
}

// Why the two arms in relayInnerDirect are chained rather than written as two independent ifs.
//
// The obvious reading is that they are already exclusive, so the chaining is decoration. They are
// not. This pins the counter-example: the blame gate and the reset gate read DIFFERENT fields, so a
// result can satisfy both at once. IsNodeAtFault comes from the error registry via the reply body;
// relayProvesEndpointHealthy reads IsNodeError, which on REST is set from the HTTP status alone.
//
// A REST 200 whose body classifies as a node fault is exactly that shape: it would count the
// endpoint up and then immediately reset it to zero, wiping the whole failure history. No registry
// row produces it today — REST's table carries only capability codes — which is why this asserts on
// the gates rather than driving a relay. The chaining is what makes it unreachable by construction
// when a REST matcher for a node-fault code is eventually added.
//
// If this test ever starts failing, the gates have become genuinely exclusive and the `else if` is
// then safe to relax. Until it does, do not.
func TestRelayInnerDirect_TheTwoGatesAreNotMutuallyExclusive(t *testing.T) {
	restFaultIn200 := &common.RelayResult{
		StatusCode:    200,
		IsNodeError:   false, // REST derives this from the status, not from the body
		IsNodeAtFault: true,  // while the registry classified the body as the node's fault
	}

	require.True(t, restFaultIn200.IsNodeAtFault, "the blame gate fires")
	require.True(t, relayProvesEndpointHealthy(restFaultIn200),
		"and so does the reset gate — the two read different fields, so both can be true at once. "+
			"relayInnerDirect chains its arms so only the first one can act")
}
