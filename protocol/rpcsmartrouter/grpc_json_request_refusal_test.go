package rpcsmartrouter

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// MAG-5676's done-when, through the router's gRPC listener with nodes that describe the method: a
// burst of JSON requests that are not JSON the method's message reads, as reads and as writes, is
// answered INVALID_ARGUMENT with one sentence, reaches no node, leaves every endpoint on and every
// provider's score as it was at a --bench-after of 3, and never reports a write as possibly
// submitted. A body that does not parse, is cut short, or puts a { or [ in front of binary bytes is
// refused before a provider is picked; a value of the wrong type is found by the conversion when
// the request is sent, and then nothing is sent either.
func TestGRPCListener_JSONRequestsTheMethodCannotReadAreRefusedAndLeaveEveryProviderOn(t *testing.T) {
	const providers, rounds = 3, 3
	benchAfter(t, 3)
	read := captureRelayEnds(t)
	var addrs []string
	var calls []*atomic.Int32
	for range providers {
		addr, count := startReflectingNode(t)
		addrs = append(addrs, addr)
		calls = append(calls, count)
	}
	client, endpoints, server := startProtobufCheckRouterServing(t, reflectedReadMethod, reflectedWriteMethod, addrs...)

	// A request the conversion reads is served, which also has the router look the method up.
	_, _, err := invokeRaw(t, client, reflectedReadMethod, []byte(`{"id":"7"}`))
	require.NoError(t, err)
	served := func() (total int32) {
		for _, count := range calls {
			total += count.Load()
		}
		return total
	}
	require.Equal(t, int32(1), served())
	time.Sleep(200 * time.Millisecond) // the served read's samples reach the optimizer asynchronously
	reports := providerReports(server, providers)

	bodies := map[string]struct {
		body           []byte
		refusedAtParse bool // refused before a provider is picked
	}{
		"cut short":                    {[]byte(`{"id":`), true},
		"a syntax error":               {[]byte(`{"id" 7}`), true},
		"a [ in front of binary bytes": {[]byte{'[', 0x00, 0x01, 0xff}, true},
		"a value of the wrong type":    {[]byte(`{"inner":5}`), false},
		"an array":                     {[]byte(`[{"id":"7"}]`), false},
	}
	for round := 0; round < rounds; round++ {
		for name, tc := range bodies {
			for _, method := range []string{reflectedReadMethod, reflectedWriteMethod} {
				seen := len(read())
				_, trailer, err := invokeRaw(t, client, method, tc.body)
				require.Equal(t, codes.InvalidArgument, status.Code(err), "%s %s: %v", method, name, err)
				require.Equal(t, routersession.ErrGRPCRequestNotValidJSON.Error(), status.Convert(err).Message(), "%s %s", method, name)
				require.NotEmpty(t, trailer.Get(common.GUID_HEADER_NAME), "the refusal carries the request id")

				events := newRelayEndsSince(read, seen)
				require.Len(t, events, 1, "%s %s: one relay_end", method, name)
				outcome := relayEndField(t, events[0], "outcome")
				require.NotEqual(t, relayOutcomeWriteUnknown, outcome, "%s %s: the request never left the router", method, name)
				if tc.refusedAtParse {
					require.Equal(t, relayOutcomeRejectedBeforeDispatch, outcome, "%s %s", method, name)
				}
			}
		}
	}
	time.Sleep(200 * time.Millisecond) // a sample would reach the optimizer asynchronously

	require.Equal(t, int32(1), served(), "no node received any of them")
	for i, endpoint := range endpoints {
		require.True(t, endpoint.IsEnabled(), "endpoint %d switched off", i)
		require.Zero(t, endpoint.ConnectionRefusals, "endpoint %d marked unhealthy", i)
	}
	require.Equal(t, reports, providerReports(server, providers), "a provider's availability or score moved")
	snapshot := server.sessionManager.ProviderRoutingSnapshot()
	require.Empty(t, snapshot.Blocked, "a provider was blocked")
	require.Len(t, snapshot.ValidAddresses, providers)

	// The router still serves a request the conversion reads. A read, not a write: a write's
	// replies from every node would set the shared message's parsing data at once.
	_, _, err = invokeRaw(t, client, reflectedReadMethod, []byte(`{"id":"8"}`))
	require.NoError(t, err)
	require.Equal(t, int32(2), served())
}

// A JSON write that no provider could convert for want of the method's definition (MAG-5675) also
// never left the router, so it is not reported as possibly submitted either.
func TestGRPCListener_AJSONWriteNoProviderDescribesIsNotPossiblySubmitted(t *testing.T) {
	read := captureRelayEnds(t)
	var addrs []string
	for range 3 {
		addr, _ := startReflectingNode(t)
		addrs = append(addrs, addr)
	}
	client, _, _ := startProtobufCheckRouterServing(t, undescribedReadMethod, undescribedWriteMethod, addrs...)

	seen := len(read())
	_, _, err := invokeRaw(t, client, undescribedWriteMethod, []byte(`{"id":"7"}`))
	require.Error(t, err)
	events := newRelayEndsSince(read, seen)
	require.Len(t, events, 1)
	require.NotEqual(t, relayOutcomeWriteUnknown, relayEndField(t, events[0], "outcome"))
}
