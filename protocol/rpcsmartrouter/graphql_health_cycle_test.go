package rpcsmartrouter

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// A GraphQL provider is taken out and brought back by the rules every interface follows; nothing
// GraphQL-only is added (MAG-5124). This walks the whole cycle on a GraphQL router, with real
// reads through the relay path, for a provider whose head polls pass while its reads fail:
//
//	50 failed reads in a row       -> out
//	3 healthy polls                -> back, on a 3-failure probation
//	3 failed reads on probation    -> out again
//	3 healthy polls                -> still out: the wait doubled
//	6 healthy polls                -> back, on probation
//	a caller's error on probation  -> proves nothing: the probation budget stays where it was
//	one clean answer on probation  -> the full budget is back
//
// A failed read here is Sui's database-down reply: an HTTP 200 carrying INTERNAL_SERVER_ERROR.
func TestRelayInnerDirect_GraphQLProviderFollowsTheSharedHealthCycle(t *testing.T) {
	const (
		read   = `{"query":"{ chainIdentifier }"}`
		fault  = `{"data":null,"errors":[{"message":"Internal error occurred while processing request","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`
		caller = `{"errors":[{"message":"bad","extensions":{"code":"BAD_USER_INPUT"}}]}`
		answer = `{"data":{"chainIdentifier":"35834a8a"}}`
		// The probe loop's default: healthy polls in a row before a first re-enable.
		k = 3
	)
	var body atomic.Value
	body.Store(fault)
	h := newDirectRelayHarnessServing(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body.Load().(string)))
	})
	h.rpcss.listenEndpoint = &routersession.RPCEndpoint{ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGraphQL}
	chainParser := newGatewayTestGraphQLParser(t)

	relay := func() {
		t.Helper()
		chainMessage, err := chainParser.ParseMsg("", []byte(read), http.MethodPost, nil, extensionslib.ExtensionInfo{})
		require.NoError(t, err)
		_, _ = h.rpcss.relayInnerDirect(
			context.Background(), h.session, &common.RelayResult{}, 5*time.Second, 30*time.Second,
			chainMessage, []byte(read), func() bool { return false }, nil,
			false, // not the router's health check
		)
	}
	pollTime := time.Now()
	healthyPolls := func(count int) {
		for i := 0; i < count; i++ {
			pollTime = pollTime.Add(time.Second)
			h.endpoint.RecordProbeVerdict(pollTime, true, k)
		}
	}

	full := int(routersession.MaxConsecutiveConnectionAttempts)
	require.Equal(t, 50, full, "premise: the full budget is 50 failures in a row")
	for i := 1; i < full; i++ {
		relay()
		require.True(t, h.endpoint.IsEnabled(), "still in after %d failed reads", i)
	}
	relay()
	require.False(t, h.endpoint.IsEnabled(), "out after 50 failed reads in a row")

	healthyPolls(k - 1)
	require.False(t, h.endpoint.IsEnabled(), "2 healthy polls are not enough")
	healthyPolls(1)
	require.True(t, h.endpoint.IsEnabled(), "back after 3 healthy polls")

	relay()
	relay()
	require.True(t, h.endpoint.IsEnabled(), "probation allows 2 failures")
	relay()
	require.False(t, h.endpoint.IsEnabled(), "out again after 3 failures on probation")

	healthyPolls(k)
	require.False(t, h.endpoint.IsEnabled(), "3 healthy polls no longer bring it back: the wait doubled")
	healthyPolls(k)
	require.True(t, h.endpoint.IsEnabled(), "back after 6 healthy polls")

	onProbation := h.endpoint.ConnectionRefusals
	body.Store(caller)
	relay()
	require.Equal(t, onProbation, h.endpoint.ConnectionRefusals, "a caller's error neither spends the probation budget nor restores the full one")
	require.True(t, h.endpoint.IsEnabled())

	body.Store(answer)
	relay()
	require.Zero(t, h.endpoint.ConnectionRefusals, "one clean answer restores the full budget")
	require.True(t, h.endpoint.IsEnabled())
}
