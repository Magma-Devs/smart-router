package rpcsmartrouter

import (
	"context"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// rateLimitWordingBody is a node's JSON-RPC error that says it rate-limited us, with no HTTP 429
// and no chain row behind it.
func rateLimitWordingBody(message string) string {
	return `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"` + message + `"}}`
}

// TestJSONRPCWrite_RateLimitWordingIsNotSentAgain_MAG5487 broadcasts a write to two primaries that
// both answer it inside an HTTP 200 with a JSON-RPC error saying they rate-limited us, with a backup
// standing by. The wording is no rate limit (a rate limit is an HTTP 429 or a chain's own row), so
// the all-rate-limited exception does not apply and the write goes out once per primary, never to
// the backup. Read as a rate limit, every copy was one, and the write was sent again.
func TestJSONRPCWrite_RateLimitWordingIsNotSentAgain_MAG5487(t *testing.T) {
	rand.InitRandomSeed()
	tc := writeInterfaceCases[0]
	require.Equal(t, spectypes.APIInterfaceJsonRPC, tc.apiInterface, "setup: the JSON-RPC write case")
	for _, message := range []string{"rate limit exceeded", "enhance_your_calm"} {
		t.Run(message, func(t *testing.T) {
			chainParser := tc.parser(t)
			limited := writeUpstream{status: http.StatusOK, body: rateLimitWordingBody(message)}
			first, second := limited, limited
			first.name, second.name = "a-"+t.Name(), "b-"+t.Name()
			backup := writeUpstream{name: "backup-" + t.Name(), status: http.StatusOK, body: tc.success}
			hits := make([]int64, 3)
			server := newWriteServer(t, tc, chainParser, []writeUpstream{first, second}, []writeUpstream{backup}, hits)

			_, _ = sendWrite(t, tc, chainParser, server)
			// A detached delivery may still be running for up to statefulDeliveryLinger; past it, a
			// copy not landed never lands.
			time.Sleep(statefulDeliveryLinger)
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]), "the first primary received the write once")
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]), "the second primary received the write once")
			require.Zero(t, atomic.LoadInt64(&hits[2]), "the write was sent again, to the backup")
		})
	}
}

// TestJSONRPCRead_RateLimitWordingHoldsNoProviderOff_MAG5487 sends a read to an upstream that
// answers it inside an HTTP 200 with a JSON-RPC error saying it rate-limited us. The read still
// goes to the peer, but the provider is not held off as if it had: the wording is no rate limit.
func TestJSONRPCRead_RateLimitWordingHoldsNoProviderOff_MAG5487(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	for _, message := range []string{"rate limit exceeded", "enhance_your_calm"} {
		t.Run(message, func(t *testing.T) {
			refuser := refusalReply{http.StatusOK, rateLimitWordingBody(message)}
			hits, server := newRefusalTestServer(t, chainParser, logs, refuser, refusalReply{http.StatusOK, refusalPeerAnswer})

			result, sendErr := sendRefusalTestRequest(t, ctx, chainParser, server)
			require.NoError(t, sendErr)
			require.JSONEq(t, refusalPeerAnswer, string(result.Reply.Data), "the read is still retried on the peer")
			require.Equal(t, [2]int64{1, 1}, loadRefusalHits(hits))
			held := slices.ContainsFunc(server.sessionManager.ProviderRoutingSnapshot().HeldOff,
				func(p routersession.HeldOffProviderInfo) bool { return p.Address == "refuser" })
			require.False(t, held, "the provider was held off for wording that is no rate limit")
		})
	}
}
