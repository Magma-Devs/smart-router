package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// TestGraphQLRelay_ReadVerdictsDecideThePeer (MAG-5121) sends reads through SendParsedRelay to two
// local upstreams: the first the only primary, the second the backup, which serves the query. What
// the first one answers decides whether the second is asked:
//
//   - an error the router cannot read (no code, an unknown code, a numeric code, a field limit) is
//     unknown: when unsure, retry, so the caller gets the peer's answer;
//   - the caller's own error is served as is, and the peer is never asked: it would say the same.
func TestGraphQLRelay_ReadVerdictsDecideThePeer(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := newGatewayTestGraphQLParser(t)

	cases := []struct {
		name      string
		first     string
		peerAsked bool
	}{
		{"test 5: a codeless whole-request error goes to the peer", `{"data":null,"errors":[{"message":"request timed out"}]}`, true},
		{"test 4: a codeless field error beside data goes to the peer", `{"data":{"chainIdentifier":null},"errors":[{"message":"failed","path":["chainIdentifier"]}]}`, true},
		{"an unknown code goes to the peer", `{"data":null,"errors":[{"message":"connection error","extensions":{"code":"postgres-error"}}]}`, true},
		{"a numeric code goes to the peer", `{"data":null,"errors":[{"message":"bad","extensions":{"code":400}}]}`, true},
		{"RESOURCE_EXHAUSTED with a path goes to the peer", `{"data":null,"errors":[{"message":"Exceeded the maximum number","path":["chainIdentifier"],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, true},
		{"a codeless 'Too many requests' goes to the peer", `{"data":null,"errors":[{"message":"Too many requests"}]}`, true},
		{"a provider fault goes to the peer", `{"data":null,"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, true},
		{"the caller's error beside a provider fault goes to the peer", `{"data":null,"errors":[{"message":"bad","extensions":{"code":"BAD_USER_INPUT"}},{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, true},
		{"a validation failure is served as is", `{"data":null,"errors":[{"message":"Unknown field","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, false},
		{"test 11: a parse failure is served as is", `{"data":null,"errors":[{"message":"recursion limit exceeded.","extensions":{"code":"GRAPHQL_PARSE_FAILED"}}]}`, false},
		{"a bad input is served as is", `{"data":null,"errors":[{"message":"bad","extensions":{"code":"BAD_USER_INPUT"}}]}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{http.StatusOK, tc.first}, refusalReply{http.StatusOK, gatewayPeerAnswer})

			firstAskedFirst := 0
			for i := 0; i < 12; i++ {
				before := loadRefusalHits(hits)
				result, sendErr := sendGatewayTestRequest(t, ctx, chainParser, server, gatewayTestQuery)
				after := loadRefusalHits(hits)
				firstAsked, peerAsked := after[0]-before[0], after[1]-before[1]
				if firstAsked == 0 {
					require.NoError(t, sendErr)
					require.Equal(t, int64(1), peerAsked)
					continue
				}
				firstAskedFirst++
				require.Equal(t, int64(1), firstAsked, "the first upstream is asked once")
				require.NoError(t, sendErr, "a provider's reply reaches the caller")
				require.NotNil(t, result.Reply)
				if tc.peerAsked {
					require.Equal(t, int64(1), peerAsked, "the read must be asked of the peer")
					require.JSONEq(t, gatewayPeerAnswer, string(result.Reply.Data), "the caller gets the peer's answer")
					continue
				}
				require.Zero(t, peerAsked, "the caller's own error must not be asked of a peer")
				require.JSONEq(t, tc.first, string(result.Reply.Data), "the caller gets the provider's reply as is")
				require.True(t, result.IsNodeError, "the caller's error carries the node-error flag, so it is never cached")
			}
			require.Positive(t, firstAskedFirst, "the first upstream was never asked first, so this case tested nothing")
		})
	}
}

// TestGraphQLRelay_UnknownOnEveryProviderIsServedAsIs (MAG-5121 test 5): when every provider sends
// the same error the router cannot read, the read is retried, and the caller then gets that
// provider reply as is, not a reply of the router's own.
func TestGraphQLRelay_UnknownOnEveryProviderIsServedAsIs(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := newGatewayTestGraphQLParser(t)

	const codeless = `{"data":null,"errors":[{"message":"Unknown field \"nope\""}]}`
	hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{http.StatusOK, codeless}, refusalReply{http.StatusOK, codeless})

	result, sendErr := sendGatewayTestRequest(t, ctx, chainParser, server, gatewayTestQuery)
	after := loadRefusalHits(hits)
	require.NoError(t, sendErr)
	require.Equal(t, [2]int64{1, 1}, after, "both providers are asked")
	require.NotNil(t, result.Reply)
	require.JSONEq(t, codeless, string(result.Reply.Data), "the caller gets a provider's reply as is")
	require.Equal(t, http.StatusOK, result.StatusCode)
}

// TestGraphQLRelay_PageInsteadOfAReply (MAG-5121 test 10) sends reads to two local upstreams that
// answer a 200 whose body is not a GraphQL reply.
//
//   - An HTML or text page is a whole reply from whatever answered: the read goes to the other
//     provider, and when every provider sends a page the client gets one of them as is, not a
//     reply of the router's own.
//   - JSON cut off on the way is a dropped connection: the read goes to the other provider, and
//     when every provider's reply is cut the client gets the router's own error.
func TestGraphQLRelay_PageInsteadOfAReply(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := newGatewayTestGraphQLParser(t)

	const (
		page = "<html><head><title>502 Bad Gateway</title></head><body>nginx</body></html>"
		cut  = `{"data":{"chainIdentifier":"358`
	)

	t.Run("a page from one provider goes to the other", func(t *testing.T) {
		hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{http.StatusOK, page}, refusalReply{http.StatusOK, gatewayPeerAnswer})
		requireFirstFailsOverToPeer(t, ctx, chainParser, server, hits)
	})

	t.Run("a page from every provider is served as is", func(t *testing.T) {
		hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{http.StatusOK, page}, refusalReply{http.StatusOK, page})
		result, sendErr := sendGatewayTestRequest(t, ctx, chainParser, server, gatewayTestQuery)
		require.Equal(t, [2]int64{1, 1}, loadRefusalHits(hits), "both providers are asked")
		require.NoError(t, sendErr, "a provider's page reaches the caller")
		require.NotNil(t, result.Reply)
		require.Equal(t, page, string(result.Reply.Data), "the caller gets the page as is")
		require.Equal(t, http.StatusOK, result.StatusCode)
		require.True(t, result.IsNodeError, "the page carries the node-error flag, so it is never cached")
	})

	t.Run("a reply cut off on one provider goes to the other", func(t *testing.T) {
		hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{http.StatusOK, cut}, refusalReply{http.StatusOK, gatewayPeerAnswer})
		requireFirstFailsOverToPeer(t, ctx, chainParser, server, hits)
	})

	t.Run("a reply cut off on every provider is the router's own error", func(t *testing.T) {
		hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{http.StatusOK, cut}, refusalReply{http.StatusOK, cut})
		result, sendErr := sendGatewayTestRequest(t, ctx, chainParser, server, gatewayTestQuery)
		require.Equal(t, [2]int64{1, 1}, loadRefusalHits(hits), "both providers are asked")
		require.Error(t, sendErr, "no provider sent a reply, so the router answers")
		if result != nil && result.Reply != nil {
			require.NotEqual(t, cut, string(result.Reply.Data), "a cut reply is never served")
		}
	})
}

// requireFirstFailsOverToPeer sends the read several times; each time the first upstream is asked
// first, the peer must be asked too and its answer must reach the caller.
func requireFirstFailsOverToPeer(t *testing.T, ctx context.Context, chainParser chainlib.ChainParser, server *RPCSmartRouterServer, hits *[2]int64) {
	t.Helper()
	firstAskedFirst := 0
	for i := 0; i < 12; i++ {
		before := loadRefusalHits(hits)
		result, sendErr := sendGatewayTestRequest(t, ctx, chainParser, server, gatewayTestQuery)
		after := loadRefusalHits(hits)
		require.NoError(t, sendErr)
		require.JSONEq(t, gatewayPeerAnswer, string(result.Reply.Data), "the caller gets the peer's answer")
		if after[0] == before[0] {
			continue
		}
		firstAskedFirst++
		require.Equal(t, int64(1), after[1]-before[1], "the read must be asked of the peer")
	}
	require.Positive(t, firstAskedFirst, "the first upstream was never asked first, so this case tested nothing")
}
