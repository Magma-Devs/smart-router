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
//   - the caller's own error is served as is, and the peer is never asked: it would say the same;
//   - an HTML page is kept and blamed, and JSON cut off on the way is a dropped connection: both go
//     to the peer.
func TestGraphQLRelay_ReadVerdictsDecideThePeer(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := newGatewayTestGraphQLParser(t)

	cases := []struct {
		name      string
		status    int // the first upstream's status; 0 means 200
		first     string
		peerAsked bool
	}{
		{"test 5: a codeless whole-request error goes to the peer", 0, `{"data":null,"errors":[{"message":"request timed out"}]}`, true},
		{"test 4: a codeless field error beside data goes to the peer", 0, `{"data":{"chainIdentifier":null},"errors":[{"message":"failed","path":["chainIdentifier"]}]}`, true},
		{"an unknown code goes to the peer", 0, `{"data":null,"errors":[{"message":"connection error","extensions":{"code":"postgres-error"}}]}`, true},
		{"a numeric code goes to the peer", 0, `{"data":null,"errors":[{"message":"bad","extensions":{"code":400}}]}`, true},
		{"RESOURCE_EXHAUSTED with a path goes to the peer", 0, `{"data":null,"errors":[{"message":"Exceeded the maximum number","path":["chainIdentifier"],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, true},
		{"a codeless 'Too many requests' goes to the peer", 0, `{"data":null,"errors":[{"message":"Too many requests"}]}`, true},
		{"a provider fault goes to the peer", 0, `{"data":null,"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, true},
		{"the caller's error beside a provider fault goes to the peer", 0, `{"data":null,"errors":[{"message":"bad","extensions":{"code":"BAD_USER_INPUT"}},{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, true},
		{"a validation failure is served as is", 0, `{"data":null,"errors":[{"message":"Unknown field","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, false},
		{"test 11: a parse failure is served as is", 0, `{"data":null,"errors":[{"message":"recursion limit exceeded.","extensions":{"code":"GRAPHQL_PARSE_FAILED"}}]}`, false},
		{"a bad input is served as is", 0, `{"data":null,"errors":[{"message":"bad","extensions":{"code":"BAD_USER_INPUT"}}]}`, false},
		{"test 10: an HTML page goes to the peer", 0, graphQLTestPage, true},
		{"test 10: a reply cut off on the way goes to the peer", 0, graphQLTestCutReply, true},

		// The verdict decides, never a REST body-text row on the error's message (#714 review,
		// 714-2). Through the shared REST table the first was filed NODE_ENDPOINT_NOT_FOUND
		// ("path not found") and asked of the peer; the second USER_INVALID_PARAMS ("decoding
		// bech32 failed" under a 500) and never asked of it. Both fail on #714.
		{"the caller's error whose message says 'path not found' is served as is", 0, `{"data":null,"errors":[{"message":"Invalid address: path not found","extensions":{"code":"BAD_USER_INPUT"}}]}`, false},
		{"a 500 saying 'decoding bech32 failed' beside FEATURE_UNAVAILABLE goes to the peer", http.StatusInternalServerError, `{"data":null,"errors":[{"message":"decoding bech32 failed"},{"message":"not here","extensions":{"code":"FEATURE_UNAVAILABLE"}}]}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			firstStatus := tc.status
			if firstStatus == 0 {
				firstStatus = http.StatusOK
			}
			hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{firstStatus, tc.first}, refusalReply{http.StatusOK, gatewayPeerAnswer})

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

// A 200 whose body is not a GraphQL reply (MAG-5121 test 10): a page from whatever answered, and
// JSON cut off on the way.
const (
	graphQLTestPage     = "<html><head><title>502 Bad Gateway</title></head><body>nginx</body></html>"
	graphQLTestCutReply = `{"data":{"chainIdentifier":"358`
)

// TestGraphQLRelay_PageInsteadOfAReply (MAG-5121 test 10) sends reads to two local upstreams that
// both answer a 200 whose body is not a GraphQL reply. One such provider beside a healthy one is
// covered by TestGraphQLRelay_ReadVerdictsDecideThePeer.
//
//   - An HTML or text page is a whole reply from whatever answered: when every provider sends a
//     page, the client gets one of them as is, not a reply of the router's own.
//   - JSON cut off on the way is a dropped connection: when every provider's reply is cut, the
//     client gets the router's own error.
func TestGraphQLRelay_PageInsteadOfAReply(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := newGatewayTestGraphQLParser(t)

	page, cut := graphQLTestPage, graphQLTestCutReply

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

// requireFirstFailsOverToPeer sends the read several times. Each time, the caller gets the peer's
// answer and the peer is asked once; when the first upstream is asked, it is asked once, so the
// read went to the peer rather than back to the same provider.
func requireFirstFailsOverToPeer(t *testing.T, ctx context.Context, chainParser chainlib.ChainParser, server *RPCSmartRouterServer, hits *[2]int64) {
	t.Helper()
	firstAskedFirst := 0
	for i := 0; i < 12; i++ {
		before := loadRefusalHits(hits)
		result, sendErr := sendGatewayTestRequest(t, ctx, chainParser, server, gatewayTestQuery)
		after := loadRefusalHits(hits)
		firstAsked, peerAsked := after[0]-before[0], after[1]-before[1]
		require.NoError(t, sendErr)
		require.NotNil(t, result.Reply)
		require.JSONEq(t, gatewayPeerAnswer, string(result.Reply.Data), "the caller gets the peer's answer")
		require.Equal(t, int64(1), peerAsked, "the peer is asked once")
		if firstAsked == 0 {
			continue
		}
		firstAskedFirst++
		require.Equal(t, int64(1), firstAsked, "the first upstream is asked once")
	}
	require.Positive(t, firstAskedFirst, "the first upstream was never asked first, so this case tested nothing")
}

// TestGraphQLRelay_StatusTableDecidesThePeer (MAG-5121 test 12, slice 3): a read's reply outside
// 2xx that no row reads is filed by GraphQL's own status table, never by the shared REST rows. Each
// of these goes to the other provider: the REST rows filed the 400, 413, 415, 422 and 409 as the
// caller's (served from the first provider, the peer never asked) and the 500 below as a chain's
// "not found".
func TestGraphQLRelay_StatusTableDecidesThePeer(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := newGatewayTestGraphQLParser(t)

	cases := []struct {
		name  string
		first refusalReply
	}{
		{"a 400 with a message", refusalReply{http.StatusBadRequest, `{"message":"bad"}`}},
		{"a 400 with a codeless GraphQL error (Change 7)", refusalReply{http.StatusBadRequest, `{"errors":[{"message":"Cannot query field"}]}`}},
		{"a 400 with a JSON-RPC error body", refusalReply{http.StatusBadRequest, `{"jsonrpc":"2.0","id":null,"error":{"code":-32604,"message":"this request method is not supported"}}`}},
		{"a 413 with text", refusalReply{http.StatusRequestEntityTooLarge, `Payload Too Large`}},
		{"a 415", refusalReply{http.StatusUnsupportedMediaType, `{}`}},
		{"a 422", refusalReply{http.StatusUnprocessableEntity, `{"message":"unprocessable"}`}},
		{"a 409", refusalReply{http.StatusConflict, `conflict`}},
		{"a 404 on the one url", refusalReply{http.StatusNotFound, `404 page not found`}},
		{"a 500 saying transaction not found", refusalReply{http.StatusInternalServerError, `transaction not found`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hits, server := newGatewayTestServer(t, chainParser, logs, tc.first, refusalReply{http.StatusOK, gatewayPeerAnswer})
			requireFirstFailsOverToPeer(t, ctx, chainParser, server, hits)
		})
	}

	// The node's own answer that the query is wrong is still the caller's: served as is.
	t.Run("a 400 with GRAPHQL_VALIDATION_FAILED is served as is", func(t *testing.T) {
		const validation = `{"errors":[{"message":"Unknown field","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`
		hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{http.StatusBadRequest, validation}, refusalReply{http.StatusOK, gatewayPeerAnswer})
		firstAskedFirst := 0
		for i := 0; i < 12; i++ {
			before := loadRefusalHits(hits)
			result, sendErr := sendGatewayTestRequest(t, ctx, chainParser, server, gatewayTestQuery)
			after := loadRefusalHits(hits)
			if after[0] == before[0] {
				continue
			}
			firstAskedFirst++
			require.NoError(t, sendErr)
			require.Zero(t, after[1]-before[1], "the caller's own error must not be asked of a peer")
			require.JSONEq(t, validation, string(result.Reply.Data))
		}
		require.Positive(t, firstAskedFirst, "the first upstream was never asked first, so this case tested nothing")
	})
}
