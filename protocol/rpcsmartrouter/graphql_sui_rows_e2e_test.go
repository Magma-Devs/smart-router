package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/magma-Devs/smart-router/utils/rand"
)

func TestSuiGraphQLRelay_InputCoercionServedByFirstProvider(t *testing.T) {
	rand.InitRandomSeed()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	specs, err := specutils.GetAllSpecsFromPath("../chainlib/testdata/sui.json")
	require.NoError(t, err)
	sui, err := specutils.ExpandSpecWithDependencies(specs, "SUI")
	require.NoError(t, err)
	parser, err := chainlib.NewGraphQLChainParser()
	require.NoError(t, err)
	parser.SetSpec(*sui)

	const query = `{"query":"{ checkpoint(sequenceNumber: \"abc\") { digest } }"}`
	const refusal = `{"data":null,"errors":[{"message":"Expected input type \"UInt53\", found \"abc\".","locations":[{"line":1,"column":30}],"path":["checkpoint"]}]}`
	hits, server := newGatewayTestServer(t, parser, logs, refusalReply{http.StatusOK, refusal}, refusalReply{http.StatusOK, refusal})
	result, err := sendGatewayTestRequest(t, context.Background(), parser, server, query)
	require.NoError(t, err)
	require.Equal(t, [2]int64{1, 0}, loadRefusalHits(hits))
	require.Equal(t, refusal, string(result.Reply.Data))
	require.True(t, result.IsNonRetryable)
}

// TestSuiGraphQLRelay_ReadRowsDecideThePeer (MAG-4909) sends Sui reads through SendParsedRelay to two
// local upstreams, as TestGraphQLRelay_ReadVerdictsDecideThePeer does for the default rows. A Sui row
// that says another provider may serve the read sends it to the peer, even where the default row
// for the same code would not; a row that says it is the caller's serves the reply as is.
func TestSuiGraphQLRelay_ReadRowsDecideThePeer(t *testing.T) {
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
		{"R5 a provider behind goes to the peer", `{"data":{"chainIdentifier":null},"errors":[{"message":"Checkpoint 4 in the future","path":["chainIdentifier"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, true},
		{"R8 an operator limit goes to the peer", `{"data":null,"errors":[{"message":"Query nesting is over 20","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, true},
		{"R9 a version gap goes to the peer", `{"data":null,"errors":[{"message":"Unknown field \"chainIdentifier\" on type \"Query\".","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, true},
		{"R20 a query timeout goes to the peer", `{"errors":[{"message":"Query timed out after 40.00s","extensions":{"code":"REQUEST_TIMEOUT"}}]}`, true},
		{"R7 a bad input is served as is", `{"data":null,"errors":[{"message":"Invalid Sui address","path":["chainIdentifier"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, false},
		{"R12 a codeless parse failure is served as is", `{"data":null,"errors":[{"message":"Failed to parse \"SuiAddress\": Invalid length","path":["chainIdentifier"]}]}`, false},
		{"R16 an on-chain limit is served as is", `{"data":{"chainIdentifier":null},"errors":[{"message":"Display output too large","path":["chainIdentifier"],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, false},
		{"R17 an unsupported filter is served as is", `{"data":null,"errors":[{"message":"Filtering by both emitting module and event type is not supported not available","path":["chainIdentifier"],"extensions":{"code":"FEATURE_UNAVAILABLE"}}]}`, false},
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
