package rpcsmartrouter

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// graphQLWriteCase is writeInterfaceCases' GraphQL entry (a Sui-shaped executeTransaction
// mutation broadcast to two primaries): the one case this subtask (MAG-5122) changes.
func graphQLWriteCase(t *testing.T) writeInterfaceCase {
	t.Helper()
	for _, tc := range writeInterfaceCases {
		if tc.apiInterface == spectypes.APIInterfaceGraphQL {
			return tc
		}
	}
	t.Fatal("setup: writeInterfaceCases has no GraphQL case")
	return writeInterfaceCase{}
}

// TestGraphQLWrite_UnclearOutcomeAnswersWithHTTP200 is the ticket's headline scenario (MAG-5122,
// "Why, with a real case"): every provider's reply leaves the write's outcome unclear (here, both
// answer a proxy's 502 — MayHaveReachedNode, so the write may have reached the node), and the
// client must get HTTP 200 with a GraphQL error document carrying write_outcome: unknown in
// extensions, not withUnclearWriteStatus's 500 — a GraphQL client's retry link re-sends a mutation
// on a 5xx, which would double-send the write this subtask exists to protect against.
func TestGraphQLWrite_UnclearOutcomeAnswersWithHTTP200(t *testing.T) {
	rand.InitRandomSeed()
	tc := graphQLWriteCase(t)
	chainParser := tc.parser(t)
	badGateway := writeUpstream{status: http.StatusBadGateway, body: `{"message":"bad gateway"}`}
	a, b := badGateway, badGateway
	a.name, b.name = "a-"+t.Name(), "b-"+t.Name()
	_, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{a, b})

	result, err := sendWrite(t, tc, chainParser, server)
	require.Error(t, err, "no provider served the write")
	require.NotNil(t, result)
	require.Equal(t, http.StatusOK, result.StatusCode,
		"a GraphQL client's retry link re-sends a mutation on a 5xx: the router must answer 200")

	cf := result.GetClientFacing()
	require.NotNil(t, cf)
	require.Equal(t, http.StatusOK, cf.UpstreamStatus)
	require.Equal(t, common.JSONContentType, cf.UpstreamContentType)

	var body struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message    string            `json:"message"`
			Extensions map[string]string `json:"extensions"`
		} `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(cf.UpstreamBody, &body))
	require.Nil(t, body.Data, "router-authored reply: no data key (subtask 1's reply shape)")
	require.Len(t, body.Errors, 1)
	require.Equal(t, "unknown", body.Errors[0].Extensions["write_outcome"])
	require.Contains(t, body.Errors[0].Message, "Status unknown")
	require.Contains(t, body.Errors[0].Message, "may have been broadcast")
	require.NotContains(t, body.Errors[0].Extensions, "tx_hash",
		"the Sui digest formula (MAG-4909) is not in the router yet; tx_hash must be omitted, not faked")

	require.Equal(t, "unknown", writeOutcomeHeader(t, result))
}

// TestGraphQLWrite_AcceptedGetsTheHeader is the other half of MAG-5122's Lava-Write-Outcome
// header: a write served by its first accepted copy is "accepted".
func TestGraphQLWrite_AcceptedGetsTheHeader(t *testing.T) {
	rand.InitRandomSeed()
	tc := graphQLWriteCase(t)
	chainParser := tc.parser(t)
	healthy := writeUpstream{status: http.StatusOK, body: tc.success}
	a, b := healthy, healthy
	a.name, b.name = "a-"+t.Name(), "b-"+t.Name()
	_, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{a, b})

	result, err := sendWrite(t, tc, chainParser, server)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "accepted", writeOutcomeHeader(t, result))
}

// TestGraphQLWrite_NeverRetriedWhenEveryCopyIsRateLimited is MAG-5122's Sending point 2: never
// re-sent, including today's re-send when every provider answered 429. With no third provider to
// retry onto, a retry here can only show up as a second hit on one of these two — which must never
// happen.
func TestGraphQLWrite_NeverRetriedWhenEveryCopyIsRateLimited(t *testing.T) {
	rand.InitRandomSeed()
	tc := graphQLWriteCase(t)
	chainParser := tc.parser(t)
	rateLimited := writeUpstream{status: http.StatusTooManyRequests, body: `{"code":-32007,"message":"50/second request limit reached"}`}
	a, b := rateLimited, rateLimited
	a.name, b.name = "a-"+t.Name(), "b-"+t.Name()
	hits, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{a, b})

	_, err := sendWrite(t, tc, chainParser, server)
	require.Error(t, err)
	time.Sleep(50 * time.Millisecond) // let any wrongly-issued retry land before counting hits
	require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]), "sent once, never re-sent")
	require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]), "sent once, never re-sent")
}

// writeOutcomeHeader reads Lava-Write-Outcome off a RelayResult's reply, or "" when absent.
func writeOutcomeHeader(t *testing.T, result *common.RelayResult) string {
	t.Helper()
	if result == nil || result.Reply == nil {
		return ""
	}
	for _, md := range result.Reply.Metadata {
		if md.Name == common.WRITE_OUTCOME_HEADER_NAME {
			return md.Value
		}
	}
	return ""
}
