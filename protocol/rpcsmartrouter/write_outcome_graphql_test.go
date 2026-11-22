package rpcsmartrouter

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
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

// TestGraphQLWrite_AcceptedOnlyOnTheChainSuccessRow is the other half of MAG-5122's
// Router-Write-Outcome header. accepted is the chain's success row only: Sui's write field carries
// effects with SUCCESS or FAILURE (a transaction that ran and paid gas, MAG-4742). Every other 2xx
// gets no header, because the router cannot yet tell it apart from "may have run": effects: null
// (MAG-4518), a reply that did not select effects, and a rejected transaction, which answers with a
// GraphQL error inside a 200 and still comes back with err == nil.
func TestGraphQLWrite_AcceptedOnlyOnTheChainSuccessRow(t *testing.T) {
	rand.InitRandomSeed()
	tc := graphQLWriteCase(t)
	for _, c := range []struct {
		name, body, header string
	}{
		{"effects SUCCESS", `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS"}}}}`, "accepted"},
		{"effects FAILURE", `{"data":{"executeTransaction":{"effects":{"status":"FAILURE","executionError":{"message":"MoveAbort"}}}}}`, "accepted"},
		{"effects null", `{"data":{"executeTransaction":{"effects":null}}}`, ""},
		{"no effects selected", tc.success, ""},
		{"rejected with a GraphQL error", `{"errors":[{"message":"Invalid user signature"}]}`, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			served := writeUpstream{status: http.StatusOK, body: c.body}
			a, b := served, served
			a.name, b.name = "a-"+t.Name(), "b-"+t.Name()
			_, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{a, b})

			result, _ := sendWrite(t, tc, chainParser, server)
			require.NotNil(t, result)
			require.Equal(t, c.header, writeOutcomeHeader(t, result))
		})
	}
}

// TestGraphQLWrite_NeverRetriedWhenEveryCopyIsRateLimited is MAG-5122's Sending point 2: never
// re-sent, including today's re-send when every provider answered 429. Both primaries answer 429,
// which holds both off, so the re-send the Stateful rate-limit carve-out allows can only go to the
// backup: the tier a write reaches when no primary can be selected, and one the broadcast did not
// reach. With two primaries and no backup the re-send has nowhere to go, so a test of that shape
// passes with the carve-out still on.
//
// The JSON-RPC and REST writes keep the carve-out (MAG-5061 owns changing it there): their backup
// is hit, which is what proves this setup can see a re-send at all.
func TestGraphQLWrite_NeverRetriedWhenEveryCopyIsRateLimited(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range writeInterfaceCases {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			rateLimited := writeUpstream{status: http.StatusTooManyRequests, body: `{"code":-32007,"message":"50/second request limit reached"}`}
			a, b := rateLimited, rateLimited
			a.name, b.name = "a-"+t.Name(), "b-"+t.Name()
			backup := writeUpstream{name: "backup-" + t.Name(), status: http.StatusOK, body: tc.success}
			hits := make([]int64, 3)
			server := newWriteServer(t, tc, chainParser, []writeUpstream{a, b}, []writeUpstream{backup}, hits)

			_, _ = sendWrite(t, tc, chainParser, server)
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]), "each primary got the broadcast once")
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]), "each primary got the broadcast once")
			if tc.apiInterface != spectypes.APIInterfaceGraphQL {
				require.Equal(t, int64(1), atomic.LoadInt64(&hits[2]),
					"control: this interface keeps the carve-out, so its re-send reaches the backup")
				return
			}
			require.Never(t, func() bool { return atomic.LoadInt64(&hits[2]) > 0 }, 200*time.Millisecond, 10*time.Millisecond,
				"a GraphQL write is never re-sent, not even when every primary answered 429")
		})
	}
}

// writeOutcomeHeader reads Router-Write-Outcome off a RelayResult's reply, or "" when absent.
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

// MAG-3800: a write pinned to a router-select-provider name the router does not have is refused
// before any provider is asked. On GraphQL its reply says Router-Write-Outcome: not_sent, as the
// listener's other refusals of a GraphQL write do (Part I R-Q, Part II Q8). The other interfaces
// carry no write outcome yet (MAG-5232), so they get no header.
func TestGraphQLWrite_APinToANameTheRouterDoesNotHaveIsNotSent(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range writeInterfaceCases {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			served := writeUpstream{status: http.StatusOK, body: tc.success}
			a, b := served, served
			a.name, b.name = "a-"+t.Name(), "b-"+t.Name()
			hits, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{a, b})
			server.sessionManager.SetConfiguredProviderNames([]string{a.name, b.name})

			result, err := sendWriteWithDirectives(t, tc, chainParser, server, map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "no-such-provider"})

			require.ErrorIs(t, err, routersession.SelectedProviderNotConfiguredError)
			require.Equal(t, [2]int64{0, 0}, [2]int64{atomic.LoadInt64(&hits[0]), atomic.LoadInt64(&hits[1])}, "no provider is asked")
			want := ""
			if tc.apiInterface == spectypes.APIInterfaceGraphQL {
				want = "not_sent"
			}
			require.Equal(t, want, writeOutcomeHeader(t, result))
		})
	}
}
