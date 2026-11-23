package rpcsmartrouter

import (
	"cmp"
	"encoding/json"
	"errors"
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
		"the request's transaction bytes are not Base64, so there is no digest: tx_hash is omitted, not faked")
	require.Contains(t, body.Errors[0].Message, "Look up the transaction on chain before signing anything new",
		"without a digest, the message says how to find the transaction anyway")

	require.Equal(t, "unknown", writeOutcomeHeader(t, result))
}

// suiSnapshotTransactionBcs is a TransactionData in BCS, Base64, from Sui's own test snapshot
// (crates/sui-indexer-alt-e2e-tests/tests/graphql/transactions/bcs.snap), and suiSnapshotDigest the
// digest Sui reports for it there.
const (
	suiSnapshotTransactionBcs = "AAACAAgqAAAAAAAAAAAgp7AycDh4qnTDEmk1eJ/R1NfhEdWRGwkkfWljBhwxK1oCAgABAQAAAQECAAABAQD8zJpCG7sTwaZqGqmPCtdQKe3pSFd3nGkVtE+UBouSHgHhFa5/+bVpRPr+9v0BXVEKL9ljttQuyKYmZAnA6kD7NgMAAAAAAAAAIEzrkQF+I9IJ4x0c30+YdFP63Y1T+vAeA9YRM4e19EVk/MyaQhu7E8GmahqpjwrXUCnt6UhXd5xpFbRPlAaLkh7oAwAAAAAAAADyBSoBAAAAAA=="
	suiSnapshotDigest         = "gHrD7i4MD93GsSzM3ZYibg2CQpBCmZFnxGdANrXFYMP"
	suiSnapshotWrite          = `executeTransaction(transactionDataBcs: $tx, signatures: ["AA=="])`
	suiEffectsQuery           = `mutation Submit($tx: Base64!) { ` + suiSnapshotWrite + ` { effects { status } } }`
)

// suiWriteOf makes the snapshot transaction available to the operation through $tx.
func suiWriteOf(t *testing.T, query string) writeInterfaceCase {
	t.Helper()
	tc := graphQLWriteCase(t)
	body, err := json.Marshal(map[string]any{"query": query, "variables": map[string]string{"tx": suiSnapshotTransactionBcs}})
	require.NoError(t, err)
	tc.body = string(body)
	return tc
}

// unknownWriteReply reads the router's unknown reply off a result: its message and extensions.
func unknownWriteReply(t *testing.T, result *common.RelayResult) (message string, extensions map[string]string) {
	t.Helper()
	require.NotNil(t, result)
	require.Equal(t, http.StatusOK, result.StatusCode)
	cf := result.GetClientFacing()
	require.NotNil(t, cf, "the router answers an unknown write itself")
	var body struct {
		Errors []struct {
			Message    string            `json:"message"`
			Extensions map[string]string `json:"extensions"`
		} `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(cf.UpstreamBody, &body))
	require.Len(t, body.Errors, 1)
	return body.Errors[0].Message, body.Errors[0].Extensions
}

// TestGraphQLWrite_UnknownReplyCarriesTheDigest (MAG-4909 Sui data 3, MAG-5122 test 7): when the
// outcome is unknown, the client gets the transaction's digest, computed from the bytes it sent with
// Sui's formula, so it can look the transaction up before signing anything new.
func TestGraphQLWrite_UnknownReplyCarriesTheDigest(t *testing.T) {
	rand.InitRandomSeed()
	tc := suiWriteOf(t, suiEffectsQuery)
	chainParser := tc.parser(t)
	badGateway := writeUpstream{status: http.StatusBadGateway, body: `{"message":"bad gateway"}`}
	a, b := badGateway, badGateway
	a.name, b.name = "a-"+t.Name(), "b-"+t.Name()
	_, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{a, b})

	result, err := sendWrite(t, tc, chainParser, server)
	require.Error(t, err)
	message, extensions := unknownWriteReply(t, result)
	require.Equal(t, "unknown", extensions["write_outcome"])
	require.Equal(t, suiSnapshotDigest, extensions["tx_hash"])
	require.Equal(t, "Status unknown - this transaction may have been broadcast. Look up "+suiSnapshotDigest+
		" before signing anything new. Re-sending the same signed bytes is safe. If you replace it, reuse the same input objects.", message)
	require.Equal(t, "unknown", writeOutcomeHeader(t, result))
}

// TestGraphQLWrite_SuiWriteRowsDecideTheOutcome (MAG-4909 write rows, MAG-5122 verdict per copy):
// each of two primaries answers the write, and Sui's rows read each copy as accepted, refused or "may
// have run". A copy that ran is the answer, with Router-Write-Outcome: accepted. With none, a copy that
// may have run leaves the outcome unknown, and the client gets the router's unknown reply instead of
// an error that would make it sign again. Copies that were all refused keep the node's reply as is.
func TestGraphQLWrite_SuiWriteRowsDecideTheOutcome(t *testing.T) {
	rand.InitRandomSeed()
	const (
		ran              = `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS"}}}}`
		ranFailed        = `{"data":{"executeTransaction":{"effects":{"status":"FAILURE","executionError":{"message":"MoveAbort"}}}}}`
		ranBeside        = `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS","balanceChanges":null}}},"errors":[{"message":"Failed to load balance changes","path":["executeTransaction","effects","balanceChanges"],"extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`
		ranBelow         = `{"data":{"executeTransaction":{"effects":null}},"errors":[{"message":"Failed to read effects","path":["executeTransaction","effects"],"extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`
		noEffects        = `{"data":{"executeTransaction":{"effects":null}}}`
		refusedBcs       = `{"data":null,"errors":[{"message":"Invalid BCS-encoded TransactionData: unexpected end of input","path":["executeTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`
		inputTypeRefusal = `{"data":null,"errors":[{"message":"Expected input type \"Base64\", found 123.","path":["executeTransaction"]}]}`
		vendorStop       = `{"data":null,"errors":[{"message":"quota exceeded","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`
		networkNo        = `{"data":null,"errors":[{"message":"Invalid argument: Transaction is rejected as invalid by more than 1/3 of validators by stake (non-retriable). Non-retriable errors: [Transaction 6sW1ruByXbFjZzVvX5b7PBY5ZCyZTzPS2fVZFuMuRH1z was already executed { v1 } with 6667 stake].","path":["executeTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`
		locked           = `{"data":null,"errors":[{"message":"Failed to execute transaction","path":["executeTransaction"],"extensions":{"chain":["code: 'The system is not in a state required for the operation's execution', message: \"Object (0x5, SequenceNumber(7), o#abc) already locked by a different transaction: TransactionDigest(x)\""],"code":"INTERNAL_SERVER_ERROR"}}]}`
		internal         = `{"data":null,"errors":[{"message":"Failed to execute transaction","path":["executeTransaction"],"extensions":{"chain":["code: 'The service is currently unavailable', message: \"timed-out before finality could be reached\""],"code":"INTERNAL_SERVER_ERROR"}}]}`
	)
	for _, c := range []struct {
		name             string
		query            string // defaults to suiEffectsQuery
		a, b             string
		statusA, statusB int    // 200 when 0
		served           string // the reply the client gets as is; "" for the router's unknown reply
		header           string
		servedStatus     int // defaults to HTTP 200
	}{
		{name: "W1 effects SUCCESS", a: ran, b: ran, served: ran, header: "accepted"},
		{name: "W1 effects FAILURE: it ran and paid gas", a: ranFailed, b: ranFailed, served: ranFailed, header: "accepted"},
		{name: "W1 with an error beside the effects", a: ranBeside, b: ranBeside, served: ranBeside, header: "accepted"},
		{name: "W2 an error below the field", a: ranBelow, b: ranBelow, served: ranBelow, header: "accepted"},
		{name: "a copy that ran wins over one that may have run", a: ran, b: networkNo, served: ran, header: "accepted"},
		{name: "W1 effects inside a 429 (MAG-5122 T14)", a: ran, statusA: http.StatusTooManyRequests, b: networkNo, served: ran, header: "accepted"},
		{name: "W1 effects inside a 503", a: ran, statusA: http.StatusServiceUnavailable, b: networkNo, served: ran, header: "accepted"},
		{
			name: "aliased null effects remain unknown", query: `mutation($tx: Base64!) { ` + suiSnapshotWrite + ` { fx: effects { status } } }`,
			a: `{"data":{"executeTransaction":{"fx":null}}}`, b: networkNo, header: "unknown",
		},
		{
			name: "typename aliased to effects is a resolved write", query: `mutation($tx: Base64!) { ` + suiSnapshotWrite + ` { effects: __typename } }`,
			a: `{"data":{"executeTransaction":{"effects":"ExecutionResult"}}}`, b: networkNo, served: `{"data":{"executeTransaction":{"effects":"ExecutionResult"}}}`, header: "accepted",
		},
		{name: "typename only", query: `mutation { __typename }`, a: `{"data":{"__typename":"Mutation"}}`, b: `{"data":{"__typename":"Mutation"}}`, served: `{"data":{"__typename":"Mutation"}}`},
		{
			name: "skipped transaction", query: `mutation($tx: Base64!, $skip: Boolean! = true) { ` + suiSnapshotWrite + ` @skip(if: $skip) { effects { status } } }`,
			a: `{"data":{}}`, b: `{"data":{}}`, served: `{"data":{}}`,
		},
		{name: "W11 effects null may have run", a: noEffects, b: noEffects, header: "unknown"},
		{name: "W9 the network refused it: may have run", a: networkNo, b: networkNo, header: "unknown"},
		{name: "W10 competing transactions: may have run", a: locked, b: locked, header: "unknown"},
		{name: "W11 an internal error: may have run", a: internal, b: internal, header: "unknown"},
		{name: "W11 every copy a bare 503 (MAG-5122 T13)", a: "upstream down", statusA: http.StatusServiceUnavailable, b: "upstream down", statusB: http.StatusServiceUnavailable, header: "unknown"},
		{name: "a refused copy beside one that may have run", a: refusedBcs, b: networkNo, header: "unknown"},
		{name: "W6 every copy refused: the node's reply as is", a: refusedBcs, b: refusedBcs, served: refusedBcs},
		{name: "W6 every copy refused under 502", a: refusedBcs, b: refusedBcs, statusA: 502, statusB: 502, served: refusedBcs, servedStatus: 502},
		{name: "W6 every copy refused under 504", a: refusedBcs, b: refusedBcs, statusA: 504, statusB: 504, served: refusedBcs, servedStatus: 504},
		{name: "W6 gateway refusal beside an ambiguous copy", a: refusedBcs, statusA: 504, b: networkNo, header: "unknown"},
		{
			name: "W7 input coercion refused before submission", query: `mutation { executeTransaction(transactionDataBcs: 123, signatures: []) { __typename } }`,
			a: inputTypeRefusal, b: inputTypeRefusal, served: inputTypeRefusal,
		},
		{name: "W12 every copy stopped by the vendor's quota: the node's reply as is", a: vendorStop, b: vendorStop, served: vendorStop},
	} {
		t.Run(c.name, func(t *testing.T) {
			tc := suiWriteOf(t, cmp.Or(c.query, suiEffectsQuery))
			chainParser := tc.parser(t)
			a := writeUpstream{name: "a-" + t.Name(), status: cmp.Or(c.statusA, http.StatusOK), body: c.a}
			b := writeUpstream{name: "b-" + t.Name(), status: cmp.Or(c.statusB, http.StatusOK), body: c.b}
			hits, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{a, b})

			result, _ := sendWrite(t, tc, chainParser, server)
			require.NotNil(t, result)
			require.Eventually(t, func() bool {
				return atomic.LoadInt64(&hits[0]) == 1 && atomic.LoadInt64(&hits[1]) == 1
			}, time.Second, time.Millisecond, "one copy per provider")
			require.Equal(t, c.header, writeOutcomeHeader(t, result))
			if c.served == "" {
				_, extensions := unknownWriteReply(t, result)
				require.Equal(t, "unknown", extensions["write_outcome"])
				require.Equal(t, suiSnapshotDigest, extensions["tx_hash"])
				return
			}
			require.Nil(t, result.GetClientFacing(), "a node's reply, not the router's")
			require.Equal(t, cmp.Or(c.servedStatus, http.StatusOK), result.StatusCode)
			require.NotNil(t, result.Reply)
			require.Equal(t, c.served, string(result.Reply.Data))
		})
	}
}

func TestGraphQLWrite_SkippedMutationWithLostReply(t *testing.T) {
	rand.InitRandomSeed()
	query := `mutation($tx: Base64!) { ... on Mutation @include(if: false) { ` + suiSnapshotWrite + ` { effects { status } } } }`
	tc := suiWriteOf(t, query)
	parser := tc.parser(t)
	_, server := newTwoPrimaryWriteServer(t, tc, parser, [2]writeUpstream{
		{name: "a-" + t.Name(), status: http.StatusBadGateway, body: "bad gateway"},
		{name: "b-" + t.Name(), status: http.StatusBadGateway, body: "bad gateway"},
	})

	result, err := sendWrite(t, tc, parser, server)
	require.Error(t, err)
	var uncertain *unclearWriteError
	require.False(t, errors.As(err, &uncertain))
	require.Empty(t, writeOutcomeHeader(t, result))
	require.Equal(t, http.StatusBadGateway, result.GetClientFacing().UpstreamStatus)
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
