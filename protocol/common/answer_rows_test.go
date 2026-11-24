package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

func boolPtr(b bool) *bool { return &b }

// PRD §2.7's rows: Cosmos' success row reads the broadcast's tx_response.code under any status; Sui's
// not-found row reads a null transaction.
var (
	cosmosBroadcastOK = &spectypes.ErrorRow{ID: "cosmos.broadcast_ok", Interface: spectypes.InterfaceList{"rest"}, Methods: []string{"POST /cosmos/tx/v1beta1/txs"}, Answer: true, At: "/tx_response/code", Code: json.RawMessage(`0`), Write: spectypes.ErrorWriteTookIt}
	suiTxNotFound     = &spectypes.ErrorRow{ID: "sui.tx_not_found", Interface: spectypes.InterfaceList{"graphql"}, Methods: []string{"transaction"}, Answer: true, At: "/data/transaction", Code: json.RawMessage(`null`), Error: "CHAIN_SUI_TX_NOT_FOUND", Class: "C3", Reason: "not_found"}
	anyAnswerIsTaken  = &spectypes.ErrorRow{ID: "x.any_answer", Interface: spectypes.InterfaceList{"rest"}, Methods: []string{"POST /tx"}, Answer: true, Write: spectypes.ErrorWriteTookIt}
	objectPresent     = &spectypes.ErrorRow{ID: "x.present", Interface: spectypes.InterfaceList{"graphql"}, Methods: []string{"object"}, Answer: true, At: "/data/object", Present: boolPtr(false), Error: "CHAIN_X_OBJECT_NOT_FOUND", Class: "C3", Reason: "not_found"}
)

func TestMatchAnswerRow(t *testing.T) {
	const broadcast = "POST /cosmos/tx/v1beta1/txs"
	rows := []*spectypes.ErrorRow{cosmosBroadcastOK, suiTxNotFound, anyAnswerIsTaken, objectPresent}

	t.Run("a success row reads the write copy's 2xx", func(t *testing.T) {
		row, ok := MatchAnswerRow(rows, "rest", broadcast, true, true, decode(t, `{"tx_response": {"code": 0, "txhash": "ABC"}}`))
		require.True(t, ok)
		require.Equal(t, "cosmos.broadcast_ok", row.ID)
	})
	t.Run("a success row with at reads a write copy's body under any status", func(t *testing.T) {
		row, ok := MatchAnswerRow(rows, "rest", broadcast, true, false, decode(t, `{"tx_response": {"code": 0, "txhash": "ABC"}}`))
		require.True(t, ok, "a 429 or 5xx carrying the accepted answer still took it (R-L)")
		require.Equal(t, "cosmos.broadcast_ok", row.ID)
	})
	t.Run("a refused broadcast does not match the success row", func(t *testing.T) {
		_, ok := MatchAnswerRow(rows, "rest", broadcast, true, true, decode(t, `{"tx_response": {"code": 32, "raw_log": "account sequence mismatch"}}`))
		require.False(t, ok)
	})
	t.Run("a code of 0 is not a string \"0\" and not a missing pointer", func(t *testing.T) {
		_, ok := MatchAnswerRow(rows, "rest", broadcast, true, true, decode(t, `{"tx_response": {"code": "0"}}`))
		require.False(t, ok)
		_, ok = MatchAnswerRow(rows, "rest", broadcast, true, true, decode(t, `{"tx_response": {"txhash": "ABC"}}`))
		require.False(t, ok)
	})
	t.Run("a success row reads only its methods and its interface", func(t *testing.T) {
		_, ok := MatchAnswerRow(rows, "rest", "GET /cosmos/tx/v1beta1/txs/ABC", false, true, decode(t, `{"tx_response": {"code": 0}}`))
		require.False(t, ok)
		_, ok = MatchAnswerRow(rows, "grpc", broadcast, true, true, decode(t, `{"tx_response": {"code": 0}}`))
		require.False(t, ok)
	})
	t.Run("a not-found row matches a null answer on a 2xx only", func(t *testing.T) {
		row, ok := MatchAnswerRow(rows, "graphql", "transaction", false, true, decode(t, `{"data": {"transaction": null}}`))
		require.True(t, ok)
		require.Equal(t, "sui.tx_not_found", row.ID)
		_, ok = MatchAnswerRow(rows, "graphql", "transaction", false, true, decode(t, `{"data": {"transaction": {"digest": "D"}}}`))
		require.False(t, ok, "an answer with data is not a not-found")
		_, ok = MatchAnswerRow(rows, "graphql", "transaction", false, true, decode(t, `{"data": {}}`))
		require.False(t, ok, "a missing key is not the null the row names")
		_, ok = MatchAnswerRow(rows, "graphql", "transaction", false, false, decode(t, `{"data": {"transaction": null}}`))
		require.False(t, ok, "a not-found row is about an answer, never a non-2xx")
	})
	t.Run("a row with methods and nothing more reads any answer on them", func(t *testing.T) {
		row, ok := MatchAnswerRow(rows, "rest", "POST /tx", true, true, decode(t, `"0xabc"`))
		require.True(t, ok)
		require.Equal(t, "x.any_answer", row.ID)
		_, ok = MatchAnswerRow(rows, "rest", "POST /tx", true, false, decode(t, `"0xabc"`))
		require.False(t, ok, "without at it reads a 2xx only")
	})
	t.Run("present: false is a missing or null value, present: true one that is neither", func(t *testing.T) {
		row, ok := MatchAnswerRow(rows, "graphql", "object", false, true, decode(t, `{"data": {}}`))
		require.True(t, ok)
		require.Equal(t, "x.present", row.ID)
		_, ok = MatchAnswerRow(rows, "graphql", "object", false, true, decode(t, `{"data": {"object": null}}`))
		require.True(t, ok)
		_, ok = MatchAnswerRow(rows, "graphql", "object", false, true, decode(t, `{"data": {"object": {"id": 1}}}`))
		require.False(t, ok)
	})
	t.Run("scope: a write row never reads a read, a read row never a write", func(t *testing.T) {
		writeOnly := &spectypes.ErrorRow{ID: "w", Interface: spectypes.InterfaceList{"rest"}, Methods: []string{"POST /tx"}, Answer: true, Scope: spectypes.ErrorRowScopeWrite, Write: spectypes.ErrorWriteTookIt}
		readOnly := &spectypes.ErrorRow{ID: "r", Interface: spectypes.InterfaceList{"rest"}, Methods: []string{"GET /tx"}, Answer: true, Scope: spectypes.ErrorRowScopeRead, Error: "CHAIN_X_NOT_FOUND", Class: "C3", Reason: "not_found", At: "/tx", Present: boolPtr(false)}
		_, ok := MatchAnswerRow([]*spectypes.ErrorRow{writeOnly}, "rest", "POST /tx", false, true, decode(t, `{}`))
		require.False(t, ok)
		_, ok = MatchAnswerRow([]*spectypes.ErrorRow{readOnly}, "rest", "GET /tx", true, true, decode(t, `{}`))
		require.False(t, ok)
		_, ok = MatchAnswerRow([]*spectypes.ErrorRow{readOnly}, "rest", "GET /tx", false, true, decode(t, `{}`))
		require.True(t, ok)
	})
	t.Run("error rows and disabled rows are not answer rows", func(t *testing.T) {
		errorRow := &spectypes.ErrorRow{ID: "e", Interface: spectypes.InterfaceList{"rest"}, Methods: []string{"POST /tx"}, Code: json.RawMessage(`5`), Error: "X"}
		disabled := &spectypes.ErrorRow{ID: "d", Interface: spectypes.InterfaceList{"rest"}, Methods: []string{"POST /tx"}, Answer: true, Disabled: true}
		_, ok := MatchAnswerRow([]*spectypes.ErrorRow{errorRow, disabled}, "rest", "POST /tx", true, true, decode(t, `{"code": 5}`))
		require.False(t, ok)
	})
}

// GraphQL aliases (PRD §2.2, §8.2): a row names fields, never aliases; the reader reads each
// occurrence of a root field with its alias replaced by the field's name, so the same field asked
// twice under two aliases is read twice, and an error's aliased path matches a row's path.
func TestMatchAnswerRowPerOccurrence(t *testing.T) {
	rows := []*spectypes.ErrorRow{suiTxNotFound}

	t.Run("an aliased root field answering null matches the not-found row", func(t *testing.T) {
		matches := MatchAnswerRowPerOccurrence(rows, []GraphQLOccurrence{{Field: "transaction", Alias: "tx"}}, false, true, decode(t, `{"data": {"tx": null}}`))
		require.Len(t, matches, 1)
		require.Equal(t, "sui.tx_not_found", matches[0].Row.ID)
		require.Equal(t, "tx", matches[0].Occurrence.Alias)
	})
	t.Run("the same field asked twice under two aliases is read twice", func(t *testing.T) {
		occurrences := []GraphQLOccurrence{{Field: "transaction", Alias: "a"}, {Field: "transaction", Alias: "b"}}
		matches := MatchAnswerRowPerOccurrence(rows, occurrences, false, true, decode(t, `{"data": {"a": null, "b": {"digest": "D"}}}`))
		require.Len(t, matches, 1, "one of the two answers is a not-found")
		require.Equal(t, "a", matches[0].Occurrence.Alias)
		matches = MatchAnswerRowPerOccurrence(rows, occurrences, false, true, decode(t, `{"data": {"a": null, "b": null}}`))
		require.Len(t, matches, 2, "both are")
	})
	t.Run("an unaliased occurrence reads the field's own entry", func(t *testing.T) {
		matches := MatchAnswerRowPerOccurrence(rows, []GraphQLOccurrence{{Field: "transaction"}}, false, true, decode(t, `{"data": {"transaction": null}}`))
		require.Len(t, matches, 1)
	})
	t.Run("an alias does not read the field's own entry", func(t *testing.T) {
		matches := MatchAnswerRowPerOccurrence(rows, []GraphQLOccurrence{{Field: "transaction", Alias: "tx"}}, false, true, decode(t, `{"data": {"transaction": null, "tx": {"digest": "D"}}}`))
		require.Empty(t, matches, "tx answered with data; the null under the field's name is another occurrence's")
	})
	t.Run("an error's aliased path is read under the field's name", func(t *testing.T) {
		view := graphQLOccurrenceView(decode(t, `{"data": {"tx": null}, "errors": [{"message": "x", "path": ["tx", "digest"]}, {"message": "y", "path": ["other"]}]}`), GraphQLOccurrence{Field: "transaction", Alias: "tx"})
		errs := view.(map[string]any)["errors"].([]any)
		require.Equal(t, []any{"transaction", "digest"}, errs[0].(map[string]any)["path"])
		require.Equal(t, []any{"other"}, errs[1].(map[string]any)["path"], "another field's path is left alone")
		data := view.(map[string]any)["data"].(map[string]any)
		_, hasAlias := data["tx"]
		require.False(t, hasAlias)
		require.Contains(t, data, "transaction")
	})
	t.Run("a reply that is not an object is returned as is", func(t *testing.T) {
		doc := decode(t, `[1, 2]`)
		require.Equal(t, doc, graphQLOccurrenceView(doc, GraphQLOccurrence{Field: "f", Alias: "a"}))
	})
}
