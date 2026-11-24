package spec

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// PRD §2.7's examples decode as written, and the values a row matches by JSON type survive as the
// file wrote them: a code of -32005, "sdk/32", 0 and null are four different things.
func TestErrorsSectionDecodesThePRDExamples(t *testing.T) {
	solana := `{"index": "SOLANA", "name": "solana", "enabled": true, "errors": {
	  "schema_version": 1,
	  "rows": [
	    { "id": "solana.block_cleaned_up", "interface": "jsonrpc", "code": -32001, "error": "CHAIN_STATE_PRUNED", "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs" },
	    { "id": "solana.min_context_slot", "interface": "jsonrpc", "code": -32016, "error": "CHAIN_SOLANA_MIN_CONTEXT_SLOT_NOT_REACHED", "class": "C3", "reason": "behind", "source": "s" },
	    { "id": "solana.blockhash_not_found", "interface": "jsonrpc", "contains": "blockhash not found", "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "class": "C2", "source": "s" }
	  ]}}`
	var spec Spec
	require.NoError(t, json.Unmarshal([]byte(solana), &spec))
	require.NotNil(t, spec.Errors)
	require.Equal(t, 1, spec.Errors.SchemaVersion)
	require.True(t, spec.Errors.Inherits(), "inherit defaults to true")
	require.Len(t, spec.Errors.Rows, 3)
	pruned := spec.Errors.Rows[0]
	require.Equal(t, InterfaceList{"jsonrpc"}, pruned.Interface)
	require.True(t, pruned.HasCode())
	require.JSONEq(t, `-32001`, string(pruned.Code))
	require.Equal(t, "", pruned.Class, "a router-owned verdict carries no class")
	require.Equal(t, ErrorRowScopeBoth, pruned.EffectiveScope())
	require.Equal(t, ErrorRowFieldText, pruned.EffectiveField())
	require.Equal(t, ErrorWriteMayHaveRun, pruned.EffectiveWrite())
	behind := spec.Errors.Rows[1]
	require.Equal(t, ErrorClassC3, behind.Class)
	require.Equal(t, ErrorReasonBehind, behind.EffectiveReason())
	blockhash := spec.Errors.Rows[2]
	require.False(t, blockhash.HasCode())
	require.Equal(t, "blockhash not found", blockhash.Contains)
	require.Equal(t, ErrorReasonInvalid, blockhash.EffectiveReason(), "a C2 row's reason defaults to invalid")

	ton := `{"errors": { "schema_version": 1,
	  "shapes": [ { "id": "ton.error", "interface": "rest", "code": "/code", "message": "/error", "source": "s" } ],
	  "rows": [ { "id": "ton.block_not_found", "interface": "rest", "scope": "read", "status": 500, "contains": "block not found",
	    "error": "CHAIN_TON_BLOCK_NOT_FOUND", "class": "C3", "reason": "not_found", "source": "s" } ] }}`
	// A fresh Spec per example: json.Unmarshal into a reused pointer merges into the structs it
	// already points at, which is not how the loader decodes a file.
	spec = Spec{}
	require.NoError(t, json.Unmarshal([]byte(ton), &spec))
	require.Len(t, spec.Errors.Shapes, 1)
	require.Equal(t, "/code", spec.Errors.Shapes[0].Code)
	require.Equal(t, "/error", spec.Errors.Shapes[0].Message)
	require.Nil(t, spec.Errors.Shapes[0].When)
	row := spec.Errors.Rows[0]
	require.JSONEq(t, `500`, string(row.Status))
	require.Equal(t, ErrorRowScopeRead, row.EffectiveScope())
	require.Equal(t, "", row.EffectiveWrite(), "a read row has no write verdict")

	cosmos := `{"errors": { "schema_version": 1,
	  "shapes": [ { "id": "cosmos.broadcast", "interface": "rest", "methods": ["POST /cosmos/tx/v1beta1/txs"],
	    "when": { "at": "/tx_response/code", "not": 0 },
	    "code": "/tx_response/code", "space": "/tx_response/codespace", "message": "/tx_response/raw_log", "source": "s" } ],
	  "rows": [
	    { "id": "cosmos.broadcast_ok", "interface": "rest", "methods": ["POST /cosmos/tx/v1beta1/txs"], "answer": true, "at": "/tx_response/code", "code": 0, "write": "took_it", "source": "s" },
	    { "id": "cosmos.tx_in_mempool", "interface": ["rest", "grpc"], "code": "sdk/19", "error": "CHAIN_TX_ALREADY_KNOWN", "write": "took_it", "source": "s" },
	    { "id": "sui.tx_not_found", "interface": "graphql", "methods": ["transaction"], "answer": true, "at": "/data/transaction", "code": null, "error": "CHAIN_SUI_TX_NOT_FOUND", "class": "C3", "reason": "not_found", "source": "s" },
	    { "id": "cosmossdk.account_not_found", "disabled": true }
	  ], "write_rows": ["rest"], "inherit": false }}`
	spec = Spec{}
	require.NoError(t, json.Unmarshal([]byte(cosmos), &spec))
	shape := spec.Errors.Shapes[0]
	require.NotNil(t, shape.When)
	require.Equal(t, "/tx_response/code", shape.When.At)
	require.JSONEq(t, `0`, string(shape.When.Not))
	require.Empty(t, shape.When.Equals)
	require.True(t, shape.AppliesToMethod("POST /cosmos/tx/v1beta1/txs"))
	require.False(t, shape.AppliesToMethod("GET /cosmos/tx/v1beta1/txs"))
	rows := spec.Errors.Rows
	require.True(t, rows[0].Answer)
	require.JSONEq(t, `0`, string(rows[0].Code))
	require.Equal(t, ErrorWriteTookIt, rows[0].EffectiveWrite())
	require.Equal(t, InterfaceList{"rest", "grpc"}, rows[1].Interface, "a list of interfaces")
	require.True(t, rows[1].AppliesToInterface("grpc"))
	require.False(t, rows[1].AppliesToInterface("jsonrpc"))
	require.JSONEq(t, `"sdk/19"`, string(rows[1].Code), "a string code stays a string")
	require.True(t, rows[2].HasCode(), "an explicit null is a code to match")
	require.JSONEq(t, `null`, string(rows[2].Code))
	require.True(t, rows[2].AppliesToMethod("transaction"))
	require.False(t, rows[2].AppliesToMethod("object"))
	require.True(t, rows[3].Disabled)
	require.False(t, rows[3].AppliesToInterface("rest"), "a disabling entry names no interface")
	require.Equal(t, []string{"rest"}, spec.Errors.WriteRows)
	require.False(t, spec.Errors.Inherits())
}

func TestErrorsSectionIsOptionalAndRoundTrips(t *testing.T) {
	var spec Spec
	require.NoError(t, json.Unmarshal([]byte(`{"index": "X"}`), &spec))
	require.Nil(t, spec.Errors)
	require.True(t, spec.Errors.Inherits(), "no section: the chain may still inherit")

	empty := `{"index": "X", "errors": {"schema_version": 1, "inherit": false}}`
	require.NoError(t, json.Unmarshal([]byte(empty), &spec))
	require.NotNil(t, spec.Errors)
	require.False(t, spec.Errors.Inherits())
	require.Empty(t, spec.Errors.Rows, "a section without rows is valid: the chain has no spec rows")

	out, err := json.Marshal(spec.Errors)
	require.NoError(t, err)
	require.JSONEq(t, `{"schema_version": 1, "inherit": false}`, string(out))

	var list InterfaceList
	require.NoError(t, json.Unmarshal([]byte(`"rest"`), &list))
	one, err := json.Marshal(list)
	require.NoError(t, err)
	require.JSONEq(t, `"rest"`, string(one), "one interface writes bare")
	require.NoError(t, json.Unmarshal([]byte(`["rest","grpc"]`), &list))
	many, err := json.Marshal(list)
	require.NoError(t, err)
	require.JSONEq(t, `["rest","grpc"]`, string(many))
	require.Error(t, json.Unmarshal([]byte(`5`), &list), "neither a name nor a list")
}
