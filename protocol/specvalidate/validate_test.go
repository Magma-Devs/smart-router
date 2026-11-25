package specvalidate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// section decodes raw strictly, as lava-specs CI reads a spec, and checks it as spec index, serving
// served.
func section(t *testing.T, index string, served []string, raw string) []Finding {
	t.Helper()
	decoded, err := DecodeSectionStrict(json.RawMessage(raw))
	require.NoError(t, err)
	servedSet := map[string]bool{}
	for _, apiInterface := range served {
		servedSet[apiInterface] = true
	}
	return Section(spectypes.Spec{Index: index, Errors: decoded}, servedSet)
}

func rules(findings []Finding) []string {
	var out []string
	for _, finding := range findings {
		out = append(out, finding.Rule)
	}
	return out
}

// PRD §2.7's examples pass: router-owned verdicts without a class, a chain's own with one, answer
// rows, a duplicate row, a shape and a disabled row.
func TestSectionAcceptsThePRDExamples(t *testing.T) {
	examples := map[string][]string{
		`{"schema_version": 1, "rows": [
			{"id": "solana.block_cleaned_up", "interface": "jsonrpc", "code": -32001, "error": "CHAIN_STATE_PRUNED", "source": "s"},
			{"id": "solana.node_unhealthy", "interface": "jsonrpc", "code": -32005, "error": "NODE_SOLANA_UNHEALTHY", "class": "C5", "source": "s"},
			{"id": "solana.min_context_slot", "interface": "jsonrpc", "code": -32016, "error": "CHAIN_SOLANA_MIN_CONTEXT_SLOT_NOT_REACHED", "class": "C3", "reason": "behind", "source": "s"},
			{"id": "solana.blockhash_not_found", "interface": "jsonrpc", "contains": "blockhash not found", "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "class": "C2", "source": "s"}]}`: {"jsonrpc"},
		`{"schema_version": 1,
			"shapes": [{"id": "ton.error", "interface": "rest", "code": "/code", "message": "/error", "source": "s"}],
			"rows": [{"id": "ton.block_not_found", "interface": "rest", "scope": "read", "status": 500, "contains": "block not found",
				"error": "CHAIN_TON_BLOCK_NOT_FOUND", "class": "C3", "reason": "not_found", "source": "s"}]}`: {"rest"},
		`{"schema_version": 1, "rows": [
			{"id": "cosmos.broadcast_ok", "interface": "rest", "methods": ["POST /cosmos/tx/v1beta1/txs"], "answer": true, "at": "/tx_response/code", "code": 0, "write": "took_it", "source": "s"},
			{"id": "cosmos.tx_in_mempool", "interface": "rest", "methods": ["POST /cosmos/tx/v1beta1/txs"], "code": "sdk/19", "error": "CHAIN_TX_ALREADY_KNOWN", "write": "took_it", "source": "s"}]}`: {"rest"},
		`{"schema_version": 1, "rows": [{"id": "sui.tx_not_found", "interface": "graphql", "methods": ["transaction"], "answer": true,
			"at": "/data/transaction", "code": null, "error": "CHAIN_SUI_TX_NOT_FOUND", "class": "C3", "reason": "not_found", "source": "s"}]}`: {"graphql"},
		`{"schema_version": 1, "rows": [{"id": "cosmossdk.account_not_found", "disabled": true}]}`: {"rest"},
	}
	for raw, served := range examples {
		require.Empty(t, section(t, "CHAIN", served, raw), raw)
	}
}

// One case per rule: the smallest section that breaks it, and the rule it is reported under.
func TestSectionReportsEachRule(t *testing.T) {
	cases := []struct {
		name, raw, rule string
		served          []string
	}{
		{"an unknown schema version", `{"schema_version": 2}`, "4.1/schema_version", nil},
		{"a class no row gives", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","code":1,"error":"CHAIN_SOLANA_A","class":"C6"}]}`, "4.1/class", nil},
		{"a reason its class doesn't take", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","code":1,"error":"CHAIN_SOLANA_A","class":"C2","reason":"pruned"}]}`, "4.1/reason", nil},
		{"a C3 without its reason", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","code":1,"error":"CHAIN_SOLANA_A","class":"C3"}]}`, "4.1/reason", nil},
		{"a write verdict R-L doesn't have", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","code":1,"error":"CHAIN_SOLANA_A","class":"C2","write":"sent"}]}`, "4.1/write", nil},
		{"write on a read row", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","scope":"read","code":1,"error":"CHAIN_SOLANA_A","class":"C2","write":"refused_it"}]}`, "4.1/write", nil},
		{"a chain's own verdict without a class", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","code":1,"error":"CHAIN_SOLANA_NEW"}]}`, "4.1/class", nil},
		{"an own name outside a family", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","code":1,"error":"CHAIN_ACME_A","class":"C2"}]}`, "4.1/error", nil},
		{"a protocol verdict", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","code":1,"error":"PROTOCOL_CONNECTION_REFUSED"}]}`, "4.2/error", nil},
		{"a rate-limit verdict", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","code":1,"error":"NODE_RATE_LIMITED"}]}`, "4.2/error", nil},
		{"a C6 verdict", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","code":1,"error":"NODE_ACCESS_DENIED"}]}`, "4.2/error", nil},
		{"the unknown default", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","code":1,"error":"UNKNOWN_ERROR"}]}`, "4.2/error", nil},
		{"already-known off a duplicate row", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","code":1,"error":"CHAIN_TX_ALREADY_KNOWN"}]}`, "4.2/error", nil},
		{"a status the status decides", `{"schema_version":1,"rows":[{"id":"x.a","interface":"rest","status":403,"contains":"nope","error":"CHAIN_SOLANA_A","class":"C2"}]}`, "4.3/status", nil},
		{"a gRPC status the status decides", `{"schema_version":1,"rows":[{"id":"x.a","interface":"grpc","status":"PERMISSION_DENIED","error":"CHAIN_SOLANA_A","class":"C2"}]}`, "4.3/status", nil},
		{"RESOURCE_EXHAUSTED on a C5 row", `{"schema_version":1,"rows":[{"id":"x.a","interface":"grpc","status":"RESOURCE_EXHAUSTED","error":"CHAIN_SOLANA_A","class":"C5"}]}`, "4.3/status", nil},
		{"an HTTP status on a gRPC row", `{"schema_version":1,"rows":[{"id":"x.a","interface":"grpc","status":404,"error":"CHAIN_SOLANA_A","class":"C2"}]}`, "4.3/status", nil},
		{"code 429", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","code":"429","error":"CHAIN_SOLANA_A","class":"C4"}]}`, "4.3/code", nil},
		{"the router's status prefix", `{"schema_version":1,"rows":[{"id":"x.a","interface":"jsonrpc","contains":"HTTP 429","error":"CHAIN_SOLANA_A","class":"C4"}]}`, "4.3/text", nil},
		{"grpc-go's gateway text", `{"schema_version":1,"rows":[{"id":"x.a","interface":"grpc","regex":"(?i)unexpected http status code","error":"CHAIN_SOLANA_A","class":"C5"}]}`, "4.3/text", nil},
		{"a route-refusal phrase on rest", `{"schema_version":1,"rows":[{"id":"x.a","interface":"rest","contains":"route not found","error":"CHAIN_SOLANA_A","class":"C3","reason":"method_off"}]}`, "4.3/text", nil},
		{"an answer row without methods", `{"schema_version":1,"rows":[{"id":"x.a","interface":"rest","answer":true,"write":"took_it"}]}`, "4.1/answer", nil},
		{"an answer row giving neither", `{"schema_version":1,"rows":[{"id":"x.a","interface":"rest","methods":["GET /a"],"answer":true,"error":"CHAIN_SOLANA_A","class":"C2"}]}`, "4.1/answer", nil},
		{"at on an error row", `{"schema_version":1,"rows":[{"id":"x.a","interface":"rest","at":"/a","code":1,"error":"CHAIN_SOLANA_A","class":"C2"}]}`, "4.1/at", nil},
		{"path off GraphQL", `{"schema_version":1,"rows":[{"id":"x.a","interface":"rest","path":true,"code":1,"error":"CHAIN_SOLANA_A","class":"C2"}]}`, "4.1/path", nil},
		{"an error row with no match", `{"schema_version":1,"rows":[{"id":"x.a","interface":"rest","status":404,"error":"CHAIN_SOLANA_A","class":"C2"}]}`, "2.2/match", nil},
		{"a regex that doesn't compile", `{"schema_version":1,"rows":[{"id":"x.a","interface":"rest","regex":"(","error":"CHAIN_SOLANA_A","class":"C2"}]}`, "2.2/regex", nil},
		{"an interface the spec doesn't serve", `{"schema_version":1,"rows":[{"id":"x.a","interface":"grpc","code":1,"error":"CHAIN_SOLANA_A","class":"C2"}]}`, "2.2/interface", []string{"jsonrpc"}},
		{"a shape pointer that isn't one", `{"schema_version":1,"shapes":[{"id":"x.s","interface":"rest","code":"code"}]}`, "4.1/shape_pointer", nil},
		{"two rows with one id", `{"schema_version":1,"rows":[{"id":"x.a","interface":"rest","code":1,"error":"CHAIN_SOLANA_A","class":"C2"},{"id":"x.a","interface":"rest","code":2,"error":"CHAIN_SOLANA_B","class":"C2"}]}`, "2.2/id", nil},
		{"a disabled row with more than its id", `{"schema_version":1,"rows":[{"id":"x.a","disabled":true,"code":1}]}`, "2.2/disabled", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			served := tc.served
			if served == nil {
				served = []string{"jsonrpc", "tendermintrpc", "rest", "grpc", "graphql"}
			}
			findings := section(t, "SOLANA", served, tc.raw)
			require.Contains(t, rules(findings), tc.rule, "%v", findings)
		})
	}
}

// The own-name families are the registry's names, upper-cased. PRD §2.7's Cosmos write-rows
// example names CHAIN_COSMOS_*, while the registry calls the family CosmosSDK: that example is
// reported until the PRD and the registry agree on the family's token.
func TestOwnNamesFollowTheRegistrysFamilies(t *testing.T) {
	raw := `{"schema_version":1,"rows":[{"id":"cosmos.insufficient_funds","interface":"rest","methods":["POST /cosmos/tx/v1beta1/txs"],"code":"sdk/5",
		"error":"%s","class":"C2","write":"refused_it","source":"s"}]}`
	require.Empty(t, section(t, "COSMOSSDK", []string{"rest"}, strings.Replace(raw, "%s", "CHAIN_COSMOSSDK_INSUFFICIENT_FUNDS", 1)))
	require.Equal(t, []string{"4.1/error"}, rules(section(t, "COSMOSSDK", []string{"rest"}, strings.Replace(raw, "%s", "CHAIN_COSMOS_INSUFFICIENT_FUNDS", 1))))
}

// A key this router version doesn't know is a schema mismatch, which drops the section as
// schema_unknown rather than a rule violation.
func TestAnUnknownKeyIsASchemaMismatch(t *testing.T) {
	dir := t.TempDir()
	proposal := `{"proposal":{"specs":[{"index":"SOLANA","api_collections":[{"collection_data":{"api_interface":"jsonrpc"}}],
		"errors":{"schema_version":1,"rows":[{"id":"solana.a","interface":"jsonrpc","code":1,"error":"CHAIN_SOLANA_A","class":"C2","severity":"high"}]}}]}}`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "solana.json"), []byte(proposal), 0o600))
	loaded, err := LoadPath(dir)
	require.NoError(t, err)
	findings := Validate(loaded)
	require.Len(t, findings, 1)
	require.Equal(t, ReasonSchemaUnknown, findings[0].Reason)
	require.Contains(t, findings[0].Detail, "severity")
}

// An interface a spec serves through its imports is one it serves.
func TestServedInterfacesFollowImports(t *testing.T) {
	specs := map[string]spectypes.Spec{
		"COSMOSSDK": {Index: "COSMOSSDK", ApiCollections: []*spectypes.ApiCollection{{CollectionData: spectypes.CollectionData{ApiInterface: "rest"}}}},
		"COSMOSHUB": {Index: "COSMOSHUB", Imports: []string{"COSMOSSDK"}, ApiCollections: []*spectypes.ApiCollection{{CollectionData: spectypes.CollectionData{ApiInterface: "grpc"}}}},
	}
	served, ok := ServedInterfaces(specs, "COSMOSHUB")
	require.True(t, ok)
	require.Equal(t, map[string]bool{"rest": true, "grpc": true}, served)
	_, ok = ServedInterfaces(map[string]spectypes.Spec{"COSMOSHUB": specs["COSMOSHUB"]}, "COSMOSHUB")
	require.False(t, ok, "an import the caller didn't load")
}

// Router CI runs the validator over every bundled section (PRD §6).
func TestBundledSpecsPassTheValidator(t *testing.T) {
	loaded, err := LoadPath("../../specs")
	require.NoError(t, err)
	require.NotEmpty(t, loaded)
	require.Empty(t, Validate(loaded))
}

// The JSON Schema names exactly the fields the router decodes, at every level.
func TestTheSchemaNamesEveryFieldTheRouterDecodes(t *testing.T) {
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Defs       map[string]struct {
			Properties map[string]struct {
				Properties map[string]json.RawMessage `json:"properties"`
			} `json:"properties"`
		} `json:"$defs"`
	}
	require.NoError(t, json.Unmarshal(Schema, &schema))
	require.Equal(t, jsonFields(reflect.TypeOf(spectypes.ErrorsSection{})), keys(schema.Properties))
	rowProperties := map[string]json.RawMessage{}
	for name := range schema.Defs["row"].Properties {
		rowProperties[name] = nil
	}
	require.Equal(t, jsonFields(reflect.TypeOf(spectypes.ErrorRow{})), keys(rowProperties))
	shapeProperties := map[string]json.RawMessage{}
	for name := range schema.Defs["shape"].Properties {
		shapeProperties[name] = nil
	}
	require.Equal(t, jsonFields(reflect.TypeOf(spectypes.ErrorShape{})), keys(shapeProperties))
	require.Equal(t, jsonFields(reflect.TypeOf(spectypes.ShapeWhen{})), keys(schema.Defs["shape"].Properties["when"].Properties))
}

// The lists the validator enforces are the router's own.
func TestTheValidatorReadsTheRoutersLists(t *testing.T) {
	require.True(t, ownNamePattern().MatchString("CHAIN_SOLANA_BLOCKHASH_NOT_FOUND"))
	require.Contains(t, common.RouteRefusalPhrases(), "endpoint not found")
	writes := keeper.WriteMethods()
	require.NotEmpty(t, writes)
	var sendRaw bool
	for _, method := range writes {
		sendRaw = sendRaw || (method.Interface == "jsonrpc" && method.Name == "eth_sendRawTransaction")
	}
	require.True(t, sendRaw, "the write-method table the lock enforces")
}

func jsonFields(typ reflect.Type) []string {
	var names []string
	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("json")
		if name, _, _ := strings.Cut(tag, ","); name != "" && name != "-" {
			names = append(names, name)
		}
	}
	return sorted(names)
}

func keys(m map[string]json.RawMessage) []string {
	var names []string
	for name := range m {
		names = append(names, name)
	}
	return sorted(names)
}

func sorted(names []string) []string {
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	return names
}
