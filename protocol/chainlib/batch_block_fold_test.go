package chainlib

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// MAG-4381: a request made of several elements (a JSON-RPC or Tendermint batch's calls, a GraphQL
// operation's root fields) gets one requested block, folded from its elements' blocks. An element
// whose api names no block at all — Sui's chainIdentifier, eth_chainId — parses to NOT_APPLICABLE,
// and the fold read that as "nobody could parse this element", which makes the whole request
// unjudgeable: `{ epoch { epochId } chainIdentifier }` was NOT_APPLICABLE, so it was never cached,
// and a deep pinned read next to it never reached the archive rule.
//
// A block that FAILED to parse also comes out as NOT_APPLICABLE, and that one must keep the request
// unjudgeable: folded away, a pinned read whose block did not parse would be cached and routed as
// a latest read. So the fold asks the element's api whether it names a block, and never reads the
// value alone. The failed-parse rows below pin that side.

// blockFoldCase is one request and the (latest, earliest) requested blocks it must fold to.
type blockFoldCase struct {
	name     string
	elements []string
	latest   int64
	earliest int64
}

// blockFoldCases is the matrix every interface is run through, written in element names the
// per-interface helpers turn into a request: "none" is an element whose api names no block,
// "latest" one that reads latest, "pinned" one at block 100, "garbage" one whose declared block
// does not parse.
var blockFoldCases = []blockFoldCase{
	{name: "none then latest stays latest", elements: []string{"none", "latest"}, latest: spectypes.LATEST_BLOCK, earliest: spectypes.LATEST_BLOCK},
	{name: "latest then none stays latest", elements: []string{"latest", "none"}, latest: spectypes.LATEST_BLOCK, earliest: spectypes.LATEST_BLOCK},
	{name: "none then a pinned block stays pinned", elements: []string{"none", "pinned"}, latest: 100, earliest: 100},
	{name: "a pinned block then none stays pinned", elements: []string{"pinned", "none"}, latest: 100, earliest: 100},
	{name: "none, latest and a pinned block", elements: []string{"none", "latest", "pinned"}, latest: spectypes.LATEST_BLOCK, earliest: 100},
	{name: "a pinned block, none and latest", elements: []string{"pinned", "none", "latest"}, latest: spectypes.LATEST_BLOCK, earliest: 100},
	{name: "latest between two blockless elements", elements: []string{"none", "latest", "none"}, latest: spectypes.LATEST_BLOCK, earliest: spectypes.LATEST_BLOCK},
	{name: "only blockless elements have no block", elements: []string{"none", "none"}, latest: spectypes.NOT_APPLICABLE, earliest: spectypes.NOT_APPLICABLE},
	// A block that did not parse keeps the request unjudgeable, wherever it sits and whatever
	// sits next to it: these hold on main and must keep holding.
	{name: "a failed parse then latest stays unjudgeable", elements: []string{"garbage", "latest"}, latest: spectypes.NOT_APPLICABLE, earliest: spectypes.NOT_APPLICABLE},
	{name: "latest then a failed parse stays unjudgeable", elements: []string{"latest", "garbage"}, latest: spectypes.NOT_APPLICABLE, earliest: spectypes.NOT_APPLICABLE},
	{name: "none then a failed parse stays unjudgeable", elements: []string{"none", "garbage"}, latest: spectypes.NOT_APPLICABLE, earliest: spectypes.NOT_APPLICABLE},
	{name: "a failed parse then none stays unjudgeable", elements: []string{"garbage", "none"}, latest: spectypes.NOT_APPLICABLE, earliest: spectypes.NOT_APPLICABLE},
	{name: "a pinned block, none and a failed parse stays unjudgeable", elements: []string{"pinned", "none", "garbage"}, latest: spectypes.NOT_APPLICABLE, earliest: spectypes.NOT_APPLICABLE},
}

func requireFoldedBlocks(t *testing.T, chainMessage ChainMessage, test blockFoldCase) {
	t.Helper()
	latest, earliest := chainMessage.RequestedBlock()
	require.Equal(t, test.latest, latest, "latest requested block")
	require.Equal(t, test.earliest, earliest, "earliest requested block")
	// The cache-write gate skips exactly this value ("cache write skipped: NOT_APPLICABLE
	// block"), so a request that names a block somewhere must not land on it.
	if test.latest != spectypes.NOT_APPLICABLE {
		require.NotEqual(t, spectypes.NOT_APPLICABLE, latest, "the request would be skipped by the cache")
	}
}

// suiBlockFoldSpec mirrors lava-specs sui.json's graphql collection where blocks are concerned:
// chainIdentifier names no block (PARSER_FUNC_EMPTY), epoch reads latest (PARSER_FUNC_DEFAULT),
// checkpoint reads its sequenceNumber argument and defaults to latest. pinnedRead is a test api with
// a declared block and no default, so an argument that does not parse fails the parse.
func suiBlockFoldSpec() spectypes.Spec {
	return spectypes.Spec{
		Index:                         "SUI",
		Name:                          "sui mainnet",
		Enabled:                       true,
		AverageBlockTime:              222,
		AllowedBlockLagForQosSync:     46,
		BlockDistanceForFinalizedData: 1,
		BlocksInFinalizationProof:     1,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGraphQL, Type: http.MethodPost},
			Apis: []*spectypes.Api{
				{
					Name:         "chainIdentifier",
					Enabled:      true,
					ComputeUnits: 10,
					Category:     spectypes.SpecCategory{Deterministic: true},
					BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
				},
				{
					Name:         "epoch",
					Enabled:      true,
					ComputeUnits: 10,
					BlockParsing: spectypes.BlockParser{ParserArg: []string{"latest"}, ParserFunc: spectypes.PARSER_FUNC_DEFAULT},
				},
				{
					Name:         "checkpoint",
					Enabled:      true,
					ComputeUnits: 20,
					BlockParsing: spectypes.BlockParser{
						ParserArg:    []string{"sequenceNumber", ":"},
						ParserFunc:   spectypes.PARSER_FUNC_PARSE_DICTIONARY,
						DefaultValue: "latest",
					},
				},
				{
					Name:         "pinnedRead",
					Enabled:      true,
					ComputeUnits: 20,
					BlockParsing: spectypes.BlockParser{
						ParserArg:  []string{"sequenceNumber", ":"},
						ParserFunc: spectypes.PARSER_FUNC_PARSE_DICTIONARY,
					},
				},
			},
		}},
	}
}

// graphQLBlockFoldField is the root field each matrix element becomes.
var graphQLBlockFoldField = map[string]string{
	"none":    "chainIdentifier",
	"latest":  "epoch { epochId }",
	"pinned":  "checkpoint(sequenceNumber: 100) { digest }",
	"garbage": `pinnedRead(sequenceNumber: \"abc\") { digest }`,
}

func TestGraphQLBlocklessRootFieldDoesNotDecideTheRequestedBlock(t *testing.T) {
	chainParser, err := NewGraphQLChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(suiBlockFoldSpec())

	// The two queries Tom's real-node run logged "cache write skipped: NOT_APPLICABLE block" for.
	for _, query := range []string{`{ epoch { epochId } chainIdentifier }`, `{ chainIdentifier checkpoint { sequenceNumber } }`} {
		t.Run(query, func(t *testing.T) {
			chainMessage, err := chainParser.ParseMsg("", []byte(`{"query":"`+query+`"}`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			requireFoldedBlocks(t, chainMessage, blockFoldCase{latest: spectypes.LATEST_BLOCK, earliest: spectypes.LATEST_BLOCK})
		})
	}

	for _, test := range blockFoldCases {
		t.Run(test.name, func(t *testing.T) {
			fields := make([]string, 0, len(test.elements))
			for _, element := range test.elements {
				fields = append(fields, graphQLBlockFoldField[element])
			}
			body := `{"query":"{ ` + strings.Join(fields, " ") + ` }"}`
			chainMessage, err := chainParser.ParseMsg("", []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			requireFoldedBlocks(t, chainMessage, test)
		})
	}
}

// evmBlockFoldSpec has the shape of an EVM jsonrpc collection where blocks are concerned:
// eth_chainId names no block, eth_blockNumber reads latest, getBlockAt reads its first argument and
// has no default, so an argument that does not parse fails the parse.
func evmBlockFoldSpec() spectypes.Spec {
	return spectypes.Spec{
		Index:                         "ETH1",
		Name:                          "ethereum mainnet",
		Enabled:                       true,
		AverageBlockTime:              12000,
		AllowedBlockLagForQosSync:     2,
		BlockDistanceForFinalizedData: 64,
		BlocksInFinalizationProof:     3,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceJsonRPC, Type: http.MethodPost},
			Apis: []*spectypes.Api{
				{
					Name:         "eth_chainId",
					Enabled:      true,
					ComputeUnits: 10,
					Category:     spectypes.SpecCategory{Deterministic: true},
					BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
				},
				{
					Name:         "eth_blockNumber",
					Enabled:      true,
					ComputeUnits: 10,
					BlockParsing: spectypes.BlockParser{ParserArg: []string{"latest"}, ParserFunc: spectypes.PARSER_FUNC_DEFAULT},
				},
				{
					Name:         "getBlockAt",
					Enabled:      true,
					ComputeUnits: 20,
					Category:     spectypes.SpecCategory{Deterministic: true},
					BlockParsing: spectypes.BlockParser{ParserArg: []string{"0"}, ParserFunc: spectypes.PARSER_FUNC_PARSE_BY_ARG},
				},
			},
		}},
	}
}

// jsonRPCBlockFoldCall is the batch element each matrix element becomes.
var jsonRPCBlockFoldCall = map[string]string{
	"none":    `"method":"eth_chainId","params":[]`,
	"latest":  `"method":"eth_blockNumber","params":[]`,
	"pinned":  `"method":"getBlockAt","params":["0x64"]`,
	"garbage": `"method":"getBlockAt","params":["not a block"]`,
}

func jsonRPCBatch(elements []string, call map[string]string) string {
	calls := make([]string, 0, len(elements))
	for i, element := range elements {
		calls = append(calls, `{"jsonrpc":"2.0","id":`+string(rune('1'+i))+`,`+call[element]+`}`)
	}
	return "[" + strings.Join(calls, ",") + "]"
}

func TestJsonRPCBatchBlocklessElementDoesNotDecideTheRequestedBlock(t *testing.T) {
	chainParser, err := NewJrpcChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(evmBlockFoldSpec())

	for _, test := range blockFoldCases {
		t.Run(test.name, func(t *testing.T) {
			chainMessage, err := chainParser.ParseMsg("", []byte(jsonRPCBatch(test.elements, jsonRPCBlockFoldCall)), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			requireFoldedBlocks(t, chainMessage, test)
		})
	}
}

// tendermintBlockFoldSpec has the shape of a tendermintrpc collection where blocks are concerned:
// status names no block, abci_info reads latest, block reads its first argument with no default.
func tendermintBlockFoldSpec() spectypes.Spec {
	return spectypes.Spec{
		Index:                         "LAV1",
		Name:                          "lava mainnet",
		Enabled:                       true,
		AverageBlockTime:              15000,
		AllowedBlockLagForQosSync:     2,
		BlockDistanceForFinalizedData: 0,
		BlocksInFinalizationProof:     1,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceTendermintRPC, Type: ""},
			Apis: []*spectypes.Api{
				{
					Name:         "status",
					Enabled:      true,
					ComputeUnits: 10,
					BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
				},
				{
					Name:         "abci_info",
					Enabled:      true,
					ComputeUnits: 10,
					BlockParsing: spectypes.BlockParser{ParserArg: []string{"latest"}, ParserFunc: spectypes.PARSER_FUNC_DEFAULT},
				},
				{
					Name:         "block",
					Enabled:      true,
					ComputeUnits: 20,
					Category:     spectypes.SpecCategory{Deterministic: true},
					BlockParsing: spectypes.BlockParser{ParserArg: []string{"0"}, ParserFunc: spectypes.PARSER_FUNC_PARSE_BY_ARG},
				},
			},
		}},
	}
}

var tendermintBlockFoldCall = map[string]string{
	"none":    `"method":"status","params":[]`,
	"latest":  `"method":"abci_info","params":[]`,
	"pinned":  `"method":"block","params":["100"]`,
	"garbage": `"method":"block","params":["not a block"]`,
}

func TestTendermintBatchBlocklessElementDoesNotDecideTheRequestedBlock(t *testing.T) {
	chainParser, err := NewTendermintRpcChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(tendermintBlockFoldSpec())

	for _, test := range blockFoldCases {
		t.Run(test.name, func(t *testing.T) {
			chainMessage, err := chainParser.ParseMsg("", []byte(jsonRPCBatch(test.elements, tendermintBlockFoldCall)), "", nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			requireFoldedBlocks(t, chainMessage, test)
		})
	}
}
