package chainlib

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

const graphqlConnectionType = "POST"

// graphqlTestSpec mirrors the shape of lava-specs sui.json's graphql collection: one api per root
// field, each carrying its own compute units and category, plus the GET_BLOCKNUM directive sui.json
// declares. That directive is on serviceConfig, an ordinary query field, read at
// serviceConfig.availableRange.last.sequenceNumber.
func graphqlTestSpec() spectypes.Spec {
	emptyBlockParsing := spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY}
	return spectypes.Spec{
		Index:                         "SUI",
		Name:                          "sui mainnet",
		Enabled:                       true,
		AverageBlockTime:              400,
		AllowedBlockLagForQosSync:     10,
		BlockDistanceForFinalizedData: 1,
		BlocksInFinalizationProof:     1,
		ApiCollections: []*spectypes.ApiCollection{
			{
				Enabled: true,
				CollectionData: spectypes.CollectionData{
					ApiInterface: spectypes.APIInterfaceGraphQL,
					Type:         graphqlConnectionType,
				},
				Apis: []*spectypes.Api{
					{
						Name:         "chainIdentifier",
						Enabled:      true,
						ComputeUnits: 10,
						Category:     spectypes.SpecCategory{Deterministic: true},
						BlockParsing: emptyBlockParsing,
					},
					{
						Name:         "checkpoint",
						Enabled:      true,
						ComputeUnits: 20,
						Category:     spectypes.SpecCategory{Deterministic: true},
						BlockParsing: emptyBlockParsing,
					},
					{
						Name:         "serviceConfig",
						Enabled:      true,
						ComputeUnits: 10,
						BlockParsing: spectypes.BlockParser{ParserArg: []string{"latest"}, ParserFunc: spectypes.PARSER_FUNC_DEFAULT},
					},
					{
						// The write surface. stateful:1 is what ValidateNoStatefulPolicies reads
						// to refuse a cross-validation policy on a transaction submission.
						Name:         "executeTransaction",
						Enabled:      true,
						ComputeUnits: 100,
						Category:     spectypes.SpecCategory{Deterministic: false, Stateful: 1},
						BlockParsing: emptyBlockParsing,
					},
				},
				ParseDirectives: []*spectypes.ParseDirective{
					{
						FunctionTag:      spectypes.FUNCTION_TAG_GET_BLOCKNUM,
						FunctionTemplate: `{"query":"{ serviceConfig { availableRange(type: \"Checkpoint\") { last { sequenceNumber } } } }"}`,
						ApiName:          "serviceConfig",
						ResultParsing: spectypes.BlockParser{
							ParserArg:  []string{"0", "data", "serviceConfig", "availableRange", "last", "sequenceNumber"},
							ParserFunc: spectypes.PARSER_FUNC_PARSE_CANONICAL,
						},
					},
				},
			},
		},
	}
}

func newGraphQLTestParser(t *testing.T) *GraphQLChainParser {
	t.Helper()
	chainParser, err := NewGraphQLChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(graphqlTestSpec())
	return chainParser
}

func TestGraphQLChainParserResolvesPerOperationApi(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	tests := []struct {
		name                 string
		body                 string
		expectedApiName      string
		expectedComputeUnits uint64
		expectedStateful     uint32
	}{
		{
			name:                 "read resolves to its own api",
			body:                 `{"query":"{ checkpoint(sequenceNumber: 329083865) { digest } }"}`,
			expectedApiName:      "checkpoint",
			expectedComputeUnits: 20,
		},
		{
			name:                 "mutation resolves to the write api",
			body:                 `{"query":"mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { effects { status } } }"}`,
			expectedApiName:      "executeTransaction",
			expectedComputeUnits: 100,
			expectedStateful:     1,
		},
		{
			name: "alias cannot buy a cheaper api",
			// Identity comes from the schema field, so an alias does not escape the mutation's
			// 100 compute units or its stateful flag.
			body:                 `{"query":"{ cheapLookingRead: executeTransaction(transactionDataBcs: \"x\") { digest } }"}`,
			expectedApiName:      "executeTransaction",
			expectedComputeUnits: 100,
			expectedStateful:     1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chainMessage, err := chainParser.ParseMsg("", []byte(test.body), graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			api := chainMessage.GetApi()
			require.Equal(t, test.expectedApiName, api.Name)
			require.Equal(t, test.expectedComputeUnits, api.ComputeUnits)
			require.Equal(t, test.expectedStateful, api.Category.Stateful)
		})
	}
}

func TestGraphQLChainParserMultiRootCombinesToTheStrictestApi(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	// One request, two root fields: structurally a batch. Compute units add up and the category
	// combines, so a request that pairs a mutation with a read is still a stateful request.
	chainMessage, err := chainParser.ParseMsg("",
		[]byte(`{"query":"{ chainIdentifier checkpoint(sequenceNumber: 1) { digest } }"}`),
		graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	api := chainMessage.GetApi()
	require.Equal(t, "chainIdentifier&checkpoint", api.Name)
	require.EqualValues(t, 30, api.ComputeUnits)
	require.EqualValues(t, 0, api.Category.Stateful)
}

func TestGraphQLChainParserMutationAmongReadsStaysStateful(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	// The hazard this interface exists to prevent: if a request that hides a transaction
	// submission among reads resolved to a read's api, the stateful guard would not see the
	// write and an enabled cross-validation policy would submit it to every provider.
	chainMessage, err := chainParser.ParseMsg("",
		[]byte(`{"query":"mutation { chainIdentifier executeTransaction(transactionDataBcs: \"x\") { digest } }"}`),
		graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	api := chainMessage.GetApi()
	require.Equal(t, "chainIdentifier&executeTransaction", api.Name)
	require.EqualValues(t, 110, api.ComputeUnits)
	require.EqualValues(t, 1, api.Category.Stateful, "a request carrying a mutation must combine to stateful")
	require.False(t, api.Category.Deterministic, "a request carrying a mutation must not be deterministic")
}

func TestGraphQLChainParserMutationIsStatefulWhateverTheSpecSays(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	// checkpoint is a deterministic read in the spec. Selected under a mutation it is a write:
	// the operation type is in the document, and the stateful guard must not have to trust the
	// spec to have no Query/Mutation name overlap.
	chainMessage, err := chainParser.ParseMsg("",
		[]byte(`{"query":"mutation { checkpoint { digest } }"}`),
		graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	api := chainMessage.GetApi()
	require.EqualValues(t, 1, api.Category.Stateful)
	require.False(t, api.Category.Deterministic)

	// The spec's own api is untouched: the same field under a query is still the read.
	readMessage, err := chainParser.ParseMsg("",
		[]byte(`{"query":"{ checkpoint { digest } }"}`),
		graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	require.EqualValues(t, 0, readMessage.GetApi().Category.Stateful)
	require.True(t, readMessage.GetApi().Category.Deterministic)
}

// TestGraphQLChainParserTipDirectiveTagsItsOwnFieldOnly pins what the GET_BLOCKNUM tag says about a
// request, against the directive sui.json declares (MAG-4373; this test used to run against a
// fictional latestCheckpoint field and was named TipObservationIsNotPoisonable). The tag follows
// the root field, so it keeps every other read and every multi-root request out of the tip. It
// cannot keep a caller's own query on the tip field out, aliases included, because on Sui that
// field is an ordinary query field. So the tag does not make a relay-harvested tip safe, and the
// relay path reads no block out of a GraphQL reply at all (rpcsmartrouter
// TestGraphQLUserRelayCarriesNoReplyBlock, TestGraphQLAliasedTipQueryForgesNoTipOrStallEvidence).
func TestGraphQLChainParserTipDirectiveTagsItsOwnFieldOnly(t *testing.T) {
	chainParser := newGraphQLTestParser(t)
	parse := func(body string) ChainMessage {
		chainMessage, err := chainParser.ParseMsg("", []byte(body), graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
		require.NoError(t, err)
		return chainMessage
	}

	// The head poll's own request must resolve to the GET_BLOCKNUM directive, or the per-
	// endpoint ChainTracker cannot read a tip at all.
	directive, _, found := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, found)
	require.True(t, IsFunctionTagOfType(parse(directive.FunctionTemplate), spectypes.FUNCTION_TAG_GET_BLOCKNUM))

	// A read of any other field must not. Under a REST-shaped spec every request would carry the
	// api name "/graphql" and so resolve to this directive.
	require.False(t, IsFunctionTagOfType(parse(`{"query":"{ checkpoint(sequenceNumber: 329083865) { digest } }"}`), spectypes.FUNCTION_TAG_GET_BLOCKNUM))

	// Nor may a request smuggle the tip field in alongside another: the combined api name
	// matches no directive, which is the safe direction.
	require.False(t, IsFunctionTagOfType(
		parse(`{"query":"{ serviceConfig { availableRange(type: \"Checkpoint\") { last { sequenceNumber } } } chainIdentifier }"}`),
		spectypes.FUNCTION_TAG_GET_BLOCKNUM))

	// A caller's own query on the tip field carries the tag exactly like the poll. Identity is the
	// schema field, never the alias, and the alias is what puts another number on the path the
	// directive reads: Sui answers this one with the network's transaction count.
	require.True(t, IsFunctionTagOfType(
		parse(`{"query":"{ serviceConfig { availableRange(type: \"Checkpoint\") { last { sequenceNumber: networkTotalTransactions } } } }"}`),
		spectypes.FUNCTION_TAG_GET_BLOCKNUM),
		"the tag cannot tell the poll from a caller's aliased query; the relay path must not read a tip from either")
}

func TestGraphQLChainParserCraftMessageFromDirective(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	directive, apiCollection, found := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, found)
	require.Equal(t, spectypes.APIInterfaceGraphQL, apiCollection.CollectionData.ApiInterface)

	crafted, err := chainParser.CraftMessage(directive, graphqlConnectionType, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "serviceConfig", crafted.GetApi().Name)
	graphqlMessage, ok := crafted.GetRPCMessage().(*rpcInterfaceMessages.GraphQLMessage)
	require.True(t, ok)
	require.Equal(t, "serviceConfig", graphqlMessage.GetMethod())
}

func TestGraphQLChainParserCraftMessageRejectsEmptyTemplate(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	// GraphQL names its operation in the body and nowhere else, so unlike gRPC an empty
	// template is a spec gap rather than an empty request. Accepting it would poll garbage.
	_, err := chainParser.CraftMessage(
		&spectypes.ParseDirective{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCKNUM, ApiName: "serviceConfig"},
		graphqlConnectionType, nil, nil)
	require.Error(t, err)
}

func TestGraphQLChainParserRejectsMalformedRequest(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	for _, body := range []string{
		`not json`,
		`{"variables":{}}`,
		`{"query":"{ checkpoint("}`,
		`{"query":"query A { chainIdentifier } query B { chainIdentifier }"}`,
	} {
		_, err := chainParser.ParseMsg("", []byte(body), graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
		require.Error(t, err, "body %q should be rejected", body)
	}
}

func TestGraphQLChainParserServesUndeclaredRootFields(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	// A root field the spec does not declare falls back to the default api, as an undeclared
	// method does on every interface. Introspection goes the same way, and a cutover check that
	// POSTs `{ __typename }` relies on it.
	for _, test := range []struct {
		body     string
		rootName string
	}{
		{`{"query":"{ __typename }"}`, "__typename"},
		{`{"query":"{ __schema { queryType { name } } }"}`, "__schema"},
		{`{"query":"{ notInTheSpec }"}`, "notInTheSpec"},
	} {
		chainMessage, err := chainParser.ParseMsg("", []byte(test.body), graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
		require.NoError(t, err, "body %q should be served", test.body)
		api := chainMessage.GetApi()
		require.Equal(t, DefaultApiName+test.rootName, api.Name)
		require.True(t, api.Enabled)
		require.EqualValues(t, 0, api.Category.Stateful)
	}

	// The default api is a deterministic read; the mutation guard still makes an undeclared
	// mutation a write.
	chainMessage, err := chainParser.ParseMsg("",
		[]byte(`{"query":"mutation { notInTheSpec(input: \"x\") { digest } }"}`),
		graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	require.Equal(t, DefaultApiName+"notInTheSpec", chainMessage.GetApi().Name)
	require.EqualValues(t, 1, chainMessage.GetApi().Category.Stateful)
	require.False(t, chainMessage.GetApi().Category.Deterministic)
}

func TestGraphQLChainParserChainBlockStats(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	allowedBlockLag, averageBlockTime, blockDistanceForFinalizedData, blocksInFinalizationProof := chainParser.ChainBlockStats()
	require.EqualValues(t, 10, allowedBlockLag)
	require.Equal(t, 400*time.Millisecond, averageBlockTime)
	require.EqualValues(t, 1, blockDistanceForFinalizedData)
	require.EqualValues(t, 1, blocksInFinalizationProof)
}

func TestGraphQLChainParserNilGuard(t *testing.T) {
	var chainParser *GraphQLChainParser

	require.NotPanics(t, func() {
		chainParser.SetSpec(spectypes.Spec{})
		chainParser.ChainBlockStats()
		_, err := chainParser.getSupportedApi("", "", "")
		require.Error(t, err)
		_, err = chainParser.ParseMsg("", []byte{}, "", nil, extensionslib.ExtensionInfo{})
		require.Error(t, err)
	})
}

func TestGraphQLInterfaceIsRegistered(t *testing.T) {
	chainParser, err := NewChainParser(spectypes.APIInterfaceGraphQL)
	require.NoError(t, err)
	require.IsType(t, &GraphQLChainParser{}, chainParser)
}

func TestGraphQLChainParserCraftMessageFromCraftData(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	directive, apiCollection, found := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, found)

	// The poll path crafts with craftData carrying the template as the body — the branch the
	// head poll actually takes (endpoint_poller.go). The crafted message must land on the
	// directive's own api, so the response is formatted against the right api's rules.
	crafted, err := chainParser.CraftMessage(directive, apiCollection.CollectionData.Type, &CraftData{
		Path:           directive.ApiName,
		Data:           []byte(directive.FunctionTemplate),
		ConnectionType: apiCollection.CollectionData.Type,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "serviceConfig", crafted.GetApi().Name)
	require.True(t, IsFunctionTagOfType(crafted, spectypes.FUNCTION_TAG_GET_BLOCKNUM))
}

func TestGraphQLChainParserCraftMessageRejectsTemplateApiMismatch(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	// A directive whose template invokes a different operation than the directive names would
	// otherwise craft a message carrying the wrong api, and no parse directive at all. Nothing
	// on the poll path reads the directive back, so the degradation would be silent.
	_, err := chainParser.CraftMessage(
		&spectypes.ParseDirective{
			FunctionTag:      spectypes.FUNCTION_TAG_GET_BLOCKNUM,
			ApiName:          "serviceConfig",
			FunctionTemplate: `{"query":"{ chainIdentifier }"}`,
		},
		graphqlConnectionType,
		&CraftData{
			Path:           "serviceConfig",
			Data:           []byte(`{"query":"{ chainIdentifier }"}`),
			ConnectionType: graphqlConnectionType,
		}, nil)
	require.Error(t, err)
}

func TestGraphQLChainParserBlockParsingFromArguments(t *testing.T) {
	// The spec shape sui.json will use for `checkpoint`: PARSE_DICTIONARY reads the named
	// GraphQL argument out of the request body, defaulting to latest when it is absent. This is
	// what separates a pinned historical read from a bare latest read — both on the same root
	// field — and it is what crossValidationFinalityLabel needs to label finality at all.
	chainParser, err := NewGraphQLChainParser()
	require.NoError(t, err)
	spec := graphqlTestSpec()
	for _, apiCollection := range spec.ApiCollections {
		for _, api := range apiCollection.Apis {
			if api.Name == "checkpoint" {
				api.BlockParsing = spectypes.BlockParser{
					ParserArg:    []string{"sequenceNumber", "="},
					ParserFunc:   spectypes.PARSER_FUNC_PARSE_DICTIONARY,
					DefaultValue: "latest",
				}
			}
		}
	}
	chainParser.SetSpec(spec)

	pinned, err := chainParser.ParseMsg("",
		[]byte(`{"query":"{ checkpoint(sequenceNumber: 329083865) { digest } }"}`),
		graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	pinnedBlock, _ := pinned.RequestedBlock()
	require.EqualValues(t, 329083865, pinnedBlock, "a pinned checkpoint must parse to its own block")

	bare, err := chainParser.ParseMsg("",
		[]byte(`{"query":"{ checkpoint { sequenceNumber } }"}`),
		graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	bareBlock, _ := bare.RequestedBlock()
	require.EqualValues(t, spectypes.LATEST_BLOCK, bareBlock, "a bare checkpoint query asks for latest")

	// A variable-pinned read resolves the same as a literal one.
	viaVariable, err := chainParser.ParseMsg("",
		[]byte(`{"query":"query P($n: UInt53!) { checkpoint(sequenceNumber: $n) { digest } }","variables":{"n":314800000}}`),
		graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	variableBlock, _ := viaVariable.RequestedBlock()
	require.EqualValues(t, 314800000, variableBlock, "a variable-pinned checkpoint must parse to its own block")
}
