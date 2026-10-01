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

// graphqlTestSpec mirrors the shape a Sui graphql collection will have: one api per root field,
// each carrying its own compute units and category, plus a GET_BLOCKNUM directive on its own
// operation name.
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
						Name:         "latestCheckpoint",
						Enabled:      true,
						ComputeUnits: 10,
						Category:     spectypes.SpecCategory{Deterministic: true},
						BlockParsing: emptyBlockParsing,
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
						FunctionTemplate: `{"query":"{ latestCheckpoint { sequenceNumber } }"}`,
						ApiName:          "latestCheckpoint",
						ResultParsing: spectypes.BlockParser{
							ParserArg:  []string{"0", "data", "latestCheckpoint", "sequenceNumber"},
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

func TestGraphQLChainParserTipObservationIsNotPoisonable(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	// The head poll's own request must resolve to the GET_BLOCKNUM directive, or the per-
	// endpoint ChainTracker cannot read a tip at all.
	pollMessage, err := chainParser.ParseMsg("",
		[]byte(`{"query":"{ latestCheckpoint { sequenceNumber } }"}`),
		graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	require.True(t, IsFunctionTagOfType(pollMessage, spectypes.FUNCTION_TAG_GET_BLOCKNUM))

	// Ordinary user traffic must not. Under a REST-shaped spec every request would carry the
	// api name "/graphql" and so resolve to this directive, and whatever the tip parser scraped
	// out of an arbitrary customer query would be recorded as a current-tip observation.
	userMessage, err := chainParser.ParseMsg("",
		[]byte(`{"query":"{ checkpoint(sequenceNumber: 329083865) { digest } }"}`),
		graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	require.False(t, IsFunctionTagOfType(userMessage, spectypes.FUNCTION_TAG_GET_BLOCKNUM))

	// Nor may a request smuggle the tip operation in alongside another: the combined api name
	// matches no directive, which is the safe direction.
	mixedMessage, err := chainParser.ParseMsg("",
		[]byte(`{"query":"{ latestCheckpoint { sequenceNumber } chainIdentifier }"}`),
		graphqlConnectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	require.False(t, IsFunctionTagOfType(mixedMessage, spectypes.FUNCTION_TAG_GET_BLOCKNUM))
}

func TestGraphQLChainParserCraftMessageFromDirective(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	directive, apiCollection, found := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, found)
	require.Equal(t, spectypes.APIInterfaceGraphQL, apiCollection.CollectionData.ApiInterface)

	crafted, err := chainParser.CraftMessage(directive, graphqlConnectionType, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "latestCheckpoint", crafted.GetApi().Name)
	graphqlMessage, ok := crafted.GetRPCMessage().(*rpcInterfaceMessages.GraphQLMessage)
	require.True(t, ok)
	require.Equal(t, "latestCheckpoint", graphqlMessage.GetMethod())
}

func TestGraphQLChainParserCraftMessageRejectsEmptyTemplate(t *testing.T) {
	chainParser := newGraphQLTestParser(t)

	// GraphQL names its operation in the body and nowhere else, so unlike gRPC an empty
	// template is a spec gap rather than an empty request. Accepting it would poll garbage.
	_, err := chainParser.CraftMessage(
		&spectypes.ParseDirective{FunctionTag: spectypes.FUNCTION_TAG_GET_BLOCKNUM, ApiName: "latestCheckpoint"},
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
	require.Equal(t, "latestCheckpoint", crafted.GetApi().Name)
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
			ApiName:          "latestCheckpoint",
			FunctionTemplate: `{"query":"{ chainIdentifier }"}`,
		},
		graphqlConnectionType,
		&CraftData{
			Path:           "latestCheckpoint",
			Data:           []byte(`{"query":"{ chainIdentifier }"}`),
			ConnectionType: graphqlConnectionType,
		}, nil)
	require.Error(t, err)
}
