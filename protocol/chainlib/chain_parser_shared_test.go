package chainlib

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/parser"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// chainParserCase is one api interface's parser, as the behaviour every parser shares sees
// it. Each interface keeps its own getSupportedApi signature (JSON-RPC and GraphQL take an
// internal path, REST matches by path template), so the rows close over it.
type chainParserCase struct {
	name string
	// newParser builds the parser the way the router does.
	newParser func() (ChainParser, error)
	// nilParser is a typed nil of the parser, for the nil-receiver guards.
	nilParser ChainParser
	// serverApis keys the given apis the way this parser looks them up.
	serverApis func(t *testing.T, apis ...*spectypes.Api) map[ApiKey]ApiContainer
	// getSupportedApi resolves name on a parser holding serverApis; nil serverApis means a
	// nil parser.
	getSupportedApi func(serverApis map[ApiKey]ApiContainer, name string) (*ApiContainer, error)
}

func plainApiContainers(_ *testing.T, apis ...*spectypes.Api) map[ApiKey]ApiContainer {
	serverApis := map[ApiKey]ApiContainer{}
	for _, api := range apis {
		serverApis[ApiKey{Name: api.Name, ConnectionType: connectionType_test}] = ApiContainer{
			api:           api,
			collectionKey: CollectionKey{ConnectionType: connectionType_test},
		}
	}
	return serverApis
}

func chainParserCases() []chainParserCase {
	return []chainParserCase{
		{
			name:       "grpc",
			newParser:  func() (ChainParser, error) { return NewGrpcChainParser() },
			nilParser:  (*GrpcChainParser)(nil),
			serverApis: plainApiContainers,
			getSupportedApi: func(serverApis map[ApiKey]ApiContainer, name string) (*ApiContainer, error) {
				var apip *GrpcChainParser
				if serverApis != nil {
					apip = &GrpcChainParser{BaseChainParser: BaseChainParser{serverApis: serverApis}}
				}
				return apip.getSupportedApi(name, connectionType_test)
			},
		},
		{
			name:       "jsonrpc",
			newParser:  func() (ChainParser, error) { return NewJrpcChainParser() },
			nilParser:  (*JsonRPCChainParser)(nil),
			serverApis: plainApiContainers,
			getSupportedApi: func(serverApis map[ApiKey]ApiContainer, name string) (*ApiContainer, error) {
				var apip *JsonRPCChainParser
				if serverApis != nil {
					apip = &JsonRPCChainParser{BaseChainParser: BaseChainParser{serverApis: serverApis}}
				}
				return apip.getSupportedApi(name, connectionType_test, "")
			},
		},
		{
			name:      "rest",
			newParser: func() (ChainParser, error) { return NewRestChainParser() },
			nilParser: (*RestChainParser)(nil),
			serverApis: func(t *testing.T, apis ...*spectypes.Api) map[ApiKey]ApiContainer {
				return restApiContainers(t, connectionType_test, apis...)
			},
			getSupportedApi: func(serverApis map[ApiKey]ApiContainer, name string) (*ApiContainer, error) {
				var apip *RestChainParser
				if serverApis != nil {
					apip = &RestChainParser{BaseChainParser: BaseChainParser{serverApis: serverApis}}
				}
				return apip.getSupportedApi(name, connectionType_test)
			},
		},
		{
			name:       "tendermintrpc",
			newParser:  func() (ChainParser, error) { return NewTendermintRpcChainParser() },
			nilParser:  (*TendermintChainParser)(nil),
			serverApis: plainApiContainers,
			getSupportedApi: func(serverApis map[ApiKey]ApiContainer, name string) (*ApiContainer, error) {
				var apip *TendermintChainParser
				if serverApis != nil {
					apip = &TendermintChainParser{BaseChainParser: BaseChainParser{serverApis: serverApis}}
				}
				return apip.getSupportedApi(name, connectionType_test)
			},
		},
		{
			name:       "graphql",
			newParser:  func() (ChainParser, error) { return NewGraphQLChainParser() },
			nilParser:  (*GraphQLChainParser)(nil),
			serverApis: plainApiContainers,
			getSupportedApi: func(serverApis map[ApiKey]ApiContainer, name string) (*ApiContainer, error) {
				var apip *GraphQLChainParser
				if serverApis != nil {
					apip = &GraphQLChainParser{BaseChainParser: BaseChainParser{serverApis: serverApis}}
				}
				return apip.getSupportedApi(name, connectionType_test, "")
			},
		},
	}
}

func TestChainParser_ChainBlockStatsFollowSpec(t *testing.T) {
	for _, tc := range chainParserCases() {
		t.Run(tc.name, func(t *testing.T) {
			apip, err := tc.newParser()
			require.NoError(t, err)

			apip.SetSpec(spectypes.Spec{
				Enabled:                       true,
				AllowedBlockLagForQosSync:     11,
				AverageBlockTime:              12000,
				BlockDistanceForFinalizedData: 13,
				BlocksInFinalizationProof:     14,
			})

			allowedBlockLag, averageBlockTime, blockDistanceForFinalizedData, blocksInFinalizationProof := apip.ChainBlockStats()
			require.EqualValues(t, 11, allowedBlockLag)
			require.Equal(t, 12*time.Second, averageBlockTime)
			require.EqualValues(t, 13, blockDistanceForFinalizedData)
			require.EqualValues(t, 14, blocksInFinalizationProof)
		})
	}
}

func TestChainParser_NilGuard(t *testing.T) {
	for _, tc := range chainParserCases() {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				tc.nilParser.SetSpec(spectypes.Spec{})
				tc.nilParser.ChainBlockStats()
				_, err := tc.getSupportedApi(nil, "")
				require.Error(t, err)
				_, err = tc.nilParser.ParseMsg("", []byte{}, "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
				require.Error(t, err)
			})
		})
	}
}

// TestChainParser_GetSupportedApi pins what every parser does with a declared, an undeclared
// and a disabled api. An undeclared api is served from a default container rather than
// refused: the per-interface copies this replaces each had an `if err == nil … else
// ErrorIs(APINotSupportedError)` branch for it, which could never fail and pinned nothing.
func TestChainParser_GetSupportedApi(t *testing.T) {
	for _, tc := range chainParserCases() {
		t.Run(tc.name, func(t *testing.T) {
			enabled := tc.serverApis(t, &spectypes.Api{Name: "API1", Enabled: true})

			apiCont, err := tc.getSupportedApi(enabled, "API1")
			require.NoError(t, err)
			require.Equal(t, "API1", apiCont.api.Name)

			apiCont, err = tc.getSupportedApi(enabled, "API2")
			require.NoError(t, err, "an undeclared api is served from the default container")
			require.Equal(t, DefaultApiName+"API2", apiCont.api.Name)
			require.True(t, apiCont.api.Enabled)

			disabled := tc.serverApis(t, &spectypes.Api{Name: "API1", Enabled: false})
			_, err = tc.getSupportedApi(disabled, "API1")
			require.ErrorContains(t, err, "api is disabled")
		})
	}
}

// requireLatestInMetadataSetsTheRequestedBlock pins the spec's SET_LATEST_IN_METADATA header on one
// interface: a request that carries it asks for the block it names, one without asks for the
// latest, and the block the node answers with is parsed from the reply. newNode builds the node,
// which answers block 244590 when the request carries the header *headerName names, and 244591
// when it does not.
func requireLatestInMetadataSetsTheRequestedBlock(t *testing.T, chainID, apiInterface string, newNode func(headerName *string) http.HandlerFunc) {
	t.Helper()
	ctx := context.Background()
	headerName := ""
	chainParser, chainRouter, _, closeServer, _, err := CreateChainLibMocks(ctx, chainID, apiInterface, newNode(&headerName), nil, "../../", nil)
	require.NoError(t, err)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	parsingForCrafting, apiCollection, ok := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, ok)
	collectionData := apiCollection.CollectionData
	headerParsingDirective, _, ok := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_SET_LATEST_IN_METADATA)
	require.True(t, ok)
	headerName = headerParsingDirective.GetApiName() // the node answers the header's block from now on
	block := 244590
	metadata := []pairingtypes.Metadata{{Name: headerParsingDirective.GetApiName(), Value: fmt.Sprintf(headerParsingDirective.FunctionTemplate, block)}}

	for _, test := range []struct {
		desc           string
		metadata       []pairingtypes.Metadata
		block          int64
		requestedBlock int64
	}{
		{desc: "no metadata", metadata: []pairingtypes.Metadata{}, block: 244591, requestedBlock: spectypes.LATEST_BLOCK},
		{desc: "with-metadata", metadata: metadata, block: 244590, requestedBlock: 244590},
	} {
		t.Run(test.desc, func(t *testing.T) {
			chainMessage, err := chainParser.ParseMsg(parsingForCrafting.ApiName, []byte{}, collectionData.Type, test.metadata, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			requestedBlock, _ := chainMessage.RequestedBlock()
			require.Equal(t, test.requestedBlock, requestedBlock)
			reply, _, _, err := chainRouter.SendNodeMsg(ctx, chainMessage, nil)
			require.NoError(t, err)
			parserInput, err := FormatResponseForParsing(reply.RelayReply, chainMessage)
			require.NoError(t, err)
			parsedInput := parser.ParseBlockFromReply(parserInput, parsingForCrafting.ResultParsing, nil)
			require.Equal(t, test.block, parsedInput.GetBlock())
		})
	}
}
