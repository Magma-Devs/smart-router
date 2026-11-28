package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// A GraphQL error's path starts at the response key, the caller's alias when the field has one. A
// row's path names fields, never aliases (PRD §2.2), so the record reads the path at the field's
// name: `tx: transaction(...)` failing at ["tx", "effects"] is read at ["transaction", "effects"].
// A path at a field asked without an alias, or at no root field, is left as it is.
func TestGraphQLErrorRecordsReadAPathAtTheFieldsName(t *testing.T) {
	ctx := context.Background()
	chainParser, err := chainlib.NewGraphQLChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(spectypes.Spec{
		Index: "SUI", Enabled: true, AverageBlockTime: 400,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGraphQL, Type: http.MethodPost},
			Apis: []*spectypes.Api{
				{Name: "transaction", Enabled: true, ComputeUnits: 10, BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY}},
				{Name: "chainIdentifier", Enabled: true, ComputeUnits: 10, BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY}},
			},
		}},
	})
	query := []byte(`{"query":"{ tx: transaction(digest: \"x\") { digest effects { status } } chainIdentifier }"}`)
	chainMsg, err := chainParser.ParseMsg("", query, http.MethodPost, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "127.0.0.1")
	usedProviders := routersession.NewUsedProviders(nil)
	rp := NewRelayProcessor(ctx, nil, RelayProcessorMetrics, RelayProcessorMetrics, newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, Stateless))
	require.NoError(t, usedProviders.TryLockSelection(ctx))
	usedProviders.AddUsed(routersession.ConsumerSessionsMap{"node@test": &routersession.SessionInfo{}}, nil)

	body := `{"data":null,"errors":[` +
		`{"message":"Could not find the referenced transaction","path":["tx","effects"],"extensions":{"code":"NOT_FOUND"}},` +
		`{"message":"Internal error","path":["chainIdentifier"],"extensions":{"code":"INTERNAL_SERVER_ERROR"}},` +
		`{"message":"Request is throttled","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`
	sendRestReply(rp, "node@test", 0, http.StatusOK, body)
	waitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_ = rp.WaitForResults(waitCtx)

	_, nodeErrors, _ := rp.GetResultsData()
	require.Len(t, nodeErrors, 1)
	records := nodeErrors[0].ErrorRecords
	require.Len(t, records, 3)
	require.Equal(t, []string{"transaction", "effects"}, records[0].Path, "the alias is read as the field's name")
	require.Equal(t, []string{"chainIdentifier"}, records[1].Path, "a field asked without an alias keeps its path")
	require.Nil(t, records[2].Path, "an error with no path has none")
}
