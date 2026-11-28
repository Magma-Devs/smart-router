package rpcsmartrouter

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	speckeeper "github.com/magma-Devs/smart-router/utils/keeper"
)

// A write the lock forced stateful bypasses the cache like any write, whatever `deterministic` says
// (PRD §5.3): the cache reads the Stateful level the lock set and never the deterministic flag, which
// the lock leaves as the spec wrote it.
func TestForcedWriteBypassesTheCacheWhateverDeterministicSays(t *testing.T) {
	catalog := map[string]spectypes.Spec{"EVMLOCK": {
		Index: "EVMLOCK", Name: "evm lock", Enabled: true, AverageBlockTime: 2000,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceJsonRPC, Type: http.MethodPost},
			Apis: []*spectypes.Api{{
				Name: "eth_sendRawTransaction", Enabled: true, ComputeUnits: 10,
				Category:     spectypes.SpecCategory{Deterministic: true, Stateful: 0},
				BlockParsing: spectypes.BlockParser{ParserArg: []string{"latest"}, ParserFunc: spectypes.PARSER_FUNC_DEFAULT},
			}},
		}},
	}}
	expanded, report, err := speckeeper.ExpandSpecWithWriteLock(catalog, "EVMLOCK")
	require.NoError(t, err)
	require.Equal(t, 1, report.Count(spectypes.APIInterfaceJsonRPC), "setup: the write was cleared and the lock forced it")
	chainParser, err := chainlib.NewChainParser(spectypes.APIInterfaceJsonRPC)
	require.NoError(t, err)
	chainParser.SetSpec(*expanded)

	chainMessage, err := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x01"],"id":1}`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, nil, "dapp", "1.2.3.4")
	require.True(t, protocolMessage.GetApi().Category.Deterministic, "the lock leaves deterministic as the spec wrote it")
	require.Equal(t, "stateful", cacheExclusionReason(protocolMessage), "neither cache tier reads or writes a write")
}
