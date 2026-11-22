package statetracker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// A chain added to lava-specs is classified with its parent's rows from the moment the router
// loads it, with no router release: loading the spec registers the family of every spec in the
// catalog, before the parser reads its own (MAG-5246). NEWEVMCHAIN is not in the router's code
// anywhere; it only imports ETH1, and so does its testnet through it.
func TestStaticSpecLoadGivesANewChainItsParentsFamily(t *testing.T) {
	dir := t.TempDir()
	newChain := `{"proposal": {"specs": [
		{"index": "NEWEVMCHAIN", "name": "new evm chain", "enabled": true, "imports": ["ETH1"],
		 "average_block_time": 2000, "api_collections": []},
		{"index": "NEWEVMCHAINT", "name": "new evm chain testnet", "enabled": true, "imports": ["NEWEVMCHAIN"],
		 "average_block_time": 2000, "api_collections": []}
	]}}`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "newevmchain.json"), []byte(newChain), 0o600))
	for _, chainID := range []string{"NEWEVMCHAIN", "NEWEVMCHAINT"} {
		_, ok := common.GetChainFamily(chainID)
		require.False(t, ok, "%s has a family before its spec loads", chainID)
	}

	chainParser, err := chainlib.NewChainParser(spectypes.APIInterfaceJsonRPC)
	require.NoError(t, err)
	err = RegisterForSpecUpdatesOrSetStaticSpecsWithToken(
		context.Background(),
		chainParser,
		[]string{"../../specs/", dir},
		routersession.RPCEndpoint{ChainID: "NEWEVMCHAINT", ApiInterface: spectypes.APIInterfaceJsonRPC},
		"", "",
	)
	require.NoError(t, err)

	for _, chainID := range []string{"NEWEVMCHAIN", "NEWEVMCHAINT"} {
		family, ok := common.GetChainFamily(chainID)
		require.True(t, ok, "%s has no family after its spec loaded", chainID)
		require.Equal(t, common.ChainFamilyEVM, family, chainID)
	}
	// The rest of the catalog loaded beside it is registered too.
	require.Equal(t, common.ChainFamilyEVM, common.GetChainFamilyOrDefault("ARBITRUMN"), "ARBITRUMN imports ARBITRUM, which imports ETH1")
	require.Equal(t, common.ChainFamilyCosmosSDK, common.GetChainFamilyOrDefault("COSMOSHUBT"), "COSMOSHUBT reaches IBC through COSMOSHUB")
}
