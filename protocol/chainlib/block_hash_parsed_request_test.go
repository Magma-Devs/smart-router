package chainlib

import (
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// TestBTCGetblockstatsHashArgumentTakesNoCacheLookup pins the one spec shape that makes the
// block-hash rule in docs/RESP-CACHE.md ("Block-hash to height mappings") non-trivial.
//
// The doc says a request whose BLOCK_HASH parser finds a hash parses to no block number, so
// the router serves it without a cache lookup (allowCacheLookup is reqBlock != NOT_APPLICABLE).
// BTC's getblockstats is the only bundled api that lists a BLOCK_HASH parser and a BLOCK_LATEST
// parser on the same argument, and ParseWithGenericParsers takes the first parser that
// succeeds. A hash argument therefore parses as a hash and no block; a height argument fails
// the hash parser (a number is not a string, and a height written as a string is shorter than
// MinimumHashLength) and falls through to the height. Swap the two parser entries in the spec
// and the doc's rule becomes false; this test is what notices.
func TestBTCGetblockstatsHashArgumentTakesNoCacheLookup(t *testing.T) {
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "BTC")
	require.NoError(t, err, "specs/btc.json ships in this repo")

	// The premise: BLOCK_HASH is listed before BLOCK_LATEST on the same path.
	var parsers []spectypes.GenericParser
	for _, collection := range spec.ApiCollections {
		if collection.CollectionData.ApiInterface != spectypes.APIInterfaceJsonRPC {
			continue
		}
		for _, api := range collection.Apis {
			if api.Name == "getblockstats" {
				parsers = api.Parsers
			}
		}
	}
	require.Len(t, parsers, 2, "getblockstats carries exactly two generic parsers")
	require.Equal(t, spectypes.PARSER_TYPE_BLOCK_HASH, parsers[0].ParseType, "BLOCK_HASH must be tried first")
	require.Equal(t, spectypes.PARSER_TYPE_BLOCK_LATEST, parsers[1].ParseType)
	require.Equal(t, parsers[0].ParsePath, parsers[1].ParsePath, "both parsers read the same argument")

	chainParser, err := NewChainParser(spectypes.APIInterfaceJsonRPC)
	require.NoError(t, err)
	chainParser.SetSpec(spec)

	const blockHash = "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72728a054"
	require.GreaterOrEqual(t, len(blockHash), 32, "a real hash is never shorter than MinimumHashLength")

	t.Run("a hash argument parses to no block, so no lookup is possible", func(t *testing.T) {
		request := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getblockstats","params":[%q]}`, blockHash)
		chainMessage, err := chainParser.ParseMsg("", []byte(request), "POST", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		requested, _ := chainMessage.RequestedBlock()
		require.Equal(t, spectypes.NOT_APPLICABLE, requested, "a hash-parsed request has no block number")
		require.Equal(t, []string{blockHash}, chainMessage.GetRequestedBlocksHashes(), "the hash itself is what the parser found")
	})

	t.Run("a height argument falls through to the height, so a lookup is possible", func(t *testing.T) {
		chainMessage, err := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","id":1,"method":"getblockstats","params":[800000]}`), "POST", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		requested, _ := chainMessage.RequestedBlock()
		require.Equal(t, int64(800000), requested)
		require.Empty(t, chainMessage.GetRequestedBlocksHashes())
	})

	t.Run("a height written as a string is too short to be a hash and falls through too", func(t *testing.T) {
		chainMessage, err := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","id":1,"method":"getblockstats","params":["800000"]}`), "POST", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		requested, _ := chainMessage.RequestedBlock()
		require.Equal(t, int64(800000), requested)
		require.Empty(t, chainMessage.GetRequestedBlocksHashes())
	})
}
