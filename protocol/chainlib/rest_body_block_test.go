package chainlib

import (
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// trxLikeRestParser is a REST chain parser over a spec shaped like TRX's: a POST collection
// carrying the archive extension (rule 127 blocks), whose block apis name the block in the body.
// The archive policy is on, as it is for a router with a node declaring the archive addon.
func trxLikeRestParser(t *testing.T) ChainParser {
	t.Helper()
	byNum := spectypes.BlockParser{ParserArg: []string{"0", "num"}, ParserFunc: spectypes.PARSER_FUNC_PARSE_CANONICAL}
	head := spectypes.BlockParser{ParserArg: []string{"latest"}, ParserFunc: spectypes.PARSER_FUNC_DEFAULT}
	spec := spectypes.Spec{
		Enabled: true,
		Index:   "TRXLIKE",
		ApiCollections: []*spectypes.ApiCollection{
			{
				Enabled:        true,
				CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceRest, Type: http.MethodPost},
				Extensions:     []*spectypes.Extension{{Name: extensionslib.ArchiveExtension, Rule: &spectypes.Rule{Block: 127}}},
				Apis: []*spectypes.Api{
					{Enabled: true, Name: "/wallet/getblockbynum", ComputeUnits: 10, BlockParsing: byNum},
					{Enabled: true, Name: "/wallet/getnowblock", ComputeUnits: 10, BlockParsing: head},
				},
			},
		},
	}
	parser, err := NewRestChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)
	parser.SetPolicyFromAddonAndExtensionMap(map[string]struct{}{extensionslib.ArchiveExtension: {}})
	return parser
}

// TestRestPostBodyBlockIsJudgedByTheArchiveRule pins MAG-5307: a REST POST request that names its
// block in the body is read for it, so a block older than the spec's archive rule goes to an
// archive node. Before, the body never reached the block parser, every such request parsed as
// NOT_APPLICABLE, and the rule never fired.
func TestRestPostBodyBlockIsJudgedByTheArchiveRule(t *testing.T) {
	parser := trxLikeRestParser(t)
	const head = 1000 // rule 127: a block below 873 is archive

	tests := []struct {
		name        string
		path        string
		body        string
		wantBlock   int64
		wantArchive bool
	}{
		{"the genesis block", "/wallet/getblockbynum", `{"num":0}`, 0, true},
		{"an old block", "/wallet/getblockbynum", `{"num":500}`, 500, true},
		{"a recent block", "/wallet/getblockbynum", `{"num":990}`, 990, false},
		{"no block named", "/wallet/getblockbynum", `{}`, spectypes.NOT_APPLICABLE, false},
		{"the head, by its own api", "/wallet/getnowblock", `{}`, spectypes.LATEST_BLOCK, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chainMessage, err := parser.ParseMsg(tc.path, []byte(tc.body), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: head})
			require.NoError(t, err)
			latest, _ := chainMessage.RequestedBlock()
			require.Equal(t, tc.wantBlock, latest)
			if tc.wantArchive {
				require.Equal(t, []string{extensionslib.ArchiveExtension}, extensionNames(chainMessage))
			} else {
				require.Empty(t, extensionNames(chainMessage))
			}
		})
	}
}
