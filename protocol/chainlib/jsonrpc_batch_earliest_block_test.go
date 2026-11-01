package chainlib

import (
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// The ETH1 fixture's archive rule is 127 blocks behind the head: at a head of 1000, block 100 is
// archive-deep and block 950 is not.
const (
	rangeTestHead     = 1000
	rangeTestDeep     = int64(100)
	rangeTestDeepHex  = "0x64"
	rangeTestRecent   = int64(950)
	rangeTestRecentHx = "0x3B6"
)

func extensionNames(chainMessage ChainMessage) []string {
	names := make([]string, 0, len(chainMessage.GetExtensions()))
	for _, extension := range chainMessage.GetExtensions() {
		names = append(names, extension.Name)
	}
	return names
}

func ethChainParserForRangeTests(t *testing.T) ChainParser {
	t.Helper()
	serverHandle := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := CreateChainLibMocks(context.Background(), "ETH1", spectypes.APIInterfaceJsonRPC, serverHandle, nil, "../../", []string{"archive", "trace"})
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	// Extensions are only evaluated once a policy allows them; the trace add-on collection carries
	// its own archive extension, so the add-on is allowed too.
	require.NoError(t, chainParser.SetPolicy(&Policy{ChainPolicies: []ChainPolicy{{
		ChainID:      "ETH1",
		Requirements: []ChainRequirement{{Addons: []string{"trace"}, Extensions: []string{"archive"}}},
	}}}, "ETH1", spectypes.APIInterfaceJsonRPC))
	return chainParser
}

// TestJsonRpcBatchRequestedBlockKeepsTheDeepestElement pins MAG-4242 item 2: a batch's earliest
// requested block is its deepest element's, whatever position that element is in, so the archive
// rule judges the batch on it. Before the fix the first element never set the earliest block, the
// combination with later elements came out at the "unset" 0, and RequestedBlock reported the
// newest block twice: a batch mixing a recent and a deep block stayed off archive.
func TestJsonRpcBatchRequestedBlockKeepsTheDeepestElement(t *testing.T) {
	chainParser := ethChainParserForRangeTests(t)
	recent := `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["` + rangeTestRecentHx + `",false]}`
	deep := `{"jsonrpc":"2.0","id":2,"method":"eth_getBlockByNumber","params":["` + rangeTestDeepHex + `",false]}`
	for name, batch := range map[string]string{
		"recent first": "[" + recent + "," + deep + "]",
		"deep first":   "[" + deep + "," + recent + "]",
	} {
		t.Run(name, func(t *testing.T) {
			chainMessage, err := chainParser.ParseMsg("", []byte(batch), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: rangeTestHead})
			require.NoError(t, err)
			latest, earliest := chainMessage.RequestedBlock()
			require.Equal(t, rangeTestRecent, latest)
			require.Equal(t, rangeTestDeep, earliest, "the earliest block must be the deepest element's")
			require.Equal(t, []string{"archive"}, extensionNames(chainMessage), "the archive rule judges the batch on its deepest element")
		})
	}
}

// A batch element at block 0 asks for the chain's first block. RequestedBlock reads an earliest
// block of 0 as "unset", so the element joins the batch's earliest block as EARLIEST_BLOCK, as a
// range lower bound of 0 does; whatever its position and whatever it is batched with, the batch
// goes to archive. A single request for block 0 is unchanged.
func TestJsonRpcBatchWithAGenesisElementGoesToArchive(t *testing.T) {
	chainParser := ethChainParserForRangeTests(t)
	element := func(id int, block string) string {
		return `{"jsonrpc":"2.0","id":` + strconv.Itoa(id) + `,"method":"eth_getBlockByNumber","params":["` + block + `",false]}`
	}
	blockless := `{"jsonrpc":"2.0","id":3,"method":"net_version","params":[]}`
	for name, batch := range map[string]string{
		"recent then genesis":      "[" + element(1, rangeTestRecentHx) + "," + element(2, "0x0") + "]",
		"genesis then recent":      "[" + element(1, "0x0") + "," + element(2, rangeTestRecentHx) + "]",
		"latest then genesis":      "[" + element(1, "latest") + "," + element(2, "0x0") + "]",
		"genesis then latest":      "[" + element(1, "0x0") + "," + element(2, "latest") + "]",
		"block-less then genesis":  "[" + blockless + "," + element(2, "0x0") + "]",
		"genesis alone in a batch": "[" + element(1, "0x0") + "]",
	} {
		t.Run(name, func(t *testing.T) {
			chainMessage, err := chainParser.ParseMsg("", []byte(batch), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: rangeTestHead})
			require.NoError(t, err)
			_, earliest := chainMessage.RequestedBlock()
			require.Equal(t, spectypes.EARLIEST_BLOCK, earliest, "a genesis element is the batch's earliest block")
			require.Equal(t, []string{"archive"}, extensionNames(chainMessage))
		})
	}

	single, err := chainParser.ParseMsg("", []byte(element(1, "0x0")), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: rangeTestHead})
	require.NoError(t, err)
	latest, earliest := single.RequestedBlock()
	require.Equal(t, int64(0), latest)
	require.Equal(t, int64(0), earliest, "a single request keeps no earliest block of its own")
	require.Equal(t, []string{"archive"}, extensionNames(single))
}
