package chainlib

import (
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestJsonRpcRangeIsJudgedOnItsLowerBound pins MAG-4242 item 12 through the real spec: trace_filter
// declares a PARSER_TYPE_BLOCK_EARLIEST parser on fromBlock, which had no implementation, so a
// range reaching deep into history was judged on its toBlock alone and never routed to archive.
func TestJsonRpcRangeIsJudgedOnItsLowerBound(t *testing.T) {
	chainParser := ethChainParserForRangeTests(t)
	traceFilter := func(filter string) []byte {
		return []byte(`{"jsonrpc":"2.0","id":1,"method":"trace_filter","params":[` + filter + `]}`)
	}
	tests := []struct {
		name         string
		filter       string
		wantEarliest int64
		wantArchive  bool
	}{
		{"deep fromBlock", `{"fromBlock":"` + rangeTestDeepHex + `","toBlock":"latest"}`, rangeTestDeep, true},
		{"from genesis", `{"fromBlock":"0x0","toBlock":"latest"}`, spectypes.EARLIEST_BLOCK, true},
		{"from the earliest tag", `{"fromBlock":"earliest","toBlock":"latest"}`, spectypes.EARLIEST_BLOCK, true},
		{"recent fromBlock", `{"fromBlock":"` + rangeTestRecentHx + `","toBlock":"latest"}`, rangeTestRecent, false},
		{"no fromBlock", `{"toBlock":"latest"}`, spectypes.LATEST_BLOCK, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chainMessage, err := chainParser.ParseMsg("", traceFilter(tc.filter), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: rangeTestHead})
			require.NoError(t, err)
			latest, earliest := chainMessage.RequestedBlock()
			require.Equal(t, spectypes.LATEST_BLOCK, latest, "toBlock stays the block the request is about")
			require.Equal(t, tc.wantEarliest, earliest)
			if tc.wantArchive {
				require.Equal(t, []string{"archive"}, extensionNames(chainMessage))
			} else {
				require.Empty(t, extensionNames(chainMessage))
			}
		})
	}
}
