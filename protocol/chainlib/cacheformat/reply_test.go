package cacheformat

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-3597: the output formatter reshapes only what is a reply. Handed anything else it
// returns the bytes untouched, so a caller of the formatter can tell a reply from a
// non-reply and no reply is ever manufactured from a web page or a truncated envelope.
func TestOutputFormatterLeavesANonReplyAlone(t *testing.T) {
	cases := []struct {
		name string
		data string
	}{
		{"a web page", `<html>502 Bad Gateway</html>`},
		{"a truncated envelope", `{"jsonrpc":"2.0","resu`},
		{"no bytes at all", ``},
		{"a bare value", `"0x64"`},
		{"an array of values", `[1,2]`},
		{"an empty array", `[]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.False(t, IsRestorableJSONRPCReply([]byte(tc.data)))
			inputFormatter, outputFormatter := FormatterForRelayRequestAndResponseJsonRPC()
			inputFormatter([]byte(`{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}`))
			require.Equal(t, tc.data, string(outputFormatter([]byte(tc.data))), "not a reply, so nothing to restore into and nothing manufactured")
		})
	}
	require.True(t, IsRestorableJSONRPCReply([]byte(` {"jsonrpc":"2.0","id":1,"result":"0x64"}`)))
	require.True(t, IsRestorableJSONRPCReply([]byte(`[{"jsonrpc":"2.0","id":1,"result":"0x64"}]`)))
}
