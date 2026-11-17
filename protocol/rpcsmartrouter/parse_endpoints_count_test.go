package rpcsmartrouter

import (
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// TestParseEndpoints_SingleVsMultiChainCount is MAG-1881 item 5.5.3: the ticket's audit found no
// Go test distinguishing a single-chain endpoints list from a multi-chain one. Both are
// legitimate config.yml content — one router process can serve several chains — and nothing
// before this pinned that the count and every entry's fields survive the viper unmarshal
// unchanged for either shape.
func TestParseEndpoints_SingleVsMultiChainCount(t *testing.T) {
	singleYAML := `
endpoints:
  - network-address: "0.0.0.0:3360"
    chain-id: ETH1
    api-interface: jsonrpc
`
	multiYAML := singleYAML + `
  - network-address: "0.0.0.0:3361"
    chain-id: COSMOSHUB
    api-interface: tendermintrpc
`

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(singleYAML)))
	single, err := ParseEndpoints(v)
	require.NoError(t, err)
	require.Len(t, single, 1, "a single-chain config must parse to exactly one endpoint")
	require.Equal(t, "ETH1", single[0].ChainID)
	require.Equal(t, common.DEFAULT_HEALTH_PATH, single[0].HealthCheckPath, "the default health path is backfilled")

	v2 := viper.New()
	v2.SetConfigType("yaml")
	require.NoError(t, v2.ReadConfig(strings.NewReader(multiYAML)))
	multi, err := ParseEndpoints(v2)
	require.NoError(t, err)
	require.Len(t, multi, 2, "a multi-chain config must parse every entry, not just the first")
	require.Equal(t, "ETH1", multi[0].ChainID)
	require.Equal(t, "jsonrpc", multi[0].ApiInterface)
	require.Equal(t, "COSMOSHUB", multi[1].ChainID)
	require.Equal(t, "tendermintrpc", multi[1].ApiInterface)
	require.Equal(t, common.DEFAULT_HEALTH_PATH, multi[1].HealthCheckPath)
}
