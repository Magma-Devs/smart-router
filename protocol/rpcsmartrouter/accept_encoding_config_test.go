package rpcsmartrouter

import (
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// TestAcceptEncodingParsesFromConfig proves the MAG-3844 option survives the real
// config path, viper.UnmarshalKey into []*RPCStaticProviderEndpoint. A tag that did
// not match would decode as "" and leave the url on identity with no error: an
// operator measuring gzip would be measuring nothing.
func TestAcceptEncodingParsesFromConfig(t *testing.T) {
	const config = `
direct-rpc:
  - name: sol-tatum
    chain-id: SOLANA
    api-interface: jsonrpc
    node-urls:
      - url: https://solana-mainnet.gateway.tatum.io
        accept-encoding: gzip
      - url: https://api.mainnet-beta.solana.com
`
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(config)))

	endpoints, err := ParseStaticProviderEndpoints(v, common.DirectRPCConfigName)
	require.NoError(t, err)
	require.Len(t, endpoints, 1)
	require.Len(t, endpoints[0].NodeUrls, 2)

	require.Equal(t, common.AcceptEncodingGzip, endpoints[0].NodeUrls[0].UpstreamAcceptEncoding())
	require.Equal(t, common.AcceptEncodingIdentity, endpoints[0].NodeUrls[1].UpstreamAcceptEncoding(),
		"a url that does not opt in keeps identity")
}

// An accept-encoding the router does not implement stops it at boot, on the
// primary and the backup lists alike, rather than quietly running identity.
func TestAcceptEncodingRejectsUnsupportedValue(t *testing.T) {
	for _, list := range []string{common.DirectRPCConfigName, common.BackupDirectRPCConfigName} {
		config := list + `:
  - name: sol-tatum
    chain-id: SOLANA
    api-interface: jsonrpc
    node-urls:
      - url: https://api.mainnet-beta.solana.com
      - url: https://solana-mainnet.gateway.tatum.io
        accept-encoding: br
`
		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader(config)))

		_, err := ParseStaticProviderEndpoints(v, list)
		require.Error(t, err, list)
		require.Contains(t, err.Error(), `unsupported accept-encoding "br"`, list)
		require.Contains(t, err.Error(), "solana-mainnet.gateway.tatum.io", "the error names the url")
	}
}
