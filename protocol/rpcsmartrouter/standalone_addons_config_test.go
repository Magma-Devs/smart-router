package rpcsmartrouter

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// TestStandaloneAddonsParsesFromConfig proves the MAG-3296 flag survives the real
// config path — viper.UnmarshalKey into []*RPCStaticProviderEndpoint, the same
// call the router boots through. A field whose mapstructure tag did not match
// would decode as false and drop the provider with no error anywhere, so this is
// the assertion that makes the flag usable rather than merely present.
func TestStandaloneAddonsParsesFromConfig(t *testing.T) {
	const config = `
endpoints:
  - chain-id: ACA
    api-interface: jsonrpc
    name: acala-evm
    network-address:
      address: 127.0.0.1:2201
    node-urls:
      - url: https://eth-rpc-acala.aca-api.network
        addons: ["evm"]
        standalone-addons: true
        skip-verifications: ["pruning"]
      - url: https://substrate.example.com
`

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(config)))

	endpoints, err := ParseStaticProviderEndpoints(v, "endpoints")
	require.NoError(t, err)
	require.Len(t, endpoints, 1)
	require.Len(t, endpoints[0].NodeUrls, 2)

	evmUrl := endpoints[0].NodeUrls[0]
	require.True(t, evmUrl.StandaloneAddons, "standalone-addons must survive config load")
	require.Equal(t, []string{"evm"}, evmUrl.Addons)
	require.False(t, evmUrl.ServesBaseCollection(),
		"an EVM-only Acala url must not answer for the Substrate base collection")
	// The neighbouring field is asserted too, so a tag-name regression on either
	// one shows up here rather than as a provider that silently fails admission.
	require.True(t, evmUrl.ShouldSkipVerification("pruning"))

	plainUrl := endpoints[0].NodeUrls[1]
	require.False(t, plainUrl.StandaloneAddons, "the default is unchanged")
	require.True(t, plainUrl.ServesBaseCollection())
}
