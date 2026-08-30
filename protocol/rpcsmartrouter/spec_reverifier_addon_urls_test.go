package rpcsmartrouter

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// TestVerificationEndpointsDoNotValidateStrippedCopies is the MAG-3296 half that
// lives in the re-verifier: an add-on node url is duplicated with its add-ons
// stripped so the chain router has both routes, and that copy used to be
// validated too. For a disjoint add-on surface — Acala's EVM against a Substrate
// base — the copy asks the node to be something it never claimed to be, and
// Validate returns on the first failing url, so the copy failed the provider.
func TestVerificationEndpointsDoNotValidateStrippedCopies(t *testing.T) {
	provider := &lavasession.RPCStaticProviderEndpoint{
		NetworkAddress: lavasession.NetworkAddressData{Address: "acala-evm:443"},
		ChainID:        "ACA",
		ApiInterface:   "jsonrpc",
		NodeUrls: []common.NodeUrl{
			{Url: "https://eth-rpc-acala.aca-api.network", Addons: []string{"evm"}},
			{Url: "https://plain.example.com"},
		},
	}

	routerEndpoint, fetcherEndpoint := verificationEndpoints(provider)

	// The router still gets both routes for the add-on url — that expansion is
	// what chain_router.go needs and is not what this fix changes.
	require.Len(t, routerEndpoint.NodeUrls, 3)
	require.Equal(t, []string{"evm"}, routerEndpoint.NodeUrls[0].Addons)
	require.Empty(t, routerEndpoint.NodeUrls[1].Addons,
		"the stripped copy is still routed")
	require.Equal(t, "https://eth-rpc-acala.aca-api.network", routerEndpoint.NodeUrls[1].Url)

	// The fetcher validates only what the provider declared.
	require.Len(t, fetcherEndpoint.NodeUrls, 2)
	require.Equal(t, provider.NodeUrls, fetcherEndpoint.NodeUrls)
	for _, url := range fetcherEndpoint.NodeUrls {
		if url.Url == "https://eth-rpc-acala.aca-api.network" {
			require.Equal(t, []string{"evm"}, url.Addons,
				"the declared url keeps its add-ons, so its collections resolve")
		}
	}

	// Identity is preserved on both, since the two differ only in node urls.
	for _, endpoint := range []*lavasession.RPCProviderEndpoint{routerEndpoint, fetcherEndpoint} {
		require.Equal(t, provider.NetworkAddress, endpoint.NetworkAddress)
		require.Equal(t, provider.ChainID, endpoint.ChainID)
		require.Equal(t, provider.ApiInterface, endpoint.ApiInterface)
	}
}

// TestVerificationEndpointsWithoutAddonsAreIdentical pins that a provider with no
// add-ons is unaffected — nothing is expanded, so nothing is dropped either.
func TestVerificationEndpointsWithoutAddonsAreIdentical(t *testing.T) {
	provider := &lavasession.RPCStaticProviderEndpoint{
		ChainID:      "ETH1",
		ApiInterface: "jsonrpc",
		NodeUrls: []common.NodeUrl{
			{Url: "https://a.example.com"},
			{Url: "https://b.example.com"},
		},
	}

	routerEndpoint, fetcherEndpoint := verificationEndpoints(provider)
	require.Equal(t, provider.NodeUrls, routerEndpoint.NodeUrls)
	require.Equal(t, provider.NodeUrls, fetcherEndpoint.NodeUrls)
}
