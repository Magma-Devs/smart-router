package lavasession

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-3296, the routing half. Admitting a provider whose add-on REPLACES the base
// api surface is only half the job: base-collection traffic must then stay off it.
// Both eligibility checks used to treat addon "" as universally served, so an
// EVM-only Acala endpoint was eligible for Substrate requests and answered -32601
// — which the client reads as a verdict on its request rather than on routing.

func standaloneEndpoint() *Endpoint {
	return &Endpoint{
		NetworkAddress:   "https://eth-rpc-acala.aca-api.network",
		Enabled:          true,
		Addons:           map[string]struct{}{"evm": {}},
		Extensions:       map[string]struct{}{"evm": {}},
		StandaloneAddons: true,
	}
}

func baseEndpoint() *Endpoint {
	return &Endpoint{
		NetworkAddress: "https://acala-rpc.aca-api.network",
		Enabled:        true,
		Addons:         map[string]struct{}{},
		Extensions:     map[string]struct{}{},
	}
}

func TestEndpointCheckSupportForServices_StandaloneAddons(t *testing.T) {
	standalone := standaloneEndpoint()
	require.True(t, standalone.CheckSupportForServices("evm", nil),
		"it serves the add-on it declares")
	require.False(t, standalone.CheckSupportForServices("", nil),
		"it must not be offered base-collection traffic it cannot answer")

	base := baseEndpoint()
	require.True(t, base.CheckSupportForServices("", nil),
		"an ordinary endpoint still serves the base collection")
	require.False(t, base.CheckSupportForServices("evm", nil))

	// The default is unchanged: an add-on endpoint that has NOT opted out still
	// answers for the base collection, which is right when the add-on extends it.
	inheriting := standaloneEndpoint()
	inheriting.StandaloneAddons = false
	require.True(t, inheriting.CheckSupportForServices("", nil))

	// The flag is inert without add-ons — there would be nothing left to serve.
	empty := &Endpoint{StandaloneAddons: true, Addons: map[string]struct{}{}}
	require.True(t, empty.CheckSupportForServices("", nil))
}

func TestIsSupportingAddon_StandaloneAddons(t *testing.T) {
	t.Run("a provider with only standalone endpoints cannot serve base", func(t *testing.T) {
		cswp := &ConsumerSessionsWithProvider{Endpoints: []*Endpoint{standaloneEndpoint()}}
		require.True(t, cswp.IsSupportingAddon("evm"))
		require.False(t, cswp.IsSupportingAddon(""))
	})

	t.Run("one ordinary endpoint is enough to serve base", func(t *testing.T) {
		cswp := &ConsumerSessionsWithProvider{Endpoints: []*Endpoint{standaloneEndpoint(), baseEndpoint()}}
		require.True(t, cswp.IsSupportingAddon(""))
		require.True(t, cswp.IsSupportingAddon("evm"))
	})

	t.Run("the default is unchanged", func(t *testing.T) {
		e := standaloneEndpoint()
		e.StandaloneAddons = false
		cswp := &ConsumerSessionsWithProvider{Endpoints: []*Endpoint{e}}
		require.True(t, cswp.IsSupportingAddon(""), "every existing deployment keeps serving base")
	})

	// Extensions are a separate axis and must not be affected by the addon opt-out.
	t.Run("extension support is untouched", func(t *testing.T) {
		cswp := &ConsumerSessionsWithProvider{Endpoints: []*Endpoint{standaloneEndpoint()}}
		require.True(t, cswp.IsSupportingExtensions([]string{"evm"}, context.Background()))
	})
}
