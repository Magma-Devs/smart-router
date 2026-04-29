//go:build enterprise

package rpcsmartrouter

import (
	"testing"

	"github.com/Magma-Devs/smart-router/licensing"
	spectypes "github.com/Magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnterpriseConfig_Edition(t *testing.T) {
	lic := &licensing.License{LicenseID: "lic-1", CustomerID: "cust-1"}
	c := enterpriseConfig{license: lic}

	assert.Equal(t, "enterprise", c.Edition())
	assert.Equal(t, lic, c.License())
	assert.True(t, c.SupportsWSSubscriptions())
	assert.True(t, c.SupportsGRPCSubscriptions())
}

func TestEnterpriseConfig_ValidateAPIInterface(t *testing.T) {
	c := enterpriseConfig{}
	allowed := []string{
		spectypes.APIInterfaceJsonRPC,
		spectypes.APIInterfaceRest,
		spectypes.APIInterfaceGrpc,
		spectypes.APIInterfaceTendermintRPC,
	}
	for i, ai := range allowed {
		t.Run(ai, func(t *testing.T) {
			require.NoError(t, c.ValidateAPIInterface(ai), "interface %s, tc #%d, i #%d", ai, i, i)
		})
	}

	t.Run("unknown rejected", func(t *testing.T) {
		err := c.ValidateAPIInterface("sneakyproto")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported api-interface")
	})
}

func TestEnterpriseConfig_ValidateTransport(t *testing.T) {
	c := enterpriseConfig{}
	urls := []string{
		"http://node:8545",
		"https://node:8545",
		"ws://node:8546",
		"wss://node:8546",
		"grpc://node:9090",
		"grpcs://node:9090",
		"node:8545",
		"",
	}
	for i, u := range urls {
		t.Run(u, func(t *testing.T) {
			require.NoError(t, c.ValidateTransport(u), "url %s, tc #%d, i #%d", u, i, i)
		})
	}
}

func TestEnterpriseConfig_ValidateSpec(t *testing.T) {
	c := enterpriseConfig{}
	specs := []string{"ETH1", "LAVA", "COSMOSSDK", "IBC", "TENDERMINT", "GRPCTEST", "anything"}
	for i, idx := range specs {
		t.Run(idx, func(t *testing.T) {
			require.NoError(t, c.ValidateSpec(idx), "spec %s, tc #%d, i #%d", idx, i, i)
		})
	}
}

func TestEnterpriseConfig_FactoriesReturnDirectImpls(t *testing.T) {
	c := enterpriseConfig{}

	wsm, err := c.CreateWSSubscriptionManager(WSSubscriptionManagerOptions{
		ConnectionType: spectypes.APIInterfaceJsonRPC,
		ChainID:        "ETH1",
		APIInterface:   spectypes.APIInterfaceJsonRPC,
	})
	require.NoError(t, err)
	require.NotNil(t, wsm)
	_, isDirect := wsm.(*DirectWSSubscriptionManager)
	assert.True(t, isDirect, "enterprise must return *DirectWSSubscriptionManager, got %T", wsm)

	grpcm, err := c.CreateGRPCSubscriptionManager(GRPCSubscriptionManagerOptions{
		ChainID:      "LAVA",
		APIInterface: spectypes.APIInterfaceGrpc,
	})
	require.NoError(t, err)
	require.NotNil(t, grpcm)
	_, isDirect = grpcm.(*DirectGRPCSubscriptionManager)
	assert.True(t, isDirect, "enterprise must return *DirectGRPCSubscriptionManager, got %T", grpcm)
}

func TestIsEnterpriseBuild_TrueWhenTagged(t *testing.T) {
	// In an enterprise build, enterprise_features.go's init() registers the
	// factory before any test runs, so IsEnterpriseBuild() must report true.
	assert.True(t, IsEnterpriseBuild(),
		"enterprise build must register factory at init; IsEnterpriseBuild()=true")
}

func TestActivateConfig_PromotesUsingRealFactory(t *testing.T) {
	snapshotConfigSingletons(t)

	// Reset to community baseline. The real factory is preserved by the snapshot
	// helper and will be restored at cleanup, so we don't unregister it here.
	configMu.Lock()
	activeConfig = communityConfig{}
	configMu.Unlock()

	lic := &licensing.License{LicenseID: "lic-real", CustomerID: "cust-real"}
	ActivateConfig(lic)

	got := ActiveConfig()
	assert.Equal(t, "enterprise", got.Edition())
	assert.Equal(t, lic, got.License())
}
