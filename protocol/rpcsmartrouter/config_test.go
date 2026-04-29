package rpcsmartrouter

import (
	"strings"
	"testing"

	"github.com/Magma-Devs/smart-router/licensing"
	"github.com/Magma-Devs/smart-router/protocol/chainlib"
	spectypes "github.com/Magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snapshotConfigSingletons captures the current globals and registers a
// t.Cleanup that restores them. Tests that mutate activeConfig or
// enterpriseFactory MUST call this; otherwise test ordering bites because
// RegisterEnterpriseConfig panics on a second registration.
func snapshotConfigSingletons(t *testing.T) {
	t.Helper()
	configMu.Lock()
	prevActive := activeConfig
	prevFactory := enterpriseFactory
	configMu.Unlock()

	t.Cleanup(func() {
		configMu.Lock()
		activeConfig = prevActive
		enterpriseFactory = prevFactory
		configMu.Unlock()
	})
}

func TestCommunityConfig_Edition(t *testing.T) {
	c := communityConfig{}
	assert.Equal(t, "community", c.Edition())
	assert.Nil(t, c.License())
	assert.False(t, c.SupportsWSSubscriptions())
	assert.False(t, c.SupportsGRPCSubscriptions())
}

func TestCommunityConfig_ValidateAPIInterface(t *testing.T) {
	cases := []struct {
		name           string
		apiInterface   string
		wantErr        bool
		wantSubstrings []string
	}{
		{"jsonrpc allowed", spectypes.APIInterfaceJsonRPC, false, nil},
		{
			"rest rejected",
			spectypes.APIInterfaceRest,
			true,
			[]string{"REST interface requires an enterprise license", "github.com/Magma-Devs/smart-router#enterprise"},
		},
		{
			"grpc rejected",
			spectypes.APIInterfaceGrpc,
			true,
			[]string{"gRPC interface requires an enterprise license", "github.com/Magma-Devs/smart-router#enterprise"},
		},
		{
			"tendermintrpc rejected",
			spectypes.APIInterfaceTendermintRPC,
			true,
			[]string{"TendermintRPC interface requires an enterprise license", "github.com/Magma-Devs/smart-router#enterprise"},
		},
		{
			"unknown interface rejected",
			"sneakyproto",
			true,
			[]string{"unsupported api-interface", "sneakyproto"},
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := communityConfig{}.ValidateAPIInterface(tc.apiInterface)
			if tc.wantErr {
				require.Error(t, err, "%s, tc #%d, i #%d", tc.name, i, i)
				for _, sub := range tc.wantSubstrings {
					assert.Contains(t, err.Error(), sub, "%s, tc #%d, i #%d", tc.name, i, i)
				}
			} else {
				assert.NoError(t, err, "%s, tc #%d, i #%d", tc.name, i, i)
			}
		})
	}
}

func TestCommunityConfig_ValidateTransport(t *testing.T) {
	cases := []struct {
		name           string
		rawURL         string
		wantErr        bool
		wantSubstrings []string
	}{
		{"http allowed", "http://node.example.com:8545", false, nil},
		{"https allowed", "https://node.example.com:8545", false, nil},
		{"https with path allowed", "https://eth.publicnode.com/v1/path?key=abc", false, nil},
		{
			"ws rejected",
			"ws://node.example.com:8546",
			true,
			[]string{"WebSocket transport", "requires an enterprise license"},
		},
		{
			"wss rejected",
			"wss://node.example.com:8546",
			true,
			[]string{"WebSocket transport", "requires an enterprise license"},
		},
		{
			"grpc rejected",
			"grpc://node.example.com:9090",
			true,
			[]string{"gRPC transport", "requires an enterprise license"},
		},
		{
			"grpcs rejected",
			"grpcs://node.example.com:9090",
			true,
			[]string{"gRPC transport", "requires an enterprise license"},
		},
		{
			"unknown scheme rejected",
			"sftp://node.example.com",
			true,
			[]string{"unsupported transport scheme"},
		},
		{"bare host treated as schemeless (Sprint 3 catches gRPC by ApiInterface)", "node.example.com:8545", false, nil},
		{"empty rejected", "", true, []string{"empty transport url"}},
		{"whitespace-only rejected", "   \t  ", true, []string{"empty transport url"}},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := communityConfig{}.ValidateTransport(tc.rawURL)
			if tc.wantErr {
				require.Error(t, err, "%s, tc #%d, i #%d", tc.name, i, i)
				for _, sub := range tc.wantSubstrings {
					assert.Contains(t, err.Error(), sub, "%s, tc #%d, i #%d", tc.name, i, i)
				}
				// The empty/whitespace-only error message intentionally omits
				// the URL (there's nothing useful to echo); for every other
				// failing case the offending URL must appear.
				if strings.TrimSpace(tc.rawURL) != "" {
					assert.Contains(t, err.Error(), tc.rawURL, "error must echo offending URL: %s, tc #%d", tc.name, i)
				}
			} else {
				assert.NoError(t, err, "%s, tc #%d, i #%d", tc.name, i, i)
			}
		})
	}
}

func TestCommunityConfig_ValidateSpec(t *testing.T) {
	c := communityConfig{}

	t.Run("ETH1 allowed", func(t *testing.T) {
		require.NoError(t, c.ValidateSpec("ETH1"))
	})

	rejected := []string{"LAVA", "COSMOSSDK", "IBC", "TENDERMINT", "GRPCTEST"}
	for i, idx := range rejected {
		t.Run(idx+" rejected", func(t *testing.T) {
			err := c.ValidateSpec(idx)
			require.Error(t, err, "spec %s, tc #%d, i #%d", idx, i, i)
			assert.Contains(t, err.Error(), "non-EVM spec")
			assert.Contains(t, err.Error(), idx)
			assert.Contains(t, err.Error(), "requires an enterprise license")
		})
	}
}

func TestCommunityConfig_FactoriesReturnNoops(t *testing.T) {
	c := communityConfig{}

	wsm, err := c.CreateWSSubscriptionManager(WSSubscriptionManagerOptions{
		ChainID:      "ETH1",
		APIInterface: spectypes.APIInterfaceJsonRPC,
	})
	require.NoError(t, err)
	require.NotNil(t, wsm)
	_, isNoop := wsm.(*NoOpWSSubscriptionManager)
	assert.True(t, isNoop, "community must return *NoOpWSSubscriptionManager, got %T", wsm)

	grpcm, err := c.CreateGRPCSubscriptionManager(GRPCSubscriptionManagerOptions{
		ChainID:      "LAVA",
		APIInterface: spectypes.APIInterfaceGrpc,
	})
	require.NoError(t, err)
	require.NotNil(t, grpcm)
	_, isNoop = grpcm.(*noopGRPCSubscriptionManager)
	assert.True(t, isNoop, "community must return *noopGRPCSubscriptionManager, got %T", grpcm)
}

func TestNoopGRPCSubscriptionManager_Behavior(t *testing.T) {
	n := newNoopGRPCSubscriptionManager("ETH1", spectypes.APIInterfaceGrpc)

	// IsStreamingMethod must report "not streaming" so that an unguarded
	// call site does not spuriously reject regular gRPC unary calls.
	streaming, methodDesc, err := n.IsStreamingMethod(t.Context(), "/example.Service/Method")
	assert.False(t, streaming, "noop must report not-streaming")
	assert.Nil(t, methodDesc)
	assert.NoError(t, err)

	conn, cleanup, err := n.GetReflectionConnection(t.Context())
	assert.Nil(t, conn)
	assert.Nil(t, cleanup)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no gRPC subscription manager configured")
}

func TestActiveConfigDefaultsToCommunity(t *testing.T) {
	snapshotConfigSingletons(t)

	configMu.Lock()
	activeConfig = communityConfig{}
	configMu.Unlock()

	got := ActiveConfig()
	assert.Equal(t, "community", got.Edition())
}

// TestIsEnterpriseBuild_FalseWhenUntagged is the symmetric counterpart to the
// enterprise-tagged TestIsEnterpriseBuild_TrueWhenTagged. In a community build
// no init() registers a factory, so IsEnterpriseBuild() must report false.
// The snapshot helper guards against test-ordering effects from other tests
// in this file that temporarily install a factory.
func TestIsEnterpriseBuild_FalseWhenUntagged(t *testing.T) {
	snapshotConfigSingletons(t)

	configMu.Lock()
	enterpriseFactory = nil
	configMu.Unlock()

	assert.False(t, IsEnterpriseBuild(),
		"community build must not register an enterprise factory at init")
}

func TestActivateConfig_NilLicense_StaysCommunity(t *testing.T) {
	snapshotConfigSingletons(t)

	// Force a clean baseline.
	configMu.Lock()
	activeConfig = communityConfig{}
	enterpriseFactory = func(*licensing.License) SmartRouterConfig {
		return fakeEnterpriseConfig{}
	}
	configMu.Unlock()

	ActivateConfig(nil)
	assert.Equal(t, "community", ActiveConfig().Edition(),
		"nil license must not promote to enterprise even when factory is registered")
}

func TestActivateConfig_LicenseWithoutFactory_StaysCommunity(t *testing.T) {
	snapshotConfigSingletons(t)

	configMu.Lock()
	activeConfig = communityConfig{}
	enterpriseFactory = nil
	configMu.Unlock()

	ActivateConfig(&licensing.License{LicenseID: "lic-test"})
	assert.Equal(t, "community", ActiveConfig().Edition(),
		"community build (no factory) must stay community even with a license")
}

func TestActivateConfig_FactoryAndLicense_PromotesToEnterprise(t *testing.T) {
	snapshotConfigSingletons(t)

	configMu.Lock()
	activeConfig = communityConfig{}
	enterpriseFactory = func(l *licensing.License) SmartRouterConfig {
		return fakeEnterpriseConfig{license: l}
	}
	configMu.Unlock()

	lic := &licensing.License{LicenseID: "lic-promoted", CustomerID: "cust-1"}
	ActivateConfig(lic)
	got := ActiveConfig()
	assert.Equal(t, "enterprise-fake", got.Edition())
	assert.Equal(t, lic, got.License())
}

func TestRegisterEnterpriseConfig_IgnoresDoubleRegistration(t *testing.T) {
	snapshotConfigSingletons(t)

	configMu.Lock()
	enterpriseFactory = nil
	configMu.Unlock()

	firstFactory := func(l *licensing.License) SmartRouterConfig {
		return fakeEnterpriseConfig{license: l, marker: "first"}
	}
	secondFactory := func(l *licensing.License) SmartRouterConfig {
		return fakeEnterpriseConfig{license: l, marker: "second"}
	}

	// First registration wins.
	RegisterEnterpriseConfig(firstFactory)

	// Second registration must NOT panic and must NOT replace the first factory.
	assert.NotPanics(t, func() { RegisterEnterpriseConfig(secondFactory) },
		"second registration must log + return, not panic")

	// Verify the first factory is still the active one by activating with a
	// license and checking the marker on the resulting config.
	ActivateConfig(&licensing.License{LicenseID: "double-reg-test"})
	got, ok := ActiveConfig().(fakeEnterpriseConfig)
	require.True(t, ok, "ActiveConfig should be fakeEnterpriseConfig after activation, got %T", ActiveConfig())
	assert.Equal(t, "first", got.marker,
		"first registration must win; second registration must be silently ignored")
}

// fakeEnterpriseConfig is a minimal SmartRouterConfig used only in tests, so
// the community test file can exercise ActivateConfig() without depending on
// the enterprise build tag. The Create* methods return nil — these tests
// never invoke them. The marker field lets a test distinguish two factory
// instances (used by TestRegisterEnterpriseConfig_IgnoresDoubleRegistration).
type fakeEnterpriseConfig struct {
	license *licensing.License
	marker  string
}

func (fakeEnterpriseConfig) Edition() string                   { return "enterprise-fake" }
func (f fakeEnterpriseConfig) License() *licensing.License     { return f.license }
func (fakeEnterpriseConfig) ValidateAPIInterface(string) error { return nil }
func (fakeEnterpriseConfig) ValidateTransport(string) error    { return nil }
func (fakeEnterpriseConfig) ValidateSpec(string) error         { return nil }
func (fakeEnterpriseConfig) SupportsWSSubscriptions() bool     { return true }
func (fakeEnterpriseConfig) SupportsGRPCSubscriptions() bool   { return true }

func (fakeEnterpriseConfig) CreateWSSubscriptionManager(WSSubscriptionManagerOptions) (chainlib.WSSubscriptionManager, error) {
	return nil, nil
}

func (fakeEnterpriseConfig) CreateGRPCSubscriptionManager(GRPCSubscriptionManagerOptions) (GRPCSubscriptionManager, error) {
	return nil, nil
}

// Compile-time assertion that the fake fully implements SmartRouterConfig.
var _ SmartRouterConfig = (*fakeEnterpriseConfig)(nil)
