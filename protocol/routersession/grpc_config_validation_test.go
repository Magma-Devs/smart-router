package routersession

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

func grpcProvider(chainID, name string, configs ...common.GrpcConfig) *RPCStaticProviderEndpoint {
	provider := &RPCStaticProviderEndpoint{}
	provider.ChainID, provider.ApiInterface, provider.Name = chainID, spectypes.APIInterfaceGrpc, name
	for i, config := range configs {
		provider.NodeUrls = append(provider.NodeUrls, common.NodeUrl{Url: "grpcs://" + name + "-" + string(rune('a'+i)) + ".example:443", GrpcConfig: config})
	}
	return provider
}

// Boot refuses a gRPC grpc-config the router cannot run as written (MAG-5134): an invalid one on
// its own, or a chain whose node urls declare different descriptor sources, across providers and
// tiers. Every problem is named at once.
func TestValidateGRPCConfigs(t *testing.T) {
	reflection := common.GrpcConfig{}
	file := common.GrpcConfig{DescriptorSource: common.GrpcDescriptorSourceFile, DescriptorSetPath: "/etc/cosmos.pb"}

	require.NoError(t, ValidateGRPCConfigs(
		[]*RPCStaticProviderEndpoint{grpcProvider("COSMOSHUB", "p1", reflection, common.GrpcConfig{DescriptorSource: common.GrpcDescriptorSourceReflection})},
		[]*RPCStaticProviderEndpoint{grpcProvider("COSMOSHUB", "backup", reflection)},
	), "reflection, spelled out or by default, is one source")
	require.NoError(t, ValidateGRPCConfigs(
		[]*RPCStaticProviderEndpoint{grpcProvider("COSMOSHUB", "p1", file), grpcProvider("SUI", "p2", reflection)},
	), "different chains may use different sources")
	require.NoError(t, ValidateGRPCConfigs([]*RPCStaticProviderEndpoint{{
		ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC, Name: "evm",
		NodeUrls: []common.NodeUrl{{Url: "https://eth.example", GrpcConfig: common.GrpcConfig{DescriptorSource: "bogus"}}},
	}}), "only gRPC node urls are checked")

	err := ValidateGRPCConfigs(
		[]*RPCStaticProviderEndpoint{grpcProvider("COSMOSHUB", "p1", reflection)},
		[]*RPCStaticProviderEndpoint{grpcProvider("COSMOSHUB", "backup", file)},
	)
	require.ErrorContains(t, err, "chain COSMOSHUB mixes descriptor-source", "a static and a backup provider disagree")
	require.ErrorContains(t, err, "file (/etc/cosmos.pb)")

	err = ValidateGRPCConfigs([]*RPCStaticProviderEndpoint{grpcProvider("COSMOSHUB", "p1",
		common.GrpcConfig{DescriptorSource: common.GrpcDescriptorSourceHybrid},
		common.GrpcConfig{DescriptorSource: "reflect"},
		common.GrpcConfig{ReflectionTimeout: time.Minute},
	)})
	require.ErrorContains(t, err, "descriptor-set-path is required", "hybrid with no path no longer runs as reflection-only silently")
	require.ErrorContains(t, err, "invalid gRPC descriptor source")
	require.ErrorContains(t, err, "reflection-timeout too long")
}
