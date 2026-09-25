package rpcsmartrouter

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// TestMultichainCrossValidationExample_EveryPolicyApplies runs the shipped cross-validating example through
// the startup checks a policy has to pass to ever apply: the preflight (it names a configured endpoint) and,
// per endpoint, the startup guard against that endpoint's spec (it names a method the spec serves). The
// example's COSMOSHUB REST policy named the gRPC method until MAG-3604, so it never applied to a request.
func TestMultichainCrossValidationExample_EveryPolicyApplies(t *testing.T) {
	v := viper.New()
	v.SetConfigFile("../../config/smartrouter_examples/smartrouter_multichain_cross_validation.yml")
	require.NoError(t, v.ReadInConfig())
	endpoints, err := ParseEndpoints(v)
	require.NoError(t, err)
	require.NoError(t, PreflightValidateCrossValidationConfig(v, endpoints))

	cfg, err := ParseCrossValidationConfig(v)
	require.NoError(t, err)
	resolver, err := NewCrossValidationPolicyResolver(cfg)
	require.NoError(t, err)
	checked := 0
	for _, endpoint := range endpoints {
		methods := resolver.PolicyMethods(endpoint.ChainID, endpoint.ApiInterface)
		if len(methods) == 0 {
			continue
		}
		spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, endpoint.ChainID)
		require.NoError(t, err)
		parser, err := chainlib.NewChainParser(endpoint.ApiInterface)
		require.NoError(t, err)
		parser.SetSpec(spec)
		// 0 configured groups skips the capacity bounds, which need the provider list; the stateful and
		// method guards are what decide whether a policy can apply at all.
		require.NoError(t, validateCrossValidationStartup(resolver, parser, endpoint.ChainID, endpoint.ApiInterface, 0, nil),
			"%s/%s: %v", endpoint.ChainID, endpoint.ApiInterface, methods)
		checked += len(methods)
	}
	require.Equal(t, len(cfg.Policies), checked, "every policy in the example belongs to an endpoint that was checked")
}

// TestMethodGuard_NamesAGrpcNamedPolicyByPosition pins that the method guard's error identifies the policy
// even when the log redactor hides part of its method: a gRPC name looks like a url to it
// ("cosmos.bank.v1beta1.Query/[redacted]"), and a gRPC name on a REST endpoint is the case the guard exists
// for, the one the shipped example had.
func TestMethodGuard_NamesAGrpcNamedPolicyByPosition(t *testing.T) {
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "COSMOSHUB")
	require.NoError(t, err)
	parser, err := chainlib.NewChainParser("rest")
	require.NoError(t, err)
	parser.SetSpec(spec)
	resolver, err := NewCrossValidationPolicyResolver(CrossValidationConfig{Policies: []CrossValidationPolicyEntry{
		{ChainID: "COSMOSHUB", ApiInterface: "rest", Method: "/cosmos/bank/v1beta1/balances/{address}", CrossValidationPolicy: CrossValidationPolicy{Enabled: true}},
		{ChainID: "COSMOSHUB", ApiInterface: "rest", Method: "cosmos.bank.v1beta1.Query/Balance", CrossValidationPolicy: CrossValidationPolicy{Enabled: true}},
	}})
	require.NoError(t, err)
	err = validateCrossValidationStartup(resolver, parser, "COSMOSHUB", "rest", 0, nil)
	require.ErrorContains(t, err, "does not serve")
	require.ErrorContains(t, err, "policy #1 cosmos.bank.v1beta1.Query/", "the position survives the redactor")
	require.NotContains(t, err.Error(), "policy #0", "the path-template policy is served")
}
