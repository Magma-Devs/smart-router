package chainlib

import (
	"testing"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// TestApiNameDefined pins the lookup the cross-validation method guard relies on (MAG-3604): the name a
// request resolves to, exactly, in any enabled collection of the interface, after the spec's imports.
func TestApiNameDefined(t *testing.T) {
	load := func(t *testing.T, chainID, apiInterface string) interface{ ApiNameDefined(string) bool } {
		t.Helper()
		spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, chainID)
		require.NoError(t, err)
		parser, err := NewChainParser(apiInterface)
		require.NoError(t, err)
		parser.SetSpec(spec)
		definer, ok := parser.(interface{ ApiNameDefined(string) bool })
		require.True(t, ok, "every chain parser answers ApiNameDefined through BaseChainParser")
		return definer
	}

	eth := load(t, "ETH1", spectypes.APIInterfaceJsonRPC)
	require.True(t, eth.ApiNameDefined("eth_getBalance"))
	require.False(t, eth.ApiNameDefined("eth_getbalance"), "names match exactly, as the policy lookup does")
	require.True(t, eth.ApiNameDefined("debug_traceBlockByNumber"), "a method of an add-on collection counts")

	rest := load(t, "COSMOSHUB", spectypes.APIInterfaceRest)
	require.True(t, rest.ApiNameDefined("/cosmos/bank/v1beta1/balances/{address}"),
		"a REST api is named by its path template, inherited here from COSMOSSDK")
	require.False(t, rest.ApiNameDefined("cosmos.bank.v1beta1.Query/Balance"), "the gRPC name is not a REST api")
	require.False(t, rest.ApiNameDefined("/cosmos/bank/v1beta1/balances/{address}/{denom}"),
		"COSMOSSDK50 disables this one, and a disabled api is never served")

	grpc := load(t, "COSMOSHUB", spectypes.APIInterfaceGrpc)
	require.True(t, grpc.ApiNameDefined("cosmos.bank.v1beta1.Query/Balance"))
}
