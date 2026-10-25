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

// TestApiNamesLike pins the hint the cross-validation method guard gives when a policy names a method the
// spec serves under no such name (MAG-3604): the spec names that differ from the policy's only by letter
// case or by a REST placeholder's spelling — the name a request actually resolves to.
func TestApiNamesLike(t *testing.T) {
	load := func(t *testing.T, chainID, apiInterface string) interface {
		ApiNamesLike(string) []string
	} {
		t.Helper()
		spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, chainID)
		require.NoError(t, err)
		parser, err := NewChainParser(apiInterface)
		require.NoError(t, err)
		parser.SetSpec(spec)
		definer, ok := parser.(interface{ ApiNamesLike(string) []string })
		require.True(t, ok, "every chain parser answers ApiNamesLike through BaseChainParser")
		return definer
	}

	eth := load(t, "ETH1", spectypes.APIInterfaceJsonRPC)
	require.Equal(t, []string{"eth_getBalance"}, eth.ApiNamesLike("eth_getbalance"), "the case-correct spec name a request resolves to")
	require.Empty(t, eth.ApiNamesLike("eth_getBalance"), "an exact name is not a lookalike of itself")
	require.Empty(t, eth.ApiNamesLike("eth_thisIsNotAMethod"), "nothing shaped like a nonexistent method")

	// REST placeholder names differ only by the spelling inside {…}. The two bech32 templates collide to
	// one regex, so serverApis keeps exactly one of them (which one is not deterministic); a policy that
	// wrote any placeholder spelling is pointed at the name a request actually resolves to. Query a third
	// spelling so the survivor is returned whichever twin it is.
	rest := load(t, "COSMOSHUB", spectypes.APIInterfaceRest)
	like := rest.ApiNamesLike("/cosmos/auth/v1beta1/bech32/{addr}")
	require.Len(t, like, 1, "the colliding twins share one slot, so exactly one name survives")
	require.Contains(t, []string{
		"/cosmos/auth/v1beta1/bech32/{address_bytes}",
		"/cosmos/auth/v1beta1/bech32/{address_string}",
	}, like[0], "the surviving twin, the name a request resolves to")
}
