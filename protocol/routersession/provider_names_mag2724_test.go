package routersession

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// MAG-2724: two providers configured under one name collapsed into a single entry — one upstream
// served every request while the other sat idle, and setting that name aside after a failure left
// the router with nothing to retry against.
//
// A provider's name is its routing identity, so the fix is to refuse the configuration rather than
// to invent a second identity for it: the router will not start, and the error tells the operator
// which names to change. These tests cover the check that makes that decision.

func mag2724Endpoint(name, chainID, apiInterface, url string) *RPCStaticProviderEndpoint {
	return &RPCStaticProviderEndpoint{
		Name:         name,
		ChainID:      chainID,
		ApiInterface: apiInterface,
		NodeUrls:     []common.NodeUrl{{Url: url}},
	}
}

func TestValidateUniqueProviderNames_RejectsCollisionWithinChainAndInterface(t *testing.T) {
	err := ValidateUniqueProviderNames([]*RPCStaticProviderEndpoint{
		mag2724Endpoint("SimTwin", "ETH1", "jsonrpc", "http://one:8545"),
		mag2724Endpoint("SimTwin", "ETH1", "jsonrpc", "http://two:8545"),
	})

	require.Error(t, err)
	// The operator has to be able to find the offending pair from the boot log alone — the router
	// is down at this point, so the message is the only thing they have to work from.
	require.Contains(t, err.Error(), "SimTwin")
	require.Contains(t, err.Error(), "ETH1")
	require.Contains(t, err.Error(), "jsonrpc")
	require.Contains(t, err.Error(), "http://one:8545")
	require.Contains(t, err.Error(), "http://two:8545")
}

func TestValidateUniqueProviderNames_ReportsEveryCollisionDeterministically(t *testing.T) {
	// Refusing to boot is worth little if it names one collision at a time and the operator has to
	// redeploy to find the next. It also must not vary run to run — the grouping is a Go map, and
	// map order is exactly the nondeterminism that made the original bug hard to pin down.
	endpoints := []*RPCStaticProviderEndpoint{
		mag2724Endpoint("alpha", "ETH1", "jsonrpc", "http://a1:8545"),
		mag2724Endpoint("alpha", "ETH1", "jsonrpc", "http://a2:8545"),
		mag2724Endpoint("beta", "ETH1", "jsonrpc", "http://b1:8545"),
		mag2724Endpoint("beta", "ETH1", "jsonrpc", "http://b2:8545"),
		mag2724Endpoint("gamma", "POLYGON1", "rest", "http://g1:8545"),
		mag2724Endpoint("gamma", "POLYGON1", "rest", "http://g2:8545"),
	}

	err := ValidateUniqueProviderNames(endpoints)
	require.Error(t, err)
	require.Contains(t, err.Error(), "alpha")
	require.Contains(t, err.Error(), "beta")
	require.Contains(t, err.Error(), "gamma")

	for i := 0; i < 20; i++ {
		require.Equal(t, err.Error(), ValidateUniqueProviderNames(endpoints).Error(),
			"same config must produce the same message on every run")
	}
}

func TestValidateUniqueProviderNames_AllowsRepeatedNameOutsideOneScope(t *testing.T) {
	// One label per upstream operator, reused across chains and interfaces, is the normal way these
	// configs are written. Each combination builds its own session manager with its own pairing map,
	// so nothing collapses — rejecting these would refuse to start working deployments.
	tests := []struct {
		name      string
		endpoints []*RPCStaticProviderEndpoint
	}{
		{
			name: "same name on different chains",
			endpoints: []*RPCStaticProviderEndpoint{
				mag2724Endpoint("alchemy", "ETH1", "jsonrpc", "http://eth:8545"),
				mag2724Endpoint("alchemy", "POLYGON1", "jsonrpc", "http://polygon:8545"),
			},
		},
		{
			name: "same name on different api-interfaces of one chain",
			endpoints: []*RPCStaticProviderEndpoint{
				mag2724Endpoint("alchemy", "ETH1", "jsonrpc", "http://eth:8545"),
				mag2724Endpoint("alchemy", "ETH1", "rest", "http://eth:8546"),
			},
		},
		{
			name: "distinct names in one scope",
			endpoints: []*RPCStaticProviderEndpoint{
				mag2724Endpoint("primary", "ETH1", "jsonrpc", "http://one:8545"),
				mag2724Endpoint("secondary", "ETH1", "jsonrpc", "http://two:8545"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, ValidateUniqueProviderNames(tt.endpoints))
		})
	}
}

func TestValidateUniqueProviderNames_RejectsCollisionAcrossStaticAndBackup(t *testing.T) {
	// Static and backup providers live in separate maps but are looked up across both by address,
	// so a name shared between the two lists is as ambiguous as one shared within either. This is
	// why the boot path passes both lists in one call instead of checking each on its own.
	static := []*RPCStaticProviderEndpoint{mag2724Endpoint("shared", "ETH1", "jsonrpc", "http://primary:8545")}
	backup := []*RPCStaticProviderEndpoint{mag2724Endpoint("shared", "ETH1", "jsonrpc", "http://backup:8545")}

	require.NoError(t, ValidateUniqueProviderNames(static), "each list is unique on its own")
	require.NoError(t, ValidateUniqueProviderNames(backup))

	err := ValidateUniqueProviderNames(static, backup)
	require.Error(t, err, "the collision only exists when the two lists are checked together")
	require.Contains(t, err.Error(), "shared")
}

func TestValidateUniqueProviderNames_ToleratesEmptyAndNilInput(t *testing.T) {
	// Boot calls this with whichever lists the config happened to define; backup-direct-rpc is
	// frequently absent, and an unset key unmarshals to nil.
	require.NoError(t, ValidateUniqueProviderNames())
	require.NoError(t, ValidateUniqueProviderNames(nil, nil))
	require.NoError(t, ValidateUniqueProviderNames([]*RPCStaticProviderEndpoint{nil}))
}
