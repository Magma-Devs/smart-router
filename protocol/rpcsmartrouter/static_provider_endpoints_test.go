package rpcsmartrouter

import (
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseStaticProviderEndpoints_BackupProvidersYAMLLoading backfills
// MAG-1872 item 1: --backup-providers enabled vs disabled through the
// YAML-config-loading path. The previous coverage exercised backup-provider
// selection via the direct API (consumer_session_manager_test.go), but did
// not validate that the YAML key-presence and absent-key branches both work
// — the path actually used by the cobra command at
// rpcsmartrouter.go:1683-1701.
func TestParseStaticProviderEndpoints_BackupProvidersYAMLLoading(t *testing.T) {
	// Geolocation is supplied via the CLI --geolocation flag at runtime; the
	// parser stamps it onto every parsed endpoint regardless of whether the
	// YAML specifies one.
	const flagGeolocation uint64 = 7

	cases := []struct {
		name          string
		yamlBody      string
		expectIsSet   bool
		wantEndpoints int
	}{
		{
			name:          "disabled_absent_key",
			yamlBody:      "endpoints:\n  - chain-id: ETH1\n",
			expectIsSet:   false,
			wantEndpoints: 0,
		},
		{
			name: "enabled_single_backup",
			yamlBody: "backup-providers:\n" +
				"  - name: backup-1\n" +
				"    chain-id: ETH1\n" +
				"    api-interface: jsonrpc\n" +
				"    node-urls:\n" +
				"      - url: https://backup1.example.com\n",
			expectIsSet:   true,
			wantEndpoints: 1,
		},
		{
			name: "enabled_multiple_backups",
			yamlBody: "backup-providers:\n" +
				"  - name: backup-1\n" +
				"    chain-id: ETH1\n" +
				"    api-interface: jsonrpc\n" +
				"    node-urls:\n" +
				"      - url: https://backup1.example.com\n" +
				"  - name: backup-2\n" +
				"    chain-id: ETH1\n" +
				"    api-interface: jsonrpc\n" +
				"    node-urls:\n" +
				"      - url: https://backup2.example.com\n",
			expectIsSet:   true,
			wantEndpoints: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("yaml")
			require.NoError(t, v.ReadConfig(strings.NewReader(tc.yamlBody)))

			require.Equal(t, tc.expectIsSet, v.IsSet(common.BackupProvidersConfigName),
				"viper.IsSet(backup-providers) must reflect YAML key presence")

			if !tc.expectIsSet {
				// Disabled branch: caller short-circuits and skips the parser.
				// No further assertion possible — the absence of the key IS
				// the contract.
				return
			}

			endpoints, err := ParseStaticProviderEndpoints(v, common.BackupProvidersConfigName, flagGeolocation)
			require.NoError(t, err)
			require.Len(t, endpoints, tc.wantEndpoints)
			for i, ep := range endpoints {
				assert.Equal(t, flagGeolocation, ep.Geolocation,
					"endpoint #%d must inherit --geolocation flag value, got %d", i, ep.Geolocation)
				assert.NoError(t, ep.Validate(),
					"parsed endpoint #%d must satisfy Validate() (chain-id, api-interface, node-urls, name)", i)
			}
		})
	}
}

// TestParseStaticProviderEndpoints_GroupLabel covers the cross-validation provider-group
// spine (Phase 0.1): the optional `group-label` YAML key must deserialize into
// RPCStaticProviderEndpoint.GroupLabel, an absent key must yield the empty string (the
// implicit "default" group), and two providers may share a label. This is the entry point
// of the group plumbing that later flows to common.ProviderInfo.ProviderGroup via the
// provider session record.
func TestParseStaticProviderEndpoints_GroupLabel(t *testing.T) {
	const flagGeolocation uint64 = 1

	const yamlBody = "direct-rpc:\n" +
		"  - name: eth-rpc-1\n" +
		"    group-label: tier-1\n" +
		"    chain-id: ETH1\n" +
		"    api-interface: jsonrpc\n" +
		"    node-urls:\n" +
		"      - url: https://a.example.com\n" +
		"  - name: eth-rpc-2\n" +
		"    group-label: tier-1\n" + // shares a group with eth-rpc-1
		"    chain-id: ETH1\n" +
		"    api-interface: jsonrpc\n" +
		"    node-urls:\n" +
		"      - url: https://b.example.com\n" +
		"  - name: eth-rpc-3\n" + // no group-label → implicit "default"
		"    chain-id: ETH1\n" +
		"    api-interface: jsonrpc\n" +
		"    node-urls:\n" +
		"      - url: https://c.example.com\n"

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(yamlBody)))

	endpoints, err := ParseStaticProviderEndpoints(v, common.DirectRPCConfigName, flagGeolocation)
	require.NoError(t, err)
	require.Len(t, endpoints, 3)

	byName := map[string]string{}
	for _, ep := range endpoints {
		byName[ep.Name] = ep.GroupLabel
	}

	assert.Equal(t, "tier-1", byName["eth-rpc-1"], "group-label must deserialize into GroupLabel")
	assert.Equal(t, "tier-1", byName["eth-rpc-2"], "providers may share a group label")
	assert.Equal(t, "", byName["eth-rpc-3"], "absent group-label must yield empty string (implicit default group)")
}

// TestParseEndpoints_GeolocationFlagBinding backfills MAG-1872 item 10: the
// --geolocation CLI flag must propagate to every parsed RPCEndpoint via
// ParseEndpoints. TestGeoOrdering in protocol/lavasession/common_test.go
// covers the sort behavior across pre-set geolocations but does NOT exercise
// the flag → endpoint roundtrip.
func TestParseEndpoints_GeolocationFlagBinding(t *testing.T) {
	// lavasession.RPCEndpoint.NetworkAddress is a plain string (HOST:PORT),
	// unlike RPCProviderEndpoint which carries a nested NetworkAddressData
	// struct. The YAML schema must match the target field type.
	const yamlBody = "endpoints:\n" +
		"  - chain-id: ETH1\n" +
		"    api-interface: jsonrpc\n" +
		"    network-address: 127.0.0.1:3333\n" +
		"  - chain-id: LAV1\n" +
		"    api-interface: rest\n" +
		"    network-address: 127.0.0.1:3334\n"

	cases := []struct {
		name        string
		geolocation uint64
	}{
		{name: "geolocation_1_USC", geolocation: 1},
		{name: "geolocation_2_USE", geolocation: 2},
		{name: "geolocation_4_EU", geolocation: 4},
		{name: "geolocation_65535_all_regions", geolocation: 65535},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("yaml")
			require.NoError(t, v.ReadConfig(strings.NewReader(yamlBody)))

			endpoints, err := ParseEndpoints(v, tc.geolocation)
			require.NoError(t, err)
			require.NotEmpty(t, endpoints, "endpoints must be parsed from YAML")
			for i, ep := range endpoints {
				assert.Equal(t, tc.geolocation, ep.Geolocation,
					"endpoint #%d must inherit CLI --geolocation flag value, got %d", i, ep.Geolocation)
				assert.NotEmpty(t, ep.HealthCheckPath,
					"endpoint #%d must have default HealthCheckPath populated when YAML omits it", i)
			}
		})
	}
}
