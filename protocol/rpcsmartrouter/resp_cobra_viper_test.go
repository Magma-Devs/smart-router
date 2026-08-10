package rpcsmartrouter_test

// Acceptance: the cobra/viper wiring exercised through the REAL cobra command's
// flags and YAML.
//
// The other RESP config tests build a synthetic pflag.FlagSet, which proves the
// loader's precedence logic but not that the SHIPPED command registers these
// flags, nor that they land where the loader looks. These tests drive
// CreateRPCSmartRouterCobraCommand() and replicate RunE's exact viper sequence
// (rpcsmartrouter.go: SetConfigFile/SetConfigType -> BindPFlags(cmd.Flags()) ->
// ReadInConfig), then assert on the real loader.
//
// No router is started, no provider is dialled, no network is touched: the
// sequence under test ends before RunE reaches any I/O.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/magma-Devs/smart-router/protocol/rpcsmartrouter"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// wireLikeRunE reproduces the startup sequence the real command performs, on
// the real global viper the command uses. Global state is reset before and
// after so these tests are order-independent regardless of what else in the
// package touches viper.
func wireLikeRunE(t *testing.T, yamlBody string, flagArgs ...string) {
	t.Helper()

	viper.Reset()
	t.Cleanup(viper.Reset)

	cmd := rpcsmartrouter.CreateRPCSmartRouterCobraCommand()
	require.NotNil(t, cmd, "the shipped cobra command must be constructible")

	// The flags must exist on the SHIPPED command — this is the part the
	// synthetic FlagSet tests cannot cover.
	require.NotNil(t, cmd.Flags().Lookup(performance.RespCacheAddressesFlagName),
		"--%s must be registered on the real command", performance.RespCacheAddressesFlagName)
	require.NotNil(t, cmd.Flags().Lookup(performance.RespCacheTopologyFlagName),
		"--%s must be registered on the real command", performance.RespCacheTopologyFlagName)

	if yamlBody != "" {
		dir := t.TempDir()
		path := filepath.Join(dir, "smartrouter.yml")
		require.NoError(t, os.WriteFile(path, []byte(yamlBody), 0o600))
		viper.SetConfigFile(path)
		viper.SetConfigType("yml")
	}

	// Explicit flags, parsed by cobra exactly as on the command line.
	if len(flagArgs) > 0 {
		require.NoError(t, cmd.Flags().Parse(flagArgs))
	}

	// RunE order: bind the command's flags, then read the config file.
	require.NoError(t, viper.BindPFlags(cmd.Flags()))
	if yamlBody != "" {
		require.NoError(t, viper.ReadInConfig())
	}
}

// YAML alone configures the backend through the real command's viper.
func TestRealCommand_RespCacheFromYAML(t *testing.T) {
	wireLikeRunE(t, `
resp-cache:
  topology: sentinel
  addresses: ["yaml-a:26379", "yaml-b:26379"]
  master-name: mymaster
  key-prefix: "yamlpfx"
`)

	cfg, enabled, err := performance.LoadRespCacheConfig(viper.GetViper())
	require.NoError(t, err)
	require.True(t, enabled, "a resp-cache block with addresses enables the backend")
	require.Equal(t, redisstore.TopologySentinel, cfg.Topology)
	require.Equal(t, []string{"yaml-a:26379", "yaml-b:26379"}, cfg.Addresses)
	require.Equal(t, "mymaster", cfg.MasterName)
	require.Equal(t, "yamlpfx", cfg.KeyPrefix)
}

// Explicit flags outrank the YAML block — the precedence the plan requires.
func TestRealCommand_FlagsOutrankYAML(t *testing.T) {
	wireLikeRunE(t, `
resp-cache:
  topology: sentinel
  addresses: ["yaml-a:26379", "yaml-b:26379"]
  master-name: mymaster
  key-prefix: "yamlpfx"
`,
		"--"+performance.RespCacheAddressesFlagName, "flag-1:6379,flag-2:6379",
		"--"+performance.RespCacheTopologyFlagName, "cluster",
	)

	cfg, enabled, err := performance.LoadRespCacheConfig(viper.GetViper())
	require.NoError(t, err)
	require.True(t, enabled)

	// Flag wins on both keys it sets...
	require.Equal(t, redisstore.TopologyCluster, cfg.Topology,
		"--%s must outrank the YAML topology", performance.RespCacheTopologyFlagName)
	require.Equal(t, []string{"flag-1:6379", "flag-2:6379"}, cfg.Addresses,
		"--%s must replace, not append to, the YAML addresses", performance.RespCacheAddressesFlagName)

	// ...and YAML still supplies everything the flags do not cover.
	require.Equal(t, "mymaster", cfg.MasterName, "unflagged YAML keys survive the overlay")
	require.Equal(t, "yamlpfx", cfg.KeyPrefix)
}

// RESP settings are never sourced from the environment: this repo binds no env
// prefix anywhere, and operators must not get surprise configuration from a
// stray shell variable.
func TestRealCommand_EnvironmentRemainsUnbound(t *testing.T) {
	for _, kv := range [][2]string{
		{"RESP_CACHE_ADDRESSES", "env-host:6379"},
		{"RESP_CACHE_TOPOLOGY", "cluster"},
		{"SMARTROUTER_RESP_CACHE_ADDRESSES", "env-host:6379"},
		{"RESP-CACHE-ADDRESSES", "env-host:6379"},
	} {
		t.Setenv(kv[0], kv[1])
	}

	wireLikeRunE(t, `
resp-cache:
  addresses: ["yaml-only:6379"]
`)

	cfg, enabled, err := performance.LoadRespCacheConfig(viper.GetViper())
	require.NoError(t, err)
	require.True(t, enabled)
	require.Equal(t, []string{"yaml-only:6379"}, cfg.Addresses,
		"environment variables must not reach RESP configuration")
	require.NotEqual(t, redisstore.TopologyCluster, cfg.Topology,
		"RESP_CACHE_TOPOLOGY must not be honoured")
}

// With no RESP configuration at all the pre-existing startup selection is
// untouched: RESP reports disabled and cache-be remains readable for the
// legacy gRPC path (UC-5 / rollback).
func TestRealCommand_NoRespConfigKeepsLegacyPath(t *testing.T) {
	wireLikeRunE(t, `
`+performance.CacheFlagName+`: "127.0.0.1:20100"
`)

	cfg, enabled, err := performance.LoadRespCacheConfig(viper.GetViper())
	require.NoError(t, err)
	require.False(t, enabled, "absent resp-cache config must not enable the RESP backend")
	require.Empty(t, cfg.Addresses)

	require.Equal(t, "127.0.0.1:20100", viper.GetString(performance.CacheFlagName),
		"the legacy cache-be selection must remain exactly as before")
}

// A flag alone (no YAML block at all) is a complete configuration.
func TestRealCommand_FlagOnlyConfiguration(t *testing.T) {
	wireLikeRunE(t, "",
		"--"+performance.RespCacheAddressesFlagName, "only-flag:6379",
	)

	cfg, enabled, err := performance.LoadRespCacheConfig(viper.GetViper())
	require.NoError(t, err)
	require.True(t, enabled, "the address flag alone enables the backend")
	require.Equal(t, []string{"only-flag:6379"}, cfg.Addresses)
	require.Equal(t, redisstore.Topology(""), cfg.Topology,
		"topology stays unset so the client defaults to standalone")
}
