package performance

import (
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// newViperWithYAML mirrors the RunE mechanics: a config file plus
// BindPFlags-registered flags (flat keys). Environment variables are
// intentionally not bound anywhere in this repo.
func newViperWithYAML(t *testing.T, yaml string) (*viper.Viper, *pflag.FlagSet) {
	t.Helper()
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(yaml)))

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String(RespCacheAddressesFlagName, "", "")
	flags.String(RespCacheTopologyFlagName, "", "")
	flags.String(CacheFlagName, "", "")
	require.NoError(t, v.BindPFlags(flags))
	return v, flags
}

func TestLoadRespCacheConfigFullBlock(t *testing.T) {
	v, _ := newViperWithYAML(t, `
resp-cache:
  topology: sentinel
  addresses: ["s1:26379", "s2:26379"]
  read-addresses: ["reader:6379"]
  master-name: mymaster
  username: datauser
  password: datapass
  sentinel-username: sentineluser
  sentinel-password: sentinelpass
  db: 2
  key-prefix: prod-eu
  dial-timeout: 500ms
  read-timeout: 30ms
  write-timeout: 100ms
  pool-size: 12
  credential-refresh-interval: 5s
  tls:
    enabled: true
    ca-file: /certs/ca.pem
    server-name: cache.internal
`)
	cfg, enabled, err := LoadRespCacheConfig(v)
	require.NoError(t, err)
	require.True(t, enabled)
	require.Equal(t, redisstore.TopologySentinel, cfg.Topology)
	require.Equal(t, []string{"s1:26379", "s2:26379"}, cfg.Addresses)
	require.Equal(t, []string{"reader:6379"}, cfg.ReadAddresses)
	require.Equal(t, "mymaster", cfg.MasterName)
	require.Equal(t, "datauser", cfg.Username)
	require.Equal(t, "sentineluser", cfg.SentinelUsername)
	require.Equal(t, "sentinelpass", cfg.SentinelPassword)
	require.Equal(t, 2, cfg.DB)
	require.Equal(t, "prod-eu", cfg.KeyPrefix)
	require.Equal(t, 500*time.Millisecond, cfg.DialTimeout)
	require.Equal(t, 30*time.Millisecond, cfg.ReadTimeout)
	require.Equal(t, 100*time.Millisecond, cfg.WriteTimeout)
	require.Equal(t, 12, cfg.PoolSize)
	require.Equal(t, 5*time.Second, cfg.CredentialRefreshInterval)
	require.True(t, cfg.TLS.Enabled)
	require.Equal(t, "/certs/ca.pem", cfg.TLS.CAFile)
	require.Equal(t, "cache.internal", cfg.TLS.ServerName)
}

func TestLoadRespCacheConfigAbsentIsDisabled(t *testing.T) {
	v, _ := newViperWithYAML(t, `cache-be: "cache:20100"`)
	_, enabled, err := LoadRespCacheConfig(v)
	require.NoError(t, err)
	require.False(t, enabled)
}

func TestLoadRespCacheConfigFlagsOutrankYAML(t *testing.T) {
	v, flags := newViperWithYAML(t, `
resp-cache:
  topology: standalone
  addresses: ["from-yaml:6379"]
`)
	require.NoError(t, flags.Set(RespCacheAddressesFlagName, "from-flag-1:6379, from-flag-2:6379"))
	require.NoError(t, flags.Set(RespCacheTopologyFlagName, "cluster"))

	cfg, enabled, err := LoadRespCacheConfig(v)
	require.NoError(t, err)
	require.True(t, enabled)
	require.Equal(t, []string{"from-flag-1:6379", "from-flag-2:6379"}, cfg.Addresses,
		"an explicitly passed flag outranks the YAML value")
	require.Equal(t, redisstore.TopologyCluster, cfg.Topology)
}

func TestLoadRespCacheConfigFlagOnlyEnables(t *testing.T) {
	v, flags := newViperWithYAML(t, ``)
	require.NoError(t, flags.Set(RespCacheAddressesFlagName, "solo:6379"))
	cfg, enabled, err := LoadRespCacheConfig(v)
	require.NoError(t, err)
	require.True(t, enabled)
	require.Equal(t, []string{"solo:6379"}, cfg.Addresses)
	require.Empty(t, cfg.Topology, "the field stays empty — standalone defaulting is applied at use, and Validate accepts it")
	require.NoError(t, cfg.Validate())
}

func TestLoadRespCacheConfigDanglingFails(t *testing.T) {
	t.Run("block without addresses", func(t *testing.T) {
		v, _ := newViperWithYAML(t, `
resp-cache:
  key-prefix: prod
`)
		_, _, err := LoadRespCacheConfig(v)
		require.ErrorContains(t, err, "dangling")
	})

	t.Run("topology flag without addresses", func(t *testing.T) {
		v, flags := newViperWithYAML(t, ``)
		require.NoError(t, flags.Set(RespCacheTopologyFlagName, "cluster"))
		_, _, err := LoadRespCacheConfig(v)
		require.ErrorContains(t, err, "dangling")
	})
}

func TestLoadRespCacheConfigValidationSurfaces(t *testing.T) {
	v, _ := newViperWithYAML(t, `
resp-cache:
  topology: ring
  addresses: ["a:1"]
`)
	_, _, err := LoadRespCacheConfig(v)
	require.ErrorContains(t, err, "unknown topology")

	v, _ = newViperWithYAML(t, `
resp-cache:
  topology: sentinel
  addresses: ["s:26379"]
`)
	_, _, err = LoadRespCacheConfig(v)
	require.ErrorContains(t, err, "master-name")
}
