package performance

import (
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
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
	flags.String(RespCacheKeyPrefixFlagName, "", "")
	flags.String(CacheFlagName, "", "")
	flags.String(CacheKeyPrefixFlagName, "", "")
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
  expiration:
    finalized: 2h
    finalized-multiplier: 1.5
    non-finalized: 1s
    non-finalized-multiplier: 1.25
    node-errors: 100ms
    blocks-hashes-to-heights: 24h
`)
	cfg, enabled, err := LoadRespCacheConfig(v)
	require.NoError(t, err)
	require.True(t, enabled)
	require.Equal(t, redisstore.TopologySentinel, cfg.Topology)
	require.Equal(t, 3*time.Hour, cfg.Expiration.Policy().Finalized, "the expiration block reaches the engine's TTL table (MAG-3631)")
	require.Equal(t, 1250*time.Millisecond, cfg.Expiration.Policy().NonFinalized)
	require.Equal(t, 100*time.Millisecond, cfg.Expiration.Policy().NodeErrors)
	require.Equal(t, 24*time.Hour, cfg.Expiration.Policy().BlocksHashesToHeights)
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

// MAG-3631: a partial expiration block keeps the defaults for what it does
// not name, and — because the block is decoded strictly — a misspelled key is
// refused rather than silently leaving the router on the defaults, which is
// the exact shape of the original defect.
func TestLoadRespCacheConfigExpirationBlock(t *testing.T) {
	v, _ := newViperWithYAML(t, `
resp-cache:
  addresses: ["a:6379"]
  expiration:
    finalized-multiplier: 1.5
`)
	cfg, enabled, err := LoadRespCacheConfig(v)
	require.NoError(t, err)
	require.True(t, enabled)
	policy := cfg.Expiration.Policy()
	require.Equal(t, 90*time.Minute, policy.Finalized, "the chart's shipped multiplier, now reachable on a RESP backend")
	require.Equal(t, core.DefaultExpirationForNonFinalized, policy.NonFinalized, "unnamed fields keep the engine's defaults")

	v, _ = newViperWithYAML(t, `
resp-cache:
  addresses: ["a:6379"]
  expiration:
    finalised: 2h
`)
	_, _, err = LoadRespCacheConfig(v)
	require.ErrorContains(t, err, "finalised", "a misspelled expiration key must not fall back to the defaults in silence")

	v, _ = newViperWithYAML(t, `
resp-cache:
  addresses: ["a:6379"]
  expiration:
    finalized: -1h
`)
	_, _, err = LoadRespCacheConfig(v)
	require.ErrorContains(t, err, "expiration.finalized")
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
  key-prefix: from-yaml
`)
	require.NoError(t, flags.Set(RespCacheAddressesFlagName, "from-flag-1:6379, from-flag-2:6379"))
	require.NoError(t, flags.Set(RespCacheTopologyFlagName, "cluster"))
	require.NoError(t, flags.Set(RespCacheKeyPrefixFlagName, "from-flag"))

	cfg, enabled, err := LoadRespCacheConfig(v)
	require.NoError(t, err)
	require.True(t, enabled)
	require.Equal(t, []string{"from-flag-1:6379", "from-flag-2:6379"}, cfg.Addresses,
		"an explicitly passed flag outranks the YAML value")
	require.Equal(t, redisstore.TopologyCluster, cfg.Topology)
	require.Equal(t, "from-flag", cfg.KeyPrefix,
		"the keyspace is the one setting that must differ per deployment, so it needs a flag form (MAG-3687)")
}

// A flag is a complete route to the keyspace: the address flag plus the
// prefix flag, no YAML block at all — what a deployment that can only pass
// flags needs.
func TestLoadRespCacheConfigKeyPrefixByFlagsAlone(t *testing.T) {
	v, flags := newViperWithYAML(t, ``)
	require.NoError(t, flags.Set(RespCacheAddressesFlagName, "solo:6379"))
	require.NoError(t, flags.Set(RespCacheKeyPrefixFlagName, "tenant-a"))
	cfg, enabled, err := LoadRespCacheConfig(v)
	require.NoError(t, err)
	require.True(t, enabled)
	require.Equal(t, "tenant-a", cfg.KeyPrefix)
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

	t.Run("key-prefix flag without addresses", func(t *testing.T) {
		v, flags := newViperWithYAML(t, ``)
		require.NoError(t, flags.Set(RespCacheKeyPrefixFlagName, "tenant-a"))
		_, _, err := LoadRespCacheConfig(v)
		require.ErrorContains(t, err, "dangling")
	})
}

// MAG-3677: a key the block does not define must fail loudly. The block decides
// which keyspace the router occupies, and a `key_prefix` that was ignored put
// the router on the shared default without a word.
func TestLoadRespCacheConfigRejectsUnknownKeys(t *testing.T) {
	t.Run("top-level typo", func(t *testing.T) {
		v, _ := newViperWithYAML(t, `
resp-cache:
  addresses: ["a:6379"]
  key_prefix: prod-eu
`)
		_, _, err := LoadRespCacheConfig(v)
		require.ErrorContains(t, err, "key_prefix")
		require.ErrorContains(t, err, "unknown keys are rejected")
	})

	t.Run("typo inside the tls block", func(t *testing.T) {
		v, _ := newViperWithYAML(t, `
resp-cache:
  addresses: ["a:6379"]
  tls:
    enable: true
    ca-file: /certs/ca.pem
`)
		_, _, err := LoadRespCacheConfig(v)
		require.ErrorContains(t, err, "enable", "nested blocks are held to the same rule")
	})

	t.Run("every defined key still loads", func(t *testing.T) {
		// The full-block test above is the positive control for this rule; this
		// pins that strictness did not start rejecting a key the block defines.
		v, _ := newViperWithYAML(t, `
resp-cache:
  addresses: ["a:6379"]
  key-prefix: prod-eu
  tls:
    enabled: true
    insecure-skip-verify: true
`)
		cfg, enabled, err := LoadRespCacheConfig(v)
		require.NoError(t, err)
		require.True(t, enabled)
		require.Equal(t, "prod-eu", cfg.KeyPrefix)
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

// MAG-3683: the ticket's configuration — credentials plus a tls block carrying
// all three file paths and no tls.enabled — must not load. Before this the
// block was inert (the files are only opened when the switch is on) and the
// router started in plaintext with the password in its first write.
func TestLoadRespCacheConfigRefusesTLSBlockWithoutSwitch(t *testing.T) {
	v, _ := newViperWithYAML(t, `
resp-cache:
  addresses: ["cache.internal:6379"]
  username: "cacheuser"
  password: "placeholder-cache-credential"
  tls:
    ca-file:   "/nonexistent/path/ca.pem"
    cert-file: "/nonexistent/path/ca.pem"
    key-file:  "/nonexistent/path/ca.pem"
`)
	_, enabled, err := LoadRespCacheConfig(v)
	require.ErrorContains(t, err, "tls.enabled")
	require.False(t, enabled)

	// Control: the same block with the switch on passes configuration and then
	// fails at construction on the first file — the failure this configuration
	// should always have produced.
	v, _ = newViperWithYAML(t, `
resp-cache:
  addresses: ["cache.internal:6379"]
  username: "cacheuser"
  password: "placeholder-cache-credential"
  tls:
    enabled: true
    ca-file:   "/nonexistent/path/ca.pem"
    cert-file: "/nonexistent/path/ca.pem"
    key-file:  "/nonexistent/path/ca.pem"
`)
	cfg, enabled, err := LoadRespCacheConfig(v)
	require.NoError(t, err)
	require.True(t, enabled)
	_, err = redisstore.New(cfg)
	require.ErrorContains(t, err, "/nonexistent/path/ca.pem", "with the switch on the files are read, and the missing one names itself")
}
