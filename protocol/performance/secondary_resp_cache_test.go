package performance

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// newViperWithSecondaryRespFlags is newViperWithYAML with the secondary-resp-cache
// flags registered as well, the way RunE binds them.
func newViperWithSecondaryRespFlags(t *testing.T, yaml string) (*viper.Viper, *pflag.FlagSet) {
	t.Helper()
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(yaml)))
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	for _, name := range []string{
		RespCacheAddressesFlagName, RespCacheTopologyFlagName, RespCacheKeyPrefixFlagName,
		SecondaryRespCacheAddressesFlagName, SecondaryRespCacheTopologyFlagName, SecondaryRespCacheKeyPrefixFlagName,
	} {
		flags.String(name, "", "")
	}
	require.NoError(t, v.BindPFlags(flags))
	return v, flags
}

func TestLoadSecondaryRespCacheConfig(t *testing.T) {
	t.Run("block enables the tier and names the store", func(t *testing.T) {
		v, _ := newViperWithSecondaryRespFlags(t, `
secondary-resp-cache:
  addresses: ["zone-a-valkey:6379"]
  key-prefix: zone-a
`)
		cfg, enabled, err := LoadSecondaryRespCacheConfig(v)
		require.NoError(t, err)
		require.True(t, enabled)
		require.Equal(t, []string{"zone-a-valkey:6379"}, cfg.Addresses)
		require.Equal(t, "zone-a", cfg.KeyPrefix)
		require.Equal(t, secondaryRespCacheLogName, cfg.Name, "the store's logs and errors must say which tier they are about")
	})

	t.Run("flags alone enable it and outrank the block", func(t *testing.T) {
		v, flags := newViperWithSecondaryRespFlags(t, `
secondary-resp-cache:
  addresses: ["from-yaml:6379"]
  key-prefix: from-yaml
`)
		require.NoError(t, flags.Set(SecondaryRespCacheAddressesFlagName, "from-flag:6379"))
		require.NoError(t, flags.Set(SecondaryRespCacheKeyPrefixFlagName, "zone-a"))
		cfg, enabled, err := LoadSecondaryRespCacheConfig(v)
		require.NoError(t, err)
		require.True(t, enabled)
		require.Equal(t, []string{"from-flag:6379"}, cfg.Addresses)
		require.Equal(t, "zone-a", cfg.KeyPrefix)
	})

	t.Run("absent is disabled, and the primary's block is not read", func(t *testing.T) {
		v, _ := newViperWithSecondaryRespFlags(t, `
resp-cache:
  addresses: ["primary:6379"]
`)
		_, enabled, err := LoadSecondaryRespCacheConfig(v)
		require.NoError(t, err)
		require.False(t, enabled)
	})

	t.Run("block without addresses is dangling", func(t *testing.T) {
		v, _ := newViperWithSecondaryRespFlags(t, `
secondary-resp-cache:
  key-prefix: zone-a
`)
		_, _, err := LoadSecondaryRespCacheConfig(v)
		require.ErrorContains(t, err, "dangling")
		require.ErrorContains(t, err, SecondaryRespCacheViperKey)
	})

	t.Run("unknown keys are rejected", func(t *testing.T) {
		v, _ := newViperWithSecondaryRespFlags(t, `
secondary-resp-cache:
  addresses: ["a:6379"]
  key_prefix: zone-a
`)
		_, _, err := LoadSecondaryRespCacheConfig(v)
		require.ErrorContains(t, err, "key_prefix")
	})

	t.Run("read-addresses is refused", func(t *testing.T) {
		v, _ := newViperWithSecondaryRespFlags(t, `
secondary-resp-cache:
  addresses: ["a:6379"]
  read-addresses: ["b:6379"]
`)
		_, enabled, err := LoadSecondaryRespCacheConfig(v)
		require.ErrorContains(t, err, "read-addresses")
		require.False(t, enabled)
	})

	t.Run("expiration is refused", func(t *testing.T) {
		v, _ := newViperWithSecondaryRespFlags(t, `
secondary-resp-cache:
  addresses: ["a:6379"]
  expiration:
    finalized: 2h
`)
		_, enabled, err := LoadSecondaryRespCacheConfig(v)
		require.ErrorContains(t, err, "expiration")
		require.False(t, enabled)
	})

	t.Run("a validation error names the secondary block", func(t *testing.T) {
		v, _ := newViperWithSecondaryRespFlags(t, `
secondary-resp-cache:
  topology: sentinel
  addresses: ["s1:26379"]
`)
		_, _, err := LoadSecondaryRespCacheConfig(v)
		require.ErrorContains(t, err, secondaryRespCacheLogName+": sentinel topology requires master-name")
	})
}

// One zone writes through its own RESP primary; the other reads that store as its
// secondary tier, by the writing zone's key prefix (MAG-4778).
func TestSecondaryRespCacheReadsAnotherZonesEntries(t *testing.T) {
	mr := miniredis.RunT(t)
	writerStore, err := redisstore.NewWithClient(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "zone-a")
	require.NoError(t, err)
	writer := NewRespCache(writerStore, core.DefaultPolicy())
	t.Cleanup(func() { _ = writer.Close() })

	hash := []byte("request-hash")
	require.NoError(t, writer.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
		RequestHash:      hash,
		ChainId:          "ETH1",
		RequestedBlock:   100,
		SeenBlock:        100,
		Finalized:        true,
		AverageBlockTime: int64(12 * time.Second),
		Response:         &pairingtypes.RelayReply{Data: []byte(`{"result":"0x1"}`), LatestBlock: 100},
	}))

	open := func(t *testing.T, keyPrefix string) *RespCache {
		t.Helper()
		reader, err := OpenSecondaryRespCache(redisstore.Config{Name: secondaryRespCacheLogName, Addresses: []string{mr.Addr()}, KeyPrefix: keyPrefix})
		require.NoError(t, err)
		t.Cleanup(func() { _ = reader.Close() })
		return reader
	}
	get := func(t *testing.T, reader CacheReader) *pairingtypes.CacheRelayReply {
		t.Helper()
		reply, err := reader.GetEntry(context.Background(), &pairingtypes.RelayCacheGet{
			RequestHash:    hash,
			ChainId:        "ETH1",
			RequestedBlock: 100,
			SeenBlock:      100,
			Finalized:      true,
		})
		require.NoError(t, err)
		return reply
	}

	before := mr.Dump()
	keys := mr.Keys()
	ttls := map[string]time.Duration{}
	for _, key := range keys {
		ttls[key] = mr.TTL(key)
	}

	reader := open(t, "zone-a")
	reply := get(t, reader)
	require.NotNil(t, reply.GetReply(), "the writing zone's keyspace is a hit")
	require.Equal(t, `{"result":"0x1"}`, string(reply.GetReply().GetData()))

	require.Nil(t, get(t, open(t, "zone-b")).GetReply(), "another keyspace on the same backend is a miss")

	require.Equal(t, before, mr.Dump(), "a read-only tier writes nothing")
	for _, key := range mr.Keys() {
		require.Equal(t, ttls[key], mr.TTL(key), "and refreshes no lifetime: %s", key)
	}

	state := reader.DebugCacheState()
	require.True(t, state.Configured)
	require.Equal(t, CacheEngineRESP, state.Engine)
	require.Nil(t, state.Lifetimes, "the writing zone owns the TTLs, so the reader reports none")
}

// The secondary's failures and probes must never move the smartrouter_resp_cache_*
// series, which an operator reads as the primary's.
func TestSecondaryRespCacheLeavesThePrimarysSeriesAlone(t *testing.T) {
	registered := getRespCacheMetrics()
	registered.connected.Set(42)
	t.Cleanup(func() { registered.connected.Set(0) })
	failedBefore := testutil.ToFloat64(registered.opsFailed.WithLabelValues(respCacheOpGet, "error")) +
		testutil.ToFloat64(registered.opsFailed.WithLabelValues(respCacheOpGet, "timeout"))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddress := listener.Addr().String()
	require.NoError(t, listener.Close())

	reader, err := OpenSecondaryRespCache(redisstore.Config{Name: secondaryRespCacheLogName, Addresses: []string{deadAddress}, KeyPrefix: "zone-a"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	require.Eventually(t, func() bool { return reader.DebugCacheState().Reachable != nil }, 10*time.Second, 50*time.Millisecond)
	require.False(t, *reader.DebugCacheState().Reachable)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _ = reader.GetEntry(ctx, &pairingtypes.RelayCacheGet{RequestHash: []byte("h"), ChainId: "ETH1", RequestedBlock: 1})

	require.Equal(t, float64(42), testutil.ToFloat64(registered.connected), "the secondary's probe did not touch the primary's gauge")
	failedAfter := testutil.ToFloat64(registered.opsFailed.WithLabelValues(respCacheOpGet, "error")) +
		testutil.ToFloat64(registered.opsFailed.WithLabelValues(respCacheOpGet, "timeout"))
	require.Equal(t, failedBefore, failedAfter, "nor the primary's failure series")
}
