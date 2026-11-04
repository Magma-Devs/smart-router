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

	// A refused key is reported as refused, before any rule written for the block that uses it:
	// two read addresses used to get the standalone rule's advice to switch topology, and a
	// negative expiration an error naming resp-cache.
	t.Run("refused keys are refused before validation", func(t *testing.T) {
		v, _ := newViperWithSecondaryRespFlags(t, `
secondary-resp-cache:
  addresses: ["a:6379"]
  read-addresses: ["b:6379", "c:6379"]
`)
		_, _, err := LoadSecondaryRespCacheConfig(v)
		require.ErrorContains(t, err, "read-addresses is not used by a read-only tier")

		v, _ = newViperWithSecondaryRespFlags(t, `
secondary-resp-cache:
  addresses: ["a:6379"]
  expiration:
    finalized: -1s
`)
		_, _, err = LoadSecondaryRespCacheConfig(v)
		require.ErrorContains(t, err, "expiration is not used by a read-only tier")
	})

	t.Run("a bad key prefix names the secondary block", func(t *testing.T) {
		v, _ := newViperWithSecondaryRespFlags(t, `
secondary-resp-cache:
  addresses: ["a:6379"]
  key-prefix: "zone*a"
`)
		_, enabled, err := LoadSecondaryRespCacheConfig(v)
		require.ErrorContains(t, err, secondaryRespCacheLogName+": invalid cache key prefix")
		require.False(t, enabled)
	})

	t.Run("a TLS file error names the secondary block", func(t *testing.T) {
		v, _ := newViperWithSecondaryRespFlags(t, `
secondary-resp-cache:
  addresses: ["a:6379"]
  tls:
    enabled: true
    ca-file: /nonexistent/path/ca.pem
`)
		cfg, enabled, err := LoadSecondaryRespCacheConfig(v)
		require.NoError(t, err)
		require.True(t, enabled)
		_, err = OpenSecondaryRespCache(cfg, DefaultSecondaryCacheTimeout)
		require.ErrorContains(t, err, secondaryRespCacheLogName+" tls: reading ca-file")
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
		reader, err := OpenSecondaryRespCache(redisstore.Config{Name: secondaryRespCacheLogName, Addresses: []string{mr.Addr()}, KeyPrefix: keyPrefix}, DefaultSecondaryCacheTimeout)
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

	reader, err := OpenSecondaryRespCache(redisstore.Config{Name: secondaryRespCacheLogName, Addresses: []string{deadAddress}, KeyPrefix: "zone-a"}, DefaultSecondaryCacheTimeout)
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

// A second tier is usually another zone, so its round trip can exceed the primary's 50ms lookup
// budget; secondary-cache-timeout is raised for exactly that hop. After a failure streak the
// tier's breaker must close on a probe answered within that budget. Held to the primary's budget,
// it stayed open while the backend answered every probe in 100ms, and the tier was skipped until
// the router restarted (MAG-4778).
func TestSecondaryRespCacheBreakerClosesWithinTheSecondaryBudget(t *testing.T) {
	mr := miniredis.RunT(t)
	proxy := newFreezableProxy(t, mr.Addr())
	const budget = 400 * time.Millisecond
	reader, err := OpenSecondaryRespCache(redisstore.Config{Name: secondaryRespCacheLogName, Addresses: []string{proxy.listener.Addr().String()}, KeyPrefix: "zone-a"}, budget)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	lookupWithin := func(within time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), within)
		defer cancel()
		_, err := reader.GetEntry(ctx, &pairingtypes.RelayCacheGet{RequestHash: []byte("cross-zone"), ChainId: "ETH1", RequestedBlock: 100, SeenBlock: 100})
		return err
	}
	lookup := func() error { return lookupWithin(budget) }
	// Establish the connection while the proxy is fast, so the cold handshake through it does not
	// count against the budget under the race detector.
	require.NoError(t, lookupWithin(2*time.Second), "sanity: a clean miss while the proxy is fast")
	require.Eventually(t, func() bool { return reader.DebugCacheState().Reachable != nil }, 5*time.Second, 10*time.Millisecond)

	// Another zone: every reply 100ms away, slower than the primary's budget and well inside this one.
	proxy.delay.Store(int64(100 * time.Millisecond))
	require.NoError(t, lookup(), "a lookup inside secondary-cache-timeout is a clean miss")

	// A transient stall opens the breaker.
	proxy.frozen.Store(true)
	for i := 0; i < respCacheBreakerThreshold; i++ {
		require.ErrorIs(t, lookup(), core.StoreError)
	}
	require.True(t, reader.readBreaker.open.Load())
	proxy.frozen.Store(false)

	require.Eventually(t, func() bool { return !reader.readBreaker.open.Load() }, 5*time.Second, 10*time.Millisecond,
		"a probe answered within secondary-cache-timeout closes the breaker")
	// The stalled connections were dropped, so this one may dial again: a wider budget than the
	// tier's keeps the handshake through the proxy from failing it under the race detector.
	require.NoError(t, lookupWithin(2*time.Second), "lookups resume as clean misses")
}
