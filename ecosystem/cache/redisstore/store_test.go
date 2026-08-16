package redisstore

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store, err := NewWithClient(client, "sr")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store, mr
}

func TestKeyPrefixValidation(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	for _, bad := range []string{"a*b", "x?", "pre[fix", "sp ace", "colon:"} {
		_, err := NewWithClient(client, bad)
		require.Error(t, err, "glob-unsafe prefix %q must be rejected", bad)
	}

	store, err := NewWithClient(client, "")
	require.NoError(t, err)
	require.Equal(t, DefaultKeyPrefix, store.prefix, "empty prefix defaults")

	_, err = NewWithClient(client, "prod-eu_1.cache")
	require.NoError(t, err)
}

func TestEnvelopeRoundTrip(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	env := core.Envelope{
		Response:         relaytypes.RelayReply{Data: []byte(`{"result":"0x64"}`), LatestBlock: 100},
		Hash:             []byte("0xblockhash"),
		OptionalMetadata: []relaytypes.Metadata{{Name: "k", Value: "v"}},
		SeenBlock:        100,
		IsCompressed:     false,
	}
	key := core.RelayKey(false, "ETH1", []byte{0x01}, 100)
	require.NoError(t, store.SetEntry(ctx, key, &env, time.Minute))

	entries, err := store.GetEntries(ctx, []string{key, core.RelayKey(true, "ETH1", []byte{0x01}, 100)})
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.NotNil(t, entries[0])
	require.Nil(t, entries[1], "unwritten variant is a miss")
	require.Equal(t, env, *entries[0])
}

func TestCorruptEntryReadsAsMiss(t *testing.T) {
	store, mr := newTestStore(t)
	key := core.RelayKey(false, "ETH1", []byte{0x02}, 100)
	require.NoError(t, mr.Set("sr:"+key, "not-json"))

	entries, err := store.GetEntries(context.Background(), []string{key})
	require.NoError(t, err, "a corrupt entry must never fail the lookup")
	require.Nil(t, entries[0])
}

// The stalled-chain pin: greater-or-EQUAL semantics mean an equal observation
// rewrites the key, refreshing its TTL — an actively observed but
// non-advancing tip must not expire.
func TestSetInt64GreaterOrEqual(t *testing.T) {
	store, mr := newTestStore(t)
	ctx := context.Background()
	key := core.SharedTipKey("ETH1", "fleet")

	require.NoError(t, store.SetInt64IfGreaterOrEqual(ctx, key, 100, 2*time.Second))
	v, found, err := store.GetInt64(ctx, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(100), v)

	require.NoError(t, store.SetInt64IfGreaterOrEqual(ctx, key, 90, 2*time.Second))
	v, _, _ = store.GetInt64(ctx, key)
	require.Equal(t, int64(100), v, "a lower write must not lower the tip")

	mr.FastForward(1 * time.Second)
	require.NoError(t, store.SetInt64IfGreaterOrEqual(ctx, key, 100, 2*time.Second))
	require.InDelta(t, (2 * time.Second).Seconds(), mr.TTL("sr:"+key).Seconds(), 0.1,
		"an equal observation refreshes the TTL — stalled chains keep their tip")

	mr.FastForward(1500 * time.Millisecond)
	_, found, _ = store.GetInt64(ctx, key)
	require.True(t, found, "tip survives past the original deadline thanks to the refresh")

	require.NoError(t, store.SetInt64IfGreaterOrEqual(ctx, key, 150, 2*time.Second))
	v, _, _ = store.GetInt64(ctx, key)
	require.Equal(t, int64(150), v)
}

// Chain-tip semantics ported from the in-memory adapter: readers honour the
// embedded freshness deadline, while the monotonic guard keeps comparing
// against the raw stored block even after it goes stale.
func TestChainTipFreshnessAndFencing(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	key := core.ChainTipKey("ETH1")

	require.NoError(t, store.SetChainTipIfGreaterOrEqual(ctx, key, 100))
	block, fresh, err := store.GetChainTip(ctx, key)
	require.NoError(t, err)
	require.True(t, fresh)
	require.Equal(t, int64(100), block)

	require.NoError(t, store.SetChainTipIfGreaterOrEqual(ctx, key, 90))
	block, _, _ = store.GetChainTip(ctx, key)
	require.Equal(t, int64(100), block, "a lower write must not move the tip backward")

	// Let the freshness window (core.DefaultExpirationForNonFinalized) lapse.
	time.Sleep(core.DefaultExpirationForNonFinalized + 100*time.Millisecond)
	_, fresh, _ = store.GetChainTip(ctx, key)
	require.False(t, fresh, "a stale tip reads as unknown")

	require.NoError(t, store.SetChainTipIfGreaterOrEqual(ctx, key, 90))
	_, fresh, _ = store.GetChainTip(ctx, key)
	require.False(t, fresh, "even stale, the stored block fences lower writes")

	require.NoError(t, store.SetChainTipIfGreaterOrEqual(ctx, key, 150))
	block, fresh, _ = store.GetChainTip(ctx, key)
	require.True(t, fresh)
	require.Equal(t, int64(150), block)
}

func TestHeightsRoundTrip(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	key := core.HeightKey("ETH1", "0xdeadbeef")

	_, found, err := store.GetHeight(ctx, key)
	require.NoError(t, err)
	require.False(t, found)

	require.NoError(t, store.SetHeight(ctx, key, 12345, time.Hour))
	v, found, err := store.GetHeight(ctx, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(12345), v)
}

// Purge is prefix-scoped: everything under this store's prefix goes, foreign
// keys survive — the shared-backend safety property.
func TestPurgePrefixIsolation(t *testing.T) {
	store, mr := newTestStore(t)
	ctx := context.Background()

	env := core.Envelope{Response: relaytypes.RelayReply{Data: []byte(`x`)}}
	require.NoError(t, store.SetEntry(ctx, core.RelayKey(false, "ETH1", []byte{0x03}, 1), &env, time.Minute))
	require.NoError(t, store.SetInt64IfGreaterOrEqual(ctx, core.SharedTipKey("ETH1", "fleet"), 10, time.Minute))
	require.NoError(t, store.SetHeight(ctx, core.HeightKey("ETH1", "0xh"), 5, time.Minute))
	require.NoError(t, mr.Set("othertenant:key", "must-survive"))
	require.NoError(t, mr.Set("sr2:lookalike", "different prefix, must survive"))

	require.NoError(t, store.Purge(ctx))

	entries, err := store.GetEntries(ctx, []string{core.RelayKey(false, "ETH1", []byte{0x03}, 1)})
	require.NoError(t, err)
	require.Nil(t, entries[0])
	_, found, _ := store.GetInt64(ctx, core.SharedTipKey("ETH1", "fleet"))
	require.False(t, found)
	require.True(t, mr.Exists("othertenant:key"), "foreign keys must survive a purge")
	require.True(t, mr.Exists("sr2:lookalike"), "a longer prefix sharing our spelling must survive")
}

// The D8 split: writes go to the write endpoint, reads to the read endpoint —
// proven with two live miniredis instances rather than spies.
func TestReadWriteRouting(t *testing.T) {
	mrWrite := miniredis.RunT(t)
	mrRead := miniredis.RunT(t)
	writeClient := redis.NewClient(&redis.Options{Addr: mrWrite.Addr()})
	readClient := redis.NewClient(&redis.Options{Addr: mrRead.Addr()})
	store, err := NewWithClients(writeClient, readClient, "sr")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	key := core.HeightKey("ETH1", "0xrouted")
	require.NoError(t, store.SetHeight(ctx, key, 42, time.Minute))
	require.True(t, mrWrite.Exists("sr:"+key), "writes land on the write endpoint")
	require.False(t, mrRead.Exists("sr:"+key), "writes never touch the read endpoint")

	_, found, err := store.GetHeight(ctx, key)
	require.NoError(t, err)
	require.False(t, found, "reads come from the read endpoint — replication is the infrastructure's job")

	require.NoError(t, mrRead.Set("sr:"+key, "77"))
	v, found, err := store.GetHeight(ctx, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(77), v, "a replicated entry on the read endpoint serves reads")

	// Purge is a write-side operation: the read endpoint is out of scope.
	require.NoError(t, store.Purge(ctx))
	require.False(t, mrWrite.Exists("sr:"+key))
	require.True(t, mrRead.Exists("sr:"+key))
}

func TestChainTipNotApplicableWhenMissing(t *testing.T) {
	store, _ := newTestStore(t)
	block, fresh, err := store.GetChainTip(context.Background(), core.ChainTipKey("NOWHERE"))
	require.NoError(t, err)
	require.False(t, fresh)
	require.Equal(t, spectypes.NOT_APPLICABLE, block)
}
