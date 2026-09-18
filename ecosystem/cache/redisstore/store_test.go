package redisstore

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
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

// A corrupt value must not be able to WEDGE tip publishing.
//
// GetInt64 already treats corruption as a miss rather than fataling, because a
// router-embedded backend on a SHARED store must not be crashable by a foreign
// writer. The write side has to honour the same invariant: if the compare-and-set
// raises on a non-numeric stored value, the SET never runs, the key is never
// overwritten, and every subsequent write fails identically — the key cannot
// self-heal and tip publishing for that chain/pod is dead until someone deletes
// it by hand.
func TestSetInt64OverCorruptValue(t *testing.T) {
	store, mr := newTestStore(t)
	ctx := context.Background()
	key := core.SharedTipKey("ETH1", "fleet")
	require.NoError(t, mr.Set("sr:"+key, "not-a-number"))

	_, found, err := store.GetInt64(ctx, key)
	require.NoError(t, err, "corruption reads as a miss, not an error")
	require.False(t, found)

	require.NoError(t, store.SetInt64IfGreaterOrEqual(ctx, key, 100, 2*time.Second),
		"a corrupt stored value must fall through to the write, not fence it")

	value, found, err := store.GetInt64(ctx, key)
	require.NoError(t, err)
	require.True(t, found, "the write must have landed — the key self-heals")
	require.Equal(t, int64(100), value)
}

// Same invariant for the chain tip. This script was already correct against a
// real backend; the test exists because it is the pair to the one above, and
// because it pins the miniredis-portable form — see setChainTipGEScript on why
// the match result is bound before tonumber rather than nested inside it.
func TestSetChainTipOverCorruptValue(t *testing.T) {
	store, mr := newTestStore(t)
	ctx := context.Background()
	key := core.ChainTipKey("ETH1")
	require.NoError(t, mr.Set("sr:"+key, "garbage-no-digits"))

	_, fresh, err := store.GetChainTip(ctx, key)
	require.NoError(t, err)
	require.False(t, fresh, "a corrupt tip reads as unknown")

	require.NoError(t, store.SetChainTipIfGreaterOrEqual(ctx, key, 100),
		"a corrupt stored value must not fence the write")

	block, fresh, err := store.GetChainTip(ctx, key)
	require.NoError(t, err)
	require.True(t, fresh)
	require.Equal(t, int64(100), block)
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

	// Purge reaches BOTH endpoints: the read store may be one the write store
	// never feeds, and an entry left there is served after every reset.
	require.NoError(t, store.Purge(ctx))
	require.False(t, mrWrite.Exists("sr:"+key))
	require.False(t, mrRead.Exists("sr:"+key), "a reset must empty the store reads come from, not only the one writes go to (MAG-3673)")
}

// MAG-3673 as it was measured: three rounds, one entry planted per store per
// round, a reset after each. The read store used to keep everything and
// accumulate while the write store was emptied every time. Two controls, as on
// the ticket: the read store is shown to hold its entry immediately before each
// purge, and a key outside the prefix survives on both stores, so the purge is
// prefix-scoped and doing work rather than deleting everything.
func TestPurgeEmptiesASeparateReadStore(t *testing.T) {
	mrWrite, mrRead := miniredis.RunT(t), miniredis.RunT(t)
	store, err := NewWithClients(
		redis.NewClient(&redis.Options{Addr: mrWrite.Addr()}),
		redis.NewClient(&redis.Options{Addr: mrRead.Addr()}),
		"sr")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	require.NoError(t, mrWrite.Set("othertenant:key", "must-survive"))
	require.NoError(t, mrRead.Set("othertenant:key", "must-survive"))

	for round := 1; round <= 3; round++ {
		key := "sr:" + core.HeightKey("ETH1", fmt.Sprintf("0x%d", round))
		require.NoError(t, mrWrite.Set(key, "1"))
		require.NoError(t, mrRead.Set(key, "1"), "planted directly: the write endpoint never feeds this store")
		require.True(t, mrRead.Exists(key), "control: the entry is there to lose, round %d", round)

		require.NoError(t, store.Purge(ctx))

		require.Empty(t, prefixedKeys(mrWrite, "sr:"), "write store emptied, round %d", round)
		require.Empty(t, prefixedKeys(mrRead, "sr:"), "read store emptied — it used to accumulate, round %d", round)
		require.True(t, mrWrite.Exists("othertenant:key"), "control: the purge is prefix-scoped on the write store")
		require.True(t, mrRead.Exists("othertenant:key"), "control: the purge is prefix-scoped on the read store")
	}
}

func prefixedKeys(mr *miniredis.Miniredis, prefix string) []string {
	var keys []string
	for _, k := range mr.Keys() {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys
}

// replicaEndpoint stands in for a reader endpoint that is a replica: it
// answers ROLE and INFO replication as configured (or lets miniredis refuse
// them, which is what a backend that supports neither does), counts the SCANs
// that reach it, and answers a write command the way a replica does, while
// SCAN still works because replicas serve reads.
type replicaEndpoint struct {
	roleReply []interface{} // nil: ROLE is not supported
	infoReply string        // "": INFO replication is not supported
	writable  bool          // true: unlinks pass through (a master, not a replica)
	scans     atomic.Int32
}

// replicaError satisfies redis.Error, which is what redis.HasErrorPrefix
// matches on — a plain errors.New would not be recognised as a server reply.
type replicaError string

func (e replicaError) Error() string { return string(e) }
func (replicaError) RedisError()     {}

// readOnlyReply is the wire form, verbatim, as a valkey 7.2 replica started
// with --replicaof answers an UNLINK; the prefix match in Purge is pinned
// against it, so it must not be shortened.
const readOnlyReply = replicaError("READONLY You can't write against a read only replica.")

func (*replicaEndpoint) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *replicaEndpoint) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		switch cmd.Name() {
		case "role":
			if roleCmd, ok := cmd.(*redis.Cmd); ok && h.roleReply != nil {
				roleCmd.SetVal(h.roleReply)
				return nil
			}
		case "info":
			if infoCmd, ok := cmd.(*redis.StringCmd); ok && h.infoReply != "" {
				infoCmd.SetVal(h.infoReply)
				return nil
			}
		case "scan":
			h.scans.Add(1)
		case "unlink":
			if !h.writable {
				cmd.SetErr(readOnlyReply)
				return readOnlyReply
			}
		}
		return next(ctx, cmd)
	}
}

func (h *replicaEndpoint) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if cmd.Name() == "unlink" && !h.writable {
				cmd.SetErr(readOnlyReply)
				return readOnlyReply
			}
		}
		return next(ctx, cmds)
	}
}

// replicaStore builds a split store whose read endpoint is the stand-in.
func replicaStore(t *testing.T, replica *replicaEndpoint) (*Store, *miniredis.Miniredis, *miniredis.Miniredis) {
	t.Helper()
	mrWrite, mrReplica := miniredis.RunT(t), miniredis.RunT(t)
	readClient := redis.NewClient(&redis.Options{Addr: mrReplica.Addr()})
	readClient.AddHook(replica)
	store, err := NewWithClients(redis.NewClient(&redis.Options{Addr: mrWrite.Addr()}), readClient, "sr")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store, mrWrite, mrReplica
}

// A read endpoint that answers neither ROLE nor INFO replication is scanned as
// before, and a genuine replica answers READONLY to the unlink. That is not a
// failed purge — the write-side purge reaches the replica through replication
// — so the reset succeeds.
func TestPurgeToleratesAReadOnlyReplica(t *testing.T) {
	replica := &replicaEndpoint{}
	store, mrWrite, mrReplica := replicaStore(t, replica)

	key := "sr:" + core.HeightKey("ETH1", "0xreplica")
	require.NoError(t, mrWrite.Set(key, "1"))
	require.NoError(t, mrReplica.Set(key, "1"))
	require.NoError(t, store.Purge(context.Background()), "a replica's READONLY is replication's business, not a failed purge")
	require.False(t, mrWrite.Exists(key))
	require.True(t, mrReplica.Exists(key), "the stand-in has no replication, so its copy stays; a real replica drops it when the write-side unlink replicates")
	require.Positive(t, replica.scans.Load(), "a reader that could not say what it is was scanned, and its READONLY tolerated at the unlink")
}

// A read endpoint that reports itself a replica is not scanned at all: every
// unlink there would answer READONLY, and SCAN walks the node's whole keyspace
// whatever the MATCH, so on a shared managed reader each reset cost one round
// trip per batch of everything stored there for no effect (review of #403).
// ROLE is asked first, INFO replication when ROLE is refused; a reader that
// calls itself a master is scanned like any separate store.
func TestPurgeDoesNotScanAReaderThatReportsItselfAReplica(t *testing.T) {
	roleReply := func(masterAddr string) []interface{} {
		host, portText, err := net.SplitHostPort(masterAddr)
		require.NoError(t, err)
		port, err := strconv.ParseInt(portText, 10, 64)
		require.NoError(t, err)
		return []interface{}{"slave", host, port, "connected", int64(12345)}
	}
	infoReply := func(masterAddr string) string {
		host, port, err := net.SplitHostPort(masterAddr)
		require.NoError(t, err)
		return "# Replication\r\nrole:slave\r\nmaster_host:" + host + "\r\nmaster_port:" + port + "\r\nmaster_link_status:up\r\n"
	}
	plant := func(t *testing.T, mrWrite, mrReplica *miniredis.Miniredis) string {
		key := "sr:" + core.HeightKey("ETH1", "0xrole")
		require.NoError(t, mrWrite.Set(key, "1"))
		require.NoError(t, mrReplica.Set(key, "1"))
		return key
	}

	t.Run("ROLE names it a replica", func(t *testing.T) {
		replica := &replicaEndpoint{}
		store, mrWrite, mrReplica := replicaStore(t, replica)
		replica.roleReply = roleReply(mrWrite.Addr())
		key := plant(t, mrWrite, mrReplica)

		require.NoError(t, store.Purge(context.Background()))
		require.False(t, mrWrite.Exists(key), "the write endpoint is purged as always")
		require.Zero(t, replica.scans.Load(), "a self-declared replica is not scanned")
		require.True(t, mrReplica.Exists(key), "the stand-in has no replication; a real replica drops it when the unlink replicates")
	})

	t.Run("INFO replication names it a replica when ROLE is refused", func(t *testing.T) {
		replica := &replicaEndpoint{}
		store, mrWrite, mrReplica := replicaStore(t, replica)
		replica.infoReply = infoReply(mrWrite.Addr())
		key := plant(t, mrWrite, mrReplica)

		require.NoError(t, store.Purge(context.Background()))
		require.False(t, mrWrite.Exists(key))
		require.Zero(t, replica.scans.Load(), "the INFO answer is enough to skip the scan")
	})

	t.Run("ROLE names it a master", func(t *testing.T) {
		replica := &replicaEndpoint{roleReply: []interface{}{"master", int64(0), []interface{}{}}, writable: true}
		store, mrWrite, mrRead := replicaStore(t, replica)
		key := plant(t, mrWrite, mrRead)

		require.NoError(t, store.Purge(context.Background()))
		require.Positive(t, replica.scans.Load(), "a separate master is a store the write endpoint never feeds: scanned and emptied")
		require.False(t, mrRead.Exists(key), "the MAG-3673 shape still empties the read store")
	})
}

func TestReplicaOfParsesRoleAndInfoReplies(t *testing.T) {
	master, isReplica := replicaOfFromRole([]interface{}{"slave", "10.0.0.5", int64(6379), "connected", int64(99)})
	require.True(t, isReplica)
	require.Equal(t, "10.0.0.5:6379", master)

	master, isReplica = replicaOfFromRole([]interface{}{"master", int64(3129659), []interface{}{}})
	require.False(t, isReplica)
	require.Empty(t, master)

	_, isReplica = replicaOfFromRole([]interface{}{"sentinel", []interface{}{"mymaster"}})
	require.False(t, isReplica, "a sentinel is not a replica")

	_, isReplica = replicaOfFromRole(nil)
	require.False(t, isReplica, "an empty reply is not a verdict")

	master, isReplica = replicaOfFromRole([]interface{}{"slave"})
	require.True(t, isReplica, "a replica that did not name its master is still a replica")
	require.Empty(t, master)

	master, isReplica = replicaOfFromInfo("# Replication\r\nrole:slave\r\nmaster_host:primary.internal\r\nmaster_port:6380\r\nmaster_link_status:up\r\n")
	require.True(t, isReplica)
	require.Equal(t, "primary.internal:6380", master)

	_, isReplica = replicaOfFromInfo("# Replication\r\nrole:master\r\nconnected_slaves:1\r\n")
	require.False(t, isReplica)

	_, isReplica = replicaOfFromInfo("")
	require.False(t, isReplica)

	require.True(t, addressListed("10.0.0.5:6379", []string{"10.0.0.5:6379"}))
	require.False(t, addressListed("10.0.0.5:6379", []string{"primary.internal:6379"}), "a hostname and the IP it resolves to do not match textually: the attribute is a hint")
}

// A read store that cannot be reached is a failed purge, reported as such and
// naming the read side: a reset that silently did half the job is the defect.
func TestPurgeReportsAnUnreachableReadStore(t *testing.T) {
	mrWrite, mrRead := miniredis.RunT(t), miniredis.RunT(t)
	store, err := NewWithClients(
		redis.NewClient(&redis.Options{Addr: mrWrite.Addr()}),
		redis.NewClient(&redis.Options{Addr: mrRead.Addr()}),
		"sr")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	mrRead.Close()

	require.ErrorContains(t, store.Purge(context.Background()), "read endpoint")
}

// Probe reports each endpoint on its own; Ping keeps folding them into the
// first failure for callers that only need a verdict.
func TestProbeReportsEachEndpointSeparately(t *testing.T) {
	mrWrite, mrRead := miniredis.RunT(t), miniredis.RunT(t)
	store, err := New(Config{Addresses: []string{mrWrite.Addr()}, ReadAddresses: []string{mrRead.Addr()}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	results := store.Probe(ctx)
	require.Len(t, results, 2)
	require.Equal(t, EndpointRoleWrite, results[0].Role)
	require.Equal(t, mrWrite.Addr(), results[0].Addresses)
	require.NoError(t, results[0].Err)
	require.Equal(t, EndpointRoleRead, results[1].Role)
	require.Equal(t, mrRead.Addr(), results[1].Addresses)
	require.NoError(t, results[1].Err)
	require.NoError(t, store.Ping(ctx))

	mrRead.Close()
	results = store.Probe(ctx)
	require.NoError(t, results[0].Err, "the write half is unaffected by a read outage")
	require.Error(t, results[1].Err, "the read half is the one reported down")
	require.Error(t, store.Ping(ctx), "Ping still reports the first failure")

	single, err := New(Config{Addresses: []string{mrWrite.Addr()}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = single.Close() })
	require.Len(t, single.Probe(ctx), 1, "with no read split there is one endpoint to report")
}

func TestChainTipNotApplicableWhenMissing(t *testing.T) {
	store, _ := newTestStore(t)
	block, fresh, err := store.GetChainTip(context.Background(), core.ChainTipKey("NOWHERE"))
	require.NoError(t, err)
	require.False(t, fresh)
	require.Equal(t, spectypes.NOT_APPLICABLE, block)
}

// The endpoint tracker must name the node actually dialled — the whole point is
// that configuration cannot answer this under sentinel or cluster.
func TestReadEndpointNamesTheDialledNode(t *testing.T) {
	mr := miniredis.RunT(t)
	store, err := New(Config{Addresses: []string{mr.Addr()}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	require.Empty(t, store.ReadEndpoint(), "nothing dialled yet")
	require.NoError(t, store.SetHeight(context.Background(), core.HeightKey("ETH1", "0xa"), 7, time.Minute))
	require.Equal(t, mr.Addr(), store.ReadEndpoint(), "reports the address the client connected to")
}

// With reads split off, the header must name the READ endpoint — that is the
// node that served the hit.
func TestReadEndpointPrefersTheReadClient(t *testing.T) {
	mrWrite, mrRead := miniredis.RunT(t), miniredis.RunT(t)
	store, err := New(Config{
		Addresses:     []string{mrWrite.Addr()},
		ReadAddresses: []string{mrRead.Addr()},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	require.NoError(t, store.SetHeight(ctx, core.HeightKey("ETH1", "0xb"), 7, time.Minute))
	_, _, err = store.GetHeight(ctx, core.HeightKey("ETH1", "0xb"))
	require.NoError(t, err)
	require.Equal(t, mrRead.Addr(), store.ReadEndpoint())
}

// MAG-3671: the ticket's trap (sentinel addresses and a master-name with the
// topology line forgotten) was indistinguishable at runtime from a
// configuration with no master-name at all. The startup line now names the
// resolved topology, but it scrolls away; GET /debug/cache-state renders
// ConfiguredEndpoints as the tier's address, so the running router can be
// asked which client it built. The topology is always the resolved one.
func TestConfiguredEndpointsNameTheTopology(t *testing.T) {
	standalone, err := New(Config{Addresses: []string{"cache.internal:6379"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = standalone.Close() })
	require.Equal(t, "cache.internal:6379 topology=standalone prefix=sr", standalone.ConfiguredEndpoints(),
		"an omitted topology is reported as the standalone it resolved to, and a standalone has no master")

	sentinel, err := New(Config{Topology: TopologySentinel, MasterName: "mymaster", Addresses: []string{"s1:26379", "s2:26379"}, ReadAddresses: []string{"reader:6379"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sentinel.Close() })
	require.Equal(t, "s1:26379,s2:26379 read=reader:6379 topology=sentinel master=mymaster prefix=sr", sentinel.ConfiguredEndpoints(),
		"under sentinel the addresses are the quorum and the master set name says what it is asked for")

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	injected, err := NewWithClient(client, "srtest")
	require.NoError(t, err)
	t.Cleanup(func() { _ = injected.Close() })
	require.NotContains(t, injected.ConfiguredEndpoints(), "topology=",
		"the NewWithClient seam sees no Config, so it does not guess a topology")
}

// MAG-3684: a deployment that skips certificate verification must be
// distinguishable from one that does not wherever the configuration is
// reported. GET /debug/cache-state renders ConfiguredEndpoints as the tier's
// address, so the flag is carried there.
func TestConfiguredEndpointsNameInsecureTLS(t *testing.T) {
	insecure, err := New(Config{Addresses: []string{"cache.internal:6379"}, TLS: TLSConfig{Enabled: true, InsecureSkipVerify: true}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = insecure.Close() })
	require.Equal(t, "cache.internal:6379 topology=standalone prefix=sr tls=insecure-skip-verify", insecure.ConfiguredEndpoints())

	verified, err := New(Config{Addresses: []string{"cache.internal:6379"}, TLS: TLSConfig{Enabled: true, ServerName: "cache.internal"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = verified.Close() })
	require.NotContains(t, verified.ConfiguredEndpoints(), "insecure", "a verifying connection carries no such marker")
}
