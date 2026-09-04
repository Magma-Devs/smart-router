package core

import (
	"context"
	"sync"
	"testing"
	"time"

	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// fakeStore is a synchronous in-memory KVStore recording every write with its
// TTL, so tests can assert key routing and TTL selection exactly.
type fakeStore struct {
	mu       sync.Mutex
	entries  map[string]*Envelope
	ttls     map[string]time.Duration
	int64s   map[string]int64
	tipBlock int64
	tipFresh bool
	tipSets  []int64
	heights  map[string]int64

	// heightCalls counts round trips to the height surface: one per GetHeights
	// call regardless of key count, so a test can tell a batched lookup from a
	// per-hash loop.
	heightCalls int

	// stickyPins backs the first-writer-wins claim surface. stickyTTLs records the TTL each
	// claim was stored with, so a test can assert the engine clamped it.
	stickyPins map[string]StickyPin
	stickyTTLs map[string]time.Duration
	// stickyErr, when set, makes both sticky operations fail — the "cannot determine the
	// claim" path that callers must not mistake for "unclaimed".
	stickyErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		entries:    map[string]*Envelope{},
		ttls:       map[string]time.Duration{},
		int64s:     map[string]int64{},
		stickyPins: map[string]StickyPin{},
		stickyTTLs: map[string]time.Duration{},
		heights:    map[string]int64{},
	}
}

func (f *fakeStore) GetEntries(ctx context.Context, keys []string) ([]*Envelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*Envelope, len(keys))
	for i, k := range keys {
		out[i] = f.entries[k]
	}
	return out, nil
}

func (f *fakeStore) SetEntry(ctx context.Context, key string, env *Envelope, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[key] = env
	f.ttls[key] = ttl
	return nil
}

func (f *fakeStore) GetSticky(ctx context.Context, key string) (StickyPin, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stickyErr != nil {
		return StickyPin{}, false, f.stickyErr
	}
	pin, ok := f.stickyPins[key]
	return pin, ok, nil
}

func (f *fakeStore) SetStickyIfAbsent(ctx context.Context, key string, pin StickyPin, ttl time.Duration) (StickyPin, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stickyErr != nil {
		return StickyPin{}, f.stickyErr
	}
	if existing, ok := f.stickyPins[key]; ok {
		return existing, nil
	}
	f.stickyPins[key] = pin
	f.stickyTTLs[key] = ttl
	return pin, nil
}

func (f *fakeStore) GetInt64(ctx context.Context, key string) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.int64s[key]
	return v, ok, nil
}

func (f *fakeStore) SetInt64IfGreaterOrEqual(ctx context.Context, key string, value int64, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.int64s[key]; !ok || value >= existing {
		f.int64s[key] = value
		f.ttls[key] = ttl
	}
	return nil
}

func (f *fakeStore) GetChainTip(ctx context.Context, key string) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.tipFresh {
		return spectypes.NOT_APPLICABLE, false, nil
	}
	return f.tipBlock, true, nil
}

func (f *fakeStore) SetChainTipIfGreaterOrEqual(ctx context.Context, key string, block int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tipSets = append(f.tipSets, block)
	return nil
}

func (f *fakeStore) GetHeight(ctx context.Context, key string) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heightCalls++
	v, ok := f.heights[key]
	return v, ok, nil
}

func (f *fakeStore) GetHeights(ctx context.Context, keys []string) ([]int64, []bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heightCalls++
	heights := make([]int64, len(keys))
	found := make([]bool, len(keys))
	for i, key := range keys {
		heights[i], found[i] = f.heights[key]
	}
	return heights, found, nil
}

func (f *fakeStore) SetHeight(ctx context.Context, key string, height int64, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heights[key] = height
	f.ttls[key] = ttl
	return nil
}

func (f *fakeStore) Purge(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = map[string]*Envelope{}
	f.int64s = map[string]int64{}
	f.heights = map[string]int64{}
	return nil
}

func testEngine(store KVStore) *Engine {
	return &Engine{
		Store: store,
		Policy: Policy{
			Finalized:             time.Hour,
			NonFinalized:          500 * time.Millisecond,
			NodeErrors:            250 * time.Millisecond,
			BlocksHashesToHeights: 48 * time.Hour,
		},
	}
}

// ---------------------------------------------------------------------------
// Key formatting
// ---------------------------------------------------------------------------

func TestKeyFormats(t *testing.T) {
	hash := []byte{0xab, 0xcd}

	require.Equal(t, "rel:f:ETH1:abcd:100", RelayKey(true, "ETH1", hash, 100))
	require.Equal(t, "rel:t:ETH1:abcd:100", RelayKey(false, "ETH1", hash, 100))

	require.Equal(t, [2]string{"rel:f:ETH1:abcd:100", "rel:t:ETH1:abcd:100"},
		RelayLookupKeys(true, "ETH1", hash, 100), "finalized request prefers the finalized namespace")
	require.Equal(t, [2]string{"rel:t:ETH1:abcd:100", "rel:f:ETH1:abcd:100"},
		RelayLookupKeys(false, "ETH1", hash, 100), "non-finalized request prefers the temp namespace")

	require.Equal(t, "tip:ETH1:ETH1jsonrpc", SharedTipKey("ETH1", "ETH1jsonrpc"))
	require.Equal(t, "chaintip:ETH1", ChainTipKey("ETH1"))
	require.Equal(t, "h2h:ETH1:0xdeadbeef", HeightKey("ETH1", "0xdeadbeef"))
}

// ---------------------------------------------------------------------------
// TTL selection
// ---------------------------------------------------------------------------

func TestPolicyForRelayEntry(t *testing.T) {
	p := Policy{Finalized: time.Hour, NonFinalized: 500 * time.Millisecond, NodeErrors: 250 * time.Millisecond}

	require.Equal(t, 100*time.Millisecond, p.ForRelayEntry(true, true, 100*time.Millisecond, nil),
		"finalized node error: average block time wins when below the node-error cap")
	require.Equal(t, 250*time.Millisecond, p.ForRelayEntry(true, true, time.Second, nil),
		"finalized node error: capped at the node-error expiration")
	require.Equal(t, time.Hour, p.ForRelayEntry(true, false, time.Second, nil),
		"finalized success: long finalized TTL")
	require.Equal(t, time.Hour, p.ForRelayEntry(false, false, time.Second, []byte("bh")),
		"non-finalized with a block hash is hash-validated on read and lives like finalized")
	require.Equal(t, 2*time.Second, p.ForRelayEntry(false, false, 16*time.Second, nil),
		"non-finalized: an eighth of the block time when above the floor")
	require.Equal(t, 500*time.Millisecond, p.ForRelayEntry(false, false, time.Second, nil),
		"non-finalized: floored at the configured non-finalized expiration")

	// A spec that omits average_block_time passes 0 here. min(0, NodeErrors) is
	// 0, and a non-positive TTL means "never expires" in both adapters — one
	// such write leaves a key no volatile-* maxmemory policy can ever evict,
	// defeating the all-keys-volatile property the backend is built around.
	require.Equal(t, 250*time.Millisecond, p.ForRelayEntry(true, true, 0, nil),
		"finalized node error with no average block time must fall back to the node-error cap, never to a permanent key")
	require.Equal(t, 250*time.Millisecond, p.ForRelayEntry(true, true, -time.Second, nil),
		"a negative average block time is the same hazard")
	require.Positive(t, p.ForRelayEntry(true, true, 0, nil),
		"no input may make a relay entry permanent")
}

// Block-hash lookups must cost ONE store round trip, not one per hash.
//
// This runs inside the caller's per-relay cache budget (common.CacheTimeout,
// 50ms). Over a remote backend a per-hash loop is one network round trip each,
// and a request carrying several hashes can spend the whole budget resolving
// them — turning a warm cache into a miss on exactly the multi-region
// deployments the RESP backend exists to serve. An adapter cannot batch across
// separate GetHeight calls, so the engine has to hand it the whole key set.
func TestGetRelayBatchesBlockHashLookups(t *testing.T) {
	store := newFakeStore()
	ctx := context.Background()
	requestHash := []byte{0x01}
	seedEntry(store, false, "ETH1", requestHash, 100, 100, []byte(`ok`))

	hashes := []string{"0xaa", "0xbb", "0xcc", "0xdd"}
	for i, hash := range hashes {
		require.NoError(t, store.SetHeight(ctx, HeightKey("ETH1", hash), int64(100+i), time.Minute))
	}
	// One the store has never seen: misses must stay index-aligned with hits.
	requested := append(append([]string{}, hashes...), "0xmissing")

	req := getReq("ETH1", requestHash, 100, 100, false)
	req.BlocksHashesToHeights = make([]*relaytypes.BlockHashToHeight, len(requested))
	for i, hash := range requested {
		req.BlocksHashesToHeights[i] = &relaytypes.BlockHashToHeight{Hash: hash}
	}

	store.mu.Lock()
	store.heightCalls = 0
	store.mu.Unlock()

	reply, _, err := testEngine(store).GetRelay(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, reply)

	store.mu.Lock()
	calls := store.heightCalls
	store.mu.Unlock()
	require.Equal(t, 1, calls,
		"%d hashes must cost one store round trip; %d means the engine is looping per hash inside a 50ms budget",
		len(requested), calls)

	for i, resolved := range reply.BlocksHashesToHeights {
		if i < len(hashes) {
			require.Equal(t, int64(100+i), resolved.Height, "hash %s resolved to the wrong height", resolved.Hash)
		} else {
			require.Equal(t, spectypes.NOT_APPLICABLE, resolved.Height, "an unknown hash must read as NOT_APPLICABLE")
		}
	}
}

// ---------------------------------------------------------------------------
// Seen-block validity (the rule the server enforces on every hit)
// ---------------------------------------------------------------------------

func seedEntry(store *fakeStore, finalized bool, chainId string, hash []byte, block, seenBlock int64, data []byte) {
	env := NewEnvelope(&relaytypes.RelayReply{Data: data}, nil, finalized, nil, seenBlock, false, 0)
	_ = store.SetEntry(context.Background(), RelayKey(finalized, chainId, hash, block), &env, time.Minute)
}

func getReq(chainId string, hash []byte, block, seenBlock int64, finalized bool) *relaytypes.RelayCacheGet {
	return &relaytypes.RelayCacheGet{
		RequestHash:    hash,
		ChainId:        chainId,
		RequestedBlock: block,
		SeenBlock:      seenBlock,
		Finalized:      finalized,
	}
}

func TestGetRelaySeenBlockValidity(t *testing.T) {
	hash := []byte{0x01}

	t.Run("stored seen block meeting expectations serves", func(t *testing.T) {
		store := newFakeStore()
		seedEntry(store, false, "LAV1", hash, 100, 100, []byte(`ok`))
		reply, hit, err := testEngine(store).GetRelay(context.Background(), getReq("LAV1", hash, 100, 100, false))
		require.NoError(t, err)
		require.True(t, hit)
		require.NotNil(t, reply.Reply)
		require.Equal(t, []byte(`ok`), reply.Reply.Data)
	})

	t.Run("stale stored seen block is rejected but still counts as a found entry", func(t *testing.T) {
		store := newFakeStore()
		seedEntry(store, false, "LAV1", hash, 100, 99, []byte(`stale`))
		reply, hit, err := testEngine(store).GetRelay(context.Background(), getReq("LAV1", hash, 100, 100, false))
		require.Error(t, err)
		require.Nil(t, reply.Reply, "rejected entry must not be served")
		require.True(t, hit, "the entry was found before rejection — server-side accounting counts it as a hit")
	})

	t.Run("caller with a lower seen block is served by an older entry", func(t *testing.T) {
		store := newFakeStore()
		seedEntry(store, false, "LAV1", hash, 100, 99, []byte(`old-but-valid`))
		reply, _, err := testEngine(store).GetRelay(context.Background(), getReq("LAV1", hash, 100, 50, false))
		require.NoError(t, err)
		require.NotNil(t, reply.Reply)
	})

	t.Run("adopted shared-state tip raises expectations and rejects", func(t *testing.T) {
		store := newFakeStore()
		seedEntry(store, false, "LAV1", hash, 100, 99, []byte(`stale`))
		store.int64s[SharedTipKey("LAV1", "fleet")] = 100

		req := getReq("LAV1", hash, 100, 50, false)
		req.SharedStateId = "fleet"
		reply, _, err := testEngine(store).GetRelay(context.Background(), req)
		require.Error(t, err, "the fleet's seen block, not the caller's, sets the validity floor")
		require.Nil(t, reply.Reply)
		require.Equal(t, int64(100), reply.SeenBlock, "the adopted tip still flows back to the caller")
	})

	t.Run("reply seen block is lifted to the caller's expectation", func(t *testing.T) {
		store := newFakeStore()
		seedEntry(store, false, "LAV1", hash, 100, 150, []byte(`fresh`))
		store.int64s[SharedTipKey("LAV1", "fleet")] = 200

		req := getReq("LAV1", hash, 100, 100, false)
		req.SharedStateId = "fleet"
		reply, _, err := testEngine(store).GetRelay(context.Background(), req)
		require.NoError(t, err, "min(seen, requested) = 100 <= 150: entry is valid")
		require.NotNil(t, reply.Reply)
		require.Equal(t, int64(200), reply.SeenBlock, "reply carries the max of entry and adopted seen block")
	})
}

// ---------------------------------------------------------------------------
// Hash validation
// ---------------------------------------------------------------------------

func TestGetRelayHashValidation(t *testing.T) {
	hash := []byte{0x02}
	blockHash := []byte("0xblockhash")

	seedWithBlockHash := func(store *fakeStore) {
		env := NewEnvelope(&relaytypes.RelayReply{Data: []byte(`hashed`)}, blockHash, false, nil, 100, false, 0)
		_ = store.SetEntry(context.Background(), RelayKey(false, "LAV1", hash, 100), &env, time.Minute)
	}

	t.Run("matching block hash serves", func(t *testing.T) {
		store := newFakeStore()
		seedWithBlockHash(store)
		req := getReq("LAV1", hash, 100, 100, false)
		req.BlockHash = blockHash
		reply, hit, err := testEngine(store).GetRelay(context.Background(), req)
		require.NoError(t, err)
		require.True(t, hit)
		require.Equal(t, []byte(`hashed`), reply.Reply.Data)
	})

	t.Run("mismatching block hash is a miss with no fallback to the other variant", func(t *testing.T) {
		store := newFakeStore()
		seedWithBlockHash(store)
		// A finalized variant also exists — but validation applies to the first
		// entry found in precedence order; there is no second candidate.
		seedEntry(store, true, "LAV1", hash, 100, 100, []byte(`finalized`))

		req := getReq("LAV1", hash, 100, 100, false)
		req.BlockHash = []byte("0xother")
		reply, hit, err := testEngine(store).GetRelay(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, reply.Reply)
		require.False(t, hit)
	})

	t.Run("finalized entry with nil hash serves without hash validation", func(t *testing.T) {
		store := newFakeStore()
		seedEntry(store, true, "LAV1", hash, 100, 100, []byte(`finalized`))
		req := getReq("LAV1", hash, 100, 100, true)
		req.BlockHash = []byte("0xwhatever")
		reply, _, err := testEngine(store).GetRelay(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, []byte(`finalized`), reply.Reply.Data)
	})
}

// ---------------------------------------------------------------------------
// LATEST resolution through the chain tip
// ---------------------------------------------------------------------------

func TestGetRelayLatestResolution(t *testing.T) {
	hash := []byte{0x03}

	t.Run("fresh chain tip resolves LATEST to a concrete key", func(t *testing.T) {
		store := newFakeStore()
		store.tipBlock, store.tipFresh = 100, true
		seedEntry(store, false, "LAV1", hash, 100, 100, []byte(`latest`))

		req := getReq("LAV1", hash, spectypes.LATEST_BLOCK, 100, false)
		reply, hit, err := testEngine(store).GetRelay(context.Background(), req)
		require.NoError(t, err)
		require.True(t, hit)
		require.Equal(t, []byte(`latest`), reply.Reply.Data)
		require.Equal(t, int64(100), req.RequestedBlock, "the request is resolved in place")
	})

	t.Run("unknown or stale tip degrades LATEST to a miss", func(t *testing.T) {
		store := newFakeStore()
		store.tipFresh = false
		seedEntry(store, false, "LAV1", hash, 100, 100, []byte(`latest`))
		store.heights[HeightKey("LAV1", "0xh")] = 42

		req := getReq("LAV1", hash, spectypes.LATEST_BLOCK, 100, false)
		req.BlocksHashesToHeights = []*relaytypes.BlockHashToHeight{{Hash: "0xh"}}
		reply, hit, err := testEngine(store).GetRelay(context.Background(), req)
		require.Error(t, err)
		require.False(t, hit)
		require.Nil(t, reply.Reply)
		require.Equal(t, int64(42), reply.BlocksHashesToHeights[0].Height,
			"block-hash→height data still flows back on the invalid-block path")
	})
}

// ---------------------------------------------------------------------------
// SetRelay write-side bookkeeping
// ---------------------------------------------------------------------------

func TestSetRelayWrites(t *testing.T) {
	store := newFakeStore()
	engine := testEngine(store)
	hash := []byte{0x04}

	err := engine.SetRelay(context.Background(), &relaytypes.RelayCacheSet{
		RequestHash:      hash,
		ChainId:          "LAV1",
		RequestedBlock:   100,
		SeenBlock:        90,
		SharedStateId:    "fleet",
		Finalized:        false,
		AverageBlockTime: int64(16 * time.Second),
		Response:         &relaytypes.RelayReply{Data: []byte(`stored`), LatestBlock: 100},
		BlocksHashesToHeights: []*relaytypes.BlockHashToHeight{
			{Hash: "0xh", Height: 100},
			{Hash: "0xunknown", Height: spectypes.NOT_APPLICABLE},
		},
	})
	require.NoError(t, err)

	entryKey := RelayKey(false, "LAV1", hash, 100)
	require.NotNil(t, store.entries[entryKey], "entry lands in the temp namespace")
	require.Equal(t, 2*time.Second, store.ttls[entryKey], "non-finalized TTL = averageBlockTime/8")
	require.Equal(t, int64(100), store.entries[entryKey].SeenBlock, "stored seen block = max(reply latest, set seen)")

	require.Equal(t, int64(100), store.int64s[SharedTipKey("LAV1", "fleet")], "shared tip published")
	require.Equal(t, 160*time.Second, store.ttls[SharedTipKey("LAV1", "fleet")], "shared tip TTL = 10x block time")
	require.Equal(t, []int64{100}, store.tipSets, "chain tip advanced")
	require.Equal(t, int64(100), store.heights[HeightKey("LAV1", "0xh")], "known height stored")
	_, unknownStored, _ := store.GetHeight(context.Background(), HeightKey("LAV1", "0xunknown"))
	require.False(t, unknownStored, "NOT_APPLICABLE heights are never stored")
}

func TestSetRelayRejectsNegativeBlock(t *testing.T) {
	store := newFakeStore()
	err := testEngine(store).SetRelay(context.Background(), &relaytypes.RelayCacheSet{
		RequestHash:    []byte{0x05},
		ChainId:        "LAV1",
		RequestedBlock: spectypes.LATEST_BLOCK,
		Response:       &relaytypes.RelayReply{Data: []byte(`x`)},
	})
	require.Error(t, err)
	require.Empty(t, store.entries, "nothing may be stored under an unresolved block")
}
