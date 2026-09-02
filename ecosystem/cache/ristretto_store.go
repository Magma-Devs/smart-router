package cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

const DbValueConfirmationAttempts = 5

// LastestCacheStore is the chain-level tip representation: stored with no
// ristretto TTL, it goes stale for readers via the embedded wall-clock
// deadline while the stored block keeps fencing lower writes indefinitely.
// (Name retained from the original implementation.)
type LastestCacheStore struct {
	latestBlock          int64
	latestExpirationTime time.Time
}

func (cv *LastestCacheStore) Cost() int64 {
	return 8 + 16
}

// ristrettoStore adapts the CacheServer's three ristretto caches onto
// core.KVStore. Relay-entry keys route by namespace prefix to the
// finalized/temp stores, tip keys live in the finalized store, and heights in
// the dedicated hashes store. Ristretto writes are asynchronous and lossy (a
// Set may be dropped or delayed), which is why the monotonic int64 ops write
// through a confirm-and-retry loop.
type ristrettoStore struct {
	cs *CacheServer
}

var _ core.KVStore = ristrettoStore{}

func (r ristrettoStore) route(key string) *ristretto.Cache[string, any] {
	switch {
	case strings.HasPrefix(key, core.RelayFinalizedPrefix):
		return r.cs.finalizedCache
	case strings.HasPrefix(key, core.RelayTempPrefix):
		return r.cs.tempCache
	case strings.HasPrefix(key, core.HeightPrefix):
		return r.cs.blocksHashesToHeightsCache
	default:
		// tip: / chaintip: — shared-state and chain tips live in the finalized store.
		return r.cs.finalizedCache
	}
}

func getNonExpiredFromCache(c *ristretto.Cache[string, any], key string) (value interface{}, found bool) {
	value, found = c.Get(key)
	if found {
		return value, true
	}
	return nil, false
}

func (r ristrettoStore) GetEntries(ctx context.Context, keys []string) ([]*core.Envelope, error) {
	entries := make([]*core.Envelope, len(keys))
	for i, key := range keys {
		value, found := getNonExpiredFromCache(r.route(key), key)
		if !found {
			continue
		}
		if cacheVal, ok := value.(core.Envelope); ok {
			entries[i] = &cacheVal
			continue
		}
		utils.LavaFormatError("entry in cache was not a CacheValue", EntryTypeError, utils.Attribute{Key: "entry", Value: fmt.Sprintf("%+v", value)})
	}
	return entries, nil
}

func (r ristrettoStore) SetEntry(ctx context.Context, key string, env *core.Envelope, ttl time.Duration) error {
	r.route(key).SetWithTTL(key, *env, env.Cost(), ttl)
	return nil
}

func (r ristrettoStore) GetInt64(ctx context.Context, key string) (int64, bool, error) {
	value, found := getNonExpiredFromCache(r.route(key), key)
	if !found {
		return 0, false, nil
	}
	if cacheValue, ok := value.(int64); ok {
		return cacheValue, true, nil
	}
	utils.LavaFormatFatal("Failed converting cache result to int64", nil, utils.LogAttr("value", value))
	return 0, false, nil
}

func (r ristrettoStore) SetInt64IfGreaterOrEqual(ctx context.Context, key string, value int64, ttl time.Duration) error {
	get := func() int64 {
		existing, found, _ := r.GetInt64(ctx, key)
		if !found {
			return 0
		}
		return existing
	}
	set := func() {
		r.route(key).SetWithTTL(key, value, 0, ttl)
	}
	performInt64WriteWithValidationAndRetry(get, set, value)
	return nil
}

func (r ristrettoStore) chainTipRaw(key string) (block int64, expiration time.Time) {
	value, found := getNonExpiredFromCache(r.cs.finalizedCache, key)
	if !found {
		return spectypes.NOT_APPLICABLE, time.Time{}
	}
	if cacheValue, ok := value.(LastestCacheStore); ok {
		return cacheValue.latestBlock, cacheValue.latestExpirationTime
	}
	utils.LavaFormatError("latestBlock value is not a LastestCacheStore", EntryTypeError, utils.Attribute{Key: "value", Value: fmt.Sprintf("%+v", value)})
	return spectypes.NOT_APPLICABLE, time.Time{}
}

func (r ristrettoStore) GetChainTip(ctx context.Context, key string) (int64, bool, error) {
	latestBlock, expirationTime := r.chainTipRaw(key)
	if latestBlock != spectypes.NOT_APPLICABLE && expirationTime.After(time.Now()) {
		return latestBlock, true, nil
	}
	return spectypes.NOT_APPLICABLE, false, nil
}

func (r ristrettoStore) SetChainTipIfGreaterOrEqual(ctx context.Context, key string, block int64) error {
	cacheStore := LastestCacheStore{latestBlock: block, latestExpirationTime: time.Now().Add(core.DefaultExpirationForNonFinalized)}
	utils.LavaFormatDebug("setting latest block", utils.Attribute{Key: "key", Value: key}, utils.Attribute{Key: "latestBlock", Value: block})
	set := func() {
		r.cs.finalizedCache.Set(key, cacheStore, cacheStore.Cost())
	}
	get := func() int64 {
		// The monotonic guard compares against the RAW stored block, stale or
		// not — a stale tip is unreadable (GetChainTip reports fresh=false) but
		// still fences lower writes.
		existingLatest, _ := r.chainTipRaw(key)
		return existingLatest
	}
	performInt64WriteWithValidationAndRetry(get, set, block)
	return nil
}

func (r ristrettoStore) GetHeight(ctx context.Context, key string) (int64, bool, error) {
	value, found := getNonExpiredFromCache(r.route(key), key)
	if !found {
		return 0, false, nil
	}
	if cacheValue, ok := value.(int64); ok {
		return cacheValue, true, nil
	}
	return 0, false, nil
}

// GetHeights reads sequentially: these are in-process map lookups, so there is
// no transport to batch over and no budget to protect.
func (r ristrettoStore) GetHeights(ctx context.Context, keys []string) ([]int64, []bool, error) {
	heights := make([]int64, len(keys))
	found := make([]bool, len(keys))
	for i, key := range keys {
		height, ok, err := r.GetHeight(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		heights[i], found[i] = height, ok
	}
	return heights, found, nil
}

func (r ristrettoStore) SetHeight(ctx context.Context, key string, height int64, ttl time.Duration) error {
	r.route(key).SetWithTTL(key, height, 1, ttl)
	return nil
}

// Purge empties every ristretto store. Ristretto's Clear() is not atomic and
// assumes no concurrent Set/Get; Wait() drains the asynchronous Set buffer so
// a Set in flight at Clear time can't survive the flush and serve a hit on
// the next Get. Nil-safe per store for partially initialised fixtures.
func (r ristrettoStore) Purge(ctx context.Context) error {
	if c := r.cs.tempCache; c != nil {
		c.Clear()
		c.Wait()
	}
	if c := r.cs.finalizedCache; c != nil {
		c.Clear()
		c.Wait()
	}
	if c := r.cs.blocksHashesToHeightsCache; c != nil {
		c.Clear()
		c.Wait()
	}
	return nil
}

// performInt64WriteWithValidationAndRetry works around ristretto's
// asynchronous, lossy writes: apply the monotonic (greater-or-equal) guard,
// write, then confirm the value actually landed and rewrite for a bounded
// number of attempts in case a concurrent drop or delay swallowed it.
func performInt64WriteWithValidationAndRetry(
	getBlockCallback func() int64,
	setBlockCallback func(),
	newInfo int64,
) {
	existingInfo := getBlockCallback()
	if existingInfo <= newInfo {
		setBlockCallback()
		go func() {
			for i := 0; i < DbValueConfirmationAttempts; i++ {
				time.Sleep(time.Millisecond)
				currentInfo := getBlockCallback()
				if currentInfo > newInfo {
					return
				}
				if currentInfo < newInfo {
					setBlockCallback()
				}
			}
		}()
	}
}
