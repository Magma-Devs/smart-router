package core

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/parser"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/lavaslices"
)

var (
	NotFoundError     = errors.New("cache entry for specific block and request wasn't found")
	HashMismatchError = errors.New("cache entry for specific block and request had a mismatching hash stored")
	EntryTypeError    = errors.New("cache entry for specific block and request had a mismatching object stored")
	// StoreError marks a failure of the underlying KVStore itself (backend
	// unreachable, timeout), as opposed to the semantic miss reasons above.
	// Joined with the raw cause so errors.Is sees both this sentinel and the
	// original chain (e.g. context.DeadlineExceeded).
	StoreError = errors.New("cache store operation failed")
)

// Engine implements the cache semantics — key derivation, finality-aware
// lookup precedence, hash validation, seen-block validity, LATEST resolution,
// shared-state tip exchange, block-hash→height bookkeeping, and TTL selection —
// over any KVStore. It is the single implementation shared by every backend;
// stores contribute representation and atomicity only.
type Engine struct {
	Store  KVStore
	Policy Policy
}

// replaceRequestedBlock maps special block constants (LATEST, SAFE, etc.) to latestBlock.
func replaceRequestedBlock(requestedBlock, latestBlock int64) int64 {
	switch requestedBlock {
	case spectypes.LATEST_BLOCK:
		return latestBlock
	case spectypes.SAFE_BLOCK:
		return latestBlock
	case spectypes.FINALIZED_BLOCK:
		return latestBlock
	case spectypes.PENDING_BLOCK:
		return latestBlock
	case spectypes.EARLIEST_BLOCK:
		return spectypes.NOT_APPLICABLE
	}
	return requestedBlock
}

// chainTip resolves the chain-level latest block, NOT_APPLICABLE when unknown
// or stale.
func (e *Engine) chainTip(ctx context.Context, chainId string) int64 {
	tip, fresh, err := e.Store.GetChainTip(ctx, ChainTipKey(chainId))
	if err != nil || !fresh {
		return spectypes.NOT_APPLICABLE
	}
	return tip
}

// GetSharedTip reads the fleet's published seen-block for a shared-state id;
// 0 when disabled, missing, or unreadable.
func (e *Engine) GetSharedTip(ctx context.Context, chainId, sharedStateId string) int64 {
	if sharedStateId == "" {
		return 0
	}
	key := SharedTipKey(chainId, sharedStateId)
	value, found, err := e.Store.GetInt64(ctx, key)
	if err != nil || !found {
		utils.LavaFormatInfo("Failed fetching state from cache for this user id", utils.LogAttr("id", key))
		return 0
	}
	utils.LavaFormatInfo("getting seen block cache", utils.LogAttr("id", key), utils.LogAttr("value", value))
	return value
}

// SetSharedTip publishes a seen-block under a shared-state id; writes are
// max-merged (greater-or-equal, so equal observations refresh the TTL).
func (e *Engine) SetSharedTip(ctx context.Context, chainId, sharedStateId string, seenBlock int64, ttl time.Duration) {
	if sharedStateId == "" {
		return
	}
	if err := e.Store.SetInt64IfGreaterOrEqual(ctx, SharedTipKey(chainId, sharedStateId), seenBlock, ttl); err != nil {
		utils.LavaFormatWarning("failed setting shared-state tip", err, utils.LogAttr("chainId", chainId))
	}
}

// getBlockHeightsFromHashes resolves every requested hash in ONE store call.
// It runs inside the caller's per-relay cache budget (common.CacheTimeout,
// 50ms), so a per-hash loop costs one round trip per hash on a remote backend
// and a request carrying several hashes can spend the whole budget here.
// Batching is the adapter's job — it cannot batch across separate GetHeight
// calls — so the engine hands it the whole key set at once.
func (e *Engine) getBlockHeightsFromHashes(ctx context.Context, chainId string, hashes []*relaytypes.BlockHashToHeight) []*relaytypes.BlockHashToHeight {
	if len(hashes) == 0 {
		return hashes
	}
	keys := make([]string, len(hashes))
	for i, hashToHeight := range hashes {
		keys[i] = HeightKey(chainId, hashToHeight.Hash)
	}

	heights, found, err := e.Store.GetHeights(ctx, keys)
	if err != nil {
		for _, hashToHeight := range hashes {
			hashToHeight.Height = spectypes.NOT_APPLICABLE
		}
		return hashes
	}
	for i, hashToHeight := range hashes {
		if found[i] {
			hashToHeight.Height = heights[i]
		} else {
			hashToHeight.Height = spectypes.NOT_APPLICABLE
		}
	}
	return hashes
}

func (e *Engine) setBlocksHashesToHeights(ctx context.Context, chainId string, blocksHashesToHeights []*relaytypes.BlockHashToHeight) {
	for _, hashToHeight := range blocksHashesToHeights {
		if hashToHeight.Height >= 0 {
			if err := e.Store.SetHeight(ctx, HeightKey(chainId, hashToHeight.Hash), hashToHeight.Height, e.Policy.BlocksHashesToHeights); err != nil {
				utils.LavaFormatWarning("failed setting block hash to height", err, utils.LogAttr("chainId", chainId))
			}
		}
	}
}

// getRelayInner is the entry lookup: both finality variants are fetched in
// precedence order and the first present one is hash-validated against the
// request. A nil stored hash serves unconditionally (finalized variant); a
// stored hash must match the request's block hash exactly.
func (e *Engine) getRelayInner(ctx context.Context, relayCacheGet *relaytypes.RelayCacheGet) (*relaytypes.CacheRelayReply, error) {
	keys := RelayLookupKeys(relayCacheGet.Finalized, relayCacheGet.ChainId, relayCacheGet.RequestHash, relayCacheGet.RequestedBlock)
	entries, err := e.Store.GetEntries(ctx, keys[:])
	if err != nil {
		return nil, errors.Join(StoreError, err)
	}
	var cacheVal *Envelope
	var cacheSource string
	for i, entry := range entries {
		if entry != nil {
			cacheVal = entry
			cacheSource = "temp_cache"
			if (i == 0) == relayCacheGet.Finalized {
				cacheSource = "finalized_cache"
			}
			break
		}
	}
	if cacheVal == nil {
		return nil, NotFoundError
	}
	if cacheVal.Hash == nil {
		utils.LavaFormatDebug("returning response",
			utils.Attribute{Key: "cache_source", Value: cacheSource},
			utils.Attribute{Key: "hash", Value: "nil"},
			utils.Attribute{Key: "response_data", Value: parser.CapStringLen(string(cacheVal.Response.Data))},
		)
		return cacheVal.ToCacheReply(), nil
	}
	if bytes.Equal(cacheVal.Hash, relayCacheGet.BlockHash) {
		utils.LavaFormatDebug("returning response",
			utils.Attribute{Key: "cache_source", Value: cacheSource},
			utils.Attribute{Key: "hash", Value: "match"},
			utils.Attribute{Key: "response_data", Value: parser.CapStringLen(string(cacheVal.Response.Data))},
		)
		return cacheVal.ToCacheReply(), nil
	}
	return nil, HashMismatchError
}

// GetRelay answers a cache lookup. The returned reply is always non-nil: on a
// miss its Reply field is nil while merged seen-block and block-hash→height
// data still flow back. hit reports whether an entry was found BEFORE the
// seen-block validity check nils the reply — a rejected entry counts as a hit
// for the server's own hit/miss accounting while the caller sees a miss —
// and err carries the miss reason for logging.
func (e *Engine) GetRelay(ctx context.Context, relayCacheGet *relaytypes.RelayCacheGet) (reply *relaytypes.CacheRelayReply, hit bool, err error) {
	cacheReply := &relaytypes.CacheRelayReply{}
	var cacheReplyTmp *relaytypes.CacheRelayReply
	var seenBlock int64

	defer func() {
		if err != nil {
			cacheReply.Reply = nil
		}
	}()

	originalRequestedBlock := relayCacheGet.RequestedBlock
	if originalRequestedBlock < 0 {
		getLatestBlock := e.chainTip(ctx, relayCacheGet.ChainId)
		relayCacheGet.RequestedBlock = replaceRequestedBlock(originalRequestedBlock, getLatestBlock)
	}

	utils.LavaFormatDebug("Got Cache Get",
		utils.Attribute{Key: "request_hash", Value: string(relayCacheGet.RequestHash)},
		utils.Attribute{Key: "finalized", Value: relayCacheGet.Finalized},
		utils.Attribute{Key: "requested_block", Value: originalRequestedBlock},
		utils.Attribute{Key: "block_hash", Value: relayCacheGet.BlockHash},
		utils.Attribute{Key: "requested_block_parsed", Value: relayCacheGet.RequestedBlock},
		utils.Attribute{Key: "seen_block", Value: relayCacheGet.SeenBlock},
	)

	var blockHashes []*relaytypes.BlockHashToHeight
	if relayCacheGet.RequestedBlock >= 0 {
		waitGroup := sync.WaitGroup{}
		waitGroup.Add(3)

		go func() {
			defer waitGroup.Done()
			cacheReplyTmp, err = e.getRelayInner(ctx, relayCacheGet)
			if cacheReplyTmp != nil {
				cacheReply = cacheReplyTmp
			}
		}()

		go func() {
			defer waitGroup.Done()
			seenBlock = e.GetSharedTip(ctx, relayCacheGet.ChainId, relayCacheGet.SharedStateId)
			if seenBlock > relayCacheGet.SeenBlock {
				relayCacheGet.SeenBlock = seenBlock
			}
		}()

		go func() {
			defer waitGroup.Done()
			blockHashes = e.getBlockHeightsFromHashes(ctx, relayCacheGet.ChainId, relayCacheGet.BlocksHashesToHeights)
		}()

		waitGroup.Wait()

		if err == nil {
			if cacheReply.SeenBlock < lavaslices.Min([]int64{relayCacheGet.SeenBlock, relayCacheGet.RequestedBlock}) {
				err = utils.LavaFormatDebug("reply seen block is smaller than our expectations",
					utils.LogAttr("cacheReply.SeenBlock", cacheReply.SeenBlock),
					utils.LogAttr("seenBlock", relayCacheGet.SeenBlock),
				)
			}
		}

		if relayCacheGet.SeenBlock > cacheReply.SeenBlock {
			cacheReply.SeenBlock = relayCacheGet.SeenBlock
		}
	} else {
		err = utils.LavaFormatDebug("Requested block is invalid",
			utils.LogAttr("requested block", relayCacheGet.RequestedBlock),
			utils.LogAttr("request_hash", string(relayCacheGet.RequestHash)),
		)
		blockHashes = e.getBlockHeightsFromHashes(ctx, relayCacheGet.ChainId, relayCacheGet.BlocksHashesToHeights)
	}

	cacheReply.BlocksHashesToHeights = blockHashes
	if blockHashes != nil {
		utils.LavaFormatDebug("block hashes:", utils.LogAttr("hashes", blockHashes))
	}

	hit = cacheReply.Reply != nil
	return cacheReply, hit, err
}

// SetRelay stores a relay entry and performs the write-side bookkeeping: the
// shared-state tip publish, the chain-level tip advance, and block-hash→height
// scalars.
func (e *Engine) SetRelay(ctx context.Context, relayCacheSet *relaytypes.RelayCacheSet) error {
	if relayCacheSet.RequestedBlock < 0 {
		return utils.LavaFormatError("invalid relay cache set data, request block is negative", nil, utils.Attribute{Key: "requestBlock", Value: relayCacheSet.RequestedBlock})
	}
	latestKnownBlock := int64(math.Max(float64(relayCacheSet.Response.LatestBlock), float64(relayCacheSet.SeenBlock)))

	cacheKey := RelayKey(relayCacheSet.Finalized, relayCacheSet.ChainId, relayCacheSet.RequestHash, relayCacheSet.RequestedBlock)
	cacheValue := NewEnvelope(relayCacheSet.Response, relayCacheSet.BlockHash, relayCacheSet.Finalized, relayCacheSet.OptionalMetadata, latestKnownBlock, relayCacheSet.IsNodeError, relayCacheSet.StatusCode)
	utils.LavaFormatDebug("Got Cache Set",
		utils.Attribute{Key: "cacheKey", Value: cacheKey},
		utils.Attribute{Key: "finalized", Value: relayCacheSet.Finalized},
		utils.Attribute{Key: "requested_block", Value: relayCacheSet.RequestedBlock},
		utils.Attribute{Key: "response_data", Value: parser.CapStringLen(string(relayCacheSet.Response.Data))},
		utils.Attribute{Key: "requestHash", Value: string(relayCacheSet.BlockHash)},
		utils.Attribute{Key: "latestKnownBlock", Value: latestKnownBlock},
		utils.Attribute{Key: "IsNodeError", Value: relayCacheSet.IsNodeError},
		utils.Attribute{Key: "BlocksHashesToHeights", Value: relayCacheSet.BlocksHashesToHeights},
	)

	ttl := e.Policy.ForRelayEntry(relayCacheSet.Finalized, relayCacheSet.IsNodeError, time.Duration(relayCacheSet.AverageBlockTime), relayCacheSet.BlockHash)
	var storeErr error
	if err := e.Store.SetEntry(ctx, cacheKey, &cacheValue, ttl); err != nil {
		utils.LavaFormatWarning("failed storing cache entry", err, utils.LogAttr("cacheKey", cacheKey))
		storeErr = errors.Join(StoreError, err)
	}

	// Tip and height bookkeeping stays best-effort even when the entry write
	// failed — and its own failures don't fail the call.
	e.SetSharedTip(ctx, relayCacheSet.ChainId, relayCacheSet.SharedStateId, latestKnownBlock, e.Policy.SharedStateTip(time.Duration(relayCacheSet.AverageBlockTime)))
	if err := e.Store.SetChainTipIfGreaterOrEqual(ctx, ChainTipKey(relayCacheSet.ChainId), latestKnownBlock); err != nil {
		utils.LavaFormatWarning("failed setting chain tip", err, utils.LogAttr("chainId", relayCacheSet.ChainId))
	}
	e.setBlocksHashesToHeights(ctx, relayCacheSet.ChainId, relayCacheSet.BlocksHashesToHeights)
	return storeErr
}

// Purge drops every entry in the underlying store.
func (e *Engine) Purge(ctx context.Context) error {
	return e.Store.Purge(ctx)
}
