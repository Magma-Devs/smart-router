package relaycore

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/Magma-Devs/smart-router/protocol/common"
	"github.com/Magma-Devs/smart-router/utils"
)

const (
	CacheMaxCost     = 20000 // each item cost would be 1
	CacheNumCounters = 20000 // expect 2000 items
	EntryTTL         = 5 * time.Minute
	// ResetTombstoneWindow drops SetSeenBlockFromKey writes that land within
	// this window after a ResetState. A relay response can travel through the
	// relay-processor pipeline for several milliseconds between when its
	// blockSeen was determined and when it reaches the consistency cache; if
	// /debug/reset-* fires in that window, the late-arriving write would
	// re-poison the just-cleared store. 100 ms is long enough to absorb a
	// pipeline's worth of in-flight responses and short enough that operators
	// triggering reset won't notice the brief write-blocked period.
	ResetTombstoneWindow = 100 * time.Millisecond
)

// Consistency interface for managing block consistency
type Consistency interface {
	SetSeenBlock(blockSeen int64, userData common.UserData)
	GetSeenBlock(userData common.UserData) (int64, bool)
	SetSeenBlockFromKey(blockSeen int64, key string)
	Key(userData common.UserData) string
}

// ConsistencyImpl is the default implementation of Consistency.
//
// mu, gen, and lastResetAtNano together serialize write paths against
// ResetState:
//
//   - mu (RWMutex): ristretto's Clear() docs warn it "is not an atomic operation
//     (but that shouldn't be a problem as it's assumed that Set/Get calls won't
//     be occurring until after this)." Writes take the read-lock around the
//     internal SetWithTTL so Clear runs under the exclusive write-lock with no
//     concurrent Set/Get in flight — making ristretto's precondition hold.
//
//   - gen (atomic counter): closes the queued-writer hole. A writer suspended
//     between function entry and RLock can acquire RLock after ResetState has
//     Cleared, see an empty cache (found=false), bypass the monotonic guard,
//     and re-poison the store with whatever stale blockSeen it captured before
//     the reset. Writes snapshot gen before RLock and drop the write if
//     ResetState advanced gen while they were waiting.
//
//   - lastResetAtNano (tombstone window): closes the fresh-post-reset hole.
//     A relay response can be in flight in the relay processor for several
//     milliseconds between when blockSeen was determined and when it reaches
//     this cache; if /debug/reset-* fires in that gap, the late-arriving
//     SetSeenBlockFromKey call enters this function only AFTER ResetState's
//     gen.Add — so its startGen snapshot already matches the post-reset gen
//     and the gen check passes. The tombstone drops any write whose RLock
//     acquires within ResetTombstoneWindow of the most recent ResetState.
type ConsistencyImpl struct {
	cache           *ristretto.Cache[string, any]
	specId          string
	mu              sync.RWMutex
	gen             atomic.Int64
	lastResetAtNano atomic.Int64
}

func (cc *ConsistencyImpl) SetLatestBlock(key string, block int64) {
	// we keep consistency data for 5 minutes
	// if in that time no new block was updated we will remove seen data and let providers return what they have
	cc.cache.SetWithTTL(key, block, 1, EntryTTL)
}

func (cc *ConsistencyImpl) GetLatestBlock(key string) (block int64, found bool) {
	storedVal, found := cc.cache.Get(key)
	if found {
		var ok bool
		block, ok = storedVal.(int64)
		if !ok {
			utils.LavaFormatError("failed to cast block from cache", nil,
				utils.Attribute{Key: "storedVal", Value: storedVal},
				utils.Attribute{Key: "specId", Value: cc.specId},
			)
		}
	}
	return block, found
}

func (cc *ConsistencyImpl) Key(userData common.UserData) string {
	return userData.DappId + "__" + userData.ConsumerIp
}

// used on subscription, where we already have the dapp key stored, but we don't keep the dappId and ip separately
//
// The Get-then-Set sequence is held under the read-lock so that ResetState's
// exclusive lock blocks every in-flight write until Clear has run. The
// generation snapshot before RLock catches writes queued behind ResetState's
// Lock; the tombstone check catches writes that entered the function during
// or just after Clear (relay-pipeline-delayed responses). Either condition
// drops the write to keep the cleared cache cleared.
func (cc *ConsistencyImpl) SetSeenBlockFromKey(blockSeen int64, key string) {
	if cc == nil {
		return
	}
	startGen := cc.gen.Load()
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	if cc.gen.Load() != startGen {
		// ResetState ran while we were waiting on RLock; drop this stale write.
		return
	}
	if reset := cc.lastResetAtNano.Load(); reset != 0 && time.Now().UnixNano()-reset < int64(ResetTombstoneWindow) {
		// Within the post-reset window — this write's blockSeen was likely
		// computed before the reset and arrived late through the relay pipeline.
		return
	}
	// seen block is only increasing
	if block, found := cc.GetLatestBlock(key); found && block >= blockSeen {
		return
	}
	cc.SetLatestBlock(key, blockSeen)
}

func (cc *ConsistencyImpl) SetSeenBlock(blockSeen int64, userData common.UserData) {
	if cc == nil {
		return
	}
	if userData.DappId == "" {
		return
	}
	key := cc.Key(userData)
	cc.SetSeenBlockFromKey(blockSeen, key)
}

func (cc *ConsistencyImpl) GetSeenBlock(userData common.UserData) (int64, bool) {
	if cc == nil {
		return 0, false
	}
	return cc.GetLatestBlock(cc.Key(userData))
}

// ResetState flushes every seen-block entry from the cache.
//
// Why this is necessary:
// SetSeenBlockFromKey's monotonic guard only writes when the new block is
// strictly greater than the stored one. A test that accidentally sends a
// millisecond timestamp as a block parameter (~1.7T) poisons the entry, and
// the guard then rejects every legitimate ~20M block update until the 5-minute
// TTL expires — and ongoing traffic keeps refreshing the entry, so the TTL
// never actually fires. Flushing the whole cache is the only recovery short of
// a process restart.
//
// Intended for the debug /debug/reset-* paths only; ristretto's Clear pauses
// and restarts the cache's processItems goroutine, so this is not suitable
// for the hot path.
//
// The exclusive lock blocks every concurrent SetSeenBlockFromKey for the
// duration of Clear, which is what ristretto's Clear doc requires to make
// the operation behave atomically. gen is advanced and lastResetAtNano is
// stamped before Clear so any SetSeenBlockFromKey that was waiting on RLock
// (gen check) or that arrives within ResetTombstoneWindow afterward (tombstone
// check) drops its write rather than re-poisoning the cleared store.
func (cc *ConsistencyImpl) ResetState() {
	if cc == nil || cc.cache == nil {
		return
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.gen.Add(1)
	cc.lastResetAtNano.Store(time.Now().UnixNano())
	cc.cache.Clear()
}

func NewConsistency(specId string) Consistency {
	cache, err := ristretto.NewCache(&ristretto.Config[string, any]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
	if err != nil {
		utils.LavaFormatFatal("failed setting up cache for consistency", err)
	}
	return &ConsistencyImpl{cache: cache, specId: specId}
}
