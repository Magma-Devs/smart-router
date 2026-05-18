package relaycore

import (
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
)

const (
	CacheMaxCost     = 20000 // each item cost would be 1
	CacheNumCounters = 20000 // expect 2000 items
	EntryTTL         = 5 * time.Minute
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
// mu serializes write paths against ResetState. ristretto's Clear() docs warn
// it "is not an atomic operation (but that shouldn't be a problem as it's
// assumed that Set/Get calls won't be occurring until after this)" — so under
// concurrent writes, a Set enqueued into setBuf during Clear can commit after
// Clear returns and survive what was supposed to be a reset. Writes take a
// read-lock; ResetState takes the exclusive write-lock, which makes ristretto's
// precondition hold rather than racing against it.
type ConsistencyImpl struct {
	cache  *ristretto.Cache[string, any]
	specId string
	mu     sync.RWMutex
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
// exclusive lock blocks every in-flight write — including the SetLatestBlock
// call below — until Clear has run, which is what makes ristretto's
// "no concurrent Set/Get during Clear" precondition hold.
func (cc *ConsistencyImpl) SetSeenBlockFromKey(blockSeen int64, key string) {
	if cc == nil {
		return
	}
	cc.mu.RLock()
	defer cc.mu.RUnlock()
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
// the operation behave atomically — without it, a Set enqueued into setBuf
// during Clear can commit after Clear returns and survive the reset.
func (cc *ConsistencyImpl) ResetState() {
	if cc == nil || cc.cache == nil {
		return
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.cache.Clear()
}

func NewConsistency(specId string) Consistency {
	cache, err := ristretto.NewCache(&ristretto.Config[string, any]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
	if err != nil {
		utils.LavaFormatFatal("failed setting up cache for consistency", err)
	}
	return &ConsistencyImpl{cache: cache, specId: specId}
}
