package chaintracker

import (
	"time"
)

const (
	DefaultAssumedBlockMemory      = 20
	DefaultBlockCheckpointDistance = 100
)

type ChainTrackerConfig struct {
	ForkCallback             func(block int64)                                 // a function to be called when a fork is detected
	NewLatestCallback        func(blockFrom int64, blockTo int64, hash string) // a function to be called when a new block is detected
	ConsistencyCallback      func(oldBlock int64, block int64)
	OldBlockCallback         func(latestBlockTime time.Time)
	FetchErrorCallback       func() // a function to be called when the latest-block fetch fails
	ServerAddress            string // if not empty will open up a grpc server for that address
	BlocksToSave             uint64
	AverageBlockTime         time.Duration // how often to query latest block
	ServerBlockMemory        uint64
	BlocksCheckpointDistance uint64 // this causes the chainTracker to trigger it's checkpoint every X blocks
	ChainId                  string
	ParseDirectiveEnabled    bool

	// FlatPollInterval, when > 0, switches this tracker to a FIXED flat cadence
	// (MAG-2159 / Topic B): the dedicated poll runs at exactly this interval, slowed only
	// by failure backoff. The adaptive /4, /2, /16 tiers AND the block-gap recalibration
	// are disconnected from scheduling (block-gap estimation still runs for block-time
	// consumers, it just no longer moves the timer). Per-endpoint trackers set it to
	// avgBlockTime/2 because relay harvest is the primary block signal, so the dedicated
	// poll is a sparse fallback. Left 0 (the default) preserves the legacy adaptive
	// cadence — used by the global tracker, whose readers are not yet harvest-fed.
	//
	// It is named for what it does (selects the fixed-interval scheduler), not a "floor":
	// a min-clamp that silently changed the scheduling algorithm would be a footgun.
	FlatPollInterval time.Duration
}

func (cnf *ChainTrackerConfig) validate() error {
	if cnf.BlocksToSave == 0 {
		return InvalidConfigErrorBlocksToSave
	}
	if cnf.AverageBlockTime == 0 {
		return InvalidConfigBlockTime
	}

	if cnf.ServerBlockMemory == 0 {
		cnf.ServerBlockMemory = DefaultAssumedBlockMemory
	}
	if cnf.BlocksCheckpointDistance == 0 {
		cnf.BlocksCheckpointDistance = DefaultBlockCheckpointDistance
	}
	// TODO: validate address is in the right format if not empty
	return nil
}
