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

	// RelayTipFresh, when set, is the traffic gate (MAG-2159 / Topic B): it reports whether a
	// FRESH relay-harvested tip already covers this endpoint. When it returns true the dedicated
	// poll skips the ENTIRE cycle — no FetchLatestBlockNum and no fork-check FetchBlockHashByNum
	// — because served traffic is keeping the tip current. The gate lives here, ABOVE the
	// generic/SVM iChainFetcherWrapper split, so it suppresses both EVM and Solana polls (the
	// per-poller hook could only ever see the generic path). A skip touches nothing: no upstream
	// call, no poll-health write, and no SVM cache mutation. Per-endpoint trackers set this; the
	// global tracker leaves it nil (not harvest-fed). Bounded by MaxRelaySkipsBeforePoll.
	RelayTipFresh func(now time.Time) bool

	// MaxRelaySkipsBeforePoll bounds consecutive gate skips: after this many skipped cycles the
	// tracker forces one real poll for independent fork/liveness verification, so relay traffic
	// reporting a stable-but-wrong tip cannot suppress the dedicated poll forever. 0 selects
	// defaultMaxRelaySkipsBeforePoll. Only meaningful when RelayTipFresh is set.
	MaxRelaySkipsBeforePoll int
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
