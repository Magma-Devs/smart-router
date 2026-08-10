package chaintracker

import "errors"

var ( // Consumer Side Errors
	InvalidConfigErrorBlocksToSave  = errors.New("blocks to save wasn't defined in config")
	InvalidConfigBlockTime          = errors.New("average block time wasn't defined in config")
	InvalidLatestBlockNumValue      = errors.New("returned latest block num should be greater than 0, but it's not")
	InvalidReturnedHashes           = errors.New("returned requestedHashes key count should be greater than 0, but it's not")
	ErrorFailedToFetchLatestBlock   = errors.New("Failed to fetch latest block from node")
	InvalidRequestedBlocks          = errors.New("provided requested blocks for function do not compose a valid request")
	RequestedBlocksOutOfRange       = errors.New("requested blocks are outside the supported range by the state tracker")
	ErrorFailedToFetchTooEarlyBlock = errors.New("server memory protection triggered, requested block is too early")
	InvalidRequestedSpecificBlock   = errors.New("provided requested specific blocks for function do not compose a stored entry")
	// ErrorPollNowNotDelivered means a PollNow request was never taken by the poll goroutine before
	// the caller's context expired — the tracker is still starting, is retrying its init, or has
	// stopped. No poll ran and no observation was written (MAG-2649).
	ErrorPollNowNotDelivered = errors.New("poll-now was not picked up by the poll loop")
	// ErrorPollNowUnsupported means the tracker cannot poll at all (DummyChainTracker). Returned
	// instead of a silent nil so a /debug/poll-now caller never mistakes "nothing polls here" for a
	// completed poll and reads a stale record as fresh (MAG-2649).
	ErrorPollNowUnsupported = errors.New("this chain tracker does not poll, poll-now is unsupported")
	// ErrorPollNowResultNotAwaited means the poll goroutine TOOK the request but the caller's
	// context expired before the cycle finished recording. The poll itself is unaffected — it runs
	// to completion and writes its observation — but this caller never saw that happen, so any
	// record it reads back predates the poll. Distinct from ErrorPollNowNotDelivered on purpose:
	// "a poll is running and I cannot tell you what it recorded" is a different claim from "no poll
	// ran", and only a sentinel makes the two distinguishable to errors.Is. Both must map to
	// "do not trust the record" at the debug boundary (MAG-2649).
	ErrorPollNowResultNotAwaited = errors.New("poll-now started but its result was not awaited")
)
