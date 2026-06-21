package chaintracker

import (
	"context"
	"time"
)

// sleepCtx waits for d, or returns early if ctx is cancelled. Returns ctx.Err() on
// cancellation (so callers can abort promptly) and nil if the full duration elapsed.
// A non-positive duration is a no-op.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func exponentialBackoff(baseTime time.Duration, fails uint64) time.Duration {
	if fails > maxFails {
		fails = maxFails
	}
	maxIncrease := BACKOFF_MAX_TIME
	backoff := baseTime * (1 << fails)
	if backoff > maxIncrease {
		backoff = maxIncrease
	}
	return backoff
}

func FindRequestedBlockHash(requestedHashes []*BlockStore, requestBlock, toBlock, fromBlock int64, finalizedBlockHashes map[int64]interface{}) (requestedBlockHash []byte, finalizedBlockHashesMapRet map[int64]interface{}) {
	for _, block := range requestedHashes {
		if block.Block == requestBlock {
			requestedBlockHash = []byte(block.Hash)
			if int64(len(requestedHashes)) == (toBlock - fromBlock + 1) {
				finalizedBlockHashes[block.Block] = block.Hash
			}
		} else {
			finalizedBlockHashes[block.Block] = block.Hash
		}
	}
	return requestedBlockHash, finalizedBlockHashes
}
