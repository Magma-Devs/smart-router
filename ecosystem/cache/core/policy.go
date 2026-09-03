package core

import (
	"time"

	"github.com/magma-Devs/smart-router/utils/lavaslices"
)

const (
	DefaultExpirationForNonFinalized       = 500 * time.Millisecond
	DefaultExpirationTimeFinalized         = time.Hour
	DefaultExpirationNodeErrors            = 250 * time.Millisecond
	DefaultExpirationBlocksHashesToHeights = 48 * time.Hour
	// SharedStateTipBlockMultiplier bounds how long a pod's published chain tip lives in the
	// shared cache: SharedStateTipBlockMultiplier * averageBlockTime. It mirrors the smart
	// router's own tip-staleness horizon (chainstate.StalenessWindow) so a dead pod's tip
	// evaporates on roughly the same timescale the router considers a tip stale, rather than
	// lingering for the full finalized (~1h) TTL.
	SharedStateTipBlockMultiplier = 10
	// MinSharedStateTipExpiration is the absolute floor for that TTL, mirroring the router-side
	// floor (chainstate.minStalenessWindow) rather than importing it — ecosystem/cache is a
	// standalone binary and must not depend on protocol/chainstate. It is a hard-coded constant,
	// NOT an operator-tunable duration, because it underwrites two guarantees the tunables cannot:
	//   - non-zero: ristretto treats a zero TTL as "never expires", so a dead pod's tip would
	//     linger forever. ExpirationNonFinalized alone cannot prevent this — it is
	//     flag * multiplier, both operator-settable, and either being 0 makes it 0.
	//   - long enough to be useful: a chain tip must outlive the gap between requests. At the
	//     500ms ExpirationNonFinalized default a tip published for a spec with no
	//     average_block_time would evaporate between relays, silently disabling shared state.
	MinSharedStateTipExpiration = 2 * time.Second
)

// DefaultPolicy mirrors the cache server's flag defaults — the TTL table a
// router-embedded backend uses unless configured otherwise.
func DefaultPolicy() Policy {
	return Policy{
		Finalized:             DefaultExpirationTimeFinalized,
		NonFinalized:          DefaultExpirationForNonFinalized,
		NodeErrors:            DefaultExpirationNodeErrors,
		BlocksHashesToHeights: DefaultExpirationBlocksHashesToHeights,
	}
}

// Policy is the TTL table for every write the engine performs, carried as
// plain values so a caller can rebuild it from live configuration per call.
type Policy struct {
	Finalized             time.Duration
	NonFinalized          time.Duration
	NodeErrors            time.Duration
	BlocksHashesToHeights time.Duration
}

// ForChain is the non-finalized entry TTL: an eighth of the chain's average
// block time, floored at the configured non-finalized expiration.
func (p Policy) ForChain(averageBlockTimeForChain time.Duration) time.Duration {
	eighthBlock := averageBlockTimeForChain / 8
	return lavaslices.Max([]time.Duration{eighthBlock, p.NonFinalized})
}

// SharedStateTip returns the TTL for a pod's published chain tip in shared-state
// mode: SharedStateTipBlockMultiplier * averageBlockTime, floored at NonFinalized and
// then at the hard MinSharedStateTipExpiration.
//
// The second floor is the load-bearing one. NonFinalized is derived from two operator
// flags (duration * multiplier), so it is neither guaranteed non-zero — a zero TTL means "never
// expires" in ristretto, so a dead pod's tip would linger forever — nor guaranteed long enough:
// at its 500ms default a tip for a spec that omits average_block_time (0 * multiplier = 0) would
// evaporate between relays and silently disable shared state. MinSharedStateTipExpiration is a
// constant precisely so no flag combination can defeat either guarantee.
func (p Policy) SharedStateTip(averageBlockTimeForChain time.Duration) time.Duration {
	ttl := averageBlockTimeForChain * SharedStateTipBlockMultiplier
	return lavaslices.Max([]time.Duration{ttl, p.NonFinalized, MinSharedStateTipExpiration})
}

// ForRelayEntry selects the TTL for a relay entry write: finalized node errors
// get min(averageBlockTime, NodeErrors) so a transient upstream error cannot
// outlive roughly one block; finalized successes get the long Finalized TTL; a
// non-finalized entry carrying a block hash is hash-validated on read and may
// live as long as a finalized one; everything else gets the chain-scaled
// non-finalized TTL.
func (p Policy) ForRelayEntry(finalized, isNodeError bool, averageBlockTime time.Duration, blockHash []byte) time.Duration {
	if finalized {
		if isNodeError {
			// A spec that omits average_block_time passes 0 here, and min(0, x)
			// is 0 — which both adapters read as "never expires" (ristretto by
			// its SetWithTTL contract, the RESP store by mapping a non-positive
			// TTL to a plain SET). One such write leaves a permanent key that no
			// volatile-* maxmemory policy can ever evict, defeating the
			// all-keys-volatile property chainTipRetention was bounded to
			// preserve. The flat NodeErrors bound is the floor.
			//
			// Fixed here rather than in the adapters because the same
			// non-positive-means-permanent semantics appear in two places per
			// adapter (SetEntry and the int64 CAS script), and the policy is the
			// single point that governs both.
			if averageBlockTime <= 0 {
				return p.NodeErrors
			}
			return lavaslices.Min([]time.Duration{averageBlockTime, p.NodeErrors})
		}
		return p.Finalized
	}
	if blockHash != nil {
		return p.Finalized
	}
	return p.ForChain(averageBlockTime)
}
