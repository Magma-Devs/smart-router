package endpointstate

// EnableForkDetectionFlagName is the operator switch for block-hash polling.
//
// The per-endpoint ChainTracker's only use for block hashes is fork detection: on every
// poll it fetches the head's hash to compare against the one it stored, and on a new head
// it walks backwards filling a block queue. Measured, that is roughly a third of all
// upstream requests on Solana and two thirds on EVM chains — and nothing in this router
// consumes the result. The fork callback writes a log line, and the hash-returning
// GetLatestBlockData has no caller (finalization proofs / data reliability are not wired
// in this fork).
//
// So the work is off by default and this flag turns it back on. The latest-block poll is
// unaffected either way: it is what feeds the endpointtip store, ChainState consensus and
// consistency pre-validation, and it runs before any hash work is even considered.
const EnableForkDetectionFlagName = "enable-fork-detection"

// HashPollingReason explains, for one tracker, whether block-hash polling is running and
// why. It exists because two different causes produce the same behaviour and an operator
// debugging a live endpoint has to be able to tell them apart:
//
//   - the chain CANNOT do hashes (no GET_BLOCK_BY_NUM in its spec — Canton, MAG-2218).
//     Turning the flag on would not change anything.
//   - the operator CHOSE not to do hashes (this flag). Turning the flag on WOULD change it.
//
// Surfaced as HashPolling on /debug/endpoint-state.
type HashPollingReason string

const (
	// HashPollingOn — fork detection is running for this tracker.
	HashPollingOn HashPollingReason = "on"

	// HashPollingOffSpecUnsupported — the chain spec declares no usable GET_BLOCK_BY_NUM,
	// so hashes are impossible regardless of the flag. Takes precedence over the operator
	// reason: it is the immutable one, and reporting it is what tells an operator that
	// turning the flag on would not help.
	HashPollingOffSpecUnsupported HashPollingReason = "off-spec-no-block-by-num"

	// HashPollingOffOperatorChoice — the chain could do hashes; fork detection is off
	// because --enable-fork-detection was not set.
	HashPollingOffOperatorChoice HashPollingReason = "off-operator-choice"
)

// HeadOnly reports whether this reason means the tracker skips all block-hash work.
func (r HashPollingReason) HeadOnly() bool { return r != HashPollingOn }

// String makes the reason printable in logs and debug rows.
func (r HashPollingReason) String() string { return string(r) }
