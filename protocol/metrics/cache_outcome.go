package metrics

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// Cache tier and lookup-outcome label values (docs/METRICS.md#cache).
// Both are closed enums: cache_tier distinguishes the primary cache from the
// optional read-only secondary, and outcome splits the former catch-all "failed"
// classification so operators can tell a clean miss from a broken or slow tier.
//
// Aliased from protocol/common, which is where the same values are also rendered into
// the per-request Lava-Cache-Tier / Lava-Cache-Outcome headers. One definition, so a
// counter and a header describing the same lookup cannot drift apart. common carries
// three further not-consulted values (off / skipped / unknown) that are deliberately
// NOT aliased here: no lookup happened, so there is nothing to count, and admitting
// them would widen a metric label enum documented as closed.
const (
	CacheTierPrimary   = common.CacheTierPrimary
	CacheTierSecondary = common.CacheTierSecondary

	CacheOutcomeHit     = common.CacheOutcomeHit
	CacheOutcomeMiss    = common.CacheOutcomeMiss
	CacheOutcomeError   = common.CacheOutcomeError
	CacheOutcomeTimeout = common.CacheOutcomeTimeout
)

// ClassifyCacheLookupOutcome maps a cache GetEntry result onto the closed outcome
// enum: hit, clean miss (no error, no reply), deadline-bounded timeout, or any
// other transport/server error. Timeouts are detected both as a raw
// context.DeadlineExceeded and as the gRPC DeadlineExceeded status the client
// surfaces when the per-lookup budget expires.
func ClassifyCacheLookupOutcome(err error, hit bool) string {
	switch {
	case hit:
		return CacheOutcomeHit
	case err == nil:
		return CacheOutcomeMiss
	case errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded:
		return CacheOutcomeTimeout
	default:
		return CacheOutcomeError
	}
}
