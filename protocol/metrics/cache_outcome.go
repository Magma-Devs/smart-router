package metrics

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Cache tier and lookup-outcome label values (docs/METRICS.md#cache).
// Both are closed enums: cache_tier distinguishes the primary cache from the
// optional read-only secondary, and outcome splits the former catch-all "failed"
// classification so operators can tell a clean miss from a broken or slow tier.
const (
	CacheTierPrimary   = "primary"
	CacheTierSecondary = "secondary"

	CacheOutcomeHit     = "hit"
	CacheOutcomeMiss    = "miss"
	CacheOutcomeError   = "error"
	CacheOutcomeTimeout = "timeout"
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
