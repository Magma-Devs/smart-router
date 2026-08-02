package performance

import "time"

const (
	CacheFlagName = "cache-be"

	// Secondary cache tier (docs/SECONDARY-CACHE-DESIGN.md): an optional read-only
	// fallback cache queried when the primary produces no hit, before provider
	// fall-through. Independent of the primary — valid with cache-be unset, and it
	// keeps serving while the primary is down (§5).
	SecondaryCacheFlagName        = "secondary-cache-be"
	SecondaryCacheTimeoutFlagName = "secondary-cache-timeout"
	SecondaryCacheModeFlagName    = "secondary-cache-mode"

	// SecondaryCacheModeReadOnly is the only access mode v1 accepts; read-write is
	// reserved for a future iteration (design §16).
	SecondaryCacheModeReadOnly = "read-only"

	// DefaultSecondaryCacheTimeout mirrors the primary lookup budget
	// (common.CacheTimeout); operators tune it up for cross-zone network hops.
	DefaultSecondaryCacheTimeout = 50 * time.Millisecond
)
