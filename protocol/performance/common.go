package performance

import "time"

const (
	CacheFlagName = "cache-be"
	// CacheKeyPrefixFlagName names the keyspace this router occupies on the gRPC
	// cache server — the cache-be counterpart of resp-cache.key-prefix. Routers
	// that share a keyspace serve each other's cached answers and resolve LATEST
	// off one chain tip, which is correct for replicas of one deployment and
	// wrong for anything reading a different node set (MAG-3521). Empty keeps the
	// shared keyspace every router occupied before the setting existed.
	CacheKeyPrefixFlagName = "cache-be-key-prefix"

	// Secondary cache tier (docs/SECONDARY-CACHE.md): an optional read-only
	// fallback cache queried when the primary produces no hit, before provider
	// fall-through. Independent of the primary — valid with cache-be unset, and it
	// keeps serving while the primary is down.
	SecondaryCacheFlagName        = "secondary-cache-be"
	SecondaryCacheTimeoutFlagName = "secondary-cache-timeout"
	SecondaryCacheModeFlagName    = "secondary-cache-mode"

	// SecondaryCacheModeReadOnly is the only access mode v1 accepts; read-write is
	// reserved for a future iteration.
	SecondaryCacheModeReadOnly = "read-only"

	// DefaultSecondaryCacheTimeout mirrors the primary lookup budget
	// (common.CacheTimeout); operators tune it up for cross-zone network hops.
	DefaultSecondaryCacheTimeout = 50 * time.Millisecond
)
