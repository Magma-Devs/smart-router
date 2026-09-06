package performance

import (
	"context"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// CacheBackend is the full surface the router consumes from a cache backend:
// the relay read/write path (GetEntry/SetEntry), liveness probing for skip
// decisions (CacheActive), the /debug/reset-all flush, and shutdown (Close).
// The gRPC cache-be client (Cache) is the canonical implementation; alternative
// backends plug in behind this seam without call sites knowing the transport.
//
// Wiring convention: an unconfigured cache travels as a typed-nil *Cache inside
// the interface, so implementations must keep every method safe on a nil
// receiver (CacheActive reports false, operations return NotInitializedError,
// Close is a no-op). Call sites holding a possibly zero-valued field must still
// nil-check the interface itself before probing.
type CacheBackend interface {
	CacheActive() bool
	GetEntry(ctx context.Context, relayCacheGet *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error)
	SetEntry(ctx context.Context, cacheSet *pairingtypes.RelayCacheSet) error
	Flush(ctx context.Context) error
	Close() error
}

var _ CacheBackend = (*Cache)(nil)

// BackendEndpointReporter is implemented by backends that can name the server
// that served a cache hit. Optional on purpose: it is a debug affordance, so it
// stays off the CacheBackend contract and call sites type-assert for it.
type BackendEndpointReporter interface {
	BackendEndpoint() string
}

// Cache engine names, and what an unreachable tier costs per relay. Closed enums:
// both are wire values of GET /debug/cache-state.
const (
	CacheEngineGRPC = "grpc"
	CacheEngineRESP = "resp"

	// CacheWhenUnreachableSkipped: the tier is bypassed before any I/O, so an
	// unreachable backend costs nothing per relay (the gRPC client returns
	// NotConnectedError up front).
	CacheWhenUnreachableSkipped = "skipped"
	// CacheWhenUnreachableAttempted: every lookup and write is still issued against
	// the dead backend and pays the full timeout (the RESP backend degrades
	// per-operation rather than flipping itself off).
	CacheWhenUnreachableAttempted = "attempted"
)

// CacheLifetimes is the TTL policy a backend is actually applying, in seconds.
//
// Reported by the backend that OWNS the policy rather than read from flags: the
// expiration flags are registered on the cache-server command, never on the
// router's, so reading them router-side returns zero on every deployment. A tier
// whose TTLs live in another pod (cache-be) reports none at all rather than a
// plausible-looking zero.
//
// These are the policy's base values. The effective TTL for a non-finalized entry
// is max(averageBlockTime/8, NonFinalized) per chain, so NonFinalizedSeconds is a
// floor, not the value any particular entry will get.
type CacheLifetimes struct {
	FinalizedSeconds    float64 `json:"finalized_seconds"`
	NonFinalizedSeconds float64 `json:"non_finalized_seconds"`
	NodeErrorsSeconds   float64 `json:"node_errors_seconds"`
}

// DebugCacheState is one cache tier as GET /debug/cache-state reports it.
//
// Deliberately a single struct behind a single method rather than an accessor per
// field: the endpoint reaches it through a runtime type assertion, so every extra
// method is another name that can be renamed without a build failure while the
// assertion silently stops matching and the endpoint degrades to "no cache" on a
// router that is caching fine.
type DebugCacheState struct {
	// Configured is decided by the backend itself, never by the caller checking the
	// interface for nil: an unconfigured cache travels as a TYPED-nil pointer inside
	// a non-nil CacheBackend (see the wiring convention above), and a typed nil
	// still satisfies DebugCacheStateReporter.
	Configured bool
	Engine     string
	Address    string
	// Reachable is nil when reachability has not been determined — a RESP backend
	// before its first probe returns, or a gRPC connection mid-dial. Distinct from
	// false, which is a positive finding that the backend is down.
	Reachable *bool
	// CheckedAt stamps a reachability value that is a periodic SNAPSHOT rather than
	// a live read, so a consumer can tell how stale it is. Zero when the value is
	// read live (the gRPC tier reads the connection state on demand).
	CheckedAt time.Time
	// Detail is the human-readable reason behind Reachable — the gRPC connectivity
	// state, or an auth-vs-network classification for RESP. An authentication
	// rejection reported only as "unreachable" sends an operator to check
	// networking when the real fault is a credential.
	Detail string
	// WhenUnreachable is what the router DOES when this tier is unreachable, one of
	// the CacheWhenUnreachable* values. Reported because Reachable=false means
	// opposite things per engine, and nothing else in the payload marks it.
	WhenUnreachable string
	// Lifetimes is nil when this backend cannot answer for its own TTLs.
	Lifetimes *CacheLifetimes
}

// DebugCacheStateReporter exposes the runtime facts needed by the debug cache
// state endpoint. It is deliberately optional so the cache backend contract
// remains focused on serving relays.
//
// Implementations must be safe on a typed-nil receiver, like the rest of
// CacheBackend, and must not mutate anything: the endpoint is documented
// read-only, and the obvious liveness accessors on the gRPC client are not
// (CacheActive spawns a reconnect).
type DebugCacheStateReporter interface {
	DebugCacheState() DebugCacheState
}

var (
	_ BackendEndpointReporter = (*Cache)(nil)
	_ BackendEndpointReporter = (*RespCache)(nil)

	// Compile-time proof that both backends still satisfy the debug reporter. The
	// endpoint reaches them through a comma-ok assertion, so without these a rename
	// on either backend is not a build failure — the assertion just stops matching
	// and /debug/cache-state quietly reports an unconfigured cache on a router whose
	// cache is working. Neither go build nor go vet would catch it.
	_ DebugCacheStateReporter = (*Cache)(nil)
	_ DebugCacheStateReporter = (*RespCache)(nil)
)

func boolPtr(b bool) *bool { return &b }

// StickySessionBackend is implemented by cache backends that can hold fleet-wide sticky-session
// claims, so one session id resolves to the same upstream on every router replica.
//
// It is a capability interface rather than part of CacheBackend because not every backend can
// serve it — but unlike BackendEndpointReporter, which is a debug affordance, this one carries a
// correctness contract. Both shipped backends implement it: the gRPC client reaches the cache
// server's engine over an RPC pair, and the RESP backend reaches the same engine in-process.
// Claims travel through the KVStore seam precisely so the RESP backend is not left out the way
// endpoint observations are — a guarantee that silently lapses on one backend is not a guarantee.
//
// A backend that does NOT implement this cannot support cross-pod stickiness, and the router
// must refuse to serve sticky traffic rather than quietly falling back to per-pod affinity.
type StickySessionBackend interface {
	GetStickySession(ctx context.Context, chainId, apiInterface, service, stickyId string) (core.StickyPin, bool, error)
	SetStickySessionIfAbsent(ctx context.Context, chainId, apiInterface, service, stickyId string, pin core.StickyPin, ttl time.Duration) (core.StickyPin, error)
}

var (
	_ StickySessionBackend = (*Cache)(nil)
	_ StickySessionBackend = (*RespCache)(nil)
)
