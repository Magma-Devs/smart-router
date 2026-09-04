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

var (
	_ BackendEndpointReporter = (*Cache)(nil)
	_ BackendEndpointReporter = (*RespCache)(nil)
)

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
	GetStickySession(ctx context.Context, chainId, apiInterface, stickyId string) (core.StickyPin, bool, error)
	SetStickySessionIfAbsent(ctx context.Context, chainId, apiInterface, stickyId string, pin core.StickyPin, ttl time.Duration) (core.StickyPin, error)
}

var (
	_ StickySessionBackend = (*Cache)(nil)
	_ StickySessionBackend = (*RespCache)(nil)
)
