package performance

import (
	"context"

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
