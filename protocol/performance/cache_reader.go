package performance

import (
	"context"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// CacheReader is the read-only surface of a cache backend. The secondary cache
// (docs/SECONDARY-CACHE.md) is held behind this interface so writes
// (SetEntry) and flushes (Flush) on it are compile-time errors — the read-only
// guarantee is structural, not conventional. It doubles as the unit-test seam:
// tests inject a fake CacheReader instead of standing up a gRPC cache server.
type CacheReader interface {
	CacheActive() bool
	GetEntry(ctx context.Context, relayCacheGet *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error)
}

// The concrete gRPC-backed cache client satisfies the read-only surface.
var _ CacheReader = (*Cache)(nil)
