package core

import (
	"context"
	"time"
)

// KVStore is the storage surface the cache Engine runs on. The engine owns all
// cache semantics (key derivation, lookup precedence, seen-block validity,
// LATEST resolution, TTL selection); a store owns only representation and
// atomicity. Keys are opaque strings produced by this package's key builders —
// adapters may route on the kind prefix (see keys.go) but must never parse
// further.
//
// Two int64 write ops carry greater-OR-EQUAL semantics deliberately: an equal
// observation must rewrite the entry (refreshing its lifetime), or an actively
// observed but non-advancing chain tip would expire out from under its readers.
//
// The chain-tip pair is separate from the plain int64 pair because its
// freshness model differs: a chain tip has a fixed freshness horizon decided at
// write time and reads report staleness, while the monotonic write guard keeps
// comparing against the raw stored value even after it goes stale — a stale tip
// is unreadable but still fences lower writes.
// StickyPin is one fleet-wide sticky-session claim: the upstream bound to a sticky id, and the
// router epoch the binding was made in. Provider is the upstream's NAME (its routing identity,
// unique per chain + api interface), never a URL — URLs carry credentials. Epoch lets a reader
// apply the same staleness rule the pod-local pin table applies, without any clock comparison:
// epochs are derived from wall clock, so every pod computes the same number independently.
type StickyPin struct {
	Provider string
	Epoch    uint64
}

type KVStore interface {
	// GetEntries fetches relay envelopes for the given keys, index-aligned with
	// the input; a nil element is a miss. Adapters should batch where the
	// transport allows (one pipeline execution); an in-process store may simply
	// read sequentially.
	GetEntries(ctx context.Context, keys []string) ([]*Envelope, error)
	SetEntry(ctx context.Context, key string, env *Envelope, ttl time.Duration) error

	// Plain monotonic int64 (shared-state tip). Missing key reads as (0, false).
	GetInt64(ctx context.Context, key string) (int64, bool, error)
	SetInt64IfGreaterOrEqual(ctx context.Context, key string, value int64, ttl time.Duration) error

	// Chain tip with a write-time freshness horizon. fresh=false means unknown
	// (missing or stale); the write guard still fences against the raw value.
	GetChainTip(ctx context.Context, key string) (block int64, fresh bool, err error)
	SetChainTipIfGreaterOrEqual(ctx context.Context, key string, block int64) error

	// Block-hash → height scalars. Missing key reads as (0, false).
	GetHeight(ctx context.Context, key string) (int64, bool, error)

	// GetHeights fetches heights for the given keys, index-aligned with the
	// input; a false in the second slice is a miss. Adapters should batch where
	// the transport allows, for the same reason GetEntries does: a relay may
	// carry several block hashes, the lookup runs inside the caller's per-relay
	// cache budget (common.CacheTimeout, 50ms), and an adapter cannot batch
	// across separate GetHeight calls — over a remote backend that is one
	// network round trip per hash, and enough of them turn a warm cache into a
	// miss. An in-process store may simply read sequentially.
	GetHeights(ctx context.Context, keys []string) ([]int64, []bool, error)

	SetHeight(ctx context.Context, key string, height int64, ttl time.Duration) error

	// Sticky-session pins, claimed FIRST-WRITER-WINS.
	//
	// A pin binds one sticky session id to one upstream so every router replica routes that id
	// to the same node. Unlike the two int64 pairs above, the write is not monotonic and not
	// last-write-wins: it must not overwrite a live claim. Pods judge upstream health locally,
	// so under last-write-wins two pods with different health views would overwrite each other
	// indefinitely and the id would never settle.
	//
	// SetStickyIfAbsent returns the EFFECTIVE pin — the claim just accepted, or the live claim
	// that beat it. Returning the winner rather than a bare "did I win" is what lets a losing
	// pod adopt the winner inside the request that raced, with no second round trip.
	//
	// A store must implement this atomically. A read-then-write in the adapter is not enough:
	// the whole point is to resolve a race between two pods.
	GetSticky(ctx context.Context, key string) (StickyPin, bool, error)
	SetStickyIfAbsent(ctx context.Context, key string, pin StickyPin, ttl time.Duration) (StickyPin, error)

	// Purge drops every entry this store holds (the FlushCache RPC).
	Purge(ctx context.Context) error
}
