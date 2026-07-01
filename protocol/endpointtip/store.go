// Package endpointtip holds the single, process-wide source of truth for the
// per-endpoint observed block tip.
//
// Why a standalone leaf package: the tip is written by the high layer
// (endpointstate's poll + relay-harvest observers) but also read by the low layer
// (lavasession's per-endpoint QoS sync-block). endpointstate already imports
// lavasession, so lavasession cannot import endpointstate to reach the observation
// store — that would be an import cycle. A leaf package that imports only the
// standard library can be imported by BOTH layers, so they share ONE store instead
// of each keeping its own copy (the divergence that this package exists to remove).
//
// The store holds the whole atomic tip triple {Block, ObservedAt, Source} — never a
// bare block — because the monotonic guard needs Block and ObservedAt together and
// the relay-freshness gate reads Source. Splitting the triple across two stores would
// reintroduce exactly the divergence this consolidation removes.
//
// Generation gating (rejecting writes from a removed/replaced tracker) is NOT done
// here: this leaf cannot know about tracker lifecycles. Callers in endpointstate
// gate on generation FIRST and only then call Set, which applies the time-monotonic
// guard.
package endpointtip

import (
	"sync"
	"time"
)

// Source identifies which path observed a tip. It mirrors endpointstate's
// ObservationSource but lives here so the leaf carries the full triple without
// importing upward.
type Source int

const (
	// SourceUnknown is the zero value: no tip observed yet.
	SourceUnknown Source = iota
	// SourcePoll is a tip observed by the dedicated ChainTracker poll.
	SourcePoll
	// SourceRelay is a tip harvested from a served relay response.
	SourceRelay
)

func (s Source) String() string {
	switch s {
	case SourcePoll:
		return "poll"
	case SourceRelay:
		return "relay"
	default:
		return "unknown"
	}
}

// Tip is the atomic per-endpoint tip triple. The three fields move together — a write
// either advances all three or none.
type Tip struct {
	Block      int64     // most recent observed block (always > 0 once set)
	ObservedAt time.Time // wall-clock of the observation that set Block (monotonic)
	Source     Source    // origin of this observation
}

// Store is the per-endpoint tip store, keyed by the composite Key. It is safe for
// concurrent use. The exported instance type (rather than only package globals) lets
// tests build an isolated store; production goes through Default().
type Store struct {
	mu   sync.RWMutex
	tips map[string]Tip
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{tips: make(map[string]Tip)}
}

// Key builds the composite store key. The tip is keyed by chain AND apiInterface AND
// url so a single process serving multiple chains (or multiple interfaces that happen
// to reuse a url string) never collides on a shared global map.
func Key(chainID, apiInterface, url string) string {
	return chainID + "|" + apiInterface + "|" + url
}

// Set applies a tip observation under the time-monotonic guard: a write whose
// ObservedAt predates the stored ObservedAt is dropped wholesale so no field
// regresses. Equal timestamps apply (last-writer-wins, the documented deterministic
// tie-break, matching the prior observation-record semantics). A non-positive block is
// ignored. Returns true iff the write was applied (the caller uses this to decide
// whether to feed the per-chain consensus tip).
//
// The guard is time-monotonic, NOT block-monotonic — it relocates the exact semantics
// the observation record used before consolidation; it is deliberately not "improved"
// to max-block.
func (s *Store) Set(key string, t Tip) bool {
	if t.Block <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.tips[key]; ok && t.ObservedAt.Before(cur.ObservedAt) {
		return false
	}
	s.tips[key] = t
	return true
}

// Get returns the stored tip and whether one exists. The returned Tip is a value copy,
// so callers never see a half-updated triple.
func (s *Store) Get(key string) (Tip, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tips[key]
	return t, ok
}

// Block is the convenience reader for callers that only need the int64 tip (the QoS
// sync-block, syncGap, and consistency-validation readers). It returns 0 when no
// positive tip has been observed.
func (s *Store) Block(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tips[key].Block
}

// Remove drops an endpoint's tip. The monitor calls this when a tracker is removed or
// the monitor stops, so a removed endpoint's stale tip cannot linger in the global map.
func (s *Store) Remove(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tips, key)
}

// Reset clears the entire store. It exists for test isolation — production never calls
// it. (Package tests share the Default() singleton; Reset keeps cases independent.)
func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tips = make(map[string]Tip)
}

// defaultStore is the process-wide singleton. Both the high layer (endpointstate
// observers) and the low layer (lavasession QoS) reach the same instance through
// Default(), which is what makes the tip live in exactly one place.
var defaultStore = NewStore()

// Default returns the process-wide singleton tip store.
func Default() *Store { return defaultStore }
