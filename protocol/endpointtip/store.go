// Package endpointtip holds the single, process-wide source of truth for the
// per-endpoint observed block tip.
//
// It is a standard-library-only leaf so both the high layer (endpointstate's poll +
// relay observers, which write it) and the low layer (lavasession's QoS sync-block,
// which reads it) can import it without a cycle — sharing ONE store rather than each
// keeping a divergent copy. It stores the whole {Block, ObservedAt, Source} triple so
// the guard and the relay-freshness gate see a consistent value.
//
// Generation gating (rejecting writes from a removed tracker) is done by the
// endpointstate callers before Set; this leaf only applies the block-monotonic guard
// with a staleness backstop (T4/C-D — see Set).
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
	ObservedAt time.Time // wall-clock of the observation that set Block (freshness stamp)
	Source     Source    // origin of this observation
}

// The staleness horizon is NOT defined here — this leaf is pure mechanism. The caller
// (endpointstate's monitor) derives it from chainstate.StalenessWindow, the one source of
// truth for the fresh/alive horizon, and passes it to Set as staleAfter.

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

// Set applies a tip observation, block-monotonic with a staleness backstop (T4/C-D):
// higher wins, equal refreshes the stamp, lower is rejected while fresh (a late
// straggler must not regress the tip — F2) and accepted once stale (a real reorg down).
//
// Staleness is the gap between the stored and incoming observation stamps (> staleAfter),
// not wall-clock — deterministic, no clock. A rejected write must not refresh the stamp,
// else a repeated straggler keeps the tip forever fresh. staleAfter<=0 is up-only.
// Returns true iff the stored tip now reflects this block (gates the consensus feed).
func (s *Store) Set(key string, t Tip, staleAfter time.Duration) bool {
	if t.Block <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.tips[key]
	switch {
	case !ok || t.Block > cur.Block:
		s.tips[key] = t
		return true
	case t.Block == cur.Block:
		if t.ObservedAt.After(cur.ObservedAt) {
			s.tips[key] = t // advance the freshness stamp, never backwards
		}
		return true
	default: // lower block
		if staleAfter > 0 && t.ObservedAt.Sub(cur.ObservedAt) > staleAfter {
			s.tips[key] = t // stored tip went stale → accept the reorg
			return true
		}
		return false // still fresh → reject the straggler (F2)
	}
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
