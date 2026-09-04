package lavasession

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/utils"
)

type StickySession struct {
	Provider string
	Epoch    uint64
	// Confirmed reports whether the fleet-wide store agreed this pod may route the session to
	// Provider — either the claim was read from the store, or this pod wrote it and won.
	//
	// It is what makes the local table safe to answer from without a network call on every
	// relay. An unconfirmed pin is this pod's private opinion, which under cross-pod stickiness
	// is exactly the split the feature removes, so it is never routed on. Pins created before
	// the feature was enabled, and those the subscription managers create for their own
	// connection-scoped affinity, are unconfirmed and keep their original single-pod meaning.
	Confirmed bool
}

// ErrStickyUnavailable means the fleet-wide claim for a session could not be established: the
// store was unreachable, or it refused the claim. It is deliberately NOT "no claim exists" —
// serving a sticky request without knowing the fleet's claim is the silent split the feature
// exists to prevent, so callers fail the request instead.
var ErrStickyUnavailable = errors.New("sticky session claim could not be established")

// SharedStickyStore is the fleet-wide claim registry, satisfied by the cache backends. Nil
// disables cross-pod stickiness and leaves the pod-local behaviour untouched.
//
// Fetch returns the live claim; found=false is a normal miss, an error means "could not
// determine" and must never be collapsed into a miss. PublishIfAbsent claims provider for
// stickyId first-writer-wins and returns the EFFECTIVE claim, so a caller that lost the race
// learns the winner from its own write and adopts it in the same request.
type SharedStickyStore interface {
	Fetch(ctx context.Context, chainID, apiInterface, stickyID string) (provider string, epoch uint64, found bool, err error)
	PublishIfAbsent(ctx context.Context, chainID, apiInterface, stickyID, provider string, epoch uint64, ttl time.Duration) (winner string, winnerEpoch uint64, err error)
}

// StickyIDDigest is the opaque key a session id travels under. The raw id is chosen by the
// client and may identify an end user; the fleet only needs a stable string to agree on, so the
// plaintext never leaves the process. Every pod derives the same digest from the same id, which
// is all the agreement requires.
func StickyIDDigest(stickyID string) string {
	sum := sha256.Sum256([]byte(stickyID))
	return hex.EncodeToString(sum[:16])
}

type StickySessionStore struct {
	lock     sync.RWMutex
	sessions map[string]*StickySession
}

func NewStickySessionStore() *StickySessionStore {
	return &StickySessionStore{
		sessions: make(map[string]*StickySession),
	}
}

func (s *StickySessionStore) Get(id string) (*StickySession, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	session, exists := s.sessions[id]
	return session, exists
}

func (s *StickySessionStore) Set(id string, session *StickySession) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.sessions[id] = session
}

func (s *StickySessionStore) Delete(id string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.sessions, id)
}

func (s *StickySessionStore) DeleteOldSessions(epoch uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for id, session := range s.sessions {
		if session.Epoch < epoch {
			utils.LavaFormatTrace("deleting sticky session", utils.LogAttr("id", id))
			delete(s.sessions, id)
		}
	}
}

// Clear drops every sticky session affinity, regardless of epoch.
// Used by the /debug/reset-all endpoint to return the router to a clean state.
func (s *StickySessionStore) Clear() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.sessions = make(map[string]*StickySession)
}

// Len returns the current number of sticky-session affinities. Used by the
// CSM state-size gauge publisher (MAG-1762) so integration tests can verify
// /debug/reset-all emptied the store.
func (s *StickySessionStore) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.sessions)
}
