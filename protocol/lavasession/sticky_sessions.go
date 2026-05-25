package lavasession

import (
	"sync"

	"github.com/Magma-Devs/smart-router/utils"
)

type StickySession struct {
	Provider string
	Epoch    uint64
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
