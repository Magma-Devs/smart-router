package cache

import (
	"sync"
	"time"
)

const (
	// expiringMapSweepEvery bounds how many writes land between expiry sweeps. Both stores
	// built on expiringMap are small (one entry per live sticky id, one per chain × interface
	// × endpoint across the fleet) and every entry carries a TTL, so a full sweep every N
	// writes keeps them bounded without a background goroutine.
	expiringMapSweepEvery = 256
)

// expiringEntry is one value plus its stamps on the SERVER's clock. Readers never see a
// writer's timestamp, so pods with skewed clocks cannot disagree about whether an entry is
// live or how old it is.
type expiringEntry[V any] struct {
	value     V
	storedAt  time.Time
	expiresAt time.Time
}

// expiringMap is the in-process backing of sticky pins and endpoint observations. It is a
// plain mutex-guarded map rather than a ristretto store on purpose: ristretto's Set is
// asynchronous and may drop a write under admission pressure, which is acceptable for relay
// responses but not for a first-writer-wins claim (a dropped write means two pods both
// believe they won) nor for a value a peer is about to make a skip-the-poll decision on.
// Expiry is lazy on read, plus the periodic sweep on write.
type expiringMap[V any] struct {
	mu     sync.Mutex
	byKey  map[string]expiringEntry[V]
	writes int
	now    func() time.Time // injectable for tests; time.Now in production
}

func newExpiringMap[V any]() *expiringMap[V] {
	return &expiringMap[V]{byKey: make(map[string]expiringEntry[V]), now: time.Now}
}

// put stores value under key for ttl, unless the key holds a live entry and keep(live)
// says to keep it. It returns the effective value — the live one kept, or value — and
// whether the write applied. An expired entry is always replaced.
func (m *expiringMap[V]) put(key string, value V, ttl time.Duration, keep func(live V) bool) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	if cur, ok := m.byKey[key]; ok && now.Before(cur.expiresAt) && keep(cur.value) {
		return cur.value, false
	}
	m.byKey[key] = expiringEntry[V]{value: value, storedAt: now, expiresAt: now.Add(ttl)}
	m.writes++
	if m.writes >= expiringMapSweepEvery {
		m.writes = 0
		for k, v := range m.byKey {
			if !now.Before(v.expiresAt) {
				delete(m.byKey, k)
			}
		}
	}
	return value, true
}

// get returns the live value for key and its age on this clock. Found is false when nothing
// was stored or the entry expired; expired entries are dropped on read.
func (m *expiringMap[V]) get(key string) (value V, age time.Duration, found bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.byKey[key]
	if !ok {
		return value, 0, false
	}
	now := m.now()
	if !now.Before(cur.expiresAt) {
		delete(m.byKey, key)
		return value, 0, false
	}
	return cur.value, now.Sub(cur.storedAt), true
}

func (m *expiringMap[V]) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byKey = make(map[string]expiringEntry[V])
	m.writes = 0
}

func (m *expiringMap[V]) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.byKey)
}
