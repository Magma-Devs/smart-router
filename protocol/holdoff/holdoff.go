// Package holdoff is the shared rate-limit hold-off registry: the one place every path
// that talks to an upstream (relay selection, spec re-verification, recovery probing,
// block polling) records a 429 and asks "may I send to this endpoint now?".
//
// Semantics, settled on MAG-2947:
//
//   - Tiered keying. A single 429 holds off the endpoint URL that returned it. When
//     EscalationDistinctURLs of one provider's URLs are held off at once, the hold-off
//     escalates to the provider name — a vendor cap is account-wide, and per-URL keying
//     alone lets a process keep hammering the account through its other chains.
//
//   - Retry-After is the floor. When the upstream said how long to wait, the hold-off is
//     at least that long; otherwise a per-URL exponential (InitialHoldoff doubling per
//     consecutive strike, capped at MaxHoldoff). Jitter only extends, never shortens —
//     coming back before the vendor's ask defeats the point, coming back spread out is
//     the point: a fleet held off by one vendor-wide limit must not return in a
//     synchronized burst.
//
//   - Any answer clears. An upstream that answered a request — even with a genuine
//     failure — is no longer refusing us for load, so the URL's strikes and the
//     provider escalation are dropped and later failures reach their normal handling
//     at the normal cadence.
//
// The registry never sleeps and never decides policy: consumers ask HeldOff/ReadyAt and
// choose what "held off" means on their path (skip the probe, prefer another endpoint,
// stretch the poll). The mapping of consumer paths to behaviour lives in
// docs/RATE-LIMIT-HOLDOFF.md — add a row there when wiring a new consumer.
package holdoff

import (
	"math/rand"
	"sync"
	"time"
)

const (
	// InitialHoldoff is the first penalty for a URL whose upstream answered 429 without
	// a usable Retry-After. Doubles per consecutive strike up to MaxHoldoff.
	InitialHoldoff = 30 * time.Second

	// MaxHoldoff caps the exponential. An upstream-sent Retry-After may exceed it, up to
	// maxUpstreamHoldoff — the vendor knows its own reset schedule better than we do.
	MaxHoldoff = 30 * time.Minute

	// maxUpstreamHoldoff bounds what a Retry-After header can demand, mirroring
	// common.MaxRetryAfter so a hostile or broken upstream cannot park an endpoint
	// indefinitely.
	maxUpstreamHoldoff = time.Hour

	// JitterFactor extends each hold-off by up to +20%, spreading expirations.
	JitterFactor = 0.2

	// EscalationDistinctURLs is how many of a provider's URLs must be held off at once
	// before the hold-off covers the whole provider name.
	EscalationDistinctURLs = 2

	// entryRetention is how long an expired, untouched entry keeps its strike memory.
	// Past it the URL starts clean again — a 429 from yesterday says nothing about now.
	entryRetention = time.Hour
)

type entry struct {
	strikes  int
	readyAt  time.Time
	lastSeen time.Time
}

type providerState struct {
	urls           map[string]*entry
	escalatedUntil time.Time
}

// Registry is safe for concurrent use. The zero value is not usable; call NewRegistry.
type Registry struct {
	mu        sync.Mutex
	providers map[string]*providerState

	// now and randFloat are injection seams for tests; production uses time.Now and a
	// seeded math/rand. randFloat is only called under mu.
	now       func() time.Time
	randFloat func() float64
}

func NewRegistry() *Registry {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &Registry{
		providers: map[string]*providerState{},
		now:       time.Now,
		randFloat: rng.Float64,
	}
}

// RecordRateLimit notes a 429 from url, owned by provider. retryAfter is what the
// upstream asked for (0 when it said nothing) and floors the hold-off. Returns the
// applied hold-off so callers can log what was decided.
func (r *Registry) RecordRateLimit(provider, url string, retryAfter time.Duration) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.gcLocked(now)

	ps := r.providers[provider]
	if ps == nil {
		ps = &providerState{urls: map[string]*entry{}}
		r.providers[provider] = ps
	}
	e := ps.urls[url]
	if e == nil {
		e = &entry{}
		ps.urls[url] = e
	}

	e.strikes++
	d := exponentialFor(e.strikes)
	if retryAfter > d {
		d = retryAfter
	}
	if d > maxUpstreamHoldoff {
		d = maxUpstreamHoldoff
	}
	d += time.Duration(r.randFloat() * JitterFactor * float64(d))
	e.readyAt = now.Add(d)
	e.lastSeen = now

	// Escalate when enough distinct URLs of this provider are held off at once. The
	// provider-wide hold-off ends when its longest-held member does — and any answered
	// request drops it sooner.
	held := 0
	latest := time.Time{}
	for _, ue := range ps.urls {
		if ue.readyAt.After(now) {
			held++
			if ue.readyAt.After(latest) {
				latest = ue.readyAt
			}
		}
	}
	if held >= EscalationDistinctURLs {
		ps.escalatedUntil = latest
	}
	return d
}

// RecordAnswer notes that url answered a request — with anything at all, success or a
// genuine failure. The URL starts clean and the provider escalation is dropped: an
// account that is serving again must not stay locked out by stale strikes.
func (r *Registry) RecordAnswer(provider, url string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ps := r.providers[provider]
	if ps == nil {
		return
	}
	delete(ps.urls, url)
	ps.escalatedUntil = time.Time{}
	if len(ps.urls) == 0 {
		delete(r.providers, provider)
	}
}

// HeldOff reports whether requests to url should be avoided right now, either because
// the URL itself is held off or its provider escalated.
func (r *Registry) HeldOff(provider, url string) bool {
	readyAt := r.ReadyAt(provider, url)
	return readyAt.After(r.now())
}

// ReadyAt returns when url stops being held off — the later of its own hold-off and its
// provider's escalation. The zero time means it is not held off at all. Selection uses
// this for the soonest-to-expire fallback when every endpoint is held off.
func (r *Registry) ReadyAt(provider, url string) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	ps := r.providers[provider]
	if ps == nil {
		return time.Time{}
	}
	ready := ps.escalatedUntil
	if e := ps.urls[url]; e != nil && e.readyAt.After(ready) {
		ready = e.readyAt
	}
	return ready
}

// exponentialFor is InitialHoldoff doubled per consecutive strike, capped at MaxHoldoff.
func exponentialFor(strikes int) time.Duration {
	d := InitialHoldoff
	for i := 1; i < strikes; i++ {
		d *= 2
		if d >= MaxHoldoff {
			return MaxHoldoff
		}
	}
	return d
}

// gcLocked forgets entries whose hold-off expired and that saw no 429 for
// entryRetention, so strike memory does not accumulate forever. Called under mu.
func (r *Registry) gcLocked(now time.Time) {
	for provider, ps := range r.providers {
		for url, e := range ps.urls {
			if e.readyAt.Before(now) && now.Sub(e.lastSeen) > entryRetention {
				delete(ps.urls, url)
			}
		}
		if len(ps.urls) == 0 && ps.escalatedUntil.Before(now) {
			delete(r.providers, provider)
		}
	}
}
