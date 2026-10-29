// Package holdoff is the shared rate-limit hold-off registry: the one place every path
// that talks to an upstream (relay selection, spec re-verification, recovery probing,
// block polling) records a 429 and asks "may I send to this endpoint now?".
//
// Semantics, settled on MAG-2947:
//
//   - Tiered keying. A single 429 holds off the endpoint URL that returned it. When
//     EscalationDistinctURLs of one provider's URLs are held off at once, the hold-off
//     escalates to the provider — a vendor cap is account-wide, and per-URL keying alone
//     lets a process keep hammering the account through the provider's other URLs. The
//     provider is one configured entry, its name scoped to the chain and api-interface
//     it serves (ProviderKey): an escalation holds that entry, not every entry that
//     shares the name on other chains and interfaces (MAG-4732).
//
//   - Retry-After is the floor. When the upstream said how long to wait, the hold-off is
//     at least that long; otherwise a per-URL exponential (InitialHoldoff doubling per
//     consecutive strike, capped at MaxHoldoff). Jitter only extends, never shortens —
//     coming back before the vendor's ask defeats the point, coming back spread out is
//     the point: a fleet held off by one vendor-wide limit must not return in a
//     synchronized burst.
//
//   - One strike per hold-off. A 429 that arrives while the URL is still held off adds no
//     strike and does not move the hold-off, unless its Retry-After asks for longer than
//     what is left. A burst refused inside one quota window is one refusal, not one per
//     request that was in flight.
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

	"github.com/magma-Devs/smart-router/protocol/common"
)

const (
	// InitialHoldoff is the first penalty for a URL whose upstream answered 429 without
	// a usable Retry-After. Doubles per consecutive strike up to MaxHoldoff.
	InitialHoldoff = 30 * time.Second

	// MaxHoldoff caps the exponential. An upstream-sent Retry-After may exceed it, up to
	// maxUpstreamHoldoff — the vendor knows its own reset schedule better than we do.
	MaxHoldoff = 30 * time.Minute

	// maxUpstreamHoldoff is the parser's Retry-After bound, shared here so capture and
	// consumption cannot silently drift apart.
	maxUpstreamHoldoff = common.MaxRetryAfter

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

// providerScopeSeparator joins the parts of a ProviderKey.
const providerScopeSeparator = "|"

// ProviderKey is the registry key of a configured provider entry: its name, scoped to the
// chain and api-interface the entry serves. A name is unique per chain and api-interface
// only, so keyed by the name alone two held urls of one entry escalated every entry that
// shares the name — a 429 storm on jsonrpc held REST too (MAG-4732). An empty name stays
// empty, so a caller's "no provider" still reads as one.
func ProviderKey(chainID, apiInterface, name string) string {
	if name == "" {
		return ""
	}
	return chainID + providerScopeSeparator + apiInterface + providerScopeSeparator + name
}

// Shared is the process-wide registry every production consumer records into and
// consults: the relay path, the re-verifier, the recovery probe and the chain trackers
// of one entry must read each other's hold-offs. Tests build their own via
// NewRegistry/NewRegistryWithClock instead of touching this one.
var Shared = NewRegistry()

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

// NewRegistryWithClock is the deterministic variant for consumer tests: time comes from
// the given clock and jitter is disabled, so applied hold-offs are exact.
func NewRegistryWithClock(now func() time.Time) *Registry {
	return &Registry{
		providers: map[string]*providerState{},
		now:       now,
		randFloat: func() float64 { return 0 },
	}
}

// RecordRateLimit notes a 429 from url, owned by provider. retryAfter is what the
// upstream asked for (0 when it said nothing) and floors the hold-off. Returns how long
// url is now held off, so callers can log what was decided and hand it on as a wait.
//
// A strike is a hold-off that ended in another 429, not a 429. One that lands while url is
// still held off is the same refusal again: a vendor's per-second quota answers a burst with
// a 429 per request already in flight, and counting each as a strike took one second of them
// to the 30-minute cap (MAG-4386). It adds no strike and leaves the hold-off where it was —
// the return is what is left of it — unless the upstream asked for longer than that, which
// extends the hold-off to the ask, still without a strike.
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

	e.lastSeen = now
	if e.readyAt.After(now) {
		if retryAfter > e.readyAt.Sub(now) {
			if d := r.boundedWithJitterLocked(retryAfter); now.Add(d).After(e.readyAt) {
				e.readyAt = now.Add(d)
				metricRecorded(provider, d)
			}
		}
	} else {
		e.strikes++
		d := exponentialFor(e.strikes)
		if retryAfter > d {
			d = retryAfter
		}
		d = r.boundedWithJitterLocked(d)
		e.readyAt = now.Add(d)
		metricRecorded(provider, d)
	}

	// Escalate when enough distinct URLs of this provider are held off at once. The
	// provider-wide hold-off ends when its longest-held member does — and any answered
	// request drops it sooner. Checked on every 429, a repeat included, as before: which
	// URLs are held is the question, not whether this 429 added a strike.
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
		if !ps.escalatedUntil.After(now) {
			metricEscalated(provider) // counted on the transition, not per refresh
		}
		ps.escalatedUntil = latest
	}
	return e.readyAt.Sub(now)
}

// boundedWithJitterLocked bounds a hold-off at maxUpstreamHoldoff and extends it by the
// jitter, bounded again so the worst case stays within the parser's limit. Called under mu.
func (r *Registry) boundedWithJitterLocked(d time.Duration) time.Duration {
	if d > maxUpstreamHoldoff {
		d = maxUpstreamHoldoff
	}
	d += time.Duration(r.randFloat() * JitterFactor * float64(d))
	if d > maxUpstreamHoldoff {
		d = maxUpstreamHoldoff
	}
	return d
}

// RecordAnswer notes that url answered a request — with anything at all, success or a
// genuine failure. The URL starts clean and the provider escalation is dropped: an
// account that is serving again must not stay locked out by stale strikes.
//
// The URL's url-only entry (provider == url: the per-url chain tracker, WS and gRPC
// subscriptions) is dropped too. Those paths record a 429 under the URL alone, and the
// relay path answers under the provider's name; without this a relay serving the URL
// fine would leave the URL's own strikes standing (MAG-4165).
func (r *Registry) RecordAnswer(provider, url string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if provider != url {
		r.clearLocked(url, url)
	}
	r.clearLocked(provider, url)
}

func (r *Registry) clearLocked(provider, url string) {
	ps := r.providers[provider]
	if ps == nil {
		return
	}
	_, hadEntry := ps.urls[url]
	if hadEntry || ps.escalatedUntil.After(r.now()) {
		metricCleared(provider) // only when the answer actually dropped a penalty
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
	return !r.ReadyAt(provider, url).IsZero()
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
	now := r.now()
	ready := ps.escalatedUntil
	if e := ps.urls[url]; e != nil && e.readyAt.After(ready) {
		ready = e.readyAt
	}
	if !ready.After(now) {
		return time.Time{}
	}
	return ready
}

// ProviderReadyAt reports whether provider is held off as a whole, and when it next has
// a URL worth trying. urls are the provider's URLs that could serve the request being
// placed: the caller knows them, the registry only knows URLs that answered 429s. A
// provider is held off while its name is escalated, or while every one of urls is held. A
// URL with no entry is free — a 429 on one url says nothing about the provider's others,
// nor about the same name's urls on another api-interface or chain (MAG-4732).
func (r *Registry) ProviderReadyAt(provider string, urls []string) (readyAt time.Time, held bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ps := r.providers[provider]
	if ps == nil {
		return time.Time{}, false
	}
	now := r.now()

	anyFree := false
	minHeld := time.Time{}
	heldURLs := 0
	for _, url := range urls {
		if e := ps.urls[url]; e != nil && e.readyAt.After(now) {
			heldURLs++
			if minHeld.IsZero() || e.readyAt.Before(minHeld) {
				minHeld = e.readyAt
			}
		} else {
			anyFree = true
		}
	}

	if ps.escalatedUntil.After(now) {
		readyAt = ps.escalatedUntil
		if !anyFree && minHeld.After(readyAt) {
			readyAt = minHeld
		}
		return readyAt, true
	}
	if heldURLs == 0 || anyFree {
		return time.Time{}, false
	}
	return minHeld, true
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
