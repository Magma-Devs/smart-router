package rpcsmartrouter

import (
	"sync"
	"time"
)

// Rate-limit backoff bounds for the re-verify pass.
//
// The first penalty is deliberately larger than a relay retry would use: this is a
// capability probe, not a request a caller is waiting on, so there is no cost to waiting and
// every reason not to add load to an upstream that just told us to slow down. The cap keeps a
// provider from disappearing from verification indefinitely — at the ceiling it is still
// re-probed regularly, just far less often than the interval.
//
// ExponentialBackoff applies its jitter AFTER capping, so an individual delay can exceed
// ReVerifyRateLimitBackoffMax by up to WebSocketJitterFactor. That is deliberate rather than
// tolerated: without it every provider held off by one vendor-wide limit would come back at
// the same instant and rebuild the burst this is meant to damp.
var (
	ReVerifyRateLimitBackoffInitial = 5 * time.Minute
	ReVerifyRateLimitBackoffMax     = time.Hour
)

const reVerifyRateLimitBackoffMultiplier = 2.0

// reverifyBackoff decides when a rate-limited provider may be probed again.
//
// Keyed by provider NAME rather than node URL. A 429 is a vendor-level signal — the account
// is over its limit, not that one endpoint is broken — so where a process serves several
// chains through the same vendor, one chain's rate-limit should slow the others too. Keying
// by URL would let a process keep hammering an account through its other chains.
//
// The zero value is not usable; call newReverifyBackoff.
type reverifyBackoff struct {
	mu    sync.Mutex
	state map[string]*providerBackoff
}

type providerBackoff struct {
	eb    *ExponentialBackoff
	until time.Time
}

func newReverifyBackoff() *reverifyBackoff {
	return &reverifyBackoff{state: map[string]*providerBackoff{}}
}

// ready reports whether this provider may be probed now. A provider that has never been
// rate-limited is always ready, so the common path costs one map lookup.
func (b *reverifyBackoff) ready(provider string, now time.Time) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.state[provider]
	if !ok {
		return true
	}
	return !now.Before(st.until)
}

// penalise records a rate-limited probe and returns how long the provider is now held off
// for. Consecutive rate-limits grow the interval; the ExponentialBackoff carries the attempt
// count and applies its own jitter, so two providers hitting the same cap do not come back
// in lockstep.
func (b *reverifyBackoff) penalise(provider string, now time.Time) time.Duration {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.state[provider]
	if !ok {
		st = &providerBackoff{eb: NewExponentialBackoff(
			ReVerifyRateLimitBackoffInitial,
			ReVerifyRateLimitBackoffMax,
			reVerifyRateLimitBackoffMultiplier,
			0, // unlimited: a rate-limit must never exhaust into "give up on this provider"
		)}
		b.state[provider] = st
	}
	delay, _ := st.eb.NextBackoff()
	st.until = now.Add(delay)
	return delay
}

// clear drops any penalty after a probe that was not rate-limited. Called on every
// non-rate-limited outcome, not only success: once the upstream is answering us again --
// even to report a genuine capability failure -- it is no longer refusing us for load, and
// the demote logic should see that failure at the normal cadence rather than through a
// backoff window.
func (b *reverifyBackoff) clear(provider string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.state, provider)
}

// heldOff reports how many providers are currently in backoff. Used for logging.
func (b *reverifyBackoff) heldOff(now time.Time) int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, st := range b.state {
		if now.Before(st.until) {
			n++
		}
	}
	return n
}
