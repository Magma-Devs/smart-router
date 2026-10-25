package rpcsmartrouter

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
)

// Verification retries (MAG-3950): a provider that is failing a verification is
// retried on its own backoff rather than at a fixed interval or the next epoch.
const (
	defaultVerificationRetryDelay    = 30 * time.Second
	defaultVerificationRetryMaxDelay = 15 * time.Minute
	// verificationRetryJitter extends each wait by up to +20%, as the rate-limit
	// hold-off does, so routers that lost the same vendor do not retry it in step.
	verificationRetryJitter = 0.2
)

// VerificationRetryDelay is how long a provider that is failing a verification waits
// for its first retry. Every pass that still finds it failing doubles the wait, up to
// VerificationRetryMaxDelay; a pass that finds nothing failing clears it. Flag-bound
// (--verification-retry-delay); tests shorten it.
var VerificationRetryDelay = defaultVerificationRetryDelay

// VerificationRetryMaxDelay caps the doubling (--verification-retry-max-delay). The
// epoch re-verifier passes at the same cadence by default, and a pass of either counts
// as an attempt, so at the cap a provider is verified about once an epoch.
var VerificationRetryMaxDelay = defaultVerificationRetryMaxDelay

// verificationRetryDelayFor is the wait after the attempts-th pass in a row that left a
// provider failing: VerificationRetryDelay, doubled for each pass after the first,
// capped at VerificationRetryMaxDelay, then extended by jitter (0 to 1) times 20%.
func verificationRetryDelayFor(attempts int, jitter float64) time.Duration {
	delay := VerificationRetryDelay
	if delay <= 0 {
		delay = defaultVerificationRetryDelay
	}
	ceiling := max(VerificationRetryMaxDelay, delay)
	for i := 1; i < attempts && delay < ceiling; i++ {
		delay *= 2
	}
	delay = min(delay, ceiling)
	return delay + time.Duration(jitter*verificationRetryJitter*float64(delay))
}

// retrySchedule is one chain's pending verification retries, one entry per provider and
// tier. Its own lock, not the router's: the retry loop reads it while it sleeps.
type retrySchedule struct {
	mu      sync.Mutex
	entries map[string]*retryEntry
	// wake is signalled whenever an entry is added or brought forward, so a sleeping
	// loop re-reads the earliest due time instead of sleeping past it.
	wake   chan struct{}
	now    func() time.Time
	jitter func() float64
}

// retryEntry is one provider's place on the schedule.
type retryEntry struct {
	tier reverifyTier
	name string
	// attempts is how many passes in a row have found it failing.
	attempts int
	// since is when it started failing; due is when it is next retried.
	since, due time.Time
}

func newRetrySchedule() *retrySchedule {
	return &retrySchedule{entries: map[string]*retryEntry{}, wake: make(chan struct{}, 1), now: time.Now, jitter: rand.Float64}
}

func retryKey(tier reverifyTier, name string) string { return tier.String() + "|" + name }

// failing records a pass that found the provider failing and schedules its next retry,
// never before notBefore (when a rate-limit hold-off ends).
func (s *retrySchedule) failing(tier reverifyTier, name string, notBefore time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	now := s.now()
	key := retryKey(tier, name)
	entry := s.entries[key]
	if entry == nil {
		entry = &retryEntry{tier: tier, name: name, since: now}
		s.entries[key] = entry
	}
	entry.attempts++
	entry.due = now.Add(verificationRetryDelayFor(entry.attempts, s.jitter()))
	if notBefore.After(entry.due) {
		entry.due = notBefore
	}
	s.mu.Unlock()
	s.signal()
}

// ensure puts a provider on the schedule, due now, unless it is on it already.
func (s *retrySchedule) ensure(tier reverifyTier, name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	key := retryKey(tier, name)
	_, known := s.entries[key]
	if !known {
		now := s.now()
		s.entries[key] = &retryEntry{tier: tier, name: name, since: now, due: now}
	}
	s.mu.Unlock()
	if !known {
		s.signal()
	}
}

// passed drops a provider whose pass found nothing failing.
func (s *retrySchedule) passed(tier reverifyTier, name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.entries, retryKey(tier, name))
	s.mu.Unlock()
}

// due lists, per tier, the providers whose retry is due.
func (s *retrySchedule) due() map[reverifyTier]map[string]struct{} {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	out := map[reverifyTier]map[string]struct{}{}
	for _, entry := range s.entries {
		if entry.due.After(now) {
			continue
		}
		if out[entry.tier] == nil {
			out[entry.tier] = map[string]struct{}{}
		}
		out[entry.tier][entry.name] = struct{}{}
	}
	return out
}

// next is how long until the earliest retry is due, and false when nothing is pending.
func (s *retrySchedule) next() (time.Duration, bool) {
	if s == nil {
		return 0, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var earliest time.Time
	for _, entry := range s.entries {
		if earliest.IsZero() || entry.due.Before(earliest) {
			earliest = entry.due
		}
	}
	if earliest.IsZero() {
		return 0, false
	}
	return max(0, earliest.Sub(s.now())), true
}

// snapshot copies the schedule, for the debug endpoint.
func (s *retrySchedule) snapshot() []retryEntry {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]retryEntry, 0, len(s.entries))
	for _, entry := range s.entries {
		out = append(out, *entry)
	}
	return out
}

func (s *retrySchedule) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// heldOffUntil is when a provider's rate-limit hold-off ends, the zero time when it is
// not held off. A retry is never sent before it.
func heldOffUntil(registry *holdoff.Registry, p *lavasession.RPCStaticProviderEndpoint) time.Time {
	if registry == nil {
		registry = holdoff.Shared
	}
	return registry.ReadyAt(p.Name, holdoffURLKey(p))
}

// runVerificationRetries retries one chain's failing verifications for the life of the
// process (MAG-3950). Each provider out of the pairing is retried on its own backoff
// (retrySchedule). While the chain has nothing serving, the whole failed list is retried
// on the dark cadence instead: a dark chain cannot serve at all, so every second of
// delay is downtime, and those passes do not push a provider's own retry back.
//
// It runs from boot whether or not boot had failures, so a provider the epoch demotes
// later is retried too; the loop it replaces ran only after a boot with failures and
// stopped once its lists were empty.
func (rpsr *RPCSmartRouter) runVerificationRetries(
	ctx context.Context,
	chainKey string,
	chainParser chainlib.ChainParser,
	rpcEndpoint *lavasession.RPCEndpoint,
	convertProvidersToSessions func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider,
) {
	schedule, registry := rpsr.retryScheduleFor(chainKey)
	if schedule == nil {
		// A chain whose inputs are not registered (unit tests) keeps its own.
		schedule = newRetrySchedule()
	}
	darkBackoff := retryDarkBaseInterval
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		dark, wait, idle := rpsr.nextVerificationRetry(chainKey, schedule, darkBackoff)
		var fire <-chan time.Time
		if !idle {
			timer.Reset(wait)
			fire = timer.C
		}
		select {
		case <-ctx.Done():
			return
		case <-schedule.wake:
			timer.Stop()
			continue
		case <-fire:
		}
		if dark {
			static, backup := rpsr.failedProvidersOf(chainKey, nil)
			rpsr.retryOutOfPairing(ctx, chainKey, chainParser, rpcEndpoint, convertProvidersToSessions, static, backup, schedule, registry, false)
			darkBackoff = min(2*darkBackoff, retryMaxInterval)
			continue
		}
		darkBackoff = retryDarkBaseInterval
		due := schedule.due()
		static, backup := rpsr.failedProvidersOf(chainKey, due)
		// A due entry that is out of the pairing no more passed meanwhile, on the epoch.
		for tier, names := range due {
			for name := range names {
				if !containsName(static, backup, tier, name) {
					schedule.passed(tier, name)
				}
			}
		}
		if len(static)+len(backup) > 0 {
			rpsr.retryOutOfPairing(ctx, chainKey, chainParser, rpcEndpoint, convertProvidersToSessions, static, backup, schedule, registry, true)
		}
	}
}

// retryScheduleFor is a chain's retry schedule and rate-limit hold-off registry, nil
// before its inputs are registered.
func (rpsr *RPCSmartRouter) retryScheduleFor(chainKey string) (*retrySchedule, *holdoff.Registry) {
	rpsr.mu.Lock()
	defer rpsr.mu.Unlock()
	if inputs := rpsr.reverifyInputs[chainKey]; inputs != nil {
		return inputs.retries, inputs.rateLimitHoldoff
	}
	return nil, nil
}

// nextVerificationRetry says when the loop next has work: on the dark cadence while the
// chain has nothing serving and providers out of the pairing, else when the schedule's
// earliest retry is due. idle means nothing is pending, and the loop waits to be woken.
// Every provider out of the pairing is put on the schedule here, whatever left it out.
func (rpsr *RPCSmartRouter) nextVerificationRetry(chainKey string, schedule *retrySchedule, darkBackoff time.Duration) (dark bool, wait time.Duration, idle bool) {
	rpsr.mu.Lock()
	failedStatic, failedBackup := rpsr.failedStaticProviders[chainKey], rpsr.failedBackupProviders[chainKey]
	dark = len(rpsr.providerSessions[chainKey]) == 0 && len(rpsr.backupProviderSessions[chainKey]) == 0
	rpsr.mu.Unlock()
	for _, p := range failedStatic {
		schedule.ensure(reverifyTierStatic, p.Name)
	}
	for _, p := range failedBackup {
		schedule.ensure(reverifyTierBackup, p.Name)
	}
	if dark && len(failedStatic)+len(failedBackup) > 0 {
		return true, darkBackoff, false
	}
	wait, pending := schedule.next()
	return false, wait, !pending
}

// failedProvidersOf copies a chain's failed lists, only the providers in due when it is
// non-nil.
func (rpsr *RPCSmartRouter) failedProvidersOf(chainKey string, due map[reverifyTier]map[string]struct{}) (static, backup []*lavasession.RPCStaticProviderEndpoint) {
	rpsr.mu.Lock()
	defer rpsr.mu.Unlock()
	pick := func(tier reverifyTier, failed []*lavasession.RPCStaticProviderEndpoint) []*lavasession.RPCStaticProviderEndpoint {
		var out []*lavasession.RPCStaticProviderEndpoint
		for _, p := range failed {
			if due == nil {
				out = append(out, p)
				continue
			}
			if _, isDue := due[tier][p.Name]; isDue {
				out = append(out, p)
			}
		}
		return out
	}
	return pick(reverifyTierStatic, rpsr.failedStaticProviders[chainKey]), pick(reverifyTierBackup, rpsr.failedBackupProviders[chainKey])
}

func containsName(static, backup []*lavasession.RPCStaticProviderEndpoint, tier reverifyTier, name string) bool {
	list := static
	if tier == reverifyTierBackup {
		list = backup
	}
	for _, p := range list {
		if p.Name == name {
			return true
		}
	}
	return false
}

// retryOutOfPairing re-verifies providers that are out of the pairing, re-admits the
// ones that pass, and schedules the next retry of the rest. A dark-cadence pass
// (countAttempt false) leaves their schedule as it was.
func (rpsr *RPCSmartRouter) retryOutOfPairing(
	ctx context.Context,
	chainKey string,
	chainParser chainlib.ChainParser,
	rpcEndpoint *lavasession.RPCEndpoint,
	convertProvidersToSessions func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider,
	static, backup []*lavasession.RPCStaticProviderEndpoint,
	schedule *retrySchedule,
	registry *holdoff.Registry,
	countAttempt bool,
) {
	utils.LavaFormatInfo("Retrying failed providers",
		utils.LogAttr("chain", rpcEndpoint.ChainID),
		utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
		utils.LogAttr("failedStatic", len(static)),
		utils.LogAttr("failedBackup", len(backup)),
	)
	recoveredStatic, stillFailedStatic := rpsr.revalidateTier(ctx, static, chainParser, rpcEndpoint, reverifyTierStatic)
	recoveredBackup, stillFailedBackup := rpsr.revalidateTier(ctx, backup, chainParser, rpcEndpoint, reverifyTierBackup)
	rpsr.readmitRecoveredProviders(chainKey, rpcEndpoint, convertProvidersToSessions, recoveredStatic, recoveredBackup)

	for _, tier := range []struct {
		tier                 reverifyTier
		recovered, stillFail []*lavasession.RPCStaticProviderEndpoint
	}{
		{reverifyTierStatic, recoveredStatic, stillFailedStatic},
		{reverifyTierBackup, recoveredBackup, stillFailedBackup},
	} {
		for _, p := range tier.recovered {
			schedule.passed(tier.tier, p.Name)
		}
		if !countAttempt {
			continue
		}
		for _, p := range tier.stillFail {
			schedule.failing(tier.tier, p.Name, heldOffUntil(registry, p))
		}
	}
}

// queueDemoted adds the providers an epoch pass demoted to a chain's failed list. A
// session the pass rebuilt is in demoted too, and in promoted: it is not out.
func queueDemoted(
	failedByChain *map[string][]*lavasession.RPCStaticProviderEndpoint,
	chainKey string,
	configured []*lavasession.RPCStaticProviderEndpoint,
	demoted []*lavasession.ConsumerSessionsWithProvider,
	promoted []string,
) {
	rebuilt := nameSet(promoted)
	queued := map[string]struct{}{}
	for _, p := range (*failedByChain)[chainKey] {
		queued[p.Name] = struct{}{}
	}
	for _, s := range demoted {
		name := s.PublicLavaAddress
		if _, back := rebuilt[name]; back {
			continue
		}
		if _, already := queued[name]; already {
			continue
		}
		for _, p := range configured {
			if p.Name == name {
				if *failedByChain == nil {
					*failedByChain = map[string][]*lavasession.RPCStaticProviderEndpoint{}
				}
				(*failedByChain)[chainKey] = append((*failedByChain)[chainKey], p)
				queued[name] = struct{}{}
				break
			}
		}
	}
}

// scheduleAfterEpoch counts an epoch pass as an attempt: every provider it left out of
// the pairing is retried on its backoff from now, and one it promoted is off the
// schedule.
func scheduleAfterEpoch(inputs *chainReverifyInputs, tier reverifyTier, out []*lavasession.RPCStaticProviderEndpoint, promoted []string) {
	for _, name := range promoted {
		inputs.retries.passed(tier, name)
	}
	for _, p := range out {
		inputs.retries.failing(tier, p.Name, heldOffUntil(inputs.rateLimitHoldoff, p))
	}
}
