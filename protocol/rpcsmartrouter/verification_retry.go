package rpcsmartrouter

import (
	"context"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
)

// Verification retries (MAG-3950): each check a provider is failing is retried on its
// own backoff rather than at a fixed interval or the next epoch, and a retry re-runs
// only the checks that are due.
const (
	defaultVerificationRetryDelay    = 30 * time.Second
	defaultVerificationRetryMaxDelay = 15 * time.Minute
	// verificationRetryJitter extends each wait by up to +20%, as the rate-limit
	// hold-off does, so routers that lost the same vendor do not retry it in step.
	verificationRetryJitter = 0.2
)

// VerificationRetryDelay is how long a check a provider is failing waits for its first
// retry. Every pass that still finds it failing doubles the wait, up to
// VerificationRetryMaxDelay; a pass that finds it passing clears it. Flag-bound
// (--verification-retry-delay); tests shorten it.
var VerificationRetryDelay = defaultVerificationRetryDelay

// VerificationRetryMaxDelay caps the doubling (--verification-retry-max-delay). The
// epoch re-verifier passes at the same cadence by default, and a pass of either counts
// as an attempt, so at the cap a failing check is re-run about once an epoch.
var VerificationRetryMaxDelay = defaultVerificationRetryMaxDelay

// verificationRetryDelayFor is the wait after the attempts-th pass in a row that found a
// check failing: VerificationRetryDelay, doubled for each pass after the first,
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

// retrySchedule is one chain's pending verification retries, one entry per provider,
// tier and failing check: each check waits on its own backoff, and a retry re-runs only
// the checks that are due (MAG-4505 C4). Its own lock, not the router's: the retry loop
// reads it while it sleeps.
type retrySchedule struct {
	mu      sync.Mutex
	entries map[retryEntryKey]*retryEntry
	// wake is signalled whenever an entry is added or brought forward, so a sleeping
	// loop re-reads the earliest due time instead of sleeping past it.
	wake   chan struct{}
	now    func() time.Time
	jitter func() float64
}

// retryEntryKey is one provider's check on the schedule. The whole pass (the zero
// CheckID) stands for a failure no single check owns: a url that could not be dialed,
// or a provider whose pass gave no verdicts to read.
type retryEntryKey struct {
	tier  reverifyTier
	name  string
	check chainlib.CheckID
}

// retryEntry is one check's place on the schedule.
type retryEntry struct {
	retryEntryKey
	// attempts is how many passes in a row have found the check failing.
	attempts int
	// since is when it started failing; due is when it is next retried.
	since, due time.Time
}

func newRetrySchedule() *retrySchedule {
	return &retrySchedule{entries: map[retryEntryKey]*retryEntry{}, wake: make(chan struct{}, 1), now: time.Now, jitter: rand.Float64}
}

func retryKey(tier reverifyTier, name string) string { return tier.String() + "|" + name }

// reconcile moves a provider's checks on the schedule after a pass that ran the checks
// in ran, all of them when ran is nil, and found the checks in failing failing.
//
// A check the pass ran that still fails waits longer (attempts doubles its delay), and
// one it ran that passes leaves the schedule. A failing check the pass did not run kept
// its verdict from an earlier pass and keeps its place; one not on the schedule yet
// joins it. With count false, a pass that ran everything does not push back the checks
// already waiting: the dark-chain passes do not count as attempts. No check is retried
// before notBefore, when a rate-limit hold-off ends.
//
// The checks a pass reschedules share one jitter, so the checks that failed together
// stay due together and are retried in one pass, until their attempts part.
func (s *retrySchedule) reconcile(tier reverifyTier, name string, ran, failing []chainlib.CheckID, notBefore time.Time, count bool) {
	if s == nil {
		return
	}
	if slices.ContainsFunc(ran, chainlib.CheckID.WholePass) {
		ran = nil // the whole pass ran every check
	}
	s.mu.Lock()
	now, jitter := s.now(), s.jitter()
	schedule := func(check chainlib.CheckID, again bool) {
		key := retryEntryKey{tier: tier, name: name, check: check}
		entry := s.entries[key]
		if entry != nil && !again {
			return
		}
		if entry == nil {
			entry = &retryEntry{retryEntryKey: key, since: now}
			s.entries[key] = entry
		}
		entry.attempts++
		entry.due = now.Add(verificationRetryDelayFor(entry.attempts, jitter))
		if notBefore.After(entry.due) {
			entry.due = notBefore
		}
	}
	for key := range s.entries {
		if key.tier != tier || key.name != name || slices.Contains(failing, key.check) {
			continue
		}
		if ran == nil || slices.Contains(ran, key.check) {
			delete(s.entries, key)
		}
	}
	for _, check := range failing {
		schedule(check, (ran == nil && count) || slices.Contains(ran, check))
	}
	s.mu.Unlock()
	s.signal()
}

// postpone pushes back a provider's checks in ran, all of them when ran is nil, after a
// pass a rate limit kept from answering: each counts an attempt, and none is retried
// before notBefore. A provider with nothing on the schedule joins it with the whole pass.
func (s *retrySchedule) postpone(tier reverifyTier, name string, ran []chainlib.CheckID, notBefore time.Time) {
	if s == nil {
		return
	}
	checks := ran
	if len(checks) == 0 || slices.ContainsFunc(checks, chainlib.CheckID.WholePass) {
		checks = s.checksOf(tier, name)
	}
	if len(checks) == 0 {
		checks = []chainlib.CheckID{{}}
	}
	s.reconcile(tier, name, checks, checks, notBefore, true)
}

// checksOf lists a provider's checks on the schedule.
func (s *retrySchedule) checksOf(tier reverifyTier, name string) []chainlib.CheckID {
	s.mu.Lock()
	defer s.mu.Unlock()
	var checks []chainlib.CheckID
	for key := range s.entries {
		if key.tier == tier && key.name == name {
			checks = append(checks, key.check)
		}
	}
	return checks
}

// ensure puts a provider on the schedule with the whole pass, due now, unless it has a
// check on it already.
func (s *retrySchedule) ensure(tier reverifyTier, name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	for key := range s.entries {
		if key.tier == tier && key.name == name {
			s.mu.Unlock()
			return
		}
	}
	now := s.now()
	key := retryEntryKey{tier: tier, name: name}
	s.entries[key] = &retryEntry{retryEntryKey: key, since: now, due: now}
	s.mu.Unlock()
	s.signal()
}

// forget drops every check of a provider.
func (s *retrySchedule) forget(tier reverifyTier, name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	for key := range s.entries {
		if key.tier == tier && key.name == name {
			delete(s.entries, key)
		}
	}
	s.mu.Unlock()
}

// due lists, per tier and provider, the checks whose retry is due.
func (s *retrySchedule) due() dueChecks {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	out := dueChecks{}
	for key, entry := range s.entries {
		if entry.due.After(now) {
			continue
		}
		if out[key.tier] == nil {
			out[key.tier] = map[string][]chainlib.CheckID{}
		}
		out[key.tier][key.name] = append(out[key.tier][key.name], key.check)
	}
	return out
}

// dueChecks is, per tier and provider, the checks whose retry is due.
type dueChecks map[reverifyTier]map[string][]chainlib.CheckID

// of is one tier's due checks by provider, nil when none is due.
func (d dueChecks) of(tier reverifyTier) map[string][]chainlib.CheckID { return d[tier] }

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
// process (MAG-3950). Each check a provider is failing, in the pairing or out of it, is
// retried on its own backoff (retrySchedule), and a retry re-runs only the provider's
// checks that are due. While the chain has nothing serving, the whole failed list is
// verified whole on the dark cadence instead: a dark chain cannot serve at all, so every
// second of delay is downtime, and those passes do not push a check's own retry back.
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
	schedule := rpsr.retryScheduleFor(chainKey)
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
			rpsr.retryOutOfPairing(ctx, chainKey, chainParser, rpcEndpoint, convertProvidersToSessions, static, backup, nil, false)
			darkBackoff = min(2*darkBackoff, retryMaxInterval)
			continue
		}
		darkBackoff = retryDarkBaseInterval
		due := schedule.due()
		static, backup := rpsr.failedProvidersOf(chainKey, due)
		pairedStatic, pairedBackup := rpsr.pairedProvidersOf(chainKey, due)
		// A due entry neither out of the pairing nor in it is not configured any more.
		for tier, names := range due {
			for name := range names {
				if !containsName(static, backup, tier, name) && !containsName(pairedStatic, pairedBackup, tier, name) {
					schedule.forget(tier, name)
				}
			}
		}
		if len(static)+len(backup) > 0 {
			rpsr.retryOutOfPairing(ctx, chainKey, chainParser, rpcEndpoint, convertProvidersToSessions, static, backup, due, true)
		}
		if len(pairedStatic)+len(pairedBackup) > 0 {
			rpsr.retryPaired(ctx, chainKey, pairedStatic, pairedBackup, due)
		}
	}
}

// pairedProvidersOf is the configured providers in the due set that are in the pairing.
func (rpsr *RPCSmartRouter) pairedProvidersOf(chainKey string, due dueChecks) (static, backup []*lavasession.RPCStaticProviderEndpoint) {
	rpsr.mu.Lock()
	defer rpsr.mu.Unlock()
	inputs := rpsr.reverifyInputs[chainKey]
	if inputs == nil {
		return nil, nil
	}
	pick := func(tier reverifyTier, sessions map[uint64]*lavasession.ConsumerSessionsWithProvider) []*lavasession.RPCStaticProviderEndpoint {
		active := byName(sessions)
		var out []*lavasession.RPCStaticProviderEndpoint
		for _, p := range inputs.configuredFor(tier) {
			_, isDue := due[tier][p.Name]
			_, isActive := active[p.Name]
			if isDue && isActive {
				out = append(out, p)
			}
		}
		return out
	}
	return pick(reverifyTierStatic, rpsr.providerSessions[chainKey]), pick(reverifyTierBackup, rpsr.backupProviderSessions[chainKey])
}

// retryPaired re-verifies providers that serve with something failing — a refused
// service or url, a url on its first strike, a failed pass under the demote grace — and
// applies the verdicts as the epoch pass does, without waiting for it. Each provider
// re-runs only its checks in due, and keeps its other verdicts. The probes run
// without the router lock, which an epoch pass holds across its own: at a retry's
// cadence that would stall every chain behind one provider's timeout. The verdicts are
// applied under it (decideReverification), and the pairing is pushed only when it moved:
// UpdateAllProviders resets the session manager's per-epoch state.
func (rpsr *RPCSmartRouter) retryPaired(ctx context.Context, chainKey string, static, backup []*lavasession.RPCStaticProviderEndpoint, due dueChecks) {
	rpsr.mu.Lock()
	inputs := rpsr.reverifyInputs[chainKey]
	var lastStatic, lastBackup map[string]chainlib.ProviderAdmission
	if inputs != nil {
		lastStatic, lastBackup = inputs.lastPassesOf(reverifyTierStatic, static), inputs.lastPassesOf(reverifyTierBackup, backup)
	}
	rpsr.mu.Unlock()
	if inputs == nil {
		return
	}
	probedStatic := probeReverification(ctx, inputs, static, due.of(reverifyTierStatic), lastStatic)
	probedBackup := probeReverification(ctx, inputs, backup, due.of(reverifyTierBackup), lastBackup)

	rpsr.mu.Lock()
	epoch := rpsr.epochTimer.GetCurrentEpoch()
	nextStatic, demotedStatic, promotedStatic := decideReverification(inputs, rpsr.providerSessions[chainKey], reverifyTierStatic, epoch, probedStatic, true)
	nextBackup, demotedBackup, promotedBackup := decideReverification(inputs, rpsr.backupProviderSessions[chainKey], reverifyTierBackup, epoch, probedBackup, true)
	queueDemoted(&rpsr.failedStaticProviders, chainKey, inputs.configuredStatic, demotedStatic, promotedStatic)
	queueDemoted(&rpsr.failedBackupProviders, chainKey, inputs.configuredBackup, demotedBackup, promotedBackup)
	if len(demotedStatic)+len(demotedBackup)+len(promotedStatic)+len(promotedBackup) == 0 {
		// Nothing moved in the pairing; what a provider is failing may have.
		rpsr.publishVerificationFailedLocked(chainKey, inputs)
		rpsr.mu.Unlock()
		return
	}
	after, err := rpsr.commitPairingLocked(chainKey, epoch, nextStatic, nextBackup, slices.Concat(demotedStatic, demotedBackup))
	rpsr.mu.Unlock()
	if err != nil {
		utils.LavaFormatWarning("retry: failed to update the pairing", err, utils.LogAttr("chainKey", chainKey))
	}
	after()
}

// lastPassesOf is, by name, the pass that failed each of these providers when it is the
// provider's last one: a provider still serving under the demote grace failed its last
// pass, and a retry keeps its verdicts from that pass, not from the older admission it
// serves on. Callers hold RPCSmartRouter.mu.
func (inputs *chainReverifyInputs) lastPassesOf(tier reverifyTier, providers []*lavasession.RPCStaticProviderEndpoint) map[string]chainlib.ProviderAdmission {
	passes := map[string]chainlib.ProviderAdmission{}
	for _, p := range providers {
		if pass, ok := inputs.lastPass[retryKey(tier, p.Name)]; ok {
			passes[p.Name] = pass
		}
	}
	return passes
}

// retryScheduleFor is a chain's retry schedule, nil before its inputs are registered.
func (rpsr *RPCSmartRouter) retryScheduleFor(chainKey string) *retrySchedule {
	rpsr.mu.Lock()
	defer rpsr.mu.Unlock()
	if inputs := rpsr.reverifyInputs[chainKey]; inputs != nil {
		return inputs.retries
	}
	return nil
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
func (rpsr *RPCSmartRouter) failedProvidersOf(chainKey string, due dueChecks) (static, backup []*lavasession.RPCStaticProviderEndpoint) {
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

// retryOutOfPairing re-verifies providers that are out of the pairing and re-admits the
// ones that pass. Each provider re-runs only its checks in due, and keeps its other
// verdicts from the pass that refused it; with due nil, every provider is verified
// whole. revalidateTier moves each one on the schedule. A dark-chain pass (countAttempt
// false) does not push back the checks already waiting.
func (rpsr *RPCSmartRouter) retryOutOfPairing(
	ctx context.Context,
	chainKey string,
	chainParser chainlib.ChainParser,
	rpcEndpoint *lavasession.RPCEndpoint,
	convertProvidersToSessions func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider,
	static, backup []*lavasession.RPCStaticProviderEndpoint,
	due dueChecks,
	countAttempt bool,
) {
	utils.LavaFormatInfo("Retrying failed providers",
		utils.LogAttr("chain", rpcEndpoint.ChainID),
		utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
		utils.LogAttr("failedStatic", len(static)),
		utils.LogAttr("failedBackup", len(backup)),
	)
	recoveredStatic, _ := rpsr.revalidateTier(ctx, static, chainParser, rpcEndpoint, reverifyTierStatic, due.of(reverifyTierStatic), countAttempt)
	recoveredBackup, _ := rpsr.revalidateTier(ctx, backup, chainParser, rpcEndpoint, reverifyTierBackup, due.of(reverifyTierBackup), countAttempt)
	rpsr.readmitRecoveredProviders(chainKey, rpcEndpoint, convertProvidersToSessions, recoveredStatic, recoveredBackup)
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
