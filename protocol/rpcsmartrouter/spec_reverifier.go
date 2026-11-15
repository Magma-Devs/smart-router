package rpcsmartrouter

import (
	"context"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/magma-Devs/smart-router/utils"
)

// SpecReVerifyConcurrency caps the number of providers validated in parallel
// per cycle. With many configured upstreams a fully serial cycle can exceed
// the epoch tick (worst case: N × SpecReVerifyAttemptTimeout). Exported for
// tests; not flag-bound.
var SpecReVerifyConcurrency = 5

// SpecReVerifyAttemptTimeout bounds a single Validate call. Not flag-bound —
// implementation detail rarely useful to operators. Exported for tests.
var SpecReVerifyAttemptTimeout = 30 * time.Second

// reverifyDemoteThreshold is how many CONSECUTIVE re-verify cycles a currently-active provider
// must fail before it is demoted out of the pairing (MAG-2445). 1 restores the old
// demote-on-first-failure behaviour.
//
// Validate is not a single probe — it already retries 3x per verification (chain_fetcher.go,
// "we give several chances for starting up"). But those retries run back-to-back with NO delay,
// so the entire budget is spent inside ~1s. Against an outage measured in seconds or minutes
// they all fail identically, which is why a 40-second blip that happened to overlap an epoch
// tick demoted the provider outright and cost it a full epoch (~15m) out of the pairing. Those
// retries were written for a COLD path (a node still booting); applyReverification reuses
// Validate on a HOT path where the failure mode is a transient outage, and there the only useful
// hysteresis is across time.
//
// This adds the missing temporal hysteresis, matching how endpoint health already works
// (MaxConsecutiveConnectionAttempts to disable, consecutiveHealthyProbes to re-enable): a blip
// must persist across two epoch boundaries to demote. Keeping a genuinely dead provider paired
// for one extra epoch is cheap — the endpoint-health path disables it within seconds and QoS
// stops routing to it, so demotion is a coarse cleanup, not the liveness mechanism.
var reverifyDemoteThreshold = 2

// reverifyTier discriminates which configured list applyReverification operates
// on. A typed enum prevents the silent miss-routing a stringly-typed tier could
// cause if a caller ever typoed "back-up" or "primary".
type reverifyTier int

const (
	reverifyTierStatic reverifyTier = iota
	reverifyTierBackup
)

func (t reverifyTier) String() string {
	switch t {
	case reverifyTierStatic:
		return "static"
	case reverifyTierBackup:
		return "backup"
	}
	return "unknown"
}

// chainReverifyInputs captures the per-chain values applyReverification needs
// each epoch tick.
type chainReverifyInputs struct {
	chainParser                chainlib.ChainParser
	rpcEndpoint                *routersession.RPCEndpoint
	convertProvidersToSessions func([]*routersession.RPCStaticProviderEndpoint) map[uint64]*routersession.ConsumerSessionsWithProvider
	configuredStatic           []*routersession.RPCStaticProviderEndpoint
	configuredBackup           []*routersession.RPCStaticProviderEndpoint
	// validateFn is the per-provider validation callback applyReverification
	// dispatches to. Production leaves it nil and applyReverification falls back
	// to validateProvider (real network probe). Tests inject a fake to exercise
	// updateEpoch's orchestration without standing up upstreams.
	validateFn func(context.Context, *routersession.RPCStaticProviderEndpoint) error
	// recordAdmission publishes a freshly computed per-collection admission so the
	// endpoint builder stops using the boot-time one. Nil in tests that do not
	// build endpoints.
	// It returns true when the admitted set actually moved, which is what tells
	// applyReverification to rebuild that provider's session.
	recordAdmission func(*routersession.RPCStaticProviderEndpoint, chainlib.ProviderAdmission) bool
	// recordReverifiedAdmission is recordAdmission for an epoch pass: a url that
	// fails for the first time keeps its place until it has failed
	// reverifyDemoteThreshold passes running. Nil falls back to recordAdmission.
	recordReverifiedAdmission func(*routersession.RPCStaticProviderEndpoint, chainlib.ProviderAdmission) bool
	// admissionFor reads the admission last published for a provider, so the
	// subscription tiers leave out the urls it refused and a rate-limited pass can
	// keep what the last one knew. Nil in tests that do not build endpoints.
	admissionFor func(*routersession.RPCStaticProviderEndpoint) chainlib.ProviderAdmission
	// refusedFor returns what was last refused to a provider that keeps serving, as the
	// metric publishes it (refusalsOf). Nil in tests that do not build endpoints.
	refusedFor func(*routersession.RPCStaticProviderEndpoint) []metrics.ProviderVerificationFailure
	// lastFailure is the check each provider last failed, keyed "<tier>|<name>" like
	// demoteFailStreak. Written on every failed verification (boot, the retry loop,
	// the epoch pass) and read only for a provider that is out of the pairing, so an
	// entry left behind by a provider that later passed is never published. Guarded
	// by RPCSmartRouter.mu; lazily allocated.
	lastFailure map[string]string
	// countFailedCheck counts one failed check run on the metric. Nil in tests that
	// do not publish metrics.
	countFailedCheck func(tier reverifyTier, provider string, failure metrics.ProviderVerificationFailure)
	// demoteFailStreak counts CONSECUTIVE failed re-verify cycles per active provider, keyed
	// "<tier>|<name>" so a name configured in both tiers cannot share a counter. It is the only
	// state that survives between epoch ticks, and it exists so a transient outage overlapping
	// one tick cannot demote (MAG-2445 — see reverifyDemoteThreshold). Cleared on success, on
	// demotion, and whenever the provider is not currently active, so a re-promoted provider
	// always starts with a full grace budget.
	//
	// Guarded by RPCSmartRouter.mu: applyReverification is called only from updateEpoch, which
	// holds that lock across both tier calls. Lazily allocated so test fixtures that build
	// chainReverifyInputs by hand need not.
	demoteFailStreak map[string]int
	// rateLimitHoldoff holds off providers that answered a probe with a rate-limit, so the
	// pass stops adding load to an upstream that just told us it is over its limit. Nil
	// falls back to the process-wide holdoff.Shared, so one vendor's cap slows every
	// chain this process probes through it; tests inject their own.
	rateLimitHoldoff *holdoff.Registry
	// urlStriking reports whether a url of the provider failed a pass it is still graced
	// for (admissionRegistry.striking). Nil in tests that do not build endpoints.
	urlStriking func(*routersession.RPCStaticProviderEndpoint) bool
	// retries is the chain's verification retry schedule (MAG-3950). Nil in tests that
	// build chainReverifyInputs by hand; every method on it is nil-safe.
	retries *retrySchedule
	// failing is what each provider is failing, with when each failure started, keyed
	// "<tier>|<name>" (trackFailures). The gauge, the transition logs and
	// /debug/verification all read it. Guarded by RPCSmartRouter.mu; lazily allocated.
	failing map[string][]metrics.TimedVerificationFailure
	// lastPass is the pass that refused each provider out of the pairing, keyed
	// "<tier>|<name>" (noteFailure). Its retry re-runs only the checks that failed there
	// and keeps the other verdicts. Dropped when the provider passes. Guarded by
	// RPCSmartRouter.mu; lazily allocated.
	lastPass map[string]chainlib.ProviderAdmission
	// verdictAt is when the probe behind each provider's last applied verdict started,
	// keyed "<tier>|<name>". A retry probe that started before it lost a race with
	// another pass, the epoch's, and is dropped: applying it would count that pass's
	// failure twice. Guarded by RPCSmartRouter.mu; lazily allocated.
	verdictAt map[string]time.Time
}

// superseded reports whether a verdict newer than a probe started at started was
// applied while it ran. Callers hold RPCSmartRouter.mu.
func (inputs *chainReverifyInputs) superseded(tier reverifyTier, name string, started time.Time) bool {
	return inputs.verdictAt[retryKey(tier, name)].After(started)
}

// appliedVerdict records that a verdict from a probe started at started was applied. A
// rate limit is no verdict, and does not count. Callers hold RPCSmartRouter.mu.
func (inputs *chainReverifyInputs) appliedVerdict(tier reverifyTier, name string, started time.Time, err error) {
	if isRateLimitFailure(err) {
		return
	}
	if inputs.verdictAt == nil {
		inputs.verdictAt = make(map[string]time.Time)
	}
	inputs.verdictAt[retryKey(tier, name)] = started
}

// checkRateLimited is the check of a provider whose only verdict so far is a rate
// limit: out of the pairing at boot because its node refused it for asking too fast.
const checkRateLimited = "rate-limited"

// noteFailure records the check a provider just failed, and the pass that refused it.
// Callers hold RPCSmartRouter.mu.
//
// A rate limit is not a failed check: the node refused to answer, so nothing is counted
// and the check the provider last failed stands, as on the epoch pass. One that has
// failed nothing else, out at boot on a 429, reads as rate-limited.
func (inputs *chainReverifyInputs) noteFailure(tier reverifyTier, name string, err error) {
	if inputs.lastFailure == nil {
		inputs.lastFailure = make(map[string]string)
	}
	key := tier.String() + "|" + name
	if pass, ok := chainlib.FailedPass(err); ok {
		if inputs.lastPass == nil {
			inputs.lastPass = make(map[string]chainlib.ProviderAdmission)
		}
		inputs.lastPass[key] = pass
	}
	if isRateLimitFailure(err) {
		if _, known := inputs.lastFailure[key]; !known {
			inputs.lastFailure[key] = checkRateLimited
		}
		return
	}
	check := failureCheck(err)
	inputs.lastFailure[key] = check
	// A retry that kept the failing check's verdict from an earlier pass ran nothing that
	// failed under it, so it counts no failed run.
	if pass, ok := chainlib.FailedPass(err); ok && !pass.Ran(check) {
		return
	}
	if inputs.countFailedCheck != nil {
		inputs.countFailedCheck(tier, name, metrics.ProviderVerificationFailure{Scope: metrics.VerificationScopeProvider, Check: check})
	}
}

// countRefusals counts each service and node url this pass refused a provider it
// admits, and each Warning check it failed, one failed run each. A retry counts only
// the checks it ran, not the verdicts it kept from an earlier pass.
func (inputs *chainReverifyInputs) countRefusals(tier reverifyTier, provider *routersession.RPCStaticProviderEndpoint, admission chainlib.ProviderAdmission) {
	if inputs.countFailedCheck == nil {
		return
	}
	for _, failure := range refusalsOf(inputs.chainParser, provider, admission.Rerun()) {
		inputs.countFailedCheck(tier, provider.Name, failure)
	}
}

// failureCheck names the check an error was charged to, "unknown" when it carries none.
func failureCheck(err error) string {
	if check := chainlib.FailedCheck(err); check != "" {
		return check
	}
	return "unknown"
}

// reverifyNodeUrls is the list a re-verify pass verifies for a provider: the urls the router
// dials (dialedNodeUrls), so a generated per-path url is its own position, as it is its own
// endpoint for relays (MAG-4214). The hold-off entries, the skip set and the pass's per-url
// results all index this one list (MAG-4732). With no chain parser (a test's validateFn) it is
// the configured list.
func reverifyNodeUrls(p *routersession.RPCStaticProviderEndpoint, chainParser chainlib.ChainParser) []common.NodeUrl {
	if chainParser == nil {
		return p.NodeUrls
	}
	return dialedNodeUrls(p.NodeUrls, chainParser, p.ApiInterface)
}

// holdoffURLKeys is the URL tier's key of each of a probed provider's dialed urls
// (reverifyNodeUrls), by position: the url itself, the key the relay path records a 429 under
// for the endpoint it dialed (its NetworkAddress, base + path for a generated url). A 429 is
// held on the url that answered it, so relay selection, which counts a provider's urls one by
// one, keeps off that url and not a sibling (MAG-4733).
func holdoffURLKeys(p *routersession.RPCStaticProviderEndpoint, chainParser chainlib.ChainParser) []string {
	urls := reverifyNodeUrls(p, chainParser)
	if len(urls) == 0 {
		return []string{p.Name}
	}
	keys := make([]string, len(urls))
	for index, url := range urls {
		keys[index] = url.Url
		if keys[index] == "" {
			keys[index] = p.Name
		}
	}
	return keys
}

// holdoffEntry is where the hold-off keeps one node url: under the provider entry's key
// (holdoff.ProviderKey) for a url that answers relays, and under the url alone for a
// websocket url — the key the WS subscription path reads and writes. Relay selection never
// counts a websocket url, so a 429 on it must not count toward the entry's escalation and
// hold the entry's https urls with it.
type holdoffEntry struct {
	provider string
	url      string
}

func holdoffEntries(providerKey string, p *routersession.RPCStaticProviderEndpoint, chainParser chainlib.ChainParser) []holdoffEntry {
	urls := holdoffURLKeys(p, chainParser)
	entries := make([]holdoffEntry, len(urls))
	for index, url := range urls {
		entries[index] = holdoffEntry{provider: providerKey, url: url}
		if socket, err := chainlib.IsUrlWebSocket(url); err == nil && socket {
			entries[index].provider = url
		}
	}
	return entries
}

// urlPassResult is what one re-verify pass learned about one node url, for the hold-off.
type urlPassResult struct {
	// throttle is the rate limit the url met, nil when it met none.
	throttle error
	// answered: the url answered with something other than a rate limit.
	answered bool
	// notAsked: the pass left the url out because it was held off.
	notAsked bool
}

// recordPassHoldoffs writes one pass's results into the hold-off. Results are merged per
// registry key first, since the same url may be configured more than once (with different
// services): a key met a rate limit when any of its urls did. Then every rate limit is
// recorded before any answer: an answer drops the entry's escalation, so a url that answered
// in this pass is free whatever the config order, and a url that answered never clears a
// 429 the same pass met on the same url. A url the pass did not ask changes nothing.
func recordPassHoldoffs(reg *holdoff.Registry, chainID, providerName string, entries []holdoffEntry, results []urlPassResult) {
	type merged struct {
		throttle error
		index    int
		answered bool
	}
	byEntry := map[holdoffEntry]*merged{}
	var order []holdoffEntry
	for index, entry := range entries {
		result := results[index]
		if result.notAsked {
			continue
		}
		m := byEntry[entry]
		if m == nil {
			m = &merged{index: index}
			byEntry[entry] = m
			order = append(order, entry)
		}
		if result.throttle != nil && m.throttle == nil {
			m.throttle, m.index = result.throttle, index
		}
		m.answered = m.answered || result.answered
	}
	for _, entry := range order {
		m := byEntry[entry]
		if m.throttle == nil {
			continue
		}
		retryAfter, _ := common.RetryAfterFrom(m.throttle)
		delay := reg.RecordRateLimit(entry.provider, entry.url, retryAfter)
		utils.FormatWarning("re-verify: provider url rate-limited, backing off", m.throttle,
			utils.LogAttr("chain", chainID),
			utils.LogAttr("provider", providerName),
			utils.LogAttr("url_index", m.index),
			utils.LogAttr("holdoff", delay.String()),
		)
	}
	for _, entry := range order {
		if m := byEntry[entry]; m.throttle == nil && m.answered {
			reg.RecordAnswer(entry.provider, entry.url)
		}
	}
}

// urlPassResults is what one probe learned per node url, for the hold-off. A url the
// pass did not ask — held off, or a retry kept its verdicts without dialing it — is
// not asked. A pass that says nothing per url (a test's validateFn) answers or limits
// every url alike; a pass that never ran (a config error) sent nothing, so it answers
// none of them.
func urlPassResults(inputs *chainReverifyInputs, result reverifyProbe, urls int, held map[int]struct{}) []urlPassResult {
	err, pass := result.err, result.admission
	perURL := inputs.validateFn == nil && pass.Verified()
	// A rate limit the pass does not tie to a url holds every url it asked, as it held
	// the provider before.
	untied := isRateLimitFailure(err) && (!perURL || pass.URLThrottle() == nil)
	results := make([]urlPassResult, urls)
	for index := range results {
		r := &results[index]
		if _, skipped := held[index]; skipped || (perURL && !pass.URLAsked(index)) {
			r.notAsked = true
			continue
		}
		switch {
		case perURL:
			r.throttle = pass.URLRateLimit(index)
			r.answered = r.throttle == nil && common.DetectConnectionError(pass.URLRefusal(index)) == nil
		case inputs.validateFn != nil:
			r.answered = common.DetectConnectionError(err) == nil && !isRateLimitFailure(err)
		}
		if r.throttle == nil && untied {
			r.throttle, r.answered = err, false
		}
	}
	return results
}

// isRateLimitFailure reports whether a failed validation was the upstream refusing us for
// asking too fast, rather than the upstream being unable to serve what it declares.
//
// The distinction is already settled elsewhere in this codebase — see the IsRateLimited
// comment on common.RelayResult: "the endpoint is healthy but busy. Callers back off but
// must not mark it unhealthy, which is why the direct-RPC availability gate excludes it."
// The relay path honours that; re-verification does too.
func isRateLimitFailure(err error) bool {
	return chainlib.IsRateLimitFailure(err)
}

// applyReverification revalidates configured providers for one tier and
// reconciles the result against the freshly-built active map. It does two
// things, in order:
//
//   - Demote: drop entries from `fresh` whose provider has now failed validation on
//     reverifyDemoteThreshold CONSECUTIVE cycles (a provider failing for the first time is kept,
//     so a transient outage overlapping one epoch tick costs nothing — MAG-2445). The demoted
//     sessions are returned so the caller can close their DirectRPCConnections
//     after UpdateAllProviders has swung over to the new map — closing inline
//     would race in-flight relays still holding a pointer to the prior map.
//   - Promote: build new sessions via inputs.convertProvidersToSessions for
//     configured providers that pass but were absent from `fresh` (returning
//     from failed-init / quarantine).
//
// The per-provider check is inputs.validateFn; production leaves it nil and we
// fall back to validateProvider (real network probe). Tests inject a fake via
// inputs.validateFn to exercise the reconciliation logic — and updateEpoch's
// orchestration around it — without standing up upstreams.
//
// Validations run in parallel, capped by SpecReVerifyConcurrency, so a worst-
// case cycle is bounded by ⌈N/conc⌉ × SpecReVerifyAttemptTimeout instead of
// N × timeout. Pure with respect to RPCSmartRouter state — no field mutation,
// no UpdateAllProviders, no tracker reconcile; updateEpoch owns those. The ONE
// exception is inputs.demoteFailStreak, the cross-cycle failure counter this
// function both reads and advances; it is guarded by RPCSmartRouter.mu, which
// updateEpoch holds across both tier calls.
//
// Configured lists are pre-filtered by chain+ApiInterface at startup (see
// relevantStaticProviderList in rpcsmartrouter.go), so no further filter is
// needed here.
//
// The third return is the names promoted this cycle. updateEpoch needs them to drop
// those providers from the failed lists — promoting here while leaving them pending
// for the retry loop produces two sessions for one provider.
func applyReverification(
	ctx context.Context,
	inputs *chainReverifyInputs,
	fresh map[uint64]*routersession.ConsumerSessionsWithProvider,
	tier reverifyTier,
	epoch uint64,
) (map[uint64]*routersession.ConsumerSessionsWithProvider, []*routersession.ConsumerSessionsWithProvider, []string) {
	configured := inputs.configuredFor(tier)
	if len(configured) == 0 {
		return fresh, nil, nil
	}
	return decideReverification(inputs, fresh, tier, epoch, probeReverification(ctx, inputs, configured, nil, nil), false)
}

// configuredFor is one tier's configured providers.
func (inputs *chainReverifyInputs) configuredFor(tier reverifyTier) []*routersession.RPCStaticProviderEndpoint {
	if tier == reverifyTierBackup {
		return inputs.configuredBackup
	}
	return inputs.configuredStatic
}

// reverifyProbe is one provider's re-verification before its verdict is applied.
type reverifyProbe struct {
	provider *routersession.RPCStaticProviderEndpoint
	// err is the verdict: nil when the provider passed.
	err error
	// admission is what this pass admitted the provider for, before the merge with the
	// last pass. validated says there is one: a test's validateFn gives none.
	admission chainlib.ProviderAdmission
	validated bool
	// started is when the probe began (chainReverifyInputs.verdictAt).
	started time.Time
	// ran is the checks the probe re-ran, nil when it verified the provider whole.
	ran []chainlib.CheckID
}

// probeReverification validates providers in parallel, capped by
// SpecReVerifyConcurrency, so a worst-case pass is bounded by ⌈N/conc⌉ ×
// SpecReVerifyAttemptTimeout instead of N × timeout. It applies no verdict and needs no
// router lock: decideReverification applies them. The rate-limit hold-off is its own
// registry, updated here:
//
// A url held off after a rate limit is not asked: the pass would send it the request it
// was told to stop sending. It reads as only rate-limited, so it keeps the verdict the
// last pass gave it, and the provider's other urls are still checked: a url refused for a
// capability problem comes back while a sibling is busy (MAG-4733). Only when every url
// is held is the probe skipped, returning the rate-limit error itself, so the
// reconciliation reaches the same inconclusive verdict it would have from a fresh 429 —
// membership unchanged, streak untouched — without spending a request to learn it.
//
// Each url is held or cleared on its own key (recordPassHoldoffs): a 429 holds the url
// that answered it. Any other answer clears the url: an upstream that is answering us
// again, even to report a genuine capability failure, is no longer refusing us for load.
// A url that got no answer (timeout, refused or reset connection, DNS) clears nothing: a
// gateway enforcing a ban often drops the connection rather than answering, and the
// url's entries are shared — RecordAnswer also drops the per-url chain tracker's
// hold-off and strikes (MAG-4165).
//
// With only, a provider named in it re-runs only those checks and keeps its other
// verdicts from its last pass: previous's, else the admission last recorded for it (a
// retry, MAG-3950). Nil only verifies every provider whole, as the epoch pass does.
func probeReverification(
	ctx context.Context,
	inputs *chainReverifyInputs,
	providers []*routersession.RPCStaticProviderEndpoint,
	only map[string][]chainlib.CheckID,
	previous map[string]chainlib.ProviderAdmission,
) []reverifyProbe {
	registry := inputs.rateLimitHoldoff
	if registry == nil {
		registry = holdoff.Shared
	}
	probe := func(c context.Context, p *routersession.RPCStaticProviderEndpoint) reverifyProbe {
		result := reverifyProbe{provider: p, started: time.Now(), ran: only[p.Name]}
		// The provider on this chain and api-interface, as relay selection keys it.
		providerKey := holdoff.ProviderKey(inputs.rpcEndpoint.ChainID, inputs.rpcEndpoint.ApiInterface, p.Name)
		entries := holdoffEntries(providerKey, p, inputs.chainParser)
		held := map[int]struct{}{}
		for index, entry := range entries {
			if registry.HeldOff(entry.provider, entry.url) {
				held[index] = struct{}{}
			}
		}
		if len(held) == len(entries) {
			utils.FormatDebug("re-verify: every url of the provider held off after a rate-limit, skipping probe",
				utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
				utils.LogAttr("provider", p.Name),
			)
			result.err = common.StatusCodeError429
			return result
		}
		if inputs.validateFn != nil {
			result.err = inputs.validateFn(c, p)
		} else {
			// Per-collection here too (MAG-3326). With the all-or-nothing Validate
			// this path re-probed the UNSTRIPPED config every tick, failed on the
			// same refused service, and demoted the whole provider once
			// demoteFailStreak reached reverifyDemoteThreshold — undoing the boot
			// admission about two ticks after boot, into a worse state than before.
			last, failed := previous[p.Name]
			if !failed && inputs.admissionFor != nil {
				last = inputs.admissionFor(p)
			}
			result.admission, result.err = revalidateProviderCollections(c, p, inputs.chainParser, SpecReVerifyAttemptTimeout, last, result.ran, held)
			result.validated = result.err == nil
		}
		recordPassHoldoffs(registry, inputs.rpcEndpoint.ChainID, p.Name, entries, urlPassResults(inputs, result, len(entries), held))
		return result
	}

	// WaitGroup + buffered-channel semaphore. Replaces an earlier errgroup —
	// the goroutines never return non-nil errors (results are stored in
	// `results[i]`), so errgroup's first-error cancellation was inert. Plain
	// WaitGroup makes that contract explicit and removes the trap of a future
	// edit accidentally short-circuiting validation.
	results := make([]reverifyProbe, len(providers))
	var wg sync.WaitGroup
	sem := make(chan struct{}, SpecReVerifyConcurrency)
	for i, p := range providers {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = probe(ctx, p)
		}()
	}
	wg.Wait()
	return results
}

// decideReverification applies one tier's probes and reconciles them against the
// pairing in fresh: demote, promote, rebuild, as applyReverification describes. Callers
// hold RPCSmartRouter.mu. Each verdict also moves the provider's checks on the retry
// schedule (MAG-3950): each check it is failing, out of the pairing or serving, is
// retried on its own backoff, and each one that passed leaves it.
//
// A probe that another pass overtook (chainReverifyInputs.superseded) is dropped, and
// its provider is left as it is. With carry, so is every provider in fresh that was not
// probed: a retry probes only the providers that are due.
func decideReverification(
	inputs *chainReverifyInputs,
	fresh map[uint64]*routersession.ConsumerSessionsWithProvider,
	tier reverifyTier,
	epoch uint64,
	probes []reverifyProbe,
	carry bool,
) (map[uint64]*routersession.ConsumerSessionsWithProvider, []*routersession.ConsumerSessionsWithProvider, []string) {
	activeNames := byName(fresh)
	healthyNames := make(map[string]struct{}, len(probes))
	decided := make([]reverifyProbe, 0, len(probes))
	probed := make(map[string]struct{}, len(probes))
	// Providers whose admitted service set moved during this pass. They are rebuilt
	// below rather than left with the endpoints they were built with.
	admissionMoved := map[string]struct{}{}
	for _, probe := range probes {
		p := probe.provider
		probed[p.Name] = struct{}{}
		if inputs.superseded(tier, p.Name, probe.started) {
			if _, active := activeNames[p.Name]; active {
				healthyNames[p.Name] = struct{}{}
			}
			continue
		}
		inputs.appliedVerdict(tier, p.Name, probe.started, probe.err)
		decided = append(decided, probe)
		if probe.err != nil || !probe.validated {
			continue
		}
		// Recording the fresh admission on every pass is what keeps a refusal from
		// outliving the failure that caused it: a service that recovers is re-admitted
		// at the next pass instead of staying refused until the process restarts.
		//
		// Counted before the merge below: what it keeps from the last pass was not
		// failed by this one. A url or service that was only rate-limited keeps what the
		// last pass knew.
		inputs.countRefusals(tier, p, probe.admission)
		admission := probe.admission
		if admission.Throttled() != nil && inputs.admissionFor != nil {
			admission = admission.KeepingThrottledFrom(inputs.admissionFor(p))
		}
		// The url grace keeps a url in a session it is already serving. A provider out of
		// the pairing is promoted with this pass's admission, so it comes back without
		// the url this pass refused, as it does from the failed-provider retry.
		record := inputs.recordReverifiedAdmission
		if _, active := activeNames[p.Name]; !active || record == nil {
			record = inputs.recordAdmission
		}
		if record != nil && record(p, admission) {
			admissionMoved[p.Name] = struct{}{}
		}
	}
	if carry {
		for name := range activeNames {
			if _, isProbed := probed[name]; !isProbed {
				healthyNames[name] = struct{}{}
			}
		}
	}

	var toAdmit []*routersession.RPCStaticProviderEndpoint
	if inputs.demoteFailStreak == nil {
		inputs.demoteFailStreak = make(map[string]int)
	}
	for _, probe := range decided {
		p, err := probe.provider, probe.err
		_, wasActive := activeNames[p.Name]
		streakKey := tier.String() + "|" + p.Name
		if err == nil {
			delete(inputs.demoteFailStreak, streakKey)
			_, moved := admissionMoved[p.Name]
			switch {
			case !wasActive:
				toAdmit = append(toAdmit, p)
				utils.FormatInfo("re-verify: "+tier.String()+" provider recovered",
					utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
					utils.LogAttr("provider", p.Name),
				)
			case moved:
				// Rebuild in place. Leaving it out of healthyNames demotes the stale
				// session, so the caller closes its connections after the pairing has
				// swung over, and toAdmit rebuilds it from config with the admission
				// just recorded. Re-validating alone would not do it: an active
				// session is refreshed from its OWN endpoints, so a service that has
				// recovered — or one newly refused — would not reach the pairing until
				// the provider happened to be demoted and promoted (MAG-3326 review).
				toAdmit = append(toAdmit, p)
				utils.FormatInfo("re-verify: rebuilding "+tier.String()+" provider, admitted services changed",
					utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
					utils.LogAttr("provider", p.Name),
				)
			default:
				healthyNames[p.Name] = struct{}{}
			}
			delete(inputs.lastPass, streakKey)
			inputs.retries.reconcile(tier, p.Name, probe.ran, inputs.retriesOf(p, probe.admission, nil), heldOffUntil(inputs.rateLimitHoldoff, p, inputs.chainParser), true)
			continue
		}
		inputs.rescheduleFailed(tier, p, probe.ran, probe.admission, err, true)
		if isRateLimitFailure(err) {
			// Inconclusive, not failed: the upstream refused us for asking too fast and told
			// us nothing about whether it can serve. Leave the streak untouched — advancing it
			// would let a busy vendor demote a healthy provider — and leave membership as-is:
			// an active provider stays paired, an inactive one is not promoted on no evidence.
			if wasActive {
				healthyNames[p.Name] = struct{}{}
			}
			utils.FormatWarning("re-verify: "+tier.String()+" rate-limited, treating as inconclusive", err,
				utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
				utils.LogAttr("provider", p.Name),
				utils.LogAttr("active", wasActive),
				utils.LogAttr("consecutiveFailures", inputs.demoteFailStreak[streakKey]),
			)
			continue
		}
		inputs.noteFailure(tier, p.Name, err)
		if refused, ok := chainlib.FailedPass(err); ok && wasActive && !refused.RefusalRan() {
			// A retry that kept the failing verdicts instead of running those checks again
			// found nothing new: the grace advances on the retry of what failed (MAG-3950).
			healthyNames[p.Name] = struct{}{}
			continue
		}
		if !wasActive {
			// Already out of the pairing — nothing to demote, and the streak governs only the
			// demote decision, so keep it clear (a later promote then gets a full grace budget).
			delete(inputs.demoteFailStreak, streakKey)
			utils.FormatDebug("re-verify: failed-init "+tier.String()+" still failing",
				utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
				utils.LogAttr("provider", p.Name),
				utils.LogAttr("err", err.Error()),
			)
			continue
		}

		// Active and failing: demote only once the failure has persisted across
		// reverifyDemoteThreshold consecutive passes (MAG-2445).
		inputs.demoteFailStreak[streakKey]++
		streak := inputs.demoteFailStreak[streakKey]
		if streak < reverifyDemoteThreshold {
			healthyNames[p.Name] = struct{}{} // grace: stays paired this pass
			utils.FormatWarning("re-verify: active "+tier.String()+" failed but kept — under demote threshold", err,
				utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
				utils.LogAttr("provider", p.Name),
				utils.LogAttr("consecutiveFailures", streak),
				utils.LogAttr("demoteAfter", reverifyDemoteThreshold),
			)
			continue
		}
		delete(inputs.demoteFailStreak, streakKey)
		utils.FormatWarning("re-verify: demoting active "+tier.String(), err,
			utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
			utils.LogAttr("provider", p.Name),
			utils.LogAttr("consecutiveFailures", streak),
		)
	}

	// Demote: drop fresh entries whose provider is unhealthy. Keep their
	// original keys to minimise churn for entries that survive — preserves the
	// freshen-loop's idx semantics. Demoted sessions are surfaced to the caller
	// (not closed here) so connection teardown happens *after* the session
	// manager has swung to the new pairing — see updateEpoch.
	next := make(map[uint64]*routersession.ConsumerSessionsWithProvider, len(fresh))
	var demoted []*routersession.ConsumerSessionsWithProvider
	for idx, s := range fresh {
		if _, ok := healthyNames[s.PublicAddress]; !ok {
			demoted = append(demoted, s)
			continue
		}
		next[idx] = s
	}

	// Promote: append fresh sessions for healthy providers absent from `fresh`.
	// Pick keys that don't collide with surviving entries.
	var promoted []string
	if len(toAdmit) > 0 {
		appendSessions(next, inputs.convertProvidersToSessions(toAdmit), epoch)
		promoted = make([]string, 0, len(toAdmit))
		for _, p := range toAdmit {
			promoted = append(promoted, p.Name)
		}
	}

	return next, demoted, promoted
}

// retriesOf is what a provider's pass leaves to retry (MAG-3950): the checks it found
// failing (chainlib.ProviderAdmission.Retries). A pass with no verdicts to read — a
// test's validateFn, a provider config the router cannot use — leaves the whole pass
// when it failed or left the provider degraded.
func (inputs *chainReverifyInputs) retriesOf(p *routersession.RPCStaticProviderEndpoint, admission chainlib.ProviderAdmission, err error) []chainlib.CheckID {
	if refused, ok := chainlib.FailedPass(err); ok {
		admission = refused
	}
	if checks := admission.Retries(); len(checks) > 0 {
		return checks
	}
	if err != nil || (!admission.Verified() && inputs.degraded(p, admission)) {
		return []chainlib.CheckID{{}}
	}
	return nil
}

// rescheduleFailed moves the checks of a provider whose pass failed: each one that
// failed waits longer. A pass a rate limit kept from answering at all, with no verdict
// to read, pushes back the checks it was due for past the hold-off instead. With count
// false, a pass that ran every check does not push back the checks already waiting.
func (inputs *chainReverifyInputs) rescheduleFailed(tier reverifyTier, p *routersession.RPCStaticProviderEndpoint, ran []chainlib.CheckID, admission chainlib.ProviderAdmission, err error, count bool) {
	notBefore := heldOffUntil(inputs.rateLimitHoldoff, p, inputs.chainParser)
	if _, verified := chainlib.FailedPass(err); !verified && isRateLimitFailure(err) {
		inputs.retries.postpone(tier, p.Name, ran, notBefore)
		return
	}
	inputs.retries.reconcile(tier, p.Name, ran, inputs.retriesOf(p, admission, err), notBefore, count)
}

// degraded reports whether a provider that passed still has something failing that a
// retry should look at again: a refused service or url, a url on its first strike, or a
// part only a rate limit kept from answering. A failing Warning check is not: it
// switches nothing off, and the epoch keeps its series current. It is read only for a
// pass with no verdicts to read (retriesOf).
func (inputs *chainReverifyInputs) degraded(p *routersession.RPCStaticProviderEndpoint, pass chainlib.ProviderAdmission) bool {
	if pass.Throttled() != nil {
		return true
	}
	if inputs.admissionFor != nil && inputs.admissionFor(p).Any() {
		return true
	}
	return inputs.urlStriking != nil && inputs.urlStriking(p)
}

// byName builds a name → session lookup so callers can answer
// "is this provider currently active" in O(1).
func byName(sessions map[uint64]*routersession.ConsumerSessionsWithProvider) map[string]*routersession.ConsumerSessionsWithProvider {
	out := make(map[string]*routersession.ConsumerSessionsWithProvider, len(sessions))
	for _, s := range sessions {
		out[s.PublicAddress] = s
	}
	return out
}

// closeDemotedDirectConnections releases the DirectRPCConnection objects
// attached to sessions removed by re-verification. The session manager's own
// purge path (closePurgedUnusedPairingsConnections) closes endpoint.Connections
// but not endpoint.DirectConnections — those are the smart-router-owned
// transports, and without this call they leak whenever a provider flaps active
// → demoted across an epoch.
//
// Intentionally fire-and-forget from a goroutine after UpdateAllProviders has
// returned: the new pairing is already live, so any in-flight relay holds a
// session pointer from the *new* map, and dropping the old transports is safe.
func closeDemotedDirectConnections(demoted []*routersession.ConsumerSessionsWithProvider) {
	for _, s := range demoted {
		for _, ep := range s.Endpoints {
			for _, dc := range ep.DirectConnections {
				if dc == nil {
					continue
				}
				if err := dc.Close(); err != nil {
					utils.FormatDebug("re-verify: error closing demoted direct connection",
						utils.LogAttr("provider", s.PublicAddress),
						utils.LogAttr("url", dc.GetURL()),
						utils.LogAttr("err", err.Error()),
					)
				}
			}
		}
	}
}

// BootValidateTimeout bounds a single provider validation during startup.
// Before MAG-2525 the two boot tiers disagreed: static providers got a 30s
// timeout while backups got a bare context.WithCancel, so one blackholed backup
// URL could hang bring-up indefinitely. Both tiers share this now.
var BootValidateTimeout = 30 * time.Second

// validateProviderTier validates one configured tier at startup, in parallel and
// bounded by SpecReVerifyConcurrency. It is the boot-time counterpart to
// applyReverification: same validateProvider primitive, same semaphore, but no
// promote/demote bookkeeping — at boot there is no prior pairing to reconcile
// against, only a pass/fail partition.
//
// Parallelism here is an availability property, not just a speed one. The tiers
// validate in sequence, so a serial static tier of N dead providers delayed the
// backup tier — and therefore bring-up on backups — by up to N × the timeout.
// Three dead primaries cost 90s before a healthy backup was even dialled.
//
// Returns failures twice: as a set for filtering, and as a slice in configured
// order. The slice seeds the retry loop, which must not vary with the order
// goroutines happen to finish in.
//
// `providers` is pre-filtered by chain + api-interface at the call site (see
// relevantStaticProviderList in rpcsmartrouter.go), so nothing is filtered here.
//
// `validate` is nil in production and falls back to the real network probe; tests
// inject a fake to exercise the partitioning and ordering without upstreams, the
// same seam chainReverifyInputs.validateFn provides for applyReverification.
func validateProviderTier(
	ctx context.Context,
	providers []*routersession.RPCStaticProviderEndpoint,
	rpcEndpoint *routersession.RPCEndpoint,
	chainParser chainlib.ChainParser,
	tier reverifyTier,
	validate func(context.Context, *routersession.RPCStaticProviderEndpoint) error,
	// It returns true when the admitted set actually moved, which is what tells
	// applyReverification to rebuild that provider's session.
	recordAdmission func(*routersession.RPCStaticProviderEndpoint, chainlib.ProviderAdmission) bool,
) (map[*routersession.RPCStaticProviderEndpoint]error, []*routersession.RPCStaticProviderEndpoint) {
	failedSet := make(map[*routersession.RPCStaticProviderEndpoint]error)
	if len(providers) == 0 {
		return failedSet, nil
	}
	if validate == nil {
		// Per-collection at boot, recording what each provider was admitted for.
		// Returning nil for a service-attributable failure is the point: the
		// provider stays, minus that service (MAG-3326).
		validate = func(c context.Context, p *routersession.RPCStaticProviderEndpoint) error {
			admission, err := validateProviderCollections(c, p, chainParser, BootValidateTimeout)
			if err != nil {
				return err
			}
			if recordAdmission != nil {
				recordAdmission(p, admission)
			}
			return nil
		}
	}

	utils.FormatInfo("Validating providers",
		utils.LogAttr("chain", rpcEndpoint.ChainID),
		utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
		utils.LogAttr("tier", tier.String()),
		utils.LogAttr("providerCount", len(providers)),
	)

	results := make([]error, len(providers))
	var wg sync.WaitGroup
	sem := make(chan struct{}, SpecReVerifyConcurrency)
	for i, p := range providers {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = validate(ctx, p)
		}()
	}
	wg.Wait()

	var failedOrdered []*routersession.RPCStaticProviderEndpoint
	for i, p := range providers {
		if err := results[i]; err != nil {
			failedSet[p] = err
			failedOrdered = append(failedOrdered, p)
			utils.FormatWarning("provider validation failed — excluding from provider list", err,
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("tier", tier.String()),
				utils.LogAttr("provider", p.Name),
			)
			continue
		}
		utils.FormatInfo("Provider validated successfully",
			utils.LogAttr("chain", rpcEndpoint.ChainID),
			utils.LogAttr("tier", tier.String()),
			utils.LogAttr("provider", p.Name),
		)
	}
	return failedSet, failedOrdered
}

// validateProviderCollections runs one spec-verification pass against one
// provider: every node url over its own connection, under a bounded attempt
// context so a hung upstream cannot stall a whole reconcile cycle. A failure
// refuses the service that owns it, else its url, and the provider survives with
// the rest (MAG-3326).
func validateProviderCollections(
	ctx context.Context,
	provider *routersession.RPCStaticProviderEndpoint,
	chainParser chainlib.ChainParser,
	timeout time.Duration,
) (chainlib.ProviderAdmission, error) {
	return revalidateProviderCollections(ctx, provider, chainParser, timeout, chainlib.ProviderAdmission{}, nil, nil)
}

// revalidateProviderCollections is validateProviderCollections for a retry: it re-runs
// only the checks in only and keeps the other verdicts from previous, the provider's
// last pass (chainlib.ChainFetcher.ValidateCollectionsAgain), so a retry re-runs what
// failed, not every check (MAG-3950). With only empty it verifies the provider whole.
// The node urls at the positions in skip are not asked (chainlib.ValidateCollectionsExcept).
func revalidateProviderCollections(
	ctx context.Context,
	provider *routersession.RPCStaticProviderEndpoint,
	chainParser chainlib.ChainParser,
	timeout time.Duration,
	previous chainlib.ProviderAdmission,
	only []chainlib.CheckID,
	skip map[int]struct{},
) (chainlib.ProviderAdmission, error) {
	// The urls the router dials, generated per-path ones included, each verified on
	// its own path (MAG-4214).
	endpoint := &routersession.RPCProviderEndpoint{
		NetworkAddress: provider.NetworkAddress,
		ChainID:        provider.ChainID,
		ApiInterface:   provider.ApiInterface,
		NodeUrls:       dialedNodeUrls(provider.NodeUrls, chainParser, provider.ApiInterface),
	}

	attemptCtx, attemptCancel := context.WithTimeout(ctx, timeout)
	defer attemptCancel()

	// Isolate the live chainParser from any mutation NewChainProxy might
	// perform during verification. For gRPC, NewGrpcChainProxy replaces the
	// parser's registry/codec with ones bound to the verification connection;
	// when attemptCtx is cancelled the connection dies and live gRPC relays
	// would hit a nil connector. For non-gRPC interfaces this is a no-op
	// (returns the original parser).
	validationParser := chainlib.CloneChainParserForValidation(chainParser)
	if err := chainlib.ProviderShapeError(endpoint, validationParser); err != nil {
		return chainlib.ProviderAdmission{}, &chainlib.VerificationFailure{Check: chainlib.CheckConfig, Err: err}
	}

	parallelConnections := uint(routersession.DefaultMaximumStreamsOverASingleConnection)
	nodeUrlRouter, err := chainlib.NewNodeUrlRouterFactory(parallelConnections, endpoint)
	if err != nil {
		return chainlib.ProviderAdmission{}, &chainlib.VerificationFailure{Check: chainlib.CheckConfig, Err: err}
	}

	verificationFetcher := chainlib.NewChainFetcher(attemptCtx, &chainlib.ChainFetcherOptions{
		ChainParser:   validationParser,
		Endpoint:      endpoint,
		NodeUrlRouter: nodeUrlRouter,
	})

	if len(only) > 0 {
		return verificationFetcher.ValidateCollectionsAgain(attemptCtx, previous, only, skip)
	}
	return verificationFetcher.ValidateCollectionsExcept(attemptCtx, skip)
}
