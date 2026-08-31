package rpcsmartrouter

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
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
	rpcEndpoint                *lavasession.RPCEndpoint
	convertProvidersToSessions func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider
	configuredStatic           []*lavasession.RPCStaticProviderEndpoint
	configuredBackup           []*lavasession.RPCStaticProviderEndpoint
	// validateFn is the per-provider validation callback applyReverification
	// dispatches to. Production leaves it nil and applyReverification falls back
	// to validateProvider (real network probe). Tests inject a fake to exercise
	// updateEpoch's orchestration without standing up upstreams.
	validateFn func(context.Context, *lavasession.RPCStaticProviderEndpoint) error
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
}

// holdoffURLKey is the URL tier's key for a probed provider: its first node URL, so two
// providers sharing a vendor name but hitting different endpoints are held off apart.
func holdoffURLKey(p *lavasession.RPCStaticProviderEndpoint) string {
	if len(p.NodeUrls) > 0 && p.NodeUrls[0].Url != "" {
		return p.NodeUrls[0].Url
	}
	return p.Name
}

// rateLimitTextSignatures covers the one transport where no status code exists to check.
//
// Every HTTP-family transport reaches us as common.StatusCodeError429 — ValidateStatusCodes
// mints it, each proxy propagates it as LavaFormat's cause, so Unwrap survives and errors.Is
// below is the real check. gRPC is different in kind: there is no HTTP status in the error at
// all. grpc-go reports codes.Unavailable and the vendor's 429 survives only inside the status
// description, so there is nothing structural to match on.
//
// Verbatim from a production failure. Keep this list minimal — a new entry here is usually a
// signal that some path is discarding a typed error, which is worth fixing at the source
// instead.
var rateLimitTextSignatures = []string{
	"429 (Too Many Requests)", // grpc transport: no status code, only the status description
}

// isRateLimitFailure reports whether a failed validation was the upstream refusing us for
// asking too fast, rather than the upstream being unable to serve what it declares.
//
// The distinction is already settled elsewhere in this codebase — see the IsRateLimited
// comment on common.RelayResult: "the endpoint is healthy but busy. Callers back off but
// must not mark it unhealthy, which is why the direct-RPC availability gate excludes it."
// The relay path honours that; re-verification did not, and a rate-limited probe demoted
// providers that were serving traffic perfectly well.
func isRateLimitFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, common.StatusCodeError429) {
		return true
	}
	msg := err.Error()
	for _, sig := range rateLimitTextSignatures {
		if strings.Contains(msg, sig) {
			return true
		}
	}
	return false
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
// for retryFailedProviders produces two sessions for one provider.
func applyReverification(
	ctx context.Context,
	inputs *chainReverifyInputs,
	fresh map[uint64]*lavasession.ConsumerSessionsWithProvider,
	tier reverifyTier,
	epoch uint64,
) (map[uint64]*lavasession.ConsumerSessionsWithProvider, []*lavasession.ConsumerSessionsWithProvider, []string) {
	var configured []*lavasession.RPCStaticProviderEndpoint
	switch tier {
	case reverifyTierStatic:
		configured = inputs.configuredStatic
	case reverifyTierBackup:
		configured = inputs.configuredBackup
	}
	if len(configured) == 0 {
		return fresh, nil, nil
	}
	probe := inputs.validateFn
	if probe == nil {
		probe = func(c context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
			return validateProvider(c, p, inputs.chainParser, SpecReVerifyAttemptTimeout)
		}
	}
	if inputs.rateLimitHoldoff == nil {
		inputs.rateLimitHoldoff = holdoff.Shared
	}

	// A provider that answered with a rate-limit is held off rather than re-probed. The
	// skip returns the rate-limit error itself, so the reconciliation below reaches the
	// same inconclusive verdict it would have from a fresh 429 — membership unchanged,
	// streak untouched — without spending a request to learn it. Any other outcome clears
	// the penalty: an upstream that is answering us again, even to report a genuine
	// capability failure, is no longer refusing us for load.
	validate := func(c context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
		urlKey := holdoffURLKey(p)
		if inputs.rateLimitHoldoff.HeldOff(p.Name, urlKey) {
			utils.LavaFormatDebug("re-verify: provider held off after a rate-limit, skipping probe",
				utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
				utils.LogAttr("provider", p.Name),
			)
			return common.StatusCodeError429
		}
		err := probe(c, p)
		if isRateLimitFailure(err) {
			retryAfter, _ := common.RetryAfterFrom(err)
			delay := inputs.rateLimitHoldoff.RecordRateLimit(p.Name, urlKey, retryAfter)
			utils.LavaFormatWarning("re-verify: provider rate-limited, backing off", err,
				utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
				utils.LogAttr("provider", p.Name),
				utils.LogAttr("holdoff", delay.String()),
			)
			return err
		}
		inputs.rateLimitHoldoff.RecordAnswer(p.Name, urlKey)
		return err
	}

	// WaitGroup + buffered-channel semaphore. Replaces an earlier errgroup —
	// the goroutines never return non-nil errors (results are stored in
	// `results[i]`), so errgroup's first-error cancellation was inert. Plain
	// WaitGroup makes that contract explicit and removes the trap of a future
	// edit accidentally short-circuiting validation.
	results := make([]error, len(configured))
	var wg sync.WaitGroup
	sem := make(chan struct{}, SpecReVerifyConcurrency)
	for i, p := range configured {
		i, p := i, p
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = validate(ctx, p)
		}()
	}
	wg.Wait()

	activeNames := byName(fresh)
	healthyNames := make(map[string]struct{}, len(configured))
	var toAdmit []*lavasession.RPCStaticProviderEndpoint
	if inputs.demoteFailStreak == nil {
		inputs.demoteFailStreak = make(map[string]int)
	}
	for i, p := range configured {
		err := results[i]
		_, wasActive := activeNames[p.Name]
		streakKey := tier.String() + "|" + p.Name
		if err == nil {
			delete(inputs.demoteFailStreak, streakKey)
			healthyNames[p.Name] = struct{}{}
			if !wasActive {
				toAdmit = append(toAdmit, p)
				utils.LavaFormatInfo("re-verify: "+tier.String()+" provider recovered",
					utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
					utils.LogAttr("provider", p.Name),
				)
			}
			continue
		}
		if isRateLimitFailure(err) {
			// Inconclusive, not failed: the upstream refused us for asking too fast and told
			// us nothing about whether it can serve. Leave the streak untouched — advancing it
			// would let a busy vendor demote a healthy provider — and leave membership as-is:
			// an active provider stays paired, an inactive one is not promoted on no evidence.
			if wasActive {
				healthyNames[p.Name] = struct{}{}
			}
			utils.LavaFormatWarning("re-verify: "+tier.String()+" rate-limited, treating as inconclusive", err,
				utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
				utils.LogAttr("provider", p.Name),
				utils.LogAttr("active", wasActive),
				utils.LogAttr("consecutiveFailures", inputs.demoteFailStreak[streakKey]),
			)
			continue
		}
		if !wasActive {
			// Already out of the pairing — nothing to demote, and the streak governs only the
			// demote decision, so keep it clear (a later promote then gets a full grace budget).
			delete(inputs.demoteFailStreak, streakKey)
			utils.LavaFormatDebug("re-verify: failed-init "+tier.String()+" still failing",
				utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
				utils.LogAttr("provider", p.Name),
				utils.LogAttr("err", err.Error()),
			)
			continue
		}

		// Active and failing: demote only once the failure has persisted across
		// reverifyDemoteThreshold consecutive cycles (MAG-2445).
		inputs.demoteFailStreak[streakKey]++
		streak := inputs.demoteFailStreak[streakKey]
		if streak < reverifyDemoteThreshold {
			healthyNames[p.Name] = struct{}{} // grace: stays paired this cycle
			utils.LavaFormatWarning("re-verify: active "+tier.String()+" failed but kept — under demote threshold", err,
				utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
				utils.LogAttr("provider", p.Name),
				utils.LogAttr("consecutiveFailures", streak),
				utils.LogAttr("demoteAfter", reverifyDemoteThreshold),
			)
			continue
		}
		delete(inputs.demoteFailStreak, streakKey)
		utils.LavaFormatWarning("re-verify: demoting active "+tier.String(), err,
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
	next := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(fresh))
	var demoted []*lavasession.ConsumerSessionsWithProvider
	for idx, s := range fresh {
		if _, ok := healthyNames[s.PublicLavaAddress]; !ok {
			demoted = append(demoted, s)
			continue
		}
		next[idx] = s
	}

	// Promote: append fresh sessions for healthy providers absent from `fresh`.
	// Pick keys that don't collide with surviving entries.
	var promoted []string
	if len(toAdmit) > 0 {
		nextIdx := uint64(0)
		for k := range next {
			if k >= nextIdx {
				nextIdx = k + 1
			}
		}
		for _, s := range inputs.convertProvidersToSessions(toAdmit) {
			s.Lock.Lock()
			s.PairingEpoch = epoch
			s.Lock.Unlock()
			next[nextIdx] = s
			nextIdx++
		}
		promoted = make([]string, 0, len(toAdmit))
		for _, p := range toAdmit {
			promoted = append(promoted, p.Name)
		}
	}

	return next, demoted, promoted
}

// byName builds a name → session lookup so callers can answer
// "is this provider currently active" in O(1).
func byName(sessions map[uint64]*lavasession.ConsumerSessionsWithProvider) map[string]*lavasession.ConsumerSessionsWithProvider {
	out := make(map[string]*lavasession.ConsumerSessionsWithProvider, len(sessions))
	for _, s := range sessions {
		out[s.PublicLavaAddress] = s
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
func closeDemotedDirectConnections(demoted []*lavasession.ConsumerSessionsWithProvider) {
	for _, s := range demoted {
		for _, ep := range s.Endpoints {
			for _, dc := range ep.DirectConnections {
				if dc == nil {
					continue
				}
				if err := dc.Close(); err != nil {
					utils.LavaFormatDebug("re-verify: error closing demoted direct connection",
						utils.LogAttr("provider", s.PublicLavaAddress),
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
	providers []*lavasession.RPCStaticProviderEndpoint,
	rpcEndpoint *lavasession.RPCEndpoint,
	chainParser chainlib.ChainParser,
	tier reverifyTier,
	validate func(context.Context, *lavasession.RPCStaticProviderEndpoint) (chainlib.ProviderAdmission, error),
) (map[*lavasession.RPCStaticProviderEndpoint]struct{}, []*lavasession.RPCStaticProviderEndpoint, map[*lavasession.RPCStaticProviderEndpoint]chainlib.ProviderAdmission) {
	failedSet := make(map[*lavasession.RPCStaticProviderEndpoint]struct{})
	admissions := make(map[*lavasession.RPCStaticProviderEndpoint]chainlib.ProviderAdmission)
	if len(providers) == 0 {
		return failedSet, nil, admissions
	}
	if validate == nil {
		validate = func(c context.Context, p *lavasession.RPCStaticProviderEndpoint) (chainlib.ProviderAdmission, error) {
			return validateProviderCollections(c, p, chainParser, BootValidateTimeout)
		}
	}

	utils.LavaFormatInfo("Validating providers",
		utils.LogAttr("chain", rpcEndpoint.ChainID),
		utils.LogAttr("apiInterface", rpcEndpoint.ApiInterface),
		utils.LogAttr("tier", tier.String()),
		utils.LogAttr("providerCount", len(providers)),
	)

	results := make([]error, len(providers))
	admitted := make([]chainlib.ProviderAdmission, len(providers))
	var wg sync.WaitGroup
	sem := make(chan struct{}, SpecReVerifyConcurrency)
	for i, p := range providers {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			admitted[i], results[i] = validate(ctx, p)
		}()
	}
	wg.Wait()

	var failedOrdered []*lavasession.RPCStaticProviderEndpoint
	for i, p := range providers {
		if err := results[i]; err != nil {
			failedSet[p] = struct{}{}
			failedOrdered = append(failedOrdered, p)
			utils.LavaFormatWarning("provider validation failed — excluding from provider list", err,
				utils.LogAttr("chain", rpcEndpoint.ChainID),
				utils.LogAttr("tier", tier.String()),
				utils.LogAttr("provider", p.Name),
			)
			continue
		}
		if admitted[i].Any() {
			admissions[p] = admitted[i]
		}
		utils.LavaFormatInfo("Provider validated successfully",
			utils.LogAttr("chain", rpcEndpoint.ChainID),
			utils.LogAttr("tier", tier.String()),
			utils.LogAttr("provider", p.Name),
			utils.LogAttr("addonsRefused", admitted[i].Any()),
		)
	}
	return failedSet, failedOrdered, admissions
}

// validateProvider runs a single spec-verification pass against one provider.
// It builds a fresh ChainRouter + ChainFetcher under a bounded attempt context
// (so a hung upstream cannot stall a whole reconcile cycle), calls Validate,
// and tears the temporary resources down regardless of outcome.
// verificationEndpoints splits the two endpoints a validation attempt needs.
//
// The ROUTER one expands every add-on url into a with-addon and a without-addon
// route, the same way startup PHASE 1 does, because chain_router.go requires both
// for routing flexibility.
//
// The FETCHER one carries only the urls the provider actually declared. The
// stripped copies are a routing artifact, not a claim the provider made, and
// MAG-3296 is what validating them costs: it makes every add-on node answer for
// the base collection as well. Where an add-on extends the base surface that is
// merely redundant; where it REPLACES it — Acala's `evm` against a Substrate base
// — the stripped copy asks a question the node cannot answer. Validate returns on
// the first failing url, so the copy's verdict became the provider's verdict and
// an EVM-only node was dropped for failing to be a Substrate node.
func verificationEndpoints(provider *lavasession.RPCStaticProviderEndpoint) (routerEndpoint, fetcherEndpoint *lavasession.RPCProviderEndpoint) {
	routedNodeUrls := make([]common.NodeUrl, 0, len(provider.NodeUrls)*2)
	for _, nodeUrl := range provider.NodeUrls {
		routedNodeUrls = append(routedNodeUrls, nodeUrl)
		if len(nodeUrl.Addons) > 0 {
			noAddonUrl := nodeUrl
			noAddonUrl.Addons = []string{}
			routedNodeUrls = append(routedNodeUrls, noAddonUrl)
		}
	}

	newEndpoint := func(nodeUrls []common.NodeUrl) *lavasession.RPCProviderEndpoint {
		return &lavasession.RPCProviderEndpoint{
			NetworkAddress: provider.NetworkAddress,
			ChainID:        provider.ChainID,
			ApiInterface:   provider.ApiInterface,
			NodeUrls:       nodeUrls,
		}
	}
	return newEndpoint(routedNodeUrls), newEndpoint(provider.NodeUrls)
}

func validateProvider(
	ctx context.Context,
	provider *lavasession.RPCStaticProviderEndpoint,
	chainParser chainlib.ChainParser,
	timeout time.Duration,
) error {
	routerEndpoint, fetcherEndpoint := verificationEndpoints(provider)

	attemptCtx, attemptCancel := context.WithTimeout(ctx, timeout)
	defer attemptCancel()

	// Isolate the live chainParser from any mutation NewChainProxy might
	// perform during verification. For gRPC, NewGrpcChainProxy replaces the
	// parser's registry/codec with ones bound to the verification connection;
	// when attemptCtx is cancelled the connection dies and live gRPC relays
	// would hit a nil connector. For non-gRPC interfaces this is a no-op
	// (returns the original parser).
	validationParser := chainlib.CloneChainParserForValidation(chainParser)

	parallelConnections := uint(lavasession.DefaultMaximumStreamsOverASingleConnection)
	verificationRouter, err := chainlib.GetChainRouter(attemptCtx, parallelConnections, routerEndpoint, validationParser)
	if err != nil {
		return err
	}

	verificationFetcher := chainlib.NewChainFetcher(attemptCtx, &chainlib.ChainFetcherOptions{
		ChainRouter: verificationRouter,
		ChainParser: validationParser,
		Endpoint:    fetcherEndpoint,
		Cache:       nil,
	})

	return verificationFetcher.Validate(attemptCtx)
}

// validateProviderCollections is validateProvider with per-collection admission
// (MAG-3326): an add-on whose verification fails is refused on its own, and the
// provider survives with the rest.
//
// Boot only. applyReverification keeps the all-or-nothing Validate deliberately:
// the epoch path already carries temporal hysteresis at provider granularity
// (reverifyDemoteThreshold, MAG-2445, added because a 40-second blip cost a
// provider a full epoch), and per-collection demotion there needs the same
// hysteresis per add-on before it can be safe. Without it one transient failure
// would strip an add-on until restart.
//
// TODO(MAG-3326): give the epoch path per-add-on hysteresis, then move it here too.
func validateProviderCollections(
	ctx context.Context,
	provider *lavasession.RPCStaticProviderEndpoint,
	chainParser chainlib.ChainParser,
	timeout time.Duration,
) (chainlib.ProviderAdmission, error) {
	routerEndpoint, verificationEndpoint := verificationEndpoints(provider)

	attemptCtx, attemptCancel := context.WithTimeout(ctx, timeout)
	defer attemptCancel()

	validationParser := chainlib.CloneChainParserForValidation(chainParser)
	parallelConnections := uint(lavasession.DefaultMaximumStreamsOverASingleConnection)
	verificationRouter, err := chainlib.GetChainRouter(attemptCtx, parallelConnections, routerEndpoint, validationParser)
	if err != nil {
		return chainlib.ProviderAdmission{}, err
	}

	verificationFetcher := chainlib.NewChainFetcher(attemptCtx, &chainlib.ChainFetcherOptions{
		ChainRouter: verificationRouter,
		ChainParser: validationParser,
		Endpoint:    verificationEndpoint,
		Cache:       nil,
	})

	return verificationFetcher.ValidateCollections(attemptCtx)
}
