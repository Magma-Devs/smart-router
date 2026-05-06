package rpcsmartrouter

import (
	"context"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
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
}

// applyReverification revalidates configured providers for one tier and
// reconciles the result against the freshly-built active map. It does two
// things, in order:
//
//   - Demote: drop entries from `fresh` whose provider failed validation
//     (these were active last cycle but are no longer healthy). The demoted
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
// no UpdateAllProviders, no tracker reconcile; updateEpoch owns those.
//
// Configured lists are pre-filtered by chain+ApiInterface at startup (see
// relevantStaticProviderList in rpcsmartrouter.go), so no further filter is
// needed here.
func applyReverification(
	ctx context.Context,
	inputs *chainReverifyInputs,
	fresh map[uint64]*lavasession.ConsumerSessionsWithProvider,
	tier reverifyTier,
	epoch uint64,
) (map[uint64]*lavasession.ConsumerSessionsWithProvider, []*lavasession.ConsumerSessionsWithProvider) {
	var configured []*lavasession.RPCStaticProviderEndpoint
	switch tier {
	case reverifyTierStatic:
		configured = inputs.configuredStatic
	case reverifyTierBackup:
		configured = inputs.configuredBackup
	}
	if len(configured) == 0 {
		return fresh, nil
	}
	validate := inputs.validateFn
	if validate == nil {
		validate = func(c context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
			return validateProvider(c, p, inputs.chainParser, SpecReVerifyAttemptTimeout)
		}
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
	for i, p := range configured {
		err := results[i]
		_, wasActive := activeNames[p.Name]
		if err == nil {
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
		if wasActive {
			utils.LavaFormatWarning("re-verify: demoting active "+tier.String(), err,
				utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
				utils.LogAttr("provider", p.Name),
			)
		} else {
			utils.LavaFormatDebug("re-verify: failed-init "+tier.String()+" still failing",
				utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
				utils.LogAttr("provider", p.Name),
				utils.LogAttr("err", err.Error()),
			)
		}
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
	}

	return next, demoted
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

// validateProvider runs a single spec-verification pass against one provider.
// It builds a fresh ChainRouter + ChainFetcher under a bounded attempt context
// (so a hung upstream cannot stall a whole reconcile cycle), calls Validate,
// and tears the temporary resources down regardless of outcome.
func validateProvider(
	ctx context.Context,
	provider *lavasession.RPCStaticProviderEndpoint,
	chainParser chainlib.ChainParser,
	timeout time.Duration,
) error {
	// Expand addon URLs the same way startup PHASE 1 does — chain_router.go
	// requires both with-addon and without-addon routes for routing flexibility.
	verificationNodeUrls := make([]common.NodeUrl, 0, len(provider.NodeUrls)*2)
	for _, nodeUrl := range provider.NodeUrls {
		verificationNodeUrls = append(verificationNodeUrls, nodeUrl)
		if len(nodeUrl.Addons) > 0 {
			noAddonUrl := nodeUrl
			noAddonUrl.Addons = []string{}
			verificationNodeUrls = append(verificationNodeUrls, noAddonUrl)
		}
	}

	verificationEndpoint := &lavasession.RPCProviderEndpoint{
		NetworkAddress: provider.NetworkAddress,
		ChainID:        provider.ChainID,
		ApiInterface:   provider.ApiInterface,
		Geolocation:    provider.Geolocation,
		NodeUrls:       verificationNodeUrls,
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

	parallelConnections := uint(lavasession.DefaultMaximumStreamsOverASingleConnection)
	verificationRouter, err := chainlib.GetChainRouter(attemptCtx, parallelConnections, verificationEndpoint, validationParser)
	if err != nil {
		return err
	}

	verificationFetcher := chainlib.NewChainFetcher(attemptCtx, &chainlib.ChainFetcherOptions{
		ChainRouter: verificationRouter,
		ChainParser: validationParser,
		Endpoint:    verificationEndpoint,
		Cache:       nil,
	})

	return verificationFetcher.Validate(attemptCtx)
}
