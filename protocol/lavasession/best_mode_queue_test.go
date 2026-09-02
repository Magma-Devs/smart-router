package lavasession

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// fixedScoreOptimizer is a ProviderOptimizer whose QoS inputs are set by the test and can
// never move afterwards. Selection itself is NOT stubbed: it runs the real UpstreamSelector,
// so these tests still exercise the production scoring, the real pickBestIndex, and the real
// ignored-provider handling in CalculateUpstreamScores.
//
// The stub exists for one reason. UpdateAllProviders kicks off asynchronous probing, and
// every probe result is fed back into the optimizer. Against a live optimizer that traffic
// races the test's own seeding: measured over 80 attempts, 3 of them saw a provider's
// composite change between two consecutive reads, because a late probe sample shifted the
// adaptive latency P10-P90 band and re-ordered the ranking underneath the assertion. That
// is a genuine 1-in-25 flake, not a slow machine, so no amount of extra sleeping fixes it.
// Dropping the Append* calls on the floor removes the race at its source.
type fixedScoreOptimizer struct {
	selector *provideroptimizer.UpstreamSelector
	qos      map[string]*pairingtypes.QualityOfServiceReport
}

func newFixedScoreOptimizer(mode provideroptimizer.SelectionMode) *fixedScoreOptimizer {
	cfg := provideroptimizer.DefaultUpstreamSelectorConfig()
	cfg.SelectionMode = mode
	// Adaptive max stays OFF (the default). ConfigureUpstreamSelector would force it on,
	// which is what makes a live optimizer's scores depend on the sample history — exactly
	// the nondeterminism these tests must not inherit.
	return &fixedScoreOptimizer{
		selector: provideroptimizer.NewUpstreamSelector(cfg),
		qos:      map[string]*pairingtypes.QualityOfServiceReport{},
	}
}

// setLatency pins one provider's QoS. Availability and sync are identical for every
// provider, so latency is the only axis that separates them and the expected ranking is
// unambiguous.
func (f *fixedScoreOptimizer) setLatency(address string, latencySeconds float64) {
	f.qos[address] = &pairingtypes.QualityOfServiceReport{
		Availability: 1.0,
		Latency:      latencySeconds,
		Sync:         1.0,
	}
}

func (f *fixedScoreOptimizer) ChooseUpstreamWithStats(ctx context.Context, allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) ([]string, *provideroptimizer.SelectionStats) {
	scores, _, details := f.selector.CalculateUpstreamScores(
		allAddresses,
		ignoredProviders,
		func(addr string) (*pairingtypes.QualityOfServiceReport, time.Time, bool) {
			qos, ok := f.qos[addr]
			return qos, time.Time{}, ok
		},
		// Equal stake for everyone: in a static-provider deployment the stake term is the
		// same constant for every candidate, so it cannot influence the ordering.
		func(string) int64 { return 1 },
	)
	if len(scores) == 0 {
		return []string{}, nil
	}
	selected, stats := f.selector.SelectUpstreamWithStats(ctx, scores, details)
	return []string{selected}, stats
}

func (f *fixedScoreOptimizer) ChooseUpstream(ctx context.Context, allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) []string {
	addresses, _ := f.ChooseUpstreamWithStats(ctx, allAddresses, ignoredProviders, cu, requestedBlock)
	return addresses
}

// The Append* methods are deliberate no-ops — see the type comment.
func (f *fixedScoreOptimizer) AppendRelayFailure(string)                             {}
func (f *fixedScoreOptimizer) AppendRelayData(string, time.Duration, uint64, uint64) {}
func (f *fixedScoreOptimizer) AppendRelayDataConsensus(string, time.Duration, uint64, uint64, provideroptimizer.SyncReference) {
}

func (f *fixedScoreOptimizer) GetReputationReportForProvider(address string) (*pairingtypes.QualityOfServiceReport, time.Time) {
	return f.qos[address], time.Time{}
}

func (f *fixedScoreOptimizer) Strategy() provideroptimizer.Strategy {
	return provideroptimizer.StrategyBalanced
}
func (f *fixedScoreOptimizer) UpdateWeights(map[string]int64, uint64) {}

// newBestModeCSM wires a ConsumerSessionManager to a Best-mode optimizer and returns it
// alongside the candidate list in the order the router itself sees, plus the ranking those
// candidates should produce (fastest first).
//
// Latencies are assigned worst-first, so the strongest candidate is the LAST entry in the
// pairing list. A loop that ignored the scores and simply walked the list would return the
// exact reverse of the expectation — which is what gives these assertions teeth.
func newBestModeCSM(t *testing.T) (csm *ConsumerSessionManager, candidates, ranked []string) {
	t.Helper()
	// The package-level protocol RNG panics if used uninitialised, and UpdateAllProviders
	// reaches it while scattering probe timings. Sibling helpers do the same.
	rand.InitRandomSeed()

	opt := newFixedScoreOptimizer(provideroptimizer.SelectionModeBest)
	csm = NewConsumerSessionManager(&RPCEndpoint{"stub", "stub", "stub", false, "/"}, opt, nil, "lava@test", NewActiveSubscriptionProvidersStorage())
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))
	time.Sleep(5 * time.Millisecond) // let probes finish, as the sibling tests do

	candidates = csm.getValidAddresses("", nil, context.Background())
	require.Greater(t, len(candidates), 3, "need enough candidates for a meaningful queue")

	for i, addr := range candidates {
		// 1.0s down to 0.1s in even steps: strictly distinct, comfortably inside the fixed
		// normalisation range, so no two candidates can collapse onto the same composite.
		opt.setLatency(addr, float64(len(candidates)-i)*0.1)
	}

	ranked = make([]string, 0, len(candidates))
	for i := len(candidates) - 1; i >= 0; i-- {
		ranked = append(ranked, candidates[i])
	}
	return csm, candidates, ranked
}

// TestBestModeYieldsDescendingQueue is the end-to-end proof of the headline behaviour in
// FAILOVER-TASKS section 1: "It becomes a queue. Sort the healthy providers by the chosen
// word, send to the first in line. If it fails, send to the second. Then the third."
//
// Everything below getValidProviderAddresses was already covered — pickBestIndex returns the
// argmax, and the selector is exercised directly in its own package. What was NOT covered is
// the step that turns a single best-pick into a queue: the wantedProviders loop calling the
// optimizer N times against a GROWING ignore list. That loop is the feature. If a future
// change stopped copying the ignore list, or stopped adding each pick to it, Best mode would
// hand back the same address N times and every request would burn its whole retry budget on
// one upstream — with no existing test failing.
func TestBestModeYieldsDescendingQueue(t *testing.T) {
	csm, _, ranked := newBestModeCSM(t)

	const wanted = 3
	got, err := csm.getValidProviderAddresses(context.Background(), wanted, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "")
	require.NoError(t, err)
	require.Len(t, got, wanted)

	require.Equal(t, ranked[:wanted], got,
		"Best mode must return the top-%d by score, fastest first", wanted)

	// Distinctness deserves its own assertion: a regression that returned the leader three
	// times would still satisfy "starts with the leader".
	seen := map[string]struct{}{}
	for _, addr := range got {
		_, duplicate := seen[addr]
		require.False(t, duplicate, "provider %s returned more than once — the ignore list is not being honoured", addr)
		seen[addr] = struct{}{}
	}
}

// TestBestModeQueueCoversEveryCandidate walks the full list rather than the top three, so a
// loop that ranked the head correctly but degenerated further down cannot pass.
func TestBestModeQueueCoversEveryCandidate(t *testing.T) {
	csm, candidates, ranked := newBestModeCSM(t)

	got, err := csm.getValidProviderAddresses(context.Background(), len(candidates), map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "")
	require.NoError(t, err)
	require.Equal(t, ranked, got, "the full queue must be every candidate, in descending score order")
}

// TestBestModeQueueIsStableAcrossCalls covers the other half of section 1 — "the answer does
// not change between two identical requests". The queue must be reproducible, not merely
// correctly ordered once.
func TestBestModeQueueIsStableAcrossCalls(t *testing.T) {
	csm, _, _ := newBestModeCSM(t)

	first, err := csm.getValidProviderAddresses(context.Background(), 3, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "")
	require.NoError(t, err)
	require.Len(t, first, 3)

	for i := 0; i < 20; i++ {
		again, err := csm.getValidProviderAddresses(context.Background(), 3, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "")
		require.NoError(t, err)
		require.Equal(t, first, again, "identical requests must produce an identical queue (call %d)", i)
	}
}

// TestBestModeQueueSkipsIgnoredProviders verifies the caller's own ignore list is respected
// on top of the loop's internal one. This is the retry path: a provider that already failed
// earlier in the same request is passed in as ignored, and must not reappear at the head of
// the queue just because it still scores highest.
func TestBestModeQueueSkipsIgnoredProviders(t *testing.T) {
	csm, _, ranked := newBestModeCSM(t)

	leader := ranked[0]
	got, err := csm.getValidProviderAddresses(context.Background(), 2, map[string]struct{}{leader: {}}, 10, 100, "", nil, common.NO_STATE, "", "")
	require.NoError(t, err)
	require.Len(t, got, 2)

	require.NotContains(t, got, leader, "an ignored provider must not be selected, however well it scores")
	require.Equal(t, ranked[1:3], got, "the queue must resume at the next-best provider")
}
