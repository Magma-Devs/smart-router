package provideroptimizer

import (
	"math"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/utils/score"
	"github.com/stretchr/testify/require"
)

// ristrettoSettle gives the async cache Set time to admit the entry, matching the existing
// provideroptimizer test convention.
const ristrettoSettle = 5 * time.Millisecond

// collapseAvailability drives a provider's availability average below the acceptable minimum the
// same way a real outage does: the prober feeds availability=0 once per cycle at ProbeUpdateWeight.
// Returns the resolved availability at the end of the outage.
//
// The per-sample sleep is load-bearing, not padding. providersStorage.Set is async (ristretto), so a
// tight loop keeps re-reading the PREVIOUS entry and every write lands on the same stale base — the
// denominator then stops at one sample's weight (0.25) instead of accumulating across the outage
// (7.5 over 30 cycles). Accumulated denominator is the quantity these tests are about, and real probe
// cycles are seconds apart, so accumulation is what the sleep reproduces.
func collapseAvailability(t *testing.T, po *ProviderOptimizer, addr string, cycles int) float64 {
	t.Helper()
	for i := 0; i < cycles; i++ {
		po.AppendProbeData(addr, 0, 0, false, 0, false, SyncReference{})
		time.Sleep(time.Millisecond)
	}
	time.Sleep(ristrettoSettle)
	data, found := po.getProviderData(addr)
	require.True(t, found, "the outage must have recorded provider data")
	avail, err := data.Availability.Resolve()
	require.NoError(t, err)
	require.Less(t, avail, score.MinAcceptableAvailability,
		"precondition: the outage must have collapsed availability below the acceptable minimum")
	return avail
}

// healthyProbeCyclesToClearThreshold counts how many healthy probe cycles the provider needs before
// its availability climbs back to score.MinAcceptableAvailability — i.e. how long it stays collapsed
// at the MinSelectionChance starvation floor. Capped so a regression fails fast instead of hanging.
func healthyProbeCyclesToClearThreshold(t *testing.T, po *ProviderOptimizer, addr string, cap int) int {
	t.Helper()
	for n := 1; n <= cap; n++ {
		po.AppendProbeData(addr, 1.0, 10*time.Millisecond, true, 0, false, SyncReference{})
		time.Sleep(ristrettoSettle)
		data, _ := po.getProviderData(addr)
		if avail, err := data.Availability.Resolve(); err == nil && avail >= score.MinAcceptableAvailability {
			return n
		}
	}
	return cap + 1 // sentinel: did not clear within the cap
}

// TestRebaseAvailabilityOnRecovery_LiftsProviderOffTheStarvationFloor is the headline behaviour: after
// an outage collapses availability, a proven-recovery signal restarts the average at EXACTLY the
// acceptable minimum — precisely the bound CalculateScore's dead-provider test uses (a strict `<`) —
// so the provider stops being collapsed to MinSelectionChance.
func TestRebaseAvailabilityOnRecovery_LiftsProviderOffTheStarvationFloor(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "recovered"

	before := collapseAvailability(t, po, addr, 30)

	po.RebaseAvailabilityOnRecovery(addr)
	time.Sleep(ristrettoSettle)

	data, found := po.getProviderData(addr)
	require.True(t, found)
	after, err := data.Availability.Resolve()
	require.NoError(t, err)

	require.Greater(t, after, before, "the rebase must lift the collapsed availability")
	// Bit-for-bit exact, not merely close: num/denom with denom==1.0 is lossless in IEEE-754. A result
	// even one ULP low re-triggers the dead-provider collapse, leaving the rebase with no effect.
	require.Equal(t, score.MinAcceptableAvailability, after,
		"the rebase must land exactly on the acceptable minimum")
	require.False(t, after < score.MinAcceptableAvailability,
		"the rebased provider must clear CalculateScore's strict availabilityDead bound")
}

// TestRebaseAvailabilityOnRecovery_RecoveryDoesNotOutlastTheOutage bounds recovery against the outage
// that caused it. It first derives, from the average the outage actually produced, how many healthy
// cycles the raw decaying-average arithmetic would demand — success weight >= (min*denom - num)/(1-min),
// i.e. four units of success per unit of failure at the 0.80 minimum — then asserts the optimizer
// completes recovery within the sustained-health bar instead. Both halves matter: the first pins WHY
// the rebase is needed, so this test still fails loudly if the arithmetic is ever changed such that
// recovery is no longer disproportionate.
func TestRebaseAvailabilityOnRecovery_RecoveryDoesNotOutlastTheOutage(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "recovered"
	const outageCycles = 30

	collapseAvailability(t, po, addr, outageCycles)

	// What recovery would cost on the raw average alone, from the real accumulated Num/Denom.
	data, _ := po.getProviderData(addr)
	num, denom := data.Availability.GetNum(), data.Availability.GetDenom()
	neededWeight := (score.MinAcceptableAvailability*denom - num) / (1 - score.MinAcceptableAvailability)
	unrebasedCycles := int(math.Ceil(neededWeight / score.ProbeUpdateWeight))
	require.Greater(t, unrebasedCycles, 3*outageCycles,
		"precondition: on the raw average a %d-cycle outage costs %d healthy cycles to undo, which is what makes the rebase necessary",
		outageCycles, unrebasedCycles)

	// The bound the optimizer must honour: sustained health clears the threshold in a handful of cycles.
	got := healthyProbeCyclesToClearThreshold(t, po, addr, 50)
	require.LessOrEqual(t, got, int(probeRecoveryStreakBar)+1,
		"recovery must complete within the sustained-health bar (+1 for the cycle that observes it), got %d", got)
}

// TestSustainedProbeHealth_RebasesWithoutAnyEndpointTransition is the reason the rebase cannot hang
// off the endpoint enable/disable edge alone. A provider whose endpoints stay nominally ENABLED
// through an outage never crosses that edge — Endpoint.RecordProbeVerdict early-returns for enabled
// endpoints — yet its score still collapsed, from the very availability=0 samples the prober fed.
// This is the shape a real outage most commonly takes.
func TestSustainedProbeHealth_RebasesWithoutAnyEndpointTransition(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "no-transition"

	collapseAvailability(t, po, addr, 30)

	// Feed exactly the bar's worth of fully-healthy cycles. No endpoint state is involved at all.
	for i := uint64(0); i < probeRecoveryStreakBar; i++ {
		po.AppendProbeData(addr, 1.0, 10*time.Millisecond, true, 0, false, SyncReference{})
		time.Sleep(ristrettoSettle)
	}

	data, _ := po.getProviderData(addr)
	avail, err := data.Availability.Resolve()
	require.NoError(t, err)
	require.GreaterOrEqual(t, avail, score.MinAcceptableAvailability,
		"sustained probe health alone must lift the score, with no endpoint re-enable involved")
}

// TestSustainedProbeHealth_FlappingProviderNeverQualifies is the central safety property. A provider
// alternating good/bad cycles must NEVER reach the bar, or it would reset its own accumulated failure
// history forever and keep drawing traffic it has not earned.
func TestSustainedProbeHealth_FlappingProviderNeverQualifies(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "flapper"

	collapseAvailability(t, po, addr, 30)

	for i := 0; i < 40; i++ {
		availability := 1.0
		if i%2 == 1 {
			availability = 0
		}
		po.AppendProbeData(addr, availability, 10*time.Millisecond, true, 0, false, SyncReference{})
		time.Sleep(time.Millisecond)
	}
	time.Sleep(ristrettoSettle)

	data, _ := po.getProviderData(addr)
	avail, err := data.Availability.Resolve()
	require.NoError(t, err)
	require.Less(t, avail, score.MinAcceptableAvailability,
		"a flapping provider must stay collapsed — an imperfect sample resets the streak")
}

// TestSustainedProbeHealth_PartialDegradationNeverQualifies — availability is the FRACTION of the
// provider's endpoints healthy this cycle, so a provider with one endpoint still down feeds < 1.0
// forever. That is genuine ongoing degradation, not stale outage debt, and must not be rebased.
func TestSustainedProbeHealth_PartialDegradationNeverQualifies(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "half-down"

	collapseAvailability(t, po, addr, 30)

	for i := 0; i < 40; i++ {
		po.AppendProbeData(addr, 0.5, 10*time.Millisecond, true, 0, false, SyncReference{})
		time.Sleep(time.Millisecond)
	}
	time.Sleep(ristrettoSettle)

	data, _ := po.getProviderData(addr)
	avail, err := data.Availability.Resolve()
	require.NoError(t, err)
	require.Less(t, avail, score.MinAcceptableAvailability,
		"a partially-degraded provider (fraction < 1.0) must never reach the sustained-health bar")
}

// TestSustainedProbeHealth_ReArmsAfterAGenuineDip — the once-per-episode latch must not be permanent:
// a provider that recovers, later genuinely fails again, and then recovers again must be rebased on
// the second recovery too.
func TestSustainedProbeHealth_ReArmsAfterAGenuineDip(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "second-recovery"

	feedHealthy := func(n int) {
		for i := 0; i < n; i++ {
			po.AppendProbeData(addr, 1.0, 10*time.Millisecond, true, 0, false, SyncReference{})
			time.Sleep(ristrettoSettle)
		}
	}

	// First outage and recovery.
	collapseAvailability(t, po, addr, 30)
	feedHealthy(int(probeRecoveryStreakBar))
	data, _ := po.getProviderData(addr)
	first, err := data.Availability.Resolve()
	require.NoError(t, err)
	require.GreaterOrEqual(t, first, score.MinAcceptableAvailability, "precondition: first recovery lifted the score")

	// Second outage, deep enough to collapse again, then a second recovery.
	collapseAvailability(t, po, addr, 60)
	feedHealthy(int(probeRecoveryStreakBar))

	data, _ = po.getProviderData(addr)
	second, err := data.Availability.Resolve()
	require.NoError(t, err)
	require.GreaterOrEqual(t, second, score.MinAcceptableAvailability,
		"a genuine dip must re-arm the latch so the next real recovery is rebased too")
}

// TestRebaseAvailabilityOnRecovery_NeverLowersAHealthyScore guards the no-downgrade rule. A perfectly
// healthy provider can reach this path (any endpoint re-enable calls it), and rebasing it to the bare
// minimum would be a demotion for recovering.
func TestRebaseAvailabilityOnRecovery_NeverLowersAHealthyScore(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "healthy"

	for i := 0; i < 20; i++ {
		po.AppendProbeData(addr, 1.0, 10*time.Millisecond, true, 0, false, SyncReference{})
	}
	time.Sleep(ristrettoSettle)
	data, _ := po.getProviderData(addr)
	before, err := data.Availability.Resolve()
	require.NoError(t, err)
	require.Greater(t, before, score.MinAcceptableAvailability, "precondition: provider is healthy")

	po.RebaseAvailabilityOnRecovery(addr)
	time.Sleep(ristrettoSettle)

	data, _ = po.getProviderData(addr)
	after, err := data.Availability.Resolve()
	require.NoError(t, err)
	require.Equal(t, before, after, "a provider already above the minimum must be left untouched")
}

// TestRebaseAvailabilityOnRecovery_IsIdempotent — one provider with several endpoints recovering in
// the same probe cycle triggers one call per endpoint. Repeated calls must not compound.
func TestRebaseAvailabilityOnRecovery_IsIdempotent(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "multi-endpoint"

	collapseAvailability(t, po, addr, 30)

	po.RebaseAvailabilityOnRecovery(addr)
	time.Sleep(ristrettoSettle)
	data, _ := po.getProviderData(addr)
	first, err := data.Availability.Resolve()
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		po.RebaseAvailabilityOnRecovery(addr)
	}
	time.Sleep(ristrettoSettle)
	data, _ = po.getProviderData(addr)
	repeated, err := data.Availability.Resolve()
	require.NoError(t, err)

	require.Equal(t, first, repeated, "repeat calls in one cycle must be a no-op, not compounding")
}

// TestRebaseAvailabilityOnRecovery_StillBrokenProviderReCollapsesFast is the safety half: the rebase
// is a probation baseline, not an amnesty. Because it deliberately restarts on a SMALL denominator, a
// provider that is not actually healthy falls straight back to collapsed on its next bad sample.
func TestRebaseAvailabilityOnRecovery_StillBrokenProviderReCollapsesFast(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "still-broken"

	collapseAvailability(t, po, addr, 30)
	po.RebaseAvailabilityOnRecovery(addr)
	time.Sleep(ristrettoSettle)

	// One failing probe cycle after the rebase.
	po.AppendProbeData(addr, 0, 0, false, 0, false, SyncReference{})
	time.Sleep(ristrettoSettle)

	data, _ := po.getProviderData(addr)
	avail, err := data.Availability.Resolve()
	require.NoError(t, err)
	require.Less(t, avail, score.MinAcceptableAvailability,
		"a provider that is still failing must collapse again immediately — the rebase is probation, not amnesty")
}

// TestRebaseAvailabilityOnRecovery_PreservesOtherDimensions — the outage invalidated availability, not
// the provider's measured latency/sync or its tracked sync block. Rebasing must touch only
// availability.
func TestRebaseAvailabilityOnRecovery_PreservesOtherDimensions(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "provider1"
	now := time.Now()
	ref := freshRef(2000, now.Add(-time.Second))

	// Build real latency/sync history and a tracked sync block, then collapse availability.
	for i := 0; i < 10; i++ {
		po.AppendProbeData(addr, 1.0, 25*time.Millisecond, true, 1500, true, ref)
	}
	time.Sleep(ristrettoSettle)
	before, _ := po.getProviderData(addr)
	latencyBefore := before.Latency.GetNum() / before.Latency.GetDenom()
	syncBefore := before.Sync.GetNum() / before.Sync.GetDenom()
	blockBefore := before.SyncBlock
	require.NotZero(t, blockBefore, "precondition: a sync block was tracked")

	collapseAvailability(t, po, addr, 30)
	po.RebaseAvailabilityOnRecovery(addr)
	time.Sleep(ristrettoSettle)

	after, _ := po.getProviderData(addr)
	require.Equal(t, latencyBefore, after.Latency.GetNum()/after.Latency.GetDenom(),
		"latency history must survive the rebase")
	require.Equal(t, syncBefore, after.Sync.GetNum()/after.Sync.GetDenom(),
		"sync history must survive the rebase")
	require.Equal(t, blockBefore, after.SyncBlock,
		"the tracked sync block must survive the rebase (monotonic floor)")
}

// TestRebaseAvailabilityOnRecovery_PreservesScoreConfig — a tuned weight/half-life must not be
// silently reset to package defaults by rebuilding the store.
func TestRebaseAvailabilityOnRecovery_PreservesScoreConfig(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "tuned"

	collapseAvailability(t, po, addr, 30)
	data, _ := po.getProviderData(addr)
	cfgBefore := data.Availability.GetConfig()

	po.RebaseAvailabilityOnRecovery(addr)
	time.Sleep(ristrettoSettle)

	data, _ = po.getProviderData(addr)
	require.Equal(t, cfgBefore, data.Availability.GetConfig(),
		"the rebase must carry the existing score config forward")
}

// TestRebaseAvailabilityOnRecovery_UnknownProviderIsNoop — a provider with no accumulated history has
// nothing to rebase (its default store already resolves to 1.0); the call must not create an entry
// that would replace that default with the lower probation value.
func TestRebaseAvailabilityOnRecovery_UnknownProviderIsNoop(t *testing.T) {
	po := setupProviderOptimizer(1)

	require.NotPanics(t, func() { po.RebaseAvailabilityOnRecovery("never-seen") })
	time.Sleep(ristrettoSettle)

	_, found := po.getProviderData("never-seen")
	require.False(t, found, "rebasing an unknown provider must not manufacture a stored entry")
}
