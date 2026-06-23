package provideroptimizer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Topic E (MAG-2160 Finding 2 / F4 / F5): the QoS sync dimension measures lag against the per-chain
// CONSENSUS baseline, resolved PER SAMPLE by the caller and passed in as a SyncReference (F4 — no
// shared mutable getter). When consensus is configured but has no fresh majority, the sample's sync
// dimension is OMITTED rather than falling back to max-block-across-providers (F5 — which one
// fast/lying reporter could inflate, penalizing the whole pod). These white-box tests pin
// resolveSyncReference, the seam both relay and probe sync paths share.

func freshRef(block uint64, at time.Time) SyncReference {
	return SyncReference{ConsensusConfigured: true, Block: block, Time: at, Fresh: true}
}

func TestResolveSyncReference_NoConsensusConfiguredIsLegacy(t *testing.T) {
	po := setupProviderOptimizer(1)
	now := time.Now()
	// ConsensusConfigured=false → legacy max-across-providers, kept warm. A 900 sample then an 800
	// sample both see 900 (the running max), exactly the pre-MAG-2160 behavior. ok is always true.
	_, _, ok := po.resolveSyncReference(SyncReference{}, 900, now)
	require.True(t, ok)
	ref, _, ok := po.resolveSyncReference(SyncReference{}, 800, now)
	require.True(t, ok)
	require.Equal(t, uint64(900), ref, "no consensus configured → legacy max-across-providers reference")
}

func TestResolveSyncReference_FreshBaselineUsed(t *testing.T) {
	po := setupProviderOptimizer(1)
	now := time.Now()
	baselineTime := now.Add(-time.Second)

	ref, refTime, ok := po.resolveSyncReference(freshRef(2000, baselineTime), 1000, now)
	require.True(t, ok, "a fresh consensus baseline is a usable reference")
	require.Equal(t, uint64(2000), ref, "the consensus baseline overrides max-across-providers")
	require.Equal(t, baselineTime, refTime)

	// A provider at 1000 measures real lag against the agreed 2000; one at/above has zero lag.
	require.Greater(t, po.calculateSyncLag(ref, refTime, 1000, now), time.Duration(0))
	require.Equal(t, time.Duration(0), po.calculateSyncLag(ref, refTime, 2000, now))
}

// TestResolveSyncReference_ConfiguredButNoBaselineOmits is the F5 core: consensus is configured but
// currently has no fresh majority (cold start / split / stale) → ok=false, so the caller OMITS the
// sync update. Critically it must NOT fall back to max-across-providers.
func TestResolveSyncReference_ConfiguredButNoBaselineOmits(t *testing.T) {
	po := setupProviderOptimizer(1)
	now := time.Now()

	// Cold start: configured, not fresh.
	_, _, ok := po.resolveSyncReference(SyncReference{ConsensusConfigured: true, Fresh: false}, 1000, now)
	require.False(t, ok, "consensus configured but no fresh majority → omit sync (no fallback)")

	// Stale consensus: configured, Fresh=false even though a block value lingers.
	_, _, ok = po.resolveSyncReference(SyncReference{ConsensusConfigured: true, Block: 2000, Fresh: false}, 1000, now)
	require.False(t, ok, "a stale baseline is not usable")

	// Fresh but zero block is treated as no baseline.
	_, _, ok = po.resolveSyncReference(SyncReference{ConsensusConfigured: true, Block: 0, Fresh: true}, 1000, now)
	require.False(t, ok, "a fresh-but-zero baseline is not usable")
}

// TestResolveSyncReference_LoneOutlierDoesNotPoison proves the poisoning vector is closed: even after
// a lone fast/lying reporter pushes the legacy max sky-high, a consensus-configured-but-stale sample
// returns ok=false (omit) instead of measuring everyone's lag against that inflated max.
func TestResolveSyncReference_LoneOutlierDoesNotPoison(t *testing.T) {
	po := setupProviderOptimizer(1)
	now := time.Now()

	// A lone outlier inflates the legacy warm store to 9_000_000 via the legacy (unconfigured) path.
	po.resolveSyncReference(SyncReference{}, 9_000_000, now)

	// With consensus configured but no fresh majority, an honest provider's sample does NOT get
	// measured against the poisoned 9_000_000 — it is simply omitted.
	_, _, ok := po.resolveSyncReference(SyncReference{ConsensusConfigured: true, Fresh: false}, 1000, now)
	require.False(t, ok, "a configured-but-stale consensus must not fall back to the poisoned max")
}

// TestAppendProbeData_TracksBlockAndFeedsDimensions pins the probe contract feed: availability is
// always fed (including failures), sync block advances monotonically, and an unhealthy cycle feeds
// no sync block. hasSync samples carry a fresh consensus SyncReference.
func TestAppendProbeData_TracksBlockAndFeedsDimensions(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "provider1"
	now := time.Now()
	ref := freshRef(2000, now.Add(-time.Second))

	// Unhealthy probe (provider fully down this cycle): availability decays, no sync block.
	po.AppendProbeData(addr, 0, 0, false, 0, false, SyncReference{})
	time.Sleep(5 * time.Millisecond) // ristretto Set is async — let it admit the entry
	data, found := po.getProviderData(addr)
	require.True(t, found, "an unhealthy probe still records a provider entry (availability decay)")
	require.Equal(t, uint64(0), data.SyncBlock, "an unhealthy probe feeds no sync block")

	// Healthy probe with a block: sync block advances.
	po.AppendProbeData(addr, 1.0, 20*time.Millisecond, true, 1500, true, ref)
	time.Sleep(5 * time.Millisecond)
	data, _ = po.getProviderData(addr)
	require.Equal(t, uint64(1500), data.SyncBlock)

	// A lower block must not regress the provider's tracked sync block (monotonic).
	po.AppendProbeData(addr, 1.0, 20*time.Millisecond, true, 1400, true, ref)
	time.Sleep(5 * time.Millisecond)
	data, _ = po.getProviderData(addr)
	require.Equal(t, uint64(1500), data.SyncBlock, "provider sync block is monotonic")
}

// TestAppendProbeData_FractionalAvailabilityDecaysScore confirms a partial-degradation sample
// (availability 0.5) is accepted and pushes the availability score below a fully-healthy provider's.
func TestAppendProbeData_FractionalAvailabilityDecaysScore(t *testing.T) {
	po := setupProviderOptimizer(1)
	now := time.Now()
	ref := freshRef(1000, now)

	for i := 0; i < 20; i++ {
		po.AppendProbeData("healthy", 1.0, 10*time.Millisecond, true, 1000, true, ref)
		po.AppendProbeData("degraded", 0.5, 10*time.Millisecond, true, 1000, true, ref)
	}
	time.Sleep(5 * time.Millisecond) // ristretto Set is async

	healthy, ok := po.getProviderData("healthy")
	require.True(t, ok)
	degraded, ok := po.getProviderData("degraded")
	require.True(t, ok)

	hAvail := healthy.Availability.GetNum() / healthy.Availability.GetDenom()
	dAvail := degraded.Availability.GetNum() / degraded.Availability.GetDenom()
	require.Greater(t, hAvail, dAvail,
		"a fraction-healthy (0.5) provider must score below a fully-healthy (1.0) one — partial degradation decays")
}

// TestAppendProbeData_NoBaselineOmitsSyncButKeepsAvailability is the probe-side F5 guard: when the
// caller passes hasSync=false (no consensus baseline this cycle), availability still feeds but sync
// is omitted — never poisoned by the legacy max.
func TestAppendProbeData_NoBaselineOmitsSyncButKeepsAvailability(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "provider1"

	// hasSync=false (no baseline) but a non-zero block — sync must NOT be recorded.
	po.AppendProbeData(addr, 1.0, 10*time.Millisecond, true, 1500, false, SyncReference{})
	time.Sleep(5 * time.Millisecond)
	data, ok := po.getProviderData(addr)
	require.True(t, ok, "availability is still recorded with no baseline")
	require.Equal(t, uint64(0), data.SyncBlock, "no baseline → sync block not advanced (omitted)")
}

// TestRelayVsProbeWeighting_RelayMovesAvailabilityMore exercises the contract's dual feed
// END-TO-END (rule D1): from the same 100% default, a single relay failure (AppendRelayFailure,
// weight 1) drops availability more than a single probe failure (AppendProbeData, weight 0.25).
func TestRelayVsProbeWeighting_RelayMovesAvailabilityMore(t *testing.T) {
	po := setupProviderOptimizer(1)

	po.AppendRelayFailure("relay")                                      // weight 1, availability 0
	po.AppendProbeData("probe", 0, 0, false, 0, false, SyncReference{}) // weight 0.25, availability 0
	time.Sleep(5 * time.Millisecond)                                    // ristretto Set is async

	relay, ok := po.getProviderData("relay")
	require.True(t, ok)
	probe, ok := po.getProviderData("probe")
	require.True(t, ok)

	relayAvail := relay.Availability.GetNum() / relay.Availability.GetDenom()
	probeAvail := probe.Availability.GetNum() / probe.Availability.GetDenom()
	require.Less(t, relayAvail, probeAvail,
		"a relay failure (weight 1) drops availability more than a probe failure (weight 0.25)")
}
