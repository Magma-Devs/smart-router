package provideroptimizer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Topic E (MAG-2160 Finding 2): the QoS sync dimension measures lag against the per-chain CONSENSUS
// baseline when one is supplied, instead of the legacy max-block-across-providers — which one
// fast/lying reporter could inflate, penalizing the whole pod. These white-box tests pin the seam
// (syncReference) that both the relay and probe sync paths share.

func TestSyncReference_PrefersConsensusBaselineWhenFresh(t *testing.T) {
	po := setupProviderOptimizer(1)
	now := time.Now()

	// No getter installed → legacy behavior: the reference is the max-across-providers, so a sample
	// reporting block B makes B the reference (zero lag against itself).
	ref, _ := po.syncReference(1000, now)
	require.Equal(t, uint64(1000), ref, "without a getter the reference is max-across-providers")

	// Install a fresh consensus baseline ahead of the provider's block.
	baselineTime := now.Add(-time.Second)
	po.SetSyncReferenceGetter(func() (uint64, time.Time, bool) { return 2000, baselineTime, true })

	ref, refTime := po.syncReference(1000, now)
	require.Equal(t, uint64(2000), ref, "a fresh consensus baseline overrides the max-across-providers reference")
	require.Equal(t, baselineTime, refTime)

	// A provider at 1000 now measures a real lag against the consensus 2000 — the Finding-2 fix: the
	// pod is judged against the agreed baseline, not one optimistic max.
	lag := po.calculateSyncLag(ref, refTime, 1000, now)
	require.Greater(t, lag, time.Duration(0), "a provider below the consensus baseline has positive sync lag")

	// A provider AT/above the baseline has zero lag (not penalized).
	require.Equal(t, time.Duration(0), po.calculateSyncLag(ref, refTime, 2000, now))
}

func TestSyncReference_FallsBackWhenBaselineNotFresh(t *testing.T) {
	po := setupProviderOptimizer(1)
	now := time.Now()
	// Cold start / no majority / stale → getter reports not-fresh.
	po.SetSyncReferenceGetter(func() (uint64, time.Time, bool) { return 0, time.Time{}, false })

	// Warm the fallback max to 1500, then a 1000 sample still sees 1500 (max-across-providers).
	po.syncReference(1500, now)
	ref, _ := po.syncReference(1000, now)
	require.Equal(t, uint64(1500), ref, "an unfresh baseline falls back to max-across-providers (never worse than legacy)")
}

func TestSyncReference_NilGetterIsLegacyBehavior(t *testing.T) {
	po := setupProviderOptimizer(1)
	now := time.Now()
	// Never set a getter: identical to the pre-MAG-2160 max-across-providers reference.
	po.syncReference(900, now)
	ref, _ := po.syncReference(800, now)
	require.Equal(t, uint64(900), ref)
}

// TestAppendProbeData_TracksBlockAndFeedsDimensions pins the probe contract feed: availability is
// always fed (including failures), sync block advances monotonically, and an unhealthy cycle feeds
// no sync block.
func TestAppendProbeData_TracksBlockAndFeedsDimensions(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "provider1"

	// Unhealthy probe (provider fully down this cycle): availability decays, no sync block.
	po.AppendProbeData(addr, 0, 0, 0, false)
	time.Sleep(5 * time.Millisecond) // ristretto Set is async — let it admit the entry
	data, found := po.getProviderData(addr)
	require.True(t, found, "an unhealthy probe still records a provider entry (availability decay)")
	require.Equal(t, uint64(0), data.SyncBlock, "an unhealthy probe feeds no sync block")

	// Healthy probe with a block: sync block advances.
	po.AppendProbeData(addr, 1.0, 20*time.Millisecond, 1500, true)
	time.Sleep(5 * time.Millisecond)
	data, _ = po.getProviderData(addr)
	require.Equal(t, uint64(1500), data.SyncBlock)

	// A lower block must not regress the provider's tracked sync block (monotonic).
	po.AppendProbeData(addr, 1.0, 20*time.Millisecond, 1400, true)
	time.Sleep(5 * time.Millisecond)
	data, _ = po.getProviderData(addr)
	require.Equal(t, uint64(1500), data.SyncBlock, "provider sync block is monotonic")
}

// TestAppendProbeData_FractionalAvailabilityDecaysScore confirms a partial-degradation sample
// (availability 0.5) is accepted and pushes the availability score below a fully-healthy provider's.
func TestAppendProbeData_FractionalAvailabilityDecaysScore(t *testing.T) {
	po := setupProviderOptimizer(1)

	// Feed many samples so the EWMA settles near the fed value for each provider.
	for i := 0; i < 20; i++ {
		po.AppendProbeData("healthy", 1.0, 10*time.Millisecond, 1000, true)
		po.AppendProbeData("degraded", 0.5, 10*time.Millisecond, 1000, true)
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

// TestRelayVsProbeWeighting_RelayMovesAvailabilityMore exercises the contract's dual feed
// END-TO-END (rule D1): from the same 100% default, a single relay failure (AppendRelayFailure,
// weight 1) drops availability more than a single probe failure (AppendProbeData, weight 0.25).
// This is also the only test that drives AppendRelayFailure through the migrated appendRelayData →
// syncReference path.
func TestRelayVsProbeWeighting_RelayMovesAvailabilityMore(t *testing.T) {
	po := setupProviderOptimizer(1)

	po.AppendRelayFailure("relay")              // weight 1, availability 0
	po.AppendProbeData("probe", 0, 0, 0, false) // weight 0.25, availability 0
	time.Sleep(5 * time.Millisecond)            // ristretto Set is async

	relay, ok := po.getProviderData("relay")
	require.True(t, ok)
	probe, ok := po.getProviderData("probe")
	require.True(t, ok)

	relayAvail := relay.Availability.GetNum() / relay.Availability.GetDenom()
	probeAvail := probe.Availability.GetNum() / probe.Availability.GetDenom()
	require.Less(t, relayAvail, probeAvail,
		"a relay failure (weight 1) drops availability more than a probe failure (weight 0.25)")
}
