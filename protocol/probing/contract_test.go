package probing

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAggregateProviderSample_EmptyVerdicts(t *testing.T) {
	_, ok := AggregateProviderSample(nil)
	require.False(t, ok, "no verdicts → no sample this cycle (not a spurious zero)")
	_, ok = AggregateProviderSample([]EndpointVerdict{})
	require.False(t, ok)
}

func TestAggregateProviderSample_FractionHealthy(t *testing.T) {
	// 3 of 5 healthy → availability 0.6 (partial degradation decays the score, not best-endpoint).
	verdicts := []EndpointVerdict{
		{Healthy: true, Latency: 30 * time.Millisecond, Block: 1000},
		{Healthy: true, Latency: 10 * time.Millisecond, Block: 1002},
		{Healthy: true, Latency: 50 * time.Millisecond, Block: 1001},
		{Healthy: false, Latency: 1 * time.Millisecond, Block: 5000}, // dead: must not contribute
		{Healthy: false, Latency: 2 * time.Millisecond, Block: 9000}, // dead: must not contribute
	}
	sample, ok := AggregateProviderSample(verdicts)
	require.True(t, ok)
	require.InDelta(t, 0.6, sample.Availability, 1e-9, "3/5 healthy")
	require.True(t, sample.HasLatency)
	require.Equal(t, 10*time.Millisecond, sample.Latency, "latency is the MIN over HEALTHY endpoints")
	require.Equal(t, uint64(1002), sample.Block, "block is the MAX over HEALTHY endpoints")
}

func TestAggregateProviderSample_UnhealthyEndpointsExcludedFromQuality(t *testing.T) {
	// The dead endpoints have the smallest latency and largest block, but must not leak into the
	// quality sample — only healthy endpoints define latency/block.
	verdicts := []EndpointVerdict{
		{Healthy: false, Latency: 1 * time.Nanosecond, Block: 1_000_000},
		{Healthy: true, Latency: 40 * time.Millisecond, Block: 1000},
	}
	sample, ok := AggregateProviderSample(verdicts)
	require.True(t, ok)
	require.InDelta(t, 0.5, sample.Availability, 1e-9)
	require.Equal(t, 40*time.Millisecond, sample.Latency, "the dead endpoint's tiny latency must not win")
	require.Equal(t, uint64(1000), sample.Block, "the dead endpoint's high block must not win")
}

func TestAggregateProviderSample_AllHealthy(t *testing.T) {
	verdicts := []EndpointVerdict{
		{Healthy: true, Latency: 25 * time.Millisecond, Block: 2000},
		{Healthy: true, Latency: 15 * time.Millisecond, Block: 2003},
	}
	sample, ok := AggregateProviderSample(verdicts)
	require.True(t, ok)
	require.Equal(t, 1.0, sample.Availability)
	require.True(t, sample.HasLatency)
	require.True(t, sample.HasBlock)
	require.Equal(t, 15*time.Millisecond, sample.Latency)
	require.Equal(t, uint64(2003), sample.Block)
}

func TestAggregateProviderSample_NoneHealthy(t *testing.T) {
	verdicts := []EndpointVerdict{
		{Healthy: false, Latency: 5 * time.Millisecond, Block: 1000},
		{Healthy: false, Latency: 7 * time.Millisecond, Block: 1001},
	}
	sample, ok := AggregateProviderSample(verdicts)
	require.True(t, ok, "a sample is still emitted — the availability=0 failure decays the score")
	require.Equal(t, 0.0, sample.Availability)
	require.False(t, sample.HasLatency, "no healthy endpoint → no latency sample, only the failure")
	require.False(t, sample.HasBlock)
	require.Equal(t, time.Duration(0), sample.Latency)
	require.Equal(t, uint64(0), sample.Block)
}

// TestAggregateProviderSample_LatencyUnknownOmitted covers the latency-unknown case (Topic D): a
// healthy endpoint with no poll latency yet (Latency 0 — e.g. relay-fed under the traffic gate, or
// recovery's first cycle) still counts toward availability, but contributes NO latency sample, so a
// fake 0 cannot win the min and clobber a real relay-fed latency.
func TestAggregateProviderSample_LatencyUnknownOmitted(t *testing.T) {
	// Both healthy, but neither has a known latency → availability full, latency omitted.
	sample, ok := AggregateProviderSample([]EndpointVerdict{
		{Healthy: true, Latency: 0, Block: 1000},
		{Healthy: true, Latency: 0, Block: 1001},
	})
	require.True(t, ok)
	require.Equal(t, 1.0, sample.Availability)
	require.False(t, sample.HasLatency, "no endpoint had a known latency → omit the dimension, don't feed 0")
	require.True(t, sample.HasBlock)
	require.Equal(t, uint64(1001), sample.Block)

	// Mixed: one healthy endpoint knows its latency, another doesn't → use the known one (not 0).
	sample, ok = AggregateProviderSample([]EndpointVerdict{
		{Healthy: true, Latency: 0, Block: 1000},                    // unknown latency
		{Healthy: true, Latency: 30 * time.Millisecond, Block: 999}, // known
	})
	require.True(t, ok)
	require.True(t, sample.HasLatency)
	require.Equal(t, 30*time.Millisecond, sample.Latency, "the known latency wins; the unknown 0 is ignored")
}

// TestAggregateProviderSample_OneSamplePerProvider documents rule E2: regardless of how many
// endpoints a provider has, aggregation yields exactly ONE sample — so a 5-endpoint provider and a
// 1-endpoint provider get equal EWMA weight per cycle (the prober makes one optimizer call from it).
func TestAggregateProviderSample_OneSamplePerProvider(t *testing.T) {
	five := make([]EndpointVerdict, 5)
	for i := range five {
		five[i] = EndpointVerdict{Healthy: true, Latency: 20 * time.Millisecond, Block: 3000}
	}
	one := []EndpointVerdict{{Healthy: true, Latency: 20 * time.Millisecond, Block: 3000}}

	s5, ok5 := AggregateProviderSample(five)
	s1, ok1 := AggregateProviderSample(one)
	require.True(t, ok5)
	require.True(t, ok1)
	// Both collapse to a single sample with the same shape — endpoint count does not skew weight.
	require.Equal(t, s1.Availability, s5.Availability)
	require.Equal(t, s1.Latency, s5.Latency)
	require.Equal(t, s1.Block, s5.Block)
}
