package provideroptimizer

import (
	"context"
	stdmath "math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// Helper function to create a QoS report
func createQoSReport(availability, latency, sync float64) *pairingtypes.QualityOfServiceReport {
	return &pairingtypes.QualityOfServiceReport{
		Availability: availability,
		Latency:      latency,
		Sync:         sync,
	}
}

// TestNewUpstreamSelector tests the creation of a new UpstreamSelector
func TestNewUpstreamSelector(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	require.NotNil(t, ws)
	require.Equal(t, 0.3, ws.availabilityWeight)
	require.Equal(t, 0.3, ws.latencyWeight)
	require.Equal(t, 0.2, ws.syncWeight)
	require.Equal(t, 0.2, ws.stakeWeight)
	require.Equal(t, 0.01, ws.minSelectionChance)
}

// TestWeightNormalization tests that weights are normalized if they don't sum to 1.0
func TestWeightNormalization(t *testing.T) {
	config := UpstreamSelectorConfig{
		AvailabilityWeight: 0.5,
		LatencyWeight:      0.5,
		SyncWeight:         0.5,
		StakeWeight:        0.5,
		MinSelectionChance: 0.01,
		Strategy:           StrategyBalanced,
	}

	ws := NewUpstreamSelector(config)

	// Weights should be normalized to sum to 1.0
	totalWeight := ws.availabilityWeight + ws.latencyWeight + ws.syncWeight + ws.stakeWeight
	require.InDelta(t, 1.0, totalWeight, 0.001)

	// Each weight should be 0.25 (0.5/2.0)
	require.InDelta(t, 0.25, ws.availabilityWeight, 0.001)
	require.InDelta(t, 0.25, ws.latencyWeight, 0.001)
	require.InDelta(t, 0.25, ws.syncWeight, 0.001)
	require.InDelta(t, 0.25, ws.stakeWeight, 0.001)
}

func TestNewUpstreamSelectorZeroTotalWeightFallsBackToDefaultWeightsButKeepsOtherConfig(t *testing.T) {
	config := UpstreamSelectorConfig{
		AvailabilityWeight: 0,
		LatencyWeight:      0,
		SyncWeight:         0,
		StakeWeight:        0,
		MinSelectionChance: 0.123,
		Strategy:           StrategyLatency,
	}

	ws := NewUpstreamSelector(config)

	// Falls back to default weights
	require.InDelta(t, 0.3, ws.availabilityWeight, 0.0001)
	require.InDelta(t, 0.3, ws.latencyWeight, 0.0001)
	require.InDelta(t, 0.2, ws.syncWeight, 0.0001)
	require.InDelta(t, 0.2, ws.stakeWeight, 0.0001)

	// Preserves other config
	require.InDelta(t, 0.123, ws.minSelectionChance, 0.0000001)
	require.Equal(t, StrategyLatency, ws.strategy)
}

func TestNewUpstreamSelectorNegativeWeightFallsBackToDefaultWeightsButKeepsOtherConfig(t *testing.T) {
	config := UpstreamSelectorConfig{
		AvailabilityWeight: -0.1,
		LatencyWeight:      0.6,
		SyncWeight:         0.3,
		StakeWeight:        0.2,
		MinSelectionChance: 0.222,
		Strategy:           StrategySyncFreshness,
	}

	ws := NewUpstreamSelector(config)

	// Falls back to default weights
	require.InDelta(t, 0.3, ws.availabilityWeight, 0.0001)
	require.InDelta(t, 0.3, ws.latencyWeight, 0.0001)
	require.InDelta(t, 0.2, ws.syncWeight, 0.0001)
	require.InDelta(t, 0.2, ws.stakeWeight, 0.0001)

	// Preserves other config
	require.InDelta(t, 0.222, ws.minSelectionChance, 0.0000001)
	require.Equal(t, StrategySyncFreshness, ws.strategy)
}

func TestNewUpstreamSelectorNaNWeightFallsBackToDefaultWeightsButKeepsOtherConfig(t *testing.T) {
	config := UpstreamSelectorConfig{
		AvailabilityWeight: 0.3,
		LatencyWeight:      0.3,
		SyncWeight:         0.2,
		StakeWeight:        0.2,
		MinSelectionChance: 0.333,
		Strategy:           StrategyAccuracy,
	}
	config.LatencyWeight = stdmath.NaN()

	ws := NewUpstreamSelector(config)

	// Falls back to default weights
	require.InDelta(t, 0.3, ws.availabilityWeight, 0.0001)
	require.InDelta(t, 0.3, ws.latencyWeight, 0.0001)
	require.InDelta(t, 0.2, ws.syncWeight, 0.0001)
	require.InDelta(t, 0.2, ws.stakeWeight, 0.0001)

	// Preserves other config
	require.InDelta(t, 0.333, ws.minSelectionChance, 0.0000001)
	require.Equal(t, StrategyAccuracy, ws.strategy)
}

func TestNewUpstreamSelectorInfWeightFallsBackToDefaultWeightsButKeepsOtherConfig(t *testing.T) {
	config := UpstreamSelectorConfig{
		AvailabilityWeight: 0.3,
		LatencyWeight:      0.3,
		SyncWeight:         0.2,
		StakeWeight:        0.2,
		MinSelectionChance: 0.444,
		Strategy:           StrategyDistributed,
	}
	config.SyncWeight = stdmath.Inf(1)

	ws := NewUpstreamSelector(config)

	// Falls back to default weights
	require.InDelta(t, 0.3, ws.availabilityWeight, 0.0001)
	require.InDelta(t, 0.3, ws.latencyWeight, 0.0001)
	require.InDelta(t, 0.2, ws.syncWeight, 0.0001)
	require.InDelta(t, 0.2, ws.stakeWeight, 0.0001)

	// Preserves other config
	require.InDelta(t, 0.444, ws.minSelectionChance, 0.0000001)
	require.Equal(t, StrategyDistributed, ws.strategy)
}

// TestCalculateScorePerfectProvider tests scoring for a perfect provider
func TestCalculateScorePerfectProvider(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	qos := createQoSReport(1.0, 0.0, 0.0) // Perfect availability, latency, sync

	score := ws.CalculateScore(qos, 1000, 10000, "provider1")

	// Perfect provider should have high score (close to 1.0)
	// availability: 1.0 * 0.3 = 0.3
	// latency: 1.0 * 0.3 = 0.3 (0 latency normalized to 1.0)
	// sync: 1.0 * 0.2 = 0.2 (0 sync normalized to 1.0)
	// stake: sqrt(0.1) * 0.2 ≈ 0.0632456 (square-root stake scaling)
	// total: 0.3 + 0.3 + 0.2 + 0.0632456 ≈ 0.8632456
	require.InDelta(t, 0.8632, score, 0.02)
}

// TestCalculateScorePoorProvider tests scoring for a poor provider
func TestCalculateScorePoorProvider(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	qos := createQoSReport(0.5, 30.0, 1200.0) // Poor availability, high latency, poor sync

	score := ws.CalculateScore(qos, 100, 10000, "provider1")

	// Poor provider should have lower score
	// availability: below minimum threshold => 0
	// latency: 0.0 * 0.3 = 0.0 (30s latency normalized to 0)
	// sync: 0.0 * 0.2 = 0.0 (1200s sync normalized to 0)
	// stake: sqrt(0.01) * 0.2 = 0.02 (square-root stake scaling)
	// total: 0.02
	require.InDelta(t, 0.02, score, 0.02)
}

// TestCalculateScoreMinimumChance ensures minimum selection chance is enforced
func TestCalculateScoreMinimumChance(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	qos := createQoSReport(0.0, 10.0, 1000.0) // Terrible metrics

	score := ws.CalculateScore(qos, 1, 1000000, "provider1")

	// Even terrible provider should get minimum chance
	require.GreaterOrEqual(t, score, 0.01)
}

// TestNormalizeLatency tests latency normalization
func TestNormalizeLatency(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	testCases := []struct {
		name     string
		latency  float64
		expected float64
	}{
		{"zero latency", 0.0, 1.0},
		{"low latency", 3.0, 0.9},
		{"medium latency", 15.0, 0.5},
		{"high latency", 30.0, 0.0},
		{"very high latency", 60.0, 0.0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			normalized := ws.normalizeLatency(tc.latency)
			require.InDelta(t, tc.expected, normalized, 0.01)
		})
	}
}

// TestNormalizeSync tests sync normalization
func TestNormalizeSync(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	testCases := []struct {
		name     string
		sync     float64
		expected float64
	}{
		{"zero sync lag", 0.0, 1.0},
		{"low sync lag", 120.0, 0.9},
		{"medium sync lag", 600.0, 0.5},
		{"high sync lag", 1200.0, 0.0},
		{"very high sync lag", 2400.0, 0.0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			normalized := ws.normalizeSync(tc.sync)
			require.InDelta(t, tc.expected, normalized, 0.01)
		})
	}
}

// TestNormalizeStake tests stake normalization
func TestNormalizeStake(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	testCases := []struct {
		name       string
		stake      int64
		totalStake int64
		expected   float64
	}{
		{"zero stake", 0, 10000, 0.0},
		// normalizeStake uses square-root scaling: normalized = sqrt(stake/totalStake)
		{"small stake", 100, 10000, 0.1},               // sqrt(0.01)
		{"medium stake", 2500, 10000, 0.5},             // sqrt(0.25)
		{"large stake", 5000, 10000, 0.7071067811865},  // sqrt(0.5)
		{"majority stake", 9000, 10000, 0.94868329805}, // sqrt(0.9)
		{"full stake", 10000, 10000, 1.0},
		{"exceeds total", 15000, 10000, 1.0}, // Capped at 1.0
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			normalized := ws.normalizeStake(float64(tc.stake), float64(tc.totalStake))
			require.InDelta(t, tc.expected, normalized, 0.01)
		})
	}
}

// TestSelectUpstreamSingleProvider tests selection with only one provider
func TestSelectUpstreamSingleProvider(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	providers := []UpstreamScore{
		{Address: "provider1", CompositeScore: 0.8, SelectionWeight: 0.8},
	}

	selected := ws.SelectUpstream(context.Background(), providers)
	require.Equal(t, "provider1", selected)
}

// TestSelectUpstreamEmptyList tests selection with empty provider list
func TestSelectUpstreamEmptyList(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	providers := []UpstreamScore{}

	selected := ws.SelectUpstream(context.Background(), providers)
	require.Equal(t, "", selected)
}

// TestSelectUpstreamDistribution tests that selection follows probability distribution
func TestSelectUpstreamDistribution(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)
	ws.SetDeterministicSeed(1234567) // Use fixed seed for deterministic test

	// Create providers with different scores
	providers := []UpstreamScore{
		{Address: "high_score", CompositeScore: 0.8, SelectionWeight: 0.8},
		{Address: "medium_score", CompositeScore: 0.4, SelectionWeight: 0.4},
		{Address: "low_score", CompositeScore: 0.2, SelectionWeight: 0.2},
	}

	// Run many selections and count results
	selections := make(map[string]int)
	iterations := 10000

	for i := 0; i < iterations; i++ {
		selected := ws.SelectUpstream(context.Background(), providers)
		selections[selected]++
	}

	// Expected probabilities:
	// high_score: 0.8 / (0.8 + 0.4 + 0.2) = 0.8 / 1.4 ≈ 0.571 (57.1%)
	// medium_score: 0.4 / 1.4 ≈ 0.286 (28.6%)
	// low_score: 0.2 / 1.4 ≈ 0.143 (14.3%)

	highScorePct := float64(selections["high_score"]) / float64(iterations)
	mediumScorePct := float64(selections["medium_score"]) / float64(iterations)
	lowScorePct := float64(selections["low_score"]) / float64(iterations)

	// Allow 5% deviation from expected
	require.InDelta(t, 0.571, highScorePct, 0.05, "high_score selection rate")
	require.InDelta(t, 0.286, mediumScorePct, 0.05, "medium_score selection rate")
	require.InDelta(t, 0.143, lowScorePct, 0.05, "low_score selection rate")

	t.Logf("Selection distribution over %d iterations:", iterations)
	t.Logf("  high_score: %.2f%% (expected ~57.1%%)", highScorePct*100)
	t.Logf("  medium_score: %.2f%% (expected ~28.6%%)", mediumScorePct*100)
	t.Logf("  low_score: %.2f%% (expected ~14.3%%)", lowScorePct*100)
}

// TestMinSelectionChanceIsAWeightFloorNotAProbabilityGuarantee documents the intended semantics:
// minSelectionChance is applied as a minimum *weight* (composite score floor), not a minimum *probability*.
// Therefore, when there are other large weights, the effective probability can be < minSelectionChance.
func TestMinSelectionChanceIsAWeightFloorNotAProbabilityGuarantee(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	config.MinSelectionChance = 0.01
	ws := NewUpstreamSelector(config)
	ws.SetDeterministicSeed(1234567)

	// Simulate one "dominant" provider and one provider that only gets the minimum weight floor.
	// Effective probability for the min provider is:
	//   p = 0.01 / (1.0 + 0.01) ≈ 0.00990099 < 0.01
	providers := []UpstreamScore{
		{Address: "dominant", CompositeScore: 1.0, SelectionWeight: 1.0},
		{Address: "min_floor", CompositeScore: config.MinSelectionChance, SelectionWeight: config.MinSelectionChance},
	}

	const iterations = 100000
	countMin := 0
	for i := 0; i < iterations; i++ {
		if ws.SelectUpstream(context.Background(), providers) == "min_floor" {
			countMin++
		}
	}

	got := float64(countMin) / float64(iterations)
	expected := config.MinSelectionChance / (1.0 + config.MinSelectionChance)

	// Statistical tolerance: keep it tight enough to detect regressions, wide enough to be stable.
	require.InDelta(t, expected, got, 0.002)
	// And explicitly prove it's not a probability guarantee.
	require.Less(t, got, config.MinSelectionChance)
}

func TestNormalizeStakeMaxInt64DoesNotOverflowOrNaN(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	max := float64(stdmath.MaxInt64)
	normalized := ws.normalizeStake(max, max)
	require.False(t, stdmath.IsNaN(normalized))
	require.False(t, stdmath.IsInf(normalized, 0))
	require.InDelta(t, 1.0, normalized, 0.000001)
}

func TestSelectUpstreamConcurrentDoesNotPanicAndReturnsValidProvider(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)
	ws.SetDeterministicSeed(1234567)

	providers := []UpstreamScore{
		{Address: "p1", CompositeScore: 0.8, SelectionWeight: 0.8},
		{Address: "p2", CompositeScore: 0.4, SelectionWeight: 0.4},
		{Address: "p3", CompositeScore: 0.2, SelectionWeight: 0.2},
	}

	const goroutines = 32
	const perG = 2000

	var total atomic.Int64
	var invalid atomic.Int64

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				selected := ws.SelectUpstream(context.Background(), providers)
				total.Add(1)
				switch selected {
				case "p1", "p2", "p3":
					// ok
				default:
					invalid.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(goroutines*perG), total.Load())
	require.Equal(t, int64(0), invalid.Load())
}

// TestSelectUpstreamEqualScores tests selection when all providers have equal scores
func TestSelectUpstreamEqualScores(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)
	ws.SetDeterministicSeed(1234567) // Use fixed seed for deterministic test

	providers := []UpstreamScore{
		{Address: "provider1", CompositeScore: 0.5, SelectionWeight: 0.5},
		{Address: "provider2", CompositeScore: 0.5, SelectionWeight: 0.5},
		{Address: "provider3", CompositeScore: 0.5, SelectionWeight: 0.5},
	}

	selections := make(map[string]int)
	iterations := 3000

	for i := 0; i < iterations; i++ {
		selected := ws.SelectUpstream(context.Background(), providers)
		selections[selected]++
	}

	// Each provider should be selected approximately equally (~33.3%)
	for _, provider := range providers {
		pct := float64(selections[provider.Address]) / float64(iterations)
		require.InDelta(t, 0.333, pct, 0.05)
	}
}

// TestSelectUpstreamZeroScores tests fallback when all scores are zero
func TestSelectUpstreamZeroScores(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)
	ws.SetDeterministicSeed(1234567) // Use fixed seed for deterministic test

	providers := []UpstreamScore{
		{Address: "provider1", CompositeScore: 0.0, SelectionWeight: 0.0},
		{Address: "provider2", CompositeScore: 0.0, SelectionWeight: 0.0},
		{Address: "provider3", CompositeScore: 0.0, SelectionWeight: 0.0},
	}

	// Should still select a provider (uniform random)
	selected := ws.SelectUpstream(context.Background(), providers)
	require.NotEmpty(t, selected)
	require.Contains(t, []string{"provider1", "provider2", "provider3"}, selected)
}

// TestStrategyLatencyAdjustment tests latency strategy score adjustments
func TestStrategyLatencyAdjustment(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	config.Strategy = StrategyLatency
	ws := NewUpstreamSelector(config)

	// High latency score should be boosted by strategy
	latency, sync := ws.applyStrategyAdjustments(0.8, 0.5)

	// Latency strategy should boost good latency scores
	require.Greater(t, latency, 0.75)
	require.InDelta(t, 0.5, sync, 0.01) // Sync should be unchanged
}

// TestStrategySyncAdjustment tests sync strategy score adjustments
func TestStrategySyncAdjustment(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	config.Strategy = StrategySyncFreshness
	ws := NewUpstreamSelector(config)

	// High sync score should be boosted by strategy
	latency, sync := ws.applyStrategyAdjustments(0.5, 0.8)

	require.InDelta(t, 0.5, latency, 0.01) // Latency should be unchanged
	require.Greater(t, sync, 0.75)         // Sync should be boosted
}

// TestCalculateUpstreamScores tests the full score calculation pipeline
func TestCalculateUpstreamScores(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	allAddresses := []string{"provider1", "provider2", "provider3"}
	ignoredProviders := map[string]struct{}{}

	providerData := map[string]*pairingtypes.QualityOfServiceReport{
		"provider1": createQoSReport(1.0, 0.1, 1.0),
		"provider2": createQoSReport(0.8, 0.3, 5.0),
		"provider3": createQoSReport(0.6, 0.5, 10.0),
	}

	stakes := map[string]int64{
		"provider1": 5000,
		"provider2": 3000,
		"provider3": 2000,
	}

	providerDataGetter := func(addr string) (*pairingtypes.QualityOfServiceReport, time.Time, bool) {
		qos, ok := providerData[addr]
		return qos, time.Now(), ok
	}

	stakeGetter := func(addr string) int64 {
		return stakes[addr]
	}

	scores, qosReports, _ := ws.CalculateUpstreamScores(allAddresses, ignoredProviders, providerDataGetter, stakeGetter)

	require.Len(t, scores, 3)
	require.Len(t, qosReports, 3)

	// provider1 should have highest score (best metrics, highest stake)
	require.Greater(t, scores[0].CompositeScore, scores[1].CompositeScore)
	require.Greater(t, scores[1].CompositeScore, scores[2].CompositeScore)

	// Verify all providers are present in reports
	for _, addr := range allAddresses {
		_, ok := qosReports[addr]
		require.True(t, ok, "QoS report missing for %s", addr)
	}
}

// TestCalculateUpstreamScoresWithIgnored tests that ignored providers are skipped
func TestCalculateUpstreamScoresWithIgnored(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	allAddresses := []string{"provider1", "provider2", "provider3"}
	ignoredProviders := map[string]struct{}{
		"provider2": {},
	}

	providerData := map[string]*pairingtypes.QualityOfServiceReport{
		"provider1": createQoSReport(1.0, 0.1, 1.0),
		"provider2": createQoSReport(0.8, 0.3, 5.0),
		"provider3": createQoSReport(0.6, 0.5, 10.0),
	}

	stakes := map[string]int64{
		"provider1": 5000,
		"provider2": 3000,
		"provider3": 2000,
	}

	providerDataGetter := func(addr string) (*pairingtypes.QualityOfServiceReport, time.Time, bool) {
		qos, ok := providerData[addr]
		return qos, time.Now(), ok
	}

	stakeGetter := func(addr string) int64 {
		return stakes[addr]
	}

	scores, qosReports, _ := ws.CalculateUpstreamScores(allAddresses, ignoredProviders, providerDataGetter, stakeGetter)

	// Should only have 2 providers (provider2 is ignored)
	require.Len(t, scores, 2)
	require.Len(t, qosReports, 2)

	// Verify provider2 is not in results
	for _, score := range scores {
		require.NotEqual(t, "provider2", score.Address)
	}
	_, ok := qosReports["provider2"]
	require.False(t, ok)
}

// TestGetConfig tests retrieving the configuration
func TestGetConfig(t *testing.T) {
	originalConfig := UpstreamSelectorConfig{
		AvailabilityWeight: 0.5,
		LatencyWeight:      0.3,
		SyncWeight:         0.15,
		StakeWeight:        0.05,
		MinSelectionChance: 0.02,
		Strategy:           StrategyLatency,
	}

	ws := NewUpstreamSelector(originalConfig)
	retrievedConfig := ws.GetConfig()

	// Weights will be normalized, so check they sum to 1.0
	totalWeight := retrievedConfig.AvailabilityWeight +
		retrievedConfig.LatencyWeight +
		retrievedConfig.SyncWeight +
		retrievedConfig.StakeWeight
	require.InDelta(t, 1.0, totalWeight, 0.001)
	require.Equal(t, originalConfig.MinSelectionChance, retrievedConfig.MinSelectionChance)
	require.Equal(t, originalConfig.Strategy, retrievedConfig.Strategy)
}

// TestUpdateStrategy tests changing the strategy
func TestUpdateStrategy(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	require.Equal(t, StrategyBalanced, ws.strategy)

	ws.UpdateStrategy(StrategyLatency)
	require.Equal(t, StrategyLatency, ws.strategy)

	ws.UpdateStrategy(StrategySyncFreshness)
	require.Equal(t, StrategySyncFreshness, ws.strategy)
}

// BenchmarkCalculateScore benchmarks score calculation
func BenchmarkCalculateScore(b *testing.B) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	qos := createQoSReport(0.95, 0.15, 2.5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ws.CalculateScore(qos, 5000, 50000, "provider1")
	}
}

// BenchmarkSelectUpstream benchmarks provider selection
func BenchmarkSelectUpstream(b *testing.B) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)
	ws.SetDeterministicSeed(1234567) // Use fixed seed for deterministic benchmark

	providers := make([]UpstreamScore, 50)
	for i := 0; i < 50; i++ {
		providers[i] = UpstreamScore{
			Address:         "provider" + string(rune(i)),
			CompositeScore:  0.5 + float64(i)*0.01,
			SelectionWeight: 0.5 + float64(i)*0.01,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ws.SelectUpstream(context.Background(), providers)
	}
}

// BenchmarkCalculateUpstreamScores benchmarks full score calculation pipeline
func BenchmarkCalculateUpstreamScores(b *testing.B) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	allAddresses := make([]string, 50)
	providerData := make(map[string]*pairingtypes.QualityOfServiceReport)
	stakes := make(map[string]int64)

	for i := 0; i < 50; i++ {
		addr := "provider" + string(rune(i))
		allAddresses[i] = addr
		providerData[addr] = createQoSReport(0.9, 0.2, 3.0)
		stakes[addr] = 1000 + int64(i)*100
	}

	ignoredProviders := map[string]struct{}{}

	providerDataGetter := func(addr string) (*pairingtypes.QualityOfServiceReport, time.Time, bool) {
		qos, ok := providerData[addr]
		return qos, time.Now(), ok
	}

	stakeGetter := func(addr string) int64 {
		return stakes[addr]
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ws.CalculateUpstreamScores(allAddresses, ignoredProviders, providerDataGetter, stakeGetter)
	}
}

// TestQoSReportGeneration tests that QoS reports are generated correctly
func TestQoSReportGeneration(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)

	allAddresses := []string{"provider1"}
	ignoredProviders := map[string]struct{}{}

	expectedLatency := 0.15
	expectedSync := 2.5
	expectedAvailability := 0.95

	providerData := map[string]*pairingtypes.QualityOfServiceReport{
		"provider1": createQoSReport(expectedAvailability, expectedLatency, expectedSync),
	}

	stakes := map[string]int64{
		"provider1": 5000,
	}

	lastUpdateTime := time.Now().Add(-5 * time.Minute)

	providerDataGetter := func(addr string) (*pairingtypes.QualityOfServiceReport, time.Time, bool) {
		qos, ok := providerData[addr]
		return qos, lastUpdateTime, ok
	}

	stakeGetter := func(addr string) int64 {
		return stakes[addr]
	}

	_, qosReports, _ := ws.CalculateUpstreamScores(allAddresses, ignoredProviders, providerDataGetter, stakeGetter)

	require.Len(t, qosReports, 1)
	report := qosReports["provider1"]

	require.Equal(t, "provider1", report.ProviderAddress)
	require.InDelta(t, expectedAvailability, report.AvailabilityScore, 0.01)
	require.InDelta(t, expectedLatency, report.LatencyScore, 0.01)
	require.InDelta(t, expectedSync, report.SyncScore, 0.01)
	require.Greater(t, report.GenericScore, 0.0)
}

// TestStrategyDistributedFlattening tests that distributed strategy flattens the score curve
func TestStrategyDistributedFlattening(t *testing.T) {
	balancedConfig := DefaultUpstreamSelectorConfig()
	balancedConfig.Strategy = StrategyBalanced
	balancedWS := NewUpstreamSelector(balancedConfig)

	distributedConfig := DefaultUpstreamSelectorConfig()
	distributedConfig.Strategy = StrategyDistributed
	distributedWS := NewUpstreamSelector(distributedConfig)

	// Apply strategy adjustments
	balancedLatency, balancedSync := balancedWS.applyStrategyAdjustments(0.8, 0.8)
	distributedLatency, distributedSync := distributedWS.applyStrategyAdjustments(0.8, 0.8)

	// Distributed strategy should flatten scores (make them lower)
	require.Less(t, distributedLatency, balancedLatency)
	require.Less(t, distributedSync, balancedSync)
}

// TestSelectUpstreamBestMode verifies SelectionModeBest always returns the highest-scoring
// provider, regardless of position in the candidate list.
func TestSelectUpstreamBestMode(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	config.SelectionMode = SelectionModeBest
	ws := NewUpstreamSelector(config)
	ws.SetDeterministicSeed(1234567)

	// Best score deliberately placed last so a first-wins scan would fail this test
	providers := []UpstreamScore{
		{Address: "low_score", CompositeScore: 0.2, SelectionWeight: 0.2},
		{Address: "medium_score", CompositeScore: 0.4, SelectionWeight: 0.4},
		{Address: "high_score", CompositeScore: 0.8, SelectionWeight: 0.8},
	}

	for i := 0; i < 1000; i++ {
		require.Equal(t, "high_score", ws.SelectUpstream(context.Background(), providers))
	}
}

// TestSelectUpstreamBestModeTieBreak verifies that exact ties are broken uniformly at
// random rather than always landing on the first candidate. This is the degraded-chain
// case: CalculateScore collapses every unhealthy provider to exactly minSelectionChance,
// so a first-wins argmax would pin all traffic onto one address.
func TestSelectUpstreamBestModeTieBreak(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	config.SelectionMode = SelectionModeBest
	ws := NewUpstreamSelector(config)
	ws.SetDeterministicSeed(1234567)

	// All tied at the starvation floor, as CalculateScore would leave them
	providers := []UpstreamScore{
		{Address: "dead1", CompositeScore: 0.01, SelectionWeight: 0.01},
		{Address: "dead2", CompositeScore: 0.01, SelectionWeight: 0.01},
		{Address: "dead3", CompositeScore: 0.01, SelectionWeight: 0.01},
	}

	selections := make(map[string]int)
	iterations := 9000
	for i := 0; i < iterations; i++ {
		selections[ws.SelectUpstream(context.Background(), providers)]++
	}

	// Uniform across the three maxima (~33.3% each)
	for _, addr := range []string{"dead1", "dead2", "dead3"} {
		share := float64(selections[addr]) / float64(iterations)
		require.InDelta(t, 1.0/3.0, share, 0.03, "tie-break not uniform for %s", addr)
	}
}

// TestSelectUpstreamBestModeTracksLeader verifies Best mode follows the score, so a
// provider that overtakes the incumbent immediately takes all traffic.
func TestSelectUpstreamBestModeTracksLeader(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	config.SelectionMode = SelectionModeBest
	ws := NewUpstreamSelector(config)
	ws.SetDeterministicSeed(1234567)

	providers := []UpstreamScore{
		{Address: "provider1", CompositeScore: 0.9, SelectionWeight: 0.9},
		{Address: "provider2", CompositeScore: 0.5, SelectionWeight: 0.5},
	}
	require.Equal(t, "provider1", ws.SelectUpstream(context.Background(), providers))

	// provider2 overtakes
	providers[1].SelectionWeight = 0.95
	require.Equal(t, "provider2", ws.SelectUpstream(context.Background(), providers))
}

// TestSelectUpstreamWeightedModeUnchanged pins the default: an unconfigured selector still
// spreads traffic proportionally, so adding Best mode did not change existing behaviour.
func TestSelectUpstreamWeightedModeUnchanged(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	ws := NewUpstreamSelector(config)
	ws.SetDeterministicSeed(1234567)
	require.Equal(t, SelectionModeWeightedRandom, ws.GetConfig().SelectionMode)

	providers := []UpstreamScore{
		{Address: "high_score", CompositeScore: 0.8, SelectionWeight: 0.8},
		{Address: "low_score", CompositeScore: 0.2, SelectionWeight: 0.2},
	}

	selections := make(map[string]int)
	iterations := 10000
	for i := 0; i < iterations; i++ {
		selections[ws.SelectUpstream(context.Background(), providers)]++
	}

	// 0.8 / 1.0 = 80% vs 20% — the low scorer must still get real traffic
	require.InDelta(t, 0.8, float64(selections["high_score"])/float64(iterations), 0.03)
	require.InDelta(t, 0.2, float64(selections["low_score"])/float64(iterations), 0.03)
}

// TestSelectUpstreamBestModeStats verifies the shared stats tail is populated identically
// in Best mode, with RNGValue left at zero since no weighted draw took place.
func TestSelectUpstreamBestModeStats(t *testing.T) {
	config := DefaultUpstreamSelectorConfig()
	config.SelectionMode = SelectionModeBest
	ws := NewUpstreamSelector(config)

	providers := []UpstreamScore{
		{Address: "low_score", CompositeScore: 0.2, SelectionWeight: 0.2},
		{Address: "high_score", CompositeScore: 0.8, SelectionWeight: 0.8},
	}
	details := []UpstreamScoreDetails{
		{Address: "low_score", Composite: 0.2},
		{Address: "high_score", Composite: 0.8},
	}

	selected, stats := ws.SelectUpstreamWithStats(context.Background(), providers, details)
	require.Equal(t, "high_score", selected)
	require.NotNil(t, stats)
	require.Equal(t, "high_score", stats.SelectedProvider)
	require.Equal(t, 0.0, stats.RNGValue)
	require.Len(t, stats.UpstreamScores, 2)
}

// TestSelectionStatsCarriesMode verifies every selection path stamps the policy that
// produced the pick, including the short-circuits where RNGValue is zero for reasons
// unrelated to the mode.
func TestSelectionStatsCarriesMode(t *testing.T) {
	twoProviders := []UpstreamScore{
		{Address: "low_score", CompositeScore: 0.2, SelectionWeight: 0.2},
		{Address: "high_score", CompositeScore: 0.8, SelectionWeight: 0.8},
	}
	oneProvider := []UpstreamScore{{Address: "solo", CompositeScore: 0.5, SelectionWeight: 0.5}}
	zeroScores := []UpstreamScore{
		{Address: "zero1", CompositeScore: 0, SelectionWeight: 0},
		{Address: "zero2", CompositeScore: 0, SelectionWeight: 0},
	}

	for _, tc := range []struct {
		name      string
		mode      SelectionMode
		providers []UpstreamScore
	}{
		{"weighted/normal", SelectionModeWeightedRandom, twoProviders},
		{"weighted/single", SelectionModeWeightedRandom, oneProvider},
		{"weighted/all_zero", SelectionModeWeightedRandom, zeroScores},
		{"best/normal", SelectionModeBest, twoProviders},
		{"best/single", SelectionModeBest, oneProvider},
		{"best/all_zero", SelectionModeBest, zeroScores},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := DefaultUpstreamSelectorConfig()
			config.SelectionMode = tc.mode
			ws := NewUpstreamSelector(config)
			ws.SetDeterministicSeed(1234567)

			_, stats := ws.SelectUpstreamWithStats(context.Background(), tc.providers, nil)
			require.NotNil(t, stats)
			require.Equal(t, tc.mode, stats.Mode)
		})
	}
}

// TestFormatSelectionStatsIncludesMode verifies the debugging header names the policy, so
// an RNG of 0 can be read correctly rather than being mistaken for a weighted draw.
func TestFormatSelectionStatsIncludesMode(t *testing.T) {
	stats := &SelectionStats{
		UpstreamScores:   []UpstreamScoreDetails{{Address: "provider1", Composite: 0.8}},
		RNGValue:         0.0,
		SelectedProvider: "provider1",
		Mode:             SelectionModeBest,
	}
	require.Contains(t, stats.FormatSelectionStats(), "| Mode: best |")

	stats.Mode = SelectionModeWeightedRandom
	stats.RNGValue = 0.42
	formatted := stats.FormatSelectionStats()
	require.Contains(t, formatted, "| Mode: weighted_random |")
	require.Contains(t, formatted, "| RNG: 0.420000 |")
	require.Contains(t, formatted, "| Selected: provider1")

	// nil receiver stays safe — the header is skipped when there are no stats
	var nilStats *SelectionStats
	require.Equal(t, "", nilStats.FormatSelectionStats())
}
