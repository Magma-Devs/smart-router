package provideroptimizer

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/utils/score"
	"github.com/stretchr/testify/require"
)

// Each getter reads its own calculator, and falls back to its own defaults on a nil optimizer, a
// nil calculator, or bounds the selector cannot normalise by (here a P10 clamped to 0).
func TestAdaptiveBounds_EachGetterReadsItsOwnCalculatorAndDefaults(t *testing.T) {
	withSamples := func(minP10, maxP10, minMax, maxMax float64, samples ...float64) *score.AdaptiveMaxCalculator {
		calc := score.NewAdaptiveMaxCalculator(time.Hour, minP10, maxP10, minMax, maxMax, 100)
		now := time.Now()
		for _, s := range samples {
			require.NoError(t, calc.AddSample(s, now))
		}
		return calc
	}
	latency := withSamples(score.AdaptiveP10MinBound, score.AdaptiveP10MaxBound, score.DefaultLatencyAdaptiveMinMax, score.DefaultLatencyAdaptiveMaxMax, 0.2, 0.4, 0.6, 3, 5)
	sync := withSamples(score.AdaptiveSyncP10MinBound, score.AdaptiveSyncP10MaxBound, score.DefaultSyncAdaptiveMinMax, score.DefaultSyncAdaptiveMaxMax, 2, 40, 80, 300, 900)
	zeroP10 := withSamples(0, 0, 1, 30, 1, 2, 3)

	latencyP10, latencyP90 := latency.GetAdaptiveBounds()
	syncP10, syncP90 := sync.GetAdaptiveBounds()
	require.NotEqual(t, [2]float64{latencyP10, latencyP90}, [2]float64{syncP10, syncP90}, "the two calculators must answer differently for this test to tell them apart")

	for _, tc := range []struct {
		name                     string
		po                       *ProviderOptimizer
		wantLatP10, wantLatP90   float64
		wantSyncP10, wantSyncP90 float64
	}{
		{"nil optimizer", nil, score.AdaptiveP10MinBound, score.DefaultLatencyAdaptiveMaxMax, score.AdaptiveSyncP10MinBound, score.DefaultSyncAdaptiveMaxMax},
		{"nil calculators", &ProviderOptimizer{}, score.AdaptiveP10MinBound, score.DefaultLatencyAdaptiveMaxMax, score.AdaptiveSyncP10MinBound, score.DefaultSyncAdaptiveMaxMax},
		{"own calculators", &ProviderOptimizer{globalLatencyCalculator: latency, globalSyncCalculator: sync}, latencyP10, latencyP90, syncP10, syncP90},
		{"unusable bounds", &ProviderOptimizer{globalLatencyCalculator: zeroP10, globalSyncCalculator: zeroP10}, score.AdaptiveP10MinBound, score.DefaultLatencyAdaptiveMaxMax, score.AdaptiveSyncP10MinBound, score.DefaultSyncAdaptiveMaxMax},
	} {
		p10, p90 := tc.po.getAdaptiveLatencyBounds()
		require.Equal(t, [2]float64{tc.wantLatP10, tc.wantLatP90}, [2]float64{p10, p90}, "%s: latency", tc.name)
		p10, p90 = tc.po.getAdaptiveSyncBounds()
		require.Equal(t, [2]float64{tc.wantSyncP10, tc.wantSyncP90}, [2]float64{p10, p90}, "%s: sync", tc.name)
	}
}
