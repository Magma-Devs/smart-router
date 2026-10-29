package provideroptimizer

import (
	"math"
	"testing"

	"github.com/magma-Devs/smart-router/utils/score"
	"github.com/stretchr/testify/require"
)

// normalizedDimension is one "lower is better" QoS dimension the selector scores: latency
// and sync lag go through the same normalization, so every case below runs for both.
type normalizedDimension struct {
	name      string
	worst     float64 // the fixed worst value the dimension falls back to
	normalize func(ws *UpstreamSelector, value float64) float64
	configure func(ws *UpstreamSelector, adaptive bool, bounds func() (p10, p90 float64))
}

var normalizedDimensions = []normalizedDimension{
	{
		name:      "latency",
		worst:     score.WorstLatencyScore,
		normalize: func(ws *UpstreamSelector, v float64) float64 { return ws.normalizeLatency(v) },
		configure: func(ws *UpstreamSelector, adaptive bool, bounds func() (float64, float64)) {
			ws.useAdaptiveLatencyMax, ws.adaptiveLatencyGetter = adaptive, bounds
		},
	},
	{
		name:      "sync",
		worst:     score.WorstSyncScore,
		normalize: func(ws *UpstreamSelector, v float64) float64 { return ws.normalizeSync(v) },
		configure: func(ws *UpstreamSelector, adaptive bool, bounds func() (float64, float64)) {
			ws.useAdaptiveSyncMax, ws.adaptiveSyncGetter = adaptive, bounds
		},
	},
}

func fixedBounds(p10, p90 float64) func() (float64, float64) {
	return func() (float64, float64) { return p10, p90 }
}

// The fixed-maximum scale: 1 at or below zero, 0 at or above the dimension's own worst
// value, linear between. Each dimension is checked at half its OWN worst value, which is
// exactly 0.5 only if it is scaled by the right constant — so latency scaled by sync's
// 1200 s, or sync by latency's 30 s, fails here.
func TestNormalizeInverted_FixedMaximum(t *testing.T) {
	for _, d := range normalizedDimensions {
		t.Run(d.name, func(t *testing.T) {
			ws := &UpstreamSelector{}
			for _, tc := range []struct {
				value, want float64
			}{
				{-1, 1},
				{0, 1},
				{d.worst / 2, 0.5},
				{d.worst, 0},
				{d.worst * 2, 0},
			} {
				require.Equal(t, tc.want, d.normalize(ws, tc.value), "value %v", tc.value)
			}
		})
	}
}

// The adaptive scale applies only when it is switched on AND has a bounds source. Either
// one missing means the fixed maximum.
func TestNormalizeInverted_AdaptiveNeedsBothTheFlagAndTheGetter(t *testing.T) {
	for _, d := range normalizedDimensions {
		t.Run(d.name, func(t *testing.T) {
			half := d.worst / 2 // 0.5 on the fixed scale; 0 on the adaptive one below

			flagOnly := &UpstreamSelector{}
			d.configure(flagOnly, true, nil)
			require.Equal(t, 0.5, d.normalize(flagOnly, half), "switched on with no bounds source")

			getterOnly := &UpstreamSelector{}
			d.configure(getterOnly, false, fixedBounds(1, 3))
			require.Equal(t, 0.5, d.normalize(getterOnly, half), "a bounds source while switched off")
		})
	}
}

// With valid P10-P90 bounds the value is clamped into the range and scaled across it.
func TestNormalizeInverted_AdaptiveRange(t *testing.T) {
	for _, d := range normalizedDimensions {
		t.Run(d.name, func(t *testing.T) {
			ws := &UpstreamSelector{}
			d.configure(ws, true, fixedBounds(1, 3))
			for _, tc := range []struct {
				value, want float64
			}{
				{0.5, 1}, // below P10: clamped up, the best score
				{1, 1},   // at P10
				{2, 0.5}, // the middle of the range
				{3, 0},   // at P90
				{5, 0},   // above P90: clamped down, the worst score
			} {
				require.Equal(t, tc.want, d.normalize(ws, tc.value), "value %v in [1, 3]", tc.value)
			}
		})
	}
}

// Bounds that cannot describe a range fall back to the fixed maximum rather than producing
// a score from them. Probed at half the worst value: 0.5 proves the fallback ran, because
// any of these bounds used as a range would give something else.
func TestNormalizeInverted_InvalidBoundsFallBackToTheFixedMaximum(t *testing.T) {
	for _, d := range normalizedDimensions {
		t.Run(d.name, func(t *testing.T) {
			for _, bad := range []struct {
				name     string
				p10, p90 float64
			}{
				{"NaN P10", math.NaN(), 3},
				{"NaN P90", 1, math.NaN()},
				{"infinite P10", math.Inf(1), 3},
				{"infinite P90", 1, math.Inf(1)},
				{"negative infinite P10", math.Inf(-1), 3},
				{"zero P10", 0, 3},
				{"zero P90", 1, 0},
				{"negative P10", -1, 3},
				{"P90 equal to P10", 2, 2},
				{"P90 below P10", 3, 1},
			} {
				ws := &UpstreamSelector{}
				d.configure(ws, true, fixedBounds(bad.p10, bad.p90))
				require.Equal(t, 0.5, d.normalize(ws, d.worst/2), bad.name)
			}
		})
	}
}
