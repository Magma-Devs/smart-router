package lavaslices

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStability(t *testing.T) {
	// Test case 1: Variance of a single data point (undefined)
	data := []float64{5.0, 5.0, 5.0}
	compare := 5.0
	stability := Stability(data, compare)
	require.Less(t, stability, 0.01)

	data = []float64{100.0, 100.0, 100.0}
	compare = 5.0
	stability = Stability(data, compare)
	require.Greater(t, stability, 1.0)

	data = []float64{10.0, 10.0, 10.0, 20.0, 5.0}
	compare = 10.0
	stability = Stability(data, compare)
	require.Greater(t, stability, 0.0)
	require.Less(t, stability, 0.5)

	data2 := []time.Duration{186 * time.Millisecond, 220 * time.Millisecond, 220 * time.Millisecond, 220 * time.Millisecond, 278 * time.Millisecond, 281 * time.Millisecond, 285 * time.Millisecond, 295 * time.Millisecond, 295 * time.Millisecond, 295 * time.Millisecond, 363 * time.Millisecond}
	compare2 := 281 * time.Millisecond // median of data2
	stability = Stability(data2, compare2)
	require.Greater(t, stability, 0.0)

	data2 = []time.Duration{938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 938 * time.Millisecond, 939 * time.Millisecond, 939 * time.Millisecond, 939 * time.Millisecond, 939 * time.Millisecond, 939 * time.Millisecond, 939 * time.Millisecond, 939 * time.Millisecond, 939 * time.Millisecond, 939 * time.Millisecond, 1876 * time.Millisecond, 1876 * time.Millisecond, 1877 * time.Millisecond, 1878 * time.Millisecond, 1908 * time.Millisecond}
	compare2 = 938 * time.Millisecond // median of data2
	stability = Stability(data2, compare2)
	require.Less(t, stability, 0.2)
}

func TestPercentile(t *testing.T) {
	// test it equals median
	for _, tt := range []struct {
		name   string
		slice  []int
		median int
	}{
		{"empty", []int{}, 0},
		{"one element", []int{1}, 1},
		{"min is first", []int{1, 2, 3}, 2},
		{"min is middle", []int{2, 1, 3}, 2},
		{"min is last", []int{3, 2, 1}, 2},
		{"min is zero", []int{3, 0, 1}, 1},
		{"min < zero", []int{3, -2, 1}, 1},
		{"min twice", []int{3, 1, 1}, 1},
		{"even length", []int{4, 4, 2, 2}, 3},
		{"even length identical", []int{4, 4, 4, 4}, 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			percentile := Percentile(tt.slice, 0.5, false)
			require.Equal(t, tt.median, percentile)
		})
	}

	for _, tt2 := range []struct {
		name       string
		slice      []time.Duration
		percentile time.Duration
		rank       float64
	}{
		{"rank empty", []time.Duration{}, 0, 0.3},
		{"rank one element", []time.Duration{1 * time.Millisecond}, 1 * time.Millisecond, 0.3},
		{"rank min is first", []time.Duration{0 * time.Millisecond, 1 * time.Millisecond, 2 * time.Millisecond}, 0, 0.33},
		{"rank min is middle", []time.Duration{2 * time.Millisecond, 1 * time.Millisecond, 3 * time.Millisecond}, 1 * time.Millisecond, 0.33},
		{"rank min is last", []time.Duration{3 * time.Millisecond, 2 * time.Millisecond, 1 * time.Millisecond}, 1 * time.Millisecond, 0.33},
		{"rank min is zero", []time.Duration{3 * time.Millisecond, 0 * time.Millisecond, 1 * time.Millisecond}, 0, 0.33},
		{"rank min < zero", []time.Duration{3 * time.Millisecond, -2 * time.Millisecond, 1 * time.Millisecond}, -2 * time.Millisecond, 0.33},
		{"rank min twice", []time.Duration{3 * time.Millisecond, 1 * time.Millisecond, 1 * time.Millisecond}, 1 * time.Millisecond, 0.33},
		{"rank even length", []time.Duration{4 * time.Millisecond, 4 * time.Millisecond, 2 * time.Millisecond, 2 * time.Millisecond}, 2 * time.Millisecond, 0.33},
		{"rank even length", []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}, 1330 * time.Microsecond, 0.33},
		{"rank even length identical", []time.Duration{4 * time.Millisecond, 4 * time.Millisecond, 4 * time.Millisecond, 4 * time.Millisecond}, 4 * time.Millisecond, 0.33},
	} {
		t.Run(tt2.name, func(t *testing.T) {
			percentile := Percentile(tt2.slice, tt2.rank, false)
			require.Equal(t, tt2.percentile, percentile)
		})
	}
}

func TestMap(t *testing.T) {
	mapFunc := func(_ int) int { return 10 }
	require.Equal(t, []int{}, Map([]int{}, mapFunc))
	require.Equal(t, []int{10}, Map([]int{1}, mapFunc))
	require.Equal(t, []int{10, 10, 10}, Map([]int{1, 2, 3}, mapFunc))
}
