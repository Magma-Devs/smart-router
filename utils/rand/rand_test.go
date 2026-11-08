package rand

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func isUniformlyDistributed(t *testing.T, f func(n int64) int64) {
	t.Helper()

	const iterations = 100000
	const maxValue = 50

	numberToCount := make(map[int64]int64, maxValue)

	for i := 0; i < iterations; i++ {
		out := f(maxValue)
		numberToCount[out]++
	}

	target := iterations / maxValue
	const maxPercentDivergence = 0.07

	for i := 0; i < maxValue; i++ {
		count := numberToCount[int64(i)]
		require.InEpsilon(t, target, count, maxPercentDivergence, "slot "+strconv.Itoa(i))
	}
}

func TestDeterminism(t *testing.T) {
	// copied from the output of the loop below
	expected := []int64{64, 70, 6, 97, 97, 4, 0, 63, 31, 51, 37, 78, 39, 35, 33, 68, 0, 24, 32, 20, 89, 40, 27, 24, 67, 7, 95, 60, 11, 26, 16, 94}

	rng := New([]byte("pre-determined-data"))

	for i := 0; i < 32; i++ {
		random := rng.Int63n(100)
		require.Equal(t, expected[i], random, "item "+strconv.Itoa(i))
	}

	Seed(rng, []byte("other-determined-data"))

	count := 0
	for i := 0; i < 32; i++ {
		random := rng.Int63n(100)
		if expected[i] == random {
			count++
		}
	}
	require.Less(t, count, 32)
}

func TestDistribution(t *testing.T) {
	rng := New([]byte("pre-determined-data"))
	t.Run("new rand", func(t *testing.T) {
		isUniformlyDistributed(t, rng.Int63n)
	})

	Seed(rng, []byte("other-determined-data"))
	t.Run("new seed", func(t *testing.T) {
		isUniformlyDistributed(t, rng.Int63n)
	})
}

// isUniformWithinSigmas is isUniformlyDistributed for a source that differs on every run. A fixed
// percentage bound is only safe on a seeded source, where it passes or fails the same way each
// time; here each slot's count is Binomial(iterations, 1/maxValue), so the bound is a number of
// standard deviations of that distribution. At six sigmas a fair source fails one of the 200 slot
// checks in TestProtocolRandDistribution about once in 2.5 million runs; the fixed 7% bound is
// 3.2 sigmas there and failed about one run in four.
func isUniformWithinSigmas(t *testing.T, f func(n int64) int64) {
	t.Helper()

	const iterations = 100000
	const maxValue = 50
	const sigmas = 6

	numberToCount := make(map[int64]int64, maxValue)
	for i := 0; i < iterations; i++ {
		numberToCount[f(maxValue)]++
	}

	p := 1.0 / maxValue
	target := iterations * p
	bound := sigmas * math.Sqrt(iterations*p*(1-p))
	for i := 0; i < maxValue; i++ {
		count := float64(numberToCount[int64(i)])
		require.InDelta(t, target, count, bound, "slot "+strconv.Itoa(i))
	}
}

func TestProtocolRandDistribution(t *testing.T) {
	InitRandomSeed()
	isUniformWithinSigmas(t, func(n int64) int64 { return int64(Intn(int(n))) })
	isUniformWithinSigmas(t, func(n int64) int64 { return Int63() % n })
	isUniformWithinSigmas(t, func(n int64) int64 { return int64(Uint64() % uint64(n)) })
	isUniformWithinSigmas(t, func(n int64) int64 { return int64(Float64() * float64(n)) })
	require.Panics(t, func() { Intn(0) })
}
