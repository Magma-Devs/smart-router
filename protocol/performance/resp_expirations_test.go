package performance

import (
	"math"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/stretchr/testify/require"
)

// chartFinalizedMultiplier and chartNonFinalizedMultiplier are the published
// chart's own defaults (charts/smart-router/values.yaml — expiration_multiplier
// and expiration_non_finalized_multiplier). They are the numbers this whole
// seam exists to carry, so they are named rather than inlined: if the chart
// changes them, the assertions below should be re-read, not silently pass.
const (
	chartFinalizedMultiplier    = 1.5
	chartNonFinalizedMultiplier = 1.25
)

func TestRespCacheExpirationsAbsentBlockKeepsDefaults(t *testing.T) {
	v, _ := newViperWithYAML(t, `cache-be: "cache:20100"`)
	expirations, err := LoadRespCacheExpirations(v)
	require.NoError(t, err)
	require.False(t, expirations.configured())
	require.Equal(t, core.DefaultPolicy(), expirations.Policy(),
		"no resp-cache block must resolve to exactly the cache server's flag defaults")
}

func TestRespCacheExpirationsConnectionOnlyBlockKeepsDefaults(t *testing.T) {
	// The regression that matters most: every resp-cache deployment written
	// before this seam existed sets connection keys only, and must keep the
	// TTLs it has today.
	v, _ := newViperWithYAML(t, `
resp-cache:
  addresses: ["cache:6379"]
  key-prefix: prod
`)
	expirations, err := LoadRespCacheExpirations(v)
	require.NoError(t, err)
	require.False(t, expirations.configured())
	require.Equal(t, core.DefaultPolicy(), expirations.Policy())
}

// TestRespCacheExpirationsCarryChartMultipliers is the ticket's question in
// executable form: a RESP deployment mirroring the chart's cache-server
// multipliers now gets the sidecar's effective table instead of the unscaled
// defaults.
func TestRespCacheExpirationsCarryChartMultipliers(t *testing.T) {
	v, _ := newViperWithYAML(t, `
resp-cache:
  addresses: ["cache:6379"]
  expiration-multiplier: 1.5
  expiration-non-finalized-multiplier: 1.25
`)
	expirations, err := LoadRespCacheExpirations(v)
	require.NoError(t, err)
	require.True(t, expirations.configured())

	policy := expirations.Policy()
	require.Equal(t, 90*time.Minute, policy.Finalized,
		"1h default scaled by the chart's 1.5 — the 30 minutes a RESP deployment silently lost")
	require.Equal(t, 625*time.Millisecond, policy.NonFinalized,
		"500ms default scaled by the chart's 1.25 — the second dropped multiplier")

	// Untouched keys keep their defaults: a block that sets two multipliers
	// must not restate the whole table to keep the rest.
	require.Equal(t, core.DefaultExpirationNodeErrors, policy.NodeErrors)
	require.Equal(t, core.DefaultExpirationBlocksHashesToHeights, policy.BlocksHashesToHeights)
}

// TestRespCacheNonFinalizedFloorBitesOnFastChains pins WHY the non-finalized
// multiplier is worth carrying. It reads as an inert knob because the
// commonly-measured 13s chain never reaches the floor — the eighth-of-a-block
// term wins there, which is exactly why the two paths look identical when
// measured on one. Below a 5s block time the floor decides, and the two
// backends diverge by the multiplier.
func TestRespCacheNonFinalizedFloorBitesOnFastChains(t *testing.T) {
	unscaled := core.DefaultPolicy()
	scaled := RespCacheExpirations{NonFinalizedMultiplier: chartNonFinalizedMultiplier}.Policy()

	for _, tc := range []struct {
		name                     string
		averageBlockTime         time.Duration
		wantUnscaled, wantScaled time.Duration
	}{
		// Real average_block_time values from specs/.
		{"250ms chain", 250 * time.Millisecond, 500 * time.Millisecond, 625 * time.Millisecond},
		{"2s chain", 2 * time.Second, 500 * time.Millisecond, 625 * time.Millisecond},
		// 5s is the crossover: an eighth of it is exactly the scaled floor.
		{"6.5s chain", 6500 * time.Millisecond, 812500 * time.Microsecond, 812500 * time.Microsecond},
		{"13s chain", 13 * time.Second, 1625 * time.Millisecond, 1625 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantUnscaled, unscaled.ForChain(tc.averageBlockTime))
			require.Equal(t, tc.wantScaled, scaled.ForChain(tc.averageBlockTime))
		})
	}
}

func TestRespCacheExpirationsExplicitDurations(t *testing.T) {
	v, _ := newViperWithYAML(t, `
resp-cache:
  addresses: ["cache:6379"]
  expiration: 2h
  expiration-non-finalized: 750ms
  expiration-finalized-node-errors: 100ms
  expiration-blocks-hashes-to-heights: 24h
`)
	expirations, err := LoadRespCacheExpirations(v)
	require.NoError(t, err)

	policy := expirations.Policy()
	require.Equal(t, 2*time.Hour, policy.Finalized)
	require.Equal(t, 750*time.Millisecond, policy.NonFinalized)
	require.Equal(t, 100*time.Millisecond, policy.NodeErrors)
	require.Equal(t, 24*time.Hour, policy.BlocksHashesToHeights)
}

// A multiplier scales the explicit base, not the default — same order the
// cache server applies it in (ExpirationFinalized = expiration * multiplier).
func TestRespCacheExpirationsMultiplierScalesExplicitBase(t *testing.T) {
	v, _ := newViperWithYAML(t, `
resp-cache:
  addresses: ["cache:6379"]
  expiration: 2h
  expiration-multiplier: 1.5
`)
	expirations, err := LoadRespCacheExpirations(v)
	require.NoError(t, err)
	require.Equal(t, 3*time.Hour, expirations.Policy().Finalized)
}

// An explicitly-set non-positive expiration is a startup error rather than a
// clamp: on a RESP backend a zero TTL is a key with NO expiry, in a store the
// customer owns and that survives restarts.
func TestRespCacheExpirationsRejectNonPositive(t *testing.T) {
	for _, tc := range []struct {
		name, yaml, wantKey string
	}{
		{"zero duration", "expiration: 0s", RespCacheExpirationKey},
		{"negative duration", "expiration-non-finalized: -1s", RespCacheExpirationNonFinalizedKey},
		{"zero multiplier", "expiration-multiplier: 0", RespCacheExpirationMultiplierKey},
		{"negative multiplier", "expiration-non-finalized-multiplier: -2", RespCacheExpirationNonFinalizedMultiplierKey},
		{"zero node errors", "expiration-finalized-node-errors: 0s", RespCacheExpirationNodeErrorsKey},
		{"zero hashes", "expiration-blocks-hashes-to-heights: 0s", RespCacheExpirationBlocksHashesToHeightsKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := newViperWithYAML(t, "resp-cache:\n  addresses: [\"cache:6379\"]\n  "+tc.yaml+"\n")
			_, err := LoadRespCacheExpirations(v)
			require.ErrorContains(t, err, tc.wantKey)
			require.ErrorContains(t, err, "greater than zero")
		})
	}
}

// The distinction the rejection above depends on: an omitted key and an
// explicit zero both unmarshal to zero, and only one of them is a mistake.
func TestRespCacheExpirationsOmittedKeyIsNotAZero(t *testing.T) {
	v, _ := newViperWithYAML(t, `
resp-cache:
  addresses: ["cache:6379"]
  expiration-multiplier: 1.5
`)
	_, err := LoadRespCacheExpirations(v)
	require.NoError(t, err, "the five keys left out must not be read as explicit zeroes")
}

// A multiplier large enough to overflow int64 saturates instead of wrapping
// negative — a negative TTL is rejected by go-redis and read as already-expired
// by ristretto, so wrapping would turn "absurdly long" into "no caching".
func TestRespCacheExpirationsOverflowSaturates(t *testing.T) {
	policy := RespCacheExpirations{FinalizedMultiplier: 1e12}.Policy()
	require.Equal(t, time.Duration(math.MaxInt64), policy.Finalized)
	require.Positive(t, policy.Finalized)
}

// SelectCacheBackend must refuse to start on a bad expiration rather than fall
// back to the defaults, which is the failure this ticket is about: a TTL that
// is silently not what the operator wrote.
func TestSelectCacheBackendRejectsInvalidExpirations(t *testing.T) {
	v, _ := newViperWithYAML(t, `
resp-cache:
  addresses: ["cache:6379"]
  expiration-multiplier: 0
`)
	backend, err := SelectCacheBackend(t.Context(), v)
	require.Error(t, err)
	require.ErrorContains(t, err, RespCacheExpirationMultiplierKey)
	require.Nil(t, backend)
}
