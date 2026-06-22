package rpcsmartrouter

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/probing"
	"github.com/stretchr/testify/require"
)

// recordingAppender captures AppendProbeData calls so tests can assert the one-sample-per-provider
// rule and the fed values.
type recordingAppender struct {
	calls []probeCall
}

type probeCall struct {
	provider     string
	availability float64
	latency      time.Duration
	hasLatency   bool
	block        uint64
	hasSync      bool
}

func (r *recordingAppender) AppendProbeData(provider string, availability float64, latency time.Duration, hasLatency bool, syncBlock uint64, hasSync bool) {
	r.calls = append(r.calls, probeCall{provider, availability, latency, hasLatency, syncBlock, hasSync})
}

func ep(url, provider string, enabled bool) *lavasession.EndpointWithDirectConnection {
	return &lavasession.EndpointWithDirectConnection{
		Endpoint:        &lavasession.Endpoint{NetworkAddress: url, Enabled: enabled},
		ProviderAddress: provider,
	}
}

func probeCfg() probing.VerdictConfig {
	return probing.VerdictConfig{StalenessWindow: 10 * time.Second, LagToleranceBlocks: 10, ReEnableHysteresis: 3}
}

// TestProbeCycle_ReEnablesRecoveredEndpoint is the headline O1 behavior: a disabled endpoint whose
// telemetry shows it is fresh and keeping up is proactively re-enabled after K probe cycles —
// recovery in seconds, not at the 15-min epoch.
func TestProbeCycle_ReEnablesRecoveredEndpoint(t *testing.T) {
	const url = "http://ep:8545"
	now := time.Unix(1_700_000_000, 0)

	// The endpoint is DISABLED (relay path backed it off) but its tracker keeps polling, so its
	// observation is fresh and at the tip.
	disabled := ep(url, "provider1", false)
	getObs := func(string) (endpointstate.EndpointObservation, bool) {
		return endpointstate.EndpointObservation{LatestBlock: 1000, ObservedAt: now.Add(-time.Second), LastPollLatency: 20 * time.Millisecond}, true
	}
	endpoints := []*lavasession.EndpointWithDirectConnection{disabled}

	// K-1 cycles: still disabled (hysteresis not satisfied).
	for i := uint64(0); i < probeCfg().ReEnableHysteresis-1; i++ {
		runProbeCycleCore(endpoints, getObs, 1000, true, now, probeCfg(), nil)
		require.False(t, disabled.Endpoint.Enabled, "must not re-enable before K healthy cycles")
	}
	// K-th cycle re-enables.
	runProbeCycleCore(endpoints, getObs, 1000, true, now, probeCfg(), nil)
	require.True(t, disabled.Endpoint.Enabled, "the probe re-enables a recovered endpoint after K cycles")
}

// TestProbeCycle_StaleEndpointStaysDisabled: a disabled endpoint whose telemetry is stale (upstream
// still down) is never re-enabled.
func TestProbeCycle_StaleEndpointStaysDisabled(t *testing.T) {
	const url = "http://ep:8545"
	now := time.Unix(1_700_000_000, 0)
	disabled := ep(url, "provider1", false)
	getObs := func(string) (endpointstate.EndpointObservation, bool) {
		// Last observation is well past the staleness window → not alive.
		return endpointstate.EndpointObservation{LatestBlock: 1000, ObservedAt: now.Add(-60 * time.Second)}, true
	}
	endpoints := []*lavasession.EndpointWithDirectConnection{disabled}
	for i := 0; i < 10; i++ {
		runProbeCycleCore(endpoints, getObs, 1000, true, now, probeCfg(), nil)
	}
	require.False(t, disabled.Endpoint.Enabled, "a still-stale endpoint must not be re-enabled")
}

// TestProbeCycle_OneSamplePerProvider: a provider with multiple endpoints yields exactly ONE
// AppendProbeData call (rule E2), with fraction-healthy availability.
func TestProbeCycle_OneSamplePerProvider(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	// provider1 has 3 endpoints: 2 healthy, 1 stale (dead) → availability 2/3.
	healthy := func(string) (endpointstate.EndpointObservation, bool) {
		return endpointstate.EndpointObservation{LatestBlock: 1000, ObservedAt: now.Add(-time.Second), LastPollLatency: 20 * time.Millisecond}, true
	}
	obsByURL := map[string]func(string) (endpointstate.EndpointObservation, bool){
		"http://a:8545": healthy,
		"http://b:8545": healthy,
		"http://c:8545": func(string) (endpointstate.EndpointObservation, bool) {
			return endpointstate.EndpointObservation{LatestBlock: 1000, ObservedAt: now.Add(-60 * time.Second)}, true // stale
		},
	}
	getObs := func(url string) (endpointstate.EndpointObservation, bool) { return obsByURL[url](url) }
	endpoints := []*lavasession.EndpointWithDirectConnection{
		ep("http://a:8545", "provider1", true),
		ep("http://b:8545", "provider1", true),
		ep("http://c:8545", "provider1", true),
	}

	rec := &recordingAppender{}
	runProbeCycleCore(endpoints, getObs, 1000, true, now, probeCfg(), rec)

	require.Len(t, rec.calls, 1, "exactly one QoS sample per provider per cycle (rule E2)")
	require.Equal(t, "provider1", rec.calls[0].provider)
	require.InDelta(t, 2.0/3.0, rec.calls[0].availability, 1e-9, "2 of 3 endpoints healthy")
	require.True(t, rec.calls[0].hasLatency)
	require.Equal(t, 20*time.Millisecond, rec.calls[0].latency, "min latency over healthy endpoints")
}

// TestProbeCycle_CoversBackupsAndMultipleProviders: every endpoint across multiple providers
// (including a backup-tier provider) is scored — one sample each.
func TestProbeCycle_CoversBackupsAndMultipleProviders(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	getObs := func(string) (endpointstate.EndpointObservation, bool) {
		return endpointstate.EndpointObservation{LatestBlock: 1000, ObservedAt: now.Add(-time.Second), LastPollLatency: 10 * time.Millisecond}, true
	}
	endpoints := []*lavasession.EndpointWithDirectConnection{
		ep("http://a:8545", "regular-provider", true),
		ep("http://b:8545", "backup-provider", true), // GetAllDirectRPCEndpoints includes backups
	}
	rec := &recordingAppender{}
	runProbeCycleCore(endpoints, getObs, 1000, true, now, probeCfg(), rec)

	require.Len(t, rec.calls, 2, "both the regular and backup providers get a sample")
	seen := map[string]bool{}
	for _, c := range rec.calls {
		seen[c.provider] = true
		require.Equal(t, 1.0, c.availability)
	}
	require.True(t, seen["regular-provider"] && seen["backup-provider"], "backups are covered, not just pairingList")
}

// TestProbeCycle_NoObservationIsUnhealthy: an endpoint the monitor has never observed (no telemetry)
// is scored unhealthy (availability 0), not skipped.
func TestProbeCycle_NoObservationIsUnhealthy(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	getObs := func(string) (endpointstate.EndpointObservation, bool) {
		return endpointstate.EndpointObservation{}, false // never observed
	}
	endpoints := []*lavasession.EndpointWithDirectConnection{ep("http://a:8545", "provider1", true)}
	rec := &recordingAppender{}
	runProbeCycleCore(endpoints, getObs, 0, false, now, probeCfg(), rec)

	require.Len(t, rec.calls, 1)
	require.Equal(t, 0.0, rec.calls[0].availability, "an unobserved endpoint scores unhealthy")
	require.False(t, rec.calls[0].hasLatency, "no telemetry → no latency sample")
	require.False(t, rec.calls[0].hasSync, "no telemetry → no sync sample")
}
