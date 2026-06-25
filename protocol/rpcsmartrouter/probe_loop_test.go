package rpcsmartrouter

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/probing"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
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
	syncRef      provideroptimizer.SyncReference
}

func (r *recordingAppender) AppendProbeData(provider string, availability float64, latency time.Duration, hasLatency bool, syncBlock uint64, hasSync bool, syncRef provideroptimizer.SyncReference) {
	r.calls = append(r.calls, probeCall{provider, availability, latency, hasLatency, syncBlock, hasSync, syncRef})
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

// freshObs is a healthy poll observation: fresh, keeping up, with a successful poll at pollTime.
func freshObs(block int64, observedAt, pollTime time.Time, latency time.Duration) endpointstate.EndpointObservation {
	return endpointstate.EndpointObservation{
		LatestBlock:             block,
		ObservedAt:              observedAt,
		LastSuccessfulPoll:      pollTime,
		LastPollLatency:         latency,
		ConsecutivePollFailures: 0,
	}
}

// TestProbeCycle_ReEnablesRecoveredEndpoint is the headline O1 + F1 + F2 behavior: a DISABLED endpoint
// whose telemetry shows DISTINCT successful polls is proactively re-enabled after K cycles, AND the
// prober restores its provider's routing state (onRecover fires). (The pre-disable / edge-triggered
// disabledAt cases are pinned in lavasession's endpoint_probe_reenable_test.go, which can drive the
// disable instant directly; here we prove the prober→endpoint→routing wiring.)
func TestProbeCycle_ReEnablesRecoveredEndpoint(t *testing.T) {
	const url = "http://ep:8545"
	base := time.Unix(1_700_000_000, 0)

	// Endpoint constructed disabled (disabledAt zero); recovery needs DISTINCT advancing polls.
	dc := ep(url, "provider1", false)
	endpoints := []*lavasession.EndpointWithDirectConnection{dc}

	var recovered []string
	onRecover := func(p string) { recovered = append(recovered, p) }
	cycle := func(i int) {
		pollTime := base.Add(time.Duration(i) * time.Second) // strictly advancing distinct polls
		now := pollTime.Add(time.Millisecond)
		getObs := func(string) (endpointstate.EndpointObservation, bool) {
			return freshObs(1000, now.Add(-time.Second), pollTime, 20*time.Millisecond), true
		}
		runProbeCycleCore(endpoints, getObs, 1000, true, provideroptimizer.SyncReference{}, now, probeCfg(), nil, onRecover)
	}

	// K-1 distinct polls: still disabled, no recovery callback.
	for i := 1; i < int(probeCfg().ReEnableHysteresis); i++ {
		cycle(i)
		require.False(t, dc.Endpoint.Enabled, "must not re-enable before K distinct healthy polls")
		require.Empty(t, recovered)
	}
	// K-th distinct poll re-enables AND restores routing.
	cycle(int(probeCfg().ReEnableHysteresis))
	require.True(t, dc.Endpoint.Enabled, "the probe re-enables a recovered endpoint after K distinct polls")
	require.Equal(t, []string{"provider1"}, recovered, "recovery restores the provider's routing state (F2)")
}

// TestProbeCycle_FailedPollDoesNotReEnable: a disabled endpoint whose last poll FAILED
// (ConsecutivePollFailures > 0) is never re-enabled — PollHealthy is false even if a stale relay keeps
// ObservedAt fresh (F1: recovery is poll-driven, and a trailing failure invalidates it).
func TestProbeCycle_FailedPollDoesNotReEnable(t *testing.T) {
	const url = "http://ep:8545"
	now := time.Unix(1_700_000_000, 0)
	dc := ep(url, "provider1", false)
	endpoints := []*lavasession.EndpointWithDirectConnection{dc}
	getObs := func(string) (endpointstate.EndpointObservation, bool) {
		// Fresh block/ObservedAt, but the last poll FAILED.
		return endpointstate.EndpointObservation{
			LatestBlock:             1000,
			ObservedAt:              now.Add(-time.Second),
			LastSuccessfulPoll:      now.Add(-2 * time.Second),
			ConsecutivePollFailures: 3,
		}, true
	}
	var recovered []string
	for i := 0; i < 10; i++ {
		runProbeCycleCore(endpoints, getObs, 1000, true, provideroptimizer.SyncReference{}, now, probeCfg(), nil, func(p string) { recovered = append(recovered, p) })
	}
	require.False(t, dc.Endpoint.Enabled, "a failed last poll must never re-enable")
	require.Empty(t, recovered)
}

// TestProbeCycle_StaleEndpointStaysDisabled: a disabled endpoint with stale telemetry (no recent
// successful poll) is never re-enabled.
func TestProbeCycle_StaleEndpointStaysDisabled(t *testing.T) {
	const url = "http://ep:8545"
	now := time.Unix(1_700_000_000, 0)
	dc := ep(url, "provider1", false)
	endpoints := []*lavasession.EndpointWithDirectConnection{dc}
	getObs := func(string) (endpointstate.EndpointObservation, bool) {
		// Last observation well past the staleness window and no fresh poll.
		return endpointstate.EndpointObservation{LatestBlock: 1000, ObservedAt: now.Add(-60 * time.Second)}, true
	}
	for i := 0; i < 10; i++ {
		runProbeCycleCore(endpoints, getObs, 1000, true, provideroptimizer.SyncReference{}, now, probeCfg(), nil, nil)
	}
	require.False(t, dc.Endpoint.Enabled, "a still-stale endpoint must not be re-enabled")
}

// TestProbeCycle_OneSamplePerProvider: a provider with multiple endpoints yields exactly ONE
// AppendProbeData call (rule E2), with fraction-healthy availability.
func TestProbeCycle_OneSamplePerProvider(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	healthy := func(string) (endpointstate.EndpointObservation, bool) {
		return freshObs(1000, now.Add(-time.Second), now.Add(-time.Second), 20*time.Millisecond), true
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
	ref := provideroptimizer.SyncReference{ConsensusConfigured: true, Block: 1000, Time: now, Fresh: true}
	runProbeCycleCore(endpoints, getObs, 1000, true, ref, now, probeCfg(), rec, nil)

	require.Len(t, rec.calls, 1, "exactly one QoS sample per provider per cycle (rule E2)")
	require.Equal(t, "provider1", rec.calls[0].provider)
	require.InDelta(t, 2.0/3.0, rec.calls[0].availability, 1e-9, "2 of 3 endpoints healthy")
	require.True(t, rec.calls[0].hasLatency)
	require.Equal(t, 20*time.Millisecond, rec.calls[0].latency, "min latency over healthy endpoints")
	require.True(t, rec.calls[0].hasSync, "a fresh baseline → sync is fed")
	require.Equal(t, ref, rec.calls[0].syncRef, "the per-interface consensus reference is threaded through")
}

// TestProbeCycle_NoBaselineOmitsSync: with no consensus baseline this cycle, availability/latency
// still feed but hasSync is false — the F5 guard wired through the prober.
func TestProbeCycle_NoBaselineOmitsSync(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	getObs := func(string) (endpointstate.EndpointObservation, bool) {
		return freshObs(1000, now.Add(-time.Second), now.Add(-time.Second), 10*time.Millisecond), true
	}
	endpoints := []*lavasession.EndpointWithDirectConnection{ep("http://a:8545", "provider1", true)}
	rec := &recordingAppender{}
	// hasBaseline=false → the prober must not set hasSync.
	runProbeCycleCore(endpoints, getObs, 0, false, provideroptimizer.SyncReference{ConsensusConfigured: true}, now, probeCfg(), rec, nil)

	require.Len(t, rec.calls, 1)
	require.Equal(t, 1.0, rec.calls[0].availability)
	require.False(t, rec.calls[0].hasSync, "no baseline → sync omitted (F5)")
}

// TestProbeCycle_CoversBackupsAndMultipleProviders: every endpoint across multiple providers
// (including a backup-tier provider) is scored — one sample each.
func TestProbeCycle_CoversBackupsAndMultipleProviders(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	getObs := func(string) (endpointstate.EndpointObservation, bool) {
		return freshObs(1000, now.Add(-time.Second), now.Add(-time.Second), 10*time.Millisecond), true
	}
	endpoints := []*lavasession.EndpointWithDirectConnection{
		ep("http://a:8545", "regular-provider", true),
		ep("http://b:8545", "backup-provider", true), // GetAllDirectRPCEndpoints includes backups
	}
	rec := &recordingAppender{}
	runProbeCycleCore(endpoints, getObs, 1000, true, provideroptimizer.SyncReference{}, now, probeCfg(), rec, nil)

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
	runProbeCycleCore(endpoints, getObs, 0, false, provideroptimizer.SyncReference{}, now, probeCfg(), rec, nil)

	require.Len(t, rec.calls, 1)
	require.Equal(t, 0.0, rec.calls[0].availability, "an unobserved endpoint scores unhealthy")
	require.False(t, rec.calls[0].hasLatency, "no telemetry → no latency sample")
	require.False(t, rec.calls[0].hasSync, "no telemetry → no sync sample")
}

// TestProbeCycleCore_ReturnsCycleCounts asserts the per-cycle telemetry runProbeCycleCore returns for
// /debug/probe-loop (MAG-2202 endpoint 4): endpoints scored, endpoints re-enabled (F1), and providers
// whose sample fed no sync evidence (F5).
func TestProbeCycleCore_ReturnsCycleCounts(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	healthy := func(string) (endpointstate.EndpointObservation, bool) {
		return freshObs(1000, now.Add(-time.Second), now.Add(-time.Second), 20*time.Millisecond), true
	}
	endpoints := []*lavasession.EndpointWithDirectConnection{
		ep("http://a:8545", "p1", true),
		ep("http://b:8545", "p2", true),
	}

	// Fresh baseline → sync fed for both providers, nothing omitted.
	scored, reEnabled, syncOmitted := runProbeCycleCore(
		endpoints, healthy, 1000, true,
		provideroptimizer.SyncReference{ConsensusConfigured: true, Block: 1000, Time: now, Fresh: true},
		now, probeCfg(), &recordingAppender{}, nil)
	require.Equal(t, 2, scored, "both endpoints scored")
	require.Equal(t, 0, reEnabled, "already-enabled endpoints are never re-enabled")
	require.Equal(t, 0, syncOmitted, "fresh baseline → no sync omitted")

	// No baseline → F5: sync omitted for every provider sample.
	_, _, syncOmitted = runProbeCycleCore(
		endpoints, healthy, 0, false,
		provideroptimizer.SyncReference{ConsensusConfigured: true},
		now, probeCfg(), &recordingAppender{}, nil)
	require.Equal(t, 2, syncOmitted, "no fresh baseline → both providers' sync omitted (F5)")
}

// TestProbeCycleCore_CountsReEnable: the cycle that crosses the K-hysteresis reports exactly one
// re-enable, and earlier cycles report none.
func TestProbeCycleCore_CountsReEnable(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	dc := ep("http://ep:8545", "provider1", false) // disabled, disabledAt zero
	endpoints := []*lavasession.EndpointWithDirectConnection{dc}

	K := int(probeCfg().ReEnableHysteresis)
	var lastReEnabled int
	for i := 1; i <= K; i++ {
		pollTime := base.Add(time.Duration(i) * time.Second) // strictly advancing distinct polls
		now := pollTime.Add(time.Millisecond)
		getObs := func(string) (endpointstate.EndpointObservation, bool) {
			return freshObs(1000, now.Add(-time.Second), pollTime, 20*time.Millisecond), true
		}
		_, reEnabled, _ := runProbeCycleCore(endpoints, getObs, 1000, true, provideroptimizer.SyncReference{}, now, probeCfg(), nil, nil)
		lastReEnabled = reEnabled
		if i < K {
			require.Equal(t, 0, reEnabled, "no re-enable before the K-th distinct healthy poll")
		}
	}
	require.True(t, dc.Endpoint.Enabled)
	require.Equal(t, 1, lastReEnabled, "the K-th cycle reports exactly one re-enable")
}

// TestValidatedProbeCadence pins the F6 validation: a non-positive configured cadence is rejected to
// the default; a positive value passes through.
func TestValidatedProbeCadence(t *testing.T) {
	require.Equal(t, defaultProbeCadence, validatedProbeCadence(0), "zero falls back to default")
	require.Equal(t, defaultProbeCadence, validatedProbeCadence(-time.Second), "negative falls back to default")
	require.Equal(t, 7*time.Second, validatedProbeCadence(7*time.Second), "a positive cadence is honored")
}
