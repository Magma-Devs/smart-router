package rpcsmartrouter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// The wait before a failing provider's next retry: 30s, doubling per pass that still
// finds it failing, capped at 15m, and extended, never shortened, by up to 20% jitter
// (MAG-3950).
func TestVerificationRetryDelayFor(t *testing.T) {
	for attempts, want := range map[int]time.Duration{
		1: 30 * time.Second, 2: time.Minute, 3: 2 * time.Minute, 4: 4 * time.Minute,
		5: 8 * time.Minute, 6: 15 * time.Minute, 20: 15 * time.Minute,
	} {
		require.Equal(t, want, verificationRetryDelayFor(attempts, 0), "attempt %d", attempts)
	}
	require.Equal(t, 36*time.Second, verificationRetryDelayFor(1, 1), "jitter extends the wait by up to 20%")
	require.Equal(t, 18*time.Minute, verificationRetryDelayFor(20, 1))

	prevDelay, prevMax := VerificationRetryDelay, VerificationRetryMaxDelay
	t.Cleanup(func() { VerificationRetryDelay, VerificationRetryMaxDelay = prevDelay, prevMax })
	VerificationRetryDelay, VerificationRetryMaxDelay = 20*time.Minute, 15*time.Minute
	require.Equal(t, 20*time.Minute, verificationRetryDelayFor(3, 0), "the cap never shortens the first delay")
	VerificationRetryDelay = 0
	require.Equal(t, 30*time.Second, verificationRetryDelayFor(1, 0), "a delay of zero falls back to the default")
}

// failingWhole puts a provider on the schedule for its whole pass, as a pass with no
// verdicts to read does (a fake probe's), and counts the attempt.
func failingWhole(schedule *retrySchedule, tier reverifyTier, name string, notBefore time.Time) {
	schedule.reconcile(tier, name, nil, []chainlib.CheckID{{}}, notBefore, true)
}

// Each check a provider is failing waits on its own backoff (MAG-4505 C4): a retry that
// re-ran one check moves only that one, a check that passes leaves the schedule, and a
// failing check the retry did not run keeps its place.
func TestRetrySchedule(t *testing.T) {
	now := time.Date(2026, 10, 25, 20, 0, 0, 0, time.UTC)
	schedule := newRetrySchedule()
	schedule.now = func() time.Time { return now }
	schedule.jitter = func() float64 { return 0 }
	debug := chainlib.CheckID{Name: "enabled", Addon: "debug"}
	archive := chainlib.CheckID{Name: "pruning", Extension: "archive"}
	nextOf := func(check chainlib.CheckID) time.Time {
		for _, entry := range schedule.snapshot() {
			if entry.tier == reverifyTierStatic && entry.name == "lava" && entry.check == check {
				return entry.due
			}
		}
		return time.Time{}
	}

	schedule.reconcile(reverifyTierStatic, "lava", nil, []chainlib.CheckID{archive, debug}, time.Time{}, true)
	select {
	case <-schedule.wake:
	default:
		t.Fatal("a new entry wakes the loop")
	}
	wait, pending := schedule.next()
	require.True(t, pending)
	require.Equal(t, 30*time.Second, wait)
	require.Empty(t, schedule.due())

	now = now.Add(30 * time.Second)
	require.ElementsMatch(t, []chainlib.CheckID{archive, debug}, schedule.due()[reverifyTierStatic]["lava"], "both checks failed in one pass, so both are due together")

	schedule.reconcile(reverifyTierStatic, "lava", []chainlib.CheckID{debug}, []chainlib.CheckID{archive, debug}, time.Time{}, true)
	require.Equal(t, now.Add(time.Minute), nextOf(debug), "the check the retry re-ran and found failing waits twice as long")
	require.Equal(t, now, nextOf(archive), "the check it did not run keeps its place")

	schedule.reconcile(reverifyTierStatic, "lava", []chainlib.CheckID{archive}, []chainlib.CheckID{debug}, time.Time{}, true)
	require.True(t, nextOf(archive).IsZero(), "a check that passes leaves the schedule")
	require.Equal(t, now.Add(time.Minute), nextOf(debug), "and the other check's schedule is its own")

	schedule.reconcile(reverifyTierStatic, "lava", []chainlib.CheckID{debug}, []chainlib.CheckID{debug}, now.Add(10*time.Minute), true)
	require.Equal(t, now.Add(10*time.Minute), nextOf(debug), "never before its rate-limit hold-off ends")

	schedule.reconcile(reverifyTierStatic, "lava", nil, []chainlib.CheckID{debug, archive}, time.Time{}, false)
	require.Equal(t, now.Add(10*time.Minute), nextOf(debug), "a whole pass that does not count leaves a waiting check in its place")
	require.Equal(t, now.Add(30*time.Second), nextOf(archive), "and puts a newly failing one on the schedule")

	schedule.ensure(reverifyTierStatic, "lava")
	require.Len(t, schedule.snapshot(), 2, "ensure leaves a provider with checks on the schedule as it is")
	schedule.ensure(reverifyTierBackup, "lava")
	require.Equal(t, []chainlib.CheckID{{}}, schedule.due()[reverifyTierBackup]["lava"], "a provider ensure puts on the schedule is due now, for its whole pass")
	schedule.postpone(reverifyTierBackup, "lava", nil, now.Add(time.Hour))
	require.Empty(t, schedule.due()[reverifyTierBackup], "a rate-limited pass pushes the provider's checks past the hold-off")

	schedule.reconcile(reverifyTierStatic, "lava", nil, nil, time.Time{}, true)
	schedule.forget(reverifyTierBackup, "lava")
	_, pending = schedule.next()
	require.False(t, pending, "a whole pass that finds nothing failing clears the provider")
	require.Empty(t, schedule.snapshot())

	var none *retrySchedule
	none.reconcile(reverifyTierStatic, "lava", nil, []chainlib.CheckID{debug}, time.Time{}, true)
	_, pending = none.next()
	require.False(t, pending, "a chain without inputs has a nil schedule, and nothing on it")
}

// The checks one pass reschedules share a jitter, so the checks that failed together are
// retried in one pass: two draws apart would split them into two passes seconds apart.
func TestRetrySchedule_ChecksThatFailTogetherStayDueTogether(t *testing.T) {
	now := time.Date(2026, 10, 25, 20, 0, 0, 0, time.UTC)
	schedule := newRetrySchedule()
	schedule.now = func() time.Time { return now }
	draws := []float64{0.1, 0.9}
	schedule.jitter = func() float64 {
		draw := draws[0]
		draws = draws[1:]
		return draw
	}
	schedule.reconcile(reverifyTierStatic, "lava", nil, []chainlib.CheckID{{Name: "enabled", Addon: "debug"}, {Name: "pruning", Extension: "archive"}}, time.Time{}, true)
	entries := schedule.snapshot()
	require.Len(t, entries, 2)
	require.Equal(t, entries[0].due, entries[1].due)
}

// A chain that still serves retries each failed provider on that provider's backoff: 30s,
// then 60s, 120s ... instead of every 3m, and not before its own retry is due (MAG-3950).
func TestVerificationRetries_ADegradedChainRetriesEachProviderOnItsBackoff(t *testing.T) {
	rand.InitRandomSeed()
	shortenRetryIntervals(t)
	rpsr := createTestRPCSmartRouter()
	sm, rpcEndpoint := createTestSessionManager("BSC", "jsonrpc")
	chainKey := rpcEndpoint.Key()
	rpsr.sessionManagers[chainKey] = sm
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{0: createTestProviderSession("healthy", 0)}
	failed := bootTestProviders("flaky", "held")
	rpsr.failedStaticProviders[chainKey] = failed
	inputs := &chainReverifyInputs{rpcEndpoint: rpcEndpoint, retries: newRetrySchedule()}
	inputs.retries.jitter = func() float64 { return 0 }
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{chainKey: inputs}

	start := time.Now()
	// As boot schedules them: one due after the first delay, one rate-limited for longer
	// than this test runs.
	failingWhole(inputs.retries, reverifyTierStatic, "flaky", time.Time{})
	failingWhole(inputs.retries, reverifyTierStatic, "held", start.Add(time.Hour))

	var mu sync.Mutex
	probed := map[string][]time.Time{}
	restore := swapBootValidate(func(_ context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
		mu.Lock()
		defer mu.Unlock()
		probed[p.Name] = append(probed[p.Name], time.Now())
		if len(probed[p.Name]) < 3 {
			return errors.New("still down")
		}
		return nil
	})
	defer restore()

	runRetryLoop(t, rpsr, chainKey, rpcEndpoint, fakeConvertSessions(0))

	require.Eventually(t, func() bool {
		rpsr.mu.Lock()
		defer rpsr.mu.Unlock()
		return len(rpsr.providerSessions[chainKey]) == 2
	}, 10*time.Second, 10*time.Millisecond, "flaky is re-admitted on its third retry")

	mu.Lock()
	defer mu.Unlock()
	at := probed["flaky"]
	require.Len(t, at, 3)
	require.GreaterOrEqual(t, at[0].Sub(start), VerificationRetryDelay, "first retry after the first delay")
	require.Less(t, at[0].Sub(start), retryMaxInterval, "not the fixed interval it used to wait")
	require.GreaterOrEqual(t, at[1].Sub(at[0]), 2*VerificationRetryDelay, "the wait doubles")
	require.GreaterOrEqual(t, at[2].Sub(at[1]), 4*VerificationRetryDelay)
	require.Empty(t, probed["held"], "a provider is not retried before its own retry is due")
	rpsr.mu.Lock()
	require.Equal(t, []string{"held"}, providerNames(rpsr.failedStaticProviders[chainKey]))
	rpsr.mu.Unlock()
}

// A provider the epoch demotes joins the failed list and is retried on its backoff, so it
// is back before the next epoch. The loop it replaces never read a list the epoch wrote,
// and had stopped anyway once boot's failures recovered (MAG-3950).
func TestUpdateEpoch_ADemotedProviderJoinsTheRetryLoop(t *testing.T) {
	withImmediateDemote(t)
	rand.InitRandomSeed()
	shortenRetryIntervals(t)
	rpsr := createTestRPCSmartRouter()
	sm, rpcEndpoint := createTestSessionManager("BSC", "jsonrpc")
	chainKey := rpcEndpoint.Key()
	rpsr.sessionManagers[chainKey] = sm
	providers := bootTestProviders("primary", "flaky")
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("primary", 1),
		1: createTestProviderSession("flaky", 1),
	}
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpcEndpoint,
		convertProvidersToSessions: fakeConvertSessions(2),
		configuredStatic:           providers,
		validateFn:                 failThese("flaky"),
		retries:                    newRetrySchedule(),
	}
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{chainKey: inputs}

	rpsr.updateEpoch(context.Background(), 2)
	require.Len(t, rpsr.providerSessions[chainKey], 1, "flaky demoted")
	require.Equal(t, []string{"flaky"}, providerNames(rpsr.failedStaticProviders[chainKey]), "it waits in the failed list for its retry")
	_, pending := inputs.retries.next()
	require.True(t, pending)

	restore := swapBootValidate(func(context.Context, *lavasession.RPCStaticProviderEndpoint) error { return nil })
	defer restore()
	runRetryLoop(t, rpsr, chainKey, rpcEndpoint, fakeConvertSessions(2))
	require.Eventually(t, func() bool {
		rpsr.mu.Lock()
		defer rpsr.mu.Unlock()
		_, pending := inputs.retries.next()
		return len(rpsr.providerSessions[chainKey]) == 2 && len(rpsr.failedStaticProviders[chainKey]) == 0 && !pending
	}, 10*time.Second, 10*time.Millisecond, "the retry loop brings it back, without waiting for the next epoch, and it leaves the schedule")
}

// One provider that hangs no longer holds every other provider's retry back by its
// timeout: a tier's probes run in parallel.
func TestRevalidateTier_ProbesInParallel(t *testing.T) {
	rpsr := createTestRPCSmartRouter()
	_, rpcEndpoint := createTestSessionManager("BSC", "jsonrpc")
	restore := swapBootValidate(func(context.Context, *lavasession.RPCStaticProviderEndpoint) error {
		time.Sleep(300 * time.Millisecond)
		return errors.New("timed out")
	})
	defer restore()

	start := time.Now()
	_, stillFailed := rpsr.revalidateTier(context.Background(), bootTestProviders("a", "b", "c", "d", "e"), nil, rpcEndpoint, reverifyTierStatic, nil, true)
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, providerNames(stillFailed), "in configured order")
	require.Less(t, time.Since(start), 900*time.Millisecond, "five hanging providers cost one timeout, not five")
}

// A retry whose probe another pass overtook drops its verdict: that pass's, newer, stands.
// Applying both would count one outage twice, or re-admit a provider the epoch already
// promoted.
func TestRevalidateTier_DropsAVerdictAnotherPassOvertook(t *testing.T) {
	rpsr := createTestRPCSmartRouter()
	_, rpcEndpoint := createTestSessionManager("BSC", "jsonrpc")
	inputs := &chainReverifyInputs{rpcEndpoint: rpcEndpoint}
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{rpcEndpoint.Key(): inputs}
	restore := swapBootValidate(func(context.Context, *lavasession.RPCStaticProviderEndpoint) error {
		// While this probe runs, an epoch pass applies its own verdict.
		rpsr.mu.Lock()
		inputs.appliedVerdict(reverifyTierStatic, "flaky", time.Now(), nil)
		rpsr.mu.Unlock()
		return nil
	})
	defer restore()

	recovered, stillFailed := rpsr.revalidateTier(context.Background(), bootTestProviders("flaky"), nil, rpcEndpoint, reverifyTierStatic, nil, true)
	require.Empty(t, recovered)
	require.Empty(t, stillFailed)
}

// A provider the epoch promoted while the retry probed it is in the pairing already: it
// leaves the failed list, and no second session is built for it.
func TestReadmitRecoveredProviders_NeverBuildsASecondSession(t *testing.T) {
	rand.InitRandomSeed()
	const chainKey = "BSC-jsonrpc"
	rpsr := createTestRPCSmartRouter()
	sm, rpcEndpoint := createTestSessionManager("BSC", "jsonrpc")
	rpsr.sessionManagers[chainKey] = sm
	recovered := bootTestProviders("primary1")
	rpsr.failedStaticProviders[chainKey] = recovered
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{0: createTestProviderSession("primary1", 1)}

	rpsr.readmitRecoveredProviders(chainKey, rpcEndpoint,
		func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
			t.Fatal("primary1 is in the pairing already: a second session would double its weight")
			return nil
		},
		recovered, nil)

	require.Len(t, rpsr.providerSessions[chainKey], 1)
	require.Empty(t, rpsr.failedStaticProviders[chainKey])
}

// runRetryLoop runs a chain's retry loop for the rest of the test, and stops it before
// the test's cleanups restore the retry delays it reads.
func runRetryLoop(t *testing.T, rpsr *RPCSmartRouter, chainKey string, rpcEndpoint *lavasession.RPCEndpoint,
	convert func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider,
) {
	t.Helper()
	runRetryLoopWithParser(t, rpsr, chainKey, nil, rpcEndpoint, convert)
}

func runRetryLoopWithParser(t *testing.T, rpsr *RPCSmartRouter, chainKey string, chainParser chainlib.ChainParser, rpcEndpoint *lavasession.RPCEndpoint,
	convert func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		rpsr.runVerificationRetries(ctx, chainKey, chainParser, rpcEndpoint, convert)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func providerNames(providers []*lavasession.RPCStaticProviderEndpoint) []string {
	names := make([]string, 0, len(providers))
	for _, p := range providers {
		names = append(names, p.Name)
	}
	return names
}
