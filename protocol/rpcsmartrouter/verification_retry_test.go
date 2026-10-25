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

func TestRetrySchedule(t *testing.T) {
	now := time.Date(2026, 10, 25, 20, 0, 0, 0, time.UTC)
	schedule := newRetrySchedule()
	schedule.now = func() time.Time { return now }
	schedule.jitter = func() float64 { return 0 }

	schedule.failing(reverifyTierStatic, "lava", time.Time{})
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
	require.Equal(t, map[reverifyTier]map[string]struct{}{reverifyTierStatic: {"lava": {}}}, schedule.due())

	schedule.failing(reverifyTierStatic, "lava", time.Time{})
	wait, _ = schedule.next()
	require.Equal(t, time.Minute, wait, "a pass that still finds it failing doubles the wait")

	schedule.failing(reverifyTierStatic, "lava", now.Add(10*time.Minute))
	wait, _ = schedule.next()
	require.Equal(t, 10*time.Minute, wait, "never before its rate-limit hold-off ends")

	schedule.ensure(reverifyTierStatic, "lava")
	wait, _ = schedule.next()
	require.Equal(t, 10*time.Minute, wait, "ensure leaves an entry already there in its place")
	schedule.ensure(reverifyTierBackup, "lava")
	require.Contains(t, schedule.due(), reverifyTierBackup, "a provider ensure puts on the schedule is due now")

	schedule.passed(reverifyTierStatic, "lava")
	schedule.passed(reverifyTierBackup, "lava")
	_, pending = schedule.next()
	require.False(t, pending)
	require.Empty(t, schedule.snapshot())

	var none *retrySchedule
	none.failing(reverifyTierStatic, "lava", time.Time{})
	_, pending = none.next()
	require.False(t, pending, "a chain without inputs has a nil schedule, and nothing on it")
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
	inputs.retries.failing(reverifyTierStatic, "flaky", time.Time{})
	inputs.retries.failing(reverifyTierStatic, "held", start.Add(time.Hour))

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
	_, stillFailed := rpsr.revalidateTier(context.Background(), bootTestProviders("a", "b", "c", "d", "e"), nil, rpcEndpoint, reverifyTierStatic)
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

	recovered, stillFailed := rpsr.revalidateTier(context.Background(), bootTestProviders("flaky"), nil, rpcEndpoint, reverifyTierStatic)
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
