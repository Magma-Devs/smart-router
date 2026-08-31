package rpcsmartrouter

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// Boot resilience (MAG-2525, subsuming MAG-2532).
//
// Before this change CreateSmartRouterEndpoint exited the process when every
// static provider failed boot verification, even with healthy backups configured.
// The rule is now: configuration errors are fatal, provider health never is. A
// chain boots on backups alone, or boots dark and retries, and the health endpoint
// — not a crash loop — is what reports that it cannot serve.
//
// The boot path itself needs live upstreams and a chain parser, so these tests
// target the extracted units that carry the behavior: tier validation, the dark
// verdict, the adaptive retry cadence, re-admission merging, and the health seed.

func bootTestEndpoint() *lavasession.RPCEndpoint {
	return &lavasession.RPCEndpoint{ChainID: "BSC", ApiInterface: "jsonrpc", NetworkAddress: "127.0.0.1:0"}
}

func bootTestProviders(names ...string) []*lavasession.RPCStaticProviderEndpoint {
	providers := make([]*lavasession.RPCStaticProviderEndpoint, 0, len(names))
	for _, n := range names {
		providers = append(providers, &lavasession.RPCStaticProviderEndpoint{
			Name: n, ChainID: "BSC", ApiInterface: "jsonrpc",
			NodeUrls: []common.NodeUrl{{Url: "http://" + n + ":8545"}},
		})
	}
	return providers
}

// failThese returns a validate func that fails exactly the named providers.
func failThese(names ...string) func(context.Context, *lavasession.RPCStaticProviderEndpoint) error {
	failing := make(map[string]struct{}, len(names))
	for _, n := range names {
		failing[n] = struct{}{}
	}
	return func(_ context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
		if _, bad := failing[p.Name]; bad {
			return errors.New("unreachable")
		}
		return nil
	}
}

func TestValidateProviderTier_PartitionsAndPreservesConfiguredOrder(t *testing.T) {
	// Enough providers to exceed SpecReVerifyConcurrency, so completion order
	// definitely differs from configured order.
	providers := bootTestProviders("p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7")

	failedSet, failedOrdered := validateProviderTier(
		context.Background(), providers, bootTestEndpoint(), nil, reverifyTierStatic,
		failThese("p6", "p1", "p3"))

	require.Len(t, failedSet, 3)
	names := make([]string, 0, len(failedOrdered))
	for _, p := range failedOrdered {
		names = append(names, p.Name)
	}
	require.Equal(t, []string{"p1", "p3", "p6"}, names,
		"failed slice must follow configured order, not goroutine completion order — it seeds the retry loop")

	// Pointer-keyed, so the healthy providers are identifiable by identity.
	for _, p := range providers {
		_, failed := failedSet[p]
		require.Equal(t, p.Name == "p1" || p.Name == "p3" || p.Name == "p6", failed, p.Name)
	}
}

func TestValidateProviderTier_EmptyTierIsNotAnError(t *testing.T) {
	failedSet, failedOrdered := validateProviderTier(
		context.Background(), nil, bootTestEndpoint(), nil, reverifyTierBackup,
		func(context.Context, *lavasession.RPCStaticProviderEndpoint) error {
			t.Fatal("validate must not be called for an empty tier")
			return nil
		})

	require.Empty(t, failedSet)
	require.Empty(t, failedOrdered)
}

// A chain configured with backups only has an empty static tier. That case skipped
// the fatal check even before this change, and must stay non-fatal.
func TestValidateProviderTier_AllProvidersFailingIsReportedNotFatal(t *testing.T) {
	providers := bootTestProviders("dead1", "dead2", "dead3")

	failedSet, failedOrdered := validateProviderTier(
		context.Background(), providers, bootTestEndpoint(), nil, reverifyTierStatic,
		failThese("dead1", "dead2", "dead3"))

	require.Len(t, failedSet, 3, "every provider failed")
	require.Len(t, failedOrdered, 3, "and every one is queued for retry rather than aborting boot")
}

func TestValidateProviderTier_RunsConcurrently(t *testing.T) {
	// Each validation blocks until released. If the tier ran serially none would
	// overlap and this would deadlock on the barrier below.
	providers := bootTestProviders("a", "b", "c", "d", "e")
	release := make(chan struct{})
	var inFlight, peak int64

	done := make(chan struct{})
	go func() {
		defer close(done)
		validateProviderTier(
			context.Background(), providers, bootTestEndpoint(), nil, reverifyTierStatic,
			func(context.Context, *lavasession.RPCStaticProviderEndpoint) error {
				cur := atomic.AddInt64(&inFlight, 1)
				for {
					old := atomic.LoadInt64(&peak)
					if cur <= old || atomic.CompareAndSwapInt64(&peak, old, cur) {
						break
					}
				}
				<-release
				atomic.AddInt64(&inFlight, -1)
				return nil
			})
	}()

	require.Eventually(t, func() bool { return atomic.LoadInt64(&inFlight) > 1 }, 5*time.Second, 5*time.Millisecond,
		"validations must overlap; a serial tier delays the backup tier by N x timeout")
	close(release)
	<-done

	require.LessOrEqual(t, atomic.LoadInt64(&peak), int64(SpecReVerifyConcurrency),
		"concurrency must stay bounded by the semaphore")
}

func TestChainIsDark(t *testing.T) {
	const chainKey = "BSC-jsonrpc"
	session := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: createTestProviderSession("p", 1)}

	for _, tc := range []struct {
		name           string
		static, backup map[uint64]*lavasession.ConsumerSessionsWithProvider
		wantDark       bool
	}{
		{"primaries healthy", session, nil, false},
		{"backups only — degraded, not dark", nil, session, false},
		{"both tiers present", session, session, false},
		{"nothing usable", nil, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rpsr := createTestRPCSmartRouter()
			if tc.static != nil {
				rpsr.providerSessions[chainKey] = tc.static
			}
			if tc.backup != nil {
				rpsr.backupProviderSessions[chainKey] = tc.backup
			}
			require.Equal(t, tc.wantDark, rpsr.chainIsDark(chainKey))
		})
	}
}

func TestRetryIntervalFor(t *testing.T) {
	require.Equal(t, retryDarkBaseInterval, retryIntervalFor(true),
		"a chain that cannot serve retries promptly — every second is downtime")
	require.Equal(t, retryMaxInterval, retryIntervalFor(false),
		"a merely degraded chain waits the full interval; retries buy redundancy, not availability")
	require.Less(t, retryDarkBaseInterval, retryMaxInterval)
}

func TestSeedInitialHealth_DarkChainReportsUnhealthyImmediately(t *testing.T) {
	// The monitor is optimistic by default, which would answer 200 on the health
	// path for a chain that came up with nothing to serve from.
	monitor := metrics.NewRelaysMonitor(time.Minute, time.Minute, "BSC", "jsonrpc")
	require.True(t, monitor.IsHealthy(), "default is optimistic")

	monitor.SeedInitialHealth(false)
	require.False(t, monitor.IsHealthy(),
		"a chain that booted dark must fail its health check rather than accept traffic it cannot serve")

	monitor.SeedInitialHealth(true)
	require.True(t, monitor.IsHealthy(), "seeding is not one-way")
}

func TestSeedInitialHealth_NilReceiverIsSafe(t *testing.T) {
	var monitor *metrics.RelaysMonitor
	require.NotPanics(t, func() { monitor.SeedInitialHealth(false) })
	require.False(t, monitor.IsHealthy(), "an absent monitor is not healthy")
}

func TestMergeRecoveredSessions(t *testing.T) {
	convert := func(providers []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
		out := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(providers))
		for i, p := range providers {
			out[uint64(i)] = createTestProviderSession(p.Name, 0)
		}
		return out
	}

	t.Run("no recoveries returns the original map untouched", func(t *testing.T) {
		existing := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: createTestProviderSession("a", 1)}
		require.Equal(t, existing, mergeRecoveredSessions(existing, nil, convert, 7))
	})

	t.Run("recovered sessions append past the highest existing index", func(t *testing.T) {
		existing := map[uint64]*lavasession.ConsumerSessionsWithProvider{
			0: createTestProviderSession("a", 1),
			5: createTestProviderSession("b", 1),
		}
		merged := mergeRecoveredSessions(existing, bootTestProviders("c", "d"), convert, 9)

		require.Len(t, merged, 4, "nothing is overwritten")
		require.Len(t, existing, 2, "copy-on-write: the old map is still safe for concurrent readers")
		for _, idx := range []uint64{0, 5, 6, 7} {
			require.Contains(t, merged, idx)
		}
		require.Equal(t, uint64(9), merged[6].PairingEpoch, "recovered sessions adopt the current epoch")
		require.Equal(t, uint64(9), merged[7].PairingEpoch)
	})

	t.Run("recovering into an empty pairing works", func(t *testing.T) {
		merged := mergeRecoveredSessions(nil, bootTestProviders("first"), convert, 3)
		require.Len(t, merged, 1, "a dark chain must be able to adopt its first provider")
		require.Equal(t, uint64(3), merged[0].PairingEpoch)
	})
}

func TestPruneRestoredFromFailed(t *testing.T) {
	const chainKey = "BSC-jsonrpc"

	t.Run("restored providers are dropped, others kept", func(t *testing.T) {
		failed := map[string][]*lavasession.RPCStaticProviderEndpoint{
			chainKey: bootTestProviders("x", "y", "z"),
		}
		pruneRestoredFromFailed(failed, chainKey, map[string]struct{}{"y": {}})

		require.Len(t, failed[chainKey], 2)
		require.Equal(t, "x", failed[chainKey][0].Name)
		require.Equal(t, "z", failed[chainKey][1].Name)
	})

	t.Run("entry is deleted once nothing is pending so the retry loop can stop", func(t *testing.T) {
		failed := map[string][]*lavasession.RPCStaticProviderEndpoint{
			chainKey: bootTestProviders("x", "y"),
		}
		pruneRestoredFromFailed(failed, chainKey, map[string]struct{}{"x": {}, "y": {}})
		require.NotContains(t, failed, chainKey)
	})

	t.Run("absent chain is a no-op", func(t *testing.T) {
		failed := map[string][]*lavasession.RPCStaticProviderEndpoint{}
		require.NotPanics(t, func() {
			pruneRestoredFromFailed(failed, chainKey, map[string]struct{}{"x": {}})
		})
	})
}

func TestReadmitRecoveredProviders_RestoresBothTiers(t *testing.T) {
	rand.InitRandomSeed()
	const chainKey = "BSC-jsonrpc"
	rpsr := createTestRPCSmartRouter()
	sm, rpcEndpoint := createTestSessionManager("BSC", "jsonrpc")
	rpsr.sessionManagers[chainKey] = sm

	// A chain that booted dark: nothing registered, both tiers pending retry.
	staticProviders := bootTestProviders("primary1")
	backupProviders := bootTestProviders("backup1")
	rpsr.failedStaticProviders[chainKey] = staticProviders
	rpsr.failedBackupProviders[chainKey] = backupProviders
	require.True(t, rpsr.chainIsDark(chainKey))

	var convertCalls int32
	convert := func(providers []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
		atomic.AddInt32(&convertCalls, 1)
		out := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(providers))
		for i, p := range providers {
			out[uint64(i)] = createTestProviderSession(p.Name, 0)
		}
		return out
	}

	rpsr.readmitRecoveredProviders(chainKey, rpcEndpoint, convert,
		staticProviders, nil, backupProviders, nil)

	require.Len(t, rpsr.providerSessions[chainKey], 1, "primary re-admitted")
	require.Len(t, rpsr.backupProviderSessions[chainKey], 1, "backup re-admitted — backups are retried too now")
	require.Empty(t, rpsr.failedStaticProviders[chainKey])
	require.Empty(t, rpsr.failedBackupProviders[chainKey])
	require.False(t, rpsr.chainIsDark(chainKey), "chain is serving again without a restart")
}

func TestReadmitRecoveredProviders_NoRecoveriesStillRecordsStillFailed(t *testing.T) {
	rand.InitRandomSeed()
	const chainKey = "BSC-jsonrpc"
	rpsr := createTestRPCSmartRouter()
	sm, rpcEndpoint := createTestSessionManager("BSC", "jsonrpc")
	rpsr.sessionManagers[chainKey] = sm

	stillFailing := bootTestProviders("primary1")
	rpsr.failedStaticProviders[chainKey] = stillFailing

	rpsr.readmitRecoveredProviders(chainKey, rpcEndpoint,
		func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
			t.Fatal("nothing recovered — must not build sessions")
			return nil
		},
		nil, stillFailing, nil, nil)

	require.Len(t, rpsr.failedStaticProviders[chainKey], 1, "still queued for the next retry")
	require.Empty(t, rpsr.providerSessions[chainKey])
	require.True(t, rpsr.chainIsDark(chainKey))
}

// A dark chain must keep retrying and start serving as soon as an upstream returns,
// without a process restart. This is MAG-2525's headline requirement.
func TestRetryFailedProviders_DarkChainAdoptsProviderWhenItReturns(t *testing.T) {
	rand.InitRandomSeed()
	shortenRetryIntervals(t)
	const chainKey = "BSC-jsonrpc"
	rpsr := createTestRPCSmartRouter()
	sm, rpcEndpoint := createTestSessionManager("BSC", "jsonrpc")
	rpsr.sessionManagers[chainKey] = sm

	providers := bootTestProviders("primary1")
	rpsr.failedStaticProviders[chainKey] = providers
	require.True(t, rpsr.chainIsDark(chainKey), "boots dark")

	// The upstream stays down for the first few attempts, then recovers.
	var attempts int32
	restoreValidate := swapBootValidate(func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
		if atomic.AddInt32(&attempts, 1) < 3 {
			return errors.New("still down")
		}
		return nil
	})
	defer restoreValidate()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		rpsr.retryFailedProviders(ctx, chainKey, nil, rpcEndpoint,
			func(providers []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
				out := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(providers))
				for i, p := range providers {
					out[uint64(i)] = createTestProviderSession(p.Name, 0)
				}
				return out
			})
	}()

	// The dark backoff doubles from its base, so three attempts land well inside this.
	require.Eventually(t, func() bool { return !rpsr.chainIsDark(chainKey) }, 10*time.Second, 5*time.Millisecond,
		"a dark chain must adopt a recovered provider on the fast backoff, not wait for the 15m epoch tick")

	// With nothing left failing, the loop reports done and exits on its own.
	select {
	case <-done:
	case <-time.After(retryMaxInterval + 5*time.Second):
		cancel()
		<-done
		t.Fatal("retry loop should self-terminate once every provider has recovered")
	}

	require.GreaterOrEqual(t, atomic.LoadInt32(&attempts), int32(3))
}

func TestRetryFailedProviders_StopsWhenNothingIsPending(t *testing.T) {
	rand.InitRandomSeed()
	shortenRetryIntervals(t)
	const chainKey = "BSC-jsonrpc"
	rpsr := createTestRPCSmartRouter()
	sm, rpcEndpoint := createTestSessionManager("BSC", "jsonrpc")
	rpsr.sessionManagers[chainKey] = sm
	// Dark, so the first wait is the short one.
	require.True(t, rpsr.chainIsDark(chainKey))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		rpsr.retryFailedProviders(ctx, chainKey, nil, rpcEndpoint,
			func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
				t.Error("no failed providers — must not build sessions")
				return nil
			})
	}()

	select {
	case <-done:
	case <-time.After(retryDarkBaseInterval + 5*time.Second):
		cancel()
		<-done
		t.Fatal("loop must exit when both failed lists are empty")
	}
}

// One chain being dark must not disturb another chain's state.
func TestRetryFailedProviders_ChainsAreIsolated(t *testing.T) {
	rand.InitRandomSeed()
	shortenRetryIntervals(t)
	rpsr := createTestRPCSmartRouter()
	const darkKey, healthyKey = "BSC-jsonrpc", "ETH1-jsonrpc"

	smDark, darkEndpoint := createTestSessionManager("BSC", "jsonrpc")
	rpsr.sessionManagers[darkKey] = smDark
	rpsr.failedStaticProviders[darkKey] = bootTestProviders("bsc-primary")

	smHealthy, _ := createTestSessionManager("ETH1", "jsonrpc")
	rpsr.sessionManagers[healthyKey] = smHealthy
	rpsr.providerSessions[healthyKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("eth-primary", 1),
	}

	require.True(t, rpsr.chainIsDark(darkKey))
	require.False(t, rpsr.chainIsDark(healthyKey))

	restoreValidate := swapBootValidate(func(context.Context, *lavasession.RPCStaticProviderEndpoint) error {
		return errors.New("still down")
	})
	defer restoreValidate()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		rpsr.retryFailedProviders(ctx, darkKey, nil, darkEndpoint,
			func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
				return nil
			})
	}()
	<-done

	require.True(t, rpsr.chainIsDark(darkKey), "BSC never recovered")
	require.False(t, rpsr.chainIsDark(healthyKey), "ETH1 is untouched by BSC being dark")
	require.Len(t, rpsr.providerSessions[healthyKey], 1)
}

// swapBootValidate substitutes the retry loop's provider probe and returns a
// restore func. The chainParser argument is irrelevant to these tests, so the
// fake takes the shorter signature.
func swapBootValidate(fn func(context.Context, *lavasession.RPCStaticProviderEndpoint) error) func() {
	prev := retryValidateFn
	retryValidateFn = func(ctx context.Context, p *lavasession.RPCStaticProviderEndpoint, _ chainlib.ChainParser) error {
		return fn(ctx, p)
	}
	return func() { retryValidateFn = prev }
}

// shortenRetryIntervals compresses the retry cadence so loop behavior can be
// exercised in milliseconds. The ratio between the two is preserved, since the
// point of the design is that a dark chain retries far sooner than a degraded one.
func shortenRetryIntervals(t *testing.T) {
	t.Helper()
	prevDark, prevMax := retryDarkBaseInterval, retryMaxInterval
	retryDarkBaseInterval = 10 * time.Millisecond
	retryMaxInterval = 500 * time.Millisecond
	t.Cleanup(func() {
		retryDarkBaseInterval, retryMaxInterval = prevDark, prevMax
	})
}

// The epoch re-verifier promotes a provider back into the pairing, but until this fix
// it left that provider sitting in the failed list. retryFailedProviders would then
// revalidate it, succeed, and merge a SECOND session for the same name —
// mergeRecoveredSessions keys by index and does not dedupe by PublicLavaAddress. The
// consumer session manager's pairing map collapses the duplicate but validAddresses
// does not, so the provider ends up with double its selection weight.
func TestUpdateEpoch_PromotedProviderLeavesTheFailedList(t *testing.T) {
	rand.InitRandomSeed()
	const chainKey = "BSC-jsonrpc"
	rpsr := createTestRPCSmartRouter()
	sm, rpcEndpoint := createTestSessionManager("BSC", "jsonrpc")
	rpsr.sessionManagers[chainKey] = sm

	// primary1 failed boot verification and is pending retry; backup1 likewise.
	staticProviders := bootTestProviders("primary1")
	backupProviders := bootTestProviders("backup1")
	rpsr.failedStaticProviders[chainKey] = staticProviders
	rpsr.failedBackupProviders[chainKey] = backupProviders

	convert := func(providers []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
		out := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(providers))
		for i, p := range providers {
			out[uint64(i)] = createTestProviderSession(p.Name, 0)
		}
		return out
	}
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{
		chainKey: {
			rpcEndpoint:                rpcEndpoint,
			convertProvidersToSessions: convert,
			configuredStatic:           staticProviders,
			configuredBackup:           backupProviders,
			validateFn:                 failThese(), // both upstreams are back
		},
	}

	rpsr.updateEpoch(context.Background(), 2)

	require.Len(t, rpsr.providerSessions[chainKey], 1, "primary promoted")
	require.Len(t, rpsr.backupProviderSessions[chainKey], 1, "backup promoted")
	require.Empty(t, rpsr.failedStaticProviders[chainKey],
		"promoted provider must not stay pending, or the retry loop merges a duplicate session")
	require.Empty(t, rpsr.failedBackupProviders[chainKey])
}

// Pruning is per tier: a name configured in both tiers must not have its still-failing
// backup dropped from the retry loop just because its static twin recovered.
func TestUpdateEpoch_PromotionPrunesOnlyItsOwnTier(t *testing.T) {
	rand.InitRandomSeed()
	const chainKey = "BSC-jsonrpc"
	rpsr := createTestRPCSmartRouter()
	sm, rpcEndpoint := createTestSessionManager("BSC", "jsonrpc")
	rpsr.sessionManagers[chainKey] = sm

	// Same provider name configured in both tiers; only the static one recovers.
	shared := bootTestProviders("shared")
	sharedBackup := bootTestProviders("shared")
	rpsr.failedStaticProviders[chainKey] = shared
	rpsr.failedBackupProviders[chainKey] = sharedBackup

	convert := func(providers []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
		out := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(providers))
		for i, p := range providers {
			out[uint64(i)] = createTestProviderSession(p.Name, 0)
		}
		return out
	}
	var tierSeen int
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{
		chainKey: {
			rpcEndpoint:                rpcEndpoint,
			convertProvidersToSessions: convert,
			configuredStatic:           shared,
			configuredBackup:           sharedBackup,
			// applyReverification runs static first, then backup.
			validateFn: func(context.Context, *lavasession.RPCStaticProviderEndpoint) error {
				tierSeen++
				if tierSeen == 1 {
					return nil // static recovered
				}
				return errors.New("backup still unreachable")
			},
		},
	}

	rpsr.updateEpoch(context.Background(), 2)

	require.Empty(t, rpsr.failedStaticProviders[chainKey], "static promoted, so it leaves its own list")
	require.Len(t, rpsr.failedBackupProviders[chainKey], 1,
		"the backup never recovered — it must stay queued for retry")
}
