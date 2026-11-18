package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// MAG-5349: a url that serves only its add-on collections (standalone-addons), none of which
// declares a way to read the head, has no tracker by design. These drive what the server does
// with such a url. The bundled ETH1 spec's `bundler` add-on is that shape: no GET_BLOCKNUM.

// The probe judges a url by its block polls. A url that never has one would read "not alive"
// every cycle and pull its provider's availability down; left out, the provider is judged by
// the urls that can be judged.
func TestProbeCycle_UntrackedByDesignURLIsLeftOutOfItsProvidersSample(t *testing.T) {
	const tracked, submitOnly = "http://rpc:8545", "http://submit:8090"
	now := time.Unix(1_700_000_000, 0)
	endpoints := []*routersession.EndpointWithDirectConnection{
		ep(tracked, "mixed", true),
		ep(submitOnly, "mixed", true),
		ep(submitOnly, "submit-only", true),
	}
	// The tracked url is healthy; the other has no observation at all, as in production.
	getObs := func(url string) (endpointstate.EndpointObservation, bool) {
		if url == tracked {
			return freshObs(1000, now.Add(-time.Second), now.Add(-time.Second), 20*time.Millisecond), true
		}
		return endpointstate.EndpointObservation{}, false
	}
	untracked := func(url string) bool { return url == submitOnly }
	availability := func(list []*routersession.EndpointWithDirectConnection) map[string]float64 {
		appender := &recordingAppender{}
		runProbeCycleCore(list, getObs, 1000, true, provideroptimizer.SyncReference{}, now, probeCfg(), appender, nil)
		out := map[string]float64{}
		for _, call := range appender.calls {
			out[call.provider] = call.availability
		}
		return out
	}

	// What the cycle fed before the url was left out: the harm this change removes.
	require.Equal(t, map[string]float64{"mixed": 0.5, "submit-only": 0}, availability(endpoints),
		"precondition: scored, a never-polled url reads as dead")

	require.Equal(t, map[string]float64{"mixed": 1}, availability(probeScoredEndpoints(endpoints, untracked)),
		"the provider is judged by its tracked url alone, and a provider with no tracked url sends no sample")
}

func TestProbeScoredEndpoints(t *testing.T) {
	a, b, c := ep("http://a", "p1", true), ep("http://b", "p1", true), ep("http://c", "p2", true)
	all := []*routersession.EndpointWithDirectConnection{a, nil, b, c}
	only := func(urls ...string) func(string) bool {
		return func(url string) bool {
			for _, u := range urls {
				if u == url {
					return true
				}
			}
			return false
		}
	}

	t.Run("none untracked: the caller's slice, untouched", func(t *testing.T) {
		got := probeScoredEndpoints(all, only())
		require.Equal(t, all, got)
		require.Same(t, &all[0], &got[0], "no copy on the common path")
	})
	t.Run("nil check: everything is scored", func(t *testing.T) {
		require.Equal(t, all, probeScoredEndpoints(all, nil))
	})
	t.Run("untracked urls are dropped, the rest keep their order", func(t *testing.T) {
		require.Equal(t, []*routersession.EndpointWithDirectConnection{a, nil, c}, probeScoredEndpoints(all, only("http://b")))
		require.Equal(t, []*routersession.EndpointWithDirectConnection{nil, b}, probeScoredEndpoints(all, only("http://a", "http://c")))
		require.Len(t, all, 4, "the caller's slice is not modified: the head-stall step still reads all of it")
		require.Equal(t, a, all[0])
	})
}

// admitStandaloneBundler pairs one provider whose only url serves just the `bundler` add-on.
func (f *reconcileFixture) admitStandaloneBundler(t *testing.T, ctx context.Context, url string) {
	t.Helper()
	conn, err := routersession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: url}, 5, "jsonrpc")
	require.NoError(t, err)
	pairing := map[uint64]*routersession.ConsumerSessionsWithProvider{
		0: routersession.NewConsumerSessionWithProvider(
			"bundler-provider",
			[]*routersession.Endpoint{{
				NetworkAddress:    url,
				Enabled:           true,
				Addons:            map[string]struct{}{"bundler": {}},
				StandaloneAddons:  true,
				DirectConnections: []routersession.DirectRPCConnection{conn},
			}},
			999999, f.epoch, int64(1),
		),
	}
	require.NoError(t, f.csm.UpdateAllProviders(f.epoch, pairing, nil))
}

// The reconcile pass runs for the life of the pod. A url with no tracker by design must be
// settled on the first pass and never again reported as an endpoint that "had no tracker".
func TestReconcileChainTrackers_UntrackedByDesignURLIsSettledOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newReconcileFixture(t, ctx)
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	f.server.smartRouterEndpointMetrics = mm

	const provider = "bundler-provider"
	const url = "http://127.0.0.1:1/mag5349-bundler"
	mm.RegisterEndpoint("ETH1", "jsonrpc", url, provider)
	f.admitStandaloneBundler(t, ctx, url)

	for pass := 0; pass < 3; pass++ {
		created, alreadyPresent, failed := f.server.reconcileChainTrackers(ctx, pass == 0)
		require.Zero(t, created, "pass %d: no tracker is created for it", pass)
		require.Zero(t, failed, "pass %d: and that is not a failure", pass)
		require.Equal(t, 1, alreadyPresent, "pass %d: it is settled", pass)
	}
	require.False(t, f.hasTracker(url))
	require.True(t, f.monitor.IsUntrackedByDesign(url))

	fingerprint := metrics.URLFingerprint(url)
	requireTrackingFlag(t, provider, url, 0, "the per-url series says the url is not tracked by design")

	// A relay it served is counted on its url series and stays counted. The check that undoes
	// a write for a REMOVED url must not take this url for one: it has no tracker either.
	// The counter lives in the process-wide registry, so an earlier run of this test in the same
	// process has already counted on this series: compare with what was there, not with zero.
	servedBefore := gatherSeries(t, "rpc_endpoint_url_relays_serviced_total", provider)[fingerprint]
	f.server.countURLRelayServiced(provider, url)
	f.server.countURLRelayServiced(provider, url)
	require.Equal(t, map[string]float64{fingerprint: servedBefore + 2}, gatherSeries(t, "rpc_endpoint_url_relays_serviced_total", provider))
	requireTrackingFlag(t, provider, url, 0, "counting a relay leaves the flag as it was")

	// The relay path asks for the url's tracker on every relay: still none, still settled.
	for _, e := range f.csm.GetAllDirectRPCEndpoints() {
		f.server.ensureEndpointChainTracker(e.ProviderAddress, e.Endpoint, e.DirectConnection)
	}
	require.False(t, f.hasTracker(url))
}

// A tracked url reads 1 on the same series, so 0 can only mean "by design".
func TestReconcileChainTrackers_TrackedURLReadsTracked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newReconcileFixture(t, ctx)
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	f.server.smartRouterEndpointMetrics = mm

	const url = "http://127.0.0.1:1/mag5349-tracked"
	mm.RegisterEndpoint("ETH1", "jsonrpc", url, "provider-0")
	f.admit(t, ctx, url)

	created, _, failed := f.server.reconcileChainTrackers(ctx, true)
	require.Equal(t, 1, created)
	require.Zero(t, failed)
	requireTrackingFlag(t, "provider-0", url, 1, "a tracked url reads 1")
}

// Two providers list one url: "addon-provider" as a standalone `bundler` url with no head,
// "base-provider" as a base url. The url is tracked for both, on the reconcile pass and on the
// relay path, whichever endpoint is walked first: the base provider must keep its block height.
func TestReconcileChainTrackers_URLSharedWithABaseProviderIsTracked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newReconcileFixture(t, ctx)
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	f.server.smartRouterEndpointMetrics = mm

	const url = "http://127.0.0.1:1/mag5349-shared"
	mm.RegisterEndpoint("ETH1", "jsonrpc", url, "addon-provider")
	mm.RegisterEndpoint("ETH1", "jsonrpc", url, "base-provider")

	standalone := &routersession.Endpoint{
		NetworkAddress:   url,
		Enabled:          true,
		Addons:           map[string]struct{}{"bundler": {}},
		StandaloneAddons: true,
	}
	base := &routersession.Endpoint{NetworkAddress: url, Enabled: true}
	pairing := map[uint64]*routersession.ConsumerSessionsWithProvider{}
	for i, p := range []struct {
		name     string
		endpoint *routersession.Endpoint
	}{{"addon-provider", standalone}, {"base-provider", base}} {
		conn, err := routersession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: url}, 5, "jsonrpc")
		require.NoError(t, err)
		p.endpoint.DirectConnections = []routersession.DirectRPCConnection{conn}
		pairing[uint64(i)] = routersession.NewConsumerSessionWithProvider(p.name, []*routersession.Endpoint{p.endpoint}, 999999, f.epoch, int64(1))
	}
	require.NoError(t, f.csm.UpdateAllProviders(f.epoch, pairing, nil))

	// The relay path meets the standalone endpoint first: alone, the url is not tracked.
	for _, e := range f.csm.GetAllDirectRPCEndpoints() {
		if e.ProviderAddress == "addon-provider" {
			f.server.ensureEndpointChainTracker(e.ProviderAddress, e.Endpoint, e.DirectConnection)
		}
	}
	require.True(t, f.monitor.IsUntrackedByDesign(url), "precondition: the standalone endpoint was seen first")

	_, _, failed := f.server.reconcileChainTrackers(ctx, true)
	require.Zero(t, failed)
	require.True(t, f.hasTracker(url), "the base provider's endpoint gives the url its tracker")
	require.False(t, f.monitor.IsUntrackedByDesign(url))

	for _, provider := range []string{"addon-provider", "base-provider"} {
		requireTrackingFlag(t, provider, url, 1, provider+": the url reads as tracked")
	}

	// Settled: a later pass creates nothing and the relay path changes nothing.
	created, _, failed := f.server.reconcileChainTrackers(ctx, false)
	require.Zero(t, created)
	require.Zero(t, failed)
	for _, e := range f.csm.GetAllDirectRPCEndpoints() {
		f.server.ensureEndpointChainTracker(e.ProviderAddress, e.Endpoint, e.DirectConnection)
	}
	require.True(t, f.hasTracker(url))
}

// The probe cycle itself, not its helpers: a provider whose only url is not tracked by design
// must get no probe sample. Scored, it would be fed availability 0 on every cycle.
func TestRunProbeCycle_LeavesOutAnUntrackedByDesignURL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newReconcileFixture(t, ctx)

	const url = "http://127.0.0.1:1/mag5349-probe-cycle"
	f.admitStandaloneBundler(t, ctx, url)
	f.server.reconcileChainTrackers(ctx, true)
	require.True(t, f.monitor.IsUntrackedByDesign(url))

	appender := &recordingAppender{}
	f.server.runProbeCycle(appender, probeCfg())
	require.Empty(t, appender.calls, "a url with no block polls by design is not judged by them")

	// The same cycle does judge a url that should have polls and has none: the rule is narrow.
	const tracked = "http://127.0.0.1:1/mag5349-probe-cycle-tracked"
	f.admit(t, ctx, tracked)
	f.server.reconcileChainTrackers(ctx, false)
	appender = &recordingAppender{}
	f.server.runProbeCycle(appender, probeCfg())
	require.Len(t, appender.calls, 1)
	require.Equal(t, "provider-0", appender.calls[0].provider)
	require.Zero(t, appender.calls[0].availability)
}

// The relay path can be the first to meet a url (a relay routed before the reconcile pass). It
// makes the decision, so it writes the flag: without it the dashboard reads the url as unknown
// until the next pass.
func TestEnsureEndpointChainTracker_WritesTheTrackingFlag(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newReconcileFixture(t, ctx)
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	f.server.smartRouterEndpointMetrics = mm

	relayPath := func() {
		for _, e := range f.csm.GetAllDirectRPCEndpoints() {
			f.server.ensureEndpointChainTracker(e.ProviderAddress, e.Endpoint, e.DirectConnection)
		}
	}

	const headless = "http://127.0.0.1:1/mag5349-relay-path-headless"
	mm.RegisterEndpoint("ETH1", "jsonrpc", headless, "bundler-provider")
	f.admitStandaloneBundler(t, ctx, headless)
	relayPath()
	requireTrackingFlag(t, "bundler-provider", headless, 0, "the relay path decided the url is not tracked")

	const tracked = "http://127.0.0.1:1/mag5349-relay-path-tracked"
	mm.RegisterEndpoint("ETH1", "jsonrpc", tracked, "provider-0")
	f.admit(t, ctx, tracked)
	relayPath()
	require.True(t, f.hasTracker(tracked))
	requireTrackingFlag(t, "provider-0", tracked, 1, "the relay path gave the url its tracker")
}

// A url's series are deleted when it leaves the pairing. Whatever brings it back must write the
// flag again, or the url has no series until the pod restarts: the reconcile pass writes it for
// every url on every pass, and /debug/reset-chaintracker-rows writes it for the rows it restores.
func TestBlockTrackingFlag_IsWrittenAgainAfterItsSeriesWereDeleted(t *testing.T) {
	for _, headless := range []bool{true, false} {
		name, provider, want := "tracked url", "provider-0", 1.0
		if headless {
			name, provider, want = "url not tracked by design", "bundler-provider", 0.0
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f := newReconcileFixture(t, ctx)
			mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
			require.NotNil(t, mm)
			f.server.smartRouterEndpointMetrics = mm

			url := "http://127.0.0.1:1/mag5349-rewrite-" + provider
			mm.RegisterEndpoint("ETH1", "jsonrpc", url, provider)
			if headless {
				f.admitStandaloneBundler(t, ctx, url)
			} else {
				f.admit(t, ctx, url)
			}
			router := &RPCSmartRouter{
				rpcServers:      map[string]*RPCSmartRouterServer{"ETH1-jsonrpc": f.server},
				sessionManagers: map[string]*routersession.ConsumerSessionManager{"ETH1-jsonrpc": f.csm},
			}

			f.server.reconcileChainTrackers(ctx, true)
			requireTrackingFlag(t, provider, url, want, "first pass")

			// The series is lost while the url keeps its row: the next pass writes it again.
			mm.ForgetEndpointURLLatestBlock("ETH1", "jsonrpc", url)
			requireNoTrackingFlag(t, provider, url)
			f.server.reconcileChainTrackers(ctx, false)
			requireTrackingFlag(t, provider, url, want, "the reconcile pass writes the flag for a url it has nothing else to do for")

			// The epoch cleanup drops the row and the series; the reset brings the row back
			// before any reconcile pass runs.
			f.monitor.RemoveTracker(url)
			mm.ForgetEndpointURLLatestBlock("ETH1", "jsonrpc", url)
			requireNoTrackingFlag(t, provider, url)
			ensured, created := reregisterChainTrackerRowsLocked(router)
			require.Equal(t, 1, ensured)
			requireTrackingFlag(t, provider, url, want, "the reset writes the flag for the row it restored")
			if headless {
				require.Zero(t, created, "a url with no tracker by design has no row to create")
				require.True(t, f.monitor.IsUntrackedByDesign(url))
			} else {
				require.Equal(t, 1, created)
			}
		})
	}
}

// trackingFlag reads one url's rpc_endpoint_url_block_tracking series. The registry is shared by
// the whole test process and the fixtures reuse provider names, so a test asks for its own url.
func trackingFlag(t *testing.T, provider, url string) (value float64, written bool) {
	t.Helper()
	value, written = gatherSeries(t, "rpc_endpoint_url_block_tracking", provider)[metrics.URLFingerprint(url)]
	return value, written
}

func requireTrackingFlag(t *testing.T, provider, url string, want float64, msg string) {
	t.Helper()
	got, written := trackingFlag(t, provider, url)
	require.True(t, written, "no series for the url: %s", msg)
	require.Equal(t, want, got, msg)
}

func requireNoTrackingFlag(t *testing.T, provider, url string) {
	t.Helper()
	_, written := trackingFlag(t, provider, url)
	require.False(t, written, "precondition: the url's series is gone")
}
