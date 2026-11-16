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
	require.Equal(t, map[string]float64{fingerprint: 0}, gatherSeries(t, "rpc_endpoint_url_block_tracking", provider),
		"the per-url series says the url is not tracked by design")

	// A relay it served is counted on its url series and stays counted. The check that undoes
	// a write for a REMOVED url must not take this url for one: it has no tracker either.
	f.server.countURLRelayServiced(provider, url)
	f.server.countURLRelayServiced(provider, url)
	require.Equal(t, map[string]float64{fingerprint: 2}, gatherSeries(t, "rpc_endpoint_url_relays_serviced_total", provider))
	require.Equal(t, map[string]float64{fingerprint: 0}, gatherSeries(t, "rpc_endpoint_url_block_tracking", provider))

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
	require.Equal(t, map[string]float64{metrics.URLFingerprint(url): 1}, gatherSeries(t, "rpc_endpoint_url_block_tracking", "provider-0"))
}
