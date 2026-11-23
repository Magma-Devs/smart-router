package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// MAG-5244 — the per-url out-of-rotation metrics as the probe cycle writes them: the call in
// runProbeCycle, its place after the head-stall step, and a url removed while a cycle runs.

// blockNode is a JSON-RPC node that answers every request with the block in head.
func blockNode(head *atomic.Int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		b := head.Load()
		result := fmt.Sprintf(`"0x%x"`, b)
		if req.Method != "eth_blockNumber" {
			result = fmt.Sprintf(`{"number":"0x%x","hash":"0x%064x"}`, b, b)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, result)
	}))
}

// urlOutSeriesByReason returns reason → value of metric's series for one node url, matched by the
// url's fingerprint so that series other tests of the package left in the default registry do not
// count.
func urlOutSeriesByReason(t *testing.T, metric, nodeURL string) map[string]float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	fp := metrics.URLFingerprint(nodeURL)
	out := map[string]float64{}
	for _, f := range families {
		if f.GetName() != metric {
			continue
		}
		for _, mt := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range mt.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["url"] != fp {
				continue
			}
			if mt.GetGauge() != nil {
				out[labels["reason"]] = mt.GetGauge().GetValue()
			} else {
				out[labels["reason"]] = mt.GetCounter().GetValue()
			}
		}
	}
	return out
}

const (
	urlOutGauge = "rpc_endpoint_url_out_of_rotation"
	urlOutExits = "rpc_endpoint_url_out_of_rotation_exits_total"
)

// startTrackers registers the per-url metrics of each url under its fixture provider name
// (provider-<i>, the order admit gave), starts the trackers and waits until each one polls.
func startTrackers(t *testing.T, ctx context.Context, f *reconcileFixture, urls ...string) {
	t.Helper()
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	f.server.smartRouterEndpointMetrics = mm
	f.admit(t, ctx, urls...)
	for i, url := range urls {
		registerEndpointMetrics(mm, f.server.listenEndpoint, common.NodeUrl{Url: url}, fmt.Sprintf("provider-%d", i))
	}
	go f.server.initializeChainTrackers(ctx)
	for _, url := range urls {
		require.Eventually(t, func() bool {
			state, _, _ := f.monitor.GetTrackerState(url)
			return state == endpointstate.EndpointChainTrackerPolling
		}, 10*time.Second, 20*time.Millisecond)
	}
}

// The gauge after a probe cycle shows what that cycle's head-stall step left. Cycle 1: a url held
// head-stalled reads head-stalled. Cycle 2: its block moved on, the step releases it, and the gauge is
// empty after that same cycle. Deleting the publish call from runProbeCycle fails cycle 1; moving it
// before runHeadStall fails cycle 2. (Marking a url takes the stall grace, 30 s at least, so the
// test releases one instead.)
func TestURLOutOfRotation_ProbeCyclePublishesWhatTheHeadStallStepLeft(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var headA, headB atomic.Int64
	headA.Store(1100)
	headB.Store(1000)
	nodeA, nodeB := blockNode(&headA), blockNode(&headB)
	defer nodeA.Close()
	defer nodeB.Close()

	f := newReconcileFixtureWith(t, ctx, nil)
	startTrackers(t, ctx, f, nodeA.URL, nodeB.URL)
	for url, want := range map[string]int64{nodeA.URL: 1100, nodeB.URL: 1000} {
		require.Eventually(t, func() bool {
			_, _, _ = f.monitor.PollNow(ctx, url)
			obs, _ := f.monitor.GetObservation(url)
			return obs.HighestBlockSeen == want
		}, 10*time.Second, 20*time.Millisecond)
	}
	var b *routersession.Endpoint
	for _, e := range f.csm.GetAllDirectRPCEndpoints() {
		if e.Endpoint.NetworkAddress == nodeB.URL {
			b = e.Endpoint
		}
	}
	require.NotNil(t, b)
	exitsBefore := urlOutSeriesByReason(t, urlOutExits, nodeB.URL)["head-stalled"]
	require.True(t, b.AddStateReason(routersession.EndpointDisableHeadStalled, time.Now(), 1000))
	cfg := probeVerdictConfigFor(30*time.Second, nil)

	f.server.runProbeCycle(nil, cfg)
	require.Equal(t, map[string]float64{"head-stalled": 1}, urlOutSeriesByReason(t, urlOutGauge, nodeB.URL),
		"cycle 1: the url is out")
	require.Equal(t, 1.0, urlOutSeriesByReason(t, urlOutExits, nodeB.URL)["head-stalled"]-exitsBefore)
	require.Empty(t, urlOutSeriesByReason(t, urlOutGauge, nodeA.URL), "the url ahead stays in rotation")

	headB.Store(1100)
	require.Eventually(t, func() bool {
		_, _, _ = f.monitor.PollNow(ctx, nodeB.URL)
		obs, _ := f.monitor.GetObservation(nodeB.URL)
		return obs.HighestBlockSeen == 1100
	}, 10*time.Second, 20*time.Millisecond)
	f.server.runProbeCycle(nil, cfg)
	_, held := b.StateReason(routersession.EndpointDisableHeadStalled)
	require.False(t, held, "cycle 2: the head-stall step released the url")
	require.Empty(t, urlOutSeriesByReason(t, urlOutGauge, nodeB.URL), "cycle 2: the gauge shows the release")
}

// cleanupStaleTrackers can remove a url after a probe cycle read its endpoints and before the cycle
// publishes. The publish must not leave the url's series behind: nothing would delete the counter.
func TestURLOutOfRotation_URLRemovedDuringTheCycleLeavesNoSeries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var head atomic.Int64
	head.Store(1000)
	node := blockNode(&head)
	defer node.Close()

	f := newReconcileFixtureWith(t, ctx, nil)
	startTrackers(t, ctx, f, node.URL)
	eps := f.csm.GetAllDirectRPCEndpoints()
	require.Len(t, eps, 1)
	e := eps[0].Endpoint

	disableForTest(t, e, routersession.EndpointDisableUnreachable)
	f.server.publishURLsOutOfRotation(eps)
	require.Equal(t, map[string]float64{"unreachable": 1}, urlOutSeriesByReason(t, urlOutGauge, node.URL))

	// The url goes out again, and is removed before the cycle that read it publishes.
	require.True(t, e.ResetHealth())
	disableForTest(t, e, routersession.EndpointDisableUnreachable)
	f.monitor.RemoveTracker(node.URL)
	f.server.smartRouterEndpointMetrics.ForgetEndpointURLLatestBlock(f.server.listenEndpoint.ChainID, f.server.listenEndpoint.ApiInterface, node.URL)
	f.server.publishURLsOutOfRotation(eps)

	require.Empty(t, urlOutSeriesByReason(t, urlOutGauge, node.URL))
	require.Empty(t, urlOutSeriesByReason(t, urlOutExits, node.URL), "a removed url keeps no counter")
}
