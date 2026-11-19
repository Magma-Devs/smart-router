package rpcsmartrouter

import (
	"maps"
	"sort"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// gatheredCounter reads the value of the one series of a counter whose labels are exactly labels, or
// 0 when there is no such series yet.
func gatheredCounter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, mtr := range mf.GetMetric() {
			lm := map[string]string{}
			for _, lp := range mtr.GetLabel() {
				lm[lp.GetName()] = lp.GetValue()
			}
			if maps.Equal(lm, labels) {
				return mtr.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// MAG-5244 — collectURLsOutOfRotation, the read-only step that turns the endpoints' own records into
// what the per-url out-of-rotation metrics publish.

// outFor finds the record of one url and sorts its reasons, which come from a map.
func outFor(t *testing.T, recs []metrics.URLOutOfRotation, provider, url string) metrics.URLOutOfRotation {
	t.Helper()
	for _, r := range recs {
		if r.Provider == provider && r.URL == url {
			sort.Strings(r.Reasons)
			return r
		}
	}
	t.Fatalf("no record for %s %s", provider, url)
	return metrics.URLOutOfRotation{}
}

func disableForTest(t *testing.T, e *routersession.Endpoint, reason routersession.EndpointDisableReason) {
	t.Helper()
	for i := uint64(0); i < routersession.MaxConsecutiveConnectionAttempts && e.IsEnabled(); i++ {
		e.MarkUnhealthy(reason)
	}
	require.False(t, e.IsEnabled())
}

func TestURLOutOfRotation_ReportsEachReasonThatHolds(t *testing.T) {
	healthy := ep("https://a.example", "provA", true)
	stuck := ep("https://b.example", "provB", true)
	require.True(t, stuck.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
	both := ep("https://c.example", "provC", true)
	disableForTest(t, both.Endpoint, routersession.EndpointDisableNodeError)
	require.True(t, both.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))

	recs, _ := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{healthy, stuck, both}, urlOutSeen{})

	require.Len(t, recs, 3, "every url is listed, in or out, so the publisher can clear the ones that came back")
	require.Empty(t, outFor(t, recs, "provA", "https://a.example").Reasons)
	require.Equal(t, []string{"head-stalled"}, outFor(t, recs, "provB", "https://b.example").Reasons)
	require.Equal(t, []string{"head-stalled", "node-error"}, outFor(t, recs, "provC", "https://c.example").Reasons)
}

func TestURLOutOfRotation_CountsAnExitOnceAcrossCycles(t *testing.T) {
	e := ep("https://a.example", "provA", true)
	eps := []*routersession.EndpointWithDirectConnection{e}
	require.True(t, e.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))

	recs, seen := collectURLsOutOfRotation(eps, urlOutSeen{})
	require.Equal(t, map[string]uint64{"head-stalled": 1}, outFor(t, recs, "provA", "https://a.example").WentOut)

	recs, seen = collectURLsOutOfRotation(eps, seen)
	require.Empty(t, outFor(t, recs, "provA", "https://a.example").WentOut, "still out, but it did not go out again")

	// Out, back and out again between two cycles: the gauge would show one hold, the count shows two.
	require.True(t, e.Endpoint.RemoveStateReason(routersession.EndpointDisableHeadStalled))
	require.True(t, e.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 101))
	require.True(t, e.Endpoint.RemoveStateReason(routersession.EndpointDisableHeadStalled))
	require.True(t, e.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 102))
	recs, _ = collectURLsOutOfRotation(eps, seen)
	require.Equal(t, map[string]uint64{"head-stalled": 2}, outFor(t, recs, "provA", "https://a.example").WentOut)
}

// A hold that started and ended between two cycles: nothing is out when the step looks, and it is
// still counted.
func TestURLOutOfRotation_CountsAHoldThatEndedBeforeTheCycle(t *testing.T) {
	e := ep("https://a.example", "provA", true)
	eps := []*routersession.EndpointWithDirectConnection{e}
	_, seen := collectURLsOutOfRotation(eps, urlOutSeen{})

	require.True(t, e.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
	require.True(t, e.Endpoint.RemoveStateReason(routersession.EndpointDisableHeadStalled))

	recs, _ := collectURLsOutOfRotation(eps, seen)
	rec := outFor(t, recs, "provA", "https://a.example")
	require.Empty(t, rec.Reasons)
	require.Equal(t, map[string]uint64{"head-stalled": 1}, rec.WentOut)
}

// The chart writes an archive url twice: two endpoints, one node, one series. Head-stall takes both
// out together, and that is one exit, not two.
func TestURLOutOfRotation_URLListedTwiceIsOneRecordAndOneExit(t *testing.T) {
	plain := ep("https://a.example", "provA", true)
	archive := ep("https://a.example", "provA", true)
	require.True(t, plain.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
	require.True(t, archive.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))

	recs, _ := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{plain, archive}, urlOutSeen{})

	require.Len(t, recs, 1)
	require.Equal(t, []string{"head-stalled"}, recs[0].Reasons)
	require.Equal(t, map[string]uint64{"head-stalled": 1}, recs[0].WentOut)
}

// One of the two entries of a url out is enough to show the url as out for that reason.
func TestURLOutOfRotation_URLListedTwiceIsOutWhenEitherEntryIs(t *testing.T) {
	plain := ep("https://a.example", "provA", true)
	archive := ep("https://a.example", "provA", true)
	disableForTest(t, archive.Endpoint, routersession.EndpointDisableUnreachable)

	recs, _ := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{plain, archive}, urlOutSeen{})

	require.Len(t, recs, 1)
	require.Equal(t, []string{"unreachable"}, recs[0].Reasons)
}

// A provider in both pools lists the same Endpoint twice. It is one endpoint.
func TestURLOutOfRotation_SameEndpointListedTwiceCountsOnce(t *testing.T) {
	e := ep("https://a.example", "provA", true)
	require.True(t, e.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
	dup := &routersession.EndpointWithDirectConnection{Endpoint: e.Endpoint, ProviderAddress: "provA", Backup: true}

	recs, seen := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{e, dup}, urlOutSeen{})

	require.Len(t, recs, 1)
	require.Equal(t, map[string]uint64{"head-stalled": 1}, recs[0].WentOut)
	require.Len(t, seen.endpoints, 1)
}

// The review's example: one node behind two entries dies. Normal requests take the plain entry out
// in one cycle, an archive request takes the archive entry out in the next. One node went out once:
// the url counts one exit, not one per entry.
func TestURLOutOfRotation_URLListedTwiceOutInTwoCyclesIsOneExit(t *testing.T) {
	plain := ep("https://a.example", "provA", true)
	archive := ep("https://a.example", "provA", true)
	eps := []*routersession.EndpointWithDirectConnection{plain, archive}

	disableForTest(t, plain.Endpoint, routersession.EndpointDisableUnreachable)
	recs, seen := collectURLsOutOfRotation(eps, urlOutSeen{})
	require.Equal(t, map[string]uint64{"unreachable": 1}, recs[0].WentOut, "cycle 1: the plain entry takes the url out")

	disableForTest(t, archive.Endpoint, routersession.EndpointDisableUnreachable)
	recs, seen = collectURLsOutOfRotation(eps, seen)
	require.Empty(t, recs[0].WentOut, "cycle 2: the archive entry joins an outage already counted")
	require.Equal(t, []string{"unreachable"}, recs[0].Reasons)

	// Both come back, then the node dies again: a new outage, counted again.
	require.True(t, plain.Endpoint.ResetHealth())
	require.True(t, archive.Endpoint.ResetHealth())
	recs, seen = collectURLsOutOfRotation(eps, seen)
	require.Empty(t, recs[0].Reasons)
	disableForTest(t, archive.Endpoint, routersession.EndpointDisableUnreachable)
	recs, _ = collectURLsOutOfRotation(eps, seen)
	require.Equal(t, map[string]uint64{"unreachable": 1}, recs[0].WentOut)
}

// Joining is per reason: the other entry being out for another reason does not hide this exit.
func TestURLOutOfRotation_URLListedTwiceOtherReasonStillCounts(t *testing.T) {
	plain := ep("https://a.example", "provA", true)
	archive := ep("https://a.example", "provA", true)
	eps := []*routersession.EndpointWithDirectConnection{plain, archive}

	disableForTest(t, plain.Endpoint, routersession.EndpointDisableUnreachable)
	_, seen := collectURLsOutOfRotation(eps, urlOutSeen{})

	disableForTest(t, archive.Endpoint, routersession.EndpointDisableNodeError)
	recs, _ := collectURLsOutOfRotation(eps, seen)
	require.Equal(t, map[string]uint64{"node-error": 1}, recs[0].WentOut)
}

// A pairing rebuild replaces the Endpoint objects, and the new one starts counting at zero. Its
// first exit counts when the url was in rotation before the rebuild. When the url was already out
// for that reason, it is the same outage carried across the rebuild, and adds nothing: the gauge
// never cleared either.
func TestURLOutOfRotation_RebuiltEndpoint(t *testing.T) {
	t.Run("url was in rotation: its first exit counts", func(t *testing.T) {
		old := ep("https://a.example", "provA", true)
		_, seen := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{old}, urlOutSeen{})

		rebuilt := ep("https://a.example", "provA", true)
		require.True(t, rebuilt.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
		recs, seen := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{rebuilt}, seen)

		require.Equal(t, map[string]uint64{"head-stalled": 1}, recs[0].WentOut)
		require.Len(t, seen.endpoints, 1, "the replaced endpoint is forgotten")
	})
	t.Run("url was already out: same outage", func(t *testing.T) {
		old := ep("https://a.example", "provA", true)
		require.True(t, old.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
		_, seen := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{old}, urlOutSeen{})

		rebuilt := ep("https://a.example", "provA", true)
		require.True(t, rebuilt.Endpoint.AddStateReason(routersession.EndpointDisableHeadStalled, stallNow, 100))
		recs, _ := collectURLsOutOfRotation([]*routersession.EndpointWithDirectConnection{rebuilt}, seen)

		require.Empty(t, recs[0].WentOut)
		require.Equal(t, []string{"head-stalled"}, recs[0].Reasons)
	})
}

// The server's own path, end to end through a real metrics manager: a url registered the way
// CreateSmartRouterEndpoint registers it goes out, and the series the probe step publishes carry
// exactly the labels of the url's other per-url series (the relay counter, written by the relay
// path), plus reason. A test of the collector or of the manager alone passes with the wiring wrong.
func TestURLOutOfRotation_ServerPublishesUnderTheURLsOwnLabels(t *testing.T) {
	const chain, provider = "OUTROT-WIRING-5244", "provider@outrotWiring"
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	listen := &routersession.RPCEndpoint{ChainID: chain, ApiInterface: spectypes.APIInterfaceRest}
	nodeURL := common.NodeUrl{Url: "http://outrot-wiring.example/api/v3", InternalPath: "/v3"}
	registerEndpointMetrics(mm, listen, nodeURL, provider)
	rpcss := &RPCSmartRouterServer{smartRouterEndpointMetrics: mm, listenEndpoint: listen}

	e := ep(nodeURL.Url, provider, true)
	eps := []*routersession.EndpointWithDirectConnection{e}
	rpcss.countURLRelayServiced(provider, nodeURL.Url)
	served := urlSeriesLabels(t, "rpc_endpoint_url_relays_serviced_total", provider)
	require.Len(t, served, 1)
	want := map[string]string{"reason": "node-error"}
	for k, v := range served[0] {
		want[k] = v
	}

	rpcss.publishURLsOutOfRotation(eps)
	require.Empty(t, urlSeriesLabels(t, "rpc_endpoint_url_out_of_rotation", provider), "in rotation: no series")

	// The tests share the default registry, so a run before this one (go test -count=N) left its count.
	exitsBefore := gatheredCounter(t, "rpc_endpoint_url_out_of_rotation_exits_total", want)
	disableForTest(t, e.Endpoint, routersession.EndpointDisableNodeError)
	rpcss.publishURLsOutOfRotation(eps)
	rpcss.publishURLsOutOfRotation(eps) // still out on the next cycle: no second exit
	require.Equal(t, []map[string]string{want}, urlSeriesLabels(t, "rpc_endpoint_url_out_of_rotation", provider))
	require.Equal(t, []map[string]string{want}, urlSeriesLabels(t, "rpc_endpoint_url_out_of_rotation_exits_total", provider))
	require.Equal(t, 1.0, gatheredCounter(t, "rpc_endpoint_url_out_of_rotation_exits_total", want)-exitsBefore)

	require.True(t, e.Endpoint.ResetHealth())
	rpcss.publishURLsOutOfRotation(eps)
	require.Empty(t, urlSeriesLabels(t, "rpc_endpoint_url_out_of_rotation", provider), "back in rotation: the gauge series goes")
	require.Len(t, urlSeriesLabels(t, "rpc_endpoint_url_out_of_rotation_exits_total", provider), 1, "the counter stays")
}
