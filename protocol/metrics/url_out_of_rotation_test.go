package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// MAG-5244 — rpc_endpoint_url_out_of_rotation and its exits counter.

const outSpec = "OUTROT-SPEC-5244"

// outSeries returns reason → value for the series of metric that belong to one provider of outSpec.
func outSeries(t *testing.T, metric, provider string) map[string]float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
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
			if labels["spec"] != outSpec || labels["endpoint_id"] != provider {
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

// grewBy returns reason → how much each counter series of the provider went up since before. The
// tests share the default registry, so a counter left by an earlier run (go test -count=N) is read
// first and subtracted.
func grewBy(before, after map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for reason, v := range after {
		if d := v - before[reason]; d != 0 {
			out[reason] = d
		}
	}
	return out
}

const (
	outGauge   = "rpc_endpoint_url_out_of_rotation"
	outCounter = "rpc_endpoint_url_out_of_rotation_exits_total"
)

func TestURLOutOfRotation_SetWhileOutAndClearedWhenBack(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	const api, provider, url = "rest", "outProvA", "https://out-a.example/rpc"
	m.RegisterEndpoint(outSpec, api, url, provider)
	exitsBefore := outSeries(t, outCounter, provider)

	m.PublishURLsOutOfRotation(outSpec, api, []URLOutOfRotation{{Provider: provider, URL: url}}, nil)
	require.Empty(t, outSeries(t, outGauge, provider), "a url in rotation has no series")

	m.PublishURLsOutOfRotation(outSpec, api, []URLOutOfRotation{{
		Provider: provider, URL: url,
		Reasons: []string{"head-stalled", "node-error"},
		WentOut: map[string]uint64{"head-stalled": 1, "node-error": 1},
	}}, nil)
	require.Equal(t, map[string]float64{"head-stalled": 1, "node-error": 1}, outSeries(t, outGauge, provider),
		"a url out for two reasons has one series per reason")

	m.PublishURLsOutOfRotation(outSpec, api, []URLOutOfRotation{{Provider: provider, URL: url, Reasons: []string{"node-error"}}}, nil)
	require.Equal(t, map[string]float64{"node-error": 1}, outSeries(t, outGauge, provider),
		"the reason that cleared goes, the one that holds stays")

	m.PublishURLsOutOfRotation(outSpec, api, []URLOutOfRotation{{Provider: provider, URL: url}}, nil)
	require.Empty(t, outSeries(t, outGauge, provider))
	require.Equal(t, map[string]float64{"head-stalled": 1, "node-error": 1}, grewBy(exitsBefore, outSeries(t, outCounter, provider)),
		"the counter keeps what happened after the url came back")
}

// A provider that leaves the pairing is no longer listed by the probe cycle. Its series must go:
// left behind, it would read "out" for ever.
func TestURLOutOfRotation_ClearedWhenTheURLIsNoLongerListed(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	const api, provider, url = "rest", "outProvB", "https://out-b.example/rpc"
	m.RegisterEndpoint(outSpec, api, url, provider)

	m.PublishURLsOutOfRotation(outSpec, api, []URLOutOfRotation{{Provider: provider, URL: url, Reasons: []string{"unreachable"}}}, nil)
	require.Equal(t, map[string]float64{"unreachable": 1}, outSeries(t, outGauge, provider))

	m.PublishURLsOutOfRotation(outSpec, api, nil, nil)
	require.Empty(t, outSeries(t, outGauge, provider))
}

// One chain's cycle must not delete another interface's series: each publishes on its own.
func TestURLOutOfRotation_AnotherInterfaceIsLeftAlone(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	const provider, url = "outProvC", "https://out-c.example/rpc"
	m.RegisterEndpoint(outSpec, "rest", url, provider)

	m.PublishURLsOutOfRotation(outSpec, "rest", []URLOutOfRotation{{Provider: provider, URL: url, Reasons: []string{"head-stalled"}}}, nil)
	m.PublishURLsOutOfRotation(outSpec, "grpc", nil, nil)
	require.Equal(t, map[string]float64{"head-stalled": 1}, outSeries(t, outGauge, provider))
}

func TestURLOutOfRotation_CounterAddsEveryExit(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	const api, provider, url = "rest", "outProvD", "https://out-d.example/rpc"
	m.RegisterEndpoint(outSpec, api, url, provider)
	exitsBefore := outSeries(t, outCounter, provider)

	// Went out and came back between two cycles: nothing is out now, but it happened.
	m.PublishURLsOutOfRotation(outSpec, api, []URLOutOfRotation{{Provider: provider, URL: url, WentOut: map[string]uint64{"head-stalled": 2}}}, nil)
	m.PublishURLsOutOfRotation(outSpec, api, []URLOutOfRotation{{Provider: provider, URL: url, WentOut: map[string]uint64{"head-stalled": 1}}}, nil)
	require.Empty(t, outSeries(t, outGauge, provider))
	require.Equal(t, map[string]float64{"head-stalled": 3}, grewBy(exitsBefore, outSeries(t, outCounter, provider)))
}

func TestURLOutOfRotation_UnregisteredURLHasNoSeries(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	m.PublishURLsOutOfRotation(outSpec, "rest", []URLOutOfRotation{{
		Provider: "outProvE", URL: "https://never-registered.example",
		Reasons: []string{"unreachable"}, WentOut: map[string]uint64{"unreachable": 1},
	}}, nil)
	require.Empty(t, outSeries(t, outGauge, "outProvE"))
	require.Empty(t, outSeries(t, outCounter, "outProvE"))
}

func TestURLOutOfRotation_ForgetDeletesGaugeAndCounter(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	const api, provider, url = "rest", "outProvF", "https://out-f.example/rpc"
	m.RegisterEndpoint(outSpec, api, url, provider)
	m.PublishURLsOutOfRotation(outSpec, api, []URLOutOfRotation{{
		Provider: provider, URL: url, Reasons: []string{"unreachable"}, WentOut: map[string]uint64{"unreachable": 1},
	}}, nil)

	m.ForgetEndpointURLLatestBlock(outSpec, api, url)
	require.Empty(t, outSeries(t, outGauge, provider))
	require.Empty(t, outSeries(t, outCounter, provider))
}

func TestURLOutOfRotation_NilManagerIsSafe(t *testing.T) {
	var m *SmartRouterMetricsManager
	m.PublishURLsOutOfRotation(outSpec, "rest", []URLOutOfRotation{{Provider: "p", URL: "u", Reasons: []string{"unreachable"}}}, nil)
}

// The exits counter of every listed url starts at 0 under each reason it is given, before the url
// goes out: a series born at its first exit hides that exit from increase(). A reason not given
// gets no zero series, and still counts from its first exit.
func TestURLOutOfRotation_CounterStartsAtZero(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	const api, provider, url = "rest", "outProvG", "https://out-g.example/rpc"
	m.RegisterEndpoint(outSpec, api, url, provider)
	reasons := []string{"unreachable", "node-error", "head-stalled"}

	m.PublishURLsOutOfRotation(outSpec, api, []URLOutOfRotation{{Provider: provider, URL: url}}, reasons)
	require.Empty(t, outSeries(t, outGauge, provider), "in rotation: no gauge series")
	require.Equal(t, map[string]float64{"unreachable": 0, "node-error": 0, "head-stalled": 0}, outSeries(t, outCounter, provider),
		"in rotation: the counter is there, at 0")

	m.PublishURLsOutOfRotation(outSpec, api, []URLOutOfRotation{{
		Provider: provider, URL: url, Reasons: []string{"unspecified"}, WentOut: map[string]uint64{"unspecified": 1},
	}}, reasons)
	require.Equal(t, map[string]float64{"unreachable": 0, "node-error": 0, "head-stalled": 0, "unspecified": 1}, outSeries(t, outCounter, provider))
}
