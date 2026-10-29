package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// positionalSpec isolates this test's series in the process-wide default registry, which the
// manager registers on and every test in the package shares.
const positionalSpec = "POSLABEL-SPEC-4244"

// seriesLabels returns the label set of every series of metric whose spec is positionalSpec.
func seriesLabels(t *testing.T, metric string) []map[string]string {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var out []map[string]string
	for _, f := range families {
		if f.GetName() != metric {
			continue
		}
		for _, mt := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range mt.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["spec"] == positionalSpec {
				out = append(out, labels)
			}
		}
	}
	return out
}

func withLabel(base map[string]string, key, value string) map[string]string {
	out := map[string]string{key: value}
	for k, v := range base {
		out[k] = v
	}
	return out
}

// TestPositionalLabels_EveryWrappedMetric guards the label ORDER of the rpc_endpoint_* series.
//
// These metrics used to go through a wrapper that took a map of label name to value, so the
// order a caller listed labels in could not matter. They now take their values positionally
// (MAG-4244 section 7), which is cheaper — no map and no reordering slice per update — but
// means each call's argument order has to match the metric's declared label order. A swap
// would not fail to compile; it would silently write, say, the provider into the function
// label, on series the stuck-tip and per-url alerts read.
//
// So every metric is driven once with a DIFFERENT value in every label position, and the
// series it produces must carry each value under its own name. A swap of spec with anything
// moves the series out of the positionalSpec filter; any other swap leaves it there with the
// wrong pairs. Either fails here.
func TestPositionalLabels_EveryWrappedMetric(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	const (
		api      = "posApi"
		endpoint = "posEndpoint"
		function = "eth_call" // passes normalizeMethodLabel untouched
		provider = "posProvider"
		nodeURL  = "https://poslabel.example/rpc"
		path     = "/posPath"
	)
	m.RegisterEndpoint(positionalSpec, api, nodeURL, provider)
	m.RegisterEndpointInternalPath(nodeURL, path)

	byEndpoint := map[string]string{"spec": positionalSpec, "apiInterface": api, "endpoint_id": endpoint}
	byFunction := withLabel(byEndpoint, "function", function)
	byURL := map[string]string{
		"spec": positionalSpec, "apiInterface": api, "endpoint_id": provider,
		"url": URLFingerprint(nodeURL), "internal_path": path,
	}

	for _, c := range []struct {
		metric string
		drive  func()
		want   map[string]string
	}{
		{"rpc_endpoint_total_relays_serviced", func() { m.AddEndpointRelayServiced(positionalSpec, api, endpoint, function) }, byFunction},
		{"rpc_endpoint_total_errored", func() { m.AddEndpointRelayErrored(positionalSpec, api, endpoint, function) }, byFunction},
		{"rpc_endpoint_total_cancelled", func() { m.AddEndpointRelayCancelled(positionalSpec, api, endpoint, function) }, byFunction},
		{"rpc_endpoint_requests_in_flight", func() { m.AddEndpointInFlightRequest(positionalSpec, api, endpoint, function) }, byFunction},
		{"rpc_endpoint_overall_health", func() { m.SetEndpointOverallHealth(positionalSpec, api, endpoint, true) }, byEndpoint},
		{"rpc_endpoint_latest_block", func() { m.SetEndpointLatestBlock(positionalSpec, api, endpoint, 7) }, byEndpoint},
		{"rpc_endpoint_fetch_latest_fails", func() { m.AddEndpointFetchLatestFail(positionalSpec, api, endpoint) }, byEndpoint},
		{"rpc_endpoint_fetch_block_fails", func() { m.AddEndpointFetchBlockFail(positionalSpec, api, endpoint) }, byEndpoint},
		{"rpc_endpoint_fetch_latest_success", func() { m.AddEndpointFetchLatestSuccess(positionalSpec, api, endpoint) }, byEndpoint},
		{"rpc_endpoint_fetch_block_success", func() { m.AddEndpointFetchBlockSuccess(positionalSpec, api, endpoint) }, byEndpoint},
		{"rpc_endpoint_tracker_requests_total", func() { m.AddEndpointTrackerRequest(positionalSpec, api, endpoint, "posKind") }, withLabel(byEndpoint, "kind", "posKind")},
		{"rpc_endpoint_tracker_gate_errors_total", func() { m.RecordTrackerGateError(positionalSpec, api, endpoint, "posOp") }, withLabel(byEndpoint, "op", "posOp")},
		{"rpc_endpoint_tracker_gate_skips_total", func() { m.RecordTrackerGateSkip(positionalSpec, api, endpoint, "posSource") }, withLabel(byEndpoint, "source", "posSource")},
		{"rpc_endpoint_url_latest_block", func() { m.SetProviderURLLatestBlock(positionalSpec, api, provider, nodeURL, 9, time.Minute) }, byURL},
		{"rpc_endpoint_url_answered_until_seconds", func() { m.SetProviderURLLatestBlock(positionalSpec, api, provider, nodeURL, 9, time.Minute) }, byURL},
		{"rpc_endpoint_url_relays_serviced_total", func() { m.AddEndpointURLRelayServiced(positionalSpec, api, provider, nodeURL) }, byURL},
	} {
		t.Run(c.metric, func(t *testing.T) {
			c.drive()
			require.Contains(t, seriesLabels(t, c.metric), c.want,
				"each value must sit under its own label name; a positional swap moves it")
		})
	}

	// Forgetting a url deletes its per-url series by the same positional values, so it must
	// remove exactly the series written above.
	m.ForgetEndpointURLLatestBlock(positionalSpec, api, nodeURL)
	for _, metric := range []string{"rpc_endpoint_url_latest_block", "rpc_endpoint_url_answered_until_seconds", "rpc_endpoint_url_relays_serviced_total"} {
		require.NotContains(t, seriesLabels(t, metric), byURL, "%s outlived ForgetEndpointURLLatestBlock", metric)
	}
}
