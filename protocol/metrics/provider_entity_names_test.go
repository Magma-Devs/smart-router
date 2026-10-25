package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// MAG-4219. A provider entity is named "<provider>/<index>". resolveProviderNames redacts an
// unregistered input that looks like a url, and a slash is enough to look like one, so a
// registered entity name must pass through untouched or every series would carry
// "chainstack/[redacted]".
func TestResolveProviderNames_RegisteredEntityNameIsNotRedacted(t *testing.T) {
	m := newSmartRouterForURLFanoutTest()
	m.RegisterEndpoint("TONT", "rest", "https://ton.example/api/v2?api_key=secret", "chainstack/0/v2")
	m.RegisterEndpoint("TONT", "rest", "https://ton.example/api", "chainstack/0")

	require.Equal(t, []string{"chainstack/0"}, m.resolveProviderNames("chainstack/0"))
	require.Equal(t, []string{"chainstack/0/v2"}, m.resolveProviderNames("chainstack/0/v2"))

	// The url itself still resolves to its names, and an unregistered url is still redacted.
	require.Equal(t, []string{"chainstack/0/v2"}, m.resolveProviderNames("https://ton.example/api/v2?api_key=secret"))
	redacted := m.resolveProviderNames("https://other.example/v1?api_key=secret")
	require.Len(t, redacted, 1)
	require.NotContains(t, redacted[0], "secret")

	// An unregistered name with a slash is still treated as a url: nothing vouches for it.
	require.Equal(t, []string{"never/[redacted]"}, m.resolveProviderNames("never/registered"))
}

// endpointPoints collects one vec's series for spec, keyed by endpoint_id.
func endpointPoints(t *testing.T, c prometheus.Collector, spec string) map[string]float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	c.Collect(ch)
	close(ch)
	out := map[string]float64{}
	for metric := range ch {
		var pb dto.Metric
		require.NoError(t, metric.Write(&pb))
		labels := map[string]string{}
		for _, lp := range pb.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		if labels["spec"] != spec {
			continue
		}
		_, dup := out[labels["endpoint_id"]]
		require.False(t, dup, "two series for endpoint_id %q", labels["endpoint_id"])
		out[labels["endpoint_id"]] = pb.GetGauge().GetValue()
	}
	return out
}

// MAG-4219, the ticket's case at the metrics layer: two urls of one configured provider, one at
// the tip and one 500 behind. Each is its own entity, so the url-keyed writers land on their own
// endpoint_id and one series per url results, where one series per provider showed the last
// writer. The health gauge of one url says nothing about the other.
func TestEntityPerURL_OneSeriesPerURL(t *testing.T) {
	m := newSmartRouterForURLFanoutTest()
	const (
		fastURL = "https://sep.vendor.example/eu?api_key=secret"
		slowURL = "https://sep.vendor.example/us?api_key=secret"
	)
	m.RegisterEndpoint("SEP1", "jsonrpc", fastURL, "chainstack/0")
	m.RegisterEndpoint("SEP1", "jsonrpc", slowURL, "chainstack/1")

	m.SetEndpointLatestBlock("SEP1", "jsonrpc", fastURL, 9_000_500)
	m.SetEndpointLatestBlock("SEP1", "jsonrpc", slowURL, 9_000_000)
	require.Equal(t, map[string]float64{"chainstack/0": 9_000_500, "chainstack/1": 9_000_000},
		endpointPoints(t, m.endpointLatestBlock.GaugeVec, "SEP1"))

	m.SetEndpointOverallHealth("SEP1", "jsonrpc", "chainstack/1", false)
	require.Equal(t, map[string]float64{"chainstack/0": 1, "chainstack/1": 0},
		endpointPoints(t, m.endpointOverallHealth.GaugeVec, "SEP1"))
}
