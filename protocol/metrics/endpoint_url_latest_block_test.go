package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// MAG-4204 option B: rpc_endpoint_url_latest_block is one series per url of a provider, for the
// stuck-provider alerts. The provider series is the lowest of these heads, and no single series
// per provider can show a url that froze while another url of the provider still trails it.

type urlSeriesPoint struct {
	url, internalPath string
	block             float64
}

// urlSeriesOf returns provider's rpc_endpoint_url_latest_block series for spec.
func urlSeriesOf(t *testing.T, m *SmartRouterMetricsManager, spec, provider string) []urlSeriesPoint {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	m.endpointURLLatestBlock.GaugeVec.Collect(ch)
	close(ch)
	var out []urlSeriesPoint
	for metric := range ch {
		var pb dto.Metric
		require.NoError(t, metric.Write(&pb))
		labels := map[string]string{}
		for _, lp := range pb.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		if labels["spec"] == spec && labels["endpoint_id"] == provider {
			out = append(out, urlSeriesPoint{labels["url"], labels["internal_path"], pb.GetGauge().GetValue()})
		}
	}
	return out
}

// byInternalPath maps the series by internal_path, failing if two series share one.
func byInternalPath(t *testing.T, points []urlSeriesPoint) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, p := range points {
		_, dup := out[p.internalPath]
		require.False(t, dup, "two series for internal_path %q", p.internalPath)
		out[p.internalPath] = p.block
	}
	return out
}

func newTONProviderWithPaths(t *testing.T) *SmartRouterMetricsManager {
	t.Helper()
	m := newTONProvider(t)
	m.RegisterEndpointInternalPath(tonV2URL, "/v2")
	m.RegisterEndpointInternalPath(tonV3URL, "/v3")
	return m
}

func TestURLLatestBlock_OneSeriesPerURL(t *testing.T) {
	m := newTONProviderWithPaths(t)
	m.SetEndpointLatestBlock("TON", "rest", tonV2URL, 1000)
	m.SetEndpointLatestBlock("TON", "rest", tonV3URL, 990)

	points := urlSeriesOf(t, m, "TON", "chainstack")
	require.Equal(t, map[string]float64{"/v2": 1000, "/v3": 990}, byInternalPath(t, points))
	require.NotEqual(t, points[0].url, points[1].url, "the two urls share a host, so the url label must still tell them apart")
}

// The case the provider series cannot show: /v2 freezes while /v3 still trails it (an indexer
// 28,000 blocks behind). The provider series follows /v3 for as long as /v3 is lower, but /v2's
// own series stands still, which is what the stuck-provider alerts read.
func TestURLLatestBlock_AFrozenURLStandsStillWhileItsSiblingLags(t *testing.T) {
	m := newTONProviderWithPaths(t)
	for v3 := int64(500); v3 < 510; v3++ {
		m.SetEndpointLatestBlock("TON", "rest", tonV2URL, 1000) // frozen, still answering
		m.SetEndpointLatestBlock("TON", "rest", tonV3URL, v3)
		require.Equal(t, float64(v3), providerLatestBlock(t, m, "TON", "chainstack"), "the provider series moves with /v3")
		require.Equal(t, map[string]float64{"/v2": 1000, "/v3": float64(v3)}, byInternalPath(t, urlSeriesOf(t, m, "TON", "chainstack")))
	}
}

// A removed url's own series must go, not stand still: a series that keeps its last value
// forever is exactly what the stuck-provider alerts fire on.
func TestURLLatestBlock_ForgetDeletesTheURLSeries(t *testing.T) {
	m := newTONProviderWithPaths(t)
	m.SetEndpointLatestBlock("TON", "rest", tonV2URL, 1000)
	m.SetEndpointLatestBlock("TON", "rest", tonV3URL, 1200)

	m.ForgetEndpointLatestBlock("TON", "rest", tonV2URL)
	require.Equal(t, map[string]float64{"/v3": 1200}, byInternalPath(t, urlSeriesOf(t, m, "TON", "chainstack")))

	m.ForgetEndpointLatestBlock("TON", "rest", tonV3URL)
	m.ForgetEndpointLatestBlock("TON", "rest", "https://never.registered")
	require.Empty(t, urlSeriesOf(t, m, "TON", "chainstack"))
}

// A shared url is one series per provider configured with it, and the harvest path moves only
// the provider it relayed to.
func TestURLLatestBlock_SharedURLIsASeriesPerProvider(t *testing.T) {
	m := newTONProviderWithPaths(t)
	m.RegisterEndpoint("TON", "rest", tonV2URL, "chainstack-backup")
	m.SetEndpointLatestBlock("TON", "rest", tonV2URL, 1000)
	m.SetProviderURLLatestBlock("TON", "rest", "chainstack", tonV2URL, 1001)

	require.Equal(t, map[string]float64{"/v2": 1001}, byInternalPath(t, urlSeriesOf(t, m, "TON", "chainstack")))
	require.Equal(t, map[string]float64{"/v2": 1000}, byInternalPath(t, urlSeriesOf(t, m, "TON", "chainstack-backup")))
}

// The url label is exported and retained: no part of a node url's path, query or userinfo, where
// an api key lives, may reach it. The hash still tells two urls on one host apart, and the same
// url always gets the same label.
func TestURLSeriesLabel_CarriesNoSecret(t *testing.T) {
	const secret = "s3cr3tK3y0123456789"
	raws := []string{
		"https://ton.vendor.example/" + secret + "/api/v2",
		"https://ton.vendor.example/" + secret + "/api/v3",
		"https://eth.vendor.example/rpc?apikey=" + secret,
		"https://user:" + secret + "@eth.vendor.example/rpc",
		"wss://eth.vendor.example/ws/" + secret,
	}
	seen := map[string]string{}
	for _, raw := range raws {
		label := urlSeriesLabel(raw)
		require.NotContains(t, label, secret, raw)
		require.NotContains(t, label, "/api/", raw)
		require.True(t, strings.Contains(label, "vendor.example"), "the host stays readable: %s", label)
		require.Equal(t, label, urlSeriesLabel(raw), "the label is stable")
		other, dup := seen[label]
		require.False(t, dup, "%s and %s share label %s", raw, other, label)
		seen[label] = raw
	}
}
