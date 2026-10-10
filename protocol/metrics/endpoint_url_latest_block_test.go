package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// MAG-4204 option B: one series per node url of a provider, for the stuck-provider alerts.
// rpc_endpoint_latest_block, the provider series, is written by every url of the provider and is
// left as it was.

const (
	tonV2URL = "https://vendor.example/api/v2"
	tonV3URL = "https://vendor.example/api/v3"
)

type urlPoint struct {
	internalPath string
	url          string
	value        float64
}

// urlPoints collects one per-url vec's series for spec and provider.
func urlPoints(t *testing.T, c prometheus.Collector, spec, provider string) []urlPoint {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	c.Collect(ch)
	close(ch)
	var out []urlPoint
	for metric := range ch {
		var pb dto.Metric
		require.NoError(t, metric.Write(&pb))
		labels := map[string]string{}
		for _, lp := range pb.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		if labels["spec"] != spec || labels["endpoint_id"] != provider {
			continue
		}
		value := pb.GetGauge().GetValue()
		if pb.GetCounter() != nil {
			value = pb.GetCounter().GetValue()
		}
		out = append(out, urlPoint{labels["internal_path"], labels["url"], value})
	}
	return out
}

// byPath maps the points by internal_path, failing if two share one.
func byPath(t *testing.T, points []urlPoint) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, p := range points {
		_, dup := out[p.internalPath]
		require.False(t, dup, "two series for internal_path %q", p.internalPath)
		out[p.internalPath] = p.value
	}
	return out
}

func newTONProvider(t *testing.T) *SmartRouterMetricsManager {
	t.Helper()
	m := newSmartRouterForURLFanoutTest()
	m.RegisterEndpoint("TON", "rest", tonV2URL, "chainstack")
	m.RegisterEndpoint("TON", "rest", tonV3URL, "chainstack")
	m.RegisterEndpointInternalPath(tonV2URL, "/v2")
	m.RegisterEndpointInternalPath(tonV3URL, "/v3")
	return m
}

// The case one series per provider cannot show: /v2 frozen while /v3, which trails it, moves on.
// Each url has its own series, and the provider series is not touched.
func TestURLLatestBlock_OneSeriesPerURL(t *testing.T) {
	m := newTONProvider(t)
	for v3 := int64(500); v3 < 505; v3++ {
		m.SetEndpointURLLatestBlock("TON", "rest", tonV2URL, 1000, DefaultURLAnswerTimeout) // frozen, still answering
		m.SetEndpointURLLatestBlock("TON", "rest", tonV3URL, v3, DefaultURLAnswerTimeout)
		require.Equal(t, map[string]float64{"/v2": 1000, "/v3": float64(v3)},
			byPath(t, urlPoints(t, m.endpointURLLatestBlock.GaugeVec, "TON", "chainstack")))
	}
	points := urlPoints(t, m.endpointURLLatestBlock.GaugeVec, "TON", "chainstack")
	require.NotEqual(t, points[0].url, points[1].url, "/v2 and /v3 share a host; the url label still tells them apart")
	for _, p := range points {
		require.Equal(t, map[string]string{"/v2": URLFingerprint(tonV2URL), "/v3": URLFingerprint(tonV3URL)}[p.internalPath], p.url)
	}
	require.Empty(t, urlPoints(t, m.endpointLatestBlock.GaugeVec, "TON", "chainstack"), "the provider series is written elsewhere")
}

// Every write moves the url's answered-until time to now plus the answer timeout its caller
// passes, DefaultURLAnswerTimeout when the caller passes none.
func TestURLLatestBlock_AnsweredUntilIsTheLastAnswerPlusTheTimeout(t *testing.T) {
	m := newTONProvider(t)
	m.RegisterEndpoint("ETH1", "jsonrpc", "https://eth.vendor.example", "solo")

	before := time.Now()
	m.SetEndpointURLLatestBlock("TON", "rest", tonV2URL, 1000, 10*time.Minute)
	m.SetProviderURLLatestBlock("TON", "rest", "chainstack", tonV3URL, 1000, 20*time.Minute)
	m.SetEndpointURLLatestBlock("ETH1", "jsonrpc", "https://eth.vendor.example", 20, 0)
	after := time.Now()

	within := func(got float64, timeout time.Duration) {
		t.Helper()
		require.GreaterOrEqual(t, got, float64(before.Add(timeout).Unix()))
		require.LessOrEqual(t, got, float64(after.Add(timeout).Unix()))
	}
	ton := byPath(t, urlPoints(t, m.endpointURLAnsweredUntil.GaugeVec, "TON", "chainstack"))
	within(ton["/v2"], 10*time.Minute)
	within(ton["/v3"], 20*time.Minute)
	within(byPath(t, urlPoints(t, m.endpointURLAnsweredUntil.GaugeVec, "ETH1", "solo"))[""], DefaultURLAnswerTimeout)
}

// A shared url is one series per provider configured with it; the relay harvest moves only the
// provider it relayed to.
func TestURLLatestBlock_SharedURLIsASeriesPerProvider(t *testing.T) {
	m := newTONProvider(t)
	m.RegisterEndpoint("TON", "rest", tonV2URL, "chainstack-backup")
	m.SetEndpointURLLatestBlock("TON", "rest", tonV2URL, 1000, DefaultURLAnswerTimeout)
	m.SetProviderURLLatestBlock("TON", "rest", "chainstack", tonV2URL, 1001, DefaultURLAnswerTimeout)

	require.Equal(t, map[string]float64{"/v2": 1001}, byPath(t, urlPoints(t, m.endpointURLLatestBlock.GaugeVec, "TON", "chainstack")))
	require.Equal(t, map[string]float64{"/v2": 1000}, byPath(t, urlPoints(t, m.endpointURLLatestBlock.GaugeVec, "TON", "chainstack-backup")))
}

// An unregistered url would name itself as its provider, and no part of a url is safe to export.
func TestURLLatestBlock_UnregisteredURLGetsNoSeries(t *testing.T) {
	m := newSmartRouterForURLFanoutTest()
	const url = "https://unregistered.vendor.example/key"
	m.SetEndpointURLLatestBlock("ETH1", "jsonrpc", url, 10, DefaultURLAnswerTimeout)
	m.SetProviderURLLatestBlock("ETH1", "jsonrpc", "p", url, 10, DefaultURLAnswerTimeout)
	m.AddEndpointURLRelayServiced("ETH1", "jsonrpc", "p", url)
	for _, c := range []prometheus.Collector{m.endpointURLLatestBlock.GaugeVec, m.endpointURLAnsweredUntil.GaugeVec, m.endpointURLRelaysServiced.CounterVec} {
		ch := make(chan prometheus.Metric, 4)
		c.Collect(ch)
		close(ch)
		require.Empty(t, ch)
	}
}

// Relays are counted per url, so a page about a url can ask whether customers' requests reach it.
func TestURLRelaysServiced_CountsPerURL(t *testing.T) {
	m := newTONProvider(t)
	for i := 0; i < 3; i++ {
		m.AddEndpointURLRelayServiced("TON", "rest", "chainstack", tonV2URL)
	}
	m.AddEndpointURLRelayServiced("TON", "rest", "chainstack", tonV3URL)
	require.Equal(t, map[string]float64{"/v2": 3, "/v3": 1}, byPath(t, urlPoints(t, m.endpointURLRelaysServiced.CounterVec, "TON", "chainstack")))
}

// A removed url loses every per-url series, and nothing else: a series left behind stands still
// forever, which is what the stuck alerts fire on.
func TestURLLatestBlock_ForgetDeletesTheURLsSeries(t *testing.T) {
	m := newTONProvider(t)
	m.SetEndpointURLLatestBlock("TON", "rest", tonV2URL, 1000, DefaultURLAnswerTimeout)
	m.SetEndpointURLLatestBlock("TON", "rest", tonV3URL, 1200, DefaultURLAnswerTimeout)
	m.AddEndpointURLRelayServiced("TON", "rest", "chainstack", tonV2URL)
	m.AddEndpointURLRelayServiced("TON", "rest", "chainstack", tonV3URL)

	m.ForgetEndpointURLLatestBlock("TON", "rest", tonV2URL)
	require.Equal(t, map[string]float64{"/v3": 1200}, byPath(t, urlPoints(t, m.endpointURLLatestBlock.GaugeVec, "TON", "chainstack")))
	require.Len(t, urlPoints(t, m.endpointURLAnsweredUntil.GaugeVec, "TON", "chainstack"), 1)
	require.Equal(t, map[string]float64{"/v3": 1}, byPath(t, urlPoints(t, m.endpointURLRelaysServiced.CounterVec, "TON", "chainstack")))

	m.ForgetEndpointURLLatestBlock("TON", "rest", "https://never.registered.example")
	var nilManager *SmartRouterMetricsManager
	nilManager.ForgetEndpointURLLatestBlock("TON", "rest", tonV2URL)
	nilManager.SetEndpointURLLatestBlock("TON", "rest", tonV2URL, 1, DefaultURLAnswerTimeout)
	nilManager.AddEndpointURLRelayServiced("TON", "rest", "chainstack", tonV2URL)
}

// The url label is exported and retained, and a node url can hold an api key in any part of it:
// the path, the query, the userinfo, or a host label. So no part of the url reaches the label,
// only a fingerprint, which still tells two urls apart and gives one url the same label every time.
func TestURLFingerprint_CarriesNothingOfTheURL(t *testing.T) {
	const secret = "s3cr3tK3y0123456789"
	raws := []string{
		"https://ton.vendor.example/" + secret + "/api/v2",
		"https://ton.vendor.example/" + secret + "/api/v3",
		"https://eth.vendor.example/rpc?apikey=" + secret,
		"https://user:" + secret + "@eth.vendor.example/rpc",
		"https://" + secret + "-1.rpc.vendor.example:443",
		secret + "-1.grpc.vendor.example:443",
	}
	seen := map[string]string{}
	for _, raw := range raws {
		label := URLFingerprint(raw)
		require.Regexp(t, `^[0-9a-f]{8}$`, label, raw)
		require.NotContains(t, label, secret, raw)
		require.False(t, strings.Contains(raw, label), "the label must not be a piece of the url: %s", raw)
		require.Equal(t, label, URLFingerprint(raw), "the label is stable")
		other, dup := seen[label]
		require.False(t, dup, "%s and %s share label %s", raw, other, label)
		seen[label] = raw
	}
}
