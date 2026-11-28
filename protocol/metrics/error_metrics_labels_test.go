package metrics

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// The counter InitErrorMetrics registers records the attempt's interface and who sent it beside the
// name and chain (MAG-5598), and never writes an empty source: the SLA queries read
// source=~"client|", where "" is a router from before the label.
func TestInitErrorMetricsLabelsTheInterfaceAndTheSource(t *testing.T) {
	InitErrorMetrics()
	t.Cleanup(func() { common.SetErrorMetricsCallback(nil) })

	common.EmitErrorMetric(common.RouterErrorConnectionRefused, "MAG5598A", common.ErrorOrigin{APIInterface: "rest", Source: common.ErrorSourceBoot})
	common.EmitErrorMetric(common.RouterErrorConnectionRefused, "MAG5598A", common.ErrorOrigin{APIInterface: "rest"})

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	got := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "smartrouter_errors_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := labelsOf(metric)
			if labels["chain_id"] != "MAG5598A" {
				continue
			}
			require.Equal(t, "rest", labels["apiInterface"])
			got[labels["source"]] += metric.GetCounter().GetValue()
		}
	}
	require.Equal(t, map[string]float64{"boot": 1, "client": 1}, got, "an origin without a source is counted as a client's, never as \"\"")
}

func labelsOf(metric *dto.Metric) map[string]string {
	labels := map[string]string{}
	for _, pair := range metric.GetLabel() {
		labels[pair.GetName()] = pair.GetValue()
	}
	return labels
}
