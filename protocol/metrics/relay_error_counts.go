package metrics

import (
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// RelayErrorCount is one (chain, interface, provider, method) series of the two per-attempt
// error counters, smartrouter_node_errors_total and smartrouter_protocol_errors_total, read
// back for GET /debug/relay-errors (MAG-4718). The two counts stay separate: the failover rule
// treats a node error and a protocol error differently, and one sum would hide that.
type RelayErrorCount struct {
	ChainID         string
	ApiInterface    string
	ProviderAddress string
	Method          string
	NodeErrors      uint64
	ProtocolErrors  uint64
}

// RelayErrorCounts reads both counters straight out of their collectors, so the debug route
// reports exactly what the metrics port would, labels included (method is the normalized
// label). One entry per labelled series with a count above zero, sorted by its labels.
// Cumulative since the process started; no debug reset clears a Prometheus counter.
func (m *SmartRouterMetricsManager) RelayErrorCounts() []RelayErrorCount {
	if m == nil {
		return nil
	}
	rows := map[[4]string]*RelayErrorCount{}
	collectCounter(m.incidentNodeErrorsTotalMetric, rows, func(row *RelayErrorCount, value uint64) { row.NodeErrors = value })
	collectCounter(m.incidentProtocolErrorsTotalMetric, rows, func(row *RelayErrorCount, value uint64) { row.ProtocolErrors = value })

	out := make([]RelayErrorCount, 0, len(rows))
	for _, row := range rows {
		out = append(out, *row)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.ChainID != b.ChainID {
			return a.ChainID < b.ChainID
		}
		if a.ApiInterface != b.ApiInterface {
			return a.ApiInterface < b.ApiInterface
		}
		if a.ProviderAddress != b.ProviderAddress {
			return a.ProviderAddress < b.ProviderAddress
		}
		return a.Method < b.Method
	})
	return out
}

// collectCounter folds every non-zero series of a counter vector labelled
// {spec, apiInterface, provider_address, method} into rows, through set.
func collectCounter(vec *prometheus.CounterVec, rows map[[4]string]*RelayErrorCount, set func(*RelayErrorCount, uint64)) {
	if vec == nil {
		return
	}
	ch := make(chan prometheus.Metric)
	go func() {
		vec.Collect(ch)
		close(ch)
	}()
	for metric := range ch {
		var written dto.Metric
		if err := metric.Write(&written); err != nil || written.GetCounter() == nil {
			continue
		}
		value := uint64(written.GetCounter().GetValue())
		if value == 0 {
			continue
		}
		labels := make(map[string]string, len(written.GetLabel()))
		for _, pair := range written.GetLabel() {
			labels[pair.GetName()] = pair.GetValue()
		}
		key := [4]string{labels["spec"], labels["apiInterface"], labels["provider_address"], labels["method"]}
		row, ok := rows[key]
		if !ok {
			row = &RelayErrorCount{ChainID: key[0], ApiInterface: key[1], ProviderAddress: key[2], Method: key[3]}
			rows[key] = row
		}
		set(row, value)
	}
}
