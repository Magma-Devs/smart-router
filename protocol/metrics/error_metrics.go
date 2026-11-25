package metrics

import (
	"sync"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/prometheus/client_golang/prometheus"
)

var errorMetricsOnce sync.Once

// InitErrorMetrics initializes the Prometheus error counter and registers
// it as the metrics callback in the error logging system.
// Call once during application startup (e.g., in consumer/provider server init).
func InitErrorMetrics() {
	errorMetricsOnce.Do(func() {
		// Label set: {error_name, error_category, retryable, chain_id, apiInterface, source}.
		//
		// Cardinality budget:
		//   - error_name:     ~100 registered codes, plus each chain's own verdicts once spec
		//                     rows act (MAG-5592)
		//   - error_category: 2 values (internal, external)
		//   - retryable:      2 values
		//   - chain_id:       ~50 live chains
		//   - apiInterface:   5 values (jsonrpc, tendermintrpc, rest, grpc, graphql), the same as
		//                     smartrouter_node_errors_total's
		//   - source:         6 values (client, probe, boot, reverify, poll, subscription; MAG-5598)
		// Theoretical max: 100 × 2 × 2 × 50 × 5 × 6 = 600,000 series.
		// Practical max is much lower: (a) a chain serves one or two interfaces, so apiInterface
		// adds little beyond chain_id; (b) source multiplies only the codes the router's own
		// traffic and refused subscriptions meet; client stays the bulk; (c) most (name, chain)
		// pairs never fire. Realistic steady-state: a small multiple of the few hundred series
		// the first four labels gave.
		//
		// category and retryable are pinned per name at registration today. They stop being 1:1
		// with error_name once spec rows act: a child spec that changes a verdict's class makes one
		// name carry a different retryable per chain_id (PRD §9). We keep both as labels for filter
		// ergonomics: "rate of retryable errors by chain" needs no join against a mapping table.
		//
		// source is who sent the attempt (common.ErrorSource). Only client is a client's own
		// request: the SLA queries and the dashboard's error pivots read source=~"client|", where
		// "" matches a router from before this label. EmitErrorMetric never writes "".
		//
		// Why error_code is NOT a label: it would be a second 1:1-with-name
		// dimension that adds no filter power. error_code still appears in
		// structured logs for operator search; the Prometheus series only
		// needs one identity key.
		counter := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "smartrouter_errors_total",
			Help: "Total classified errors by name, category, retryability, chain, interface and who sent the attempt (source).",
		}, []string{"error_name", "error_category", "retryable", "chain_id", "apiInterface", "source"})

		// Best-effort registration. Three cases:
		//  1. Success → use the counter we just created.
		//  2. Already registered (e.g. from a previous test run or a
		//     duplicate InitErrorMetrics call) → reuse the existing
		//     collector so both callers share the same counter.
		//  3. Any OTHER registration failure (collector with same name but
		//     mismatched labels, Prometheus internal error, etc.) → log a
		//     warning so operators know smartrouter_errors_total will not be
		//     scrapable. The callback is still wired up so the counter
		//     keeps accumulating in-process and can be reached via direct
		//     introspection, but /metrics will be missing the series.
		counter = RegisterOrReuse(prometheus.DefaultRegisterer, counter, func(err error) {
			utils.FormatWarning("failed to register smartrouter_errors_total Prometheus counter; metric will not appear in /metrics scrapes", err)
		})

		common.SetErrorMetricsCallback(func(_ uint32, errorName string, errorCategory string, retryable bool, chainID string, apiInterface string, source common.ErrorSource) {
			retryStr := "false"
			if retryable {
				retryStr = "true"
			}
			counter.With(prometheus.Labels{
				"error_name":     errorName,
				"error_category": errorCategory,
				"retryable":      retryStr,
				"chain_id":       chainID,
				"apiInterface":   apiInterface,
				"source":         string(source),
			}).Inc()
		})
	})
}
