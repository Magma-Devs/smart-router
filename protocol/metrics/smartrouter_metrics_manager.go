package metrics

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/prometheus/client_golang/prometheus"
)

// registerOrReuse is RegisterOrReuse into the default registry, panicking on any other failure:
// the manager's collectors are the metrics page, so a router without them must not start.
func registerOrReuse[T prometheus.Collector](c T) T {
	return RegisterOrReuse(prometheus.DefaultRegisterer, c, PanicOnRegisterFailure)
}

// Compile-time check: SmartRouterMetricsManager must satisfy ConsumerMetricsManagerInf.
var _ ConsumerMetricsManagerInf = (*SmartRouterMetricsManager)(nil)

// SmartRouterMetricsManager manages metrics for the RPC Smart Router.
// It follows the post-unification metric spec with:
// - Endpoint-scoped metrics: rpc_endpoint_*
// - Router-scoped metrics: smartrouter_*
//
// Relay/latency metrics include a `function` label so a single metric covers
// both the per-function breakdown and, via sum aggregation, the endpoint/router
// aggregate — no duplicate metrics needed.
type SmartRouterMetricsManager struct {
	// Endpoint-scoped metrics (labels: spec, apiInterface, endpoint_id, function)
	endpointTotalRelaysServiced *MappedLabelsCounterVec  // rpc_endpoint_total_relays_serviced
	endpointTotalErrored        *MappedLabelsCounterVec  // rpc_endpoint_total_errored
	endpointTotalCancelled      *MappedLabelsCounterVec  // rpc_endpoint_total_cancelled
	endpointEndToEndLatency     *prometheus.HistogramVec // rpc_endpoint_end_to_end_latency_milliseconds
	endpointInFlight            *MappedLabelsGaugeVec    // rpc_endpoint_requests_in_flight

	// Endpoint-scoped metrics (labels: spec, apiInterface, endpoint_id)
	endpointOverallHealth          *MappedLabelsGaugeVec   // rpc_endpoint_overall_health
	endpointOverallHealthBreakdown *prometheus.GaugeVec    // rpc_endpoint_overall_health_breakdown {spec, apiInterface}
	endpointSelectionScore         *prometheus.GaugeVec    // rpc_endpoint_selection_score {spec, apiInterface, endpoint_id, score_type}
	optimizerSelectionScore        *prometheus.GaugeVec    // rpc_optimizer_selection_score {spec, endpoint_id, score_type}
	endpointLatestBlock            *MappedLabelsGaugeVec   // rpc_endpoint_latest_block
	endpointURLLatestBlock         *MappedLabelsGaugeVec   // rpc_endpoint_url_latest_block
	endpointURLAnsweredUntil       *MappedLabelsGaugeVec   // rpc_endpoint_url_answered_until_seconds
	endpointURLRelaysServiced      *MappedLabelsCounterVec // rpc_endpoint_url_relays_serviced_total
	endpointFetchLatestFails       *MappedLabelsCounterVec // rpc_endpoint_fetch_latest_fails
	endpointFetchBlockFails        *MappedLabelsCounterVec // rpc_endpoint_fetch_block_fails
	endpointFetchLatestSuccess     *MappedLabelsCounterVec // rpc_endpoint_fetch_latest_success
	endpointFetchBlockSuccess      *MappedLabelsCounterVec // rpc_endpoint_fetch_block_success
	endpointTrackerRequests        *MappedLabelsCounterVec // rpc_endpoint_tracker_requests_total
	endpointTrackerGateSkips       *MappedLabelsCounterVec // rpc_endpoint_tracker_gate_skips_total
	endpointTrackerGateErrors      *MappedLabelsCounterVec // rpc_endpoint_tracker_gate_errors_total

	// Router-scoped metrics (labels: spec, apiInterface, function)
	routerTotalRelaysServiced *prometheus.CounterVec   // smartrouter_total_relays_serviced
	routerTotalErrored        *prometheus.CounterVec   // smartrouter_total_errored
	routerEndToEndLatency     *prometheus.HistogramVec // smartrouter_end_to_end_latency_milliseconds

	// Router-scoped scalar metrics (no labels)
	routerOverallHealth          prometheus.Gauge
	routerOverallHealthBreakdown *prometheus.GaugeVec // smartrouter_overall_health_breakdown {spec, apiInterface}

	// Router-scoped metrics (labels: spec, apiInterface)
	routerLatestBlock            *prometheus.GaugeVec
	routerLatestBlockRegressions *prometheus.CounterVec // smartrouter_latest_block_regressions_total {spec, apiInterface, reason}
	routerChainIdle              *prometheus.GaugeVec   // smartrouter_chain_idle {spec, apiInterface}
	routerProtocolVersion        *prometheus.GaugeVec
	routerWsConnectionsActive    *prometheus.GaugeVec
	routerWsSubscriptionsTotal   *prometheus.CounterVec
	routerWsSubscriptionErrors   *prometheus.CounterVec
	subscriptionClientsDropped   *prometheus.CounterVec

	// Cross-validation group metrics
	crossValidationRequestsTotalMetric              *prometheus.CounterVec // smartrouter_cross_validation_requests_total        {spec, apiInterface, method}
	crossValidationSuccessTotalMetric               *prometheus.CounterVec // smartrouter_cross_validation_success_total         {spec, apiInterface, method}
	crossValidationFailedTotalMetric                *prometheus.CounterVec // smartrouter_cross_validation_failed_total          {spec, apiInterface, method}
	crossValidationProviderAgreementsTotalMetric    *prometheus.CounterVec // smartrouter_cross_validation_provider_agreements_total    {spec, apiInterface, method, provider_address}
	crossValidationProviderDisagreementsTotalMetric *prometheus.CounterVec // smartrouter_cross_validation_provider_disagreements_total {spec, apiInterface, method, provider_address}
	crossValidationMismatchTotalMetric              *prometheus.CounterVec // smartrouter_cross_validation_mismatch_total {spec, apiInterface, method, group, finality} — bounded alerting surface
	crossValidationFailuresTotalMetric              *prometheus.CounterVec // smartrouter_cross_validation_failures_total {spec, apiInterface, method, reason} — bounded by the CrossValidationReason* enum
	crossValidationStragglerTotalMetric             *prometheus.CounterVec // smartrouter_cross_validation_straggler_total {spec, apiInterface, method, outcome} — bounded by the CrossValidationStragglerOutcome* enum

	// Incident group metrics
	incidentNodeErrorsTotalMetric     *prometheus.CounterVec   // smartrouter_node_errors_total         {spec, apiInterface, provider_address, method}
	incidentProtocolErrorsTotalMetric *prometheus.CounterVec   // smartrouter_protocol_errors_total     {spec, apiInterface, provider_address, method}
	incidentRetriesTotalMetric        *prometheus.CounterVec   // smartrouter_retries_total             {spec, apiInterface, method}
	incidentRetriesSuccessMetric      *prometheus.CounterVec   // smartrouter_retries_success_total     {spec, apiInterface, method}
	incidentRetriesFailedMetric       *prometheus.CounterVec   // smartrouter_retries_failed_total      {spec, apiInterface, method}
	incidentRetryAttemptsHistogram    *prometheus.HistogramVec // smartrouter_retry_attempts            {spec, apiInterface, method}
	incidentConsistencyTotalMetric    *prometheus.CounterVec   // smartrouter_consistency_total         {spec, apiInterface, method}
	incidentConsistencySuccessMetric  *prometheus.CounterVec   // smartrouter_consistency_success_total {spec, apiInterface, method}
	incidentConsistencyFailedMetric   *prometheus.CounterVec   // smartrouter_consistency_failed_total  {spec, apiInterface, method}
	incidentHedgeTotalMetric          *prometheus.CounterVec   // smartrouter_hedge_total               {spec, apiInterface, method}
	incidentHedgeSuccessMetric        *prometheus.CounterVec   // smartrouter_hedge_success_total       {spec, apiInterface, method}
	incidentHedgeFailedMetric         *prometheus.CounterVec   // smartrouter_hedge_failed_total        {spec, apiInterface, method}
	incidentHedgeAttemptsHistogram    *prometheus.HistogramVec // smartrouter_hedge_attempts            {spec, apiInterface, method}

	// Cache group metrics (labels: spec, apiInterface, method, cache_tier; failed adds outcome)
	cacheRequestsTotalMetric *prometheus.CounterVec   // smartrouter_cache_requests_total
	cacheSuccessTotalMetric  *prometheus.CounterVec   // smartrouter_cache_success_total
	cacheFailedTotalMetric   *prometheus.CounterVec   // smartrouter_cache_failed_total {…, outcome=miss|error|timeout}
	cacheLatencyHistogram    *prometheus.HistogramVec // smartrouter_cache_latency_milliseconds — observed on EVERY attempted lookup
	// Cache writes (labels: spec, apiInterface, method; skipped adds reason)
	cacheWriteSkippedTotalMetric *prometheus.CounterVec   // smartrouter_cache_write_skipped_total {…, reason=size}
	cacheEntryBytesHistogram     *prometheus.HistogramVec // smartrouter_cache_entry_bytes — body size of every entry handed to the backend

	// CSM state-store size gauges (labels: spec, apiInterface). Expose otherwise
	// black-box internal state so integration tests can verify /debug/reset-all
	// emptied each store. See MAG-1762.
	csmBlockedProvidersCount       *prometheus.GaugeVec   // smartrouter_csm_blocked_providers
	csmPrevEpochBlockedProviders   *prometheus.GaugeVec   // smartrouter_csm_previous_epoch_blocked_providers
	csmProviderBlocked             *prometheus.GaugeVec   // smartrouter_csm_provider_blocked
	csmBlockedProvidersByReason    *prometheus.GaugeVec   // smartrouter_csm_blocked_providers_by_reason
	csmBlockedBackupProvidersCount *prometheus.GaugeVec   // smartrouter_csm_blocked_backup_providers
	endpointServingTier            *prometheus.GaugeVec   // smartrouter_endpoint_serving_tier
	providerVerificationFailed     *prometheus.GaugeVec   // smartrouter_provider_verification_failed
	csmStickySessionsCount         *prometheus.GaugeVec   // smartrouter_csm_sticky_sessions
	csmStickyClaims                *prometheus.CounterVec // smartrouter_csm_sticky_claims_total
	csmReportedProvidersCount      *prometheus.GaugeVec   // smartrouter_csm_reported_providers
	// verificationFailuresMu guards verificationFailures, the failing series last
	// published per provider with when each failure started, so a republish deletes
	// only the ones that cleared and keeps each start.
	verificationFailuresMu sync.Mutex
	verificationFailures   map[string][]publishedVerificationFailure
	// verificationNow is the clock a failure's start is read from; nil is time.Now.
	verificationNow func() time.Time
	// verificationCheckFailures counts failed check runs: smartrouter_provider_verification_check_failures_total
	verificationCheckFailures *prometheus.CounterVec

	// Router-scoped request group metrics (labels: spec, apiInterface, provider_address, method)
	routerRequestsTotal      *prometheus.CounterVec
	routerRequestsSuccess    *prometheus.CounterVec
	routerRequestsFailed     *prometheus.CounterVec
	routerRequestsRead       *prometheus.CounterVec
	routerRequestsWrite      *prometheus.CounterVec
	routerRequestsDebugTrace *prometheus.CounterVec
	routerRequestsArchive    *prometheus.CounterVec
	routerRequestsBatch      *prometheus.CounterVec

	// extensionUnavailableTotal counts replies served without an extension the caller asked for
	// (MAG-3935). Labels: spec, apiInterface, extension — the extension label is bounded by the
	// spec's extension names, see RecordExtensionUnavailable.
	extensionUnavailableTotal *prometheus.CounterVec // smartrouter_extension_unavailable_total

	// Batch-request shape metrics. The `method` label on every family above is collapsed
	// for batches (see batch_method_label.go) — batchSize carries the element count the
	// collapse drops, and batchSignatureOverflow reports when the signature cap binds.
	batchSize              *prometheus.HistogramVec
	batchSignatureOverflow *prometheus.CounterVec
	// batchSignatures bounds how many distinct batch signatures may become label values.
	batchSignatures *batchSignatureRegistry

	// Unmatched-API ("Default-*") method names are bounded the same way (see
	// default_method_label.go) — defaultMethodOverflow reports when the cap binds.
	defaultMethodOverflow *prometheus.CounterVec
	// defaultMethods bounds how many distinct Default-* names may become label values.
	defaultMethods *batchSignatureRegistry

	// Per-customer request group (MAG-4081), recorded once per client request by
	// SetRelayMetrics. The customer label is the caller's project-id header, bounded by
	// customers (see customer_label.go); customerOverflow reports when that cap binds.
	customerRequestsTotal   *prometheus.CounterVec   // smartrouter_customer_requests_total
	customerRequestsFailed  *prometheus.CounterVec   // smartrouter_customer_requests_failed_total
	customerEndToEndLatency *prometheus.HistogramVec // smartrouter_customer_end_to_end_latency_milliseconds
	customerOverflow        *prometheus.CounterVec   // smartrouter_customer_overflow_total
	customers               *batchSignatureRegistry

	// Internal state
	lock                    sync.RWMutex
	endpointsHealthChecksOk uint64
	// urlToProviderNames maps an endpoint URL to every provider name configured to use it.
	// URL-keyed metric emissions (e.g. ChainTracker OnNewBlock callbacks) must fan out to
	// ALL provider names sharing the URL — otherwise only the last-registered provider gets
	// the metric label, leaving its peers stuck at the zero value on dashboards.
	urlToProviderNames map[string][]string
	// urlInternalPaths maps a registered url to the spec path it serves ("/v2", or "" for the
	// root), for the internal_path label of the per-url series. Guarded by lock.
	urlInternalPaths map[string]string
	// urlFingerprints maps a registered url to its URLFingerprint, computed once at registration
	// rather than on every relay. Guarded by lock.
	urlFingerprints    map[string]string
	optimizerQoSClient *ConsumerOptimizerQoSClient
}

// DefaultURLAnswerTimeout is the shortest time a url counts as answering after an answer. A chain
// server whose trackers can go longer between two answered polls (the traffic gate's skips plus
// the poll, and one poll interval more) passes that instead.
const DefaultURLAnswerTimeout = 2 * time.Minute

// SmartRouterMetricsManagerOptions contains configuration for the metrics manager.
//
// NetworkAddress selects the behaviour:
//   - "disabled"      → no manager is created (returns nil)
//   - "" (empty)      → manager is created and collectors are registered, but no
//     HTTP server is started (register-only; used by tests and
//     embedded callers)
//   - "host:port"     → manager is created and the metrics HTTP server listens there
type SmartRouterMetricsManagerOptions struct {
	NetworkAddress     string
	OptimizerQoSClient *ConsumerOptimizerQoSClient
}

// NewSmartRouterMetricsManager creates a new SmartRouterMetricsManager instance
func NewSmartRouterMetricsManager(options SmartRouterMetricsManagerOptions) *SmartRouterMetricsManager {
	if options.NetworkAddress == DisabledFlagOption {
		utils.LavaFormatWarning("prometheus endpoint inactive, option is disabled", nil)
		return nil
	}

	// Every collector below is registered as it is built, into the manager's own field; on
	// duplicate registration registerOrReuse hands back the already-registered collector, so
	// the fields always point at what Prometheus actually exposes.
	m := &SmartRouterMetricsManager{
		batchSignatures: newBatchSignatureRegistry(),
		defaultMethods:  newDefaultMethodRegistry(),
		customers:       newCustomerRegistry(),
		// Start fail-closed (0 = not-ready) so /readyz reports 503 until the
		// RelaysMonitorAggregator's first health check confirms at least one
		// chain can serve relays and flips this to 1 via UpdateHealthCheckStatus.
		// This keeps a freshly-booted pod out of the k8s Service load-balancer
		// until it has actually verified a provider.
		endpointsHealthChecksOk: 0,
		urlToProviderNames:      make(map[string][]string),
		optimizerQoSClient:      options.OptimizerQoSClient,
	}

	// =========================================================================
	// Endpoint-scoped metrics (rpc_endpoint_*)
	// labels: spec, apiInterface, endpoint_id, function
	// Aggregate with sum by (...) to drop labels you don't need.
	// =========================================================================

	endpointFunctionLabels := []string{"spec", "apiInterface", "endpoint_id", "function"}
	endpointLabels := []string{"spec", "apiInterface", "endpoint_id"}

	m.endpointTotalRelaysServiced = NewMappedLabelsCounterVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_total_relays_serviced",
		Help:       "Total relays successfully serviced by this RPC endpoint, by function.",
		Labels:     endpointFunctionLabels,
		Registerer: prometheus.DefaultRegisterer,
	})

	m.endpointTotalErrored = NewMappedLabelsCounterVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_total_errored",
		Help:       "Total errored relays for this RPC endpoint, by function.",
		Labels:     endpointFunctionLabels,
		Registerer: prometheus.DefaultRegisterer,
	})

	m.endpointTotalCancelled = NewMappedLabelsCounterVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_total_cancelled",
		Help:       "Relays cancelled by the router before completion, by function — relay-race losers on stateful broadcasts, and client disconnects. NOT an endpoint fault: these are deliberately excluded from rpc_endpoint_total_errored and from QoS/availability scoring. A high rate here is normal on write-heavy traffic (one winner, N-1 cancelled) and is the signal for tuning broadcast fan-out.",
		Labels:     endpointFunctionLabels,
		Registerer: prometheus.DefaultRegisterer,
	})

	m.endpointInFlight = NewMappedLabelsGaugeVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_requests_in_flight",
		Help:       "Current number of in-flight relays for this RPC endpoint, by function.",
		Labels:     endpointFunctionLabels,
		Registerer: prometheus.DefaultRegisterer,
	})

	m.endpointOverallHealth = NewMappedLabelsGaugeVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_overall_health",
		Help:       "Health status of this RPC endpoint (1=healthy, 0=unhealthy).",
		Labels:     endpointLabels,
		Registerer: prometheus.DefaultRegisterer,
	})

	m.endpointOverallHealthBreakdown = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "rpc_endpoint_overall_health_breakdown",
		Help: "Health check status per chain for RPC endpoints (1=healthy, 0=unhealthy).",
	}, []string{"spec", "apiInterface"}))

	m.endpointSelectionScore = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "rpc_endpoint_selection_score",
		Help: "Latest selection score for each RPC endpoint by score type (availability/latency/sync/stake/composite).",
	}, []string{"spec", "apiInterface", "endpoint_id", "score_type"}))

	m.optimizerSelectionScore = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "rpc_optimizer_selection_score",
		Help: "Periodic optimizer selection score per provider by score type (availability/latency/sync/stake/composite). Chain-level, not scoped to apiInterface.",
	}, []string{"spec", "endpoint_id", "score_type"}))

	m.endpointLatestBlock = NewMappedLabelsGaugeVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_latest_block",
		Help:       "Latest block known by this RPC endpoint.",
		Labels:     endpointLabels,
		Registerer: prometheus.DefaultRegisterer,
	})

	// One series per node url of a provider, for the stuck-provider alerts (MAG-4204). Every url
	// of a provider writes the provider series above, so that series cannot show a url that
	// froze while another url of the provider moves on.
	urlLabels := []string{"spec", "apiInterface", "endpoint_id", "url", "internal_path"}
	m.endpointURLLatestBlock = NewMappedLabelsGaugeVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_url_latest_block",
		Help:       "Latest block one node url of a provider answered with. url is a fingerprint of the node url: the first 8 hex digits of its sha256.",
		Labels:     urlLabels,
		Registerer: prometheus.DefaultRegisterer,
	})
	m.endpointURLAnsweredUntil = NewMappedLabelsGaugeVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_url_answered_until_seconds",
		Help:       "Unix time until which a node url counts as answering: its last answer, a poll or a relay, plus max(2m, 6 x the interval its tracker polls at). A url past it has stopped answering: it is down, not stuck.",
		Labels:     urlLabels,
		Registerer: prometheus.DefaultRegisterer,
	})
	m.endpointURLRelaysServiced = NewMappedLabelsCounterVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_url_relays_serviced_total",
		Help:       "Relays one node url of a provider served successfully.",
		Labels:     urlLabels,
		Registerer: prometheus.DefaultRegisterer,
	})

	m.endpointFetchLatestFails = NewMappedLabelsCounterVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_fetch_latest_fails",
		Help:       "Total failed latest-block fetch operations for this RPC endpoint.",
		Labels:     endpointLabels,
		Registerer: prometheus.DefaultRegisterer,
	})

	m.endpointFetchBlockFails = NewMappedLabelsCounterVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_fetch_block_fails",
		Help:       "Total failed specific-block fetch operations for this RPC endpoint.",
		Labels:     endpointLabels,
		Registerer: prometheus.DefaultRegisterer,
	})

	m.endpointFetchLatestSuccess = NewMappedLabelsCounterVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_fetch_latest_success",
		Help:       "Total successful latest-block fetch operations for this RPC endpoint.",
		Labels:     endpointLabels,
		Registerer: prometheus.DefaultRegisterer,
	})

	m.endpointFetchBlockSuccess = NewMappedLabelsCounterVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_fetch_block_success",
		Help:       "Total successful specific-block fetch operations for this RPC endpoint.",
		Labels:     endpointLabels,
		Registerer: prometheus.DefaultRegisterer,
	})

	// Counts the actual upstream requests the per-endpoint ChainTracker sends, split by what
	// the request was for. This is the ONLY metric that tracks tracker REQUEST VOLUME:
	// rpc_endpoint_fetch_latest_{success,fails} count EVENTS (a new block was detected / the
	// latest-block fetch failed), so neither moves when the number of requests changes — which
	// is why the tracker's block-hash traffic was invisible until it was measured by hand.
	//
	// kind is bounded to trackerRequestKind* below. Watch kind="block_hash" go to zero when
	// fork detection is off (--enable-fork-detection), and kind="latest_block" stay flat.
	m.endpointTrackerRequests = NewMappedLabelsCounterVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_tracker_requests_total",
		Help:       "Total upstream requests sent by the per-endpoint chain tracker, by kind (latest_block, block_hash).",
		Labels:     []string{"spec", "apiInterface", "endpoint_id", "kind"},
		Registerer: prometheus.DefaultRegisterer,
	})

	// Counts the poll cycles the tracker's traffic gate suppressed, split by what made the
	// poll redundant: source="relay" (served traffic kept the tip fresh), source="peer"
	// (another pod's poll, borrowed through the cache backend — the fleet gate, MAG-2981), or
	// source="idle" (the chain served no client relay for the idle window, so nobody needs
	// the poll's answer — idle mode).
	// Together with rpc_endpoint_tracker_requests_total this shows both halves of the
	// tracker's cadence: requests sent, and ticks that sent nothing and why.
	m.endpointTrackerGateSkips = NewMappedLabelsCounterVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_tracker_gate_skips_total",
		Help:       "Total per-endpoint tracker poll cycles suppressed by the traffic gate, by source (relay, peer, idle).",
		Labels:     []string{"spec", "apiInterface", "endpoint_id", "source"},
		Registerer: prometheus.DefaultRegisterer,
	})

	// Counts FAILED calls to the fleet observation store, by operation. Without it a broken
	// cache backend and a fleet with nothing to share are indistinguishable — both leave
	// rpc_endpoint_tracker_gate_skips_total{source="peer"} at zero.
	m.endpointTrackerGateErrors = NewMappedLabelsCounterVec(MappedLabelsMetricOpts{
		Name:       "rpc_endpoint_tracker_gate_errors_total",
		Help:       "Total failed fleet-observation-store calls made by the per-endpoint tracker gate, by op (fetch, publish).",
		Labels:     []string{"spec", "apiInterface", "endpoint_id", "op"},
		Registerer: prometheus.DefaultRegisterer,
	})

	// =========================================================================
	// Endpoint-scoped histogram (labels: spec, apiInterface, endpoint_id, function)
	// =========================================================================

	m.endpointEndToEndLatency = registerOrReuse(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rpc_endpoint_end_to_end_latency_milliseconds",
		Help:    "Distribution of end-to-end relay latency for this RPC endpoint by function in milliseconds. Use histogram_quantile() for percentiles.",
		Buckets: latencyBuckets,
	}, endpointFunctionLabels))

	// =========================================================================
	// Router-scoped metrics (smartrouter_*)
	// labels: spec, apiInterface, function  — aggregate with sum by to drop function
	// =========================================================================

	routerFunctionLabels := []string{"spec", "apiInterface", "function"}

	m.routerTotalRelaysServiced = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_total_relays_serviced",
		Help: "Total relays successfully serviced by the smart router, by function.",
	}, routerFunctionLabels))

	m.routerTotalErrored = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_total_errored",
		Help: "Total errored relays on the smart router, by function.",
	}, routerFunctionLabels))

	m.routerEndToEndLatency = registerOrReuse(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "smartrouter_end_to_end_latency_milliseconds",
		Help:    "Distribution of end-to-end relay latency across all endpoints on the smart router by function in milliseconds. Use histogram_quantile() for percentiles.",
		Buckets: latencyBuckets,
	}, routerFunctionLabels))

	m.routerOverallHealth = registerOrReuse(prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "smartrouter_overall_health",
		Help: "Overall health of the RPC smart router (1=healthy, 0=unhealthy).",
	}))
	// 0, not 1. This gauge is the reporting half of the same state /readyz
	// serves, and endpointsHealthChecksOk above starts fail-closed at 0 — so
	// setting it to 1 here made the two disagree from birth. Nothing has
	// verified a provider at construction time, and the first health check is a
	// full RelaysMonitorAggregator interval away, so a 1 here is a claim the
	// process cannot support: every router reported healthy for its first
	// minutes, and a router that never became healthy went on reporting it.
	m.routerOverallHealth.Set(0)

	m.routerOverallHealthBreakdown = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_overall_health_breakdown",
		Help: "Health check status per chain on the smart router (1=healthy, 0=unhealthy).",
	}, []string{"spec", "apiInterface"}))

	m.routerLatestBlock = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_latest_block",
		Help: "Latest block known by the RPC smart router for this chain/interface.",
	}, []string{"spec", "apiInterface"}))

	// Counts the chain tip moving DOWN (MAG-4153). smartrouter_latest_block is a per-pod gauge that
	// is also written on every relay, so a drop in it cannot be told apart from a pod restart; this
	// counter is the alertable signal. reason is chainstate.RegressionReason* — a closed set of two.
	m.routerLatestBlockRegressions = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_latest_block_regressions_total",
		Help: "Times the router's latest block for this chain/interface moved backwards, by reason (consensus, stale_tip).",
	}, []string{"spec", "apiInterface", "reason"}))

	// Idle mode per chain/interface: 1 while the chain has served no client relay for the idle
	// window and its dedicated polls are paused (or on the idle heartbeat), 0 while active. Left
	// unset when idle mode is off, so a missing series means "not configured", not "active".
	m.routerChainIdle = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_chain_idle",
		Help: "Idle mode state for this chain/interface (1=idle: no client relay within --chain-tracker-idle-after, tracker polls paused; 0=active).",
	}, []string{"spec", "apiInterface"}))

	m.routerProtocolVersion = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_protocol_version",
		Help: "Protocol version of the RPC smart router encoded as major*1e6+minor*1e3+patch.",
	}, []string{"version"}))

	m.routerWsConnectionsActive = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_ws_connections_active",
		Help: "Number of currently active WebSocket connections served by the smart router.",
	}, []string{"spec", "apiInterface"}))

	m.routerWsSubscriptionsTotal = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_ws_subscriptions_total",
		Help: "Total WebSocket subscription requests received by the smart router.",
	}, []string{"spec", "apiInterface"}))

	m.routerWsSubscriptionErrors = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_ws_subscription_errors_total",
		Help: "Total failed WebSocket subscription requests on the smart router.",
	}, []string{"spec", "apiInterface"}))

	m.subscriptionClientsDropped = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_subscription_clients_dropped_total",
		Help: "Clients dropped from a shared subscription (WebSocket or gRPC) because they did not keep up with its pushes.",
	}, []string{"spec", "apiInterface"}))

	crossValidationLabels := []string{"spec", "apiInterface", "method"}
	crossValidationProviderLabels := []string{"spec", "apiInterface", "method", "provider_address"}
	m.crossValidationRequestsTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_cross_validation_requests_total",
		Help: "Total number of cross-validated requests.",
	}, crossValidationLabels))
	m.crossValidationSuccessTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_cross_validation_success_total",
		Help: "Total number of cross-validated requests that reached consensus.",
	}, crossValidationLabels))
	m.crossValidationFailedTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_cross_validation_failed_total",
		Help: "Total number of cross-validated requests that failed to reach consensus.",
	}, crossValidationLabels))
	m.crossValidationProviderAgreementsTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_cross_validation_provider_agreements_total",
		Help: "Total number of times a provider agreed with the cross-validation consensus.",
	}, crossValidationProviderLabels))
	m.crossValidationProviderDisagreementsTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_cross_validation_provider_disagreements_total",
		Help: "Total number of times a provider's response disagreed with the cross-validation consensus.",
	}, crossValidationProviderLabels))
	// Bounded alerting surface: keyed by group (operator-defined, low cardinality) and finality
	// (finalized/not_finalized/unknown) instead of provider address. Post-finality divergence is the
	// high-signal alert. Records only SUCCESSFUL content outliers after a quorum was reached, on
	// deterministic methods — NOT quorum failures or node/protocol errors (see
	// SetCrossValidationMismatchMetric).
	m.crossValidationMismatchTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_cross_validation_mismatch_total",
		Help: "Successful cross-validation content outliers (a successful response that disagreed with a reached consensus on a deterministic method), by provider group and finality. Excludes quorum failures and node/protocol errors.",
	}, []string{"spec", "apiInterface", "method", "group", "finality"}))
	// Bounded failure breakdown: the existing cross_validation_failed_total is unlabeled-by-reason, so a
	// dashboard cannot tell a structural capacity failure (fall back) from a quorum disagreement (retry may
	// help). This series adds the reason, drawn from the closed CrossValidationReason* enum (low cardinality),
	// without touching the existing series' label sets. Covers both quorum-time and request-time fail-fast.
	m.crossValidationFailuresTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_cross_validation_failures_total",
		Help: "Cross-validation failures by reason (CrossValidationReason* enum: insufficient-responses, no-agreement, diversity-unmet, group-quorum-unmet, insufficient-capacity, insufficient-groups). Bounded — the reason set is a closed enum.",
	}, []string{"spec", "apiInterface", "method", "reason"}))
	// Straggler outcomes (MAG-2187): providers that were still in flight at the quorum early-exit reply
	// (surfaced in the pending-providers header) and how their late response resolved against the reached
	// consensus. Bounded — outcome is the closed CrossValidationStragglerOutcome* enum. A late confirmed
	// content dissent additionally feeds crossValidationMismatchTotalMetric (the alerting surface).
	m.crossValidationStragglerTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_cross_validation_straggler_total",
		Help: "Cross-validation straggler resolutions (responses that arrived after the quorum early-exit reply), by outcome (agreed, disagreed, node-error, protocol-error, not-received). Bounded — the outcome set is a closed enum.",
	}, []string{"spec", "apiInterface", "method", "outcome"}))

	// =========================================================================
	// Incident group metrics
	// =========================================================================
	incidentProviderLabels := []string{"spec", "apiInterface", "provider_address", "method"}
	incidentMethodLabels := []string{"spec", "apiInterface", "method"}

	m.incidentNodeErrorsTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_node_errors_total",
		Help: "Total node errors received from RPC endpoints by the smart router.",
	}, incidentProviderLabels))
	m.incidentProtocolErrorsTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_protocol_errors_total",
		Help: "Total protocol errors (transport/timeout) encountered by the smart router.",
	}, incidentProviderLabels))
	m.incidentRetriesTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_retries_total",
		Help: "Total relay retries attempted by the smart router (extra attempts beyond the first).",
	}, incidentMethodLabels))
	m.incidentRetriesSuccessMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_retries_success_total",
		Help: "Retried relay requests on the smart router that ultimately succeeded.",
	}, incidentMethodLabels))
	m.incidentRetriesFailedMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_retries_failed_total",
		Help: "Retried relay requests on the smart router that ultimately failed.",
	}, incidentMethodLabels))
	m.incidentRetryAttemptsHistogram = registerOrReuse(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "smartrouter_retry_attempts",
		Help:    "Distribution of how many provider attempts were needed per retried request.",
		Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}, incidentMethodLabels))
	m.incidentConsistencyTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_consistency_total",
		Help: "Total relay requests on the smart router that enforced a minimum seen block (consistency).",
	}, incidentMethodLabels))
	m.incidentConsistencySuccessMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_consistency_success_total",
		Help: "Consistency-enforced relay requests on the smart router that succeeded.",
	}, incidentMethodLabels))
	m.incidentConsistencyFailedMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_consistency_failed_total",
		Help: "Consistency-enforced relay requests on the smart router that failed.",
	}, incidentMethodLabels))
	m.incidentHedgeTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_hedge_total",
		Help: "Total relay requests on the smart router that triggered a hedge (batch ticker).",
	}, incidentMethodLabels))
	m.incidentHedgeSuccessMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_hedge_success_total",
		Help: "Hedged relay requests on the smart router that succeeded.",
	}, incidentMethodLabels))
	m.incidentHedgeFailedMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_hedge_failed_total",
		Help: "Hedged relay requests on the smart router that failed.",
	}, incidentMethodLabels))
	m.incidentHedgeAttemptsHistogram = registerOrReuse(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "smartrouter_hedge_attempts",
		Help:    "Distribution of how many hedge relays were sent per hedged request.",
		Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}, incidentMethodLabels))

	// =========================================================================
	// CSM state-store size gauges (MAG-1762)
	// One gauge per black-box state store that /debug/reset-all promises to
	// clear. Tests scrape these via /metrics to verify the post-condition
	// without firing a probe relay (which could repopulate the same stores).
	// =========================================================================
	csmStateLabels := []string{"spec", "apiInterface"}
	m.csmBlockedProvidersCount = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_csm_blocked_providers",
		Help: "Number of providers currently blocked and receiving no traffic (ConsumerSessionManager.currentlyBlockedProviderAddresses). Non-zero means the chain is degraded. Goes to 0 after /debug/reset-all.",
	}, csmStateLabels))
	m.csmPrevEpochBlockedProviders = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_csm_previous_epoch_blocked_providers",
		Help: "Size of ConsumerSessionManager.previousEpochBlockedProviders (cross-epoch known-bad provider memory, briefly populated at an epoch boundary and cleared once the re-block pass runs). Goes to 0 after /debug/reset-all. This is what smartrouter_csm_blocked_providers reported before the metric was corrected.",
	}, csmStateLabels))
	m.csmProviderBlocked = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_csm_provider_blocked",
		Help: "Whether this specific provider is currently blocked (1=blocked, 0=serving). Companion to the smartrouter_csm_blocked_providers count, for identifying WHICH provider went out. Republished in full on every state-size tick, so it converges even when the blocked list is drained wholesale.",
	}, []string{"spec", "apiInterface", "provider_address"}))
	m.csmBlockedProvidersByReason = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_csm_blocked_providers_by_reason",
		Help: "How many providers are currently blocked, split by WHY (MAG-2599): all-endpoints-disabled, explicit-block-signal, blocked-in-previous-epoch, unspecified. Counts both the regular and the backup pool, so it sums to smartrouter_csm_blocked_providers + smartrouter_csm_blocked_backup_providers. Those two count memberships and this one counts providers, so a provider blocked in both pools is counted twice there and once here. Deliberately carries no provider label — every reason is republished on every state-size tick, including the zeros, which is what keeps the series self-correcting; a provider label would leave the previous reason's series stuck at 1 after a provider is re-blocked for a different reason. Use smartrouter_csm_provider_blocked or /debug/provider-routing for WHICH provider.",
	}, []string{"spec", "apiInterface", "reason"}))
	m.csmBlockedBackupProvidersCount = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_csm_blocked_backup_providers",
		Help: "Size of ConsumerSessionManager.blockedBackupProviders (per-epoch backup-provider failure memory). Goes to 0 after /debug/reset-all.",
	}, csmStateLabels))
	m.csmStickySessionsCount = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_csm_sticky_sessions",
		Help: "Number of live sticky-session affinities tracked by the smart router. Goes to 0 after /debug/reset-all.",
	}, csmStateLabels))
	m.csmStickyClaims = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_csm_sticky_claims_total",
		Help: "Cross-pod sticky-session claim resolutions by outcome. " +
			"local_hit: answered from this pod's confirmed table, no round trip. " +
			"adopted: routed by a live claim read from the registry rather than one held in memory — normally another pod's, the signal that cross-pod stickiness is doing its job, and zero here on a multi-replica fleet means it is wired but never firing; a pod also reads back its own claim after dropping its local copy (see invalidated). " +
			"claimed: this pod's write created the claim. " +
			"lost_race: a peer's claim was already live and named a DIFFERENT upstream, so this pod adopted it. " +
			"(A peer claim naming the same upstream this pod would have picked is indistinguishable from winning, and counts as claimed.) " +
			"no_candidate: this pod had no upstream to offer, which is a pairing problem rather than a registry one. " +
			"invalidated: a local claim was dropped because its upstream could not serve here; a climbing series means claims outlive the upstreams they name. " +
			"error: the claim could not be established, and the request was failed rather than served off an unverified pin.",
	}, []string{"spec", "apiInterface", "outcome"}))
	m.csmReportedProvidersCount = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_csm_reported_providers",
		Help: "Number of providers currently in the ReportedProviders unresponsiveness register. Goes to 0 after /debug/reset-all.",
	}, csmStateLabels))
	m.endpointServingTier = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_endpoint_serving_tier",
		Help: "Which provider tier an endpoint is serving from: 2=primaries, 1=DEGRADED (backups only), 0=DARK (no healthy providers). Since MAG-2525 a dark chain no longer crash-loops, so this gauge — not pod restarts — is the signal that a chain cannot serve. Expected time at 0 depends on how the chain went dark: dark at boot is retried from ~2s (doubling to a 3m ceiling), but a chain demoted to dark after booting healthy is only re-checked by the 15m epoch re-verifier. Size alert windows for the 15m case.",
	}, csmStateLabels))
	m.providerVerificationFailed = registerOrReuse(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartrouter_provider_verification_failed",
		Help: "While a configured provider is failing a verification: the unix time in seconds the failure started, labelled with the check it fails. A provider that moves to another check in the same scope and service keeps its start. scope=provider: the provider is out of the pairing (failed at boot, or demoted by the epoch re-verifier). scope=service: the provider still serves but lost one add-on or extension (service) to that check. scope=url: the provider still serves but one of its node urls was refused, whole (no service) or for one service another url still serves. severity: Fail when the failure takes a url or the provider out, PartialFail when it switches off only that service. The series exists only while the failure lasts. check is the spec verification's name, or latest-block, unsupported-addon, connect, config, not-run, rate-limited, unknown. A provider out at boot is retried every 3m (from 2s, doubling, while its chain is dark); one demoted later and every service refusal is re-checked every 15m epoch, so size alert windows above both.",
	}, verificationFailureLabels))
	m.verificationCheckFailures = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_provider_verification_check_failures_total",
		Help: "Failed verification check runs, one per run that failed. Labels as smartrouter_provider_verification_failed: scope=provider when the failure costs the whole provider, scope=service when it costs one add-on or extension (service), scope=url when it costs one node url. A provider out at boot is re-run every 3m; an admitted provider and every service are re-run every 15m epoch, so each failure appears at that cadence. A rate-limited run is inconclusive and not counted.",
	}, verificationFailureLabels))

	// =========================================================================
	// Request group metrics
	// =========================================================================
	routerRequestLabels := []string{"spec", "apiInterface", "provider_address", "method"}

	m.routerRequestsTotal = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_requests_total",
		Help: "Total number of requests on the smart router.",
	}, routerRequestLabels))
	m.routerRequestsSuccess = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_requests_success_total",
		Help: "Total number of successful requests on the smart router.",
	}, routerRequestLabels))
	m.routerRequestsFailed = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_requests_failed_total",
		Help: "Total number of failed requests on the smart router.",
	}, routerRequestLabels))
	m.routerRequestsRead = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_requests_read_total",
		Help: "Total number of read (stateful=0) requests on the smart router.",
	}, routerRequestLabels))
	m.routerRequestsWrite = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_requests_write_total",
		Help: "Total number of write (stateful=1) requests on the smart router.",
	}, routerRequestLabels))
	m.routerRequestsDebugTrace = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_requests_debug_trace_total",
		Help: "Total number of debug/trace addon requests on the smart router.",
	}, routerRequestLabels))
	m.routerRequestsArchive = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_requests_archive_total",
		Help: "Total number of archive requests on the smart router.",
	}, routerRequestLabels))
	m.routerRequestsBatch = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_requests_batch_total",
		Help: "Total number of batch requests on the smart router.",
	}, routerRequestLabels))
	m.extensionUnavailableTotal = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_extension_unavailable_total",
		Help: "Requests served without an extension the caller asked for with lava-extension, because no node on this router offers it, per extension. Counted once per request per such extension, on the same replies that carry the Lava-Extension-Unavailable response header. An extension the router adds on its own (archive for a deep eth_call) is never counted. Non-zero means callers depend on an extension this deployment has no node for.",
	}, []string{"spec", "apiInterface", "extension"}))

	m.batchSize = registerOrReuse(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "smartrouter_batch_size",
		Help: "Number of sub-requests per batch request. This carries the magnitude that the collapsed method label intentionally drops. Single-element batches are indistinguishable from single requests at this layer and are not observed.",
		// Batch sizes are small-and-clustered with a long tail; buckets start at 2
		// because a 1-element batch never reaches this histogram.
		Buckets: []float64{2, 3, 5, 10, 25, 50, 100, 250, 500},
	}, []string{"spec", "apiInterface"}))
	m.batchSignatureOverflow = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_batch_signature_overflow_total",
		Help: `Label normalizations that fell into method="batch:other". reason="cap": the spec exhausted its distinct-signature budget. reason="wide": one batch spanned more distinct methods than a signature may name. Non-zero means per-batch-type breakdowns are lossy — raise the cap or accept the aggregation.`,
	}, []string{"spec", "reason"}))
	m.defaultMethodOverflow = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_default_method_overflow_total",
		Help: `Label normalizations that fell into method="Default-other": the spec exhausted its distinct unmatched-API name budget. Sustained growth means clients are hitting URLs the spec does not match — concrete IDs in the path mint a new name per request — which usually indicates a spec gap (e.g. a path variant the templates miss).`,
	}, []string{"spec"}))

	// Per-customer request group (MAG-4081). Request-level, unlike smartrouter_requests_*,
	// which count every attempt (retries, hedges, cross-validation) and the router's own
	// tracker traffic. The latency histogram leaves method out: it is the family whose
	// series multiply, and per-customer latency is asked per chain.
	customerRequestLabels := []string{"spec", "apiInterface", "customer", "method"}
	m.customerRequestsTotal = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_customer_requests_total",
		Help: `Client requests, once each with their final outcome, by the caller's project-id header. customer="DefaultDappID": no header. customer="other": past the per-spec customer cap (see smartrouter_customer_overflow_total). Subscription pushes are not counted; the subscribe request is.`,
	}, customerRequestLabels))
	m.customerRequestsFailed = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_customer_requests_failed_total",
		Help: "Client requests that ended in an error to the client, by the caller's project-id header. Success rate = 1 - failed / smartrouter_customer_requests_total.",
	}, customerRequestLabels))
	m.customerEndToEndLatency = registerOrReuse(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "smartrouter_customer_end_to_end_latency_milliseconds",
		Help:    "End-to-end latency of successful client requests in milliseconds, by the caller's project-id header. Use histogram_quantile() for percentiles.",
		Buckets: latencyBuckets,
	}, []string{"spec", "apiInterface", "customer"}))
	m.customerOverflow = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_customer_overflow_total",
		Help: `Label normalizations that fell into customer="other": the spec exhausted its distinct-customer budget, or the project-id value was too long to be an id. Non-zero means per-customer numbers are lossy for this spec.`,
	}, []string{"spec"}))

	cacheLabels := []string{"spec", "apiInterface", "method", "cache_tier"}
	m.cacheRequestsTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_cache_requests_total",
		Help: "Total number of cache lookup attempts, per cache tier (primary|secondary). Skipped/inactive tiers are not counted.",
	}, cacheLabels))
	m.cacheSuccessTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_cache_success_total",
		Help: "Total number of cache lookups that returned a cached response (hits), per cache tier.",
	}, cacheLabels))
	m.cacheFailedTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_cache_failed_total",
		Help: "Total number of cache lookups that did not produce a cached response, per cache tier, split by outcome (miss = clean not-found, error = transport/server error, timeout = lookup budget exceeded).",
	}, append(append([]string{}, cacheLabels...), "outcome")))
	m.cacheLatencyHistogram = registerOrReuse(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "smartrouter_cache_latency_milliseconds",
		Help:    "Distribution of cache lookup latency in milliseconds, per cache tier, observed on every attempted lookup (hits and non-hits).",
		Buckets: latencyBuckets,
	}, cacheLabels))
	cacheWriteLabels := []string{"spec", "apiInterface", "method"}
	m.cacheWriteSkippedTotalMetric = registerOrReuse(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_cache_write_skipped_total",
		Help: "Replies the router served but did not write to its cache, by reason. reason=size: the reply body exceeded --cache-max-entry-bytes.",
	}, append(append([]string{}, cacheWriteLabels...), "reason")))
	m.cacheEntryBytesHistogram = registerOrReuse(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "smartrouter_cache_entry_bytes",
		Help:    "Body size in bytes of every reply the router hands to its cache backend for writing, before the backend encodes it.",
		Buckets: cacheEntryBytesBuckets,
	}, cacheWriteLabels))

	// Start the HTTP server only when a listen address is configured. An empty
	// address leaves the manager in register-only mode (collectors registered, no
	// socket) — see SmartRouterMetricsManagerOptions.
	if options.NetworkAddress != "" {
		mux := m.metricsMux(prometheus.DefaultRegisterer, prometheus.DefaultGatherer)

		go func() {
			utils.LavaFormatInfo("prometheus endpoint listening", utils.Attribute{Key: "Listen Address", Value: options.NetworkAddress})
			if err := http.ListenAndServe(options.NetworkAddress, mux); err != nil {
				utils.LavaFormatError("metrics endpoint failed", err, utils.Attribute{Key: "Listen Address", Value: options.NetworkAddress})
			}
		}()
	}

	return m
}

// metricsMux is every route the metrics server answers: the page, gathered from the given registry,
// and the health routes.
func (m *SmartRouterMetricsManager) metricsMux(registerer prometheus.Registerer, gatherer prometheus.Gatherer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", newMetricsPageHandler(registerer, gatherer))
	m.registerHTTPHandlers(mux)
	return mux
}

// registerHTTPHandlers wires the health/readiness routes onto the given mux.
// Split out from the constructor so tests can exercise the handlers against a
// real manager without binding a socket or touching the Prometheus default
// registerer.
func (m *SmartRouterMetricsManager) registerHTTPHandlers(mux *http.ServeMux) {
	// Overall-health / readiness: 200 when at least one chain can serve relays
	// (endpointsHealthChecksOk == 1, maintained by the RelaysMonitorAggregator),
	// else 503.
	overallHealthHandler := func(w http.ResponseWriter, r *http.Request) {
		statusCode := http.StatusOK
		message := "Health status OK"
		if atomic.LoadUint64(&m.endpointsHealthChecksOk) == 0 {
			statusCode = http.StatusServiceUnavailable
			message = "Unhealthy"
		}
		w.WriteHeader(statusCode)
		w.Write([]byte(message))
	}

	mux.HandleFunc("/metrics/overall-health", overallHealthHandler)
	mux.HandleFunc("/metrics/health-overall", overallHealthHandler)

	// Kubernetes liveness probe. Deliberately dumb: a static 200 that only
	// proves the process is up and the mux is serving. No dependency checks —
	// a failing liveness probe restarts the pod, and a restart can't fix
	// "no healthy providers", so provider health belongs in /readyz, not here.
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Kubernetes readiness probe. Reflects real serving capacity: 200 only when
	// at least one chain can serve relays, else 503 so k8s pulls the pod out of
	// the Service load-balancer without restarting it. Shares the flag with
	// /metrics/health-overall; this is the k8s-idiomatic alias.
	mux.HandleFunc("/readyz", overallHealthHandler)
}

// =============================================================================
// Setter helpers
// =============================================================================

// The setters below share one shape: name a series, then touch one collector. The helpers carry
// the collector's nil guard, so a partially-built manager (test fixtures build only the
// collectors they assert on) never panics in a setter; the setters keep the manager's own guard.

func incCounter(vec *prometheus.CounterVec, labels ...string) {
	if vec != nil {
		vec.WithLabelValues(labels...).Inc()
	}
}

func setGauge(vec *prometheus.GaugeVec, value float64, labels ...string) {
	if vec != nil {
		vec.WithLabelValues(labels...).Set(value)
	}
}

func observeHistogram(vec *prometheus.HistogramVec, value float64, labels ...string) {
	if vec != nil {
		vec.WithLabelValues(labels...).Observe(value)
	}
}

// incOutcome counts one event on total and on success or failed.
func incOutcome(total, success, failed *prometheus.CounterVec, ok bool, labels ...string) {
	incCounter(total, labels...)
	if ok {
		incCounter(success, labels...)
	} else {
		incCounter(failed, labels...)
	}
}

func incMapped(vec *MappedLabelsCounterVec, labels map[string]string) {
	if vec != nil {
		vec.WithLabelValues(labels).Inc()
	}
}

func addMapped(vec *MappedLabelsGaugeVec, delta float64, labels map[string]string) {
	if vec != nil {
		vec.WithLabelValues(labels).Add(delta)
	}
}

func setMapped(vec *MappedLabelsGaugeVec, value float64, labels map[string]string) {
	if vec != nil {
		vec.WithLabelValues(labels).Set(value)
	}
}

// boolGauge is a gauge's value for a yes/no state: 1 or 0.
func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// endpointSeries names an endpoint-scoped series: {spec, apiInterface, endpoint_id}.
func endpointSeries(spec, apiInterface, endpointID string) map[string]string {
	return map[string]string{"spec": spec, "apiInterface": apiInterface, "endpoint_id": endpointID}
}

// endpointFunctionSeries names an endpoint-scoped series by function, normalizing the function
// label: {spec, apiInterface, endpoint_id, function}.
func (m *SmartRouterMetricsManager) endpointFunctionSeries(spec, apiInterface, endpointID, function string) map[string]string {
	function = m.normalizeMethodLabel(spec, function)
	return map[string]string{"spec": spec, "apiInterface": apiInterface, "endpoint_id": endpointID, "function": function}
}

// incForEveryProvider counts one event on vec for every provider configured with endpointID, on
// the {spec, apiInterface, endpoint_id, key} series.
func (m *SmartRouterMetricsManager) incForEveryProvider(vec *MappedLabelsCounterVec, spec, apiInterface, endpointID, key, value string) {
	if vec == nil {
		return
	}
	for _, providerName := range m.resolveProviderNames(endpointID) {
		incMapped(vec, map[string]string{"spec": spec, "apiInterface": apiInterface, "endpoint_id": providerName, key: value})
	}
}

// =============================================================================
// Endpoint-scoped metric setters
// =============================================================================

// SetEndpointOverallHealth sets the health status for an RPC endpoint
func (m *SmartRouterMetricsManager) SetEndpointOverallHealth(spec, apiInterface, endpointID string, healthy bool) {
	if m == nil {
		return
	}
	setMapped(m.endpointOverallHealth, boolGauge(healthy), endpointSeries(spec, apiInterface, endpointID))
}

// SetEndpointOverallHealthBreakdown sets the aggregate health for a spec/apiInterface across endpoints
func (m *SmartRouterMetricsManager) SetEndpointOverallHealthBreakdown(spec, apiInterface string, healthy bool) {
	if m == nil {
		return
	}
	setGauge(m.endpointOverallHealthBreakdown, boolGauge(healthy), spec, apiInterface)
}

// SetEndpointLatestBlock sets the latest block known by an RPC endpoint.
// When endpointID is a URL shared by multiple providers (typical for backup-primary pairs
// with identical node-urls), emits the metric once per provider so every dashboard row
// sees the update — not just the last-registered one.
func (m *SmartRouterMetricsManager) SetEndpointLatestBlock(spec, apiInterface, endpointID string, block int64) {
	if m == nil {
		return
	}
	for _, providerName := range m.resolveProviderNames(endpointID) {
		setMapped(m.endpointLatestBlock, float64(block), endpointSeries(spec, apiInterface, providerName))
	}
}

// URLFingerprint is the url label of the per-url series: the first 8 hex digits of the node url's
// sha256, and nothing of the url itself. A node url can hold an api key in any part of it, so no
// part of it is exported. /debug/endpoint-state shows it beside each endpoint, on a router run
// with its debug server.
func URLFingerprint(rawURL string) string {
	return utils.Digest(rawURL, 4)
}

// RegisterEndpointInternalPath records the spec path a registered url serves ("/v2", or "" for
// the root), for the internal_path label of the per-url series. TON's /v2 and /v3 urls share a
// host, so the fingerprint alone does not tell an operator which of them an alert is about.
func (m *SmartRouterMetricsManager) RegisterEndpointInternalPath(endpointID, internalPath string) {
	if m == nil {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.urlInternalPaths == nil {
		m.urlInternalPaths = make(map[string]string)
	}
	m.urlInternalPaths[endpointID] = internalPath
}

// SetEndpointURLLatestBlock records the head one url answered with on rpc_endpoint_url_latest_block,
// for every provider configured with the url, and moves the url's answered-until time to
// answerTimeout from now (DefaultURLAnswerTimeout if answerTimeout is not positive). The server
// passes max(DefaultURLAnswerTimeout, 6 x its trackers' poll interval): up to 4 polls the traffic
// gate skips, the poll after them, and one interval for that poll's answer. So a url that answers
// every poll never reads as down between two answers, as long as the tip store accepts what it
// answers: a poll answering below a still-fresh higher tip (a peer pod's, or a relay's read at a
// fresher commitment) moves nothing. Every answered poll the tip store accepts calls it, a
// repeat of the same block included, so a url that keeps answering with one block stands still on
// its series while its answered-until time keeps moving: that is stuck. A url that stops answering
// stands still on both: that is down.
//
// Only a registered url gets a series: an unregistered one would name itself as its provider, and
// a url, even redacted, is not safe to export.
func (m *SmartRouterMetricsManager) SetEndpointURLLatestBlock(spec, apiInterface, endpointURL string, block int64, answerTimeout time.Duration) {
	if m == nil || m.endpointURLLatestBlock == nil || block <= 0 {
		return
	}
	answeredUntil := answeredUntilFrom(time.Now(), answerTimeout)
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, providerName := range m.urlToProviderNames[endpointURL] {
		m.setURLLatestBlockLocked(spec, apiInterface, providerName, endpointURL, block, answeredUntil)
	}
}

// SetProviderURLLatestBlock is SetEndpointURLLatestBlock for a caller that knows which provider the
// url belongs to: the relay-harvest path, which reports the provider it relayed to. Only that
// provider's series moves.
func (m *SmartRouterMetricsManager) SetProviderURLLatestBlock(spec, apiInterface, providerName, endpointURL string, block int64, answerTimeout time.Duration) {
	if m == nil || m.endpointURLLatestBlock == nil || block <= 0 {
		return
	}
	answeredUntil := answeredUntilFrom(time.Now(), answerTimeout)
	m.lock.RLock()
	defer m.lock.RUnlock()
	if _, registered := m.urlToProviderNames[endpointURL]; !registered {
		return
	}
	m.setURLLatestBlockLocked(spec, apiInterface, providerName, endpointURL, block, answeredUntil)
}

// answeredUntilFrom is the answered-until time of an answer at now.
func answeredUntilFrom(now time.Time, answerTimeout time.Duration) time.Time {
	if answerTimeout <= 0 {
		answerTimeout = DefaultURLAnswerTimeout
	}
	return now.Add(answerTimeout)
}

// setURLLatestBlockLocked writes one url series and its answered-until time. The caller holds lock,
// for reading.
func (m *SmartRouterMetricsManager) setURLLatestBlockLocked(spec, apiInterface, providerName, endpointURL string, block int64, answeredUntil time.Time) {
	labels := m.urlSeriesLabelsLocked(spec, apiInterface, providerName, endpointURL)
	m.endpointURLLatestBlock.WithLabelValues(labels).Set(float64(block))
	if m.endpointURLAnsweredUntil != nil {
		m.endpointURLAnsweredUntil.WithLabelValues(labels).Set(float64(answeredUntil.Unix()))
	}
}

// AddEndpointURLRelayServiced counts a relay one url of a provider served successfully, so a page
// about that url can ask whether customers' requests reach it, not just its provider.
func (m *SmartRouterMetricsManager) AddEndpointURLRelayServiced(spec, apiInterface, providerName, endpointURL string) {
	if m == nil || m.endpointURLRelaysServiced == nil {
		return
	}
	m.lock.RLock()
	defer m.lock.RUnlock()
	if _, registered := m.urlToProviderNames[endpointURL]; !registered {
		return
	}
	m.endpointURLRelaysServiced.WithLabelValues(m.urlSeriesLabelsLocked(spec, apiInterface, providerName, endpointURL)).Inc()
}

// ForgetEndpointURLLatestBlock deletes a url's per-url series for every provider configured with
// it. The caller is a removed tracker: a series left behind would stand still forever, which is
// what the stuck-provider alerts fire on.
func (m *SmartRouterMetricsManager) ForgetEndpointURLLatestBlock(spec, apiInterface, endpointURL string) {
	if m == nil || m.endpointURLLatestBlock == nil {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, providerName := range m.urlToProviderNames[endpointURL] {
		labels := prometheus.Labels(m.urlSeriesLabelsLocked(spec, apiInterface, providerName, endpointURL))
		m.endpointURLLatestBlock.GaugeVec.Delete(labels)
		if m.endpointURLAnsweredUntil != nil {
			m.endpointURLAnsweredUntil.GaugeVec.Delete(labels)
		}
		if m.endpointURLRelaysServiced != nil {
			m.endpointURLRelaysServiced.CounterVec.Delete(labels)
		}
	}
}

// urlSeriesLabelsLocked names one url's per-url series. The caller holds lock.
func (m *SmartRouterMetricsManager) urlSeriesLabelsLocked(spec, apiInterface, providerName, endpointURL string) map[string]string {
	fingerprint, ok := m.urlFingerprints[endpointURL]
	if !ok {
		fingerprint = URLFingerprint(endpointURL)
	}
	return map[string]string{
		"spec":          spec,
		"apiInterface":  apiInterface,
		"endpoint_id":   providerName,
		"url":           fingerprint,
		"internal_path": m.urlInternalPaths[endpointURL],
	}
}

// AddEndpointRelayServiced increments the relay counter for an endpoint and function
func (m *SmartRouterMetricsManager) AddEndpointRelayServiced(spec, apiInterface, endpointID, function string) {
	if m == nil {
		return
	}
	incMapped(m.endpointTotalRelaysServiced, m.endpointFunctionSeries(spec, apiInterface, endpointID, function))
}

// AddEndpointRelayCancelled increments the cancelled counter for an endpoint and function.
// Cancelled means WE stopped the relay — a relay-race loser or a client disconnect — so it
// is deliberately not an error (MAG-2648).
func (m *SmartRouterMetricsManager) AddEndpointRelayCancelled(spec, apiInterface, endpointID, function string) {
	if m == nil {
		return
	}
	incMapped(m.endpointTotalCancelled, m.endpointFunctionSeries(spec, apiInterface, endpointID, function))
}

// AddEndpointRelayErrored increments the error counter for an endpoint and function
func (m *SmartRouterMetricsManager) AddEndpointRelayErrored(spec, apiInterface, endpointID, function string) {
	if m == nil {
		return
	}
	incMapped(m.endpointTotalErrored, m.endpointFunctionSeries(spec, apiInterface, endpointID, function))
}

// SetEndpointEndToEndLatency observes a latency sample for an endpoint and function
func (m *SmartRouterMetricsManager) SetEndpointEndToEndLatency(spec, apiInterface, endpointID, function string, latencyMs float64) {
	if m == nil {
		return
	}
	observeHistogram(m.endpointEndToEndLatency, latencyMs, spec, apiInterface, endpointID, m.normalizeMethodLabel(spec, function))
}

// AddEndpointInFlightRequest increments the in-flight counter for an endpoint and function
func (m *SmartRouterMetricsManager) AddEndpointInFlightRequest(spec, apiInterface, endpointID, function string) {
	if m == nil {
		return
	}
	addMapped(m.endpointInFlight, 1, m.endpointFunctionSeries(spec, apiInterface, endpointID, function))
}

// SubEndpointInFlightRequest decrements the in-flight counter for an endpoint and function
func (m *SmartRouterMetricsManager) SubEndpointInFlightRequest(spec, apiInterface, endpointID, function string) {
	if m == nil {
		return
	}
	addMapped(m.endpointInFlight, -1, m.endpointFunctionSeries(spec, apiInterface, endpointID, function))
}

// AddEndpointFetchLatestFail increments the failed latest-block fetch counter
func (m *SmartRouterMetricsManager) AddEndpointFetchLatestFail(spec, apiInterface, endpointID string) {
	if m == nil {
		return
	}
	incMapped(m.endpointFetchLatestFails, endpointSeries(spec, apiInterface, endpointID))
}

// AddEndpointFetchBlockFail increments the failed specific-block fetch counter
func (m *SmartRouterMetricsManager) AddEndpointFetchBlockFail(spec, apiInterface, endpointID string) {
	if m == nil {
		return
	}
	incMapped(m.endpointFetchBlockFails, endpointSeries(spec, apiInterface, endpointID))
}

// AddEndpointFetchLatestSuccess increments the successful latest-block fetch counter
func (m *SmartRouterMetricsManager) AddEndpointFetchLatestSuccess(spec, apiInterface, endpointID string) {
	if m == nil {
		return
	}
	incMapped(m.endpointFetchLatestSuccess, endpointSeries(spec, apiInterface, endpointID))
}

// AddEndpointFetchBlockSuccess increments the successful specific-block fetch counter
func (m *SmartRouterMetricsManager) AddEndpointFetchBlockSuccess(spec, apiInterface, endpointID string) {
	if m == nil {
		return
	}
	incMapped(m.endpointFetchBlockSuccess, endpointSeries(spec, apiInterface, endpointID))
}

// =============================================================================
// Router-scoped metric setters
// =============================================================================

// SetRouterLatestBlock sets the latest block known by the smart router for a chain/interface
func (m *SmartRouterMetricsManager) SetRouterLatestBlock(spec, apiInterface string, block int64) {
	if m == nil {
		return
	}
	setGauge(m.routerLatestBlock, float64(block), spec, apiInterface)
}

// RecordRouterLatestBlockRegression counts ONE downward move of the router's latest block for a
// chain/interface (MAG-4153). reason is a chainstate.RegressionReason* value.
func (m *SmartRouterMetricsManager) RecordRouterLatestBlockRegression(spec, apiInterface, reason string) {
	if m == nil {
		return
	}
	incCounter(m.routerLatestBlockRegressions, spec, apiInterface, reason)
}

// SetChainIdle publishes a chain/interface's idle-mode state: 1 while it has served no client
// relay for the idle window and its tracker polls are paused, 0 once a client relay wakes it.
// Written by the probe loop every cycle (current state, not transitions), never on the relay hot path.
func (m *SmartRouterMetricsManager) SetChainIdle(spec, apiInterface string, idle bool) {
	if m == nil {
		return
	}
	setGauge(m.routerChainIdle, boolGauge(idle), spec, apiInterface)
}

// =============================================================================
// Health check methods
// =============================================================================

// UpdateOverallHealthStatus updates the overall health check status for the smart router
func (m *SmartRouterMetricsManager) UpdateOverallHealthStatus(healthy bool) {
	if m == nil {
		return
	}
	value := uint64(0)
	gaugeValue := 0.0
	if healthy {
		value = 1
		gaugeValue = 1.0
	}
	atomic.StoreUint64(&m.endpointsHealthChecksOk, value)
	m.routerOverallHealth.Set(gaugeValue)
}

// IsHealthy returns true if the smart router is healthy
func (m *SmartRouterMetricsManager) IsHealthy() bool {
	if m == nil {
		return true
	}
	return atomic.LoadUint64(&m.endpointsHealthChecksOk) == 1
}

// =============================================================================
// Endpoint registration and management
// =============================================================================

// RegisterEndpoint registers an endpoint with the metrics manager.
// endpointID is the raw URL (used internally for URL→name resolution via the chain tracker callbacks).
// providerName is used as endpoint_id in all Prometheus metrics.
func (m *SmartRouterMetricsManager) RegisterEndpoint(spec, apiInterface, endpointID, providerName string) {
	if m == nil {
		return
	}

	m.lock.Lock()
	// Append with dedup: multiple providers can share a URL, and the same (URL, provider)
	// pair can be registered more than once (e.g. a provider with duplicate node-urls).
	if existing := m.urlToProviderNames[endpointID]; !slices.Contains(existing, providerName) {
		m.urlToProviderNames[endpointID] = append(existing, providerName)
	}
	if _, ok := m.urlFingerprints[endpointID]; !ok {
		if m.urlFingerprints == nil {
			m.urlFingerprints = make(map[string]string)
		}
		m.urlFingerprints[endpointID] = URLFingerprint(endpointID)
	}
	m.lock.Unlock()

	// Initialize health to healthy so always-healthy endpoints appear in Prometheus from startup.
	// The relay code only calls SetEndpointOverallHealth on state transitions (unhealthy→healthy),
	// so without this initialization, endpoints that never fail would have no health metric at all.
	m.SetEndpointOverallHealth(spec, apiInterface, providerName, true)
}

// resolveProviderNames returns every provider name configured for a given endpoint URL.
// URL-keyed metric emissions must iterate this slice so that every provider on a shared
// URL gets the same metric update. Falls back to `[urlOrName]` when the input is already
// a provider name or is otherwise unknown, preserving backward-compatible single-name
// semantics for callers that pass a provider name directly.
func (m *SmartRouterMetricsManager) resolveProviderNames(urlOrName string) []string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if names, ok := m.urlToProviderNames[urlOrName]; ok && len(names) > 0 {
		return slices.Clone(names)
	}
	// Unregistered input is either a provider name (passed straight through by
	// callers that already resolved it) or a url that never reached
	// RegisterEndpoint. RedactIfURL leaves the name alone — a lava address is not
	// a url — and keeps a node-url's api key out of a scraped, retained label.
	return []string{utils.RedactIfURL(urlOrName)}
}

// resolveProviderName returns the first provider name configured for a given endpoint URL,
// or the input if no mapping exists. Use this only for callers that do not emit per-provider
// metrics (e.g. relay hot path which already passes the provider name directly). For URL-keyed
// metric emissions, use resolveProviderNames and iterate.
func (m *SmartRouterMetricsManager) resolveProviderName(urlOrName string) string {
	names := m.resolveProviderNames(urlOrName)
	return names[0]
}

// =============================================================================
// Convenience methods for recording relay results
// =============================================================================

// recordRelayOutcome counts one completed relay on both relay-counter families: the router's
// smartrouter_total_relays_serviced / smartrouter_total_errored and the endpoint's
// rpc_endpoint_total_* twins, the latter labelled with endpointID as given. A success also
// observes the endpoint's latency.
// Note: router end-to-end latency is NOT recorded here — it is recorded separately
// via RecordRouterEndToEndLatency from SendParsedRelay, which captures the full
// request duration including provider selection overhead, not just the network hop.
func (m *SmartRouterMetricsManager) recordRelayOutcome(spec, apiInterface, endpointID, function string, success bool, latencyMs float64) {
	function = m.normalizeMethodLabel(spec, function)
	if !success {
		incCounter(m.routerTotalErrored, spec, apiInterface, function)
		m.AddEndpointRelayErrored(spec, apiInterface, endpointID, function)
		return
	}
	incCounter(m.routerTotalRelaysServiced, spec, apiInterface, function)
	m.AddEndpointRelayServiced(spec, apiInterface, endpointID, function)
	m.SetEndpointEndToEndLatency(spec, apiInterface, endpointID, function, latencyMs)
}

// RecordRouterEndToEndLatency records the full end-to-end latency for the router,
// measured from when SendParsedRelay started to when the result was returned.
// This captures the true client-visible latency including provider selection overhead.
func (m *SmartRouterMetricsManager) RecordRouterEndToEndLatency(spec, apiInterface, function string, latencyMs float64) {
	if m == nil {
		return
	}
	observeHistogram(m.routerEndToEndLatency, latencyMs, spec, apiInterface, m.normalizeMethodLabel(spec, function))
}

// =============================================================================
// Methods for direct RPC relay tracking
// These are called from rpcsmartrouter_server.go during relay processing
// =============================================================================

// RecordDirectRelayStart records the start of an in-flight request
func (m *SmartRouterMetricsManager) RecordDirectRelayStart(spec, apiInterface, endpointID, function string) {
	if m == nil {
		return
	}
	m.AddEndpointInFlightRequest(spec, apiInterface, endpointID, function)
}

// RelayOutcome is how a direct relay ended. It is three-valued rather than a success
// bool because "cancelled" is neither (MAG-2648): the router aborted the relay itself —
// a relay-race loser on a stateful broadcast, or a client that hung up — so the endpoint
// neither served nor failed it, and must not be scored either way.
type RelayOutcome int

const (
	RelayOutcomeSuccess RelayOutcome = iota
	RelayOutcomeError
	// RelayOutcomeCancelled: we stopped it. Records the in-flight decrement and the
	// cancelled counter, and nothing else.
	RelayOutcomeCancelled
)

// RecordDirectRelayEnd records the end of an in-flight request and updates metrics.
// relay carries the request classification (IsWrite/IsArchive/IsDebugTrace/IsBatch)
// that the call site should populate on the shared RelayMetrics before calling here.
//
// Every outcome — including cancelled — MUST reach the in-flight decrement below, which
// pairs the increment from RecordDirectRelayStart. Returning early for cancelled relays
// would leak rpc_endpoint_requests_in_flight upward forever, one per race loser.
func (m *SmartRouterMetricsManager) RecordDirectRelayEnd(spec, apiInterface, endpointID, function string, latencyMs float64, outcome RelayOutcome, relay *RelayMetrics) {
	if m == nil {
		return
	}
	// Observe the batch's element count BEFORE collapsing the label, and do it only
	// here: this is the single once-per-completed-relay entry point, so the other
	// recorders normalize without observing and a batch is counted exactly once.
	// A cancelled relay never completed, so it is not observed.
	if outcome != RelayOutcomeCancelled {
		m.observeBatchSize(spec, apiInterface, function)
	}
	function = m.normalizeMethodLabel(spec, function)

	m.SubEndpointInFlightRequest(spec, apiInterface, endpointID, function)

	// A cancelled relay stops here: the in-flight gauge is balanced and the cancellation
	// is counted, but no latency is observed (the relay never finished, so the number is
	// meaningless) and none of the router request-group counters move — that keeps
	// requests_total == requests_success + requests_failed a true invariant.
	if outcome == RelayOutcomeCancelled {
		m.AddEndpointRelayCancelled(spec, apiInterface, endpointID, function)
		return
	}

	success := outcome == RelayOutcomeSuccess
	m.recordRelayOutcome(spec, apiInterface, endpointID, function, success, latencyMs)
	m.incRequestGroup(success, relay, spec, apiInterface, m.resolveProviderName(endpointID), function)
}

// incRequestGroup counts one request on the smartrouter_requests_* group: the total, success or
// failed, then the request's class from relay — batch, or read or write plus debug/trace and archive.
func (m *SmartRouterMetricsManager) incRequestGroup(success bool, relay *RelayMetrics, labels ...string) {
	incOutcome(m.routerRequestsTotal, m.routerRequestsSuccess, m.routerRequestsFailed, success, labels...)
	if relay != nil && relay.IsBatch {
		incCounter(m.routerRequestsBatch, labels...)
		return
	}
	if relay != nil && relay.IsWrite {
		incCounter(m.routerRequestsWrite, labels...)
	} else {
		incCounter(m.routerRequestsRead, labels...)
	}
	if relay != nil && relay.IsDebugTrace {
		incCounter(m.routerRequestsDebugTrace, labels...)
	}
	if relay != nil && relay.IsArchive {
		incCounter(m.routerRequestsArchive, labels...)
	}
}

// RecordCacheHitRequest records a request served from the router's in-process cache
// in the router-level request-group counters. Cache hits are always successful so only
// the success counter is incremented (alongside total). The provider_address label is
// set to "Cached" so dashboards can distinguish cache traffic from endpoint traffic.
func (m *SmartRouterMetricsManager) RecordCacheHitRequest(spec, apiInterface, function string, relay *RelayMetrics) {
	if m == nil {
		return
	}
	// Cache hits bypass RecordDirectRelayEnd entirely, so this path observes the batch
	// size itself — otherwise cached batch traffic would be missing from the histogram.
	m.observeBatchSize(spec, apiInterface, function)
	function = m.normalizeMethodLabel(spec, function)

	m.incRequestGroup(true, relay, spec, apiInterface, "Cached", function)
}

// RecordBlockFetch records block fetch operations (for chain tracker/state poller).
// Fans out across every provider sharing the URL so the fetch counters reflect the real
// upstream success/failure rate for all of them — not just the last-registered name.
func (m *SmartRouterMetricsManager) RecordBlockFetch(spec, apiInterface, endpointID string, isLatest bool, success bool) {
	if m == nil {
		return
	}

	for _, providerName := range m.resolveProviderNames(endpointID) {
		if isLatest {
			if success {
				m.AddEndpointFetchLatestSuccess(spec, apiInterface, providerName)
			} else {
				m.AddEndpointFetchLatestFail(spec, apiInterface, providerName)
			}
		} else {
			if success {
				m.AddEndpointFetchBlockSuccess(spec, apiInterface, providerName)
			} else {
				m.AddEndpointFetchBlockFail(spec, apiInterface, providerName)
			}
		}
	}
}

// TrackerRequestKind values for RecordTrackerRequest. Deliberately a closed set of two —
// this is a Prometheus label, so it must never carry a raw method name.
const (
	// TrackerRequestKindLatestBlock — the "what is your latest block?" poll
	// (eth_blockNumber, getLatestBlockhash, ...). Sent on every non-gated poll tick.
	TrackerRequestKindLatestBlock = "latest_block"

	// TrackerRequestKindBlockHash — a block-hash fetch (eth_getBlockByNumber, getBlock).
	// Only fork detection asks for these, so this series is zero when it is off.
	TrackerRequestKindBlockHash = "block_hash"
)

// AddEndpointTrackerRequest increments the tracker upstream-request counter.
//
// Guards the counter itself, not just the manager: MappedLabelsCounterVec.WithLabelValues has
// no nil receiver check, and partially-built managers exist (test fixtures construct only the
// counters they assert on). A metric must never be the thing that takes the poll goroutine down.
func (m *SmartRouterMetricsManager) AddEndpointTrackerRequest(spec, apiInterface, endpointID, kind string) {
	if m == nil {
		return
	}
	incMapped(m.endpointTrackerRequests, map[string]string{"spec": spec, "apiInterface": apiInterface, "endpoint_id": endpointID, "kind": kind})
}

// TrackerGateSkipSource values for RecordTrackerGateSkip. A closed set of three, mirroring the
// three reasons a dedicated poll can be skipped without contacting upstream.
const (
	// TrackerGateSkipSourceRelay — served relay traffic kept this endpoint's tip fresh.
	TrackerGateSkipSourceRelay = "relay"
	// TrackerGateSkipSourcePeer — another pod polled this endpoint and published the result
	// to the cache backend (MAG-2981).
	TrackerGateSkipSourcePeer = "peer"
	// TrackerGateSkipSourceIdle — the chain served no client relay for the idle window, so
	// nobody would read the poll's answer (idle mode, --chain-tracker-idle-after).
	TrackerGateSkipSourceIdle = "idle"
)

// TrackerGateOp values for RecordTrackerGateError: the two calls the gate makes to the fleet
// observation store.
const (
	// TrackerGateOpFetch — reading a peer's observation before polling failed, so the pod polled.
	TrackerGateOpFetch = "fetch"
	// TrackerGateOpPublish — publishing this pod's own observation to the fleet store failed.
	TrackerGateOpPublish = "publish"
)

// RecordTrackerGateError records ONE failed fleet-store call, fanned out like RecordTrackerGateSkip.
// It is what separates "the cache backend is unreachable or slow" from "no peer has anything to
// share": both otherwise present only as peer skips sitting at zero.
func (m *SmartRouterMetricsManager) RecordTrackerGateError(spec, apiInterface, endpointID, op string) {
	if m == nil {
		return
	}
	m.incForEveryProvider(m.endpointTrackerGateErrors, spec, apiInterface, endpointID, "op", op)
}

// RecordTrackerGateSkip records ONE poll cycle the traffic gate suppressed for an endpoint,
// fanned out across every provider sharing the URL like RecordTrackerRequest.
func (m *SmartRouterMetricsManager) RecordTrackerGateSkip(spec, apiInterface, endpointID, source string) {
	if m == nil {
		return
	}
	m.incForEveryProvider(m.endpointTrackerGateSkips, spec, apiInterface, endpointID, "source", source)
}

// RecordTrackerRequest records ONE upstream request sent by the per-endpoint chain tracker.
// Fans out across every provider sharing the URL, the same way RecordBlockFetch does, so the
// per-provider view reflects the real load each of them puts on that upstream.
//
// Counted at the transport chokepoint (EndpointPoller.sendRawRequest), so it reflects requests
// the node actually received — not poll ticks. A tick suppressed by the relay traffic gate
// sends nothing and is therefore not counted, which is the point: this is the series that
// makes a traffic reduction provable.
func (m *SmartRouterMetricsManager) RecordTrackerRequest(spec, apiInterface, endpointID, kind string) {
	if m == nil {
		return
	}
	m.incForEveryProvider(m.endpointTrackerRequests, spec, apiInterface, endpointID, "kind", kind)
}

// =============================================================================
// ConsumerMetricsManagerInf implementation
// =============================================================================

// RecordEndToEndLatency is a no-op for SmartRouter — end-to-end latency is
// recorded via RecordRouterEndToEndLatency in SendParsedRelay.
func (m *SmartRouterMetricsManager) RecordEndToEndLatency(string, string, string, float64) {}

// RecordProviderLatency is a no-op for SmartRouter — provider latency is
// already captured per-endpoint via RecordDirectRelayEnd.
func (m *SmartRouterMetricsManager) RecordProviderLatency(string, string, string, string, float64) {}

func (m *SmartRouterMetricsManager) SetRelayNodeErrorMetric(chainId string, apiInterface string, providerAddress string, method string) {
	if m == nil {
		return
	}
	incCounter(m.incidentNodeErrorsTotalMetric, chainId, apiInterface, providerAddress, m.normalizeMethodLabel(chainId, method))
}

func (m *SmartRouterMetricsManager) RecordCacheResult(chainId, apiInterface, method, cacheTier, outcome string, latencyMs float64) {
	if m == nil {
		return
	}
	method = m.normalizeMethodLabel(chainId, method)
	incCounter(m.cacheRequestsTotalMetric, chainId, apiInterface, method, cacheTier)
	if outcome == CacheOutcomeHit {
		incCounter(m.cacheSuccessTotalMetric, chainId, apiInterface, method, cacheTier)
	} else {
		incCounter(m.cacheFailedTotalMetric, chainId, apiInterface, method, cacheTier, outcome)
	}
	// Every attempted lookup is observed — hit-only latency hid exactly the tail
	// that matters for a network-hop tier.
	observeHistogram(m.cacheLatencyHistogram, latencyMs, chainId, apiInterface, method, cacheTier)
}

// RecordExtensionUnavailable counts one request served without extension, a caller-requested
// extension no node on this router offers (MAG-3935). The extension label is bounded without a
// registry of its own: only names the spec defines as extensions (plus "websocket") survive
// SeparateAddonsExtensions before they can be recorded as unavailable, so a caller cannot mint
// series by sending arbitrary lava-extension values.
func (m *SmartRouterMetricsManager) RecordExtensionUnavailable(chainId, apiInterface, extension string) {
	if m == nil {
		return
	}
	incCounter(m.extensionUnavailableTotal, chainId, apiInterface, extension)
}

// RecordCacheWriteSkipped counts a reply the router served but chose not to write to its
// cache; reason is the closed CacheWriteSkipReason* enum.
func (m *SmartRouterMetricsManager) RecordCacheWriteSkipped(chainId, apiInterface, method, reason string) {
	if m == nil {
		return
	}
	incCounter(m.cacheWriteSkippedTotalMetric, chainId, apiInterface, m.normalizeMethodLabel(chainId, method), reason)
}

// RecordCacheEntryWritten observes the body size of a reply handed to the cache backend.
func (m *SmartRouterMetricsManager) RecordCacheEntryWritten(chainId, apiInterface, method string, bodyBytes int) {
	if m == nil {
		return
	}
	observeHistogram(m.cacheEntryBytesHistogram, float64(bodyBytes), chainId, apiInterface, m.normalizeMethodLabel(chainId, method))
}

func (m *SmartRouterMetricsManager) SetProtocolError(chainId string, apiInterface string, providerAddress string, method string) {
	if m == nil {
		return
	}
	incCounter(m.incidentProtocolErrorsTotalMetric, chainId, apiInterface, providerAddress, m.normalizeMethodLabel(chainId, method))
}

func (m *SmartRouterMetricsManager) RecordIncidentRetry(chainId string, apiInterface string, method string, count uint64, success bool) {
	if m == nil {
		return
	}
	method = m.normalizeMethodLabel(chainId, method)
	observeHistogram(m.incidentRetryAttemptsHistogram, float64(count), chainId, apiInterface, method)
	incOutcome(m.incidentRetriesTotalMetric, m.incidentRetriesSuccessMetric, m.incidentRetriesFailedMetric, success, chainId, apiInterface, method)
}

func (m *SmartRouterMetricsManager) RecordIncidentConsistency(chainId string, apiInterface string, method string, success bool) {
	if m == nil {
		return
	}
	method = m.normalizeMethodLabel(chainId, method)
	incOutcome(m.incidentConsistencyTotalMetric, m.incidentConsistencySuccessMetric, m.incidentConsistencyFailedMetric, success, chainId, apiInterface, method)
}

func (m *SmartRouterMetricsManager) RecordIncidentHedgeResult(chainId string, apiInterface string, method string, count uint64, success bool) {
	if m == nil {
		return
	}
	method = m.normalizeMethodLabel(chainId, method)
	observeHistogram(m.incidentHedgeAttemptsHistogram, float64(count), chainId, apiInterface, method)
	incOutcome(m.incidentHedgeTotalMetric, m.incidentHedgeSuccessMetric, m.incidentHedgeFailedMetric, success, chainId, apiInterface, method)
}

func (m *SmartRouterMetricsManager) SetCrossValidationMetric(chainId, apiInterface, method string, success bool, agreeingProviders, disagreeingProviders []string) {
	if m == nil {
		return
	}
	method = m.normalizeMethodLabel(chainId, method)
	incOutcome(m.crossValidationRequestsTotalMetric, m.crossValidationSuccessTotalMetric, m.crossValidationFailedTotalMetric, success, chainId, apiInterface, method)
	for _, provider := range agreeingProviders {
		incCounter(m.crossValidationProviderAgreementsTotalMetric, chainId, apiInterface, method, provider)
	}
	for _, provider := range disagreeingProviders {
		incCounter(m.crossValidationProviderDisagreementsTotalMetric, chainId, apiInterface, method, provider)
	}
}

// SetCrossValidationMismatchMetric records one SUCCESSFUL cross-validation content outlier — a successful
// response whose content disagreed with a reached consensus — for that provider's group, labeled by
// finality (finalized/not_finalized/unknown). It is NOT incremented for quorum failures, no-agreement,
// diversity-unmet, or node/protocol errors (those belong to the structured failure signal). This is the
// bounded alerting surface: group is operator-defined and finality is tri-valued, so it stays
// low-cardinality. Callers must only invoke it for deterministic
// methods — non-deterministic methods legitimately return different responses and would be noise.
func (m *SmartRouterMetricsManager) SetCrossValidationMismatchMetric(chainId, apiInterface, method, group, finality string) {
	if m == nil {
		return
	}
	if group == "" {
		group = common.DefaultProviderGroup
	}
	incCounter(m.crossValidationMismatchTotalMetric, chainId, apiInterface, m.normalizeMethodLabel(chainId, method), group, finality)
}

// SetCrossValidationFailureMetric records one cross-validation failure broken down by reason. reason is one
// of the closed CrossValidationReason* enum (common.CrossValidationReason*), so the series stays bounded.
// This complements the unlabeled cross_validation_failed_total: it lets a dashboard separate a structural
// capacity failure (the client should fall back) from a quorum disagreement (a retry may help). A caller
// that has no reason (reason == "") is skipped rather than emitting an empty label value. The existing
// failed_total series is left untouched, so dashboards keyed off it keep working.
func (m *SmartRouterMetricsManager) SetCrossValidationFailureMetric(chainId, apiInterface, method, reason string) {
	if m == nil || reason == "" {
		return
	}
	incCounter(m.crossValidationFailuresTotalMetric, chainId, apiInterface, m.normalizeMethodLabel(chainId, method), reason)
}

// SetCrossValidationStragglerMetric records how a cross-validation straggler resolved (MAG-2187): a
// provider that was still in flight when the quorum early-exit reply was built, whose late response was
// compared against the reached consensus by the async watcher. outcome is one of the closed
// CrossValidationStragglerOutcome* enum (agreed, disagreed, node-error, protocol-error, not-received),
// so the series stays bounded. A caller with no outcome (outcome == "") is skipped rather than emitting
// an empty label value.
func (m *SmartRouterMetricsManager) SetCrossValidationStragglerMetric(chainId, apiInterface, method, outcome string) {
	if m == nil || outcome == "" {
		return
	}
	incCounter(m.crossValidationStragglerTotalMetric, chainId, apiInterface, m.normalizeMethodLabel(chainId, method), outcome)
}

func (m *SmartRouterMetricsManager) UpdateHealthCheckStatus(status bool) {
	m.UpdateOverallHealthStatus(status)
}

func (m *SmartRouterMetricsManager) UpdateHealthcheckStatusBreakdown(chainId, apiInterface string, status bool) {
	if m == nil {
		return
	}
	setGauge(m.routerOverallHealthBreakdown, boolGauge(status), chainId, apiInterface)
}

func (m *SmartRouterMetricsManager) SetProviderSelected(chainId string, apiInterface string, _ string, allProviderScores []ProviderSelectionScores, _ float64) {
	if m == nil {
		return
	}
	for _, scores := range allProviderScores {
		endpointID := scores.ProviderAddress
		m.endpointSelectionScore.WithLabelValues(chainId, apiInterface, endpointID, "availability").Set(scores.Availability)
		m.endpointSelectionScore.WithLabelValues(chainId, apiInterface, endpointID, "latency").Set(scores.Latency)
		m.endpointSelectionScore.WithLabelValues(chainId, apiInterface, endpointID, "sync").Set(scores.Sync)
		m.endpointSelectionScore.WithLabelValues(chainId, apiInterface, endpointID, "stake").Set(scores.Stake)
		m.endpointSelectionScore.WithLabelValues(chainId, apiInterface, endpointID, "composite").Set(scores.Composite)
	}
}

// SetBlockedProvider publishes whether one specific provider is blocked. The aggregate count in
// SetCSMBlockedProvidersCount says how many went out; this says which.
//
// Was an empty stub, so every blockProvider / restore call site published nothing and the smart
// router had no per-provider blocked signal at all.
//
// providerEndpoint is deliberately NOT a label, and the parameter is ignored. Two reasons:
//
//  1. It would be a credential leak. Callers pass Endpoints[0].NetworkAddress, which is the raw
//     configured node URL — and node URLs embed API keys in their path or query. The relay path
//     already labels by provider name for exactly this reason ("use provider name (configured
//     name) instead of raw URL to avoid leaking API keys"), and docs/METRICS.md states the rule
//     outright: a credential must never reach a Prometheus series.
//  2. It would go stale. Blocking is a per-provider decision, but Endpoints[0] is one arbitrary
//     endpoint of several. If that value changes between the block and the unblock, the 0 lands
//     on a new series and the old one is orphaned at 1 forever.
//
// The signature keeps the parameter because it is fixed by ConsumerMetricsManagerInf.
func (m *SmartRouterMetricsManager) SetBlockedProvider(chainId, apiInterface, providerAddress, _ string, isBlocked bool) {
	if m == nil {
		return
	}
	setGauge(m.csmProviderBlocked, boolGauge(isBlocked), chainId, apiInterface, providerAddress)
}

func (m *SmartRouterMetricsManager) SetQOSMetrics(chainId string, apiInterface string, _ string, _ string, _ *pairingtypes.QualityOfServiceReport, _ *pairingtypes.QualityOfServiceReport, _ int64, _ uint64, _ time.Duration, _ bool) {
}

func (m *SmartRouterMetricsManager) ResetSessionRelatedMetrics() {}

// ResetBlockedProvidersMetrics rebases the per-provider blocked gauge onto a new pairing.
// UpdateAllProviders calls it on every epoch transition with the new epoch's provider set.
//
// Without this, a provider that was blocked and then dropped from the pairing leaves a series
// stuck at 1 that nothing can ever clear: the unblock path only publishes for addresses it finds
// in the standing block list, and that provider is no longer in it. Over many epochs those
// phantoms accumulate — a slow cardinality leak that also reads as a permanent outage.
//
// Drop every series for this chain first, then seed 0 for each provider in the new pairing, so
// providers that left are gone rather than frozen. A provider that the re-block pass immediately
// re-blocks is corrected by the next publishStateSizes tick, which republishes the full truth.
func (m *SmartRouterMetricsManager) ResetBlockedProvidersMetrics(chainId, apiInterface string, providerAddressToEndpoint map[string]string) {
	if m == nil {
		return
	}
	m.csmProviderBlocked.DeletePartialMatch(prometheus.Labels{"spec": chainId, "apiInterface": apiInterface})
	for providerAddress := range providerAddressToEndpoint {
		m.csmProviderBlocked.WithLabelValues(chainId, apiInterface, providerAddress).Set(0)
	}
}

// SetCSMBlockedProvidersByReason publishes how many providers are blocked for each reason.
//
// The caller passes a complete map — every known reason, zeros included — and this writes all of
// them. That is what makes the series self-correcting: a reason that stops applying is actively set
// to 0 rather than left at its last value. Silently dropping the zeros would reintroduce exactly the
// stale-series bug MAG-3106 fixed for the per-provider gauge.
func (m *SmartRouterMetricsManager) SetCSMBlockedProvidersByReason(chainId, apiInterface string, countsByReason map[string]int) {
	if m == nil {
		return
	}
	for reason, count := range countsByReason {
		setGauge(m.csmBlockedProvidersByReason, float64(count), chainId, apiInterface, reason)
	}
}

// SetCSMBlockedProvidersCount publishes the number of providers currently blocked — the operator-
// facing "is this chain degraded" signal, and the post-condition integration tests assert against
// /metrics after /debug/reset-all (MAG-1762).
func (m *SmartRouterMetricsManager) SetCSMBlockedProvidersCount(chainId, apiInterface string, count int) {
	if m == nil {
		return
	}
	setGauge(m.csmBlockedProvidersCount, float64(count), chainId, apiInterface)
}

// SetCSMPreviousEpochBlockedProvidersCount publishes the size of csm.previousEpochBlockedProviders,
// the cross-epoch carry-over set. Split out from the gauge above, which now reports the thing its
// name promises.
func (m *SmartRouterMetricsManager) SetCSMPreviousEpochBlockedProvidersCount(chainId, apiInterface string, count int) {
	if m == nil {
		return
	}
	setGauge(m.csmPrevEpochBlockedProviders, float64(count), chainId, apiInterface)
}

// SetCSMBlockedBackupProvidersCount publishes the size of csm.blockedBackupProviders.
func (m *SmartRouterMetricsManager) SetCSMBlockedBackupProvidersCount(chainId, apiInterface string, count int) {
	if m == nil {
		return
	}
	setGauge(m.csmBlockedBackupProvidersCount, float64(count), chainId, apiInterface)
}

// Serving-tier gauge values. A chain that cannot serve no longer exits the
// process (MAG-2525), so this gauge carries the signal that pod restarts used to.
const (
	ServingTierDark     = 0 // no healthy provider in either tier — endpoint reports unhealthy
	ServingTierDegraded = 1 // no healthy primaries; serving from backups only
	ServingTierPrimary  = 2 // serving from primaries
)

// ServingTier maps healthy per-tier provider counts onto the serving posture.
// Backups count as serving: the router falls back to them on primary exhaustion,
// so "all primaries down" is degraded rather than dark.
func ServingTier(healthyStatic, healthyBackup int) int {
	switch {
	case healthyStatic > 0:
		return ServingTierPrimary
	case healthyBackup > 0:
		return ServingTierDegraded
	default:
		return ServingTierDark
	}
}

// SetEndpointServingTier publishes which provider tier an endpoint is serving
// from. Alert on < 2 for degraded and == 0 for a chain that is dark.
func (m *SmartRouterMetricsManager) SetEndpointServingTier(chainId, apiInterface string, healthyStatic, healthyBackup int) {
	if m == nil {
		return
	}
	setGauge(m.endpointServingTier, float64(ServingTier(healthyStatic, healthyBackup)), chainId, apiInterface)
}

// Scopes of smartrouter_provider_verification_failed.
const (
	VerificationScopeProvider = "provider"
	VerificationScopeService  = "service"
	VerificationScopeURL      = "url"
)

// Severities of a failing verification, as MAG-3950 names them: Fail takes a url or the
// provider out, PartialFail switches off only the service the check covers.
const (
	VerificationSeverityFail        = "Fail"
	VerificationSeverityPartialFail = "PartialFail"
)

// verificationFailureLabels are the labels of both verification-failure metrics.
var verificationFailureLabels = []string{"spec", "apiInterface", "provider", "tier", "scope", "service", "check", "severity"}

// ProviderVerificationFailure is one failing check of one provider. Service is
// empty unless the failure costs one service.
type ProviderVerificationFailure struct {
	Scope   string
	Service string
	Check   string
	// Severity is the label as published. Empty reads it from what the failure costs:
	// PartialFail when it costs one service, Fail when it costs a url or the provider.
	Severity string
}

// withSeverity is f with its severity label filled in.
func (f ProviderVerificationFailure) withSeverity() ProviderVerificationFailure {
	if f.Severity != "" {
		return f
	}
	f.Severity = VerificationSeverityFail
	if f.Service != "" {
		f.Severity = VerificationSeverityPartialFail
	}
	return f
}

// CountVerificationCheckFailure counts one failed run of a verification check.
// tier is "static" or "backup".
func (m *SmartRouterMetricsManager) CountVerificationCheckFailure(spec, apiInterface, provider, tier string, failure ProviderVerificationFailure) {
	if m == nil {
		return
	}
	failure = failure.withSeverity()
	incCounter(m.verificationCheckFailures, spec, apiInterface, provider, tier, failure.Scope, failure.Service, failure.Check, failure.Severity)
}

// publishedVerificationFailure is a failing series as last published, with when its
// failure started.
type publishedVerificationFailure struct {
	ProviderVerificationFailure
	since time.Time
}

// SetProviderVerificationFailures publishes the checks a configured provider is
// failing right now, each valued with the unix time its failure started, and removes
// the series of checks it no longer fails. An empty list clears the provider. tier is
// "static" or "backup".
//
// Only series that cleared are deleted, never the whole provider first: a scrape
// landing between a delete and a re-set would read a gap, and a gap resets an
// alert's pending timer.
func (m *SmartRouterMetricsManager) SetProviderVerificationFailures(spec, apiInterface, provider, tier string, failures []ProviderVerificationFailure) {
	if m == nil {
		return
	}
	key := spec + "|" + apiInterface + "|" + provider + "|" + tier
	m.verificationFailuresMu.Lock()
	defer m.verificationFailuresMu.Unlock()
	now := time.Now()
	if m.verificationNow != nil {
		now = m.verificationNow()
	}
	previous := m.verificationFailures[key]
	published := func(set []publishedVerificationFailure, f ProviderVerificationFailure) bool {
		return slices.ContainsFunc(set, func(p publishedVerificationFailure) bool { return p.ProviderVerificationFailure == f })
	}
	next := make([]publishedVerificationFailure, 0, len(failures))
	for _, f := range failures {
		f = f.withSeverity()
		if published(next, f) {
			continue
		}
		next = append(next, publishedVerificationFailure{ProviderVerificationFailure: f, since: failureStart(previous, f, now)})
	}
	for _, old := range previous {
		if !published(next, old.ProviderVerificationFailure) {
			m.providerVerificationFailed.DeleteLabelValues(spec, apiInterface, provider, tier, old.Scope, old.Service, old.Check, old.Severity)
		}
	}
	for _, p := range next {
		m.providerVerificationFailed.WithLabelValues(spec, apiInterface, provider, tier, p.Scope, p.Service, p.Check, p.Severity).Set(float64(p.since.Unix()))
	}
	if m.verificationFailures == nil {
		m.verificationFailures = map[string][]publishedVerificationFailure{}
	}
	if len(next) == 0 {
		delete(m.verificationFailures, key)
		return
	}
	m.verificationFailures[key] = next
}

// failureStart is when f started failing: its own series' start when it was already
// published, else the earliest start in the same scope and service, so a provider
// that moves to another check keeps the time it started failing, else now.
func failureStart(previous []publishedVerificationFailure, f ProviderVerificationFailure, now time.Time) time.Time {
	var earliest time.Time
	for _, p := range previous {
		if p.ProviderVerificationFailure == f {
			return p.since
		}
		if p.Scope == f.Scope && p.Service == f.Service && (earliest.IsZero() || p.since.Before(earliest)) {
			earliest = p.since
		}
	}
	if earliest.IsZero() {
		return now
	}
	return earliest
}

// SetCSMStickySessionsCount publishes the number of live sticky-session affinities.
func (m *SmartRouterMetricsManager) SetCSMStickySessionsCount(chainId, apiInterface string, count int) {
	if m == nil {
		return
	}
	setGauge(m.csmStickySessionsCount, float64(count), chainId, apiInterface)
}

// RecordStickyClaim counts one cross-pod sticky-session claim resolution by outcome.
func (m *SmartRouterMetricsManager) RecordStickyClaim(chainId, apiInterface, outcome string) {
	if m == nil {
		return
	}
	incCounter(m.csmStickyClaims, chainId, apiInterface, outcome)
}

// SetCSMReportedProvidersCount publishes the size of the ReportedProviders register.
func (m *SmartRouterMetricsManager) SetCSMReportedProvidersCount(chainId, apiInterface string, count int) {
	if m == nil {
		return
	}
	setGauge(m.csmReportedProvidersCount, float64(count), chainId, apiInterface)
}

// SetRelayMetrics records the per-customer request group (MAG-4081). The listeners call it once
// per client request, through RPCConsumerLogs.AddMetricFor*, after the reply and with the
// request's final outcome in Success. It does not touch the per-endpoint families: those are
// per attempt and owned by RecordDirectRelayEnd, and counting them here too would double count.
//
// Not counted: a request the router could not parse (no method — it never reached a chain call),
// and each push on an open subscription (the subscribe request itself is counted).
func (m *SmartRouterMetricsManager) SetRelayMetrics(relayMetric *RelayMetrics, err error) {
	if m == nil || relayMetric == nil || relayMetric.ApiMethod == "" || relayMetric.ApiMethod == SubscriptionDeliveryMethod {
		return
	}
	spec, apiInterface := relayMetric.ChainID, relayMetric.APIType
	customer := m.normalizeCustomerLabel(spec, relayMetric.ProjectHash)
	method := m.normalizeMethodLabel(spec, relayMetric.ApiMethod)
	incCounter(m.customerRequestsTotal, spec, apiInterface, customer, method)
	if !relayMetric.Success {
		incCounter(m.customerRequestsFailed, spec, apiInterface, customer, method)
		return
	}
	observeHistogram(m.customerEndToEndLatency, float64(relayMetric.Latency), spec, apiInterface, customer)
}

// SetVersion records the protocol version of the smart router.
func (m *SmartRouterMetricsManager) SetVersion(version string) {
	if m == nil {
		return
	}
	SetVersionInner(m.routerProtocolVersion, version)
}

func (m *SmartRouterMetricsManager) SetWsSubscriptionRequestMetric(chainId, apiInterface string) {
	if m == nil {
		return
	}
	incCounter(m.routerWsSubscriptionsTotal, chainId, apiInterface)
}

func (m *SmartRouterMetricsManager) SetFailedWsSubscriptionRequestMetric(chainId, apiInterface string) {
	if m == nil {
		return
	}
	incCounter(m.routerWsSubscriptionErrors, chainId, apiInterface)
}

func (m *SmartRouterMetricsManager) SetSubscriptionClientDroppedMetric(chainId, apiInterface string) {
	if m == nil {
		return
	}
	incCounter(m.subscriptionClientsDropped, chainId, apiInterface)
}

func (m *SmartRouterMetricsManager) SetWebSocketConnectionActive(chainId, apiInterface string, add bool) {
	if m == nil {
		return
	}
	delta := -1.0
	if add {
		delta = 1
	}
	if m.routerWsConnectionsActive != nil {
		m.routerWsConnectionsActive.WithLabelValues(chainId, apiInterface).Add(delta)
	}
}

func (m *SmartRouterMetricsManager) StartSelectionStatsUpdater(ctx context.Context, updateInterval time.Duration) {
	if m == nil || m.optimizerQoSClient == nil {
		return
	}
	if updateInterval <= 0 {
		utils.LavaFormatWarning("StartSelectionStatsUpdater: invalid updateInterval, selection stats will not be reported", nil,
			utils.LogAttr("updateInterval", updateInterval))
		return
	}
	go func() {
		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, report := range m.optimizerQoSClient.GetReportsToSend() {
					endpointID := report.ProviderAddress
					m.optimizerSelectionScore.WithLabelValues(report.ChainId, endpointID, "availability").Set(report.SelectionAvailability)
					m.optimizerSelectionScore.WithLabelValues(report.ChainId, endpointID, "latency").Set(report.SelectionLatency)
					m.optimizerSelectionScore.WithLabelValues(report.ChainId, endpointID, "sync").Set(report.SelectionSync)
					m.optimizerSelectionScore.WithLabelValues(report.ChainId, endpointID, "stake").Set(report.SelectionStake)
					m.optimizerSelectionScore.WithLabelValues(report.ChainId, endpointID, "composite").Set(report.SelectionComposite)
				}
			}
		}
	}()
}
