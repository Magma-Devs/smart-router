# Smart Router — Metrics Reference

Every metric the Smart Router exposes over Prometheus, with its type, labels, and
meaning. All metrics are defined under [`protocol/metrics/`](../protocol/metrics/).

## Exposition

Metrics are served in Prometheus text format from an HTTP server started by the
metrics manager.

| Path | Format | Description |
| --- | --- | --- |
| `/metrics` | Prometheus | All registered metrics ([`promhttp.Handler()`](../protocol/metrics/smartrouter_metrics_manager.go#L615)) |
| `/metrics/overall-health` | text | `200 Health status OK` if ≥1 endpoint is healthy, else `503 Unhealthy` |
| `/metrics/health-overall` | text | Alias of the above (backward-compat path) |

### Configuration

| Flag / env | Default | Effect |
| --- | --- | --- |
| `--metrics-listen-address` | `disabled` | Address to expose Prometheus metrics on, e.g. `:7779` or `localhost:7779`. The literal `disabled` turns the metrics server off entirely. |
| `--optimizer-qos-server-address` | `""` | Push optimizer QoS reports to this address. Independent of `/metrics` — the `lava_rpc_optimizer_selection_score` gauge is collected and exposed regardless (see below). |

```bash
# enable, then scrape
smartrouter ... --metrics-listen-address ":7779"
curl http://localhost:7779/metrics
```

The flag is defined at
[`rpcsmartrouter.go:1975`](../protocol/rpcsmartrouter/rpcsmartrouter.go#L1975); the flag
name and the `disabled` sentinel live in
[`flags.go`](../protocol/metrics/flags.go#L8).

> **Optimizer scores are always on.** The optimizer-QoS client is created
> unconditionally, so `lava_rpc_optimizer_selection_score` is populated on `/metrics`
> whether or not `--optimizer-qos-server-address` is set — the address only adds a remote
> push. (There is no separate `GET /provider_optimizer_metrics` endpoint; that handler was
> removed along with the dead consumer metrics manager.)

### Conventions

- **Latency histograms** all share the same buckets, in **milliseconds**
  ([`buckets.go:7`](../protocol/metrics/buckets.go#L7)):
  `1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000`.
- **Attempt histograms** (`retry_attempts`, `hedge_attempts`) use integer buckets `1…10`.
- **Common labels**:
  - `spec` — chain spec id (e.g. `ETH1`, `LAV1`).
  - `apiInterface` — `jsonrpc`, `tendermintrpc`, `rest`, `grpc`.
  - `endpoint_id` — the configured upstream RPC endpoint.
  - `provider_address` — provider the relay was routed to.
  - `method` — RPC method name.
  - `function` — relay function class; the `function` label lets one metric serve both
    the per-function breakdown and (via `sum by (...)`) the aggregate.
- **Boolean gauges** encode `1 = true / healthy / present`, `0 = false / unhealthy / absent`.
- **Protocol version** gauges encode `major*1e6 + minor*1e3 + patch`.
- Metrics are registered with `registerOrReuse`, which returns the already-registered
  collector instead of panicking on a duplicate — so re-registration (e.g. across test
  runs sharing the default registry) is safe.

---

## Smart Router metrics

These are the metrics specific to running as a Smart Router, defined in
[`smartrouter_metrics_manager.go`](../protocol/metrics/smartrouter_metrics_manager.go).
They split into **endpoint-scoped** (`lava_rpc_endpoint_*`) and **router-scoped**
(`lava_rpcsmartrouter_*`) families.

### Endpoint-scoped — `lava_rpc_endpoint_*`

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_rpc_endpoint_total_relays_serviced` | Counter | `spec`, `apiInterface`, `endpoint_id`, `function` | Relays successfully served by this endpoint. |
| `lava_rpc_endpoint_total_errored` | Counter | `spec`, `apiInterface`, `endpoint_id`, `function` | Errored relays for this endpoint. |
| `lava_rpc_endpoint_requests_in_flight` | Gauge | `spec`, `apiInterface`, `endpoint_id`, `function` | Relays currently in flight to this endpoint. |
| `lava_rpc_endpoint_end_to_end_latency_milliseconds` | Histogram | `spec`, `apiInterface`, `endpoint_id`, `function` | End-to-end latency per function for this endpoint. |
| `lava_rpc_endpoint_overall_health` | Gauge | `spec`, `apiInterface`, `endpoint_id` | Endpoint health (1 healthy / 0 unhealthy). |
| `lava_rpc_endpoint_overall_health_breakdown` | Gauge | `spec`, `apiInterface` | Aggregate health per chain/interface. |
| `lava_rpc_endpoint_selection_score` | Gauge | `spec`, `apiInterface`, `endpoint_id`, `score_type` | Selection scores by `score_type` (availability / latency / sync / stake / composite). |
| `lava_rpc_endpoint_latest_block` | Gauge | `spec`, `apiInterface`, `endpoint_id` | Latest block reported by the endpoint. |
| `lava_rpc_endpoint_fetch_latest_fails` | Counter | `spec`, `apiInterface`, `endpoint_id` | Failed latest-block fetches. |
| `lava_rpc_endpoint_fetch_block_fails` | Counter | `spec`, `apiInterface`, `endpoint_id` | Failed specific-block fetches. |
| `lava_rpc_endpoint_fetch_latest_success` | Counter | `spec`, `apiInterface`, `endpoint_id` | Successful latest-block fetches. |
| `lava_rpc_endpoint_fetch_block_success` | Counter | `spec`, `apiInterface`, `endpoint_id` | Successful specific-block fetches. |

### Optimizer

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_rpc_optimizer_selection_score` | Gauge | `spec`, `endpoint_id`, `score_type` | Periodic optimizer selection score per provider, by `score_type`. |

### Router-scoped — `lava_rpcsmartrouter_*`

#### Core relay & health

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_rpcsmartrouter_total_relays_serviced` | Counter | `spec`, `apiInterface`, `function` | Relays served by the router. |
| `lava_rpcsmartrouter_total_errored` | Counter | `spec`, `apiInterface`, `function` | Errored relays. |
| `lava_rpcsmartrouter_end_to_end_latency_milliseconds` | Histogram | `spec`, `apiInterface`, `function` | Router-level end-to-end latency. |
| `lava_rpcsmartrouter_overall_health` | Gauge | — | Overall router health (1 / 0). |
| `lava_rpcsmartrouter_overall_health_breakdown` | Gauge | `spec`, `apiInterface` | Per-chain/interface health. |
| `lava_rpcsmartrouter_latest_block` | Gauge | `spec`, `apiInterface` | Latest block known to the router. |
| `lava_rpcsmartrouter_protocol_version` | Gauge | `version` | Encoded protocol version. |

#### WebSocket

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_rpcsmartrouter_ws_connections_active` | Gauge | `spec`, `apiInterface` | Active WebSocket connections. |
| `lava_rpcsmartrouter_ws_subscriptions_total` | Counter | `spec`, `apiInterface` | Total WebSocket subscription requests. |
| `lava_rpcsmartrouter_ws_subscription_errors_total` | Counter | `spec`, `apiInterface` | Failed WebSocket subscription requests. |

#### Request breakdown

`requests_total` = `success` + `failed`. `read`/`write` partition by statefulness;
`debug_trace` and `archive` are orthogonal addon flags; `batch` is mutually exclusive
with read/write.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_rpcsmartrouter_requests_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | All requests. |
| `lava_rpcsmartrouter_requests_success_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Successful requests. |
| `lava_rpcsmartrouter_requests_failed_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Failed requests. |
| `lava_rpcsmartrouter_requests_read_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Read (stateless) requests. |
| `lava_rpcsmartrouter_requests_write_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Write (stateful) requests. |
| `lava_rpcsmartrouter_requests_debug_trace_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Debug/trace addon requests. |
| `lava_rpcsmartrouter_requests_archive_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Archive requests. |
| `lava_rpcsmartrouter_requests_batch_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Batch requests. |

#### Errors

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_rpcsmartrouter_node_errors_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Node errors returned by endpoints. |
| `lava_rpcsmartrouter_protocol_errors_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Protocol/transport errors (connection/session failures). |

#### Retries

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_rpcsmartrouter_retries_total` | Counter | `spec`, `apiInterface`, `method` | Retry attempts triggered (beyond the first try). |
| `lava_rpcsmartrouter_retries_success_total` | Counter | `spec`, `apiInterface`, `method` | Retried requests that succeeded. |
| `lava_rpcsmartrouter_retries_failed_total` | Counter | `spec`, `apiInterface`, `method` | Retried requests that failed. |
| `lava_rpcsmartrouter_retry_attempts` | Histogram | `spec`, `apiInterface`, `method` | Attempts per retried request (buckets 1…10). |

#### Consistency

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_rpcsmartrouter_consistency_total` | Counter | `spec`, `apiInterface`, `method` | Requests enforcing consistency (seenBlock). |
| `lava_rpcsmartrouter_consistency_success_total` | Counter | `spec`, `apiInterface`, `method` | Consistency-enforced requests that succeeded. |
| `lava_rpcsmartrouter_consistency_failed_total` | Counter | `spec`, `apiInterface`, `method` | Consistency-enforced requests that failed. |

#### Hedging

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_rpcsmartrouter_hedge_total` | Counter | `spec`, `apiInterface`, `method` | Hedge (batch-ticker) relays sent. |
| `lava_rpcsmartrouter_hedge_success_total` | Counter | `spec`, `apiInterface`, `method` | Hedged requests that succeeded. |
| `lava_rpcsmartrouter_hedge_failed_total` | Counter | `spec`, `apiInterface`, `method` | Hedged requests that failed. |
| `lava_rpcsmartrouter_hedge_attempts` | Histogram | `spec`, `apiInterface`, `method` | Hedge relays per request (buckets 1…10). |

#### Cross-validation

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_rpcsmartrouter_cross_validation_requests_total` | Counter | `spec`, `apiInterface`, `method` | Cross-validated requests. |
| `lava_rpcsmartrouter_cross_validation_success_total` | Counter | `spec`, `apiInterface`, `method` | Requests that reached consensus. |
| `lava_rpcsmartrouter_cross_validation_failed_total` | Counter | `spec`, `apiInterface`, `method` | Requests that failed to reach consensus. |
| `lava_rpcsmartrouter_cross_validation_provider_agreements_total` | Counter | `spec`, `apiInterface`, `method`, `provider_address` | Times a provider agreed with consensus. |
| `lava_rpcsmartrouter_cross_validation_provider_disagreements_total` | Counter | `spec`, `apiInterface`, `method`, `provider_address` | Times a provider disagreed with consensus. |

#### Cache

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_rpcsmartrouter_cache_requests_total` | Counter | `spec`, `apiInterface`, `method` | Cache lookup attempts. |
| `lava_rpcsmartrouter_cache_success_total` | Counter | `spec`, `apiInterface`, `method` | Cache hits. |
| `lava_rpcsmartrouter_cache_failed_total` | Counter | `spec`, `apiInterface`, `method` | Cache misses. |
| `lava_rpcsmartrouter_cache_latency_milliseconds` | Histogram | `spec`, `apiInterface`, `method` | Cache lookup latency. |

#### CSM state-store sizes (diagnostics)

Expose otherwise black-box Consumer-Session-Manager state so integration tests can
assert `/debug/reset-all` emptied each store (see MAG-1762). All drop to `0` after a reset.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_rpcsmartrouter_csm_blocked_providers` | Gauge | `spec`, `apiInterface` | Size of the previous-epoch blocked-providers store. |
| `lava_rpcsmartrouter_csm_blocked_backup_providers` | Gauge | `spec`, `apiInterface` | Size of the blocked-backup-providers store. |
| `lava_rpcsmartrouter_csm_sticky_sessions` | Gauge | `spec`, `apiInterface` | Live sticky-session affinities. |
| `lava_rpcsmartrouter_csm_reported_providers` | Gauge | `spec`, `apiInterface` | Size of the reported-providers register. |

---

## Shared metrics

### Classified errors — `lava_errors_*`

Defined in [`error_metrics.go`](../protocol/metrics/error_metrics.go).

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lava_errors_total` | Counter | `error_name`, `error_category`, `retryable`, `chain_id` | Errors classified by name, category, retryability, and chain. |

---

## Removed families

`SmartRouterMetricsManager` is the only metrics manager constructed at runtime
([`rpcsmartrouter.go`](../protocol/rpcsmartrouter/rpcsmartrouter.go)). Three metric
families that used to live in this package were never instantiated by the smart router
and have been removed — listed here so they don't resurface in dashboards or alerts:

| Family | Former home | Why removed |
| --- | --- | --- |
| `lava_consumer_*` | `consumer_metrics_manager.go` | `ConsumerMetricsManager` had no call site; the smart router implements `ConsumerMetricsManagerInf` directly. |
| `lava_provider_*` | `provider_metrics_manager.go` | `ProviderMetricsManager` was only ever a nil field on the chaintracker — no metric was emitted. |
| `lava_health_*` / `lava_*_entities` | `health_metrics.go` | `NewHealthMetrics` had no call site; it stood up its own orphaned HTTP server. The live health path (`RelaysMonitorAggregator`) is unaffected. |
