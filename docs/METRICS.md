# Smart Router — Metrics Reference

Every metric the Smart Router exposes over Prometheus, with its type, labels, and
meaning. Metrics are defined under [`protocol/metrics/`](../protocol/metrics/), except the
[rate-limit hold-off](#rate-limit-hold-off) pair, which the registry that owns the events
emits itself from [`protocol/holdoff/metrics.go`](../protocol/holdoff/metrics.go).

## Exposition

Metrics are served in Prometheus text format from an HTTP server started by the
metrics manager.

| Path | Format | Description |
| --- | --- | --- |
| `/metrics` | Prometheus | All registered metrics ([`promhttp.Handler()`](../protocol/metrics/smartrouter_metrics_manager.go#L623)) |
| `/metrics/overall-health` | text | `200 Health status OK` if ≥1 endpoint is healthy, else `503 Unhealthy` |
| `/metrics/health-overall` | text | Alias of the above (backward-compat path) |

### Configuration

| Flag / env | Default | Effect |
| --- | --- | --- |
| `--metrics-listen-address` | `disabled` | Address to expose Prometheus metrics on, e.g. `:7779` or `localhost:7779`. The literal `disabled` turns the metrics server off entirely. |
| `--optimizer-qos-sampling-interval` | `1s` | How often the optimizer-QoS sampler refreshes the `rpc_optimizer_selection_score` gauge and — when `--usage-otel-enabled` is set — emits `optimizer_qos` events to the OTel usage pipeline. |
| `--enable-fork-detection` | `false` | Turns per-endpoint block-hash polling on. Off by default, which pins `rpc_endpoint_tracker_requests_total{kind="block_hash"}` at zero and is the single largest term in tracker request volume. Process-wide: it applies to every chain the process serves, and cannot be scoped to one of them. |

```bash
# enable, then scrape
smartrouter ... --metrics-listen-address ":7779"
curl http://localhost:7779/metrics
```

The flag is defined at
[`rpcsmartrouter.go:1974`](../protocol/rpcsmartrouter/rpcsmartrouter.go#L1974); the flag
name and the `disabled` sentinel live in
[`flags.go`](../protocol/metrics/flags.go#L8).

> **Optimizer scores are always on.** The optimizer-QoS client is created
> unconditionally, so `rpc_optimizer_selection_score` is populated on `/metrics`
> regardless of telemetry config. Remote shipping of these reports now flows through the
> OTel usage pipeline (`--usage-otel-enabled`), not a dedicated push address. (There is no
> separate `GET /provider_optimizer_metrics` endpoint; that handler was removed along with
> the dead consumer metrics manager.)

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
  - `kind` — closed-set request classifier (`rpc_endpoint_tracker_requests_total` only):
    `latest_block` or `block_hash`. Never a raw method name — that would be unbounded.
  - `source` — closed-set gate classifier (`rpc_endpoint_tracker_gate_skips_total` only):
    `relay` or `peer`.
  - `op` — closed-set fleet-store operation (`rpc_endpoint_tracker_gate_errors_total` only):
    `fetch` or `publish`.
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
They split into **endpoint-scoped** (`rpc_endpoint_*`) and **router-scoped**
(`smartrouter_*`) families.

### Endpoint-scoped — `rpc_endpoint_*`

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `rpc_endpoint_total_relays_serviced` | Counter | `spec`, `apiInterface`, `endpoint_id`, `function` | Relays successfully served by this endpoint. |
| `rpc_endpoint_total_errored` | Counter | `spec`, `apiInterface`, `endpoint_id`, `function` | Errored relays for this endpoint. Excludes relays the router itself cancelled — see `rpc_endpoint_total_cancelled`. |
| `rpc_endpoint_total_cancelled` | Counter | `spec`, `apiInterface`, `endpoint_id`, `function` | Relays the router aborted before completion: relay-race losers on stateful broadcasts, and client disconnects. **Not an endpoint fault** — excluded from `rpc_endpoint_total_errored` and from QoS/availability scoring. |
| `rpc_endpoint_requests_in_flight` | Gauge | `spec`, `apiInterface`, `endpoint_id`, `function` | Relays currently in flight to this endpoint. |
| `rpc_endpoint_end_to_end_latency_milliseconds` | Histogram | `spec`, `apiInterface`, `endpoint_id`, `function` | End-to-end latency per function for this endpoint. |
| `rpc_endpoint_overall_health` | Gauge | `spec`, `apiInterface`, `endpoint_id` | Endpoint health (1 healthy / 0 unhealthy). |
| `rpc_endpoint_overall_health_breakdown` | Gauge | `spec`, `apiInterface` | Aggregate health per chain/interface. |
| `rpc_endpoint_selection_score` | Gauge | `spec`, `apiInterface`, `endpoint_id`, `score_type` | Selection scores by `score_type` (availability / latency / sync / stake / composite). |
| `rpc_endpoint_latest_block` | Gauge | `spec`, `apiInterface`, `endpoint_id` | Latest block reported by the endpoint. |
| `rpc_endpoint_fetch_latest_fails` | Counter | `spec`, `apiInterface`, `endpoint_id` | Latest-block fetch failures. An **event** counter, not a request counter — see the note below. |
| `rpc_endpoint_fetch_block_fails` | Counter | `spec`, `apiInterface`, `endpoint_id` | Failed specific-block fetches. |
| `rpc_endpoint_fetch_latest_success` | Counter | `spec`, `apiInterface`, `endpoint_id` | New-block **detections** by the chain tracker, not successful requests — see the note below. |
| `rpc_endpoint_fetch_block_success` | Counter | `spec`, `apiInterface`, `endpoint_id` | Successful specific-block fetches. |
| `rpc_endpoint_tracker_requests_total` | Counter | `spec`, `apiInterface`, `endpoint_id`, `kind` | Upstream requests the per-endpoint chain tracker actually sent, by `kind` (`latest_block`, `block_hash`). The only metric that measures tracker **request volume** — see the note below. |
| `rpc_endpoint_tracker_gate_errors_total` | Counter | `spec`, `apiInterface`, `endpoint_id`, `op` | Failed calls to the shared fleet observation store, by `op`: `fetch` (reading a peer's observation before polling — the pod polled instead) or `publish` (sharing this pod's own observation). Non-zero means the cache backend is unreachable, slow, or older than the observation RPCs. |
| `rpc_endpoint_tracker_gate_skips_total` | Counter | `spec`, `apiInterface`, `endpoint_id`, `source` | Poll cycles the tracker's traffic gate suppressed (no upstream request sent), by `source`: `relay` (served traffic kept the tip fresh) or `peer` (another pod's poll, borrowed through the cache backend — needs `--shared-state` + `--cache-be`). The other half of the tracker's cadence next to `rpc_endpoint_tracker_requests_total`. |

> **Tracker requests vs. fetch events.** `rpc_endpoint_fetch_latest_success` counts NEW BLOCK
> detections and `rpc_endpoint_fetch_latest_fails` counts latest-block fetch failures. Neither
> counts requests, and neither sits on the block-hash path at all — so a change in how much the
> chain tracker asks of an upstream node moves neither of them. `rpc_endpoint_tracker_requests_total`
> is the one that does: it increments at the transport chokepoint, once per request the node
> actually received.
>
> `kind` is a closed set of two. `latest_block` is the "what is your latest block?" poll
> (`eth_blockNumber`, `getLatestBlockhash`, ...), sent on every poll tick the relay traffic gate
> does not suppress. `block_hash` is a block-hash fetch (`eth_getBlockByNumber`, `getBlock`),
> which only fork detection asks for — so that series is **exactly zero** unless the router runs
> with `--enable-fork-detection`, and the drop to zero is what makes the traffic reduction
> provable. `/debug/endpoint-state` reports the matching `HashPolling` reason per endpoint
> (`on`, `off-operator-choice`, or `off-spec-no-block-by-num`).
>
> `rpc_endpoint_tracker_gate_skips_total` is the complement: one increment per poll tick the gate
> suppressed, labelled by what made the poll redundant. `source="relay"` is the relay traffic gate
> (served traffic kept the endpoint's tip fresh). `source="peer"` is the fleet gate: with
> `--shared-state` and a cache backend, every pod publishes its successful polls to the cache and
> borrows a fresh observation from another pod instead of polling, so an endpoint is polled about
> once per interval fleet-wide rather than once per pod. A pod never borrows its own observation,
> and the gate's skip budget still forces a local poll every few ticks, so `requests_total` never
> drops to zero on a live endpoint. On a multi-replica deployment, `peer` skips climbing while
> `requests_total{kind="latest_block"}` flattens is the feature working.
>
> `peer` stuck at zero has several unrelated causes. `rpc_endpoint_tracker_gate_errors_total`
> narrows it to two groups, and the log separates the pair that share a metric shape:
>
> | Reading | Cause |
> |---|---|
> | `op="fetch"` climbing, `publish` flat | the cache backend is **slow** — reachable, but answering the gate's read past its budget (`min(200ms, pollInterval/4)`) while still inside `publish`'s 1s timeout |
> | **both** ops climbing steadily at full tick rate | either the backend is **unreachable** or it **predates the observation RPCs** — these two are indistinguishable by metric; see the log line below |
> | errors flat at zero | no peer has anything to share — expected on a single replica, since a pod never borrows its own observation |
> | errors flat at zero, `publish` succeeding | the published TTL is clamped below the freshness window. The TTL is `min(2 × avgBlockTime, MaxEndpointObservationTTL=5m)` and must outlive a freshness window of `avgBlockTime`, so this bites only above a **5-minute** block time |
>
> Both ops climbing at full tick rate is ambiguous **by design** — `op` names the operation, not the
> outcome — so use the log to tell the two apart:
>
> - **unreachable**: `cache service connection error detected, triggering reconnection` with
>   `code = Unavailable`, followed by a reconnect loop that recovers on its own.
> - **out of date**: `fleet tracker gate: the cache backend does not implement endpoint observations`,
>   logged **once per listen endpoint** (one adapter is built per chain+interface), and it will not
>   clear until the backend is upgraded.
>
> A URL mismatch between pods presents as `peer` at zero with errors **also** at zero, exactly like a
> healthy single replica: observations are keyed by `chain | apiInterface | sha256(url)`, so pods that
> disagree about an endpoint's URL by even one character publish into separate keys and never see each
> other. If peers should be sharing and are not, confirm every pod polls the byte-identical URL before
> suspecting the cache.
>
> Errors are advisory — every failure degrades to a local poll — so a low, steady rate costs cadence
> and nothing else.
>
> Turning fork detection off can make `rpc_endpoint_fetch_latest_success` **increase** on
> endpoints with flaky hash fetches. With it on, a failed hash fetch aborts the poll cycle before
> the new-block callback fires, so new blocks stop being counted at all; without the hash step
> they are recorded again.
>
> Like `rpc_endpoint_fetch_*`, this counter fans out across every provider configured on a shared
> URL, so `endpoint_id` carries a provider name. Group by `endpoint_id` when reading it — a bare
> `sum()` counts one physical request once per provider sharing that URL.

> **Reading cancelled relays.** A stateful method (`stateful: 1` in the spec — e.g.
> `eth_sendRawTransaction`, Solana `sendTransaction`) is broadcast to *every* endpoint; the
> first response wins and the rest are cancelled. A healthy endpoint under write traffic will
> therefore show a high `rpc_endpoint_total_cancelled` rate **by design** — with N endpoints,
> roughly `(N-1)/N` of every broadcast. That is the number to watch when tuning broadcast
> fan-out; it is not a fault signal.
>
> The two counters partition the non-success outcomes: `total_errored` is the endpoint's
> fault, `total_cancelled` is ours. Cancelled relays still decrement
> `rpc_endpoint_requests_in_flight`, and never move the `smartrouter_requests_*` family, so
> `requests_total == requests_success + requests_failed` remains exact.
>
> Measured on a 3-endpoint SOLANAT router (MAG-2648): 90 stateful broadcasts produced 103
> serviced + 167 cancelled = 270 relays = 90 × 3, with zero errored.

### Optimizer

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `rpc_optimizer_selection_score` | Gauge | `spec`, `endpoint_id`, `score_type` | Periodic optimizer selection score per provider, by `score_type`. |

### Router-scoped — `smartrouter_*`

#### Core relay & health

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_total_relays_serviced` | Counter | `spec`, `apiInterface`, `function` | Relays served by the router. |
| `smartrouter_total_errored` | Counter | `spec`, `apiInterface`, `function` | Errored relays. |
| `smartrouter_end_to_end_latency_milliseconds` | Histogram | `spec`, `apiInterface`, `function` | Router-level end-to-end latency. |
| `smartrouter_overall_health` | Gauge | — | Overall router health (1 / 0). |
| `smartrouter_overall_health_breakdown` | Gauge | `spec`, `apiInterface` | Per-chain/interface health. |
| `smartrouter_latest_block` | Gauge | `spec`, `apiInterface` | Latest block known to the router. |
| `smartrouter_protocol_version` | Gauge | `version` | Encoded protocol version. |

#### WebSocket

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_ws_connections_active` | Gauge | `spec`, `apiInterface` | Active WebSocket connections. |
| `smartrouter_ws_subscriptions_total` | Counter | `spec`, `apiInterface` | Total WebSocket subscription requests. |
| `smartrouter_ws_subscription_errors_total` | Counter | `spec`, `apiInterface` | Failed WebSocket subscription requests. |

#### Request breakdown

`requests_total` = `success` + `failed`. `read`/`write` partition by statefulness;
`debug_trace` and `archive` are orthogonal addon flags; `batch` is mutually exclusive
with read/write.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_requests_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | All requests. |
| `smartrouter_requests_success_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Successful requests. |
| `smartrouter_requests_failed_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Failed requests. |
| `smartrouter_requests_read_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Read (stateless) requests. |
| `smartrouter_requests_write_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Write (stateful) requests. |
| `smartrouter_requests_debug_trace_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Debug/trace addon requests. |
| `smartrouter_requests_archive_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Archive requests. |
| `smartrouter_requests_batch_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Batch requests. |

#### Batch requests and the `method` label

For a batch request there is no single method, so the `method` label carries a
**batch signature**: the sorted set of distinct sub-methods, prefixed with `batch:`.

```
[eth_call ×30, eth_getBalance]   →   method="batch:eth_call+eth_getBalance"
[eth_call ×3]                    →   method="batch:eth_call"
```

Order and repetition are deliberately dropped. They are what a raw joined label would
encode, and they make cardinality unbounded — order contributes permutations, repetition
contributes one series per batch length. The set is the part that identifies the *type*
of batch, and it is what client code actually determines. The magnitude that repetition
used to encode moves to `smartrouter_batch_size`.

Because the signature carries the method label on the whole request-breakdown family,
per-batch-type success rate, latency, and provider breakdown all work:

```promql
# error rate per batch type
sum by (method) (rate(smartrouter_requests_failed_total{method=~"batch:.*"}[5m]))
  / sum by (method) (rate(smartrouter_requests_total{method=~"batch:.*"}[5m]))
```

Single-method requests are untouched — `method="eth_call"` means exactly what it always
did, so a `method=~"batch:.*"` / `method!~"batch:.*"` pair cleanly splits batch from
single traffic.

Two bounds keep the label space finite. A spec may register at most **64** distinct
signatures, and one signature may name at most **8** distinct methods; anything beyond
either lands in `method="batch:other"`. Both are declared in
[`batch_method_label.go`](../protocol/metrics/batch_method_label.go).

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_batch_size` | Histogram | `spec`, `apiInterface` | Sub-requests per batch. Single-element batches produce no separator in the api name, so they are indistinguishable from single requests here and are not observed. |
| `smartrouter_batch_signature_overflow_total` | Counter | `spec`, `reason` | Normalizations that fell into `batch:other`. `reason="cap"`: the spec exhausted its 64-signature budget. `reason="wide"`: one batch spanned more than 8 distinct methods. |

**Alert on the overflow counter.** While it is zero, batch-type breakdowns are complete.
Once it is non-zero they are lossy — some batch types are being merged into `batch:other`
— which is a signal to raise the cap, not to distrust the other series.

#### Errors

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_node_errors_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Node errors returned by endpoints. |
| `smartrouter_protocol_errors_total` | Counter | `spec`, `apiInterface`, `provider_address`, `method` | Protocol/transport errors (connection/session failures). |

#### Retries

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_retries_total` | Counter | `spec`, `apiInterface`, `method` | Retry attempts triggered (beyond the first try). |
| `smartrouter_retries_success_total` | Counter | `spec`, `apiInterface`, `method` | Retried requests that succeeded. |
| `smartrouter_retries_failed_total` | Counter | `spec`, `apiInterface`, `method` | Retried requests that failed. |
| `smartrouter_retry_attempts` | Histogram | `spec`, `apiInterface`, `method` | Attempts per retried request (buckets 1…10). |

#### Rate-limit hold-off

Emitted by the [hold-off registry](RATE-LIMIT-HOLDOFF.md) itself, so every consumer path
(re-verify, hot path, recovery probe, ws subscriptions) is covered without wiring. A
rate-limited relay releases its session with no QoS sample in either direction, so a
vendor cap does not show up in `rpc_endpoint_selection_score` — this pair is the direct
signal that an upstream is refusing us for load.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_rate_limit_holdoffs_total` | Counter | `provider`, `event` | Registry events. `event` is the closed set `recorded` (a 429 held an endpoint off), `escalated` (the hold-off widened to the whole provider name — counted once on the transition, not per refresh), `cleared` (an answer dropped a standing penalty — not counted when there was nothing to clear). |
| `smartrouter_rate_limit_holdoff_seconds` | Histogram | `provider` | Applied hold-off duration per `recorded` event, in **seconds** (buckets `15, 30, 60, 120, 300, 600, 1800, 3600` — not the shared millisecond latency buckets). Shows upstream `Retry-After` magnitudes against the exponential default. |

`provider` is the configured provider name on the relay / probe / re-verify paths. The
ws-subscription path has no provider name and keys the registry by node URL; those keys
are reduced to `scheme://host` before they become a label, because node URLs can embed
API keys in their path or query and a credential must never reach a Prometheus series.

```promql
# upstreams currently tripping their caps
sum by (provider) (increase(smartrouter_rate_limit_holdoffs_total{event="recorded"}[15m])) > 0

# a vendor-wide cap: the hold-off escalated across a provider's URLs
increase(smartrouter_rate_limit_holdoffs_total{event="escalated"}[1h]) > 0

# how long upstreams are telling us to wait (p90)
histogram_quantile(0.9,
  sum by (provider, le) (rate(smartrouter_rate_limit_holdoff_seconds_bucket[1h])))
```

#### Consistency

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_consistency_total` | Counter | `spec`, `apiInterface`, `method` | Requests enforcing consistency (seenBlock). |
| `smartrouter_consistency_success_total` | Counter | `spec`, `apiInterface`, `method` | Consistency-enforced requests that succeeded. |
| `smartrouter_consistency_failed_total` | Counter | `spec`, `apiInterface`, `method` | Consistency-enforced requests that failed. |

#### Hedging

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_hedge_total` | Counter | `spec`, `apiInterface`, `method` | Hedge (batch-ticker) relays sent. |
| `smartrouter_hedge_success_total` | Counter | `spec`, `apiInterface`, `method` | Hedged requests that succeeded. |
| `smartrouter_hedge_failed_total` | Counter | `spec`, `apiInterface`, `method` | Hedged requests that failed. |
| `smartrouter_hedge_attempts` | Histogram | `spec`, `apiInterface`, `method` | Hedge relays per request (buckets 1…10). |

#### Cross-validation

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_cross_validation_requests_total` | Counter | `spec`, `apiInterface`, `method` | Cross-validated requests. Includes request-time structural fail-fasts (`insufficient-capacity` / `insufficient-groups`) that abort **before fanning out to any provider**, so this is *not* the same as the number of provider fan-outs. |
| `smartrouter_cross_validation_success_total` | Counter | `spec`, `apiInterface`, `method` | Requests that reached consensus. |
| `smartrouter_cross_validation_failed_total` | Counter | `spec`, `apiInterface`, `method` | Requests that did not return a consensus answer — quorum-time failures (no-agreement / diversity / per-group) **and** request-time structural fail-fasts that never tried (`insufficient-capacity` / `insufficient-groups`). `requests_total == success_total + failed_total`. |
| `smartrouter_cross_validation_provider_agreements_total` | Counter | `spec`, `apiInterface`, `method`, `provider_address` | Times a provider agreed with consensus. |
| `smartrouter_cross_validation_provider_disagreements_total` | Counter | `spec`, `apiInterface`, `method`, `provider_address` | Times a provider disagreed with consensus. |
| `smartrouter_cross_validation_mismatch_total` | Counter | `spec`, `apiInterface`, `method`, `group`, `finality` | **Content outliers** by group: one increment **per distinct outlier group per successful deterministic cross-validation request** (a response whose `SHA256(reply.data)` diverged from the reached consensus) — not a per-provider counter. Only emitted when a quorum was reached; quorum failures and node/protocol errors are excluded (failures report a `lava-cross-validation-failure-reason` instead). `finality` is `finalized` / `not_finalized` / `unknown`; post-finality divergence is the high-signal alert. Bounded cardinality (keyed by operator-defined `group`, not provider address). |
| `smartrouter_cross_validation_failures_total` | Counter | `spec`, `apiInterface`, `method`, `reason` | **Failures by reason** — the by-reason breakdown of `cross_validation_failed_total` (which stays unlabeled, so existing dashboards are unaffected). `reason` is the closed `lava-cross-validation-failure-reason` enum: quorum-time `no-agreement` / `insufficient-responses` / `diversity-unmet` / `group-quorum-unmet`, or request-time structural `insufficient-capacity` / `insufficient-groups`. Use it to separate a structural failure (client should fall back) from a quorum disagreement (a retry may help). Bounded cardinality (the reason set is a closed enum). |
| `smartrouter_cross_validation_straggler_total` | Counter | `spec`, `apiInterface`, `method`, `outcome` | **Straggler resolutions** (MAG-2187): providers still in flight when the quorum early-exit built the reply (the `lava-cross-validation-pending-providers` header), classified against the reached consensus once their late response lands. `outcome` is the closed enum `agreed` / `disagreed` / `node-error` / `protocol-error` / `not-received` (nothing arrived before the watcher deadline). A late confirmed `disagreed` on a deterministic method also increments `_mismatch_total` for the straggler's group — deduped so the once-per-distinct-group-per-request contract holds across the reply-time and straggler paths combined. Only emitted when a quorum was reached. Bounded cardinality (closed outcome enum). |

> The `_mismatch_total` series is the group-level alerting surface for [outliers](../protocol/rpcsmartrouter/README.md#outlier-behavior). When enough providers still agree, a divergent provider is outvoted and recorded here; per-provider detail lives in the `cross-validation outlier detected` info log and the `lava-cross-validation-disagreeing-providers` header. Dissenters that answered *after* the quorum closed reach it through the [straggler path](../protocol/rpcsmartrouter/README.md#straggler-behavior) (`cross-validation straggler resolved` info log + `_straggler_total`).

#### Cache

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_cache_requests_total` | Counter | `spec`, `apiInterface`, `method` | Cache lookup attempts. |
| `smartrouter_cache_success_total` | Counter | `spec`, `apiInterface`, `method` | Cache hits. |
| `smartrouter_cache_failed_total` | Counter | `spec`, `apiInterface`, `method` | Cache misses. |
| `smartrouter_cache_latency_milliseconds` | Histogram | `spec`, `apiInterface`, `method` | Cache lookup latency. |

#### CSM state-store sizes (diagnostics)

Expose otherwise black-box Consumer-Session-Manager state so integration tests can
assert `/debug/reset-all` emptied each store (see MAG-1762). All drop to `0` after a reset.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_csm_blocked_providers` | Gauge | `spec`, `apiInterface` | Size of the previous-epoch blocked-providers store. |
| `smartrouter_csm_blocked_backup_providers` | Gauge | `spec`, `apiInterface` | Size of the blocked-backup-providers store. |
| `smartrouter_csm_sticky_sessions` | Gauge | `spec`, `apiInterface` | Live sticky-session affinities. |
| `smartrouter_csm_reported_providers` | Gauge | `spec`, `apiInterface` | Size of the reported-providers register. |

#### Serving tier (availability)

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_endpoint_serving_tier` | Gauge | `spec`, `apiInterface` | Which provider tier the endpoint is serving from: `2` = primaries, `1` = degraded (backups only), `0` = dark (no healthy providers). |

Before MAG-2525 an endpoint with no healthy provider exited the process, so a
CrashLoopBackOff was the de-facto alert. It now boots and reports unhealthy instead,
which is what makes a restart during a failover survivable — and it means **this gauge,
not pod restarts, is the signal that a chain cannot serve**. It is republished from
every path that mutates the live pairing (boot, background retry, epoch
promote/demote, config reload), so it tracks the current state rather than the boot
verdict.

Suggested alerts:

| Condition | Meaning |
| --- | --- |
| `smartrouter_endpoint_serving_tier == 0` | Endpoint is dark — all relays 5xx. Page. |
| `smartrouter_endpoint_serving_tier < 2` | Serving on backups only. Redundancy is gone; the next failure is an outage. |

Sizing the `for:` window: recovery from `0` is not one cadence. A chain that was **dark
at boot** is retried on an adaptive schedule starting at ~2s and doubling to a 3m
ceiling, so it typically self-heals in seconds. A chain that booted healthy and was
later **demoted to dark** has no retry loop — demoted providers are not fed into the
failed-provider lists — and is only re-checked by the 15m epoch re-verifier. Alert
windows should assume the 15m case.

---

## Shared metrics

### Classified errors — `smartrouter_errors_*`

Defined in [`error_metrics.go`](../protocol/metrics/error_metrics.go).

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `smartrouter_errors_total` | Counter | `error_name`, `error_category`, `retryable`, `chain_id` | Errors classified by name, category, retryability, and chain. |
