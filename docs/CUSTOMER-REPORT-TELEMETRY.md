# Customer request telemetry (MAG-4276)

This schema supports weekly customer reports. It does not generate reports,
PDFs, schedules or customer deliveries. All implementation lives in smart-router.

## Request and outcome contract

A customer request is one logical request accepted by a client-facing chain
listener, before parsing or provider selection. JSON-RPC batch envelopes count
once. Each incoming WebSocket application message counts once, including
subscribe/unsubscribe and admission rejections. WebSocket upgrade, ping/pong and
server-pushed notifications do not count. gRPC unary calls count once; a
subscription or reflection stream counts its establishment once. A stream's
later deliveries, cancellation, restoration and upstream termination do not
retroactively revise its establishment outcome.

A server-owned `request_kind` distinguishes `customer`, `internal` and
`notification`. Request headers and API method names cannot set that identity.
Router-crafted health/init/verification, upstream reflection and replay probes
have no customer lifecycle. A customer's call to a method also used by a probe
still counts. Dedicated listener health endpoints and CORS preflight are outside
the chain-request contract. Gateway rejections and gRPC transport failures before
callback dispatch require ingress telemetry; they are not router relay outcomes.

Receipt is recorded synchronously. Completion is the listener's final relay
verdict, after error classification (including gRPC status conversion), before
handing the response to the transport. It describes the router's resolved
response, not proof that the client's network received every byte.

- `success`: the final response is successful at the chain API level.
- `failure`: parsing/admission/no-provider/timeout/final node or transport failure.
  HTTP 200 with a node error is a failure. A batch with any failed item is a
  failed envelope; `partial_batch=true` identifies mixed successes/failures.
  This reporting definition does not change `--batch-node-error-on-any` routing.
- `cancelled`: the request ends with a cancellation error. Cancellation of a
  detached hedge loser is only attempt evidence, never the customer's verdict.
- An unknown write is `failure` for reporting, with `write_outcome="unknown"`:
  this means no confirmed success, **not** proof the transaction was unexecuted.
  Existing broadcast/retry/read-your-writes rules and client wording are retained.

`recovery` is an exclusive value on each terminal record:

| Value | Evidence |
| --- | --- |
| `none` | No recovery, or final outcome was not successful |
| `retry` | An observed failed provider and a different successful final provider |
| `reroute` | An actual selection bypassed an unavailable intended provider or an applicable blocked primary pool, and the selected alternative served successfully |
| `both` | Both kinds of evidence on the same successful customer request |

Recovered requests = successful `retry` + `reroute` + `both` records. `both`
counts once. Normal load balancing, capability filtering, unrelated blocked
providers, healthy parallel attempts, pending attempts and cancelled losers are
not recovery evidence. Cache serving is reported separately from provider
recovery. Do not reconstruct recovery by multiplying provider downtime by total
traffic or by treating attempts-minus-one as failed attempts.

Reroute instrumentation reads existing causal CSM decisions: all applicable
primaries blocked/unavailable -> backup, unavailable local sticky pin -> alternative, and
unavailable preferred provider -> alternative. It does not invent a provider
that the optimizer would have chosen before filtering. Partial-pool outages
without an explicit intended provider therefore do not claim counterfactual
recoveries. Subscription setup records its serving endpoint. A configured primary
subscription tier that is unavailable, followed by successful backup setup, is
also a reroute; an intentionally backup-only configuration is not. The configured
intent survives dark startup. These conservative boundaries avoid attributing
routine backup selection to an outage. They must be preserved in client-facing explanations.

## Prometheus

The existing `smartrouter_requests_*`, `rpc_endpoint_*`, cache lookup and incident
metrics retain their original behavior. They describe provider attempts,
lookups or incidents and must not be added to the new request counters.

| Metric | Native labels |
| --- | --- |
| `smartrouter_customer_requests_received_total` | `spec`, `apiInterface` |
| `smartrouter_customer_request_outcomes_total` | `spec`, `apiInterface`, `outcome`, `recovery`, `cache_hit`, `error_name` |
| `smartrouter_customer_telemetry_info` | `schema_version="1"` (value 1) |

`outcome` and `recovery` use the bounded values above. `cache_hit` is `true` or
`false`; `error_name` is an existing registry identity, or `none` on success.
Request IDs, provider names/lists, methods and raw messages are not metric labels.
The collector may attach `cluster`, customer, pod and chain labels; the router
has no customer identifier metric label. In shared deployments, tenant-specific
reports require filtering `relay_usage.project` or an ingress/collector tenant
partition. A deployment-wide Prometheus series cannot separate those tenants.

The examples assume the existing collector's `cluster` label uniquely identifies
the customer's deployment. Evaluate at the report end time with the same 7-day
window. Retain `sum by (spec)` for chain rows; omit it for global totals.

Received:

```promql
sum by (spec) (increase(smartrouter_customer_requests_received_total{cluster="$cluster"}[7d]))
```

Completed (the denominator), successful, recovered, cache-served and final failures:

```promql
sum by (spec) (increase(smartrouter_customer_request_outcomes_total{cluster="$cluster"}[7d]))
sum by (spec) (increase(smartrouter_customer_request_outcomes_total{cluster="$cluster",outcome="success"}[7d]))
sum by (spec) (increase(smartrouter_customer_request_outcomes_total{cluster="$cluster",outcome="success",recovery=~"retry|reroute|both"}[7d]))
sum by (spec) (increase(smartrouter_customer_request_outcomes_total{cluster="$cluster",cache_hit="true"}[7d]))
sum by (spec) (increase(smartrouter_customer_request_outcomes_total{cluster="$cluster",outcome="failure"}[7d]))
```

Success rate = successful/completed * 100. Report cancellations separately; they
remain in the denominator. Customer cache rate = cache-served/completed * 100.
The old cache lookup hit rate remains hits/lookups and answers a different
question because attempts can consult more than one tier. Cache-served node-error
responses have `cache_hit=true` and `outcome=failure`.

A zero denominator means no completed requests, not 100% success. A missing
success/recovery/failure series can be filled with zero **only** after verifying
schema coverage for every participating pod. Verify it by joining the info gauge
with expected running router targets (`up`/deployment inventory). In a mixed
rollout, an absent marker on a pod is missing coverage, not zero customer traffic.

Receipt and completion use different timestamps. A request spanning the weekly
boundary can belong to different windows. After draining a test workload:
`received = success + failure + cancelled`, `recovered <= success`, and the sum of
chain rows equals global totals. Do not require receipt and completion counts to
match in every arbitrary time window.

Prometheus `increase` is scrape-based and extrapolated; resets, short-lived pods,
new label series and scrape gaps can lose or estimate counts. It is not an exact
billing ledger. For exact window membership use persisted terminal snapshots'
`started_at_ns` / `completed_at_ns`, deduplicate by deployment + GUID and audit
collection gaps. No historical backfill is created by deploying this schema.

## Canonical terminal log and OTel

The existing INFO `relay finished` lifecycle is resolved at the listener, after
unknown-write and API-level verdicts. Earlier processor diagnostics are DEBUG
`relay decision`. Parse/admission failures now also get a terminal INFO record.

The top-level log fields include `schema_version`, `terminal`, `request_kind`,
`GUID`, the existing request-id key, `chain`, `api_interface`, `api_method`,
`outcome`, `recovery`, `cache_hit` and `error_name`. The repository log formatter
renders attributes as strings. `customer_request` contains a JSON-encoded
snapshot string with typed fields, start/completion/duration, provider/cache tier,
stop reason, registry code/category/retryability, bounded redacted error text,
observed attempt outcomes and causal reroute decisions. A pending attempt means
no recorded result at completion, not a failed provider. Snapshot arrays are
copied and closed before export; later hedge/CV results cannot change them.

`relay_usage` carries the same snapshot plus its existing project/CU/classification
fields. Existing `cache_hit`, `provider` and `success` are populated from the final
verdict. Notifications preserve their usage/billing events, explicitly marked
`request_kind="notification"` without a terminal snapshot. The optional OTel sink
also exports flattened terminal identity/outcome/recovery attributes. Prometheus
counting and stdout INFO records do not depend on OTel being enabled.

Example Loki final failures (adapt stream selector to the deployed labels):

```logql
{cluster="$cluster"} | json | message="relay finished" | terminal="true" | request_kind="customer" | outcome="failure"
```

Top error families, counting canonical terminal records rather than attempt logs:

```logql
topk(5, sum by (error_name) (count_over_time({cluster="$cluster"} | json | message="relay finished" | terminal="true" | request_kind="customer" | outcome="failure" [7d])))
```

To inspect the nested snapshot:

```logql
{cluster="$cluster"} | json | terminal="true" | outcome="failure" | line_format "{{.customer_request}}" | json
```

Use GUID and the propagated request ID to join each final failure to existing
classified attempt logs, disabled-provider/hold-off/pool-state events. Count
affected customer GUIDs, not error lines. Describe observed failure families,
providers, chains and time ranges; terminal logs alone do not prove a root cause.
Ingestion that duplicates records needs deduplication before incident counts.

## Deployment work outside this repository

1. Scrape and retain all three new metrics in Alloy/Prometheus/Mimir. Preserve
   native `spec` and `apiInterface` and the customer/deployment identity.
2. Collect unsampled INFO terminal logs, retain the JSON snapshot and request IDs,
   and check Loki line/attribute limits. Verify retention covers the report window.
3. Update existing `loki:sr_final_*` recording rules to select the canonical
   customer terminal record once. Do not combine old ERROR events with new
   terminal events; node errors, parse errors and unknown writes would duplicate.
4. If OTel usage is enabled, verify collector routing and schema preservation.
   Export is best-effort with bounded queues; delivery is not guaranteed by the
   router counter. Audit missing events separately from zero failures.
5. Deploy and verify new-schema coverage across all customer pods before switching
   weekly dashboards. Keep old-version periods explicitly incomplete.
6. Verify a drained known workload end to end: ingress -> router -> metric scrape
   -> Mimir and stdout/OTel -> Loki, reconcile by chain and unique customer GUID.
   Gateway-only rejections remain an explicitly separate ingress metric.

Infrastructure configuration, deployed recording rules and customer dashboards
must be changed in their owning repositories. This router PR does not claim that
new telemetry is already present in production Grafana or Loki.
