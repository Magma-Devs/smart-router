# Customer request telemetry (MAG-4276)

This schema supports weekly customer reports. It does not generate reports,
PDFs, schedules or customer deliveries. All implementation lives in smart-router.

## Request and outcome contract

A customer request is one logical request accepted by a client-facing chain
listener, before parsing or provider selection. JSON-RPC batch envelopes count
once. Each incoming WebSocket application message counts once, including
subscribe/unsubscribe and a frame the connection's rate limit refuses. WebSocket
upgrade, ping/pong and server-pushed notifications do not count. gRPC unary calls
count once; a subscription or reflection stream counts its establishment once. A
stream's later deliveries, cancellation, restoration and upstream termination do
not retroactively revise its establishment outcome.

A server-owned `request_kind` distinguishes `customer`, `internal` and
`notification`. Request headers and API method names cannot set that identity.
Router-crafted health/init/verification, upstream reflection and replay probes
have no customer lifecycle. A customer's call to a method also used by a probe
still counts. Dedicated listener health endpoints and CORS preflight are outside
the chain-request contract, and so is an HTTP request a listener refuses before
parsing because it does not serve the method (answered 405): nothing in it is a
chain request, and `relay_end` writes nothing for it either. Gateway rejections
and gRPC transport failures before callback dispatch require ingress telemetry;
they are not router relay outcomes.

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
recoveries. Subscription setup records its serving upstream under the provider
name `relay_end` writes for it. A configured primary subscription tier that is
unavailable, followed by successful backup setup, is also a reroute; an
intentionally backup-only configuration is not. The configured intent survives
dark startup. These conservative boundaries avoid attributing routine backup
selection to an outage. They must be preserved in client-facing explanations.

## Prometheus

The existing `smartrouter_requests_*`, `rpc_endpoint_*`, cache lookup and incident
metrics retain their original behavior. They describe provider attempts,
lookups or incidents and must not be added to the new request counters.

The per-customer `smartrouter_customer_requests_total`,
`smartrouter_customer_requests_failed_total` and latency histogram (MAG-4081)
also remain available. They count parsed method requests under a bounded
`customer` label and omit parse failures. Their completion reads the same
API-level final Success verdict as the terminal snapshot, so an HTTP 200 node
error is unsuccessful there too. Each parsed request updates each family once;
never add the parsed-request total to the all-request received/outcome totals.

| Metric | Native labels |
| --- | --- |
| `smartrouter_customer_requests_received_total` | `spec`, `apiInterface` |
| `smartrouter_customer_request_outcomes_total` | `spec`, `apiInterface`, `outcome`, `recovery`, `cache_hit`, `error_name` |
| `smartrouter_customer_telemetry_info` | `schema_version="1"` (value 1) |

`outcome` and `recovery` use the bounded values above. `cache_hit` is `true` or
`false`; `error_name` is an existing registry identity, or `none` on success.
Request IDs, provider names/lists, methods and raw messages are not metric labels.
The collector may attach `cluster`, customer, pod and chain labels; the new
all-request counters have no customer identifier metric label. The existing
parsed-request family has its own bounded `customer` label. In shared deployments, tenant-specific
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

## Terminal snapshot, relay_end and OTel

The router writes one log event per relay, `relay_end`
([RELAY-END-EVENT.md](RELAY-END-EVENT.md)), at every log level, and the customer
lifecycle adds no line of its own: the Loki recording rules and the customer
alert that count relays select `relay_end`, and a second line would be counted
twice. The customer request's terminal snapshot travels on the usage event
instead.

The snapshot, `customer_request`, is resolved at the listener, after the
unknown-write and API-level verdicts, for parse and admission failures as well. It
carries `schema_version`, `terminal`, `request_kind`, `GUID`, the propagated
request id, `chain`, `api_interface`, `api_method`, start/completion/duration,
`outcome`, `recovery`, `cache_hit` and cache tier, the serving provider, stop
reason, the registry's error name/code/category/retryability, bounded redacted
error text, the observed attempt outcomes and the causal reroute decisions. A
pending attempt means no recorded result at completion, not a failed provider.
Snapshot arrays are copied and closed at completion; later hedge/CV results
cannot change them. Its `GUID` is the one `relay_end` and every narrative line of
the request carry, so the two join on it.

`relay_usage` carries the snapshot plus its existing project/CU/classification
fields, and `request_kind`. Its `cache_hit`, `provider`, `success` and `latency_ms`
are populated from the final verdict; `provider` is `Cached` on a cache hit.
Pushed notifications keep their usage/billing events, marked
`request_kind="notification"` without a snapshot. The optional OTel sink exports
the snapshot as the `customer_request` attribute beside flattened `terminal`,
`outcome`, `recovery`, `error_name`, `request_id`, `GUID` and `completed_at_ns`.
Prometheus counting does not depend on OTel being enabled.

Loki drill-down reads `relay_end`. Its `outcome` is the relay's, not the
customer's: `ok` is a success; every other value is a failure for this report.
The two sets differ at the edges, on purpose: `relay_end` has no `cancelled`
outcome, and a WebSocket frame the rate limit refuses is a counted customer
failure with no `relay_end`. Final failures (adapt the stream selector to the
deployed labels):

```logql
{cluster="$cluster"} |= "\"event\":\"relay_end\"" | json | outcome!="ok" | selection!="subscription"
```

Top error families, counting relays rather than attempt lines. `error_code` on
`relay_end` is the registry name the Prometheus `error_name` label carries:

```logql
topk(5, sum by (error_code) (count_over_time({cluster="$cluster"} |= "\"event\":\"relay_end\"" | json | outcome!="ok" [7d])))
```

Use the GUID and the propagated request id to join a failure to the request's
classified attempt lines and to the disabled-provider, hold-off and pool-state
events. Count affected customer GUIDs, not error lines. Describe observed failure
families, providers, chains and time ranges; terminal records alone do not prove
a root cause. Ingestion that duplicates records needs deduplication before
incident counts.

## Deployment work outside this repository

1. Scrape and retain all three new metrics in Alloy/Prometheus/Mimir. Preserve
   native `spec` and `apiInterface` and the customer/deployment identity.
2. `relay_end` is already collected, and the Loki recording rules
   (devops-magmadevs) and the customer server-errors alert
   (smart-router-infrastructure) already read it. This schema changes neither.
3. If OTel usage is enabled, verify collector routing and schema preservation,
   including the `customer_request` attribute's size against the collector's
   attribute limits. Export is best-effort with bounded queues; delivery is not
   guaranteed by the router counter. Audit missing events separately from zero
   failures.
4. Deploy and verify new-schema coverage across all customer pods before switching
   weekly dashboards. Keep old-version periods explicitly incomplete.
5. Verify a drained known workload end to end: ingress -> router -> metric scrape
   -> Mimir, and `relay_end`/OTel -> Loki; reconcile by chain and unique customer
   GUID. Gateway-only rejections remain an explicitly separate ingress metric.

Infrastructure configuration, deployed recording rules and customer dashboards
are changed in their owning repositories. This router change does not put new
telemetry in production Grafana or Loki by itself.
