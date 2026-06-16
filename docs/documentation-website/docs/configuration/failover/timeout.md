# Timeout

Two timeouts apply to every relay: an **overall budget** for the whole request, and a **per-attempt budget** for each upstream try. Hedging and retries operate inside these budgets.

## The two timeouts

| Timeout | Default | Set by |
|---|---|---|
| Overall (whole relay) | implementation default | `--default-processing-timeout` CLI flag |
| Per-attempt (each upstream try) | `1s` floor | `--min-relay-timeout` CLI flag, or `lava-relay-timeout` header |

If both are set:

- The **overall** timeout caps the entire relay including all retries and hedges. When it fires, the client gets whatever's most useful (the best partial response, or a timeout error).
- The **per-attempt** timeout aborts a single upstream try and lets [retry](retry.md) move on. It's the more important knob for tuning tail latency.

## The header override

Clients can override the per-attempt timeout for a specific request:

```
lava-relay-timeout: 12s
```

Format: any Go duration (`500ms`, `5s`, `1m30s`). The override is subject to the `--min-relay-timeout` floor — you can ask for less but the floor applies. Use this for known-slow methods (`debug_traceTransaction` on a deep block) without raising the global default.

See [Directives](../../api/directives.md).

## When to tune

| Symptom | Adjust |
|---|---|
| p99 latency dominated by one slow attempt | lower `--min-relay-timeout` (faster failover) |
| Heavy methods (`debug_*`) always timing out | raise `lava-relay-timeout` per-request, not the global floor |
| Whole-relay timeouts in logs | raise `--default-processing-timeout` or investigate why retries aren't succeeding |
| Lots of clients hitting timeouts on first attempt | raise `--min-relay-timeout` |

## Common pitfalls

- **Setting per-attempt > overall.** Doesn't crash, but means the first attempt's timeout is effectively the overall budget and retry never gets to run.
- **Setting per-attempt below upstream RTT.** Every attempt times out before a healthy provider can respond. Watch the metrics; if your timeout-rate is near 100% even on healthy upstreams, raise the floor.
- **Header override below the floor.** The floor wins. If a client sends `lava-relay-timeout: 100ms` and your floor is `1s`, the attempt gets `1s`.

## Observability

| Metric | Meaning |
|---|---|
| `smartrouter_relay_timeout_total` | total timeouts (per-attempt and overall combined) |
| `smartrouter_relay_attempt_duration_seconds` | per-attempt duration histogram |
| `smartrouter_relay_total_duration_seconds` | end-to-end relay duration histogram |
| Tracing | each attempt span has a duration and an outcome status |
