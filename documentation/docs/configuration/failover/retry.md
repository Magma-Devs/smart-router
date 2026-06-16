# Retry

When an upstream returns a **retryable** error, Smart Router rotates to a different provider and tries again. The pool is filtered to providers that haven't already failed for this relay; the [selection policy](../projects/selection-policies.md) picks the next candidate.

## What's retryable

The error classifier ([`protocol/common/error_classifier.go`](https://github.com/Magma-Devs/smart-router/blob/main/protocol/common/error_classifier.go)) decides per-error:

| Class | Examples | Behaviour |
|---|---|---|
| **Retryable** | network timeout, 5xx upstream response, RPC `Server error` codes, transient JSON-RPC errors | rotate provider, retry |
| **Terminal** | client errors (4xx), bad request shape, signed-tx already-known | return to caller immediately |
| **Unsupported method** | upstream doesn't expose this method (`-32601`, "method not found") | non-retryable; surface to caller |

Same-response retries are deduplicated: if two providers return the identical response, Smart Router won't burn a third attempt looking for a different answer. The dedup is a hash cache in [`protocol/lavaprotocol/relay_retries_manager.go`](https://github.com/Magma-Devs/smart-router/blob/main/protocol/lavaprotocol/relay_retries_manager.go).

## Limits

| Limit | Value | Notes |
|---|---|---|
| Max attempts per relay | 10 | hardcoded constant `MaximumNumberOfTickerRelayRetries` |
| Overall budget | `--default-processing-timeout` | ends retries even if attempts remain |
| Per-attempt budget | `--min-relay-timeout` floor, or `lava-relay-timeout` header | retries get the same per-attempt timeout |

The 10-attempt cap is not currently exposed as a YAML knob.

## When retries kick in

Retries are **always on** for retryable errors. There's no opt-out for individual relays. If you want a single attempt with no retry on failure, the closest control is to lower the overall `--default-processing-timeout` and accept that the first failure surfaces immediately — but you'll lose other failover behaviour at the same time.

## When retries don't help

Some classes of failure look retryable on the wire but won't recover by switching providers — for example:

- A bad request (malformed JSON, unknown method) — always terminal.
- A consensus-level error returned by every healthy provider (the chain itself rejected it).
- A method your providers genuinely don't support.

The classifier handles these cases without burning the budget.

## Pinning to one provider

The `Lava-Provider-Address` header pins the request to a specific upstream. If that upstream fails, retry kicks in **on the rest of the pool** — pinning isn't a way to disable retry. See [Directives](../../api/directives.md).

## Observability

| Metric | Meaning |
|---|---|
| `smartrouter_relay_retries_total` | total retries attempted |
| `smartrouter_relay_retries_per_request` | distribution of retry counts per relay |
| `incident_retry_*` | per-incident retry telemetry (Kafka analytics) |
| Tracing | each retry attempt is a span; correlate via the trace ID in response headers |
