# Consensus

Cross-validation: send the **same request** to multiple providers in parallel and require **agreement** before returning the response. Catches one-off lying or buggy providers before bad data reaches your client.

## How it works

When cross-validation is active for a relay:

1. Smart Router fans the request out to `MaxParticipants` providers in parallel.
2. Responses are collected as they arrive.
3. As soon as `AgreementThreshold` providers return matching responses, that response is returned.
4. If providers disagree past the timeout, the relay fails or returns the most-agreed answer (depending on configuration).

![Cross-validation consensus — Smart Router fans out to three providers, two return matching response A, one returns disagreeing response B, threshold of two met](../../assets/diagrams/consensus.svg)

| Parameter | Meaning |
|---|---|
| `MaxParticipants` | how many providers to query in parallel |
| `AgreementThreshold` | how many must match before the response is accepted |

Default values are `{MaxParticipants: 1, AgreementThreshold: 1}` — i.e., effectively disabled. Cross-validation is opt-in per relay rather than always-on, because it multiplies upstream cost.

## When to use it

| Scenario | Cross-validate? |
|---|---|
| Critical writes (token transfers, contract calls) | yes — but check responses, not just submission acks |
| Indexer-style reads of finalized data | optional — cache hit rate already absorbs most cost |
| `eth_getLogs` results that downstream code depends on | yes — providers commonly disagree on log ordering |
| `debug_*` traces | yes if you don't trust a single provider |
| `eth_call` against contracts at fixed block | yes — deterministic, easy to compare |
| `eth_call` against pending block | no — non-deterministic by definition |
| `eth_blockNumber` / latest block reads | no — providers will naturally disagree by 1 block |

## Configuration

The cross-validation parameters live in [`protocol/common/types.go`](https://github.com/Magma-Devs/smart-router/blob/main/protocol/common/types.go) as `CrossValidationParams`. Activation is currently driven by the chain spec's `Selection` value rather than a YAML knob; if you need to enable it for a chain that doesn't have it on by default, that's a chain-spec-level change.

A future release will expose `MaxParticipants` and `AgreementThreshold` as YAML knobs per project / network / method.

## Trade-offs

- **Cost**: a request with `MaxParticipants: 3` costs 3× upstream calls. Pair with caching so the multiplier doesn't apply to cache hits.
- **Latency**: the response is gated on the slowest of the agreeing providers. Combine with [hedging](hedge.md) to mitigate.
- **Determinism**: the comparator is response-shape-aware (it knows array order can be normalized for some methods, that some fields are timestamp-based, etc.). Don't expect raw byte equality.

## Difference from integrity

- [**Integrity**](integrity.md) catches *out-of-sync* providers before the relay is sent (pre-request lag check).
- **Consensus** catches *wrong* responses by comparing across providers after they return.

You can run both. They aren't mutually exclusive.

## Observability

| Metric | Meaning |
|---|---|
| `cross_validation_*` | participation, agreement, disagreement counts |
| `url_fanout_*` | per-request fan-out telemetry |
| Tracing | each fan-out attempt is a parallel span; the comparator's verdict is a span event |
