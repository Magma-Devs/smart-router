# Selection policies

Smart Router doesn't pick upstreams randomly. Every relay flows through a QoS-weighted selector that scores providers on latency, sync freshness, and reliability — then picks one according to the **strategy** you configure.

## How selection works

1. The chain parser identifies the request's category (latest vs. archive, heavy vs. light, free vs. paid).
2. The provider optimizer narrows the pool to upstreams that support the relevant API interface and category.
3. The weighted selector scores each candidate on:
    - **Latency** — recent observed response times.
    - **Sync** — how close the provider is to the chain head.
    - **Availability** — recent error and timeout rates.
4. The active strategy adjusts those weights (see below).
5. A weighted-random pick is made, with a configurable minimum-selection floor that keeps every healthy provider in rotation.

Source: [`protocol/provideroptimizer/provider_optimizer.go`](https://github.com/Magma-Devs/smart-router/blob/main/protocol/provideroptimizer/provider_optimizer.go), [`weighted_selector.go`](https://github.com/Magma-Devs/smart-router/blob/main/protocol/provideroptimizer/weighted_selector.go).

## Strategies

| Strategy | Optimizes for | When to use |
|---|---|---|
| `Balanced` *(default)* | latency + sync + availability | most workloads |
| `Latency` | lowest response time | latency-sensitive reads (UI, fast quote) |
| `SyncFreshness` | provider closest to chain head | indexers, mempool watchers |
| `Accuracy` | response correctness (favors providers passing cross-validation) | financial / critical reads |
| `Distributed` | spread across providers | avoiding hot spots, fairness |
| `Cost` | cheapest per compute unit | high-volume non-critical traffic |
| `Privacy` | provider diversity per stream | minimising correlation across calls |

## Per-request override

Pin a single request to a specific provider with the `Lava-Provider-Address` header. The optimizer is bypassed for that request; failover policies still apply. See [Directives](../../api/directives.md).

## Related

- [Failover & retry](../failover/index.md) — what happens when the picked provider fails or returns suspect data
- [Hedge](../failover/hedge.md) — race two providers in parallel
- [Consensus](../failover/consensus.md) — require agreement across providers
