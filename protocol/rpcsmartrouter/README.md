# RPC Smart Router

The RPC Smart Router is a centralised RPC gateway that routes requests to pre-configured provider endpoints with QoS-based selection, caching, and automatic failover.

## Key Features

### Provider Configuration
- **Static Providers**: Define trusted RPC endpoints in YAML config
- **Backup Providers**: Automatic failover to backup tier when primaries fail
- **Multi-provider Support**: Mix Alchemy, Infura, self-hosted, and other providers

### Intelligent Routing
- **QoS-based Selection**: Routes to best-performing providers
- **Automatic Failover**: Seamlessly switches to backups on provider failure
- **Health Monitoring**: Continuous provider health checks
- **Strategy Options**: Balanced, latency, sync-freshness

### Features
- **Smart Caching**: Two-layer caching reduces provider load
- **Transaction Broadcasting**: Sends transactions to all providers for faster propagation
- **WebSocket Support**: Full support for subscription-based APIs
- **Metrics & Monitoring**: Prometheus metrics and health endpoints

## Configuration

Create a YAML config file (see `config/smartrouter_examples/smartrouter_lava.yml` for a full example):

```yaml
endpoints:
  - chain-id: ETH1
    api-interface: jsonrpc
    network-address: 0.0.0.0:3333

direct-rpc:
  - name: alchemy-primary
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY

  - name: infura-primary
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: https://mainnet.infura.io/v3/YOUR_KEY

backup-providers:
  - name: backup-alchemy
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: https://eth-mainnet.g.alchemy.com/v2/BACKUP_KEY
```

## Cross-Validation

Cross-validation fans a single request out to **N providers in parallel**, hashes each
successful response (`SHA256(reply.data)`), and only returns an answer once a **quorum** of
providers agree on the same hash. It defends against a single compromised or buggy provider
returning a wrong-but-well-formed answer. It is intended for **read** methods.

Cross-validation can be turned on two ways, which compose via `clamp(caller, floor, cap)`:

- **Per-request headers** (the caller opts in per call):
  - `lava-cross-validation-max-participants: N` — fan out to N providers.
  - `lava-cross-validation-agreement-threshold: M` — require M identical responses.
- **Per-method operator policy** (config-driven, below). An operator policy can *mandate*
  cross-validation even with no caller headers (`enabled: true`), set a **floor** the caller
  may exceed, a **cap** that overrides a stricter caller, or *forbid* caller-driven
  cross-validation entirely for a method (`forbid-caller-cv: true`).

> **Write / stateful methods.** An **operator policy** that enables cross-validation on a
> stateful (write) method is **rejected at startup** — that path is guarded. By default the
> legacy **caller-header** path is *not* guarded: a request that sends the headers above still
> selects cross-validation on any method, *including writes*, ahead of the normal stateful
> fan-out (backwards compatibility). To close that off for a specific method, set
> `forbid-caller-cv: true` on its policy (see below) — the router then ignores the CV headers
> and routes the method normally. Cross-validating a write *response* (e.g. a transaction hash
> echoed back) does not independently verify anything, so prefer policy-driven cross-validation
> on read methods and leave writes to the stateful path.

### Provider group labels

Tag each provider with an optional `group-label` (vendor/operator/region). Providers with no
label fold into the implicit `"default"` group. Group labels are what the **diversity** and
**per-group quorum** policies below reason about — a quorum drawn entirely from one vendor is
not the independent confirmation cross-validation is meant to give.

```yaml
direct-rpc:
  - name: drpc-eth
    chain-id: ETH1
    api-interface: jsonrpc
    group-label: "drpc"          # <-- this provider's cross-validation group
    node-urls:
      - url: https://eth.drpc.org
  - name: publicnode-eth
    chain-id: ETH1
    api-interface: jsonrpc
    group-label: "publicnode"
    node-urls:
      - url: https://ethereum-rpc.publicnode.com
```

### Per-method policies

An optional top-level `cross-validation:` block sets policy per `(chain-id, api-interface,
method)`. Omitting it entirely keeps the header-driven behavior above, fully backwards
compatible. `chain-id`/`api-interface` match case-insensitively; `method` matches exactly.
Each numeric knob is either a bare number `N` (meaning `{floor: N}`) or an object
`{floor: F, cap: C}`.

```yaml
cross-validation:
  policies:
    - chain-id: ETH1
      api-interface: jsonrpc
      method: eth_getBalance
      enabled: true                 # mandate CV here even without caller headers
      agreement-threshold: 2        # at least 2 identical responses
      max-participants: 3           # fan out to 3 providers
      min-groups: 2                 # the agreeing responses must span >= 2 distinct groups

    - chain-id: ETH1
      api-interface: jsonrpc
      method: eth_call
      enabled: true
      agreement-threshold: 2        # quorum size WITHIN each group
      min-groups: 2                 # number of groups that must each reach their own quorum
      max-participants: 4           # must be >= min-groups * agreement-threshold
      per-group-quorum: true        # see "Per-group quorum" below

    - chain-id: ETH1
      api-interface: jsonrpc
      method: eth_sendRawTransaction
      forbid-caller-cv: true        # disable CV for this method even if the caller sends CV headers
```

| Knob | Meaning |
| --- | --- |
| `enabled` | `true` mandates CV for this method even with no caller headers. |
| `forbid-caller-cv` | `true` disables CV for this method: the caller's CV headers are ignored and the method routes by its normal category. Mutually exclusive with `enabled` (rejected at startup if both set); the other knobs are ignored when set. |
| `max-participants` | How many providers to fan out to. |
| `agreement-threshold` | How many identical responses form a quorum (in per-group mode, *within each group*). |
| `min-groups` | Distinct provider groups the quorum must span (`1` = no diversity requirement). |
| `per-group-quorum` | Upgrade `min-groups` to per-group quorum (operator-only; requires `min-groups > 1`). |

There are three mutually exclusive intents for a method's policy: **mandate** CV (`enabled: true`),
**forbid** caller-driven CV (`forbid-caller-cv: true`), or neither (omit both / `enabled: false`) —
which leaves the method caller-driven (the caller's CV headers still work). A policy that sets both
`enabled` and `forbid-caller-cv` is a startup error.

**Group diversity (`min-groups`)** requires the single agreeing quorum to be returned by
providers from at least `min-groups` distinct groups. **Per-group quorum** (`per-group-quorum:
true`) is stronger: each of `min-groups` groups must *independently* reach `agreement-threshold`
matching responses, and the per-group winners must agree. This is the trusted-reference +
corroborator model — one group is the reference, others must independently corroborate it.
It requires `max-participants >= min-groups * agreement-threshold` (rejected at config-load
otherwise), and the router front-loads `agreement-threshold` providers per group during
selection so a QoS-dominant group cannot starve the others.

A policy that cannot be satisfied by the configured fleet (too few groups, or too few providers
per group for per-group quorum) is **rejected at startup**, and the resolved
provider→group layout is logged.

### Response headers

A cross-validated response that **reached the relay stage** (a quorum was attempted — whether it
succeeded or failed on disagreement) carries the full header set so a client can see what happened
without debug mode:

| Header | Value |
| --- | --- |
| `lava-cross-validation-status` | `success` or `failed`. |
| `lava-cross-validation-all-providers` | All providers queried (comma-separated). |
| `lava-cross-validation-agreeing-providers` | Providers whose response matched the consensus. |
| `lava-cross-validation-disagreeing-providers` | Providers that dissented (node/protocol errors and hash-divergent responses; on a quorum failure, every successful provider, since there is no consensus to agree with). |
| `lava-cross-validation-failure-reason` | On failure only — a stable enum (below). |

A **request-time structural fail-fast** (a capacity/diversity check that aborts *before any
upstream relay runs* — the `insufficient-capacity` / `insufficient-groups` reasons below) carries
**only** `lava-cross-validation-status: failed` and `lava-cross-validation-failure-reason`. The
provider-list headers are omitted because no providers were queried; `failure-reason` is the
discriminator the client needs. (The request still increments
`cross_validation_requests_total` / `cross_validation_failed_total`.)

The router does **not** automatically retry with a different provider set on a quorum failure;
the structured signal lets the client decide its next action. The failure headers reach clients on
all interfaces: as HTTP response headers on JSON-RPC, REST, and Tendermint, and as gRPC **trailers**
on the gRPC interface (an errored gRPC call returns a trailers-only response, so read them with the
`grpc.Trailer(&md)` call option rather than as headers).

### Failure reasons

`lava-cross-validation-failure-reason` is one of a small closed enum. **Quorum-time** reasons
mean responses came back but didn't agree — a retry against a different set *may* help:

- `no-agreement` — enough responses, but none agreed up to the threshold.
- `insufficient-responses` — fewer successful responses than the threshold.
- `diversity-unmet` — a quorum agreed but did not span `min-groups` groups.
- `group-quorum-unmet` — (per-group mode) too few groups reached their own internal quorum, or the per-group winners disagreed.

**Request-time (structural)** reasons mean the candidate set could not even be assembled — a
retry against the same router will *not* help (the fleet structurally lacks the
providers/groups), so the client should fall back:

- `insufficient-capacity` — too few candidate providers/sessions for `max-participants` or the threshold.
- `insufficient-groups` — too few candidate groups (or, for per-group, too few groups with enough providers) for `min-groups`.

### Outlier behavior

There is **no separate "outlier detection" step** for responses — an outlier is simply *a
successful response whose `SHA256(reply.data)` differs from the reached consensus hash*. The
quorum computation buckets every response by hash and the largest qualifying bucket wins; an
outlier forms its own bucket-of-one and **loses the vote**. The outlier never becomes the
returned answer, and it is **not removed by a detect-then-filter pass** — it is minority-loses,
not a pre-filter.

Whether an outlier blocks the request depends on the policy: it is excluded from the result
**as long as the agreeing providers still meet `agreement-threshold`**. Under a strict policy
like 3-of-3 (`agreement-threshold == max-participants`), a single divergent provider drops the
agreeing count below the threshold, so the request correctly **fails** with a quorum-failure
reason rather than returning a quorum that excludes the outlier.

An outlier provider is **not penalized, blocked, or deprioritized** by cross-validation (QoS
scoring is separate). It is recorded for observability only, and *only* when a quorum was
actually reached on a deterministic method:

- `smartrouter_cross_validation_mismatch_total{spec, apiInterface, method, group, finality}`
  is incremented **once per distinct outlier group** for a successful deterministic quorum — not
  once per provider. (Non-deterministic methods legitimately differ and are not counted; a quorum
  *failure* emits a `lava-cross-validation-failure-reason` instead, never this metric.) The
  `finality` label (`finalized` / `not_finalized` / `unknown`) lets alerting prioritize
  post-finality divergence over benign pre-finality propagation lag.
- Provider-level detail is available separately: each dissenting provider is emitted in an info
  log (`cross-validation outlier detected`, with provider/group/finality/hashes) and listed in the
  `lava-cross-validation-disagreeing-providers` response header.

So a single divergent provider is observed and outvoted **when enough providers still agree**;
under a strict (unanimous) policy it instead causes a quorum failure.

## Usage

```bash
smartrouter config.yml --use-static-spec specs/
```

### Common Flags

```bash
--cache-be "127.0.0.1:7778"          # Enable caching
--strategy balanced                   # Provider selection strategy
--metrics-listen-address ":7779"     # Prometheus metrics
--log_level debug                    # Log verbosity
```

### Usage telemetry (OTel)

Off by default. When enabled, the smart router emits two event types as
OTLP/HTTP logs to a host-local OpenTelemetry collector — `relay_usage` (one
per relay) and `optimizer_qos` (one per (chain, provider) per sampling tick).
The collector fans out to whatever backend(s) you choose (S3 / Kafka /
ClickHouse) via exporter YAML — no smart-router code change to swap
destinations. With `--usage-otel-enabled=false` the relay/QoS paths pay one
inlinable no-op call and nothing else.

```bash
--usage-otel-enabled                       # master switch (both event types); off by default
--usage-otel-endpoint "127.0.0.1:4318"     # OTLP/HTTP collector endpoint
--usage-otel-service-name "smartrouter"
--usage-otel-service-instance-id "$HOSTNAME-eth"  # default: hostname-pid
```

## Architecture

```
User Request --> Smart Router --> Provider Selection (QoS-based)
                      |
                Try Primary Providers
                      |
               [If all fail] --> Try Backup Providers
                      |
               Cache Response (optional)
                      |
               Return to User
```

## Failover Flow

1. **Primary Attempt**: Tries direct-rpc providers first (best QoS selected)
2. **Failure Detection**: Detects errors, timeouts, or unavailability
3. **Automatic Failover**: Switches to backup providers transparently
4. **Recovery**: Monitors primary providers and switches back when healthy

## Monitoring

```bash
# Prometheus metrics
curl http://localhost:7779/metrics

# Health check
curl http://localhost:3333/lava/health
```
