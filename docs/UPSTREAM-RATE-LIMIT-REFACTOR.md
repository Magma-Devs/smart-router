# Upstream Rate Limiting (429) — Diagnosis & Refactor Proposal

Status: proposal. Written from a read of the code as of `fix/mag-2860-grpc-descriptor-cold-lookup`.

The router keeps drawing 429s from upstreams. This document traces where the
requests actually come from, names the structural cause, and proposes a
prioritized refactor.

**The through-line:** poll cadence, verification, and retry each independently
decide how hard to hit an upstream. None of them knows about the others, and
none of them knows about the *host*. There is no place in the system that can
answer "how many requests per second are we sending to `base.lava.build`?" —
so there is no place that can cap it.

---

## 1. Where the requests come from

### 1.1 Per-endpoint tracker polls — the dominant source

`FlatPollInterval` is derived directly from the spec's block time
(`protocol/endpointstate/endpoint_monitor.go:287`):

```go
FlatPollInterval: m.averageBlockTime / 2,
```

Against the block times declared in `specs/`:

| Chain | `average_block_time` | Poll interval | Polls/sec/endpoint |
|---|---|---|---|
| `APT1` | 200 ms | 100 ms | 10 |
| `ARBITRUM` | 250 ms | 125 ms | 8 |
| `SOLANA` | 400 ms | 200 ms | 5 |
| `HYPERLIQUID` | 1000 ms | 500 ms | 2 |
| `BASE` | 2000 ms | 1 s | 1 |
| `ETH1` | 13000 ms | 6.5 s | 0.15 |

A non-SVM poll is **not one request**. Per cycle,
`fetchAllPreviousBlocksIfNecessary` issues:

1. `FetchLatestBlockNum` — `protocol/chaintracker/chain_tracker.go:485`
2. `forkChanged` → one `FetchBlockHashByNum`, **on every tick even when the
   block has not changed** — `chain_tracker.go:429`
3. on a new block, `fetchAllPreviousBlocks` → `readHashes` walks backwards from
   the tip until it finds a hash overlap — `chain_tracker.go:372-387`

So `BASE` costs roughly 3 rps/endpoint of pure telemetry. The
`solana-testnet` router in `values/core/values_internal.yml` has three
endpoints — one of them keyless PublicNode — which is ~15 rps of polling with
zero user traffic.

The MAG-2159 traffic gate does not rescue this. Gate freshness is
`avgBlockTime` (`endpoint_monitor.go:203`) and
`defaultMaxRelaySkipsBeforePoll = 4` (`chain_tracker.go:82`), so even under
saturating relay traffic there is a hard floor of one real poll per five
intervals — 1 rps on Solana.

### 1.2 Verification is a burst, not a trickle

Every epoch tick (`common.StandaloneEpochDuration`, 15 m default),
`applyReverification` runs `validateProvider` per configured provider,
5-wide (`SpecReVerifyConcurrency`). Each call:

- builds a **brand new `ChainRouter`** — fresh dials, fresh TLS handshakes —
  `protocol/rpcsmartrouter/spec_reverifier.go:432`
- expands every addon URL into with-addon **and** without-addon copies,
  doubling the URL count — `spec_reverifier.go:403-411`
- runs the full spec verification set, up to 3 attempts each —
  `protocol/chainlib/chain_fetcher.go:136-141`
- optionally fetches the latest block up to 3 more times —
  `chain_fetcher.go:116-121`

`ETH1` declares 7 verifications, `COSMOSSDK` declares 9.

### 1.3 Identity is the provider name, not the upstream host

In `values/core/values.yml`, `Lava1` / `Lava2` / `Lava3` all point at
`https://base.lava.build:443/`.

The chain tracker happens to dedupe — `EndpointMonitor.trackers` is keyed on
`endpoint.NetworkAddress`, which is set to `url.Url`
(`protocol/rpcsmartrouter/rpcsmartrouter.go:2199`) — so polls do not triple.

Verification does **not** dedupe: `applyReverification` iterates *providers*.
That is 3× the verification burst against one host.

The retry path also treats them as three independent providers. With
`MaxRelayRetries = 6` (`rpcsmartrouter_server.go:46`), a single 429'd client
request can be sent back to the same physical host six more times — at exactly
the moment that host asked us to slow down.

### 1.4 429 is treated as a health signal, and inconsistently

`shouldFailSessionForResult` exempts body-level rate limits via the
`IsRateLimited` carve-out (`rpcsmartrouter_server.go:1511`) but returns `true`
for HTTP 429 two lines earlier (`:1505`):

```go
if relayResult.StatusCode >= 500 || relayResult.StatusCode == 429 {
    return true
}
// Node error delivered inside a 2xx body — scoreable unless a carve-out claims it.
return relayResult.IsNodeError &&
    !relayResult.IsNonRetryable &&
    !relayResult.IsRateLimited &&
    !relayResult.IsDataScope
```

The same condition gets two verdicts depending on whether it arrives as an
HTTP status or a JSON body. The HTTP path scores against availability and
accrues `ConsecutiveErrors` toward blocklisting — so a 429ing upstream is
demoted, the pool shrinks, load concentrates on the survivors, they 429, and
it cascades.

The `SubCategoryRateLimit` contract (`protocol/common/error_registry.go`) is
explicitly "back off without marking unhealthy". Line 1505 violates it.

### 1.5 Nothing honors `Retry-After`

Zero occurrences of `Retry-After` / `X-RateLimit-Reset` across the repo. A 429
feeds the same generic exponential backoff as a connection reset
(`exponentialBackoff`, capped at `BACKOFF_MAX_TIME = 1m`), discarding the
precise wait the upstream just handed us.

### 1.6 No upstream request budget exists

- `DefaultMaxConnsPerHost = 0 // unlimited` —
  `protocol/common/http_transport.go:31`
- The only `rate.Limiter`s in the tree are for WS/gRPC subscribe/unsubscribe
  (`websocket_config.go`, `grpc_streaming_config.go`) — client-facing, not
  upstream-facing.

---

## 2. Proposed refactor

Ordered by the sequence I would actually land them, not by size.

### Step 0 — Instrument first

Add a counter on upstream requests labeled by **caller**
(`poll` / `verify` / `replay` / `relay`) and by **canonical host**. This is an
hour of work and it tells you whether Steps B+C are sufficient or whether Step
A is mandatory. Expectation from the code: polls dominate by an order of
magnitude on the sub-second chains.

Without this, every later step is unfalsifiable.

### A. A per-upstream-host budget, shared by every caller

Introduce a canonical upstream key — scheme + host + port + path, credentials
and API keys stripped — so `Lava1`/`Lava2`/`Lava3` collapse to one identity.
Attach a token bucket per key.

Every caller draws from it, with priority:

| Caller | Priority | On empty bucket |
|---|---|---|
| Client relay | high | proceed (never starve the data plane) |
| Recovery replay | low | skip this cycle |
| Verification | low | skip this cycle |
| Tracker poll | low | skip this cycle |

`DirectRPCConnection.SendRequest` is the single choke point in direct mode —
wrapping there gives total coverage without touching any caller.

This is the only change that *structurally* caps what the router can do to a
host. Everything else reduces the constant factor; this one makes the bound
explicit and enforceable.

### B. Decouple poll cadence from block time

Replace `avgBlockTime / 2` with `max(avgBlockTime/2, minPollInterval)`, with
`minPollInterval` configurable per chain and defaulting to ~1 s.

The deeper fix is to state the actual requirement: we do not need a 200 ms-fresh
tip on Solana, we need a tip fresh enough for consistency pre-validation.
Derive cadence from that tolerance, not from the block time. Block time is a
property of the chain; poll cadence is a property of what we do with the tip.

Config-shaped change, 5–10× reduction on the fast chains.

### C. Make one poll cost one request

The SVM tracker already does this: `getLatestBlockhash` returns slot **and**
hash in a single call (`protocol/chaintracker/svm_chain_tracker.go:75-104`).
Generalize the pattern.

- **EVM**: `eth_getBlockByNumber("latest", false)` returns number and hash in
  one round-trip, replacing the `FetchLatestBlockNum` + `forkChanged` pair.
- **Fork detection**: give it its own, much slower cadence instead of
  piggybacking on every tip poll. `forkChanged` firing a hash fetch on an
  unchanged block (`chain_tracker.go:429`) is the single most wasteful call in
  the poll path.

### D. Verification becomes a fallback, not a periodic tax

This is the largest architectural win, and it is the same principle the
chaintracker traffic gate already implements — never applied to verification.

1. **Traffic-gate it.** A provider that has served relays successfully since
   the last tick does not need a synthetic probe. We already classify every
   served relay. Actively verify only endpoints with no recent traffic
   evidence.
2. **Split static from live.** Chain-id, genesis hash, and `enabled` are static
   facts — verify once at boot, cache for the process lifetime. Only
   pruning/archive depth needs periodic re-checking.
3. **Verify per upstream key, not per provider.** Fan one result out to every
   provider entry sharing the key.
4. **Reuse the live `ChainRouter`** instead of re-dialing every cycle
   (`spec_reverifier.go:432`). The re-dial is why this is a burst.
5. **Stagger across the epoch** rather than firing 5-wide at the tick.

### E. Make 429 backpressure, not a verdict

1. Parse `Retry-After` / `X-RateLimit-Reset` and feed it into the host bucket
   (Step A) as a next-allowed timestamp, instead of the generic exponential
   backoff.
2. Give HTTP 429 the same `IsRateLimited` carve-out that body-level rate limits
   already get (`rpcsmartrouter_server.go:1505`). Rate limiting must not feed
   availability or `ConsecutiveErrors`, or we blocklist our way into a smaller
   pool and a worse storm.
3. Never retry a 429 onto the same upstream key. If no other key is available,
   return 429 to the client rather than burning `MaxRelayRetries` into a rate
   limiter.

Note that (2) is a correctness fix independent of the rest — the current
behavior actively worsens the storm it is reacting to.

### F. Cross-replica coordination

Each pod polls independently. `values/core/values.yml:125` records that
gk8-prod once ran six chains at 3 replicas — that was 3× the poll load on every
upstream, and the HPA ratcheted there on a memory target that never comes back
down.

Replicas are pinned to 1 today, which is the right call. If autoscaling is
reopened, elect a single poller per (chain, endpoint) and share the tip through
the cache-be pod. Scale on request rate, never on memory.

---

## 3. Sequencing

| Order | Step | Shape | Why here |
|---|---|---|---|
| 1 | Step 0 — instrument | ~1 h | Makes everything below measurable |
| 2 | B + C | config + small code | Biggest ratio for the effort |
| 3 | E | small code | Correctness — current behavior amplifies |
| 4 | A | medium | The structural cap |
| 5 | D | medium/large | Removes the burst entirely |
| 6 | F | deployment | Only if autoscaling is reopened |

---

## 4. Out of scope (different 429, noted to avoid conflation)

Boot pulls ~129 specs from GitHub unauthenticated; restarts get throttled and
the router hangs while the pod reports Ready. Different source, different fix
(auth token + local spec cache). Tracked separately.
