# Secondary Cache Backend — Design

| | |
|---|---|
| **Status** | Draft v2 (review findings 1–10 applied) |
| **PRD** | `docs/PRD_ Secondary Cache Backend.pdf` (Victor Suarez, 2026-04-02) |
| **Baseline** | Verified against `main` @ `f94c1d4` — every symbol and line cited below was re-checked against the checkout, not carried over from prior drafts |
| **Scope** | Optional read-only secondary cache, queried when the primary does not produce a hit, before provider fall-through |

## 1. Overview

Add an optional second cache backend to the SmartRouter. When the primary cache does not
produce a hit (miss, error, timeout, or primary inactive/unconfigured), the router queries the
secondary; on a hit it sanitizes the entry, serves it, and backfills the primary through the
existing caching populator using the exact key that hit. The secondary is strictly read-only,
best-effort, independently configured, and treated as a **cross-zone trust boundary** (§4).

Backwards compatibility (UC-5) is scoped precisely: with no secondary configured, **relay
handling and response bytes are unchanged**. Observability is *not* byte-for-byte identical —
existing cache metric series gain a `cache_tier` label and the latency histogram semantics
change (§12); this is called out for rollout rather than hidden behind an "identical" claim.

The router stays zone-unaware: it sees "primary" and "secondary", nothing else. Network-level
read-only enforcement and cross-zone directionality remain the operator's responsibility.

## 2. Current state (verified integration points)

- **Client**: `performance.Cache` (`protocol/performance/cache.go:144`) — gRPC client for the
  `RelayerCache` service. `InitCache` (`:150`) returns a **non-nil** client even when the
  initial dial fails and eagerly starts the reconnect loop (`:161`); single-flight reconnect
  with 5 s backoff; `codes.Unavailable` resets the client; `CacheActive()` (`:183`) gates use.
  Exposes `GetEntry` (`:166`), `SetEntry` (`:187`), `Flush` (`:209`).
- **Config**: flag `cache-be` (`performance.CacheFlagName`), read via viper so flag, env, and
  YAML all work (`rpcsmartrouter.go:2500-2510`); a failed initial dial logs and continues.
- **Lookup**: single call site in `sendRelayToEndpoint`. Everything — including the
  cross-validation / stateful / force-refresh bypass logging and the `NOT_APPLICABLE` check —
  nests under `if rpcss.cache.CacheActive()` (`rpcsmartrouter_server.go:3184`). Key material:
  `HashCacheRequest` (`:3221`) plus `requestedBlockForCache` resolved for `LATEST_BLOCK` as
  gated tip → `RelayPrivateData.SeenBlock` → 0 (`:3233-3249`). `SeenBlock` is itself stamped
  from the guarded tip at parse time (single producer, `:963-970`) — since `969c076` it is a
  tip snapshot, not per-user consistency state. GET under 50 ms
  (`common.CacheTimeout`, `protocol/common/timeout.go:16`). Any error is treated as a miss
  (`:3309` requires `cacheError == nil`).
- **Hit path** (`:3309-3358`): `outputFormatter` restores the JSON-RPC id; `CACHED_ERROR` GUID
  placeholder substitution (`:3320-3328`); result built with `ProviderAddress: ""` which
  `appendHeadersToRelayResult` renders as `Lava-Provider-Address: Cached` (`:4075-4082`), kept
  as the last "resolver" entry in the provider list (`:4159`).
- **Populator**: `tryCacheWrite` (`:2947`) — eligibility (stateful, `IsNodeError` `:2963`,
  status codes `:2999/:3007`, `NOT_APPLICABLE`), finalization from the **gated** tip (`:3038`,
  helper `:2905`), write-side LATEST resolution `Reply.LatestBlock` → `relayData.SeenBlock` →
  **skip** (`:3042-3057`), deep copy (`:3069`), async `SetEntry` with 5 s timeout and
  `IsNodeError: false` always (`:3083-3096`). Second writer: `ChainFetcher.populateCache`
  (`protocol/chainlib/chain_fetcher.go:242`).
- **Metrics** (`protocol/metrics/smartrouter_metrics_manager.go:1159-1172`): `RecordCacheResult`
  increments `smartrouter_cache_requests_total` always, `..._success_total` on hit, and lumps
  **miss, error, and timeout together** in `..._failed_total`; the latency histogram is
  observed **only on hits** (`:1171`). `RecordCacheHitRequest` (`:1085`) counts hits in the
  router request counters under `provider_address="Cached"`.
- **Cache server**: stores the whole `RelayReply` with `Sig` zeroed (`handlers.go:529`) but
  **`Metadata` preserved**; `CacheValue` (`handlers.go:46-52`) does **not** persist
  `RelayCacheSet.IsNodeError` — the flag only picks the TTL (`handlers.go:367`), so a GET
  cannot report entry kind today.

## 3. PRD traceability

Must-haves (PRD "Requirements → Must Have") and acceptance criteria (PRD "Acceptance
Criteria"), mapped to design sections and tests (§14):

| PRD item | Design | Tests |
|---|---|---|
| M1. Secondary queried on primary miss, before providers | §5 | T1, T16 |
| M2. Independently configurable connection details | §11 | T8 |
| M3. Read-only — router never writes to it | §9 | T10 |
| M4. Backfill governed by existing populator logic | §6 | T2, T3 |
| M5. Configurable lookup timeout; exceeded ⇒ miss | §5, §11 | T5 |
| M6. Failure never affects serving; same resilience as primary | §10 | T5, T6 |
| M7. Backwards compatible when unset | §1, §15 | T7 |
| M8. No provider-identifying metadata exposed from secondary entries | §4 | T11 |
| AC-Lookup: queries secondary on primary miss / hit serves / miss routes to providers / unset ⇒ today's behavior | §5 | T1, T4, T7, T16 |
| AC-ReadOnly: never written / populator-governed backfill / eligible entry lands in primary and next lookup hits primary | §6, §9 | T2, T10 |
| AC-Resilience: failure skipped / reconnects / timeout ⇒ miss | §10 | T5, T6 |
| AC-Config: independent details / access mode / timeout configurable | §11 | T8, T9 |
| AC-Security: strip or omit provider-identifying metadata (or document confirmed-clean) | §4 | T11 |
| N1. Configurable access mode (read-write later) | §11, §16 | T9 |
| N2. N cache tiers | declined, §16 | — |
| N3. Prometheus parity with `cache_tier` label | §12 | T15 |
| N4. Tracing visibility per tier | §12 | T15 |
| N5. Log secondary config at startup | §11 | T8 |

## 4. Trust boundary & sanitization (security)

The secondary cache is a **different trust domain**. Its entries may have been written by other
router versions, other software lineages (upstream lava writers), or a misconfigured/compromised
writer — none of the write-path hygiene this repo implements can be assumed. Concretely, the
current format *can* carry identity-bearing data: `RelayReply.Metadata` stores arbitrary
upstream response headers (`direct_rpc_relay.go:464,645,766,821`), the cache server preserves
`Metadata` while zeroing only `Sig` (`handlers.go:528-554`), and `SigBlocks` /
`FinalizedBlocksHashes` round-trip untouched. The earlier draft's "confirmed no-op" claim held
only for entries written by *this* router version; it is withdrawn. Sanitization is an
**active requirement**.

**Rule: every secondary-cache reply is deep-copied and sanitized before any use — both the
response served to the caller and the payload handed to primary backfill are the sanitized
copy.** (Order: clone → sanitize → `outputFormatter` → serve; the same sanitized clone feeds
§6.)

```go
// protocol/performance/sanitize.go (new)
func SanitizeForeignCacheReply(reply *pairingtypes.RelayReply) {
    reply.Sig = nil       // provider signature over the response
    reply.SigBlocks = nil // provider signature over finalization data
    kept := reply.Metadata[:0]
    for _, m := range reply.Metadata {
        if !isIdentityHeader(m.Name) {
            kept = append(kept, m)
        }
    }
    reply.Metadata = kept
}
```

`isIdentityHeader` (case-insensitive): name has prefix `lava-`, **or** name ∈
{`provider-latest-block`, `smart-router-version`}. `CacheRelayReply.OptionalMetadata` from the
secondary is dropped entirely (this router never writes it; foreign content has no consumer
here). `FinalizedBlocksHashes` is kept: it is chain data, not identity, and nothing on the
cache-hit path consumes it.

**Why a bounded denylist is sufficient**: the identity surface of this protocol is enumerable.
Every response header the router lineage mints is defined in `protocol/common/endpoints.go:19-48`
and is `lava-`-prefixed — covering `Lava-Provider-Address`, `Lava-Retries`,
`Lava-Errored-Providers`, `Lava-Node-Errors-providers`, `Lava-Reported-Providers`,
`lava-providers-block`, `lava-fast-tx-participants`, all `lava-cross-validation-*` participant/
status headers, `lava-selection-stats`, `Lava-Guid`, debug headers — with exactly two
exceptions, `Provider-Latest-Block` and `Smart-Router-Version`, which the denylist names
explicitly. Non-identity upstream node headers (e.g. `Content-Type`) are deliberately kept so a
secondary hit serves the same header surface as a primary hit. A guard test (T11b) iterates the
exported header constants in `protocol/common` and fails if any response-side constant is
neither `lava-`-prefixed nor in the named-exception set — so the denylist cannot silently rot.
An allowlist was considered and rejected: it would strip functional pass-through headers and
diverge secondary-hit behavior from primary-hit behavior for no identity benefit.

Locally minted headers are unaffected: `Lava-Provider-Address: Cached` is appended by
`appendHeadersToRelayResult` *after* serving decisions, exactly as for primary hits (T11 asserts
it survives).

v1 applies sanitization to the **secondary tier only**; the primary stays same-zone-trusted
(unchanged behavior). Extending it to the primary is listed as a hardening option (§16).

## 5. Lookup state machine

The bypass predicates move **out** of the primary-activity gate so a healthy secondary works
when the primary is down or unconfigured (today all lookup logic nests under
`rpcss.cache.CacheActive()`, `:3184`):

```
bypass = crossValidation || stateful(CONSISTENCY_SELECT_ALL_PROVIDERS)
      || forceCacheRefresh || reqBlock == NOT_APPLICABLE            // evaluated once

if !bypass && (primary.CacheActive() || secondary.CacheActive()):
    hashKey, outputFormatter = protocolMessage.HashCacheRequest(chainId)   // computed once
    requestedBlockForCache   = resolve(reqBlock)   // gated tip → SeenBlock → 0, computed once

    if primary.CacheActive():
        r1 = primary.GetEntry(ctx@50ms, {hashKey, requestedBlockForCache, SharedStateId, SeenBlock, …})
        adoptSharedStateTip(r1.SeenBlock)          // primary only — never for secondary
        if hit(r1): serve(r1); return              // unchanged from today

    if secondary.CacheActive():
        r2 = secondary.GetEntry(ctx@secondary-cache-timeout,
                                {hashKey, requestedBlockForCache, SharedStateId: "", SeenBlock, …})
        if hit(r2):
            reply = SanitizeForeignCacheReply(deepCopy(r2.Reply))          // §4
            serve(reply)                            // formatter, CACHED_ERROR GUID subst,
                                                    // ProviderAddress:"" ⇒ "Cached" — same as primary hit
            if !nodeError(r2) && primary.CacheActive():
                go backfill(reply, requestedBlockForCache)                 // §6
            return

    harvest = mergeBlockHashes(r1, r2)              // §8 — from miss replies of both tiers

→ provider routing (unchanged)
```

Full outcome table (`hit` = `err == nil && reply.GetReply() != nil`; `error` and `timeout` are
both treated as a miss for control flow, distinguished only in metrics §12):

| Primary | Secondary | Behavior |
|---|---|---|
| any | any, **bypass true** | no lookups, no tier metrics; providers (or CV/stateful flow) |
| unconfigured/inactive | unconfigured/inactive | straight to providers |
| hit | (not attempted) | serve from primary — unchanged |
| miss / error / timeout | unconfigured/inactive | providers (today's behavior) |
| miss / error / timeout | hit (normal entry) | sanitize → serve → async backfill to primary |
| miss / error / timeout | hit (node-error entry) | sanitize → serve (GUID substituted) → **no backfill** (§7) |
| miss / error / timeout | miss / error / timeout | merge block-hash info (§8) → providers |
| unconfigured/inactive | hit | sanitize → serve; **backfill naturally skipped** (no active primary) |
| unconfigured/inactive | miss / error / timeout | providers |

**Decision: the secondary is independent of the primary.** The PRD never conditions the
secondary on a primary's existence, so none is imposed — `secondary-cache-be` is valid with
`cache-be` unset, and a configured secondary keeps serving while the primary is down or
reconnecting (last two rows). No startup restriction, no special casing: reads work, and
backfill is skipped by the existing `CacheActive()` gate inside the populator (`:2953`).
Startup logs an advisory warning for the secondary-only topology (§11), since nothing backfills
and repeat requests re-cross the zone boundary. Latency budget: the secondary adds at most
`secondary-cache-timeout` to a request, and only on the primary-no-hit path.

Two rules carried over unchanged from the primary hit path apply to the secondary verbatim:
a cached reply's `LatestBlock` never feeds tip state (MAG-2160 rule, comment at `:3339-3344`),
and the secondary reply's `SeenBlock` is **not** passed to `adoptSharedStateTip` — post-`969c076`
that field's only consumer is fleet-scoped shared-state tip exchange between pods behind the
*same* cache, which a foreign-zone cache is not. The field is live on every reply — the cache
server echoes back `max(stored entry value, caller's value)` even with shared state off
(`ecosystem/cache/handlers.go:229-231`) — so ignoring it for the secondary is an active rule
(T13), not dead code. The anti-lie guard would re-check a fed value
anyway, so this is scoping, not safety: a read-only data fallback must not double as a
cross-zone coordination channel.

## 6. Backfill: exact data flow and key ownership

**Problem** (why "call `tryCacheWrite` unchanged" is wrong): the GET key block and the SET key
block are resolved by *different* logic. Lookup resolves `LATEST_BLOCK` → gated tip →
`SeenBlock` → 0 (`:3233-3249`); the populator resolves `LATEST_BLOCK` → `Reply.LatestBlock` →
`SeenBlock` → **skip** (`:3042-3057`). `Reply.LatestBlock` is method-dependent and frequently 0
(e.g. `eth_call` — nothing to parse a height from), and for a *cached* reply it is whatever was
current when the entry was originally written. If the tip advances between `ParseRelay`
(which stamps `SeenBlock`) and the lookup, a secondary hit at block N would backfill at N−1, or
not at all — and the acceptance criterion "subsequent lookups for the same key hit the primary"
fails.

**Design**: the block that produced the secondary hit **owns the key**. The populator is split
so the resolution step can be supplied by the caller:

```go
// tryCacheWrite(ctx, pm, rr) becomes a thin wrapper over:
func (rpcss *RPCSmartRouterServer) tryCacheWriteResolved(
    ctx context.Context,
    protocolMessage chainlib.ProtocolMessage,
    relayResult *common.RelayResult,
    resolvedBlock *int64, // nil ⇒ resolve from Reply.LatestBlock/SeenBlock (today's behavior)
)
```

- Provider paths (`:1795`, `:1059`) and `ChainFetcher` are untouched — they pass `nil` and
  behave exactly as today.
- The secondary-hit path passes `&requestedBlockForCache` — the very value used in the
  successful GET — so the primary SET lands on the identical server-side key
  (`hash ‖ LittleEndian(block)`).
- **Everything else in the populator is retained**: stateful / node-error / status-code /
  `NOT_APPLICABLE` eligibility, hash recomputation via `HashCacheRequest` (deterministic for
  the same protocol message, so GET/SET hashes match by construction), finalization from the
  raw requested-block sentinel and the gated tip (`:3038` — the hint replaces only the *key*
  resolution, not the finalization input), local `relayData.SeenBlock` and local
  `SharedStateId` in the SET payload (the foreign entry's `SeenBlock` never enters the
  primary), deep copy, async goroutine, 5 s `CacheWriteTimeout`.
- Input is the **sanitized** clone from §4; the populator's own deep copy (`:3069`) stays as
  the race-safety barrier between serving and writing.
- Backfill is invoked only when the entry is not a node error (§7) and the primary is active.

## 7. Cached node errors: explicit entry-kind contract

`CacheRelayReply` carries no `IsNodeError`, and the `CACHED_ERROR` placeholder has **no
producer in this repository** (verified: the only two references are the consumer-side
substitution at `:3320/:3325`; this router always writes `IsNodeError: false`, `:3094`). The
substring is inherited compatibility with upstream-lava writers, not a contract this repo can
rely on. Node-error entries in a foreign cache are therefore exactly the case we must assume.

**Primary mechanism — extend the entry contract** (types are hand-written JSON-over-gRPC
structs, so the change is additive and wire-compatible in both directions: unknown fields are
ignored, missing fields decode to `false`):

- `types/relay/cache.go`: add `IsNodeError bool` to `CacheRelayReply`.
- `ecosystem/cache/handlers.go`: persist `RelayCacheSet.IsNodeError` on `CacheValue` (today it
  only selects the TTL, `:367`, and is lost) and return it in `ToCacheReply`.

**Formalized fallback — placeholder contract**: the exact fragment
`"Error_GUID":"CACHED_ERROR"` is promoted from an inline literal to an exported constant
(`common.CachedErrorGUIDPlaceholder`), documented as the legacy node-error marker, with fixture
tests locking the byte-exact form the substitution path already depends on.

**Decision rule**: `nodeError(entry) = entry.IsNodeError || bytes.Contains(reply.Data,
placeholder)`. Node-error entries are **served** (with GUID substitution, identical to the
primary hit path today) but **never backfilled** — a cached node error must not be re-written
into the primary as a normal successful response, and writing it with `IsNodeError: false`
would give it a success TTL.

Compatibility matrix: new router + new backend → explicit flag (authoritative); new router +
old backend → flag absent (`false`) + placeholder heuristic (matches today's serving behavior);
old router + new backend → extra JSON field ignored. Primary-tier behavior is unchanged in all
cases (the primary never backfills itself).

## 8. Block-hash → height merge

Both tiers can return `BlocksHashesToHeights` on a *miss*, feeding archive-extension upgrade
via `getEarliestBlockHashRequestedFromCacheReply` (`:1217-1231` — folds entries with
`Height >= 0` into `(latest = max, earliest = min)`, `NOT_APPLICABLE` excluded).

With two miss replies the inputs are **merged, never overwritten**: the fold runs over the
concatenation of both replies' mapping lists (equivalently: `earliest = min(valid earliests)`,
`latest = max(valid latests)`). Consequences, each locked by T14:

- a secondary reply with no mappings contributes nothing and cannot erase primary-derived
  values (the fold is append-only);
- complementary mappings from the two tiers combine;
- conflicting heights for the same hash both enter the fold — `earliest` takes the smaller,
  `latest` the larger, deterministically; a debug log records the disagreement (a foreign cache
  disagreeing with the primary on a hash→height fact is diagnostic signal).

## 9. Read-only boundary (structural)

`*performance.Cache` exposes `SetEntry` and `Flush`; "we just don't call them" is a convention,
not a boundary. The secondary is held behind a narrow reader interface:

```go
// protocol/performance
type CacheReader interface {
    CacheActive() bool
    GetEntry(ctx context.Context, get *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error)
}
```

- `*Cache` satisfies it; the server field is `secondaryCache performance.CacheReader`. Writes
  and flushes on that field are **compile-time errors**.
- The secondary is additionally never wired into the write-capable surfaces: `tryCacheWrite*`
  and `ChainFetcher` receive only the primary, and the `/debug/reset-all` `cacheFlusher`
  (`rpcsmartrouter.go:475`) keeps pointing at the primary — flushing a foreign zone's cache
  would be a destructive cross-zone write.
- Test seam: unit tests inject a fake `CacheReader` (hits, misses, delays, poisoned entries)
  with no listening gRPC server — this is what makes T1–T5, T11–T14, T16 cheap.
- Honest limit: a type assertion back to `*Cache` could recover the writer. That is not
  preventable in Go; T10 covers the behavioral guarantee (no SET/FLUSH RPC ever reaches the
  secondary across the full debug-flush and relay flows), and review owns the rest.

The primary keeps its concrete `*performance.Cache` type and full capabilities.

## 10. Resilience

Both tiers get identical machinery by construction — the secondary is a second
`performance.InitCache` instance: non-fatal failed initial dial with eager background reconnect
(`cache.go:157-162`), single-flight 5 s-backoff reconnect loop, client reset on
`codes.Unavailable`, `NotConnectedError` short-circuit while down. Per-request behavior: any
secondary error, timeout, or inactivity is a miss; the request proceeds to providers. The
secondary can never fail a request — only add up to `secondary-cache-timeout` of latency on the
primary-no-hit path.

## 11. Configuration

| Key | Type | Default | Meaning |
|---|---|---|---|
| `secondary-cache-be` | string | `""` (disabled) | Secondary cache address; same formats as `cache-be` (host:port or `unix:` socket) |
| `secondary-cache-timeout` | duration | `50ms` | Per-lookup budget; on expiry the lookup is a miss |
| `secondary-cache-mode` | string | `read-only` | v1 accepts only `read-only`; `read-write` reserved (§16) |

All three registered next to `cache-be` and read via viper (`rpcsmartrouter.go:2500-2510`
pattern), so flag / env / YAML all work with standard viper precedence (flag > env > config
file) — locked by T8.

**Startup validation** (fail fast with a clear error):

- `secondary-cache-timeout > 0` — zero or negative is rejected, not "disabled";
- `secondary-cache-mode ∈ {read-only}`;
- `secondary-cache-timeout` or `secondary-cache-mode` set while `secondary-cache-be` is empty →
  error (dangling configuration is a deployment mistake, not a default);
- `secondary-cache-be` set while `cache-be` is empty → **allowed** (secondary-only topology,
  §5) with a startup warning that no primary backfill will occur — a missing primary is more
  often a forgotten `cache-be` than a deliberate choice, but it must not block the legitimate
  read-only-edge topology;
- `secondary-cache-be == cache-be` → warning (double lookup of the same store is legal but
  almost certainly misconfiguration).

**Initial connection errors**: same contract as the primary — log the error, keep the non-nil
client, let the background loop reconnect (`InitCache` already guarantees this). Startup logs
the secondary's address, mode, and timeout on one line (PRD N5).

Examples:

```yaml
# config.yml
cache-be: "cache-internal:20100"
secondary-cache-be: "cache-shared.other-zone:20100"
secondary-cache-timeout: 75ms
secondary-cache-mode: read-only
```

```
smartrouter ... --cache-be cache-internal:20100 \
  --secondary-cache-be cache-shared.other-zone:20100 \
  --secondary-cache-timeout 75ms --secondary-cache-mode read-only
```

## 12. Observability

Current facts (§2): non-hits are lumped into one counter and latency is hit-only. Metric parity
is a PRD nice-to-have; this design adopts **full outcome-aware instrumentation** rather than
labeling the current shape "parity":

- All four metrics gain `cache_tier ∈ {primary, secondary}`:
  `smartrouter_cache_requests_total`, `..._success_total`, `..._failed_total`,
  `..._latency_milliseconds`.
- `smartrouter_cache_failed_total` additionally gains `outcome ∈ {miss, error, timeout}`
  (timeout = `context.DeadlineExceeded`; error = any other non-nil `GetEntry` error; miss =
  clean nil-reply).
- The latency histogram is observed for **every attempted lookup**, not only hits — a deliberate
  semantic change (hit-only latency hid exactly the tail that matters for a network-hop
  secondary). `RecordCacheResult` becomes
  `(chainId, apiInterface, method, tier, outcome, latencyMs)`.
- **Skipped ≠ attempted**: a tier that is unconfigured, inactive, or bypassed emits nothing for
  that request. `requests_total` counts attempts only.
- `RecordCacheHitRequest` (router request counters, `provider_address="Cached"`) fires for a
  hit on either tier, unchanged in shape.

Tracing: each attempted tier lookup records its own `smartrouter.CacheLookup` span with
`cache.tier` and `cache.outcome`. Parent relay span: `cache.hit = true` iff any tier hit, plus
`cache.tier = <serving tier>` on a hit. For primary-miss → secondary-hit: primary span
`outcome=miss`, secondary span `outcome=hit`, parent `cache.hit=true, cache.tier=secondary`.

Compatibility: existing primary series gain the `cache_tier="primary"` label and the histogram
gains non-hit observations even when no secondary is configured. Aggregating queries
(`sum by (spec)`) survive; exact label matchers and latency-based alerts need updating —
`docs/METRICS.md` and the release notes must state this (§15).

## 13. Mixed cache engines (corrected claim)

Precisely: the router speaks the `RelayerCache` gRPC contract
(`/smartrouter.pairing.RelayerCache/{GetRelay,SetRelay,Health,FlushCache}`), and **any backend
exposed through that contract works as either tier** — that much is by construction. Mixed
engines are supported **only** insofar as both backends implement that contract with equivalent
topology, authentication, and TLS capabilities; today's client dials skip-TLS gRPC/unix-socket
only, so a backend requiring TLS or RESP semantics is *not* reachable. If the RESP-compatible
backend PRD introduces a second client abstraction, integrating it as a tier is an **explicit
dependency on that work** — this design's `CacheReader` seam (§9) is where such an adapter would
plug in, but the adapter itself is out of scope here.

## 14. Verification plan

Unit tests use the `CacheReader` fake (§9) plus the existing `cache_skip_paths_test.go`
patterns; integration uses two real `smartrouter cache` instances, seeding the secondary via a
test-only `SetRelay` client.

| # | Test | PRD link |
|---|---|---|
| T1 | Primary miss → secondary hit → response served, zero provider dispatch | M1, AC-Lookup |
| T2 | **Exact-key backfill**: LATEST request, tip advances after `ParseRelay`, fake entry's `Reply.LatestBlock=0` → secondary hit at tip-block N → primary SET observed at key block N → identical follow-up lookup hits primary | M4, AC-ReadOnly |
| T3 | Backfill is async and race-safe: served bytes never mutate after serve; populator deep-copy intact; backfill runs through real `tryCacheWriteResolved` eligibility (429/504/stateful entries rejected) | M4 |
| T4 | Both tiers miss → normal provider routing | AC-Lookup |
| T5 | Secondary timeout (delayed fake) treated as miss within `secondary-cache-timeout`; non-timeout errors likewise; request unaffected | M5, M6, AC-Resilience |
| T6 | Secondary transient failure → reconnect loop restores service (integration: kill/restart the cache process) | M6, AC-Resilience |
| T7 | No secondary configured: relay behavior and response bytes unchanged; full existing suite green | M7, AC-Lookup |
| T8 | Config: independent connection details; YAML vs flag vs env precedence; startup log line present | M2, AC-Config, N5 |
| T9 | Startup validation: invalid mode, zero/negative timeout, dangling secondary options → clean startup error; secondary-only config boots successfully with the advisory warning logged | AC-Config |
| T10 | No `SetRelay`/`FlushCache` ever reaches the secondary: full relay + `/debug/reset-all` flows against an RPC-recording fake; compile-time seam (`CacheReader`) in place | M3, AC-ReadOnly |
| T11 | **Poisoned entry**: secondary seeded with `Metadata` containing `Lava-Provider-Address`, `lava-cross-validation-all-providers`, `Provider-Latest-Block`, non-empty `Sig`/`SigBlocks` → caller response contains none of them; primary backfill payload contains none of them; locally minted `Lava-Provider-Address: Cached` still present | M8, AC-Security |
| T11b | Header-constant guard: every response-side header constant in `protocol/common` is `lava-`-prefixed or in the named-exception set | M8 |
| T12 | Cached node error: entry with `IsNodeError=true` (and separately, legacy placeholder-only) → served with GUID substitution, **never backfilled**; normal entries backfill | M4, §7 |
| T13 | Secondary reply's `SeenBlock` never reaches `adoptSharedStateTip`/ChainState; primary's still does | §5 |
| T14 | Block-hash merge: primary-only, secondary-only, complementary, conflicting mappings → min/max fold; empty secondary erases nothing | §8 |
| T15 | Metrics & tracing: per-tier series with correct `outcome`; skipped tiers emit nothing; latency observed on non-hits; span/parent-span attributes for miss-then-hit | N3, N4 |
| T16 | Primary inactive (down) + secondary hit → served; primary unconfigured + secondary configured → served, backfill skipped | M1, M6 |

## 15. Rollout & documentation changes

- **Rollout**: `secondary-cache-be` unset ⇒ `secondaryCache == nil` ⇒ every new branch skipped;
  relay behavior unchanged (T7). Enabling/removing is config + restart. Observability changes
  ship even without a secondary (label additions, histogram semantics — §12) and are the one
  operator-visible delta; release notes must flag dashboard/alert updates.
- **Documentation to update**: `docs/METRICS.md` (new labels, `outcome`, latency semantics,
  "Cached" note), CLI flag help text, `docs/LOCAL-COMPOSE.md` (optional two-cache overlay
  example), the public SmartRouter caching docs referenced by the PRD, and the release notes.

## 16. Open decisions

- **`read-write` secondary mode** (PRD nice-to-have): deferred; the mode flag validates now, so
  adding it later is config-compatible. Semantics (fan-out writes, failure coupling) need their
  own review.
- **Sanitizing primary-tier hits too**: v1 trusts the same-zone primary (unchanged behavior);
  applying §4 to both tiers is a cheap hardening candidate.
- **N-tier chain**: declined per PRD scoping; `CacheReader` + the state machine generalize to a
  list mechanically if ever needed.
- **Dashboard migration**: exact-matcher queries and latency alerts affected by §12 need an
  owner and a window; tracked with the rollout, not this design.
