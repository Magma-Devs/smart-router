# Development Plan: RESP-Compatible Cache Backend

| | |
|---|---|
| **Source PRD** | `docs/PRD_ RESP-Compatible Cache Backend.pdf` (Draft, 2026-04-01, Victor Suarez) |
| **Status** | Implemented (all phases on `feat/resp-cache-backend`) — **rev 7 (2026-08-06)** resolves the second implementation review and, unlike rev 6, is itself a tracked file (rev 6 existed only in the working tree, so commit `0199089` never contained it; any claim that it did was unsupported). Rev 7: the bare-metal lane is ownership-safe (never signals a process by executable name, never force-removes an unlabelled container, refuses on a foreign listener — with a regression harness) and now runs the checked-in HTTP+WS fixture without `--skip-websocket-verification`; a real cobra/viper acceptance test drives `CreateRPCSmartRouterCobraCommand()`; the parity suite gains node-error TTL and equal-tip-refresh cases across BOTH backends; health probes classify authentication failures via `redis.IsAuthError` and log them without leaking credentials; the sentinel drill waits for the sentinel view to converge before asserting the master address; and every Phase-6/testing-strategy/risk statement is reconciled with the artifacts that actually exist (key schema, D11 YAML validity, timeout provenance, sentinel control-plane rotation, cluster resharding). Rev 6 resolved the first implementation review: `ContextTimeoutEnabled` on every client with a discriminating budget test; sentinel rotation recorded as an accepted per-connection deviation (upstream go-redis gap); executable dockerized TLS/mTLS and cluster lanes; example-config endpoint drift fixed + fail-closed readiness timing documented. Rev 5 resolved review round 3 (sentinel control-plane auth, go-redis pin ≥ v9.20.0, Cluster-safe purge); rev 4 round 2; rev 2 round 1. |
| **Baseline** | `main`. The plan assumes only what exists on `main` today: no cache backend interface, no per-tier/outcome cache metrics, no entry-kind (`IsNodeError`/`StatusCode`) persistence. |
| **Terminology** | The PRD says `lavap cache`; in this repo the default cache is the `smartrouter cache` subcommand (`cmd/smartrouter/main.go`, served from `ecosystem/cache/`). |

## 1. What the PRD asks for

Let the router use a RESP-compatible (Redis/Valkey) backend **directly** as an alternative to the default `smartrouter cache` process, to gain persistence, shared state across router replicas, HA via Sentinel/Cluster failover, and multi-region replication through managed infrastructure (ElastiCache Global Datastore, MemoryDB Multi-Region).

Must-haves distilled:

1. RESP backend as an alternative primary cache; **feature parity** with the default cache.
2. **Standalone**, **Sentinel** (auto-failover), and **Cluster** (sharded) topologies; Cluster reached via a single **configuration endpoint** (automatic topology discovery).
3. **AUTH** (static) and **TLS**; **dynamic credential providers** for token rotation (IAM-style), without coupling to any cloud SDK.
4. **Read/write endpoint separation** for replica reads in multi-region topologies.
5. **Backwards compatible**: no RESP config → existing `--cache-be` gRPC cache behavior, zero changes for current deployments (UC-5).
6. All connection details configurable **without code changes**.
7. Failure resilience: unreachable backend → degraded mode (fall through to providers), observable via Prometheus (UC-4).

Nice-to-haves: connection health monitoring / reconnection logging; documented `maxmemory-policy` recommendation.

## 2. Where the code is today

The cache is split across a client seam in the router and a standalone gRPC server process:

| Piece | Location | Notes |
|---|---|---|
| Cache server process | `ecosystem/cache/` (`server.go`, `handlers.go`, `command.go`) | `smartrouter cache <addr>`; gRPC over h2c; three in-process ristretto stores (finalized, temp, block-hash→height) |
| Cache semantics | `ecosystem/cache/handlers.go` | Key derivation (`formatHashKey` = requestHash ‖ LE(block)), store selection (`findInAllCaches`), seen-block validity check, `LATEST_BLOCK` substitution, shared-state tip with retry-confirm (`performInt64WriteWithValidationAndRetry`), gzip above 1 KiB, differentiated TTL table |
| Wire types | `types/relay/` (`cache.go`, `grpc_service.go`, `proto_compat.go`) | Hand-written structs; gRPC frames carry **JSON**, not protobuf |
| Router client | `protocol/performance/cache.go` | `Cache` (gRPC client) — `GetEntry` / `SetEntry` / `Flush` / `CacheActive`, background reconnect loop. The router's only cache abstraction — a concrete struct, no interface |
| Call sites | `protocol/rpcsmartrouter/rpcsmartrouter_server.go` (lookup ~`:3279`, populator write `tryCacheWrite` `:3042` → `SetEntry` `:3178`), `protocol/chainlib/chain_fetcher.go:41,256` | Lookup budget: `common.CacheTimeout` = 50 ms; writes are async best-effort |
| Config | `protocol/performance/common.go` | `--cache-be` only, via flag or viper YAML (an explicitly passed flag outranks the YAML value; env vars are unbound by design) |
| Metrics | `protocol/metrics/smartrouter_metrics_manager.go:104-108` | `smartrouter_cache_{requests,success,failed}_total` + latency histogram, labels `spec, apiInterface, method` — hit/miss only: an error or timeout is indistinguishable from a clean miss |

Two facts shape the whole design:

- **There is no backend interface at all.** `RPCSmartRouterServer.cache` and `ChainFetcher` hold the concrete `*performance.Cache` — every consumer is hard-wired to the gRPC client.
- **All cache *semantics* live server-side**, coupled to ristretto. A RESP backend that merely maps `GetRelay`/`SetRelay` onto GET/SET would have to re-implement all of it to meet the parity requirement.

There are currently **zero** Redis/RESP references or dependencies in the repo; the only cache dependency is `ristretto/v2`.

## 3. Target architecture

```
                 ┌────────────────────────────────────────────────┐
                 │ router (rpcsmartrouter / chainlib call sites)  │
                 │        performance.CacheBackend interface      │
                 └───────────────┬───────────────┬────────────────┘
                                 │               │
                 ┌───────────────▼──┐        ┌───▼─────────────────────────┐
                 │ performance.Cache│        │ performance.RespCache (new) │
                 │ (gRPC client)    │        │ cache engine, in-process    │
                 └───────────────┬──┘        └───┬─────────────────────────┘
                                 │               │ go-redis UniversalClient
                 ┌───────────────▼──┐        ┌───▼─────────────────────────┐
                 │ smartrouter cache│        │ Redis / Valkey              │
                 │ gRPC server      │        │ standalone/sentinel/cluster │
                 │  cache engine    │        └─────────────────────────────┘
                 │  ristretto store │
                 └──────────────────┘
```

The **cache engine** (extracted from `ecosystem/cache/handlers.go`) is the single implementation of cache semantics, parameterized over a small KV-store interface. The gRPC server keeps it with a ristretto adapter; the new RESP backend runs the same engine inside the router over a go-redis adapter. Parity is achieved by construction, not by re-implementation.

### Design decisions

**D1 — Router-embedded RESP backend, no intermediary process.** UC-1 says the router connects to the RESP backend directly. Alternative considered — keeping the `smartrouter cache` process and swapping its store to Redis — preserves the logic in one place but keeps the extra hop and the extra deployment the PRD wants to make optional. Rejected as the primary path (it falls out almost for free later if ever needed, since the server will sit on the same engine).

**D2 — Extract a storage-agnostic cache engine.** Move the semantic logic out of `RelayerCacheServer` into a package (`ecosystem/cache/core`) exposing roughly:

```go
type Engine struct { store KVStore; ttl TTLPolicy }
func (e *Engine) GetRelay(ctx, *RelayCacheGet) (*CacheRelayReply, error)
func (e *Engine) SetRelay(ctx, *RelayCacheSet) error
func (e *Engine) Flush(ctx) error

type KVStore interface {
    GetEntries(ctx, keys []string) ([]*Envelope, error) // index-aligned, nil = miss; RESP: one pipeline, ristretto: sequential reads
    SetEntry(ctx, key string, v *Envelope, ttl time.Duration) error
    GetInt64(ctx, key string) (int64, bool, error)
    SetInt64IfGreaterOrEqual(ctx, key string, v int64, ttl time.Duration) error // tip CAS; equality refreshes TTL
    GetHeight(ctx, key string) (int64, bool, error)      // block-hash → height
    SetHeight(ctx, key string, h int64, ttl time.Duration) error
    Purge(ctx) error
}
```

The engine owns: key formatting, seen-block validation, `LATEST_BLOCK` substitution, node-error write-TTL selection (on `main`, `RelayCacheSet.IsNodeError` only picks the TTL and is not persisted), gzip compression, the TTL table, shared-state tip semantics. Code is moved **verbatim** where possible so the existing test suites keep passing.

**D3 — Tip atomicity is an adapter concern; equality must refresh TTL.** `performInt64WriteWithValidationAndRetry` exists because ristretto writes are async/lossy; Redis is synchronous. The server's rule is `existing <= new` — an **equal** observation rewrites the entry, refreshing its TTL, which is what keeps an actively-observed but non-advancing tip (stalled chain) alive today. The KV op is therefore `SetInt64IfGreaterOrEqual`: retry-confirm in the ristretto adapter (existing code, moved), and a single-key Lua compare-and-set in the Redis adapter (`new >= current` or key absent → `SET PX`; single-key scripts are Cluster-safe, no hash tags needed). A strict greater-only CAS would silently let a stalled chain's tip expire — the parity suite pins the refresh-on-equal behavior.

**D4 — go-redis v9 via `UniversalClient`.** One options struct maps to `NewClient` / `NewFailoverClient` (Sentinel, `master-name`) / `NewClusterClient` (seeded with the configuration endpoint; the client performs topology discovery). Gives us TLS, `StreamingCredentialsProvider` (in-place re-auth on rotation, D9 — API floor v9.11.0; the production pin is ≥ v9.20.0, see D9), pool stats, and per-op contexts in one dependency. No cloud SDKs. **`ContextTimeoutEnabled: true` is mandatory on every client**: without it go-redis bounds socket I/O only by Read/WriteTimeout (seconds) and the router's per-lookup budget silently stops applying to a slow backend — the degraded-mode suite pins this with a frozen-connection test where only the caller's deadline can be the effective bound.

**D5 — Key schema (Cluster-safe, prefix-scoped, two relay namespaces).** Configurable prefix (default `sr`):

Keys are **kind-first**, not chain-first — the kind prefix follows the configurable
prefix so a `SCAN MATCH` can target one namespace across all chains
(`ecosystem/cache/core/keys.go`, applied by `Store.key` in `redisstore/store.go`):

- Relay entries, finalized store: `{prefix}:rel:f:{chainId}:{hex(requestHash)}:{block}`
- Relay entries, non-finalized store: `{prefix}:rel:t:{chainId}:{hex(requestHash)}:{block}`
- Block-hash→height: `{prefix}:h2h:{chainId}:{blockHash}`
- Shared-state tip: `{prefix}:tip:{chainId}:{uniqueId}`
- Chain tip (latest-block resolution): `{prefix}:chaintip:{chainId}`

Worked example at the default prefix: `sr:rel:f:ETH1:9f86d0…:1234`, `sr:chaintip:ETH1`.

The finalized/temp split is **behavior, not just eviction segregation**, so it survives the mapping. `SetRelay` routes writes into distinct stores by the `Finalized` flag; the same `(hash, block)` identity can legitimately hold both variants at once — and they store *different values* (`formatCacheValue`: the non-finalized variant retains the block `Hash` used for hit validation, the finalized one clears it, and TTL selection keys off hash presence). `findInAllCaches` gives lookups a finality-dependent precedence order with fallback to the other store. A single shared Redis key would make the second write clobber the first — last-write-wins, a parity break. The RESP adapter therefore keeps two namespaces: a lookup fetches both variants through `KVStore.GetEntries` — one pipeline execution, which in Cluster mode means one round trip per involved shard, issued concurrently (the two keys usually hash to different slots) — and applies the same precedence order. These remain independent single-key commands (no `MULTI`, no multi-key Lua), so Cluster mode still needs no hash-tag co-location.

**D6 — Value envelope: JSON + existing gzip.** One versioned envelope struct mirroring `main`'s `CacheValue` field-for-field (response bytes, `SeenBlock` — the server-side seen-block validity check compares against it, so it must persist — block hash, optional metadata, `IsCompressed`), marshaled as JSON — consistent with this fork's JSON wire codec (`types/relay/proto_compat.go`) and its additive-fields compatibility rules. Gzip above the existing 1 KiB threshold is retained; it also cuts cross-region replication bandwidth, which is the PRD's cost driver. A leading version field leaves room for a binary codec later — and for additive fields (e.g. future entry-kind extensions such as `IsNodeError`/`StatusCode`).

**D7 — TTLs map to native Redis expiry; eviction is documented, not implemented.** The existing TTL table (finalized default 1 h, non-finalized `max(avgBlockTime/8, 500ms)`, node errors `min(avgBlockTime, 250ms)`, h2h 48 h, tip `SharedStateTipExpiration()`) is applied per-`SET` via `PX`. Memory pressure is handled by the server's `maxmemory-policy`; docs will recommend `volatile-lru` (every key we write carries a TTL) with `allkeys-lru` as the aggressive alternative, plus tradeoffs (PRD nice-to-have).

**D8 — Read/write split via two client instances.** `addresses` builds the write client; optional `read-addresses` builds a separate read client (Global Datastore exposes distinct endpoints per region). `GetRelay` → read client; `SetRelay`/tip CAS/flush → write client; unset `read-addresses` → one shared client. Replication lag on replica reads degrades to a cache miss — the seen-block validity check runs in the engine, so a stale replica can never serve a block older than the client's `seenBlock`.

**D9 — Credential providers: streaming, so rotation reaches live connections.** The PRD's criterion is re-authentication "without restart or connection loss" — reload-on-reconnect is not enough. go-redis exposes `StreamingCredentialsProvider` — the field appeared in v9.9.0 as a documented placeholder; the working subscribe → re-auth path exists from **v9.11.0** (verified in that tag's `redis.go` — the historical API floor). The production pin is stricter: **≥ v9.20.0**, preferably the current vetted release — v9.19.0 fixed a repeated close-wrapper resource leak and v9.20.0 a `Pool.CloseConn` hook leak, both of which matter for a long-lived pool with a rotation-driven connection lifecycle. Connections subscribe to the provider, and when it pushes new credentials the client re-`AUTH`s **existing pooled connections in place**. Config supports three sources behind one internal provider interface mapped onto it:
- `username` + `password` — static AUTH (one-shot provider),
- `password-file` — watched for changes (poll/fsnotify); an update pushes fresh credentials to every subscribed connection. Covers Kubernetes-mounted rotating secrets and IRSA-style token refreshers with no cloud-SDK coupling,
- a Go interface hook for custom builds (e.g. an IAM SigV4 token signer) plugging into the same streaming path.

Sentinel deployments carry a second, separate credential pair: the **sentinel control plane** authenticates independently of the data nodes. `sentinel-username` / `sentinel-password[-file]` map to go-redis `SentinelUsername`/`SentinelPassword` (present in both `FailoverOptions` and `UniversalOptions`); without them, discovery against hardened sentinels fails before a data connection is ever attempted.

**Accepted deviation — sentinel rotation is per-connection, not in-place.** go-redis (verified in v9.22) accepts `StreamingCredentialsProvider` in `FailoverOptions` but `NewFailoverClient` never initializes the streaming re-auth manager, so the first operation nil-panics. Sentinel therefore resolves data-node credentials fresh on every connection attempt (`CredentialsProviderContext`): rotation applies on reconnects and failovers, and the standard dual-credential grace window makes it operationally seamless. Standalone and cluster keep the full in-place streaming path. Disclosed in the operator guide; revisit when the upstream gap is fixed.

The Phase 3 rotation test proves the criterion literally, and it requires a **real Valkey/Redis** — miniredis implements neither `CONFIG` nor `CLIENT`, so it cannot express it. A docker-backed integration test with a one-connection pool rotates between two ACL users and pushes the new credentials through the provider while ops keep flowing; it asserts that the ORIGINAL connection id survives and reports the new `user=` in `CLIENT LIST` (the actual proof of in-place re-`AUTH` — mere op success proves nothing, since Redis never de-authenticates existing connections on a password change) and that an instrumented dialer stays bounded. Note: go-redis schedules re-auth via pool-event hooks and may legitimately dial ONE extra connection while the original re-auths (checkout prefers a fresh conn over blocking), so the criterion is "original connection survives with the new identity, no reconnect storm" — not literally zero dials.

**D10 — Flush, health, failure mode.**
- `Flush` (`/debug/reset-all` path) = prefix-scoped `SCAN` + `UNLINK`, with `UNLINK` issued as **pipelined single-key commands** — a multi-key `UNLINK` over a scan page would fail with `CROSSSLOT` in Cluster, where multi-key operations require all keys in one hash slot (masters are iterated in Cluster mode). `FLUSHDB` is never issued — the backend may be shared. `SCAN MATCH` patterns are **globs**, so the prefix is restricted to `[A-Za-z0-9._-]+` (non-empty; `*`, `?`, `[` rejected at startup) — a glob-unsafe prefix could silently match and delete unrelated keys. Deployments sharing one backend that need flush isolation must use distinct prefixes; documented in the operator guide.
- `CacheActive()` = configured; a background `PING` loop drives a connectivity gauge and reconnect logging (nice-to-have UC). go-redis reconnects per-operation, so no bespoke reconnect loop is needed (unlike the gRPC client).
- Degraded mode requires **no new logic**: call sites already treat `GetEntry` errors/timeouts as misses within the 50 ms `CacheTimeout` budget, and writes are async best-effort. We verify with tests and add the connection-failure counter (UC-4).

**D11 — Config surface and precedence (UC-5).** A viper/YAML block (flags for the common path), validated fail-fast at startup:

```yaml
resp-cache:
  topology: standalone            # standalone | sentinel | cluster
  addresses: ["cache.example:6379"]  # sentinel: sentinel addrs; cluster: configuration endpoint
  master-name: ""                 # sentinel only
  read-addresses: []              # optional read endpoint(s)
  username: ""
  password: ""                    # or:
  password-file: ""
  sentinel-username: ""           # sentinel control-plane auth — distinct from node creds
  sentinel-password: ""           # or:
  sentinel-password-file: ""
  credential-refresh-interval: 10s  # password-file poll cadence (0 = 10s default)
  tls:
    enabled: false
    ca-file: ""
    cert-file: ""
    key-file: ""
    server-name: ""
    insecure-skip-verify: false
  key-prefix: "sr"                # [A-Za-z0-9._-]+ only — used in SCAN MATCH globs
  db: 0                           # standalone/sentinel only
  # Recommended EXAMPLE values, not Smart Router defaults. Left unset (zero),
  # each delegates to the pinned go-redis default (5s read/write) — see
  # docs/RESP-CACHE.md, which describes them as "client defaults". Socket I/O
  # is bounded by the CALLER's context regardless, because ContextTimeoutEnabled
  # is set on every client (D4); these values only cap an otherwise-unbounded
  # wait when no caller deadline is in play.
  dial-timeout: 500ms
  read-timeout: 30ms
  write-timeout: 100ms
  pool-size: 0                    # 0 = go-redis default
```

Rules: `resp-cache` set → RESP is the primary backend — **including when `cache-be` is also set**, with a prominent startup log naming the winner. Coexistence is deliberate, not an error: the PRD's rollback flow is "remove the RESP configuration and the previous cache takes over", which only works if a preserved `cache-be` may sit alongside the RESP block. No `resp-cache` → exactly today's behavior (`cache-be` → gRPC client, byte-for-byte unchanged; neither → no cache) — that is UC-5. Validation stays fail-fast for the RESP block itself (unknown topology, missing `master-name` in sentinel mode, `sentinel-*` credentials with a non-sentinel topology, unreadable TLS/credential files, an empty or glob-unsafe `key-prefix`, dangling sub-options without addresses).

## 4. Phased delivery plan

Phases are sequential; each is a small reviewable PR (squash-merge) that leaves `main` green. Estimates are focused dev-days including tests.

### Phase 0 — Backend interface seam (refactor only) — ~1–2 d
- Define `performance.CacheBackend` from scratch (in `protocol/performance/`): `CacheActive() bool`, `GetEntry`, `SetEntry`, `Flush`, `Close() error` — today's concrete `Cache` becomes its first implementation (`var _ CacheBackend = (*Cache)(nil)`; its `Close` stops the background reconnect loop and closes the gRPC conn), and the router's graceful shutdown gains a backend `Close` call.
- Retype `RPCSmartRouterServer.cache`, constructor params, `ChainFetcher`'s cache field (`chain_fetcher.go:41`), and the debug-server flush wiring off the concrete `*performance.Cache`.
- **Exit:** no behavior change; full test suite green.

### Phase 1 — Extract the cache engine from `handlers.go` — ~3–4 d
- New `ecosystem/cache/core`: `Engine`, `KVStore`, `TTLPolicy`, envelope type (D2); move logic verbatim from `RelayerCacheServer`.
- Ristretto `KVStore` adapter wrapping the three existing stores (incl. retry-confirm `SetInt64IfGreaterOrEqual`); `RelayerCacheServer` becomes a thin gRPC shim over the engine.
- **Exit:** every existing `ecosystem/cache` test passes unchanged (`flush_test`, `shared_state_tip_test`, `metrics_test`); new engine unit tests for key formatting, TTL selection, and the seen-block validity rule (undertested on `main` — pin it before porting it). This is the riskiest phase — mitigation is verbatim movement plus the existing behavioral suites.

### Phase 2 — RESP adapter + `RespCache` backend (standalone, static AUTH) — ~4–5 d
- Add `github.com/redis/go-redis/v9`; Redis `KVStore` adapter: `GET`/`SET PX`, single-key Lua CAS for the tip, `SCAN`+`UNLINK` prefix purge, `PING`.
- `performance.RespCache`: engine + adapter behind `CacheBackend`; per-op timeouts honoring caller contexts. Its `Close` owns full teardown: both read/write clients, the credential-provider subscription, and the `PING` health loop.
- Key schema (D5) + JSON/gzip envelope (D6).
- **Parity harness (new):** a small helper that spins the real `ecosystem/cache` server on an in-process listener (`RegisterRelayerCacheServer` + `bufconn`) and returns a connected `performance.Cache` client; the behavioral suites are parameterized to run against both that and `RespCache` over **miniredis** (in-process Redis with TTL clock control; no Docker in unit tests).
- **Exit:** parity suite green on both backends, including node-error TTLs, seen-block rejection, compression round-trip, tip monotonicity **with TTL refresh on equal observations** (stalled-chain case), and finalized/non-finalized **variant coexistence** under one logical `(hash, block)` identity — write both variants, assert lookup precedence and fallback match the gRPC backend.

### Phase 3 — Topologies, TLS, credentials, read/write split — ~3–4 d
- `UniversalOptions` mapping: standalone / sentinel (`master-name`) / cluster (configuration endpoint as seed).
- TLS config plumbing; streaming credential providers (static / file-watch / interface hook, D9).
- Dual read/write clients (D8).
- **Exit:** unit tests for config→client mapping (incl. sentinel control-plane credentials → `SentinelUsername`/`SentinelPassword`, and their sentinel-only validation); AUTH against miniredis; provider-plumbing unit tests (fake provider → pushes reach the subscription listener); **live-rotation integration test against real Valkey** (docker-backed — miniredis lacks `CONFIG`/`CLIENT`): one-connection pool, rotate between two ACL users via the provider, ops uninterrupted, the original `CLIENT ID` survives showing the new `user=`, dial count bounded (no reconnect storm); **TLS acceptance** against `miniredis.RunTLS` — trusted CA + correct server name succeeds, wrong or missing CA fails, plus an mTLS round-trip exercising the client cert/key fields; read/write op routing (spy clients).

### Phase 4 — Router wiring + config surface — ~2 d
- `RespCacheConfig` + fail-fast `Validate()` in `protocol/performance/`; viper block + flags; registration and init in `protocol/rpcsmartrouter/rpcsmartrouter.go` alongside the existing cache init.
- Precedence rules (D11): RESP wins when both backends are configured (loud startup log); removing the RESP block reverts to the preserved `cache-be`; fail-fast validation for the RESP block itself.
- **Exit:** config validation matrix test; cobra/viper wiring test through the real cobra command's flags and YAML (`protocol/rpcsmartrouter/resp_cobra_viper_test.go` — builds `CreateRPCSmartRouterCobraCommand()`, asserts the shipped command registers `--resp-cache-addresses`/`--resp-cache-topology`, replays RunE's `BindPFlags`→`ReadInConfig` order on the real global viper, and proves flag-over-YAML precedence, that RESP env vars stay unbound, and that absent RESP config leaves the legacy `cache-be` path intact) (flags outrank YAML; env vars stay unbound, per repo convention); precedence + rollback regression (both set → RESP serves; RESP block removed → gRPC serves; neither → no cache); UC-5 regression: no RESP config → identical startup path.

### Phase 5 — Observability + resilience — ~2 d
- `smartrouter_resp_cache_connection_errors_total`, `smartrouter_resp_cache_failed_total{op="get"|"set", kind="error"|"timeout"}`, connectivity gauge from the `PING` loop, go-redis pool stats gauges; reconnect/auth-failure logging — the health probe preserves the `Ping` error rather than reducing it to a boolean, classifies it with `redis.IsAuthError`, and logs startup failures and connected→disconnected transitions with a `failure=authentication|connection` attribute. Auth detail collapses to a fixed phrase so no credential, username or server credential-reply reaches the log; logging stays transition-only so a persistent failure cannot flood it.
- The existing `smartrouter_cache_*` hit/miss series keep firing unchanged through the `CacheBackend` seam — no dashboard migration. They cannot tell an error or timeout from a clean miss, so the new `resp_cache` counters carry that split; they are UC-4's alerting hook. Future work can layer per-tier/outcome labels onto the shared series without conflicting.
- Degraded-mode tests: backend down at startup, backend dies mid-run, slow backend (miniredis latency) → misses within budget, router serves via providers, counters increment (UC-4).
- **Exit:** metrics documented in `docs/METRICS.md`; failure tests green — including the budget-discrimination test: a frozen ESTABLISHED connection with client Read/WriteTimeout set long (5s) must return within the caller's 150ms deadline, provable only with `ContextTimeoutEnabled` (the cold-handshake case is separately bounded by `dial-timeout`).

### Phase 6 — Integration lanes + docs — ~3–4 d
- `docker/docker-compose.resp-cache.yml` overlay: valkey service replacing the cache container; router configured via YAML. This is the ONLY compose overlay — TLS, Sentinel and Cluster are covered by self-contained docker-gated Go tests instead (below), which provision and tear down their own containers and therefore need no committed compose variant or shell driver.
- `scripts/pre_setups/init_smartrouter_eth_resp_cache.sh`: bare-metal lane in the style of the existing `pre_setups` scripts (backgrounded processes per repo convention — the script must leave the router running). It runs the CHECKED-IN `smartrouter_eth_resp_cache.yml`, so the lane validates the same HTTP+WS fixture operators are handed, and does NOT pass `--skip-websocket-verification` (bypassing the ETH1 websocket requirement would mask the very property the fixture correction fixed); `--resp-cache-addresses` overrides the config's compose address, which also exercises flag-over-YAML precedence. **Ownership safety:** the lane never signals a process because its executable is named `smartrouter` and never force-removes a same-named container — it reclaims only what it can prove it created (pid file with a start-time fingerprint; container carrying its own label) and otherwise refuses to run, leaving foreign processes untouched. `RUN_DEMO=1` asserts router `/lava/health`, `/metrics/overall-health`, the connectivity gauge, a real relay result, keys under the prefix, and a genuine cache hit. `scripts/pre_setups/test_resp_lane_ownership.sh` is the regression harness proving a foreign listener causes a clean refusal with no signal sent.
- Sentinel failover drill: an executable docker-gated test (`TestSentinelFailover`) — primary + replica + 3 sentinels with `requirepass` so discovery exercises the control-plane credentials; the test stops the primary and asserts the SAME store resumes writing after promotion with no re-construction, then waits for the sentinel view itself to converge on the promoted replica before asserting the reported master address (UC-2 acceptance).
- Cluster: an executable docker-gated test (`TestClusterDocker`, run green) — three masters joined through ONE configuration endpoint, writes spread across slots, cross-slot pipelined lookups, and the purge deleting one prefix on every master while a second prefix survives in full.
- Real-server TLS: an executable docker-gated test (`TestTLSDockerValkey`, run green) — Valkey terminating mTLS (`--tls-auth-clients yes`) with the generated PKI mounted in; certificate-less clients rejected by the real server.
- Readiness timing documented: `/metrics/overall-health` is fail-closed until the first relays health-check cycle (`--relays-health-interval`, 5m default) — a freshly started stack reads 503 while already serving; not a cache property. `RelaysMonitorAggregator.StartMonitoring` acts only on the first ticker TICK (there is no immediate initial sweep), so the 503 window is the FULL interval, not a fraction of it. The lane therefore runs with a shortened `--relays-health-interval` (`HEALTH_INTERVAL`, 15s) and POLLS the endpoint to a bounded deadline: readiness is observed rather than slept for, and the assertion stays real instead of being weakened to accommodate the production default. The same ordering bites twice more and the lane handles both explicitly: the RPC listener binds AFTER the backend-selection log line, so `/lava/health` is polled rather than sampled once, and the ownership pid is recorded only once the port is actually bound — an unrecorded router cannot be distinguished from a foreign one, which would make the next run refuse on the lane's own leftover process.
- Customer-facing `docs/RESP-CACHE.md`: config reference, topology examples (ElastiCache/MemoryDB endpoints), `maxmemory-policy` recommendation + tradeoffs, cold-start rollout note (config-change-only switch, no data migration), flush semantics. Update `docs/LOCAL-COMPOSE.md`.
- **Exit:** demo lanes pass; docs reviewed.

**Total: ~18–24 dev-days.** Critical path: 0 → 1 → 2; phases 3–5 partially parallelizable across two people after Phase 2.

## 5. Acceptance-criteria traceability

| PRD acceptance criterion | Covered by |
|---|---|
| Router with RESP backend uses it for all cache operations | Phases 0/2/4; parity suite + compose lane |
| Connection details (endpoint, TLS, AUTH, topology) configurable without code changes | Phase 4 (YAML/flags); Phase 3 |
| No RESP config → default cache (backwards compatible); rollback = remove RESP block | Phase 4 precedence + rollback regressions (both set → RESP wins; RESP removed → preserved `cache-be` takes over; neither → unchanged) |
| All caching features work equivalently | Phases 1–2 (shared engine + parity suite, incl. finalized/temp variant coexistence and stalled-tip TTL refresh) |
| Connects to Standalone / Sentinel / Cluster | Phase 3; Phase 6 lanes |
| Sentinel failover → transparent reconnect, no restart | Phase 3 (failover client + sentinel control-plane auth mapping); Phase 6 drill with authenticated sentinels |
| AUTH + TLS supported | Phase 3 (TLS acceptance vs `miniredis.RunTLS` incl. negative cases + mTLS; real-server TLS lane in Phase 6) |
| Dynamic credential providers; rotation without restart/connection loss | Phase 3 (streaming provider, D9; real-Valkey live-rotation test with stable `CLIENT ID`). Standalone/cluster: in-place. Sentinel: per-connection refresh — accepted deviation (upstream gap, see D9) |
| Read/write endpoints configured separately; ops routed accordingly | Phase 3 (dual clients + routing test) |
| Cluster configuration endpoint / topology discovery | Phase 3; Phase 6 cluster lane |
| Cache failures don't crash the router; degraded mode; Prometheus-visible | Phase 5 (new resp-cache failure + connection counters; degraded-mode tests) |

## 6. Testing strategy summary

- **Unit / parity:** behavioral suites parameterized over both backends; miniredis for the RESP side (deterministic TTL clock, AUTH support, no Docker). Engine tests pin key schema and TTL policy.
- **Integration:** one compose overlay (standalone valkey) + one `pre_setups` bare-metal script with a metric-asserting `RUN_DEMO` mode, following the existing `pre_setups`/compose-overlay patterns. Sentinel, Cluster and TLS are NOT compose artifacts: each is a self-contained docker-gated Go test (`TestSentinelFailover`, `TestClusterDocker`, `TestTLSDockerValkey`) that provisions and tears down its own containers. Each is gated on BOTH docker availability and an explicit env var (`RESP_CACHE_TEST_SENTINEL_DOCKER`, `RESP_CACHE_TEST_CLUSTER_DOCKER`, `RESP_CACHE_TEST_TLS_DOCKER`) — without them the tests `SKIP` and the package still reports `ok`, so a reader who does not set them will not see the lanes run.
- **Regression:** the entire existing cache test surface must stay green after Phases 0–1 with zero test edits (except imports) — that is the parity guarantee for UC-5.

## 7. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Engine extraction destabilizes the existing cache (highest risk) | Verbatim code movement; existing behavioral suites as the safety net; land as its own PR before any RESP code |
| Concurrent cache-subsystem work landing mid-flight (`ecosystem/cache/handlers.go`, the router cache path, and the metrics manager are shared surfaces) | Land Phase 1 early: the engine extraction subsumes the `handlers.go` surface, and later features rebase onto the `CacheBackend`/engine seams |
| Semantic drift between async ristretto and sync Redis (write visibility timing) | `SetInt64IfGreaterOrEqual` abstraction (D3); parity suite runs identical scenarios on both |
| Extra RTTs per op (entry + tip + h2h) inflate lookup latency | Same-AZ RTT ≪ 50 ms `CacheTimeout` budget; pipeline where ops batch; per-op read timeout ~30 ms. **No lane asserts a latency threshold** — the compose/bare-metal lanes prove function, not timing, and a wall-clock bound measured against a live public endpoint would be flaky rather than informative. What IS enforced executably is the stronger property: the CALLER's deadline bounds socket I/O (`ContextTimeoutEnabled` on every client, D4), pinned by `TestRespCacheSlowBackendTimesOutWithinBudget` and `TestRespCacheCallerBudgetBoundsEstablishedConnections` — so a slow backend cannot exceed the relay's budget regardless of the configured read timeout |
| Cluster cross-slot pitfalls | No multi-key commands by design (D5); single-key Lua only; the cluster lane exercises configuration-endpoint discovery, cross-slot writes/pipelined reads, and prefix-scoped purge across every master. **Resharding tolerance is NOT covered**: `TestClusterDocker` builds a static three-master cluster and never migrates slots during operation. Live-reshard behaviour rests on go-redis's MOVED/ASK handling and remains unproven here. |
| JSON envelope bloat → replication bandwidth cost | Gzip retained ≥1 KiB; versioned envelope allows a binary codec later without migration |
| Credential rotation misfires (provider stalls, token expires before the push) | In-place streaming re-auth means rotation itself causes no reconnects by design; a missed or late push surfaces as auth failures on next use — caught by auth-failure logging, the connection-error counter, and the live-rotation test |
| Shared-backend flush blast radius | Prefix-scoped `SCAN`+`UNLINK` only; `FLUSHDB` never issued |
| New dependency (go-redis v9) | Single, widely-deployed BSD-2 pure-Go dep; pinned in `go.mod` |

## 8. Stance on PRD open questions

1. **Does the current cache protocol map cleanly to RESP?** Yes. Only two operations aren't plain KV: the shared-state tip write (single-key Lua CAS with refresh-on-equal, D3) and flush (prefix scan, D10). Everything else is per-key GET/SET with TTLs; the finalized/temp store split maps to two key namespaces with the same lookup precedence (D5). No blockers found.
2. **Circuit breaker for provider saturation during cache failure?** Out of scope. Current degraded fall-through is preserved; the new connection metrics give operators the alerting hook. Recommend a follow-up ticket for load-shedding once real degradation data exists.
3. **Configurable failure modes?** Out of scope for v1 (fall-through only). The `CacheBackend` seam is the natural place to add a failure-mode policy later; noted as a design hook, not built.
4. **Reachable-but-slow backend?** Already handled: lookups run under the 50 ms `CacheTimeout` budget and slow responses are treated as misses, recorded with the `timeout` outcome label; the RESP client adds its own dial/read/write timeouts. No new mechanism needed.
5. **Populator coordination across routers sharing one cache?** Unchanged: each router still decides independently what to populate and performs its own upstream fetch + write — the shared backend does **not** deduplicate that work; it makes the unconditional overwrites harmless (identical payloads, last-write-wins) and lets the first completed write serve every router's subsequent reads, which is where the resource saving actually comes from. Cross-region access-pattern coordination is explicitly deferred until multi-region telemetry exists — matching the PRD's "pre-existing question, not a blocker".

## 9. Out of scope / follow-ups

- A read-only secondary cache tier (two-zone deployments), RESP-backed or otherwise. The `CacheBackend` seam makes it a small follow-up (a read-only view is a strict subset of the interface), and multi-region replication largely obviates the two-tier zone pattern anyway.
- Swapping the `smartrouter cache` server's store to Redis (falls out of the engine split if ever wanted).
- AWS-specific IAM token signing (enabled via the credential-provider hook, deliberately not bundled).
- Circuit breaker / configurable failure modes (open questions 2–3).
- Binary value codec (envelope is versioned to allow it).
