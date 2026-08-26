# RESP Cache Backend (Redis / Valkey)

The Smart Router can run its cache against any **RESP-compatible backend**
(Redis, Valkey, and managed services such as AWS ElastiCache or MemoryDB)
instead of the default `smartrouter cache` sidecar. The router executes the
same cache engine in-process — lookup rules, validity checks, and TTLs are
identical to the default cache — and stores entries in the backend you
configure.

What this buys you over the in-memory sidecar:

- **Persistence** — cache survives router and cache restarts; no re-warming
  from your upstream nodes.
- **Shared state** — every router replica reads and writes the same cache, so
  horizontal scaling stops costing hit rate.
- **High availability** — Sentinel or Cluster failover is handled by the
  backend and followed transparently by the router.
- **Multi-region replication** — with infrastructure such as ElastiCache
  Global Datastore, entries cached in one region serve reads in others; the
  router only needs the read/write endpoint split below.

The goal is resource efficiency — fewer calls to (and fewer copies of) your
blockchain node infrastructure. Latency wins are a side effect.

## Quick start

Docker Compose (starts a valkey next to the router):

```bash
SR_CONFIG=config/smartrouter_examples/smartrouter_eth_resp_cache.yml \
  docker compose -f docker/docker-compose.yml \
                 -f docker/docker-compose.resp-cache.yml up --build
```

Bare metal against an existing backend — one flag:

```bash
smartrouter config.yml --resp-cache-addresses "my-valkey:6379"
```

or the config block (the full surface lives here; an explicitly passed flag
outranks the YAML value, and environment variables are not read):

```yaml
resp-cache:
  addresses: ["my-valkey:6379"]
```

## Configuration reference

Everything lives under the `resp-cache:` block. Setting any of it **without
`addresses`** is rejected at startup (dangling configuration), as is every
invalid combination below — the router never starts half-configured.

| Key | Default | Meaning |
| --- | --- | --- |
| `topology` | `standalone` | `standalone` \| `sentinel` \| `cluster`. |
| `addresses` | — (required) | Standalone: the node address. Sentinel: the **sentinel** addresses. Cluster: the **configuration endpoint** used as the discovery seed — never a node list; the client discovers topology itself. |
| `read-addresses` | *(unset)* | Optional separate endpoint(s) for **reads** (reader endpoints). Writes stay on `addresses`. Selects an *endpoint*, not a replica role — read the topology caveat in the multi-region note below before using it with `sentinel` or `cluster`. |
| `master-name` | — | Sentinel only (required there): the monitored master set name. |
| `username` / `password` | *(unset)* | Static data-node credentials (AUTH / ACL). |
| `password-file` | *(unset)* | Rotation-capable credentials: the file is polled and changes are pushed to **live connections**, which re-authenticate in place — no restart, no connection loss. The file holds the password, or `username:password` to rotate the ACL user too. Mutually exclusive with `password`. |
| `credential-refresh-interval` | `10s` | Poll cadence for `password-file`. |
| `sentinel-username` / `sentinel-password` / `sentinel-password-file` | *(unset)* | **Sentinel control-plane** credentials — sentinels authenticate independently of the data nodes; hardened deployments fail discovery without these. Only valid with `topology: sentinel`. |
| `db` | `0` | Logical database (standalone/sentinel only; rejected for cluster). |
| `key-prefix` | `sr` | Namespace for every key. Restricted to `[A-Za-z0-9._-]+` (flush uses it as a `SCAN MATCH` glob). Give each deployment sharing a backend its own prefix — flush isolation follows from it. |
| `tls.enabled` | `false` | TLS to the backend. |
| `tls.ca-file` | *(system pool)* | PEM CA bundle for server verification. |
| `tls.cert-file` / `tls.key-file` | *(unset)* | Client keypair for mTLS (both or neither). |
| `tls.server-name` | *(unset)* | Overrides the verification/SNI name. |
| `tls.insecure-skip-verify` | `false` | Skips server verification (testing only). |
| `dial-timeout` / `read-timeout` / `write-timeout` | client defaults | Per-operation network limits. Note: the handshake of a **fresh** connection is bounded by `dial-timeout`, not by the relay's per-lookup budget — keep it tight if a hung backend must not slow cold lookups. |
| `pool-size` | client default | Connection pool size (per client; the read client has its own). |

TTLs are the cache engine's own (finalized ~1h, non-finalized scaled to the
chain's block time, short-lived node errors) — the same table the default
cache uses.

## Topologies

**Standalone** — one address; also the shape for managed *primary/reader
endpoints* (cluster-mode-disabled):

```yaml
resp-cache:
  addresses: ["cache.internal:6379"]
```

**Sentinel** — automatic failover. The router connects to the sentinels,
discovers the primary, and follows promotions transparently — no restart, no
manual intervention. Note the two independent credential domains:

```yaml
resp-cache:
  topology: sentinel
  addresses: ["sentinel-1:26379", "sentinel-2:26379", "sentinel-3:26379"]
  master-name: "mymaster"
  password-file: /etc/smartrouter/resp-cache.pw
  sentinel-password-file: /etc/smartrouter/resp-sentinel.pw
```

**Cluster** — sharded. Point at the **configuration endpoint** (e.g. the
ElastiCache cluster configuration endpoint); node membership, slots, and
replicas are discovered and tracked automatically:

```yaml
resp-cache:
  topology: cluster
  addresses: ["my-cluster.cfg.euw1.cache.amazonaws.com:6379"]
```

## Multi-region reads (read/write split)

With replicating infrastructure (ElastiCache Global Datastore, MemoryDB
Multi-Region), give routers in secondary regions their local reader endpoint:

```yaml
resp-cache:
  addresses: ["primary.global.cache:6379"]      # writes
  read-addresses: ["reader.eu-west-1.cache:6379"] # reads
```

Reads go to `read-addresses`, writes (including cache population and flush) to
`addresses`. Replication lag is safe by construction: an entry that hasn't
replicated yet is a plain cache miss, and the router's block-freshness
validation (seen-block rules) runs on every hit — a lagging replica can never
serve data older than what the client has already seen.

**Topology caveat — this selects an endpoint, not a replica role.** Under
`standalone` the addresses are dialled exactly as given, so a managed reader
endpoint really does serve the reads; that is the shape this feature is for.
Under `sentinel` and `cluster` the read client runs its **own discovery** from
the seeds you give it and resolves to the master(s) of whatever topology they
front — so pointing it at replicas of the *same* deployment routes your reads
straight back to the primary and buys nothing. It is still meaningful when it
points at a **separate replicated deployment** (a regional cluster or sentinel
set that the infrastructure replicates into), which is why the router logs a
warning here rather than rejecting the config. If you want replica reads
within one sentinel set or cluster, that is not currently supported — use the
managed reader endpoint in `standalone` shape instead.

## Credential rotation (IAM-style tokens)

Use `password-file` with whatever refreshes the file (Kubernetes secret
mounts, a sidecar token refresher). On change, the router pushes the new
credentials to every live connection, which re-authenticates **in place** —
verified against real Valkey by rotating between two ACL users and observing
the same server-side connection re-authenticate as the new user. Custom
credential sources (e.g. an IAM SigV4 signer) can implement the
`CredentialsSource` interface in `ecosystem/cache/redisstore`; the router
deliberately bundles no cloud SDKs.

Sentinel-topology caveat: the go-redis failover client (v9.22) does not
support in-place streaming re-auth, so under `topology: sentinel` rotated
credentials are resolved fresh **per connection attempt** — they apply on
reconnects and failovers rather than being pushed to idle connections. Keep
the previous credential valid for a rotation grace window (standard ACL
dual-credential practice) and rotation is seamless there too.

## Flush semantics

The router's `/debug/reset-all` flushes the RESP backend **prefix-scoped**:
`SCAN` over `key-prefix:*` with single-key `UNLINK`s. `FLUSHDB` is never
issued, so a shared backend's other tenants (and other prefixes) are
untouched. If two deployments must be flush-isolated, give them distinct
prefixes.

## Sizing and eviction (`maxmemory-policy`)

Recommended: **`volatile-lru`** with a `maxmemory` fitting your working set.

- Every key the router writes carries a TTL, so `volatile-lru` can evict
  across the router's whole keyspace by recency — and it will never touch
  non-TTL keys owned by other applications on a shared backend.
- `allkeys-lru` behaves identically on a dedicated backend and is the safer
  choice if you ever write non-TTL keys under memory pressure being preferable
  to write errors; on a shared backend it can evict other tenants' data.
- Avoid `noeviction` for cache workloads: at `maxmemory` the router's cache
  writes start failing (visible in `smartrouter_resp_cache_failed_total`)
  until TTLs free space — the cache keeps serving hits, but stops growing.

Blockchain cache entries skew heavily toward the long finalized TTL, so steady
state approaches `maxmemory` and stays there — that is eviction working as
intended, not a leak.

## Failure behavior and monitoring

A failing backend **never fails requests**: lookups degrade to cache misses
within the relay's budget and requests proceed to your upstreams; writes are
best-effort. Recovery is automatic. Alert on the dedicated series (full
reference in [METRICS.md](METRICS.md#resp-cache-backend--smartrouter_resp_cache_)):

- `smartrouter_resp_cache_connected` — 0 after a failed health probe (PING,
  10s cadence); reachability transitions are also logged.
- `smartrouter_resp_cache_failed_total{op, kind}` — backend-level operation
  failures (never clean misses), with `kind` splitting `error` from `timeout`
  so saturation reads differently from outage.
- `smartrouter_resp_cache_connection_errors_total`, pool gauges.

The shared `smartrouter_cache_*` hit/miss series keep working unchanged.

## Rollout, precedence, and rollback

Switching backends is a configuration change; the RESP cache starts cold (no
data migrates).

- `resp-cache:` configured → the RESP backend serves, **including when
  `cache-be:` is also set** (the router logs a prominent warning naming the
  precedence). Keeping `cache-be:` in place is deliberate — it is the
  rollback path.
- Rollback = delete the `resp-cache:` block (or flag). The preserved
  `cache-be:` takes over on the next start.
- Neither configured → no cache, exactly as before. Existing deployments need
  zero changes.

## Local testing lanes

- **Compose**: the quick start above; inspect keys with
  `docker exec smart-router-valkey-1 valkey-cli --scan --pattern 'sr:*'`.
- **Bare metal**: `scripts/pre_setups/init_smartrouter_eth_resp_cache.sh`
  (docker-backed valkey + router; `RUN_DEMO=1` proves relay → populate →
  RESP-backed hit and leaves everything running).
- **Live credential rotation** (real Valkey, ACL users, in-place re-auth):
  `docker run --rm -p 127.0.0.1:63790:6379 valkey/valkey:7.2` then
  `RESP_CACHE_TEST_VALKEY_ADDR=127.0.0.1:63790 go test ./ecosystem/cache/redisstore -run TestLiveRotationAgainstRealValkey -v`.
- **Sentinel failover** (dockerized primary/replica/3 sentinels, kill the
  primary, the same store keeps serving through the promotion):
  `RESP_CACHE_TEST_SENTINEL_DOCKER=1 go test ./ecosystem/cache/redisstore -run TestSentinelFailover -v -timeout 5m`.
- **Real-server TLS/mTLS** (dockerized Valkey terminating TLS with
  `--tls-auth-clients yes`; certificate-less clients rejected):
  `RESP_CACHE_TEST_TLS_DOCKER=1 go test ./ecosystem/cache/redisstore -run TestTLSDockerValkey -v -timeout 3m`.
- **Real cluster** (three dockerized masters joined via one configuration
  endpoint; writes spread across slots, cross-slot pipelined lookups, and the
  purge deletes one prefix on every master while a second prefix survives):
  `RESP_CACHE_TEST_CLUSTER_DOCKER=1 go test ./ecosystem/cache/redisstore -run TestClusterDocker -v -timeout 5m`.

Readiness timing note: `/metrics/overall-health` (and the container health
that follows it) starts **fail-closed** and reports 503 until the first
relays health-check cycle completes — `--relays-health-interval` defaults to
5 minutes. A freshly started stack showing 503 while serving relays is
warming up, not broken; pass a shorter interval for demos.
