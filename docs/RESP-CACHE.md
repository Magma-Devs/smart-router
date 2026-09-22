# RESP Cache Backend (Redis / Valkey)

The Smart Router can run its cache against any **RESP-compatible backend** (Redis, Valkey,
and managed services such as AWS ElastiCache or MemoryDB) instead of the default
`smartrouter cache` sidecar. The router executes the same cache engine in-process — lookup
rules, validity checks, and TTLs are identical to the default cache — and stores entries in
the backend you configure.

Parity is structural rather than reimplemented: the cache semantics live in one
storage-agnostic engine (`ecosystem/cache/core`) that both the sidecar and this backend
execute, over a Ristretto store and a Redis/Valkey adapter respectively. The router consumes
both through one interface, so no call site can tell them apart.

## When to use it

The default sidecar holds cache state in memory, per process. Reach for a RESP backend when
that costs you something concrete:

- **Persistence** — cache survives router and cache restarts; no re-warming from your
  upstream nodes.
- **Shared state** — every router replica reads and writes the same cache, so horizontal
  scaling stops costing hit rate.
- **High availability** — Sentinel or Cluster failover is handled by the backend and followed
  transparently by the router.
- **Multi-region replication** — with infrastructure such as ElastiCache Global Datastore,
  entries cached in one region serve reads in others; the router only needs the read/write
  endpoint split below.

The goal is resource efficiency — fewer calls to (and fewer copies of) your blockchain node
infrastructure. Latency wins are a side effect.

## Quick start

One flag against an existing backend:

```bash
smartrouter config.yml --resp-cache-addresses "my-valkey:6379"
```

Or the config block — the full surface lives here; an explicitly passed flag outranks the
YAML value, and environment variables are not read:

```yaml
resp-cache:
  addresses: ["my-valkey:6379"]
```

If more than one router deployment shares the backend, also give each its own `key-prefix`
(or `--resp-cache-key-prefix`) — see
[Sharing a backend between routers](#sharing-a-backend-between-routers).

Docker Compose (starts a valkey next to the router):

```bash
SR_CONFIG=config/smartrouter_examples/smartrouter_eth_resp_cache.yml \
  docker compose -f docker/docker-compose.yml \
                 -f docker/docker-compose.resp-cache.yml up --build
```

## Configuration reference

Everything lives under the `resp-cache:` block. Setting any of it **without `addresses`** is
rejected at startup (dangling configuration), as is every invalid combination below — the
router never starts half-configured. A key the block does not define is rejected too: a
misspelled `key-prefix` used to be ignored and put the router on the shared default without
a word.

| Key | Default | Meaning |
| --- | --- | --- |
| `topology` | `standalone` | `standalone` \| `sentinel` \| `cluster`. |
| `addresses` | — (required) | Standalone: the node address. Sentinel: the **sentinel** addresses. Cluster: the **configuration endpoint** used as the discovery seed — never a node list; the client discovers topology itself. |
| `read-addresses` | *(unset)* | Optional separate endpoint(s) for **reads** (reader endpoints). Writes stay on `addresses`. Selects an *endpoint*, not a replica role — see the caveat under [Multi-region reads](#multi-region-reads-readwrite-split). |
| `master-name` | — | Sentinel only (required there): the monitored master set name. Refused under any other topology — a `master-name` with the `topology: sentinel` line forgotten is dangling configuration, since the router would otherwise dial the first sentinel address as a plain data node. |
| `username` / `password` | *(unset)* | Static data-node credentials (AUTH / ACL). Without `tls.enabled: true` they are sent readable on every new connection; the router warns about that once at startup (`credentials are configured without tls`), which is fine for a loopback or private-network backend and the thing to fix for any other. |
| `password-file` | *(unset)* | Rotation-capable credentials: the file is polled and changes are pushed to **live connections**, which re-authenticate in place — no restart, no connection loss (standalone and cluster; under sentinel rotation applies on reconnect — see [Credential rotation](#credential-rotation)). Holds the password, or `username:password` to rotate the ACL user too — so a password containing `:` cannot be expressed here. Surrounding ASCII whitespace (spaces, tabs, CR, LF) and a leading UTF-8 byte order mark are trimmed, from the file and from each half of `username:password`; every other byte is sent as part of the credential, and a file that is empty once trimmed is refused at startup. Mutually exclusive with `password`. |
| `credential-refresh-interval` | `10s` | Poll cadence for `password-file`. |
| `sentinel-username` / `sentinel-password` / `sentinel-password-file` | *(unset)* | **Sentinel control-plane** credentials — sentinels authenticate independently of the data nodes; hardened deployments fail discovery without these. Only valid with `topology: sentinel`, and read once at startup (rotating them needs a restart). The file is read like `password-file`: ASCII whitespace and a leading byte order mark trimmed, every other byte kept, an empty file refused. |
| `db` | `0` | Logical database (standalone/sentinel only; rejected for cluster). |
| `key-prefix` | `sr` | The keyspace this router occupies; flag form `--resp-cache-key-prefix` (outranks the block). Restricted to `[A-Za-z0-9._-]+` (flush uses it as a `SCAN MATCH` glob). **Routers on one prefix serve each other's cached answers and resolve `latest` off one chain tip**, and the default puts every router that leaves it unset in one keyspace — give each deployment sharing a backend its own prefix unless its routers are replicas reading the same nodes; flush isolation follows from it. See [Sharing a backend between routers](#sharing-a-backend-between-routers). |
| `tls.enabled` | `false` | TLS to the backend. **Required (`true`) whenever any other `tls.*` key is set** — a `tls` block without it is refused at startup as dangling configuration, because the alternative is a plaintext connection carrying `username`/`password` readable on the wire. That holds for an explicit `enabled: false` beside those keys too. To run without TLS, remove the other `tls.*` keys or the whole block; `tls: {enabled: false}` on its own is accepted. |
| `tls.ca-file` | *(system pool)* | PEM CA bundle for server verification. |
| `tls.cert-file` / `tls.key-file` | *(unset)* | Client keypair for mTLS (both or neither). |
| `tls.server-name` | *(unset)* | Overrides the verification/SNI name. |
| `tls.insecure-skip-verify` | `false` | Skips server verification (testing only). |
| `dial-timeout` / `read-timeout` / `write-timeout` | `500ms` dial; client defaults for read/write | Per-operation network limits. A **fresh** connection's dial and TLS handshake are bounded by `dial-timeout` *and* by the caller's own deadline, whichever is sooner — the default is deliberately sub-second so a black-holed backend cannot make cold lookups linger. |
| `pool-size` | client default | Connection pool size (per client; the read client has its own). |
| `expiration.finalized` | `1h` | Lifetime of a settled (finalized) answer. The sidecar's `--expiration`. Every `expiration.*` duration needs a unit (`1h`, `3600s`, `250ms`): a bare number is read as nanoseconds, so `3600` is 3.6µs, and any lifetime that resolves below one millisecond — the precision a RESP expiry has — is refused at startup with a message saying what the number became. |
| `expiration.finalized-multiplier` | `1` | Multiplier on `expiration.finalized`. The sidecar's `--expiration-multiplier`, which the published chart sets to `1.5` (90 minutes) — this is where a router on a RESP backend keeps that. A multiplied lifetime that resolves below one millisecond (the precision a RESP expiry has), or past what a duration can hold, is refused at startup: below that the client would round every write up to 1ms, and to the store a zero lifetime is a key that never expires, not one that expires at once. |
| `expiration.non-finalized` | `500ms` | Floor for a recent (non-finalized) answer; the effective TTL is max(averageBlockTime/8, this). The sidecar's `--expiration-non-finalized`. |
| `expiration.non-finalized-multiplier` | `1` | Multiplier on `expiration.non-finalized`. The sidecar's `--expiration-non-finalized-multiplier`. |
| `expiration.node-errors` | `250ms` | Cap on a cached node error for a finalized block. The sidecar's `--expiration-finalized-node-errors`. |
| `expiration.blocks-hashes-to-heights` | `48h` | Lifetime of a block-hash→height mapping. The sidecar's `--expiration-blocks-hashes-to-heights`. |

TTLs default to the cache engine's own table (finalized 1h, non-finalized scaled to the chain's
block time with a 500ms floor, short-lived node errors) — the same defaults the sidecar applies
from its flags. The sidecar's flags are set by the chart; a router on a RESP backend takes the
same values from the `expiration` block above, and `GET /debug/cache-state` reports the
lifetimes actually in force under `lifetimes`. Without the block, a cache moved from the sidecar
to a RESP backend runs on the defaults, not on whatever the chart had set for the sidecar.

One budget lives on the **router**, not in this block: every cache **lookup** runs inside
the per-relay `--cache-timeout` flag (default `50ms`, sized for a same-zone backend; writes
are asynchronous under a separate 5s budget). A warm-connection read costs one network
round trip, so a backend further away than the budget times out on **every** lookup —
`smartrouter_resp_cache_failed_total{kind="timeout",op="get"}` climbs while writes keep
landing, and no relay is ever served as `Cached`. For a cross-region backend raise the flag
above the round-trip time (e.g. `--cache-timeout 400ms`), and prefer co-locating the router
with the backend: a hit always costs ~1 RTT, and a miss waits out the budget before falling
through to the upstream. (`--secondary-cache-timeout` is the same knob for the secondary
tier.)

Config values are **not** environment-expanded: a `${VAR}` written here is read literally as
the value.

## Topologies

**Standalone** — one address; also the shape for managed *primary/reader endpoints*
(cluster-mode-disabled):

```yaml
resp-cache:
  addresses: ["cache.internal:6379"]
```

**Sentinel** — automatic failover. The router connects to the sentinels, discovers the
primary, and follows promotions transparently — no restart, no manual intervention. Note the
two independent credential domains:

```yaml
resp-cache:
  topology: sentinel
  addresses: ["sentinel-1:26379", "sentinel-2:26379", "sentinel-3:26379"]
  master-name: "mymaster"
  password-file: /etc/smartrouter/resp-cache.pw          # data nodes
  sentinel-password-file: /etc/smartrouter/resp-sentinel.pw  # sentinels
```

Supplying only the data-node credential is the common misconfiguration — a hardened sentinel
set fails *discovery*, before any data node is reached.

**Cluster** — sharded. Point at the **configuration endpoint** (e.g. the ElastiCache cluster
configuration endpoint); node membership, slots, and replicas are discovered and tracked
automatically:

```yaml
resp-cache:
  topology: cluster
  addresses: ["my-cluster.cfg.euw1.cache.amazonaws.com:6379"]
```

## Multi-region reads (read/write split)

With replicating infrastructure (ElastiCache Global Datastore, MemoryDB Multi-Region), give
routers in secondary regions their local reader endpoint:

```yaml
resp-cache:
  addresses: ["primary.global.cache:6379"]        # writes
  read-addresses: ["reader.eu-west-1.cache:6379"] # reads
```

Reads go to `read-addresses` and writes (cache population) to `addresses`; a reset reaches both,
see **Resets reach both endpoints** below.
Replication lag is safe by construction: an entry that hasn't replicated yet is a plain cache
miss, and the router's block-freshness validation (seen-block rules) runs on every hit — a
lagging replica can never serve data older than what the client has already seen.

**This selects an endpoint, not a replica role.** Under `standalone` the addresses are dialled
exactly as given, so a managed reader endpoint really does serve the reads; that is the shape
this feature is for. Under `sentinel` and `cluster` the read client runs its **own discovery**
from the seeds you give it and resolves to the master(s) of whatever topology they front — so
pointing it at replicas of the *same* deployment routes your reads straight back to the
primary. It is still meaningful pointed at a **separate replicated deployment**, which is why
the router logs a warning rather than rejecting the config. Replica reads within one sentinel
set or cluster are not supported; use the managed reader endpoint in `standalone` shape.

**Resets reach both endpoints.** `/debug/reset-all` scans and unlinks under the key prefix on
the read endpoint as well as the write endpoint, because a separate read store is one the
write endpoint never feeds, and an entry left there kept being served after every reset. Before
scanning the read endpoint the router asks it `ROLE` (or `INFO replication` where `ROLE` is not
answered): one that reports itself a replica is not scanned at all, since every unlink there
would answer `READONLY` and a `SCAN` walks the node's whole keyspace whatever the `MATCH`, which
on a shared managed reader is a full walk per reset for no effect. The master it names, and
whether that is the configured write address, are logged at debug level. A read endpoint that
answers neither is scanned, and a `READONLY` at the unlink is left to replication the same way;
the reset still succeeds. A read endpoint that cannot be reached fails the reset, naming the
read side.

## Credential rotation

Use `password-file` with whatever refreshes the file (Kubernetes secret mounts, a sidecar
token refresher). On change, the router pushes the new credentials to every live connection,
which re-authenticates **in place** — no reconnect, no dropped operations. Custom credential
sources (e.g. an IAM SigV4 signer) can implement the `CredentialsSource` interface in
`ecosystem/cache/redisstore`; the router deliberately bundles no cloud SDKs.

Under `topology: sentinel` the go-redis failover client (v9.22) does not support in-place
streaming re-auth, so rotated credentials are resolved fresh **per connection attempt** — they
apply on reconnects and failovers rather than being pushed to idle connections. Keep the
previous credential valid for a rotation grace window (standard ACL dual-credential practice)
and rotation is seamless there too. This applies to the **data-node** password under sentinel,
not just `sentinel-password-file`. Because no in-place re-auth is possible there, the router
does not run the rotation poller under sentinel at all; it logs once at startup that rotation
applies on reconnect, rather than reporting rotations it cannot deliver.

> **A password containing `:` cannot be expressed in a password file.** The first colon is
> always the `username:password` separator, so a file holding `p@ss:word` authenticates as user
> `p@ss` with password `word`. That fails closed, but it surfaces as an opaque `WRONGPASS` —
> the router deliberately withholds the server's auth reply from logs — so the router logs a
> warning once at startup when the file contains a colon, naming only the parsed username.
> Either avoid `:` in the password or use the explicit `username:password` form deliberately.

> **What the file trim removes, and what it keeps.** Surrounding ASCII whitespace (spaces,
> tabs, CR, LF) and a leading UTF-8 byte order mark are trimmed, on the file and on each half of
> `username:password`; every other byte is the credential. A credential that begins with another
> invisible rune (a zero-width space, a non-breaking space) is sent as written, and the router
> warns once at startup naming the file and the code point, never the value. A file that is empty
> once trimmed is refused at startup; one that is empty for a moment mid-rotation (a secret mount
> being rewritten) is a failed read, and the live connections keep the credentials they have.

If the file becomes unreadable while the router runs — a mount that dropped, a rotation that
failed, a permission change, a file that is empty once trimmed — the router keeps the credentials
it last read, on every connection: live connections are untouched, and a connection opened during
the outage (pool growth, a reconnect after a network blip, an idle replacement) is handed the same
credentials rather than failing its setup on the unreadable file. The router says so **once** when
the outage starts, once more if its cause changes, and once when the file is readable again; a
rotation that lands with the recovery is pushed to every connection, including those opened
during the outage. It does not repeat the warning on every poll, so a long outage costs one
log line rather than one per interval.

## Sizing and eviction (`maxmemory-policy`)

Recommended: **`volatile-lru`** with a `maxmemory` fitting your working set.

- Every key the router writes carries a TTL, so `volatile-lru` can evict across the router's
  whole keyspace by recency — and it will never touch non-TTL keys owned by other applications
  on a shared backend.
- `allkeys-lru` behaves identically on a dedicated backend and is the safer choice if you ever
  write non-TTL keys under memory pressure; on a shared backend it can evict other tenants'
  data.
- Avoid `noeviction` for cache workloads: at `maxmemory` the router's cache writes start
  failing (visible in `smartrouter_resp_cache_failed_total`) until TTLs free space — the cache
  keeps serving hits, but stops growing.

Blockchain cache entries skew heavily toward the long finalized TTL, so steady state approaches
`maxmemory` and stays there — that is eviction working as intended, not a leak.

## Failure behavior and monitoring

A failing backend **never fails requests**: lookups degrade to cache misses within the relay's
budget and requests proceed to your upstreams; writes are best-effort. Recovery is automatic.
Alert on the dedicated series (full reference in
[METRICS.md](METRICS.md#resp-cache-backend--smartrouter_resp_cache_)):

- `smartrouter_resp_cache_connected` — 0 after a failed health probe (PING, 10s cadence);
  reachability transitions are also logged, and an authentication rejection is reported as
  such rather than as "unreachable" (the credential itself is never logged).
- `smartrouter_resp_cache_failed_total{op, kind}` — backend-level operation failures (never
  clean misses), with `kind` splitting `error` from `timeout` so saturation reads differently
  from outage.
- `smartrouter_resp_cache_connection_errors_total`, pool gauges.

The shared `smartrouter_cache_*` hit/miss series keep working unchanged.

A router started with `--debug-relays` adds `Lava-Cache-Backend` to cache-served responses,
naming the node that served the hit — the current master under sentinel, the touched shard
under cluster. It is debug-gated because it exposes internal infrastructure addresses.

### `GET /debug/cache-state`

A router started with `--debug-address` serves a read-only snapshot of which backend is
actually caching for it:

```bash
curl -s http://127.0.0.1:6161/debug/cache-state | jq
```

```json
{
  "schema_version": 1,
  "engine": "resp",
  "tiers": {
    "primary": {
      "configured": true,
      "engine": "resp",
      "address": "redis-primary:6379 read=reader.eu-west-1:6379 topology=standalone prefix=sr",
      "reachable": true,
      "reachable_checked_at": "2026-09-09T14:31:02Z",
      "reachable_detail": "no error reported",
      "when_unreachable": "attempted",
      "lifetimes": {"finalized_seconds": 3600, "non_finalized_seconds": 0.5, "node_errors_seconds": 60}
    },
    "secondary": {"configured": false, "engine": "", "address": "", "reachable": null,
                  "reachable_checked_at": "", "reachable_detail": "", "when_unreachable": "", "lifetimes": null}
  }
}
```

Five things are easy to misread:

- **`address` is the tier's configuration summary, not one address.** For a RESP tier it
  carries the write addresses, `read=` when reads are split, `topology=` as **resolved** (an
  omitted `topology:` line shows as `standalone`, which is how a sentinel block missing that
  line reads at runtime), `master=` under sentinel, and `prefix=` for the keyspace. The
  startup line says the same and then scrolls away; this field does not.
- **`reachable` has three values.** `true`, `false`, and `null` for *not yet determined* — a
  RESP backend before its first probe returns, a `cache-be` connection mid-dial. `null` is not
  "unreachable"; a router polled immediately after startup legitimately answers it. Whether a
  tier exists is `configured`, never the presence of this field.
- **`when_unreachable` is why `reachable: false` is not one fact.** For a RESP tier it is
  `attempted`: the backend is still asked on every relay and pays the full cache timeout each
  time. For a `cache-be` tier it is `skipped`: the client returns not-connected before any
  I/O, so an unreachable tier costs nothing. Same flag, opposite bill.
- **`reachable_checked_at` marks a snapshot.** The RESP verdict comes from the 10s health
  probe, so it can be up to ~13s old. The `cache-be` verdict is read live from the connection
  and carries no timestamp.
- **`lifetimes` is `null` for a `cache-be` tier.** Those TTLs are configured in, and applied
  by, the cache-server pod; this router does not know them. `null` says so — a number would
  assert a value no deployment uses. Where they are reported, they are the policy's base
  values: the effective non-finalized TTL is `max(averageBlockTime/8, non_finalized_seconds)`
  per chain, so that field is a floor.

Reading this endpoint never touches the backend. That is deliberate rather than incidental:
the obvious liveness accessor on the `cache-be` client dials on demand, so a monitoring scrape
would otherwise change the state it is measuring, and a backend that recovered between two
polls would look healthy *because of* the first poll.

> **Bind the debug listener to loopback.** This is the first `/debug` route to publish the
> router's own backing-store addresses, and the debug listener has **no authentication** — any
> route on it is readable by anything that can reach the port. No credential is exposed
> (`password` / `password-file` are separate configuration and never appear here), so this is
> an exposure decision rather than a leak: under `topology: sentinel` these are sentinel
> control-plane addresses, and they name infrastructure worth not advertising. Pass
> `--debug-address 127.0.0.1:6161` rather than `:6161`, and reach it through
> `kubectl port-forward` in a cluster.

## Sharing a backend between routers

A keyspace is one cache. Every router in it reads and writes the same entries, resolves
`latest` / `safe` / `finalized` / `pending` through the same chain tip, shares the same
block-hash→height mappings, and — under `--shared-state` — the same seen-block and
sticky-session claims. That is exactly right for **replicas of one deployment**: they read
the same nodes, so an answer one of them cached is the answer any of them would have fetched.

It is wrong for two routers that declare the same chain but read **different nodes** — a
paid tier beside a free one, a canary beside production, a router being migrated onto a new
node set while the old one still serves. In one keyspace whichever router asks first decides
the answer both give for as long as the entry lives, the response reports `Cached` in place
of a node name, and nothing on either side signals it (MAG-3521). The condition is therefore:
**routers sharing a keyspace must read the same nodes.** Anything else needs its own
keyspace:

| Backend | Setting | Default |
| --- | --- | --- |
| RESP (this page) | `resp-cache.key-prefix` / `--resp-cache-key-prefix` | `sr` — one shared keyspace for every router that leaves it unset |
| gRPC sidecar (`cache-be`) | `cache-be-key-prefix` / `--cache-be-key-prefix` | empty — the shared keyspace every router occupied before the setting existed |

Both take the same character set (`[A-Za-z0-9._-]+`), so one value works on either backend.
On the RESP backend the prefix heads every key (`<prefix>:rel:f:ETH1:…`); on the sidecar it
travels inside each request and the server folds it into every key it derives
(`rel:f:<prefix>:ETH1:…`), which is why **the sidecar has to be a build that knows the
field** — an older `smart-router cache` drops it on the wire and isolates nothing. The router
does not have to take that on trust: the server echoes the prefix it scoped by on every reply,
and a router that sent one and reads no echo warns once per connection
(`cache-be-key-prefix is set but the cache server did not echo it`) and keeps serving from the
shared keyspace. `GET /debug/cache-state` names the keyspace in the tier's `address`
(`prefix=…`) on both backends, so two routers can be checked for separation without sending
traffic; on the sidecar the prefix reads `prefix=… (unconfirmed)` until the first reply on a
connection and `prefix=… (ignored by the cache server)` once a reply came back without the
echo, and bare `prefix=…` only once the server has confirmed it.

A prefix is a **cooperative namespace, not a tenant boundary**. The sidecar has no
authentication, so any client that can reach it can name any keyspace; the server refuses a
prefix that is malformed (outside the character set, `InvalidArgument`), never one that
belongs to someone else. Isolation between deployments that must not read each other's
answers is a network question, not a naming one.

What a prefix does **not** do: replicas that share a keyspace on purpose still share one
chain tip, and that tip is a monotonic maximum with no downward path before expiry — one
replica publishing a false high block pins `latest` resolution for its whole fleet. That is a
trust problem rather than a naming one and is tracked separately (MAG-3755).

## Flush semantics

The router's `/debug/reset-all` flushes the RESP backend **prefix-scoped**: `SCAN` over
`key-prefix:*` with single-key `UNLINK`s. `FLUSHDB` is never issued, so a shared backend's
other tenants (and other prefixes) are untouched. If two deployments must be flush-isolated,
give them distinct prefixes.

The gRPC sidecar is the exception: its in-memory store cannot enumerate keys by prefix, so a
`/debug/reset-all` on any router empties **every** keyspace on that sidecar — the prefix
scopes what a router reads and writes, and is not a flush boundary there. Routers that must
be flush-isolated from each other need separate sidecars, or the RESP backend.

## Precedence and rollback

Switching backends is a configuration change; the RESP cache starts cold (no data migrates).

- `resp-cache:` configured → the RESP backend serves, **including when `cache-be:` is also
  set** (the router logs a prominent warning naming the precedence). Keeping `cache-be:` in
  place is deliberate — it is the rollback path.
- Rollback = delete the `resp-cache:` block (or flag). The preserved `cache-be:` takes over on
  the next start. Nothing is migrated and nothing is destroyed; the RESP data ages out on its
  own TTLs.
- Neither configured → the default cache, exactly as before. Existing deployments need zero
  changes: the RESP backend is never constructed, so its metrics are absent rather than zero.

## Caveats

- **Fleet tracker gate is not carried over.** The per-endpoint chain-tracker gate (MAG-2981)
  lets pods borrow each other's successful upstream polls. It is a `cache-be` *RPC* backed by a
  dedicated in-memory store on the cache server, not a cache-engine behaviour, so it does not
  travel through the key/value seam this backend implements. A router on the RESP backend logs
  a warning once per listen endpoint and **polls locally** — the same degradation already
  applied to a `cache-be` that predates the RPC. Everything else the sidecar caches (relay
  entries, chain tip, shared-state seen-block, block-hash→height) works identically. If you
  need the peer gate, stay on `cache-be`.
- **Sentinel credential rotation** applies per connection attempt, not in place — see
  [Credential rotation](#credential-rotation).
- **`read-addresses` selects an endpoint, not a replica role** — see
  [Multi-region reads](#multi-region-reads-readwrite-split).
- **Cold start.** No data migrates in either direction when switching backends.

## Local testing lanes

Scenario lanes — each brings up its own infrastructure, checks itself, and prints how to drive
it. All are ownership-safe: they reclaim only what they started and refuse to run if their
ports are held by anything else.

| Lane | Covers |
| --- | --- |
| `scripts/pre_setups/init_smartrouter_eth_redis_demo.sh` | Redis + router; drop-in caching, and degraded mode when the backend is paused |
| `scripts/pre_setups/init_smartrouter_eth_redis_sentinel.sh` | Primary + replica + 3 sentinels, both credential planes. `--failover` kills the primary and follows the promotion, `--recover` rejoins it, `--status` reports the topology |
| `scripts/pre_setups/init_smartrouter_eth_redis_multiregion.sh` | Two regions and two routers; `--demo` shows a region-local read served from a replica while writes go to the primary region |
| `scripts/pre_setups/init_smartrouter_eth_lavap_cache.sh` | No RESP infrastructure — router + cache sidecar only |
| `scripts/pre_setups/init_smartrouter_eth_resp_cache.sh` | Single-node lane against the checked-in example config (`RUN_DEMO=1` for an end-to-end check) |

Test lanes — the docker-gated ones fail hard if docker is unreachable rather than skipping, so
a documented command never prints PASS for a lane that verified nothing:

- **Cross-backend parity** (every behavioural case against both the gRPC cache server and the
  RESP backend, no docker needed): `go test ./protocol/performance -run TestParity -v`
- **Sentinel failover**: `RESP_CACHE_TEST_SENTINEL_DOCKER=1 go test ./ecosystem/cache/redisstore -run TestSentinelFailover -v -timeout 5m`
- **Cluster** (three masters joined via one configuration endpoint; cross-slot pipelined
  lookups and prefix-scoped purge across masters):
  `RESP_CACHE_TEST_CLUSTER_DOCKER=1 go test ./ecosystem/cache/redisstore -run TestClusterDocker -v -timeout 5m`
- **Real-server TLS/mTLS** (Valkey with `--tls-auth-clients yes`; certificate-less clients
  rejected): `RESP_CACHE_TEST_TLS_DOCKER=1 go test ./ecosystem/cache/redisstore -run TestTLSDockerValkey -v -timeout 3m`
- **Live credential rotation** — needs a server you provide; running only the `go test` line
  fails with a dial error:

  ```bash
  docker run --rm -d --name rot-valkey -p 127.0.0.1:63795:6379 valkey/valkey:7.2
  RESP_CACHE_TEST_VALKEY_ADDR=127.0.0.1:63795 \
    go test ./ecosystem/cache/redisstore -run TestLiveRotationAgainstRealValkey -v
  docker rm -f rot-valkey
  ```

## Walkthrough

Five scenarios, each driven by a lane that brings up its own infrastructure and verifies
itself before handing over. The standalone, sentinel and multi-region lanes use distinct
ports and can run at the same time; the `lavap-cache` lane reuses the standard ports, so stop
the standalone one first.

### Setup

```bash
scripts/pre_setups/init_smartrouter_eth_redis_demo.sh
```

| | Where | Role |
| --- | --- | --- |
| Redis | docker, `127.0.0.1:63790` | the cache backend (`volatile-lru`, 256mb) |
| cache sidecar | `127.0.0.1:20100` | configured but unused — the rollback path |
| Smart Router | `0.0.0.0:3360`, metrics `:7779` | caches into Redis |

It ends with a self-check, so a broken stack is caught before you start:

```
[Smoke] relay -> cache write -> cache hit
  router /lava/health            200
  relay eth_blockNumber          ok
  cache hits                     7
  resp_cache_connected           1
  keys in redis under 'sr:'      3

  SMOKE PASS — the stack is demo-ready.
```

Knobs, all optional: `ETH_RPC_URL_1/2/3` and `ETH_WS_URL_1/2/3` for your own upstreams
(defaults are public, key-less endpoints), `REDIS_PASSWORD` to enable AUTH, `REDIS_IMAGE` to
swap Redis for Valkey, `RESP_CACHE=off` to start without the RESP block, `SKIP_SMOKE=1`, and
`ROUTER_PORT` / `METRICS_PORT` / `CACHE_PORT` / `REDIS_PORT` to move ports.

The lanes never run `killall`: each reclaims only the processes it recorded starting (pid plus
a start-time fingerprint), removes only its own labelled containers, and **refuses to start**
if its ports are held by anything else — printing a ready-made override. Generated configs and
credential files land in `debugging/`, which is gitignored, because upstream URLs may embed API
keys. Tear down with `--stop`.

### 1. Drop-in external cache

Ask for a **finalized** historical block. That matters: a finalized entry gets the 1h TTL and
stays inspectable, while a `latest` query is cached with a sub-second TTL and expires before
you can look at it — which reads as "nothing was cached".

```bash
curl -s -X POST http://127.0.0.1:3360 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1",false],"id":1}'

docker exec smartrouter-demo-redis redis-cli --scan --pattern 'sr:*'
```

```
sr:chaintip:ETH1
sr:rel:f:ETH1:653e91892d1884713eacc837c0e571c471d73b777760cce79eef589e8e6c6d88:1
```

`rel:f:` is the finalized variant of the relay entry; `chaintip:` is the chain-level tip the
sidecar also keeps. The TTL policy travelled with them:

```bash
docker exec smartrouter-demo-redis redis-cli ttl "$(docker exec smartrouter-demo-redis \
  redis-cli --scan --pattern 'sr:rel:f:*' | head -1)"     # 3599
docker exec smartrouter-demo-redis redis-cli ttl sr:chaintip:ETH1        # 86399
```

Issue the same request again and it is served from the backend:

```
Lava-Provider-Address: Cached
Lava-Cache-Backend: 127.0.0.1:63790
```

`Lava-Cache-Backend` appears only under `--debug-relays` (which the lanes pass). To read a
stored value, note that `response.data` is base64 and, above the compression threshold,
gzipped:

```bash
# Derive the key rather than retyping it — the hash is per-request.
K=$(docker exec smartrouter-demo-redis redis-cli --scan --pattern 'sr:rel:f:*' | head -1)

docker exec smartrouter-demo-redis redis-cli get "$K" | jq 'del(.response.data)'
docker exec smartrouter-demo-redis redis-cli get "$K" | jq -r '.response.data' | base64 -d | gunzip | jq .
```

If `jq '.is_compressed'` reports `false` (entries below the compression
threshold), drop the `gunzip`. `gunzip: unexpected end of file` means the `get`
returned nothing — usually a key that no longer exists, or a container that is
not running.

Re-run the lane with `REDIS_PASSWORD=…` to bring Redis up with `--requirepass` and point the
router at a password *file* — the rotation-capable credential form rather than an inline
literal.

### 2. Backend failure

Freeze the backend and reissue the request that was being served from cache:

```bash
docker pause smartrouter-demo-redis

curl -s -X POST http://127.0.0.1:3360 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1",false],"id":1}'
```

The relay still answers — it goes to the providers instead. After ~15s (10s probe cadence, 3s
PING timeout):

```bash
curl -s http://127.0.0.1:7779/metrics | grep smartrouter_resp_cache
```

```
smartrouter_resp_cache_connected 0
smartrouter_resp_cache_connection_errors_total 2
smartrouter_resp_cache_failed_total{kind="timeout",op="get"} 2
smartrouter_resp_cache_failed_total{kind="timeout",op="set"} 1
```

No request failed and the router never restarted. `kind` separates a frozen backend
(`timeout`) from one that is gone (`error`), so saturation alerts differently from an outage.

```bash
docker unpause smartrouter-demo-redis
```

Recovery is automatic within ~10s and logged (`resp-cache backend reachable again`); the same
request reads `Cached` again, because the entry was never lost — only unreachable.

Use `pause`, not `stop`: the container runs with `--rm`, so stopping it deletes it and the
lane has to be re-run.

### 3. Sentinel failover

```bash
scripts/pre_setups/init_smartrouter_eth_redis_sentinel.sh
```

A primary, a replica and three sentinels, with the data plane and the sentinel control plane
authenticated independently — and a router config that holds **no data-node address at all**:

```yaml
resp-cache:
  topology: sentinel
  addresses: ["127.0.0.1:26390", "127.0.0.1:26391", "127.0.0.1:26392"]
  master-name: "mymaster"
  password-file: …          # data nodes
  sentinel-password-file: … # the sentinels themselves
```

This lane listens on **`:3370`**, not `:3360`, so it can run alongside the standalone one.
Sending these commands to `:3360` reaches the standalone router, which answers happily while
naming its own single Redis.

```bash
scripts/pre_setups/init_smartrouter_eth_redis_sentinel.sh --status
# sentinel reports master: <lan-ip>:63811
#   sr-demo-primary    master
#   sr-demo-replica    slave

scripts/pre_setups/init_smartrouter_eth_redis_sentinel.sh --failover
```

```
master before:  <lan-ip>:63811
[Failover] stopping the primary (sr-demo-primary)
[Failover] waiting for the sentinels to promote the replica
  promoted: <lan-ip>:63811  ->  <lan-ip>:63812
[Failover] the router was never restarted; relaying again
  relay after:    {"id":1,"jsonrpc":"2.0","result":{"difficulty":"0x3ff800000"…
  smartrouter_resp_cache_connected 1
```

The router re-asked the sentinels and followed the new master; the connectivity gauge never
left 1. With `--debug-relays`, `Lava-Cache-Backend` flips from `…:63811` to `…:63812` on the
next cache hit, so the promotion is observable from outside the system. Call the endpoint
twice — the first relay warms the cache, the second carries the header.

`--recover` restarts the stopped node and confirms the sentinels demote it to a replica of the
new master rather than letting it fight for the role.

`<lan-ip>` is your host's own LAN address, detected at startup. Sentinel does not proxy — it
hands the client an address to dial — so every announced address must resolve both inside
docker (sentinels monitoring each other and the data nodes) and on the host (the router).
`127.0.0.1` means "itself" inside a container, container names do not resolve on the host, and
`host.docker.internal` does not resolve on macOS hosts; the LAN address is the one form that
works on both sides. Only the **port** identifies the node here: `63811` is the original
primary, `63812` the replica. `--status` warns if your host's address has changed since the
lane started. In Kubernetes or a VPC every node already has one routable address and none of
this applies.

### 4. Multi-region read/write split

```bash
scripts/pre_setups/init_smartrouter_eth_redis_multiregion.sh
```

| | | |
| --- | --- | --- |
| region A redis | `127.0.0.1:63821` | the write endpoint — where the data lives |
| region B redis | `127.0.0.1:63822` | a replica of A — region B's local reader |
| router-A `:3380` | reads and writes region A | the region with the blockchain nodes |
| router-B `:3381` | writes → A, reads → B | a secondary region |

The two generated configs differ by exactly one line:

```yaml
resp-cache:
  addresses:      ["127.0.0.1:63821"]   # writes — both routers
  read-addresses: ["127.0.0.1:63822"]   # router-B only: reads served locally
```

```bash
scripts/pre_setups/init_smartrouter_eth_redis_multiregion.sh --demo
```

```
[1] relay block 0x5 through router-A (region A, where the nodes are)
      sr:rel:f:ETH1:a706caf8…:5
[2] the infrastructure replicates it to region B
      sr:rel:f:ETH1:a706caf8…:5
[3] the SAME request through router-B — served locally, no node call
    Lava-Provider-Address: Cached
    Lava-Cache-Backend: 127.0.0.1:63822
[4] a DIFFERENT block (0x6) through router-B — writes go to region A
    new entry in region A (the WRITE endpoint):
      sr:rel:f:ETH1:7868f0c4…:6
    region B never received a write — it gets the entry by replication
```

Step 3 is the point of the feature: region B answered from its own replica without calling a
blockchain node, and the header names which node served it. Step 4 shows the other half —
writes are directed to the write endpoint even when the router reads elsewhere.

Two things to keep in mind: Redis's own replication stands in for the replicating
infrastructure a real deployment would use, and `read-addresses` selects an endpoint rather
than a replica role, so this lane uses `standalone` — the shape the feature is for. Under
`sentinel`/`cluster` the read client re-discovers and lands back on the master.

### 5. No RESP infrastructure

```bash
scripts/pre_setups/init_smartrouter_eth_redis_demo.sh --stop
scripts/pre_setups/init_smartrouter_eth_lavap_cache.sh
```

Router plus cache sidecar, no Redis and no `resp-cache:` block — today's deployment shape:

```
  cache backend          lavap cache (cache-be)
  relay                  ok
  cache hits             1
  resp-cache metrics     none (backend never built)

  UC-5 PASS — current behavior preserved; existing deployments need no changes.
```

Relay by hand — call it twice, the second is served from the sidecar:

```bash
curl -s -X POST http://127.0.0.1:3360 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1",false],"id":1}'
```

The `smartrouter_resp_cache_*` series are *absent* rather than zero: they register only when a
RESP backend is constructed, so a deployment with no `resp-cache:` block is not running this
feature in a disabled state — it is on exactly the path it ran before.

### 6. Persistence across restarts

The cache lives outside the router, so restarting the router does not cost the cached data.

```bash
scripts/pre_setups/init_smartrouter_eth_redis_demo.sh --persistence
```

```
[1] cache block 0x8 through this router
    entry in redis                     sr:rel:f:ETH1:33a8a0bb…:8
    hits before restart                9

[2] restart the router (redis keeps running, untouched)
    hits after restart (fresh process) 0

[3] the FIRST request after the restart
    served by                          Cached — no upstream call
    hits now                           1

  PASS — a restarted router serves a warm cache; nothing was re-fetched.
```

The counter reading `0` immediately after the restart is what makes the next line meaningful:
a fresh process, and its very first request is already a hit. The in-memory sidecar comes back
empty and re-fetches every entry from the upstream nodes.

Redis-side durability across a *backend* restart (RDB/AOF, persistent volumes) is an
infrastructure choice and outside what the router controls.

### 7. Shared state across replicas

Two routers, one backend: what one caches, the other serves.

```bash
scripts/pre_setups/init_smartrouter_eth_redis_demo.sh --shared-state
```

```
[1] starting a second router on :3365 against the SAME redis
    peer router                        UP on :3365
    peer cache hits (fresh)            0

[2] cache block 0x9 through router 1 (:3360) only
    entries in redis                   2

[3] the FIRST request for 0x9 on router 2 — which never saw it
    served by                          Cached — router 2 made no upstream call

  PASS — replicas share one cache; scaling out does not cost hit rate.
```

Router 2 never relayed that block, and answered without an upstream call. With the
per-process sidecar each replica keeps its own copy, so a request landing on a cold replica is
a miss — which is what makes horizontal scaling cost hit rate today.

Router 2 stays up on `:3365`; `--stop` removes both.

### Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| Lane refuses: "port(s) are held by process(es) it does not own" | Another stack holds them. The lane prints a ready-made override; nothing was killed. |
| `/metrics/overall-health` returns 503 right after boot | Fail-closed until the first relays health sweep. The lanes shorten the cadence to 15s; wait one cycle. |
| `--scan` shows only `sr:chaintip:ETH1` | A `latest` query was relayed — sub-second TTL. Use a finalized block. |
| `Lava-Cache-Backend` missing | Either the response was not a cache hit (check `Lava-Provider-Address`), or the router lacks `--debug-relays`. Call the endpoint twice. |
| Header names an unexpected backend | You are talking to a different lane's router — check the port (standalone `:3360`, sentinel `:3370`, multi-region `:3380`/`:3381`). |
| `resp_cache_connected` is 0 | Backend unreachable — the container may have been `docker stop`ped, which deletes it (`--rm`). Re-run the lane. |
| Relays fail or the smoke check fails | Public endpoint rate limits. Set `ETH_RPC_URL_1/2` and `ETH_WS_URL_1/2` to your own endpoints. |
| Startup fails: `tls.* options are set but tls.enabled is not true` | The `tls` block carries `ca-file`, `cert-file`, `key-file`, `server-name` or `insecure-skip-verify` while `enabled` is missing or written as anything but `true` (`false`, `0`, `null`, `""`). The router will not open a plaintext connection on a block that reads as encrypted, and a switch turned off by hand with the paths left in place is refused the same way. Set `enabled: true` (the files are then read and verified at startup), or drop the other `tls.*` keys: a bare `tls: {enabled: false}`, or no block at all, runs without TLS. |

Readiness timing note: `/metrics/overall-health` (and the container health that follows it)
starts **fail-closed** and turns 200 once at least one chain has verified a provider — at
endpoint-setup completion on a healthy boot. A 503 that persists means no upstream answers the
health probe; while unhealthy the router re-probes every `--relays-health-unhealthy-interval`
(default 15s), so recovery shows within seconds. The lanes also shorten `--relays-health-interval`
so the periodic backstop sweep runs often enough to watch.
