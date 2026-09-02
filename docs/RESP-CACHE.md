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

Docker Compose (starts a valkey next to the router):

```bash
SR_CONFIG=config/smartrouter_examples/smartrouter_eth_resp_cache.yml \
  docker compose -f docker/docker-compose.yml \
                 -f docker/docker-compose.resp-cache.yml up --build
```

## Configuration reference

Everything lives under the `resp-cache:` block. Setting any of it **without `addresses`** is
rejected at startup (dangling configuration), as is every invalid combination below — the
router never starts half-configured.

| Key | Default | Meaning |
| --- | --- | --- |
| `topology` | `standalone` | `standalone` \| `sentinel` \| `cluster`. |
| `addresses` | — (required) | Standalone: the node address. Sentinel: the **sentinel** addresses. Cluster: the **configuration endpoint** used as the discovery seed — never a node list; the client discovers topology itself. |
| `read-addresses` | *(unset)* | Optional separate endpoint(s) for **reads** (reader endpoints). Writes stay on `addresses`. Selects an *endpoint*, not a replica role — see the caveat under [Multi-region reads](#multi-region-reads-readwrite-split). |
| `master-name` | — | Sentinel only (required there): the monitored master set name. |
| `username` / `password` | *(unset)* | Static data-node credentials (AUTH / ACL). |
| `password-file` | *(unset)* | Rotation-capable credentials: the file is polled and changes are pushed to **live connections**, which re-authenticate in place — no restart, no connection loss (standalone and cluster; under sentinel rotation applies on reconnect — see [Credential rotation](#credential-rotation)). Holds the password, or `username:password` to rotate the ACL user too — so a password containing `:` cannot be expressed here. Mutually exclusive with `password`. |
| `credential-refresh-interval` | `10s` | Poll cadence for `password-file`. |
| `sentinel-username` / `sentinel-password` / `sentinel-password-file` | *(unset)* | **Sentinel control-plane** credentials — sentinels authenticate independently of the data nodes; hardened deployments fail discovery without these. Only valid with `topology: sentinel`, and read once at startup (rotating them needs a restart). |
| `db` | `0` | Logical database (standalone/sentinel only; rejected for cluster). |
| `key-prefix` | `sr` | Namespace for every key. Restricted to `[A-Za-z0-9._-]+` (flush uses it as a `SCAN MATCH` glob). Give each deployment sharing a backend its own prefix — flush isolation follows from it. |
| `tls.enabled` | `false` | TLS to the backend. |
| `tls.ca-file` | *(system pool)* | PEM CA bundle for server verification. |
| `tls.cert-file` / `tls.key-file` | *(unset)* | Client keypair for mTLS (both or neither). |
| `tls.server-name` | *(unset)* | Overrides the verification/SNI name. |
| `tls.insecure-skip-verify` | `false` | Skips server verification (testing only). |
| `dial-timeout` / `read-timeout` / `write-timeout` | client defaults | Per-operation network limits. The handshake of a **fresh** connection is bounded by `dial-timeout`, not by the relay's per-lookup budget — keep it tight if a hung backend must not slow cold lookups. |
| `pool-size` | client default | Connection pool size (per client; the read client has its own). |

TTLs are the cache engine's own (finalized ~1h, non-finalized scaled to the chain's block
time, short-lived node errors) — the same table the default cache uses.

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

Reads go to `read-addresses`, writes (including cache population and flush) to `addresses`.
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

## Flush semantics

The router's `/debug/reset-all` flushes the RESP backend **prefix-scoped**: `SCAN` over
`key-prefix:*` with single-key `UNLINK`s. `FLUSHDB` is never issued, so a shared backend's
other tenants (and other prefixes) are untouched. If two deployments must be flush-isolated,
give them distinct prefixes.

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

Readiness timing note: `/metrics/overall-health` (and the container health that follows it)
starts **fail-closed** and reports 503 until the first relays health-check cycle completes —
`--relays-health-interval` defaults to 5 minutes. A freshly started stack showing 503 while
serving relays is warming up, not broken; the lanes shorten the interval for this reason.
