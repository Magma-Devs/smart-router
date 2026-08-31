# Secondary Cache

The Smart Router can read from an optional **secondary cache** — a second cache
backend consulted when the primary cache has no answer, *before* falling through
to your upstream nodes. The router only ever **reads** it: a hit is served to
the caller and copied into the router's own primary cache; nothing is ever
written to the secondary.

The typical deployment is zone-segregated: an external-zone router reads the
internal (trusted) zone's cache on a miss, reusing data that zone already
fetched from its nodes — fewer redundant upstream calls, no duplicated node
infrastructure, and a strictly one-way, read-only relationship between zones.

## How it works

```
request ──► primary cache ──hit──► served ("Cached")
                │ miss / down
                ▼
            secondary cache ──hit──► served ("Cached") + copied into primary
                │ miss / down / timeout
                ▼
            upstream nodes (normal routing)
```

- **Read-only.** The router has no write or flush path to the secondary — not
  for responses, not for admin operations. Enforcing that the secondary
  endpoint itself is reachable read-only across zones (network policy,
  directionality) remains your responsibility as the operator.
- **Best effort.** A secondary that is slow, down, or unreachable never affects
  request serving: the lookup is bounded by a configurable timeout, any
  error counts as a miss, and the request proceeds to your upstreams. The
  router reconnects in the background, exactly like the primary cache.
- **Backfill.** After a secondary hit, the entry is written into the router's
  *own* primary cache under the same eligibility rules applied to upstream
  responses (cached node errors and error statuses are served but never
  re-written as successes). The next identical request hits the primary
  directly — the secondary is consulted once per entry, not per request.
- **Node errors are labelled the same from either tier.** A cached node error
  is served with the `lava-identified-node-error` response header, exactly as a
  live node error is — so a replayed error is never mistaken for a success, and
  which cache answered makes no difference to what the caller sees.
- **Independent of the primary.** The secondary keeps serving while the primary
  is down, and is even valid with no primary configured at all (reads work;
  nothing backfills — the router logs an advisory warning for this topology).
- **No identity leakage.** Cache entries *do* carry data that identifies where
  they came from — the upstream's response headers plus the writer's
  signatures. Every entry crossing in from the secondary is copied and stripped
  of all three (`Metadata`, `Sig`, `SigBlocks`) before it is served or
  backfilled, so the caller sees only the response body and the router's own
  `Lava-Provider-Address: Cached` header — byte-identical to a primary-cache
  hit. The stripped copy is the only copy used, so nothing unsanitized can
  reach your primary cache either.

Any backend that speaks the Smart Router cache protocol works as either tier —
the secondary is simply a second `smartrouter cache` address. The two tiers are
independent connections, so a mixed pairing (a different supported cache engine
on each tier) is a supported configuration, not a special case.

## What happens in each situation

Five situations cover everything the tier can do. Each one is observable from
outside the router — the last column is what you look at to confirm it.

| Situation | What the router does | How you see it |
| --- | --- | --- |
| **Primary misses, secondary has it** | Serves the secondary's answer to the caller, then copies it into the primary. No upstream call. | Response header `Lava-Provider-Address: Cached`; `smartrouter_cache_success_total{cache_tier="secondary"}` increments |
| **Both tiers miss** | Falls through to your upstream nodes exactly as it does today. The only cost is the one extra lookup, bounded by the timeout. | `smartrouter_cache_failed_total{cache_tier="secondary",outcome="miss"}` increments; the response carries a real upstream address |
| **Secondary is configured read-only** | Reads it, never writes it — for responses or for admin operations. | No write traffic from this router in the secondary's log. The router has no code path that can write it (see below) |
| **Secondary is down, slow, or unreachable** | Skips the tier and goes to upstreams. Reconnects in the background. Request serving is unaffected. | `outcome="error"` or `outcome="timeout"` on `smartrouter_cache_failed_total`; latency stays inside `secondary-cache-timeout` |
| **No secondary configured** (the default) | Identical to previous releases. Nothing is added to the request path. | No `cache_tier="secondary"` series exist at all |

Two of these are worth calling out because they are guaranteed rather than
merely implemented:

- **Read-only is enforced by the compiler.** The secondary is held behind a
  read-only interface that exposes only "is it up?" and "get an entry." The
  write and flush operations are not merely unused — they are not reachable,
  so no future change can start writing to the secondary without that being a
  deliberate, visible redesign.
- **Backfill decisions are not re-implemented.** A secondary hit is handed to
  the same component that decides whether an upstream response is cacheable,
  carrying the entry's true state. A cached node error or an error status is
  served to the caller but rejected for backfill by that component's own
  rules — so the two tiers can never drift apart in what they consider
  cacheable.

## Configuration

One flag (or YAML key) enables it; the rest have defaults:

```bash
smartrouter config.yml --cache-be "cache-internal:20100" \
  --secondary-cache-be "cache-shared.other-zone:20100"
```

or in the config file:

```yaml
cache-be: "cache-internal:20100"
secondary-cache-be: "cache-shared.other-zone:20100"
secondary-cache-timeout: 100ms      # optional
secondary-cache-mode: read-only     # optional (the default and only mode)
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `secondary-cache-be` | *(unset — disabled)* | Secondary cache address; same formats as `cache-be` (`host:port` or `unix:` socket). |
| `secondary-cache-timeout` | `50ms` | Per-lookup budget. An exceeded lookup counts as a miss and the request falls through. Raise it for cross-zone network hops. |
| `secondary-cache-mode` | `read-only` | Access mode. `read-only` is the only supported value. |

Configuration comes from flags or the YAML config file (an explicitly passed
flag overrides the YAML value). Environment variables are not supported.

The router fails fast on misconfiguration: a timeout or mode set *without* an
address, a zero/negative timeout, or `read-write` mode all abort startup with a
clear error. It warns (but starts) when the secondary equals the primary
address, or when a secondary is configured with no primary. When enabled, the
startup log prints the full secondary configuration on one line.

Removing the configuration fully reverts the router to single-cache behavior;
with no secondary configured, behavior is unchanged from previous releases.

## Observability

Cache metrics are split per tier via the `cache_tier` label
(`primary` | `secondary`):

```bash
curl -s http://<router>:7779/metrics | grep smartrouter_cache_success_total
# smartrouter_cache_success_total{...,cache_tier="primary"}    — served from the router's own cache
# smartrouter_cache_success_total{...,cache_tier="secondary"}  — rescued from the secondary
```

Non-hits are classified in `smartrouter_cache_failed_total` by `outcome`:
`miss` (not found), `error` (transport/server error), or `timeout` (budget
exceeded) — so a broken or slow secondary is immediately distinguishable from a
cold one. Lookup latency is recorded per tier for every attempt. With tracing
enabled, each lookup is a span carrying `cache.tier` and `cache.outcome`, and
the request's root span records which tier served it. See
[METRICS.md](METRICS.md#cache) for the full reference and the dashboard
migration note.

## Seeing it work

### The two-zone walkthrough

`scripts/pre_setups/init_smartrouter_eth_secondary_cache.sh` reproduces the
zone-segregated topology on a single machine — two caches and two routers:

```
internal zone:  router :3361  ──read/write──►  cache :20101
external zone:  router :3360  ──read/write──►  cache :20100
                              ──READ-ONLY───►  cache :20101   ← the secondary
```

```bash
export ETH_RPC_URL_1="https://<your-eth-endpoint>"
RUN_DEMO=1 ./scripts/pre_setups/init_smartrouter_eth_secondary_cache.sh
```

`RUN_DEMO=1` runs the flagship flow end to end and prints its pass criteria: one
request through the *internal* router warms the internal cache from that zone's
trusted nodes; the same request through the *external* router misses its own
empty primary, is rescued by the secondary, and is served as `Cached`; an
immediate repeat is served by the external router's *own* primary, because the
first one backfilled it.

To drive that flow — and each of the other four PRD use cases — by hand, drop
`RUN_DEMO` and follow [Manual demo walkthrough](#manual-demo-walkthrough) below.

### Reading the logs

To see the tier's decisions **after** a run — the startup config line plus one
line per lookup:

```bash
export LOGS_DIR="$(git rev-parse --show-toplevel)/debugging/logs"
grep -i 'secondary cache' $LOGS_DIR/SMARTROUTER_EXTERNAL.log
# INF secondary cache configured  address=127.0.0.1:20101 mode=read-only timeout=100ms
# DBG secondary cache lookup produced no hit  requestedBlockForCache=…
# DBG secondary cache hit  chainId=ETH1 isNodeError=false requestedBlock=18000000
```

To watch **live**, run this in one terminal and send requests from another.
`tail -f` prints only lines written from the moment it starts, so an idle
router produces no output — that is not a failure:

```bash
tail -f $LOGS_DIR/SMARTROUTER_EXTERNAL.log | grep --line-buffered -i 'secondary cache'
```

The per-lookup lines are `DBG`, so the router needs `--log-level debug` (the
two-zone script sets it). The startup lines are `INF` and always appear when a
secondary is configured.

### Docker Compose

For a container-based setup, layer
`docker/docker-compose.secondary-cache.yml` on top of the base and cache
overlays — see
[LOCAL-COMPOSE.md](LOCAL-COMPOSE.md#enabling-a-read-only-secondary-cache). Note
that both compose caches start empty, so demonstrating an actual secondary
*hit* needs the two-zone script above, which supplies a second router to warm
the shared cache.

## Manual demo walkthrough

One demo per use case in *PRD: Secondary Cache Backend*. Each is
copy-pasteable, runs in about a minute, and ends in a single line of output you
read as pass or fail.

| PRD use case | Demo | Passes when |
| --- | --- | --- |
| **UC-1** Primary miss with secondary fallback | [UC-1](#uc-1--the-secondary-rescue) | The external router answers `Cached` without touching a node, then backfills so the next repeat is local |
| **UC-2** Secondary miss (full fall-through) | [UC-2](#uc-2--full-fall-through) | A real upstream answers; the miss is recorded as a clean `miss`, not an error |
| **UC-3** Secondary configured read-only | [UC-3](#uc-3--the-router-never-writes-the-secondary) | Writes on the secondary stay flat while the control primary grows |
| **UC-4** Secondary unreachable | [UC-4](#uc-4--a-dead-secondary-never-affects-serving) | Requests keep succeeding, and the tier reconnects untouched |
| **UC-5** No secondary configured | [UC-5](#uc-5--unconfigured-is-fully-backwards-compatible) | Zero secondary metric series and zero secondary log lines |

Run them in order the first time: UC-4 and UC-5 restart services, so they are
easiest to do last.

### Before you start

Start the two-zone lab. Leave `RUN_DEMO` off — UC-1 below *is* that flow, run
by hand:

```bash
cd "$(git rev-parse --show-toplevel)"
export ETH_RPC_URL_1="https://<your-eth-endpoint>"
./scripts/pre_setups/init_smartrouter_eth_secondary_cache.sh
```

Then, **in every shell you use**, export the log directory the checks read. An
unset `LOGS_DIR` silently collapses the paths to `/FILE.log` ("No such file or
directory"):

```bash
export LOGS_DIR="$(git rev-parse --show-toplevel)/debugging/logs"
ls $LOGS_DIR      # CACHE_EXTERNAL.log  CACHE_INTERNAL.log  SMARTROUTER_*.log
```

Give the routers ~20 s after the script returns to establish a chain tip before
sending the first request.

| Component | Address | Role in the demo |
| --- | --- | --- |
| router-external | `:3360`, metrics `:7779` | the router under test — primary `:20100`, secondary `:20101` |
| router-internal | `:3361`, metrics `:7780` | the trusted zone, used only to warm the shared cache |
| cache-external | `:20100` | the external zone's own primary |
| cache-internal | `:20101` | the internal zone's primary **and** the external router's read-only secondary |

> **Stopping a service: target the listener.** `lsof -ti:20101` matches *every*
> process holding a socket on that port — including both routers, which hold
> **client** connections to the secondary. Piping that into `kill` takes the
> routers down too and invalidates the test. Always add `-sTCP:LISTEN` so only
> the server is hit.

Each demo uses its own block number so they never warm each other's entries.
They are also **one-shot**: once a block has been served it is cached, so
re-running a demo needs a fresh block — bump the last hex digits. To start over
completely, rerun the setup script; the caches are in-memory, so restarting
them empties both tiers, and the script also clears the logs.

### UC-1 — The secondary rescue

*A primary miss is answered by the other zone's cache with no provider call —
and the backfill makes it one cross-zone lookup per entry, not per request.*

```bash
B='{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x112a880",false],"id":1}'
H='Content-Type: application/json'

# 1. warm the INTERNAL zone (its router, its trusted nodes, its cache)
curl -s -X POST http://127.0.0.1:3361 -H "$H" -d "$B" >/dev/null; sleep 1

# 2. same request on the EXTERNAL router — its own primary is empty
curl -si -X POST http://127.0.0.1:3360 -H "$H" -d "$B" | grep -i lava-provider-address

# 3. repeat it verbatim — step 2's backfill now answers locally
curl -si -X POST http://127.0.0.1:3360 -H "$H" -d "$B" | grep -i lava-provider-address

sleep 1
curl -s http://127.0.0.1:7779/metrics | grep '^smartrouter_cache_success_total'
```

Steps 2 and 3 both print `Lava-Provider-Address: Cached` — indistinguishable to
the caller — and the tiers split one hit each:

```
smartrouter_cache_success_total{…,cache_tier="secondary"} 1   ← step 2, the cross-zone rescue
smartrouter_cache_success_total{…,cache_tier="primary"}   1   ← step 3, served after the backfill
```

That one-to-one split is the whole feature in a single output: a request that
would have hit an upstream node was answered by another zone's cache, and it
cost exactly one cross-zone lookup.

### UC-2 — Full fall-through

*Both tiers miss: normal provider routing, and the secondary's only cost is the
one lookup it is bounded to.*

```bash
B='{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1020201",false],"id":1}'
curl -si -X POST http://127.0.0.1:3360 -H 'Content-Type: application/json' -d "$B" \
  | grep -i lava-provider-address
sleep 1
curl -s http://127.0.0.1:7779/metrics | grep '^smartrouter_cache_failed_total' \
  | grep 'cache_tier="secondary"'
```

The header names a real upstream endpoint instead of `Cached`, and the lookup
is classified as a clean miss:

```
smartrouter_cache_failed_total{…,cache_tier="secondary",outcome="miss"} 1
```

`outcome` is what separates a *cold* tier from a *broken* one: `miss` here
versus `error`/`timeout` in UC-4. The lookup's cost is in
`smartrouter_cache_latency_milliseconds{…,cache_tier="secondary"}`, and it can
never exceed `secondary-cache-timeout`.

### UC-3 — The router never writes the secondary

*Read-only in practice, not just by declaration.* Counting writes on the
secondary alone proves nothing — zero could just mean nothing happened. Count
**both** caches over the same requests, so the router's own primary is the
control:

```bash
before_secondary=$(grep -c 'Got Cache Set' $LOGS_DIR/CACHE_INTERNAL.log)
before_primary=$(grep -c 'Got Cache Set' $LOGS_DIR/CACHE_EXTERNAL.log)

# three blocks neither cache holds, through the EXTERNAL router
for b in 0x1010101 0x1010102 0x1010103; do
  curl -s -X POST http://127.0.0.1:3360 -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"$b\",false],\"id\":1}" >/dev/null
done
sleep 2

echo "secondary (read-only): $before_secondary -> $(grep -c 'Got Cache Set' $LOGS_DIR/CACHE_INTERNAL.log)"
echo "own primary (control): $before_primary -> $(grep -c 'Got Cache Set' $LOGS_DIR/CACHE_EXTERNAL.log)"
# secondary (read-only): 3 -> 3     ← unchanged
# own primary (control): 2 -> 5     ← +3, so the router was definitely writing
```

The internal cache *will* show writes from its own router — that is the
internal zone populating its own primary, which is the whole point. Only the
delta caused by external-zone traffic matters, and it is zero.

### UC-4 — A dead secondary never affects serving

*Kill the secondary mid-flight; serving continues, and the tier heals itself.*

Mark the router log before you start. It is opened in append mode, so it can
already hold reconnect lines from an earlier cycle — reading those is the
easiest way to misjudge this demo. (`wc -l` pads with spaces on macOS, and
`tail -n +` rejects that, hence the `tr`.)

```bash
MARK=$(wc -l < $LOGS_DIR/SMARTROUTER_EXTERNAL.log | tr -d '[:space:]')

lsof -ti:20101 -sTCP:LISTEN | xargs kill        # stop ONLY the cache server
sleep 2
nc -z 127.0.0.1 20101 2>/dev/null && echo "still up — the kill missed" || echo "secondary is down"
```

Serve two blocks neither cache holds:

```bash
for b in 0x1030301 0x1030302; do
  curl -s --max-time 25 -X POST http://127.0.0.1:3360 -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"$b\",false],\"id\":1}" | head -c 500; echo
done
sleep 1
curl -s http://127.0.0.1:7779/metrics | grep '^smartrouter_cache_failed_total' \
  | grep 'cache_tier="secondary"' | grep eth_getBlockByNumber
```

Both requests return real block data from upstream, and the metrics show:

```
smartrouter_cache_failed_total{…,cache_tier="secondary",method="eth_getBlockByNumber",outcome="error"} 1
```

Exactly **one** `error`, then silence — the client detects the dead connection
once, marks the tier inactive, and subsequent requests skip it entirely rather
than paying the timeout every time. (Filtering on the method keeps the router's
own `eth_blockNumber` tip queries, which carry their own series, out of the
way.)

#### Restart it and watch the router heal

`screen -d -m` detaches immediately, so a cache that fails to start produces
**no terminal output at all** — the failure goes to `CACHE_INTERNAL.log`.
Confirm the listener is actually back before you judge the router:

```bash
screen -d -m -S cache_internal bash -c "smartrouter cache 127.0.0.1:20101 \
  --metrics_address 0.0.0.0:20201 --log_level debug 2>&1 | tee -a $LOGS_DIR/CACHE_INTERNAL.log"

for i in $(seq 1 10); do nc -z 127.0.0.1 20101 2>/dev/null && break; sleep 1; done
nc -z 127.0.0.1 20101 2>/dev/null \
  && echo "cache is back" \
  || { echo "cache did NOT restart:"; tail -3 $LOGS_DIR/CACHE_INTERNAL.log; }
```

Then read only the lines written since the kill:

```bash
sleep 12
tail -n +$MARK $LOGS_DIR/SMARTROUTER_EXTERNAL.log | grep -i 'cache service'
```

```
WRN cache service connection error detected, triggering reconnection  address=127.0.0.1:20101
INF cache service reconnection loop started                           address=127.0.0.1:20101
DBG cache service connection attempt failed  error="context deadline exceeded"
DBG cache service connection attempt failed  error="context deadline exceeded"
INF cache service connected successfully                              address=127.0.0.1:20101
INF cache service reconnection succeeded, exiting reconnect loop      address=127.0.0.1:20101
```

The loop retries every ~8 s (a 3 s dial plus the 5 s interval) and reconnects
within seconds of the cache coming back.

> **Seeing only the first two lines does not mean the reconnect is broken — it
> means the cache is not up.** The loop retries indefinitely and will succeed
> the moment the port answers. Two things make this look worse than it is: the
> retry line is `DBG` and contains no "reconnect", so a `grep -i reconnect`
> hides it; and `context deadline exceeded` is simply how the blocking 3 s dial
> reports "nothing is listening", not a lookup timeout. The usual causes are a
> restart that hit `bind: address already in use` because the cache was never
> actually down, or an unset `$LOGS_DIR` in that shell — `tee` then dies on
> `/CACHE_INTERNAL.log` and takes the cache process down with it. The
> `cache is back` check above catches both.

#### Confirm secondary hits resume

The restarted cache is empty, so re-warm through the internal router — using a
block **no earlier demo has used**. A block that is already backfilled into the
external primary is answered from there, and you would be reading `Cached` from
the wrong tier:

```bash
B='{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1194567",false],"id":1}'
curl -s -X POST http://127.0.0.1:3361 -H 'Content-Type: application/json' -d "$B" >/dev/null
sleep 2
curl -si -X POST http://127.0.0.1:3360 -H 'Content-Type: application/json' -d "$B" \
  | grep -i lava-provider-address
sleep 1
curl -s http://127.0.0.1:7779/metrics | grep '^smartrouter_cache_success_total' \
  | grep 'cache_tier="secondary"'
```

```
Lava-Provider-Address: Cached
smartrouter_cache_success_total{…,cache_tier="secondary",method="eth_getBlockByNumber"} 1   ← +1
```

The header alone cannot close this step: a primary hit and a secondary hit are
byte-identical to the caller by design. The `cache_tier` counter is what tells
you which tier answered.

### UC-5 — Unconfigured is fully backwards compatible

*Restart the external router without the flag; the tier leaves no trace at
all.*

```bash
lsof -ti:3360 -sTCP:LISTEN | xargs kill; sleep 3
smartrouter config/smartrouter_examples/smartrouter_eth_zone_external.yml \
  --log-level debug --cache-be '127.0.0.1:20100' \
  --use-static-spec specs/ethereum.json --skip-websocket-verification \
  --metrics-listen-address ':7779' 2>&1 | tee $LOGS_DIR/SMARTROUTER_NOSEC.log &
sleep 25

curl -s http://127.0.0.1:7779/metrics | grep -c 'cache_tier="secondary"'   # 0
grep -ic 'secondary cache' $LOGS_DIR/SMARTROUTER_NOSEC.log                 # 0
```

Zero secondary metric series and zero secondary log lines, while the primary
tier and request serving continue normally. Rerun the setup script to put the
lab back the way it was.

### Bonus — misconfiguration aborts startup

Not a use case, but the flip side of "independently configurable": all three
bad configurations exit non-zero with a specific message rather than starting
in a surprising state.

```bash
CFG=config/smartrouter_examples/smartrouter_eth_zone_external.yml
smartrouter $CFG --secondary-cache-be '127.0.0.1:20101' --secondary-cache-mode read-write
# exit=1  secondary-cache-mode must be "read-only" (read-write is reserved for a future iteration), got "read-write"

smartrouter $CFG --secondary-cache-timeout 100ms
# exit=1  secondary cache options are set while secondary-cache-be is empty — dangling
#         configuration (set secondary-cache-be or drop secondary-cache-timeout/secondary-cache-mode)

smartrouter $CFG --secondary-cache-be '127.0.0.1:20101' --secondary-cache-timeout 0s
# exit=1  secondary-cache-timeout must be greater than zero, got 0s
```

### Tearing the lab down

```bash
killall smartrouter
screen -wipe
```

## Scope of this release

Deliberately out of scope, and rejected at startup rather than silently
ignored:

- **Read-write secondary mode.** `secondary-cache-mode` accepts only
  `read-only`; `read-write` aborts startup with an explicit
  "reserved for a future iteration" error. The setting exists so that read-only
  is an explicit, auditable choice in your config rather than an implicit
  default.
- **More than two tiers.** The design is one primary and one optional
  secondary. An ordered chain of N tiers is not supported.

## Use-case coverage

Traceability against *PRD: Secondary Cache Backend* (April 2, 2026). What the
router does in each case is described under
[What happens in each situation](#what-happens-in-each-situation) and can be
reproduced by hand from [Manual demo
walkthrough](#manual-demo-walkthrough); this section records where each one is
*proven*, so the claims above are auditable rather than asserted.

### Use cases

| PRD use case | Status | Proven by |
| --- | --- | --- |
| **UC-1** Primary miss with secondary fallback — served without hitting providers, populator decides the backfill | Covered | `TestSecondaryCacheHitServesWithoutPrimary`, `TestSecondaryHitBackfillsPrimaryWithExactKeyAndValidSeenBlock` (backfill goes through a **real** cache server, so a follow-up primary GET provably hits) |
| **UC-2** Secondary miss — full fall-through, no overhead beyond the lookup | Covered | `TestSecondaryCacheTimeoutAndErrorAreMisses` — one subtest per outcome (`clean not-found`, `timeout`, `error`); `TestMergeBlockHashHeights` pins that a miss still contributes its block-hash data |
| **UC-3** Secondary configured read-only — never modified by this router | Covered | Structural: `performance.CacheReader` exposes only `CacheActive`/`GetEntry`, so writes are a compile error. `TestSecondaryCacheConfigValidate` pins that any mode but `read-only` aborts startup |
| **UC-4** Secondary unreachable — skipped, no impact, reconnects | Covered | `TestSecondaryCacheTimeoutAndErrorAreMisses` (bounded by the timeout), `TestSecondaryCacheActiveNilSafety`; reconnection is the primary's own loop, shared unchanged |
| **UC-5** No secondary configured — fully backwards compatible | Covered | `TestSecondaryCacheActiveNilSafety` (nil interface *and* typed-nil client), `TestSecondaryCacheFlagAndYamlWiring` |

### Must-have requirements

| Requirement | Status | Notes |
| --- | --- | --- |
| Optional secondary, queried on primary miss before providers | Met | Also runs when the primary is down or unconfigured, not only on a miss |
| Independently configurable connection details | Met | `secondary-cache-be`, separate connection from `cache-be` |
| Read-only — the router never writes to it | Met | Enforced by the type system, not by convention |
| Backfill governed by the existing caching populator | Met | The populator is called unconditionally with the entry's true state; it owns every eligibility rule. `TestSecondaryHitWithRejectedStatusServesButNeverBackfills` and `TestSecondaryCachedNodeErrorsServeButNeverBackfill` show it rejecting |
| Configurable lookup timeout; exceeded = miss | Met | `secondary-cache-timeout`, default `50ms` |
| Failure never affects serving; primary-grade resilience | Met | Same reconnect loop and non-blocking failure semantics as the primary |
| Backwards compatible when unconfigured | Met | No secondary series in metrics, no added step in the request path |
| No provider-identifying metadata exposed | Met — and **not** a no-op | The format *does* carry it (`Sig`, `SigBlocks`, `Metadata` holding upstream response headers). All three are dropped on a private copy that becomes the only copy used. `TestSanitizeForeignCacheReplyDropsAllMetadataAndSignatures`, `TestSecondaryPoisonedEntrySanitizedForCallerAndBackfill` |

### Nice-to-have requirements

| Requirement | Status |
| --- | --- |
| Prometheus metric parity with a `cache_tier` label | Built, exactly as the PRD suggested — one label rather than separate metric names, plus an `outcome` label splitting miss/error/timeout |
| Secondary lookups visible in traces | Built — each lookup is a span carrying `cache.tier` and `cache.outcome`; the root span records which tier served |
| Log the secondary configuration at startup | Built — one line with address, mode, and timeout |
| Configurable access mode | Built as an explicit setting; only `read-only` is accepted. Read-write is deferred and rejected at startup rather than ignored |
| Support for N cache tiers | Not built — the PRD marks this optional and scopes to one secondary |

### Open question, answered

> *Should mixed cache engine configurations be supported?*

Yes. The two tiers are independent connections that speak the Smart Router
cache protocol; the storage engine behind each address is that cache service's
own concern and is invisible to the router. Pairing different supported engines
across tiers needs no special handling.

### Where the proof lives

| Layer | Location |
| --- | --- |
| Unit and integration tests | `protocol/rpcsmartrouter/secondary_cache_test.go`, `protocol/performance/secondary_config_test.go`, `protocol/performance/sanitize_test.go`, `types/relay/cache_test.go` |
| Startup/config validation, end to end against the real binary | `tests/infrastructure/binary_startup/test_secondary_cache_config_validation.py` in `Magma-Devs/smart-router-automation` — fail-fast cases, advisory warnings, startup visibility, and flag-beats-YAML precedence |
| Live two-zone flow | `scripts/pre_setups/init_smartrouter_eth_secondary_cache.sh` with `RUN_DEMO=1`; one hands-on demo per use case in [Manual demo walkthrough](#manual-demo-walkthrough) |
