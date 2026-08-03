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
- **Independent of the primary.** The secondary keeps serving while the primary
  is down, and is even valid with no primary configured at all (reads work;
  nothing backfills — the router logs an advisory warning for this topology).
- **No identity leakage.** Responses served from the secondary carry none of
  the originating environment's metadata or signatures: upstream headers are
  stripped, and the caller sees the router's own `Lava-Provider-Address:
  Cached` header, identical to a primary-cache hit.

Any backend that speaks the Smart Router cache protocol works as either tier —
the secondary is simply a second `smartrouter cache` address.

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

## Trying it locally

- **Docker Compose:** layer `docker/docker-compose.secondary-cache.yml` on top
  of the base and cache overlays — see
  [LOCAL-COMPOSE.md](LOCAL-COMPOSE.md#enabling-a-read-only-secondary-cache).
- **Bare metal, full two-zone demo:**
  `scripts/pre_setups/init_smartrouter_eth_secondary_cache.sh` starts two
  caches and two routers and (with `RUN_DEMO=1`) proves the
  secondary-hit → backfill → primary-hit flow end to end.
