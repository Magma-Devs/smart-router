# Cross-pod sticky sessions — end-to-end test

```
nginx (round-robin)  ->  router-a | router-b | router-c  ->  node-a..node-d
                                    \
                                     cache-be (fleet-wide sticky claims)
```

```bash
docker/sticky-test/run.sh                          # run it
KEEP=1 docker/sticky-test/run.sh                   # leave the stack up
STICKY_SHARED_STATE=false docker/sticky-test/run.sh # negative control: MUST fail
```

Exit codes: `0` pass, `1` an assertion failed, `2` the harness itself is untrustworthy.

## What it proves

A request carrying `lava-stickiness: <id>` reaches the same upstream no matter which replica
serves it. Three replicas behind a plain round-robin ingress mean a session lands on a
different pod almost every request, which is exactly the arrangement that used to break it.

## Why the control phases come first

Two ways this test can pass while proving nothing, both checked before any real assertion:

1. **The ingress does not spread.** If every request lands on one replica, pod-local
   stickiness — which has always worked — makes the run pass. The driver reads
   `X-Router-Pod` (set by nginx from `$upstream_addr`) and aborts if fewer than two
   replicas served traffic.
2. **Selection is degenerate.** If one upstream serves everything anyway, "always the same
   upstream" is trivially true. The driver aborts unless the no-header phase used more
   than one upstream.

Related traps the harness already handles:

- **Keep-alive.** A reused connection pins to one pod through a round-robin ingress. Every
  request sends `Connection: close`, and nginx does not hold connections to the routers.
- **Response caching.** A repeated request is a cache hit on the second pod and never
  reaches an upstream, hiding which one the router picked. Every request asks for a
  distinct block, so the cache always misses and an upstream is always contacted.
- **Head lag.** The fake nodes report identical heads by default. A lagging node can be
  scored down and stop being selected, which would skew the measurement. Set `HEAD_OFFSET`
  to reintroduce lag deliberately.

## Negative control

`STICKY_SHARED_STATE=false` turns the feature off. The sticky phase must then FAIL. Measured
with it off: one session id served by two upstreams, and 9 of 12 sessions split across
upstreams — the reported customer bug. A harness that passes either way measures nothing.

## Reading the metrics

With `KEEP=1`, each replica exposes metrics on :7801, :7802, :7803, and its debug server on
:9801, :9802, :9803:

```bash
curl -s :7801/metrics | grep smartrouter_csm_sticky_claims_total
curl -s :9801/debug/sticky-claims
```

The route reports the same seven counts per endpoint, cumulative for the replica's lifetime,
with `PodID` naming the replica that answered and `SharedSticky` saying whether the claim
registry is wired (`SharedStickyReason` says why not: with `STICKY_SHARED_STATE=false` it reads
`--shared-state not set`). The counts are per replica: read each port, or compare two readings
only when their `PodID` matches.

- `claimed` — this pod created the claim. Summed across pods it should equal the number of
  DISTINCT session ids used, because a claim is first-writer-wins fleet-wide.
- `adopted` — this pod routed by a claim it read from the registry: a peer's, or its own after
  dropping its local copy (`invalidated` moves with it then). **Zero here on every pod means the
  feature is wired but never firing.**
- `local_hit` — answered from this pod's confirmed table with no round trip.
- `error` — the claim could not be established and the request was failed rather than
  served off an unverified pin.

---

# The customer's bug — false gaps (`run-falsegap.sh`)

The test above asks *"does one session id reach one upstream across pods"*. This one asks the
question the customer actually cares about: *"does my ingestion still record gaps that are not
there"*.

```bash
docker/sticky-test/run-falsegap.sh
KEEP=1 docker/sticky-test/run-falsegap.sh
```

## Their sequence, run as one unit

```
eth_blockNumber              -> H
eth_getBlockByNumber(H)      -> the block, or null
```

If the two calls land on different upstreams and the second is behind, it answers `null` inside
a successful response. Nothing retries, and the caller records a gap in a chain that has none.

## Shaped after a real deployment

`router-lagging-pool.yml` follows the shape of a real production ConfigMap: three primary
providers, `debug`/`trace` addon urls, a `backup-direct-rpc` tier, the listener on `:3000`, and
`metrics-listen-address` declared in the config rather than as a flag.

Two deliberate deviations, both documented in that file:

- The fakes replace real nodes, so verifications are skipped.
- **The `ws://` urls are dropped.** With them in place all three primaries were excluded at
  startup — one unreachable node-url costs the whole provider, and
  `--skip-websocket-verification` does not cover it. Every relay then fell to the backup tier.

## Why the chain has to move

`BLOCK_TIME=1` makes every fake advance one block per second, keeping its fixed distance behind
the tip.

This is not for realism. With a frozen chain every round asks for the same block, and the second
round onward is answered from cache without any upstream being contacted — **including a cached
`null`**. The first version of this harness reported 30/30 false gaps while the nodes recorded
almost no traffic at all. The measurement described the cache, not provider selection.

## Phases

| phase | what it does | fails the run? |
|---|---|---|
| **A** | no affinity — **must** produce false gaps | yes, aborts if it cannot |
| **B** | with affinity — must produce none | yes |
| **C** | distinct sessions must still spread over the pool | yes |
| **D** | breaks the pinned node and reports what the retry does | no, informational |

Phase A is load-bearing. A clean phase B means nothing unless phase A proved the harness can see
the bug.

Phase D documents a known limit rather than a regression: a pin is dropped on retry (MAG-2228),
so a genuine error on the pinned node can send the retry to an upstream that is behind. It is
reported, never asserted.

## Measured

| | false gaps in 20 rounds |
|---|---|
| no affinity | 4–8 |
| with affinity | 0, across all 3 replicas, work still spread over all 3 upstreams |

Theory predicts roughly a third of unpinned rounds should fail with heads at H, H−1, H−2.


---

# Production-readiness scenarios (`run-qa.sh`)

```bash
docker/sticky-test/run-qa.sh                          # 8 scenarios, ~6 min
STICKY_SHARED_STATE=false docker/sticky-test/run-qa.sh # negative control: all but select-provider MUST fail
```

Brings the stack up once and runs every scenario against it, including the ones that need the
infrastructure disturbed — a replica restarted, the registry stopped and started again.

| Scenario | What it proves | Detects a broken feature? |
|---|---|---|
| Customer sequence across replicas | head-then-fetch over 3 replicas: both on one upstream, no false gaps | yes |
| Session id on every call | the id must be on the head request too, not only the fetch | yes |
| Concurrent requests on one id | simultaneous requests for one id do not split across upstreams | yes |
| `lava-select-provider` beats a claim | naming a provider explicitly still wins | no, by design |
| Claims hold before a restart | each id stays on one upstream across all 3 replicas; records the claims | yes |
| A replica restarts | the restarted pod, its table empty, adopts the claims made before the restart | yes |
| Registry unreachable | sticky fails closed; traffic without the header is unaffected | yes |
| Registry restored | sticky works again | yes |

One of these does not discriminate, and that is stated on purpose rather than left to look
stronger than it is: `select-provider` does not depend on shared state at all.

**Every affinity check asks with 6 session ids** (MAG-4129). Without the registry each replica
picks for itself, and the three pick the same upstream by chance at least one time in nine (2 of
24 trials), so a check that asked with one id passed with the feature disabled: concurrency did,
and so did the restart check, once in three runs. A replica sends its share of a session's
requests almost entirely to one upstream, so more requests per id barely help; more ids do.
Every one of 6 has to hold, so a feature-off run passes a check by chance about once in half a
million.

**The head-then-fetch checks assert the pin, not only the gap.** A false gap needs the head and
the fetch on different upstreams *and* the fetch's upstream behind, so with the feature off only
9 of 36 rounds gapped while 23 of 36 split, and a run that counted gaps alone passed about one
time in twenty (once in four negative-control runs). The customer and id-on-every-call scenarios
now also require every pinned round's two replies to name the same upstream: 0 of 40 split with
the feature on.

**The restart check needs claims older than the restart.** `claims-before-restart` makes them,
one request per replica per id, and writes them to `$STICKY_QA_STATE` (`run-qa.sh` gives each run
its own file); `restarted-pod` reads them back after router-a restarts and requires every id to
keep its upstream. It used to make its claim after the restart, which tested a fresh claim, not
an adopted one. Run by hand against a `KEEP=1` stack, the order is the same: `claims-before-restart`,
`docker compose -f docker-compose.falsegap.yml restart router-a`, `restarted-pod`.

**Reading a failure.** Which upstream answered is read from the router's `Lava-Provider-Address`,
not from the `servedBy` the fake writes into the body: a reply from the response cache carries the
body of whichever upstream filled the cache, and its header says `Cached`. A sticky reply that
says `Cached` fails the scenario as a MAG-4122 regression instead of being counted as an upstream.
A false gap in a round that carries the id on both calls prints both replies' upstream,
`Lava-Retries` and replica, and names the path that produced it.

**Refused rounds are not gaps.** A pinned call fails rather than move to another upstream when
the router believes the pinned one is more than 10 blocks behind the tip (MAG-4123). The fakes
advance a block a second, faster than the router polls their heads, so here about one pinned head
call in ten is refused with `pinned provider is too far behind the chain tip`. The customer and
id-on-every-call scenarios report those rounds and leave them out of the gap count: the caller saw
an error, which a silent gap is not. A scenario fails when fewer than half its rounds were
answered, because too few are then left to judge the pin.

**Closed gap.** A sticky request used to be answered from the response cache before any upstream
was chosen, so the pin was not consulted: on a cold start, 24 sticky requests produced 19 claim
resolutions. A request carrying `lava-stickiness`, `lava-select-provider` or `lava-providers-block`
no longer reads the cache (MAG-3864, MAG-3520), so every sticky request resolves a claim.
