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

With `KEEP=1`, each replica exposes metrics on :7801, :7802, :7803:

```bash
curl -s :7801/metrics | grep smartrouter_csm_sticky_claims_total
```

- `claimed` — this pod created the claim. Summed across pods it should equal the number of
  DISTINCT session ids used, because a claim is first-writer-wins fleet-wide.
- `adopted` — this pod took a claim a peer had made. **Zero here on every pod means the
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

## Shaped after their deployment

`router-kraken.yml` follows their real ConfigMap: their three provider names, the reth
`debug`/`trace` addon urls, a `backup-direct-rpc` tier, the listener on `:3000`, and
`metrics-listen-address` declared in the config rather than as a flag.

Two deliberate deviations, both documented in that file:

- The fakes replace real erigon/reth nodes, so verifications are skipped.
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
