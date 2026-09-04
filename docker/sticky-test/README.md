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
