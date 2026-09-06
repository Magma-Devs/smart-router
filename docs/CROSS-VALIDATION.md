# Cross-Validation

Cross-validation fans a single **read** request out to several upstream providers in
parallel, hashes each successful response (`SHA256(reply.data)`), and only returns an
answer once a **quorum** of providers agree on the same hash — optionally requiring the
agreeing providers to span multiple distinct vendor **groups**. It defends against a single
compromised or buggy provider returning a wrong-but-well-formed answer.

This page is the **operator-facing setup guide**: how to wire it from a config file, with
two ready-to-run example configs. For the full knob/header/failure-reason reference and the
internals (outlier handling, per-group quorum selection, gRPC trailers), see the in-package
reference: [`protocol/rpcsmartrouter/README.md` → Cross-Validation](../protocol/rpcsmartrouter/README.md#cross-validation).

## When to use it

Cross-validation trades latency and upstream cost (you pay for N relays instead of 1) for
**answer integrity**. Reach for it on high-value, deterministic reads where a single wrong
answer is expensive — balances, receipts, `eth_call` against a contract — not on every
method, and never as a substitute for the stateful fan-out on writes.

## Two ingredients

Cross-validation only does something useful when both pieces are present:

1. **Multiple distinct sources per `chain<>interface`.** A quorum drawn from one vendor is
   not independent confirmation. Configure ≥ 2 providers for the same `(chain-id,
   api-interface)`, each tagged with a `group-label` (vendor / operator / region). Providers
   with no label fold into the implicit `"default"` group.
2. **A policy (or caller headers) that mandates a quorum.** Either a per-request header
   (`lava-cross-validation-*`) or a per-method `cross-validation:` policy block. The two
   compose as `clamp(caller, floor, cap)`.

```yaml
direct-rpc:
  - name: eth-publicnode
    group-label: "publicnode"    # <-- group A
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: https://ethereum-rpc.publicnode.com
  - name: eth-tenderly
    group-label: "tenderly"      # <-- group B
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: https://mainnet.gateway.tenderly.co

cross-validation:
  policies:
    - chain-id: ETH1
      api-interface: jsonrpc
      method: eth_getBalance
      enabled: true              # mandate CV even with no caller headers
      agreement-threshold: 2     # 2 identical responses form the quorum
      max-participants: 2        # fan out to both
      min-groups: 2              # the quorum must span both groups (publicnode + tenderly)
```

### How caller headers compose with a policy

Each numeric knob resolves as `clamp(caller, floor, cap)`. The scalar shorthand above
(`agreement-threshold: 2`) means `{floor: 2}` — a **floor** is an operator *minimum* the
caller may exceed, so on that policy a caller asking for a stricter quorum gets it. A **cap**
is an operator *maximum*, and it bounds how strict a caller may ask to be: a request above the
cap is clamped down to it, and a knob written with `floor == cap` is pinned to that value
whatever the caller sends.

```yaml
      max-participants: { floor: 3, cap: 3 }     # pinned: a caller asking for 6 gets 3
      agreement-threshold: { floor: 2, cap: 3 }  # caller may raise 2 -> 3, and no further
```

So the shorthand "a caller may make cross-validation stricter, never weaker" holds only **up
to the configured cap**. The cap is the one mechanism by which the router validates less
strictly than a caller asked, and it is an explicit operator decision — an operator who wants
a method's fan-out pinned regardless of caller headers sets `floor == cap`, and one who wants
the headers ignored outright sets `forbid-caller-cv: true`.

With no policy for a method the caller's headers are the only authority, and any
self-consistent shape is honored — including the degenerate `max-participants: 1` with
`agreement-threshold: 1`, which returns `lava-cross-validation-status: success` after a
single response because the requested quorum of one was met. Nothing is compared in that
case; `…-all-providers` and `…-agreeing-providers` each list the one provider queried, so a
client that needs real corroboration should read those lists rather than `status` alone.

## Worked examples

Two bundled example configs demonstrate the full setup end to end:

| Example config | What it shows |
| --- | --- |
| [`config/smartrouter_examples/smartrouter_cosmos.yml`](../config/smartrouter_examples/smartrouter_cosmos.yml) | **Two** distinct sources per Cosmos Hub interface (PublicNode · Polkachu), each `group-label`'d — the *fleet* a diversity policy needs. No policy block, so it's caller-driven until you add one. |
| [`config/smartrouter_examples/smartrouter_multichain_cross_validation.yml`](../config/smartrouter_examples/smartrouter_multichain_cross_validation.yml) | A full multi-chain fleet with **two** sources per `chain<>interface` (PublicNode + a second public vendor) **and an active `cross-validation:` policy block** mandating `min-groups: 2` corroboration on `eth_getBalance`, `eth_getTransactionReceipt`, Solana `getEpochInfo`, and a Cosmos Hub REST bank balance. |

Its plain (no-policy) sibling, [`smartrouter_multichain.yml`](../config/smartrouter_examples/smartrouter_multichain.yml), has the same two-source-per-interface fleet but leaves cross-validation off — diff the two to see exactly what the `cross-validation:` block adds.

Run the cross-validating multichain example:

```bash
smartrouter config/smartrouter_examples/smartrouter_multichain_cross_validation.yml \
  --use-static-spec specs/ --skip-websocket-verification
```

At startup the router logs the resolved provider→group layout and **rejects** a policy the
configured fleet can never satisfy (e.g. `min-groups: 3` with only two groups), so a
misconfiguration fails fast rather than silently degrading.

## What the caller sees

A cross-validated response carries headers describing the quorum
(`lava-cross-validation-status`, `…-agreeing-providers`, `…-disagreeing-providers`,
`…-pending-providers`, and on failure `…-failure-reason`). The quorum early-exits once the
threshold is met, so a provider that answers too late is reported as **pending** rather than
silently dropped — `disagreeing-providers` only ever lists dissent the router actually received,
and the straggler's late answer is still compared against the consensus asynchronously (log +
`smartrouter_cross_validation_straggler_total`). The router does **not** auto-retry on a quorum
failure — the structured signal lets the client decide whether to retry (quorum-time reasons) or
fall back (structural reasons). Header names, the closed failure-reason enum, and the gRPC-trailer
caveat are tabulated in the [in-package reference](../protocol/rpcsmartrouter/README.md#response-headers).

A router started with `--debug-address` additionally serves `GET /debug/cross-validation-events`, a
read-only per-request record of the dissent it observed (which provider, which group, which
recording path) for test suites that cannot scrape the metrics port — see
[Reading recorded dissent](../protocol/rpcsmartrouter/README.md#reading-recorded-dissent).

## Caveats

- **Writes.** An *operator policy* on a stateful (write) method is rejected at startup.
  Cross-validating a write response verifies nothing — leave writes to the stateful fan-out.
  To also block the legacy *caller-header* path on a specific write, set
  `forbid-caller-cv: true` on its policy.
- **Cost & latency.** N relays per request. Scope policies to the methods that warrant it.
- **Public endpoints are best-effort.** The example fleets use rate-limited community
  endpoints; for production, point at your own nodes or keyed gateways.
- **No value-threshold escalation.** There is no knob that raises a method's policy for an
  individual request based on the value it carries (parsing `eth_sendRawTransaction` for a
  native amount, decoding ERC-20 calldata). That was descoped; a method's policy is the same
  for every request to it. Callers that need a stricter quorum on a specific request can still
  ask for one with the headers, up to the policy's cap.

## Local testing lanes

Two lanes, each bringing up its own stack, checking itself, and then printing how to drive
it by hand. They use distinct ports and can run at the same time — which is the point of the
second one, since comparing a configured router against an unconfigured one is the
backwards-compatibility claim.

| Lane | Covers |
| --- | --- |
| [`scripts/pre_setups/init_smartrouter_cv_demo.sh`](../scripts/pre_setups/init_smartrouter_cv_demo.sh) | Six simulator providers in three groups, six policies. Sub-demos `--uc1` `--uc2` `--uc4` `--uc5` `--uc6` (or `--all`) drive per-method policy, group diversity, mismatch metrics, the structured failure signal, and outlier exclusion. |
| [`scripts/pre_setups/init_smartrouter_cv_default.sh`](../scripts/pre_setups/init_smartrouter_cv_default.sh) | The same fleet with **no** `cross-validation:` block and **no** group labels — the pre-feature shape, proving existing deployments and callers are untouched. |

Both are ownership-safe: they reclaim only the process they recorded starting (pid plus a
start-time fingerprint), never run `killall`, and **refuse to start** if their ports are held
by anything else, printing a ready-made override. Generated configs land in `debugging/`,
which is gitignored. Tear down with `--stop`; `--status` reports what is up.

They drive the sibling **`provider_simulator`** rather than real endpoints, because half of
these behaviours only exist when providers *disagree* and real endpoints agree on finalized
state — a shared-truth fleet can never manufacture a dissent. The simulator's per-method body
override returns a valid-but-divergent result, which is exactly the one input the mismatch
surface admits: a successful content outlier on a deterministic method. Its latency knob
decides who wins the race to quorum, so the reply-time and straggler paths are selected
deterministically rather than by luck. Point the lanes at a checkout with `SIM_DIR=…` if it
does not sit next to this one.

The router-side unit tests need no infrastructure:

```bash
go test ./protocol/rpcsmartrouter -run 'CrossValidation|Quorum|Group' -v   # policy resolution, fail-fast, event recorder
go test ./protocol/relaycore     -run 'Diversity|Straggler|Quorum'   -v   # quorum computation, group selection, the async watcher
go test ./protocol/metrics       -run 'CrossValidation'              -v   # label shapes and cardinality
```

## Walkthrough

Six scenarios. The first five run against one router that stays up between them, so you can
interleave your own `curl`s; the sixth is the second lane.

### Setup

```bash
scripts/pre_setups/init_smartrouter_cv_demo.sh
```

| | Where | Role |
| --- | --- | --- |
| provider_simulator | control `127.0.0.1:19000`, eth-sim on `18545-18547` + `18560-18562` | six upstreams whose answers you control |
| Smart Router | `0.0.0.0:3396`, metrics `:7794`, debug `:6796` | six policies over three provider groups |

The fleet is deliberately lopsided in a useful way — three groups of two:

```yaml
tier-1   sim-1  sim-2
external sim-3  sim-4
archive  sim-5  sim-6
```

It ends with a self-check, so a broken stack is caught before you start:

```
[Smoke] policy load -> fan-out -> quorum
    router /lava/health                    200
    policies loaded                        policies=6
    distinct groups                        distinctGroups=3
    mandated cross-validation              success
    agreeing providers                     sim-2,sim-4

  SMOKE PASS — the stack is demo-ready.
```

`policies=6` and `distinctGroups=3` come from the router's own startup line — the resolved
provider→group layout, logged once so an operator can confirm the diversity a config actually
yields rather than the diversity it looks like it asks for.

Knobs, all optional: `SIM_DIR` for the simulator checkout, `ROUTER_PORT` / `METRICS_PORT` /
`DEBUG_PORT` / `NEG_PORT` to move ports, `SKIP_SMOKE=1`.

### 1. A method's policy outranks what the caller asked for (UC-1)

```bash
scripts/pre_setups/init_smartrouter_cv_demo.sh --uc1
```

```
[1] 'eth_getBalance' has a policy with enabled: true — no caller headers sent
    status                                 success
    all-providers                          sim-1,sim-2,sim-3,sim-4,sim-5,sim-6
    agreeing-providers                     sim-2,sim-3

[2] 'eth_getBlockByNumber' has NO policy and the caller sent no headers
  PASS  not cross-validated — a policy is scoped to its own method

[4] 'eth_gasPrice' carries forbid-caller-cv — the same headers are IGNORED
  PASS  cross-validation is off for this method whatever the caller asks

[5] precedence on 'eth_getCode' — floor 2/cap 3 threshold, max-participants pinned to 3
    a caller asking for MORE than the cap is clamped down to it:
    requested                              max-participants 6, threshold 6
    all-providers                          sim-2,sim-4,sim-6
    a caller asking for LESS than the floor gets the floor:
    requested                              max-participants 1, threshold 1
    all-providers                          sim-1,sim-4,sim-6
```

Step 5 is the answer to "which wins, the header or the policy?": **neither, categorically** —
each knob resolves as `clamp(caller, floor, cap)`. A caller who asks for six providers on a
capped method gets three; a caller who asks for one gets three as well. The two directions are
the same rule, and `forbid-caller-cv` (step 4) is the escape hatch for a method where the
caller should have no say at all.

The resolution is logged per request at debug level (`CrossValidation mode enabled
(policy-resolved)`), so you can see which authority produced the numbers a given relay used.

### 2. A quorum that isn't independent is not a quorum (UC-2)

```bash
scripts/pre_setups/init_smartrouter_cv_demo.sh --uc2
```

`eth_getTransactionCount` carries `min-groups: 2`. The interesting case is the failure: every
provider outside `tier-1` is made to answer a *distinct* wrong value, so the only pair that
agrees is `sim-1` + `sim-2` — a legitimate count quorum, drawn entirely from one vendor.

```
[2] a count quorum that all came from ONE group must be REJECTED
    status                                 failed
    failure-reason                         diversity-unmet
```

`eth_call` uses the stronger form (`per-group-quorum: true`): each of two groups must reach
its *own* quorum of two and the winners must then agree. Breaking one provider in every group
leaves no group able to reach its internal quorum:

```
[4] break ONE provider in EVERY group — no group can reach its own quorum
    status                                 failed
    failure-reason                         group-quorum-unmet
```

Two distinct reasons for two distinct policies, which is what lets a client tell "the fleet
disagreed" from "the fleet agreed but not diversely enough".

The lane then starts two throwaway routers with policies the fleet can never satisfy, and both
refuse to boot:

```
[5] a policy the fleet can NEVER satisfy is refused at STARTUP, not per request
  PASS  refused to start — min-groups: 4 over a 3-group fleet
        ERR cross-validation min-groups policy cannot be satisfied: configured provider groups are fewer than required
  PASS  refused to start — per-group-quorum with max-participants 3 < 2 * 2
        ERR invalid cross-validation configuration error="… per-group-quorum needs max-participants >= min-groups * agreement-threshold"
```

The second one is caught by the config preflight, before a single provider is dialled. A
misconfigured diversity policy is a deployment error, not a per-request failure — so it costs
you a failed rollout rather than a silent quorum you were not actually getting.

### 3. Divergence after quorum reaches the alerting surface (UC-4)

```bash
scripts/pre_setups/init_smartrouter_cv_demo.sh --uc4
```

`sim-6` (group `archive`) is made to answer first, and to answer wrong:

```
[1] reply-time dissent: sim-6 (archive) answers FIRST and answers wrong
    status                                 success
    disagreeing-providers                  sim-6
    mismatch_total{group=archive}          0 -> 1

smartrouter_cross_validation_mismatch_total{apiInterface="jsonrpc",finality="finalized",group="archive",method="eth_getBalance",spec="ETH1"} 1
```

The request succeeded — the honest majority answered — and the divergence is still visible.
The labels are the point: `group` rather than provider address keeps cardinality bounded and
stable enough to alert on, and `finality="finalized"` is the high-signal case, because
divergence on settled state cannot be explained away as propagation lag. That label is why the
lane queries block `0x1000000` rather than `latest`.

The second scenario delays the same outlier past the quorum early-exit, so it is still in
flight when the reply ships:

```
[2] straggler dissent: the SAME outlier, delayed past the quorum early-exit
    status                                 success
    pending-providers                      sim-1,sim-3,sim-5,sim-6
    straggler_total{outcome=disagreed}     0 -> 1
```

It is reported as **pending**, never silently dropped and never accused in
`disagreeing-providers` — which lists only dissent the router actually received. An async
watcher compares its late answer against the reached consensus and increments the same
mismatch counter, deduped per group. A dissenter that answers after the quorum closed still
reaches alerting instead of vanishing.

Per-request detail lives at `GET /debug/cross-validation-events` (the lane passes
`--debug-address`), which names the provider, its group, and which path recorded it:

```bash
curl -s "http://127.0.0.1:6796/debug/cross-validation-events?outcome=disagreed" | jq
```

### 4. A quorum failure is not a generic upstream error (UC-5)

```bash
scripts/pre_setups/init_smartrouter_cv_demo.sh --uc5
```

```
[1] QUORUM-TIME failure: all six answer differently, so nothing reaches 2
    status                                 failed
    failure-reason                         no-agreement
    all-providers                          sim-1,sim-2,sim-3,sim-4,sim-5,sim-6

[2] STRUCTURAL failure: a caller asks for more providers than exist
    status                                 failed
    failure-reason                         insufficient-capacity
  PASS  no provider lists — nothing was queried, and the headers say so

[3] the contrast: a plain upstream error carries NO cross-validation headers
  PASS  'quorum failure' is distinguishable from a generic upstream error
```

The split in the reason enum is the client's decision procedure. A **quorum-time** reason
(`no-agreement`, `insufficient-responses`, `diversity-unmet`, `group-quorum-unmet`) means
responses came back and did not agree — a retry against a different set may help. A
**request-time structural** reason (`insufficient-capacity`, `insufficient-groups`) means the
candidate set could not even be assembled, so retrying this router will not help and the client
should fall back. The structural case carries no provider lists precisely because no provider
was queried; `failure-reason` is the whole signal.

The same split is on the metrics side, so alerts can distinguish them without parsing logs:

```
smartrouter_cross_validation_failures_total{method="eth_getBalance",reason="no-agreement",…} 1
smartrouter_cross_validation_failures_total{method="eth_getBlockByNumber",reason="insufficient-capacity",…} 1
smartrouter_cross_validation_failures_total{method="eth_getTransactionCount",reason="diversity-unmet",…} 1
smartrouter_cross_validation_failures_total{method="eth_call",reason="group-quorum-unmet",…} 1
```

Neither request was retried against a different provider set. That is deliberate: the router
applies the policy, returns a structured result, and emits metrics; retry, fallback and
resubmission policy belong to the client, which is the only party that knows what the request
was for.

### 5. An outlier is outvoted, not filtered (UC-6)

```bash
scripts/pre_setups/init_smartrouter_cv_demo.sh --uc6
```

The same single divergence, under two policies:

```
[1] tolerant policy ('eth_getBalance', 2 of 6): one provider diverges
    status                                 success
    disagreeing-providers                  sim-5
    result returned to the client          0x0

[2] unanimous policy ('eth_getStorageAt', 6 of 6): the SAME single divergence
    status                                 failed
    failure-reason                         no-agreement
```

There is no separate outlier-detection step. An outlier is just a successful response whose
hash differs from the reached consensus: it forms its own bucket of one and loses the vote.
It never becomes the answer (`0x0`, not the injected value) and it is never silently dropped —
it is named in `disagreeing-providers` and counted on the mismatch metric.

Whether it *blocks* the request therefore falls out of the policy rather than being a separate
rule. Under `2 of 6` the agreeing providers still clear the threshold, so the outlier is
excluded and the client is served. Under `6 of 6` there is no slack: the largest bucket is five
against a threshold of six, and the request correctly fails rather than returning a quorum that
quietly excluded a dissenter. Choosing unanimity is choosing to fail closed.

### 6. A deployment that never adopts any of this (UC-7)

```bash
scripts/pre_setups/init_smartrouter_cv_default.sh
```

A second router on `:3399` whose config has no `cross-validation:` block, no `group-label:`
on any provider, and no thresholds:

```
[1] the startup log carries no policy layout at all
  PASS  absent, not 'policies=0' — the resolver was never populated

[2] 'eth_getBalance' — the method the demo lane MANDATES — is plain here
  PASS  one provider, one relay — nothing changed for existing callers

[3] a caller that DOES send the headers still gets cross-validation
    status                                 success
    all-providers                          sim-1,sim-2,sim-3

  UC-7 PASS — existing deployments and existing callers need zero changes.
```

The policy-layout line being *absent* rather than reading `policies=0` is the honest signal:
the resolver was never populated, so the router is on the path it ran before this feature
existed, not on the new path in a disabled state. The two configs differ by exactly the block
this page is about:

```bash
diff debugging/smartrouter_cv_default.yml debugging/smartrouter_cv_demo.yml
```

The lane also demonstrates the degenerate caller request worth knowing about:

```
[5] the degenerate caller request the docs warn about
    (max-participants 1 + agreement-threshold 1: a 'quorum' of one)
    status                                 success
    all-providers                          sim-3
```

`status: success` after a single response, having compared nothing. It is self-consistent and
so it is honored, which is why a client that needs real corroboration should read the provider
lists rather than `status` alone — and why an operator who cannot audit every caller sets a
policy floor instead.

### Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| Lane refuses: "port(s) are held by process(es) it does not own" | Another stack holds them. The lane prints a ready-made override; nothing was killed. |
| `$SIM_DIR/run.py not found` | The simulator checkout is not a sibling. Pass `SIM_DIR=/path/to/provider_simulator`. |
| A dissent shows up as *pending* instead of *disagreeing* | The quorum early-exited before the outlier answered. Give the honest providers a `latency_ms` so the outlier wins the race — that is exactly what the lane does to pick the reply-time path. |
| `finality="unknown"` on the mismatch metric | The request did not carry a resolvable block number, or the chain tracker had not learned the head yet. Query a concrete finalized block, not `latest`. |
| A cross-validated method answers without fanning out | Something served it from cache. These lanes configure no cache for that reason; if you add one, vary the request parameters. |
| Router exits at startup with a cross-validation error | Working as intended for an unsatisfiable policy — compare `min-groups` and `max-participants` against the `distinctGroups` / `groupSizes` in the startup log. |
| `/debug/cross-validation-events` returns 503 | The recorder is not installed: the router is missing `--debug-address`. A 503 is not an empty result — "nothing was recorded" and "nothing dissented" are opposite answers. |
