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
