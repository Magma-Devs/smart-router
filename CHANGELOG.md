# Changelog

All notable changes to smart-router are documented here. Each release
section is also published verbatim as the body of its GitHub release —
see [RELEASING.md](docs/RELEASING.md) for the workflow.

Versions follow [Semantic Versioning](https://semver.org/). Commit hashes
in `### Changes` link to the canonical commit on GitHub via reference-style
links collected at the bottom of each section.

## v1.5.0 — 2026-09-03

### Highlights

Release v1.5.0 introduces a two-tier caching architecture, allowing operators to configure a read-only secondary cache with exact-key backfill alongside the primary tier. To support distributed deployments, this release adds a RESP-compatible cache backend for Redis or Valkey, accompanied by outcome-aware metrics to monitor hit rates across both cache tiers. Secondary cache reads are strictly isolated by a foreign-reply sanitizer that allowlists transport headers and drops foreign chain state, such as `LatestBlock` data, preventing external state from leaking into the local router. The cache protocol now persists entry kinds like `IsNodeError` to accurately return upstream faults, and the router will issue a warning rather than aborting if it detects a dangling secondary cache configuration. Finally, WebSocket subscription tracking is corrected to key active subscriptions by their specific RPC method rather than just their parameters, preventing state collisions across identical parameter sets.

### Changes

#### New Features
- feat(cache): persist and return entry kind (IsNodeError) through the cache protocol ([#248]) [`e6a9aa1`]
- feat(performance): add read-only CacheReader seam and foreign-reply sanitizer ([#248]) [`e5315ed`]
- feat(rpcsmartrouter): two-tier cache lookup — read-only secondary with exact-key backfill ([#248]) [`55668a7`]
- feat(metrics): outcome-aware per-tier cache observability ([#248]) [`768882a`]
- feat(cache): RESP-compatible (Redis/Valkey) cache backend ([#261]) [`443fef5`]

#### Bug fixes
- fix(rpcsmartrouter): key a subscription by its method, not only its params (MAG-3378) ([#366]) [`bd64f59`]
- fix(secondary-cache): review corrections — exact-key validity, drop-all sanitize, populator-owned eligibility ([#248]) [`9483ed8`]
- refactor(cache): consolidate test helpers and improve secondary cache handling ([#248]) [`934237e`]
- fix(cache): commit the test helpers the cache suite is built against ([#248]) [`0cc19cd`]
- fix(secondary-cache): drop the foreign LatestBlock in the sanitizer ([#248]) [`6898b06`]
- fix(secondary-cache): record the tier outcome after the reply copy ([#248]) [`eefcced`]
- fix(secondary-cache): warn instead of aborting on dangling secondary config ([#248]) [`7bb4923`]
- fix(secondary-cache): allowlist transport headers instead of dropping all metadata ([#248]) [`d8f50a9`]
- fix(secondary-cache): stop foreign chain state reaching the router past the sanitizer ([#248]) [`6ee7cde`]
- fix(secondary-cache): clamp the backfill validity-floor lift to the local gated tip ([#248]) [`b694e9f`]

#### Documentation updates
- docs: add secondary cache backend design (PRD: Secondary Cache Backend) ([#248]) [`68021e8`]
- docs(cache): customer-facing secondary cache overview ([#248]) [`35030b1`]
- docs(secondary-cache): retire references to the deleted design doc ([#248]) [`5e81083`]
- docs(secondary-cache): enhance manual demo walkthrough and clarify flow steps ([#248]) [`0d6468d`]
- docs(secondary-cache): update demo instructions for block serving and cache recovery ([#248]) [`ffae317`]
- docs(secondary-cache): align the overview with the review corrections ([#248]) [`a0aaddb`]
- docs(secondary-cache): correct the tier-symmetry and scope claims ([#248]) [`69df6b4`]

#### Other work
- removed design doc ([#248]) [`f599345`]

[#248]: https://github.com/magma-Devs/smart-router/pull/248
[#261]: https://github.com/magma-Devs/smart-router/pull/261
[#366]: https://github.com/magma-Devs/smart-router/pull/366
[`0cc19cd`]: https://github.com/magma-Devs/smart-router/commit/0cc19cd403b2ef8edaec8602d93e665aa1c2c240
[`0d6468d`]: https://github.com/magma-Devs/smart-router/commit/0d6468dd4ae0ee67be8ca90109dc1321bd58934b
[`35030b1`]: https://github.com/magma-Devs/smart-router/commit/35030b130e4e718080f45393114dd380004d5181
[`443fef5`]: https://github.com/magma-Devs/smart-router/commit/443fef5bf9d30c918acf1de2a526ebf6955ea5b6
[`55668a7`]: https://github.com/magma-Devs/smart-router/commit/55668a72438deeba6f102f175df0e4a3992cb03f
[`5e81083`]: https://github.com/magma-Devs/smart-router/commit/5e81083766f5e3809262c3cd89155bd9fba0fabd
[`68021e8`]: https://github.com/magma-Devs/smart-router/commit/68021e81a55c3a3c8bc4d0993aad090ff3889b70
[`6898b06`]: https://github.com/magma-Devs/smart-router/commit/6898b0617e1338819fa898af608ec382f707fcca
[`69df6b4`]: https://github.com/magma-Devs/smart-router/commit/69df6b461c1592e6dab6e31be2bfcac607eed76d
[`6ee7cde`]: https://github.com/magma-Devs/smart-router/commit/6ee7cde0ffab0d65447b3e4044002b47063ba6a1
[`768882a`]: https://github.com/magma-Devs/smart-router/commit/768882a25579a7197c35ca18796991d662204617
[`7bb4923`]: https://github.com/magma-Devs/smart-router/commit/7bb492376b976dc1c46282796a942bc0a0524839
[`934237e`]: https://github.com/magma-Devs/smart-router/commit/934237e3c4d587cf6ef98a90f74d9f024a937ed2
[`9483ed8`]: https://github.com/magma-Devs/smart-router/commit/9483ed8d0d6220adf5fc425d92443111847ada76
[`a0aaddb`]: https://github.com/magma-Devs/smart-router/commit/a0aaddbeae10b1ed74b3a5eddc599a4e390cbe68
[`b694e9f`]: https://github.com/magma-Devs/smart-router/commit/b694e9f93d41dd3e5f6c4f82f802a666a9e4648c
[`bd64f59`]: https://github.com/magma-Devs/smart-router/commit/bd64f59b504fbf97833cd2763ab4757b0bfe2e00
[`d8f50a9`]: https://github.com/magma-Devs/smart-router/commit/d8f50a9e5fe1c99055bf5eeedd4f10b8cf7a1bf9
[`e5315ed`]: https://github.com/magma-Devs/smart-router/commit/e5315ed97c3f7a10fc065f8c2793515ed7669734
[`e6a9aa1`]: https://github.com/magma-Devs/smart-router/commit/e6a9aa1475b5764c74fda68bacee91ff892626f5
[`eefcced`]: https://github.com/magma-Devs/smart-router/commit/eefccede6d190a141986cf6f5231aee9c22e3631
[`f599345`]: https://github.com/magma-Devs/smart-router/commit/f59934531c5face2c4a98bee902c18354b735c89
[`ffae317`]: https://github.com/magma-Devs/smart-router/commit/ffae3173cf1bb9db16a960a5127b5351fc0031e2

## v1.4.1 — 2026-09-02

### Highlights

Release v1.4.1 introduces new upstream routing controls, adding a best-score provider selection mode and `--qos-selection-priority` weight presets to tune how traffic is distributed. Observability is expanded to diagnose routing decisions; `INFO` logs now explicitly report why specific providers are blocked or excluded, which node served a given relay, and why a pairing pool is empty. For complex deployments, node URLs can now declare standalone add-ons, ensuring base-collection traffic is kept off specialized endpoints while allowing the router to admit a provider per collection at boot. WebSocket subscription lifecycles are now strictly managed; the router unsubscribes using the exact method the client called, correctly names subscription pushes after their payload, and actively tells the upstream node to stop pushing data when a client disconnects. Finally, this release secures operational outputs by redacting node-url credentials and header values from all logs, bounds gRPC reflection with `reflection-timeout`, and prevents whole-body null replies from crashing the router process.

### Changes

#### New Features
- feat(skills): add the can-i-merge ship-check skill (MAG-3110) ([#334]) [`ab640f5`]
- feat(observability): surface routing exclusion reasons at INFO ([#335]) [`0be8ce8`]
- feat(provideroptimizer): add best-score provider selection mode ([#262]) [`2f36ffe`]
- feat(provideroptimizer): add --qos-selection-priority weight presets ([#262]) [`4c3a5a6`]
- feat(observability): record and surface why a provider is blocked (MAG-2599) ([#336]) [`563161d`]
- feat(config): let a node url declare that its add-ons stand alone (MAG-3296) ([#345]) [`610cd1f`]
- feat(chainlib): admit a provider per collection at boot (MAG-3326) ([#358]) [`477859f`]
- feat(observability): say who served the relay and why it stopped (MAG-3331) ([#362]) [`1563c01`]
- feat(observability): report the pairing inventory and why the pool is empty (MAG-3331) ([#351]) [`3975c5a`]

#### Bug fixes
- fix(skills): restore the HEAD-prefix strip in the gate 5 caller scan (MAG-3110) ([#334]) [`980af8a`]
- fix(skills): escape the table pipe, note that the gate 5 helper needs re-pasting (MAG-3110) ([#334]) [`0b60f8b`]
- fix(observability): make the promoted lines carry their answer ([#335]) [`2a01cd8`]
- fix(metrics): report the providers that are actually blocked ([#332]) [`d395ec2`]
- refactor(provideroptimizer): rename WeightedSelector to ProviderSelector ([#262]) [`38543be`]
- refactor(provideroptimizer): rename the selector to UpstreamSelector ([#262]) [`35b39f1`]
- fix(rpcsmartrouter): finish the ChooseUpstream rename in the MAG-2156 test ([#262]) [`d5fe0b1`]
- fix(provideroptimizer): make the Best-mode tie-break deterministic ([#262]) [`e054e91`]
- fix(provideroptimizer): keep Best deterministic when every score is zero ([#262]) [`4fe83e1`]
- fix(rpcsmartrouter): normalize the selection weights before the selector sees them ([#262]) [`91fa76f`]
- fix(observability): Reported must mean actually reported, not requested (MAG-2599) ([#336]) [`9c42bf9`]
- fix(lavasession): keep the block record in step with both blocked stores (MAG-2599) ([#336]) [`6ce74b2`]
- fix(lavasession): announce every block that ends, not only the last one ([#336]) [`9068e1c`]
- fix(lavasession): keep the record true on repeat blocks, backups and carry-over ([#336]) [`53b0938`]
- refactor: the binary's home directory is ~/.smart-router, not ~/.lava ([#342]) [`bf61811`]
- fix(chainlib): probe the head with the collection a node serves (MAG-3296) ([#345]) [`fadbefd`]
- fix(chainlib): let a verification borrow from its own collection (MAG-3296) ([#345]) [`681c472`]
- fix(lavasession): keep base-collection traffic off a standalone-addons endpoint (MAG-3296) ([#345]) [`445e184`]
- fix(rpcsmartrouter): stop standalone-addons making an endpoint serve nothing ([#345]) [`263facb`]
- fix(security): redact node-url credentials before they leave the process ([#352]) [`bdaaf8d`]
- fix(security): withhold credential header values from logs ([#352]) [`b9fb24b`]
- fix(security): redact scheme-less node URLs ([#352]) [`4aecfc6`]
- fix(ci): restore PR gate until chart 6 migration ([#356]) [`a34d51d`]
- fix(rpcsmartrouter): unsubscribe with the method the client called (MAG-3297) ([#344]) [`7ae1f10`]
- fix(rpcsmartrouter): guard the synthesised api name, and test what goes upstream ([#344]) [`b7943bd`]
- fix(chainlib): bound gRPC reflection with the configured reflection-timeout ([#363]) [`9029c01`]
- fix(rpcsmartrouter): relay subscription pushes named after their payload (MAG-3345) ([#360]) [`f431d4f`]
- fix(ci): complete chart 6 PR gate migration ([#364]) [`74469a7`]
- fix(lavasession): make the epoch's probe branch reachable again (MAG-3190) ([#348]) [`3619691`]
- fix(ws): tell the node to stop pushing when a client disconnects ([#365]) [`46b0d78`]
- fix(chainlib): stop a whole-body null reply from ending the router process (MAG-3077) ([#333]) [`28a838d`]

#### Documentation updates
- docs(changelog): rewrite the v1.4.0 highlights ([#329]) [`a37ce92`]
- docs(cross-validation): state that a cap bounds caller strictness (MAG-3035) ([#331]) [`0659251`]
- docs(rpcsmartrouter): reattach the routerConfigOptimizerWeights doc comment ([#262]) [`3031370`]

#### Build process updates
- ci: the PR-gate DEV_MODE selector resolves the router pod label at runtime ([#347]) [`eb1c4ed`]
- ci: select router pods by app.smart-router-id, no old-key fallback ([#347]) [`8068928`]
- ci: the PR-gate targets the smart-router namespace, not lava-infra ([#347]) [`2b252df`]

[#262]: https://github.com/magma-Devs/smart-router/pull/262
[#329]: https://github.com/magma-Devs/smart-router/pull/329
[#331]: https://github.com/magma-Devs/smart-router/pull/331
[#332]: https://github.com/magma-Devs/smart-router/pull/332
[#333]: https://github.com/magma-Devs/smart-router/pull/333
[#334]: https://github.com/magma-Devs/smart-router/pull/334
[#335]: https://github.com/magma-Devs/smart-router/pull/335
[#336]: https://github.com/magma-Devs/smart-router/pull/336
[#342]: https://github.com/magma-Devs/smart-router/pull/342
[#344]: https://github.com/magma-Devs/smart-router/pull/344
[#345]: https://github.com/magma-Devs/smart-router/pull/345
[#347]: https://github.com/magma-Devs/smart-router/pull/347
[#348]: https://github.com/magma-Devs/smart-router/pull/348
[#351]: https://github.com/magma-Devs/smart-router/pull/351
[#352]: https://github.com/magma-Devs/smart-router/pull/352
[#356]: https://github.com/magma-Devs/smart-router/pull/356
[#358]: https://github.com/magma-Devs/smart-router/pull/358
[#360]: https://github.com/magma-Devs/smart-router/pull/360
[#362]: https://github.com/magma-Devs/smart-router/pull/362
[#363]: https://github.com/magma-Devs/smart-router/pull/363
[#364]: https://github.com/magma-Devs/smart-router/pull/364
[#365]: https://github.com/magma-Devs/smart-router/pull/365
[`0659251`]: https://github.com/magma-Devs/smart-router/commit/0659251cf5da1fec546e2f8bb18f1882f44c21ab
[`0b60f8b`]: https://github.com/magma-Devs/smart-router/commit/0b60f8bf472e6c9b14f41c5454f51080c08c5d71
[`0be8ce8`]: https://github.com/magma-Devs/smart-router/commit/0be8ce86dca698825088159baa302cd7b91939e7
[`1563c01`]: https://github.com/magma-Devs/smart-router/commit/1563c01aa7e4ea57d569af0ebe4a10c57d823a73
[`263facb`]: https://github.com/magma-Devs/smart-router/commit/263facbb74043ece89c990efbe3c2afdab9c9557
[`28a838d`]: https://github.com/magma-Devs/smart-router/commit/28a838dc7955c7be8dbdf8540e2b8fe282f1a504
[`2a01cd8`]: https://github.com/magma-Devs/smart-router/commit/2a01cd8d3e02678f6f44a04c6c0545240a98ef51
[`2b252df`]: https://github.com/magma-Devs/smart-router/commit/2b252dfc34748a51830c2a07220a8ab141d4481b
[`2f36ffe`]: https://github.com/magma-Devs/smart-router/commit/2f36ffe66de2627ef9b6a2031cfb7f65a1ebb8c0
[`3031370`]: https://github.com/magma-Devs/smart-router/commit/303137013beb401f9734b03684c2ebc4730bafb0
[`35b39f1`]: https://github.com/magma-Devs/smart-router/commit/35b39f13d6b6af2c5ba46489a3905cee3fd73659
[`3619691`]: https://github.com/magma-Devs/smart-router/commit/36196910aae9ae92c50631c67a4a25900f0f7b96
[`38543be`]: https://github.com/magma-Devs/smart-router/commit/38543bef05ab42d372721e13e504d3613106b5ac
[`3975c5a`]: https://github.com/magma-Devs/smart-router/commit/3975c5a220ce0f0b4c0f8d065126fda5d83a99c7
[`445e184`]: https://github.com/magma-Devs/smart-router/commit/445e184ef3d57d0ee57b4a99157cf9943110e6f2
[`46b0d78`]: https://github.com/magma-Devs/smart-router/commit/46b0d786139a8ff293ea5af356c04777bbb9079c
[`477859f`]: https://github.com/magma-Devs/smart-router/commit/477859fbffc47b0c10e419f9f0fecf55b7368396
[`4aecfc6`]: https://github.com/magma-Devs/smart-router/commit/4aecfc6bc85ddd4db9715625802872ba3173f539
[`4c3a5a6`]: https://github.com/magma-Devs/smart-router/commit/4c3a5a6428cfb92c677ea9b6f11ff40e4f93b60e
[`4fe83e1`]: https://github.com/magma-Devs/smart-router/commit/4fe83e1ef2a4b911fc59306acf514cabed4f91ee
[`53b0938`]: https://github.com/magma-Devs/smart-router/commit/53b0938d03a6b97e78d7b159db95db3ad9b5ea45
[`563161d`]: https://github.com/magma-Devs/smart-router/commit/563161db677b8e30ed9075d7cdcfa9b4cf2af6de
[`610cd1f`]: https://github.com/magma-Devs/smart-router/commit/610cd1f20ca6083bd58f2b7fa91c3a16fcd4f776
[`681c472`]: https://github.com/magma-Devs/smart-router/commit/681c472ddf809d87eeeaefcea77270aa6bc530b4
[`6ce74b2`]: https://github.com/magma-Devs/smart-router/commit/6ce74b28e7842636bf243881ff832c9a06dcc6d3
[`74469a7`]: https://github.com/magma-Devs/smart-router/commit/74469a717289624672a6a822dc75e7d27993d918
[`7ae1f10`]: https://github.com/magma-Devs/smart-router/commit/7ae1f100d05bebb8cc0d09ef801a1752d07363fd
[`8068928`]: https://github.com/magma-Devs/smart-router/commit/806892826d0bb0564694629211d41df6c16510a1
[`9029c01`]: https://github.com/magma-Devs/smart-router/commit/9029c0189da90dc32fee87a94cf1516069783e6e
[`9068e1c`]: https://github.com/magma-Devs/smart-router/commit/9068e1cd45eb55df64783a5a441d1a84ac8f1bc8
[`91fa76f`]: https://github.com/magma-Devs/smart-router/commit/91fa76fa12512ce1dfb3295d546a038c8384c20f
[`980af8a`]: https://github.com/magma-Devs/smart-router/commit/980af8aa4d75765b796a7ce5b8098a599c4f0387
[`9c42bf9`]: https://github.com/magma-Devs/smart-router/commit/9c42bf9670c4c09246d60cd1a855e271fcf61e2c
[`a34d51d`]: https://github.com/magma-Devs/smart-router/commit/a34d51da42e0068ed6a7740fddceef8a4de0243e
[`a37ce92`]: https://github.com/magma-Devs/smart-router/commit/a37ce925cd74598165e3892152152c80faa2c43e
[`ab640f5`]: https://github.com/magma-Devs/smart-router/commit/ab640f5e71ee6099d15ebc60a1a668c1179bc637
[`b7943bd`]: https://github.com/magma-Devs/smart-router/commit/b7943bd4550d3f90436344ca99a144b8cbf29e36
[`b9fb24b`]: https://github.com/magma-Devs/smart-router/commit/b9fb24bd25384e512be477d8ed6c8927016d8ab7
[`bdaaf8d`]: https://github.com/magma-Devs/smart-router/commit/bdaaf8d7cf15ea572b49c250426915bf01afaf29
[`bf61811`]: https://github.com/magma-Devs/smart-router/commit/bf618114905da76362be9e7e2657dccb2b2526bf
[`d395ec2`]: https://github.com/magma-Devs/smart-router/commit/d395ec2df9cfcd5c78ad2292580a8d31e39a1f00
[`d5fe0b1`]: https://github.com/magma-Devs/smart-router/commit/d5fe0b1f8ddd5c852ef2de60f35a96bce09b7cab
[`e054e91`]: https://github.com/magma-Devs/smart-router/commit/e054e9130708d8dc5c7f6176237db48f6248951c
[`eb1c4ed`]: https://github.com/magma-Devs/smart-router/commit/eb1c4ed94a0d54a802d78b4186d5588328b595e5
[`f431d4f`]: https://github.com/magma-Devs/smart-router/commit/f431d4f9b30ac231e3381ae86f137dfda6c56379
[`fadbefd`]: https://github.com/magma-Devs/smart-router/commit/fadbefd8c3c263de4d3975ae3a4bf9b06236eb33

## v1.4.0 — 2026-08-24

### Highlights

v1.4.0 is a stability and upstream-load release. The router now acts on what a rate-limited upstream tells it, polls customer nodes a fraction as often, serves gRPC server-streaming end to end, and survives several boot and config conditions that used to take a pod down. 103 commits across 45 pull requests.

**Upgrade notes — read before rolling out**

- **`--chain-tracker-polling-multiplier` is removed** ([#309]). It has been a no-op since the global chain tracker went away; the router now rejects it at startup with `unknown flag`. Remove it from hand-rolled launch args (systemd units, compose files, scripts). Helm users are covered by chart 5.19.0 or later, which no longer renders it. The live replacements are `--chain-tracker-poll-divisor` and `--enable-fork-detection`.
- **Two providers sharing one name refuse to boot** ([#275]). A provider's `name` is its routing identity, so two nodes sharing one on the same chain and api-interface collapsed into a single entry and served at half capacity. The router now exits with a message naming every collision; `smartrouter health` still loads such a config and warns. Reusing a name across chains stays legal.
- **Block-hash polling (fork detection) is off by default** ([#307]). It was the tracker's largest source of upstream requests, and nothing in the router consumed its result. `--enable-fork-detection` turns it back on. `/debug/endpoint-state` reports the live state as `HashPolling`, distinguishing `off-operator-choice` from `off-spec-no-block-by-num`.
- **Internal-path routing** ([#297]). On chains whose spec declares `internal-path` collections (AVAX / AVALANCHE C, P and X chains, MONERO, TON v2/v3, STRK versioned RPC), a relay now dials the node-url that serves the api's path instead of whichever endpoint selection picked. A url that already ends in the path is that path's endpoint. Chains with no internal paths are untouched.
- **`lava-select-provider` error sentinels split** ([#286]). `SelectedProviderUnavailableError` now reads "…is not a valid provider for this request"; the already-failed case is `SelectedProviderAlreadyFailedError`. Anything matching the old sentinel text needs updating. The client-facing descriptions are unchanged.
- **A per-node-url `timeout:` on a gRPC node is now honoured** ([#293]); it previously read as zero on the gRPC provider path.

**Rate limits: the router backs off when told to** ([#313], [#314], [#316], [#317], [#318], [#319], [#320], [#321], [#325])

Until now the router parsed `Retry-After` and then discarded it: a 429 counted as an availability failure, demoted providers that were merely busy, and was re-asked at the same cadence — often by every pod at once. This release adds a shared, tiered hold-off registry (`protocol/holdoff`, documented in `docs/RATE-LIMIT-HOLDOFF.md`) and wires every upstream-facing path into it.

- A 429 holds off the URL that returned it, with the upstream's `Retry-After` as the floor (otherwise 30s doubling per strike, capped at 30m, plus up to 20% jitter, never more than 1h). Two held-off URLs of one provider escalate to a provider-wide hold-off, because vendor caps are account-wide. Any answered request clears it.
- Rate limits are recognised on every transport: HTTP status, rate-limit texts inside a 2xx body, WebSocket upgrade rejections, and gRPC (`RESOURCE_EXHAUSTED` backed by a pushback delay, or the known rate-limit texts a vendor's HTTP edge leaves inside `Unavailable`).
- A rate-limited relay is neither a failure nor a success: no QoS sample, no health verdict, and selection prefers providers that are not held off. Spec re-verification treats a 429 as inconclusive instead of a demotion, and `Validate` ends its three-attempt retry burst on the first 429.
- Stateful relays and batches retry on a rate limit — the upstream refused before executing anything — and a chain whose every attempt was refused answers 503 instead of 500. `Retry-After` is never surfaced to the client.
- The chain tracker floors its poll backoff with `Retry-After`; WebSocket subscription reconnects and gRPC streaming selection skip held-off endpoints.
- New metrics: `smartrouter_rate_limit_holdoffs_total{provider, event="recorded|escalated|cleared"}` and `smartrouter_rate_limit_holdoff_seconds{provider}`.

**Tracker load on customer nodes**

- `--enable-fork-detection` (default off) removes the block-hash request from every tracker tick: 87.6 → 7.3 requests/min per endpoint measured on a 15s chain, 65–82% fewer on EVM chains ([#307]).
- `--chain-tracker-poll-divisor` makes the per-endpoint poll cadence configurable as a ratio of block time, `avgBlockTime/divisor`, range `[0.25, 8]`, default `2` (unchanged). At `0.25` the tracker polls once per four block times. The router warns once per chain at startup when the chosen cadence combined with the traffic gate's skip budget could outrun the staleness window ([#308], [#323]).
- Fleet tracker gate: with `--shared-state` and `--cache-be`, every successful poll is published to the cache backend, and a pod skips its own poll when a peer polled the endpoint within the freshness window, adopting the peer's block as a `peer` tip source. Fleet-wide this converges to about one real poll per interval; each pod still polls locally every fifth tick, so pod-local faults stay detectable. `rpc_endpoint_tracker_gate_skips_total{source="relay"|"peer"}` shows it working ([#322]).
- `rpc_endpoint_tracker_requests_total{kind="latest_block"|"block_hash"}` counts what the tracker actually sends ([#307]).

**gRPC**

- Server-streaming methods are served end to end. Streaming is decided from the spec's `SUBSCRIBE` directive rather than a reflection lookup, a shared upstream stream survives the first client's disconnect, and a joining client gets its own subscription id ([#292]).
- The gRPC connector has a coherent lifecycle: no nil-connector panic after `Close`, no per-relay teardown, a failed initialisation is retried instead of poisoning the endpoint, and pool capacity is per instance rather than a racy process-wide global ([#289], [#294]).
- gRPC status errors carry their real code, so `INVALID_ARGUMENT` and `NOT_FOUND` replies are no longer recorded (and cached) as successes, and a relay the router itself cancelled is no longer booked against endpoint health with a latency sample that was never taken ([#288]).
- A reconnect no longer tears down its own subscription and double-decrements the subscription count ([#290]).
- Descriptor lookups run on grpc-config's `reflection-timeout`, rooted at the connection's lifetime, and connections are prewarmed before the endpoint is published — cross-validation over gRPC can reach quorum against a slow-reflection endpoint ([#300]). `descriptor-source: file|hybrid` with `descriptor-set-path` is consulted on every request path, so chains whose nodes serve no reflection can boot ([#293]).
- `grpcs://` dial addresses are normalised on the pool-refill path, which previously never grew the pool ([#295]). Block extraction is capped at 32 MB on both transports, above any legal block ([#303]).

**Boot and configuration**

- The router boots on any usable provider instead of exiting when every primary fails verification: healthy backups serve (degraded), and an all-dark chain boots, returns 5xx, and retries from about 2s. The new gauge `smartrouter_endpoint_serving_tier` (2 = primaries, 1 = backups only, 0 = dark) replaces the crash as the signal to alert on ([#265]).
- `smartrouter /abs/path/config.yml` works: an argument that names a path is resolved as a path, and a bare name still searches `.`, `./config` and `~/.lava` ([#298]).
- A self-contradictory cross-validation policy is rejected before any provider is dialed rather than one line after `RPCSmartRouter Listening` ([#327]), and a `lava-cross-validation-max-participants` header above 50 no longer ends the process — it is refused against the live endpoint count ([#326]).
- `skip-verifications` actually skips: it still fired the latest-block probe on a skipped verification's behalf, which demoted providers on a 429. `skip-verifications: ["*"]` skips everything for one node-url, and `--skip-all-verifications` does so process-wide — the flag is for getting a router up now, the wildcard for anything ongoing ([#296]).
- The blocked-provider list is released only after primary and backup are both exhausted, so "every primary is blocked" serves the backup tier instead of a just-blocked primary ([#328]). `lava-select-provider` matches case-insensitively and returns the canonical name ([#285]).

**REST routing and metric cardinality**

- REST api matching ignores a trailing slash on either side and picks the most specific api when several match a path. 43% of live TEZOS traffic was falling through to a synthetic `Default-*` api at a flat 20 CU with block parsing pinned to latest ([#312], [#315]).
- Unmatched `Default-*` method labels fold concrete ids to a shape (`/blocks/{}/header`) and are capped at 32 per spec, with `smartrouter_default_method_overflow_total{spec}` when the cap binds. The `method` label that had grown to 46k values is bounded ([#269]).

**Operator visibility**

- `GET /debug/cross-validation-events` is a per-request record of cross-validation dissent — reply-time outliers and straggler resolutions — with the `lava-guid`, provider, both hashes, and whether the row is the one that moved `smartrouter_cross_validation_mismatch_total`. Debug mode only (`--debug-address`), bounded ring of 4096 ([#284]).
- The cache write for `earliest`, `pending`, `safe` and `finalized` block tags is skipped instead of failing with a warning on every relay ([#291]).
- Go 1.26.6 clears six stdlib advisories; dependabot is exempt from the Jira gate and OpenTelemetry bumps move as one group ([#276], [#277]).

### Changes

#### ⚠ Breaking changes
- refactor!: remove the dead --chain-tracker-polling-multiplier flag ([#309]) [`e1d71b9`]

#### New Features
- feat(smart-router/debug): add GET /debug/cross-validation-events (MAG-2772) ([#284]) [`290555e`]
- feat(chaintracker): make fork detection opt-in behind a flag (MAG-2920) ([#307]) [`04a8139`]
- feat(smart-router): make the per-endpoint poll cadence configurable ([#308]) [`55f3294`]
- feat(smart-router): capture Retry-After from a rate-limited upstream ([#306]) [`1c2db0a`]
- feat(smart-router): add --skip-all-verifications, mirroring --skip-websocket-verification ([#296]) [`8261326`]
- feat(smart-router): shared tiered rate-limit hold-off registry ([#316]) [`9edd4a6`]
- feat(holdoff): add NewRegistryWithClock for deterministic consumer tests ([#316]) [`069ea42`]
- feat(holdoff): expose the process-wide Shared registry ([#316]) [`b84f30e`]
- feat(holdoff): provider-level ProviderReadyAt query for selection ([#316]) [`534962d`]
- feat(smart-router): recognize 429s on ws + gRPC, floor the tracker poll with Retry-After ([#318]) [`9e12e26`]
- feat(smart-router): expose rate-limit hold-off events as metrics ([#321]) [`50d3360`]
- feat(smart-router): allow a poll cadence slower than the chain's block time (MAG-2985) ([#323]) [`5d60293`]
- feat(smart-router): log the resolved poll cadence unconditionally (MAG-2985) ([#323]) [`5e3bb1e`]
- feat(smart-router): fleet tracker gate — share per-endpoint poll observations across pods (MAG-2981) ([#322]) [`95d26e5`]
- feat(smart-router): publish every first-hand endpoint observation to the fleet store (MAG-2981) ([#322]) [`8a0b6be`]
- feat(smart-router): serve gRPC server-streaming methods (MAG-2643) ([#292]) [`df08017`]

#### Bug fixes
- fix(metrics): move the otel log sink onto attribute.KeyValue ([#277]) [`433293e`]
- fix(smart-router): boot on any usable provider instead of exiting (MAG-2525) ([#265]) [`b16fd25`]
- fix(smart-router): keep subscription tiers in step with the live pairing (MAG-2525) ([#265]) [`6c6a0f5`]
- fix(smart-router): refuse to start when two providers share a name (MAG-2724) ([#275]) [`58be84d`]
- fix(lavasession): match lava-select-provider case-insensitively ([#285]) [`b63f66b`]
- fix(lavasession): resolve the pinned provider before consulting the ignored set ([#285]) [`7f0aede`]
- fix(rpcsmartrouter): score retryable node errors against availability (MAG-2156) ([#283]) [`341897e`]
- fix(rpcsmartrouter): carve rate limits out of availability scoring, test the gate (MAG-2156) ([#283]) [`0adbc7c`]
- fix(smart-router): stop gRPC reconnect from tearing down its own subscription ([#290]) [`c7f58ec`]
- fix(smart-router): make gRPC subscription cleanup idempotent and single-sited ([#290]) [`4910d8e`]
- fix(smart-router): make gRPC cleanup release resources, not just bookkeeping ([#290]) [`ec7c0a1`]
- fix(smart-router): classify gRPC status errors and cancellations correctly ([#288]) [`94c760c`]
- fix(smart-router): tighten gRPC cancellation guard and pin cross-transport reach ([#288]) [`92af4b0`]
- fix(smart-router): keep the rpc-error guard broad, classify gRPC InvalidArgument (MAG-2549) ([#288]) [`df0e8e3`]
- fix(smart-router): keep gRPC data-scope codes out of endpoint scoring (MAG-2549) ([#288]) [`eeddfc1`]
- fix(smart-router): guard gRPC connector against use after Close ([#289]) [`f5348da`]
- fix(smart-router): make gRPC connector teardown a lifetime, not a flag ([#289]) [`3db6209`]
- fix(smart-router): own the gRPC connector's context, not the first relay's ([#289]) [`04896aa`]
- fix(smart-router): retry a failed gRPC initialization instead of latching it ([#289]) [`083605d`]
- fix(smart-router): split the gRPC connector's lifetime from its dial deadline ([#289]) [`4127703`]
- fix(smart-router): skip the cache write for unresolved block tags (MAG-2842) ([#291]) [`b043ad1`]
- fix(smart-router): consult grpc-config's descriptor-source on every request path ([#293]) [`1f6c93e`]
- fix(smart-router): close the three gaps review found in descriptor-source wiring ([#293]) [`e9f7c66`]
- fix(smart-router): scope gRPC connector pool capacity per instance, and pin MAG-2538 parser isolation ([#294]) [`d03d1a4`]
- fix(smart-router): scope the health probe's ws gate to the endpoint (MAG-2333) ([#295]) [`54e828a`]
- fix(smart-router): normalize the dial address on every gRPC connection path (MAG-2333) ([#295]) [`54bcd90`]
- fix(smart-router): report a real latest block on passing health legs (MAG-2333) ([#295]) [`af87fcd`]
- fix(smart-router): stop backfilling a height onto failed health legs (MAG-2333) ([#295]) [`24e4606`]
- fix(smart-router): harden the Retry-After capture and cover its call sites ([#306]) [`117d8e9`]
- fix(smart-router): stop the relay timeout from bounding gRPC descriptor lookups ([#300]) [`2804602`]
- fix(smart-router): prewarm gRPC descriptors before an endpoint is published ([#300]) [`3dfabba`]
- refactor(lavasession): drop the unreachable gRPC compression parameter ([#311]) [`a2fc17e`]
- fix(chainlib): make skip-verifications actually skip, and add a skip-all wildcard ([#296]) [`ddb8052`]
- refactor!: remove the dead --chain-tracker-polling-multiplier flag ([#309]) [`e1d71b9`]
- fix(smart-router): dial the upstream that serves the api's internal path ([#297]) [`a7c7811`]
- fix(smart-router): match the internal path exactly, and give probes their own sentinel ([#297]) [`444be5e`]
- fix(smart-router): don't append a collection LABEL to a url, and don't emit a url twice ([#297]) [`e3bb5f2`]
- fix(smart-router): a url that already ends in the path is that path's endpoint ([#297]) [`dbe4066`]
- fix(smart-router): generate a path only on the transport that serves it ([#297]) [`ca7f24e`]
- fix(lavasession): split the two lava-select-provider failures apart ([#286]) [`2debe75`]
- fix(lavasession): keep the unavailable-provider message true for every cause ([#286]) [`de5cec0`]
- fix(chainlib): treat a trailing slash as optional when matching REST apis ([#312]) [`1d26b2c`]
- fix(chainlib): pick the most specific api when several match a REST path ([#312]) [`0d822a0`]
- refactor(chainlib): require a precompiled REST matcher, and correct the tie-break note ([#312]) [`74e0007`]
- fix(smart-router): cap response size on the gRPC block-extraction path (MAG-2557) ([#303]) [`8b078a8`]
- fix(smart-router): raise the gRPC block-extraction cap above the block ceiling ([#303]) [`c5605b8`]
- fix(smart-router): one block-extraction cap, sized above every legal block ([#303]) [`f5fc1b0`]
- fix(smart-router): keep typed 429 errors readable through every wrap ([#313]) [`55754d7`]
- fix(smart-router): resolve a config argument that is a path as a path ([#298]) [`2932340`]
- fix(smart-router): restore the fmt import and split path intent from name lookup ([#298]) [`077f9bf`]
- fix(smart-router): keep the search paths for relative config paths ([#298]) [`64e32fe`]
- fix(smart-router): mirror viper's suffix order and report the search that ran ([#298]) [`6e3360b`]
- fix(metrics): bound the method label for unmatched (Default-*) apis ([#269]) [`52f3f18`]
- fix(metrics): collapse concrete ids in unmatched paths to a shape ([#269]) [`e44526f`]
- fix(metrics): route the straggler metric through the method-label funnel ([#269]) [`5509480`]
- fix(metrics): raise minOpaqueIDLen to 43 so route segments never collapse ([#269]) [`8f0f762`]
- fix(smart-router): a rate-limited re-verify is inconclusive, and held off ([#316]) [`737454f`]
- fix(smart-router): a rate-limited relay is neither failure nor success, and holds off ([#317]) [`49f3fdd`]
- fix(holdoff): enforce expiry and retry-after bounds ([#314]) [`ddd0245`]
- fix(smart-router): a rate-limited Validate attempt ends the retry burst ([#319]) [`fa3304d`]
- fix(smart-router): ws subscriptions hold off a rate-limited endpoint instead of scoring it ([#320]) [`21cc863`]
- fix(smart-router): validate the cross-validation config before the router boots (MAG-3022) ([#327]) [`153072c`]
- fix(smart-router): a cross-validation header can no longer stop the router ([#326]) [`62889e4`]
- fix(smart-router): borrow a peer poll observation only while the local poll is healthy (MAG-2981) ([#322]) [`3f128a0`]
- fix(smart-router): bound the fleet gate's cache read to its own cadence, and report its failures (MAG-2981) ([#322]) [`5a5e2b6`]
- fix(smart-router): surface an out-of-date cache backend in the gate error counter (MAG-2981) ([#322]) [`90d16e1`]
- fix(smart-router): retry stateful and batch relays on a rate limit, answer 503 when fully capped ([#325]) [`cfb7ec1`]
- fix(smart-router): keep the rate-limit retry carve-out off the ticker hedge, stamp 503 on a copy ([#325]) [`3d7b79f`]
- refactor(chainlib): drop the provider-relay subscription plumbing (MAG-2643) ([#292]) [`45b4694`]
- fix(smart-router): send the initial request on an empty gRPC subscribe (MAG-2643) ([#292]) [`e7befa7`]
- fix(smart-router): read the SUBSCRIBE directive, not category.subscription (MAG-2643) ([#292]) [`c663849`]
- fix(scripts): make the Sui pre-setup actually boot the router ([#292]) [`fea7af2`]
- fix(smart-router): make the gRPC rate limiter goroutine-safe (MAG-2643) ([#292]) [`d3c6695`]
- fix(smart-router): close three teardown races in the gRPC manager (MAG-2643) ([#292]) [`9ffd186`]
- fix(smart-router): keep transport-owned headers off gRPC streams (MAG-2643) ([#292]) [`226ec20`]
- fix(lavasession): release the blocked provider list only after backup fails ([#328]) [`5f38402`]
- fix(lavasession): do not release the blocked list a second time in one relay ([#328]) [`dd857f8`]

#### Documentation updates
- docs(smart-router): fix stale and orphaned comments from the MAG-2525 diff ([#265]) [`4ad6411`]
- docs(metrics): catalog the rate-limit hold-off pair in METRICS.md ([#321]) [`d614bc4`]
- docs(holdoff): mark the tracker and ws/gRPC consumer rows live ([#324]) [`d612cdc`]
- docs(metrics): correct the gate-errors attribution table (MAG-2981) ([#322]) [`498c298`]
- docs(scripts): correct the Sui cross-validation section ([#292]) [`b8197dc`]

#### Build process updates
- ci: exempt dependabot from the Jira gate and group the otel bumps ([#276]) [`4b94712`]
- build: move the go directive to 1.26.6 to clear six stdlib advisories ([#276]) [`5515103`]
- ci: strip retired periodic-probe args in PR gate ([#266]) [`be5548d`]
- ci(pr-gate): strip --chain-tracker-polling-multiplier from live router args ([#309]) [`93f3156`]

#### Other work
- perf(chainlib): skip the slash-insensitive match when neither side has a slash ([#315]) [`09fa4d6`]

[#265]: https://github.com/magma-Devs/smart-router/pull/265
[#266]: https://github.com/magma-Devs/smart-router/pull/266
[#269]: https://github.com/magma-Devs/smart-router/pull/269
[#275]: https://github.com/magma-Devs/smart-router/pull/275
[#276]: https://github.com/magma-Devs/smart-router/pull/276
[#277]: https://github.com/magma-Devs/smart-router/pull/277
[#283]: https://github.com/magma-Devs/smart-router/pull/283
[#284]: https://github.com/magma-Devs/smart-router/pull/284
[#285]: https://github.com/magma-Devs/smart-router/pull/285
[#286]: https://github.com/magma-Devs/smart-router/pull/286
[#288]: https://github.com/magma-Devs/smart-router/pull/288
[#289]: https://github.com/magma-Devs/smart-router/pull/289
[#290]: https://github.com/magma-Devs/smart-router/pull/290
[#291]: https://github.com/magma-Devs/smart-router/pull/291
[#292]: https://github.com/magma-Devs/smart-router/pull/292
[#293]: https://github.com/magma-Devs/smart-router/pull/293
[#294]: https://github.com/magma-Devs/smart-router/pull/294
[#295]: https://github.com/magma-Devs/smart-router/pull/295
[#296]: https://github.com/magma-Devs/smart-router/pull/296
[#297]: https://github.com/magma-Devs/smart-router/pull/297
[#298]: https://github.com/magma-Devs/smart-router/pull/298
[#300]: https://github.com/magma-Devs/smart-router/pull/300
[#303]: https://github.com/magma-Devs/smart-router/pull/303
[#306]: https://github.com/magma-Devs/smart-router/pull/306
[#307]: https://github.com/magma-Devs/smart-router/pull/307
[#308]: https://github.com/magma-Devs/smart-router/pull/308
[#309]: https://github.com/magma-Devs/smart-router/pull/309
[#311]: https://github.com/magma-Devs/smart-router/pull/311
[#312]: https://github.com/magma-Devs/smart-router/pull/312
[#313]: https://github.com/magma-Devs/smart-router/pull/313
[#314]: https://github.com/magma-Devs/smart-router/pull/314
[#315]: https://github.com/magma-Devs/smart-router/pull/315
[#316]: https://github.com/magma-Devs/smart-router/pull/316
[#317]: https://github.com/magma-Devs/smart-router/pull/317
[#318]: https://github.com/magma-Devs/smart-router/pull/318
[#319]: https://github.com/magma-Devs/smart-router/pull/319
[#320]: https://github.com/magma-Devs/smart-router/pull/320
[#321]: https://github.com/magma-Devs/smart-router/pull/321
[#322]: https://github.com/magma-Devs/smart-router/pull/322
[#323]: https://github.com/magma-Devs/smart-router/pull/323
[#324]: https://github.com/magma-Devs/smart-router/pull/324
[#325]: https://github.com/magma-Devs/smart-router/pull/325
[#326]: https://github.com/magma-Devs/smart-router/pull/326
[#327]: https://github.com/magma-Devs/smart-router/pull/327
[#328]: https://github.com/magma-Devs/smart-router/pull/328
[`04896aa`]: https://github.com/magma-Devs/smart-router/commit/04896aa091f0d7c656fd6fe85f97c36af3ccba7e
[`04a8139`]: https://github.com/magma-Devs/smart-router/commit/04a8139b7d970e1e626fdbbccf273425866b8ae6
[`069ea42`]: https://github.com/magma-Devs/smart-router/commit/069ea42293c3388ea61706b0541dfe2398e7b1a2
[`077f9bf`]: https://github.com/magma-Devs/smart-router/commit/077f9bf25ad2263416a4df5eff0cf7c8b3554847
[`083605d`]: https://github.com/magma-Devs/smart-router/commit/083605d9125413c1cf11985073d913339046a4e7
[`09fa4d6`]: https://github.com/magma-Devs/smart-router/commit/09fa4d639a4961163138fcb47de58c16f6a6833a
[`0adbc7c`]: https://github.com/magma-Devs/smart-router/commit/0adbc7c8e38d50455541f7609e1db614ab10abc3
[`0d822a0`]: https://github.com/magma-Devs/smart-router/commit/0d822a0fa4553cf1e5be9a056bfe5a63bad42745
[`117d8e9`]: https://github.com/magma-Devs/smart-router/commit/117d8e9fe39c3a8f9fa18b5a0830f87e9e729c78
[`153072c`]: https://github.com/magma-Devs/smart-router/commit/153072c0131372c42a4d16b902a0a90d8b7477e6
[`1c2db0a`]: https://github.com/magma-Devs/smart-router/commit/1c2db0a6a1e3e6c9004359e0217ba85f68bed2be
[`1d26b2c`]: https://github.com/magma-Devs/smart-router/commit/1d26b2c1814cc4ea87c68b02c46a7ecaff080b85
[`1f6c93e`]: https://github.com/magma-Devs/smart-router/commit/1f6c93e723226a530c57fffd7f1afa4206d1705f
[`21cc863`]: https://github.com/magma-Devs/smart-router/commit/21cc8636be2385d46a65b074733667f3b0755da2
[`226ec20`]: https://github.com/magma-Devs/smart-router/commit/226ec20ceec16e9f8b239612859b202ad7f8aa28
[`24e4606`]: https://github.com/magma-Devs/smart-router/commit/24e4606754825403791c4d812da2abb89bf843d5
[`2804602`]: https://github.com/magma-Devs/smart-router/commit/28046026ecd2fa9ffbfec28ca40004d9faf8c46e
[`290555e`]: https://github.com/magma-Devs/smart-router/commit/290555e3b74838acc1324534aca2ac23b381a840
[`2932340`]: https://github.com/magma-Devs/smart-router/commit/2932340c06ddf337569bfe344b006e3e7688317a
[`2debe75`]: https://github.com/magma-Devs/smart-router/commit/2debe7533fd586b1da8ebf2a94e97edf68cdb3d5
[`341897e`]: https://github.com/magma-Devs/smart-router/commit/341897e2c3a0c1986059696c58a425f16ea90852
[`3d7b79f`]: https://github.com/magma-Devs/smart-router/commit/3d7b79f159f3dd287976a5f1277f7e5cf3ea004e
[`3db6209`]: https://github.com/magma-Devs/smart-router/commit/3db6209af77eece18fcbce820808a88699be022e
[`3dfabba`]: https://github.com/magma-Devs/smart-router/commit/3dfabba93fa7684f172619ed4514ade34d0f4bbf
[`3f128a0`]: https://github.com/magma-Devs/smart-router/commit/3f128a0f3a5b36fdcfad3325b17db12077568c31
[`4127703`]: https://github.com/magma-Devs/smart-router/commit/412770355ed27b36bcf19e9d783a52731aea3e12
[`433293e`]: https://github.com/magma-Devs/smart-router/commit/433293e963dd3be671a4bd871468e24e39ff7300
[`444be5e`]: https://github.com/magma-Devs/smart-router/commit/444be5e2d27a2b43c5907d76180e9f76af010711
[`45b4694`]: https://github.com/magma-Devs/smart-router/commit/45b46948bad134af4c54e79fedaac4a819c7b501
[`4910d8e`]: https://github.com/magma-Devs/smart-router/commit/4910d8e255dc1e575784653f413f2d2a94b17fff
[`498c298`]: https://github.com/magma-Devs/smart-router/commit/498c298cef25fa22a4a6e6dc5d361ea343f3bd84
[`49f3fdd`]: https://github.com/magma-Devs/smart-router/commit/49f3fddd7072e6e3cda1c3aa03d53d25242f7217
[`4ad6411`]: https://github.com/magma-Devs/smart-router/commit/4ad64114ba00454727c0b04ed99a815472ce5c3c
[`4b94712`]: https://github.com/magma-Devs/smart-router/commit/4b94712f586d6b0fde2ab92c5021110ef7f7d080
[`50d3360`]: https://github.com/magma-Devs/smart-router/commit/50d33601fde5eeeaa05a46b0079ee105489de92c
[`52f3f18`]: https://github.com/magma-Devs/smart-router/commit/52f3f1872f758d87077d8ad541a1258d3db29189
[`534962d`]: https://github.com/magma-Devs/smart-router/commit/534962d45294b83673fe9f2be6876a9dd84ffb64
[`54bcd90`]: https://github.com/magma-Devs/smart-router/commit/54bcd90059f75efe7ca409f379c71c675edee0f9
[`54e828a`]: https://github.com/magma-Devs/smart-router/commit/54e828ada5f8f35bcd4c9bceed143bfae3b5a04d
[`5509480`]: https://github.com/magma-Devs/smart-router/commit/5509480e2f22f31ea709b944040023f02bf3f808
[`5515103`]: https://github.com/magma-Devs/smart-router/commit/5515103c4192cb9f77249b3fb52b73f86588a2bc
[`55754d7`]: https://github.com/magma-Devs/smart-router/commit/55754d7ae1073aa116cc56d11470ad06b1ed3f2e
[`55f3294`]: https://github.com/magma-Devs/smart-router/commit/55f3294025688020b0acc40da96750824afbcaae
[`58be84d`]: https://github.com/magma-Devs/smart-router/commit/58be84d64d09f49193d81ba78cbcd057e0061150
[`5a5e2b6`]: https://github.com/magma-Devs/smart-router/commit/5a5e2b6d17d53bee92e31887338b74c09340072c
[`5d60293`]: https://github.com/magma-Devs/smart-router/commit/5d602938e6e2a098703976ed06649544c85573d9
[`5e3bb1e`]: https://github.com/magma-Devs/smart-router/commit/5e3bb1e3baba3df80f6d2eb47c433d657872bd1c
[`5f38402`]: https://github.com/magma-Devs/smart-router/commit/5f38402fce82ae50fe7767c19a1774eaeb707ad5
[`62889e4`]: https://github.com/magma-Devs/smart-router/commit/62889e467f59c95213bb98421d3a7673ab1a831e
[`64e32fe`]: https://github.com/magma-Devs/smart-router/commit/64e32fe241fe7545bb532b3f5b600ed44d1cf9a5
[`6c6a0f5`]: https://github.com/magma-Devs/smart-router/commit/6c6a0f5b5b87d7b4df5c67a696309e1d1d3dbc0e
[`6e3360b`]: https://github.com/magma-Devs/smart-router/commit/6e3360ba78fecaa74035e920875a1475bbc30113
[`737454f`]: https://github.com/magma-Devs/smart-router/commit/737454f6110de2a7fb6c6ed9b2d10dfa859a0050
[`74e0007`]: https://github.com/magma-Devs/smart-router/commit/74e00071af3b3b19bce69641c5b694417472490b
[`7f0aede`]: https://github.com/magma-Devs/smart-router/commit/7f0aedea612d0acd8b90dd0ac9ab343f10c7ae9b
[`8261326`]: https://github.com/magma-Devs/smart-router/commit/826132650bf81492dd39216fd988af0aa7d11b8f
[`8a0b6be`]: https://github.com/magma-Devs/smart-router/commit/8a0b6bee36ec54fd82a0c78ff67e6589a9255287
[`8b078a8`]: https://github.com/magma-Devs/smart-router/commit/8b078a8784e584ce48e5e297de06faf0a932cad0
[`8f0f762`]: https://github.com/magma-Devs/smart-router/commit/8f0f762f9a061632b4db1dfd069852116cfd594b
[`90d16e1`]: https://github.com/magma-Devs/smart-router/commit/90d16e18ee0951c07375748d630777a8dd7b1d00
[`92af4b0`]: https://github.com/magma-Devs/smart-router/commit/92af4b0204e97b19d916fd502b3ebb1f07fb6e8c
[`93f3156`]: https://github.com/magma-Devs/smart-router/commit/93f3156b1287285fef225657750c06a0982c46db
[`94c760c`]: https://github.com/magma-Devs/smart-router/commit/94c760cd0418bc24b067bfd97c1f464170c43d68
[`95d26e5`]: https://github.com/magma-Devs/smart-router/commit/95d26e54793fbe67141e43fbd71a0227efa34bfa
[`9e12e26`]: https://github.com/magma-Devs/smart-router/commit/9e12e26c2055e432b4aa957430d6975e8cba3168
[`9edd4a6`]: https://github.com/magma-Devs/smart-router/commit/9edd4a656c9ad6b12496e7d2df51a4827b47abe8
[`9ffd186`]: https://github.com/magma-Devs/smart-router/commit/9ffd1868b82cbbf31855dc83a1557f185cb6aa04
[`a2fc17e`]: https://github.com/magma-Devs/smart-router/commit/a2fc17ecd3fa7093a1aaa9c8800a488476a8e5cd
[`a7c7811`]: https://github.com/magma-Devs/smart-router/commit/a7c781195042213a0e767d1acb6cd599488c640b
[`af87fcd`]: https://github.com/magma-Devs/smart-router/commit/af87fcd9a93bd9f31944945830898a281c95d343
[`b043ad1`]: https://github.com/magma-Devs/smart-router/commit/b043ad1bd632c1f446b2410f6ef408667b513f3b
[`b16fd25`]: https://github.com/magma-Devs/smart-router/commit/b16fd251a07b57ee2c176cecbbb2baf5dee7bace
[`b63f66b`]: https://github.com/magma-Devs/smart-router/commit/b63f66bdef408ea48c25f6094d5654b937929f3b
[`b8197dc`]: https://github.com/magma-Devs/smart-router/commit/b8197dc800bfa8e422be52e4b8ecfb2b436c656a
[`b84f30e`]: https://github.com/magma-Devs/smart-router/commit/b84f30e93a24888242862708b92c9544c7d8754e
[`be5548d`]: https://github.com/magma-Devs/smart-router/commit/be5548d6fd18b0f652c36a4dbeda1668023c78c1
[`c5605b8`]: https://github.com/magma-Devs/smart-router/commit/c5605b877da597b9d080e2adea1cad314eb546ae
[`c663849`]: https://github.com/magma-Devs/smart-router/commit/c663849086377cc8ff1d41112475e9b630865e90
[`c7f58ec`]: https://github.com/magma-Devs/smart-router/commit/c7f58ec5f07018a4976093671fc2bb0cbf9f407d
[`ca7f24e`]: https://github.com/magma-Devs/smart-router/commit/ca7f24e0c5a4ae130653d7ae45256c2fe62642cb
[`cfb7ec1`]: https://github.com/magma-Devs/smart-router/commit/cfb7ec13533eca5e98d023548e5a555d4b13f117
[`d03d1a4`]: https://github.com/magma-Devs/smart-router/commit/d03d1a4859e948ff4890dffb55e77510b7344825
[`d3c6695`]: https://github.com/magma-Devs/smart-router/commit/d3c66952b80e67ac2cc8c86b8cec83cce4960016
[`d612cdc`]: https://github.com/magma-Devs/smart-router/commit/d612cdc6048c488cefb4b5e1fa35cedf82538e91
[`d614bc4`]: https://github.com/magma-Devs/smart-router/commit/d614bc4a95403d2ed12bdd33d1ad4c086e86fa86
[`dbe4066`]: https://github.com/magma-Devs/smart-router/commit/dbe40669e9ca4dd55ff53027bcd18ddb78468ae2
[`dd857f8`]: https://github.com/magma-Devs/smart-router/commit/dd857f8a1a0bbff8698b3c09aa8739ce083d9451
[`ddb8052`]: https://github.com/magma-Devs/smart-router/commit/ddb80521cf1023fb7808e3b543000f37e5feca6f
[`ddd0245`]: https://github.com/magma-Devs/smart-router/commit/ddd0245fff5ac493a2f7357a7116012a6119a7d2
[`de5cec0`]: https://github.com/magma-Devs/smart-router/commit/de5cec03f5b477ee1fbb6ec22828fe152e79b988
[`df08017`]: https://github.com/magma-Devs/smart-router/commit/df080170137018634a38a809bd4bcefeb77094a9
[`df0e8e3`]: https://github.com/magma-Devs/smart-router/commit/df0e8e3f914d7aa09398e851ac65b7147298ef1b
[`e1d71b9`]: https://github.com/magma-Devs/smart-router/commit/e1d71b9d514cd3da8303de2796be5372ff5c17d4
[`e3bb5f2`]: https://github.com/magma-Devs/smart-router/commit/e3bb5f242754eda56452e7775d92a921b89ba8ec
[`e44526f`]: https://github.com/magma-Devs/smart-router/commit/e44526f5d48f9101434494133c2d2caec9fdef82
[`e7befa7`]: https://github.com/magma-Devs/smart-router/commit/e7befa7d68e91ae4f49703dec0bb0355ea48be5e
[`e9f7c66`]: https://github.com/magma-Devs/smart-router/commit/e9f7c66d8e6c867811e011479808930949bea783
[`ec7c0a1`]: https://github.com/magma-Devs/smart-router/commit/ec7c0a155e2cb80a363f7bbffde2c76c7e6a4486
[`eeddfc1`]: https://github.com/magma-Devs/smart-router/commit/eeddfc1d8cbc4b71100513b119ca8c73bc5f1b80
[`f5348da`]: https://github.com/magma-Devs/smart-router/commit/f5348da2abdbaae03a19df12a691752e732488b9
[`f5fc1b0`]: https://github.com/magma-Devs/smart-router/commit/f5fc1b0aa881e57f6f2e6bfc7c4ac31504024310
[`fa3304d`]: https://github.com/magma-Devs/smart-router/commit/fa3304d0d51c5835bcb6b96f2bb95fe9be108d00
[`fea7af2`]: https://github.com/magma-Devs/smart-router/commit/fea7af2ca827c5e9309501f8c0de89c7479cb76f

## v1.3.2 — 2026-08-12

### Highlights

Smart Router v1.3.2 expands operator visibility by introducing two new endpoints to the debug HTTP server. To assist in diagnosing upstream routing decisions, SREs can query `GET /debug/provider-scores` to inspect current QoS metrics, which explicitly identifies any chains that have not yet generated scoring data. Operators can also manually trigger immediate state updates using `POST /debug/poll-now`, an endpoint designed with strict 504 timeout semantics that guarantees unwitnessed polls are never reported as completed. Finally, protocol routing behavior is corrected in the session layer by allowing a specification's `Content-Type` to override the direct-RPC default, ensuring payloads are formatted correctly for strict upstream nodes.

### Changes

#### New Features
- feat(smart-router/debug): add POST /debug/poll-now (MAG-2649) ([#258]) [`acbeb1d`]
- feat(smart-router/debug): add GET /debug/provider-scores (MAG-2707) ([#259]) [`6b45d72`]

#### Bug fixes
- fix(smart-router/debug): never report an unwitnessed poll as a completed one (MAG-2649 review) ([#258]) [`2161fd2`]
- fix(smart-router/debug): name the chains that produced no scores (MAG-2707 review) ([#259]) [`0d0c32a`]
- fix(lavasession): let a spec content-type override the direct-RPC default (MAG-2744) ([#268]) [`0a0d287`]

#### Documentation updates
- docs(smart-router/debug): correct /debug/poll-now's 504 semantics (MAG-2649 review) ([#258]) [`dcb916d`]

[#258]: https://github.com/magma-Devs/smart-router/pull/258
[#259]: https://github.com/magma-Devs/smart-router/pull/259
[#268]: https://github.com/magma-Devs/smart-router/pull/268
[`0a0d287`]: https://github.com/magma-Devs/smart-router/commit/0a0d28787d9b169e08aa419a3876afb5e0205e14
[`0d0c32a`]: https://github.com/magma-Devs/smart-router/commit/0d0c32a1a5c7f171e8cb14f04f0961b2b1064881
[`2161fd2`]: https://github.com/magma-Devs/smart-router/commit/2161fd2f2c1cf858ccabd4d34b6ea1169fbb6847
[`6b45d72`]: https://github.com/magma-Devs/smart-router/commit/6b45d72c0496ae570642f8c67352dde1ed784cc2
[`acbeb1d`]: https://github.com/magma-Devs/smart-router/commit/acbeb1d88579010227f92e41677ec20419641f2e
[`dcb916d`]: https://github.com/magma-Devs/smart-router/commit/dcb916d74f311f58209b1fa985065a61625be4f9

## v1.3.1 — 2026-08-06

### Highlights

Smart Router v1.3.1 introduces a head-only mode for the chain tracker, enabling support for chains that do not expose block-by-number queries while correctly handling absent parse directives. To improve failover accuracy and QoS scoring, the router now gates probe re-enables by replaying the specific failing relay and drops the finality distance calculation from the `EndpointLagThreshold`. Protocol routing receives targeted stability updates, ensuring that authentication headers are correctly forwarded during gRPC dials and that `nil` upstream subscriptions are safely rejected instead of triggering a panic. Finally, internal error handling is stabilized by nil-guarding all `LavaWrappedError` methods, alongside observability updates that add tracking for relay cancellations and remove unused write-only `EndpointMetrics` state to reduce overhead.

### Changes

#### New Features
- feat(chaintracker): head-only mode for chains with no block-by-number (MAG-2218) ([#245]) [`5eccf8e`]

#### Bug fixes
- fix(grpc): forward auth-headers on gRPC dials (MAG-2218) ([#244]) [`b834713`]
- fix(chaintracker): treat a nil parse directive as absent when selecting head-only ([#245]) [`265f0b5`]
- fix(relaycore): drop the finality distance from EndpointLagThreshold ([#246]) [`a48ea47`]
- fix(smart-router): gate probe re-enable on replaying the failing relay (MAG-2550) ([#247]) [`7d42b19`]
- fix(smart-router): bound the relay-probe gate and judge honestly (MAG-2550 review) ([#247]) [`e77029b`]
- fix(common): nil-guard every LavaWrappedError method that reads LavaErr ([#252]) [`ba45a3b`]
- fix(rpcsmartrouter): reject a nil upstream subscription instead of panicking (MAG-2685) ([#253]) [`cfb3254`]
- refactor(smart-router): remove dead code unreachable from all entry points (MAG-2690) ([#254]) [`79a5ce8`]
- refactor(smart-router): remove test-only-reachable dead code (MAG-2691) ([#254]) [`84bbbd2`]
- refactor(metrics): remove write-only EndpointMetrics tracking state (MAG-2691) ([#254]) [`0aa7e86`]
- refactor(smart-router): clean up leftovers from the dead-code sweep (MAG-2690) ([#257]) [`dcbd11a`]

#### Other work
- MAG-2667 require valid Jira ticket for pull requests ([#249]) [`864f6e3`]
- Enhance metrics and error handling for relay cancellations ([#252]) [`5982011`]

[#244]: https://github.com/magma-Devs/smart-router/pull/244
[#245]: https://github.com/magma-Devs/smart-router/pull/245
[#246]: https://github.com/magma-Devs/smart-router/pull/246
[#247]: https://github.com/magma-Devs/smart-router/pull/247
[#249]: https://github.com/magma-Devs/smart-router/pull/249
[#252]: https://github.com/magma-Devs/smart-router/pull/252
[#253]: https://github.com/magma-Devs/smart-router/pull/253
[#254]: https://github.com/magma-Devs/smart-router/pull/254
[#257]: https://github.com/magma-Devs/smart-router/pull/257
[`0aa7e86`]: https://github.com/magma-Devs/smart-router/commit/0aa7e864b8e3885a6d2bf4266174d4b2e199054a
[`265f0b5`]: https://github.com/magma-Devs/smart-router/commit/265f0b59a1c43861aa3fea17fb02129a3bad67cb
[`5982011`]: https://github.com/magma-Devs/smart-router/commit/59820119b1bc6a1e52708a3a4538e6000b704f01
[`5eccf8e`]: https://github.com/magma-Devs/smart-router/commit/5eccf8eba875cc7700fafdbb85b27e8446c5a6da
[`79a5ce8`]: https://github.com/magma-Devs/smart-router/commit/79a5ce8d3dda60dc5e87f1f544135ea41572cb56
[`7d42b19`]: https://github.com/magma-Devs/smart-router/commit/7d42b19b43c104ce2c3ce70e3bbdbfe31ca1a92b
[`84bbbd2`]: https://github.com/magma-Devs/smart-router/commit/84bbbd2f98674ee781c2902e374ecba3f79f2208
[`864f6e3`]: https://github.com/magma-Devs/smart-router/commit/864f6e3b2d87ea39ffa9ffd7d5f71440f87bebd6
[`a48ea47`]: https://github.com/magma-Devs/smart-router/commit/a48ea473bbace391c1bb44297e5ae2684da64adf
[`b834713`]: https://github.com/magma-Devs/smart-router/commit/b8347134f2ac7695e14372c5ffb216c47c78957f
[`ba45a3b`]: https://github.com/magma-Devs/smart-router/commit/ba45a3b67f2dd84c8397be48f7e68c8cf5dc2c4a
[`cfb3254`]: https://github.com/magma-Devs/smart-router/commit/cfb3254e03b425b57d4e0ed3e77b7e13de5cd882
[`dcbd11a`]: https://github.com/magma-Devs/smart-router/commit/dcbd11aeea8b786fc1b1e60fb56092d6a09fa732
[`e77029b`]: https://github.com/magma-Devs/smart-router/commit/e77029bcd324db3dd10d1cccaf14efbc73e347b2

## v1.3.0 — 2026-07-30

### Highlights

Smart Router v1.3.0 overhauls chain state consistency by replacing per-user block tracking with a block-monotonic, guarded chain tip that maintains cross-pod synchronization. This self-healing tip architecture is supported by routing adjustments that prevent providers from being demoted after a single failed cycle, alongside fixes that correctly route REST POST block polls to their specific method paths. For observability, the router now mirrors this guarded tip directly into the `smartrouter_latest_block` metric and prevents cardinality explosion by collapsing batch method labels into bounded signatures. Operators managing active deployments gain new administrative controls through the `/debug/reset-probe-backoff` and `/debug/reset-chaintracker-rows` endpoints to manually clear internal routing state. Additionally, a dedicated `/debug/time-warp` endpoint allows operators to manipulate ChainState TTL staleness, while asynchronous comparisons now correctly report cross-validation stragglers as pending rather than prematurely failing the request.

### Changes

#### New Features
- feat(smart-router/debug): add /debug/reset-probe-backoff + /debug/reset-chaintracker-rows (MAG-2395) ([#223]) [`210f5ad`]
- feat(metrics): collapse batch method labels into bounded signatures ([#242]) [`6fef8e0`]

#### Bug fixes
- fix(relaycore): single provider address on all-transport-errors failure result (MAG-2351) ([#213]) [`91aecab`]
- fix(relaycore): address review — minimize to header fix, drop over-reach (MAG-2351) ([#213]) [`e2c6902`]
- refactor(spec): remove 15 unused spec fields and their dead code ([#218]) [`e19a913`]
- fix(rpcsmartrouter): report CV stragglers as pending + compare late responses async (MAG-2187) ([#212]) [`816efce`]
- fix(smart-router/debug): review feedback on reset-chaintracker-rows (MAG-2395) ([#223]) [`499c728`]
- fix(rpcsmartrouter): wire /debug/time-warp into ChainState TTL/staleness (MAG-2307) ([#222]) [`c011a80`]
- fix(lavasession): give each router key its own unwanted set (MAG-2442) ([#221]) [`b73bd69`]
- fix(rpcsmartrouter): dedicated ChainState time-warp endpoint + real-clock timestamps (MAG-2307 review) ([#224]) [`b75d68b`]
- fix(rpcsmartrouter): address PR #224 review — distinct response field, reset-all test, doc note (MAG-2307) ([#224]) [`bf1edab`]
- fix(consistency): measure endpoints against the guarded chain tip; retire per-user seenBlock ([#225]) [`969c076`]
- fix(chainstate): address PR #225 review — reset-all clears the tip; docs + tests ([#225]) [`548c5fa`]
- refactor(endpointtip): block-monotonic per-endpoint tip with a staleness backstop ([#235]) [`ee734d5`]
- refactor(endpointtip): anchor freshness stamp forward; fix staleness-window docs ([#235]) [`aea96f5`]
- refactor(probing): derive liveness horizon via chainstate.StalenessWindow ([#235]) [`18dec04`]
- fix(shared-state): rebuild cross-pod consistency on the guarded chain tip (T10) ([#235]) [`901ac07`]
- refactor(shared-state): log adopt outcome so a rejected peer tip is visible ([#235]) [`d13f601`]
- fix(cache): floor the shared-state tip TTL at a hard minimum ([#235]) [`44e04e8`]
- refactor(chainstate): delete the bootstrap atomic; readers take the self-healing tip (T3) ([#233]) [`364a269`]
- fix(chainstate): clarify tip advancement logic in SetLatestBlock documentation ([#233]) [`ea05611`]
- fix(endpointstate): send REST POST block polls to the method path (MAG-2597) ([#236]) [`6bb6c73`]
- fix(endpointstate): honor CustomMessage path on REST non-GET, pin poll routing (MAG-2597) ([#236]) [`885af1c`]
- fix(rpcsmartrouter): keep ChainTrackers reconciled; don't demote on one bad cycle ([#237]) [`e10961f`]
- fix(metrics): mirror the guarded tip into smartrouter_latest_block on every poll observation ([#239]) [`075baef`]

#### Documentation updates
- docs(endpointtip): correct stale time-monotonic comments to block-monotonic (T4) ([#235]) [`f228207`]
- docs(rpcsmartrouter): fix the T4 doc the sweep missed; separate adopt outcomes ([#235]) [`5c8f443`]
- docs(endpointtip): document the one write that stores a hybrid tip triple ([#235]) [`16fc4d3`]
- docs(chainstate): retire stale bootstrap-atomic references after T3 (review follow-up) ([#233]) [`629f647`]

#### Build process updates
- ci: make public readiness smoke non-blocking in PR gate ([#227]) [`0c9f441`]

#### Other work
- fix MAG-2392 stale provider fallback ([#214]) [`c7e1022`]
- unit test ([#233]) [`a161ba5`]

[#212]: https://github.com/magma-Devs/smart-router/pull/212
[#213]: https://github.com/magma-Devs/smart-router/pull/213
[#214]: https://github.com/magma-Devs/smart-router/pull/214
[#218]: https://github.com/magma-Devs/smart-router/pull/218
[#221]: https://github.com/magma-Devs/smart-router/pull/221
[#222]: https://github.com/magma-Devs/smart-router/pull/222
[#223]: https://github.com/magma-Devs/smart-router/pull/223
[#224]: https://github.com/magma-Devs/smart-router/pull/224
[#225]: https://github.com/magma-Devs/smart-router/pull/225
[#227]: https://github.com/magma-Devs/smart-router/pull/227
[#233]: https://github.com/magma-Devs/smart-router/pull/233
[#235]: https://github.com/magma-Devs/smart-router/pull/235
[#236]: https://github.com/magma-Devs/smart-router/pull/236
[#237]: https://github.com/magma-Devs/smart-router/pull/237
[#239]: https://github.com/magma-Devs/smart-router/pull/239
[#242]: https://github.com/magma-Devs/smart-router/pull/242
[`075baef`]: https://github.com/magma-Devs/smart-router/commit/075baefce5c2f8465dc92b27c5aae92c059e6125
[`0c9f441`]: https://github.com/magma-Devs/smart-router/commit/0c9f441d45755aa5938a56d6ba03e6452b9b72ff
[`16fc4d3`]: https://github.com/magma-Devs/smart-router/commit/16fc4d31b909171ec37737037b9fd0901fcc9db8
[`18dec04`]: https://github.com/magma-Devs/smart-router/commit/18dec0449a17abb53c1c3a0668886bf37dea7f30
[`210f5ad`]: https://github.com/magma-Devs/smart-router/commit/210f5ad7d018a98e5bf3d0e750a3e3c615a57d03
[`364a269`]: https://github.com/magma-Devs/smart-router/commit/364a269e29b6469e2ce25098f3ee3b51311c4f2f
[`44e04e8`]: https://github.com/magma-Devs/smart-router/commit/44e04e819cb892423cac9f2e50ac14d1e13e7c97
[`499c728`]: https://github.com/magma-Devs/smart-router/commit/499c728f06e09886368ba05f636bf4fb6b9dca3b
[`548c5fa`]: https://github.com/magma-Devs/smart-router/commit/548c5fa6e432a11aec9fc2a1f8f91428bfe7a226
[`5c8f443`]: https://github.com/magma-Devs/smart-router/commit/5c8f44300f8b5bb94c48a5cbd646ba273ab743da
[`629f647`]: https://github.com/magma-Devs/smart-router/commit/629f647b7595e5bd1c54662b29dbbca44a279d79
[`6bb6c73`]: https://github.com/magma-Devs/smart-router/commit/6bb6c73bd217f0d13095ad9e3af83f5aa3dc9eec
[`6fef8e0`]: https://github.com/magma-Devs/smart-router/commit/6fef8e0bfa7211b01811be23e61eea78673c424b
[`816efce`]: https://github.com/magma-Devs/smart-router/commit/816efced02b365fef4abe5dfd618e04f9e477d15
[`885af1c`]: https://github.com/magma-Devs/smart-router/commit/885af1c6e26c881ab6f85e2ddbbce4220009b2e6
[`901ac07`]: https://github.com/magma-Devs/smart-router/commit/901ac078292766cc03a8e6b132738c4e5430b9fa
[`91aecab`]: https://github.com/magma-Devs/smart-router/commit/91aecab8a037991016c95c3d64ef252021617d39
[`969c076`]: https://github.com/magma-Devs/smart-router/commit/969c076fd876ce80cd0fb5040b523708083aa0b4
[`a161ba5`]: https://github.com/magma-Devs/smart-router/commit/a161ba501b6f5b5136b136cd577410f9b0ed8b8c
[`aea96f5`]: https://github.com/magma-Devs/smart-router/commit/aea96f55ec6417d5676b96e9adf0333b7379f8c4
[`b73bd69`]: https://github.com/magma-Devs/smart-router/commit/b73bd69d42a65d81b06696cb81e822cd2c2c8575
[`b75d68b`]: https://github.com/magma-Devs/smart-router/commit/b75d68b6801891d753e72b68ac840a726e68a348
[`bf1edab`]: https://github.com/magma-Devs/smart-router/commit/bf1edab363127e5f96ba65e92effd12bfe11fe7c
[`c011a80`]: https://github.com/magma-Devs/smart-router/commit/c011a8049aecefdc0b5acb1bd7b1f6eed0162632
[`c7e1022`]: https://github.com/magma-Devs/smart-router/commit/c7e10222a6c1540281643ac7940f097920bd9a89
[`d13f601`]: https://github.com/magma-Devs/smart-router/commit/d13f601ed6842a4e86ae37a84ae6f1cb9e9d8643
[`e10961f`]: https://github.com/magma-Devs/smart-router/commit/e10961f82c12ab746d3d1667d75e4bac7825ff42
[`e19a913`]: https://github.com/magma-Devs/smart-router/commit/e19a913387ff3d4a6c278480213c12e85dcd2468
[`e2c6902`]: https://github.com/magma-Devs/smart-router/commit/e2c690243fa8d31e2d9c0a0b31dffa33ec9c2265
[`ea05611`]: https://github.com/magma-Devs/smart-router/commit/ea05611f4a67373ef22a2eaaf63476bd66e773b7
[`ee734d5`]: https://github.com/magma-Devs/smart-router/commit/ee734d5da59e56e1e7c96fcc9a18d8f8aca28e73
[`f228207`]: https://github.com/magma-Devs/smart-router/commit/f2282072bd827af9b9b29e2d1d410709a42a116d

## v1.2.0 — 2026-07-14

### Highlights

Smart Router v1.2.0 introduces a major redesign to its ChainTracker and probing subsystems, shifting from isolated per-endpoint observations to a ChainState consensus model driven by a proactive prober. This architectural update is paired with routing fixes that actively starve dead providers from organic selection and ensure the endpoint tip store is properly cleared during state resets. For configuration management, the spec fetcher now downloads API specifications as a single compressed tarball, eliminating the requirement for operators to supply a GitHub token when accessing public repositories. Integrators serving browser-based clients can now use the newly added `--cors-expose-headers` flag to explicitly expose specific HTTP response headers through the gateway. Finally, the interactive setup wizard has been updated to perform OS-adaptive prerequisite checks before initiating the configuration flow.

### Changes

#### New Features
- feat(wizard): add OS-adaptive prerequisite check before the flow ([#194]) [`7f845c3`]
- feat(cors): add --cors-expose-headers to expose response headers to browsers ([#199]) [`cbbed33`]
- feat: ChainTracker & Probing redesign (MAG-2157): per-endpoint observations → ChainState consensus → proactive prober ([#143]) [`6a8ec51`]
- feat(specfetcher): fetch specs as one tarball, no GitHub token needed for public repos ([#211]) [`a60806f`]

#### Bug fixes
- fix(chaintracker-redesign): address code-review findings ([#209]) [`6c870c2`]
- fix(endpointstate): clear the endpointtip store on reset (10th review finding) ([#209]) [`d4018ab`]
- fix(rpcsmartrouter): close bootstrap-atomic bypass + prefer store over stale poll atomic ([#209]) [`a4816ac`]
- fix(provideroptimizer): starve dead providers from organic selection (MAG-2237) ([#210]) [`75e01c3`]

#### Documentation updates
- docs: dashboard v2 promoted to repo root (drop cd v2) ([#201]) [`2fbe813`]

#### Build process updates
- ci: add Helm compatibility validation to PR gate ([#202]) [`447ead6`]

[#143]: https://github.com/magma-Devs/smart-router/pull/143
[#194]: https://github.com/magma-Devs/smart-router/pull/194
[#199]: https://github.com/magma-Devs/smart-router/pull/199
[#201]: https://github.com/magma-Devs/smart-router/pull/201
[#202]: https://github.com/magma-Devs/smart-router/pull/202
[#209]: https://github.com/magma-Devs/smart-router/pull/209
[#210]: https://github.com/magma-Devs/smart-router/pull/210
[#211]: https://github.com/magma-Devs/smart-router/pull/211
[`2fbe813`]: https://github.com/magma-Devs/smart-router/commit/2fbe813c518cc0f8ae77a39ffe4d0f5d46fc25f0
[`447ead6`]: https://github.com/magma-Devs/smart-router/commit/447ead67cc4aea5a5229266150abacf2428f2ff7
[`6a8ec51`]: https://github.com/magma-Devs/smart-router/commit/6a8ec5197c571287319e02dc826cfa9f20073bdb
[`6c870c2`]: https://github.com/magma-Devs/smart-router/commit/6c870c223ac3a30ec5128c3bfe293dbb95814ac4
[`75e01c3`]: https://github.com/magma-Devs/smart-router/commit/75e01c3aef2be1b0b5c61586698bb2a163483e5a
[`7f845c3`]: https://github.com/magma-Devs/smart-router/commit/7f845c3187442ff7642d75c5d4c09c94182f3ef9
[`a4816ac`]: https://github.com/magma-Devs/smart-router/commit/a4816ac2d82028e641335ab1b8f357166b90a1e7
[`a60806f`]: https://github.com/magma-Devs/smart-router/commit/a60806f3821033e6acc0f308674e9b3f2902c892
[`cbbed33`]: https://github.com/magma-Devs/smart-router/commit/cbbed33952fb81abc8a13f95d3216b97e0117474
[`d4018ab`]: https://github.com/magma-Devs/smart-router/commit/d4018ab1f6342a9b76b861e63551f2538105db84

## v1.1.0 — 2026-07-02

### Highlights

Smart Router v1.1.0 introduces a breaking change to its wire contract by renaming all gRPC service names and telemetry prefixes to `smartrouter`, requiring operators to upgrade any unmodified Lava peers to maintain interoperability. For production orchestration, this release adds standard Kubernetes `/livez` and `/readyz` health probes alongside new diagnostic endpoints, such as `GET /debug/runtime-config`, to inspect the active configuration state. Operators can now manually clear error states and recover upstream connections using the newly introduced `/debug/reset-all` and `/debug/reset-endpoint-health` routes. Routing behavior receives critical fixes, ensuring the gateway correctly fails over from a pinned provider during retries and accepts WebSocket subscriptions even when parameters are omitted. Finally, the release artifacts now include signed SBOMs for supply chain verification, and the bundled configuration examples have been rewritten to demonstrate multi-source cross-validation across networks like Ethereum, Solana, and Bitcoin.

### Changes

#### ⚠ Breaking changes
- refactor: rebrand gRPC service names + telemetry to smartrouter ([#182]) [`711679f`]
  - the gRPC service name is a wire contract with upstream providers/cache; a renamed router no longer interops with unmodified Lava peers.

#### New Features
- feat(smart-router/debug): recover endpoint health in /debug/reset-all + add /debug/reset-endpoint-health ([#144]) [`e01b492`]
- feat(examples): multi-source CV examples + cross-validation doc ([#171]) [`69d3f44`]
- feat(examples): retarget example chains to ETH/SOL/BTC/Hyperliquid/Cosmos/Aptos, drop Lava endpoints ([#173]) [`5f72982`]
- feat(smart-router/debug): add GET /debug/runtime-config ([#139]) [`177b408`]
- feat(release): two-stage release flow — born prerelease, graduate to move :latest ([#180]) [`523b4ec`]
- feat(metrics): add /livez + /readyz k8s health probes ([#184]) [`249f35d`]
- feat(release): allow forcing changelog regen for a recreated tag ([#186]) [`1207705`]

#### Bug fixes
- fix(smart-router): fail over from a pinned provider on retry (MAG-2228) ([#170]) [`2f5a784`]
- fix(examples): allow plaintext gRPC for Polkachu Cosmos upstream ([#173]) [`cb5e7d8`]
- fix(examples): drop Polkachu's non-existent tendermint websocket leg ([#173]) [`9a0930b`]
- fix(chaintracker): guard nil oldBlockCallback in notUpdated (MAG-2219) ([#177]) [`ab65426`]
- fix(smart-router): accept WS subscriptions with omitted params (MAG-2246) ([#176]) [`7b7fe87`]
- fix(ci): pin govulncheck to repo-checkout: false ([#181]) [`2968fa5`]
- fix(ci): run govulncheck directly instead of via wrapper action ([#181]) [`f26c3be`]
- refactor: rebrand gRPC service names + telemetry to smartrouter ([#182]) [`711679f`]
- fix(release): ship SBOMs by scoping to binary artifacts + sign them ([#185]) [`2b7ccba`]

#### Documentation updates
- docs(license): fix inverted defined term for Enterprise uses ([#174]) [`c08200d`]
- docs(contributing): grant commercial relicensing rights on contributions ([#174]) [`86b2d82`]
- docs: fill empty contact placeholders ([#174]) [`5ceb840`]
- docs(contributing): drop empty "Join The Project Team" stub ([#174]) [`7bb078e`]
- docs(license): add SPDX LicenseRef identifier ([#174]) [`4b98860`]
- docs(license): split commercial terms into LICENSING.md so GitHub detects PolyForm ([#175]) [`bdc4864`]
- docs(readme): align messaging with docs site ([#178]) [`3725826`]
- docs(readme): add license note + update banner alt text ([#178]) [`374e4a3`]
- docs(readme): remove license note from top (keep alt text update) ([#178]) [`74d0ed7`]
- docs: add AI agent setup instructions ([#179]) [`c3456d8`]

#### Build process updates
- ci: move internal cluster host to a repo variable ([#174]) [`7b559d7`]

#### Other work
- Update README.md ([#183]) [`435614e`]

[#139]: https://github.com/magma-Devs/smart-router/pull/139
[#144]: https://github.com/magma-Devs/smart-router/pull/144
[#170]: https://github.com/magma-Devs/smart-router/pull/170
[#171]: https://github.com/magma-Devs/smart-router/pull/171
[#173]: https://github.com/magma-Devs/smart-router/pull/173
[#174]: https://github.com/magma-Devs/smart-router/pull/174
[#175]: https://github.com/magma-Devs/smart-router/pull/175
[#176]: https://github.com/magma-Devs/smart-router/pull/176
[#177]: https://github.com/magma-Devs/smart-router/pull/177
[#178]: https://github.com/magma-Devs/smart-router/pull/178
[#179]: https://github.com/magma-Devs/smart-router/pull/179
[#180]: https://github.com/magma-Devs/smart-router/pull/180
[#181]: https://github.com/magma-Devs/smart-router/pull/181
[#182]: https://github.com/magma-Devs/smart-router/pull/182
[#183]: https://github.com/magma-Devs/smart-router/pull/183
[#184]: https://github.com/magma-Devs/smart-router/pull/184
[#185]: https://github.com/magma-Devs/smart-router/pull/185
[#186]: https://github.com/magma-Devs/smart-router/pull/186
[`1207705`]: https://github.com/magma-Devs/smart-router/commit/1207705aa741cda9dc58d71e871f7630ac069bbb
[`177b408`]: https://github.com/magma-Devs/smart-router/commit/177b4084b7061deae6f079cee6dacdd7e872115d
[`249f35d`]: https://github.com/magma-Devs/smart-router/commit/249f35d18e115c04a4450ffae359998b83637da7
[`2968fa5`]: https://github.com/magma-Devs/smart-router/commit/2968fa517f727213066550195c2d439025850f1c
[`2b7ccba`]: https://github.com/magma-Devs/smart-router/commit/2b7ccba4f837bf8678b226a8afde1cc25e0061bc
[`2f5a784`]: https://github.com/magma-Devs/smart-router/commit/2f5a784451913a1e81a3901f9ff67178ba1bd8e9
[`3725826`]: https://github.com/magma-Devs/smart-router/commit/37258266959c17a267cb2f77aa3c359ac1f447c7
[`374e4a3`]: https://github.com/magma-Devs/smart-router/commit/374e4a329b52e197b61603d7bf723406d853fbb3
[`435614e`]: https://github.com/magma-Devs/smart-router/commit/435614e5f6a400850aa3b2792fd594e4400701b3
[`4b98860`]: https://github.com/magma-Devs/smart-router/commit/4b988603d247a528de983da9441db7fcf3d86190
[`523b4ec`]: https://github.com/magma-Devs/smart-router/commit/523b4ecdf5617ee3a7c6c00dc0a2568ffffa9bdc
[`5ceb840`]: https://github.com/magma-Devs/smart-router/commit/5ceb84053b48aea33b68992595ea9506d6cf695b
[`5f72982`]: https://github.com/magma-Devs/smart-router/commit/5f729827a5cb79c904f8d617023c8cc591d0f96b
[`69d3f44`]: https://github.com/magma-Devs/smart-router/commit/69d3f44c3b012997f6cc8bb5e0d97af5536e4d4c
[`711679f`]: https://github.com/magma-Devs/smart-router/commit/711679f7e78b0a0e90344247fe9b4e5da967151b
[`74d0ed7`]: https://github.com/magma-Devs/smart-router/commit/74d0ed7f11857ca56b61ede2752ae11aa6e2d605
[`7b559d7`]: https://github.com/magma-Devs/smart-router/commit/7b559d7317aaf57f1912ec1d3bd20c247563a2cb
[`7b7fe87`]: https://github.com/magma-Devs/smart-router/commit/7b7fe8778666508e93f444e6302913a18444bc7c
[`7bb078e`]: https://github.com/magma-Devs/smart-router/commit/7bb078e843b6f4c68bc7742fe72f35247d01f1a4
[`86b2d82`]: https://github.com/magma-Devs/smart-router/commit/86b2d82695fca23bffe1ce7c3693885c1ac45afc
[`9a0930b`]: https://github.com/magma-Devs/smart-router/commit/9a0930b5eddb4271a23b8c00d4da8156a63b917c
[`ab65426`]: https://github.com/magma-Devs/smart-router/commit/ab654266498e2b18e321f6babca2042bfd3418f9
[`bdc4864`]: https://github.com/magma-Devs/smart-router/commit/bdc4864b77548738e07e3cb0cac72434963366eb
[`c08200d`]: https://github.com/magma-Devs/smart-router/commit/c08200d94b80d02878aae0d4cceff6ae281614dd
[`c3456d8`]: https://github.com/magma-Devs/smart-router/commit/c3456d8a7cebda4dda4c3e29257e81ade952f50b
[`cb5e7d8`]: https://github.com/magma-Devs/smart-router/commit/cb5e7d8d09cecc391ce2b0d82d05791cad3e54af
[`e01b492`]: https://github.com/magma-Devs/smart-router/commit/e01b492b6d1bb5ec1f8d3d7e8d7f4b0b8132a2eb
[`f26c3be`]: https://github.com/magma-Devs/smart-router/commit/f26c3be7516751882b350789620f3fba9134b76a

## v1.0.5 — 2026-06-28

### Highlights

Smart Router v1.0.5 introduces an interactive configuration wizard to assist operators in generating upstream routing definitions and gateway settings. To support this setup process, the release now bundles a complete set of example configuration files for all supported chains. Once configured, integrators can validate their deployments using the new `smartrouter health` CLI command, which executes spec-driven diagnostic checks against the running instance. Finally, this release resolves a cross-origin request bug by ensuring the gateway correctly emits the `Access-Control-Allow-Headers: *` response header whenever the `cors-headers` configuration field is empty.

### Changes

#### New Features
- feat(health): add spec-driven `smartrouter health` CLI command ([#140]) [`5b5679b`]
- feat(wizard): interactive Go/Charm config wizard for smart-router ([#142]) [`7b71b38`]
- feat: add example configs for all bundled chains ([#160]) [`89cf8ff`]

#### Bug fixes
- fix(cors): emit Access-Control-Allow-Headers "*" when cors-headers is empty ([#145]) [`215c8f4`]
- fix: correct license typo ([#148]) [`1453612`]

#### Documentation updates
- docs(provider-optimizer): correct stale availability-cliff comments (0.90 -> 0.80) ([#141]) [`9774263`]
- docs(readme): align docker pull docs with public release ([#148]) [`e776280`]

#### Build process updates
- ci: trigger PR gate after approval ([#156]) [`ea5763d`]
- ci: run PR gate directly after approval ([#161]) [`4be0d2f`]
- ci: fix PR gate YAML indentation ([#162]) [`c8f0998`]
- ci: restore PR gate approval dispatcher ([#164]) [`21ecc36`]
- ci: strip legacy concurrent-providers arg in PR gate ([#166]) [`de80bed`]
- ci: fix concurrent-providers YAML indentation ([#167]) [`1f24ae4`]
- ci: strip legacy provider optimizer args in PR gate ([#168]) [`3875aab`]
- ci: fix legacy arg stripping in PR gate ([#169]) [`4eff13d`]

[#140]: https://github.com/magma-Devs/smart-router/pull/140
[#141]: https://github.com/magma-Devs/smart-router/pull/141
[#142]: https://github.com/magma-Devs/smart-router/pull/142
[#145]: https://github.com/magma-Devs/smart-router/pull/145
[#148]: https://github.com/magma-Devs/smart-router/pull/148
[#156]: https://github.com/magma-Devs/smart-router/pull/156
[#160]: https://github.com/magma-Devs/smart-router/pull/160
[#161]: https://github.com/magma-Devs/smart-router/pull/161
[#162]: https://github.com/magma-Devs/smart-router/pull/162
[#164]: https://github.com/magma-Devs/smart-router/pull/164
[#166]: https://github.com/magma-Devs/smart-router/pull/166
[#167]: https://github.com/magma-Devs/smart-router/pull/167
[#168]: https://github.com/magma-Devs/smart-router/pull/168
[#169]: https://github.com/magma-Devs/smart-router/pull/169
[`1453612`]: https://github.com/magma-Devs/smart-router/commit/1453612c7968ae3f4d38af82eb4b1f56bd0cc1c7
[`1f24ae4`]: https://github.com/magma-Devs/smart-router/commit/1f24ae46a7815d586d8c74330b54e9b6e7402a51
[`215c8f4`]: https://github.com/magma-Devs/smart-router/commit/215c8f4562da9e94b4fc9b1b65aa676e374c492f
[`21ecc36`]: https://github.com/magma-Devs/smart-router/commit/21ecc36d375752577b48af9d748126485672b688
[`3875aab`]: https://github.com/magma-Devs/smart-router/commit/3875aab79cf33072cc88eea6c93e2d9f338f039f
[`4be0d2f`]: https://github.com/magma-Devs/smart-router/commit/4be0d2f57e611923476f1c566b7e4fae42170b0d
[`4eff13d`]: https://github.com/magma-Devs/smart-router/commit/4eff13dfda00cc9082c29ebef3b58bce28f9fdcd
[`5b5679b`]: https://github.com/magma-Devs/smart-router/commit/5b5679b47b1fcd573aa300b7c6df53a97ed91d9e
[`7b71b38`]: https://github.com/magma-Devs/smart-router/commit/7b71b380c4f0a25d141166d1b87485d470bc7ba3
[`89cf8ff`]: https://github.com/magma-Devs/smart-router/commit/89cf8ff915b2ad2ea48dff78c19bc8169f61cc88
[`9774263`]: https://github.com/magma-Devs/smart-router/commit/97742634189805403c9afb8d20d2b617d7c35452
[`c8f0998`]: https://github.com/magma-Devs/smart-router/commit/c8f09986fba943f42afd7ec7e1a3d3648c4f2dab
[`de80bed`]: https://github.com/magma-Devs/smart-router/commit/de80beda8d537dd298103705323f4dc07b278332
[`e776280`]: https://github.com/magma-Devs/smart-router/commit/e7762808462fc549fd24d2942cdeaefe9d96614c
[`ea5763d`]: https://github.com/magma-Devs/smart-router/commit/ea5763d1bd06265a00fc33d9234c4e5bb97b0346

## v1.0.4 — 2026-06-22

### Highlights

Smart Router v1.0.4 introduces critical breaking changes to observability and configuration, requiring operators to update dashboards, alerts, and startup scripts before upgrading. All Prometheus metrics have been stripped of the legacy `lava` prefix, meaning any monitoring infrastructure referencing `lava_rpc*` or the specific `lava_errors_total` counter must be migrated to the new `smartrouter_*` and `rpc_*` namespaces, with errors now tracked under `smartrouter_errors_total`. Additionally, the default OpenTelemetry `service.name` has been changed from `lava-rpcsmartrouter` to `smartrouter`, which will break existing trace filtering and aggregation if not adjusted in collector configurations. Finally, the CLI flags used to tune upstream routing weights have been renamed; operators must replace all instances of `provider-optimizer-*` weight flags with their new `qos-*` equivalents to prevent unrecognized flag errors during startup.

### Changes

#### ⚠ Breaking changes
- refactor!: drop the lava prefix from smart-router metric names ([#138]) [`4e21206`]
  - All Prometheus metric names emitted by the smart router are renamed. Any dashboard, alerting rule, recording rule, or scrape relabeling that references the old `lava_rpc*` names must be updated to the new `smartrouter_*` / `rpc_*` names. The default OTel service.name also changes from "lava-rpcsmartrouter" to "smartrouter".
- refactor!: rename lava_errors_total -> smartrouter_errors_total ([#138]) [`bce3a24`]
  - The lava_errors_total Prometheus counter is renamed to smartrouter_errors_total. Dashboards/alerts referencing the old name must be updated.
- refactor(flags)!: rename provider-optimizer-* weight flags to qos-* ([#137]) [`abe524c`]

#### Bug fixes
- fix(ci): align dev-sim-prtests naming ([#130]) [`f60a314`]
- refactor!: drop the lava prefix from smart-router metric names ([#138]) [`4e21206`]
- refactor!: rename lava_errors_total -> smartrouter_errors_total ([#138]) [`bce3a24`]
- refactor(flags)!: rename provider-optimizer-* weight flags to qos-* ([#137]) [`abe524c`]

#### Documentation updates
- docs: fix stale lava_errors_* section header in METRICS.md ([#138]) [`028f9dc`]

[#130]: https://github.com/magma-Devs/smart-router/pull/130
[#137]: https://github.com/magma-Devs/smart-router/pull/137
[#138]: https://github.com/magma-Devs/smart-router/pull/138
[`028f9dc`]: https://github.com/magma-Devs/smart-router/commit/028f9dcc98de680e221808f47bcb8551823b1cbf
[`4e21206`]: https://github.com/magma-Devs/smart-router/commit/4e21206602b59004c675b776481038dea1295ce0
[`abe524c`]: https://github.com/magma-Devs/smart-router/commit/abe524c8ca9a2d75eb0e4a7b07272001fb4692c6
[`bce3a24`]: https://github.com/magma-Devs/smart-router/commit/bce3a249c195c9951172b32d098e3ffc4d26c6cf
[`f60a314`]: https://github.com/magma-Devs/smart-router/commit/f60a3143f278d42e8e3842f22419a52533574207

## v1.0.3 — 2026-06-21

### Highlights

Smart Router v1.0.3 introduces two breaking changes that require immediate operator action: the `--geolocation` CLI flag has been removed entirely, and the deprecated `static-providers:` and `backup-providers:` YAML configuration keys are no longer read. Invocations passing `--geolocation` will now fail with an "unknown flag" error, and operators must rename the legacy configuration keys to `direct-rpc:` and `backup-direct-rpc:` to prevent startup failures, while also updating any dashboards that rely on the dropped `geo_location` attribute in optimizer-QoS metrics. Beyond these breaking changes, this release implements a group-aware cross-validation engine that evaluates responses across diverse provider sets using per-method policies, exposing validation failures through a new `disagreeing-providers` header and dedicated mismatch metrics. WebSocket connections now support JSON-formatted requests and assign unique wire IDs to safely multiplex concurrent calls without re-dialing closed sockets. Connection resilience is adjusted by increasing the maximum consecutive connection attempts from 5 to 50 and removing the dead per-socket `isHealthy` selection gate, while the provider optimizer now calculates sync scores using per-endpoint blocks. Finally, the `/debug/pprof` endpoint is no longer exposed on the cache metrics port to prevent unintended profiling access.

### Changes

#### ⚠ Breaking changes
- chore!: remove geolocation entirely from smart-router ([#134]) [`afe1805`]
  - the --geolocation CLI flag is removed. Invocations that pass --geolocation will now fail with "unknown flag". The emitted optimizer-QoS metric also drops its geo_location attribute. Update any scripts, deployments, or dashboards that reference them.
- chore!: drop deprecated static-providers/backup-providers config keys ([#135]) [`1735bdc`]
  - smart-router no longer reads "static-providers:" or "backup-providers:" YAML keys. Configs still using them must rename to "direct-rpc:" / "backup-direct-rpc:" or the router fails to start with "requires direct-rpc endpoints configuration".

#### New Features
- feat(cross-validation): add provider group-label spine (Phase 0.1) ([#102]) [`47a337a`]
- feat(cross-validation): per-method policy resolver (Phase 1.1 core) ([#102]) [`d8f6808`]
- feat(cross-validation): wire per-method policy resolver into selection (Phase 1.1) ([#102]) [`080d114`]
- feat(cross-validation): group-aware quorum termination + gate (Phase 1.2b/1.2c) ([#102]) [`2388b8c`]
- feat(cross-validation): group-aware provider selection (Phase 1.2a) ([#102]) [`8f7beab`]
- feat(cross-validation): group + finality mismatch metrics (Phase 1.3) ([#102]) [`9ed936d`]
- feat(cross-validation): disagreeing-providers header + validation-set scope guard ([#102]) [`0b2a3ac`]
- feat(cross-validation): per-group quorum (Phase 2.3) ([#102]) [`ff0b56a`]
- feat(cross-validation): close PRD-contract gaps + restore golangci-lint ([#102]) [`e3ae66e`]
- feat(rpcsmartrouter): warn when CV group-diversity rests on small groups ([#102]) [`f9cd04c`]
- feat(changelog): flag breaking changes in Highlights and Changes ([#136]) [`eed776e`]

#### Bug fixes
- fix(smart-router/health): stop gating selection on the per-socket isHealthy bit ([#100]) [`e868552`]
- refactor(smart-router/health): rip out the dead per-socket healthy bit & its debug reset ([#100]) [`4f5f208`]
- fix(smart-router/health): guard against a nil direct-connection element ([#100]) [`39bbe65`]
- fix(protocol/lavasession): increase max consecutive connection attempts from 5 to 50 ([#100]) [`1c67b2f`]
- fix(cross-validation): address Phase 0/1.1 review findings ([#102]) [`5d2cff8`]
- fix(cross-validation): tighten min-groups capacity, float parsing, guard fail-closed ([#102]) [`6bab82c`]
- fix(cross-validation): diverse-quorum selection, post-filter capacity, failure reason ([#102]) [`ae4c7ba`]
- fix(cross-validation): preserve response hashes + scope mismatch metric to outliers (Section 1.3) ([#102]) [`513409a`]
- fix(cross-validation): surface failure-reason header on request-time fail-fast ([#102]) [`cd4f5ad`]
- fix(cross-validation): set fail-fast reason on all-sessions-failed-consistency path ([#102]) [`e675c83`]
- fix(cross-validation): per-group selection prefers groups that can reach threshold ([#102]) [`6940865`]
- fix(cross-validation): per-group nil-reply early-exit + runtime capacity guards ([#102]) [`d8bafac`]
- fix(cross-validation): count request-time fail-fast in CV metrics; doc accuracy ([#102]) [`b6f7244`]
- fix(scripts): correct make target and config path in setup scripts ([#102]) [`584925f`]
- fix(scripts): point UC-1 test at reachable Lava mainnet endpoint; keep router up ([#102]) [`e2aea61`]
- fix(cross-validation): close 4 review findings (caller policy weakening, dropped pin, failure-reason + outlier mislabels) ([#102]) [`8e05be4`]
- fix(cross-validation): close review findings 5-7 (header MinGroups default, nil early-exit, fail-fast reason precedence) ([#102]) [`ffa38d6`]
- fix(relaycore): canonicalize response before cross-validation hashing ([#102]) [`b154d8b`]
- refactor(cross-validation): drop intPtr helper for Go 1.26 new(expr) ([#102]) [`22e51f9`]
- refactor(cross-validation): extract default group label into a constant ([#102]) [`8daa448`]
- refactor(lavasession): name the group-blind selection sentinels ([#102]) [`f030eca`]
- refactor(relaycore): name the no-cross-validation default knob value ([#102]) [`c81a8b5`]
- refactor(relaycore): extract selectQuorumWinner with unit tests ([#102]) [`1b929b0`]
- refactor(rpcsmartrouter): require integer cross-validation knobs ([#102]) [`5984395`]
- refactor(rpcsmartrouter): extract policyKeySeparator constant ([#102]) [`3687599`]
- refactor(rpcsmartrouter): filter policies by key prefix, not split-compare ([#102]) [`e33eac9`]
- fix(cross-validation): reconcile main's CV-mode hashing gate + test signatures after rebase ([#102]) [`f925ac0`]
- fix(cache): stop exposing /debug/pprof on the cache metrics endpoint ([#128]) [`10d8464`]
- fix(provider-optimizer): use per-endpoint block for sync-score (MAG-1748) ([#132]) [`d329b9c`]

#### Documentation updates
- docs(smart-router/health): note the nil-connection guard is defensive ([#100]) [`e57a7b2`]
- docs(smart-router/health): spell out the 5→50 backoff leniency tradeoff in the relay-path comment ([#100]) [`3a79c18`]
- docs(cross-validation): document CV config, headers, outlier behavior (Phase 2.4) ([#102]) [`61f94fa`]
- docs(cross-validation): tighten outlier-behavior accuracy ([#102]) [`6aaef2e`]
- docs(metrics): note structural fail-fasts in CV requests/failed totals ([#102]) [`e1a994f`]
- docs(relaycore): name common.DefaultProviderGroup in group-folding comments ([#102]) [`df053f3`]
- docs(lavasession,rpcsmartrouter): name common.DefaultProviderGroup in group comments ([#102]) [`ba03abc`]

#### Build process updates
- ci: validate PR artifact on dev-sim-prtests ([#123]) [`fe45489`]
- ci: rename dev-sim PR validation workflow ([#124]) [`171cfca`]
- ci: add dev-sim runtime PR validation ([#125]) [`9264e2a`]
- ci: add dev-prtests Kubernetes rollout validation ([#126]) [`a15654c`]
- ci: run automation readiness in PR gate ([#127]) [`58d64d5`]

#### Other work
- add support for send request as json format to websocket ([#68]) [`92c4013`]
- Enhance WebSocketDirectRPCConnection to support unique wire IDs for concurrent requests and ensure closed connections do not re-dial. Added tests for concurrent requests with the same caller ID and verified behavior after connection closure. ([#68]) [`7688977`]
- solana init enviroment scripts ([#100]) [`14c3dd9`]
- docs+test(cross-validation): correct mismatch metric text + glue test (Section 1.3 P3) ([#102]) [`4ffb1fa`]
- style(relaycore): gofmt import ordering in two files ([#102]) [`77c70f6`]
- chore!: remove geolocation entirely from smart-router ([#134]) [`afe1805`]
- chore!: drop deprecated static-providers/backup-providers config keys ([#135]) [`1735bdc`]

[#100]: https://github.com/magma-Devs/smart-router/pull/100
[#102]: https://github.com/magma-Devs/smart-router/pull/102
[#123]: https://github.com/magma-Devs/smart-router/pull/123
[#124]: https://github.com/magma-Devs/smart-router/pull/124
[#125]: https://github.com/magma-Devs/smart-router/pull/125
[#126]: https://github.com/magma-Devs/smart-router/pull/126
[#127]: https://github.com/magma-Devs/smart-router/pull/127
[#128]: https://github.com/magma-Devs/smart-router/pull/128
[#132]: https://github.com/magma-Devs/smart-router/pull/132
[#134]: https://github.com/magma-Devs/smart-router/pull/134
[#135]: https://github.com/magma-Devs/smart-router/pull/135
[#136]: https://github.com/magma-Devs/smart-router/pull/136
[#68]: https://github.com/magma-Devs/smart-router/pull/68
[`080d114`]: https://github.com/magma-Devs/smart-router/commit/080d1145122b549215697e67d4aec95efbdb1932
[`0b2a3ac`]: https://github.com/magma-Devs/smart-router/commit/0b2a3acc2adc417e6c38d4d8e1cc02577c88f861
[`10d8464`]: https://github.com/magma-Devs/smart-router/commit/10d84646ec374ad9b44b903fa350fa0fb2234ed2
[`14c3dd9`]: https://github.com/magma-Devs/smart-router/commit/14c3dd9066506ae80fdaf3fe17979a76e4dfa9f9
[`171cfca`]: https://github.com/magma-Devs/smart-router/commit/171cfca85fdd2f69e04bd75ca6fb11f1d3ba6b67
[`1735bdc`]: https://github.com/magma-Devs/smart-router/commit/1735bdc3f5b2f0c863361fe91308fdc848d687b8
[`1b929b0`]: https://github.com/magma-Devs/smart-router/commit/1b929b0b33529583aa59d2ee082bf19f307148bb
[`1c67b2f`]: https://github.com/magma-Devs/smart-router/commit/1c67b2f84ac632b9a61cbdd81d2985102461dc3e
[`22e51f9`]: https://github.com/magma-Devs/smart-router/commit/22e51f996206b2f8b3b9c8780d2a422083c7dda9
[`2388b8c`]: https://github.com/magma-Devs/smart-router/commit/2388b8c4e5903f14f11212e82e1fa33260cb5bf9
[`3687599`]: https://github.com/magma-Devs/smart-router/commit/3687599ad59a51109f8e60a69318d67ae2780712
[`39bbe65`]: https://github.com/magma-Devs/smart-router/commit/39bbe65a31899f3430506dd0e6e941306f1a4e0b
[`3a79c18`]: https://github.com/magma-Devs/smart-router/commit/3a79c189d413e4294ee4a2ac1101d5c1bb5805d6
[`47a337a`]: https://github.com/magma-Devs/smart-router/commit/47a337a696ae31f16f371ac8e587518b4fb143f8
[`4f5f208`]: https://github.com/magma-Devs/smart-router/commit/4f5f2080b4bc6446d18adf178ae72c21b83423c2
[`4ffb1fa`]: https://github.com/magma-Devs/smart-router/commit/4ffb1fa5a90a2780453b595bcb94b09265d9747c
[`513409a`]: https://github.com/magma-Devs/smart-router/commit/513409a13b102ea880800e0e1f90f5c0bb28936a
[`584925f`]: https://github.com/magma-Devs/smart-router/commit/584925f7124b941d3c8a62c1330fca5236dfb0e5
[`58d64d5`]: https://github.com/magma-Devs/smart-router/commit/58d64d5a4a412842b281f1282cf410c752383544
[`5984395`]: https://github.com/magma-Devs/smart-router/commit/598439515635b97978624c694ff3c861c33601fd
[`5d2cff8`]: https://github.com/magma-Devs/smart-router/commit/5d2cff8d022879758f6ffbb0f2ee5d368ca32490
[`61f94fa`]: https://github.com/magma-Devs/smart-router/commit/61f94fa6c4f480b065eb40310713bfa81d70a127
[`6940865`]: https://github.com/magma-Devs/smart-router/commit/6940865d83349824ead96e8a756eb9dfbd789035
[`6aaef2e`]: https://github.com/magma-Devs/smart-router/commit/6aaef2eef5443f2b6182c5808ee6d02cc5af1f97
[`6bab82c`]: https://github.com/magma-Devs/smart-router/commit/6bab82c9466327d2c480e03f1a0774f82623aa5d
[`7688977`]: https://github.com/magma-Devs/smart-router/commit/76889773f2f27fb50b928c6b91835111e9df26fd
[`77c70f6`]: https://github.com/magma-Devs/smart-router/commit/77c70f6f3e05dcd8df10b72479132824e35f9a0f
[`8daa448`]: https://github.com/magma-Devs/smart-router/commit/8daa4481cad35640ba887624445fb79a50d7d6bc
[`8e05be4`]: https://github.com/magma-Devs/smart-router/commit/8e05be4868b2f930d18638613d93a44d3fc31a62
[`8f7beab`]: https://github.com/magma-Devs/smart-router/commit/8f7beab7db905502c0708c5a4cf44fd59bf1e592
[`9264e2a`]: https://github.com/magma-Devs/smart-router/commit/9264e2a0439256863e8b32d6d45996d89fc7e819
[`92c4013`]: https://github.com/magma-Devs/smart-router/commit/92c4013e4ecd309e884401c4141c4ba8e30210db
[`9ed936d`]: https://github.com/magma-Devs/smart-router/commit/9ed936dc6161568ff140a86a3f3ae95425f7ff81
[`a15654c`]: https://github.com/magma-Devs/smart-router/commit/a15654c408a9fe2a184bf81bb4702f54d673d5bd
[`ae4c7ba`]: https://github.com/magma-Devs/smart-router/commit/ae4c7ba59bba0c1efa17b0ef4f11adf1765c3cc5
[`afe1805`]: https://github.com/magma-Devs/smart-router/commit/afe1805c9cd84166439ba12ee88720b6a11fc630
[`b154d8b`]: https://github.com/magma-Devs/smart-router/commit/b154d8bb1c7fc8c7a5d88887237609f4388355a7
[`b6f7244`]: https://github.com/magma-Devs/smart-router/commit/b6f7244c1b8f562dce807deec2349c2789b17b63
[`ba03abc`]: https://github.com/magma-Devs/smart-router/commit/ba03abc25e46d309be117df5dc75080abd5e8059
[`c81a8b5`]: https://github.com/magma-Devs/smart-router/commit/c81a8b537a6e380c5f7328aa449cf54380f21592
[`cd4f5ad`]: https://github.com/magma-Devs/smart-router/commit/cd4f5ad5c41bae3fffb909b524cd864b7d9aa0bb
[`d329b9c`]: https://github.com/magma-Devs/smart-router/commit/d329b9c87e090292caa6989c1c3f6f5ce759c363
[`d8bafac`]: https://github.com/magma-Devs/smart-router/commit/d8bafac0a30563e99449ade6e7488778e720d9de
[`d8f6808`]: https://github.com/magma-Devs/smart-router/commit/d8f6808a95066be7768dbd545b200cb84f017b29
[`df053f3`]: https://github.com/magma-Devs/smart-router/commit/df053f3ceb548bfca0dadd4a91d71a96b29ba540
[`e1a994f`]: https://github.com/magma-Devs/smart-router/commit/e1a994f3444179d6e0171c0220ec82cdac3e97c7
[`e2aea61`]: https://github.com/magma-Devs/smart-router/commit/e2aea612148bec489d6c93e95dcfb3171084a501
[`e33eac9`]: https://github.com/magma-Devs/smart-router/commit/e33eac95dd43f36dee7a2ecf86307c36c31d094a
[`e3ae66e`]: https://github.com/magma-Devs/smart-router/commit/e3ae66eb6710ca093cb3497e63336c43e3acbc07
[`e57a7b2`]: https://github.com/magma-Devs/smart-router/commit/e57a7b272da2994e43ad69962819ee785372387c
[`e675c83`]: https://github.com/magma-Devs/smart-router/commit/e675c831cc1fc0b5a2e1ec3c2a52991812755132
[`e868552`]: https://github.com/magma-Devs/smart-router/commit/e8685529a09a1dc6ac5848a7fd71b8c834046907
[`eed776e`]: https://github.com/magma-Devs/smart-router/commit/eed776ee3ee3191ac920067cf037f4ddeb6ecd08
[`f030eca`]: https://github.com/magma-Devs/smart-router/commit/f030eca0d8b241a07bf06b03dcdc8b486a3ccea1
[`f925ac0`]: https://github.com/magma-Devs/smart-router/commit/f925ac0d6fb8cfc50296f0dedb84ba875fde9c65
[`f9cd04c`]: https://github.com/magma-Devs/smart-router/commit/f9cd04c76a453b7dfd6b574ac9cffb7a4ecc456e
[`fe45489`]: https://github.com/magma-Devs/smart-router/commit/fe45489b54bbf04ac8a6b360aa3598f9c2bd68d5
[`ff0b56a`]: https://github.com/magma-Devs/smart-router/commit/ff0b56a4c88b50bf9b1dfcbec0ad78ac836a59f4
[`ffa38d6`]: https://github.com/magma-Devs/smart-router/commit/ffa38d678bd89df8f38c0099d3e4a024a3fe8e6d

## v1.0.2 — 2026-06-18

### Highlights

Smart Router v1.0.2 transitions telemetry to an OpenTelemetry usage pipeline, retiring legacy metrics flags like `--show-provider-address-in-metrics` and `--optimizer-qos-listen` while exposing optimizer scores by default. For live operations, the release introduces a debug-mode `/debug/logs` ring-buffer endpoint and `/debug/reset-pairing`, enabling SREs to rebuild provider pairings from configuration without restarting the gateway. Upstream routing behavior now correctly categorizes REST 501 responses as non-retryable `NodeError`s, and new polling-relief flags allow operators to slow chain-tracker polling and widen the consistency gate to reduce node load. Caching and validation mechanics have been adjusted to isolate explicit `lava-extension` directives into dedicated cache lanes and canonicalize payloads prior to cross-validation hashing. Finally, integrators can evaluate the gateway using a new self-contained Docker Compose stack that mounts the live `SR_CONFIG` directly into the local dashboard.

### Changes

#### New Features
- feat(docker): local compose stack + consolidate Dockerfiles under docker/ [`ebf77bd`]
- feat(config): EVM example configs (eth, multi-chain, cached) + specs [`0294fb4`]
- feat(smart-router/debug): add /debug/reset-pairing to rebuild pairing from config [`ec51de4`]
- feat: add debug-mode-only /debug/logs ring-buffer endpoint [`ffcc2aa`]
- feat(docker): self-contained router + dashboard compose stack [`d4be733`]
- feat(docker): mount live SR_CONFIG into the dashboard, drop static topology [`e767abf`]
- feat(docker): enable dashboard local mode (localhost:<port> URLs) [`2cee8ea`]
- feat(chaintracker): polling-relief flags to slow polling + widen consistency gate [`1a8a4a9`]
- feat: OTel usage pipeline + project-id rename + reporter-flow removal [`a9efff8`]

#### Bug fixes
- fix(rpcsmartrouter): read cache-be from config file too (via viper) [`c23ad57`]
- fix(rpcsmartrouter): treat REST 501 as a non-retryable NodeError (MAG-1576) [`a85ad21`]
- refactor(metrics): remove dead consumer & provider metrics managers [`e5e4c9d`]
- refactor(metrics): retire dead --show-provider-address-in-metrics flag [`aca7ec5`]
- refactor(metrics): always expose optimizer scores, drop --optimizer-qos-listen [`7cced3b`]
- refactor(metrics): remove dead lava_health_* metrics server [`717f8b5`]
- refactor(metrics): fold StartHTTPServer into NetworkAddress, gofmt fixup [`9d58730`]
- fix(cache): give explicit lava-extension directives their own cache lane [`793c0f8`]
- refactor(statetracker): drop dead ConsumerStateQuery stub [`f7d9a20`]
- refactor(statetracker): remove dead SpecUpdater + always-nil updater param [`363d232`]
- fix: guard debug-buffer sink logger with atomic.Pointer [`459ed13`]
- fix(rpcclient): adapt metrics.Timer to go-ethereum v1.17.0 [`efad27b`]
- refactor: remove standalone dead code (unreachable funcs) [`a8cca59`]
- refactor(common): remove zero-reference dead helpers [`29c0890`]
- fix(chaintracker): read polling-relief flags after config-file load [`3a811bc`]
- fix(lavasession): renew provider second chance after proven recovery [`69491ec`]
- fix(relaycore): canonicalize response before cross-validation hashing [`60826c1`]

#### Documentation updates
- docs(metrics): add metrics reference, link from README [`1dbdb20`]
- docs(metrics): drop the Removed families section [`5d7b609`]
- docs(metrics): fix stale source anchors and version comment [`03e62fe`]
- docs(compose): add LOCAL-COMPOSE.md + link it from the README [`36051f0`]
- docs(readme): document dashboard default credentials and overrides [`c079b6e`]
- docs(common): drop deleted LogCodedWarning from EmitErrorMetric doc [`fc436a8`]
- docs: document removed Kafka/reports/QoS-push flags as breaking [`cc15e8c`]
- docs: revert README OTel/Kafka migration table [`13d9718`]
- docs: update source-available license and README notes [`50ce5ab`]
- docs: move RELEASING and error-registry design into docs/ [`8aa0347`]

#### Build process updates
- ci: add manual PR gate artifact build workflow ([#109]) [`13f0333`]
- ci: add dev-sim-prtests preflight to PR gate workflow ([#111]) [`df90b81`]
- ci: add dev-sim-prtests preflight to PR gate workflow ([#112]) [`e207742`]
- ci: diagnose ssh key loading in preflight [`11722a3`]
- ci: remove stale binary preflight check [`830d443`]
- ci: validate PR artifact on dev-sim-prtests ([#122]) [`312dbd6`]

#### Other work
- Add Dependabot configuration [`3bd3380`]
- perf(relaycore): gate CV hashing on CrossValidation mode + guard trailing data [`3bbd313`]

[#109]: https://github.com/magma-Devs/smart-router/pull/109
[#111]: https://github.com/magma-Devs/smart-router/pull/111
[#112]: https://github.com/magma-Devs/smart-router/pull/112
[#122]: https://github.com/magma-Devs/smart-router/pull/122
[`0294fb4`]: https://github.com/magma-Devs/smart-router/commit/0294fb4d55448ca6c71febfb44bdc04957f9d030
[`03e62fe`]: https://github.com/magma-Devs/smart-router/commit/03e62fec43a3a4deedfc46d2df2918d563af8429
[`11722a3`]: https://github.com/magma-Devs/smart-router/commit/11722a308027ec77cbcdbf0a73cc47cb4b8a3a48
[`13d9718`]: https://github.com/magma-Devs/smart-router/commit/13d9718ea962e0bcd381b3f9ff1c15a40897036f
[`13f0333`]: https://github.com/magma-Devs/smart-router/commit/13f033344d14784697274b1707523645e81e92b6
[`1a8a4a9`]: https://github.com/magma-Devs/smart-router/commit/1a8a4a92c0ddcdaeaacf3233bf70d1c3beeef6bf
[`1dbdb20`]: https://github.com/magma-Devs/smart-router/commit/1dbdb209d3a1db4d75b6a72ee76194f6285b1270
[`29c0890`]: https://github.com/magma-Devs/smart-router/commit/29c089088cc0b5b5d9685a02c11e6b0c54c5d566
[`2cee8ea`]: https://github.com/magma-Devs/smart-router/commit/2cee8ea18c378afb5eb83f10000a98fa53848814
[`312dbd6`]: https://github.com/magma-Devs/smart-router/commit/312dbd63362f72d586f106b436ad96bad2cc4e5a
[`36051f0`]: https://github.com/magma-Devs/smart-router/commit/36051f0f1170e4d78fc254b37623d291bc69cac3
[`363d232`]: https://github.com/magma-Devs/smart-router/commit/363d232d6ba9cf782706a068dc2c1476177bec6a
[`3a811bc`]: https://github.com/magma-Devs/smart-router/commit/3a811bce94f73a46e7255007a4a094676fc9646b
[`3bbd313`]: https://github.com/magma-Devs/smart-router/commit/3bbd313bc4a1210631fc36a9d14efaf6b344888a
[`3bd3380`]: https://github.com/magma-Devs/smart-router/commit/3bd3380cd32a2f9e518b301d822eed4ac4a9f904
[`459ed13`]: https://github.com/magma-Devs/smart-router/commit/459ed132e8d476653040037c4f5947dc0030f2a2
[`50ce5ab`]: https://github.com/magma-Devs/smart-router/commit/50ce5aba4aac2ed0152fb0025c834c4a381579e6
[`5d7b609`]: https://github.com/magma-Devs/smart-router/commit/5d7b6090527e02dc5efdb028688b367a47579836
[`60826c1`]: https://github.com/magma-Devs/smart-router/commit/60826c1c8e299608b8b5db0ff612daf7e349b6ac
[`69491ec`]: https://github.com/magma-Devs/smart-router/commit/69491ecb153f830c26715b53fafa58fe45b33b09
[`717f8b5`]: https://github.com/magma-Devs/smart-router/commit/717f8b59f6d68e8e168dce8ae3ed1720574a95ba
[`793c0f8`]: https://github.com/magma-Devs/smart-router/commit/793c0f8b6105564ee1a084a65eced2538c6035f5
[`7cced3b`]: https://github.com/magma-Devs/smart-router/commit/7cced3b45fc7676d49001bf4db13ce07952bd97c
[`830d443`]: https://github.com/magma-Devs/smart-router/commit/830d4431f78f236780c736d5fc12e4b926be15fd
[`8aa0347`]: https://github.com/magma-Devs/smart-router/commit/8aa034754eb0467aa8a4946ebd9d6e77a4921a36
[`9d58730`]: https://github.com/magma-Devs/smart-router/commit/9d5873008ef43bdd2b77786fb1e433ccd21df214
[`a85ad21`]: https://github.com/magma-Devs/smart-router/commit/a85ad2197805630c41ebd7319d57c343a8f8f7c0
[`a8cca59`]: https://github.com/magma-Devs/smart-router/commit/a8cca59de915844589ec15e431cca09833a16015
[`a9efff8`]: https://github.com/magma-Devs/smart-router/commit/a9efff8849b7aecf956a3f447dbe6111a6ed43cc
[`aca7ec5`]: https://github.com/magma-Devs/smart-router/commit/aca7ec5138ec09178b322464935e64c4fa1b70af
[`c079b6e`]: https://github.com/magma-Devs/smart-router/commit/c079b6efaa10ed06401e79f195a429e10a91fe91
[`c23ad57`]: https://github.com/magma-Devs/smart-router/commit/c23ad5755c8e79417a1f4ebbe26333c6e94b58c2
[`cc15e8c`]: https://github.com/magma-Devs/smart-router/commit/cc15e8c3f03716aaf6734bb809a59515031f4196
[`d4be733`]: https://github.com/magma-Devs/smart-router/commit/d4be73327b3e732769e684ebe6b5df3e90efceba
[`df90b81`]: https://github.com/magma-Devs/smart-router/commit/df90b811450276767c941bfb20593bcdfb7b8500
[`e207742`]: https://github.com/magma-Devs/smart-router/commit/e2077428f54aa60e1de3d5d391e55184ebe439ae
[`e5e4c9d`]: https://github.com/magma-Devs/smart-router/commit/e5e4c9dffc550a33d2e3c1cd92e62166388aad2f
[`e767abf`]: https://github.com/magma-Devs/smart-router/commit/e767abf788a9f8a4b74ccef208b2a8a23ccba71b
[`ebf77bd`]: https://github.com/magma-Devs/smart-router/commit/ebf77bdba09dcc24d933f891f4906b7e907889de
[`ec51de4`]: https://github.com/magma-Devs/smart-router/commit/ec51de4ebf5cb437f7cbd60aa749555c7d49cf3a
[`efad27b`]: https://github.com/magma-Devs/smart-router/commit/efad27bb8789d1f668beecd4d337954857ab99f1
[`f7d9a20`]: https://github.com/magma-Devs/smart-router/commit/f7d9a20d53d81fd4a7564cb4d5ba7680bb81fa0c
[`fc436a8`]: https://github.com/magma-Devs/smart-router/commit/fc436a8a5a5ceb92b2240ffc3386463eb33266db
[`ffcc2aa`]: https://github.com/magma-Devs/smart-router/commit/ffcc2aa6e1c30fa4cf67f6793ec52a5ddc9b0f48

## v1.0.1 — 2026-06-02

### Highlights

Smart Router v1.0.1 introduces the `/debug/reset-scores` endpoint, allowing operators to manually clear chain-tracker and connection-health state without restarting the gateway. This release resolves multiple WebSocket lifecycle bugs, ensuring the router correctly echoes subscription IDs and explicitly replies to `eth_unsubscribe` requests instead of hanging the client connection. Upstream routing behavior now properly preserves native Tendermint response fields and prevents gRPC connection pools from tearing down prematurely during probe context cancellations. To support larger payloads and complex queries, the underlying `fasthttp` `ReadBufferSize` has been increased to 128 KiB, and the Lava-Retries mechanism now safely absorbs parallel-batch failures. Finally, skipping the synchronous boot epoch tick removes a blocking operation during initialization, allowing the router to accept traffic faster upon startup.

### Changes

#### New Features
- feat(rpcsmartrouter): clear chain-tracker and connection-health state on /debug/reset-scores ([#58]) [`50d969c`]

#### Bug fixes
- fix(rpcsmartrouter): echo WS subscribe id + fix unsubscribe race (MAG-1824) ([#43]) [`38c0635`]
- fix(rpcsmartrouter): preserve upstream Tendermint fields + clarify cleanup ownership ([#43]) [`35fd886`]
- fix(grpc-connector): don't tear down pool on probe ctx cancellation (MAG-1926) ([#54]) [`fa3aabe`]
- fix(rpcsmartrouter): skip the synchronous boot epoch tick (MAG-1926) ([#54]) [`d316750`]
- fix(rpcsmartrouter): absorb parallel-batch failures in Lava-Retries ([#55]) [`9d3f9fc`]
- fix(chainlib): reply to eth_unsubscribe instead of hanging the client ([#56]) [`f041230`]
- fix(chainlib): raise fasthttp ReadBufferSize to 128 KiB ([#59]) [`ebfbbab`]

#### Documentation updates
- docs(readme): make release badge auto-bump per release; switch to static URL ([#53]) [`554cfb1`]

[#43]: https://github.com/magma-Devs/smart-router/pull/43
[#53]: https://github.com/magma-Devs/smart-router/pull/53
[#54]: https://github.com/magma-Devs/smart-router/pull/54
[#55]: https://github.com/magma-Devs/smart-router/pull/55
[#56]: https://github.com/magma-Devs/smart-router/pull/56
[#58]: https://github.com/magma-Devs/smart-router/pull/58
[#59]: https://github.com/magma-Devs/smart-router/pull/59
[`35fd886`]: https://github.com/magma-Devs/smart-router/commit/35fd88625d2b64a7e3538b4871b58c8e068206ce
[`38c0635`]: https://github.com/magma-Devs/smart-router/commit/38c0635229057d0c1666aa9372e13967eef70e8d
[`50d969c`]: https://github.com/magma-Devs/smart-router/commit/50d969c67e96eed1bd3be8e149e0f782cc741a0c
[`554cfb1`]: https://github.com/magma-Devs/smart-router/commit/554cfb15879fbc7fd832c398d11c4619fc7537d4
[`9d3f9fc`]: https://github.com/magma-Devs/smart-router/commit/9d3f9fc15d6eb0c98c3b8da38871b1689dbec1e1
[`d316750`]: https://github.com/magma-Devs/smart-router/commit/d316750f4388958b58d81dc88a55f81ae92f8c71
[`ebfbbab`]: https://github.com/magma-Devs/smart-router/commit/ebfbbab8707da91e2aa0048592c334ffd3734176
[`f041230`]: https://github.com/magma-Devs/smart-router/commit/f041230612820b3a5c5d175a050a3bfa943daad8
[`fa3aabe`]: https://github.com/magma-Devs/smart-router/commit/fa3aabe901c8465f71c865d5e42048ddde8472df

## v1.0.0 — 2026-05-19

### Highlights

Smart Router v1.0.0 is the first stable release of Magma's multi-protocol RPC gateway: a single static binary (or multi-arch Docker image) that proxies JSON-RPC, REST, gRPC, and Tendermint RPC traffic against pools of QoS-scored upstream providers. Unlike generic L4/L7 load balancers, the router speaks each chain's wire format and applies RPC-aware semantics — caching by method and parameters, distinguishing transient timeouts from "block not yet produced", retrying against alternate providers on retryable failures, and backing off providers silently serving stale block data while still returning `200 OK`.

Release artifacts ship with a verifiable supply chain: the SHA-256 checksum file is cosign-keyless-signed via GitHub Actions OIDC and Sigstore (no keys to manage; verification recipe in `RELEASING.md`), the multi-arch Docker image lives at `ghcr.io/magma-devs/smart-router:v1.0.0`, and native binaries target `GOAMD64=v3` (Haswell+) and `GOARM64=v8.2` (ARMv8.2+) for modern hardware.
