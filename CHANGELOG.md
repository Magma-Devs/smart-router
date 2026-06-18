# Changelog

All notable changes to smart-router are documented here. Each release
section is also published verbatim as the body of its GitHub release —
see [RELEASING.md](docs/RELEASING.md) for the workflow.

Versions follow [Semantic Versioning](https://semver.org/). Commit hashes
in `### Changes` link to the canonical commit on GitHub via reference-style
links collected at the bottom of each section.

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
