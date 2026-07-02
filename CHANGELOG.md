# Changelog

All notable changes to smart-router are documented here. Each release
section is also published verbatim as the body of its GitHub release —
see [RELEASING.md](docs/RELEASING.md) for the workflow.

Versions follow [Semantic Versioning](https://semver.org/). Commit hashes
in `### Changes` link to the canonical commit on GitHub via reference-style
links collected at the bottom of each section.

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
