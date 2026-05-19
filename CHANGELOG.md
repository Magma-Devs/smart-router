# Changelog

All notable changes to smart-router are documented here. Each release
section is also published verbatim as the body of its GitHub release —
see [RELEASING.md](RELEASING.md) for the workflow.

Versions follow [Semantic Versioning](https://semver.org/). Commit hashes
in `### Changes` link to the canonical commit on GitHub via reference-style
links collected at the bottom of each section.

## v1.0.0-rc1 — 2026-05-19

### Highlights

TODO: write a 2-4 sentence Highlights paragraph for this release.

### Changes

#### New Features
- feat: Added OTel tracing ([#13]) [`63e79d2`]
- feat(debug): implement /debug/reset-all endpoint to clear internal state [`ffa7b9c`]
- feat(rpcsmartrouter): expose CSM state-store sizes as Prometheus gauges [`4b495e7`]
- feat(metrics): add CSM state-store size gauges for Prometheus monitoring [`c08f340`]
- feat(rpcsmartrouter): flush external cache-be pod from /debug/reset-all (MAG-1764) [`07e7ed7`]
- feat(rpcsmartrouter): add lava-hedge-triggered header (MAG-1818) [`569e368`]
- feat(cli): print commit alongside version in `smartrouter version` [`a42ea02`]
- feat(release): consolidate Docker image build into GoReleaser [`573a9b8`]
- feat(release): auto-generate CHANGELOG + cosign-sign checksums on tag push [`2a964ed`]
- feat(release): link each Changes bullet to its PR; fix dropped-last-commit bug [`0a6e46f`]

#### Bug fixes
- fix(chain-tracker): filter providers by ApiInterface + refresh example configs ([#3]) [`0ecdf02`]
- fix(rpcsmartrouter): keep sessionsLatestBatch in sync after consistency filter [`939cdaa`]
- fix(relaypolicy): stop CrossValidation immediately on batch errors [`9f780db`]
- fix(rpcsmartrouter): release surviving sessions when CV guard fails fast [`f3d91d4`]
- fix(rpcsmartrouter): retry endpoint chain tracker startup [`1efe630`]
- fix(rpcsmartrouter): drop tracker state writes after RemoveTracker [`a49a58b`]
- fix: Duplicate provider names cause collateral exclusion [`ccf143d`]
- fix(rpcsmartrouter): wire --maximum-streams-per-connection to actual use sites [`a826a56`]
- fix(rpcsmartrouter): honor force-cache-refresh on retry path [`e20abbf`]
- fix(relaycore): preserve relay-timeout and debug-relay across archive re-parse [`5c82205`]
- fix(rpcsmartrouter): make Lava-Provider-Address ordering deterministic [`720f33f`]
- fix(rpcsmartrouter): propagate HTTP status into classifier message (MAG-1666) [`66e1cdc`]
- fix(build): drop retired cmd/lavap targets from Makefile [`b1bc1e3`]
- fix(rpcsmartrouter): flush seen-block cache from /debug/reset-* handlers [`3ed6b02`]
- fix: solana slot unit mismatch ([#21]) [`705d8be`]
- fix(rpcsmartrouter): drain blocked providers in /debug/reset-all [`7bc4caa`]
- fix(chainlib): detect malformed upstream responses for retry [`833e547`]
- fix(rpcsmartrouter): MAG-1871 keep resolver last in provider header [`9d99da6`]
- fix(release): wire version into release Dockerfile [`8772838`]
- fix(wire): rename Smartrouter-Version → Smart-Router-Version [`acf5098`]
- fix(release): set GOAMD64=v3 / GOARM64=v8.2 in release Dockerfile [`259663a`]
- fix(ci): reserve :latest for release pipeline, dev pushes use :main [`7f54689`]
- fix(release): migrate to goreleaser v2 dockers_v2 + archives.ids [`addfd19`]
- fix(release): use dockers_v2 annotations: for OCI metadata [`3ac4733`]
- fix(release): apply GOAMD64=v3 / GOARM64=v8.2 via builds[] not env [`d1f2e35`]
- fix(relaycore): serialize Consistency writes against ResetState [`5dda0f8`]
- fix(relaycore): close queued-writer + fresh-post-reset races in ResetState [`dbf66f2`]
- fix(rpcsmartrouter): make HTTP status authoritative for 4xx (MAG-1870) [`3a8b758`]
- fix(MAG-1866): JSON-RPC error response must be Object per spec §5.1, not stringified envelope [`3817f6e`]
- fix(MAG-1866): preserve spec compliance under ReturnMaskedErrors=true [`27d23a6`]
- fix(release): exclude self-tag in bumper + drop skip-CI marker on chore commit [`d5c35f7`]
- fix(release): use RELEASE_BOT_PAT for CHANGELOG push to protected main [`ce815ba`]

#### Documentation updates
- docs: remove stale cmd/lavap references from README [`e556c7e`]
- docs(release): document the release workflow and artifacts [`f7ecbf2`]
- docs(release): define semver semantics for smart-router [`f4a67c8`]
- docs(release): document local reproduction via goreleaser snapshot [`1846364`]
- docs(release): document Docker buildx setup for make snapshot [`1cbbfa9`]
- docs(readme): fill in gaps surfaced by release-pipeline validation [`95a2a0d`]
- docs: trim README sections and remove redundant Makefile aliases [`611560f`]
- docs(readme): What/Quick Start/How/Why structure, badges, SECURITY.md, full chain list [`b9474c0`]
- docs(readme): openui-style banner header + track docs/assets [`165d099`]

#### Build process updates
- ci: validate .goreleaser.yaml on every PR [`9d5175b`]

#### Other work
- added graceful shutdown [`bbbdd13`]
- resolved wg race, removed duplicated import, removed duplicated shutdown [`d37dceb`]
- if a provider is failing to set up, do not panic, add to retry list [`6b9c36c`]
- calling UpdateAllProviders inside a mutex to avoid race in epoch update [`7bad9d9`]
- build and publish smart-router image to ghcr ([#15]) [`be0db0b`]
- Simplify comments in direct_rpc_relay.go [`35d1d33`]
- remove unit test and bump version to 6.2.1 [`3969572`]
- [MAG-1889] added earlier cache reconnection [`9c12a7d`]
- [MAG-1889] use tracked chain tip for cache finalization [`4b2cbb2`]
- [MAG-1891] silence cobra --help dump on fatal startup error (noisy logs) [`e385afa`]
- Added support of backup providers to WS/gRPC [`8948f92`]
- handled dgm.grpcEndpoints[0] cases [`0b54350`]

[#13]: https://github.com/magma-Devs/smart-router/pull/13
[#15]: https://github.com/magma-Devs/smart-router/pull/15
[#21]: https://github.com/magma-Devs/smart-router/pull/21
[#3]: https://github.com/magma-Devs/smart-router/pull/3
[`07e7ed7`]: https://github.com/magma-Devs/smart-router/commit/07e7ed767a58d774ec77a27e92205236a4a364c6
[`0a6e46f`]: https://github.com/magma-Devs/smart-router/commit/0a6e46fd9f5ee6df8777b70e7928974ddd5f1b29
[`0b54350`]: https://github.com/magma-Devs/smart-router/commit/0b54350007d52dcbe00d609cd063ee1d98a60950
[`0ecdf02`]: https://github.com/magma-Devs/smart-router/commit/0ecdf0231506957f04e21f84bcae0161d62f77bb
[`165d099`]: https://github.com/magma-Devs/smart-router/commit/165d099a9556b330645f76539ddb3a6343e53943
[`1846364`]: https://github.com/magma-Devs/smart-router/commit/18463647457188e0a0617cfbd5ca4a6bf8c9b244
[`1cbbfa9`]: https://github.com/magma-Devs/smart-router/commit/1cbbfa92884daea8287ca9a8df5927f572c6df32
[`1efe630`]: https://github.com/magma-Devs/smart-router/commit/1efe630d2bfd7d3e91bf5dc40e844041c3e8e323
[`259663a`]: https://github.com/magma-Devs/smart-router/commit/259663a81edfb34195bc7ad4550c1e6bd7662c0a
[`27d23a6`]: https://github.com/magma-Devs/smart-router/commit/27d23a6321fd712813bd54a9cf641c5fbbb3410e
[`2a964ed`]: https://github.com/magma-Devs/smart-router/commit/2a964edeadce473382980f81ad60e62eaa553deb
[`35d1d33`]: https://github.com/magma-Devs/smart-router/commit/35d1d33008f6b2f4ef1cf23e2f5013f39a3ce7b2
[`3817f6e`]: https://github.com/magma-Devs/smart-router/commit/3817f6e4121819c3e064e870fe1c13a55632bbc1
[`3969572`]: https://github.com/magma-Devs/smart-router/commit/39695729ab67608131d07be9b21229fe401e73ba
[`3a8b758`]: https://github.com/magma-Devs/smart-router/commit/3a8b758eb7231dbe3d6e20dcc5fdcede39fb978e
[`3ac4733`]: https://github.com/magma-Devs/smart-router/commit/3ac47332b92c7f4dd606ebfa180aff5dbb861de4
[`3ed6b02`]: https://github.com/magma-Devs/smart-router/commit/3ed6b026d7f6e4c68d71c915baee8727a17ae3cc
[`4b2cbb2`]: https://github.com/magma-Devs/smart-router/commit/4b2cbb2424cdb855b02473e69c90baa94e0881f2
[`4b495e7`]: https://github.com/magma-Devs/smart-router/commit/4b495e725d773c5ce0cce7fa616038bd90927a58
[`569e368`]: https://github.com/magma-Devs/smart-router/commit/569e3683c44530d83cb0b2e81e8ff42a91ea58c9
[`573a9b8`]: https://github.com/magma-Devs/smart-router/commit/573a9b87475d0c89ec9d90359847287cb5bf9ac6
[`5c82205`]: https://github.com/magma-Devs/smart-router/commit/5c82205136ae994ef1527e21f3773bcc04b72933
[`5dda0f8`]: https://github.com/magma-Devs/smart-router/commit/5dda0f8e102151b539bba868a5886dc6dc193d6f
[`611560f`]: https://github.com/magma-Devs/smart-router/commit/611560fb1ae797a3965f29428d00407bb9c31d8e
[`63e79d2`]: https://github.com/magma-Devs/smart-router/commit/63e79d23118ccad2f9295ae40f0b2b5477490f6f
[`66e1cdc`]: https://github.com/magma-Devs/smart-router/commit/66e1cdcfef4729c9986736ddd191b4b2894fda92
[`6b9c36c`]: https://github.com/magma-Devs/smart-router/commit/6b9c36ca52ae5cc2257552510a5afb0a94b882cb
[`705d8be`]: https://github.com/magma-Devs/smart-router/commit/705d8beeaaef5d7896ede6e62d6506a0b4098ea0
[`720f33f`]: https://github.com/magma-Devs/smart-router/commit/720f33fbc4331e98017f9f75cc2df79c99ab509b
[`7bad9d9`]: https://github.com/magma-Devs/smart-router/commit/7bad9d95e5a355d688a51261a0443e0d3295ffa6
[`7bc4caa`]: https://github.com/magma-Devs/smart-router/commit/7bc4caab718bd87c651ce5a69a9d3e2f6a8cf6c6
[`7f54689`]: https://github.com/magma-Devs/smart-router/commit/7f54689bbdb5aa50cfb69614777678867c5924ba
[`833e547`]: https://github.com/magma-Devs/smart-router/commit/833e547f8f613c641137ee8f6407af1a68f7641e
[`8772838`]: https://github.com/magma-Devs/smart-router/commit/8772838b68d8616bd16ae0d62c37599e6794d594
[`8948f92`]: https://github.com/magma-Devs/smart-router/commit/8948f92e3d6e983829e97219624a853d4e2aadb6
[`939cdaa`]: https://github.com/magma-Devs/smart-router/commit/939cdaaa809174fc85bbf7d4993cd66872eb92ce
[`95a2a0d`]: https://github.com/magma-Devs/smart-router/commit/95a2a0d017643a7f4cd75b8205b1484dabc4ed64
[`9c12a7d`]: https://github.com/magma-Devs/smart-router/commit/9c12a7d7594ea42921e9eba92a086dbe481f179a
[`9d5175b`]: https://github.com/magma-Devs/smart-router/commit/9d5175b5c7ffc3cf58d220771f59a4592fe921bc
[`9d99da6`]: https://github.com/magma-Devs/smart-router/commit/9d99da6f8e6f2f5d5862fc142a036fc0986b4656
[`9f780db`]: https://github.com/magma-Devs/smart-router/commit/9f780db599e1f66493fa890dddf87cc126bea13a
[`a42ea02`]: https://github.com/magma-Devs/smart-router/commit/a42ea024c45764d3a7c5a1c194a33f98529f0a19
[`a49a58b`]: https://github.com/magma-Devs/smart-router/commit/a49a58b32280d99cf6eb1e2bdb9046133d7fa39a
[`a826a56`]: https://github.com/magma-Devs/smart-router/commit/a826a56c6ea32ee6454478ab4c878623741af875
[`acf5098`]: https://github.com/magma-Devs/smart-router/commit/acf509846aee59e1b6e52cea5eb6436fbb178ae1
[`addfd19`]: https://github.com/magma-Devs/smart-router/commit/addfd1942188908e950a1e6da9189b5a9c801974
[`b1bc1e3`]: https://github.com/magma-Devs/smart-router/commit/b1bc1e39f1916bdfc6073f7b712aa811c689a497
[`b9474c0`]: https://github.com/magma-Devs/smart-router/commit/b9474c0a5f092030496685424f131747c714e93a
[`bbbdd13`]: https://github.com/magma-Devs/smart-router/commit/bbbdd1321e964749dcf777f02fd193d37216464b
[`be0db0b`]: https://github.com/magma-Devs/smart-router/commit/be0db0b413cf82e14b27c9a9be8f0d90669dd922
[`c08f340`]: https://github.com/magma-Devs/smart-router/commit/c08f3405aa529e9190d0a41e6fdad3451a8ce5ea
[`ccf143d`]: https://github.com/magma-Devs/smart-router/commit/ccf143da01867f1be0fd16e840ff97f4a3071e1e
[`ce815ba`]: https://github.com/magma-Devs/smart-router/commit/ce815ba5f366085818ce13ea3f7519f77bd87851
[`d1f2e35`]: https://github.com/magma-Devs/smart-router/commit/d1f2e35e0ffacc0432fe5f01b2fdb8dad5d0d8ed
[`d37dceb`]: https://github.com/magma-Devs/smart-router/commit/d37dceb156b258c29ee7df9693baa0f7b9f73e5a
[`d5c35f7`]: https://github.com/magma-Devs/smart-router/commit/d5c35f7f655fca04568893e17aa07e397ca6bc61
[`dbf66f2`]: https://github.com/magma-Devs/smart-router/commit/dbf66f2d3397477061bd4475fa45379c7a60115c
[`e20abbf`]: https://github.com/magma-Devs/smart-router/commit/e20abbf1b98055f42d7fa78eab1ae3c689c8fdbb
[`e385afa`]: https://github.com/magma-Devs/smart-router/commit/e385afa8932074bc60ea37b47d5b1f07a8315ad9
[`e556c7e`]: https://github.com/magma-Devs/smart-router/commit/e556c7e60d68fcb6b4d29e70deebe4feb5ba81b0
[`f3d91d4`]: https://github.com/magma-Devs/smart-router/commit/f3d91d4d4d052d1ac31ff2716d86eae695e02d92
[`f4a67c8`]: https://github.com/magma-Devs/smart-router/commit/f4a67c812832c67c83140ed22ca3ccca7aca8aee
[`f7ecbf2`]: https://github.com/magma-Devs/smart-router/commit/f7ecbf22bca36a4c1b40ee1ade0429da95c6a7d9
[`ffa7b9c`]: https://github.com/magma-Devs/smart-router/commit/ffa7b9ca47ed24942c2b16a520f027a31834841f
