# Changelog

All notable changes to smart-router are documented here. Each release
section is also published verbatim as the body of its GitHub release —
see [RELEASING.md](docs/RELEASING.md) for the workflow.

Versions follow [Semantic Versioning](https://semver.org/). Commit hashes
in `### Changes` link to the canonical commit on GitHub via reference-style
links collected at the bottom of each section.

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
