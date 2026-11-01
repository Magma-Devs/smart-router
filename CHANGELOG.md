# Changelog

All notable changes to smart-router are documented here. Each release
section is also published verbatim as the body of its GitHub release —
see [RELEASING.md](docs/RELEASING.md) for the workflow.

Versions follow [Semantic Versioning](https://semver.org/). Commit hashes
in `### Changes` link to the canonical commit on GitHub via reference-style
links collected at the bottom of each section.

## v1.6.5 — 2026-10-29

### Highlights

**Readiness follows reachability.** A router's `/readyz` and `smartrouter_overall_health` now say whether it reaches its upstreams. Each chain+interface's health check ends in one of three verdicts:

- **No upstream answers:** every try timed out, failed to connect, or had no upstream to go to. The chain turns unhealthy. A client relay that no upstream answered brings the check forward to within `--relays-health-unhealthy-interval` (default `15s`). A chain+interface that turns unhealthy brings forward the checks of the router's other interfaces too. A router that loses its egress no longer waits out the 5-minute healthy cadence, and an idle interface no longer holds it Ready.
- **An upstream answers with an error, an error status or a rate limit:** nothing changes. Every replica shares its upstreams and gets the same answer, so it is no reason to take one out of rotation. The answer does not prove the chain serves either, so a chain that is already unhealthy stays unhealthy.
- **An upstream answers with a result:** the chain is healthy. Earlier releases passed the check on any reply, including a gRPC call that hit the router's own deadline, which kept a router without egress Ready indefinitely.

**Who notices:** operators. An upstream outage that every replica shares no longer empties a router's Service, and `smartrouter_overall_health` stays `1` through it, so alert on the error share for that, not on readiness. The `Health Check Interval Check` log line gains `verdict` (`healthy`, `inconclusive` or `unreachable`), and its `health result` is now the chain's health after the check. `docs/METRICS.md` ("When they change") states the rule. This is MAG-4617, MAG-4639 and MAG-4641. MAG-4641 landed inside #584's commit, so its line below was added by hand.

Release v1.6.5 also refines upstream health verification and router readiness by ensuring each node URL is verified over its own isolated connection and each health-check attempt uses a dedicated relay processor. Operators can also track exactly which configured providers failed verification—and which specific check triggered the failure—through newly published metrics. Finally, for gRPC traffic, this release corrects how connection failures and timeouts are classified during upstream routing. A call that hits the router's internal deadline is now properly recorded as a timeout, and a refused connection is marked as unreachable, preventing the router from misreporting either event as a valid answer from the upstream node.

### Changes

#### New Features
- feat(metrics): publish which configured providers failed verification, and which check (MAG-4594) ([#572]) [`316fad1`]
- feat(readiness): readiness follows reachability, three-valued probe (MAG-4641) ([#586]) [`bb43ae9`]
  - added by hand: #586 merged into #584's branch and reached `main` inside #584's squash commit, so the generator listed no line for it. This is the readiness rule in the Highlights.

#### Bug fixes
- fix: verify every node url over its own connection (MAG-4231) ([#463]) [`dc512c7`]
- fix(readiness): turn NotReady when a router's relays all fail (MAG-4617) ([#581]) [`b6d951c`]
  - its rule, that any reply but a result fails the check, was replaced before this release by MAG-4641: see the Highlights.
- fix(grpc): a call that hits the router's own deadline is a timeout, not the node's answer (MAG-4625) ([#583]) [`ec999d5`]
- fix(grpc): a refused connection is unreachable, not the node's answer (MAG-4355) ([#585]) [`f4dd073`]
- fix(readiness): give each health-check try its own relay processor (MAG-4639) ([#584]) [`bb43ae9`]
  - this commit also carries MAG-4641 ([#586]).

#### Documentation updates
- docs(changelog): say v1.6.4 drops the two distrusted roots, and correct v1.6.2 (MAG-4596) ([#575]) [`ec7a319`]

[#463]: https://github.com/magma-Devs/smart-router/pull/463
[#572]: https://github.com/magma-Devs/smart-router/pull/572
[#575]: https://github.com/magma-Devs/smart-router/pull/575
[#581]: https://github.com/magma-Devs/smart-router/pull/581
[#583]: https://github.com/magma-Devs/smart-router/pull/583
[#584]: https://github.com/magma-Devs/smart-router/pull/584
[#585]: https://github.com/magma-Devs/smart-router/pull/585
[#586]: https://github.com/magma-Devs/smart-router/pull/586
[`316fad1`]: https://github.com/magma-Devs/smart-router/commit/316fad1e8e9e4af246a97ebf5ca71393de8d6fe6
[`b6d951c`]: https://github.com/magma-Devs/smart-router/commit/b6d951caf1c28c3e632b915b54cda1ea3d39af0f
[`bb43ae9`]: https://github.com/magma-Devs/smart-router/commit/bb43ae96b4671a2d13bba2c22f948e486082e495
[`dc512c7`]: https://github.com/magma-Devs/smart-router/commit/dc512c7aaf617bc07570ba3c246fb4a08277f511
[`ec7a319`]: https://github.com/magma-Devs/smart-router/commit/ec7a319f20b48915ec28d3c6fea618e5e3f0b725
[`ec999d5`]: https://github.com/magma-Devs/smart-router/commit/ec999d5045e8525cf2e161c5c6ee7a13244ef564
[`f4dd073`]: https://github.com/magma-Devs/smart-router/commit/f4dd0733f32a17aaa5334d4bebb981f9df725cd9

## v1.6.4 — 2026-10-23

### Highlights

**Security: the image no longer trusts the two roots removed over the eMudhra compromise.** v1.6.4 builds on `gcr.io/distroless/static-debian13:debug`, whose trust store (`ca-certificates 20250419+deb13u1`) no longer contains *emSign ECC Root CA - G3* or *AAA Certificate Services*, the two roots Debian and Mozilla removed over the eMudhra intermediate compromise. The previous base, `static-debian12`, is no longer updated by distroless, so v1.6.3 and every earlier image still trust both. **Correction to the v1.6.2 notes:** they say the fix is a rebuild "not a change to the image's CA bundle". The root removals had already been published, and this release carries them. The Go 1.26.10 check from v1.6.2 still rejects any chain through the eight compromised intermediates; the trust store now refuses chains through those two roots as well, including chains through eMudhra CAs outside Go's list. A host whose certificate chain ends only at one of the two roots now fails TLS verification, as it already does in Mozilla, Chrome and Debian. The same fix for the v1.5 line is v1.5.10.

The release also introduces a breaking change to error handling and event logging that requires operators to update their telemetry pipelines to `relay_end` schema 2. The `status` field for a failed relay no longer writes `0`; it now records the upstream's actual HTTP status code, or `502` when all transport attempts fail. Additionally, the router no longer intercepts errors to return a generic HTTP 500 text response, meaning client applications will now receive the upstream node's exact status code and response body. Integrators must consult `docs/RELAY-END-EVENT.md` to verify the schema 2 contract and update clients to parse upstream-native error payloads. Beyond these protocol corrections, this release adjusts the failover routing logic to include a `lastChanceBorrow` condition, which permits the router to lend a head-stalled upstream URL when no other healthy nodes are available to serve the request.

### Changes

#### ⚠ Breaking changes
- fix(protocol)!: return the upstream's own error to the client instead of the router's text (MAG-4218) ([#551]) [`8645963`]
  - relay_end is schema 2. status on a failed relay is the status the client gets: the upstream's own status when an upstream answered, 502 when every attempt failed in transport, where schema 1 wrote 0. docs/RELAY-END-EVENT.md is the contract for schema 2 and lists the versions. The reply the client reads on this path changes too: the node's status and body instead of HTTP 500 with the router's text.

#### Bug fixes
- fix(protocol)!: return the upstream's own error to the client instead of the router's text (MAG-4218) ([#551]) [`8645963`]
- fix(lavasession): the last chance lends a head-stalled URL when nothing else can serve (MAG-4589) ([#570]) [`9eda1ae`]
- refactor(lavasession): name the last-chance condition lastChanceBorrow (MAG-4589) ([#570]) [`926b0c1`]
- fix(docker): move the runtime base to distroless static-debian13 (MAG-4596) ([#573]) [`1a74447`]
  - this is the security fix above: the image's trust store no longer contains emSign ECC Root CA - G3 or AAA Certificate Services.

[#551]: https://github.com/magma-Devs/smart-router/pull/551
[#570]: https://github.com/magma-Devs/smart-router/pull/570
[#573]: https://github.com/magma-Devs/smart-router/pull/573
[`1a74447`]: https://github.com/magma-Devs/smart-router/commit/1a744477d55b53922d49a5a44ef10c0c350d494e
[`8645963`]: https://github.com/magma-Devs/smart-router/commit/8645963eaa9d3c21ceb1d8d7033526434879de96
[`926b0c1`]: https://github.com/magma-Devs/smart-router/commit/926b0c1be737cf4bbfaaeefdbbb13d59d85b3d1d
[`9eda1ae`]: https://github.com/magma-Devs/smart-router/commit/9eda1ae4b974a1632ec9a5771f4e25d47ca851b1

## v1.6.3 — 2026-10-22

### Highlights

Smart Router v1.6.3 expands the `relay-end` event payload with deeper transaction and caller telemetry for production observability. Integrators can now track submitted transactions directly via the new `tx_hash`, `tx_status`, and `tx_error` fields, while identity fields like `customer`, `request_id`, `task_id`, and `tx_id` map upstream requests back to the originating client. The event payload also introduces request and reply sizes, block heights, and the upstream `node_code`, complemented by two new narrative log lines written per relay when the router operates at the `Debug` log level. Finally, this release resolves REST routing bugs by ensuring REST health checks correctly send their GET template as the URL without a body, and by applying a unified body-method rule for REST relays that preserves the original API name when given an empty template.

### Changes

#### New Features
- feat(relay-end): name the transaction a submit sends (MAG-4455) ([#556]) [`07bf136`]
- feat(relay-end): name the customer and the caller's ids (MAG-4456) ([#556]) [`2d42393`]
- feat(relay-end): add request and reply sizes, blocks and node codes (MAG-4457) ([#556]) [`10f1ae0`]
- feat(logs): write two per-relay narrative lines at Debug (MAG-4054) ([#569]) [`5144af3`]

#### Bug fixes
- fix(rpcsmartrouter): a REST health check sends its GET template as the URL, with no body (MAG-4516) ([#561]) [`d44e0f6`]
- fix(rpcsmartrouter): one body-method rule for REST relays, and an empty template keeps the api name (MAG-4516) ([#561]) [`7373514`]

#### Documentation updates
- docs(changelog): say v1.6.2 distrusts the compromised eMudhra intermediates (MAG-4522) ([#564]) [`c3a120f`]
- docs(relay-end): document tx_hash, tx_status and tx_error by chain (MAG-4455) ([#556]) [`8095142`]
- docs(relay-end): document customer, request_id, task_id and tx_id (MAG-4456) ([#556]) [`f8b606f`]
- docs(relay-end): document sizes, blocks and node_code (MAG-4457) ([#556]) [`4d7d9bf`]

[#556]: https://github.com/magma-Devs/smart-router/pull/556
[#561]: https://github.com/magma-Devs/smart-router/pull/561
[#564]: https://github.com/magma-Devs/smart-router/pull/564
[#569]: https://github.com/magma-Devs/smart-router/pull/569
[`07bf136`]: https://github.com/magma-Devs/smart-router/commit/07bf136ef06e9e192a56a2f1739ce93440e04e2a
[`10f1ae0`]: https://github.com/magma-Devs/smart-router/commit/10f1ae0d18c70b7ee5f45a755aef5b13414bc05c
[`2d42393`]: https://github.com/magma-Devs/smart-router/commit/2d4239333b9b28833da99c79a28132d216b8f787
[`4d7d9bf`]: https://github.com/magma-Devs/smart-router/commit/4d7d9bf8b41ff30ac468945969bb175d1b3d677e
[`5144af3`]: https://github.com/magma-Devs/smart-router/commit/5144af32b829d145338f635e1765f7a09df104ca
[`7373514`]: https://github.com/magma-Devs/smart-router/commit/7373514c19f6e9c9057fc116ef44375aa09a753a
[`8095142`]: https://github.com/magma-Devs/smart-router/commit/8095142a85bca67e3e95d250667f1639c2de5a14
[`c3a120f`]: https://github.com/magma-Devs/smart-router/commit/c3a120f0d6b79e991cb2cf8a954a774221fc6616
[`d44e0f6`]: https://github.com/magma-Devs/smart-router/commit/d44e0f69e18abbd10c81cf9101d09d79f5361c43
[`f8b606f`]: https://github.com/magma-Devs/smart-router/commit/f8b606f2c63cd77a3a508596e86cd6029547b1d0

## v1.6.2 — 2026-10-21

### Highlights

**Security: v1.6.2 distrusts the compromised eMudhra intermediate CAs.** It is built on Go 1.26.10, which makes `crypto/x509` reject any certificate chain that contains one of eight eMudhra intermediate CA certificates whose private keys were compromised ([GO-2026-6809](https://pkg.go.dev/vuln/GO-2026-6809), CVE-2026-111943). With one of those keys, anyone who can intercept the router's TLS connection to an upstream node or a TLS cache can present a certificate for that host that every earlier release accepts, then read or alter the relayed traffic, including upstream API keys carried in URLs or headers. v1.6.2 refuses such a certificate whichever trust store it verifies against. The check is compiled into the binary, so the fix is this rebuild, not a change to the image's CA bundle. Connections pinned to a private CA (an endpoint's `cacert-pem`, or the RESP cache's `tls.ca-file`) were never exposed. Every earlier release is affected: move to v1.6.2, or to v1.5.9 on the 1.5 line.

The release also introduces three breaking changes affecting GraphQL request validation and write broadcasting during rate limits. First, the router now outright refuses GraphQL requests that expand to more than 300 root fields or nest fragments deeper than 32 levels at the root, rather than relaying them to upstream nodes. Second, when a GraphQL request combines a root under an enabled cross-validation policy with one under a `forbid-caller-cv` policy, the router now enforces cross-validation under the enabled policy without the caller's headers, rather than routing the request to a single provider. Third, write broadcasting behavior during rate-limit hold-offs has changed: writes are now broadcast to all enabled providers regardless of their hold-off status, meaning `lava-fast-tx-participants` will list every provider, recovered providers may serve the winning reply indicated by the `Lava-Provider-Address` header, and still-limited vendors will receive the write and return a 429. Beyond these breaking changes, this release fixes an issue where a single block-less element would prevent the router from caching batch or multi-root GraphQL requests.

### Changes

#### ⚠ Breaking changes
- fix(chainlib)!: bound the root fields a GraphQL request expands to (MAG-4360) ([#549]) [`d3b38e6`]
  - a GraphQL request that expands to more than 300 root fields, or nests fragments more than 32 deep at its root, is refused by the router instead of being relayed to a node.
- fix(rpcsmartrouter)!: an enabled cross-validation policy applies beside a forbidden GraphQL root (MAG-4361) ([#550]) [`0d38f68`]
  - a GraphQL request that selects a root under an enabled cross-validation policy together with a root under a forbid-caller-cv policy is cross-validated under the enabled policy, without the caller's headers, instead of being served by a single provider.
- fix(lavasession)!: broadcast a write to every provider during a rate-limit hold-off (MAG-4388) ([#538]) [`52c429c`]
  - during a rate-limit hold-off a write is broadcast to every enabled provider, held off or not, instead of only to the providers that are not held off (or, with every provider held off, to the soonest to expire). lava-fast-tx-participants lists every provider again, a held-off provider that has recovered can be the one whose reply is returned (Lava-Provider-Address), and a vendor that is still rate-limiting receives the write and answers it with a 429.

#### Bug fixes
- fix(chainlib)!: bound the root fields a GraphQL request expands to (MAG-4360) ([#549]) [`d3b38e6`]
- fix(rpcsmartrouter)!: an enabled cross-validation policy applies beside a forbidden GraphQL root (MAG-4361) ([#550]) [`0d38f68`]
- fix(chainlib): a block-less element no longer makes a batch or multi-root GraphQL request uncacheable (MAG-4381) ([#547]) [`871d770`]
- fix(lavasession)!: broadcast a write to every provider during a rate-limit hold-off (MAG-4388) ([#538]) [`52c429c`]

#### Build process updates
- build(deps): move to Go 1.26.10 so govulncheck passes ([#560]) [`2639009`]
  - this is the security fix above: Go 1.26.10 rejects any certificate chain through the eight compromised eMudhra intermediate CAs (GO-2026-6809).

[#538]: https://github.com/magma-Devs/smart-router/pull/538
[#547]: https://github.com/magma-Devs/smart-router/pull/547
[#549]: https://github.com/magma-Devs/smart-router/pull/549
[#550]: https://github.com/magma-Devs/smart-router/pull/550
[#560]: https://github.com/magma-Devs/smart-router/pull/560
[`0d38f68`]: https://github.com/magma-Devs/smart-router/commit/0d38f68b0b54d6e1448408f581d15d99f5a59158
[`2639009`]: https://github.com/magma-Devs/smart-router/commit/263900903c76671f52cdc54278c92fcedca8b743
[`52c429c`]: https://github.com/magma-Devs/smart-router/commit/52c429c2cdc5c6b58e27bee3b2c57cfc968b8dc0
[`871d770`]: https://github.com/magma-Devs/smart-router/commit/871d770fa0796ca584e89c5b3dd206d9f95aa2f9
[`d3b38e6`]: https://github.com/magma-Devs/smart-router/commit/d3b38e672e0486d9c38dcfdac6105e3da8802594

## v1.6.1 — 2026-10-20

### Highlights

Smart Router v1.6.1 introduces four breaking changes to gRPC TLS validation and GraphQL error handling that operators must address before upgrading. First, gRPC TLS settings are now strictly enforced across all dialers: a `cacert-pem` that is unreadable or fails to verify the dialed host will now refuse the connection instead of falling back to system roots, requiring operators to provide valid certificates or explicitly set the `allow-insecure` flag for self-signed nodes. Second, GraphQL routing changes three behaviors: queries nested deeper than 256 levels are outright refused, 2xx responses lacking both `data` and `errors` objects are scored as node failures rather than valid answers, and 4xx responses containing JSON-RPC error envelopes trigger a retry on a new provider while filing as `NODE_METHOD_NOT_ALLOWED` in `smartrouter_errors_total` instead of returning immediately to the client. Beyond these breaking changes, this release consolidates request telemetry into a single `relay_end` log event that records the chain name, skipped upstreams, stale fallbacks, and a detailed history of every retry attempt with its send time and retryable verdict. Finally, the router optimizes upstream connection management by honoring hold-offs from JSON-RPC 429 rate limit bodies, sharing a single gRPC stream per subscription, and actively dropping dead WebSocket connections and slow subscription clients.

### Changes

#### ⚠ Breaking changes
- fix(common)!: one TLS config and dial builder for every gRPC dialer, one reply-header filter for every tier (MAG-4279) ([#524]) [`16a42b9`]
  - the gRPC TLS settings (cacert-pem, cert-pem/key-pem, allow-insecure) now mean the same thing on every dialer, and cacert-pem is enforced. The unary connector verifies the server against cacert-pem: it used to skip verification once a CA was loaded, so a cacert-pem that did not sign the server, or whose certificate does not name the dialled host or IP, now refuses the dial (set allow-insecure for a self-signed development node). Subscription streams now verify against cacert-pem and present cert-pem/key-pem; they used to read allow-insecure alone. cert-pem/key-pem is presented even when cacert-pem is set. A cacert-pem that cannot be loaded (unreadable, or holding no PEM certificate) now fails every dial instead of falling back to the system roots.
- fix(chainlib)!: a JSON-RPC error envelope under a caller's status on a GraphQL endpoint is the gateway refusing the route (MAG-4379) ([#545]) [`5eff4a0`]
  - on the graphql interface, a 4xx reply carrying a JSON-RPC error envelope under a status the registry files as the caller's (400, 406, 411, 412, 414-417, 422, 428, 431) is no longer returned to the client as the answer. It is retried on another provider, so the body, Lava-Retries and Lava-Provider-Address the client gets change, and it is filed as NODE_METHOD_NOT_ALLOWED instead of USER_INVALID_REQUEST in smartrouter_errors_total.
- fix(chainlib)!: a GraphQL 2xx with neither data nor errors is a node error, not the answer (MAG-4369) ([#534]) [`f14fdac`]
  - a GraphQL reply under a 2xx that carries neither a data object nor errors is no longer the answer. A read is retried on another provider, so the body, Lava-Retries and Lava-Provider-Address the client gets change; a write waits for another provider's answer instead of returning the gateway's body at once; and the endpoint that sent it is scored as failing instead of having its failure count reset.
- fix(chainlib)!: bound how deeply a GraphQL document may nest (MAG-4359) ([#548]) [`150d616`]
  - a GraphQL request nested more than 256 levels deep is refused by the router as a request it cannot parse, instead of being relayed to a node.

#### New Features
- feat(utils): a GUID carries the second it was minted (MAG-4399) ([#539]) [`f017c87`]
- feat(utils): counter, not random bits, below the GUID second (MAG-4399) ([#539]) [`16a167f`]
- feat(logs): one relay_end event per relay, at every log level (MAG-4400) ([#543]) [`9fc7505`]
- feat(logs): name the chain on relay_end (MAG-4400) ([#543]) [`9e23602`]
- feat(logs): list relay_end attempts whenever a relay took more than one (MAG-4400) ([#543]) [`4a7f9ae`]
- feat(logs): give each relay_end attempt its batch and send time (MAG-4400) ([#543]) [`4d9a48c`]
- feat(logs): write each failed attempt's retryable verdict on relay_end (MAG-4400) ([#543]) [`f5a7f0a`]
- feat(logs): give each WebSocket frame its own GUID (MAG-4400) ([#543]) [`552fd47`]
- feat(logs): name skipped upstreams and stale fallbacks on relay_end (MAG-4400) ([#543]) [`ec71d3a`]
- feat(logs): read a Cosmos transaction's code into relay_end (MAG-4400) ([#543]) [`af0da11`]
- feat(logs): write relay_end for subscription requests (MAG-4400) ([#543]) [`7058ea7`]

#### Bug fixes
- fix: JSON-RPC 429 bodies are rate limits; tracker honours hold-offs (MAG-4165) ([#488]) [`8ad0ff6`]
- fix(rpcsmartrouter): retire dead WebSocket connections, drop slow subscription clients, and share one gRPC stream per subscribe (MAG-4242) ([#519]) [`9d1c7fc`]
- fix(lavasession): leave the endpoint alone on a plain session acquisition (MAG-4242) ([#520]) [`027cc1c`]
- fix: eleven behaviour drifts between duplicated code paths (MAG-4278) ([#523]) [`f4ebf95`]
- fix(common)!: one TLS config and dial builder for every gRPC dialer, one reply-header filter for every tier (MAG-4279) ([#524]) [`16a42b9`]
- fix(lavasession): one descriptor resolver for the unary connection and the subscription stream pool (MAG-4279) ([#525]) [`409e5cd`]
- fix(rpcsmartrouter): read no tip out of a GraphQL user relay's reply (MAG-4373) ([#535]) [`e7c6e4c`]
- fix(holdoff): count a 429 burst as one strike (MAG-4386) ([#536]) [`d3413bb`]
- fix(chainlib): type a GraphQL RESOURCE_EXHAUSTED head-poll answer as a rate limit (MAG-4378) ([#544]) [`fc5d729`]
- fix(chainlib)!: a JSON-RPC error envelope under a caller's status on a GraphQL endpoint is the gateway refusing the route (MAG-4379) ([#545]) [`5eff4a0`]
- fix(metrics): keep the customer label valid UTF-8, and count an empty project-id as no customer ([#553]) [`64cb8f1`]
- refactor(logs): name the event writer utils.LogEvent; lint fixes (MAG-4400) ([#543]) [`dbb6ff1`]
- fix(logs): write relay_end stateful as a JSON boolean (MAG-4400) ([#543]) [`9d03a90`]
- fix(logs): withhold an unlisted method only when the URL named it (MAG-4400) ([#543]) [`dfdc63f`]
- fix(logs): write a lone slash for relay_end's root-path method (MAG-4400) ([#543]) [`e17bde7`]
- fix(logs): write no failed-processing line beside relay_end (MAG-4400) ([#543]) [`8a7b3a9`]
- fix(relaycore): classify a GraphQL node fault by its error code, not by the 200 it arrived in (MAG-4375) ([#541]) [`2ecb74f`]
- fix(chainlib)!: a GraphQL 2xx with neither data nor errors is a node error, not the answer (MAG-4369) ([#534]) [`f14fdac`]
- fix(chainlib)!: bound how deeply a GraphQL document may nest (MAG-4359) ([#548]) [`150d616`]

#### Documentation updates
- docs(changelog): rewrite the v1.6.0 highlights and breaking changes ([#527]) [`a70ce1c`]
- docs(logs): relay_end error is prose; match on outcome and error_code (MAG-4400) ([#543]) [`154be71`]
- docs(logs): select relay_end writes without the json label clash (MAG-4400) ([#543]) [`ddb149f`]

[#488]: https://github.com/magma-Devs/smart-router/pull/488
[#519]: https://github.com/magma-Devs/smart-router/pull/519
[#520]: https://github.com/magma-Devs/smart-router/pull/520
[#523]: https://github.com/magma-Devs/smart-router/pull/523
[#524]: https://github.com/magma-Devs/smart-router/pull/524
[#525]: https://github.com/magma-Devs/smart-router/pull/525
[#527]: https://github.com/magma-Devs/smart-router/pull/527
[#534]: https://github.com/magma-Devs/smart-router/pull/534
[#535]: https://github.com/magma-Devs/smart-router/pull/535
[#536]: https://github.com/magma-Devs/smart-router/pull/536
[#539]: https://github.com/magma-Devs/smart-router/pull/539
[#541]: https://github.com/magma-Devs/smart-router/pull/541
[#543]: https://github.com/magma-Devs/smart-router/pull/543
[#544]: https://github.com/magma-Devs/smart-router/pull/544
[#545]: https://github.com/magma-Devs/smart-router/pull/545
[#548]: https://github.com/magma-Devs/smart-router/pull/548
[#553]: https://github.com/magma-Devs/smart-router/pull/553
[`027cc1c`]: https://github.com/magma-Devs/smart-router/commit/027cc1c66cf30eff4c3780cd1e28610aba65e59f
[`150d616`]: https://github.com/magma-Devs/smart-router/commit/150d61648b01b7d882c65a03e4ffbb9d38761c60
[`154be71`]: https://github.com/magma-Devs/smart-router/commit/154be718da8bcce1d3454282ee1100afe650f1be
[`16a167f`]: https://github.com/magma-Devs/smart-router/commit/16a167fead7e67a8b6fbad9ceefb509b3ed58262
[`16a42b9`]: https://github.com/magma-Devs/smart-router/commit/16a42b9305c2f05eb49326a4d3962ad5b236229f
[`2ecb74f`]: https://github.com/magma-Devs/smart-router/commit/2ecb74f137b9d8850755d14a66467106b51c73fb
[`409e5cd`]: https://github.com/magma-Devs/smart-router/commit/409e5cd8acdb759153666956c2b99ecbfde81f5f
[`4a7f9ae`]: https://github.com/magma-Devs/smart-router/commit/4a7f9ae282a83fcdacd5471f657aecaeea2ea08e
[`4d9a48c`]: https://github.com/magma-Devs/smart-router/commit/4d9a48c97c39dfc4ed2973262a6498300059cd1c
[`552fd47`]: https://github.com/magma-Devs/smart-router/commit/552fd473625a4b5e750bbb9ccb70de2d491899e8
[`5eff4a0`]: https://github.com/magma-Devs/smart-router/commit/5eff4a0e24e4ae8c5365fe13e94d9c991ea5e3e6
[`64cb8f1`]: https://github.com/magma-Devs/smart-router/commit/64cb8f1c17a26a1b03abe0174f24f3c7e9ce08b3
[`7058ea7`]: https://github.com/magma-Devs/smart-router/commit/7058ea7554ee48e1328dbad4653c297b836a388e
[`8a7b3a9`]: https://github.com/magma-Devs/smart-router/commit/8a7b3a934f946ddba8e56c833191a0ac808844e4
[`8ad0ff6`]: https://github.com/magma-Devs/smart-router/commit/8ad0ff6f0529e50c653358ee8bf5471abe99484b
[`9d03a90`]: https://github.com/magma-Devs/smart-router/commit/9d03a90562626db933a62841a99d5e167ddd40c5
[`9d1c7fc`]: https://github.com/magma-Devs/smart-router/commit/9d1c7fcb71448a08da53e6c4fc2839a15899a02f
[`9e23602`]: https://github.com/magma-Devs/smart-router/commit/9e23602f473db036fd58533b9799ce2c5be0b05d
[`9fc7505`]: https://github.com/magma-Devs/smart-router/commit/9fc75050d1cab4031bfccaa67ab8111a7fd1aa77
[`a70ce1c`]: https://github.com/magma-Devs/smart-router/commit/a70ce1cbd2dd5daba263d119559d43fb426a15c1
[`af0da11`]: https://github.com/magma-Devs/smart-router/commit/af0da115c1a9802e7ebe40413918f8918e828ead
[`d3413bb`]: https://github.com/magma-Devs/smart-router/commit/d3413bb9bf31af7c44c17d02f3d57d72eae7c8d5
[`dbb6ff1`]: https://github.com/magma-Devs/smart-router/commit/dbb6ff162e31be47638915c0e38735d45f7fe63e
[`ddb149f`]: https://github.com/magma-Devs/smart-router/commit/ddb149fe21cff9b28aff20e5676a1bfd9c153f75
[`dfdc63f`]: https://github.com/magma-Devs/smart-router/commit/dfdc63feaf4e6d7cf6004d21a28f3a319f32cbca
[`e17bde7`]: https://github.com/magma-Devs/smart-router/commit/e17bde7cf83f26222b7e4faa0cd0307391c06b8e
[`e7c6e4c`]: https://github.com/magma-Devs/smart-router/commit/e7c6e4c11b99314e13e9d7424b85e096bb7c5fc6
[`ec71d3a`]: https://github.com/magma-Devs/smart-router/commit/ec71d3aea026e40cabc08d89af12c250526c6f04
[`f017c87`]: https://github.com/magma-Devs/smart-router/commit/f017c87a470b32eb24f951a403890b4685f8dc07
[`f14fdac`]: https://github.com/magma-Devs/smart-router/commit/f14fdac5177bdb4a22aba0c768182d33ca45da08
[`f4ebf95`]: https://github.com/magma-Devs/smart-router/commit/f4ebf950d0d007643f49d744febc766ec4eb9037
[`f5a7f0a`]: https://github.com/magma-Devs/smart-router/commit/f5a7f0a909cb9e322282291d2853da4d9611f4ce
[`fc5d729`]: https://github.com/magma-Devs/smart-router/commit/fc5d7293a97825800688aada46e1c7090c8aac9e

## v1.6.0 — 2026-10-14

### Highlights

v1.6.0 changes seven behaviours a client or an operator may depend on. They are listed under **⚠ Breaking changes** below, each with the surface it touches and what to do: four change what a client sees on a reply (upstream response headers, `lava-relay-timeout`, retries after a node refusal, hedge accounting), one can stop the router at boot (cross-validation policy validation), one makes a transaction submission ignore the caller's cross-validation headers, and one changes which requests reach an archive node.

#### Archive routing is decided once, before the first attempt

The router decides whether a request needs an archive node from the request itself, before it picks an endpoint. A request is historical, and goes only to the endpoints that declare the `archive` extension, when any of these holds:

- it names a block deeper than the chain's archive depth, the `rule.block` of the `archive` extension in the chain's spec (127 blocks behind the head on Ethereum);
- it names the `earliest` block;
- it carries `lava-extension: archive`.

Endpoints without the extension are skipped whichever tier they are in: on a fleet where only a backup declares `archive`, a historical request goes straight to that backup, and a request for a recent block stays on the primaries. An archive-routed request counts CU at the extension's `cu_multiplier`. A retry re-sends the same request to another endpoint of the same pool; it no longer adds `archive` on the second attempt ([#389]), which used to pull a plain `latest` read into the backup tier, at archive cost, after one failure. The router also reads more of the request than before ([#514]): a JSON-RPC batch is judged on its deepest element, and a range method such as `trace_filter` on its `fromBlock`.

Requests whose block the router cannot read are not routed to archive on their own: methods that name a block or transaction by hash (`eth_getTransactionReceipt`, `eth_getBlockByHash`, `debug_traceTransaction`, `trace_transaction`, Tendermint `block_by_hash`, and the like) and EIP-1898 block objects (`{"blockHash": …}` or `{"blockNumber": …}`) on `eth_call`, `eth_getBalance`, `eth_getStorageAt`, `eth_getTransactionCount`, `eth_getCode` and `eth_getProof`. They stay on the primaries unless the caller sends `lava-extension: archive`. When a caller asks for an extension no node on the router offers, the request is still served and the reply says so in the new `Lava-Extension-Unavailable` header, counted in `smartrouter_extension_unavailable_total` ([#450]).

#### Also new since v1.4.0, the last graduated release

v1.5.0 through v1.5.8 were pre-releases, so an upgrade from v1.4.0 brings everything in their sections below as well. Three of those changes are large enough to repeat here.

- **REST errors are classified the way JSON-RPC errors are.** Until v1.5.5 a REST reply counted as the node's answer unless its status was 5xx or 429, so a Horizon `transaction_failed`, an Aptos `resource_not_found` or a Cosmos tx-not-found was filed as a success, never classified, and on a write the first of them ended the broadcast while a sibling node was still executing the transaction. Every status outside 2xx is now a node error with its own retry and fault verdict ([#448], v1.5.8): a 404 is data the node does not hold, retried on another node and never scored; 401, 402, 403, 407, 426 and 451 are the endpoint refusing the router, retried elsewhere without blaming the node; 409 is a transaction already known, so in a broadcast the accepting node's reply wins over the duplicates; a 400 "block beyond head" is a missing block; malformed-request statuses (400, 406, 411, 412, 414 to 417, 422, 428, 431) are the caller's error. A 5xx that carries the node's own JSON body reaches the caller as-is instead of a router-made 500, an empty 404 or 405 from a gateway in front of the node is a route refusal rather than the node's answer ([#435], v1.5.5), and a node answer the endpoint is not at fault for logs at DEBUG rather than ERROR. The rule was probed live against twelve REST API families; the full status map is section 9 of `docs/ERROR-REGISTRY-DESIGN.md`. v1.6.0 continues it: five `NODE_*` refusal codes are retried on another provider (breaking change 2), a "data not held" answer scores on sync only ([#460]), and an upstream EOF, DNS failure or http-as-https counts against the endpoint ([#518]).
- **Solana replies are read by path, not decoded.** Every served reply on a Solana router used to be decoded whole to find `result.context.slot`; a 5.9 MB `getBlock` reply cost a full copy and 2.8 ms for a member that `getBlock` never carries. Since v1.5.4 ([#428]) the slot is read by path, `getBlock` replies are skipped outright (8 ns, no allocation), a reply whose result is a list is answered from its first byte, and a truncated reply yields no slot instead of a wrong one. The cache write snapshots the reply instead of deep-copying it through JSON, which took 33 ms and 15 to 20 MB per block and now takes about 100 ns ([#431]), and block-height extraction no longer decodes a whole block to run a block-hash rule that could only return 0 ([#430]). In v1.6.0 the frozen-head detector counts a url's highest slot, so a Solana node whose `context.slot` and finalized slot disagree while both stand still is still taken out of rotation ([#494]).
- **Heavy responses cost less memory and CPU.** A node url can ask its upstream for gzip with `accept-encoding: gzip` ([#432], v1.5.4); the reply is inflated while it is read, with pooled readers, and a block shrinks four to six times on the wire. An upstream body whose length is announced is read into one buffer of that size instead of a chain of growing buffers ([#429]; for 5.9 MB, 5.6 MiB and 3 allocations instead of 13.9 MiB and 31). JSON-RPC block-height extraction stops at 1 MiB, since a larger JSON-RPC reply is a log filter, a full block or a trace, never the tip ([#430]). Since v1.5.6, `--cache-max-entry-bytes` (default 1 MiB, `0` for no cap) serves a reply above the cap without writing it to the cache, which protects the hit rate of ordinary entries and the cache server's CPU; `smartrouter_cache_write_skipped_total{reason="size"}` and `smartrouter_cache_entry_bytes` show what it skips ([#434]). v1.6.0 adds the audit's efficiency fixes ([#511]): regexps compiled once instead of per error, metric increments counted inline instead of on a goroutine each, and the node-errors header built in one buffer.

#### What's new

- **Chain tracker idle mode** ([#505]), off by default. `--chain-tracker-idle-after <duration>` pauses a chain's dedicated upstream polls once it has served no client relay for that long; `--chain-tracker-idle-poll-interval` keeps a slow heartbeat instead (`0` polls nothing while idle). The next client relay wakes the chain with one poll of every endpoint before it is routed. `smartrouter_chain_idle` reports the state; the operator guide is `docs/CHAIN-TRACKER-IDLE-MODE.md`.
- **A url stuck on a frozen head leaves rotation** ([#494]). A node that keeps answering with the same latest block while another url of the chain has moved past it is taken out of rotation after 10 × `--chain-tracker-poll-divisor` tracker cycles (at least 3), and returns once it answers above the block it was stuck on. A provider with every url out is blocked, so traffic moves to the next primary or to the backup tier.
- **One latest-block series per url** ([#501]). `rpc_endpoint_url_latest_block{spec, apiInterface, endpoint_id, url, internal_path}` carries each url's own head (the `url` label is scheme and host plus an 8-hex fingerprint, never the api key), so an alert can tell a frozen url from one that trails. `rpc_endpoint_latest_block` now reports a provider's lowest url head. A removed url's series is deleted, not left standing.
- **Per-customer metrics from the `project-id` header** ([#513]). `smartrouter_customer_requests_total` and `smartrouter_customer_requests_failed_total` (`spec`, `apiInterface`, `customer`, `method`), `smartrouter_customer_end_to_end_latency_milliseconds` (`spec`, `apiInterface`, `customer`) and `smartrouter_customer_overflow_total` (`spec`). A request without the header counts as `DefaultDappID`; each spec admits 512 distinct customers and later values collapse to `other`. The header never reaches an upstream or the cache key.
- **Native GraphQL API interface** ([#480]), first used for Sui. Each root field is its own API, so cross-validation policies, the stateful-write guard, compute units and tip parsing apply per operation instead of collapsing onto one `/graphql` path.
- **Cross-validation names the plurality on failure** ([#491]). `lava-cross-validation-plurality-size` and `lava-cross-validation-plurality-providers` carry the largest set of providers that returned the same response when the quorum is not reached; the providers list is empty on a tie.
- **Chain tip regressions are counted** ([#506]). `smartrouter_latest_block_regressions_total{spec, apiInterface, reason}` (`reason` is `consensus` or `stale_tip`) with an ERROR log, and `smartrouter_latest_block` follows the tip down.
- **`GET /debug/sticky-claims`** ([#442]) reports the sticky-session claim outcomes per endpoint, cumulative since the pod started.
- **Endpoint observations travel on the RESP cache backend** ([#390]), so the fleet tracker gate works there too and replicas that share a cache stop each polling every upstream.

#### Notable fixes

- **Failover and sessions.** A blocked provider's disabled endpoint is lent once per request as a real last chance instead of "No pairings available" ([#504]). An upstream connection that answers only 5xx is retired after two in a row, at most once per 5 s, so a load balancer pinned to a dead backend no longer holds the router on 503s until a restart ([#507]). A relay is never handed a WebSocket endpoint ([#487]); a direct RPC session is tied to the endpoint selection chose ([#493]); every provider in `lava-providers-block` is honoured, not just the first two ([#451]); a "data not held" answer scores on sync only ([#460]).
- **Cross-validation.** The sessions gathered for a request whose quorum is out of reach are released ([#423]); a round stops at the attempt window when only a quiet group is missing ([#454]); startup warns on every min-groups and max-participants shortfall, measured against the configured providers rather than the ones up at boot ([#439]); the health check takes one provider's latest block, not a quorum ([#438]); cross-validation outranks a caller pin at any min-groups ([#393]).
- **REST.** PUT and PATCH bodies are relayed ([#455]); a JSON body under a generic Content-Type is sent as JSON ([#456]); a non-default Content-Type is part of the cache key ([#457]); a request path can never change the upstream host ([#479]).
- **gRPC.** A binary call is forwarded as raw bytes ([#446]); idle pooled upstream channels are closed and live streams pinged ([#447]); the chain parser's proto registry is locked against concurrent relays ([#445]); requests parse from reflection snapshots, not relays ([#503]); the descriptor cache is keyed by service, so same-named methods across Cosmos services no longer share one descriptor ([#514]); subscription restores are bounded and classified, and a given-up client stream ends with an error ([#489]); gRPC ABORTED is classified as `NODE_ABORTED`, a non-retryable transaction outcome that is not the endpoint's fault ([#458]).
- **WebSocket.** A shutdown panic in upstream subscriptions is gone ([#477]); `unsubscribe_all` gets a reply ([#478]); a subscription that became active between two checks is joined ([#508]); a teardown wins over a restore in flight ([#509]); `relay_usage` events carry the client's Origin ([#474]).
- **Tracing and request headers.** The caller's tracing ids are read on every interface and copied before they outlive the request ([#416]); every request header is owned past the listener, and a pinned provider resolves to the router's own string ([#443]).
- **Cache and classification.** The chain tip is bounded by what the router believed, and a stale tip yields ([#421]); a dead RESP cache reads as an outage on reads, not a slow one ([#436]); a relay's cache key is computed once, on a copy of the request ([#515]); an upstream EOF, DNS failure or http-as-https is the endpoint's fault ([#518]); batch retries follow the flag, not a hash a batch never has ([#517]); a tagged directive resolves from the url's own internal-path collection ([#482]).

#### Reverted before the tag

The backup-reserve hedge ([#449]) was reverted by [#516] before v1.6.0 was cut. The budget only ends a request, attempts go out one window apart, and the backup tier is reached once the primary pool is exhausted, as in v1.5.8. The [#449] commits still appear under Bug fixes below because that list is generated from history.

#### Removed

The legacy relay-metrics reporter and the New Relic agent are gone ([#472], [#471]); no deployment configured either. Prometheus metrics and the OTel relay-usage sink are unchanged.

#### Build

Go 1.26.9 and golang.org/x/net v0.60.0 ([#502]).

### Changes

#### ⚠ Breaking changes

Each entry says what changed, who notices, and what to do. The commits are also listed under their own group below.

1. **Upstream response headers are filtered** ([#420], MAG-3104, [`efbe226`], [`ce4dfd8`])

   An upstream's response headers reach the client only if they are transport headers (`Content-Type`, `Content-Encoding`, `Retry-After`, `Date`) or headers the chain spec declares in the reply direction (`pass_reply`, `pass_both` or `pass_ignore`) for the API interface, taken as the union across the interface's collections. Vendor, CDN, cookie and quota headers that passed through before are dropped. The router's own `Lava-*` headers are unchanged, and a cache hit written by an older router replays the same reduced set.

   *Who notices:* a client that reads an upstream's own headers off a reply, such as a quota, a request id or a cookie.

   *What to do:* declare the header in the chain spec's `headers` with a reply-direction kind. Nothing else makes it pass.

2. **A node's refusal is retried on another provider** ([#492], MAG-2771, [`fb11aec`])

   `NODE_METHOD_NOT_FOUND` (2001), `NODE_UNIMPLEMENTED` (2008), `NODE_ENDPOINT_NOT_FOUND` (2009), `NODE_METHOD_NOT_ALLOWED` (2010) and `NODE_UNAUTHORIZED` (2016) are a node's claim about itself, not a fact about the chain, so the router now asks another provider instead of returning the refusal at once; the refusing endpoint is neither blamed nor scored. `NODE_LIMIT_EXCEEDED` (2011) stays non-retryable, and the refusal of a method the spec does not list (relayed as a `Default-` API) stays final. When the retry gets a different answer, the caller receives the answer that ended the request, not the first refusal.

   *Who notices:* a client that reads `Lava-Retries` and `Lava-Provider-Address`; on these replies the values change. A refusal every provider shares now costs up to the retry limit in upstream calls. The public error-codes page lags this release and still lists the five codes as non-retryable.

   *What to do:* nothing for most clients. Treat a refusal on these codes as the fleet's answer rather than one node's.

3. **`lava-relay-timeout` is bounded** ([#452], MAG-3988 and MAG-3600, [`5c2809f`])

   The header is no longer used verbatim. A value that is not a positive duration is ignored, a value under 300ms is raised to 300ms, and a value above the request's own budget is held to that budget unless the operator sets `--max-caller-relay-timeout` (default `0`: no extension). The reply carries `Lava-Relay-Timeout-Applied` with the window actually used. Before this release a zero or negative value crashed the router, and a large one held a request well past its budget.

   *Who notices:* a caller that raised the header per request for heavy methods beyond the method's own budget, as the public docs used to recommend; it now gets the method's own budget.

   *What to do:* to let callers extend budgets, run with `--max-caller-relay-timeout`, for example `5m`, the outbound client's per-attempt limit. The public docs (`api/directives.md`, `configuration/failover/timeout.md`, `reference/cli.md`) describe the bounded behaviour.

4. **A hedge's losing attempt is counted and named again** ([#440], MAG-3762, [`62bd7ee`])

   For a request where a hedge fires, `Lava-Retries` is at least 1 and the losing provider is named in `Lava-Provider-Address` before the one that answered. This is the behaviour of v1.5.1 and earlier; v1.5.2 and v1.5.3 dropped the losing attempt. The retry metrics (`smartrouter_retries_total`, `smartrouter_retries_success_total`, `smartrouter_retries_failed_total`, `smartrouter_retry_attempts`) count hedged requests again, so the retry and hedging families overlap.

   *Who notices:* dashboards and alerts built on the retry metrics or on `Lava-Retries` against v1.5.2 or v1.5.3.

   *What to do:* expect the v1.5.1 numbers.

5. **A cross-validation policy that can never apply stops the router at startup** ([#441], MAG-3604, [`d7e638b`])

   A policy whose `chain-id` and `api-interface` match no configured endpoint, or whose method the endpoint's spec does not serve (a REST policy names the path template, not a gRPC method name), is refused before anything binds or dials. The error names the policy by its position in `cross-validation.policies` and the endpoints that do exist. Such a policy used to load and never apply, while the startup line counted it as loaded. A policy with neither `enabled` nor `forbid-caller-cv` is a no-op and is left alone; the "policies loaded" line now reports each endpoint's own count and methods.

   *Who notices:* an operator with a stale or misspelled policy; the router does not boot.

   *What to do:* delete or correct the policy the error names before rolling out.

6. **A write routes as a write, whatever headers the caller sends** ([#437], MAG-3603, [`5215861`])

   A method in the spec's stateful category no longer takes the caller's `lava-cross-validation-*` headers. It routes by its category and the headers have no effect: HTTP 200, one answer, no `lava-cross-validation-*` response headers. A cross-validated write used to broadcast the transaction to every stateful node and then report HTTP 500, because only the first node can accept it. A malformed header pair on such a method (an unparseable value, one header without its companion, an agreement threshold above max-participants) used to be refused before dispatch and is now ignored; it is still rejected on a read. A `forbid-caller-cv: true` policy written on a write is redundant and harmless.

   *Who notices:* a client library that sends the cross-validation headers on every request, transaction submissions included.

   *What to do:* nothing, except stop expecting cross-validation response headers on a write.

7. **A retry no longer promotes a request to `archive`** ([#389], MAG-2734, [`c7c605a`])

   Archive routing is decided once, before the first attempt, from the block the request names (see Highlights); a retry re-sends the same request. Requests whose block the router cannot read, by-hash methods and EIP-1898 block objects, lose the automatic archive route they had through the retry.

   *Who notices:* a caller that relied on `eth_getTransactionReceipt`, `debug_traceTransaction`, `trace_transaction`, an `eth_call` with `{"blockHash": …}` or `{"blockNumber": …}`, and the like reaching an archive node on the second attempt. Those requests stay on the primaries.

   *What to do:* send `lava-extension: archive` on requests that need archive state. A chain whose historical requests are misjudged by height is fixed in its spec's `rule.block`.

#### New Features
- feat(rpcsmartrouter): report the sticky-session claim outcomes on GET /debug/sticky-claims (MAG-3860) ([#442]) [`9a9bf07`]
- feat(chainlib): add a native graphql api interface for Sui (MAG-2863) ([#480]) [`edc9b10`]
- feat(cache): carry endpoint observations on the KV seam so the fleet tracker gate works on the RESP backend ([#390]) [`fbe2856`]
- feat(rpcsmartrouter): name the plurality when cross-validation fails (MAG-2192) ([#491]) [`20dfe05`]
- feat(rpcsmartrouter): count and log the chain's latest block going backwards (MAG-4153) ([#506]) [`043f0fd`]
- feat(metrics): one rpc_endpoint_url_latest_block series per url of a provider, for the stuck alerts (MAG-4204) ([#501]) [`0bbe3b6`]
- feat(rpcsmartrouter): take a url that keeps answering a frozen block out of rotation (MAG-3986) ([#494]) [`55bb865`]
- feat(metrics): count requests, failures and latency per customer from the project-id header (MAG-4081) ([#513]) [`baa9078`]
- feat(chain-tracker): idle mode, pause tracker polls on a quiet chain and wake on the next client request (MAG-3829) ([#505]) [`a2d8148`]

#### Bug fixes
- fix(common): classify gRPC ABORTED and DATA_LOSS as node errors (MAG-3995) ([#458]) [`54591e5`]
- fix(common): NODE_ABORTED is a transaction outcome, and DATA_LOSS stays unregistered (MAG-3995) ([#458]) [`62da26b`]
- fix(common): say what Sui's ABORTED is, and is not, in the NODE_ABORTED rationale (MAG-3995) ([#458]) [`36542ee`]
- fix(rpcsmartrouter): forward only the upstream headers the transport or the spec needs (MAG-3104) ([#420]) [`ce4dfd8`]
- fix(rpcsmartrouter)!: keep the reply headers a chain's whole API interface declares, pass_ignore included (MAG-3104) ([#420]) [`efbe226`]
- fix(lavasession): a pin no other provider can satisfy must not release the block ([#393]) [`3cd14f0`]
- fix(rpcsmartrouter): cross-validation outranks a caller pin, at any minGroups ([#393]) [`97d5ed9`]
- fix(lavasession): resolve the pin before a release is judged able to serve it (MAG-3106) ([#393]) [`906a2d8`]
- fix(lavasession): hand out a copy of the errored providers, not the live map (MAG-3103) ([#418]) [`44d75c6`]
- fix(resp-cache): a dead cache reads as an outage on reads, not as a slow one ([#436]) [`f5e9b21`]
- fix(resp-cache): record reachability only where one endpoint stands behind it ([#436]) [`9ac667e`]
- fix(resp-cache): record a dial fault on what answered, not on whose clock ran out (MAG-3653) ([#436]) [`0e1b9ee`]
- fix(rpcsmartrouter): a health check takes one provider's latest block, not a quorum ([#438]) [`e9dc693`]
- fix(rpcsmartrouter): seal the internal relay against cross-validation, not merely unwire it ([#438]) [`e3655f5`]
- fix(relaycore): name the real reason when an internal relay ignores cross-validation headers (MAG-3746) ([#438]) [`dcc8ad3`]
- fix(chainlib): carry the client's Origin on websocket relay_usage events ([#474]) [`a7918e8`]
- fix(rpcsmartrouter): check min-groups against the configured providers, not the ones up at boot (MAG-3751) ([#439]) [`49fa95d`]
- fix(rpcsmartrouter): warn at startup for a max-participants shortfall too, and pin the boot path in a test (MAG-3751) ([#439]) [`c017377`]
- fix(rpcsmartrouter): warn on every cross-validation shortfall at startup, and keep the layout line's keys (MAG-3751) ([#439]) [`d3042be`]
- fix(rpcsmartrouter): name the answering pod on /debug/sticky-claims, and say what adopted does not prove (MAG-3860) ([#442]) [`6cb23ea`]
- fix(rpcsmartrouter): say what a reset does to the sticky claim counts, and why SharedSticky is false (MAG-3860) ([#442]) [`c4e2a59`]
- fix(chainlib): honour every provider in lava-providers-block, not just the first two (MAG-3080) ([#451]) [`283ff53`]
- fix(relaycore): hedge to the backup tier before the budget ends when primaries go quiet (MAG-3923) ([#449]) [`2d18323`]
- fix(relaycore): count a hedge asked for in the same instant as the reserve hedge (MAG-3923) ([#449]) [`334fda5`]
- fix(lavasession): log the reserve hedge's backup selection as a hedge, not a fallback (MAG-3923) ([#449]) [`ea5ad0a`]
- fix(relaycore): keep counting a hedge in flight when a node error drives a retry (MAG-3923) ([#449]) [`a90ef62`]
- refactor(lavasession): keep the name of every dispatched session, not only the count (MAG-3762) ([#440]) [`aa005c3`]
- fix(rpcsmartrouter)!: count and name a hedge's losing attempt again (MAG-3762) ([#440]) [`62bd7ee`]
- fix(rpcsmartrouter): release a rejected session under the key it was taken with (MAG-3762) ([#440]) [`9178c5b`]
- fix(rpcsmartrouter): list Lava-Provider-Address in the order the attempts went out (MAG-3762) ([#440]) [`531ea7a`]
- fix(cache): bound the chain tip by what the router believed, and let a stale tip yield (MAG-3755) ([#421]) [`1df3854`]
- fix(cache): keep a no-tip pod's own claim, never publish a zero tip, and correct the prose around the guard (MAG-3755) ([#421]) [`b1ec6c6`]
- fix(rpcsmartrouter): release the sessions a cross-validation request gathered when the quorum is out of reach ([#423]) [`0c51f7a`]
- fix(rpcsmartrouter): match AddUsed when releasing an entry that has no session (MAG-3286) ([#423]) [`85e9f2d`]
- fix(rpcsmartrouter): release undispatched sessions through one helper, under each session's own key (MAG-3286) ([#423]) [`d8f2d2b`]
- fix(chainlib): read the caller's tracing headers on every interface (MAG-3798) ([#416]) [`5d1e294`]
- fix(chainlib): copy the caller's tracing ids before they outlive the request (MAG-3798) ([#416]) [`8c2610e`]
- refactor(chainlib): stamp Tendermint's tracing ids after the header-map reads (MAG-3798) ([#416]) [`996f0a7`]
- fix(chainlib): log the caller's tracing ids at info on Tendermint RPC and gRPC (MAG-3798) ([#416]) [`5b7fe36`]
- fix(lavasession): resolve a pinned provider to the router's own string, and own every request header past the listener (MAG-3881) ([#443]) [`bf7abc5`]
- fix(chainlib): prove the header copy through the listeners, and keep /metrics up when one metric fails (MAG-3881) ([#443]) [`e6b1a8a`]
- fix(chainlib): settle where a gRPC metadata value lives, and prove it against a real server (MAG-3881) ([#443]) [`18e581a`]
- fix(chainlib): own the limiter's User-Agent, and let the detached-header test fail for a dropped header (MAG-3881) ([#443]) [`920f46c`]
- fix(rpcclient): stop a shutdown panic in upstream subscriptions ([#477]) [`b3feebe`]
- fix(chainlib): reply to a websocket unsubscribe_all ([#478]) [`56dba11`]
- fix(grpc): lock the chain parser's proto registry against concurrent relays ([#445]) [`c9713ed`]
- fix(chainlib): relay PUT and PATCH request bodies on REST (MAG-4008) ([#455]) [`d007d91`]
- fix(chainlib): tell the caller when a requested extension no node offers is dropped (MAG-3935) ([#450]) [`91065f6`]
- fix(rpcsmartrouter)!: refuse a cross-validation policy that could never apply, and log each endpoint's own (MAG-3604) ([#441]) [`d7e638b`]
- fix(relaycore): stop cross-validation at the attempt window when only a quiet group is missing (MAG-3993) ([#454]) [`6675c4f`]
- fix(relaycore)!: a write routes as a write, whatever headers the caller sends ([#437]) [`5215861`]
- fix(rpcsmartrouter): ignore a non-positive lava-relay-timeout instead of crashing (MAG-3988) ([#452]) [`5c2809f`]
- fix(chainlib): send JSON bodies under a generic Content-Type as JSON (MAG-4006) ([#456]) [`058d8e1`]
- fix(chainlib): keep non-default Content-Types in the cache key (MAG-4007) ([#457]) [`0a6b18b`]
- fix(rpcsmartrouter): score a "data not held" answer on sync only, not as a success ([#460]) [`4b87c8f`]
- fix(grpc): close idle pooled upstream channels and ping live streams ([#447]) [`3b7cdfc`]
- fix(grpc): forward a binary gRPC call to the node as raw bytes ([#446]) [`f27b719`]
- fix(redisstore): let a value the observation read rejects stop fencing the publish (MAG-3732) ([#390]) [`0eaa47a`]
- fix(performance): put the RESP endpoint-observation calls behind the breaker (MAG-3732) ([#390]) [`a362acf`]
- fix(cache): share RESP endpoint observations across key prefixes, as cache-be does (MAG-3732) ([#390]) [`7efa7b3`]
- fix(cache): keep RESP endpoint observations under key-prefix (MAG-3732) ([#390]) [`1949100`]
- fix(common): a REST request path can never change the upstream host ([#479]) [`d13051c`]
- fix(specs): bring the mirror's cosmos REST and Aptos stateful marks in line with lava-specs (MAG-4155) ([#486]) [`c4485a7`]
- fix(lavasession): never hand a relay a websocket endpoint (MAG-3477) ([#487]) [`1eee645`]
- fix(rpcsmartrouter): --epoch-duration help says epochs always run, defaulting to 15m (MAG-4154) ([#484]) [`2fe412c`]
- fix(grpc): bound and classify gRPC subscription restores, and end given-up client streams with an error (MAG-4177) ([#489]) [`14b3034`]
- fix(grpc): bound restores by uptime, and keep a subscription's own end out of the reconnect (MAG-4177) ([#489]) [`ed8e917`]
- fix(grpc): hold off a stream's rate-limited endpoint, strand no end status, and stop polling reconnects (MAG-4177) ([#489]) [`a2d90c4`]
- fix(lavasession): tie a direct RPC session to the endpoint selection chose (MAG-4025) ([#493]) [`12ff203`]
- fix(lavasession): retire an upstream connection that answers only 5xx (MAG-4211) ([#507]) [`1a7e4e1`]
- fix(common)!: ask another provider when a node refuses on its own account (MAG-2771) ([#492]) [`fb11aec`]
- fix(metrics): report a provider's lowest url head so one stuck url still fires the stuck-provider alerts (MAG-4204) ([#501]) [`33093ee`]
- fix(metrics): count a stuck url on every answered poll, and drop a url only after 3 failed polls (MAG-4204) ([#501]) [`165c923`]
- fix(metrics): keep a failing url's head, leave ws urls out, and undo a head written after its url was removed (MAG-4204) ([#501]) [`9332503`]
- fix(metrics): label a url's series by its fingerprint alone (MAG-4204) ([#501]) [`0ca8efd`]
- fix(metrics): drop a url's own series once it has given no answer for 2 minutes (MAG-4204) ([#501]) [`5e90850`]
- refactor(metrics): MAG-4204 option B as written: keep the provider series, and add per-url series that tell stuck from down ([#501]) [`63af99e`]
- fix(metrics): drop a removed url's relay counter, and compare answered_until with timestamp() (MAG-4204) ([#501]) [`5a17276`]
- fix(chainlib): resolve a tagged directive from the url's own internal-path collection (MAG-4105) ([#482]) [`d8532fc`]
- refactor(chainlib): one keyed read for the add-on and base steps of the per-url lookup (MAG-4105) ([#482]) [`feb43f9`]
- fix(chainlib): parse gRPC requests from reflection snapshots, not relays (MAG-4217) ([#503]) [`435fcf0`]
- fix(lavasession): give a blocked provider a real last chance instead of "No pairings available" (MAG-4232) ([#504]) [`b33b7fc`]
- fix(chainlib): compute a relay's cache key once, on a copy of the request (MAG-4242) ([#515]) [`bc6dfec`]
- fix(common): classify an upstream EOF, DNS failure and http-as-https as the endpoint's fault (MAG-4242) ([#518]) [`bcf9d7d`]
- fix(chainlib): key the gRPC descriptor cache by service, keep a batch's deepest block, and read a range's lower bound (MAG-4242) ([#514]) [`803d485`]
- fix(rpcsmartrouter): join a WebSocket subscription that became active between the two checks (MAG-2546) ([#508]) [`d966dbe`]
- fix(rpcsmartrouter): let a WebSocket teardown win over a restore in flight (MAG-2547) ([#509]) [`30c7545`]
- fix(metrics): size each url's answered-until time from the poll interval the tracker runs and the traffic gate (MAG-4249) ([#510]) [`2fd1c1e`]
- fix(relaypolicy): decide batch retries by the flag, not by a hash a batch never has (MAG-4242) ([#517]) [`8e325e8`]

#### Documentation updates
- docs(common): say what MayHaveReachedNode does and does not do for NODE_ABORTED (MAG-3995) ([#458]) [`1b94175`]
- docs: correct what a pinned cross-validation request did before the override (MAG-4143) ([#393]) [`536e39f`]
- docs(resp-cache): scope the kind="error" read reading to standalone (MAG-3653) ([#436]) [`f10367e`]
- docs(cross-validation): index the readiness symptom where an operator will look ([#438]) [`c56ca12`]
- docs(metrics): say what the health readings do and do not attest (MAG-3746) ([#438]) [`573bb7a`]
- docs(cross-validation): the three ATTENTION cases, the layout line's keys, and the backup-only exception (MAG-3751) ([#439]) [`5abd0cb`]
- docs(cache): say the router writes no block-hash to height mappings (MAG-3807) ([#444]) [`b778757`]
- docs(rpcsmartrouter): say in the code that no block-hash→height mapping is ever learned (MAG-3807) ([#444]) [`9537e50`]
- docs(cache): pin the block-hash rule to getblockstats, and name the route #389 removes (MAG-3807) ([#444]) [`63f1b56`]
- docs(redisstore): name the upstream go-redis issue for the re-auth race ([#413]) [`a3ec336`]
- docs(redisstore): name the go-redis release path that loses the re-auth wake-up ([#413]) [`77c4eda`]
- docs(cache): say the chain-tip fence rests on writer clocks, and what a hit's Provider-Latest-Block carries (MAG-3755) ([#421]) [`783ae7a`]
- docs(cache): stop the KVStore type comment saying a stale tip still fences lower writes (MAG-3755) ([#421]) [`8ab1762`]
- docs(utils): state the tracing-id ownership rule, not which listener meets it (MAG-3798) ([#416]) [`fb88150`]
- docs(rpcsmartrouter): say why gRPC streams carry tracing ids and WebSocket does not (MAG-3798) ([#416]) [`ad25ad6`]
- docs(rpcsmartrouter): say which log lines carry the tracing ids and where they stop (MAG-3798) ([#416]) [`9c4679f`]
- docs(utils): state the tracing-id ownership rule, not which listener meets it (MAG-3881) ([#443]) [`4369814`]
- docs(cache): say which servers' clocks an observation's age spans (MAG-3732) ([#390]) [`72be694`]
- docs(chainlib): an undeclared graphql root field is served, not refused (MAG-4270) ([#521]) [`149d2f5`]

#### Build process updates
- build(deps): move to Go 1.26.9 and golang.org/x/net v0.60.0 so govulncheck passes (MAG-4230) ([#502]) [`4a0bc8a`]

#### Other work
- MAG-2734 fix(relaypolicy): stop rewriting the request on retry ([#389]) [`c7c605a`]
- MAG-2734 fix(relaycore): make the retry guard reach the attempt it guards ([#389]) [`9568adf`]
- MAG-2734 test(relaycore): drop the retries manager from tests that landed on main after the removal ([#389]) [`40dc024`]
- MAG-2734 style(rpcsmartrouter): gofmt the XRPL test server literal ([#389]) [`c977835`]
- MAG-2734 docs: stop describing the removed archive upgrade and retry bans as live ([#389]) [`543b871`]
- MAG-2734 docs(relaypolicy): name the requests the archive rule cannot judge ([#389]) [`f4d15f2`]
- perf(utils): look up the gRPC spelling of the tracing headers without allocating (MAG-3798) ([#416]) [`175fda1`]
- revert: drop the backup-reserve hedge of #449 (MAG-3923) ([#516]) [`5d514dd`]
- perf(protocol): enable six linters and act on the audit's lint-derived efficiency signals (MAG-4244) ([#511]) [`1979be8`]

[#389]: https://github.com/magma-Devs/smart-router/pull/389
[#390]: https://github.com/magma-Devs/smart-router/pull/390
[#393]: https://github.com/magma-Devs/smart-router/pull/393
[#413]: https://github.com/magma-Devs/smart-router/pull/413
[#416]: https://github.com/magma-Devs/smart-router/pull/416
[#418]: https://github.com/magma-Devs/smart-router/pull/418
[#420]: https://github.com/magma-Devs/smart-router/pull/420
[#421]: https://github.com/magma-Devs/smart-router/pull/421
[#423]: https://github.com/magma-Devs/smart-router/pull/423
[#428]: https://github.com/magma-Devs/smart-router/pull/428
[#429]: https://github.com/magma-Devs/smart-router/pull/429
[#430]: https://github.com/magma-Devs/smart-router/pull/430
[#431]: https://github.com/magma-Devs/smart-router/pull/431
[#432]: https://github.com/magma-Devs/smart-router/pull/432
[#434]: https://github.com/magma-Devs/smart-router/pull/434
[#435]: https://github.com/magma-Devs/smart-router/pull/435
[#436]: https://github.com/magma-Devs/smart-router/pull/436
[#437]: https://github.com/magma-Devs/smart-router/pull/437
[#438]: https://github.com/magma-Devs/smart-router/pull/438
[#439]: https://github.com/magma-Devs/smart-router/pull/439
[#440]: https://github.com/magma-Devs/smart-router/pull/440
[#441]: https://github.com/magma-Devs/smart-router/pull/441
[#442]: https://github.com/magma-Devs/smart-router/pull/442
[#443]: https://github.com/magma-Devs/smart-router/pull/443
[#444]: https://github.com/magma-Devs/smart-router/pull/444
[#445]: https://github.com/magma-Devs/smart-router/pull/445
[#446]: https://github.com/magma-Devs/smart-router/pull/446
[#447]: https://github.com/magma-Devs/smart-router/pull/447
[#448]: https://github.com/magma-Devs/smart-router/pull/448
[#449]: https://github.com/magma-Devs/smart-router/pull/449
[#450]: https://github.com/magma-Devs/smart-router/pull/450
[#451]: https://github.com/magma-Devs/smart-router/pull/451
[#452]: https://github.com/magma-Devs/smart-router/pull/452
[#454]: https://github.com/magma-Devs/smart-router/pull/454
[#455]: https://github.com/magma-Devs/smart-router/pull/455
[#456]: https://github.com/magma-Devs/smart-router/pull/456
[#457]: https://github.com/magma-Devs/smart-router/pull/457
[#458]: https://github.com/magma-Devs/smart-router/pull/458
[#460]: https://github.com/magma-Devs/smart-router/pull/460
[#471]: https://github.com/magma-Devs/smart-router/pull/471
[#472]: https://github.com/magma-Devs/smart-router/pull/472
[#474]: https://github.com/magma-Devs/smart-router/pull/474
[#477]: https://github.com/magma-Devs/smart-router/pull/477
[#478]: https://github.com/magma-Devs/smart-router/pull/478
[#479]: https://github.com/magma-Devs/smart-router/pull/479
[#480]: https://github.com/magma-Devs/smart-router/pull/480
[#482]: https://github.com/magma-Devs/smart-router/pull/482
[#484]: https://github.com/magma-Devs/smart-router/pull/484
[#486]: https://github.com/magma-Devs/smart-router/pull/486
[#487]: https://github.com/magma-Devs/smart-router/pull/487
[#489]: https://github.com/magma-Devs/smart-router/pull/489
[#491]: https://github.com/magma-Devs/smart-router/pull/491
[#492]: https://github.com/magma-Devs/smart-router/pull/492
[#493]: https://github.com/magma-Devs/smart-router/pull/493
[#494]: https://github.com/magma-Devs/smart-router/pull/494
[#501]: https://github.com/magma-Devs/smart-router/pull/501
[#502]: https://github.com/magma-Devs/smart-router/pull/502
[#503]: https://github.com/magma-Devs/smart-router/pull/503
[#504]: https://github.com/magma-Devs/smart-router/pull/504
[#505]: https://github.com/magma-Devs/smart-router/pull/505
[#506]: https://github.com/magma-Devs/smart-router/pull/506
[#507]: https://github.com/magma-Devs/smart-router/pull/507
[#508]: https://github.com/magma-Devs/smart-router/pull/508
[#509]: https://github.com/magma-Devs/smart-router/pull/509
[#510]: https://github.com/magma-Devs/smart-router/pull/510
[#511]: https://github.com/magma-Devs/smart-router/pull/511
[#513]: https://github.com/magma-Devs/smart-router/pull/513
[#514]: https://github.com/magma-Devs/smart-router/pull/514
[#515]: https://github.com/magma-Devs/smart-router/pull/515
[#516]: https://github.com/magma-Devs/smart-router/pull/516
[#517]: https://github.com/magma-Devs/smart-router/pull/517
[#518]: https://github.com/magma-Devs/smart-router/pull/518
[#521]: https://github.com/magma-Devs/smart-router/pull/521
[`043f0fd`]: https://github.com/magma-Devs/smart-router/commit/043f0fd4e68a96b0fbdb54c8a89dd216d599b044
[`058d8e1`]: https://github.com/magma-Devs/smart-router/commit/058d8e10d5decd1b1791ad394828b85aac34467a
[`0a6b18b`]: https://github.com/magma-Devs/smart-router/commit/0a6b18b44f7b0ff1888e4f7f8d4b2adbfbf9772d
[`0bbe3b6`]: https://github.com/magma-Devs/smart-router/commit/0bbe3b697249a099559043dbd61d4c87beb71cd6
[`0c51f7a`]: https://github.com/magma-Devs/smart-router/commit/0c51f7a0f8c515370d19b29a63530424f130307a
[`0ca8efd`]: https://github.com/magma-Devs/smart-router/commit/0ca8efdcba4a0477f70e5b849b9db310eb0465bf
[`0e1b9ee`]: https://github.com/magma-Devs/smart-router/commit/0e1b9eec3bfb79e1ae3c5bbf857b9d472449b32d
[`0eaa47a`]: https://github.com/magma-Devs/smart-router/commit/0eaa47a3084c99ea3368b260d6ccf8a827491442
[`12ff203`]: https://github.com/magma-Devs/smart-router/commit/12ff2037f1716f3992af0e3d9bf7ca873c0598f1
[`149d2f5`]: https://github.com/magma-Devs/smart-router/commit/149d2f5d28935b74f1ee5326498698fcf9fd9af6
[`14b3034`]: https://github.com/magma-Devs/smart-router/commit/14b303425a9a54db50c20d52183100b1df6d03d5
[`165c923`]: https://github.com/magma-Devs/smart-router/commit/165c9232fa496c489f5154028e6d7ff6eb583269
[`175fda1`]: https://github.com/magma-Devs/smart-router/commit/175fda13ad0f618aafc349cd1d9a64570370b35c
[`18e581a`]: https://github.com/magma-Devs/smart-router/commit/18e581a2c9ef2abd20414cf9c22c00e890a4947e
[`1949100`]: https://github.com/magma-Devs/smart-router/commit/1949100f15fbfac6be5be48777ac6dd1a22c40d2
[`1979be8`]: https://github.com/magma-Devs/smart-router/commit/1979be87701c02da9d28afa2064af1a64707127c
[`1a7e4e1`]: https://github.com/magma-Devs/smart-router/commit/1a7e4e1911f76e70e7be076000f67d6e12e7378b
[`1b94175`]: https://github.com/magma-Devs/smart-router/commit/1b94175ad5545823a56a5b167d87d067b0cd5c68
[`1df3854`]: https://github.com/magma-Devs/smart-router/commit/1df3854455bbd1c732169b61fe624c914c9a3d1c
[`1eee645`]: https://github.com/magma-Devs/smart-router/commit/1eee645819b52661fffa7026a7da0eaed5393cfe
[`20dfe05`]: https://github.com/magma-Devs/smart-router/commit/20dfe05b1a8b74a66ee66d44d081127745384686
[`283ff53`]: https://github.com/magma-Devs/smart-router/commit/283ff533a27541feae959658eb9339801d9232b5
[`2d18323`]: https://github.com/magma-Devs/smart-router/commit/2d18323c03a9610f8f56b7a167fb4fad7945dcbc
[`2fd1c1e`]: https://github.com/magma-Devs/smart-router/commit/2fd1c1ed18a6f7bb8eb5c3cb9b7e7141184e49c8
[`2fe412c`]: https://github.com/magma-Devs/smart-router/commit/2fe412c9d594bee8121e676ee360f94155f03c95
[`30c7545`]: https://github.com/magma-Devs/smart-router/commit/30c754570edfae5322d9227a19f33491da967bdb
[`33093ee`]: https://github.com/magma-Devs/smart-router/commit/33093eed0bc78b48faeb64d99d7e27a614fbc852
[`334fda5`]: https://github.com/magma-Devs/smart-router/commit/334fda5f29e184b186f466c01fa5bc97f86dc2c6
[`36542ee`]: https://github.com/magma-Devs/smart-router/commit/36542eea89a9277fcea4b41d3612ef9d2f7827f7
[`3b7cdfc`]: https://github.com/magma-Devs/smart-router/commit/3b7cdfc12edb53e8d0f80514b8f6a95628a9e8c1
[`3cd14f0`]: https://github.com/magma-Devs/smart-router/commit/3cd14f072b08324a028b83ca0d290907282df04c
[`40dc024`]: https://github.com/magma-Devs/smart-router/commit/40dc024117f99918bc94ff759afe29db878d9ce5
[`435fcf0`]: https://github.com/magma-Devs/smart-router/commit/435fcf011f23c391090710575ac2a64977635295
[`4369814`]: https://github.com/magma-Devs/smart-router/commit/43698148b0e93c2b2f6aebd42e2b9cc09db779ae
[`44d75c6`]: https://github.com/magma-Devs/smart-router/commit/44d75c66b94dc1cc5602c5b5b07aa40e674c77fa
[`49fa95d`]: https://github.com/magma-Devs/smart-router/commit/49fa95d62f37cb446b62dbc525ad5f4599ae340a
[`4a0bc8a`]: https://github.com/magma-Devs/smart-router/commit/4a0bc8af72a81be70058bc3fc9368758fbf0cc54
[`4b87c8f`]: https://github.com/magma-Devs/smart-router/commit/4b87c8fa5ed7a36d62ecf87f38bbfd4236ed6b47
[`5215861`]: https://github.com/magma-Devs/smart-router/commit/521586179efc8cea09d6e81e778a49703f890466
[`531ea7a`]: https://github.com/magma-Devs/smart-router/commit/531ea7a47b32132deb686809b807690c1bccd842
[`536e39f`]: https://github.com/magma-Devs/smart-router/commit/536e39fc7baad214d660470ebec21575a0e73380
[`543b871`]: https://github.com/magma-Devs/smart-router/commit/543b87167d0be5af97e1d2d3ae84deb742c3bd6d
[`54591e5`]: https://github.com/magma-Devs/smart-router/commit/54591e57e18b17f8861a20a3a3feb7c1d9588f59
[`55bb865`]: https://github.com/magma-Devs/smart-router/commit/55bb8650f3f4bb951d0ce49943e4875cc604d8ba
[`56dba11`]: https://github.com/magma-Devs/smart-router/commit/56dba11aad03c51dd5b6f9be6a7a5a06d38bb78e
[`573bb7a`]: https://github.com/magma-Devs/smart-router/commit/573bb7a858c0bc9e582e7a6b743bdcdec53f7b71
[`5a17276`]: https://github.com/magma-Devs/smart-router/commit/5a17276d54c3981503b519fde0cb312918961f05
[`5abd0cb`]: https://github.com/magma-Devs/smart-router/commit/5abd0cb4cf09f9e58f137819638c8900bd3751ff
[`5b7fe36`]: https://github.com/magma-Devs/smart-router/commit/5b7fe362abdb5662d747dd8fc885627faac617dc
[`5c2809f`]: https://github.com/magma-Devs/smart-router/commit/5c2809f4c16c97df760b5a33c18012e4f45f4c75
[`5d1e294`]: https://github.com/magma-Devs/smart-router/commit/5d1e2942c0dbad4650b73a5bca022a8a8cbbbb5d
[`5d514dd`]: https://github.com/magma-Devs/smart-router/commit/5d514dd7d13dd43a46537daa9976eb097d831fe0
[`5e90850`]: https://github.com/magma-Devs/smart-router/commit/5e90850f9a0b6f6068ebb7c60f9088f1dfc17fad
[`62bd7ee`]: https://github.com/magma-Devs/smart-router/commit/62bd7ee85c7234e22f5fc487f8449b16095f95a0
[`62da26b`]: https://github.com/magma-Devs/smart-router/commit/62da26b0dee7f6f9706197f79d71a9229757fd21
[`63af99e`]: https://github.com/magma-Devs/smart-router/commit/63af99e24b8fa18d0f905de7b805f65d7a192fcf
[`63f1b56`]: https://github.com/magma-Devs/smart-router/commit/63f1b56aabd04c4184c4179ea2eb620391f1bf5f
[`6675c4f`]: https://github.com/magma-Devs/smart-router/commit/6675c4fe5fcfd45a0240dafbc6176a84f113e402
[`6cb23ea`]: https://github.com/magma-Devs/smart-router/commit/6cb23ea1c3c1159da396bd94f51adf9a8594b069
[`72be694`]: https://github.com/magma-Devs/smart-router/commit/72be69468d7c7bc90a2a8ab103b423ee0b77a304
[`77c4eda`]: https://github.com/magma-Devs/smart-router/commit/77c4edacdf50615d0db80c8338fe19e48d6f72f7
[`783ae7a`]: https://github.com/magma-Devs/smart-router/commit/783ae7a9e25879312139a53ee8af4cd6847dc6ae
[`7efa7b3`]: https://github.com/magma-Devs/smart-router/commit/7efa7b3908f52d1acf299df886ee8215606cf298
[`803d485`]: https://github.com/magma-Devs/smart-router/commit/803d4852ab9e6868b86454dba2aa7092986b330b
[`85e9f2d`]: https://github.com/magma-Devs/smart-router/commit/85e9f2da85fde8cb61748bacfdea8fe83e6f4278
[`8ab1762`]: https://github.com/magma-Devs/smart-router/commit/8ab1762943424a59f1d553e7dc51a09ce4195249
[`8c2610e`]: https://github.com/magma-Devs/smart-router/commit/8c2610e566450ba8cbcd4bbf84cc9842c1861b38
[`8e325e8`]: https://github.com/magma-Devs/smart-router/commit/8e325e87a53483fdce9b290407ffdbdb3160a77b
[`906a2d8`]: https://github.com/magma-Devs/smart-router/commit/906a2d820bf19f778e96174137e8cbe5e49cd2e1
[`91065f6`]: https://github.com/magma-Devs/smart-router/commit/91065f6ed53e01e3d9f31382c2b2284d99849dc9
[`9178c5b`]: https://github.com/magma-Devs/smart-router/commit/9178c5b7f1234a7b8b80a6e9c82ab9e07b70c1c8
[`920f46c`]: https://github.com/magma-Devs/smart-router/commit/920f46c923ce5bc15c208b3f37f9a3aafc17484c
[`9332503`]: https://github.com/magma-Devs/smart-router/commit/9332503c778b38014300111fadae0849dcb8834b
[`9537e50`]: https://github.com/magma-Devs/smart-router/commit/9537e5083b6775207af753c0094954f6609005dd
[`9568adf`]: https://github.com/magma-Devs/smart-router/commit/9568adf75541f864e192c37071943baf88803d86
[`97d5ed9`]: https://github.com/magma-Devs/smart-router/commit/97d5ed9d8693fe73d8ebfd533fbaead16e4f99fd
[`996f0a7`]: https://github.com/magma-Devs/smart-router/commit/996f0a79fbbbbfa6bc8ae29c94bfeb4d37d45327
[`9a9bf07`]: https://github.com/magma-Devs/smart-router/commit/9a9bf07e2b60e9f4cbfa28cf817ab753b3906392
[`9ac667e`]: https://github.com/magma-Devs/smart-router/commit/9ac667ee6532e60fd1ea6e9d6595e1fe6d2ce1e8
[`9c4679f`]: https://github.com/magma-Devs/smart-router/commit/9c4679fab45b7f95c0801a05952bf5794dcfcb5e
[`a2d8148`]: https://github.com/magma-Devs/smart-router/commit/a2d8148bddd63e03afa0a14858333741f770c8e1
[`a2d90c4`]: https://github.com/magma-Devs/smart-router/commit/a2d90c470170acec1a468d066a685b9d14b83a60
[`a362acf`]: https://github.com/magma-Devs/smart-router/commit/a362acf5001d9de8505b7fa215a1f018d06ce38b
[`a3ec336`]: https://github.com/magma-Devs/smart-router/commit/a3ec336cec7cc63ad0498788166173c0c5659da2
[`a7918e8`]: https://github.com/magma-Devs/smart-router/commit/a7918e8aa5b892f3ca9f6c32950b649d4619d774
[`a90ef62`]: https://github.com/magma-Devs/smart-router/commit/a90ef6298d7396532b1d76ee92df6d0a33890b84
[`aa005c3`]: https://github.com/magma-Devs/smart-router/commit/aa005c393f761acaf09407a0f7d3c037c4087942
[`ad25ad6`]: https://github.com/magma-Devs/smart-router/commit/ad25ad6d02af3e8b0372d8372dac07a1bfd0d96a
[`b1ec6c6`]: https://github.com/magma-Devs/smart-router/commit/b1ec6c677019962c8c7414943286920987d1265b
[`b33b7fc`]: https://github.com/magma-Devs/smart-router/commit/b33b7fcdcd4e829eec1dfbd489ce60e354d0f3bd
[`b3feebe`]: https://github.com/magma-Devs/smart-router/commit/b3feebe05cc6feceeb4a3b5b8c2a62f6af2a8969
[`b778757`]: https://github.com/magma-Devs/smart-router/commit/b7787577b49386edf3043e9bebdd7dd083035c35
[`baa9078`]: https://github.com/magma-Devs/smart-router/commit/baa90780922f5736edb7053a7fe0d92c051df8f2
[`bc6dfec`]: https://github.com/magma-Devs/smart-router/commit/bc6dfec2c1636a9aa11c1bdfac6f72259c04667a
[`bcf9d7d`]: https://github.com/magma-Devs/smart-router/commit/bcf9d7dbf9d0f5fbd7077bde1b51b482e50cd7ab
[`bf7abc5`]: https://github.com/magma-Devs/smart-router/commit/bf7abc5088573f4a4530703517d1f98052e8887b
[`c017377`]: https://github.com/magma-Devs/smart-router/commit/c017377d4105cbef4b07c283164b30d09478b142
[`c4485a7`]: https://github.com/magma-Devs/smart-router/commit/c4485a7b63d22888c263aff7e312723e3610ba04
[`c4e2a59`]: https://github.com/magma-Devs/smart-router/commit/c4e2a594e6cd0ec2804486e204c11f088faeacf9
[`c56ca12`]: https://github.com/magma-Devs/smart-router/commit/c56ca123fa9a89d7c8a429b59f0dea97ee80f1b4
[`c7c605a`]: https://github.com/magma-Devs/smart-router/commit/c7c605aaa8d177c7260c6b8c855c95a5729a5a08
[`c9713ed`]: https://github.com/magma-Devs/smart-router/commit/c9713ed66f4c0a2c7c9a0e2ee65cea2b2a826add
[`c977835`]: https://github.com/magma-Devs/smart-router/commit/c97783537a162937ffe0238a0dce01c3e7617197
[`ce4dfd8`]: https://github.com/magma-Devs/smart-router/commit/ce4dfd89fe7613ddf858aedf377e86d629ca7fb6
[`d007d91`]: https://github.com/magma-Devs/smart-router/commit/d007d913d418da6ad673f40946862f7126a3f534
[`d13051c`]: https://github.com/magma-Devs/smart-router/commit/d13051cfd337dd5ddaff3c907067155563f49c86
[`d3042be`]: https://github.com/magma-Devs/smart-router/commit/d3042be9e49c80fe845cc5bdf167549ba10380c9
[`d7e638b`]: https://github.com/magma-Devs/smart-router/commit/d7e638b7d6e6fc6c6bc4a197a4907f6002910ae8
[`d8532fc`]: https://github.com/magma-Devs/smart-router/commit/d8532fc947faf497674e7405e3cf42c65e9d9893
[`d8f2d2b`]: https://github.com/magma-Devs/smart-router/commit/d8f2d2b7a232505976c147016a2c71dcb628cd1d
[`d966dbe`]: https://github.com/magma-Devs/smart-router/commit/d966dbe599d19d013170f1dc9e99353f8d40170f
[`dcc8ad3`]: https://github.com/magma-Devs/smart-router/commit/dcc8ad3e210e3bed6f158c93071e9f5645607f0b
[`e3655f5`]: https://github.com/magma-Devs/smart-router/commit/e3655f5cadb24c3868071ef3d2622d9570289678
[`e6b1a8a`]: https://github.com/magma-Devs/smart-router/commit/e6b1a8a535af45bf6a49b985cbd7a272e461636f
[`e9dc693`]: https://github.com/magma-Devs/smart-router/commit/e9dc693dc32f6e13373586a93b9ff26670af20e0
[`ea5ad0a`]: https://github.com/magma-Devs/smart-router/commit/ea5ad0a931af3d26ba67be01e331d9b3f5e8b2b3
[`ed8e917`]: https://github.com/magma-Devs/smart-router/commit/ed8e917ff05d07e14513cff48c4fbff400e10ed9
[`edc9b10`]: https://github.com/magma-Devs/smart-router/commit/edc9b10d50597897022af035d54f30077a2c98a3
[`efbe226`]: https://github.com/magma-Devs/smart-router/commit/efbe2261ae1bb268fbd946bd2bf16cafc53b31b0
[`f10367e`]: https://github.com/magma-Devs/smart-router/commit/f10367e9f2ab9fadeee9e9831ede8b547e4d8ec7
[`f27b719`]: https://github.com/magma-Devs/smart-router/commit/f27b719d7bb9927db79b3b138d30f07852c71aeb
[`f4d15f2`]: https://github.com/magma-Devs/smart-router/commit/f4d15f2352a66a32bc0b3ce66c0ecf8a46fcbf42
[`f5e9b21`]: https://github.com/magma-Devs/smart-router/commit/f5e9b21530b54d39b963e5d77f0aa5766c50becc
[`fb11aec`]: https://github.com/magma-Devs/smart-router/commit/fb11aec93d4f2c6c3592a6d87664d82ff0ce3556
[`fb88150`]: https://github.com/magma-Devs/smart-router/commit/fb88150fbb86eca861a12ffd2605bb8a0487bbce
[`fbe2856`]: https://github.com/magma-Devs/smart-router/commit/fbe2856075ab581a1943323c64a2ac897e230393
[`feb43f9`]: https://github.com/magma-Devs/smart-router/commit/feb43f9c239cb40f83b27a1ecbee7340c1040ea1

## v1.5.8 — 2026-10-03

### Highlights

This release introduces a breaking change to upstream QoS scoring and routing behavior; operators must update their monitoring thresholds and routing configurations because the router no longer scores late write deliveries against an upstream, and routing pins are now evaluated as a preference rather than a strict constraint. For transaction broadcasts, writes now run to completion instead of cancelling slower upstreams, and subsequent nonce reads or hash lookups are explicitly routed to the specific upstream that accepted the initial write. REST payload handling has been updated to assign deliberate retry and fault verdicts to every HTTP status code, treating all non-2xx responses as node errors while ensuring that 5xx errors return the upstream's original JSON payload instead of a generic router-generated 500 status. Error classification has also been corrected across protocols to identify REST 409 codes and Besu's "Known transaction" messages as duplicate submissions. Finally, rejected XRPL submissions are now correctly categorized as node errors rather than valid client answers, and websocket connections now return the exact same batch refusal messages as their HTTP counterparts.

### Changes

#### ⚠ Breaking changes
- fix(rpcsmartrouter)!: cap and stop scoring a write's late deliveries, and send pins as a preference (MAG-4032) ([#476]) [`19894e3`]

#### New Features
- feat(rpcsmartrouter): send a write's nonce read and hash lookups to the upstream that accepted it (MAG-4032) ([#476]) [`00b33c4`]
- feat(chainlib): treat every REST status outside 2xx as a node error ([#448]) [`3be46ec`]
- feat(common): classify a REST 409 as "transaction already known" ([#448]) [`5edb3f4`]
- feat(common): give every REST status a deliberate retry and fault verdict ([#448]) [`bb1e708`]

#### Bug fixes
- fix(chainlib): treat a rejected XRPL submit as a node error, not the answer (MAG-4033) ([#462]) [`6c32999`]
- fix(chainlib): address #462 review: XRPL API errors, batched submits, one rule shape (MAG-4033) ([#462]) [`a24c811`]
- fix(chainlib): close three of the #462 review blockers (MAG-4033) ([#462]) [`3b5d84e`]
- fix(chainlib): tell a websocket caller the same batch refusal as HTTP (MAG-4033) ([#462]) [`b34b6f9`]
- fix(rpcsmartrouter): let a write's broadcast finish instead of cancelling the slower upstreams (MAG-4032) ([#476]) [`d109e6d`]
- fix(common): read Besu's "Known transaction" as a duplicate submission (MAG-4032) ([#476]) [`a27d724`]
- fix(rpcsmartrouter)!: cap and stop scoring a write's late deliveries, and send pins as a preference (MAG-4032) ([#476]) [`19894e3`]
- fix(common): keep the new REST status rows out of the JSON-RPC and gRPC tables ([#448]) [`d41f422`]
- fix(rpcsmartrouter): return a REST 5xx's own JSON reply instead of a router-made 500 ([#448]) [`839a842`]
- fix(common): stop blaming the endpoint for a REST reply that is the caller's answer ([#448]) [`4394a0c`]
- fix(relaycore): a node error the endpoint is not at fault for is not an ERROR ([#448]) [`314d498`]

#### Documentation updates
- docs(error-registry): make section 9 describe this branch, not its first commit ([#448]) [`2f83948`]
- docs(error-registry): record the verdicts and the two rules behind them ([#448]) [`e017423`]
- docs(error-registry): make the prose match the verdicts the review settled ([#448]) [`9b80ad8`]
- docs(error-registry): record decision 7, the broadcast fix and the status map ship together ([#448]) [`e541b11`]

[#448]: https://github.com/magma-Devs/smart-router/pull/448
[#462]: https://github.com/magma-Devs/smart-router/pull/462
[#476]: https://github.com/magma-Devs/smart-router/pull/476
[`00b33c4`]: https://github.com/magma-Devs/smart-router/commit/00b33c48154b5498f695d2cf1becea031f65da49
[`19894e3`]: https://github.com/magma-Devs/smart-router/commit/19894e3b2f5061b9e9d7cd109a5f293595e72ab8
[`2f83948`]: https://github.com/magma-Devs/smart-router/commit/2f83948aca2175cee700d6e7ae9eac6b32c93eb6
[`314d498`]: https://github.com/magma-Devs/smart-router/commit/314d498568dbfdcc430948291c5eeec207f83c91
[`3b5d84e`]: https://github.com/magma-Devs/smart-router/commit/3b5d84eb58b2800f13d4cd266c9147eab5a8b220
[`3be46ec`]: https://github.com/magma-Devs/smart-router/commit/3be46ec794efeaded922a82dbfa06455f0759e74
[`4394a0c`]: https://github.com/magma-Devs/smart-router/commit/4394a0ce8beccb71ff91fc2b5cce5c19960fbd87
[`5edb3f4`]: https://github.com/magma-Devs/smart-router/commit/5edb3f4c63d78d8c706358503153f9969da2d519
[`6c32999`]: https://github.com/magma-Devs/smart-router/commit/6c32999d2d56c36e2c0761792fed808f3f78c77d
[`839a842`]: https://github.com/magma-Devs/smart-router/commit/839a84223e3e3442e8ffffb2e3b30a2ba2c34eae
[`9b80ad8`]: https://github.com/magma-Devs/smart-router/commit/9b80ad888ebf6f310556ab1c608a729e990fa4ec
[`a24c811`]: https://github.com/magma-Devs/smart-router/commit/a24c811cd6301a9815eb232a9c108d55f4a3353f
[`a27d724`]: https://github.com/magma-Devs/smart-router/commit/a27d7241bc5f45b4c2170a5c0d1392746d15903d
[`b34b6f9`]: https://github.com/magma-Devs/smart-router/commit/b34b6f99641f95379699b3ab6a0f31115b7a6ae7
[`bb1e708`]: https://github.com/magma-Devs/smart-router/commit/bb1e708da1a60dcaa21a7fb6959c041ef806f40d
[`d109e6d`]: https://github.com/magma-Devs/smart-router/commit/d109e6d3b17e246efbc80fb1d242926c3fdccb76
[`d41f422`]: https://github.com/magma-Devs/smart-router/commit/d41f4224285b4f245debc3cfd9d7ac21d3722271
[`e017423`]: https://github.com/magma-Devs/smart-router/commit/e01742397b039f64d1fa51117f3c604fe394055a
[`e541b11`]: https://github.com/magma-Devs/smart-router/commit/e541b1130e1ff51cb783e236812d3c42c0339a51

## v1.5.7 — 2026-09-30

### Highlights

This release focuses on improving protocol compliance and error handling across the gateway's supported interfaces. For REST payloads, the router now correctly forwards the client's original `Content-Type` header to upstream providers. JSON-RPC routing behavior has also been corrected so that an error response containing an empty message string is treated as a node failure rather than a valid client answer, ensuring these responses correctly trigger provider failover. Additionally, websocket connections can now successfully match asynchronous replies to requests that were originally dispatched with a `null` ID. Finally, for operators verifying release artifacts in their deployment pipelines, the release checksum file is now signed and distributed as a cosign v3 bundle.

### Changes

#### Bug fixes
- fix(chainlib): forward the client's Content-Type on REST bodies (MAG-2745) ([#385]) [`ea59ca8`]
- fix(chainlib): treat a JSON-RPC error with an empty message as a node error, not the answer ([#453]) [`50aef72`]
- fix(rpcclient): match a websocket reply to a request sent with a null id ([#461]) [`105da39`]
- fix(release): sign the checksum file as a cosign v3 bundle ([#465]) [`1e9b620`]

[#385]: https://github.com/magma-Devs/smart-router/pull/385
[#453]: https://github.com/magma-Devs/smart-router/pull/453
[#461]: https://github.com/magma-Devs/smart-router/pull/461
[#465]: https://github.com/magma-Devs/smart-router/pull/465
[`105da39`]: https://github.com/magma-Devs/smart-router/commit/105da39f4a24f2cd73a228cd274ae1211b63f66c
[`1e9b620`]: https://github.com/magma-Devs/smart-router/commit/1e9b620cb7b946752d6836e2455e6eb7ab036907
[`50aef72`]: https://github.com/magma-Devs/smart-router/commit/50aef7251a3f451cc5cb84526a9a5fc5f7d8e5b1
[`ea59ca8`]: https://github.com/magma-Devs/smart-router/commit/ea59ca8d4fbde6c0afc2a8b8e9dc0c2a4598acd2

## v1.5.6 — 2026-09-28

### Highlights

This release introduces an opt-in size limit for payloads written to the finalization-aware cache, providing operators with tighter control over memory and storage consumption. Previously, unusually large RPC responses—such as extensive log filters or bulk block fetches—could consume disproportionate cache space and trigger premature eviction of frequently accessed data. By configuring a maximum byte size for cacheable entries, the router will now evaluate the payload size before committing it to the cache backend. If a response exceeds the configured threshold, the gateway bypasses the storage layer entirely and returns the data directly to the client. This selective caching strategy protects the cache hit rate for standard-sized, high-volume queries while still successfully fulfilling massive requests via the upstream provider.

### Changes

#### New Features
- feat(cache): opt-in cap on the size of entries the router writes ([#434]) [`a613e7b`]

[#434]: https://github.com/magma-Devs/smart-router/pull/434
[`a613e7b`]: https://github.com/magma-Devs/smart-router/commit/a613e7bd498801403c596ff012566ce4b7f7ea90

## v1.5.5 — 2026-09-27

### Highlights

This release refines upstream error handling by changing how the router interprets empty HTTP 404 and 405 status codes from provider gateways. Previously, an empty 404 or 405 was treated as a valid canonical answer from the underlying blockchain node, which could result in returning false empty responses to clients. The router now correctly classifies these specific gateway responses as node-level errors rather than valid payload data. By recognizing these as infrastructure failures, the router immediately triggers provider failover and correctly penalizes the failing upstream in the QoS scoring matrix. This classification also prevents the finalization-aware cache from being poisoned with incorrect missing-data states, ensuring accurate RPC results even when individual upstream gateways misbehave.

### Changes

#### Bug fixes
- fix(chainlib): treat a gateway's empty 404/405 as a node error, not the node's answer ([#435]) [`8804058`]

[#435]: https://github.com/magma-Devs/smart-router/pull/435
[`8804058`]: https://github.com/magma-Devs/smart-router/commit/880405853d1ea5a7885175aa68245423ddd01df2

## v1.5.4 — 2026-09-27

### Highlights

This release optimizes upstream networking and response processing, starting with the ability for node URLs to explicitly request `gzip` compression from their upstreams. To further reduce allocation overhead, upstream response bodies are now read into a single buffer when the content length is announced, and cache writes now snapshot the reply instead of performing a deep copy. Solana RPC processing is notably faster; the router now extracts tip slots directly by path, skips `getBlock` replies, evaluates list-based results from the first byte, and safely handles truncated replies. Finally, JSON-RPC block extraction is now strictly capped at 1 MiB and skips block-hash rules to prevent excessive memory consumption on oversized payloads.

### Changes

#### New Features
- feat(lavasession): let a node url ask its upstream for gzip (MAG-3844) ([#432]) [`887e2bc`]

#### Bug fixes
- fix(rpcsmartrouter): skip block-hash rules and cap JSON-RPC block extraction at 1 MiB ([#430]) [`a81a695`]
- fix(rpcsmartrouter): a cut Solana reply yields no tip slot, and say what the harvest reads (MAG-3843) ([#428]) [`2cbe875`]
- fix(rpcsmartrouter): snapshot the reply for the cache write instead of deep-copying it ([#431]) [`7f92995`]

#### Documentation updates
- docs(rpcsmartrouter): drop stale deep-copy wording, pin the snapshot's Metadata clone end to end ([#431]) [`17d4414`]

#### Other work
- perf(rpcsmartrouter): read the Solana tip slot by path, and skip getBlock replies (MAG-3843) ([#428]) [`686dc92`]
- perf(rpcsmartrouter): answer a Solana reply whose result is a list from its first byte (MAG-3843) ([#428]) [`cfb21b3`]
- perf(lavasession): read an upstream body into one buffer when its length is announced (MAG-3845) ([#429]) [`8d2ad1d`]

[#428]: https://github.com/magma-Devs/smart-router/pull/428
[#429]: https://github.com/magma-Devs/smart-router/pull/429
[#430]: https://github.com/magma-Devs/smart-router/pull/430
[#431]: https://github.com/magma-Devs/smart-router/pull/431
[#432]: https://github.com/magma-Devs/smart-router/pull/432
[`17d4414`]: https://github.com/magma-Devs/smart-router/commit/17d4414f53a5cef987e989a34039f7ec4112edfb
[`2cbe875`]: https://github.com/magma-Devs/smart-router/commit/2cbe875222049f33727a7d16062bbabaf69fd834
[`686dc92`]: https://github.com/magma-Devs/smart-router/commit/686dc92223296cce759ff02085eab723b0e767f7
[`7f92995`]: https://github.com/magma-Devs/smart-router/commit/7f929956a6ba3545583ea79842a513bf19f9ac02
[`887e2bc`]: https://github.com/magma-Devs/smart-router/commit/887e2bcf09538a936adb5ebda9b25c74f76cd650
[`8d2ad1d`]: https://github.com/magma-Devs/smart-router/commit/8d2ad1d075bd5e47ad8e4e87118e4f2e9c44752e
[`a81a695`]: https://github.com/magma-Devs/smart-router/commit/a81a6954fd1094962bb2acdc880bceabb73d120a
[`cfb21b3`]: https://github.com/magma-Devs/smart-router/commit/cfb21b33c28c1bf9fc92f3a58b7b7f4a9e4fcfc5

## v1.5.3 — 2026-09-23

### Highlights

Release v1.5.3 introduces four breaking changes, starting with strict decoding for the `resp-cache:` configuration block where unknown or misspelled keys now cause startup failures instead of falling back to defaults, requiring operators to remove or correct invalid keys in their charts before upgrading. Within that block, `resp-cache.tls` settings like `ca-file` or `cert-file` will fail startup if configured without `enabled: true`; operators must explicitly set `enabled: true` to use TLS, or remove the keys entirely to run in plaintext. Additionally, standalone cache topologies will refuse to start if more than one endpoint is defined in `addresses` or `read-addresses`, requiring operators to either reduce the list to a single address or explicitly set `topology: sentinel` or `topology: cluster`. For websocket clients, streamed subscription notifications like `eth_subscription` pushes no longer carry an injected `id`, meaning integrators must match pushes using `params.subscription` instead. Beyond these breaking changes, this release improves cache correctness by preventing node-bound methods from being cached and applying finalized lifetimes to hash-fetched answers once their block is final, while also serving gRPC reflection directly from internal snapshots rather than forwarding it. Finally, to improve backend resilience, unreachable RESP caches now trip a circuit breaker that is split by endpoint, and a socket leak caused by keeping failed setup connections alive indefinitely has been resolved.

### Changes

#### ⚠ Breaking changes
- fix(redisstore)!: refuse a tls block that was written without its switch (MAG-3683) ([#396]) [`71b5e52`]
  - a `resp-cache.tls` block that carries `ca-file`, `cert-file`, `key-file`, `server-name` or `insecure-skip-verify` without `enabled: true` now fails startup with "tls.* options are set but tls.enabled is not true — dangling configuration". This includes a block that writes `enabled: false` beside those keys, which v1.5.x accepted and ran as a plaintext connection. To run without TLS, remove the other tls.* keys or the whole block; `tls: {enabled: false}` on its own and `tls: {}` are still accepted. To run with TLS, set `enabled: true`, after which the files are read and verified at startup.
- fix(cache)!: give every router its own keyspace on a shared cache (MAG-3521) ([#397]) [`047a743`]
  - the `resp-cache:` configuration block is decoded strictly. A key the block does not define, including a misspelled one that v1.5.x ignored while the router ran on the default it was meant to override, now fails startup with "invalid resp-cache block (unknown keys are rejected so a misspelled setting cannot fall back to a default unnoticed)" naming the key; nested blocks such as `tls:` are held to the same rule. Correct or remove the key. A chart or template that renders an extra key under `resp-cache:` must be fixed before the router is upgraded.
- fix(rpcsmartrouter)!: judge a secondary cache entry by its contents before serving it (MAG-3597) ([#412]) [`3314109`]
  - streamed websocket subscription notifications (messages with no top-level `result` or `error`, such as `eth_subscription` pushes) no longer carry the `id` the router used to inject from the subscribe request. JSON-RPC 2.0 notifications have no id. A client that matched pushes on that injected id must key on `params.subscription` instead. Replies (the subscribe and unsubscribe acknowledgements, cached answers) still get the caller's id back.
- fix(redisstore)!: refuse a standalone address list longer than one (MAG-3672) ([#401]) [`018f821`]
  - a standalone `resp-cache` block (the default topology) with more than one entry under `addresses` or `read-addresses` now fails startup with "addresses are configured but topology is standalone, which dials exactly one". On v1.5.x such a block started and served every request from its first address, silently ignoring the rest. Keep a single address per list, or set `topology: sentinel` or `topology: cluster` if the list was meant for one of those.

#### Bug fixes
- fix(scripts): re-depth the moved cache lanes and keep the ownership harness ([#384]) [`970ab87`]
- fix(redisstore)!: refuse a tls block that was written without its switch (MAG-3683) ([#396]) [`71b5e52`]
- fix(redisstore): warn when credentials are configured without tls ([#396]) [`d5713cd`]
- fix(redisstore): compare the whole tls block instead of naming its keys ([#396]) [`0879471`]
- fix(cache)!: give every router its own keyspace on a shared cache (MAG-3521) ([#397]) [`047a743`]
- fix(cache): echo the keyspace a cache server scoped by, and say when it did not ([#397]) [`ecbaae1`]
- fix(cache): hold the key grammar at the server too, and name a gRPC prefix the RESP backend outranks ([#397]) [`236dbda`]
- fix(redisstore): trim both sides of the credential file, not only the right (MAG-3685) ([#398]) [`7086919`]
- fix(redisstore): read a credential file by one rule on both sides, both halves and both paths ([#398]) [`6ce9cda`]
- fix(redisstore): report an unreadable credential file once per outage, not per tick (MAG-3690) ([#399]) [`5267043`]
- fix(redisstore): make Close wait for the credential watcher to return ([#399]) [`0b10772`]
- fix(redisstore): hand a connection opened during a credential outage the last credentials read ([#399]) [`3ac5f29`]
- fix(redisstore): refuse a master-name whose topology line was forgotten (MAG-3671) ([#400]) [`8dd9c76`]
- fix(redisstore): name the resolved topology and master set on the debug endpoint too ([#400]) [`10b3a28`]
- fix(redisstore): purge the read endpoint too when reads are split (MAG-3673) ([#403]) [`94803cc`]
- fix(redisstore): ask the read endpoint its role before purging it, and skip the scan of a replica ([#403]) [`10b0e71`]
- fix(redisstore): make the cache lifetimes reachable on a RESP backend (MAG-3631) ([#405]) [`fc58a0c`]
- fix(redisstore): refuse an expiration lifetime that its multiplier rounds away ([#405]) [`f2c6f3e`]
- fix(redisstore): hold every cache lifetime to the millisecond a RESP expiry can express ([#405]) [`72ecaa9`]
- fix(cacheformat): return a caller's text request id byte for byte on a cache hit (MAG-3611) ([#408]) [`9ca4861`]
- fix(rpcsmartrouter): never cache an answer the spec says is not reproducible across nodes (MAG-3461) ([#409]) [`deeeb19`]
- fix(rpcsmartrouter): gate the cache on a node-bound method list, not the spec's deterministic flag ([#409]) [`2d7c2ac`]
- fix(rpcsmartrouter): file a latest-tagged answer under the key its lookup computes (MAG-3460) ([#410]) [`4cd9660`]
- fix(rpcsmartrouter): never file a lagging node's answer under the tip it would be served as ([#410]) [`4b6c95a`]
- fix(rpcsmartrouter): keep an answer fetched by hash for the finalized lifetime once its block is final (MAG-3462) ([#411]) [`97ad8e8`]
- fix(rpcsmartrouter)!: judge a secondary cache entry by its contents before serving it (MAG-3597) ([#412]) [`3314109`]
- fix(cacheformat): refuse a JSON-RPC envelope that answers nothing before the cache hit is accepted ([#412]) [`0b00d20`]
- fix(rpcsmartrouter): judge a primary cache entry by its contents too, so the tier makes no difference ([#412]) [`276d19a`]
- fix(redisstore)!: refuse a standalone address list longer than one (MAG-3672) ([#401]) [`018f821`]
- fix(redisstore): say out loud when certificate verification is off (MAG-3684) ([#402]) [`7566236`]
- fix(redisstore): warn about skipped certificate verification from the store, and show the marker in the docs example ([#402]) [`b8bdaac`]
- fix(performance): name which half of a split cache failed its probe (MAG-3674) ([#404]) [`41cb10f`]
- fix(redisstore): probe every endpoint within the same deadline, not one after the other ([#404]) [`2a7aef5`]
- fix(performance): classify a failed probe from every endpoint, not the first one met ([#404]) [`691b068`]
- fix(performance): trip a breaker when the RESP cache is unreachable (MAG-3676) ([#406]) [`6077548`]
- fix(performance): count symbolic-block tip failures and guard the sticky calls with the breaker ([#406]) [`fa45fb9`]
- fix(performance): split the breaker by endpoint, and keep it open on a backend slower than its budget ([#406]) [`bf642ed`]
- fix(redisstore): stop keeping connections that failed setup alive for the life of the process (MAG-3728) ([#407]) [`5883ac0`]
- fix(redisstore): prune collected subscriptions on every refresh and subscribe, and anchor the rotation tests ([#407]) [`9f66d86`]
- fix(grpc): serve reflection from snapshots instead of forwarding it ([#419]) [`7b4402a`]

#### Documentation updates
- docs(resp-cache): say what the dangling-tls refusal covers and the off-shape it accepts ([#396]) [`3cf9bcf`]
- docs(resp-cache): say how a router learns its prefix was dropped, and what a prefix is not ([#397]) [`2570da8`]
- docs(skill): can-i-merge gains the size question and the adversary review round ([#414]) [`40c2755`]
- docs(resp-cache): say what bounds the open sockets during an outage, and why the prune sits on the hot path ([#407]) [`5547aeb`]
- docs(secondary-cache): dangling timeout/mode warns, it does not abort startup (MAG-3538) ([#417]) [`4c52acc`]

[#384]: https://github.com/magma-Devs/smart-router/pull/384
[#396]: https://github.com/magma-Devs/smart-router/pull/396
[#397]: https://github.com/magma-Devs/smart-router/pull/397
[#398]: https://github.com/magma-Devs/smart-router/pull/398
[#399]: https://github.com/magma-Devs/smart-router/pull/399
[#400]: https://github.com/magma-Devs/smart-router/pull/400
[#401]: https://github.com/magma-Devs/smart-router/pull/401
[#402]: https://github.com/magma-Devs/smart-router/pull/402
[#403]: https://github.com/magma-Devs/smart-router/pull/403
[#404]: https://github.com/magma-Devs/smart-router/pull/404
[#405]: https://github.com/magma-Devs/smart-router/pull/405
[#406]: https://github.com/magma-Devs/smart-router/pull/406
[#407]: https://github.com/magma-Devs/smart-router/pull/407
[#408]: https://github.com/magma-Devs/smart-router/pull/408
[#409]: https://github.com/magma-Devs/smart-router/pull/409
[#410]: https://github.com/magma-Devs/smart-router/pull/410
[#411]: https://github.com/magma-Devs/smart-router/pull/411
[#412]: https://github.com/magma-Devs/smart-router/pull/412
[#414]: https://github.com/magma-Devs/smart-router/pull/414
[#417]: https://github.com/magma-Devs/smart-router/pull/417
[#419]: https://github.com/magma-Devs/smart-router/pull/419
[`018f821`]: https://github.com/magma-Devs/smart-router/commit/018f8219119f562e2c5d5f8bbe9413e0c476a1cc
[`047a743`]: https://github.com/magma-Devs/smart-router/commit/047a7438f8a1f5cd7e6af3698f794032a6524ac2
[`0879471`]: https://github.com/magma-Devs/smart-router/commit/087947188aa4bf3c177ca4fefc71a241549cb3d5
[`0b00d20`]: https://github.com/magma-Devs/smart-router/commit/0b00d2032266b134233ef1ddf3271dc50d3a2976
[`0b10772`]: https://github.com/magma-Devs/smart-router/commit/0b10772cd6d5e89b5f0190f492474768e9edfd1a
[`10b0e71`]: https://github.com/magma-Devs/smart-router/commit/10b0e71a4d90bd9aa2a85b34aceb57068da2f68a
[`10b3a28`]: https://github.com/magma-Devs/smart-router/commit/10b3a2870effc494416fe7061632d9d09bc5c805
[`236dbda`]: https://github.com/magma-Devs/smart-router/commit/236dbdab108a34dd69d9c566c2fcf858cd7a6165
[`2570da8`]: https://github.com/magma-Devs/smart-router/commit/2570da84a33a4128c7391ff49e2105831f4a6e98
[`276d19a`]: https://github.com/magma-Devs/smart-router/commit/276d19a6f82525779fcef0f6c318b798a7feee1a
[`2a7aef5`]: https://github.com/magma-Devs/smart-router/commit/2a7aef5057d9d3a7c050259fb03f2fc9b5515d8d
[`2d7c2ac`]: https://github.com/magma-Devs/smart-router/commit/2d7c2ac3becc8021c876811bc2ebd14e763d2bcf
[`3314109`]: https://github.com/magma-Devs/smart-router/commit/3314109ec0b7f47ce89d6042d46c4f743d4e5fb9
[`3ac5f29`]: https://github.com/magma-Devs/smart-router/commit/3ac5f293b9f5a2407094766c6b2c4cfce2c2e610
[`3cf9bcf`]: https://github.com/magma-Devs/smart-router/commit/3cf9bcf1d5b5679c229c209f2f219771da57fe27
[`40c2755`]: https://github.com/magma-Devs/smart-router/commit/40c2755a29a2f7cba31fa79812773168580005de
[`41cb10f`]: https://github.com/magma-Devs/smart-router/commit/41cb10f139af5ba0f073868f164efa62e1f27b3a
[`4b6c95a`]: https://github.com/magma-Devs/smart-router/commit/4b6c95afb4dcd46cd5dc440a40104d6f75edf4be
[`4c52acc`]: https://github.com/magma-Devs/smart-router/commit/4c52acca6614c9c8bb49be3740c0be709fea2f36
[`4cd9660`]: https://github.com/magma-Devs/smart-router/commit/4cd9660d0b330f909cc6a93aa43035270cba70e5
[`5267043`]: https://github.com/magma-Devs/smart-router/commit/52670430fac2e75da9bd196e9e1bed37f000e1bf
[`5547aeb`]: https://github.com/magma-Devs/smart-router/commit/5547aebcc3a25a681a98d872c24823c855b8c9ee
[`5883ac0`]: https://github.com/magma-Devs/smart-router/commit/5883ac0390661db35b25f121dc586deb5f0e7911
[`6077548`]: https://github.com/magma-Devs/smart-router/commit/60775480fc89c2023bee1347465e790e1b00e551
[`691b068`]: https://github.com/magma-Devs/smart-router/commit/691b06824f5e6344d11e3fc558938b27c43049ab
[`6ce9cda`]: https://github.com/magma-Devs/smart-router/commit/6ce9cda3754706f4bae766288bf50e8575db33ef
[`7086919`]: https://github.com/magma-Devs/smart-router/commit/70869190666947a0d32c511e9445e6f51b602168
[`71b5e52`]: https://github.com/magma-Devs/smart-router/commit/71b5e52e1d523347b4e81d943c21698bec86b272
[`72ecaa9`]: https://github.com/magma-Devs/smart-router/commit/72ecaa92d3bda33b185737eeff07983d95c8a896
[`7566236`]: https://github.com/magma-Devs/smart-router/commit/7566236147051ba48bca50f0f28c15d4c7261eab
[`7b4402a`]: https://github.com/magma-Devs/smart-router/commit/7b4402a05fb211787db4d8ef9f0f6545446081b6
[`8dd9c76`]: https://github.com/magma-Devs/smart-router/commit/8dd9c7657191a49d098e787323b1527a160cae1a
[`94803cc`]: https://github.com/magma-Devs/smart-router/commit/94803cc760224d2c64fabced07ab78df942a0e7e
[`970ab87`]: https://github.com/magma-Devs/smart-router/commit/970ab87196adb583e01d2c8427e7884b781fd100
[`97ad8e8`]: https://github.com/magma-Devs/smart-router/commit/97ad8e8212172787049ccbdcf807340fcedb0b97
[`9ca4861`]: https://github.com/magma-Devs/smart-router/commit/9ca486115cda27b3b7123cd5e977c374963692fc
[`9f66d86`]: https://github.com/magma-Devs/smart-router/commit/9f66d8618c76a0fe415e30571f7f6fb29643e4c8
[`b8bdaac`]: https://github.com/magma-Devs/smart-router/commit/b8bdaac40f086b03a4868c10ae640215612ae360
[`bf642ed`]: https://github.com/magma-Devs/smart-router/commit/bf642ed24c4ca16c04595ab9bbcd18c2fffda7e9
[`d5713cd`]: https://github.com/magma-Devs/smart-router/commit/d5713cdceeb50191f874c8d4794c99ba1bebd98e
[`deeeb19`]: https://github.com/magma-Devs/smart-router/commit/deeeb19bcf3dcd1fb9f5db0dab41482bb70be9b2
[`ecbaae1`]: https://github.com/magma-Devs/smart-router/commit/ecbaae12dff025681ad4b54becef4c6007a4e7e1
[`f2c6f3e`]: https://github.com/magma-Devs/smart-router/commit/f2c6f3eed4a26c136ed2f7a49e13396833f4e302
[`fa45fb9`]: https://github.com/magma-Devs/smart-router/commit/fa45fb9ba056ff32140d01ff7fa0398da5ad4013
[`fc58a0c`]: https://github.com/magma-Devs/smart-router/commit/fc58a0c1c2cab070ff97fdcb94ca38ec0327dd9a

## v1.5.2 — 2026-09-17

### Highlights

Release v1.5.2 introduces a fleet-wide claim registry to resolve sticky sessions across distributed router deployments, ensuring consistent provider routing for pinned requests. For finer control over upstream health, operators can now configure the endpoint disable threshold using the new `--bench-after` flag, while the router explicitly records the exact reason an endpoint is taken out of rotation to aid fault isolation. Cache observability is also expanded; a new debug endpoint exposes internal cache state, and integrators can now read which cache tier served a request directly off the reply. On the networking side, stateful relays can now broadcast to the backup tier, and websocket connections are upgraded on any path and kept alive to maintain persistent client sessions. Finally, this release resolves a memory leak that caused the router heap to grow indefinitely, and corrects per-attempt relay timeouts to trigger subsequent retry attempts rather than terminating the request entirely.

### Changes

#### New Features
- feat(lavasession): let stateful relays broadcast to the backup tier (MAG-3338) ([#353]) [`9297d7b`]
- feat(cache): make the tier that served a request readable off the reply (MAG-3540) ([#377]) [`5b84ef8`]
- feat(debug): add cache state endpoint ([#376]) [`27510de`]
- feat(cache): fleet-wide sticky-session claim registry ([#372]) [`ac6011c`]
- feat(session): resolve sticky sessions through the fleet claim registry ([#372]) [`5ed8a7e`]
- feat(observability): record WHY an endpoint was taken out of rotation ([#349]) [`9acdf92`]
- feat(common): record whether the endpoint was at fault, separately from retryability ([#340]) [`aa08042`]
- feat(smartrouter): add --bench-after, the endpoint disable threshold ([#340]) [`c33fdf6`]

#### Bug fixes
- fix(cv-lanes): address the 15 review findings on the two lanes ([#375]) [`0cc8789`]
- fix(lavasession): broadcast to a provider configured in both tiers once ([#353]) [`0848dd9`]
- fix(debug): address the cache-state review ([#376]) [`6b8e529`]
- fix(session): scope sticky claims per service, restore pin precedence, stop wedging ([#372]) [`c857382`]
- fix(session): carry the pin into the provider refill ([#372]) [`a808d41`]
- fix(session): stop the sticky refill spinning on a provider it already set aside ([#372]) [`d73625d`]
- fix(session): serve unpinned rather than failing when no primary can be claimed ([#372]) [`d6aa2d1`]
- fix(observability): one fault, one disable reason — address review ([#349]) [`f4d69ce`]
- fix(relay): make the per-attempt timeout trigger the next attempt, not kill this one ([#374]) [`9730cf5`]
- fix(relay): report an unconfirmed write as unclear, not failed ([#374]) [`4630899`]
- fix(relay): blame a hung endpoint on budget exhaustion, not on its window ([#374]) [`569ae3c`]
- fix(relay): close the three defects the second review found ([#374]) [`875c97b`]
- fix(relay): treat a connection that died mid-delivery as an unknown write outcome ([#374]) [`d6479d4`]
- fix(relay): an unclassified failure must not claim the write was never sent ([#374]) [`976631c`]
- fix(rpcsmartrouter): bound the router's own relays, not just the customer's ([#374]) [`8469439`]
- fix(relaycore): count a hedge when it fires, and warn once per request ([#374]) [`919888f`]
- fix(relaycore): stop leaking a goroutine per request when attempts outlive it ([#374]) [`165d292`]
- fix(relaycore): do not call a caller's disconnect our own timeout ([#374]) [`c80c327`]
- fix(rpcsmartrouter): let a hang reach the per-URL health machinery again ([#374]) [`cbc2003`]
- fix(relay): tell the client a write outcome is unclear whenever it is ([#374]) [`c1db76b`]
- fix(relaycore): deliver a straggler response the watcher is about to read ([#374]) [`077d152`]
- fix(relaycore): do not let a dispatcher's reason shadow the timeout ([#374]) [`79392c4`]
- fix(rpcsmartrouter): count the write's endpoints across every batch ([#374]) [`468d076`]
- fix(rpcsmartrouter): preserve timeouts for undispatched writes ([#374]) [`304beff`]
- fix: stop the router heap growing for the life of the process ([#386]) [`cc92100`]
- fix(chainlib): upgrade websockets on any path and keep them alive ([#355]) [`8d0a03a`]
- fix(release): pin GoReleaser so a tag push cannot break on an upstream patch ([#392]) [`199fc7d`]
- refactor(lavasession): drop the two legacy provider-block triggers ([#340]) [`dfa6182`]
- fix(rpcsmartrouter): count the node errors the endpoint counter was blind to ([#340]) [`0f701e6`]
- fix(lavasession): adapt the rebased-in epoch tests to the dropped block trigger ([#340]) [`90e3e9b`]
- fix(rpcsmartrouter): chain the health arms, quiet the session-cap log ([#340]) [`b8047ad`]
- fix(lavasession): drop the retired-session ceiling, not just the block it fed ([#340]) [`553b9e8`]
- fix(rpcsmartrouter): name the disable reason on the node-error arm ([#340]) [`451799f`]

#### Documentation updates
- docs(cross-validation): add a walkthrough and a demo lane per use case ([#375]) [`d014099`]
- docs(lavasession): record the primary-block decision where the next reader looks ([#353]) [`b3e3ade`]
- docs(cache): tell operators to read the tier off the reply, not the counter ([#377]) [`a632efd`]
- docs(test): name the real ticket for the known-wrong classifier rows ([#349]) [`e46a667`]
- docs: correct the operator-facing text the section 2 deletions invalidated ([#340]) [`8a5e605`]

#### Build process updates
- ci: stop PR test routers between validation runs ([#373]) [`d3cec95`]
- ci(pr-gate): skip the pruning verification on PublicNode so the fresh install comes up ([#382]) [`fc977c7`]
- ci: pin every workflow's actions to commit SHAs ([#395]) [`9ea206f`]
- build(release): stage the image binary through extra_files and move to GoReleaser v2.18.2 ([#394]) [`e5aa4f7`]

#### Other work
- style(relaycore): keep the RelayProcessor struct's alignment block intact ([#374]) [`1dfb1a3`]

[#340]: https://github.com/magma-Devs/smart-router/pull/340
[#349]: https://github.com/magma-Devs/smart-router/pull/349
[#353]: https://github.com/magma-Devs/smart-router/pull/353
[#355]: https://github.com/magma-Devs/smart-router/pull/355
[#372]: https://github.com/magma-Devs/smart-router/pull/372
[#373]: https://github.com/magma-Devs/smart-router/pull/373
[#374]: https://github.com/magma-Devs/smart-router/pull/374
[#375]: https://github.com/magma-Devs/smart-router/pull/375
[#376]: https://github.com/magma-Devs/smart-router/pull/376
[#377]: https://github.com/magma-Devs/smart-router/pull/377
[#382]: https://github.com/magma-Devs/smart-router/pull/382
[#386]: https://github.com/magma-Devs/smart-router/pull/386
[#392]: https://github.com/magma-Devs/smart-router/pull/392
[#394]: https://github.com/magma-Devs/smart-router/pull/394
[#395]: https://github.com/magma-Devs/smart-router/pull/395
[`077d152`]: https://github.com/magma-Devs/smart-router/commit/077d15267fa7057ddab50715cce7028382f5c822
[`0848dd9`]: https://github.com/magma-Devs/smart-router/commit/0848dd92445ba010f470692701c22306e71740a1
[`0cc8789`]: https://github.com/magma-Devs/smart-router/commit/0cc8789c9da98153246f3ca8e55ef1855d772721
[`0f701e6`]: https://github.com/magma-Devs/smart-router/commit/0f701e6b6af0d20cb65bd14e61fe083828f4528d
[`165d292`]: https://github.com/magma-Devs/smart-router/commit/165d29232364ae1c7a5922dff59c2aec1c918ba1
[`199fc7d`]: https://github.com/magma-Devs/smart-router/commit/199fc7de225794e52bbbb05dffbc26eafe492544
[`1dfb1a3`]: https://github.com/magma-Devs/smart-router/commit/1dfb1a3f174db4050022c19a1048bc7d48597c9b
[`27510de`]: https://github.com/magma-Devs/smart-router/commit/27510de445d843776cf61f827476e01f8c80b06b
[`304beff`]: https://github.com/magma-Devs/smart-router/commit/304beffb1281d3f4879f4366d85eba3b504f0552
[`451799f`]: https://github.com/magma-Devs/smart-router/commit/451799f61516af92bbef2fed85836d88750f2490
[`4630899`]: https://github.com/magma-Devs/smart-router/commit/46308991169f7e6b5330b7739e4550afbd9b9664
[`468d076`]: https://github.com/magma-Devs/smart-router/commit/468d076bd3b874731ad17428a76a2a8b379e9464
[`553b9e8`]: https://github.com/magma-Devs/smart-router/commit/553b9e8cbc19cfac79fbf81544142944372cfbd3
[`569ae3c`]: https://github.com/magma-Devs/smart-router/commit/569ae3ce3260eb1df6d90cdeeaba889d5c403ad7
[`5b84ef8`]: https://github.com/magma-Devs/smart-router/commit/5b84ef8c1a963b370b5ef8a659263aaba4c00e98
[`5ed8a7e`]: https://github.com/magma-Devs/smart-router/commit/5ed8a7efaf950dba7b5a366be1b056ebdd635a17
[`6b8e529`]: https://github.com/magma-Devs/smart-router/commit/6b8e529caa8dab663e7164b114efed8e9976596f
[`79392c4`]: https://github.com/magma-Devs/smart-router/commit/79392c4b79448a1ca43b8d6c34fe6f9482b75c9c
[`8469439`]: https://github.com/magma-Devs/smart-router/commit/8469439d904e83c6e08d57babc10bffe3fdc9e71
[`875c97b`]: https://github.com/magma-Devs/smart-router/commit/875c97b69750a7bf4e28423c402e96cb9cc744f0
[`8a5e605`]: https://github.com/magma-Devs/smart-router/commit/8a5e605439ba324d25a609b064038a4d3932d70c
[`8d0a03a`]: https://github.com/magma-Devs/smart-router/commit/8d0a03a78708dc5259ff736cd2515c88303c4d16
[`90e3e9b`]: https://github.com/magma-Devs/smart-router/commit/90e3e9b6336bdc46f5c215bfe576cf4d15c159ee
[`919888f`]: https://github.com/magma-Devs/smart-router/commit/919888f9d019a9abccb01233564e6f4130ccd3cd
[`9297d7b`]: https://github.com/magma-Devs/smart-router/commit/9297d7b6e5dc469a5a552d085a8b32874fa11af7
[`9730cf5`]: https://github.com/magma-Devs/smart-router/commit/9730cf519bbd370a8745ba2bb012ff80dd7251c1
[`976631c`]: https://github.com/magma-Devs/smart-router/commit/976631c81f26829bd719e6fc5b3c7620a85617e3
[`9acdf92`]: https://github.com/magma-Devs/smart-router/commit/9acdf922c435cca2fd0a1570d6082a581db5a85c
[`9ea206f`]: https://github.com/magma-Devs/smart-router/commit/9ea206fd78b25333da9ed230b2b67c7286a528f9
[`a632efd`]: https://github.com/magma-Devs/smart-router/commit/a632efd78354b375b953a4b16d965de861553d85
[`a808d41`]: https://github.com/magma-Devs/smart-router/commit/a808d418c7d0446336689f9153f7f18db5959827
[`aa08042`]: https://github.com/magma-Devs/smart-router/commit/aa080423520e153f479c7a16ea6cfc271924204e
[`ac6011c`]: https://github.com/magma-Devs/smart-router/commit/ac6011c933de71054d3c159556abde38dfe4ed57
[`b3e3ade`]: https://github.com/magma-Devs/smart-router/commit/b3e3ade95fef4d3088a053c9c5edd00ca565b2cb
[`b8047ad`]: https://github.com/magma-Devs/smart-router/commit/b8047adda88e1fb4196532cadd8d3c7abeec11df
[`c1db76b`]: https://github.com/magma-Devs/smart-router/commit/c1db76bf79a62af876d9aa8714d13d7def5dd22e
[`c33fdf6`]: https://github.com/magma-Devs/smart-router/commit/c33fdf604ada80ef1c842abd8aefa2960d0c55e7
[`c80c327`]: https://github.com/magma-Devs/smart-router/commit/c80c3278b8423cfe9a5b213782b9a802ccf1e01d
[`c857382`]: https://github.com/magma-Devs/smart-router/commit/c8573825b570bc08f2ba55a0ea490cfe28d883eb
[`cbc2003`]: https://github.com/magma-Devs/smart-router/commit/cbc2003d05413828fad03e567f985c143eab042d
[`cc92100`]: https://github.com/magma-Devs/smart-router/commit/cc92100a635c6498172595a1bbed77dc196bb63f
[`d014099`]: https://github.com/magma-Devs/smart-router/commit/d0140997209b96c5f5e85cff275c2993dff6eddf
[`d3cec95`]: https://github.com/magma-Devs/smart-router/commit/d3cec959f5809094a7512c7837729719532896b0
[`d6479d4`]: https://github.com/magma-Devs/smart-router/commit/d6479d449df1b067d1ccf5f33448c3c1f63e8583
[`d6aa2d1`]: https://github.com/magma-Devs/smart-router/commit/d6aa2d1a4b17f18e34640ddd8e45ed0cbb3d4e0f
[`d73625d`]: https://github.com/magma-Devs/smart-router/commit/d73625d8e05b4522e2b5065d0b9aacef14eb8827
[`dfa6182`]: https://github.com/magma-Devs/smart-router/commit/dfa6182ee2b9055f4224be6876db8631e1c212b6
[`e46a667`]: https://github.com/magma-Devs/smart-router/commit/e46a667690d5558ce20bb6905f17dee7fb2ae870
[`e5aa4f7`]: https://github.com/magma-Devs/smart-router/commit/e5aa4f778369db98bc84158c5bc13452f9271fcc
[`f4d69ce`]: https://github.com/magma-Devs/smart-router/commit/f4d69cef63f48edb2f5e3735610f3f5bf273c3c4
[`fc977c7`]: https://github.com/magma-Devs/smart-router/commit/fc977c7eb7024c76b03b1d413947e90a1c2a2a4d

## v1.5.1 — 2026-09-06

### Highlights

Release v1.5.1 introduces a configurable lookup budget for the RESP cache backend to help operators strictly bound remote cache latency. By setting the new `--cache-timeout` flag, integrators can prevent slow external cache queries from degrading overall RPC response times. On the orchestration side, this release corrects the behavior of the `/readyz` endpoint so that it is observable immediately upon boot. The readiness probe now registers internal health transitions faster, ensuring orchestrators and load balancers can accurately route traffic without artificial delays during startup or failover events.

### Changes

#### New Features
- feat(resp-cache): configurable cache lookup budget (--cache-timeout) + remote lab test lanes ([#371]) [`e4331c4`]

#### Bug fixes
- fix(metrics): make /readyz observed from boot and fast on health transitions ([#354]) [`ee1e437`]

[#354]: https://github.com/magma-Devs/smart-router/pull/354
[#371]: https://github.com/magma-Devs/smart-router/pull/371
[`e4331c4`]: https://github.com/magma-Devs/smart-router/commit/e4331c4a50d00ef64ea3c6cc924d9b3cda293260
[`ee1e437`]: https://github.com/magma-Devs/smart-router/commit/ee1e437e663fca716da7abe7860dd138b007bb76

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
