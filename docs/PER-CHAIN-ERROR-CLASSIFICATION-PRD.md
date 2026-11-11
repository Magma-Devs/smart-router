# PRD: per-chain error classification in lava-specs

**Status:** draft for R&D approval (MAG-4910), revision 2. Approvers: Nimrod or Tom, on the four
must-haves, the format and the split. Blocks MAG-4898 (the per-chain research), which starts once
this is approved. Revision 2 answers the review on PR #661 (findings 661-1 to 661-8) and a second
review of the same text; section 15 maps every finding to the change it got.

**Baseline.** Everything marked "today" was read on 2026-11-10 at smart-router `origin/main`
`6c237073` (v1.6.8-2), lava-specs `main` `bda05974`, smart-router-helm-chart `main` `2446606` and
smart-router-infrastructure `main` `99f0dbb4`. File and line references are to those commits.
Revision 1 read smart-router `2d99d4b7` and lava-specs `8c1474c`. Since then MAG-4817 moved the XRP
rows into `error_classifier.go` and grew them from 9 to 15, and #546, #580, #582, #656, #662, #667
and #678 changed the classifier, the relay processor and the spec keys the router reads.

**Where this departs from the ticket.** Four facts in MAG-4910 read differently in the code, and
the design below follows the code:

- The release image does not bundle `specs/`. Only the dev image and `docker-compose.yml` use the
  repo folder; every deployed router loads lava-specs `main` at boot (2.3).
- "Slot already used is always unknown" (Q45) is not today's behaviour. It is a rule to build,
  and section 7.3 says how.
- The ticket locks vendor wording out of specs, and does not say the router's own table has any.
  Three rows name a vendor in their comment or commit. Two carry a vendor's wording: Alchemy's
  `-32600 "unsupported method"` and a `blocked … method|rpc` row written for QuickNode's paid
  tier. The third, "pending state is not supported", is attributed to Tatum but is Erigon's own
  text (2.1). The rows the ticket calls geth-wording rows are node-software wording (geth, Erigon,
  Nethermind), which R-M allows; their problem is that they apply to every JSON-RPC chain (2.1).
- A chain row can shadow a rate-limit row today, because chain rows run first. The locked rules
  in section 7 have to close that, not only the write path.

## 1. Goal

Each chain's spec in lava-specs carries that chain's error classification: which code or message
from that chain's nodes means which registered verdict. The router keeps everything that decides
what a verdict *does*: the interface rules (JSON-RPC, REST, gRPC, GraphQL), connection errors,
status rules, rate limits, the unknown default, retry and scoring, and write safety, including the
write methods that must stay stateful and the "slot already used" answers.

The design rests on one sentence: **a spec row may say what a node's answer means; it may never
say what the router does about it.** Rows point at verdicts the router already registers. They
cannot define a verdict, a retry, a status rule or a write outcome.

## 2. Today

### 2.1 Classification is code

- Rows are hard-coded in `protocol/common/error_classifier.go`. Each row is a matcher plus a
  pointer to a registered `*LavaError` (`protocol/common/error_registry.go:556-559`). The matcher
  kinds are `CodeEquals`, `MessageContains` (case-insensitive), `MessageRegex` (case-sensitive
  unless `(?i)`), `CodeAndMessage`, `HTTPStatusContains` and `GRPCCodeEquals`
  (`error_registry.go:432-550`). The XRP rows use a helper, `xrplMessageStartsWith`
  (`error_classifier.go:184-188`), which builds a `MessageRegex` anchored at the start of the
  message after an optional `HTTP <status>: ` prefix. It is not a seventh kind.
- `ClassifyError` decides a connection error before any row, then runs the chain-family rows,
  then the transport rows, then returns `UNKNOWN_ERROR`. First match wins
  (`error_classifier.go:788-820`). `UNKNOWN_ERROR` is retryable and may-have-reached-node, so an
  unmatched error fails open (`error_registry.go:589-596`).
- Because chain rows run first, **a chain row can shadow any transport row**, the rate-limit and
  status rows included. The six tables use that on purpose: Solana's `-32005` ("node is
  unhealthy", `:31`) shadows the generic `-32005` (`:222`), which is EVM's "limit exceeded".
- Chain-family rows exist for six families only (`error_classifier.go:24-167`): Solana (14 rows),
  Bitcoin (10), Cosmos SDK (4), Starknet (16), NEAR (4) and XRP (15, at `:108-166` since MAG-4817).
  There is **no EVM family table**. Every geth, Erigon, Nethermind and Besu row sits in the generic
  JSON-RPC table, so it applies to every JSON-RPC chain, Solana and Starknet included
  (`error_classifier.go:269-280, 293-318, 339-346`, and `:241`). Aptos, Stellar, TON, Tron,
  Cardano and Sui are mapped families with no rows, and EVM is mapped (48 keys) with no table.
- `chainFamilyMap` (`error_registry.go:280-368`) maps 106 chain ids. Four of them (AVAXC, AVAXCT,
  AVAXP, AVAXPT, `:288-289`) are not lava-specs names, which are AVALANCHEC, AVALANCHECT,
  AVALANCHEP and AVALANCHEPT. lava-specs declares 306 chain indexes in 158 files, and 204 of them
  are not in the map, so they get the transport rows only. Examples left out: GNOSIS, LINEA,
  SCROLL, MONAD, KAVA, SEI, POLKADOT, AVALANCHEC.
- Transport rows: JSON-RPC 98, REST 68, gRPC 32 (`error_classifier.go:201-627`), counting the rows
  the init step builds from the 19 `httpStatusRows`: a digit-matching row per status on every
  table, and a code row per status on REST as well. Three rows name a vendor:
  - Alchemy's `-32600 "unsupported method"` (`:208-212`, PR #632): a vendor's wording, which R-0.2
    rules out.
  - "Provider-disabled methods (e.g. QuickNode paid tier)", `blocked … \b(?:method|rpc)\b`
    (`:255-258`): written for a vendor's wording.
  - "pending state is not supported" (`:234-241`, #667, MAG-5021). Its comment, commit and fixture
    attribute it to Tatum, but the string is Erigon's own `errPendingStateNotSupported`
    (erigontech/erigon `rpc/jsonrpc/eth_call.go:61`), so it is node-software wording.
- How a status reaches the classifier. On REST the code is the HTTP status
  (`direct_rpc_relay.go:911`). GraphQL is classified on the REST table, with a status the GraphQL
  reader derives from the reply and its HTTP status (`graphqlMessage.go:490`,
  `direct_rpc_relay.go:1034`). On JSON-RPC the code is the body's JSON-RPC code, or the HTTP status
  when the body has none, and a non-2xx reply's message is prefixed `HTTP <status>: `
  (`direct_rpc_relay.go:703-720`); every direct-RPC HTTP error is worded the same way
  (`chainlib/node_error_handler.go:80`).
  `HTTPStatusContains` matches a status number anywhere in the message, bounded by non-digits
  (`error_registry.go:502-534`), so the "429" in Bitcoin's "min relay fee not met, 429 < 1000"
  matches it. On gRPC the code is the gRPC status.
- The gRPC table checks InvalidArgument, NotFound, OutOfRange, Unimplemented and Aborted before
  the rate-limit phrases (`error_classifier.go:482-502`, phrases `:512-514`, Unavailable `:517`),
  and `TestGRPCRateLimitTexts_TheCodeRowsAboveStillDecide`
  (`grpc_rate_limit_vocabulary_test.go:57`) pins that order. The comment at `:518-538` records why
  DeadlineExceeded, PermissionDenied, Unauthenticated, ResourceExhausted, Canceled and DataLoss
  are left without a row.
- Test fixtures: `protocol/common/testdata/error_fixtures.json` holds 102 cases (EVM 43, Solana
  18, Starknet 14, Bitcoin 9, XRP 9, NEAR 4, Cosmos SDK 2, Aptos 2, Stellar 1; by transport 97
  JSON-RPC, 3 REST, 2 gRPC), replayed by `TestErrorFixtures` (`error_fixtures_test.go:56`).
  `TestFaultAxis_EveryRegisteredCodeIsAccountedFor` (`error_fault_axis_test.go:203`) fails when a
  registered verdict has no fault decision. The status rules are tested elsewhere:
  `TestRESTStatusRows` and `TestRESTStatusRowsDoNotReachOtherTransports`
  (`protocol/common/rest_status_rows_test.go:15, 77`), `TestEndpointFault_AnswerShapes`
  (`protocol/rpcsmartrouter/endpoint_fault_test.go:35`) and `TestGRPCStatusError`
  (`protocol/lavasession/direct_rpc_connection_test.go:466`).
- A new chain error therefore needs a code release. MAG-4824 (v1.6.7), MAG-4817 and MAG-5021
  (both v1.6.8) are examples.

### 2.2 How a write is protected today

- The spec's `category.stateful` (`types/spec/api_collection.go:105`) sends a write to every
  provider at once.
- `Policy.Decide` stops a stateful relay after that fan-out, whatever the verdicts, with one
  exception: every failure so far was a rate limit, the attempts completed, and the relay is not a
  stateful batch with a node error (`protocol/relaypolicy/policy.go:40-60`).
- `onlyRateLimited` (`protocol/relaycore/relay_processor.go:1520`, fed at `:669`) counts a node
  error as a rate limit when its classification carries the rate-limit sub-category
  (`error_classifier.go:759`, set on the result at `protocol/common/endpoints.go:698`). The same
  flag holds the endpoint off: a rate-limited outcome records a hold-off of 30 seconds, doubling on
  repeats (`rpcsmartrouter_server.go:3241-3255`). **So the verdict a row names decides whether a
  write may be sent again, and whether the providers that gave it are held off.**
- The unclear-write decision comes from silence and from the may-have-reached-node flag on
  protocol errors, which are classified with no chain family (`relaycore/results_manager.go:72`).
  A node error is a reply and passes through (`writeOutcomeIsUnknown` and
  `unclearWriteOutcomeFor`, `rpcsmartrouter_server.go:1447-1508`, the reply rule at `:1490-1492`).
  The reply is an `*unclearWriteError` with four reasons (`:1421-1445`, #546).
- REST decides whether a 5xx body is the node's own reply with no chain family and an empty
  message (`ServerErrorIsNodeReply`,
  `protocol/chainlib/chainproxy/rpcInterfaceMessages/restMessage.go:74-78`, called at
  `rpcsmartrouter_server.go:6032`). That keeps a gateway's 502 or 504 on a write "unclear", and no
  chain row reaches it.
- Rows reach more than node replies, though: a direct-RPC transport or status error gets its retry
  and health verdicts with the chain family (`rpcsmartrouter/error_mapper.go:23-37, 71-83`, through
  `chainlib/node_error_handler.go:119-126`).
- A sibling's "already known" or "nonce too low" after another node accepted the write is released
  without a score (`rpcsmartrouter_server.go:2688-2704`, applied at `:3227-3240`; the release reads
  only the rate-limit flag). There is no slot-already-used group: each such answer keeps its own
  verdict and the node's body is forwarded.

### 2.3 Specs already change behaviour, and load unpinned

- Spec keys that drive behaviour today: `stateful`, `timeout_ms` (`protocol/chainlib/common.go:737-738`),
  `deterministic` (`rpcsmartrouter_server.go:6345, 6542`), `hanging_api` (`:3343`,
  `recovery_probe.go:136`), `block_parsing`, `parsers`, `imports`, a spec's `enabled` (a disabled
  spec serves no api, `base_chain_parser.go:798`), verification `severity` (MAG-3950, v1.6.8) and
  request `headers`, now on every interface (#662).
- `imports` is resolved by `DoExpandSpec` (`types/spec/expand.go:20-103`, the merge at `:72-100`).
  Only `api_collections` are inherited; within a collection the child's entry wins by name
  (`api_collection.go:580-582`, `combinable.go:37-67`). 144 of the 158 root files import another.
- **Unknown JSON keys are dropped without a word**, on both load paths (`utils/keeper/spec.go:23-26`,
  `utils/specfetcher/fetcher.go:399-411`). An older router given a spec with a new section ignores
  it. The spec struct has no version field (`types/spec/spec.go:11-21`), and no spec carries an
  `errors` key today.
- `--use-static-spec` (`cobra_common.go:79`) accepts a local file or folder, or a GitHub or GitLab
  URL (`utils/specfetcher/fetcher.go:120-264`). A URL with no ref fetches the default branch
  (`github.go:33-37`). A ref is not checked to be a tag or commit. The resolved commit is never
  recorded, because the tarball's pax header that carries it is skipped (`tarball.go:73-75`). The
  boot log names the source (`statetracker/state_tracker.go:83-86`), but nothing exports the
  source, ref or commit as a metric; the only related gauge is `smartrouter_protocol_version`
  (`smartrouter_metrics_manager.go:428-431`).
- The release image ships no specs: `docker/Dockerfile.release:26` and `Dockerfile.ci:13` copy the
  binary only, and the goreleaser archives are binary-only (`.goreleaser.yaml:48-57`). The dev
  image copies the repo's `specs/` folder (`docker/Dockerfile:30`), and `docker-compose.yml:53`
  defaults to it. That folder holds 15 files, 14 of them also in lava-specs, and all 14 differ from
  lava-specs.
- The router refuses to start with no spec source (`rpcsmartrouter.go:2569-2581`; the message
  still names the retired `--static-spec-paths`).
- The Helm chart's default is `staticSpecs.url: https://github.com/magma-devs/lava-specs` with no
  ref (`smart-router-helm-chart/charts/smart-router/values.yaml:450-458`, passed as is by
  `templates/routers_deployment.yaml:368-369`). No tenant in smart-router-infrastructure overrides
  it. **Every deployed router loads lava-specs `main` at boot**, so a merged spec reaches each
  router at its next restart.

### 2.4 lava-specs has no release discipline yet

- No tags, local or remote. Default branch `main`. No `CODEOWNERS` file, now or in the history.
- `spec_guards.yml` runs on every PR: the guard scripts' self-tests, `check_node_admin_rpcs.sh`,
  `check_chain_identity.sh` and `check_disabled_count.sh`. `spec_pipeline.yml` runs the agent's
  phases 8 to 11 once, when a spec PR opens, a hand-edited one included, with the add-collection
  guard (#163); a commit pushed after that is not re-checked by it.
- `check_stateful.sh` runs only inside the agent (its method-schema gate and Phase 10a, `SKILL.md:675,
  696-698`). It knows a write by an exact-name list (`check_stateful.sh:97-119`: Substrate, EVM,
  Cosmos and Tendermint, Solana, Starknet), so it misses the writes of the other families: Bitcoin's
  `sendrawtransaction`, `submitpackage`, `bb_sendTransaction` and `POST /tx/send`, NEAR's `send_tx`,
  XRP's `submit` and `submit_multisigned`, Sui's `ExecuteTransaction` and `executeTransaction`, and
  Concordium's `SendBlockItem` and `PUT /v0/submit…`. It also never evaluates a gRPC or Tendermint
  api: those collections carry `"type": ""`, and the empty field collapses in its `read` at
  `:188`, so Cosmos `BroadcastTx` and `broadcast_tx_*` are never checked where they are declared.
  Today no write is left non-stateful: a corrected replica over all 10,311 api rows on `main` finds
  132 writes, all stateful.
- `check_unused_fields.sh` rejects fifteen removed key names at any depth (`:27`), including
  `title`, `description`, `local` and `subscription`, and the router's `bundled_specs_guard_test.go`
  does the same for `specs/`. It runs in `create_spec.yml` and in `spec_pipeline.yml`'s post-fix
  guard, not in `spec_guards.yml`. The errors section must not use those names.
- `check_update_diff.sh` fails on any removed field, and keys array elements by position, so
  inserting a row in an array reads as a change to every later row. In its update modes the
  create-spec skill never re-emits a file: every write is one `jq` assignment. So an existing
  errors section survives an update. New-chain mode builds from templates, so the section appears
  in a new spec only once the templates describe it (MAG-4898 part 3).

## 3. The split

| Lives in the spec (per chain) | Stays in router code |
|---|---|
| Which node code or message means which **registered verdict** for this chain's nodes | The verdict registry: names, codes, category, sub-category, retryable, may-have-reached-node |
| Each row's rulebook class, interface, read or write scope, source and versions checked | What a verdict makes the router do: retry, stop, hold off, mark unhealthy, score, cache |
| Inheritance of rows through `imports`, with override and removal by row id | Interface rules: the JSON-RPC envelope, REST status, gRPC codes, GraphQL `errors` |
| A chain's own wording added to the slot-already-used group (7.3) | Connection errors, HTTP and gRPC status rules, rate-limit detection, the unknown default |
| | Write safety: fan-out, the single stop, the unknown outcome, the slot-already-used signatures, the write methods that must stay stateful |
| | The rows that name a locked verdict (XRP's three, 7.6), the source allowlist (7.5) |

What moves is the six family tables, less XRP's three router-owned rows, and later, chain by
chain, the geth-wording rows (section 11). What never moves is listed in section 7.

## 4. Format

### 4.1 The section

One `errors` object per spec, next to `api_collections`:

```json
{
  "index": "SOLANA",
  "api_collections": [ "..." ],
  "errors": {
    "schema_version": 1,
    "rows": [ "..." ]
  }
}
```

- `schema_version` is an integer, and it is the **schema** part of the lava-specs tag (section 5).
  Every section in one tag carries the tag's schema number, and CI enforces it, so a child and its
  parents never disagree. A router knows the schema versions it reads. It drops a section with any
  other version whole, for that chain only (7.4). It never reads part of a section.
- `imports` is optional: the specs whose rows this section inherits. Absent means the spec's own
  `imports`, the default. `[]` inherits no rows. It changes only the rows; the spec's api
  inheritance is untouched. Section 9 uses it to keep today's coverage at the first tag.
- The section is optional. A spec without it loads on every router customers run today, because
  unknown keys are dropped (2.3). A router that reads sections treats a missing one as "no chain
  rows of its own", which is today's behaviour for 204 chains.

### 4.2 The row

```json
{
  "id": "solana.blockhash_not_found",
  "match": { "contains": "blockhash not found" },
  "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND",
  "class": "C6",
  "interface": "jsonrpc",
  "scope": "write",
  "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/sdk/src/transaction/error.rs",
  "versions": ["agave 2.1"]
}
```

| Field | Required | Meaning |
|---|---|---|
| `id` | yes | Stable name, unique within the chain after inheritance. Convention `<family>.<snake_case>`. A child overrides a parent's row by repeating its id. |
| `match` | yes | Exactly one of `{"code": N}`, `{"contains": "text"}`, `{"regex": "re2"}` or `{"code": N, "contains": "text"}`. `contains` is case-insensitive, as `MessageContains` is. `regex` is Go RE2, case-sensitive unless it says `(?i)`, and must compile. There is no status, gRPC-code or header matcher: those are interface rules and stay in code, and 7.2 limits what `code` may name on each interface. |
| `error` | yes | The **name** of a registered router error, for example `CHAIN_BLOCK_NOT_FOUND`. A name the router does not register fails validation, and so do the names in 7.2. |
| `class` | yes | The rulebook class, C2 to C7. The router keeps one table of the classes each registered error may carry, and a row that disagrees fails validation. The class is for the reader and the change log; the router acts on `error`. |
| `interface` | no | `jsonrpc`, `rest`, `grpc`, `graphql` or `tendermintrpc`. Absent means every interface the chain serves after inheritance. |
| `scope` | yes | `read`, `write` or `both`. A `write` row is evaluated only for stateful apis, a `read` row only for the rest. Because the router keeps write methods stateful (7.5), a spec cannot move a write out of the write rows. |
| `source` | yes | A link to the official definition: the protocol spec, the node software's docs, or the source file that defines the error, pinned to a tag or commit. It must be on the router's allowlist (7.5). |
| `versions` | yes | The node software versions the row was checked against. At least one. |
| `slot_used` | no | `true` adds this row's answers to the slot-already-used group (7.3). Only on `write` or `both` rows. It can widen the router's group, never narrow it. |
| `disabled` | no | `true` removes the parent row with the same `id`, for this spec and its children. A disabling row carries only `id`, `disabled` and optionally `note`; any other field fails validation. |
| `note` | no | One line for the reader. Not `description` and not `title`: both are rejected key names (2.4). |

### 4.3 Matching

For a node error on chain X, the router evaluates, in order, first match wins:

1. connection errors, decided before any row, as today;
2. **the locked prefix**, all router code:
   1. explicit rate limits: the transport table's `NODE_RATE_LIMITED` rows that read a rate-limit
      signal, that is `CodeEquals(429)` and the phrases "rate limit", "too many requests" and
      "enhance_your_calm" (JSON-RPC `:283-290`, REST `CodeEquals(429)` from `:621`), on gRPC the four
      `GRPCRateLimitTexts` (`:512-514`), and XRP's `tooBusy`/`slowDown` row (7.6). On gRPC a phrase
      counts only when the status is not one of the five the gRPC table checks first today
      (InvalidArgument, NotFound, OutOfRange, Unimplemented, Aborted), which is today's order. The
      row that matches a status number anywhere in the text, `HTTPStatusContains(429)`, is **not**
      in the prefix: it stays after the chain rows, where it is today;
   2. XRP's `tefALREADY` row (7.6);
   3. the locked statuses (7.2): a reply whose HTTP status, read as the code on REST and GraphQL or
      from the `HTTP <status>: ` prefix on every other interface, is 401, 403, 409, 429, 502, 503,
      504, 520 to 527 or 530, or whose gRPC status is DeadlineExceeded, PermissionDenied,
      ResourceExhausted, Unavailable or Unauthenticated, is decided by the transport table and
      never reaches step 3;
3. X's spec rows, as resolved by inheritance (4.4), in file order, filtered by interface and scope;
4. the router's built-in rows for X's family, until the switch removes that table (section 9);
   after it, the router-owned rows that must follow the spec rows (XRP's engine-result catch-all,
   7.6);
5. the remaining transport rows;
6. `UNKNOWN_ERROR`.

Step 2 is the only change to today's order. Replayed at `6c237073` with the XRP placement of 7.6,
it gives today's verdict for all 102 fixtures and for the 30 counterexamples of 8.2 that must keep
theirs. Revision 1's prefix moved every row that produces `NODE_RATE_LIMITED`, the
digit-matching status row included, and ignored the gRPC order. It also passed all 102 fixtures,
yet it turned six counterexamples into `NODE_RATE_LIMITED`: Bitcoin's `-26` "min relay fee not met,
429 < 1000" and "mempool min fee not met, 429 < 1000", Agave's `-32005` "Node is behind by 429
slots", and a gRPC Aborted, InvalidArgument and NotFound that carry a rate-limit phrase. The fee in
the Bitcoin message is the client's own number, so under revision 1 a client could hold off a
chain's providers, and open the write re-send of 2.2, with one deliberately priced transaction. The
fixtures hold 97 JSON-RPC cases, 3 REST and 2 gRPC, which is why the replay alone could not see it.

What step 2 changes by design: below it, a chain row can no longer hide an explicit rate limit, a
locked status or XRP's already-known answer. A reply in a locked status now takes the status's
verdict even when a chain row matches its text. In the replay, a REST 403 whose body says "account
not found" becomes `NODE_ACCESS_DENIED` instead of `CHAIN_ACCOUNT_NOT_FOUND`, and a gRPC
ResourceExhausted that carries "insufficient fee" becomes `UNKNOWN_ERROR`, the router's deliberate
answer to that status, instead of `CHAIN_INSUFFICIENT_FEE`. No fixture has that shape. If MAG-4898
finds a chain whose nodes give a locked status a meaning of their own, that is a change to the
router's status rule, ticketed as such. A chain row may still shadow any other transport row, which
is what chain rows are for: Solana's `-32005` still shadows the generic `-32005`.

Two decisions sit outside the rows, and the reader keeps them there: the connection-error step,
which runs before any row, and REST's "is this 5xx the node's own reply", which classifies with no
chain family (2.2). One more path is why ResourceExhausted is locked: a gRPC rate limit that only
the reply metadata shows is applied only when the rows return `UNKNOWN_ERROR`
(`direct_rpc_relay.go:1208-1209`), so any row that matched such a reply would hide it.

### 4.4 Inheritance

Rows inherit through `imports` the way `api_collections` do, unless the section names its own
`imports` (4.1). The child's rows come first, then each parent's rows the child did not override or
disable by `id`, in the parents' order. ETH1's rows reach BASE, and COSMOSSDK's reach COSMOSHUB. A
child that has no section of its own inherits its parents' rows as they are. Only `rows` inherit;
`schema_version` is the tag's, so a child's and its parents' sections always share it.

This is new code in `DoExpandSpec`, which today merges only `api_collections` (`expand.go:72-100`).
With 144 importing files it is the common case, not the edge.

Two facts shape the first tag (Appendix A). STRKS, Starknet Sepolia, imports nothing today, so it
would lose the 16 Starknet rows; its spec is a near-copy of STRK's, and with `"imports": ["STRK"]`
its expanded apis are byte-identical to today's (checked with the router's own
`ExpandSpecWithDependencies`, `utils/keeper/spec.go:89`). And 36 chains would gain rows they do not
have today, because they import a family root that `chainFamilyMap` does not give them. Seven specs
reach both COSMOSSDK and ETH1; that is no conflict while ETH1 carries no rows, and a section's own
`imports` settles it if ETH1 ever does.

### 4.5 Examples

The examples move three of today's family tables as they are. Each `error` is the registered
name the code row points at today. Today's rows apply to reads and writes alike, so a row moved
as it is has `scope: both`; the `read` and `write` scopes below show the field, and narrowing a
row to one is a behaviour change (Agave's `sendTransaction` can answer `-32016`, and a Cosmos SDK
simulate, a read, can answer "account sequence mismatch"). The `class` letters, `source` links
and `versions` show the shape only; MAG-4898 supplies the researched values, and the router's
class table checks them.

**Solana** (`solana.json`, no imports; 14 rows today at `error_classifier.go:25-45`, four shown):

```json
"errors": {
  "schema_version": 1,
  "rows": [
    { "id": "solana.block_cleaned_up", "match": { "code": -32001 },
      "error": "CHAIN_STATE_PRUNED", "class": "C6", "scope": "read",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs",
      "versions": ["agave 2.1"] },
    { "id": "solana.node_unhealthy", "match": { "code": -32005 },
      "error": "NODE_SOLANA_UNHEALTHY", "class": "C3", "scope": "both",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs",
      "versions": ["agave 2.1"] },
    { "id": "solana.min_context_slot", "match": { "code": -32016 },
      "error": "CHAIN_SOLANA_MIN_CONTEXT_SLOT_NOT_REACHED", "class": "C6", "scope": "read",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs",
      "versions": ["agave 2.1"] },
    { "id": "solana.blockhash_not_found", "match": { "contains": "blockhash not found" },
      "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "class": "C6", "scope": "write",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/sdk/src/transaction/error.rs",
      "versions": ["agave 2.1"],
      "note": "a recent blockhash that rolled off the 150-slot window; common under load" }
  ]
}
```

**Bitcoin** (`btc.json`, no imports; 10 rows today at `:46-60`, three shown). The code rows run
before the phrase rows, so Bitcoin Core's mempool conflict, sent under `-26`, is
`CHAIN_BITCOIN_VERIFY_REJECTED` today, not `CHAIN_DOUBLE_SPEND`. Whichever verdict a row gives, 7.3
decides "slot already used" from the answer itself.

```json
"errors": {
  "schema_version": 1,
  "rows": [
    { "id": "bitcoin.in_warmup", "match": { "code": -28 },
      "error": "NODE_BITCOIN_WARMUP", "class": "C3", "scope": "both",
      "source": "https://github.com/bitcoin/bitcoin/blob/v28.0/src/rpc/protocol.h",
      "versions": ["bitcoind 28"] },
    { "id": "bitcoin.verify_rejected", "match": { "code": -26 },
      "error": "CHAIN_BITCOIN_VERIFY_REJECTED", "class": "C6", "scope": "write",
      "source": "https://github.com/bitcoin/bitcoin/blob/v28.0/src/rpc/protocol.h",
      "versions": ["bitcoind 28"] },
    { "id": "bitcoin.mempool_conflict", "match": { "contains": "txn-mempool-conflict" },
      "error": "CHAIN_DOUBLE_SPEND", "class": "C6", "scope": "write",
      "source": "https://github.com/bitcoin/bitcoin/blob/v28.0/src/validation.cpp",
      "versions": ["bitcoind 28"] }
  ]
}
```

**Cosmos SDK** (`cosmossdk.json`, imports IBC and TENDERMINT; 4 rows today at `:61-74`).
COSMOSHUB, OSMOSIS and the other Cosmos chains inherit these through `imports` and add their own.

```json
"errors": {
  "schema_version": 1,
  "rows": [
    { "id": "cosmossdk.sequence_mismatch", "match": { "contains": "account sequence mismatch" },
      "error": "CHAIN_INVALID_SEQUENCE", "class": "C6", "scope": "write",
      "source": "https://github.com/cosmos/cosmos-sdk/blob/v0.50.10/x/auth/ante/sigverify.go",
      "versions": ["cosmos-sdk 0.50"] },
    { "id": "cosmossdk.insufficient_fee", "match": { "contains": "insufficient fee" },
      "error": "CHAIN_INSUFFICIENT_FEE", "class": "C6", "scope": "write",
      "source": "https://github.com/cosmos/cosmos-sdk/blob/v0.50.10/types/errors/errors.go",
      "versions": ["cosmos-sdk 0.50"],
      "note": "also matches the plural \"insufficient fees\"" },
    { "id": "cosmossdk.account_not_found", "match": { "contains": "account not found" },
      "error": "CHAIN_ACCOUNT_NOT_FOUND", "class": "C6", "scope": "both",
      "source": "https://github.com/cosmos/cosmos-sdk/blob/v0.50.10/x/auth/keeper/grpc_query.go",
      "versions": ["cosmos-sdk 0.50"],
      "note": "kept per family on purpose: EVM phrasing differs and would false-match calls against unfunded addresses" }
  ]
}
```

**XRP** (`ripple.json`, no imports; 15 rows today at `:108-166`). Twelve move as `regex` rows,
because `xrplMessageStartsWith` is an anchored regex; three stay in router code (7.6). One shown:

```json
{ "id": "xrp.past_seq", "match": { "regex": "^(?:HTTP \\d+: )?tefPAST_SEQ\\b" },
  "error": "CHAIN_NONCE_TOO_LOW", "class": "C6", "scope": "write",
  "source": "https://github.com/XRPLF/rippled/blob/2.3.0/include/xrpl/protocol/TER.h",
  "versions": ["rippled 2.3"] }
```

**Disabling and section imports.** STRKS takes the Starknet rows with one spec-level import, and a
chain that must not inherit a parent's row disables it:

```json
{ "index": "STRKS", "imports": ["STRK"], "api_collections": [ "..." ] }
```

```json
"errors": { "schema_version": 1, "rows": [ { "id": "cosmossdk.account_not_found", "disabled": true,
  "note": "this chain answers unfunded accounts with an empty balance, not an error" } ] }
```

### 4.6 What the format cannot say

There is no field for a retry, a retry limit, a status code, a gRPC code, a connection error, a
timeout, a hold-off, a score, a write outcome or a new verdict. A row that needs one of those is
a router change, ticketed as such.

## 5. Must-have 1: spec versioning

- **Tags.** lava-specs gets annotated tags `vS.B.A` (schema.behaviour.additive), starting at
  `v1.0.0`, cut from `main` once the Step 0 checks are in place (section 9).
  - **schema** changes when an older router cannot read the new format. It is the
    `schema_version` the sections carry.
  - **behaviour** changes when a row changes how a chain's errors are handled: a new or changed
    `error`, a `match` that widens or narrows a verdict, a removed or disabled row, a changed
    `scope`, a new `slot_used`, or a chain that gains or loses inherited rows.
  - **additive** changes when a new row or chain changes nothing that exists, and for every
    change outside the errors section, such as a new api, a verification or a parser fix.
- **Change log.** `CHANGELOG.md` in lava-specs gets one entry per tag, written by the PR that
  bumps it. The entry has a line per chain: what changed, old verdict, new verdict, why, and the
  PR. CI fails a tag whose entry is missing, or whose chains do not match the diff.
- **Routers load a pinned tag or commit.** The fetcher resolves the ref to a commit through the
  host's API before fetching, so the tarball and the recorded commit are the same object.
- **A source that is not pinned, or not usable, still boots.** The router never refuses to start
  over its spec source, and never hides what it fell back to:
  - an unpinned remote source (no ref, or a branch): the router boots on its bundled tag (6) and
    logs a warning naming the source it did not load and why. With `--allow-unpinned-spec`, for
    development, it loads the source as asked and reports `pinned="false"`;
  - a broken source (the host does not answer, the ref does not exist, the tarball is not one, a
    spec does not parse): the router boots on its bundled tag and reports the reason;
  - a stale source (a pinned tag older than the bundled one): the router loads it as asked, since
    that is a deliberate rollback, and reports `stale="true"`;
  - a local folder: loaded as is, `pinned="local"`.
- **Logged, exported, shown.** The boot log and the health endpoint name the requested source, the
  loaded source, the ref, the resolved commit and a content hash (SHA-256 over the loaded files,
  sorted by name). One gauge,
  `smartrouter_spec_source_info{requested, loaded, ref, commit, content_hash, pinned, stale, fallback_reason}`
  = 1, sits next to the existing `smartrouter_protocol_version`. The dashboard shows the pair
  (router version, spec tag) per router.

## 6. Must-have 2: aligned with the router version

- **Compatibility file.** `compatibility.json` at the lava-specs root:

  ```json
  { "schema_versions": { "1": { "min_router": "v1.8.0" } } }
  ```

  It has one line per schema version: the first router release that reads it. The router does not
  need the file to stay safe, because it drops a section whose schema it does not know (7.4). CI
  and the release process read it: it says which router releases a tag must boot on (8.2), and
  which router a customer needs before a tag's new rows act.
- **The release unit is the pair.** Each router release names one lava-specs tag in
  `specs/SPEC_TAG` and ships that tag's files inside the release image. The repo's `specs/` folder
  stays for local runs and tests, but `make specs` fills it from `SPEC_TAG`, and a CI check fails
  when the folder differs from the tag by a byte. The router's CI runs its own validator over every
  section in the bundled tag, so the bundled sections are valid for the router that carries them.
  `grpctest.json`, which exists only in the router, moves to a test fixture directory. Release
  notes name the pair, and that pair is what an on-prem change board approves.
- **Default source.** With no `--use-static-spec`, the router loads the bundled tag; today it
  refuses to start (`rpcsmartrouter.go:2569-2581`). `--use-static-spec` overrides it with a pinned
  tag, a commit or a local folder. The Helm chart's default becomes the bundled tag, and a tenant
  that wants a newer tag pins it on purpose in `cluster.yaml`.
- **The fallback is the bundled tag, before and after the switch.** Whenever a chain's section
  from the loaded source cannot be used (an unknown schema, a broken rule, a source the router did
  not load), the router uses that chain's section from its bundled tag. Before the switch the
  bundled section and the code family table agree (Step 1 proves it), so nothing changes when the
  router falls back. After the switch deletes the family tables, the bundled section is the only
  built-in classification a chain has, and it stays tested: 8.3 runs the fallback after the switch.
  If the bundled tag has no section for the chain, the chain gets the transport rows only, as the
  204 unmapped chains do today, and the reason says so.
- **Newer schema, older router.** A router that meets a `schema_version` it does not read uses the
  chain's bundled section. It logs one warning at boot naming the chain, the schema and the router
  version it would need, and counts
  `smartrouter_spec_errors_section_dropped_total{chain_id, reason="schema_unknown"}`.

## 7. Must-have 3: write protection

### 7.1 What is locked by construction

Rows name verdicts, and verdicts and their consequences are code, so the format alone keeps a
spec away from retries, limits, status rules and write outcomes (4.6). What the code adds:

- Whatever verdict a row names, the stateful policy stops a write after its one fan-out, unless
  every failure was a rate limit (2.2).
- The unclear-write decision reads protocol errors classified with no chain family and treats
  every node error as a reply, and REST's 5xx node-reply rule runs with no chain family, so no row
  can make an unclear write look settled (2.2, 4.3).

Rows do reach more than node replies: a direct-RPC transport or status error gets its retry and
health verdicts with the chain family (2.2). So the locks below are run-time rules in the router,
not only checks in CI. Five doors remain, and 7.2 to 7.6 close them: a row that names a rate-limit
verdict, which opens the one exception under which a write is sent again; a row that hides a rate
limit, a status or one of the router's own verdicts; a slot-already-used answer reported as
anything but unknown; a spec that drops `stateful` from a write or quotes a vendor; and the XRP rows
that name locked verdicts.

### 7.2 Verdicts and matches a row may not use

The validator (section 8) reads these from the registry and the router's tables, never from a
copied list. A row fails when its `error` is:

- any verdict with the rate-limit sub-category: today `NODE_RATE_LIMITED`, `NODE_LIMIT_EXCEEDED`
  and `PROTOCOL_RATE_LIMITED`. `onlyRateLimited` counts such a node error as a rate limit, and a
  write whose every failure was a rate limit is sent again (2.2), so a row naming one could make
  the router re-send a write a node executed, and hold off the providers that answered;
- any `PROTOCOL_*` verdict (38 of 134): the router's own, never a node's reply;
- `CHAIN_TX_ALREADY_KNOWN`: the read-your-writes logic turns on it (`read_your_writes.go:218-231`),
  so its rows stay in code: JSON-RPC's "already known" rows (`error_classifier.go:310, 311, 316`),
  REST 409 (`:449`) and XRP's `tefALREADY` (`:117`, 7.6);
- `UNKNOWN_ERROR`: a row that says "unknown" should not exist.

A row also fails when its match could replace a status rule or hide a rate limit, on any interface
it applies to. A row without `interface` applies to every interface its chain serves after
inheritance, and the validator checks it against each:

- **`code` alone may not be a status.** An HTTP status (100 to 599) on any interface: REST and
  GraphQL classify with a status as the code, and JSON-RPC does too whenever the body carries no
  JSON-RPC code (2.1). And any `code` on `grpc`, where the code is the gRPC status and
  the router decides every one of them: six have rows, and the comment at `error_classifier.go:518-538`
  records why the others have none.
- **`code` with `contains` may name a status**, so a row can read a node's own text inside one, as
  the router's own `CodeAndMessage(500, …)` rows do (`:413-416`), except a locked status of 4.3
  step 2, which no row may name at all.
- **`code: 429`** fails in any form.
- **A `contains` or `regex` fails when it matches** a rate-limit phrase (the `NODE_RATE_LIMITED`
  rows' text and `GRPCRateLimitTexts`), or the text a locked status reaches the classifier as: the
  `HTTP <status>: ` prefix and the bare status number that `HTTPStatusContains` reads. The validator
  runs each such text through the row's matcher.

At run time, step 2 of 4.3 keeps explicit rate limits, the locked statuses and XRP's already-known
answer ahead of every spec row, whatever a row's text says. The checks above are the early warning;
4.3 is the guarantee. The locked statuses are the ones whose verdict carries write safety or
endpoint health: 401 and 403 (credentials), 409 (`CHAIN_TX_ALREADY_KNOWN` on a Horizon write),
429, the gateway and edge statuses 502 to 504, 520 to 527 and 530, and gRPC DeadlineExceeded,
PermissionDenied, ResourceExhausted, Unavailable and Unauthenticated. Statuses whose body is the
node's own answer, such as 400, 404 and 500, stay readable by chain rows (MAG-4061 needs TON's
answers inside a 500).

### 7.3 Slot already used

**Slot already used is always unknown** (rulebook Q45, changed 4 Nov). This is a rule to build;
today each such answer keeps its own verdict and the node's body is forwarded (2.2).

- **The router decides it from the answer itself**, with a signature table in its own code that
  reads the chain family, the code and the message, before and apart from row matching. Which row
  matched, from a spec or from code, does not matter, so no row can move an answer out of the group.
  The group cannot be a list of verdicts: Bitcoin's code rows run before its phrase rows, so a
  mempool conflict is `CHAIN_BITCOIN_VERIFY_REJECTED` and a spent input `CHAIN_BITCOIN_VERIFY_ERROR`,
  and neither verdict says "slot used" on its own.
- **When a write ends with no attempt accepted and any answer matches a signature, the router
  reports the write as unknown.** R&D chooses the wire form; today's unclear-write reply is an
  `*unclearWriteError` (`rpcsmartrouter_server.go:1421-1445`), and this adds a fifth reason.
- **Proposed signatures**; MAG-4898 completes them per node software:

  | Family | Code | Message | Today's verdict |
  |---|---|---|---|
  | any | any | contains "nonce too low" | `CHAIN_NONCE_TOO_LOW` |
  | any | any | contains "account sequence mismatch" | `CHAIN_INVALID_SEQUENCE` |
  | XRP | any | starts with `tefPAST_SEQ` | `CHAIN_NONCE_TOO_LOW` |
  | Bitcoin | -26 | contains "txn-mempool-conflict" | `CHAIN_BITCOIN_VERIFY_REJECTED` |
  | Bitcoin | -25 | contains "missingorspent" or "missing inputs" | `CHAIN_BITCOIN_VERIFY_ERROR` |
  | Bitcoin | any | contains "already spent" or "double spend" | `CHAIN_DOUBLE_SPEND` |
  | Bitcoin | -27 | any | `CHAIN_BITCOIN_ALREADY_IN_CHAIN` |
  | Starknet | 52 | any | `CHAIN_STARKNET_INVALID_NONCE` |
  | Starknet | 59 | any | `CHAIN_STARKNET_DUPLICATE_TX` |

  Checked at `6c237073`: these catch the fixtures `geth_nonce_too_low`, `geth_nonce_too_low_verbose`,
  `nethermind_nonce_too_low`, `bitcoin_verify_error`, `bitcoin_already_in_chain`,
  `starknet_invalid_nonce`, `starknet_duplicate_tx` and `xrpl_tef_past_seq`, and none of the
  already-known answers (EVM "already known", XRP `tefALREADY`).
- Bitcoin `-27` and Starknet `59` say "this same transaction is already there", not "the slot is
  taken". They are in the group because revision 1 put them there; with MAG-4059's ranking R&D may
  report them as accepted instead (section 11).
- **A spec may widen the group, never narrow it.** A `write` or `both` row may carry
  `"slot_used": true`, and the router then treats an answer that row matches as a signature.
- **Write rows stay in code for 30 days.** Until the side-by-side run (section 9, step 1) shows
  zero mismatches on write-path errors for 30 days, the router evaluates spec `write` rows, counts
  them, and acts on its code rows. Reads switch after 7 days.

### 7.4 A section that breaks a rule

The section is dropped **for that chain only**. The router uses the chain's section from its
bundled tag (6), logs one error at boot naming the chain, the row id and the rule, counts
`smartrouter_spec_errors_section_dropped_total{chain_id, reason}`, and boots. A bad row in one spec
never takes a router down and never degrades another chain. lava-specs CI runs the same validator
first, so this path is for a router meeting a tag it was not tested with, or a hand-edited local
folder.

After the switch this fallback is what keeps a chain's classification. Without it, a dropped Solana
section would send "Node is unhealthy" to the generic `-32005` row, `NODE_LIMIT_EXCEEDED`, which is
non-retryable and carries the rate-limit sub-category: a read would stop at the first unhealthy node
instead of failing over, and that node would be held off instead of marked unhealthy. 8.3 tests
exactly this case after the switch.

### 7.5 Locks the router enforces on every spec

Two of the ticket's locks are about what a spec says, not what a row says. The router enforces them
on every spec it loads, from any source, so a local folder or a custom source cannot step around
them.

- **A write method stays stateful.** The router keeps a table of write methods in its own code,
  keyed by api interface and name (and family, for a generic name such as XRP's `submit`). It is
  seeded from `check_stateful.sh`'s list plus the writes that list misses (2.4), and MAG-4898
  completes it. After `DoExpandSpec` (called from `utils/keeper/spec.go:79`) and before the chain
  parser receives the spec, an api in the table that the spec does not mark `stateful` is loaded as
  stateful. The router logs one warning per chain naming the apis, and counts
  `smartrouter_spec_lock_enforced_total{chain_id, rule="stateful"}`. `cmd/specvalidate` exports the
  same table, so lava-specs CI fails such a PR before any router sees it, and `check_stateful.sh`
  reads the table instead of its own copy, which also ends its gRPC and Tendermint blind spot.
  Today this flips nothing: every one of the 132 writes on lava-specs `main` is stateful.
- **No vendor wording.** A row's `source` must be on the router's allowlist of node-software and
  protocol-spec origins (the repositories of the node implementations the six families run, and the
  protocol specs; MAG-4898 completes it), with the ref pinned to a tag or a commit. The router
  checks it at boot, and a row that fails drops its chain's section (7.4). CI also fetches the
  cited file at the cited ref and checks that the row's text, the `contains` string or the longest
  literal in a `regex`, appears in it, so a vendor's phrase cannot ride on a node-software link.

The router's own vendor rows are transport rows and stay in code whatever this PRD decides;
section 11 asks whether R-0.2 removes them.

### 7.6 Router-owned rows

Three XRP rows stay in router code after the switch, because they name a locked verdict or depend
on the rows around them:

- `tooBusy`, `slowDown` → `NODE_RATE_LIMITED` (`error_classifier.go:127`): a rate-limit verdict.
  Evaluated in 4.3 step 2, with the other explicit rate limits.
- `tefALREADY` → `CHAIN_TX_ALREADY_KNOWN` (`:117`): read-your-writes turns on this verdict.
  Evaluated in 4.3 step 2, ahead of the spec rows, so no spec row can match its text first.
- the engine-result catch-all `te[fmlr]…` → `CHAIN_TX_REJECTED` (`:119`, with `xrplRejectionCode`
  at `:171`): it classifies every rejection-shaped result the XRPL reader flags
  (`IsXRPLRejection`), which is interface logic, and it must follow XRP's specific engine-result
  rows (`tefPAST_SEQ`, `terPRE_SEQ`), which move to the spec. Evaluated in 4.3 step 4, after the spec
  rows.

The other twelve XRP rows move to `ripple.json` as `regex` rows (4.5). Moving these three earlier or
later in the order changes no verdict: their tokens match no other XRP or transport row.

## 8. Must-have 4: tests that gate every spec change

No tag is cut, and no tag reaches a customer, until all of these pass.

### 8.1 lava-specs CI, on every pull request

- **Schema check.** A JSON Schema for the `errors` section, kept in lava-specs, applied to every
  changed spec.
- **Locked-rule check.** One Go validator in smart-router, `cmd/specvalidate`, is the only
  implementation of 4.2, 7.2, 7.3 and 7.5. lava-specs CI builds it from each router release the
  boot matrix covers (8.2), and each router runs the same code at boot, so CI and boot cannot
  disagree for any release in service.
- **Stateful check.** The validator's write-method table (7.5), on every PR, as a job in
  `spec_guards.yml`. Today `check_stateful.sh` runs only inside the agent, once when a PR opens
  (2.4).
- **Write-safety job** in `spec_guards.yml`: the validator's write rules plus a diff, keyed by row
  `id` rather than by position, that flags any added, removed or changed `write` or `both` row,
  and any `slot_used`, and labels the PR `write-rows`.
- **Source check** (7.5): every row's `source` on the allowlist, pinned, and its text found in the
  cited file.
- **Owner review for write rows.** `CODEOWNERS` matches paths and cannot name a JSON section, so
  the gate is a required check: a PR labelled `write-rows` needs an approving review from the
  owners team that a new `CODEOWNERS` file names. The same file makes the spec owners review every
  root spec.
- **Change-log check** (section 5), and `check_unused_fields.sh`, which keeps `description` and
  `title` out of rows, moved into `spec_guards.yml` so it runs on every PR (2.4).

### 8.2 Against the router

- **Boot matrix.** Every chain whose spec changed boots, with the PR's specs as the source, on every
  router release in service from the schema's `min_router` up. Each release runs its own validator
  at load. A section dropped at boot (7.4) fails the job unless the PR says so.
- **Replay.** The 102 cases in `error_fixtures.json`, and the router's own classifier and status
  suites (`TestRESTStatusRows`, `TestRESTStatusRowsDoNotReachOtherTransports`,
  `TestGRPCRateLimitTexts_TheCodeRowsAboveStillDecide`, `TestEndpointFault_AnswerShapes`,
  `TestGRPCStatusError`), run against the spec rows and must give the same registered error, unless
  the change-log entry names the case and its new verdict. The fixtures grow with MAG-4898: one
  case per row, citing the row's `source`.
- **Counterexamples.** Two fixed sets of answers the fixtures miss, because the fixtures are almost
  all JSON-RPC. Every answer in the first set must keep today's verdict: a status or rate-limit token
  inside an unrelated answer (Bitcoin `-26` "min relay fee not met, 429 < 1000" and "mempool min fee
  not met, 429 < 1000", Agave `-32005` "Node is behind by 429 slots"); a rate-limit phrase inside
  each of the five gRPC statuses the gRPC table checks first; JSON-RPC code 429; XRP's router-owned
  rows and its other engine results, with and without an `HTTP <status>: ` prefix; and each
  slot-already-used signature as a complete code-and-message pair. Every answer in the second set
  must take the locked status's verdict although a chain row matches its text: a REST 403 and 409, a
  gRPC Unavailable and ResourceExhausted, and a JSON-RPC reply that arrived as `HTTP 503: `. A
  relay-policy test adds that a stateful write whose every answer is the min-fee rejection stops
  after its fan-out and holds no endpoint off.
- **Slot-already-used pairs.** Every signature of 7.3, Bitcoin's `-25` and `-26` included, sent as a
  complete code-and-message pair through the stateful path, must end unknown; the same pair on a
  read keeps its verdict.
- **Coverage check.** For every chain, the rows it gets after inheritance are compared with the
  previous tag's. A chain that loses rows fails the job, and a chain that gains rows must be named in
  the change-log entry as a behaviour change.
- **Generated row tests.** For every changed row, CI builds a node error from the row's `match`
  (the code, or a message containing the text) and asserts the row's `error` and `class` on the
  router version under test.

### 8.3 Bad-source tests (router repo)

One test per failure. Each asserts what the router loads, that the log names the cause, and that
the metric carries the reason:

- an unpinned remote source boots on the bundled tag with `fallback_reason="unpinned"`; with
  `--allow-unpinned-spec` it loads the source and reports `pinned="false"`;
- a source that is not a tarball, a tag that does not exist, a host that does not answer, and a spec
  that does not parse each boot on the bundled tag, with their own reason;
- a pinned tag older than the bundled one loads, with `stale="true"`;
- a row that breaks each rule in 7.2, 7.3 and 7.5, and a section with an unknown schema, drop that
  chain's section, and the chain classifies with its bundled section;
- **after the switch**, with the family tables deleted: Solana's section dropped from the loaded
  source still classifies `-32005` "Node is unhealthy" as `NODE_SOLANA_UNHEALTHY`, from the bundled
  section;
- a spec that marks `eth_sendRawTransaction` with `stateful` 0 is loaded stateful, and
  `smartrouter_spec_lock_enforced_total` counts it.

### 8.4 Rollout gate

Managed deployments take a new tag on one canary router per cluster first, then the rest after a
metrics window. The proposal is one full day of production traffic, so each chain's daily cycle
is covered. The window compares `smartrouter_errors_total` by `error_name` and `chain_id` before
and after (`protocol/metrics/error_metrics.go:42-44`), and requires the dropped-section and
lock-enforced counters at zero. On-prem gets the tested pair only. Tags are immutable, so a tag that
fails the canary is not withdrawn; the next tag reverts the row, with a change-log line that says
so.

## 9. Rollout

**Step 0, before the router reads a single row.** This is router and CI work. It moves no fixture's
verdict, and it lets a locked status decide its own reply (4.3). It changes three more things a
client or operator can see: an unpinned spec source boots on the bundled tag (section 5), a write
method a spec leaves non-stateful is loaded stateful (7.5, none today), and a failed write whose
answers include a slot-already-used signature is reported unclear instead of carrying the node's
rejection (7.3).

- pinning, the bundled-tag fallback, `--allow-unpinned-spec`, and the source fields in the log and
  the metric (sections 5 and 6);
- `compatibility.json`, `specs/SPEC_TAG`, `make specs`, the folder-matches-tag check, and the
  bundled tag in the release image (section 6);
- `cmd/specvalidate`, the schema, the `spec_guards.yml` jobs, `CODEOWNERS`, the change-log check
  and the source check (8.1);
- the locked prefix of 4.3 step 2, and the router-owned XRP rows (7.6);
- the write-method table and the stateful lock (7.5);
- the slot-already-used signatures (7.3);
- the AVAX* keys in `chainFamilyMap` renamed (section 11);
- the Helm chart's default moved from the bare URL to a tag;
- lava-specs `v1.0.0`.

**Step 1, side by side.** The six family tables, less XRP's three router-owned rows, are copied
into their root specs with no verdict change, STRKS imports STRK, and each of the 36 chains that
would gain rows gets `"errors": {"schema_version": 1, "imports": []}` (Appendix A), so every chain
keeps exactly today's rows. That tag is additive, because no router acts on it yet. The router
reads the sections and evaluates spec rows next to its code rows, acting on the code rows.
`smartrouter_spec_error_rows_mismatch_total{chain_id, kind, row_id, code_error, spec_error, scope}`
counts three kinds of disagreement: `differ` (both matched, different verdicts), `code_only` (a code
family row matched and no spec row did: a chain that lost coverage) and `spec_only` (a spec row
matched and no code family row did: a chain that gained it).
`smartrouter_spec_error_rows_hits_total{chain_id, row_id}` counts every spec-row hit. The expected
count of every kind is zero, and any mismatch is a bug in the move.

**Step 2, switch.** A router setting with three values decides which rows act. The order in each
window:

| Setting | Read apis | Write apis |
|---|---|---|
| `shadow` (Step 1) | prefix, family table, transport; spec rows counted only | same as reads |
| `reads` (after 7 days with zero read mismatches) | prefix, spec rows, family table, transport | prefix, family table, transport; spec rows counted only |
| `all` (after 30 days with zero write mismatches) | prefix, spec rows, family table, transport | prefix, spec rows, family table, transport |
| tables deleted (next release) | prefix, spec rows, router-owned rows, transport | same as reads |

"Prefix" is 4.3 step 2, active from Step 0. Flipping the setting needs no release. The six family
tables are deleted, except the router-owned rows, in the release after `all` has run through one
canary window; from that release on, the fallback is the bundled tag's sections (6, 7.4), and 8.3
runs it in the router's CI.

**Step 3, per-chain rows open up.** MAG-4898 lands research rows per chain through the gates in
section 8, and the 36 gains of Appendix A open one by one, by removing a chain's `"imports": []`.
Each such change is a behaviour bump with a change-log line and a product call on its PR.

## 10. Observability

| Signal | Where | Why |
|---|---|---|
| requested and loaded source, ref, resolved commit, content hash, pinned, stale, fallback reason | boot log, `/health`, `smartrouter_spec_source_info` | "which specs is this router running, and why" has one answer |
| `smartrouter_spec_errors_section_dropped_total{chain_id, reason}` | router | a bad or unreadable section is visible, not silent |
| `smartrouter_spec_lock_enforced_total{chain_id, rule}` | router | a spec that tried to drop `stateful` from a write is visible |
| `smartrouter_spec_error_rows_mismatch_total{chain_id, kind, row_id, code_error, spec_error, scope}` | router, steps 1 and 2 | the switch criterion |
| `smartrouter_spec_error_rows_hits_total{chain_id, row_id}` | router | which rows matter; feeds the geth-wording decision in section 11 |
| router version and spec tag per router | dashboard | the release pair, per fleet |

## 11. Open for R&D

- **The generic geth-wording rows.** "execution reverted", "nonce too low", "already known", the
  hex-input phrases and the tx-pool texts (`error_classifier.go:269-346`, and Erigon's "pending
  state is not supported" at `:241`) apply to every JSON-RPC chain today. Either keep them generic,
  or move them to an ETH1 section that BASE, ARBITRUM and the other EVM chains inherit. Decide after
  30 days of the hits metric show where they fire outside EVM. Narrowing them is a behaviour bump.
- **The router's vendor rows.** Alchemy's `-32600 "unsupported method"` and the QuickNode
  `blocked … method|rpc` row are vendor wording, which R-0.2 rules out. Keep them as router-owned
  transport rows, or remove them. The "pending state is not supported" row is Erigon's wording; its
  comment, commit and fixture should say so.
- **Bitcoin `-27` and Starknet `59`.** Report "this transaction is already there" as unknown, as
  7.3 proposes, or as accepted, in line with MAG-4059's ranking.
- **The write-method table and the source allowlist.** Section 7.5 seeds both; MAG-4898 completes
  them, family by family.
- **Does the create-spec agent keep an unknown key when it rewrites a spec?** In the update,
  add-testnet and add-collection modes it does, by construction (2.4). New-chain mode needs a
  template and a reference page (MAG-4898 part 3). Confirm with a test in the skill's
  `scripts/test_*.sh`.
- **`chainFamilyMap` after the move.** It stays: the transport rows and the fault axis key on the
  family, and `IsSolanaFamily` (`protocol/common/svm.go:10-13`) drives slot handling from it. The
  AVAXC and AVAXCT keys become AVALANCHEC and AVALANCHECT (EVM); AVALANCHEP and AVALANCHEPT serve
  `platform.*` only and belong to no family. The five disabled Cosmos bases the map files under
  Cosmos SDK (Appendix A) can leave it. A lava-specs CI check fails a new spec whose index the map
  does not know.
- **Effort.** The ticket's first estimate is 6 to 8 engineer-weeks. Revision 2 adds the bundled-tag
  fallback, the stateful and source locks and the slot-already-used rule to Step 0. A first split,
  for R&D to resize: Step 0 about 4 to 5 weeks, the reader and inheritance about 2, side-by-side
  and metrics about 1, and the switch with its post-switch tests about 1.5. That is about 9 weeks,
  above the ticket's range.

The on-prem source question of revision 1 is answered by section 6: the bundled tag, always.

## 12. Later

Customer overrides, as a small overlay file in the router config: read rows only, within the
locked rules of section 7, checked by the same validator, and logged and exported like a spec
source. Never a fork of lava-specs.

## 13. Out of scope

- How the router reads the section: parsing, caching and the matcher. R&D designs it against
  sections 4 and 7.
- The per-chain research and the agent change (MAG-4898).
- Ranking several node errors on a failed write (MAG-4059) and remembering accepted hashes
  (MAG-4748). Both are router behaviour, so they stay in code whatever this PRD decides.

## 14. Done when

- Nimrod or Tom approves the four must-haves, the format and the split.
- MAG-4898 is unblocked with section 4 and Appendix A linked as the approved format and the first
  tag's coverage.
- The router work is ticketed: Step 0, Step 1 (reader, inheritance, side-by-side metrics),
  Step 2 (the setting, the switch and the deletion), and the open items of section 11.

## Appendix A. Inheritance coverage at lava-specs `bda05974`

Rows placed on each family root reach every spec that imports it, directly or through another
spec. Compared with `chainFamilyMap` (`error_registry.go:280-368`):

| Family | Root | Map keys | Specs that reach the root | Lose rows | Gain rows |
|---|---|---|---|---|---|
| Solana | SOLANA | 4 | 4 | 0 | 1 |
| Bitcoin | BTC | 8 | 17 | 0 | 10 |
| Cosmos SDK | COSMOSSDK | 28 | 47 | 5 | 25 |
| Starknet | STRK | 2 | 0 | 1 | 0 |
| NEAR | NEAR | 2 | 1 | 0 | 0 |
| XRP | XRP | 2 | 1 | 0 | 0 |

**Losses (6).** STRKS, which carries traffic; Step 1 adds `"imports": ["STRK"]`. And
COSMOSSDK45DEP, COSMOSWASM, ETHERMINT, IBC and TENDERMINT, which import nothing and are
`enabled: false`, so they serve no api and lose nothing at run time.

**Gains (36), opted out in Step 1 and opened in Step 3.**

- Solana: SOLANAD.
- Bitcoin: BTCS, BTCT4, DASH, DASHT, PEP, PEPT, QTUM, QTUMT, ZCASH, ZCASHT.
- Cosmos SDK, not in the map: AKASH, AKASHT, BABYLON, BABYLONT, DYDX, DYDXT, ELYS, FETCHAI,
  FETCHAIT, INJECTIVE, INJECTIVET, KAVA, KAVAT, NEUTRON, NEUTRONT, SEI, SEIT, SIDET, STRIDE, STRIDET,
  TSC, TSCT.
- Cosmos SDK, filed under EVM in the map: AGR, AGRT, CANTO.

**Map keys that are not lava-specs indexes.** AVAXC, AVAXCT, AVAXP and AVAXPT (section 11).

SOLANAD's gain covers the rows only: slot handling follows `chainFamilyMap`, not the rows, so it is
not Solana-family for slots either way.

## 15. Changes in revision 2

| Finding | Change | Where |
|---|---|---|
| 661-1, XRP rows break the locked rules | Three XRP rows stay router-owned and are placed in the order explicitly; the other twelve move as `regex` rows | 4.3, 4.5, 7.6 |
| 661-2, inheritance changes coverage | STRKS imports STRK, checked to leave its apis unchanged; the 36 gains are opted out in Step 1 and opened as behaviour bumps in Step 3; the mismatch metric counts `code_only` and `spec_only`; a coverage check gates every tag | 4.1, 4.4, 8.2, 9, Appendix A |
| 661-3, slot-already-used misses Bitcoin | The router decides the group from the full answer, whatever row matched, with Bitcoin `-25` and `-26` signatures; tests send complete pairs | 7.3, 8.2 |
| 661-4, status lock covers 19 HTTP rows only | `code` is checked per interface, gRPC included; message matchers are checked against status text; the locked statuses run ahead of spec rows at run time | 4.3, 7.2 |
| 661-5, stateful and vendor locks only in CI or review; unpinned source refuses to boot | The router enforces the write-method table and the source allowlist on every spec; an unpinned or broken source boots on the bundled tag and reports why | 5, 7.5, 8.3 |
| 661-6, the rate-limit prefix moves verdicts | The prefix holds only explicit rate-limit signals, keeps today's gRPC order, and leaves the digit-matching 429 row in place; counterexamples join the gate | 4.3, 8.2 |
| 661-7, factual errors | Fixed in `ad519324`; every "today" fact re-read at the new baseline | 2 |
| 661-8, format and rollout gaps | `disabled` and `slot_used` defined; one schema per tag; the order in each Step 2 window; the QuickNode row counted | 4.1, 4.2, 4.4, 9, 2.1 |
| Second review: gRPC precedence | The five gRPC statuses checked before the rate-limit phrases keep that precedence | 4.3 |
| Second review: message matchers | The status lock covers message matchers and is enforced at run time | 7.2, 4.3 |
| Second review: fallback contradicts the cutover | The fallback is the bundled tag's section before and after the switch, and 8.3 tests it after the switch | 6, 7.4, 8.3, 9 |
| Facts moved since revision 1 | New baseline `6c237073` / `bda05974`: XRP has 15 rows, 102 fixtures, 306 indexes, `check_stateful.sh`'s gaps, the third vendor-named row | 2, 4.5 |

## References

- Smart Router error rulebook v1.0, R&D review (Notion `40cf5ce8bcd38108bcf3fd93f85e8af5`):
  R-M (rows keyed by chain, node software and interface; no vendor layer), R-L (writes), R-K
  (all-fail ranking), R-0.2 (no vendor wording), Q45 (slot already used) and the change log.
- `docs/ERROR-REGISTRY-DESIGN.md`: the registered verdicts the rows point at.
- MAG-4898 (the per-chain research this blocks), MAG-4059 (write error ranking), MAG-4748
  (accepted hashes), MAG-4824, MAG-4817 and MAG-5021 (classification changes that needed a code
  release), MAG-4061 (chain answers inside a REST 500).
- lava-specs: `.github/workflows/spec_guards.yml`, `spec_pipeline.yml`, `create_spec.yml`;
  `.claude/skills/create-spec/scripts/check_stateful.sh` (`:97-119`, `:188`),
  `check_unused_fields.sh`, `check_update_diff.sh`.
- smart-router-helm-chart `charts/smart-router/values.yaml:450-458` (chart `main` `2446606`): the
  unpinned default.
- erigontech/erigon `rpc/jsonrpc/eth_call.go:61`: `errPendingStateNotSupported`.
