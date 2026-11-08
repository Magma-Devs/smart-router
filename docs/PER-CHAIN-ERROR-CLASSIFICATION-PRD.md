# PRD: per-chain error classification in lava-specs

**Status:** draft for R&D approval (MAG-4910). Approvers: Nimrod or Tom, on the four must-haves,
the format and the split. Blocks MAG-4898 (the per-chain research), which starts once this is
approved.

**Baseline.** Everything marked "today" was read on 2026-11-08 at smart-router `origin/main`
`2d99d4b7` and lava-specs `origin/main` `8c1474c` (2026-11-03). File and line references are to
those commits.

**Where this departs from the ticket.** Four facts in MAG-4910 read differently in the code, and
the design below follows the code:

- The release image does not bundle `specs/`. Only the dev image and `docker-compose.yml` use the
  repo folder; every deployed router loads lava-specs `main` at boot (2.3).
- "Slot already used is always unknown" (Q45) is not today's behaviour. It is a rule to build,
  and section 7.3 says where.
- The rows the ticket calls vendor wording are mostly node-software wording (geth, Erigon,
  Nethermind), which R-M allows. One row is a vendor's (Alchemy's `-32600 "unsupported method"`).
  The geth rows' problem is that they apply to every JSON-RPC chain (2.1).
- A chain row can shadow a rate-limit row today, because chain rows run first. The locked rules
  in section 7 have to close that, not only the write path.

## 1. Goal

Each chain's spec in lava-specs carries that chain's error classification: which code or message
from that chain's nodes means which registered verdict. The router keeps everything that decides
what a verdict *does*: the interface rules (JSON-RPC, REST, gRPC, GraphQL), connection errors,
status rules, rate limits, the unknown default, retry and scoring, and write safety.

The design rests on one sentence: **a spec row may say what a node's answer means; it may never
say what the router does about it.** Rows point at verdicts the router already registers. They
cannot define a verdict, a retry, a status rule or a write outcome.

## 2. Today

### 2.1 Classification is code

- Rows are hard-coded in `protocol/common/error_classifier.go`. Each row is a matcher plus a
  pointer to a registered `*LavaError` (`protocol/common/error_registry.go:556-559`). The matcher
  kinds are `CodeEquals`, `MessageContains` (case-insensitive), `MessageRegex`, `CodeAndMessage`,
  `HTTPStatusContains` and `GRPCCodeEquals` (`error_registry.go:433-550`).
- `ClassifyError` decides a connection error before any row, then runs the chain-family rows,
  then the transport rows, then returns `UNKNOWN_ERROR`. First match wins
  (`error_classifier.go:691-723`). `UNKNOWN_ERROR` is retryable and may-have-reached-node, so an
  unmatched error fails open (`error_registry.go:589-596`).
- Because chain rows run first, **a chain row can shadow any transport row**, the rate-limit and
  status rows included. The six tables use that on purpose: Solana's `-32005` ("node is
  unhealthy") shadows the generic `-32005`, which is EVM's "limit exceeded".
- Chain-family rows exist for six families only: Solana (14 rows), Bitcoin (10), Cosmos SDK (4),
  Starknet (16), NEAR (4) and XRP (9, built in `error_xrpl.go`) (`error_classifier.go:23-104`).
  There is **no EVM family table**. Every geth, Erigon, Nethermind and Besu row sits in the
  generic JSON-RPC table, so it applies to every JSON-RPC chain, Solana and Starknet included
  (`error_classifier.go:177-188, 201-226, 247-254`). Aptos, Stellar, TON, Tron, Cardano and Sui
  are mapped families with no rows.
- `chainFamilyMap` (`error_registry.go:280-368`) maps 106 chain ids. Four of them (AVAXC, AVAXCT,
  AVAXP, AVAXPT) are not lava-specs names, which are AVALANCHEC, AVALANCHECT, AVALANCHEP and
  AVALANCHEPT. lava-specs declares 304 chain indexes, and 202 of them are not in the map, so they
  get the transport rows only. Examples left out: GNOSIS, LINEA, SCROLL, MONAD, KAVA, SEI,
  POLKADOT, AVALANCHEC.
- Transport rows: JSON-RPC 97, REST 68, gRPC 32 (`error_classifier.go:117-530`). One row matches
  a vendor's wording, which R-0.2 rules out: Alchemy's `-32600 "unsupported method"`
  (`:124-128`, commit `2953bc92`).
- Test fixtures: `protocol/common/testdata/error_fixtures.json` holds 101 cases (EVM 42, Solana
  18, Starknet 14, Bitcoin 9, XRP 9, NEAR 4, Cosmos SDK 2, Aptos 2, Stellar 1), replayed by
  `TestErrorFixtures`. `TestFaultAxis_EveryRegisteredCodeIsAccountedFor` fails when a registered
  verdict has no fault decision.
- A new chain error therefore needs a code release. MAG-4824 is an example.

### 2.2 How a write is protected today

- The spec's `category.stateful` (`types/spec/api_collection.go:105`) sends a write to every
  provider at once.
- `Policy.Decide` stops a stateful relay after that fan-out, whatever the verdicts, with one
  exception: every failure so far was a rate limit, and the attempts completed
  (`protocol/relaypolicy/policy.go:40-60`).
- `onlyRateLimited` counts a node error as a rate limit when its classification carries the
  rate-limit sub-category (`protocol/relaycore/relay_processor.go:1489`). **So the verdict a row
  names is what can open the one exception under which a write is sent again.**
- The unknown-write verdict comes from silence and from the may-have-reached-node flag on
  transport errors. A node error is a reply and passes through (`writeOutcomeIsUnknown`,
  `protocol/rpcsmartrouter/rpcsmartrouter_server.go:1405-1460`).
- REST decides whether a 5xx body is the node's own reply with no chain family
  (`ServerErrorIsNodeReply`, `protocol/chainlib/chainproxy/rpcInterfaceMessages/restMessage.go:74`).
  That keeps a gateway's 502 or 504 on a write "unclear", and no chain row reaches it.
- A sibling's "already known" or "nonce too low" after another node accepted the write is
  released without a score (`rpcsmartrouter_server.go:2565-2582`). There is no
  slot-already-used group: each such answer keeps its own verdict and the node's body is
  forwarded.

### 2.3 Specs already change behaviour, and load unpinned

- Spec keys that drive behaviour today: `stateful`, `timeout_ms` (`protocol/chainlib/common.go:733-734`),
  `deterministic` (`rpcsmartrouter_server.go:6107, 6303`), `hanging_api` (`:3215`,
  `recovery_probe.go:136`), `block_parsing`, `parsers` and `imports`.
- `imports` is resolved by `DoExpandSpec` (`types/spec/expand.go:20-104`). Only
  `api_collections` are inherited; within a collection the child's entry wins by name
  (`api_collection.go:580-582`, `combinable.go:37-67`). 150 of the 157 root specs import another.
- **Unknown JSON keys are dropped without a word** (`utils/keeper/spec.go:23-26`). An older
  router given a spec with a new section ignores it. The spec struct has no version field
  (`types/spec/spec.go:11-21`), and no spec carries an `errors` key today.
- `--use-static-spec` accepts a local file or folder, or a GitHub or GitLab URL
  (`utils/specfetcher/fetcher.go:120-204`). A URL with no ref fetches the default branch
  (`github.go:33-37`). A ref is not checked to be a tag or commit. The resolved commit is never
  recorded, because the tarball's pax header that carries it is skipped (`tarball.go:71-73`).
  Nothing exports the source, ref or commit as a metric.
- The release image ships no specs: `docker/Dockerfile.release` copies the binary only, and the
  goreleaser archives are binary-only. The dev image copies the repo's `specs/` folder
  (`docker/Dockerfile:30`). That folder holds 15 files, 14 of them also in lava-specs, and all 14
  differ from lava-specs.
- The Helm chart's default is `staticSpecs.url: https://github.com/magma-devs/lava-specs` with
  no ref (`smart-router-helm-chart/charts/smart-router/values.yaml:370-378`). No tenant in
  smart-router-infrastructure overrides it. **Every deployed router loads lava-specs `main` at
  boot**, so a merged spec reaches each router at its next restart.

### 2.4 lava-specs has no release discipline yet

- No tags, local or remote. Default branch `main`.
- `spec_guards.yml` runs on every PR: the guard scripts' self-tests, `check_node_admin_rpcs.sh`
  and `check_disabled_count.sh`. `spec_pipeline.yml` runs the agent's phases 8 to 11 when a PR
  opens. `check_stateful.sh` runs only inside the agent's method-schema gate, never on a
  hand-edited PR. There is no `CODEOWNERS` file.
- `check_unused_fields.sh` rejects fifteen removed key names at any depth, including `title`,
  `description`, `local` and `subscription`. The router's `bundled_specs_guard_test.go` does the
  same for `specs/`. The errors section must not use those names.
- In its update modes the create-spec skill never re-emits a file: every write is one `jq`
  assignment, and `check_update_diff.sh` fails on any removed field. So an existing errors section
  survives an update. New-chain mode builds from templates, so the section appears in a new spec
  only once the templates describe it (MAG-4898 part 3).

## 3. The split

| Lives in the spec (per chain) | Stays in router code |
|---|---|
| Which node code or message means which **registered verdict** for this chain's nodes | The verdict registry: names, codes, category, sub-category, retryable, may-have-reached-node |
| Each row's rulebook class, interface, read or write scope, source and versions checked | What a verdict makes the router do: retry, stop, hold off, mark unhealthy, score, cache |
| Inheritance of rows through `imports`, with override by row id | Interface rules: the JSON-RPC envelope, REST status, gRPC codes, GraphQL `errors` |
| | Connection errors, HTTP and gRPC status rules, rate-limit detection, the unknown default |
| | Write safety: fan-out, the single stop, the unknown outcome, the slot-already-used group |

What moves is the six family tables, and later, chain by chain, the geth-wording rows (section
11). What never moves is listed in section 7.

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
  A router knows the schema versions it reads. It drops a section with any other version whole,
  for that chain only (7.4). It never reads part of a section.
- The section is optional. A spec without it loads on every router customers run today, because
  unknown keys are dropped (2.3). A router that reads sections treats a missing one as "no chain
  rows", which is today's behaviour for 202 chains.

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
| `match` | yes | Exactly one of `{"code": N}`, `{"contains": "text"}`, `{"regex": "re2"}` or `{"code": N, "contains": "text"}`. `contains` is case-insensitive, as `MessageContains` is. `regex` is Go RE2, case-sensitive unless it says `(?i)`, and must compile. There is no status, gRPC-code or header matcher: those are interface rules and stay in code. |
| `error` | yes | The **name** of a registered router error, for example `CHAIN_BLOCK_NOT_FOUND`. A name the router does not register fails validation, and so do the names in 7.2. |
| `class` | yes | The rulebook class, C2 to C7. The router keeps one table of the classes each registered error may carry, and a row that disagrees fails validation. The class is for the reader and the change log; the router acts on `error`. |
| `interface` | no | `jsonrpc`, `rest`, `grpc`, `graphql` or `tendermintrpc`. Absent means every interface the chain serves. |
| `scope` | yes | `read`, `write` or `both`. A `write` row is evaluated only for stateful apis, a `read` row only for the rest. |
| `source` | yes | A link to the official definition: the protocol spec, the node software's docs, or the source file that defines the error. Reviewers hold the line on "official" (R-0.2: no vendor docs, no forums). |
| `versions` | yes | The node software versions the row was checked against. At least one. |
| `note` | no | One line for the reader. Not `description` and not `title`: both are rejected key names (2.4). |

### 4.3 Matching

For a node error on chain X, the router evaluates, in order, first match wins:

1. connection errors, decided before any row, as today;
2. **the router's rate-limit rows**: the transport rows that produce `NODE_RATE_LIMITED`, that is
   code 429 and the rate-limit phrases (`error_classifier.go:191-198, 421, 475`; the gRPC phrases
   are `GRPCRateLimitTexts`, `retry_after.go:129`);
3. X's spec rows, as resolved by inheritance (4.4), in file order, filtered by interface and scope;
4. the router's built-in rows for X's family, until the switch removes that table (section 9);
5. the remaining transport rows;
6. `UNKNOWN_ERROR`.

Step 2 is the only change to today's order. Today a family row runs before every transport row,
so a chain row could hide a rate limit, and the endpoint would be scored down instead of held
off. None of the six tables matches a rate-limit input, so moving those rows to the front changes
no verdict; the fixture replay in 8.2 proves it. Below step 2 a chain row may still shadow a
transport row, which is what chain rows are for (Solana's `-32005`).

Two decisions sit outside the rows, and the reader keeps them there: the connection-error step,
which runs before any row, and REST's "is this 5xx the node's own reply", which classifies with no
chain family (2.2). No spec row reaches either.

### 4.4 Inheritance

Rows inherit through `imports` the way `api_collections` do. The child's rows come first, then
each parent's rows the child did not override by `id`, in the parents' order. ETH1's rows reach
BASE, and COSMOSSDK's reach COSMOSHUB. A child removes a parent's row by repeating its `id` with
`"disabled": true`. Only `rows` inherit; `schema_version` is the child's own.

This is new code in `DoExpandSpec`, which today merges only `api_collections`
(`expand.go:68-97`). With 150 importing specs it is the common case, not the edge.

### 4.5 Examples

The examples move three of today's family tables as they are. Each `error` is the registered
name the code row points at today, so the move changes no verdict. The `class` letters, `source`
links and `versions` show the shape only; MAG-4898 supplies the researched values, and the
router's class table checks them.

**Solana** (`solana.json`, no imports; 14 rows today at `error_classifier.go:24-44`, four shown):

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

**Bitcoin** (`btc.json`, no imports; 10 rows today at `:45-59`, three shown). The
`txn-mempool-conflict` row maps to `CHAIN_DOUBLE_SPEND`, one of the slot-already-used verdicts in
7.3: a spec may add a chain's wording to that group, never move it out.

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

**Cosmos SDK** (`cosmossdk.json`, imports IBC and TENDERMINT; 4 rows today at `:60-73`).
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
    `error`, a `match` that widens or narrows a verdict, a removed row, a changed `scope`.
  - **additive** changes when a new row or chain changes nothing that exists, and for every
    change outside the errors section, such as a new api, a verification or a parser fix.
- **Change log.** `CHANGELOG.md` in lava-specs gets one entry per tag, written by the PR that
  bumps it. The entry has a line per chain: what changed, old verdict, new verdict, why, and the
  PR. CI fails a tag whose entry is missing, or whose chains do not match the diff.
- **Routers load a pinned tag or commit, never a branch.** `--use-static-spec` refuses a remote
  source whose ref is absent or names a branch, unless the operator passes
  `--allow-unpinned-spec`. That flag is for development: it warns at boot and reports
  `pinned="false"` below. The fetcher resolves the ref to a commit through the host's API before
  fetching, so the tarball and the recorded commit are the same object.
- **Logged, exported, shown.** The boot log and the health endpoint name the source, the ref, the
  resolved commit and a content hash (SHA-256 over the loaded files, sorted by name). One gauge,
  `smartrouter_spec_source_info{source, ref, commit, content_hash, pinned}` = 1, sits next to the
  existing `smartrouter_protocol_version`. The dashboard shows the pair (router version, spec
  tag) per router.

## 6. Must-have 2: aligned with the router version

- **Compatibility file.** `compatibility.json` at the lava-specs root:

  ```json
  { "schema_versions": { "1": { "min_router": "v1.8.0" } } }
  ```

  It has one line per schema version: the first router release that reads it. The router does not
  need the file to stay safe, because it drops a section whose schema it does not know (7.4). CI
  and the release process read it: it says which router versions a tag must boot on (8.2), and
  which router a customer needs before a tag's new rows act.
- **The release unit is the pair.** Each router release names one lava-specs tag in
  `specs/SPEC_TAG` and ships that tag's files inside the release image. The repo's `specs/` folder
  stays for local runs and tests, but `make specs` fills it from `SPEC_TAG`, and a CI check fails
  when the folder differs from the tag by a byte. `grpctest.json`, which exists only in the router,
  moves to a test fixture directory. Release notes name the pair, and that pair is what an
  on-prem change board approves.
- **Default source.** With no `--use-static-spec`, the router loads the bundled tag; today it
  refuses to start (`rpcsmartrouter.go:2519-2531`). `--use-static-spec` overrides it with a pinned
  tag, a commit or a local folder. The Helm chart's default becomes the bundled tag, and a tenant
  that wants a newer tag pins it on purpose in `cluster.yaml`.
- **Newer schema, older router.** A router that meets a `schema_version` it does not read uses
  its built-in rows for that chain. It logs one warning at boot naming the chain, the schema and
  the router version it would need, and counts
  `smartrouter_spec_errors_section_dropped_total{chain_id, reason="schema_unknown"}`.

## 7. Must-have 3: write protection

### 7.1 What is locked by construction

Rows name verdicts, and verdicts and their consequences are code, so the format alone keeps a
spec away from retries, limits, status rules and write outcomes (4.6). What the code adds:

- Whatever verdict a row names, the stateful policy stops a write after its one fan-out (2.2).
- Rows classify node errors only. The unknown-write verdict reads silence and transport errors,
  and REST's 5xx node-reply rule runs with no chain family, so no row can make an unclear write
  look settled (2.2, 4.3).

Three doors remain, and 7.2 and 7.3 close them: a row that names a rate-limit verdict, which opens
the one exception under which a write is sent again; a row that hides a real rate limit or names
one of the router's own verdicts; and a row that maps a slot-already-used answer to the wrong
verdict.

### 7.2 Verdicts and matches a row may not use

The validator (section 8) reads these from the registry, never from a copied list. A row fails
when its `error` is:

- any verdict with the rate-limit sub-category: today `NODE_RATE_LIMITED`, `NODE_LIMIT_EXCEEDED`
  and `PROTOCOL_RATE_LIMITED`. `onlyRateLimited` counts such a node error as a rate limit, and a
  write whose every failure was a rate limit is sent again (2.2), so a row naming one could make
  the router re-send a write a node executed;
- any `PROTOCOL_*` verdict (38 today): the router's own, never a node's reply;
- `CHAIN_TX_ALREADY_KNOWN`: the lost-broadcast and read-your-writes logic turns on it
  (`rpcsmartrouter_server.go:2565-2582`, `read_your_writes.go:218-231`), so its wording stays in
  code;
- `UNKNOWN_ERROR`: a row that says "unknown" should not exist.

A row also fails when its `match` could hide a rate limit or a status rule:

- `code: 429`, or a `contains` or `regex` that matches any of the router's rate-limit phrases
  (the `NODE_RATE_LIMITED` rows' text and `GRPCRateLimitTexts`). The validator runs each phrase
  through the row's matcher. Step 2 of 4.3 also keeps those rows ahead of spec rows at run time.
- On `rest` and `graphql` rows, where `code` is the HTTP status, a code in the router's status
  table (`httpStatusRows`, 19 statuses).

### 7.3 Write rows

- **Slot already used is always unknown** (rulebook Q45, changed 4 Nov). This is a rule to build;
  today each such answer keeps its own verdict and the node's body is forwarded (2.2). The router
  defines the group in code. The proposal is `CHAIN_NONCE_TOO_LOW`, `CHAIN_INVALID_SEQUENCE`,
  `CHAIN_DOUBLE_SPEND`, `CHAIN_BITCOIN_ALREADY_IN_CHAIN`, `CHAIN_STARKNET_INVALID_NONCE` and
  `CHAIN_STARKNET_DUPLICATE_TX`. The router reports a write whose answers fall in the group as
  unknown, whatever row matched; R&D chooses the wire form, and today's unclear-write reply is
  `errUnknownWriteOutcome` (`rpcsmartrouter_server.go:1411`). A spec may add a chain's wording to
  the group. The validator keeps the group's known phrases (`nonce too low`, `tefPAST_SEQ`,
  `account sequence mismatch`, `already spent`, `txn-mempool-conflict`, `inputs spent`, and those
  MAG-4898 finds). It fails a `write` or `both` row that matches one of them while pointing outside
  the group. That is the "cannot move a write out of unknown" rule, made checkable.
- **Write rows stay in code for 30 days.** Until the side-by-side run (section 9, step 1) shows
  zero mismatches on write-path errors for 30 days, the router evaluates spec `write` rows, counts
  them, and acts on its code rows. Reads switch after 7 days.

### 7.4 A section that breaks a rule

The section is dropped **for that chain only**. The router uses its built-in rows for the chain,
logs one error at boot naming the chain, the row id and the rule, counts
`smartrouter_spec_errors_section_dropped_total{chain_id, reason}`, and boots. A bad row in one
spec never takes a router down and never degrades another chain. lava-specs CI runs the same
validator first, so this path is for a router meeting a tag it was not tested with, or a
hand-edited local folder.

## 8. Must-have 4: tests that gate every spec change

No tag is cut, and no tag reaches a customer, until all of these pass.

### 8.1 lava-specs CI, on every pull request

- **Schema check.** A JSON Schema for the `errors` section, kept in lava-specs, applied to every
  changed spec.
- **Locked-rule check.** One Go validator in smart-router, `cmd/specvalidate`, is the only
  implementation of 4.2, 7.2 and 7.3. lava-specs CI builds it from the router release that
  `compatibility.json` names for the section's schema, and the router runs the same code at boot,
  so CI and boot cannot disagree.
- **`check_stateful.sh` on every PR**, as a job in `spec_guards.yml`. Today it runs only inside
  the create-spec agent, so a hand-edited PR is not checked (2.4).
- **Write-safety job** in `spec_guards.yml`: the validator's write rules plus a diff that flags any
  added, removed or changed `write` or `both` row and labels the PR `write-rows`.
- **Owner review for write rows.** `CODEOWNERS` matches paths and cannot name a JSON section, so
  the gate is a required check: a PR labelled `write-rows` needs an approving review from the
  owners team that a new `CODEOWNERS` file names. The same file makes the spec owners review every
  root spec.
- **Change-log check** (section 5), and the existing `check_unused_fields.sh`, which keeps
  `description` and `title` out of rows.

### 8.2 Against the router

- **Boot matrix.** Every chain whose spec changed boots, with the PR's specs as the source, on
  every router version the compatibility file lists. A section dropped at boot (7.4) fails the job
  unless the PR says so.
- **Fixture replay.** The 101 cases in `error_fixtures.json` are replayed through the spec rows and
  must give the same registered error, unless the change-log entry names the case and its new
  verdict. The fixtures grow with MAG-4898: one case per row, citing the row's `source`.
- **Generated row tests.** For every changed row, CI builds a node error from the row's `match`
  (the code, or a message containing the text) and asserts the row's `error` and `class` on the
  router version under test.

### 8.3 Bad-source tests (router repo)

One test per failure: a tarball that is not one, a tag that does not exist, a row that breaks each
rule in 7.2 and 7.3, a section with an unknown schema, and an unpinned URL without
`--allow-unpinned-spec`. Each asserts that the router boots or refuses as specified, that the log
names the cause, and that the metric carries the reason.

### 8.4 Rollout gate

Managed deployments take a new tag on one canary router per cluster first, then the rest after a
metrics window. The proposal is one full day of production traffic, so each chain's daily cycle
is covered. The window compares `smartrouter_errors_total` by `error_name` and `chain_id` before
and after, and requires the dropped-section counter at zero. On-prem gets the tested pair only.
Tags are immutable, so a tag that fails the canary is not withdrawn; the next tag reverts the
row, with a change-log line that says so.

## 9. Rollout

**Step 0, before the router reads a single row.** This is router and CI work with no behaviour
change:

- pinning, `--allow-unpinned-spec`, and the resolved commit and content hash in the log and the
  metric (section 5);
- `compatibility.json`, `specs/SPEC_TAG`, `make specs`, the folder-matches-tag check, and the
  bundled tag in the release image (section 6);
- `cmd/specvalidate`, the schema, the `spec_guards.yml` jobs, `CODEOWNERS` and the change-log check
  (8.1);
- the rate-limit rows moved to the front of the order (4.3, step 2);
- the Helm chart's default moved from the bare URL to a tag;
- lava-specs `v1.0.0`.

**Step 1, side by side.** The router reads the sections and evaluates spec rows next to its code
rows, acting on the code rows.
`smartrouter_spec_error_rows_mismatch_total{chain_id, row_id, code_error, spec_error, scope}`
counts every disagreement, and `smartrouter_spec_error_rows_hits_total{chain_id, row_id}` every
spec-row hit. The six family tables are copied into their specs with no verdict change, so the
expected mismatch count is zero, and any mismatch is a bug in the move.

**Step 2, switch.** Reads switch to spec rows after 7 days with zero read mismatches, and writes
after 30 days with zero write mismatches (7.3). The switch is one router release that deletes the
six family tables from code, so it is reviewable and revertible as one commit.

**Step 3, per-chain rows open up.** MAG-4898 lands research rows per chain through the gates in
section 8. The ticket estimates about 36 chains change behaviour; each such change is a behaviour
bump with a change-log line and a product call on its PR.

## 10. Observability

| Signal | Where | Why |
|---|---|---|
| source, ref, resolved commit, content hash, pinned | boot log, `/health`, `smartrouter_spec_source_info` | "which specs is this router running" has one answer |
| `smartrouter_spec_errors_section_dropped_total{chain_id, reason}` | router | a bad or unreadable section is visible, not silent |
| `smartrouter_spec_error_rows_mismatch_total{chain_id, row_id, code_error, spec_error, scope}` | router, step 1 | the switch criterion |
| `smartrouter_spec_error_rows_hits_total{chain_id, row_id}` | router | which rows matter; feeds the geth-wording decision in section 11 |
| router version and spec tag per router | dashboard | the release pair, per fleet |

## 11. Open for R&D

- **The generic geth-wording rows.** "execution reverted", "nonce too low", "already known", the
  hex-input phrases and the tx-pool texts (`error_classifier.go:177-254`) apply to every JSON-RPC
  chain today. Either keep them generic, or move them to an ETH1 section that BASE, ARBITRUM and
  the other EVM chains inherit. Decide after 30 days of the hits metric show where they fire
  outside EVM. Narrowing them is a behaviour bump.
- **Does the create-spec agent keep an unknown key when it rewrites a spec?** In the update,
  add-testnet and add-collection modes it does, by construction (2.4). New-chain mode needs a
  template and a reference page (MAG-4898 part 3). Confirm with a test in the skill's
  `scripts/test_*.sh`.
- **Which spec source does the on-prem deployment pull?** The proposal is the bundled tag, always.
  A customer who needs a newer tag gets a router release that bundles it, so the pair stays the
  approved unit.
- **`chainFamilyMap` after the move.** It stays, because the transport rows and the fault axis key
  on the family. The four AVAX* keys move to lava-specs' names, and a lava-specs CI check fails a
  new spec whose index the map does not know.
- **Effort.** The ticket's first estimate is 6 to 8 engineer-weeks. A first split, for R&D to
  resize: Step 0 about 3 weeks, the reader and inheritance about 2, side-by-side and metrics about
  1, and the switch about 1.

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
- MAG-4898 is unblocked with section 4 linked as the approved format.
- The router work is ticketed: Step 0, Step 1 (reader, inheritance, side-by-side metrics),
  Step 2 (the switch), and the open items of section 11.

## References

- Smart Router error rulebook v1.0, R&D review (Notion `40cf5ce8bcd38108bcf3fd93f85e8af5`):
  R-M (rows keyed by chain, node software and interface; no vendor layer), R-L (writes), R-K
  (all-fail ranking), R-0.2 (no vendor wording), Q45 (slot already used) and the change log.
- `docs/ERROR-REGISTRY-DESIGN.md`: the registered verdicts the rows point at.
- MAG-4898 (the per-chain research this blocks), MAG-4059 (write error ranking), MAG-4748
  (accepted hashes), MAG-4824 (a classification change that needed a code release).
- lava-specs: `.github/workflows/spec_guards.yml`, `spec_pipeline.yml`, `create_spec.yml`;
  `.claude/skills/create-spec/scripts/check_stateful.sh`, `check_unused_fields.sh`,
  `check_update_diff.sh`.
- smart-router-helm-chart `charts/smart-router/values.yaml:370-378`: the unpinned default.
