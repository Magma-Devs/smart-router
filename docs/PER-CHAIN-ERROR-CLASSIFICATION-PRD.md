# PRD: per-chain error classification in lava-specs

**Status:** draft for R&D approval (MAG-4910), revision 3.

- **Who approves:** Nimrod or Tom, on the must-haves, the format and the split. Revision 3 takes
  Must-have 1 (spec versioning) out of scope. The ticket says the PRD isn't approvable without all
  four, so the approver also has to accept that departure (1.3).
- **What it blocks:** MAG-4898, the per-chain research, which starts once this is approved.
- **What revision 3 changes:**
  - It takes Must-have 1 out of scope.
  - It closes four gaps a review of revision 2 found: a write the spec leaves out, spec rows that
    can still shadow protected decisions, a Nethermind nonce answer, and vendor wording arriving
    through a custom source.
  - It reorders the document so the decisions come first.

  Appendix C maps every finding, and every revision-2 section number, to where it went.

**Baseline.** Anything this document calls "today" was read on 2026-11-10 at the commits below, and
every file and line reference points at them:

- smart-router `origin/main` `6c237073` (v1.6.8-2)
- lava-specs `main` `bda05974`
- smart-router-helm-chart `main` `2446606`
- smart-router-infrastructure `main` `99f0dbb4`

Revision 1 was read at smart-router `2d99d4b7` and lava-specs `8c1474c`. A fair amount has moved
since then. MAG-4817 moved the XRP rows into `error_classifier.go` and grew them from 9 to 15.
PRs #546, #580, #582, #656, #662, #667 and #678 changed the classifier, the relay processor and
the spec keys the router reads.

## 1. Decision summary

This section is what we're asking R&D to approve. Everything after it is the detail behind it, and
Appendix A has the evidence for every "today" claim.

### 1.1 What we're proposing

- **Each chain's spec carries its own error rows.** A row says which code or message from that
  chain's nodes means which verdict the router already registers. Today these rows are Go code, so
  every new chain error needs a router release (A.1).
- **The router keeps everything that decides what a verdict does**: the interface rules,
  connection errors, status rules, rate limits, the unknown default, retry and scoring, and write
  safety.
- **One rule holds the design together: a spec row may say what a node's answer means; it may
  never say what the router does about it.** Rows point at verdicts the router already registers.
  They can't define a verdict, a retry, a status rule or a write outcome.
- **Rows inherit through `imports`**, the way `api_collections` do, so ETH1's rows reach BASE and
  COSMOSSDK's reach COSMOSHUB (2.5).
- **The router and a lava-specs commit ship as a pair.** Each router release bundles one tested
  lava-specs commit in its image, and that commit's sections are the fallback whenever a loaded
  section can't be used (section 4).
- **Every lock is enforced by the router at run time**, not only in CI, so no spec source can step
  around it (section 3).
- **Nothing acts until it's proven.** Spec rows first run next to the code rows and are only
  counted. Reads switch after 7 days with zero mismatches, and writes after 30 (5.1).

### 1.2 The invariants

These hold for every spec, from every source, and the router enforces them itself:

1. **A write fans out once, then stops**, unless every failure was a rate limit. So no row may name
   a rate-limit verdict (3.1, 3.2).
2. **No spec row runs ahead of the locked prefix**: the explicit rate limits, the router's
   already-known recognisers, the locked HTTP statuses, and every gRPC status the router has
   decided (2.4).
3. **A row can't name** a `PROTOCOL_*` verdict, a rate-limit verdict, `CHAIN_TX_ALREADY_KNOWN` or
   `UNKNOWN_ERROR` (3.2).
4. **A write that fails with a "slot already used" answer is reported as unknown**, whatever row
   matched (3.3).
5. **A known write method is always stateful**: whether the spec marks it, leaves the flag off, or
   doesn't declare the method at all (3.4).
6. **A row acts only if lava-specs CI checked and signed it.** That's how vendor wording stays out,
   whatever the source (3.4).
7. **A section that breaks any of these is dropped for that chain only.** The chain falls back to
   its bundled section, and the router still boots (4.4).

### 1.3 Where this departs from the ticket

**Deliberate departures.** The approver has to accept these:

- **Must-have 1 (spec versioning) is out of scope.** There are no lava-specs tags, no change log,
  no pinning of the spec source, and no metric for which source a router loaded. What follows from
  that:
  - Routers keep loading whatever spec source they're given. The bundled commit (4.2) is the only
    spec version a router release is tied to.
  - A lava-specs PR that moves a verdict names it in its own description, in place of a
    change-log entry. The fixture replay of Must-have 4 needs that record (5.2).
  - The ticket's bad-source tests for a "stale" or "unpinned" source have nothing left to test, so
    they're dropped. The broken-source test stays (5.4).
  - The `schema.behaviour.additive` version is gone. `schema_version` stays as a plain integer,
    because Must-have 2's compatibility file needs it (4.1).
- **Chain rows can still read text inside HTTP 400, 404, 500 and the other unlocked HTTP
  statuses.** The ticket says no spec may "override the HTTP / gRPC status rules". Every gRPC
  status the router has decided is locked (2.4). On HTTP, though, a chain row may still read the
  node's own text inside an unlocked status, because that body is the node's answer: MAG-4061
  needs TON's answers inside a 500. The HTTP statuses that are locked are the ones whose verdict
  carries write safety or endpoint health (3.2).

**Ticket facts that read differently in the code.** Where the two disagree, the design follows the
code:

- **The release image doesn't bundle `specs/`.** Only the dev image and `docker-compose.yml` use
  the repo folder. Every deployed router loads lava-specs `main` at boot (A.3).
- **"Slot already used is always unknown" (Q45) isn't how the router behaves today.** It's a rule
  we still have to build (3.3).
- **The router's own table has vendor rows too.** The ticket keeps vendor wording out of specs, but
  doesn't mention that the router's table already has some. Three rows name a vendor in their
  comment or commit:
  - Two really are a vendor's wording: Alchemy's `-32600 "unsupported method"`, and a
    `blocked … method|rpc` row written for QuickNode's paid tier.
  - The third, "pending state is not supported", is credited to Tatum, but it's actually Erigon's
    own text (A.1).

  The rows the ticket calls geth-wording rows are node-software wording (geth, Erigon, Nethermind),
  which R-M allows. Their real problem is that they apply to every JSON-RPC chain (A.1).
- **A chain row can shadow a rate-limit row today**, because chain rows run first. The locked
  prefix closes that gap, along with the other protected decisions (2.4).

### 1.4 What's still open

Section 6 lists what R&D still has to decide. In short:

- whether the geth-wording rows stay generic or move to an ETH1 section;
- whether the router's own vendor rows stay;
- whether Bitcoin `-27` and Starknet `59` count as unknown or as accepted;
- completing the write-method table and the source allowlist;
- where the signed manifests live, and who holds the signing key;
- the effort, which revision 3 changes in both directions.

## 2. The design

### 2.1 The split

Here's what moves into the specs and what stays in the router:

| Lives in the spec (per chain) | Stays in router code |
|---|---|
| Which node code or message means which **registered verdict** for this chain's nodes | The verdict registry: names, codes, category, sub-category, retryable, may-have-reached-node |
| Each row's rulebook class, interface, read or write scope, source and versions checked | What a verdict makes the router do: retry, stop, hold off, mark unhealthy, score, cache |
| Inheritance of rows through `imports`, with override and removal by row id | Interface rules: the JSON-RPC envelope, REST status, gRPC codes, GraphQL `errors` |
| A chain's own wording added to the slot-already-used group (3.3) | Connection errors, HTTP and gRPC status rules, rate-limit detection, the already-known recognisers, the unknown default |
| | Write safety: fan-out, the single stop, the unknown outcome, the slot-already-used signatures, the write methods that must stay stateful |
| | The rows that name a locked verdict (XRP's three, 3.5), the source allowlist and the manifest signing key (3.4) |

In practice, what moves is the six family tables (minus XRP's three router-owned rows), and later,
chain by chain, the geth-wording rows (section 6). Section 3 lists what never moves.

### 2.2 The errors section

Each spec gets one `errors` object, next to `api_collections`:

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

- **`schema_version`** is an integer. Every section at a given lava-specs commit carries the same
  number, and CI enforces it, so a child and its parents never disagree. A router knows which
  schema versions it can read (4.1). It drops a section with any other version whole, for that
  chain only (4.4). It never reads half a section.
- **`imports`** is optional, and lists the specs whose rows this section inherits. If it's absent,
  the section follows the spec's own `imports`. `[]` means inherit no rows. It only affects rows;
  the spec's api inheritance is untouched. Step 1 of the rollout uses it to keep today's coverage
  (5.1).
- **The section itself is optional.** A spec without it loads on every router customers run today,
  because unknown keys are dropped (A.3). A router that does read sections treats a missing one as
  "no chain rows of its own", which is exactly how 204 chains behave today.

### 2.3 The row

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
| `id` | yes | A stable name, unique within the chain after inheritance. By convention `<family>.<snake_case>`. A child overrides a parent's row by repeating its id. |
| `match` | yes | Exactly one of `{"code": N}`, `{"contains": "text"}`, `{"regex": "re2"}` or `{"code": N, "contains": "text"}`. `contains` is case-insensitive, like `MessageContains`. `regex` is Go RE2, case-sensitive unless it says `(?i)`, and must compile. There's no status, gRPC-code or header matcher: those are interface rules and stay in code. 3.2 limits what `code` may name on each interface. |
| `error` | yes | The **name** of a registered router error, for example `CHAIN_BLOCK_NOT_FOUND`. Validation fails on a name the router doesn't register, and on the names in 3.2. |
| `class` | yes | The rulebook class, C2 to C7. The router keeps a table of the classes each registered error may carry, and a row that disagrees fails validation. The class is for people; the router acts on `error`. |
| `interface` | no | `jsonrpc`, `rest`, `grpc`, `graphql` or `tendermintrpc`. If it's absent, the row applies to every interface the chain serves after inheritance. |
| `scope` | yes | `read`, `write` or `both`. A `write` row is only evaluated for stateful apis, and a `read` row only for the rest. Since the router keeps write methods stateful (3.4), a spec can't move a write out from under the write rows. |
| `source` | yes | A link to the official definition (the protocol spec, the node software's docs, or the source file that defines the error), pinned to a tag or commit. It must be on the router's allowlist, and the row must be in a signed manifest (3.4). |
| `versions` | yes | The node software versions the row was checked against. At least one. |
| `slot_used` | no | `true` adds this row's answers to the slot-already-used group (3.3). Only allowed on `write` or `both` rows. It can widen the router's group, never narrow it. |
| `disabled` | no | `true` removes the parent row with the same `id`, for this spec and its children. A disabling row carries only `id`, `disabled` and optionally `note`; any other field fails validation. |
| `note` | no | One line for the reader. It's called `note` because `description` and `title` are both rejected key names (A.4). |

### 2.4 Matching order

For a node error on chain X, the router walks this list in order, and the first match wins:

1. **Connection errors**, decided before any row, as today.
2. **The locked prefix**, all router code:
   1. **Explicit rate limits.** These are the transport table's `NODE_RATE_LIMITED` rows that read
      a rate-limit signal: `CodeEquals(429)` and the phrases "rate limit", "too many requests" and
      "enhance_your_calm" (JSON-RPC `:283-290`, REST `CodeEquals(429)` from `:621`); on gRPC, the
      four `GRPCRateLimitTexts` (`:512-514`); and XRP's `tooBusy`/`slowDown` row (3.5).
      - On gRPC a phrase only counts when the status isn't one of the five the gRPC table checks
        first today (InvalidArgument, NotFound, OutOfRange, Unimplemented, Aborted). That keeps
        today's order.
      - The row that matches a status number anywhere in the text, `HTTPStatusContains(429)`, is
        **not** in the prefix. It stays after the chain rows, where it is today.
   2. **The already-known recognisers.** JSON-RPC's "already known", "alreadyknown" and Besu's
      `known transaction` rows (`error_classifier.go:310, 311, 316`), and XRP's `tefALREADY` row
      (3.5). Read-your-writes turns on `CHAIN_TX_ALREADY_KNOWN` (`read_your_writes.go:218-231`),
      so no spec row may answer one of these first. REST 409, Horizon's duplicate, is a locked
      status in the next step.
   3. **The locked statuses.** A reply is decided by the transport table, and never reaches step 3,
      when:
      - its HTTP status is 401, 403, 409, 429, 502, 503, 504, 520 to 527 or 530. The status is read
        as the code on REST and GraphQL, and from the `HTTP <status>: ` prefix on every other
        interface;
      - or its gRPC status is one the router has decided: the six with a row (InvalidArgument,
        NotFound, OutOfRange, Unimplemented, Aborted and Unavailable, `error_classifier.go:482-517`)
        or the six the comment at `:518-538` leaves without one on purpose (DeadlineExceeded,
        PermissionDenied, ResourceExhausted, Unauthenticated, Canceled and DataLoss).

      So on gRPC, spec rows only ever see Unknown, AlreadyExists, FailedPrecondition and Internal.
3. **X's spec rows**, as resolved by inheritance (2.5), in file order, filtered by interface and
   scope.
4. **The router's built-in rows for X's family**, until the switch removes that table (5.1). After
   the switch, this step holds the router-owned rows that have to come after the spec rows (XRP's
   engine-result catch-all, 3.5).
5. **The remaining transport rows.**
6. **`UNKNOWN_ERROR`.**

**Step 2 is the only change to today's order, and it's been replayed.** At `6c237073`, with the
XRP placement of 3.5, it gives today's verdict for all 102 fixtures, and for all 56 counterexamples
in 5.3 that must keep theirs. Those 56 include 20 XRP engine results with and without an
`HTTP 500: ` prefix, 12 slot-already-used pairs, and 15 already-known answers spread over five
families. Both gRPC fixtures are now decided in the locked-status step, with the same verdict as
today.

**Why the rate-limit part is this narrow.** Revision 1's prefix moved every row that produces
`NODE_RATE_LIMITED`, the digit-matching status row included, and ignored the gRPC order. It still
passed all 102 fixtures, yet it turned six counterexamples into `NODE_RATE_LIMITED`:

- Bitcoin's `-26` "min relay fee not met, 429 < 1000";
- Bitcoin's "mempool min fee not met, 429 < 1000";
- Agave's `-32005` "Node is behind by 429 slots";
- a gRPC Aborted, InvalidArgument and NotFound, each carrying a rate-limit phrase.

The fee in the Bitcoin message is the client's own number. So under revision 1, a client could
hold off a chain's providers, and open the write re-send of A.2, with one deliberately priced
transaction. The fixtures are 97 JSON-RPC cases, 3 REST and 2 gRPC, which is why replaying them
alone couldn't catch it.

**What step 2 changes on purpose.** Below it, a chain row can no longer hide an explicit rate
limit, an already-known answer, a locked HTTP status or a decided gRPC status. A reply like that now
takes the router's verdict even when a chain row matches its text. Cases from the replay:

- a REST 403 whose body says "account not found" becomes `NODE_ACCESS_DENIED` instead of
  `CHAIN_ACCOUNT_NOT_FOUND`;
- a REST 409 whose body says "account sequence mismatch" becomes `CHAIN_TX_ALREADY_KNOWN` instead
  of `CHAIN_INVALID_SEQUENCE`;
- a JSON-RPC reply that arrived as `HTTP 503: account sequence mismatch` becomes
  `NODE_SERVICE_UNAVAILABLE` instead of `CHAIN_INVALID_SEQUENCE`;
- "insufficient fee" inside a gRPC Unavailable, ResourceExhausted or Aborted becomes
  `NODE_SERVICE_UNAVAILABLE`, `UNKNOWN_ERROR` (the router's deliberate answer to that status) or
  `NODE_ABORTED`, instead of `CHAIN_INSUFFICIENT_FEE`;
- "account not found" inside a gRPC NotFound becomes `NODE_DATA_NOT_HELD`, and "account sequence
  mismatch" inside a gRPC InvalidArgument becomes `USER_INVALID_PARAMS`.

An XRP reply that arrived as `HTTP 503: tefPAST_SEQ` becomes `NODE_SERVICE_UNAVAILABLE` too. On a
write it still ends unknown, because 3.3 reads the answer, not the verdict.

No fixture has that shape. If MAG-4898 finds a chain whose nodes give a locked status a meaning of
their own, that's a change to the router's status rule, and it gets ticketed as one. A chain row
can still shadow any other transport row, which is what chain rows are for: Solana's `-32005`
still shadows the generic `-32005`.

**Banning a verdict isn't enough; the prefix is what protects a decision.** The review of
revision 2 showed two rows that name allowed verdicts and still override a protected decision when
they run first. Both now lose to the prefix:

- `{contains: "block not found"}` → `CHAIN_BLOCK_NOT_FOUND`. Today it turns a gRPC InvalidArgument
  "block not found" into a retryable `CHAIN_BLOCK_NOT_FOUND`. With the prefix, the status decides
  first and the answer stays `USER_INVALID_PARAMS`, which isn't retryable.
- `{code: -32000}` → `CHAIN_TX_REJECTED`. Today it hides EVM's `-32000 "already known"` from
  read-your-writes. With the prefix, the already-known step decides first and the answer stays
  `CHAIN_TX_ALREADY_KNOWN`.

Both were replayed by putting the row at the head of the family table.

**Two decisions stay outside the rows**, and the new section reader leaves them there:

- the connection-error step, which runs before any row;
- REST's "is this 5xx the node's own reply", which classifies with no chain family (A.2).

One more path explains why ResourceExhausted is locked. A gRPC rate limit that only shows up in the
reply metadata is applied only when the rows return `UNKNOWN_ERROR`
(`direct_rpc_relay.go:1208-1209`), so any row that matched such a reply would hide it.

### 2.5 Inheritance

Rows inherit through `imports` the same way `api_collections` do, unless the section names its own
`imports` (2.2). The resulting order is:

- the child's own rows first;
- then each parent's rows that the child didn't override or disable by `id`, in the parents'
  order.

So ETH1's rows reach BASE, and COSMOSSDK's reach COSMOSHUB. A child with no section of its own
inherits its parents' rows unchanged. Only `rows` inherit; `schema_version` is the same across the
whole repo at any commit, so a child's section and its parents' always share it.

This is new code in `DoExpandSpec`, which today only merges `api_collections` (`expand.go:72-100`).
With 144 importing files, inheritance is the common case, not an edge case.

**Two facts shape the first commit that carries sections** (Appendix B):

- **STRKS would lose the Starknet rows.** STRKS (Starknet Sepolia) imports nothing today, so it
  would lose all 16 Starknet rows. Its spec is a near-copy of STRK's, and with
  `"imports": ["STRK"]` its expanded apis are byte-identical to today's (checked with the router's
  own `ExpandSpecWithDependencies`, `utils/keeper/spec.go:89`).
- **36 chains would gain rows** they don't have today, because they import a family root that
  `chainFamilyMap` doesn't give them.

Seven specs reach both COSMOSSDK and ETH1. That's no conflict while ETH1 carries no rows, and if
ETH1 ever does, a section's own `imports` settles it.

### 2.6 Examples

The examples below move three of today's family tables as they are. A few things to keep in mind
while reading them:

- Each `error` is the registered name today's code row points at.
- Today's rows apply to reads and writes alike, so a row moved as-is gets `scope: both`. The
  `read` and `write` scopes below are there to show the field. Narrowing a row to one scope is a
  behaviour change: Agave's `sendTransaction` can answer `-32016`, and a Cosmos SDK simulate, which
  is a read, can answer "account sequence mismatch".
- The `class` letters, `source` links and `versions` only show the shape. MAG-4898 supplies the
  researched values, and the router's class table checks them.

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
`CHAIN_BITCOIN_VERIFY_REJECTED` today, not `CHAIN_DOUBLE_SPEND`. That's fine: whichever verdict a
row gives, 3.3 decides "slot already used" from the answer itself.

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
since `xrplMessageStartsWith` is an anchored regex anyway; three stay in router code (3.5). One
shown:

```json
{ "id": "xrp.past_seq", "match": { "regex": "^(?:HTTP \\d+: )?tefPAST_SEQ\\b" },
  "error": "CHAIN_NONCE_TOO_LOW", "class": "C6", "scope": "write",
  "source": "https://github.com/XRPLF/rippled/blob/2.3.0/include/xrpl/protocol/TER.h",
  "versions": ["rippled 2.3"] }
```

**Disabling and section imports.** STRKS picks up the Starknet rows with a single spec-level
import, and a chain that mustn't inherit a parent's row disables it:

```json
{ "index": "STRKS", "imports": ["STRK"], "api_collections": [ "..." ] }
```

```json
"errors": { "schema_version": 1, "rows": [ { "id": "cosmossdk.account_not_found", "disabled": true,
  "note": "this chain answers unfunded accounts with an empty balance, not an error" } ] }
```

### 2.7 What the format cannot say

There's no field for a retry, a retry limit, a status code, a gRPC code, a connection error, a
timeout, a hold-off, a score, a write outcome or a new verdict, and that's deliberate. A row that
needs any of those is a router change, and gets ticketed as one.

## 3. Write protection and the other locks (Must-have 3)

### 3.1 What is locked by construction

Rows name verdicts, and verdicts and their consequences are code, so the format alone keeps a spec
away from retries, limits, status rules and write outcomes (2.7). On top of that, the code already
guarantees two things:

- Whatever verdict a row names, the stateful policy stops a write after its one fan-out, unless
  every failure was a rate limit (A.2).
- No row can make an unclear write look settled. The unclear-write decision reads protocol errors
  classified with no chain family and treats every node error as a reply, and REST's 5xx
  node-reply rule also runs with no chain family (A.2, 2.4).

That isn't the whole story, though. Rows reach more than node replies: a direct-RPC transport or
status error gets its retry and health verdicts with the chain family (A.2). So the locks below are
run-time rules in the router, not just checks in CI. That leaves five doors open, and 3.2 to 3.5
close them:

1. a row that names a rate-limit verdict, which opens the one exception under which a write is
   sent again;
2. a row that hides a rate limit, a status, or an answer the router acts on, such as "already
   known";
3. a slot-already-used answer reported as anything but unknown;
4. a spec that drops `stateful` from a write, by clearing the flag or by leaving the method out, or
   a row that quotes a vendor;
5. the XRP rows that name locked verdicts.

### 3.2 Verdicts and matches a row may not use

The validator (5.2) reads all of these from the registry and the router's tables, never from a
copied list.

**Verdicts a row may not name.** A row fails when its `error` is:

- **Any verdict with the rate-limit sub-category**: today `NODE_RATE_LIMITED`,
  `NODE_LIMIT_EXCEEDED` and `PROTOCOL_RATE_LIMITED`. `onlyRateLimited` counts such a node error as
  a rate limit, and a write whose every failure was a rate limit is sent again (A.2). So a row
  naming one could make the router re-send a write a node already executed, and hold off the
  providers that answered.
- **Any `PROTOCOL_*` verdict** (38 of 134). These are the router's own, never a node's reply.
- **`CHAIN_TX_ALREADY_KNOWN`.** The read-your-writes logic turns on it
  (`read_your_writes.go:218-231`), so its recognisers stay in code and run in the locked prefix
  (2.4): JSON-RPC's "already known" rows (`error_classifier.go:310, 311, 316`), REST 409 (`:449`)
  and XRP's `tefALREADY` (`:117`, 3.5).
- **`UNKNOWN_ERROR`.** A row that says "unknown" shouldn't exist.

**Matches a row may not use.** A row also fails when its match could replace a status rule or hide
a rate limit, on any interface it applies to. A row without `interface` applies to every interface
its chain serves after inheritance, and the validator checks it against each one:

- **`code` alone can't be a status.** No HTTP status (100 to 599) on any interface, because REST
  and GraphQL classify with a status as the code, and JSON-RPC does too whenever the body carries
  no JSON-RPC code (A.1). And no `code` at all on `grpc`, where the code is the gRPC status, and
  the router decides every status that has a row or was left without one on purpose (2.4).
- **`code` with `contains` may name an unlocked HTTP status**, so a row can read a node's own text
  inside one, the way the router's own `CodeAndMessage(500, …)` rows do (`:413-416`). A locked
  status from 2.4 step 2 can't be named at all.
- **`code: 429` fails** in any form.
- **A `contains` or `regex` fails if it matches** a rate-limit phrase (the `NODE_RATE_LIMITED`
  rows' text and `GRPCRateLimitTexts`), an already-known phrase, or the text a locked status
  reaches the classifier as: the `HTTP <status>: ` prefix and the bare status number that
  `HTTPStatusContains` reads. The validator runs each of those texts through the row's matcher.

**Banning a verdict isn't enough on its own.** A row can name an allowed verdict and still override
a protected decision, just by matching first (2.4 has the two examples). So the protected decisions
run in the locked prefix, ahead of every spec row, whatever a row's text says. The checks above are
the early warning in CI; the prefix is the guarantee.

**Why these statuses are locked.** On HTTP, they're the ones whose verdict carries write safety or
endpoint health:

- 401 and 403 (credentials);
- 409 (`CHAIN_TX_ALREADY_KNOWN` on a Horizon write);
- 429;
- the gateway and edge statuses 502 to 504, 520 to 527 and 530.

On gRPC, every status the router has decided is locked, because on gRPC the status is the code,
and the ticket locks the gRPC status rules. HTTP statuses whose body is the node's own answer, such
as 400, 404 and 500, stay readable by chain rows (MAG-4061 needs TON's answers inside a 500). That
carve-out is a departure from the ticket (1.3).

### 3.3 Slot already used

**Slot already used is always unknown** (rulebook Q45, changed 4 Nov). This is a rule we still
have to build: today each such answer keeps its own verdict, and the node's body is forwarded
(A.2).

- **The router decides it from the answer itself.** A signature table in the router's own code
  reads the chain family, the code and the message, before and apart from row matching. It doesn't
  matter which row matched, from a spec or from code, so no row can move an answer out of the
  group. The group can't be a list of verdicts: Bitcoin's code rows run before its phrase rows, so
  a mempool conflict is `CHAIN_BITCOIN_VERIFY_REJECTED` and a spent input is
  `CHAIN_BITCOIN_VERIFY_ERROR`, and neither verdict says "slot used" on its own.
- **When a write ends with no attempt accepted and any answer matches a signature, the router
  reports the write as unknown.** R&D picks the wire form. Today's unclear-write reply is an
  `*unclearWriteError` (`rpcsmartrouter_server.go:1421-1445`), and this adds a fifth reason.
- **Proposed signatures.** MAG-4898 completes them per node software:

  | Family | Code | Message | Today's verdict |
  |---|---|---|---|
  | any | any | contains "nonce too low" | `CHAIN_NONCE_TOO_LOW` |
  | any | any | contains "nonce is too low" (Nethermind's processor, `error_classifier.go:294`) | `CHAIN_NONCE_TOO_LOW` |
  | any | any | contains "account sequence mismatch" | `CHAIN_INVALID_SEQUENCE` |
  | XRP | any | starts with `tefPAST_SEQ` | `CHAIN_NONCE_TOO_LOW` |
  | Bitcoin | -26 | contains "txn-mempool-conflict" | `CHAIN_BITCOIN_VERIFY_REJECTED` |
  | Bitcoin | -25 | contains "missingorspent" or "missing inputs" | `CHAIN_BITCOIN_VERIFY_ERROR` |
  | Bitcoin | any | contains "already spent" or "double spend" | `CHAIN_DOUBLE_SPEND` |
  | Bitcoin | -27 | any | `CHAIN_BITCOIN_ALREADY_IN_CHAIN` |
  | Starknet | 52 | any | `CHAIN_STARKNET_INVALID_NONCE` |
  | Starknet | 59 | any | `CHAIN_STARKNET_DUPLICATE_TX` |

  Checked at `6c237073`, these catch nine fixtures: `geth_nonce_too_low`,
  `geth_nonce_too_low_verbose`, `nethermind_nonce_too_low`, `nethermind_nonce_too_low_processor`,
  `bitcoin_verify_error`, `bitcoin_already_in_chain`, `starknet_invalid_nonce`,
  `starknet_duplicate_tx` and `xrpl_tef_past_seq`. They catch none of the four already-known
  fixtures (EVM "already known", XRP `tefALREADY`). No fixture carries `CHAIN_INVALID_SEQUENCE`
  today, so the "account sequence mismatch" signature is covered only by its counterexample pair
  until MAG-4898 adds Cosmos write fixtures.
- **Every nonce and sequence fixture has to be caught.** The gate (5.3) fails if a fixture whose
  verdict is `CHAIN_NONCE_TOO_LOW`, `CHAIN_INVALID_SEQUENCE` or `CHAIN_STARKNET_INVALID_NONCE`
  reaches the write path without matching a signature. That way a new node-software wording can't
  slip out of the group the way Nethermind's "transaction nonce is too low" did in revision 2.
- **Bitcoin `-27` and Starknet `59` are a judgement call.** They say "this same transaction is
  already there", not "the slot is taken". They're in the group because revision 1 put them there;
  with MAG-4059's ranking, R&D may report them as accepted instead (section 6).
- **A spec can widen the group, never narrow it.** A `write` or `both` row may carry
  `"slot_used": true`, and the router then treats any answer that row matches as a signature.
- **Write rows stay in code for 30 days.** Until the side-by-side run (5.1, Step 1) shows zero
  mismatches on write-path errors for 30 days, the router evaluates spec `write` rows and counts
  them, but acts on its code rows. Reads switch after 7 days.

### 3.4 Locks the router enforces on every spec

Two of the ticket's locks are about what a spec says, not what a row says. The router enforces both
on every spec it loads, from any source, so a local folder or a custom source can't step around
them.

**A write method stays stateful, whether the spec declares it or not.**

- The router keeps a table of write methods in its own code, keyed by api interface and name (and
  by family, for a generic name like XRP's `submit`). It's seeded from `check_stateful.sh`'s list
  plus the writes that list misses (A.4), and MAG-4898 completes it.
- **At load**, after `DoExpandSpec` (called from `utils/keeper/spec.go:79`) and before the chain
  parser receives the spec, any api in the table that the spec doesn't mark `stateful` is loaded as
  stateful anyway. The router logs one warning per chain naming the apis, and counts
  `smartrouter_spec_lock_enforced_total{chain_id, rule="stateful"}`.
- **At request time**, the lock also covers a write the spec doesn't declare at all. For a method
  the spec doesn't have, the parser builds a `Default-` api on the fly (`getSupportedApi` →
  `defaultApiContainer`, `base_chain_parser.go:532-560, 579`; REST at `rest.go:158`). Every
  interface reaches it: JSON-RPC, GraphQL, gRPC and tendermintrpc through `getSupportedApi`
  (`jsonRPC.go:121`, `graphql.go:166`, `grpc.go:137-143`, `tendermintRPC.go:48-54`). Today that
  api has no `Stateful` and sets `Deterministic: true`, so a write the spec leaves out goes to a
  single provider and can be cached. The review of revision 2 confirmed it: with
  `eth_sendRawTransaction` removed from the spec, the request became
  `Default-eth_sendRawTransaction` with `stateful` 0. With the lock, a method in the table gets a
  stateful, non-deterministic default api, and the router counts it on the same counter with
  `rule="stateful_default"`.
- `cmd/specvalidate` exports the same table, so lava-specs CI fails a PR that clears `stateful`
  on a write before any router sees it. `check_stateful.sh` reads the table instead of keeping its
  own copy, which also ends its gRPC and Tendermint blind spot.
- Today the load-time lock flips nothing: all 132 writes on lava-specs `main` are already stateful.

**No vendor wording: a row acts only if lava-specs CI signed it.**

- **Allowlist.** A row's `source` must be on the router's allowlist of node-software and
  protocol-spec origins (the repositories of the node implementations the six families run, plus
  the protocol specs; MAG-4898 completes it), with the link pinned to a tag or a commit.
- **Content check, in CI.** CI fetches the cited file at the cited ref, and checks that the row's
  text (the `contains` string, or the longest literal in a `regex`) actually appears in it. That
  way a vendor's phrase can't ride in on a node-software link.
- **Signed manifest, checked at run time.** The router can't fetch every cited file at boot, so CI
  signs what it checked. After the source check passes on a merged commit, lava-specs CI writes a
  manifest of the SHA-256 of every row's canonical JSON (keys sorted, no whitespace), per chain, and
  signs it with a key only that CI job holds. The router embeds the public key. It acts on a
  chain's section only if every row's hash is in a manifest with a valid signature; otherwise it
  drops the section (4.4).
- **What that means for each source.** A custom source or a local folder can't carry a row CI
  never saw, even one that cites an allowed, pinned link. An unmodified lava-specs checkout works
  anywhere, because its manifest travels with it, and the bundled commit's manifest ships in the
  image. Customer rows wait for the overlay in section 7, which needs an approval path of its own.
- **Still open** (section 6): where the manifests are published, who holds the key, how it
  rotates (a rotation needs a router release, since the key is embedded), and how a signed row
  that later turns out to be wrong is revoked.

The router's own vendor rows are transport rows, and they stay in code whatever this PRD decides.
Section 6 asks whether R-0.2 removes them.

### 3.5 Router-owned rows

Three XRP rows stay in router code after the switch, because they either name a locked verdict or
depend on the rows around them:

- **`tooBusy`, `slowDown` → `NODE_RATE_LIMITED`** (`error_classifier.go:127`). It's a rate-limit
  verdict, so it's evaluated in 2.4 step 2 with the other explicit rate limits.
- **`tefALREADY` → `CHAIN_TX_ALREADY_KNOWN`** (`:117`). Read-your-writes turns on this verdict.
  It's evaluated in 2.4 step 2 with the other already-known recognisers, ahead of the spec rows, so
  no spec row can match its text first.
- **The engine-result catch-all `te[fmlr]…` → `CHAIN_TX_REJECTED`** (`:119`, with
  `xrplRejectionCode` at `:171`). It classifies every rejection-shaped result the XRPL reader flags
  (`IsXRPLRejection`), which is interface logic, and it has to come after XRP's specific
  engine-result rows (`tefPAST_SEQ`, `terPRE_SEQ`), which move to the spec. It's evaluated in 2.4
  step 4, after the spec rows.

The other twelve XRP rows move to `ripple.json` as `regex` rows (2.6). Moving these three earlier
or later in the order changes no verdict, because their tokens match no other XRP or transport row.

## 4. Compatibility and fallback (Must-have 2)

### 4.1 Schema versions and the compatibility file

`schema_version` moves only when an older router can't read the new format. With Must-have 1 out
of scope (1.3), it's the only version number lava-specs carries.

A `compatibility.json` at the lava-specs root has one line per schema version, naming the first
router release that reads it:

```json
{ "schema_versions": { "1": { "min_router": "v1.8.0" } } }
```

The router doesn't need this file to stay safe, because it drops any section whose schema it
doesn't know (4.4). The file is for CI and the release process: it says which router releases a
lava-specs change must boot on (5.3), and which router a customer needs before new rows take
effect.

### 4.2 The release pair

Each router release names one tested lava-specs commit in `specs/SPEC_COMMIT` and ships that
commit's files inside the release image.

- The repo's `specs/` folder stays for local runs and tests, but `make specs` fills it from
  `SPEC_COMMIT`, and a CI check fails if the folder differs from that commit by even a byte.
- The router's CI runs its own validator and the manifest check (3.4) over every section in the
  bundled commit, so the bundled sections are always valid for the router that carries them.
- `grpctest.json`, which only exists in the router, moves to a test fixture directory.
- Release notes name the pair (router version and lava-specs commit), and that pair is what an
  on-prem change board approves.

### 4.3 Default source

With no `--use-static-spec`, the router loads its bundled specs (today it refuses to start,
`rpcsmartrouter.go:2569-2581`). `--use-static-spec` still takes any URL or folder, as it does
today; with Must-have 1 out of scope, the router doesn't pin or check that source. Whatever it
loads, every errors section still has to pass the locks in section 3, signature included.

The Helm chart's default becomes the bundled specs, and a tenant that wants newer specs sets a
source on purpose in `cluster.yaml`.

### 4.4 Fallback: when a section can't be used

Whenever a chain's section from the loaded source can't be used, the router uses that chain's
section from its bundled specs instead. That happens when:

- the section has a `schema_version` the router can't read (a newer schema on an older router);
- a row breaks a rule in section 3, or isn't in a validly signed manifest;
- the router couldn't load the source at all (the host doesn't answer, the URL or ref doesn't
  exist, what comes back isn't an archive, a spec doesn't parse). Then the router boots on its
  bundled specs for everything, not just the errors sections.

In each case the router logs one error at boot naming the chain, the row id and the rule (or the
reason), counts `smartrouter_spec_errors_section_dropped_total{chain_id, reason}` (for example
`reason="schema_unknown"` or `reason="unsigned_row"`), or
`smartrouter_spec_source_fallback_total{reason}` when the whole source failed, and boots. A bad row
in one spec never takes a router down and never degrades another chain. lava-specs CI runs the same
validator first, so in practice this path is for a router meeting specs it wasn't tested with, or a
hand-edited folder.

**The fallback is the same before and after the switch.**

- Before the switch, the bundled section and the code family table agree (Step 1 proves it), so
  falling back changes nothing.
- After the switch deletes the family tables, the bundled section is the only built-in
  classification a chain has. It stays tested: 5.4 runs the fallback after the switch.
- If the bundled specs have no section for the chain, the chain gets only the transport rows, just
  like the 204 unmapped chains today, and the reason says so.

**After the switch, this fallback is what keeps a chain classified.** Without it, a dropped Solana
section would send "Node is unhealthy" to the generic `-32005` row, `NODE_LIMIT_EXCEEDED`, which is
non-retryable and carries the rate-limit sub-category. A read would then stop at the first
unhealthy node instead of failing over, and that node would be held off instead of marked
unhealthy. 5.4 tests exactly this case.

## 5. Rollout and acceptance gates (Must-have 4)

### 5.1 Rollout

**Step 0: before the router reads a single row.** This is router and CI work. It moves no
fixture's verdict, and it lets the locked prefix decide its own replies (2.4). It does change three
more things a client or operator can see:

- with no `--use-static-spec`, or with a source it can't load, the router boots on its bundled
  specs instead of refusing to start (4.3, 4.4);
- a write method is relayed stateful even when a spec clears the flag or leaves the method out
  (3.4). No declared write is non-stateful today; an undeclared one is served today as a cacheable,
  single-provider read;
- a failed write whose answers include a slot-already-used signature is reported unclear, instead
  of carrying the node's rejection (3.3).

What Step 0 delivers:

- `compatibility.json`, `specs/SPEC_COMMIT`, `make specs`, the folder-matches-commit check, the
  bundled specs in the release image, booting with no source, and the source fallback (section 4);
- `cmd/specvalidate`, the schema, the `spec_guards.yml` jobs, `CODEOWNERS`, the source check and
  the manifest signing (5.2, 3.4);
- the locked prefix of 2.4 step 2, and the router-owned XRP rows (3.5);
- the write-method table, with both the load-time and the request-time lock (3.4);
- the slot-already-used signatures (3.3);
- the AVAX* keys in `chainFamilyMap` renamed (section 6);
- the Helm chart's default moved from the bare URL to the bundled specs.

**Step 1: side by side.** The six family tables (minus XRP's three router-owned rows) are copied
into their root specs with no verdict change. STRKS imports STRK, and each of the 36 chains that
would gain rows gets `"errors": {"schema_version": 1, "imports": []}` (Appendix B), so every chain
keeps exactly today's rows. No router acts on that commit yet.

The router reads the sections and evaluates spec rows next to its code rows, but acts on the code
rows. Two metrics track it:

- `smartrouter_spec_error_rows_mismatch_total{chain_id, kind, row_id, code_error, spec_error, scope}`
  counts three kinds of disagreement:
  - `differ`: both matched, with different verdicts;
  - `code_only`: a code family row matched and no spec row did, meaning a chain lost coverage;
  - `spec_only`: a spec row matched and no code family row did, meaning a chain gained it.
- `smartrouter_spec_error_rows_hits_total{chain_id, row_id}` counts every spec-row hit.

The expected count for every kind is zero. Any mismatch is a bug in the move.

**Step 2: switch.** A router setting with three values decides which rows act. Here's the order in
each window:

| Setting | Read apis | Write apis |
|---|---|---|
| `shadow` (Step 1) | prefix, family table, transport; spec rows counted only | same as reads |
| `reads` (after 7 days with zero read mismatches) | prefix, spec rows, family table, transport | prefix, family table, transport; spec rows counted only |
| `all` (after 30 days with zero write mismatches) | prefix, spec rows, family table, transport | prefix, spec rows, family table, transport |
| tables deleted (next release) | prefix, spec rows, router-owned rows, transport | same as reads |

- "Prefix" means 2.4 step 2, which is active from Step 0.
- Flipping the setting doesn't need a release.
- The six family tables, except the router-owned rows, are deleted in the release after `all` has
  run through one canary window. From that release on, the fallback is the bundled sections (4.4),
  and 5.4 runs it in the router's CI.

**Step 3: per-chain rows open up.** MAG-4898 lands researched rows per chain through the gates
below, and the 36 gains of Appendix B open one at a time, by removing a chain's `"imports": []`.
Each of those is a behaviour change, named in its PR with a product call (5.2).

### 5.2 lava-specs CI, on every pull request

No lava-specs change reaches a customer's router until all of the gates in 5.2 to 5.5 pass.

- **Schema check.** A JSON Schema for the `errors` section, kept in lava-specs and applied to every
  changed spec.
- **Locked-rule check.** One Go validator in smart-router, `cmd/specvalidate`, is the only
  implementation of 2.3, 3.2, 3.3 and 3.4. lava-specs CI builds it from each router release the
  boot matrix covers (5.3), and each router runs the same code at boot, so CI and boot can't
  disagree for any release in service.
- **Stateful check.** The validator's write-method table (3.4), run on every PR as a job in
  `spec_guards.yml`. Today `check_stateful.sh` only runs inside the agent, once, when a PR opens
  (A.4).
- **Write-safety job** in `spec_guards.yml`: the validator's write rules, plus a diff keyed by row
  `id` (not by position) that flags any added, removed or changed `write` or `both` row and any
  `slot_used`, and labels the PR `write-rows`.
- **Source check and signing** (3.4): every row's `source` is on the allowlist and pinned, and its
  text is found in the cited file. Once a PR merges, CI signs the manifest for that commit.
- **Owner review for write rows.** `CODEOWNERS` matches paths and can't name a JSON section, so the
  gate is a required check instead: a PR labelled `write-rows` needs an approving review from the
  owners team named in a new `CODEOWNERS` file. The same file makes the spec owners review every
  root spec.
- **Behaviour changes named in the PR.** With no change log (1.3), the PR is the record. A PR that
  moves a verdict (a new or changed `error`, a `match` that widens or narrows a verdict, a removed
  or disabled row, a changed `scope`, a new `slot_used`, or a chain that gains or loses inherited
  rows) lists each affected chain in its description, with the old verdict, the new one and why,
  and carries a `behaviour-change` label. The replay and the coverage check (5.3) fail a PR that
  moves a verdict without it.
- **`check_unused_fields.sh`**, which keeps `description` and `title` out of rows, moves into
  `spec_guards.yml` so it runs on every PR (A.4).

### 5.3 Against the router

- **Boot matrix.** Every chain whose spec changed boots, with the PR's specs as the source, on
  every router release in service from the schema's `min_router` up. Each release runs its own
  validator at load. A section dropped at boot (4.4) fails the job unless the PR says so.
- **Replay.** The 102 cases in `error_fixtures.json`, plus the router's own classifier and status
  suites (`TestRESTStatusRows`, `TestRESTStatusRowsDoNotReachOtherTransports`,
  `TestGRPCRateLimitTexts_TheCodeRowsAboveStillDecide`, `TestEndpointFault_AnswerShapes`,
  `TestGRPCStatusError`), run against the spec rows and must give the same registered error, unless
  the PR names the case and its new verdict (5.2). The fixtures grow with MAG-4898: one case per
  row, citing the row's `source`.
- **Counterexamples.** The fixtures are almost all JSON-RPC, so they miss some answers. Two fixed
  sets fill that gap.
  - *Must keep today's verdict:*
    - a status or rate-limit token inside an unrelated answer (Bitcoin `-26` "min relay fee not
      met, 429 < 1000" and "mempool min fee not met, 429 < 1000", Agave `-32005` "Node is behind
      by 429 slots");
    - a rate-limit phrase inside each of the five gRPC statuses the gRPC table checks first;
    - JSON-RPC code 429;
    - XRP's router-owned rows and its other engine results, with and without an
      `HTTP <status>: ` prefix;
    - each already-known answer (EVM "already known", Nethermind "AlreadyKnown", Besu "Known
      transaction") on EVM, Solana, Bitcoin, Starknet and Cosmos chains;
    - each slot-already-used signature as a complete code-and-message pair.
  - *Must take the router's verdict, even though a chain row matches the text:* a REST 403 and
    409; a gRPC Unavailable, ResourceExhausted, NotFound, InvalidArgument and Aborted; and a
    JSON-RPC reply that arrived as `HTTP 503: `.
  - A relay-policy test adds that a stateful write whose every answer is the min-fee rejection
    stops after its fan-out and holds no endpoint off.
- **Attack rows.** The two rows from the review of revision 2 must not change a verdict:
  `{contains: "block not found"}` → `CHAIN_BLOCK_NOT_FOUND` against a gRPC InvalidArgument, and
  `{code: -32000}` → `CHAIN_TX_REJECTED` against EVM's "already known".
- **Slot-already-used pairs.** Every signature in 3.3, Bitcoin's `-25` and `-26` included, sent as a
  complete code-and-message pair through the stateful path, must end unknown. The same pair on a
  read keeps its verdict. Every fixture whose verdict is `CHAIN_NONCE_TOO_LOW`,
  `CHAIN_INVALID_SEQUENCE` or `CHAIN_STARKNET_INVALID_NONCE` must match a signature.
- **Coverage check.** For every chain, the rows it gets after inheritance are compared with the
  rows it gets on lava-specs `main` before the PR. A chain that loses rows fails the job, and a
  chain that gains rows must be named in the PR as a behaviour change (5.2).
- **Generated row tests.** For every changed row, CI builds a node error from the row's `match`
  (the code, or a message containing the text) and asserts the row's `error` and `class` on the
  router version under test.

### 5.4 Bad-source and lock tests (router repo)

One test per failure. Each one asserts what the router loads, that the log names the cause, and
that the metric carries the reason:

- With no `--use-static-spec`, the router boots on its bundled specs.
- A source that isn't an archive, a ref that doesn't exist, a host that doesn't answer, and a spec
  that doesn't parse each boot on the bundled specs, with their own reason on
  `smartrouter_spec_source_fallback_total`.
- A row that breaks each rule in 3.2, 3.3 and 3.4, a row missing from the manifest, a manifest
  signed with the wrong key, and a section with an unknown schema each drop that chain's section,
  and the chain classifies with its bundled section.
- **After the switch**, with the family tables deleted: Solana's section dropped from the loaded
  source still classifies `-32005` "Node is unhealthy" as `NODE_SOLANA_UNHEALTHY`, from the bundled
  section.
- A spec that marks `eth_sendRawTransaction` with `stateful` 0 is loaded stateful, and
  `smartrouter_spec_lock_enforced_total{rule="stateful"}` counts it.
- A spec that leaves `eth_sendRawTransaction` out still relays it stateful and uncached, and
  `smartrouter_spec_lock_enforced_total{rule="stateful_default"}` counts it.

The ticket's "stale" and "unpinned" source tests are dropped with Must-have 1 (1.3).

### 5.5 Canary

Managed deployments take a new router release, and with it new bundled specs, on one canary router
per cluster first. The rest follow after a metrics window.

- **Window length.** The proposal is one full day of production traffic, so each chain's daily
  cycle is covered.
- **What it compares.** `smartrouter_errors_total` by `error_name` and `chain_id`, before and after
  (`protocol/metrics/error_metrics.go:42-44`). The dropped-section, source-fallback and
  lock-enforced counters must stay at zero.
- **On-prem** only gets the tested pair.
- **A pair that fails the canary is rolled back** to the previous release. The fix lands as a new
  lava-specs commit and a new release that bundles it.

### 5.6 Observability

| Signal | Where | Why |
|---|---|---|
| `smartrouter_spec_errors_section_dropped_total{chain_id, reason}` | router | a bad, unsigned or unreadable section shows up instead of failing silently |
| `smartrouter_spec_source_fallback_total{reason}` | router | a source the router couldn't load shows up, with the reason |
| `smartrouter_spec_lock_enforced_total{chain_id, rule}` | router | a spec that cleared `stateful` on a write (`stateful`), or left a write out (`stateful_default`), shows up |
| `smartrouter_spec_error_rows_mismatch_total{chain_id, kind, row_id, code_error, spec_error, scope}` | router, steps 1 and 2 | the switch criterion |
| `smartrouter_spec_error_rows_hits_total{chain_id, row_id}` | router | which rows actually matter; feeds the geth-wording decision in section 6 |

## 6. Open for R&D

- **The generic geth-wording rows: keep them generic, or move them to ETH1?** "execution
  reverted", "nonce too low", "already known", the hex-input phrases and the tx-pool texts
  (`error_classifier.go:269-346`, plus Erigon's "pending state is not supported" at `:241`) apply
  to every JSON-RPC chain today. We can keep them generic, or move them to an ETH1 section that
  BASE, ARBITRUM and the other EVM chains inherit. Decide once 30 days of the hits metric show
  where they fire outside EVM. Narrowing them is a behaviour change. The already-known rows stay
  router-owned either way (2.4).
- **The router's vendor rows: keep or remove?** Alchemy's `-32600 "unsupported method"` and the
  QuickNode `blocked … method|rpc` row are vendor wording, which R-0.2 rules out. We can keep them
  as router-owned transport rows, or remove them. Separately, the "pending state is not supported"
  row is Erigon's wording, and its comment, commit and fixture should say so.
- **Bitcoin `-27` and Starknet `59`: unknown or accepted?** Report "this transaction is already
  there" as unknown, as 3.3 proposes, or as accepted, in line with MAG-4059's ranking.
- **The write-method table and the source allowlist.** Section 3.4 seeds both, and MAG-4898
  completes them family by family.
- **The manifest and its key.** Where the signed manifests are published (committed by a follow-up
  CI job, or attached to the commit some other way), who holds the signing key, how it rotates
  given that the router embeds it, and how a signed row that turns out to be wrong is revoked.
- **Does the create-spec agent keep an unknown key when it rewrites a spec?** In the update,
  add-testnet and add-collection modes it does, by construction (A.4). New-chain mode needs a
  template and a reference page (MAG-4898 part 3). A test in the skill's `scripts/test_*.sh` should
  confirm it.
- **What happens to `chainFamilyMap` after the move?** It stays: the transport rows and the fault
  axis key on the family, and `IsSolanaFamily` (`protocol/common/svm.go:10-13`) drives slot
  handling from it. Around it:
  - the AVAXC and AVAXCT keys become AVALANCHEC and AVALANCHECT (EVM);
  - AVALANCHEP and AVALANCHEPT serve `platform.*` only and belong to no family;
  - the five disabled Cosmos bases the map files under Cosmos SDK (Appendix B) can leave it;
  - a lava-specs CI check fails a new spec whose index the map doesn't know.
- **Effort.** The ticket's first estimate is 6 to 8 engineer-weeks. Revision 2's first split came
  to about 9: Step 0 about 4 to 5 weeks, the reader and inheritance about 2, side-by-side and
  metrics about 1, and the switch with its post-switch tests about 1.5. Revision 3 takes the
  pinning, tag and source-metric work out of Step 0, and adds the manifest signing, the
  request-time stateful lock and the wider prefix, so R&D should resize it.

## 7. Later

- **Customer overrides**, as a small overlay file in the router config. They'd be read rows only,
  within the locked rules of section 3, checked by the same validator, with an approval path of
  their own in place of the CI signature, and logged and exported like a spec source. Never a fork
  of lava-specs.
- **Spec versioning (Must-have 1)**, if R&D picks it back up: lava-specs tags, a change log, a
  pinned spec source and a metric for the loaded source. Revision 2's section 5 has a full design.

## 8. Out of scope

- Must-have 1, spec versioning (1.3).
- How the router reads the section: parsing, caching and the matcher. R&D designs that against
  sections 2 and 3.
- The per-chain research and the agent change (MAG-4898).
- Ranking several node errors on a failed write (MAG-4059), and remembering accepted hashes
  (MAG-4748). Both are router behaviour, so they stay in code whatever this PRD decides.

## 9. Done when

- Nimrod or Tom approves Must-haves 2 to 4, the format and the split, and accepts the departures in
  1.3, Must-have 1's removal included.
- MAG-4910 is updated to record that Must-have 1 is out of scope.
- MAG-4898 is unblocked, with section 2 and Appendix B linked as the approved format and the first
  commit's coverage.
- The router work is ticketed: Step 0, Step 1 (reader, inheritance, side-by-side metrics), Step 2
  (the setting, the switch and the deletion), and the open items of section 6.

## Appendix A. How things work today

This is how things work right now, at the baseline commits. Every claim carries a file and line
reference.

### A.1 Classification is code

**How a row works.** Rows are hard-coded in `protocol/common/error_classifier.go`. Each one is a
matcher plus a pointer to a registered `*LavaError` (`protocol/common/error_registry.go:556-559`).
There are six matcher kinds (`error_registry.go:432-550`):

- `CodeEquals`
- `MessageContains` (case-insensitive)
- `MessageRegex` (case-sensitive unless `(?i)`)
- `CodeAndMessage`
- `HTTPStatusContains`
- `GRPCCodeEquals`

The XRP rows use a helper, `xrplMessageStartsWith` (`error_classifier.go:184-188`). It isn't a
seventh kind: it builds a `MessageRegex` anchored at the start of the message, after an optional
`HTTP <status>: ` prefix.

**The order rows are tried in.** `ClassifyError` goes top to bottom, and the first match wins
(`error_classifier.go:788-820`):

1. connection errors, decided before any row;
2. the chain-family rows;
3. the transport rows;
4. `UNKNOWN_ERROR`.

`UNKNOWN_ERROR` is retryable and may-have-reached-node, so an error nothing matches fails open
(`error_registry.go:589-596`).

**A chain row can shadow anything.** Because chain rows run first, a chain row can shadow any
transport row, the rate-limit and status rows included. The six tables rely on that on purpose:
Solana's `-32005` ("node is unhealthy", `:31`) shadows the generic `-32005` (`:222`), which is
EVM's "limit exceeded".

**Only six families have rows** (`error_classifier.go:24-167`):

- Solana: 14
- Bitcoin: 10
- Cosmos SDK: 4
- Starknet: 16
- NEAR: 4
- XRP: 15 (at `:108-166` since MAG-4817)

There is **no EVM family table**. Every geth, Erigon, Nethermind and Besu row sits in the generic
JSON-RPC table, so it applies to every JSON-RPC chain, Solana and Starknet included
(`error_classifier.go:269-280, 293-318, 339-346`, and `:241`). Aptos, Stellar, TON, Tron, Cardano
and Sui are mapped to families that have no rows, and EVM is mapped (48 keys) with no table at all.

**Most chains aren't in the family map.** `chainFamilyMap` (`error_registry.go:280-368`) maps 106
chain ids. lava-specs declares 306 chain indexes across 158 files, and 204 of them aren't in the
map, so they only get the transport rows. GNOSIS, LINEA, SCROLL, MONAD, KAVA, SEI, POLKADOT and
AVALANCHEC are a few of the ones left out. Four of the map's own keys (AVAXC, AVAXCT, AVAXP,
AVAXPT, `:288-289`) aren't lava-specs names at all; lava-specs calls them AVALANCHEC, AVALANCHECT,
AVALANCHEP and AVALANCHEPT.

**Transport rows.** JSON-RPC has 98, REST 68 and gRPC 32 (`error_classifier.go:201-627`). Those
counts include the rows the init step builds from the 19 `httpStatusRows`: a digit-matching row
per status on every table, plus a code row per status on REST.

**Three transport rows name a vendor:**

- Alchemy's `-32600 "unsupported method"` (`:208-212`, PR #632). That's a vendor's wording, which
  R-0.2 rules out.
- "Provider-disabled methods (e.g. QuickNode paid tier)", `blocked … \b(?:method|rpc)\b`
  (`:255-258`). It was written for a vendor's wording.
- "pending state is not supported" (`:234-241`, #667, MAG-5021). Its comment, commit and fixture
  credit Tatum, but the string is Erigon's own `errPendingStateNotSupported` (erigontech/erigon
  `rpc/jsonrpc/eth_call.go:61`), so it's really node-software wording.

**How a status reaches the classifier.** It depends on the interface:

- **REST:** the code is the HTTP status (`direct_rpc_relay.go:911`).
- **GraphQL:** classified on the REST table, with a status the GraphQL reader derives from the
  reply and its HTTP status (`graphqlMessage.go:490`, `direct_rpc_relay.go:1034`).
- **JSON-RPC:** the code is the body's JSON-RPC code, or the HTTP status when the body has none. A
  non-2xx reply's message gets an `HTTP <status>: ` prefix (`direct_rpc_relay.go:703-720`), and
  every direct-RPC HTTP error is worded the same way (`chainlib/node_error_handler.go:80`).
- **gRPC:** the code is the gRPC status.

One trap worth knowing about: `HTTPStatusContains` matches a status number anywhere in the message,
as long as non-digits sit on both sides of it (`error_registry.go:502-534`). So the "429" in
Bitcoin's "min relay fee not met, 429 < 1000" matches it.

**The gRPC table's order is pinned by a test.** It checks InvalidArgument, NotFound, OutOfRange,
Unimplemented and Aborted before the rate-limit phrases (`error_classifier.go:482-502`; phrases at
`:512-514`, Unavailable at `:517`), and `TestGRPCRateLimitTexts_TheCodeRowsAboveStillDecide`
(`grpc_rate_limit_vocabulary_test.go:57`) pins that order. The comment at `:518-538` explains why
DeadlineExceeded, PermissionDenied, Unauthenticated, ResourceExhausted, Canceled and DataLoss have
no row.

**What tests cover it:**

- `protocol/common/testdata/error_fixtures.json` holds 102 cases, replayed by `TestErrorFixtures`
  (`error_fixtures_test.go:56`). By chain: EVM 43, Solana 18, Starknet 14, Bitcoin 9, XRP 9,
  NEAR 4, Cosmos SDK 2, Aptos 2, Stellar 1. By transport: 97 JSON-RPC, 3 REST, 2 gRPC.
- `TestFaultAxis_EveryRegisteredCodeIsAccountedFor` (`error_fault_axis_test.go:203`) fails when a
  registered verdict has no fault decision.
- The status rules are tested elsewhere: `TestRESTStatusRows` and
  `TestRESTStatusRowsDoNotReachOtherTransports` (`protocol/common/rest_status_rows_test.go:15, 77`),
  `TestEndpointFault_AnswerShapes` (`protocol/rpcsmartrouter/endpoint_fault_test.go:35`) and
  `TestGRPCStatusError` (`protocol/lavasession/direct_rpc_connection_test.go:466`).

**The upshot: a new chain error needs a code release.** MAG-4824 (v1.6.7), MAG-4817 and MAG-5021
(both v1.6.8) are examples.

### A.2 How a write is protected today

**Stateful means one fan-out, then a stop.**

- The spec's `category.stateful` (`types/spec/api_collection.go:105`) sends a write to every
  provider at once.
- `Policy.Decide` then stops a stateful relay after that fan-out, whatever the verdicts, with one
  exception (`protocol/relaypolicy/policy.go:40-60`): every failure so far was a rate limit, the
  attempts completed, and the relay isn't a stateful batch with a node error.

**The verdict a row names decides that exception.** `onlyRateLimited`
(`protocol/relaycore/relay_processor.go:1520`, fed at `:669`) counts a node error as a rate limit
when its classification carries the rate-limit sub-category (`error_classifier.go:759`, set on the
result at `protocol/common/endpoints.go:698`). The same flag holds the endpoint off: a
rate-limited outcome records a 30-second hold-off that doubles on repeats
(`rpcsmartrouter_server.go:3241-3255`). **So the verdict a row names decides whether a write may
be sent again, and whether the providers that gave it get held off.** This is why 3.2 locks the
rate-limit verdicts.

**Unclear writes don't depend on chain rows.**

- The unclear-write decision comes from silence, and from the may-have-reached-node flag on
  protocol errors, which are classified with no chain family (`relaycore/results_manager.go:72`).
- A node error is a reply, and it passes through (`writeOutcomeIsUnknown` and
  `unclearWriteOutcomeFor`, `rpcsmartrouter_server.go:1447-1508`; the reply rule is at
  `:1490-1492`). The reply is an `*unclearWriteError` with four reasons (`:1421-1445`, #546).
- REST decides whether a 5xx body is the node's own reply with no chain family and an empty
  message (`ServerErrorIsNodeReply`,
  `protocol/chainlib/chainproxy/rpcInterfaceMessages/restMessage.go:74-78`, called at
  `rpcsmartrouter_server.go:6032`). That keeps a gateway's 502 or 504 on a write "unclear", and no
  chain row can reach it.

**But rows reach more than node replies.** A direct-RPC transport or status error gets its retry
and health verdicts with the chain family (`rpcsmartrouter/error_mapper.go:23-37, 71-83`, through
`chainlib/node_error_handler.go:119-126`).

**There's no slot-already-used group yet.** If another node already accepted the write, a
sibling's "already known" or "nonce too low" is released without a score
(`rpcsmartrouter_server.go:2688-2704`, applied at `:3227-3240`; the release reads only the
rate-limit flag). Otherwise each such answer keeps its own verdict, and the node's body is
forwarded.

**An undeclared method is a read.** For a method the spec doesn't declare, the parser builds a
`Default-` api with no `Stateful` and `Deterministic: true` (`base_chain_parser.go:532-560`,
reached from `:579`; REST at `rest.go:158`). It's relayed to a single provider, and can be cached.
Disabled apis are stored, so a disabled method is refused (`:582`) rather than falling through to
it (`:848-851`, MAG-4185).

### A.3 Specs already change behaviour, and load unpinned

**Specs aren't passive data.** These keys already drive router behaviour:

- `stateful`
- `timeout_ms` (`protocol/chainlib/common.go:737-738`)
- `deterministic` (`rpcsmartrouter_server.go:6345, 6542`)
- `hanging_api` (`:3343`, `recovery_probe.go:136`)
- `block_parsing`, `parsers` and `imports`
- a spec's `enabled`: a disabled spec serves no api (`base_chain_parser.go:798`)
- verification `severity` (MAG-3950, v1.6.8)
- request `headers`, now on every interface (#662)

**How imports work.** `imports` is resolved by `DoExpandSpec` (`types/spec/expand.go:20-103`, the
merge at `:72-100`). Only `api_collections` are inherited, and within a collection the child's
entry wins by name (`api_collection.go:580-582`, `combinable.go:37-67`). 144 of the 158 root files
import another.

**Unknown keys are dropped silently**, on both load paths (`utils/keeper/spec.go:23-26`,
`utils/specfetcher/fetcher.go:399-411`). An older router given a spec with a new section just
ignores it. The spec struct has no version field (`types/spec/spec.go:11-21`), and no spec carries
an `errors` key today.

**Nothing pins the source.** `--use-static-spec` (`cobra_common.go:79`) accepts a local file or
folder, or a GitHub or GitLab URL (`utils/specfetcher/fetcher.go:120-264`). From there:

- A URL with no ref fetches the default branch (`github.go:33-37`).
- Nothing checks that a ref is a tag or a commit.
- The resolved commit is never recorded, because the tarball's pax header that carries it is
  skipped (`tarball.go:73-75`).
- The boot log names the source (`statetracker/state_tracker.go:83-86`), but no metric exports the
  source, ref or commit. The only related gauge is `smartrouter_protocol_version`
  (`smartrouter_metrics_manager.go:428-431`).

Revision 3 leaves all of this as it is (1.3).

**The release image ships no specs.** `docker/Dockerfile.release:26` and `Dockerfile.ci:13` copy
only the binary, and the goreleaser archives are binary-only too (`.goreleaser.yaml:48-57`). The
dev image copies the repo's `specs/` folder (`docker/Dockerfile:30`), and `docker-compose.yml:53`
defaults to it. That folder holds 15 files; 14 of them also exist in lava-specs, and all 14 differ
from their lava-specs copies.

**No source, no boot.** The router refuses to start without a spec source
(`rpcsmartrouter.go:2569-2581`). The error message still names the retired `--static-spec-paths`.

**So every deployed router loads lava-specs `main`.** The Helm chart's default is
`staticSpecs.url: https://github.com/magma-devs/lava-specs` with no ref
(`smart-router-helm-chart/charts/smart-router/values.yaml:450-458`, passed through as-is by
`templates/routers_deployment.yaml:368-369`), and no tenant in smart-router-infrastructure
overrides it. A merged spec reaches each router the next time it restarts.

### A.4 lava-specs has no release discipline yet

- **No tags**, local or remote. The default branch is `main`.
- **No `CODEOWNERS` file**, now or anywhere in the history.
- **What runs on a PR.** `spec_guards.yml` runs on every PR: the guard scripts' self-tests,
  `check_node_admin_rpcs.sh`, `check_chain_identity.sh` and `check_disabled_count.sh`.
  `spec_pipeline.yml` runs the agent's phases 8 to 11 once, when a spec PR opens (hand-edited ones
  included), with the add-collection guard (#163). A commit pushed after that isn't re-checked by
  it.
- **`check_stateful.sh` has two blind spots.** It only runs inside the agent (its method-schema
  gate and Phase 10a, `SKILL.md:675, 696-698`), and:
  - It recognises a write by an exact-name list (`check_stateful.sh:97-119`: Substrate, EVM,
    Cosmos and Tendermint, Solana, Starknet), so it misses the other families' writes: Bitcoin's
    `sendrawtransaction`, `submitpackage`, `bb_sendTransaction` and `POST /tx/send`; NEAR's
    `send_tx`; XRP's `submit` and `submit_multisigned`; Sui's `ExecuteTransaction` and
    `executeTransaction`; and Concordium's `SendBlockItem` and `PUT /v0/submit…`.
  - It never evaluates a gRPC or Tendermint api. Those collections carry `"type": ""`, and the
    empty field collapses in its `read` at `:188`, so Cosmos `BroadcastTx` and `broadcast_tx_*`
    are never checked where they're declared.

  The good news is that no write is left non-stateful today. A corrected replica run over all
  10,311 api rows on `main` finds 132 writes, and every one of them is stateful.
- **Removed key names are rejected.** `check_unused_fields.sh` rejects fifteen removed key names at
  any depth (`:27`), including `title`, `description`, `local` and `subscription`, and the
  router's `bundled_specs_guard_test.go` does the same for `specs/`. It runs in `create_spec.yml`
  and in `spec_pipeline.yml`'s post-fix guard, but not in `spec_guards.yml`. The errors section
  must not use any of those names.
- **Diffs are positional.** `check_update_diff.sh` fails on any removed field, and it keys array
  elements by position, so inserting a row reads as a change to every row after it.
- **Updates don't rewrite files.** In its update modes the create-spec skill never re-emits a
  file: every write is a single `jq` assignment, so an existing errors section survives an update.
  New-chain mode builds from templates, so a new spec only gets the section once the templates
  describe it (MAG-4898 part 3).

## Appendix B. Inheritance coverage at lava-specs `bda05974`

Rows placed on a family root reach every spec that imports it, directly or through another spec.
Here's how that compares with `chainFamilyMap` (`error_registry.go:280-368`):

| Family | Root | Map keys | Specs that reach the root | Lose rows | Gain rows |
|---|---|---|---|---|---|
| Solana | SOLANA | 4 | 4 | 0 | 1 |
| Bitcoin | BTC | 8 | 17 | 0 | 10 |
| Cosmos SDK | COSMOSSDK | 28 | 47 | 5 | 25 |
| Starknet | STRK | 2 | 0 | 1 | 0 |
| NEAR | NEAR | 2 | 1 | 0 | 0 |
| XRP | XRP | 2 | 1 | 0 | 0 |

**Losses (6).**

- **STRKS**, which carries traffic. Step 1 adds `"imports": ["STRK"]`.
- **COSMOSSDK45DEP, COSMOSWASM, ETHERMINT, IBC and TENDERMINT**, which import nothing and are
  `enabled: false`, so they serve no api and lose nothing at run time.

**Gains (36), opted out in Step 1 and opened in Step 3.**

- Solana: SOLANAD.
- Bitcoin: BTCS, BTCT4, DASH, DASHT, PEP, PEPT, QTUM, QTUMT, ZCASH, ZCASHT.
- Cosmos SDK, not in the map: AKASH, AKASHT, BABYLON, BABYLONT, DYDX, DYDXT, ELYS, FETCHAI,
  FETCHAIT, INJECTIVE, INJECTIVET, KAVA, KAVAT, NEUTRON, NEUTRONT, SEI, SEIT, SIDET, STRIDE,
  STRIDET, TSC, TSCT.
- Cosmos SDK, filed under EVM in the map: AGR, AGRT, CANTO.

SOLANAD's gain covers the rows only. Slot handling follows `chainFamilyMap`, not the rows, so it
isn't Solana-family for slots either way.

**Map keys that aren't lava-specs indexes:** AVAXC, AVAXCT, AVAXP and AVAXPT (section 6).

## Appendix C. Revision history

### C.1 Where revision 2's sections went

Anna's review on PR #661 (661-1 to 661-8) and the review of revision 2 cite revision 2's numbers.
They map to revision 3 like this:

| Revision 2 | Revision 3 |
|---|---|
| Header: status, baseline, departures | Header, 1.3 |
| 1. Goal | 1.1 |
| 2.1 to 2.4. Today | Appendix A.1 to A.4 |
| 3. The split | 2.1 |
| 4.1 to 4.6. Format | 2.2 to 2.7 |
| 5. Must-have 1: spec versioning | Out of scope (1.3, 8); revision 3 keeps no part of it |
| 6. Must-have 2 | 4.1 to 4.4 |
| 7.1, 7.2, 7.3 | 3.1, 3.2, 3.3 |
| 7.4. A section that breaks a rule | 4.4 |
| 7.5, 7.6 | 3.4, 3.5 |
| 8.1 to 8.4. Tests | 5.2 to 5.5 |
| 9. Rollout | 5.1 |
| 10. Observability | 5.6 |
| 11, 12, 13, 14 | 6, 7, 8, 9 |
| Appendix A | Appendix B |
| 15. Changes in revision 2 | C.3 |

### C.2 Changes in revision 3

| Finding | Change | Where |
|---|---|---|
| Author's decision: Must-have 1 out of scope | Tags, the change log, source pinning and the source metric are removed. The release pair names a lava-specs commit, verdict changes are named in the PR, and the stale and unpinned source tests are dropped. Listed as a departure | 1.3, 4, 5.2, 5.4, 7, 8 |
| Review of revision 2, 1: a write the spec leaves out isn't stateful | The write-method table also applies at request time, to the `Default-` api | 3.4, 5.4, A.2 |
| Review of revision 2, 2: spec rows can shadow protected decisions | The already-known recognisers and every decided gRPC status join the locked prefix. The HTTP carve-out is listed as a departure, and the two attack rows join the gate | 1.3, 2.4, 3.2, 5.3 |
| Review of revision 2, 3: Nethermind's "transaction nonce is too low" | A "nonce is too low" signature, and every nonce and sequence fixture must be caught | 3.3, 5.3 |
| Review of revision 2, 4: vendor wording through a custom source | A row acts only if it's in a manifest lava-specs CI signed | 3.4, 4.4, 5.2, 5.4 |
| Review of revision 2: structure | Decisions first, then design, compatibility and rollout; the current state and the revision history move to appendices | whole document |
| 661-4 and 661-5 | Revision 2 closed these only in part (the status lock missed gRPC and already-known; the vendor lock had no run-time content check). The rows above finish them | 2.4, 3.4 |

### C.3 Changes in revision 2

Section numbers in this table are revision 2's; C.1 maps them.

| Finding | Change | Where |
|---|---|---|
| 661-1, XRP rows break the locked rules | Three XRP rows stay router-owned and get an explicit place in the order; the other twelve move as `regex` rows | 4.3, 4.5, 7.6 |
| 661-2, inheritance changes coverage | STRKS imports STRK, checked to leave its apis unchanged; the 36 gains are opted out in Step 1 and opened as behaviour bumps in Step 3; the mismatch metric counts `code_only` and `spec_only`; a coverage check gates every tag | 4.1, 4.4, 8.2, 9, Appendix A |
| 661-3, slot-already-used misses Bitcoin | The router decides the group from the full answer, whatever row matched, with Bitcoin `-25` and `-26` signatures; tests send complete pairs | 7.3, 8.2 |
| 661-4, status lock covers 19 HTTP rows only | `code` is checked per interface, gRPC included; message matchers are checked against status text; the locked statuses run ahead of spec rows at run time | 4.3, 7.2 |
| 661-5, stateful and vendor locks only in CI or review; unpinned source refuses to boot | The router enforces the write-method table and the source allowlist on every spec; an unpinned or broken source boots on the bundled tag and reports why | 5, 7.5, 8.3 |
| 661-6, the rate-limit prefix moves verdicts | The prefix holds only explicit rate-limit signals, keeps today's gRPC order, and leaves the digit-matching 429 row where it is; the counterexamples join the gate | 4.3, 8.2 |
| 661-7, factual errors | Fixed in `ad519324`; every "today" fact re-read at the new baseline | 2 |
| 661-8, format and rollout gaps | `disabled` and `slot_used` defined; one schema per tag; the order in each Step 2 window spelled out; the QuickNode row counted | 4.1, 4.2, 4.4, 9, 2.1 |
| Second review: gRPC precedence | The five gRPC statuses checked before the rate-limit phrases keep that precedence | 4.3 |
| Second review: message matchers | The status lock covers message matchers and is enforced at run time | 7.2, 4.3 |
| Second review: fallback contradicts the cutover | The fallback is the bundled tag's section before and after the switch, and 8.3 tests it after the switch | 6, 7.4, 8.3, 9 |
| Facts moved since revision 1 | New baseline `6c237073` / `bda05974`: XRP has 15 rows, 102 fixtures, 306 indexes, `check_stateful.sh`'s gaps, the third vendor-named row | 2, 4.5 |

## References

- Smart Router error rulebook v1.0, R&D review (Notion `40cf5ce8bcd38108bcf3fd93f85e8af5`):
  - R-M: rows keyed by chain, node software and interface; no vendor layer;
  - R-L: writes;
  - R-K: all-fail ranking;
  - R-0.2: no vendor wording;
  - Q45: slot already used;
  - and the change log.
- `docs/ERROR-REGISTRY-DESIGN.md`: the registered verdicts the rows point at.
- Jira:
  - MAG-4910: this PRD's ticket;
  - MAG-4898: the per-chain research this blocks;
  - MAG-4059: write error ranking;
  - MAG-4748: accepted hashes;
  - MAG-4824, MAG-4817 and MAG-5021: classification changes that needed a code release;
  - MAG-4061: chain answers inside a REST 500;
  - MAG-4185: disabled apis refused instead of served as a default read.
- lava-specs: `.github/workflows/spec_guards.yml`, `spec_pipeline.yml`, `create_spec.yml`;
  `.claude/skills/create-spec/scripts/check_stateful.sh` (`:97-119`, `:188`),
  `check_unused_fields.sh`, `check_update_diff.sh`.
- smart-router-helm-chart `charts/smart-router/values.yaml:450-458` (chart `main` `2446606`): the
  unpinned default.
- erigontech/erigon `rpc/jsonrpc/eth_call.go:61`: `errPendingStateNotSupported`.
