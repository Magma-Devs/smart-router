# Per-chain error classification in lava-specs (MAG-4910, revision 9)

Implementation spec. Approval: Nimrod or Tom, on Must-haves 2 to 4, the format and the split;
product (Omer) on each row's write verdict (Q229). Blocks MAG-4898. Baseline for every file:line:
smart-router `6c237073`, lava-specs `bda05974`, smart-router-helm-chart `2446606`,
smart-router-infrastructure `99f0dbb4`. Rulebook: Part I v1.0, frozen 2 Nov 2026, with its change
log to 16 Nov, the Reference page, and Parts II to V v1.0 final (References); where this spec and
the rulebook differ, the rulebook wins. The full reasoning and revision history are in this file's
git history (`861d71a2`).

**Decisions that depart from the ticket** (MAG-4910 updated 17 Nov 2026):

- Must-have 1 (spec versioning: tags, change log, source pinning, source metric) is out of scope.
  Routers load whatever source they're given; a release is tied only to its bundled commit.
- Status rules are the rulebook's (Part I §1; here §3.2): the status alone decides HTTP 401, 402,
  403 and 429 and gRPC PERMISSION_DENIED and UNAUTHENTICATED, and gRPC RESOURCE_EXHAUSTED by its
  own rule; any other well-formed error is decided by its row, a chain's row included, whatever the
  status; only a reply no row matches falls to the status. The ticket locked every status rule.
- The chain's list is the rulebook's (Q229; Part II §7, Part III §8, Part IV §8, Part V §7): each
  row carries a class and reason and a write verdict product approves; rows may read an answer
  (success and not-found rows); a chain may give `C4` (its quota rows) and name
  `CHAIN_TX_ALREADY_KNOWN` (its duplicate rows), which the ticket forbade; the spec also says where
  the chain's errors sit (shapes) and when its write rows ship.
- A row may name a verdict the router doesn't register, giving its class and reason (§2.3), so a
  new chain error needs no router release; the ticket's rows map to registered errors. The node
  versions checked live in the evidence (§4.4), not in the row.

**Out of scope:** how the router parses, caches and matches (R&D's design); the per-chain research
(MAG-4898); remembering accepted hashes (MAG-4748); gRPC streams; subscriptions beyond counting
their errors (§5.7); the router's own replies (R-C); the chain-list items that aren't
classification (§2.6).

## 1. Model and invariants

A **row** maps a node software's documented answer (never a vendor's, rulebook R-0.2) to a
**verdict**, named in `error`: a router-owned verdict, which keeps the router's class and flags, or
the chain's own, which the row gives a **class** and **reason** (§2.3); the router derives the
flags that decide retry and blame from them. On a write copy a row also gives the copy's **write
verdict** (§5.2). Most rows match the **error record** the router builds from a failed or refused
reply (§3.1); **answer rows** match a 2xx answer (success and not-found rows). A **shape** says
where a chain's errors sit in its replies (§2.4). Rows and shapes live in the chain's spec and
inherit through its `imports` (§2.5). The router keeps: the record and each interface's standard
reading; the rulebook's order (§3.2); the flags, what each does (retry, fault, scoring, hold-off,
caching) and which ones each class sets; connection rules; the unknown default; write safety (§5);
and the **router-owned verdicts**: every verdict a transport row points at, the 25 the router's
logic references by identity (`CHAIN_TX_ALREADY_KNOWN`, `CHAIN_BLOCK_NOT_FOUND`,
`CHAIN_SOLANA_MISSING_LONG_TERM`, `NODE_RATE_LIMITED`, `NODE_GATEWAY_TIMEOUT`, `NODE_BAD_GATEWAY`,
`NODE_ENDPOINT_NOT_FOUND`, 16 protocol, 2 user), all protocol, connection and status verdicts, and
`UNKNOWN_ERROR`.

Invariants the router enforces at run time, for every spec from every source:

1. A write goes out once, one copy per provider, and is never sent again: no retry, no hedge, no
   re-send after a 429 (R-L). Main still re-sends a write whose every copy was rate limited, except
   on GraphQL (MAG-5122); MAG-5232 removes that, and until it ships a spec's `C4` row acts on reads
   only.
2. No spec row runs ahead of the router's connection and status rules (§3.2 steps 1 and 3), with
   the two exceptions the rulebook sets: on a write copy, the chain's success and duplicate rows
   come first (step 2); on gRPC RESOURCE_EXHAUSTED, the chain's over-limit row beats the status
   (step 3).
3. A row can't name a `PROTOCOL_*`, rate-limit, `C6` or `UNKNOWN_ERROR` verdict, or
   `CHAIN_TX_ALREADY_KNOWN` except on a duplicate row. A row naming another router-owned verdict has
   no `class` or `reason`: the verdict keeps the router's, so no spec changes a verdict the router
   owns.
4. A write copy's verdict follows R-L (§5.2). A refusal the status decides counts on every chain
   from day one; a verdict read from a body counts only through the chain's own rows, once its write
   rows ship (`write_rows`, §2.1). Until then a node's 2xx body in the protocol's shape, a bare REST
   `C2` status and gRPC INVALID_ARGUMENT go back as the node sent them, with no
   `Lava-Write-Outcome`; anything else that may have run gets the router's unknown reply. A
   slot-already-used answer is never rejected.
5. A write method a spec declares is stateful whether the spec marks it or clears it. A write the
   spec omits is the rulebook's accepted risk: handled as a read (Part I §1, "Unknown methods").
6. A chain's effective policy acts only if it matches a signed approval covering its rows, shapes,
   markers, their order and its imports.
7. An invalid policy falls back whole to the bundled policy, unrelated chains keep theirs, the
   router boots, and a gauge, log line and alert name the chain and reason.
8. No row or shape matches a vendor's own code, wording or field: they hold the node software's
   documented codes, messages and fields (R-0.2, §4.4).
9. Classification never changes a node's reply. When the router returns one, the client gets it
   with only R-A's edits (the caller's id restored, our credentials masked); on gRPC the same
   status, message, details and listed trailers, and a proto-equal message (R-A, Q144). Rows decide
   how the router treats a reply: retry, endpoint health, metrics, which node's reply the client
   gets (R-K) and a write's outcome. The router answers in a node's place only where R-C's closed
   list says, an unknown write included (R-L); on main today, where no node answered or a write is
   unclear (MAG-4218, `withUnclearWriteStatus`).

## 2. Spec format

### 2.1 The `errors` section

```json
{ "index": "COSMOSHUB", "imports": ["COSMOSSDK"], "api_collections": [ "..." ],
  "errors": { "schema_version": 1, "shapes": [ "..." ], "rows": [ "..." ], "write_rows": ["rest"] } }
```

- `schema_version`: integer, identical in every section at a lava-specs commit (CI enforces). A
  router drops a section with an unknown version whole; the chain and its importers fall back.
- `rows`: the chain's own rows (§2.2); they run before the rows it inherits (§2.5).
- `shapes`: where the chain's errors sit (§2.4).
- `write_rows`: the interfaces whose write rows have shipped (R-L, "When R-L applies"). Product
  approves it (Q229); it acts only at stage `all` (§7) and only on a router that reads that
  interface's write replies (gRPC: MAG-5128). It switches on a body's write verdicts (§5.2),
  `Lava-Write-Outcome` on 2xx bodies (MAG-5232) and the chain's unknown-write alert (R-L). Not
  inherited; migrated rows never set it.
- `plain_text`: REST methods whose 2xx plain-text body is an answer (Part V §3.1, R-D6; MAG-3971).
- `inherit`: `false` inherits no rows (Step 1's opt-outs, Appendix B). The parents' shapes and
  `plain_text` still apply, so an opted-out chain reads its replies as before; a child drops a
  shape only by disabling its `id`. Doesn't affect api inheritance.
- Defaults, as if written: `rows`, `shapes`, `write_rows`, `plain_text` `[]`; `inherit` `true`; per
  row, `scope` `"both"`, `field` `"text"`, `reason` `"invalid"` on a `C2` row, and `write`
  `"may_have_run"` on a row whose `scope` isn't `read` (a `read` row has no `write`). A section
  without rows is valid: `{"schema_version": 1, "inherit": false}` leaves the chain no spec rows.
- The section is optional; older routers ignore the key, and a spec with no section keeps today's
  behaviour (Q229). A missing section means no local rows; the chain may still inherit. The approval
  covers the complete effective result, empty included.

### 2.2 Row

```json
{ "id": "solana.blockhash_not_found", "interface": "jsonrpc", "contains": "blockhash not found",
  "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "class": "C2",
  "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/sdk/src/transaction/error.rs" }
```

| Field | Req. | Rule |
|---|---|---|
| `id` | yes | Unique within the chain after inheritance; `<family>.<snake_case>`. A child replaces a parent's row by repeating its `id`. |
| `interface` | yes | `jsonrpc`, `tendermintrpc`, `rest`, `graphql` or `grpc`, or a list of them, each one the declaring spec serves: a row written for one interface never reads another's replies (R-B; MAG-5253). |
| `methods` | no | Api identities (§5.3: verb and route on REST, service/method on gRPC, root field on GraphQL); absent = every api. Required on answer rows, and on a row whose node software documents the reply for some methods only (Stellar `POST /transactions_async`). |
| `scope` | no | `read`, `write` or `both` (default): `write` rows run only for stateful apis, `read` rows only for the rest. |
| `answer` | no | `true`: the row reads the reply's answer (§3.1) instead of an error record (below): a 2xx (gRPC OK) reply's, and, for a success row with `at`, a write copy's body under any status (§3.2 step 2; R-L). |
| `status` | match | The reply's status (§3.1): an HTTP status, or a gRPC status as the rulebook writes it (`"NOT_FOUND"`); never one the status decides (§4.3). |
| `code` | match | The record's code (§3.1) or, with `at`, the value there; number, string, boolean or null, matched by JSON type and value: `-32005`, `"sdk/32"`, `"tx_bad_seq"`. |
| `contains` | match | Case-insensitive substring, as `MessageContains`. |
| `regex` | match | Go RE2, case-sensitive unless `(?i)`; must compile. Not with `contains`. |
| `field` | no | What `contains` or `regex` reads: the record's `"text"` (default) or `"detail"` (§3.1); only with one of them. A string is read as its value; an object, array, number or boolean as its compact JSON text; a null or absent `detail` matches nothing. |
| `path` | no | GraphQL only: the error's path: `false` (none), `true` (any), or a root field name (a path that starts there). |
| `at` | no | Answer rows only: a JSON pointer into the answer (on gRPC, into the decoded message); `code` matches the value there. |
| `present` | match | Answer rows only, with `at`: `true` when the pointer resolves to a value that isn't null, `false` otherwise. |
| `error` | error rows | The verdict's name: a router-owned verdict a row may name (§4.2), e.g. `CHAIN_STATE_PRUNED`, or the chain's own, `CHAIN_<FAMILY>_*` / `NODE_<FAMILY>_*`; migrated verdicts keep their names. It appears in `smartrouter_errors_total{error_name}`, logs and the `relay_end` event, which key on names (`relay_end.go:598-603`); a chain's own verdict has no number. Clients never see it (invariant 9). |
| `class` | own verdict | `C2`, `C3`, `C4` or `C5` (§2.3); absent when `error` is router-owned. One class and reason per name across the chain's effective rows. |
| `reason` | `C3` | `C2`: `invalid` (default) or `over_limit`; `C3`: `not_found`, `pruned`, `behind`, `limit`, `version` or `method_off`; none otherwise (§2.3). |
| `write` | no | On a write copy: `took_it`, `refused_it` or `may_have_run` (default), as R-L's copy verdicts (§5.2). Product approves it (Q229). |
| `source` | yes | The node software's official definition, pinned to a tag or commit, on the router's allowlist (§4.4). |
| `disabled` | no | `true` removes the inherited row with this `id` for this spec and its children; carries only `id`, `disabled`, optional `note`. |
| `note` | no | One line. (`description` and `title` are rejected key names in lava-specs.) |

Every match key a row has must hold. An error row has at least one of `code`, `contains`, `regex`;
a row with `interface: "grpc"` only, or one for a bare HTTP 409 (Part I §1), may have `status`
alone. An answer row has `methods`, and either `at` with `code` or `present`, or nothing more (any
answer on those methods); it gives either `write: "took_it"` and nothing else (a **success row**,
R-L's "took it"; one without `at` reads a 2xx only) or `error`, `class: "C3"` and
`reason: "not_found"` (a **not-found row** for a null answer, R-F). No header matcher.

On GraphQL, `methods`, `path` and an answer row's `at` name fields, never aliases. The router reads
each occurrence of a root field in the request separately (R-O), with every alias in its selection
replaced by the field's name: `{ tx: transaction(digest: …) }` answering `{"data": {"tx": null}}`
is read as `/data/transaction` = null, and an error with path `["tx"]` as path `["transaction"]`.

### 2.3 Class

The rulebook's classes (Part I's class table): every provider reply gets exactly one, decided once.
A row gives the chain's own verdict `C2`, `C3`, `C4` or `C5`, with a reason where the class takes
one; the router assigns the rest.

| Class | Meaning (rulebook) | In a spec row | Router flags today |
|---|---|---|---|
| `C1` | Answer: the node answered, with data or with an empty or null result | a success row (`write: took_it`) | none |
| `C2` | Caller's answer: the request itself is wrong, and every node would say the same. Reason `invalid` (invalid params, reverted, insufficient funds, nonce too low, which on a write may have run: §5.2) or `over_limit` (a size, depth, range or result limit the chain software enforces on every node) | `class`, `reason` | `none`, not retryable: not at fault |
| `C3` | Cannot serve here, another node may. Reason `not_found` (an explicit "not found", or a null a not-found row marks: at most one extra provider, R-F), `pruned`, `behind` (behind the block or slot asked for), `limit` (an operator limit that differs per provider), `version` (a server version gap) or `method_off` (a method or feature off) | `class`, `reason` | `data_scope` (`not_found`, `pruned`, `behind`) or `node_capability` (`limit`, `version`, `method_off`), retryable: not at fault |
| `C4` | Throttled: our traffic is limited for a time: HTTP 429 whatever the body, or a rate-limit or quota answer | `class` on the chain's quota rows | `rate_limit`: the URL is held off |
| `C5` | Provider fault: the provider failed: a 5xx (not 501), a 408, a timeout, a dropped connection, a malformed reply, or an "internal error", "unhealthy" or "syncing" answer | `class` | `none`, retryable: at fault |
| `C6` | Provider refuses us: HTTP 401, 402 or 403, whatever the body (gRPC PERMISSION_DENIED, UNAUTHENTICATED) | never: the status decides | `NODE_UNAUTHORIZED`, `NODE_ACCESS_DENIED`: `node_capability`, retryable |
| Unknown (not a class) | An error no rule recognises | never | `UNKNOWN_ERROR`, the router's default: retryable |

Also not classes: a composite reply (a batch, a partial GraphQL reply) is classified per element
(R-O); a stale answer is a `C1` (freshness is the consistency gate's). A `C3` reason sets today's
flag because the router treats the two flags differently (`relay_processor.go:634`,
`rpcsmartrouter_server.go:7202`). The last column is the router at `6c237073`, not the rulebook's
targets: R-F's one extra provider for `not_found` (MAG-5279); pauses and probes (R-H, R-I);
`caller_error` and `over_limit` counting (R-D); an unknown not scored (R-G, MAG-5248 item 3). A
router-owned verdict's class and reason are the router's, exported to the validator (§4.1); main
gives `CHAIN_SOLANA_MISSING_LONG_TERM` (`-32009`) the caller's flags where the rulebook says `C3`
`not_found` (R-F; MAG-5279). Fault as `EndpointAtFault` (`error_registry.go:136-152`); retry as §3.3
says. A chain's own verdict is `CategoryExternal`; the validator prints each row's fault decision.

### 2.4 Shapes

A shape says where a chain's errors sit in its replies when its node software doesn't use the
interface's standard place (§3.1): the chain list's "where the error sits" and REST error shape
(Part I §1, Part IV §8, Part V §7).

```json
"shapes": [
  { "id": "cosmos.rest_error", "interface": "rest", "code": "/code", "message": "/message",
    "source": "https://github.com/cosmos/cosmos-sdk/blob/v0.50.10/..." },
  { "id": "cosmos.broadcast", "interface": "rest", "methods": ["POST /cosmos/tx/v1beta1/txs"],
    "when": { "at": "/tx_response/code", "not": 0 },
    "code": "/tx_response/code", "space": "/tx_response/codespace", "message": "/tx_response/raw_log",
    "source": "https://github.com/cosmos/cosmos-sdk/blob/v0.50.10/..." }
]
```

| Field | Req. | Rule |
|---|---|---|
| `id` | yes | As a row's; a child replaces or disables an inherited shape by `id`. |
| `interface` | yes | One interface the declaring spec serves. |
| `methods` | no | As a row's; absent = every api. A body check applies only to the methods it was written for (R-B). |
| `code` | yes | JSON pointer to the chain's code. With `space`, another pointer, the code is `"<space>/<code>"` (Cosmos `"sdk/32"`). |
| `message` | no | JSON pointer to the message; absent = the interface's standard text (§3.1). |
| `when` | no | For a 2xx (gRPC OK) reply: `at`, a pointer, and one check, `equals` or `not`, a JSON value. When it holds, the reply is an error inside a success (`in_success`). Absent: the shape reads only replies that are already errors (a non-2xx body, a JSON-RPC error object). |
| `source`, `disabled`, `note` | | As a row's. |

- A non-2xx REST body in a declared shape is a well-formed error; any other non-2xx body is bare
  (Part V §3.1).
- Pointers are RFC 6901. One that doesn't resolve (a missing key, an index out of range, a step
  into a value that isn't an object or array) or that resolves to null is missing. `when` holds
  only if its pointer resolves: `equals` when the value matches by JSON type and value, `not` when
  it doesn't. A shape applies only if `when` holds (where given) and `code`, and `space` where
  given, resolve to a string or an integer; a `message` that doesn't resolve to a string leaves the
  interface's standard text.
- The first shape that applies wins, in inheritance order; the record takes its code and message.
  A shape that doesn't apply falls through to the next; when none applies, the interface's standard
  reading decides (§3.1).
- Today's chain readers become shapes in Step 1: the Cosmos `tx_response` check in `cosmossdk.json`,
  on the broadcast path only (MAG-5254 item 9); Tendermint's broadcast checks in `tendermint.json`,
  `result.code` ≠ 0 on `broadcast_tx_sync` and `broadcast_tx_async` and `result.check_tx.code` ≠ 0
  on `broadcast_tx_commit` (MAG-5248 item 10), which every spec with a tendermintrpc collection
  imports. NEAR's `error.cause.name` becomes a shape in `near.json` with MAG-4253. XRPL's result
  fields stay router code with its rows (§5.5).

### 2.5 Inheritance

New code in `DoExpandSpec` (today merges only `api_collections`, `expand.go:72-100`; 144 of 158
root files import another).

- Order: the child's rows, then each parent's rows the child didn't replace or disable by `id`, in
  the parents' order; shapes the same way. `plain_text` adds the parents' methods. `write_rows`
  isn't inherited: each chain sets its own. `"inherit": false` drops the parents' rows only.
- Resolve and validate the whole graph before any policy acts. Errors: missing parents, cycles,
  duplicate local ids, mixed schemas, incompatible definitions of one inherited id (the child must
  replace or disable it explicitly), a disabling entry naming no inherited id, one name with two
  classes or reasons in the chain's effective rows. A diamond yields one copy at first occurrence.
- First commit: STRKS imports nothing today and would lose the 16 Starknet rows → add
  `"imports": ["STRK"]` (expanded apis stay byte-identical, checked with
  `ExpandSpecWithDependencies`, `utils/keeper/spec.go:89`). 36 chains would gain rows (Appendix B)
  → `"errors": {"schema_version": 1, "inherit": false}` in Step 1, opened one by one in Step 3.
  The 25 of them that serve the Cosmos broadcast route and Tendermint RPC keep the
  `cosmossdk.json` and `tendermint.json` shapes they read today.

### 2.6 What the chain's list holds, and where

The rulebook's chain lists (Part II §7, Part III §8, Part IV §8, Part V §7) hold more than
classification:

| Item | Home |
|---|---|
| Error-meaning, refusal, may-have-run, not-submitted, over-limit, quota, not-found and behind rows | rows (§2.2) |
| Success and duplicate rows | rows with `write: took_it` (§5.2) |
| Not-found null rows | answer rows (`C3`, `not_found`) |
| Where the error sits; the methods each body check covers | shapes (§2.4); XRPL in router code (§5.5) |
| Write methods | the api's `stateful`, kept by the router's table (§5.3) |
| When the write rows ship | `write_rows` (§2.1) |
| Plain-text endpoints (REST) | `plain_text` (§2.1) |
| Supported node-software versions | the evidence (§4.4) |
| Write timeout | the api's `timeout_ms` |
| Reply headers to pass through | the spec's header rules (`kind: pass_reply`) |
| Methods, fields and empty shapes never cached | the cache tasks (MAG-5123 GraphQL, MAG-5249 JSON-RPC); per-method empty shapes in a later `schema_version` (§10) |
| Transaction-hash formula with a test value; signed-bytes encoding; the unknown-reply sentence | router code per node software (MAG-5232; MAG-5257 REST, MAG-5128 gRPC, MAG-4909 Sui) |

### 2.7 Examples

**Solana** (`solana.json`; 14 rows today at `error_classifier.go:25-45`, four shown, migrated
unchanged). `CHAIN_STATE_PRUNED` is router-owned (used by `:354-355`, `:413`, `:444`), so its row
has no `class`.

```json
"errors": {
  "schema_version": 1,
  "rows": [
    { "id": "solana.block_cleaned_up", "interface": "jsonrpc", "code": -32001, "error": "CHAIN_STATE_PRUNED",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs" },
    { "id": "solana.node_unhealthy", "interface": "jsonrpc", "code": -32005, "error": "NODE_SOLANA_UNHEALTHY", "class": "C5",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs" },
    { "id": "solana.min_context_slot", "interface": "jsonrpc", "code": -32016, "error": "CHAIN_SOLANA_MIN_CONTEXT_SLOT_NOT_REACHED", "class": "C3", "reason": "behind",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs" },
    { "id": "solana.blockhash_not_found", "interface": "jsonrpc", "contains": "blockhash not found", "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "class": "C2",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/sdk/src/transaction/error.rs" }
  ]
}
```

**A child changes a class** (a child of `solana.json`, for this chain and its children only, once
the switch deletes the code tables, §7; the validator reports "node at fault, retry elsewhere").
Every inherited row naming the same verdict is replaced too, or the chain gives one name two
classes (§4.1):

```json
"errors": { "schema_version": 1, "rows": [
  { "id": "solana.blockhash_not_found", "interface": "jsonrpc", "contains": "blockhash not found", "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "class": "C5",
    "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/sdk/src/transaction/error.rs",
    "note": "this cluster's RPC nodes lag; another node may know the blockhash" } ] }
```

**A chain's own verdict, with its REST shape** (TON):

```json
"errors": { "schema_version": 1,
  "shapes": [ { "id": "ton.error", "interface": "rest", "code": "/code", "message": "/error",
    "source": "https://github.com/ton-blockchain/ton/blob/v2024.10/..." } ],
  "rows": [ { "id": "ton.block_not_found", "interface": "rest", "scope": "read", "status": 500, "contains": "block not found",
    "error": "CHAIN_TON_BLOCK_NOT_FOUND", "class": "C3", "reason": "not_found",
    "source": "https://github.com/ton-blockchain/ton/blob/v2024.10/..." } ] }
```

**Write rows** (Cosmos broadcast over REST, with the shape above; product approves each `write`):

```json
"rows": [
  { "id": "cosmos.broadcast_ok", "interface": "rest", "methods": ["POST /cosmos/tx/v1beta1/txs"], "answer": true,
    "at": "/tx_response/code", "code": 0, "write": "took_it", "source": "..." },
  { "id": "cosmos.tx_in_mempool", "interface": "rest", "methods": ["POST /cosmos/tx/v1beta1/txs"], "code": "sdk/19",
    "error": "CHAIN_TX_ALREADY_KNOWN", "write": "took_it", "source": "..." },
  { "id": "cosmos.insufficient_funds", "interface": "rest", "methods": ["POST /cosmos/tx/v1beta1/txs"], "code": "sdk/5",
    "error": "CHAIN_COSMOS_INSUFFICIENT_FUNDS", "class": "C2", "write": "refused_it", "source": "..." }
]
```

A "not submitted" answer (Stellar Horizon 503 `TRY_AGAIN_LATER`, documented for
`POST /transactions_async` only, read from `tx_status` by a Stellar shape) is a row with
`"class": "C5", "write": "refused_it"`: the copy is refused and the class stays. A not-found row
(Sui GraphQL, `transaction(digest)` answering null; Part II D14) is
`{ "id": "sui.tx_not_found", "interface": "graphql", "methods": ["transaction"], "answer": true,
"at": "/data/transaction", "code": null, "error": "CHAIN_SUI_TX_NOT_FOUND", "class": "C3",
"reason": "not_found", "source": "..." }`.

**Import and disable:**

```json
{ "index": "STRKS", "imports": ["STRK"], "api_collections": [ "..." ] }
```
```json
"errors": { "schema_version": 1, "rows": [ { "id": "cosmossdk.account_not_found", "disabled": true } ] }
```

XRP: eleven of its 15 rows move to `ripple.json` as `regex` rows, without the optional
`HTTP <n>: ` group (`xrplMessageStartsWith`, `error_classifier.go:187`; the record's text has no
prefix); all name router-owned verdicts, so none has a `class`. Four stay in code (§5.5).

## 3. Classification at run time

### 3.1 Error record

The router builds one record per failed or refused reply, per batch element (R-O; MAG-5248 item 8,
XRP MAG-5049) and per entry of a GraphQL `errors` array (Part II §3.4; MAG-5121), on every
interface. Every reader of the reply uses that record: the senders, the results manager, the cache
tiers and the subscription managers (Appendix A). Spec rows match only the record, or an answer
(answer rows). Router rows keep the inputs they read today.

| Field | Content |
|---|---|
| `interface` | The api's interface, one of five (the classifier gets three transports today, `error_classifier.go:768-777`). |
| `status` | The HTTP status as received, on GraphQL too (extension codes never make a status, Part II §3.1); the gRPC status; 0 on a WebSocket frame. A gRPC status carrying grpc-go's "unexpected HTTP status code received from server: NNN" has HTTP status NNN (Part III D12). |
| `code` | The chain's code, number or string: JSON-RPC and Tendermint `error.code`; a GraphQL error's `extensions.code` when it is a string (a numeric one counts as no code); a shape's (§2.4). |
| `text` | `error.message`; on REST the shape's message, else the body's `message`, `error` or `error.message`, else the body (`restMessage.go:99-127`); a GraphQL error's `message`; the gRPC status message. |
| `detail` | JSON as received: JSON-RPC and Tendermint `error.data`; a GraphQL error's `extensions`; on gRPC the status details as an array, each rendered with protojson (proto field names) and compacted, from types linked into the router (`google.rpc.*`) or its bundled descriptors, never a provider's reflection, else `{"@type": "<type URL>"}`. Absent when the reply has none. |
| `path` | GraphQL: the error's path; absent when none. |
| `in_success` | A shape found the error inside a 2xx or gRPC OK reply. |

A well-formed error is a JSON-RPC or Tendermint error object with an integer code, a non-empty
GraphQL `errors` array, a gRPC status other than OK, or a REST body in the chain's shape (§2.4);
anything else is bare. A 2xx in the protocol's shape is an answer: on REST any valid JSON value
(Part V R-D6), or plain text on a `plain_text` method; on GraphQL anything but `{}`,
`{"data": null}` without errors, or `errors: []` without data (Part II P4: `C5`); on gRPC an OK
whose message decodes as the method's type, read with bundled definitions (MAG-5128). A Tendermint
`abci_query` whose `result.response.code` ≠ 0 is an answer that is never cached (MAG-5248 item 10).
A cached node error is never served, and on GraphQL a reply with errors is never cached (R-F;
MAG-5249, MAG-5123): the cache tiers use the record only to refuse such entries. A shape that finds
an error inside a success makes the reply a node error for the router's decisions; the client
still gets the reply as is (invariant 9).

Main differs: it reads Cosmos `tx_response` on every REST 2xx of every chain
(`restMessage.go:55-60`; MAG-5254 item 9) and `abci_query`'s `result.response` as an error on every
method (`tendermintRPCMessage.go:39-51`; MAG-5248 item 10); it reads one GraphQL error per reply,
and on a query only six fault codes (`graphqlMessage.go:462-481`; MAG-5121); it reads NEAR's code
from a message that is always "Server error" (MAG-4253); it drops gRPC details (MAG-5129); and it
serves cached node errors (`rpcsmartrouter_server.go:2211-2212`; MAG-5249).

### 3.2 Matching order (rulebook Part I §1; Parts II to V §3)

For a reply on chain X, first match wins:

1. **No reply.** A connection error or the router's own deadline: no reply, `C5` on a read (R-C).
   The caller's own deadline (gRPC `grpc-timeout`) or a caller gone: counted only, no blame, no
   retry (Part III D10). A connection dropped during the call: no reply, never matched against rows
   (MAG-5130).
2. **On a write copy, once the chain's write rows ship:** the chain's success and duplicate rows
   (`write: took_it`) → took it, whatever the status (Part II P1, Part IV P2, Part V P2; R-L); a
   success row without `at` reads a 2xx only (§2.2). A plain success beats a duplicate.
3. **The status decides** (router code): HTTP 429 → `C4`; HTTP 401, 402, 403 and gRPC
   UNAUTHENTICATED and PERMISSION_DENIED → `C6`, whatever the body. gRPC RESOURCE_EXHAUSTED: the
   chain's over-limit row, or grpc-go's "received message larger than max" sent by the node → `C2`
   `over_limit`; else a rate-limit or quota row, or `grpc-retry-pushback-ms` or `retry-after`
   metadata → `C4`; else the same text from the router's own receive limit → no reply (`C5` on a
   read, may have run on a write); else unknown.
4. **A well-formed error, through rows, whatever the status** (the record, §3.1):
   1. The router-owned XRP rows that come first (§5.5).
   2. X's spec rows, resolved by inheritance, in file order, filtered by interface, method and
      scope.
   3. The router's rows: the XRP catch-all (§5.5) and, until the switch deletes them, the family
      rows; then the interface rows: on JSON-RPC and Tendermint the generic code rows (the
      `-32600`/`-32700` rule for a request the router parsed with `jsonrpc`, `method` and an id, a
      request without `jsonrpc` being `C2`; the generic `-32005`, MAG-5248 item 4); on GraphQL
      Part II §3.2's default rows; on REST, none once MAG-5253 moves its node-software text rows
      into chain rows; on gRPC, none (its table is step 6).

   A well-formed error no row matches goes to step 6, except under a 2xx, where it is unknown.
5. **A 2xx answer in the protocol's shape** (§3.1): the chain's not-found rows first (answer rows,
   R-F), then `C1`, null and empty included.
6. **The bare status**: HTTP 408 → `C5`; 409 → the chain's 409 rows, else `C3`; REST 400, 406, 411,
   412, 414–417, 422, 428, 431 → `C2`; REST 413 → `C2` `over_limit`; REST 404, 405 → `C3`; any
   other 4xx, and every bare 4xx on JSON-RPC, Tendermint and GraphQL → `C3`; 501 → `C3`; 500,
   502–599 → `C5`; a 2xx not in the protocol's shape → `C5`; 1xx, 3xx, anything else → unknown.
   gRPC: INVALID_ARGUMENT → `C2`; NOT_FOUND → `C3` `not_found`; OUT_OF_RANGE → `C3` `behind`;
   UNIMPLEMENTED, FAILED_PRECONDITION → `C3`; DEADLINE_EXCEEDED, INTERNAL, UNAVAILABLE, DATA_LOSS,
   and CANCELLED sent by the node → `C5`; ALREADY_EXISTS, ABORTED, UNKNOWN → unknown; an OK that
   doesn't decode → `C5`.

Steps 1 to 3 are the **prefix**: router code ahead of every spec row, except the chain's success
and duplicate rows on a write (step 2) and its over-limit row on gRPC RESOURCE_EXHAUSTED (step 3).
The status is the record's `status`, never a number elsewhere in the body: two fixtures,
`evm_http_429` and `evm_http_503`, carry `HTTP status <n>:` text the router never produces, and stay
with the transport rows.

Main differs: a long list of status-only rows (`error_classifier.go:421-449, 555-591`); JSON-RPC
rows that read the status as the sender's `HTTP <n>: ` prefix (`direct_rpc_relay.go:716-718`);
generic rate-limit wording and a 429 code in the body ahead of every row (`:283-290`; on JSON-RPC
MAG-5248 removes them, and a 2xx with a 429 code is unknown, Part IV decision 1; gRPC's texts,
MAG-5132); generic already-known wording on every JSON-RPC chain (`:310, 311, 316`; the rulebook
makes them the EVM node software's exact duplicate rows, MAG-5242); GraphQL statuses derived from
extension codes (`graphqlMessage.go:48-55`; MAG-5121); gRPC 7, 16, 9 and node-sent 4, 13, 15, 1
unmapped, plus its wording and digit rows (MAG-5132); the caller's deadline labelled as the
router's own (Part III D10, epic MAG-5126); and the gRPC rate limit from metadata applied only when
rows return `UNKNOWN_ERROR` (`direct_rpc_relay.go:1208-1209`). The interface cleanup tasks bring
main in line (MAG-5230 JSON-RPC and Tendermint, MAG-5252 REST, MAG-5126 gRPC, MAG-5095 GraphQL).
Until they land, spec rows take the place of main's family rows in main's order, which is what
Step 1's side by side compares (§7).

### 3.3 Retry

A read is retried by its class (Part I's class table): `C2` never; `C3` up to the error limit, a
`not_found` row at most once more (R-F; MAG-5279), and a node's refusal of a method the spec doesn't
list ends the request (Part I §1, "Unknown methods"; REST: MAG-5254 item 5); `C4`, `C5`, `C6` and
unknown up to the error limit. When every provider fails, the reply is chosen by class, not by
arrival (R-K: `C2` > `C3` > `C5` / unknown > `C4` > `C6`; MAG-5232). A write is never retried
(invariant 1). WebSocket subscriptions: §5.7.

Main differs on JSON-RPC and Tendermint, where the HTTP status overrides the verdict
(`direct_rpc_relay.go:500-501`): under a 5xx (`:600-612`), a verdict that would end the relay fails
over carrying the status's verdict and decides only endpoint health (`retryVerdict`,
`rpcsmartrouter/error_mapper.go:39-64`), so Solana `-32002` under HTTP 500 is classified
`CHAIN_SOLANA_SIMULATION_FAILED` and retried as `NODE_INTERNAL_ERROR`; under another non-2xx the
status pass can turn a retryable verdict non-retryable (MAG-1870, `:722-766`). MAG-5248 item 5
(MAG-4448) brings it in line; until then spec rows inherit main's order.

## 4. Validation (`cmd/specvalidate`, shared by lava-specs CI and the router at boot)

Rules are read from the router's registry, vocabulary and tables, never a copied list. A failing
section makes the chain's policy invalid → fallback (§5.6).

### 4.1 Rows, classes and shapes

- A `class` other than `C2`, `C3`, `C4`, `C5`, a `reason` its class doesn't take (`C2`: `invalid`,
  `over_limit`; `C3`: `not_found`, `pruned`, `behind`, `limit`, `version`, `method_off`; none on
  `C4`, `C5`), a `write` other than `took_it`, `refused_it`, `may_have_run`, or any value the router
  version doesn't know (schema mismatch).
- A row naming the chain's own verdict without a `class` (or a `C3` without a `reason`), or a
  router-owned verdict (the set in §1, exported to the validator with each one's class and reason)
  with either. A chain wanting another outcome for a router-owned verdict names its own.
- One name with two classes or reasons in a chain's effective rows.
- A chain's own name outside `CHAIN_<FAMILY>_*` / `NODE_<FAMILY>_*`.
- An answer row without `methods`, or one that gives anything but a success (`write: took_it`
  alone) or a not-found (`error`, `C3`, `not_found`); `at` or `present` on an error row; `path` off
  `graphql`; `write` on a `read` row.
- A shape whose `interface` the spec doesn't serve, whose pointers aren't JSON Pointers (RFC 6901),
  or, on `grpc`, whose method's definition the router doesn't bundle (MAG-5128).
- A migrated verdict whose class and reason don't give the flags of the router's definition, while
  the router still carries it (Steps 1–2). The 41 family-only verdicts are the chain's own from Step
  1, so their rows carry a class, and a reason on `C3`.

### 4.2 Verdicts a row may not name

Rate-limit sub-category (`NODE_RATE_LIMITED`, `NODE_LIMIT_EXCEEDED`, `PROTOCOL_RATE_LIMITED`; a
chain's quota row names its own `C4` verdict); `C6` (`NODE_UNAUTHORIZED`, `NODE_ACCESS_DENIED`: the
status decides, §3.2 step 3); any `PROTOCOL_*` (38); `UNKNOWN_ERROR`; `CHAIN_TX_ALREADY_KNOWN`
except on a duplicate row (`write: took_it`).

### 4.3 Matches a row may not use

Checked per interface the row applies to:

- `status` the status decides (HTTP 401, 402, 403, 429; gRPC PERMISSION_DENIED, UNAUTHENTICATED)
  or that isn't a status of that interface; gRPC RESOURCE_EXHAUSTED except on a `C2` `over_limit`
  or `C4` row.
- `code: 429`, number or string: a 429 code inside a 2xx body is unknown (Part IV decision 1).
- `contains`/`regex` matching an `HTTP <status>: ` prefix of a status the status decides, grpc-go's
  "unexpected HTTP status code received from server", or, on `rest` and `graphql` until MAG-5253
  removes the router's route rows, a route-refusal phrase ("endpoint not found", "route not found",
  "path not found", "method not allowed", `error_classifier.go:390-393`), whatever status the row
  names.

### 4.4 Source and evidence

`source`: the node software's official definition, on the router's allowlist of node-software and
protocol-spec origins (MAG-4898 completes it), pinned to a tag or commit; never one vendor's codes,
wording or fields (R-0.2). CI fetches it at that commit and requires representative replies for
every matcher and regex alternative (longest-literal check is lint only); each names the node
software and version it came from, the versions checked. Each new row also needs a production
example from the last 7 days with its Lava-Guid (Q229), kept in the PR's private evidence and never
in lava-specs, which is public: no documentation or no production example, no row (on a write the
reply stays may have run). R&D proposes and reviews matchers, classes, reasons and shapes; product
(Omer) approves each row's `write` and the chain's `write_rows`, and only those (Q229). The
validator replays the router's write-safety evidence through each chain's effective policy as write
copies, with its write rows on, and rejects a policy that gives a fixture a verdict outside the
ones its kind allows (`rule_violation`):

| Evidence | Allowed verdicts |
|---|---|
| Status refusal with a refusal body (no success or duplicate answer in it): HTTP 401, 402, 403, 429; gRPC UNAUTHENTICATED, PERMISSION_DENIED, RESOURCE_EXHAUSTED sent by the node | refused it |
| Already-known answer | took it, may have run |
| Slot-already-used answer (§5.2 signatures) | may have run |
| Rate-limit or quota answer inside a 2xx | refused it, may have run |

### 4.5 Stateful check

The validator exports the router's write-method table (§5.3) and fails a PR that clears `stateful`
on a listed write. `check_stateful.sh` reads this table instead of its own list (16 names, one of
them a REST route) and runs on every spec PR (MAG-4413).

## 5. Run-time rules in the router

### 5.1 Prefix

§3.2 steps 1 to 3, the prefix, run before every spec row, whatever validation said about a row's
text, with the two exceptions step 2 and step 3 name.

### 5.2 Write verdicts and slot already used (rulebook R-L, Q45)

- Each write copy ends in one verdict (R-L):
  - **took it**: a success row or a duplicate row (`write: took_it`), whatever the status (§3.2
    step 2);
  - **refused it**: a refusal the status decides (HTTP 401, 402, 403, 429; gRPC UNAUTHENTICATED,
    PERMISSION_DENIED, RESOURCE_EXHAUSTED sent by the node), from day one; a row with
    `write: refused_it` (a refusal or "not submitted" row keeps its class; a quota row counts only
    inside a 2xx);
  - **not sent**: the router's own transport proves no byte left;
  - **may have run**: anything else: a row with no `write`, a slot-already-used answer, a timeout,
    any other non-2xx, an answer no success row matches.
- A verdict read from a body comes only from the chain's own rows (spec rows and §5.5's), and only
  once the chain's write rows ship (`write_rows`); under a non-2xx status no generic interface row
  changes a copy's verdict (Q210). The write's outcome follows from its copies: accepted, unknown,
  rejected or not_sent (R-L; MAG-5232).
- Until the write rows ship, a node's 2xx body in the protocol's shape (REST: valid JSON), a bare
  REST `C2` status and gRPC INVALID_ARGUMENT go back as the node sent them, with no
  `Lava-Write-Outcome`; anything else that may have run (a timeout, a cut connection, any non-2xx
  but 401, 402, 403, 429, a 2xx not in the protocol's shape) is unknown and gets the router's
  unknown reply from day one (Part IV §5, Part V §5; MAG-5232, MAG-5256 item 1). When a 2xx body
  arrives beside a copy that may have run, main's handling stays until R&D confirms it (Part IV §5).
- A signature table in router code reads the record's family, interface, code and text,
  independent of row matching:

  | Family | Interface | Code | Text | Today's verdict |
  |---|---|---|---|---|
  | any | any | any | contains "nonce too low" | `CHAIN_NONCE_TOO_LOW` |
  | any | any | any | contains "nonce is too low" (`error_classifier.go:294`) | `CHAIN_NONCE_TOO_LOW` |
  | any | any | any | contains "account sequence mismatch" | `CHAIN_INVALID_SEQUENCE` |
  | XRP | jsonrpc | any | `tefPAST_SEQ` at start | `CHAIN_NONCE_TOO_LOW` |
  | Bitcoin | jsonrpc | -26 | contains "txn-mempool-conflict" | `CHAIN_BITCOIN_VERIFY_REJECTED` |
  | Bitcoin | jsonrpc | -25 | contains "missingorspent" or "missing inputs" | `CHAIN_BITCOIN_VERIFY_ERROR` |
  | Bitcoin | any | any | contains "already spent" or "double spend" | `CHAIN_DOUBLE_SPEND` |
  | Starknet | jsonrpc | 52 | any | `CHAIN_STARKNET_INVALID_NONCE` |

  Family is the chain's `chainFamilyMap` family (TEZOS, TEZOST, POLKADOTASSETHUB and POLYMESH get
  one with MAG-5246). MAG-4898 completes this table per node software and interface; each addition
  is a write-safety change (§8.1). Candidates to verify: Cosmos `sdk/32`, Stellar `tx_bad_seq`
  (Horizon 400), Aptos `SEQUENCE_NUMBER_TOO_OLD`, Cardano `BadInputsUTxO` (Blockfrost), Sui object
  versions already used.
- A copy that matches a signature may have run: never refused it, whatever its row's `write` says,
  on every interface. A match never by itself makes the router speak: the day-one rules above still
  apply. A 2xx slot-already-used body goes back as the node sent it, with no `Lava-Write-Outcome`,
  while Part IV's decision 3 parks the rule (MAG-4748; Part I R-B parks Cosmos code 32 with it); on
  GraphQL there is no park, so once Sui's write rows ship, a write whose copies only matched a
  signature gets the router's unknown reply (Part II D16; MAG-5122). The signatures read every
  record, including a JSON-RPC or Tendermint 5xx body carried as `HTTPStatusError`
  (`direct_rpc_relay.go:600-612`) and an error a shape found in a success.
- Record the signature before retry decisions: no further fan-out, not even via the all-rate-limited
  exception, even if a classifier also calls the answer a rate limit. Accepted siblings follow the
  accepted-result rules; reads classify as usual.
- Duplicate rows are exact, never a loose phrase (R-L): Bitcoin `-27`, Starknet `59`, Cosmos
  `sdk/19`, Stellar 409 `DUPLICATE` and TON "duplicate message" are candidates for MAG-4898; the
  generic EVM wording ("already known", "AlreadyKnown", "Known transaction") becomes the EVM node
  software's duplicate rows in ETH1 with its write rows (MAG-5242).
- Rows on stateful apis switch after the side by side shows zero write mismatches for 30 days; reads
  switch after 7 (§7). That switch is not `write_rows`: migrated rows carry no `write`.

### 5.3 Write methods stay stateful

- Table in router code keyed by interface + name (+ family for generic names like XRP `submit`),
  seeded from every api lava-specs marks stateful at `bda05974` (Tezos, Polkadot Asset Hub and
  Polymesh paths included, Part V) and `check_stateful.sh:97-119`, plus their misses: JSON-RPC
  (Bitcoin `sendrawtransaction`, `submitpackage`, `bb_sendTransaction`; NEAR `send_tx`; XRP
  `submit`, `submit_multisigned`; Sui `sui_executeTransactionBlock`; EVM
  `eth_sendRawTransactionConditional` and `eth_sendBundle`, Q224; Tendermint `broadcast_evidence`);
  REST (Bitcoin `POST /tx/send`; Aptos `POST /transactions`, `POST /transactions/batch`; Stellar
  `POST /transactions`, `POST /transactions_async`; TON `POST /sendBoc`, `/sendBocReturnHash`,
  `/message`; Tron `POST /wallet/broadcasttransaction`, `/wallet/broadcasthex`; Cardano
  `POST /tx/submit`; Concordium `PUT /v0/submit…`); gRPC (Sui `ExecuteTransaction`, Concordium
  `SendBlockItem`). GraphQL needs no entry: every mutation is already a write
  (`chainlib/graphql.go:259-268`). MAG-4898 completes it.
- At load (after `DoExpandSpec`, `utils/keeper/spec.go:79`, before the chain parser): a listed api
  not marked `stateful` is loaded stateful; warn per chain; set the gauge
  `smartrouter_spec_lock_enforced{spec, apiInterface, rule="stateful"}` to the number of apis
  forced, written at boot for every chain and interface, zeros included. A gauge, because this
  happens before the first scrape: a counter first scraped at its final value never shows an
  `increase()`. Today all 132 writes are stateful.
- A write the spec doesn't declare is handled as a read, the rulebook's accepted risk (Part I §1,
  "Unknown methods"): `smartrouter_requests_total{method=~"Default-.*"}` and the weekly review catch
  it, and the known EVM ones are added to the specs (Q224).
- Identity = the parser's canonical form (verb + route on REST, full service/method on gRPC, root
  field name on GraphQL, never the alias), never a display name or the `Default-` prefix; a
  URI-form Tendermint request (`GET /broadcast_tx_sync?tx=…`, `tendermintRPC.go:97-115`) has the
  JSON-RPC form's identity; family from the router's chain identity. Apply before scope selection,
  cache lookup, provider selection and retry; bypass cache reads and writes whatever
  `deterministic` says; check each batch member before aggregation; disabled methods stay rejected.

### 5.4 Signed approval of the effective policy

- Manifest entry per chain: chain id, schema version, interfaces declared by the expanded spec,
  resolved import graph + section digests, final ordered rows and shapes after replacements and
  removals (every field), `write_rows` and `plain_text`, the lava-specs commit, and the
  official-source commits and file digests used in review. Empty results have an entry.
- The entry names the deployment context (candidate / general) and the permitted stage (`shadow`,
  `reads`, `all`). The router applies the more restrictive of this and its setting.
- Validator and loader share one canonical serializer and hash (duplicate keys rejected, array order
  kept, a default such as `scope` or `inherit` hashed as if written, format version in the signed
  payload).
- The router embeds the trust roots, recomputes each policy from the loaded source, and acts only if
  signature and digest match. Unknown signer, revoked approval, missing entry, mismatch → fallback.
  No URL, folder or config bypass.
- Spec snapshot, manifest and signature ship as one artifact; the image bundles the same. A raw
  checkout without it supplies api definitions only; its error policies fall back to bundled.
- Trust lifecycle (delivery, key custody, rotation, revocation of a signer or digest) is specified
  and tested before any external policy acts; updates reach routers via a trusted release or an
  authenticated trust update, never via the spec being verified. No fleet-wide revocation or
  anti-replay is claimed.

### 5.5 Router-owned XRP rows

Until the XRP chapter ships its rows, XRPL's result fields (`result.error` when `result.status` is
`"error"`, the engine result on `submit`; `xrplEngineResult.go:64-104`, picked per family,
`jsonRPCChainFamily.go:14-35`) are read in router code, and four XRP rows stay there:
`tooBusy`/`slowDown` → `NODE_RATE_LIMITED` (`error_classifier.go:127`, a rate-limit row; on a write,
"not submitted", R-L), `tefALREADY` → `CHAIN_TX_ALREADY_KNOWN` (`:117`, a duplicate) and
`noPermission`/`forbidden` → `NODE_ACCESS_DENIED` (`:128`; the rulebook's `C6` is decided by the
status alone, so the XRP chapter gives it a class, §10) run at §3.2 step 4.1; the `te[fmlr]…`
catch-all → `CHAIN_TX_REJECTED` (`:119`, `xrplRejectionCode` `:171`, uses `IsXRPLRejection`) runs at
step 4.3, after the spec rows `tefPAST_SEQ`/`terPRE_SEQ`.

### 5.6 Fallback

A chain's effective policy from the loaded source is replaced by its complete bundled policy when:
unknown `schema_version`; a row or shape breaks §4; no valid approval; an unresolvable import. If
the source as a whole fails (host down, ref missing, not an archive, nothing parses) the router
boots on the bundled specs for everything.

- Resolve external and bundled graphs separately; never mix a bundled parent with external
  overrides. Each dependant falls back whole; unrelated chains keep their policies. Api definitions
  stay from the loaded source unless the whole source failed; the stateful guard still applies.
  Validate the fallback against the interfaces the chain serves. One atomic choice per chain, before
  serving.
- If the bundled specs have no effective rows for the chain, or a served interface is outside the
  bundled approval, the chain gets the router's own rules only (§3.2 steps 1, 3, 4.1, 4.3, 5 and 6),
  logged as reduced coverage. The §5.2 signatures and the XRPL reader are router code and apply in
  every state, whole-source failure included; a write copy left without its chain's rows is judged
  by the day-one rules (§5.2), the safe direction.
- Log chain + cause; count `smartrouter_spec_errors_section_dropped_total{chain_id, reason}`,
  reason ∈ {`schema_unknown`, `rule_violation`, `approval_missing`, `signature_invalid`,
  `policy_mismatch`, `approval_revoked`, `dependency_invalid`, `source_file_failed`,
  `bundled_no_rows`, `interface_outside_context`}; whole-source failures count
  `smartrouter_spec_source_fallback_total{reason}`, reason ∈ {`host_unreachable`, `ref_not_found`,
  `not_archive`, `no_spec_parsed`}.
- Gauges: `smartrouter_spec_policy_fallback{spec, apiInterface, reason}` = 1 while on bundled policy
  or reduced coverage; `smartrouter_spec_source_fallback{reason}` = 1 while the whole source is
  replaced. Write every reason at boot, zeros included (as
  `smartrouter_csm_blocked_providers_by_reason`, `smartrouter_metrics_manager.go:581-584,
  1688-1700`). `/livez` and `/readyz` are unaffected.
- Today a bad source is fatal (`rpcsmartrouter.go:2567-2581`, `:3244-3249`) and bad files beside
  good ones are skipped silently (`utils/keeper/spec.go:111-128`,
  `utils/specfetcher/fetcher.go:380-392`); both become fallbacks (the latter per chain,
  `source_file_failed`).

### 5.7 WebSocket subscriptions

Subscriptions stay outside the rulebook's Layer 1 until its subscription chapter: today's handling
stays (Part I §1, Part IV §7). This design only counts their errors: a subscribe the node refuses,
and an error frame delivered to an open subscription (`listenForUpstreamMessages`,
`direct_ws_subscription_manager.go:1386-1398`), on `jsonrpc` and `tendermintrpc`, build a record and
take a verdict through §3.2, counted in `smartrouter_errors_total` with `source="subscription"`
(R-G, Q183) and in `relay_end`. Selection, retry, restore, teardown and the URL's health stay as
today: every refusal but a 429 counts a failure (`:444-458`, called at `:645, 656`), and a delivered
error frame ends the subscription with no count (`:1386-1398`). Connect, upgrade and transport
failures keep the connection rules.

## 6. Compatibility and release pair

- `schema_version` changes only when an older router can't read the format; the class vocabulary
  is part of the format. `compatibility.json` at the lava-specs root:
  `{ "schema_versions": { "1": { "min_router": "v1.8.0" } } }`.
- Each router release names one lava-specs commit in `specs/SPEC_COMMIT` and ships its files.
  `make specs` fills `specs/` from it; CI compares byte-for-byte and verifies manifest + signature.
  Router CI runs the validator and manifest check over every bundled section. `grpctest.json` moves
  to a test fixture directory. Release notes name the pair.
- No `--use-static-spec` → bundled specs (today: refuse to start, `rpcsmartrouter.go:2569-2581`).
  `--use-static-spec` still takes any URL or folder, unpinned; its sections must pass §4–§5. Helm
  default (`charts/smart-router/values.yaml:450-458`, bare lava-specs URL today) → bundled specs;
  tenants set a source in `cluster.yaml` if they want one.

## 7. Rollout

**Step 0 (no spec row read yet).** Visible changes: boot on bundled specs instead of refusing; a
declared write relayed stateful even when the spec clears it; a slot-already-used answer ends a
write's fan-out; a cached node error is never served (R-F); subscription errors counted (§5.7);
`smartrouter_errors_total` gains `apiInterface` and `source`. Deliverables: `compatibility.json`,
`SPEC_COMMIT`, `make specs`, folder-matches-commit check, bundled specs in the image, source
fallback (§6, §5.6); `cmd/specvalidate`, schema, `spec_guards.yml` jobs, owner-review and evidence
checks, approval pipeline (§4, §8.1); the error record per reply, batch element and GraphQL error,
with `apiInterface` passed to classification, and the shape and answer-row engine (§3.1, §2.4,
§2.2); the prefix ahead of every spec row (§3.2 steps 1 to 3; the status rules themselves are
MAG-5248 item 1, MAG-5254 item 3 and, on gRPC, MAG-5132) and the router-owned XRP rows (§5.5);
exported router-owned set with each verdict's class and reason, and the class checks (§4.1);
write-method table with the load-time lock and per-interface identity (§5.3); slot-already-used
signatures (§5.2); Helm default → bundled.

**Step 1 (side by side).** Copy the six family tables (minus the four XRP rows of §5.5) into their
root specs unchanged: each row with `interface` set to the interfaces it reads today, and no
`write`; no chain sets `write_rows`. A row naming one of the 41 family-only verdicts carries the
class of today's flags and, on `C3`, the reason its node software documents; all fit. A row naming
one of the 17 verdicts also used by transport rows or logic names a router-owned verdict and carries
none. Three family-table verdicts can't be named (`NODE_RATE_LIMITED`, `NODE_ACCESS_DENIED`, and
`CHAIN_TX_ALREADY_KNOWN` outside a duplicate row): all are in the four XRP rows kept in code.
Today's Cosmos and Tendermint checks become shapes (§2.4). NEAR's four rows match only once
MAG-4253's shape reads `error.cause.name`: NEAR stays out of the zero-mismatch evidence until then.
STRKS imports STRK; the 36 gaining chains get `"inherit": false` (Appendix B), which keeps their
parents' shapes: the 25 Cosmos ones still read a failed broadcast inside a 200. The reader loads
rows and shapes, evaluates them beside the code, acts on the code. Reader constraints: registry
maps are init-only and lock-free, `registerError` panics on duplicates
(`error_registry.go:602-629`) → a chain's own verdicts live in a per-chain policy outside
`errorRegistry`; `ClassifyError` is keyed by `ChainFamily` (`error_classifier.go:788, 801`) → key
by chain policy; router logic compares its verdicts by pointer (`svm_block_hash_retry.go:99`,
`read_your_writes.go:230`, `rpcsmartrouter_server.go:1599-1603`) → a row naming a router-owned
verdict resolves to the router's own `*LavaError`; `LavaError.Is` and `LavaWrappedError.Is` match
by code (`error_registry.go:184-187, 662-667`) and code 0 is `UNKNOWN_ERROR`'s (`:589-590, 617`) →
a chain's own verdict, which has no number, gets an identity of its own, so `errors.Is` never
equates two of them or one with `UNKNOWN_ERROR`. Metrics:
`smartrouter_spec_error_rows_mismatch_total{chain_id, apiInterface, kind, row_id, code_error, spec_error, scope}`
with `kind` ∈ {`differ`, `code_only`, `spec_only`} and
`smartrouter_spec_error_rows_hits_total{chain_id, apiInterface, row_id, scope}`. Expected: zero
mismatches; compare the old and new family stages with the same prefix on both sides, and count
coverage at the family stage as well as the final verdict, per interface.

**Step 2 (switch).** A router setting:

| Setting | Reads | Writes |
|---|---|---|
| `shadow` | prefix, family table, transport; spec rows counted | same |
| `reads` (7 days, zero read mismatches) | prefix, spec rows, family table, transport | prefix, family table, transport; spec rows counted |
| `all` (30 days, zero write mismatches) | prefix, spec rows, family table, transport | same as reads |
| tables deleted (next release) | prefix, spec rows and router-owned XRP rows (§3.2 step 4), transport | same |

The setting can't exceed the approved stage (§5.4); an early `all` logs and stays. `all` decides
which rows classify; it is not `write_rows`, which alone switches on write verdicts read from a body
(§2.1). Record the policy digest, window start/end and observed coverage per chain, interface and
scope (denominators: `smartrouter_requests_read_total` and `smartrouter_requests_write_total`,
summed by `spec` and `apiInterface`); a policy change or mismatch restarts the window; exercise
unused rows with fixtures. Rollback before deletion = previous setting + artifact; after deletion =
previous router release + bundle. The family tables and the 41 code verdict definitions are deleted
in the release after `all` has passed one canary (`chainFamilyMap` stays: fault axis, transport
rows, signatures, the XRPL reader, `IsSolanaFamily` `protocol/common/svm.go:10-13`); that release
bundles an `all`-approved policy and keeps the bundled policy for any chain whose external policy
has only `shadow`/`reads` permission.

**Step 3.** MAG-4898 lands researched rows and shapes, new verdicts included; the 36 gains open by
removing `"inherit": false`, each a behaviour change with product sign-off and, for a new verdict,
class or reason, R&D review. A chain's write rows ship when product approves its write verdicts and
sets `write_rows`, with a customer note (Part V §5.2): EVM first, with MAG-5242 moving the generic
already-known wording into ETH1's duplicate rows; Cosmos with its success (code 0) and duplicate
(19) rows; Sui GraphQL with MAG-4909 and MAG-5122; Sui gRPC with MAG-5186 and MAG-5128's Sui part,
never before. Cardano's Blockfrost not-found rows, and the shape they read, ship before MAG-5258's
new success rate.

## 8. Gates

### 8.1 lava-specs CI, every PR

- JSON Schema for `errors` on every changed spec.
- `cmd/specvalidate`, built from each router release the boot matrix covers.
- Stateful check (§4.5) as a `spec_guards.yml` job.
- Write-safety job: validator write rules + diff of effective policies for every affected chain and
  descendant (row and shape contents by `id`, order separately). A change to any row's `write` or a
  chain's `write_rows` needs product's approval (Q229); a change to a row or shape on a stateful api
  needs the write-safety owners' review; both are required checks (labels are informational). A new
  `CODEOWNERS` file names the owners teams (spec owners, write-safety owners, product, R&D); since
  it can't target a JSON section, the required checks compute what changed and demand the right
  approval on the current head.
- Source evidence, production examples included (§4.4); every root and descendant policy passes
  validation and compatibility before its digest can be signed.
- R&D review (required check) for a new verdict, a class or reason change or a shape change in any
  chain's effective policy; also fails a migrated verdict whose class and reason no longer give the
  router's flags.
- Behaviour changes named in the PR description (old → new verdict/class/reason/write, why) with a
  `behaviour-change` label; replay and coverage check fail otherwise.
- `check_unused_fields.sh` moves into `spec_guards.yml`.
- Testing and signing are separate: PR jobs validate with an isolated test trust root; after merge a
  trusted job re-checks the exact snapshot, verifies owner approvals and publishes a candidate
  artifact for the canary (separate trust context); after the canary the promotion job signs the
  production manifest for those digests and stage. General deployments consume only promoted
  artifacts.

### 8.2 Against the router

Gates that assert the rulebook's order where main differs (§3.2, §3.3, §5.2) run once the task named
there lands; until then they assert main's order with spec rows in place of the family rows.

- Boot matrix: every changed chain (descendants included) boots with the PR's specs on every router
  release in service from `min_router` up; a dropped section fails unless declared.
- Replay: the 102 fixtures in `protocol/common/testdata/error_fixtures.json` plus
  `TestRESTStatusRows`, `TestRESTStatusRowsDoNotReachOtherTransports`,
  `TestRESTMessageRowsBeatTheStatusRow`, `TestGRPCRateLimitTexts_TheCodeRowsAboveStillDecide`,
  `TestEndpointFault_AnswerShapes`, `TestGRPCStatusError` give the same verdict name and flags
  unless declared; fixtures carry the expected class and reason. `TestFaultAxis_RegistryContract`'s
  rows for the 41 (`error_fault_axis_test.go:151-164`) move to a fixture read from the bundled
  sections.
- Fixtures per interface. The schema gains `interface` (five values), `status`, `body` (or frame),
  `details` (gRPC), `path` (GraphQL), `in_success`, and the node software and version. Each fixture
  runs through its interface's reader, shapes and record, not `ClassifyError` alone (the harness
  takes three transports today, `error_fixtures_test.go:43-47`; 97 of the 102 are JSON-RPC, none
  Tendermint or GraphQL). Every row and shape needs a fixture on each interface it applies to. The
  chapters' test cases that touch classification join: Part II T11, T12, T14, T18, T21, T22, a 200
  RESOURCE_EXHAUSTED with a path (not `C4`) and a 200 FEATURE_UNAVAILABLE (`C3`, never `C6`); Part
  III T13, T16, T18, T19, T20 and UNAVAILABLE carrying a gateway 403 (`C6`); Part IV T24; Part V
  T2, T7, T8, T11, T13, T20. The five fixtures whose status exists only as text
  (`rest_404_not_found`, `rest_429_rate_limit`, `rest_503_unavailable`, `evm_http_429`,
  `evm_http_503`) keep running through `ClassifyError`.
- Order (§3.2): 429 with a JSON-RPC `-32603` body → `C4`; REST 401, 402, 403 with family text and
  gRPC PERMISSION_DENIED, UNAUTHENTICATED → `C6`; a gateway 402 or 429 inside a gRPC status → `C6`
  or `C4` (D12); a well-formed JSON-RPC error under HTTP 500 → the row its code matches, not the
  status; gRPC RESOURCE_EXHAUSTED with an over-limit row and a wait → `C2` `over_limit`; 522 with an
  unmatched JSON-RPC body → `C5`; 400 with an unmatched body → `C3` on JSON-RPC, Tendermint and
  GraphQL, `C2` on REST; 200 with an unmatched `-32000` → unknown (MAG-5248 items 2 and 6); a 2xx
  with a 429 code → unknown; a body text beginning `HTTP 503: ` is never read as a status; an ETH1
  row written for `jsonrpc` never reads a Hedera `rest` reply (MAG-5253). Keep today's verdict:
  Bitcoin `-26` "min relay fee not met, 429 < 1000" and "mempool min fee not met, 429 < 1000",
  Agave `-32005` "Node is behind by 429 slots"; a rate-limit phrase inside gRPC 3/5/10/11/12 stays
  with its code row. Fail validation: `{status: 402, contains}`, `{status: 429, code}`,
  `{status: "PERMISSION_DENIED", contains}`, `{status: "RESOURCE_EXHAUSTED", contains}` on a row
  that isn't `C2` `over_limit` or `C4`, `{contains: "route not found"}` on `rest` before MAG-5253.
  Relay-policy: a stateful write whose every answer is the min-fee rejection stops after fan-out and
  holds nothing off.
- Attack row as a loaded spec row: `{code: -32000}` → `CHAIN_TX_REJECTED` vs EVM "already known":
  no verdict change.
- Retry by class (§3.3), end to end through `relayInnerDirect` with spec-loaded rows, once MAG-5248
  item 5 lands: a `C2` row under JSON-RPC HTTP 500 (Solana `-32002`,
  `CHAIN_SOLANA_SIMULATION_FAILED`) ends the read; a `C3` `not_found` row asks at most one more
  provider (MAG-5279); a node's refusal of a method the spec doesn't list ends the request. Until
  then `direct_http_status_body_test.go:92, 140` and
  `TestDirectRPCRelaySender_HTTPStatusOverridesRetryableBodyCode_MAG1870`
  (`direct_rpc_integration_test.go:530`) pass unchanged with the rows loaded from specs.
- Write verdicts (§5.2): before `write_rows`, a node's 2xx body, a bare REST 400 and gRPC
  INVALID_ARGUMENT on a write go back as is with no `Lava-Write-Outcome`, an empty or HTML 2xx and a
  bare 5xx get the router's unknown reply, a 401, 402, 403 or 429 refuses the copy; after
  `write_rows`, a success row's or duplicate row's copy took it, also under a 401 or 429 (Part II
  T14), while a success row without `at` never reads a non-2xx (a REST 429 with a JSON body stays
  refused), a `refused_it` row's copy is refused and keeps its class (Horizon 503
  `TRY_AGAIN_LATER`: `C5`), a 2xx no success row matches may have run, a generic interface row under
  a 5xx never sets a verdict (Q210), and a migrated row without `write` may have run. The §4.4
  replay passes every bundled policy and fails one whose duplicate row matches a 429 fixture's body
  or whose row says `refused_it` on a slot-already-used answer.
- Slot-already-used: every signature as a full pair through the stateful path of each interface it
  lists is never refused it; a 2xx body with it goes back as the node sent it, with no
  `Lava-Write-Outcome` (JSON-RPC and Cosmos, Part IV decision 3), and on GraphQL after Sui's write
  rows ship gets the router's unknown reply; under a 5xx it is unknown from day one; the same pair
  on a read keeps its verdict; every fixture with `CHAIN_NONCE_TOO_LOW`, `CHAIN_INVALID_SEQUENCE`
  or `CHAIN_STARKNET_INVALID_NONCE` matches a signature; mixed with rate-limit answers, or matching
  a rate-limit phrase too, no second fan-out; include an accepted sibling and prefixed XRP replies.
- Reply as is (invariant 9), in masked and unmasked mode: a reply a spec row classifies and a
  success a shape marks (Cosmos `tx_response` and Tendermint `result.code` on a broadcast, XRP
  `tefPAST_SEQ`) reach the client with only R-A's edits, byte for byte elsewhere; on gRPC the same
  status, message, details and listed trailers and a proto-equal message (MAG-5129); an accepted or
  rejected write forwards a node's reply, an unknown write gets the router's.
- Coverage: each chain's effective rows and shapes vs the PR's base commit; gains, losses, reorders
  or class, reason or write changes fail unless declared and approved.
- Shapes: per shape, a reply it reads and one it leaves alone, on each method it names; a REST body
  outside every shape is bare; an error a shape finds in a success never ends a write's fan-out as
  its success; a missing, null or wrong-type `when`, `code` or `space` pointer leaves the shape
  unapplied and the next one reads the reply; a chain with `"inherit": false` still reads its
  parents' shapes (DYDX: a broadcast with `tx_response.code` ≠ 0 inside a 200 is a node error).
- GraphQL aliases: an aliased root field (`tx: transaction(…)` answering null) matches the
  not-found row, the same field asked twice under two aliases is read twice, and an error's aliased
  path matches a row's `path`.
- `field: "detail"`: a string `error.data` is read as its value, an object or array as its compact
  JSON, a null or absent `detail` matches nothing; on gRPC, details rendered from linked or bundled
  types only, an unresolvable one as `{"@type": …}`, the same on every provider; a row with `field`
  but no `contains` or `regex` fails validation.
- Generated row tests: per changed row a source-backed positive reply and a near-miss (one per
  regex alternative), tested against the complete effective policy, recording the winning row,
  verdict, class, reason and write verdict, reporting unreachable rows.
- WebSocket (§5.7), on `jsonrpc` and `tendermintrpc`: a refused subscribe and an error frame
  delivered to the subscription's error channel (injected there on `tendermintrpc`) are counted
  with their verdict, `apiInterface` and `source="subscription"`; the URL's health, restore and
  teardown are as today.

### 8.3 Router repo tests (bad source, locks, fallback)

- No `--use-static-spec`, non-archive, missing ref, silent host, unparseable spec → bundled specs
  with the right `smartrouter_spec_source_fallback_total` reason.
- Each §4 rule, missing/wrong-key/revoked approval, unknown schema → the chain's bundled policy.
  Class cases: an unknown class or reason, a chain's own verdict without a class, a `C3` without a
  reason, a router-owned verdict with either, `write` on a read row, an answer row without
  `methods`, a shape with a pointer that isn't RFC 6901, one name with two classes or reasons in a
  chain, a migrated verdict whose class and reason no longer give the router's flags, a row naming
  a §4.2 verdict, each fail; a chain's own verdict loads and the relay
  path reads its class's flags; two of a chain's own verdicts are distinct under `errors.Is`, and
  neither is `UNKNOWN_ERROR`; a row a child replaces applies only to the child (ETH1 `C5` vs BASE
  `C2`).
- Reordering/deleting approved rows, deleting a section, altering imports, moving a row between
  chains, widening interfaces → digest verification fails; approved empty policy passes. Invalid
  parent → dependants on bundled policy, unrelated chains untouched; cycles, mixed schemas,
  duplicate ids, diamonds never mix external and bundled rows.
- Vendor-only regex alternative fails review; unapproved custom policy never acts via an official
  URL or candidate signature; `all` with shadow/read permission activates no write rows; production
  binary rejects candidate-only approval; after deletion a partially approved policy keeps bundled.
- After the switch: Solana's section dropped → `-32005` "Node is unhealthy" still
  `NODE_SOLANA_UNHEALTHY` from the bundled section.
- On reduced coverage and on whole-source fallback, XRP HTTP 200 replies carrying `tefPAST_SEQ` and
  `tooBusy` still reach the §5.2 signature and the rate limit.
- `eth_sendRawTransaction` with `stateful` 0 → loaded stateful, and the gauge
  `smartrouter_spec_lock_enforced{rule="stateful"}` reads 1 for that chain and interface (0
  elsewhere, read from the collector right after load); omitted → handled as a read (the
  rulebook's accepted risk, §5.3); repeat cleared/disabled per write interface (a URI-form
  Tendermint `broadcast_tx_sync`, Aptos `POST /transactions/batch`, gRPC `BroadcastTx` included),
  batches, cache bypass, `deterministic: true` on a declared write.
- Every fallback case asserts the gauges (1 for the affected chain/interface/reason, 0 elsewhere,
  0 after reload with a valid policy, read from the collector).
- Alert rules proven in smart-router-infrastructure by extending the promtool replay in
  `tests/grafana-alert-rules/test_silent_when_idle.py:129-156` (render per-tenant rules via
  `expand_tenant`; gauge 1 past `for` fires, back to 0 resolves). For `sr-spec-lock-enforced`: the
  lock gauge at 1 from the first sample fires (the startup case an `increase()` misses) and
  resolves when the series resets after a restart. Plus `scripts/grafana-alert-rules.py check`; CI
  `.github/workflows/grafana-plan-pr.yaml:121-124`. Publish rules only after a router emitting the
  gauges is on the fleet (`check` can't see whether a metric exists,
  `docs/OBSERVABILITY.md:1325-1330`).

### 8.4 Canary

One canary router per cluster, one full day of production traffic (minimum; the 7/30-day windows of
§7 still apply). Compare `smartrouter_errors_total` by `error_name`, `chain_id` and `apiInterface`
(`protocol/metrics/error_metrics.go:42-44`); dropped-section and source-fallback counters at zero,
the lock gauge at zero, both fallback gauges 0. On-prem gets the tested pair only. Failed canary →
previous artifact (and router release if the bundle changed); the next release bundles the promoted
commit.

## 9. Observability

| Signal | Purpose |
|---|---|
| `smartrouter_spec_errors_section_dropped_total{chain_id, reason}` | rejected section, with reason |
| `smartrouter_spec_source_fallback_total{reason}` | source couldn't load |
| `smartrouter_spec_lock_enforced{spec, apiInterface, rule}` (gauge) | writes forced stateful at load (`stateful`), as state since boot |
| `smartrouter_spec_write_rows{spec, apiInterface}` (gauge) | 1 where the chain's write rows have shipped (`write_rows` at stage `all`); the unknown-write alert (R-L; MAG-5232) reads it |
| `smartrouter_spec_error_rows_mismatch_total{…}`, `smartrouter_spec_error_rows_hits_total{…}` | switch criterion; which rows matter |
| `smartrouter_spec_policy_fallback{spec, apiInterface, reason}`, `smartrouter_spec_source_fallback{reason}` | current fallback state; alerts read them |
| `smartrouter_errors_total{error_name, error_category, retryable, chain_id, apiInterface, source}` | gains `apiInterface` (five values) and `source` (`client`, `probe`, `boot`, `reverify`, `poll`, `subscription`; R-G, Q183; dashboards read `source="client"`); a chain's own verdicts appear as `error_name`; a child's class change means one name may carry different `retryable` per `chain_id`, so update the 1:1 comment and the "~100 codes" budget (`error_metrics.go:21-28`) and dashboards |

Alert rules in smart-router-infrastructure `grafana/alert-rules/smart-router.yaml`, house style
(`uid`, `title` and `summary` naming `{tenant}`, `for`, `== bool`/`> bool`, `severity` only label,
description with the first operator action), all `warning`:

| Rule | Expr | For |
|---|---|---|
| `sr-spec-policy-fallback` | `max by (cluster, location, router, spec, apiInterface, reason) (smartrouter_spec_policy_fallback{cluster="{tenant}"}) == bool 1` | 10m |
| `sr-spec-source-fallback` | `max by (cluster, location, router, reason) (smartrouter_spec_source_fallback{cluster="{tenant}"}) == bool 1` | 30m |
| `sr-spec-lock-enforced` | `max by (cluster, location, router, spec, apiInterface, rule) (smartrouter_spec_lock_enforced{cluster="{tenant}"}) > bool 0` | 0s |

`sr-spec-lock-enforced` reads values, not `increase()`, so load-time enforcement that happens before
the first scrape still fires. It stays firing until the spec is fixed and the router restarts; the
description names the rule (`stateful`).

## 10. Open decisions

| Decision | Needed before |
|---|---|
| Rulebook lines that contradict Part I §1 or the 16 Nov change log, for the rulebook owner: R-E's example "the chain row beats the 429" (Part I), Part IV and Part V check P3 (an over-limit row before a 429, or before 401 to 403), Part I's "bare 400 on REST: the copy is refused" and "gRPC 3: rejected" tests and Q200, Q210(b), Part III §1 and §3.1 (a chain row changing 7, 16 or 8), Part III T13 (code 32 not parked). This spec follows Part I §1 and the change log | the rulebook's next edit |
| XRP `noPermission`/`forbidden` (today `NODE_ACCESS_DENIED`): the rulebook's `C6` is status-only, so the XRP chapter gives it a class (§5.5) | XRP's rows move to `ripple.json` |
| Retry by class on JSON-RPC and Tendermint (§3.3; main lets the status decide, MAG-5248 item 5) | a spec relies on a `C2` row under a JSON-RPC 5xx |
| Per-method empty shapes and never-cache fields (R-F; Part II §7): a later `schema_version`, with MAG-5249 and MAG-5123 | a chain needs them in its spec |
| R-0.2: the generic geth-wording rows (`error_classifier.go:269-346`, `:241`, minus the rate-limit rows MAG-5248 removes and the already-known rows MAG-5242 moves) move to ETH1, after 30 days of hits data; the 17 specs that serve `eth_*` without reaching ETH1 at `bda05974` (TRX, VECHAIN, ACA, QUAI, HYPERLIQUID, …) get an import or their own rows first; the router's vendor rows (Alchemy `-32600`, QuickNode's `blocked …` row) go with MAG-5248 (decided 12 Nov) | those behaviour changes ship |
| REST: the generic table's seven node-software text rows become chain rows (COSMOSSDK `rest`, POLKADOTASSETHUB, POLYMESH, EOS, MULTIVERSX) under MAG-5253; spec rows reach chains through imports, so they don't wait for MAG-5246 | MAG-5253 ships |
| bitcoind `-8` (`C3` `behind`, Q146) and `-5` rows for BTC's spec; Solana `-32009` as `C3` `not_found` (MAG-5279) | MAG-4898's Bitcoin and Solana rows |
| Initial write-method table | Step 0 write guards ship |
| Docs: verdict names can come from a chain's spec, with no number (`ERROR-REGISTRY-DESIGN.md`, public docs, `docs/RELAY-END-EVENT.md:65, 138` say "error registry name") | Step 2 |
| Source evidence and production examples for migrated rows; the R&D owners team for verdict, class, reason and shape review | Step 1 |
| Manifest delivery, signing service, key custody, rotation, revocation; candidate trust context | any external policy acts |
| Signature, duplicate and shape candidates per interface (§5.2, §2.4): MAG-4898's evidence for each | each ships |
| create-spec agent keeps unknown keys in update modes by construction; new-chain mode needs a template (MAG-4898 part 3); add a test in `scripts/test_*.sh` | Step 3 |
| Effort: ticket said 6–8 engineer-weeks; revision 2 split ≈9; the approval pipeline, the prefix, chain-defined verdicts, shapes, answer rows and write verdicts, and interface parity (record, per-interface fixtures) add to it | R&D sizes |

Later: customer overrides as an overlay file (read rows, same validator, own approval path; Q229: an
override may only make a write reply safer); spec versioning if picked up again.

## Appendix A. Code map (today, at `6c237073`)

| Component | Where |
|---|---|
| Family tables (6 families, 58 verdicts: 41 family-only, 17 shared) | `protocol/common/error_classifier.go:24-167` (Solana `:25-45`, Bitcoin `:46-60`, Cosmos `:61-74`, Starknet, NEAR, XRP `:108-166`) |
| Transport tables (JSON-RPC 98, REST 68, gRPC 32 rows; 19 `httpStatusRows`) | `error_classifier.go:201-627`; REST `:378-452`; gRPC `:458-544`; `httpStatusRows` `:555-591`, init `:615-627` |
| Matcher kinds: `CodeEquals`, `MessageContains`, `MessageRegex`, `CodeAndMessage`, `HTTPStatusContains` (any digit-bounded number, `error_registry.go:502-534`), `GRPCCodeEquals`; XRP helper `xrplMessageStartsWith` | `error_registry.go:432-550`; `error_classifier.go:184-188` |
| `ClassifyError` order; `chainFamilyMap` (106 ids; 204 of lava-specs' 306 indexes unmapped) | `error_classifier.go:788-820` (family `:801-807`, transport `:810-816`); `error_registry.go:280-368` |
| Registry: 133 `registerError` + `UNKNOWN_ERROR`; maps by code and name; struct; `EndpointAtFault` | `error_codes.go`; `error_registry.go:589-596, 602-630, 157-170, 136-152`; sub-category enum `:40-50` |
| Code ranges | PROTOCOL 1001–1053 (38, internal), NODE 2001–2150 (23), CHAIN 3001–3363 (65), USER 4001–4007 (7) |
| Rate-limit flag → re-send (`Policy.Decide`, `onlyRateLimited`) and hold-off | `relaypolicy/policy.go:40-60`; `relaycore/relay_processor.go:1520, 669`; `endpoints.go:698`; `rpcsmartrouter_server.go:3241-3255` |
| Unclear-write decision (`writeOutcomeIsUnknown`, `unclearWriteOutcomeFor`); REST node-reply rule (`ServerErrorIsNodeReply`); 5xx scored before fault flags (`shouldFailSessionForResult`) | `rpcsmartrouter_server.go:1447-1508, 1421-1445, 6032, 2585-2620`; `relaycore/results_manager.go:72`; `restMessage.go:74-78` |
| Transport/status errors classified with the chain family | `rpcsmartrouter/error_mapper.go:23-37, 71-83`; `node_error_handler.go:119-126` |
| Retry precedence on JSON-RPC/Tendermint: `retryVerdict` + `endsRelay` (5xx), MAG-1870 status pass (other non-2xx) | `rpcsmartrouter/error_mapper.go:39-64`; `direct_rpc_relay.go:500-501, 600-612, 698-766`; tests `direct_http_status_body_test.go:92, 140`, `direct_rpc_integration_test.go:530` |
| Read-your-writes, SVM block retry, node error handler (identity comparisons) | `read_your_writes.go:218-231`; `svm_block_hash_retry.go:99`; `node_error_handler.go:23, 164` |
| How a status reaches the classifier | REST `direct_rpc_relay.go:911`; GraphQL `graphqlMessage.go:33-55, 490`, `direct_rpc_relay.go:1034`; JSON-RPC prefix `direct_rpc_relay.go:703-720`, `node_error_handler.go:80` |
| Interfaces → classifier transports (graphql → REST, tendermintrpc → JSON-RPC) | `error_classifier.go:768-777`; sender dispatch `direct_rpc_relay.go:500-510` |
| What each reader passes to the classifier today: JSON-RPC and Tendermint `code` and `message`; REST `message`/`error`/`error.message`, else the body (first 1 KB); GraphQL one error's `message` and `extensions.code`; gRPC status and message (the results manager falls back to the body) | `rpcInterfaceMessages/jsonRPCMessage.go:101-128`, `tendermintRPCMessage.go:39-59`; `restMessage.go:99-127`; `graphqlMessage.go:141-151, 462-498`; `direct_rpc_relay.go:1128-1213`; `relaycore/results_manager.go:151-153` |
| Chain reply checks in code today (shapes from Step 1, §2.4; XRPL stays code, §5.5) | Cosmos `restMessage.go:55-60, 83-96`; Tendermint `tendermintRPCMessage.go:39-51`; XRP `xrplEngineResult.go:64-104`, `jsonRPCChainFamily.go:14-35`; GraphQL statuses derived from extension codes `graphqlMessage.go:48-55` (MAG-5121 removes them) |
| Cache serving a node error; cached classification | `rpcsmartrouter_server.go:2211-2212`; `classifyCachedNodeError` `:2216-2238` (JSON-RPC and Tendermint only) |
| Classification sites: senders; results manager; cache (JSON-RPC and Tendermint only); subscriptions (relay_end only) | `direct_rpc_relay.go:720, 911, 1034, 1202, 1279`; `relaycore/results_manager.go:191, 200`; `rpcsmartrouter_server.go:2216-2238`; `subscription_relay_end.go:66-99` |
| WebSocket subscribe failures and delivered error frames | `direct_ws_subscription_manager.go:444-458, 645, 656, 1386-1398` |
| Default api for undeclared methods | `base_chain_parser.go:532-560, 579`; `rest.go:158`; disabled apis `:582, 848-851` |
| Spec loading, imports (`DoExpandSpec`; child wins by name within a collection), unknown keys dropped, source | `utils/keeper/spec.go:23-26, 79, 89, 111-128`; `utils/specfetcher/fetcher.go:120-264, 380-411`; `types/spec/expand.go:20-103`; `api_collection.go:580-582`; `combinable.go:37-67`; `cobra_common.go:79`; `rpcsmartrouter.go:2567-2581` |
| Fixtures and tests (`TestErrorFixtures`, `TestFaultAxis_EveryRegisteredCodeIsAccountedFor`, `TestFaultAxis_RegistryContract`, REST/gRPC status suites) | `testdata/error_fixtures.json` (102: 97 jsonrpc, 3 rest, 2 grpc); `error_fixtures_test.go:43-47` (three transports), `:56`; `error_fault_axis_test.go:32-164, 203`; `rest_status_rows_test.go:15, 77, 139`; `grpc_rate_limit_vocabulary_test.go:57`; `endpoint_fault_test.go:35`; `direct_rpc_connection_test.go:466`; `unsupported_method_scoring_invariant_test.go:49-61, 86-100`; `error_registry_test.go:42-57` |
| Metrics, readiness; per-interface denominators (`smartrouter_requests_*{spec, apiInterface, provider_address, method}`, `smartrouter_ws_subscription_errors_total{spec, apiInterface}`) | `protocol/metrics/error_metrics.go:21-44`; `smartrouter_metrics_manager.go:438-446, 581-584, 625-648, 766-793, 1688-1700` |
| Images and chart | `docker/Dockerfile.release:26`, `Dockerfile.ci:13`, `docker/Dockerfile:30`, `docker-compose.yml:53`; chart `values.yaml:450-458`, `templates/routers_deployment.yaml:368-369` |
| lava-specs CI | `spec_guards.yml`, `spec_pipeline.yml`, `create_spec.yml`; `check_stateful.sh:97-119, 188`; `check_unused_fields.sh:27`; `check_update_diff.sh` |
| Infra alerting | `grafana/alert-rules/smart-router.yaml`; `scripts/grafana-alert-rules.py`; `tests/grafana-alert-rules/`; `docs/OBSERVABILITY.md:1138-1370` |

## Appendix B. Step 1 coverage (lava-specs `bda05974`)

| Family | Root | Map keys | Specs reaching root | Lose | Gain | Interfaces (specs, root included) |
|---|---|---|---|---|---|---|
| Solana | SOLANA | 4 | 4 | 0 | 1 | jsonrpc |
| Bitcoin | BTC | 8 | 17 | 0 | 10 | jsonrpc (18); rest (5: BTC, BTCT, BTCT4, BCH, BCHT) |
| Cosmos SDK | COSMOSSDK | 28 | 47 | 5 | 25 | rest, grpc, tendermintrpc (48); jsonrpc (10: EVM on CANTO, EVMOS, EVMOST, SEI, SEIT, TSC, TSCT; celestia-node on CELESTIA, CELESTIATA, CELESTIATM) |
| Starknet | STRK | 2 | 0 | 1 | 0 | jsonrpc |
| NEAR | NEAR | 2 | 1 | 0 | 0 | jsonrpc |
| XRP | XRP | 2 | 1 | 0 | 0 | jsonrpc |

Collections in lava-specs: jsonrpc 383 in 106 files, rest 197 in 70, grpc 55 in 32, tendermintrpc
49 in 28, graphql 3 (all in `sui.json`).

Losses: STRKS (add `"imports": ["STRK"]`); COSMOSSDK45DEP, COSMOSWASM, ETHERMINT, IBC, TENDERMINT
(disabled, nothing to lose). Gains (opt out in Step 1 with `"inherit": false`): SOLANAD; BTCS,
BTCT4, DASH, DASHT, PEP, PEPT, QTUM, QTUMT, ZCASH, ZCASHT; AKASH, AKASHT, BABYLON, BABYLONT, DYDX,
DYDXT, ELYS, FETCHAI, FETCHAIT, INJECTIVE, INJECTIVET, KAVA, KAVAT, NEUTRON, NEUTRONT, SEI, SEIT,
SIDET, STRIDE, STRIDET, TSC, TSCT; AGR, AGRT, CANTO.

## References

Rulebook (Notion, copied 2026-11-17): Part I v1.0, frozen 2 Nov 2026, with its change log to 16 Nov
(`40cf5ce8bcd38108bcf3fd93f85e8af5`): the class table, §1 (status, body, bare status), R-0.2, R-A to
R-L, R-O, Q45; the Reference page (`40cf5ce8bcd381b3bb27f9fbf35bd0e0`): Q144, Q183, Q210, Q224,
Q229; Part II GraphQL (`40ef5ce8bcd3818987f5c1e5459e0626`): §3, §5, §7, D9–D18; Part III gRPC
(`415f5ce8bcd381c6afd0ed2011470739`): §3, §5, §8, D10, D12; Part IV JSON-RPC
(`415f5ce8bcd381a5bc5ae50a847c1995`): §3, §5, §7, §8, decision 3, Appendix F (Tendermint); Part V
REST (`416f5ce8bcd381a896dcc677fac5d923`): §3, §5, §7, R-D6. `docs/ERROR-REGISTRY-DESIGN.md`. Jira:
MAG-4910, MAG-4898, MAG-4059, MAG-4748, MAG-4909, MAG-4253, MAG-4413; the interface cleanup epics
MAG-5230 (JSON-RPC and Tendermint: MAG-5242, MAG-5248, MAG-5249, MAG-5279), MAG-5252 (REST:
MAG-5253, MAG-5254, MAG-5256 to MAG-5258), MAG-5126 (gRPC: MAG-5128 to MAG-5132, MAG-5186) and
MAG-5095 (GraphQL: MAG-5121 to MAG-5123); MAG-5232 (writes on every interface); MAG-5246 (chain
families). The record (§3.1) takes whatever those tasks land.
