# Per-chain error classification in lava-specs (MAG-4910, revision 7)

Implementation spec. Approval: Nimrod or Tom, on Must-haves 2 to 4, the format and the split. Blocks
MAG-4898. Baseline for every file:line: smart-router `6c237073`, lava-specs `bda05974`,
smart-router-helm-chart `2446606`, smart-router-infrastructure `99f0dbb4`. The full reasoning and
revision history are in this file's git history (`861d71a2`).

**Decisions that depart from the ticket** (MAG-4910 to be updated):

- Must-have 1 (spec versioning: tags, change log, source pinning, source metric) is out of scope.
  Routers load whatever source they're given; a release is tied only to its bundled commit.
- Chain rows may read inside HTTP 400, 404, 410, 422 and 500 on REST, 400 and 500 on the other HTTP
  interfaces, and gRPC InvalidArgument, NotFound, AlreadyExists, FailedPrecondition, OutOfRange,
  Internal and Unknown (the node's own answers). Every other HTTP status with a router rule, and
  every other gRPC status, is locked. Rows can also match the status, string codes and the error's
  `data` or gRPC details.
- A row may name a verdict the router doesn't register, giving its class (§2.3), so a new chain
  error needs no router release; the ticket's rows map to registered errors. The router derives
  retry and blame from the class. The node versions checked live in the evidence (§4.4), not in the
  row.
- WebSocket subscription errors go through the same record and rows, for metrics and endpoint
  health (§5.7).

**Out of scope:** how the router parses, caches and matches (R&D's design); the per-chain research
(MAG-4898); write-error ranking (MAG-4059); remembering accepted hashes (MAG-4748); gRPC streams;
the router's own replies, when no node's reply goes back (the rulebook's interface chapters).

## 1. Model and invariants

A **row** maps a node's code or message to a **verdict**, named in `error`: a router-owned verdict,
which keeps the router's flags, or the chain's own, which the row gives a **class** (`C2`,
`C3-data`, `C3-capability` or `C5`, §2.3); the router derives the flags that decide retry and blame
from the class. Rows match the **error record** the router builds from a reply (§3.1); they live in
the chain's spec and inherit through its `imports` (§2.4). The router keeps: the record and how
every reply is read, chain shapes included; the flags, what each does (retry, fault, scoring,
hold-off, caching) and which ones each class sets; interface, status and connection rules;
rate-limit detection; the unknown default; write safety; and the **router-owned verdicts**: every
verdict a transport row points at, the 25 the router's logic references by identity
(`CHAIN_TX_ALREADY_KNOWN`, `CHAIN_BLOCK_NOT_FOUND`, `CHAIN_SOLANA_MISSING_LONG_TERM`,
`NODE_RATE_LIMITED`, `NODE_GATEWAY_TIMEOUT`, `NODE_BAD_GATEWAY`, `NODE_ENDPOINT_NOT_FOUND`, 16
protocol, 2 user), all protocol, connection and status verdicts, and `UNKNOWN_ERROR`.

Invariants the router enforces at run time, for every spec from every source:

1. A write fans out once and is re-sent only under the router's own rate-limit rule
   (`relaypolicy.Policy.Decide`). No row can widen that: no class means a rate limit, and no row
   names a rate-limit verdict (§4.2).
2. No spec row runs ahead of the locked prefix (§3.2).
3. A row can't name a `PROTOCOL_*`, rate-limit, `CHAIN_TX_ALREADY_KNOWN` or `UNKNOWN_ERROR` verdict.
   A row naming another router-owned verdict has no `class`: the verdict keeps the router's flags,
   so no spec changes a verdict the router owns.
4. A write can be reported rejected only through a row that matched the chain's own code or text,
   never on a status alone (a bare REST 400, a bare gRPC InvalidArgument), and never for a
   slot-already-used answer, whatever row matched. Until MAG-4748 ships, a slot-used answer carries
   no `Lava-Write-Outcome`; after it, the outcome is `unknown` (rulebook Q45). Either way the client
   gets the node's reply as is (invariant 9).
5. A known write method is stateful whether the spec marks it, clears it or omits it.
6. A chain's effective policy acts only if it matches a signed approval covering its rows, their
   order and its imports.
7. An invalid policy falls back whole to the bundled policy, unrelated chains keep theirs, the
   router boots, and a gauge, log line and alert name the chain and reason.
8. No row's verdict blames the endpoint (a `C5` verdict, or a router-owned one with
   `EndpointAtFault`) for a reply whose status is a readable client status: HTTP 400, 404, 410 or
   422 on REST, HTTP 400 on the other HTTP interfaces, or gRPC InvalidArgument, NotFound,
   AlreadyExists, FailedPrecondition or OutOfRange (§3.2 step 3).
9. Classification never changes what the client receives. When the router returns a node's reply,
   the client gets the status and body as the node sent them, in masked mode too
   (`failed_relay_reply.go:35-38`). Rows only decide how the router treats a reply: retry, endpoint
   health, metrics, and which node's reply the client gets. The router answers in a node's place
   only where it does today: no node answered, or a write is unclear (MAG-4218,
   `withUnclearWriteStatus`).

## 2. Spec format

### 2.1 The `errors` section

```json
{ "index": "SOLANA", "api_collections": [ "..." ],
  "errors": { "schema_version": 1, "rows": [ "..." ] } }
```

- `schema_version`: integer, identical in every section at a lava-specs commit (CI enforces). A
  router drops a section with an unknown version whole; the chain and its importers fall back.
- `rows`: the chain's own rows (§2.2); they run before the rows it inherits through the spec's
  `imports` (§2.4).
- `inherit`: `false` inherits no rows (Step 1's opt-outs, Appendix B). Doesn't affect api
  inheritance.
- Defaults, applied as if written: `rows` `[]`; `inherit` `true`; per row, `scope` `"both"`,
  `interface` every interface the chain serves, `field` `"text"`. A section without rows is valid:
  `{"schema_version": 1, "inherit": false}` leaves the chain no spec rows.
- The section is optional; older routers ignore the key. A missing section means no local rows; the
  chain may still inherit. The approval covers the complete effective result, empty included.

### 2.2 Row

```json
{ "id": "solana.blockhash_not_found", "contains": "blockhash not found",
  "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "class": "C2",
  "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/sdk/src/transaction/error.rs" }
```

| Field | Req. | Rule |
|---|---|---|
| `id` | yes | Unique within the chain after inheritance; `<family>.<snake_case>`. A child replaces a parent's row by repeating its `id`. |
| `code` | match | The record's chain code (§3.1), number or string, matched by JSON type and value: `-32005`, `"sdk/32"`, `"tx_bad_seq"`. |
| `status` | no | The record's status, a readable one only (§4.3): an HTTP status, or a gRPC code name such as `"NotFound"`. |
| `contains` | match | Case-insensitive substring, as `MessageContains`. |
| `regex` | match | Go RE2, case-sensitive unless `(?i)`; must compile. Not with `contains`. |
| `field` | no | What `contains` or `regex` reads: the record's `"text"` (default) or `"detail"` (§3.1); only with one of them. A string is read as its value; an object, array, number or boolean as its compact JSON text (`json.Compact` of the bytes received, keys in the node's order); a null or absent `detail` matches nothing. |
| `error` | yes | The verdict's name: a router-owned verdict a row may name (§4.2), e.g. `CHAIN_STATE_PRUNED`, or the chain's own, `CHAIN_<FAMILY>_*` / `NODE_<FAMILY>_*`; migrated verdicts keep their names. It appears in `smartrouter_errors_total{error_name}`, logs and the `relay_end` event, which key on names (`relay_end.go:598-603`); a chain's own verdict has no number. Clients never see it (invariant 9). |
| `class` | own verdict | The class of the chain's own verdict: `C2`, `C3-data`, `C3-capability` or `C5` (§2.3); one class per name across the chain's effective rows. Absent when `error` is router-owned: that verdict keeps the router's flags. |
| `interface` | no | `jsonrpc`, `tendermintrpc`, `rest`, `graphql`, `grpc`, or a list of them; default every interface the chain serves. Migrated code rows carry `jsonrpc`: the family codes are JSON-RPC codes. |
| `scope` | no | `read`, `write` or `both` (default). `write` rows run only for stateful apis, `read` rows only for the rest. Migrated rows keep `both`, today's reach; narrowing one is a declared behaviour change (§8.1): Agave's `sendTransaction` can answer `-32016`, and a Cosmos simulate (a read) can answer "account sequence mismatch". |
| `source` | yes | Official definition, pinned to a tag or commit, on the router's allowlist (§4.4). The node versions checked are in the evidence (§4.4), not the row. |
| `disabled` | no | `true` removes the inherited row with this `id` for this spec and its children; carries only `id`, `disabled`, optional `note`. |
| `note` | no | One line. (`description` and `title` are rejected key names in lava-specs.) |

A row has at least one of `code`, `contains`, `regex`, and every match key it has must hold. No
header matcher.

### 2.3 Class

The rulebook's classes (Part I v1.0, frozen 2 Nov 2026, with its change log): every provider reply
gets exactly one, decided once. A row gives one of four to the chain's own verdict; the router
assigns the rest.

| Class | Meaning (rulebook) | In a spec row | Router flags today |
|---|---|---|---|
| `C1` | Answer: the node answered, with data or with an empty or null result | not an error | none |
| `C2` | Caller's answer: the request itself is wrong, and every node would say the same. Reason invalid (invalid params, reverted, insufficient funds) or over_limit (a size, depth, range or result limit the chain software enforces on every node) | `class` | `none`, not retryable: not at fault |
| `C3-data` | Cannot serve here, another node may: a "not found" or "pruned" answer, or this node is behind the block or slot asked for | `class` | `data_scope`, retryable: not at fault |
| `C3-capability` | Cannot serve here, another node may: a method or feature is off on this node | `class` | `node_capability`, retryable: not at fault |
| `C4` | Throttled: our traffic is limited for a time: HTTP 429 whatever the body, or a rate-limit or quota answer | never: rate-limit and quota rows are router code (§3.2 step 2.1, §4.2) | `rate_limit`: the URL is held off |
| `C5` | Provider fault: the provider failed: a 5xx (not 501), a 408, a timeout, a dropped connection, a malformed reply, or an "internal error", "unhealthy" or "syncing" answer | `class` | `none`, retryable: at fault |
| `C6` | Provider refuses us: HTTP 401, 402 or 403, whatever the body (gRPC PermissionDenied, Unauthenticated: MAG-5132) | only by naming a router-owned verdict (XRP's `noPermission` row) | `NODE_UNAUTHORIZED`, `NODE_ACCESS_DENIED`: `node_capability`, retryable |
| Unknown (not a class) | An error no rule recognises | never | `UNKNOWN_ERROR`, the router's default: retryable |

Also not classes: a composite reply (a batch, a partial GraphQL reply) is classified per element; a
stale answer is a `C1` (freshness is the consistency gate's). `C3` has two values here because the
router treats the two halves differently (`relay_processor.go:634`,
`rpcsmartrouter_server.go:7202`). The last column is the router at `6c237073`, not the rulebook's
targets (pauses, probes, ejection, `over_limit` counting). Fault as `EndpointAtFault`
(`error_registry.go:136-152`); retry as §3.3 says. A chain's own verdict is `CategoryExternal`; the
validator prints each row's fault decision.

### 2.4 Inheritance

New code in `DoExpandSpec` (today merges only `api_collections`, `expand.go:72-100`; 144 of 158
root files import another).

- Order: the child's rows, then each parent's rows the child didn't replace or disable by `id`, in
  the parents' order.
- Resolve and validate the whole graph before any policy acts. Errors: missing parents, cycles,
  duplicate local ids, mixed schemas, incompatible definitions of one inherited id (the child must
  replace or disable it explicitly), a disabling row naming no inherited id, one name with two
  classes in the chain's effective rows. A diamond yields one copy at first occurrence.
- First commit: STRKS imports nothing today and would lose the 16 Starknet rows → add
  `"imports": ["STRK"]` (expanded apis stay byte-identical, checked with
  `ExpandSpecWithDependencies`, `utils/keeper/spec.go:89`). 36 chains would gain rows (Appendix B)
  → `"errors": {"schema_version": 1, "inherit": false}` in Step 1, opened one by one in Step 3.

### 2.5 Examples

**Solana** (`solana.json`; 14 rows today at `error_classifier.go:25-45`, four shown, migrated
unchanged, code rows with `interface: jsonrpc`). `CHAIN_STATE_PRUNED` is router-owned (used by
`:354-355`, `:413`, `:444`), so its row has no `class`.

```json
"errors": {
  "schema_version": 1,
  "rows": [
    { "id": "solana.block_cleaned_up", "code": -32001, "interface": "jsonrpc", "error": "CHAIN_STATE_PRUNED",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs" },
    { "id": "solana.node_unhealthy", "code": -32005, "interface": "jsonrpc", "error": "NODE_SOLANA_UNHEALTHY", "class": "C5",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs" },
    { "id": "solana.min_context_slot", "code": -32016, "interface": "jsonrpc", "error": "CHAIN_SOLANA_MIN_CONTEXT_SLOT_NOT_REACHED", "class": "C3-data",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs" },
    { "id": "solana.blockhash_not_found", "contains": "blockhash not found", "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "class": "C2",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/sdk/src/transaction/error.rs" }
  ]
}
```

**A child changes a class** (a child of `solana.json`, for this chain and its children only; the
validator reports "node at fault, retry elsewhere"). Every inherited row naming the same verdict is
replaced too, or the chain gives one name two classes (§4.1):

```json
"errors": { "schema_version": 1, "rows": [
  { "id": "solana.blockhash_not_found", "contains": "blockhash not found", "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "class": "C5",
    "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/sdk/src/transaction/error.rs",
    "note": "this cluster's RPC nodes lag; another node may know the blockhash" } ] }
```

**A chain's own verdict** (TON):

```json
"errors": { "schema_version": 1, "rows": [
  { "id": "ton.block_not_found", "status": 500, "contains": "block not found", "interface": "rest", "scope": "read",
    "error": "CHAIN_TON_BLOCK_NOT_FOUND", "class": "C3-data",
    "source": "https://github.com/ton-blockchain/ton/blob/v2024.10/..." } ] }
```

**Import and disable:**

```json
{ "index": "STRKS", "imports": ["STRK"], "api_collections": [ "..." ] }
```
```json
"errors": { "schema_version": 1, "rows": [ { "id": "cosmossdk.account_not_found", "disabled": true } ] }
```

XRP: twelve of its 15 rows move to `ripple.json` as `regex` rows (`xrplMessageStartsWith` is an
anchored regex); all name router-owned verdicts, so none has a `class`. Three stay in code (§5.5).

## 3. Classification at run time

### 3.1 Error record

The router builds one record per failed or refused reply on every interface, and every reader of
the reply uses that record: the senders, the results manager, both cache tiers (today
`classifyCachedNodeError` classifies JSON-RPC and Tendermint only,
`rpcsmartrouter_server.go:2216-2238`) and the subscription managers (Appendix A). Spec rows match
only the record. Router rows (§3.2 steps 2, 4, 5) keep the inputs they read today.

| Field | Content |
|---|---|
| `interface` | The api's interface, one of five (the classifier gets three transports today, `error_classifier.go:768-777`). |
| `status` | The HTTP status, on GraphQL the status the reader derives from the reply (`ClassificationStatus`, `graphqlMessage.go:483-507`); the gRPC code; 0 on a WebSocket frame. |
| `code` | The chain's own code, number or string: `error.code` on JSON-RPC and Tendermint, the GraphQL error's `extensions.code`, or a chain reader's (Cosmos `tx_response` as `"<codespace>/<code>"`, e.g. `"sdk/32"`; an XRP error or engine result). |
| `text` | What today's readers extract: `error.message`; on REST the body's `message`, `error` or `error.message`, else the body (`restMessage.go:99-127`); the GraphQL error's `message`; the gRPC status message; a chain reader's text. |
| `detail` | JSON as received: JSON-RPC and Tendermint `error.data`; on gRPC the status details as an array, each in protojson form, a detail whose type the router can't resolve as `{"@type": "<type URL>"}`. Absent when the reply has none. |
| `in_success` | A chain reader found the error inside a 2xx or gRPC OK reply. |

How a reply is read is router code: each interface's standard shape (the JSON-RPC error object;
the HTTP status; the GraphQL `errors` array, read as today, `graphqlMessage.go:462-481`, with the
extension codes the router maps to statuses, `:48-55`; the gRPC status and its details) and its
chain readers: Cosmos `tx_response` (`restMessage.go:55-60, 83-96`), Tendermint `result.response`
(`tendermintRPCMessage.go:39-51`) and XRP results (`xrplEngineResult.go:64-104`, picked per family,
`jsonRPCChainFamily.go:14-35`). A chain reader that finds an error inside a success makes the reply
a node error for the router's decisions: classified, counted, never taken as a write's success; the
client still gets the reply as is (invariant 9). A new chain shape (MAG-4898) is a router change,
and a write-safety change when a §5.2 signature reads it.

### 3.2 Matching order

For a record on chain X, first match wins:

1. **Connection errors** (as today).
2. **Locked prefix** (router code; Tendermint reads the JSON-RPC rules and GraphQL the REST ones,
   as today):
   1. Explicit rate limits: JSON-RPC `CodeEquals(429)` and the phrases "rate limit", "too many
      requests", "enhance_your_calm" (`error_classifier.go:283-290`); REST `CodeEquals(429)`
      (`:621`); the four `GRPCRateLimitTexts` (`:512-514`), only when the gRPC status is not one of
      3, 5, 10, 11, 12 (today's order); XRP `tooBusy`/`slowDown`. **Not** `HTTPStatusContains(429)`,
      which stays after the chain rows.
   2. Already-known recognisers, keyed by family and interface like §5.2: JSON-RPC "already known",
      "alreadyknown", `(?i)\bknown transaction\b` (`:310, 311, 316`); XRP `tefALREADY` (`:117`).
      (REST 409 is a locked status below.) Other interfaces' answers are added as researched (§5.2).
   3. Locked statuses, decided by the transport rules **on the status alone**:
      - HTTP 401, 402, 403, 405, 406, 407, 408, 409, 411, 412, 413, 414–417, 426, 428, 429, 431,
        451, 501, 502, 503, 504, 520–527, 530 on every interface, plus 404, 410, 422 off REST
        (`:421-449, 555-591`). Statuses with no router rule (418, 425) are not locked.
      - gRPC Canceled, DeadlineExceeded, PermissionDenied, ResourceExhausted, Aborted,
        Unimplemented, Unavailable, DataLoss, Unauthenticated (1, 4, 7, 8, 10, 12, 14, 15, 16;
        `:482-538`), so spec rows see gRPC Unknown, InvalidArgument, NotFound, AlreadyExists,
        FailedPrecondition, OutOfRange and Internal.
      - Classify (status, "") on REST, GraphQL and gRPC, or (0, `HTTP <status>: `) on JSON-RPC and
        Tendermint, per transport; `UNKNOWN_ERROR` if that transport has no status verdict; never
        borrow REST's 409 meaning elsewhere.
      - The status is the record's `status` (today's JSON-RPC rows read it as the sender's anchored
        `HTTP <n>: ` prefix, `direct_rpc_relay.go:716-718`), never a number elsewhere in the body.
        Two fixtures, `evm_http_429` and `evm_http_503`, use `HTTP status <n>:`, which the router
        never produces; they stay with the transport rows.
      - On GraphQL the status is the one the reader derives (§3.1), so the statuses it takes from
        Sui's extension codes (`graphqlMessage.go:33-55`) count like real ones.
      - The REST route-refusal text rows (`:390-393`) run before the status decision (keeps
        `TestRESTMessageRowsBeatTheStatusRow`, `rest_status_rows_test.go:139`).
3. **X's spec rows**, resolved by inheritance, file order, filtered by interface and scope. **No
   blame inside a readable client status**: when the record's status is 400, 404, 410 or 422 on
   REST, 400 on GraphQL, JSON-RPC and Tendermint, or gRPC InvalidArgument, NotFound, AlreadyExists,
   FailedPrecondition or OutOfRange, a matching row whose verdict is at fault (a `C5` verdict, or a
   router-owned one with `EndpointAtFault`) is not applied, whatever its matcher. The reply takes
   step 2.3's status-only decision instead (`USER_INVALID_REQUEST`,
   `NODE_DATA_NOT_HELD`, `CHAIN_STATE_PRUNED`, `USER_INVALID_PARAMS`, or `UNKNOWN_ERROR` where the
   transport has no row: JSON-RPC and Tendermint 400, gRPC AlreadyExists and FailedPrecondition;
   none blames the endpoint, and `UNKNOWN_ERROR` still feeds the availability gate, as today),
   counted as
   `smartrouter_spec_lock_enforced_total{chain_id, apiInterface, rule="status_fault"}`.
   Falling through to the later rows instead would be unsafe: Solana's `-32005` would reach the
   generic `-32005`, a rate limit.
4. **The router's family rows** until the switch deletes them; afterwards the router-owned rows
   that must follow spec rows (XRP `te[fmlr]…` catch-all).
5. **Remaining transport rows.**
6. **`UNKNOWN_ERROR`.**

Step 2 is the only change to today's order. Replayed at `6c237073`: 102 fixtures and 72 keep-set
counterexamples unchanged. Changes by design (locked statuses now win over a matching chain row):
REST 403 "account not found" → `NODE_ACCESS_DENIED`; REST 409 "account sequence mismatch" →
`CHAIN_TX_ALREADY_KNOWN`; REST 402/408/451/501/405 with family text → the status verdict; JSON-RPC
`HTTP 503: …` → `NODE_SERVICE_UNAVAILABLE` (also for `-32602` bodies and `tefPAST_SEQ`, which on a
write still matches its §5.2 signature); JSON-RPC `HTTP 408: …` → `UNKNOWN_ERROR` (no JSON-RPC 408
row); gRPC 14/8/10 "insufficient fee" → `NODE_SERVICE_UNAVAILABLE`/`UNKNOWN_ERROR`/`NODE_ABORTED`;
gRPC 1/4/7/8/15/16 with text a gRPC text row matches ("not implemented",
`error_classifier.go:540-542`) → `UNKNOWN_ERROR` (was `NODE_UNIMPLEMENTED`); once
spec rows act, a family row that blames the endpoint (Solana `-32005`, Bitcoin `-28`/`-10`/`-9`,
Starknet `63`, NEAR `NOT_SYNCED_YET`, XRP `noNetwork`/`amendmentBlocked`/`internal…`) answering
under JSON-RPC HTTP 400 → `UNKNOWN_ERROR` (step 3; no fixture has this shape, and Step 1 counts it
as `differ` if traffic does; no family serving gRPC has a blaming row). Node-answer statuses are
otherwise untouched (REST 400 "unknown block" stays `CHAIN_BLOCK_NOT_FOUND`, REST 500 "is not
available, lowest height" stays `CHAIN_STATE_PRUNED`, Aptos 410 and toncenter 422 keep their
verdicts, gRPC 3 "account sequence mismatch" stays `CHAIN_INVALID_SEQUENCE`, gRPC 5 "account not
found" stays `CHAIN_ACCOUNT_NOT_FOUND`). A gRPC rate limit visible only in reply metadata applies
only when rows return `UNKNOWN_ERROR` (`direct_rpc_relay.go:1208-1209`), hence ResourceExhausted
is locked.

### 3.3 Retry precedence

A verdict's `retryable` decides whether a read tries the next provider; writes follow the stateful
policy regardless (invariant 1). The JSON-RPC and Tendermint sender (`direct_rpc_relay.go:500-501`)
already lets the HTTP status override a verdict's retry decision, and this spec keeps that order:

- Status ≥ 500 (`direct_rpc_relay.go:600-612`): if the verdict would end the relay and the status
  alone wouldn't, the attempt fails over carrying the status's verdict; the verdict decides only
  endpoint health (`retryVerdict`, `rpcsmartrouter/error_mapper.go:39-64`). Solana `-32002` under
  HTTP 500 is classified `CHAIN_SOLANA_SIMULATION_FAILED` (the endpoint isn't blamed) and retried
  as `NODE_INTERNAL_ERROR`.
- Another non-2xx status: the status pass can turn a retryable verdict non-retryable, never the
  reverse (MAG-1870, `direct_rpc_relay.go:722-766`).
- REST, GraphQL and gRPC: the verdict applies as written (TON's 500 row, §2.5). On gRPC a rate limit
  seen only in reply metadata still applies when the rows give `UNKNOWN_ERROR`
  (`direct_rpc_relay.go:1208-1209`).
- WebSocket subscriptions: retry, restore and teardown don't read the verdict (§5.7).

Whether a non-retryable verdict inside a JSON-RPC 5xx should end the relay instead is open (review
613-1, §10).

## 4. Validation (`cmd/specvalidate`, shared by lava-specs CI and the router at boot)

Rules are read from the router's registry, vocabulary and tables, never a copied list. A failing
section makes the chain's policy invalid → fallback (§5.6).

### 4.1 Classes

- A `class` other than `C2`, `C3-data`, `C3-capability`, `C5`, or one the router version doesn't
  know (schema mismatch).
- A row naming the chain's own verdict without a `class`, or a router-owned verdict (the set in §1,
  exported to the validator) with one. A chain wanting another outcome for a router-owned verdict
  names its own.
- One name with two classes in a chain's effective rows.
- A chain's own name outside `CHAIN_<FAMILY>_*` / `NODE_<FAMILY>_*`.
- A migrated verdict whose class doesn't give the flags of the router's definition, while the
  router still carries it (Steps 1–2). The 41 family-only verdicts are the chain's own from Step 1,
  so their rows carry a class.

### 4.2 Verdicts a row may not name

Rate-limit sub-category (`NODE_RATE_LIMITED`, `NODE_LIMIT_EXCEEDED`, `PROTOCOL_RATE_LIMITED`); any
`PROTOCOL_*` (38); `CHAIN_TX_ALREADY_KNOWN`; `UNKNOWN_ERROR`.

### 4.3 Matches a row may not use

Checked per interface the row applies to:

- `status` that is locked or isn't a status of that interface. Readable: HTTP 400, 404, 410, 422,
  500 on `rest`; 400 and 500 on `graphql`, `jsonrpc`, `tendermintrpc`; InvalidArgument, NotFound,
  AlreadyExists, FailedPrecondition, OutOfRange, Internal, Unknown on `grpc`. A client status (all
  but 500, Internal and Unknown) on a row whose verdict is at fault is invalid: it could never apply
  (§3.2 step 3).
- `code: 429`, number or string.
- `contains`/`regex` matching a rate-limit phrase (`NODE_RATE_LIMITED` rows, `GRPCRateLimitTexts`),
  an already-known phrase, an `HTTP <status>: ` prefix of a locked status, a bare locked status
  number as `HTTPStatusContains` reads it, or, on `rest` and `graphql`, a route-refusal phrase
  ("endpoint not found", "route not found", "path not found", "method not allowed",
  `error_classifier.go:390-393`), whatever status the row names. No family row's text overlaps
  those phrases at `6c237073`.

A row with no `status` can't be tied to a status statically; for it the no-blame rule is enforced
per reply (§3.2 step 3).

### 4.4 Source and evidence

`source` on the router's allowlist of node-software and protocol-spec origins (MAG-4898 completes
it), pinned to a tag or commit. CI fetches the file at that commit and requires official evidence
and representative replies for every matcher and regex alternative (longest-literal check is lint
only); each reply names the node software and version it came from, the versions checked. A spec
owner approves matcher + evidence; write rows also need the write-safety owner.

### 4.5 Stateful check

The validator exports the router's write-method table (§5.3) and fails a PR that clears `stateful`
on a listed write. `check_stateful.sh` reads this table instead of its own list (16 names, one of
them a REST route).

## 5. Run-time locks in the router

### 5.1 Locked prefix

§3.2 step 2. Enforced regardless of what validation said about a row's text.

### 5.2 Slot already used is never rejected (rulebook Q45)

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
  | Bitcoin | jsonrpc | -27 | any | `CHAIN_BITCOIN_ALREADY_IN_CHAIN` |
  | Starknet | jsonrpc | 52 | any | `CHAIN_STARKNET_INVALID_NONCE` |
  | Starknet | jsonrpc | 59 | any | `CHAIN_STARKNET_DUPLICATE_TX` |

  Family is the chain's `chainFamilyMap` family. MAG-4898 completes this table and the
  already-known recognisers (§3.2 step 2.2) per node software and interface; each addition is a
  write-safety change (§8.1). Candidates to verify:

  - slot already used: Cosmos code `sdk/32`, Stellar `tx_bad_seq`
    (Horizon 400), Aptos `SEQUENCE_NUMBER_TOO_OLD`, Cardano `BadInputsUTxO` (Blockfrost), Sui
    object versions already used;
  - already known: Cosmos `sdk/19`, TON "duplicate message".

  Bitcoin `-27` / Starknet `59` ("already there") may instead be reported accepted under MAG-4059
  (§10).
- A write with no accepted attempt and any signature match is never reported rejected, on every
  interface, and the client gets the node's reply as is: a match never makes the router answer in
  the node's place (invariant 9). Until MAG-4748 ships the reply carries no `Lava-Write-Outcome`;
  after it, `unknown` (rulebook Q45). The signatures read every record, including a JSON-RPC or
  Tendermint 5xx body carried as `HTTPStatusError` (`direct_rpc_relay.go:600-612`) and an error a
  chain reader found in a success (§3.1).
- Record the signature before retry decisions: no further fan-out, not even via the all-rate-limited
  exception, even if a classifier also calls the answer a rate limit. Accepted siblings follow the
  accepted-result rules; reads classify as usual.
- Write rows stay in code until the side-by-side shows zero write mismatches for 30 days; reads
  switch after 7.

### 5.3 Write methods stay stateful

- Table in router code keyed by interface + name (+ family for generic names like XRP `submit`),
  seeded from `check_stateful.sh:97-119` plus its misses on every interface: JSON-RPC (Bitcoin
  `sendrawtransaction`, `submitpackage`, `bb_sendTransaction`; NEAR `send_tx`; XRP `submit`,
  `submit_multisigned`; Sui `sui_executeTransactionBlock`); REST (Bitcoin `POST /tx/send`; Aptos
  `POST /transactions`, `POST /transactions/batch`; Stellar `POST /transactions`,
  `POST /transactions_async`; TON `POST /sendBoc`, `/sendBocReturnHash`, `/message`; Tron
  `POST /wallet/broadcasttransaction`, `/wallet/broadcasthex`; Cardano `POST /tx/submit`;
  Concordium `PUT /v0/submit…`); gRPC (Sui `ExecuteTransaction`, Concordium `SendBlockItem`).
  GraphQL needs no entry: every mutation is already a write (`chainlib/graphql.go:259-268`).
  MAG-4898 completes it.
- At load (after `DoExpandSpec`, `utils/keeper/spec.go:79`, before the chain parser): a listed api
  not marked `stateful` is loaded stateful; warn per chain; set the gauge
  `smartrouter_spec_lock_enforced{spec, apiInterface, rule="stateful"}` to the number of apis
  forced, written at boot for every chain and interface, zeros included. A gauge, because this
  happens before the first scrape: a counter first scraped at its final value never shows an
  `increase()`. Today all 132 writes are stateful.
- At request time: an undeclared method gets a `Default-` api (`getSupportedApi` →
  `defaultApiContainer`, `base_chain_parser.go:532-560, 579`; `rest.go:158`; via `jsonRPC.go:121`,
  `graphql.go:166`, `grpc.go:137-143`, `tendermintRPC.go:48-54`), today with no `Stateful` and
  `Deterministic: true`. A listed method gets a stateful, non-deterministic default api; count
  `smartrouter_spec_lock_enforced_total{chain_id, apiInterface, rule="stateful_default"}`.
- Identity = the parser's canonical form (verb + route on REST, full service/method on gRPC, root
  field name on GraphQL, never the alias), never a display name or the `Default-` prefix; a
  URI-form Tendermint request (`GET /broadcast_tx_sync?tx=…`, `tendermintRPC.go:97-115`) has the
  JSON-RPC form's identity; family from the router's chain identity. Apply before scope selection,
  cache lookup, provider selection and retry; bypass cache reads and writes whatever
  `deterministic` says; check each batch member before aggregation; disabled methods stay rejected.

### 5.4 Signed approval of the effective policy

- Manifest entry per chain: chain id, schema version, interfaces declared by the expanded spec,
  resolved import graph + section digests, final ordered rows after replacements and removals
  (every field), the lava-specs commit, and the official-source commits and file digests used in
  review. Empty results have an entry.
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

Stay in code: `tooBusy`/`slowDown` → `NODE_RATE_LIMITED` (`error_classifier.go:127`, §3.2 step 2.1);
`tefALREADY` → `CHAIN_TX_ALREADY_KNOWN` (`:117`, §3.2 step 2.2); `te[fmlr]…` catch-all →
`CHAIN_TX_REJECTED` (`:119`, `xrplRejectionCode` `:171`, uses `IsXRPLRejection`), §3.2 step 4, after
the spec rows `tefPAST_SEQ`/`terPRE_SEQ`.

### 5.6 Fallback

A chain's effective policy from the loaded source is replaced by its complete bundled policy when:
unknown `schema_version`; a row breaks §4; no valid approval; an unresolvable import. If the source
as a whole fails (host down, ref missing, not an archive, nothing parses) the router boots on the
bundled specs for everything.

- Resolve external and bundled graphs separately; never mix a bundled parent with external
  overrides. Each dependant falls back whole; unrelated chains keep their policies. Api definitions
  stay from the loaded source unless the whole source failed; the stateful guard still applies.
  Validate the fallback against the interfaces the chain serves. One atomic choice per chain, before
  serving.
- If the bundled specs have no effective rows for the chain, or a served interface is outside the
  bundled approval, the chain gets the prefix and the transport rows, logged as reduced coverage.
  The chain readers (§3.1) and the §5.2 signatures are router code and apply in every state,
  whole-source failure included.
- Log chain + cause; count `smartrouter_spec_errors_section_dropped_total{chain_id, reason}`,
  reason ∈ {`schema_unknown`, `rule_violation`, `approval_missing`, `signature_invalid`,
  `policy_mismatch`, `approval_revoked`, `dependency_invalid`, `source_file_failed`,
  `bundled_no_rows`, `interface_outside_context`}; whole-source failures count
  `smartrouter_spec_source_fallback_total{reason}`, reason ∈ {`host_unreachable`, `ref_not_found`,
  `not_archive`, `no_spec_parsed`}.
- Gauges: `smartrouter_spec_policy_fallback{spec, apiInterface, reason}` = 1 while on bundled policy
  or reduced coverage; `smartrouter_spec_source_fallback{reason}` = 1 while the whole source is
  replaced. Write every reason at boot, zeros included (as `smartrouter_csm_blocked_providers_by_reason`,
  `smartrouter_metrics_manager.go:581-584, 1688-1700`). `/livez` and `/readyz` are unaffected.
- Today a bad source is fatal (`rpcsmartrouter.go:2567-2581`, `:3244-3249`) and bad files beside
  good ones are skipped silently (`utils/keeper/spec.go:111-128`,
  `utils/specfetcher/fetcher.go:380-392`); both become fallbacks (the latter per chain,
  `source_file_failed`).

### 5.7 WebSocket subscriptions

- A subscribe the node refuses, and an error frame delivered to an open subscription
  (`listenForUpstreamMessages`, `direct_ws_subscription_manager.go:1386-1398`), on `jsonrpc` and
  `tendermintrpc`, each build a record (`status` 0; `code`, `text` and `detail` from the frame's
  error object) and go through §3.2 with the subscribe api's scope. Submit-and-watch methods are
  writes: a §5.2 match is never reported rejected. The client gets the node's frame as the node
  sent it. relay_end uses the same verdict.
- The verdict is counted in `smartrouter_errors_total` and decides the URL's availability as on the
  relay path (`shouldFailSessionForResult`, `rpcsmartrouter_server.go:2585-2620`): a rate limit
  holds it off; a verdict that path scores (`UNKNOWN_ERROR` included) counts a failure
  (`AppendRelayFailure`); anything else leaves it alone. Today every refusal but a 429 counts a
  failure (`direct_ws_subscription_manager.go:444-458`, called at `:645, 656`), and a delivered
  error frame ends the subscription with no count and no health effect (`:1386-1398`).
- Selection, retry, restore and teardown are unchanged by this design. Connect, upgrade and
  transport failures keep the connection rules.

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

**Step 0 (no spec row read yet).** Visible changes: boot on bundled specs instead of
refusing; writes relayed stateful even when cleared or undeclared; a slot-already-used answer
ends a write's fan-out; locked statuses decide their own replies; WebSocket subscription errors
counted and judged by their verdict (§5.7); node errors served from cache get their verdict on
every interface; `smartrouter_errors_total` gains `apiInterface`. Deliverables:
`compatibility.json`, `SPEC_COMMIT`, `make specs`, folder-matches-commit check, bundled specs in
the image, source fallback (§6, §5.6); `cmd/specvalidate`, schema, `spec_guards.yml` jobs,
owner-review and evidence checks, approval pipeline (§4, §8.1); the error record on every
interface and reader, with `apiInterface` passed to classification and the Cosmos reader's code
(§3.1); locked prefix and router-owned XRP rows (§3.2, §5.5); exported router-owned set and the
class checks (§4.1); write-method table with both locks and per-interface identity (§5.3);
slot-already-used signatures and recognisers with their interface column (§5.2); the WebSocket
rules (§5.7); Helm default → bundled.

**Step 1 (side by side).** Copy the six family tables (minus the three XRP rows) into their root
specs unchanged. A row naming one of the 41 family-only verdicts carries the class of today's
flags; all fit one of the four. A row naming one of the 17 verdicts also used by transport rows or
logic names a router-owned verdict and carries none. The one family-table verdict no class fits,
`NODE_RATE_LIMITED`, is named only by an XRP row that stays in code. STRKS imports STRK; the 36
gaining chains get `"inherit": false` (Appendix B). The reader loads rows, evaluates them beside the
code, acts on the code. Reader constraints: registry maps are init-only and lock-free,
`registerError` panics on duplicates (`error_registry.go:602-629`) → a chain's own verdicts live in
a per-chain policy outside `errorRegistry`; `ClassifyError` is keyed by `ChainFamily`
(`error_classifier.go:788, 801`) → key by chain policy; router logic compares its verdicts by
pointer (`svm_block_hash_retry.go:99`, `read_your_writes.go:230`,
`rpcsmartrouter_server.go:1599-1603`) → a row naming a router-owned verdict resolves to the
router's own `*LavaError`; `LavaError.Is` and `LavaWrappedError.Is` match
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
| tables deleted (next release) | prefix, spec rows, router-owned rows, transport | same |

The setting can't exceed the approved stage (§5.4); an early `all` logs and stays. Record the policy
digest, window start/end and observed coverage per chain, interface and scope (denominators:
`smartrouter_requests_read_total` and `smartrouter_requests_write_total`, summed by `spec` and
`apiInterface`); a policy change or mismatch restarts the window; exercise unused rows with
fixtures. Rollback before deletion = previous setting + artifact; after deletion = previous router
release + bundle. The family tables and the 41 code verdict definitions are deleted in the release
after `all` has passed one canary (`chainFamilyMap` stays: fault axis, transport rows, signatures,
chain readers, `IsSolanaFamily` `protocol/common/svm.go:10-13`); that release bundles an
`all`-approved policy and keeps the bundled policy for any chain whose external policy has only
`shadow`/`reads` permission.

**Step 3.** MAG-4898 lands researched rows, new verdicts included; the 36 gains open by removing
`"inherit": false`, each a behaviour change with product sign-off and, for a new verdict or class,
R&D review.

## 8. Gates

### 8.1 lava-specs CI, every PR

- JSON Schema for `errors` on every changed spec.
- `cmd/specvalidate`, built from each router release the boot matrix covers.
- Stateful check (§4.5) as a `spec_guards.yml` job.
- Write-safety job: validator write rules + diff of effective policies for every affected chain and
  descendant (row contents by `id`, order separately); any change touching a `write` or `both` row
  needs write-owner review via a required check (labels are informational). A new
  `CODEOWNERS` file names the owners teams (spec owners, write-safety owners, R&D); since it can't
  target a JSON section, the required checks compute what changed and demand the right approval on
  the current head.
- Source evidence (§4.4); every root and descendant policy passes validation and compatibility
  before its digest can be signed.
- R&D review (required check) for a new verdict or a class change in any chain's effective rows;
  also fails a migrated verdict whose class no longer gives the router's flags.
- Behaviour changes named in the PR description (old → new verdict/class, why) with a
  `behaviour-change` label; replay and coverage check fail otherwise.
- `check_unused_fields.sh` moves into `spec_guards.yml`.
- Testing and signing are separate: PR jobs validate with an isolated test trust root; after merge a
  trusted job re-checks the exact snapshot, verifies owner approvals and publishes a candidate
  artifact for the canary (separate trust context); after the canary the promotion job signs the
  production manifest for those digests and stage. General deployments consume only promoted
  artifacts.

### 8.2 Against the router

- Boot matrix: every changed chain (descendants included) boots with the PR's specs on every router
  release in service from `min_router` up; a dropped section fails unless declared.
- Replay: the 102 fixtures in `protocol/common/testdata/error_fixtures.json` plus
  `TestRESTStatusRows`, `TestRESTStatusRowsDoNotReachOtherTransports`,
  `TestRESTMessageRowsBeatTheStatusRow`, `TestGRPCRateLimitTexts_TheCodeRowsAboveStillDecide`,
  `TestEndpointFault_AnswerShapes`, `TestGRPCStatusError` give the same verdict name and flags unless
  declared; fixtures carry the expected class. `TestFaultAxis_RegistryContract`'s rows for the 41
  (`error_fault_axis_test.go:151-164`) move to a fixture read from the bundled sections.
- Fixtures per interface. The schema gains `interface` (five values), `status`, `body` (or frame),
  `details` (gRPC), `in_success`, and the node software and version. Each fixture runs through its
  interface's reader and record, not `ClassifyError` alone (the harness takes three transports
  today, `error_fixtures_test.go:43-47`; 97 of the 102 are JSON-RPC, none Tendermint or GraphQL).
  Every row needs a fixture on each interface it applies to. The five fixtures whose status exists
  only as text (`rest_404_not_found`, `rest_429_rate_limit`, `rest_503_unavailable`,
  `evm_http_429`, `evm_http_503`) keep running through `ClassifyError`.
- Counterexamples. Keep today's verdict: Bitcoin `-26` "min relay fee not met, 429 < 1000" and
  "mempool min fee not met, 429 < 1000", Agave `-32005` "Node is behind by 429 slots"; a rate-limit
  phrase inside gRPC 3/5/10/11/12; JSON-RPC code 429; XRP router-owned rows and other engine results
  with/without `HTTP <status>: `; each already-known answer (EVM "already known", Nethermind
  "AlreadyKnown", Besu "Known transaction") on EVM, Solana, Bitcoin, Starknet, Cosmos; each
  slot-already-used signature as a full pair. Take the router's verdict: REST 403, 409, 402, 408,
  451, 501 with family text; gRPC 14, 8, 10; JSON-RPC `HTTP 503:` and `HTTP 408:`. Text rows beat
  status: REST 403/405 "route not found" → `NODE_ENDPOINT_NOT_FOUND`. Stay readable: REST 400
  "unknown block", 500 "is not available, lowest height", 410 `version_pruned`, 422
  `failed to parse ton_addr`, 404 with a chain's own `not_found`; gRPC 3 "account sequence
  mismatch", gRPC 5 "account not found". Fail validation: `{status: 408, contains}`,
  `{status: 402, contains}`, `{status: "Unavailable", contains}`, `{status: 404, contains: "not
  found"}` with no `interface` on a chain that serves JSON-RPC, a row with `status` 400 or
  `"InvalidArgument"` naming `NODE_INTERNAL_ERROR`, `{contains: "route not found"}` on `rest`.
  Isolate the status: JSON-RPC `-32602` with HTTP 503 and
  HTTP 503 "insufficient funds" → `NODE_SERVICE_UNAVAILABLE`; HTTP 401 keeps its per-transport
  verdict; JSON-RPC 409 → `UNKNOWN_ERROR`; a body beginning `HTTP 503: ` without a transport status
  isn't locked. Relay-policy: a stateful write whose every answer is the min-fee rejection stops
  after fan-out and holds nothing off.
- Attack row as a loaded spec row: `{code: -32000}` → `CHAIN_TX_REJECTED` vs EVM "already known":
  no verdict change.
- No blame inside a readable client status (§3.2 step 3): spec rows naming `NODE_INTERNAL_ERROR`
  (router-owned, at fault), a `contains` and a `regex` one matched under REST 400, 404, 410 and
  422, GraphQL 400, JSON-RPC HTTP 400, and gRPC InvalidArgument, NotFound, AlreadyExists,
  FailedPrecondition and OutOfRange, and a code-only one under JSON-RPC HTTP 400 and GraphQL 400,
  each give the status-only verdict and count `rule="status_fault"`; the same rows on an HTTP 200
  JSON-RPC error and under gRPC Internal apply; Solana `-32005` under JSON-RPC HTTP 400 gives
  `UNKNOWN_ERROR`, never the generic `-32005` rate limit.
- Retry precedence (§3.3), end to end through `relayInnerDirect` with spec-loaded rows: JSON-RPC
  HTTP 500 + Solana `-32002` is classified `CHAIN_SOLANA_SIMULATION_FAILED`, the endpoint isn't
  blamed, and the attempt fails over as `NODE_INTERNAL_ERROR`; a non-2xx status
  whose status pass is non-retryable makes a retryable spec verdict non-retryable; REST 500 and gRPC
  Internal with a non-retryable spec verdict end the relay. The cases in
  `direct_http_status_body_test.go:92, 140` and
  `TestDirectRPCRelaySender_HTTPStatusOverridesRetryableBodyCode_MAG1870`
  (`direct_rpc_integration_test.go:530`) pass unchanged with the rows loaded from specs.
- Slot-already-used: every signature as a full pair through the stateful path of each interface it
  lists is never reported rejected, and the client gets the node's reply byte for byte with no
  `Lava-Write-Outcome`; the same pair on a read keeps its verdict; every fixture with
  `CHAIN_NONCE_TOO_LOW`, `CHAIN_INVALID_SEQUENCE` or `CHAIN_STARKNET_INVALID_NONCE` matches a
  signature; mixed with rate-limit answers, or matching a rate-limit phrase too, no second fan-out;
  include an accepted sibling and prefixed XRP replies.
- Reply as is (invariant 9), on every interface, in masked and unmasked mode: a reply a spec row
  classifies and a success a chain reader marks (Cosmos `tx_response`, Tendermint `result.response`,
  XRP `tefPAST_SEQ`) reach the client with the node's status and body byte for byte; a write whose
  copies all answered with marked replies returns one of them; a bare REST 400 or gRPC
  InvalidArgument on a write goes back as is, with no `Lava-Write-Outcome`.
- Coverage: each chain's effective rows vs the PR's base commit; gains, losses, reorders or class
  changes fail unless declared and approved.
- `field: "detail"`: a string `error.data` is read as its value, an object or array as its compact
  JSON, a null or absent `detail` matches nothing; on gRPC, details in protojson form and an
  unresolvable type as `{"@type": …}`; a row with `field` but no `contains` or `regex` fails
  validation.
- Generated row tests: per changed row a source-backed positive reply and a near-miss (one per
  regex alternative), tested against the complete effective policy, recording the winning row,
  verdict and class, reporting unreachable rows.
- WebSocket (§5.7), on `jsonrpc` and `tendermintrpc`: a refused subscribe and an error frame
  delivered to the subscription's error channel (injected there on `tendermintrpc`) are counted
  with their verdict and `apiInterface`; a client-fault refusal counts no failure, an unclassified
  or at-fault one counts a failure, a rate-limited one holds the URL off; restore and teardown are
  unchanged.

### 8.3 Router repo tests (bad source, locks, fallback)

- No `--use-static-spec`, non-archive, missing ref, silent host, unparseable spec → bundled specs
  with the right `smartrouter_spec_source_fallback_total` reason.
- Each §4 rule, missing/wrong-key/revoked approval, unknown schema → the chain's bundled policy.
  Class cases: an unknown class, a chain's own verdict without a class, a router-owned verdict with
  one, one name with two classes in a chain, a migrated verdict whose class no longer gives the
  router's flags, a row naming a §4.2 verdict, each fail; a chain's own verdict loads and the relay
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
  elsewhere, read from the collector right after load); omitted → relayed stateful and uncached,
  `rule="stateful_default"` counted; repeat cleared/omitted/disabled per write interface (a
  URI-form Tendermint `broadcast_tx_sync`, Aptos `POST /transactions/batch`, gRPC `BroadcastTx`
  included), batches, cache bypass, `deterministic: true` on a declared write.
- Every fallback case asserts the gauges (1 for the affected chain/interface/reason, 0 elsewhere,
  0 after reload with a valid policy, read from the collector).
- Alert rules proven in smart-router-infrastructure by extending the promtool replay in
  `tests/grafana-alert-rules/test_silent_when_idle.py:129-156` (render per-tenant rules via
  `expand_tenant`; gauge 1 past `for` fires, back to 0 resolves). For `sr-spec-lock-enforced`: the
  lock gauge at 1 from the first sample fires (the startup case an `increase()` misses), a counter
  first observed at 1 and flat also fires, and both resolve when the series reset after a
  restart. Plus `scripts/grafana-alert-rules.py check`; CI
  `.github/workflows/grafana-plan-pr.yaml:121-124`. Publish rules only after a router emitting the
  gauges is on the fleet (`check` can't see whether a metric exists, `docs/OBSERVABILITY.md:1325-1330`).

### 8.4 Canary

One canary router per cluster, one full day of production traffic (minimum; the 7/30-day windows of
§7 still apply). Compare `smartrouter_errors_total` by `error_name`, `chain_id` and `apiInterface`
(`protocol/metrics/error_metrics.go:42-44`); dropped-section and source-fallback counters at zero,
the lock gauge and counter at zero, both fallback gauges 0. On-prem gets the tested pair only. Failed canary → previous
artifact (and router release if the bundle changed); the next release bundles the promoted commit.

## 9. Observability

| Signal | Purpose |
|---|---|
| `smartrouter_spec_errors_section_dropped_total{chain_id, reason}` | rejected section, with reason |
| `smartrouter_spec_source_fallback_total{reason}` | source couldn't load |
| `smartrouter_spec_lock_enforced{spec, apiInterface, rule}` (gauge) | writes forced stateful at load (`stateful`), as state since boot |
| `smartrouter_spec_lock_enforced_total{chain_id, apiInterface, rule}` | request-time enforcement: `stateful_default`, `status_fault` |
| `smartrouter_spec_error_rows_mismatch_total{…}`, `smartrouter_spec_error_rows_hits_total{…}` | switch criterion; which rows matter |
| `smartrouter_spec_policy_fallback{spec, apiInterface, reason}`, `smartrouter_spec_source_fallback{reason}` | current fallback state; alerts read them |
| `smartrouter_errors_total{error_name, error_category, retryable, chain_id, apiInterface}` | gains `apiInterface` (five values; WebSocket errors included, §5.7); a chain's own verdicts appear as `error_name`; a child's class change means one name may carry different `retryable` per `chain_id`, so update the 1:1 comment and the "~100 codes" budget (`error_metrics.go:21-28`) and dashboards |

Alert rules in smart-router-infrastructure `grafana/alert-rules/smart-router.yaml`, house style
(`uid`, `title` and `summary` naming `{tenant}`, `for`, `== bool`/`> bool`, `severity` only label,
description with the first operator action), all `warning`:

| Rule | Expr | For |
|---|---|---|
| `sr-spec-policy-fallback` | `max by (cluster, location, router, spec, apiInterface, reason) (smartrouter_spec_policy_fallback{cluster="{tenant}"}) == bool 1` | 10m |
| `sr-spec-source-fallback` | `max by (cluster, location, router, reason) (smartrouter_spec_source_fallback{cluster="{tenant}"}) == bool 1` | 30m |
| `sr-spec-lock-enforced` | `(max by (cluster, location, router, spec, apiInterface, rule) (smartrouter_spec_lock_enforced{cluster="{tenant}"}) > bool 0) or (max by (cluster, location, router, chain_id, rule) (smartrouter_spec_lock_enforced_total{cluster="{tenant}"}) > bool 0)` | 0s |

`sr-spec-lock-enforced` reads values, not `increase()`, so load-time enforcement that happens before
the first scrape still fires. It stays firing until the spec is fixed and the router restarts; the
description names the rule (`stateful`, `stateful_default`, `status_fault`).

## 10. Open decisions

| Decision | Needed before |
|---|---|
| Update MAG-4910 for the four departures | approval is recorded |
| Bitcoin `-27` / Starknet `59` ("already there"): a slot-used signature or accepted (MAG-4059) | Step 0 write-safety ships |
| Whether a non-retryable verdict inside a JSON-RPC 5xx should end the relay instead of failing over (review 613-1, `error_mapper.go:39-45`); until then the status decides retry (§3.3) | a spec relies on `retryable: false` under a JSON-RPC 5xx |
| Initial write-method table | Step 0 write guards ship |
| Docs: verdict names can come from a chain's spec, with no number (`ERROR-REGISTRY-DESIGN.md`, public docs, `docs/RELAY-END-EVENT.md:65, 138` say "error registry name") | Step 2 |
| Source evidence for migrated rows; the R&D owners team for verdict and class review | Step 1 |
| Manifest delivery, signing service, key custody, rotation, revocation; candidate trust context | any external policy acts |
| Generic geth-wording rows (`error_classifier.go:269-346`, `:241`): keep generic or move to ETH1, after 30 days of hits data; the router's vendor rows (Alchemy `-32600`, QuickNode's `blocked …` row): keep or remove | those behaviour changes ship |
| Signature and already-known candidates per interface (§5.2): MAG-4898's evidence for each | each ships |
| create-spec agent keeps unknown keys in update modes by construction; new-chain mode needs a template (MAG-4898 part 3); add a test in `scripts/test_*.sh` | Step 3 |
| Effort: ticket said 6–8 engineer-weeks; revision 2 split ≈9; the approval pipeline, request-time lock, wider prefix, chain-defined verdicts and interface parity (record, per-interface fixtures, WebSocket) add to it | R&D sizes |

Later: customer overrides as an overlay file (read rows, same validator, own approval path); spec
versioning if picked up again.

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
| Chain readers (stay router code, §3.1) | Cosmos `restMessage.go:55-60, 83-96`; Tendermint `tendermintRPCMessage.go:39-51`; XRP `xrplEngineResult.go:64-104`, `jsonRPCChainFamily.go:14-35`; GraphQL extension codes `graphqlMessage.go:48-55` (stay router code) |
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

Rulebook Part I v1.0, frozen 2 Nov 2026, with its change log to 16 Nov (Notion
`40cf5ce8bcd38108bcf3fd93f85e8af5`): classes C1–C6, R-M, R-L, R-K, R-0.2, Q45; the gRPC status
table in MAG-5132. `docs/ERROR-REGISTRY-DESIGN.md`. Jira: MAG-4910, MAG-4898, MAG-4059,
MAG-4748, MAG-5128. The rulebook's interface chapters change how each reader judges a reply
(MAG-5230 JSON-RPC and Tendermint, MAG-5252 REST, MAG-5095 GraphQL, MAG-5126 gRPC); the record
(§3.1) takes whatever they land.
