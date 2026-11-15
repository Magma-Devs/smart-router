# Per-chain error classification in lava-specs (MAG-4910, revision 5)

Implementation spec. Approval: Nimrod or Tom, on Must-haves 2 to 4, the format and the split. Blocks
MAG-4898. Baseline for every file:line: smart-router `6c237073`, lava-specs `bda05974`,
smart-router-helm-chart `2446606`, smart-router-infrastructure `99f0dbb4`. The full reasoning and
revision history are in this file's git history (`861d71a2`).

**Decisions that depart from the ticket** (MAG-4910 to be updated):

- Must-have 1 (spec versioning: tags, change log, source pinning, source metric) is out of scope.
  Routers load whatever source they're given; a release is tied only to its bundled commit.
- Chain rows may read text inside HTTP 400, 404, 410, 422 and 500 on REST and 400 and 500 on other
  interfaces (the node's own answers). Every other HTTP status with a router rule, and every gRPC
  status the router has decided, is locked.
- Each chain's spec defines the verdicts its rows point at, with the flags that decide retry and
  fault. The router keeps what each flag does and the verdicts its own logic references.

**Out of scope:** how the router parses, caches and matches (R&D's design); the per-chain research
(MAG-4898); write-error ranking (MAG-4059); remembering accepted hashes (MAG-4748).

## 1. Model and invariants

A **row** says which node code or message means which **verdict**. A **verdict** is a name, a code
and flags (`sub_category`, `retryable`, `class`). Both live in the chain's spec and inherit through
`imports`. The router keeps: the flag vocabulary and what each flag does (retry, fault, scoring,
hold-off, caching); interface, status and connection rules; rate-limit detection; the unknown
default; write safety; and the **router-owned verdicts**: every verdict a transport row points at,
the 25 the router's logic references by identity (`CHAIN_TX_ALREADY_KNOWN`, `CHAIN_BLOCK_NOT_FOUND`,
`CHAIN_SOLANA_MISSING_LONG_TERM`, `NODE_RATE_LIMITED`, `NODE_GATEWAY_TIMEOUT`, `NODE_BAD_GATEWAY`,
`NODE_ENDPOINT_NOT_FOUND`, 16 protocol, 2 user), all protocol, connection and status verdicts, and
`UNKNOWN_ERROR`.

Invariants the router enforces at run time, for every spec from every source:

1. A write fans out once, then stops, unless every failure was a rate limit; no row may name a
   rate-limit verdict (§4, §5.3).
2. No spec row runs ahead of the locked prefix (§3).
3. A row can't name a `PROTOCOL_*`, rate-limit, `CHAIN_TX_ALREADY_KNOWN` or `UNKNOWN_ERROR` verdict;
   a spec can't define a rate-limit or internal verdict or redefine a router-owned name or code.
4. A write that fails with a slot-already-used answer is reported unknown, whatever row matched.
5. A known write method is stateful whether the spec marks it, clears it or omits it.
6. A chain's effective policy acts only if it matches a signed approval covering rows, verdicts,
   order and imports.
7. An invalid policy falls back whole to the bundled policy, unrelated chains keep theirs, the
   router boots, and a gauge, log line and alert name the chain and reason.

## 2. Spec format

### 2.1 The `errors` section

```json
{ "index": "SOLANA", "api_collections": [ "..." ],
  "errors": { "schema_version": 1, "imports": ["..."], "verdicts": [ "..." ], "rows": [ "..." ] } }
```

- `schema_version`: integer, identical in every section at a lava-specs commit (CI enforces). A
  router drops a section with an unknown version whole; the chain and its importers fall back.
- `imports` (optional): specs whose rows and verdicts this section inherits. Absent = the spec's own
  `imports`; `[]` = inherit nothing. Doesn't affect api inheritance.
- `verdicts` (optional): the chain's own verdicts. `rows`: the classification rows.
- The section is optional; older routers ignore the key. A missing section means no local rows; the
  chain may still inherit. The approval covers the complete effective result, empty included.

### 2.2 Row

```json
{ "id": "solana.blockhash_not_found", "match": { "contains": "blockhash not found" },
  "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "interface": "jsonrpc", "scope": "write",
  "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/sdk/src/transaction/error.rs",
  "versions": ["agave 2.1"] }
```

| Field | Req. | Rule |
|---|---|---|
| `id` | yes | Unique within the chain after inheritance; `<family>.<snake_case>`. A child overrides a parent's row by repeating it. |
| `match` | yes | Exactly one of `{"code": N}`, `{"contains": s}`, `{"regex": re2}`, `{"code": N, "contains": s}`. `contains` case-insensitive (as `MessageContains`); `regex` Go RE2, case-sensitive unless `(?i)`, must compile. No status, gRPC-code or header matcher. |
| `error` | yes | A verdict name the chain defines or inherits, or an unlocked router-owned one (e.g. `CHAIN_STATE_PRUNED`). |
| `interface` | no | `jsonrpc`, `rest`, `grpc`, `graphql`, `tendermintrpc`; absent = every interface the chain serves. |
| `scope` | yes | `read`, `write` or `both`. `write` rows run only for stateful apis, `read` rows only for the rest. |
| `source` | yes | Official definition, pinned to a tag or commit, on the router's allowlist (§4.4). |
| `versions` | yes | Node software versions checked; at least one. |
| `slot_used` | no | `true` adds the row's answers to the slot-already-used group; `write`/`both` rows only; widens, never narrows. |
| `disabled` | no | `true` removes the inherited row with this `id` for this spec and its children; carries only `id`, `disabled`, optional `note`. |
| `note` | no | One line. (`description` and `title` are rejected key names in lava-specs.) |

### 2.3 Verdict

```json
{ "name": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "code": 3304, "sub_category": "none",
  "retryable": false, "class": "C6", "note": "a blockhash the cluster no longer accepts" }
```

| Field | Req. | Rule |
|---|---|---|
| `name` | yes | One origin spec per name across lava-specs; never router-owned. A chain that inherits a verdict overrides it by repeating the name with the fields it changes. New names: `CHAIN_<FAMILY>_*` / `NODE_<FAMILY>_*`; migrated verdicts keep their names. Clients never see it (node bodies are forwarded unchanged, `failed_relay_reply.go:35-38`); it appears in `smartrouter_errors_total{error_name}`, logs and the `relay_end` event. |
| `code` | on definition | Integer, unique across lava-specs, never router-owned. An override omits it or repeats it unchanged. Migrated verdicts keep their codes; new ones use the reserved range (proposal 5000–5999, router uses 1001–4007; §10). Logs and `relay_end` only, not a metric label (`error_metrics.go:37-40`). |
| `sub_category` | yes | `none`, `data_scope` or `node_capability`. Fault axis as `EndpointAtFault` (`error_registry.go:136-152`): `data_scope`/`node_capability` never at fault and must be `retryable: true`; `none` + retryable = node at fault; `none` + non-retryable = caller's fault. |
| `retryable` | yes | Whether a read tries the next provider. Writes follow the stateful policy regardless. |
| `class` | yes | Rulebook class C2–C7; informational. |
| `note` | no | One line. |

Every spec verdict is `CategoryExternal`; there is no `category` field. There is no
`may_have_reached_node` field (the router reads it only on transport failures,
`rpcsmartrouter_server.go:1498`, `relay_end.go:588`). The validator prints each verdict's computed
fault decision.

**Example: Solana** (`solana.json`; 14 rows today at `error_classifier.go:25-45`, four shown).
`CHAIN_STATE_PRUNED` (3204) is router-owned (used by `:354-355`, `:413`, `:444`) and only named.

```json
"errors": {
  "schema_version": 1,
  "verdicts": [
    { "name": "NODE_SOLANA_UNHEALTHY", "code": 2150, "sub_category": "none", "retryable": true, "class": "C3" },
    { "name": "CHAIN_SOLANA_MIN_CONTEXT_SLOT_NOT_REACHED", "code": 3311, "sub_category": "data_scope", "retryable": true, "class": "C6" },
    { "name": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "code": 3304, "sub_category": "none", "retryable": false, "class": "C6" }
  ],
  "rows": [
    { "id": "solana.block_cleaned_up", "match": { "code": -32001 }, "error": "CHAIN_STATE_PRUNED", "scope": "read",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs", "versions": ["agave 2.1"] },
    { "id": "solana.node_unhealthy", "match": { "code": -32005 }, "error": "NODE_SOLANA_UNHEALTHY", "scope": "both",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs", "versions": ["agave 2.1"] },
    { "id": "solana.min_context_slot", "match": { "code": -32016 }, "error": "CHAIN_SOLANA_MIN_CONTEXT_SLOT_NOT_REACHED", "scope": "read",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs", "versions": ["agave 2.1"] },
    { "id": "solana.blockhash_not_found", "match": { "contains": "blockhash not found" }, "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "scope": "write",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/sdk/src/transaction/error.rs", "versions": ["agave 2.1"] }
  ]
}
```

**Example: a chain-specific verdict** (TON, MAG-4061). Note that `shouldFailSessionForResult`
(`rpcsmartrouter_server.go:2585-2620`) scores every 5xx before reading the fault flags, so this row
changes the verdict but not the endpoint's availability; that half of MAG-4061 is separate router
work.

```json
"errors": { "schema_version": 1,
  "verdicts": [ { "name": "CHAIN_TON_BLOCK_NOT_FOUND", "code": 5001, "sub_category": "data_scope", "retryable": true, "class": "C6" } ],
  "rows": [ { "id": "ton.block_not_found", "match": { "code": 500, "contains": "block not found" },
    "error": "CHAIN_TON_BLOCK_NOT_FOUND", "interface": "rest", "scope": "read",
    "source": "https://github.com/ton-blockchain/ton/blob/v2024.10/...", "versions": ["ton 2024.10"] } ] }
```

**Example: import and disable:**

```json
{ "index": "STRKS", "imports": ["STRK"], "api_collections": [ "..." ] }
```
```json
"errors": { "schema_version": 1, "rows": [ { "id": "cosmossdk.account_not_found", "disabled": true } ] }
```

XRP: twelve of its 15 rows move to `ripple.json` as `regex` rows (`xrplMessageStartsWith` is an
anchored regex); all point at router-owned verdicts, so it defines none. Three stay in code (§5.5).

### 2.4 Inheritance

New code in `DoExpandSpec` (today merges only `api_collections`, `expand.go:72-100`; 144 of 158
root files import another).

- Order: the child's rows, then each parent's rows the child didn't override or disable by `id`, in
  the parents' order. Verdicts are keyed by `name`; an override changes `sub_category`,
  `retryable`, `class`, `note`, never `code`. A verdict can't be removed.
- A row's `error` must resolve, after inheritance, to a defined/inherited verdict or an unlocked
  router-owned one; otherwise the policy is invalid.
- Resolve and validate the whole graph before any policy acts. Errors: missing parents, cycles,
  duplicate local ids or names, mixed schemas, incompatible definitions of one inherited id or name
  (the child must override or disable explicitly), a disabling row naming no inherited id. A
  diamond yields one copy at first occurrence.
- Repeating a name the chain doesn't inherit is a second definition and fails (§4.1).
- First commit: STRKS imports nothing today and would lose the 16 Starknet rows → add
  `"imports": ["STRK"]` (expanded apis stay byte-identical, checked with
  `ExpandSpecWithDependencies`, `utils/keeper/spec.go:89`). 36 chains would gain rows (Appendix B)
  → `"errors": {"schema_version": 1, "imports": []}` in Step 1, opened one by one in Step 3.

## 3. Matching order

For a node error on chain X, first match wins:

1. **Connection errors** (as today).
2. **Locked prefix** (router code):
   1. Explicit rate limits: JSON-RPC `CodeEquals(429)` and the phrases "rate limit", "too many
      requests", "enhance_your_calm" (`error_classifier.go:283-290`); REST `CodeEquals(429)`
      (`:621`); the four `GRPCRateLimitTexts` (`:512-514`), only when the gRPC status is not one of
      3, 5, 10, 11, 12 (today's order); XRP `tooBusy`/`slowDown`. **Not** `HTTPStatusContains(429)`,
      which stays after the chain rows.
   2. Already-known recognisers: JSON-RPC "already known", "alreadyknown", `(?i)\bknown transaction\b`
      (`:310, 311, 316`); XRP `tefALREADY` (`:117`). (REST 409 is a locked status below.)
   3. Locked statuses, decided by the transport rules **on the status alone**: HTTP 401, 402, 403,
      405, 406, 407, 408, 409, 411, 412, 413, 414–417, 426, 428, 429, 431, 451, 501, 502, 503, 504,
      520–527, 530 on every interface, plus 404, 410, 422 off REST (`:421-449, 555-591`); gRPC 1, 3,
      4, 5, 7, 8, 10, 11, 12, 14, 15, 16 (`:482-538`), so spec rows see only gRPC Unknown,
      AlreadyExists, FailedPrecondition, Internal. Classify (status, "") on REST/GraphQL or
      (0, `HTTP <status>: `) on JSON-RPC/Tendermint, per transport, `UNKNOWN_ERROR` if that
      transport has no status verdict; never borrow REST's 409 meaning elsewhere. The status comes
      from transport metadata or an anchored legacy `HTTP <n>: ` prefix identified by the adapter,
      never from a number elsewhere in the body (two fixtures, `evm_http_429` and `evm_http_503`,
      use `HTTP status <n>:`, which the router never produces; they stay with the transport rows).
      Statuses the GraphQL reader synthesises from Sui extension codes (`graphqlMessage.go:33-55`)
      count like real ones. The REST route-refusal text rows (`:390-393`) run before the status
      decision (keeps `TestRESTMessageRowsBeatTheStatusRow`, `rest_status_rows_test.go:139`).
      Statuses with no router rule (418, 425) are not locked.
3. **X's spec rows**, resolved by inheritance, file order, filtered by interface and scope.
4. **The router's family rows** until the switch deletes them; afterwards the router-owned rows
   that must follow spec rows (XRP `te[fmlr]…` catch-all).
5. **Remaining transport rows.**
6. **`UNKNOWN_ERROR`.**

Step 2 is the only change to today's order. Replayed at `6c237073`: 102 fixtures and 72 keep-set
counterexamples unchanged. Changes by design (locked statuses now win over a matching chain row):
REST 403 "account not found" → `NODE_ACCESS_DENIED`; REST 409 "account sequence mismatch" →
`CHAIN_TX_ALREADY_KNOWN`; REST 402/408/451/501/405 with family text → the status verdict; JSON-RPC
`HTTP 503: …` → `NODE_SERVICE_UNAVAILABLE` (also for `-32602` bodies and `tefPAST_SEQ`, which still
ends unknown on a write via §5.2); JSON-RPC `HTTP 408: …` → `UNKNOWN_ERROR` (no JSON-RPC 408 row);
gRPC 14/8/10 "insufficient fee" → `NODE_SERVICE_UNAVAILABLE`/`UNKNOWN_ERROR`/`NODE_ABORTED`; gRPC 5
"account not found" → `NODE_DATA_NOT_HELD`; gRPC 3 "account sequence mismatch" →
`USER_INVALID_PARAMS`. Node-answer statuses are untouched (REST 400 "unknown block" stays
`CHAIN_BLOCK_NOT_FOUND`, REST 500 "is not available, lowest height" stays `CHAIN_STATE_PRUNED`,
Aptos 410 and toncenter 422 keep their verdicts). A gRPC rate limit visible only in reply metadata
applies only when rows return `UNKNOWN_ERROR` (`direct_rpc_relay.go:1208-1209`), hence
ResourceExhausted is locked.

## 4. Validation (`cmd/specvalidate`, shared by lava-specs CI and the router at boot)

Rules are read from the router's registry, vocabulary and tables, never a copied list. A failing
section makes the chain's policy invalid → fallback (§5.6).

### 4.1 Verdicts a spec may not define

- A router-owned `name` or `code` (the exported set, §1). A chain wanting a different outcome adds
  its own row + verdict ahead of the transport rows.
- `sub_category` = `rate_limit` (hold-off and write re-send lever) or `unsupported_method` (in the
  enum, `error_registry.go:41`; hard-stops retry and caches, `node_error_handler.go:131-145,
  193-206`).
- `data_scope`/`node_capability` with `retryable: false` (router tests forbid it,
  `unsupported_method_scoring_invariant_test.go:49-61, 86-100`).
- Any sub-category or flag the router version doesn't know (schema mismatch).
- A `code` outside the reserved range, unless it is one of the 41 migrated verdicts.
- A `name` or `code` already defined by a spec the chain doesn't inherit from (CI across
  lava-specs; at load both chains fall back with `verdict_collision`).
- A migrated verdict whose copy differs from the router's definition in any field, while the router
  still carries it (Steps 1–2).

### 4.2 Verdicts a row may not name

Rate-limit sub-category (`NODE_RATE_LIMITED`, `NODE_LIMIT_EXCEEDED`, `PROTOCOL_RATE_LIMITED`); any
`PROTOCOL_*` (38); `CHAIN_TX_ALREADY_KNOWN`; `UNKNOWN_ERROR`; any verdict the chain can't see.

### 4.3 Matches a row may not use

Checked per interface the row applies to:

- `code` alone equal to an HTTP status (100–599) on any interface; any `code` on `grpc`.
- `code` + `contains` naming an HTTP status other than 400/404/410/422/500 on `rest` or 400/500 on
  `graphql`/`jsonrpc`/`tendermintrpc`. Inside 400/404/410/422 the verdict must have
  `EndpointAtFault` false; inside 404 the text may not match "endpoint not found", "route not
  found", "path not found", "method not allowed" (`error_classifier.go:390-393`). Codes outside
  100–599 are unrestricted.
- `code: 429` in any form.
- `contains`/`regex` matching a rate-limit phrase (`NODE_RATE_LIMITED` rows, `GRPCRateLimitTexts`),
  an already-known phrase, an `HTTP <status>: ` prefix of a locked status, or a bare locked status
  number as `HTTPStatusContains` reads it.

### 4.4 Source and evidence

`source` on the router's allowlist of node-software and protocol-spec origins (MAG-4898 completes
it), pinned to a tag or commit. CI fetches the file at that commit and requires official evidence
and representative replies for every matcher and regex alternative (longest-literal check is lint
only). A spec owner approves matcher + evidence; write rows also need the write-safety owner.

### 4.5 Stateful check

The validator exports the router's write-method table (§5.3) and fails a PR that clears `stateful`
on a listed write. `check_stateful.sh` reads this table instead of its own list.

## 5. Run-time locks in the router

### 5.1 Locked prefix

§3 step 2. Enforced regardless of what validation said about a row's text.

### 5.2 Slot already used → unknown (rulebook Q45)

- A signature table in router code reads family, code and message, independent of row matching:

  | Family | Code | Message | Today's verdict |
  |---|---|---|---|
  | any | any | contains "nonce too low" | `CHAIN_NONCE_TOO_LOW` |
  | any | any | contains "nonce is too low" (`error_classifier.go:294`) | `CHAIN_NONCE_TOO_LOW` |
  | any | any | contains "account sequence mismatch" | `CHAIN_INVALID_SEQUENCE` |
  | XRP | any | `tefPAST_SEQ` at start, after optional anchored `HTTP <status>: ` | `CHAIN_NONCE_TOO_LOW` |
  | Bitcoin | -26 | contains "txn-mempool-conflict" | `CHAIN_BITCOIN_VERIFY_REJECTED` |
  | Bitcoin | -25 | contains "missingorspent" or "missing inputs" | `CHAIN_BITCOIN_VERIFY_ERROR` |
  | Bitcoin | any | contains "already spent" or "double spend" | `CHAIN_DOUBLE_SPEND` |
  | Bitcoin | -27 | any | `CHAIN_BITCOIN_ALREADY_IN_CHAIN` |
  | Starknet | 52 | any | `CHAIN_STARKNET_INVALID_NONCE` |
  | Starknet | 59 | any | `CHAIN_STARKNET_DUPLICATE_TX` |

  MAG-4898 completes it per node software. Bitcoin `-27` / Starknet `59` ("already there") may
  instead be reported accepted under MAG-4059 (§10).
- A write with no accepted attempt and any signature match is reported unknown; add a fifth reason
  to `*unclearWriteError` (`rpcsmartrouter_server.go:1421-1445`). R&D picks the wire form.
- Record the signature before retry decisions: no further fan-out, not even via the all-rate-limited
  exception, even if a classifier also calls the answer a rate limit. Accepted siblings follow the
  accepted-result rules; reads classify as usual.
- A `write`/`both` row with `"slot_used": true` adds its matches to the group (widen only).
- Write rows stay in code until the side-by-side shows zero write mismatches for 30 days; reads
  switch after 7.

### 5.3 Write methods stay stateful

- Table in router code keyed by interface + name (+ family for generic names like XRP `submit`),
  seeded from `check_stateful.sh:97-119` plus its misses (Bitcoin `sendrawtransaction`,
  `submitpackage`, `bb_sendTransaction`, `POST /tx/send`; NEAR `send_tx`; XRP `submit`,
  `submit_multisigned`; Sui `ExecuteTransaction`, `executeTransaction`; Concordium `SendBlockItem`,
  `PUT /v0/submit…`; Cosmos `BroadcastTx`, `broadcast_tx_*`). MAG-4898 completes it.
- At load (after `DoExpandSpec`, `utils/keeper/spec.go:79`, before the chain parser): a listed api
  not marked `stateful` is loaded stateful; warn per chain; count
  `smartrouter_spec_lock_enforced_total{chain_id, rule="stateful"}`. Today all 132 writes are
  stateful.
- At request time: an undeclared method gets a `Default-` api (`getSupportedApi` →
  `defaultApiContainer`, `base_chain_parser.go:532-560, 579`; `rest.go:158`; via `jsonRPC.go:121`,
  `graphql.go:166`, `grpc.go:137-143`, `tendermintRPC.go:48-54`), today with no `Stateful` and
  `Deterministic: true`. A listed method gets a stateful, non-deterministic default api; count
  `rule="stateful_default"`.
- Identity = the parser's canonical form (verb + route on REST, full service/method on gRPC), never
  a display name or the `Default-` prefix; family from the router's chain identity. Apply before
  scope selection, cache lookup, provider selection and retry; bypass cache reads and writes
  whatever `deterministic` says; check each batch member before aggregation; disabled methods stay
  rejected.

### 5.4 Signed approval of the effective policy

- Manifest entry per chain: chain id, schema version, interfaces declared by the expanded spec,
  resolved import graph + section digests, effective verdicts after overrides, final ordered rows
  after overrides/removals (every field), the lava-specs commit, and the official-source commits and
  file digests used in review. Empty results have an entry.
- The entry names the deployment context (candidate / general) and the permitted stage (`shadow`,
  `reads`, `all`). The router applies the more restrictive of this and its setting.
- Validator and loader share one canonical serializer and hash (duplicate keys rejected, array order
  kept, absent `imports` ≠ `[]`, format version in the signed payload).
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

Stay in code: `tooBusy`/`slowDown` → `NODE_RATE_LIMITED` (`error_classifier.go:127`, §3 step 2.1);
`tefALREADY` → `CHAIN_TX_ALREADY_KNOWN` (`:117`, §3 step 2.2); `te[fmlr]…` catch-all →
`CHAIN_TX_REJECTED` (`:119`, `xrplRejectionCode` `:171`, uses `IsXRPLRejection`), §3 step 4, after
the spec rows `tefPAST_SEQ`/`terPRE_SEQ`.

### 5.6 Fallback

A chain's effective policy from the loaded source is replaced by its complete bundled policy when:
unknown `schema_version`; a row or verdict breaks §4; a row names an unseen verdict; a verdict
collision; no valid approval; an unresolvable import. If the source as a whole fails (host down,
ref missing, not an archive, nothing parses) the router boots on the bundled specs for everything.

- Resolve external and bundled graphs separately; never mix a bundled parent with external
  overrides. Each dependant falls back whole; unrelated chains keep their policies. Api definitions
  stay from the loaded source unless the whole source failed; the stateful guard still applies.
  Validate the fallback against the interfaces the chain serves. One atomic choice per chain, before
  serving.
- If the bundled specs have no effective rows for the chain, or a served interface is outside the
  bundled approval, the chain gets the prefix and transport rows only, logged as reduced coverage.
- Log chain + cause; count `smartrouter_spec_errors_section_dropped_total{chain_id, reason}`,
  reason ∈ {`schema_unknown`, `rule_violation`, `approval_missing`, `signature_invalid`,
  `policy_mismatch`, `approval_revoked`, `dependency_invalid`, `verdict_collision`,
  `source_file_failed`, `bundled_no_rows`, `interface_outside_context`}; whole-source failures count
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

## 6. Compatibility and release pair

- `schema_version` changes only when an older router can't read the format; the verdict vocabulary
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

**Step 0 (no spec row read yet).** Visible changes: boot on bundled specs instead of refusing;
writes relayed stateful even when cleared or undeclared; slot-already-used writes reported unclear;
locked statuses decide their own replies. Deliverables: `compatibility.json`, `SPEC_COMMIT`,
`make specs`, folder-matches-commit check, bundled specs in the image, source fallback (§6, §5.6);
`cmd/specvalidate`, schema, `spec_guards.yml` jobs, owner-review and evidence checks, approval
pipeline (§4, §8.1); locked prefix and router-owned XRP rows (§3, §5.5); exported router-owned set,
reserved code range, exact-copy check for the 41 migrating verdicts (§4.1); write-method table with
both locks (§5.3); slot-already-used signatures (§5.2); `chainFamilyMap` AVAX* keys renamed (§10);
Helm default → bundled.

**Step 1 (side by side).** Copy the six family tables (minus the three XRP rows) into their root
specs unchanged, with the 41 family-only verdicts at today's codes and flags; the 17 family-table
verdicts also used by transport rows or logic stay router-owned and are only named. STRKS imports
STRK; the 36 gaining chains get `"imports": []` (Appendix B). The reader loads rows and verdicts,
evaluates spec rows beside code rows, acts on code rows. Reader constraints: registry maps are
init-only and lock-free, `registerError` panics on duplicates (`error_registry.go:602-629`) → spec
verdicts live in a per-chain policy outside `errorRegistry`, an override is a per-chain view;
`ClassifyError` is keyed by `ChainFamily` (`error_classifier.go:788, 801`) → key by chain policy;
router logic compares its verdicts by pointer (`svm_block_hash_retry.go:99`,
`read_your_writes.go:230`, `rpcsmartrouter_server.go:1599-1603`) → a row naming a router-owned
verdict resolves to the router's own `*LavaError`. Metrics:
`smartrouter_spec_error_rows_mismatch_total{chain_id, kind, row_id, code_error, spec_error, scope}`
with `kind` ∈ {`differ`, `code_only`, `spec_only`} and
`smartrouter_spec_error_rows_hits_total{chain_id, row_id}`. Expected: zero mismatches; compare the
old and new family stages with the same prefix on both sides, and count coverage at the family stage
as well as the final verdict.

**Step 2 (switch).** A router setting:

| Setting | Reads | Writes |
|---|---|---|
| `shadow` | prefix, family table, transport; spec rows counted | same |
| `reads` (7 days, zero read mismatches) | prefix, spec rows, family table, transport | prefix, family table, transport; spec rows counted |
| `all` (30 days, zero write mismatches) | prefix, spec rows, family table, transport | same as reads |
| tables deleted (next release) | prefix, spec rows, router-owned rows, transport | same |

The setting can't exceed the approved stage (§5.4); an early `all` logs and stays. Record the policy
digest, window start/end and observed coverage per chain and scope; a policy change or mismatch
restarts the window; exercise unused rows with fixtures. Rollback before deletion = previous setting
+ artifact; after deletion = previous router release + bundle. The family tables and the 41 code
verdict definitions are deleted in the release after `all` has passed one canary; that release
bundles an `all`-approved policy and keeps the bundled policy for any chain whose external policy
has only `shadow`/`reads` permission.

**Step 3.** MAG-4898 lands researched rows and chain-specific verdicts; the 36 gains open by removing
`"imports": []`, each a behaviour change with product sign-off and, for verdicts, R&D review.

## 8. Gates

### 8.1 lava-specs CI, every PR

- JSON Schema for `errors` on every changed spec.
- `cmd/specvalidate`, built from each router release the boot matrix covers.
- Stateful check (§4.5) as a `spec_guards.yml` job.
- Write-safety job: validator write rules + diff of effective policies for every affected chain and
  descendant (row contents by `id`, order separately); any change touching `write`, `both` or
  `slot_used` needs write-owner review via a required check (labels are informational). A new
  `CODEOWNERS` file names the owners teams (spec owners, write-safety owners, R&D); since it can't
  target a JSON section, the required checks compute what changed and demand the right approval on
  the current head.
- Source evidence (§4.4); every root and descendant policy passes validation and compatibility
  before its digest can be signed.
- R&D review (required check) for any change to a chain's effective verdicts; also fails a name/code
  defined by a non-inherited spec, a code outside the range, an override changing a code, a
  migrating verdict differing from the router's copy.
- Behaviour changes named in the PR description (old → new verdict/flags, why) with a
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
  declared. Fixtures carry expected flags; the replay checks each spec verdict's recorded fault
  decision equals `EndpointAtFault` of its flags. `TestFaultAxis_RegistryContract`'s rows for the
  41 (`error_fault_axis_test.go:151-164`) move to a fixture read from the bundled sections;
  `TestRegistry_CodeRanges` (`error_registry_test.go:42-57`) learns the reserved range.
- Counterexamples. Keep today's verdict: Bitcoin `-26` "min relay fee not met, 429 < 1000" and
  "mempool min fee not met, 429 < 1000", Agave `-32005` "Node is behind by 429 slots"; a rate-limit
  phrase inside gRPC 3/5/10/11/12; JSON-RPC code 429; XRP router-owned rows and other engine results
  with/without `HTTP <status>: `; each already-known answer (EVM "already known", Nethermind
  "AlreadyKnown", Besu "Known transaction") on EVM, Solana, Bitcoin, Starknet, Cosmos; each
  slot-already-used signature as a full pair. Take the router's verdict: REST 403, 409, 402, 408,
  451, 501 with family text; gRPC 14, 8, 5, 3, 10; JSON-RPC `HTTP 503:` and `HTTP 408:`. Text rows
  beat status: REST 403/405 "route not found" → `NODE_ENDPOINT_NOT_FOUND`. Stay readable: REST 400
  "unknown block", 500 "is not available, lowest height", 410 `version_pruned`, 422
  `failed to parse ton_addr`, 404 with a chain's own `not_found`. Fail validation:
  `{code: 408, contains}`, `{code: 402, contains}`, `{code: 404, contains: "not found"}`, a row
  inside 400 naming `NODE_INTERNAL_ERROR`. Isolate the status: JSON-RPC `-32602` with HTTP 503 and
  HTTP 503 "insufficient funds" → `NODE_SERVICE_UNAVAILABLE`; HTTP 401 keeps its per-transport
  verdict; JSON-RPC 409 → `UNKNOWN_ERROR`; a body beginning `HTTP 503: ` without a transport status
  isn't locked. Relay-policy: a stateful write whose every answer is the min-fee rejection stops
  after fan-out and holds nothing off.
- Attack rows as loaded spec rows: `{contains: "block not found"}` → `CHAIN_BLOCK_NOT_FOUND` vs gRPC
  InvalidArgument, `{code: -32000}` → `CHAIN_TX_REJECTED` vs EVM "already known": no verdict changes.
- Slot-already-used: every signature as a full pair through the stateful path ends unknown; the same
  pair on a read keeps its verdict; every fixture with `CHAIN_NONCE_TOO_LOW`, `CHAIN_INVALID_SEQUENCE`
  or `CHAIN_STARKNET_INVALID_NONCE` matches a signature; mixed with rate-limit answers, or matching a
  rate-limit phrase too, no second fan-out; include an accepted sibling and prefixed XRP replies.
- Coverage: each chain's effective rows and verdicts vs the PR's base commit; gains, losses,
  reorders or flag changes fail unless declared and approved.
- Generated row tests: per changed row a source-backed positive reply and a near-miss (one per
  regex alternative), tested against the complete effective policy, recording the winning row,
  verdict (name + flags) and class, reporting unreachable rows.

### 8.3 Router repo tests (bad source, locks, fallback)

- No `--use-static-spec`, non-archive, missing ref, silent host, unparseable spec → bundled specs
  with the right `smartrouter_spec_source_fallback_total` reason.
- Each §4 rule, missing/wrong-key/revoked approval, unknown schema → the chain's bundled policy.
  Verdict cases: router-owned redefinition, `rate_limit`, unknown sub-category, out-of-range code,
  migrating copy differs, row naming an unseen verdict; `verdict_collision` for two non-inheriting
  definers; a valid chain-specific verdict loads and the relay path reads its flags; a child
  override applies only to the child (ETH1 retryable vs BASE non-retryable); an override changing a
  code, or incompatible inherited definitions without an override, fail.
- Reordering/deleting approved rows, deleting a section, altering imports, moving a row between
  chains, widening interfaces → digest verification fails; approved empty policy passes. Invalid
  parent → dependants on bundled policy, unrelated chains untouched; cycles, mixed schemas,
  duplicate ids, diamonds never mix external and bundled rows.
- Vendor-only regex alternative fails review; unapproved custom policy never acts via an official
  URL or candidate signature; `all` with shadow/read permission activates no write rows; production
  binary rejects candidate-only approval; after deletion a partially approved policy keeps bundled.
- After the switch: Solana's section dropped → `-32005` "Node is unhealthy" still
  `NODE_SOLANA_UNHEALTHY` from the bundled section.
- `eth_sendRawTransaction` with `stateful` 0 → loaded stateful, `rule="stateful"` counted; omitted →
  relayed stateful and uncached, `rule="stateful_default"` counted; repeat cleared/omitted/disabled
  per write interface, batches, cache bypass, `deterministic: true` on a declared write.
- Every fallback case asserts the gauges (1 for the affected chain/interface/reason, 0 elsewhere,
  0 after reload with a valid policy, read from the collector).
- Alert rules proven in smart-router-infrastructure by extending the promtool replay in
  `tests/grafana-alert-rules/test_silent_when_idle.py:129-156` (render per-tenant rules via
  `expand_tenant`; gauge 1 past `for` fires, back to 0 resolves; counter step fires
  `sr-spec-lock-enforced`) and `scripts/grafana-alert-rules.py check`; CI
  `.github/workflows/grafana-plan-pr.yaml:121-124`. Publish rules only after a router emitting the
  gauges is on the fleet (`check` can't see whether a metric exists, `docs/OBSERVABILITY.md:1325-1330`).

### 8.4 Canary

One canary router per cluster, one full day of production traffic (minimum; the 7/30-day windows of
§7 still apply). Compare `smartrouter_errors_total` by `error_name` and `chain_id`
(`protocol/metrics/error_metrics.go:42-44`); dropped-section, source-fallback and lock-enforced
counters at zero; both fallback gauges 0. On-prem gets the tested pair only. Failed canary → previous
artifact (and router release if the bundle changed); the next release bundles the promoted commit.

## 9. Observability

| Signal | Purpose |
|---|---|
| `smartrouter_spec_errors_section_dropped_total{chain_id, reason}` | rejected section, with reason |
| `smartrouter_spec_source_fallback_total{reason}` | source couldn't load |
| `smartrouter_spec_lock_enforced_total{chain_id, rule}` | `stateful` / `stateful_default` |
| `smartrouter_spec_error_rows_mismatch_total{…}`, `smartrouter_spec_error_rows_hits_total{…}` | switch criterion; which rows matter |
| `smartrouter_spec_policy_fallback{spec, apiInterface, reason}`, `smartrouter_spec_source_fallback{reason}` | current fallback state; alerts read them |
| `smartrouter_errors_total{error_name, error_category, retryable, chain_id}` | unchanged shape; spec verdicts appear as `error_name`; an override means one name may carry different `retryable` per `chain_id`, so update the 1:1 comment and the "~100 codes" budget (`error_metrics.go:21-28`) and dashboards |

Alert rules in smart-router-infrastructure `grafana/alert-rules/smart-router.yaml`, house style
(`uid`, `title` and `summary` naming `{tenant}`, `for`, `== bool`/`> bool`, `severity` only label,
description with the first operator action), all `warning`:

| Rule | Expr | For |
|---|---|---|
| `sr-spec-policy-fallback` | `max by (cluster, location, router, spec, apiInterface, reason) (smartrouter_spec_policy_fallback{cluster="{tenant}"}) == bool 1` | 10m |
| `sr-spec-source-fallback` | `max by (cluster, location, router, reason) (smartrouter_spec_source_fallback{cluster="{tenant}"}) == bool 1` | 30m |
| `sr-spec-lock-enforced` | `increase(smartrouter_spec_lock_enforced_total{cluster="{tenant}"}[30m]) > bool 0` | 0s |

## 10. Open decisions

| Decision | Needed before |
|---|---|
| Update MAG-4910 for the three departures | approval is recorded |
| Wire form for unknown writes; Bitcoin `-27` / Starknet `59` unknown or accepted (MAG-4059) | Step 0 write-safety ships |
| Initial write-method table | Step 0 write guards ship |
| Reserved code range (proposal 5000–5999; split e.g. 5000–5499 `NODE_*`, 5500–5999 `CHAIN_*` to match `TestRegistry_CodeRanges` and `docs/ERROR-REGISTRY-DESIGN.md`); docs follow-up (`ERROR-REGISTRY-DESIGN.md`, public docs, `docs/RELAY-END-EVENT.md:65, 138`) | Step 0 validator |
| Source evidence for migrated rows; the R&D owners team for verdict review | Step 1 |
| Manifest delivery, signing service, key custody, rotation, revocation; candidate trust context | any external policy acts |
| Generic geth-wording rows (`error_classifier.go:269-346`, `:241`): keep generic or move to ETH1, after 30 days of hits data; the router's vendor rows (Alchemy `-32600`, QuickNode's `blocked …` row): keep or remove; "pending state is not supported" is Erigon's text, fix its comment | those behaviour changes ship |
| `chainFamilyMap` stays (fault axis, transport rows, `IsSolanaFamily` `protocol/common/svm.go:10-13`); rename AVAXC/AVAXCT → AVALANCHEC/AVALANCHECT; AVALANCHEP/AVALANCHEPT belong to no family; drop the five disabled Cosmos bases; CI fails a new spec the map doesn't know | Step 0 |
| create-spec agent keeps unknown keys in update modes by construction; new-chain mode needs a template (MAG-4898 part 3); add a test in `scripts/test_*.sh` | Step 3 |
| Effort: ticket said 6–8 engineer-weeks; revision 2 split ≈9; the approval pipeline, request-time lock, wider prefix and per-chain verdicts add to it | R&D sizes |

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
| Read-your-writes, SVM block retry, node error handler (identity comparisons) | `read_your_writes.go:218-231`; `svm_block_hash_retry.go:99`; `node_error_handler.go:23, 164` |
| How a status reaches the classifier | REST `direct_rpc_relay.go:911`; GraphQL `graphqlMessage.go:33-55, 490`, `direct_rpc_relay.go:1034`; JSON-RPC prefix `direct_rpc_relay.go:703-720`, `node_error_handler.go:80` |
| Default api for undeclared methods | `base_chain_parser.go:532-560, 579`; `rest.go:158`; disabled apis `:582, 848-851` |
| Spec loading, imports (`DoExpandSpec`; child wins by name within a collection), unknown keys dropped, source | `utils/keeper/spec.go:23-26, 79, 89, 111-128`; `utils/specfetcher/fetcher.go:120-264, 380-411`; `types/spec/expand.go:20-103`; `api_collection.go:580-582`; `combinable.go:37-67`; `cobra_common.go:79`; `rpcsmartrouter.go:2567-2581` |
| Fixtures and tests (`TestErrorFixtures`, `TestFaultAxis_EveryRegisteredCodeIsAccountedFor`, `TestFaultAxis_RegistryContract`, REST/gRPC status suites) | `testdata/error_fixtures.json` (102); `error_fixtures_test.go:56`; `error_fault_axis_test.go:32-164, 203`; `rest_status_rows_test.go:15, 77, 139`; `grpc_rate_limit_vocabulary_test.go:57`; `endpoint_fault_test.go:35`; `direct_rpc_connection_test.go:466`; `unsupported_method_scoring_invariant_test.go:49-61, 86-100`; `error_registry_test.go:42-57` |
| Metrics, readiness | `protocol/metrics/error_metrics.go:21-44`; `smartrouter_metrics_manager.go:581-584, 766-793, 1688-1700` |
| Images and chart | `docker/Dockerfile.release:26`, `Dockerfile.ci:13`, `docker/Dockerfile:30`, `docker-compose.yml:53`; chart `values.yaml:450-458`, `templates/routers_deployment.yaml:368-369` |
| lava-specs CI | `spec_guards.yml`, `spec_pipeline.yml`, `create_spec.yml`; `check_stateful.sh:97-119, 188`; `check_unused_fields.sh:27`; `check_update_diff.sh` |
| Infra alerting | `grafana/alert-rules/smart-router.yaml`; `scripts/grafana-alert-rules.py`; `tests/grafana-alert-rules/`; `docs/OBSERVABILITY.md:1138-1370` |

## Appendix B. Step 1 coverage (lava-specs `bda05974`)

| Family | Root | Map keys | Specs reaching root | Lose | Gain |
|---|---|---|---|---|---|
| Solana | SOLANA | 4 | 4 | 0 | 1 |
| Bitcoin | BTC | 8 | 17 | 0 | 10 |
| Cosmos SDK | COSMOSSDK | 28 | 47 | 5 | 25 |
| Starknet | STRK | 2 | 0 | 1 | 0 |
| NEAR | NEAR | 2 | 1 | 0 | 0 |
| XRP | XRP | 2 | 1 | 0 | 0 |

Losses: STRKS (add `"imports": ["STRK"]`); COSMOSSDK45DEP, COSMOSWASM, ETHERMINT, IBC, TENDERMINT
(disabled, nothing to lose). Gains (opt out in Step 1 with `"imports": []`): SOLANAD; BTCS, BTCT4,
DASH, DASHT, PEP, PEPT, QTUM, QTUMT, ZCASH, ZCASHT; AKASH, AKASHT, BABYLON, BABYLONT, DYDX, DYDXT,
ELYS, FETCHAI, FETCHAIT, INJECTIVE, INJECTIVET, KAVA, KAVAT, NEUTRON, NEUTRONT, SEI, SEIT, SIDET,
STRIDE, STRIDET, TSC, TSCT; AGR, AGRT, CANTO. Map keys that aren't lava-specs indexes: AVAXC, AVAXCT,
AVAXP, AVAXPT.

## References

Rulebook v1.0 (Notion `40cf5ce8bcd38108bcf3fd93f85e8af5`): R-M, R-L, R-K, R-0.2, Q45.
`docs/ERROR-REGISTRY-DESIGN.md`. Jira: MAG-4910, MAG-4898, MAG-4059, MAG-4748, MAG-4061, MAG-4185,
MAG-4824, MAG-4817, MAG-5021, MAG-2771. erigontech/erigon `rpc/jsonrpc/eth_call.go:61`.
