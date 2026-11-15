# PRD: per-chain error classification in lava-specs

**Status:** draft for R&D approval (MAG-4910), revision 5. No implementation is approved by this
document yet.

- **Who approves:** Nimrod or Tom, on Must-haves 2 to 4, the format and the split. Must-have 1
  (spec versioning) is out of scope by the author's decision since revision 3. MAG-4910 still lists
  it as required, so the ticket needs updating to match (1.3, 9).
- **What it blocks:** MAG-4898, the per-chain research, which starts once this is approved.
- **What revision 5 changes:** the verdicts a chain's rows point at are defined in the chain's
  spec too, with the flags that decide retry, fault and scoring, so a chain-specific verdict or a
  changed consequence is a lava-specs PR rather than a router release. The router keeps the flag
  vocabulary, what each flag does, and the verdicts its own machinery depends on (1.1, 2.3, 3.2).
  Names and codes stay unique across lava-specs.
- **What revision 4 changed:**
  - It keeps revision 3's scope and decisions-first structure.
  - It binds approval to each chain's complete effective policy, including row order, imports
    and deliberate empty policies, instead of accepting any collection of individually signed rows.
  - It defines how source review, candidate testing, signing and canary promotion fit together.
  - It makes write guards apply before caching and retry decisions, and defines fallback for
    chains that depend on a rejected parent.
  - It narrows the HTTP carve-out to the statuses whose body is the node's own answer (400, 404,
    410, 422 and 500 on REST; 400 and 500 elsewhere) and locks every other status the router has
    a rule for (1.3, 3.2).
  - It makes every fallback visible: two state gauges, alert rules in smart-router-infrastructure,
    and tests that prove them before they're published (4.4, 5.4, 5.6).

  Appendix C records the revisions and maps revision-2 section numbers to the current structure.

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

- **Each chain's spec carries its own error rows and the verdicts they point at.** A row says
  which code or message from that chain's nodes means which verdict. A verdict says what that
  answer is: its name and code, whether it's retryable, and whether it's the endpoint's fault, the
  caller's, or a question of what the node holds. Today both are Go code, so every new chain error
  needs a router release (A.1).
- **The router keeps the vocabulary and the machinery.** It defines what each flag does (retry,
  fault, scoring, hold-off, caching), the interface rules, connection errors, status rules, rate
  limits, the unknown default, write safety, and the verdicts its own logic depends on. A spec
  chooses flags from a closed menu; it can't add a consequence the router doesn't have.
- **Choosing or changing a verdict changes behaviour**, so it goes through the review and rollout
  gates in section 5, with R&D reviewing the verdicts and the spec owners the rows. Rows and
  verdicts cannot define retry rules, status rules or write outcomes.
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
   `UNKNOWN_ERROR`, and **a spec can't define** a verdict with the rate-limit sub-category or the
   router's internal category, or redefine a name or code the router owns (3.2).
4. **A write that fails with a "slot already used" answer is reported as unknown**, whatever row
   matched (3.3).
5. **A known write method is always stateful**: whether the spec marks it, leaves the flag off, or
   doesn't declare the method at all (3.4).
6. **A chain's effective policy acts only if it passed source review and the release gates, and
   matches a signed approval.** The approval covers its context and ordering, not just its rows
   (3.4). Every source must pass the same check.
7. **An invalid effective policy falls back as a whole, and never silently.** The affected chain
   uses its bundled policy. Dependants are checked separately; unrelated chains keep their valid
   policies, and the router still boots. A gauge, a log line and an alert name the chain and the
   reason for as long as the fallback lasts (4.4).

### 1.3 Where this departs from the ticket

**Deliberate departures.** These are the author's decisions. They're recorded here so the approver
sees them and so MAG-4910 can be brought in line:

- **Must-have 1 (spec versioning) is out of scope** (decided 2026-11-11). There are no lava-specs
  tags, no change log, no pinning of the spec source, and no metric for which source a router
  loaded. What follows from that:
  - Routers keep loading whatever spec source they're given. The bundled commit (4.2) is the only
    spec version a router release is tied to.
  - A lava-specs PR that moves a verdict names it in its own description, in place of a
    change-log entry. The fixture replay of Must-have 4 needs that record (5.2).
  - Stale and unpinned sources remain possible. Testing their rejection is deferred with this
    requirement; it is not made unnecessary by dropping versioning. Broken-source and approval
    checks remain mandatory (5.4). A signature proves approval, not freshness.
  - The `schema.behaviour.additive` version is gone. `schema_version` stays as a plain integer,
    because Must-have 2's compatibility file needs it (4.1).
- **Chain rows can read text inside the HTTP statuses whose body is the node's own answer: 400,
  404, 410, 422 and 500 on REST, and 400 and 500 on every other interface.** The ticket says no
  spec may "override the HTTP / gRPC status rules". Every gRPC status the router has decided is
  locked, and so is every other HTTP status the router has a rule for (2.4). The readable statuses
  are the ones the REST chains the router serves answer in (Horizon, Aptos, toncenter, the Cosmos
  gRPC-gateway, sidecar, nodeos), and the router's own table already reads text inside 400 and 500
  (`error_classifier.go:404, 413-416`). MAG-4061 needs TON's answers inside a 500. Inside the four
  4xx statuses a row may only name a verdict that doesn't blame the endpoint (3.2).
- **A chain's spec defines the verdicts its rows point at, flags included** (decided 2026-11-15).
  The ticket's goal keeps "what each class does" in the router. Revision 5 splits that: the spec
  says which flags a verdict carries (retryable and the sub-category), and the router keeps what
  each flag does and every verdict its own machinery references (2.3, 3.2). The
  ticket's locks still hold: no spec can name or define a rate-limit verdict, touch the status
  rules, raise a retry limit or move a write out of unknown. What changes is that a chain can get a
  verdict of its own, or change one's consequences, without a router release. R&D still reviews
  every verdict change (5.2).

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

The scope departures in 1.3 are settled for this PRD. MAG-4910 still lists all four must-haves,
so the ticket has to be updated to match before approval is recorded on it (9).

Section 6 lists the remaining decisions. Their gates are:

| Decision | Required before |
|---|---|
| Update MAG-4910 for the versioning deferral and the HTTP body carve-out | Approval is recorded on the ticket |
| Choose the wire form for unknown writes, including Bitcoin `-27` and Starknet `59` | Step 0 write-safety changes ship |
| Complete and review the initial protected-method table | Step 0 write guards ship |
| Complete and review source evidence for migrated rows | Step 1 starts |
| Fix the reserved code range for spec-defined verdicts | Step 0 ships the validator |
| Name the R&D owners team that reviews verdict changes | Step 1 starts |
| Specify manifest delivery, trust keys, rotation and revocation, and prove candidate promotion | Any external policy acts |
| Decide whether to narrow generic geth wording and remove existing vendor rows | Those separate behaviour changes ship |

The open work includes:

- whether the geth-wording rows stay generic or move to an ETH1 section;
- whether the router's own vendor rows stay;
- whether Bitcoin `-27` and Starknet `59` count as unknown or as accepted;
- completing the write-method table and the source allowlist;
- the reserved code range for spec-defined verdicts, and how the public error catalogue follows
  them;
- where the signed manifests live, and who holds the signing key;
- the effort for the complete approval and rollout pipeline.

## 2. The design

### 2.1 The split

Here's what moves into the specs and what stays in the router:

| Lives in the spec (per chain) | Stays in router code |
|---|---|
| Which node code or message means which **verdict** for this chain's nodes | The flag vocabulary: the sub-categories a verdict may carry, and what `retryable`, `data_scope` and `node_capability` make the router do: retry, stop, blame or excuse the endpoint, score |
| The chain's **verdicts**: name, code, sub-category, retryable, class, inherited and overridable through `imports` | The **router-owned verdicts**: every verdict a transport row points at, the 25 the router's logic references by identity, the protocol, connection and status verdicts, `UNKNOWN_ERROR` (3.2); and the transport-failure flag `may_have_reached_node`, which only those carry |
| Each row's interface, read or write scope, source and versions checked | Retry counts, hold-off timing, the caching rules |
| Inheritance of rows and verdicts through `imports`, with override by id or name and removal of rows | Interface rules: the JSON-RPC envelope, REST status, gRPC codes, GraphQL `errors` |
| A chain's own wording added to the slot-already-used group (3.3) | Connection errors, HTTP and gRPC status rules, rate-limit detection, the already-known recognisers, the unknown default |
| | Write safety: fan-out, the single stop, the unknown outcome, the slot-already-used signatures, the write methods that must stay stateful |
| | The XRP rows that must remain router-owned (3.5), the source allowlist and the manifest verification keys (3.4); private signing keys stay in trusted CI |

In practice, what moves is the six family tables (minus XRP's three router-owned rows) and the 41
verdicts only those tables use, and later, chain by chain, the geth-wording rows (section 6).
Section 3 lists what never moves.

### 2.2 The errors section

Each spec gets one `errors` object, next to `api_collections`:

```json
{
  "index": "SOLANA",
  "api_collections": [ "..." ],
  "errors": {
    "schema_version": 1,
    "verdicts": [ "..." ],
    "rows": [ "..." ]
  }
}
```

- **`verdicts`** is the chain's own part of the verdict registry: the verdicts its rows may point
  at that aren't router-owned, each with the flags that decide what the router does with it (2.3).
  It's optional: a chain whose rows only use router-owned or inherited verdicts has none.
- **`rows`** says which node answer means which verdict (2.3).
- **`schema_version`** is an integer. Every section at a given lava-specs commit carries the same
  number, and CI enforces it, so a child and its parents never disagree. A router knows which
  schema versions it can read (4.1). It drops a section with any other version whole, and that
  chain and any chain that imports it fall back (4.4); unrelated chains keep their policies. It
  never reads half a section.
- **`imports`** is optional, and lists the specs whose rows and verdicts this section inherits. If
  it's absent, the section follows the spec's own `imports`. `[]` means inherit nothing. It only
  affects the errors section; the spec's api inheritance is untouched. Step 1 of the rollout uses
  it to keep today's coverage (5.1).
- **The section itself is optional.** Older routers ignore the added key (A.3). On a router that
  reads it, a missing section means no local rows; the chain may still inherit rows. The signed
  manifest must approve that complete result, including an intentionally empty policy (3.4).
  Removing a section or its imports is therefore a policy change, not a way to bypass approval.

### 2.3 The row and the verdict

**The row** says what a node's answer means:

```json
{
  "id": "solana.blockhash_not_found",
  "match": { "contains": "blockhash not found" },
  "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND",
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
| `error` | yes | The **name** of a verdict: one the chain defines or inherits (below), or a router-owned one that isn't locked, for example `CHAIN_STATE_PRUNED`. Validation fails on a name the chain can't see after inheritance, and on the names in 3.2. |
| `interface` | no | `jsonrpc`, `rest`, `grpc`, `graphql` or `tendermintrpc`. If it's absent, the row applies to every interface the chain serves after inheritance. |
| `scope` | yes | `read`, `write` or `both`. A `write` row is only evaluated for stateful apis, and a `read` row only for the rest. Since the router keeps write methods stateful (3.4), a spec can't move a write out from under the write rows. |
| `source` | yes | A link to the official definition (the protocol spec, the node software's docs, or the source file that defines the error), pinned to a tag or commit. It must be on the router's allowlist, with reviewed evidence covered by the effective policy's approval (3.4). |
| `versions` | yes | The node software versions the row was checked against. At least one. |
| `slot_used` | no | `true` adds this row's answers to the slot-already-used group (3.3). Only allowed on `write` or `both` rows. It can widen the router's group, never narrow it. |
| `disabled` | no | `true` removes the parent row with the same `id`, for this spec and its children. A disabling row carries only `id`, `disabled` and optionally `note`; any other field fails validation. |
| `note` | no | One line for the reader. It's called `note` because `description` and `title` are both rejected key names (A.4). |

**The verdict** says what that answer is, in the router's vocabulary:

```json
{
  "name": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND",
  "code": 3304,
  "sub_category": "none",
  "retryable": false,
  "class": "C6",
  "note": "a blockhash the cluster no longer accepts; the caller must re-sign"
}
```

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | Stable, and never a router-owned name (3.2). A verdict has one origin: the spec that defines it, and no other spec may define the same name at the same commit. A chain that inherits the verdict may **override** it by repeating the name with the fields it changes (2.5); that isn't a second definition. New verdicts follow `CHAIN_<FAMILY>_*` or `NODE_<FAMILY>_*`; a verdict migrated from the router's table keeps its name. A node's own error body is forwarded to the client unchanged (`failed_relay_reply.go:35-38`), so clients never see a verdict; operators do, in `smartrouter_errors_total{error_name}`, the coded log fields and the `relay_end` event's `error_code`. |
| `code` | yes on a definition, not on an override | An integer, unique across lava-specs at a commit, and never a router-owned code. An override keeps the origin's code: it omits the field or repeats it unchanged. A migrated verdict keeps its code (the 41 in 5.1 Step 1); a new one takes a value from the reserved range (section 6 picks it; the proposal is 5000 to 5999, clear of the router's 1001 to 4007). It appears in structured logs and the `relay_end` event, not as a metric label (`error_metrics.go:37-40`) and never in a client reply. |
| `sub_category` | yes | Exactly `none`, `data_scope` or `node_capability`. With `retryable` it decides the fault axis exactly as the router's own rule does (`error_registry.go:136-152`): `data_scope` and `node_capability` are never the endpoint's fault; `none` with `retryable: true` is the node saying it's broken, and the endpoint is at fault; `none` with `retryable: false` is the caller's fault. `data_scope` and `node_capability` require `retryable: true`, the rule the router's registry tests pin (3.2). `rate_limit` and `unsupported_method` can't be used (3.2). |
| `retryable` | yes | Whether the router tries the next provider for a read. On a write the stateful policy decides, whatever this says (3.1). |
| `class` | yes | The rulebook class, C2 to C7. For people and the change record; the router acts on the flags. |
| `note` | no | One line for the reader. |

Every spec-defined verdict is in the router's external category: a node or a caller answered. The
internal category (the router's own failures) and the rate-limit sub-category are router-owned
(3.2). There's no `may_have_reached_node` field either: the router reads that flag only on
transport failures (`rpcsmartrouter_server.go:1498`, `relay_end.go:588`), and a spec row never
classifies one, so a spec verdict is always a settled reply. The validator prints the fault
decision each verdict's flags produce, so a reviewer sees what a spec is asking for in the router's
terms.

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
   3. **The locked statuses.** A reply is decided by the transport table, and never reaches the
      spec rows (step 3), when:
      - its HTTP status is one the router has a rule for, other than the node-answer statuses of
        3.2: on REST those are 400, 404, 410, 422 and 500; on GraphQL, JSON-RPC and Tendermint
        only 400 and 500. So the locked set is 401, 402, 403, 405, 406, 407, 408, 409, 411, 412,
        413, 414 to 417, 426, 428, 429, 431, 451, 501, 502, 503, 504, 520 to 527 and 530 on every
        interface (`error_classifier.go:421-449, 555-591`), plus 404, 410 and 422 off REST. The
        status is read as the code on REST and GraphQL, and from the `HTTP <status>: ` prefix on
        every other interface. Within this step the REST table's four route-refusal text rows
        ("endpoint not found", "route not found", "path not found", "method not allowed",
        `:390-393`) run before the status decision, so a 403 or 405 whose body says "route not
        found" keeps `NODE_ENDPOINT_NOT_FOUND`, as `TestRESTMessageRowsBeatTheStatusRow` pins
        (`rest_status_rows_test.go:139`). A status the router has no rule for at all (418 or 425,
        say) has no decision to protect, so it isn't locked: a `contains` or `regex` row may match
        its body. It still can't be named in `code` (3.2), and without a match it falls to
        `UNKNOWN_ERROR`, as today;
      - or its gRPC status is one the router has decided: the six with a row (InvalidArgument,
        NotFound, OutOfRange, Unimplemented, Aborted and Unavailable, `error_classifier.go:482-517`)
        or the six the comment at `:518-538` leaves without one on purpose (DeadlineExceeded,
        PermissionDenied, ResourceExhausted, Unauthenticated, Canceled and DataLoss).

      So on gRPC, spec rows only ever see Unknown, AlreadyExists, FailedPrecondition and Internal.

      **HTTP decisions use the status alone.** After the earlier explicit-rate and already-known
      checks, do not run the full transport classifier on the original RPC code and message to
      implement this lock: an ordinary phrase or RPC code could win before the status row. Use
      the router's transport rules with only the extracted status: a status code and empty message
      on REST/GraphQL, or code 0 and `HTTP <status>: ` on JSON-RPC/Tendermint. Preserve the
      transport distinction (HTTP 401 is not classified identically on REST and JSON-RPC), and
      return `UNKNOWN_ERROR` if that transport has no status verdict. Never borrow REST's 409
      duplicate-transaction meaning for another interface. An actual gRPC status is decided by
      the gRPC transport rules with no chain rows, preserving their code/text order; an HTTP
      transport failure without a gRPC status uses the HTTP-only decision instead.

      Read status from transport metadata when available. A legacy `HTTP <status>: ` prefix must
      be anchored and identified by the transport adapter; a number or a node-controlled phrase
      elsewhere in the body is not a new status. The cases and expected results belong to the
      router's tests, independently of any spec table.
3. **X's spec rows**, as resolved by inheritance (2.5), in file order, filtered by interface and
   scope.
4. **The router's built-in rows for X's family**, until the switch removes that table (5.1). After
   the switch, this step holds the router-owned rows that have to come after the spec rows (XRP's
   engine-result catch-all, 3.5).
5. **The remaining transport rows.**
6. **`UNKNOWN_ERROR`.**

**Step 2 changes the order; migration must preserve the remaining behaviour.** Revision 3 records
a scratch replay at `6c237073` of all 102 fixtures and 56 keep-set counterexamples: 20 XRP engine
results with and without `HTTP 500: `, 12 slot-used pairs, 15 already-known answers across five
families, and nine rate-limit cases. That is evidence for the proposed ordering, not validation of
an implemented loader, approval check or write policy. Section 5 requires the replay through the
actual implementation, including the attack rows and deliberately changed status cases below.

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
- a JSON-RPC `-32602` reply that arrived as `HTTP 503: invalid params` takes
  `NODE_SERVICE_UNAVAILABLE`, even though the ordinary RPC-code row would pick
  `USER_INVALID_PARAMS` if the original inputs were reused;
- "insufficient fee" inside a gRPC Unavailable, ResourceExhausted or Aborted becomes
  `NODE_SERVICE_UNAVAILABLE`, `UNKNOWN_ERROR` (the router's deliberate answer to that status) or
  `NODE_ABORTED`, instead of `CHAIN_INSUFFICIENT_FEE`;
- "account not found" inside a gRPC NotFound becomes `NODE_DATA_NOT_HELD`, and "account sequence
  mismatch" inside a gRPC InvalidArgument becomes `USER_INVALID_PARAMS`.

- a REST 402, 408, 451 or 501 whose body a Cosmos row matches takes the status's verdict
  (`NODE_ACCESS_DENIED`, `NODE_SERVICE_UNAVAILABLE`, `NODE_ACCESS_DENIED`, `NODE_UNIMPLEMENTED`)
  instead of the row's;
- a REST 405 whose body says "txn-mempool-conflict" becomes `NODE_METHOD_NOT_ALLOWED` instead of
  `CHAIN_DOUBLE_SPEND`;
- a JSON-RPC reply that arrived as `HTTP 408: account sequence mismatch` becomes `UNKNOWN_ERROR`:
  408 is locked, and the JSON-RPC table has no 408 row (the 408 rule is REST-only, `:442`), so the
  transport's answer is "no verdict", not the chain row's.

The node-answer statuses are untouched: nodeos's REST 400 "unknown block" stays
`CHAIN_BLOCK_NOT_FOUND`, the Cosmos gateway's REST 500 "is not available, lowest height" stays
`CHAIN_STATE_PRUNED`, Aptos's REST 410 and toncenter's REST 422 keep today's status verdicts, and a
Cosmos row still reads "account not found" inside a REST 404, or inside a 418 that no router rule
covers.

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

Revision 3 replayed both by putting the row at the head of the family table. The acceptance gate
must also exercise them as loaded spec rows, so a validator rejection cannot substitute for proving
the router-owned decision still wins at run time.

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
inherits its parents' rows unchanged. `schema_version` is the same across the whole repo at any
commit, so a child's section and its parents' share it in a valid source.

Verdicts inherit the same way, keyed by `name`. A child that repeats an inherited verdict's name
overrides its flags for itself and its own children, and that's the granularity the design is
for: BASE can make an ETH1 verdict non-retryable without touching a row. An override changes
`sub_category`, `retryable`, `class` or `note`; it never changes the `code`, which stays the
origin's. A spec that repeats a name it doesn't inherit isn't overriding,
it's defining the name a second time, and that fails (3.2). A verdict can't be removed, only
overridden; an inherited verdict no row uses is harmless. A row's `error` must resolve, after
inheritance, to a verdict the chain defines or inherits, or to a router-owned verdict that isn't
locked (3.2); otherwise the policy is invalid and the chain falls back (4.4).

Resolve and validate the entire dependency graph before making any policy active. Missing parents,
cycles, duplicate local row ids or verdict names, mixed schemas and incompatible definitions of
the same inherited id or name are errors. Identical rows reached through a diamond appear once, at
their first occurrence. A child must explicitly override or disable conflicting inherited ids;
parent order must not
silently choose between different definitions. A disabling row that names no inherited id fails.
The resolved order, import choices, overrides and removals are covered by the approval in 3.4.
An invalid dependency makes each affected chain use its complete bundled policy (4.4).

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

The examples below take three of today's family tables as they are, verdicts included, one XRP
row, and a new TON verdict. A few things to keep in mind while reading them:

- Each `error` is the name today's code row points at. Where the verdict is used only by that
  family's table, it's defined in the example with today's code and flags (the 41 of 5.1 Step 1).
  Where a transport row or router logic also uses it, it's router-owned and the row just names it:
  `CHAIN_STATE_PRUNED` and `CHAIN_NONCE_TOO_LOW` below.
- Today's rows apply to reads and writes alike, so a row moved as-is gets `scope: both`. The
  `read` and `write` scopes below are there to show the field. Narrowing a row to one scope is a
  behaviour change: Agave's `sendTransaction` can answer `-32016`, and a Cosmos SDK simulate, which
  is a read, can answer "account sequence mismatch".
- The `class` letters, `source` links and `versions` only show the shape. MAG-4898 supplies the
  researched values. The flags are today's, read from `error_codes.go`.

**Solana** (`solana.json`, no imports; 14 rows today at `error_classifier.go:25-45`, four shown).
Three of the four verdicts are the family's own; `CHAIN_STATE_PRUNED` (3204) is router-owned,
because the JSON-RPC pruned-state rows (`:354-355`) and the REST 500 and 410 rows (`:413`, `:444`)
use it too.

```json
"errors": {
  "schema_version": 1,
  "verdicts": [
    { "name": "NODE_SOLANA_UNHEALTHY", "code": 2150, "sub_category": "none",
      "retryable": true, "class": "C3", "note": "the node is behind or unhealthy (-32005)" },
    { "name": "CHAIN_SOLANA_MIN_CONTEXT_SLOT_NOT_REACHED", "code": 3311,
      "sub_category": "data_scope", "retryable": true, "class": "C6",
      "note": "the node hasn't reached the requested slot yet (-32016)" },
    { "name": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "code": 3304, "sub_category": "none",
      "retryable": false, "class": "C6", "note": "a blockhash the cluster no longer accepts" }
  ],
  "rows": [
    { "id": "solana.block_cleaned_up", "match": { "code": -32001 },
      "error": "CHAIN_STATE_PRUNED", "scope": "read",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs",
      "versions": ["agave 2.1"] },
    { "id": "solana.node_unhealthy", "match": { "code": -32005 },
      "error": "NODE_SOLANA_UNHEALTHY", "scope": "both",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs",
      "versions": ["agave 2.1"] },
    { "id": "solana.min_context_slot", "match": { "code": -32016 },
      "error": "CHAIN_SOLANA_MIN_CONTEXT_SLOT_NOT_REACHED", "scope": "read",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/rpc-client-api/src/custom_error.rs",
      "versions": ["agave 2.1"] },
    { "id": "solana.blockhash_not_found", "match": { "contains": "blockhash not found" },
      "error": "CHAIN_SOLANA_BLOCKHASH_NOT_FOUND", "scope": "write",
      "source": "https://github.com/anza-xyz/agave/blob/v2.1.0/sdk/src/transaction/error.rs",
      "versions": ["agave 2.1"],
      "note": "a recent blockhash that rolled off the 150-slot window; common under load" }
  ]
}
```

**Bitcoin** (`btc.json`, no imports; 10 rows today at `:46-60`, three shown). The code rows run
before the phrase rows, so Bitcoin Core's mempool conflict, sent under `-26`, is
`CHAIN_BITCOIN_VERIFY_REJECTED` today, not `CHAIN_DOUBLE_SPEND`. That's fine: whichever verdict a
row gives, 3.3 decides "slot already used" from the answer itself. All three verdicts are the
family's own.

```json
"errors": {
  "schema_version": 1,
  "verdicts": [
    { "name": "NODE_BITCOIN_WARMUP", "code": 2101, "sub_category": "none",
      "retryable": true, "class": "C3", "note": "RPC_IN_WARMUP (-28): the node is still starting" },
    { "name": "CHAIN_BITCOIN_VERIFY_REJECTED", "code": 3342, "sub_category": "none",
      "retryable": false, "class": "C6", "note": "RPC_VERIFY_REJECTED (-26): rejected by network rules" },
    { "name": "CHAIN_DOUBLE_SPEND", "code": 3015, "sub_category": "none",
      "retryable": false, "class": "C6", "note": "an input this transaction spends is already spent" }
  ],
  "rows": [
    { "id": "bitcoin.in_warmup", "match": { "code": -28 },
      "error": "NODE_BITCOIN_WARMUP", "scope": "both",
      "source": "https://github.com/bitcoin/bitcoin/blob/v28.0/src/rpc/protocol.h",
      "versions": ["bitcoind 28"] },
    { "id": "bitcoin.verify_rejected", "match": { "code": -26 },
      "error": "CHAIN_BITCOIN_VERIFY_REJECTED", "scope": "write",
      "source": "https://github.com/bitcoin/bitcoin/blob/v28.0/src/rpc/protocol.h",
      "versions": ["bitcoind 28"] },
    { "id": "bitcoin.mempool_conflict", "match": { "contains": "txn-mempool-conflict" },
      "error": "CHAIN_DOUBLE_SPEND", "scope": "write",
      "source": "https://github.com/bitcoin/bitcoin/blob/v28.0/src/validation.cpp",
      "versions": ["bitcoind 28"] }
  ]
}
```

**Cosmos SDK** (`cosmossdk.json`, imports IBC and TENDERMINT; 4 rows today at `:61-74`, three
shown). COSMOSHUB, OSMOSIS and the other Cosmos chains inherit these rows and verdicts through
`imports` and add their own. A chain that answers unfunded accounts differently can override
`CHAIN_ACCOUNT_NOT_FOUND`'s flags by repeating its name, or disable the row (below).

```json
"errors": {
  "schema_version": 1,
  "verdicts": [
    { "name": "CHAIN_INVALID_SEQUENCE", "code": 3012, "sub_category": "none",
      "retryable": false, "class": "C6", "note": "the account sequence doesn't match (the Cosmos nonce)" },
    { "name": "CHAIN_INSUFFICIENT_FEE", "code": 3013, "sub_category": "none",
      "retryable": false, "class": "C6", "note": "the fee is below what the node requires" },
    { "name": "CHAIN_ACCOUNT_NOT_FOUND", "code": 3107, "sub_category": "none",
      "retryable": false, "class": "C6", "note": "the account or contract doesn't exist" }
  ],
  "rows": [
    { "id": "cosmossdk.sequence_mismatch", "match": { "contains": "account sequence mismatch" },
      "error": "CHAIN_INVALID_SEQUENCE", "scope": "write",
      "source": "https://github.com/cosmos/cosmos-sdk/blob/v0.50.10/x/auth/ante/sigverify.go",
      "versions": ["cosmos-sdk 0.50"] },
    { "id": "cosmossdk.insufficient_fee", "match": { "contains": "insufficient fee" },
      "error": "CHAIN_INSUFFICIENT_FEE", "scope": "write",
      "source": "https://github.com/cosmos/cosmos-sdk/blob/v0.50.10/types/errors/errors.go",
      "versions": ["cosmos-sdk 0.50"],
      "note": "also matches the plural \"insufficient fees\"" },
    { "id": "cosmossdk.account_not_found", "match": { "contains": "account not found" },
      "error": "CHAIN_ACCOUNT_NOT_FOUND", "scope": "both",
      "source": "https://github.com/cosmos/cosmos-sdk/blob/v0.50.10/x/auth/keeper/grpc_query.go",
      "versions": ["cosmos-sdk 0.50"],
      "note": "kept per family on purpose: EVM phrasing differs and would false-match calls against unfunded addresses" }
  ]
}
```

**XRP** (`ripple.json`, no imports; 15 rows today at `:108-166`). Twelve move as `regex` rows,
since `xrplMessageStartsWith` is an anchored regex anyway; three stay in router code (3.5). All
fifteen point at router-owned verdicts, so `ripple.json` defines none. One row shown:

```json
{ "id": "xrp.past_seq", "match": { "regex": "^(?:HTTP \\d+: )?tefPAST_SEQ\\b" },
  "error": "CHAIN_NONCE_TOO_LOW", "scope": "write",
  "source": "https://github.com/XRPLF/rippled/blob/2.3.0/include/xrpl/protocol/TER.h",
  "versions": ["rippled 2.3"] }
```

**A chain-specific verdict** (the point of revision 5). TON answers a future block and every
rejected `sendBoc` with a 500 (MAG-4061). With a verdict of its own, `ton.json` can say that
answer is about data the node doesn't hold, not a broken node, without a router release:

```json
"errors": { "schema_version": 1,
  "verdicts": [ { "name": "CHAIN_TON_BLOCK_NOT_FOUND", "code": 5001, "sub_category": "data_scope",
    "retryable": true, "class": "C6", "note": "LITE_SERVER_NOTREADY: the block isn't in this node's ltdb yet" } ],
  "rows": [ { "id": "ton.block_not_found", "match": { "code": 500, "contains": "block not found" },
    "error": "CHAIN_TON_BLOCK_NOT_FOUND", "interface": "rest", "scope": "read",
    "source": "https://github.com/ton-blockchain/ton/blob/v2024.10/...", "versions": ["ton 2024.10"] } ] }
```

The validator reports that these flags make the answer "not the endpoint's fault, retry
elsewhere". The availability gate that still scores every 5xx is router work (3.2).

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

A spec configures behaviour by choosing flags from the router's vocabulary, and the router defines
what each flag does. So there's no field for a retry count or limit, a hold-off, a status rule, a
connection error, a timeout, a score, a write outcome, or a new flag or category, and that's
deliberate: those are the consequences Must-have 3 keeps in code, and the router's own machinery
depends on them being the same for every chain. (A row's `code` doubles as the HTTP status on REST
and GraphQL, and 3.2 limits it to the node-answer statuses.) A chain that needs a consequence the
vocabulary doesn't have is a router change, and gets ticketed as one.

## 3. Write protection and the other locks (Must-have 3)

### 3.1 What is locked by construction

Rows name verdicts, verdicts carry flags from a closed vocabulary, and what each flag does is code,
so the format alone keeps a spec away from retry limits, status rules and write outcomes (2.7). On
top of that, the code already guarantees two things:

- Whatever verdict a row names, the stateful policy stops a write after its one fan-out, unless
  every failure was a rate limit (A.2).
- No row can make an unclear write look settled. The unclear-write decision reads protocol errors
  classified with no chain family and treats every node error as a reply, and REST's 5xx
  node-reply rule also runs with no chain family (A.2, 2.4).

That isn't the whole story, though. Rows reach more than node replies: a direct-RPC transport or
status error gets its retry and health verdicts with the chain family (A.2). So the locks below are
run-time rules in the router, not just checks in CI. That leaves five doors open, and 3.2 to 3.5
close them:

1. a row that names a rate-limit verdict, or a spec that defines one, which opens the one
   exception under which a write is sent again;
2. a row that hides a rate limit, a status, or an answer the router acts on, such as "already
   known";
3. a slot-already-used answer reported as anything but unknown;
4. a spec that drops `stateful` from a write, by clearing the flag or by leaving the method out, or
   a row that quotes a vendor;
5. the XRP rows that name locked verdicts.

### 3.2 Verdicts and matches a row may not use

The validator (5.2) reads all of these from the router's registry of owned verdicts, its flag
vocabulary and its tables, never from a copied list.

**Verdicts a spec may not define.** A `verdicts` entry fails validation, and the chain's policy
falls back (4.4), when:

- **Its `name` or `code` is router-owned.** The router exports that set from `cmd/specvalidate`:
  every verdict a transport row points at, the 25 registered verdicts the router's logic
  references by identity or code (among them `CHAIN_TX_ALREADY_KNOWN`, `CHAIN_BLOCK_NOT_FOUND`,
  `CHAIN_SOLANA_MISSING_LONG_TERM`, `NODE_RATE_LIMITED`, `NODE_BAD_GATEWAY`, `NODE_GATEWAY_TIMEOUT`,
  `NODE_ENDPOINT_NOT_FOUND`), the protocol, connection and status verdicts, and `UNKNOWN_ERROR`
  (A.1). A chain that wants a different outcome for one of those situations adds its own row,
  pointing at its own verdict, ahead of the transport rows, the way Solana's `-32005` shadows the
  generic one. It never changes a router-owned verdict's flags.
- **Its `sub_category` is `rate_limit` or `unsupported_method`.** The rate-limit flag is the lever
  for hold-off and the write re-send (A.2), so only router-owned verdicts carry it. The router's
  enum still has `unsupported_method` (`error_registry.go:41`) though no verdict does: it stops the
  retry regardless of `retryable` and caches the reply (`node_error_handler.go:131-145, 193-206`),
  which is exactly the kind of consequence 2.7 keeps out of a spec's hands.
- **Its `sub_category` is `data_scope` or `node_capability` with `retryable: false`.** The router's
  own registry tests forbid that pair (`unsupported_method_scoring_invariant_test.go:49-61,
  86-100`): a non-retryable capability verdict ends the request at the first provider that says
  no, the failure MAG-2771 removed.
- **It tries to be internal.** There's no `category` field: every spec verdict is external, a node
  or a caller answered. The internal category is the router's own failures.
- **It uses a sub-category or flag the router version doesn't know.** That's a schema mismatch,
  and the compatibility file says which router reads it (4.1).
- **Its `code` is outside the reserved range**, unless it's one of the 41 migrated verdicts keeping
  its code (5.1 Step 1).
- **It defines a name or code a spec it doesn't inherit from already defines.** CI catches this
  across lava-specs at a commit. The router catches it at load too: when two loaded specs define
  the same name or code and neither inherits it from the other, both chains' policies are invalid
  with `reason="verdict_collision"`, so a hand-edited folder can't make one chain's code mean two
  things. Repeating an inherited name is an override, not a definition (2.5).
- **It's a migrating verdict that differs from the router's copy.** While the router still carries
  one of the 41 in code (Steps 1 and 2), the spec's definition must match it in every field.

**Verdicts a row may not name.** A row fails when its `error` is:

- **Any verdict with the rate-limit sub-category**: today `NODE_RATE_LIMITED`,
  `NODE_LIMIT_EXCEEDED` and `PROTOCOL_RATE_LIMITED`. `onlyRateLimited` counts such a node error as
  a rate limit, and a write whose every failure was a rate limit is sent again (A.2). So a row
  naming one could make the router re-send a write a node already executed, and hold off the
  providers that answered.
- **Any `PROTOCOL_*` verdict** (38 of 134, the router's internal category). These are the
  router's own, never a node's reply.
- **`CHAIN_TX_ALREADY_KNOWN`.** The read-your-writes logic turns on it
  (`read_your_writes.go:218-231`), so its recognisers stay in code and run in the locked prefix
  (2.4): JSON-RPC's "already known" rows (`error_classifier.go:310, 311, 316`), REST 409 (`:449`)
  and XRP's `tefALREADY` (`:117`, 3.5).
- **`UNKNOWN_ERROR`.** A row that says "unknown" shouldn't exist.
- **A verdict the chain can't see.** The name must be one the chain defines or inherits after
  2.5, or a router-owned verdict outside this list.

**Matches a row may not use.** A row also fails when its match could replace a status rule or hide
a rate limit, on any interface it applies to. A row without `interface` applies to every interface
its chain serves after inheritance, and the validator checks it against each one:

- **`code` alone can't be a status.** No HTTP status (100 to 599) on any interface, because REST
  and GraphQL classify with a status as the code, and JSON-RPC does too whenever the body carries
  no JSON-RPC code (A.1). And no `code` at all on `grpc`, where the code is the gRPC status, and
  the router decides every status that has a row or was left without one on purpose (2.4).
- **Among HTTP statuses, `code` with `contains` may name only a node-answer status.** On `rest`
  that's 400, 404, 410, 422 or 500. On `graphql`, `jsonrpc` and `tendermintrpc` it's 400 or 500
  only: the evidence for the other three is all REST, and a JSON-RPC 404 is a route refusal. On
  JSON-RPC the status reaches the classifier as the code only when the body carries no JSON-RPC
  code (A.1). That lets a row read a node's own text inside one, the way the router's own
  `CodeAndMessage(400, …)` and `CodeAndMessage(500, …)` rows do (`:404, 413-416`). Every other
  HTTP status the router has a rule for is locked (2.4 step 2) and can't be named at all. A `code`
  outside 100 to 599 is an ordinary chain or JSON-RPC code, and this rule doesn't touch it. Two
  more checks apply inside the node-answer statuses:
  - inside 400, 404, 410 and 422 the row's `error` must be a verdict the router doesn't blame the
    endpoint for (`EndpointAtFault` false: a caller error, a data-scope or a node-capability
    verdict). Inside 500 a row may excuse the endpoint, which is MAG-4061's whole point;
  - inside 404 the text may not match the route-refusal phrases the transport table reads
    ("endpoint not found", "route not found", "path not found", "method not allowed",
    `:390-393`), so a `contains: "not found"` row can't swallow a gateway's "route not found".
- **`code: 429` fails** in any form.
- **A `contains` or `regex` fails if it matches** a rate-limit phrase (the `NODE_RATE_LIMITED`
  rows' text and `GRPCRateLimitTexts`), an already-known phrase, or the text a locked status
  reaches the classifier as: the `HTTP <status>: ` prefix and the bare status number that
  `HTTPStatusContains` reads. The validator runs each of those texts through the row's matcher.

**Banning a verdict isn't enough on its own.** A row can name an allowed verdict and still override
a protected decision, just by matching first (2.4 has the two examples). So the protected decisions
run in the locked prefix, ahead of every spec row, whatever a row's text says. The checks above are
the early warning in CI; the prefix is the guarantee.

**Which HTTP statuses are locked, and why.** The router's own table already splits its 38 status
rows (`error_classifier.go:421-449, 555-591`) into answers that come from the node and decisions
about the endpoint in front of it. The lock follows that split:

- **Readable by chain rows on REST: 400, 404, 410, 422 and 500.** Their bodies are the node's own
  answer on the REST chains the router serves: Horizon's rejected transaction and `not_found`,
  Aptos's `resource_not_found` and `version_pruned`, toncenter's `{"ok":false, …}` inside a 422 or
  a 500, the Cosmos gRPC-gateway's pruned height inside a 500, sidecar's and nodeos's request
  errors (`restMessage_test.go:674-686`, `rest_integration_test.go:282-322, 361-374, 398-410,
  708-719`, MAG-4061). No gateway or CDN emits 410 or 422. Off REST only 400 and 500 are readable:
  nothing shows a node answering inside a JSON-RPC or GraphQL 404, 410 or 422, and a JSON-RPC 404
  is a route refusal (`:569`).
- **Locked: credentials and refusals.** 401, 402, 403, 407, 426 and 451 are the endpoint answering
  about its own configuration (`NODE_ACCESS_DENIED`, `:436-440` and `:564`;
  `error_codes.go:324-339`). A row reading inside one could strand a request on a refusing
  provider, or blame it.
- **Locked: the timeout.** 408 is the endpoint giving up on the request, at fault like a 503
  (`:441-442`).
- **Locked: HTTP framing and negotiation.** 405, 406, 411, 412, 413, 414 to 417, 428 and 431 are
  the HTTP layer talking, never the chain (`:421-431, 570-571`). Aptos's and Horizon's 415 is the
  router's own Content-Type problem (`rest_content_type.go:24-27`,
  `direct_rpc_connection_test.go:648-651`).
- **Locked: 409, 429 and 501.** 409 drives read-your-writes (`:449`), 429 drives hold-off and the
  write re-send (`:572`), and 501 is a route the node doesn't implement, retried elsewhere
  unblamed (`:574-577`).
- **Locked: the edge.** 502, 503, 504, 520 to 527 and 530 are a gateway or CDN speaking in the
  node's place. Horizon's `TRY_AGAIN_LATER` and a syncing Beacon node do answer inside a 503, but
  503 carries endpoint health; if those deserve another verdict, that's a router rule change to
  ticket.

On gRPC, every status the router has decided is locked, because on gRPC the status is the code,
and the ticket locks the gRPC status rules. The GraphQL reader turns Sui's own extension codes
inside a 200 into a status (`graphqlMessage.go:33-55`). `RESOURCE_EXHAUSTED` → 429,
`FEATURE_UNAVAILABLE` → 403 and `REQUEST_TIMEOUT` → 504 land on locked statuses, so a Sui row
can't reinterpret them. `INTERNAL_SERVER_ERROR` → 500 lands on a readable one, so a Sui row inside
500 may excuse it, the same as a real 500. A mutation's caller-side error is filed as a 400
(`:503-504`), also readable. (`UNAVAILABLE` → 503 and `DEADLINE_EXCEEDED` → 504 are kept for
non-Sui servers, `:46-47`.)

The five readable statuses are a departure from the ticket, which locks the HTTP status rules
wholesale (1.3). One thing the carve-out doesn't do: it doesn't close MAG-4061 on its own. At
`6c237073`, `shouldFailSessionForResult` (`rpcsmartrouter_server.go:2585-2620`) scores every 5xx
before it reads the fault flags, so a TON row inside a 500 changes the verdict but not the
endpoint's availability. That second half is router work under MAG-4061.

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
- **Record the signature before retry decisions.** Once any attempt has returned a slot-used
  answer, the write cannot open another fan-out, including through the all-rate-limited exception.
  This holds even if a classifier labels that same answer as a rate limit. The flag survives
  aggregation of other replies and cannot be cleared by a spec row. An accepted sibling still
  follows the router's accepted-result rules; reads keep ordinary classification.
- **Proposed signatures.** MAG-4898 completes them per node software:

  | Family | Code | Message | Today's verdict |
  |---|---|---|---|
  | any | any | contains "nonce too low" | `CHAIN_NONCE_TOO_LOW` |
  | any | any | contains "nonce is too low" (Nethermind's processor, `error_classifier.go:294`) | `CHAIN_NONCE_TOO_LOW` |
  | any | any | contains "account sequence mismatch" | `CHAIN_INVALID_SEQUENCE` |
  | XRP | any | `tefPAST_SEQ` at the start, after an optional anchored `HTTP <status>: ` prefix, with a token boundary | `CHAIN_NONCE_TOO_LOW` |
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
- Match the original request's method identity using the interface parser's canonical form, not
  an api display name or the generated `Default-` prefix. REST identities include the HTTP verb
  and route; gRPC uses the full service/method. Family comes from the router's chain identity,
  not from an editable error section. Check each batch member before aggregation, while retaining
  existing batch and disabled-method rejection rules. This guard never enables a disabled api.
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
- **Before use**, every accepted request for a protected method is stateful and ineligible for
  cache reads and writes, whether its api was declared or generated. A spec's `deterministic`
  setting cannot undo that. Apply the guard before scope selection, cache lookup, provider
  selection and retry policy; an accepted batch containing a protected write cannot become a read
  through category aggregation.
- `cmd/specvalidate` exports the same table, so lava-specs CI fails a PR that clears `stateful`
  on a write before any router sees it. `check_stateful.sh` reads the table instead of keeping its
  own copy, which also ends its gRPC and Tendermint blind spot.
- Today the load-time lock flips nothing: all 132 writes on lava-specs `main` are already stateful.

**No vendor wording: the effective policy needs a signed approval.**

- **Allowlist.** A row's `source` must be on the router's allowlist of node-software and
  protocol-spec origins (the repositories of the node implementations the six families run, plus
  the protocol specs; MAG-4898 completes it), with the link pinned to a tag or a commit.
- **Evidence and review, in CI.** Fetch the cited file at its resolved commit and require official
  source evidence and representative replies for every matcher. An exact literal check is useful
  lint, but the longest literal in a regex does not establish the origin of its other alternatives
  or wildcards. A spec owner must approve the complete matcher and its evidence, including each
  regex alternative; write rows also need the write-safety owner. A vendor phrase attached to an
  official URL fails that review. Signing attests to this reviewed evidence; it is not an automatic
  proof that text is vendor-free.
- **Approve the complete effective policy.** A manifest entry binds the chain id, schema version,
  interfaces declared by the expanded spec, resolved error-import graph and its source-section
  digests, the chain's effective verdicts after overrides, and the final ordered rows after
  overrides and removals. Every row and verdict field is included.
  An intentionally empty result has an entry too. Hash membership of individual rows is
  insufficient: reordering,
  dropping or recombining approved rows can change the first match without changing any row.
  The manifest also records the originating lava-specs commit and the resolved official-source
  commits and file digests used in review. A citation tag moving later does not change that
  evidence.
- **Bind activation permission too.** The signed approval names the deployment context (candidate
  or general deployment) and the permitted activation stage (`shadow`, `reads` or `all`) for the
  policy digest. The router applies the most restrictive of this permission and its configured
  setting. The pipeline can advance permission for unchanged policy content after the required
  evidence passes; changing a source or setting cannot advance it. An approval for shadow
  evaluation is not an approval to affect responses.
- **One canonical representation.** The validator and loader share the canonical serializer and
  hash algorithm. It rejects duplicate JSON keys, preserves array order and distinguishes absent
  imports from `[]`. Its format version is part of the signed payload. CI and every supported
  loader must produce identical policy digests from the same inputs; ambiguity rejects the policy.
- **Verify before activation.** A trusted CI promotion job signs the manifest after the gates in
  section 5. The router embeds the verification trust roots and recomputes each effective policy
  from the loaded source. The manifest format must be supported, and both the signature and the
  complete policy digest must match before any external row acts. Unknown signers, revoked
  approvals, missing entries and digest mismatches fall back under 4.4. There is no URL,
  local-folder or configuration bypass for ordinary releases.
- **Deliver specs and approval together.** CI publishes an artifact containing the spec snapshot,
  manifest and signature; the bundle in the router image has the same components. A local folder
  works when it carries the matching artifact. A raw Git checkout or GitHub source archive without
  its approval can supply api definitions, but its error policies fall back to the bundled ones.
  Customer-authored rows wait for the overlay in section 7 and its separate approval path.
- **Trust lifecycle is a release gate.** Before external policies act, R&D must specify artifact
  delivery, key custody and rotation, and revocation of a signer or individual policy digest. A
  newly trusted key or revocation must reach routers through a trusted router release or an
  independently authenticated trust update, never through the spec being verified. Offline routers
  retain their installed trust state; this draft does not claim immediate fleet-wide revocation or
  protection from replaying an old, still-approved policy. Freshness is deferred with versioning.

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
of scope (1.3), it's the only version number lava-specs carries. The flag vocabulary a verdict may
use (2.3) is part of the format: a router version knows a fixed set of sub-categories and flags, so
a new value is a new schema version, and a section using it on an older router is dropped like any
other unknown schema (4.4).

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

- The repo's `specs/` folder stays for local runs and tests, but `make specs` fills its spec files
  from `SPEC_COMMIT`. CI compares those files byte-for-byte with the named commit and separately
  verifies the generated manifest and signature. `SPEC_COMMIT` and approval artifacts are release
  metadata, not extra spec files to compare with the upstream Git tree.
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

Whenever a chain's effective error policy from the loaded source can't be used, the router uses
that chain's complete effective policy from the bundled specs instead. That happens when:

- the section has a `schema_version` the router can't read (a newer schema on an older router);
- a row or a verdict breaks a rule in section 3, a row names a verdict the chain can't see (3.2),
  two loaded specs collide on a verdict name or code, the effective policy has no valid signed
  approval, or an import dependency cannot be resolved and validated;
- the router couldn't load the source at all (the host doesn't answer, the URL or ref doesn't
  exist, what comes back isn't an archive, a spec doesn't parse). Then the router boots on its
  bundled specs for everything, not just the errors sections.

Resolve the external and bundled graphs separately. Do not replace a rejected external parent
with a bundled parent while retaining the external child's overrides: that combination was never
approved. Each chain whose effective policy depends on the rejected input falls back as a whole;
an unrelated chain with a valid policy keeps it. API definitions remain from the loaded source
unless the whole source failed, and the write-method guard still applies to them. Validate the
fallback against the interfaces the chain actually serves; a bundled row cannot silently acquire
an interface that was outside its approved context.

In each case the router logs the chain and the cause, including the row or dependency when known,
and counts `smartrouter_spec_errors_section_dropped_total{chain_id, reason}`. The reasons are a
closed set: `schema_unknown`, `rule_violation`, `approval_missing`, `signature_invalid`,
`policy_mismatch`, `approval_revoked`, `dependency_invalid`, `verdict_collision` (two unrelated
specs define the same verdict name or code, 3.2), `source_file_failed` (a file that failed to load
beside good ones), `bundled_no_rows` (the bundled specs have no effective rows for the chain) and
`interface_outside_context` (a served interface the bundled policy's approval doesn't cover). A
chain can carry more than one at once, say a drop reason and `bundled_no_rows`.
The source as a whole fails only when nothing loads; that counts
`smartrouter_spec_source_fallback_total{reason}` with `host_unreachable`, `ref_not_found`,
`not_archive` or `no_spec_parsed`. The router boots. Fallback is one atomic choice per chain, made
before serving requests, not a partially accepted list of rows.

**The fallback is never silent.** Logs and counters record that it happened. A gauge shows that
it's still happening, and an alert tells someone:

- `smartrouter_spec_policy_fallback{spec, apiInterface, reason}` is 1 while a chain and interface
  serve on their bundled policy, or on the reduced transport-only coverage described below, and 0
  otherwise. `reason` is the closed set above. Every reason is written at boot, zeros included,
  the same complete-map discipline as `smartrouter_csm_blocked_providers_by_reason`
  (`smartrouter_metrics_manager.go:581-584, 1688-1700`), so a rule matches on presence (the
  series exists at 0) rather than `absent()`, and no reason is missing from the enum. The
  decision is made once per boot; a fix is a new source and a restart.
- `smartrouter_spec_source_fallback{reason}` is 1 while the whole source has been replaced by the
  bundled specs, with the source reasons above.
- The gauges say `spec` and the counters say `chain_id`. Both are the chain id; the router already
  uses both names (`error_metrics.go:44`, `smartrouter_metrics_manager.go:584`). Alloy adds
  `chain` and `router` from the pod labels to every series, and the store adds `cluster`.
- Two of the three Grafana alert rules in 5.6 read these gauges; the third reads the lock counter.
  smart-router-infrastructure is where every router alert lives
  (`grafana/alert-rules/smart-router.yaml`); the chart ships none.
- `/livez` and `/readyz` don't move. Readiness reflects provider health
  (`smartrouter_metrics_manager.go:766-793`), and a router on its bundled policy is serving. The
  alert is the signal.

For comparison, today a bad source is fatal at boot and a bad file is silent (A.3). The alerts that
fire for a crash-looping router name the pod or the host, never the spec. Both cases become a
fallback with a gauge and an alert; the skipped-file case falls back per chain with
`reason="source_file_failed"`.

**The fallback is the same before and after the switch.**

- Before the switch, the bundled section and the code family table agree (Step 1 proves it), so
  falling back changes nothing.
- After the switch deletes the family tables, the bundled section is the only built-in
  classification a chain has. It stays tested: 5.4 runs the fallback after the switch.
- If the bundled specs have no effective rows for the chain, or a served interface is outside the
  bundled policy's approved context, that chain/interface gets the locked prefix and remaining
  transport rows only. The log identifies that reduced coverage. This is not silently counted as
  successful activation of the external policy.

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
- `cmd/specvalidate`, the schema, the `spec_guards.yml` jobs, owner-review checks, the source
  evidence checks, and the candidate-to-release approval pipeline (5.2, 3.4);
- the locked prefix of 2.4 step 2, and the router-owned XRP rows (3.5);
- the validator's side of the per-chain verdict registry: the exported router-owned set, the
  reserved code range, and the exact-copy check for the 41 migrating verdicts (2.3, 3.2). Loading
  verdicts from a spec is reader work, so it lands with Step 1;
- the write-method table, with both the load-time and the request-time lock (3.4);
- the slot-already-used signatures (3.3);
- the AVAX* keys in `chainFamilyMap` renamed (section 6);
- the Helm chart's default moved from the bare URL to the bundled specs.

**Step 1: side by side.** The six family tables (minus XRP's three router-owned rows) are copied
into their root specs with no verdict change, and so are the 41 verdicts only those tables use,
with their exact codes and flags (A.1); the 17 family-table verdicts that transport rows or router
logic also use stay router-owned and are only named. STRKS imports STRK, and each of the 36 chains
that would gain rows gets `"errors": {"schema_version": 1, "imports": []}` (Appendix B), so every
chain keeps exactly today's rows and verdicts. No router acts on that commit yet.

The router reads the sections, rows and verdicts both, and evaluates spec rows next to its code
rows, but acts on the code rows. A migrated verdict's copy has to match the router's definition
field for field to load at all (3.2), so the comparison here is by verdict name; the flags are
proven equal by validation, not by traffic.

Three facts about today's code shape the reader, whatever R&D's design turns out to be. The
registry maps are filled at init and read without locks, and `registerError` panics on a
duplicate code or name (`error_registry.go:602-629`), so spec verdicts live in a per-chain policy
resolved at load, outside `errorRegistry`, and an override is a per-chain view, not a second
registration. `ClassifyError` is keyed by `ChainFamily` (`error_classifier.go:788, 801`); the
chain's policy becomes that key. And the router compares its own verdicts by pointer identity
(`svm_block_hash_retry.go:99`, `read_your_writes.go:230`, `rpcsmartrouter_server.go:1599-1603`),
so a row that names a router-owned verdict must resolve to the router's own `*LavaError`, never to
a copy built from the spec, or read-your-writes and the SVM block retry stop firing. Two metrics
track the run:

- `smartrouter_spec_error_rows_mismatch_total{chain_id, kind, row_id, code_error, spec_error, scope}`
  counts three kinds of disagreement:
  - `differ`: both matched, with different verdicts;
  - `code_only`: a code family row matched and no spec row did, meaning a chain lost coverage;
  - `spec_only`: a spec row matched and no code family row did, meaning a chain gained it.
- `smartrouter_spec_error_rows_hits_total{chain_id, row_id}` counts every spec-row hit.

The expected count for every kind is zero. Any mismatch is a bug in the move. Compare the old and
new family stages with the same locked prefix and router-owned rows on both sides; count coverage
at the family stage as well as the final verdict, so a transport fallback cannot hide a lost row.
The Step 0 changes are tested separately and are not unexplained shadow mismatches.

**Step 2: switch.** A router setting with three values decides which rows act. Here's the order in
each window:

| Setting | Read apis | Write apis |
|---|---|---|
| `shadow` (Step 1) | prefix, family table, transport; spec rows counted only | same as reads |
| `reads` (after 7 days with zero read mismatches) | prefix, spec rows, family table, transport | prefix, family table, transport; spec rows counted only |
| `all` (after 30 days with zero write mismatches) | prefix, spec rows, family table, transport | prefix, spec rows, family table, transport |
| tables deleted (next release) | prefix, spec rows, router-owned rows, transport | same as reads |

- "Prefix" means 2.4 step 2, which is active from Step 0.
- The table describes the effective stage after applying the signed activation permission (3.4).
  Flipping the setting doesn't need a release, but cannot advance a policy beyond its approval.
  An early request for `all` leaves the policy at its approved stage and logs the reason.
- During the initial migration, the controlled candidate first runs in shadow, then may act on
  reads after the seven-day gate and writes after the thirty-day gate. Promotion to general use
  follows the active canary window for the stage being promoted. A read-only approval keeps writes
  on their code tables; it does not approve the `all` stage.
- A zero counter without traffic is not evidence of parity. Record the policy digest, the start
  and end of each window, and observed coverage for every chain and scope being enabled. Exercise
  unused rows with the fixture suite. A policy change or mismatch restarts its affected window.
- Rollback before deletion restores the previous setting and approved artifact. After deletion,
  `shadow` cannot restore absent code tables: rollback requires the previous router release and
  its tested bundle. Do not leave the switch UI implying otherwise.
- The six family tables, except the router-owned rows, are deleted in the release after `all` has
  run through one canary window, and with them the code definitions of the 41 migrated verdicts,
  which are spec-owned from then on. From that release on, the fallback is the bundled sections
  (4.4), and 5.4 runs it in the router's CI.
- A release that has deleted the tables bundles an `all`-approved policy. It does not activate an
  external policy with only `shadow` or `reads` permission: it retains the complete bundled policy
  for that chain until the external policy is fully approved. This avoids relying on deleted code
  or combining rows from different policy snapshots.
  The old migration selector no longer controls serving in that release. A stale `shadow` or
  `reads` setting logs a configuration warning and retains the complete bundled policy; it cannot
  request execution of tables that no longer exist.

**Step 3: per-chain rows and verdicts open up.** MAG-4898 lands researched rows per chain through
the gates below, and with them the first chain-specific verdicts (TON's 500 answers in 2.6, for
one). The 36 gains of Appendix B open one at a time, by removing a chain's `"imports": []`. Each of
those is a behaviour change, named in its PR with a product call and, for a new or changed
verdict, R&D's review (5.2).

### 5.2 lava-specs CI, on every pull request

An error policy is eligible for general deployment only after the gates in 5.2 to 5.5 pass. The
controlled canary is the production evaluation before that promotion. These guarantees cover the
error policies; signing them does not certify unrelated api definitions from arbitrary sources.

- **Schema check.** A JSON Schema for the `errors` section, kept in lava-specs and applied to every
  changed spec.
- **Locked-rule check.** One Go validator in smart-router, `cmd/specvalidate`, is the only
  implementation of 2.3, 3.2, 3.3 and 3.4. lava-specs CI builds it from each router release the
  boot matrix covers (5.3), and each router runs the same code at boot, so CI and boot can't
  disagree for any release in service.
- **Stateful check.** The validator's write-method table (3.4), run on every PR as a job in
  `spec_guards.yml`. Today `check_stateful.sh` only runs inside the agent, once, when a PR opens
  (A.4).
- **Write-safety job** in `spec_guards.yml`: the validator's write rules and a diff of effective
  policies for every affected chain, including descendants. Compare row contents by `id` and
  compare their execution order separately. Changes to imports, overrides, removals, interface
  coverage or an optional section can change a write policy even when no local row changed.
  Any such change affecting `write`, `both` or `slot_used` requires write-owner review.
- **Source evidence and approval** (3.4): every matcher has reviewed official evidence and
  fixtures. Every root and descendant policy passes the same validation and compatibility tests
  before its digest is eligible for signing.
- **Owner review for write rows.** `CODEOWNERS` matches paths and can't name a JSON section, so the
  gate is a required check instead. It computes whether write policy changed and requires an
  approving review from the owners team named in a new `CODEOWNERS` file for the current PR head.
  It may add `write-rows` for visibility, but removing a label cannot bypass the check. The same
  file makes the spec owners review source evidence; a changed commit invalidates stale approval.
- **R&D review for verdicts.** The same kind of check computes whether any chain's effective
  verdicts changed (a new verdict, changed flags, an override, or an inherited verdict a chain
  gains or loses) and requires an approving review from the R&D owners team named in `CODEOWNERS`.
  Flags decide retry, fault and scoring, so the people who review that in the router review it
  here. The check also fails a name or code defined by a spec the chain doesn't inherit from, a
  code outside the reserved range, an override that changes a code, and a migrating verdict whose
  copy differs from the router's (3.2).
- **Behaviour changes named in the PR.** With no change log (1.3), the PR is the record. A PR that
  moves a verdict (a new or changed `error`, a `match` that widens or narrows a verdict, a removed
  or disabled row, a changed `scope`, a new `slot_used`, a new or changed verdict or its flags, or
  a chain that gains or loses inherited rows or verdicts) lists each affected chain in its
  description, with the old verdict or flags, the new ones and why, and carries a
  `behaviour-change` label. The replay and the coverage check (5.3) fail a PR that
  moves a verdict without it.
- **`check_unused_fields.sh`**, which keeps `description` and `title` out of rows, moves into
  `spec_guards.yml` so it runs on every PR (A.4).

**Testing and signing are separate stages.** PR validation must not require a production signature
that can only be issued after merge, and PR-controlled code must not run with the signing key:

1. The PR jobs resolve candidate policies, validate them and run the boot matrix. End-to-end
   signature tests use an isolated test trust root; ordinary release binaries never trust it.
2. After merge, a trusted job repeats the required checks on the exact candidate snapshot and
   verifies its owner approvals. It publishes the candidate artifact for the controlled canary.
   The canary uses a separate trust context unavailable through an ordinary spec-source setting.
3. After the canary passes, the promotion job issues the production-trusted manifest for those
   same policy digests and the activation stage that passed. Any policy change creates a new
   candidate and reruns its gates. General deployments and the release bundle consume only this
   promoted artifact; an approval for reads cannot be used as evidence that writes passed.

Artifact publication and the canary trust mechanism must be specified and tested before Step 1;
"sign every merged commit" is not a substitute for this promotion gate.

### 5.3 Against the router

- **Boot matrix.** Every chain whose effective policy or api context changed, including importing
  descendants, boots with the PR's specs as the source on
  every router release in service from the schema's `min_router` up. Each release runs its own
  validator at load. A section dropped at boot (4.4) fails the job unless the PR says so.
- **Replay.** The 102 cases in `error_fixtures.json`, plus the router's own classifier and status
  suites (`TestRESTStatusRows`, `TestRESTStatusRowsDoNotReachOtherTransports`,
  `TestRESTMessageRowsBeatTheStatusRow`,
  `TestGRPCRateLimitTexts_TheCodeRowsAboveStillDecide`, `TestEndpointFault_AnswerShapes`,
  `TestGRPCStatusError`), run against the spec rows and verdicts and must give the same verdict
  name with the same flags, unless the PR names the case and its new verdict (5.2). The fixtures
  carry the expected flags as well as the name, and grow with MAG-4898: one case per row, citing
  the row's `source`. The fault decision for a spec verdict is recorded in its fixture, not in the
  router's hand-written `faultAxisTable` (`error_fault_axis_test.go:32-149`), and the replay
  checks that it equals `EndpointAtFault` of the verdict's flags. The rows
  `TestFaultAxis_RegistryContract` keeps for the 41 migrating verdicts (`:151-164`) move to a
  fixture read from the bundled sections when their code definitions go, and
  `TestRegistry_CodeRanges` (`error_registry_test.go:42-57`, 1000 to 4999 today) learns the
  reserved range.
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
    409; a REST 402, 408, 451 and 501 whose body a family row matches; a gRPC Unavailable,
    ResourceExhausted, NotFound, InvalidArgument and Aborted; a JSON-RPC reply that arrived as
    `HTTP 503: ` (`NODE_SERVICE_UNAVAILABLE`); and one that arrived as `HTTP 408: `
    (`UNKNOWN_ERROR`: JSON-RPC has no 408 row, so the lock yields the transport's no-verdict
    answer, not the chain row's).
  - *Must keep the router's own text rows ahead of the status:* a REST 403 and 405 whose body says
    "route not found" stay `NODE_ENDPOINT_NOT_FOUND` (`rest_status_rows_test.go:139`).
  - *Must stay readable by chain rows (the node-answer statuses, 3.2):* nodeos's REST 400
    "unknown block", the Cosmos gateway's REST 500 "is not available, lowest height", Aptos's REST
    410 `version_pruned`, toncenter's REST 422 `failed to parse ton_addr`, and a REST 404 with a
    chain's own `not_found` body. Each keeps the verdict the router's own row or status gives it
    today, and a spec row inside the same status can specialise it.
  - *Must fail validation:* `{code: 408, contains: …}` and `{code: 402, contains: …}` (locked
    statuses); `{code: 404, contains: "not found"}` (matches the route-refusal phrases); and a
    row inside 400 naming `NODE_INTERNAL_ERROR` (blames the endpoint inside a caller-error
    status).
  - *Must isolate the HTTP status decision:* a JSON-RPC `-32602` with HTTP 503, HTTP 503 with
    "insufficient funds" in the body, HTTP 401 on REST and JSON-RPC, and HTTP 409 on JSON-RPC.
    The first two take `NODE_SERVICE_UNAVAILABLE`; the 401 cases retain their transport-specific
    verdicts; JSON-RPC 409 stays `UNKNOWN_ERROR`. A node's own text beginning `HTTP 503: ` without
    a corresponding transport status must not acquire the HTTP lock.
  - A relay-policy test adds that a stateful write whose every answer is the min-fee rejection
    stops after its fan-out and holds no endpoint off.
- **Attack rows.** The two rows from the review of revision 2 must not change a verdict:
  `{contains: "block not found"}` → `CHAIN_BLOCK_NOT_FOUND` against a gRPC InvalidArgument, and
  `{code: -32000}` → `CHAIN_TX_REJECTED` against EVM's "already known".
- **Slot-already-used pairs.** Every signature in 3.3, Bitcoin's `-25` and `-26` included, sent as a
  complete code-and-message pair through the stateful path, must end unknown. The same pair on a
  read keeps its verdict. Every fixture whose verdict is `CHAIN_NONCE_TOO_LOW`,
  `CHAIN_INVALID_SEQUENCE` or `CHAIN_STARKNET_INVALID_NONCE` must match a signature. The last two
  are migrating verdicts, so after the switch the router's CI reads their definitions from the
  bundled specs.
- **Slot-used retry guard.** Mix a slot-used answer with rate-limit answers, and separately let
  the same answer also match a rate-limit phrase. Neither case opens another fan-out. Include an
  accepted sibling to confirm accepted-result handling and XRP replies with an HTTP prefix.
- **Coverage check.** For every chain, the rows and verdicts it gets after inheritance are
  compared with what it gets at the PR's fixed base commit. A chain that gains, loses or reorders
  effective rows, or whose effective verdicts change in name or flags, fails unless the exact
  behaviour change is declared and approved under 5.2. An approved removal
  or disabling row is possible; an unexplained coverage change is not.
- **Row fixtures and generated tests.** For every changed row, supply a source-backed positive
  reply and a near-miss; add examples for each regex alternative. CI generates a test for each
  changed row from those fixtures and checks them against the complete effective policy, not only
  the isolated matcher. A regex need not have an automatically generated example, and a later row
  can be shadowed by an earlier one. The test records the intended winning
  row, its verdict (name and flags) and class, and reports unintended unreachable rows. Synthetic
  code and literal cases supplement the real replies rather than replacing them.

### 5.4 Bad-source and lock tests (router repo)

One test per failure. Each one asserts what the router loads, that the log names the cause, and
that the metric carries the reason:

- With no `--use-static-spec`, the router boots on its bundled specs.
- A source that isn't an archive, a ref that doesn't exist, a host that doesn't answer, and a spec
  that doesn't parse each boot on the bundled specs, with their own reason on
  `smartrouter_spec_source_fallback_total`.
- A row that breaks each rule in 3.2, 3.3 and 3.4, a missing approval, a wrong-key signature, a
  revoked approval, and an unknown schema each fall back to the affected chain's bundled policy.
- A verdict that redefines a router-owned name or code, carries `rate_limit`, uses a sub-category
  the router doesn't know, takes a code outside the reserved range, or differs from the router's
  definition of a migrating verdict, and a row naming a verdict the chain can't see, each make the
  policy invalid and the chain falls back. Two loaded specs that define the same name or code
  without inheriting it both fall back with `verdict_collision`. A chain-specific verdict with
  valid flags loads, its rows classify to it, and the relay path reads its flags: a `data_scope`
  verdict isn't scored against the endpoint, a non-retryable one stops the read.
- A child that overrides an inherited verdict's flags gets the override on its own traffic while
  the parent and its other children keep the original: with ETH1's verdict retryable and BASE's
  override non-retryable, the same answer fails over on ETH1 and stops on BASE. An override that
  changes the `code`, and two incompatible definitions of one inherited name without an explicit
  override, each fail validation.
- Reordering or deleting approved rows, deleting a section, altering imports, moving a row to a
  different chain, or expanding its interfaces fails policy-digest verification even if every
  individual row appeared in a previously approved manifest. A correctly approved empty policy
  remains valid.
- A missing or invalid parent makes each affected descendant use its complete bundled policy;
  unrelated chains retain their approved external policies. Test cycles, mixed schemas,
  duplicate/conflicting ids and diamond imports without silently mixing external and bundled rows.
- A regex with one official literal and a separate vendor-only alternative fails source review;
  an unapproved custom policy never acts by attaching an official URL or a candidate signature.
- Requesting `all` with shadow or read-only permission cannot activate write rows. A production
  binary rejects a candidate-only approval. After table deletion, a partially approved external
  policy retains the complete bundled policy; it never falls through to missing code tables.
- **After the switch**, with the family tables deleted: Solana's section dropped from the loaded
  source still classifies `-32005` "Node is unhealthy" as `NODE_SOLANA_UNHEALTHY`, from the bundled
  section.
- A spec that marks `eth_sendRawTransaction` with `stateful` 0 is loaded stateful, and
  `smartrouter_spec_lock_enforced_total{rule="stateful"}` counts it.
- A spec that leaves `eth_sendRawTransaction` out still relays it stateful and uncached, and
  `smartrouter_spec_lock_enforced_total{rule="stateful_default"}` counts it.
- Repeat the cleared, omitted and disabled-method cases for each supported write interface;
  disabled methods remain rejected. Check accepted batch handling, cache bypass and a declared
  write whose spec sets `deterministic: true`, not only the final `stateful` flag.

**The fallback has to be visible, not just happen.** Each fallback case above also asserts:

- `smartrouter_spec_policy_fallback` reads 1 for the affected chain, interface and reason, 0 for
  every other reason and for the unaffected chains, and 0 again when the test re-runs the load
  against the same collector with a valid policy. The whole-source cases assert
  `smartrouter_spec_source_fallback` the same way. The unit test reads the collector; nothing
  scrapes the metrics port.
- The alert rules are proven in smart-router-infrastructure by extending the promtool replay that
  already covers `sr-router-unhealthy`
  (`tests/grafana-alert-rules/test_silent_when_idle.py:129-156`): render each per-tenant rule
  through `expand_tenant`, hold the gauge at 1 past its `for` and assert it fires, then return it
  to 0 and assert it resolves (a case the existing fixture doesn't have). For
  `sr-spec-lock-enforced`, the fixture steps the counter once inside the 30m window and the rule
  fires; a flat counter keeps it silent. The shipped file passes
  `scripts/grafana-alert-rules.py check`. The infra CI runs both on every PR that touches
  `grafana/**` or the tests (`.github/workflows/grafana-plan-pr.yaml:121-124`).
- The rules are published only once a router that emits the gauges is on the fleet. `check` can't
  tell whether a metric exists (`docs/OBSERVABILITY.md:1325-1330`), so a rule published early would
  stay green forever.

The ticket's "stale" and "unpinned" source tests are deferred with Must-have 1 (1.3); they return
if versioning does (7).

### 5.5 Canary

Managed deployments evaluate a candidate router/bundle or external policy artifact on one canary
router per cluster first. The rest consume it only after promotion (5.2). Spec-only releases use
the same gate; a signature must not make a policy generally available before its canary.

- **Window length.** The proposal is one full day of production traffic, so each chain's daily
  cycle is covered. This is a minimum for ordinary compatible policy updates. The initial migration
  still requires the seven-day read and thirty-day write windows in 5.1; signing or passing a
  one-day canary does not waive them. Candidate policies can be evaluated in shadow while those
  windows run, before they are eligible to act or be promoted for general use.
- **What it compares.** `smartrouter_errors_total` by `error_name` and `chain_id`, before and after
  (`protocol/metrics/error_metrics.go:42-44`). The dropped-section, source-fallback and
  lock-enforced counters must stay at zero, and both fallback gauges (4.4) must read 0 on the
  canary for the whole window.
- **On-prem** only gets the tested pair.
- **A failed canary rolls back** to the previous approved policy artifact; a router/bundle change
  also rolls back the router release. A spec-only fix does not require a new router binary when
  its schema and verdicts are compatible. The next router release bundles the promoted commit.

### 5.6 Observability

| Signal | Where | Why |
|---|---|---|
| `smartrouter_spec_errors_section_dropped_total{chain_id, reason}` | router | a bad, unsigned or unreadable section shows up instead of failing silently |
| `smartrouter_spec_source_fallback_total{reason}` | router | a source the router couldn't load shows up, with the reason |
| `smartrouter_spec_lock_enforced_total{chain_id, rule}` | router | a spec that cleared `stateful` on a write (`stateful`), or left a write out (`stateful_default`), shows up |
| `smartrouter_spec_error_rows_mismatch_total{chain_id, kind, row_id, code_error, spec_error, scope}` | router, steps 1 and 2 | the switch criterion |
| `smartrouter_spec_error_rows_hits_total{chain_id, row_id}` | router | which rows actually matter; feeds the geth-wording decision in section 6 |
| `smartrouter_spec_policy_fallback{spec, apiInterface, reason}` | router | which chains are on their bundled policy right now, and why; the alert reads it (4.4) |
| `smartrouter_spec_source_fallback{reason}` | router | the whole source was replaced by the bundled specs |
| `smartrouter_errors_total{error_name, error_category, retryable, chain_id}` | router, exists today | unchanged in shape; `error_name` now includes spec-defined verdicts, so a new chain verdict shows up on the existing dashboards without a change. A per-chain override means one `error_name` can carry different `retryable` values by `chain_id`; the label set already has both (`error_metrics.go:44`), but the comment that calls them 1:1 derivable and budgets "~100 registered codes" (`:21-28`) and the dashboards built on it need updating |

**Alert rules.** They go in smart-router-infrastructure, `grafana/alert-rules/smart-router.yaml`,
in the house style: a `uid`, a `title` and a `summary` that name the tenant, a `for`, a `== bool`
or `> bool` expression, `severity` as the only stamped label, and a description that names the
first thing to do (there's no runbook link convention). They're templated per tenant like
`sr-tip-frozen`. The repo's rule for who hears what is "a customer served worse is a page; a
stalled upstream the router covers is news", and a router on its bundled specs is still serving,
so all three are warnings. Severities stay a proposal for R&D and ops.

| Rule | Expression | For | Severity | Description says |
|---|---|---|---|---|
| `sr-spec-policy-fallback`, "{tenant}: a chain is serving on its bundled error policy" | `max by (cluster, location, router, spec, apiInterface, reason) (smartrouter_spec_policy_fallback{cluster="{tenant}"}) == bool 1` | 10m | warning | the chain (`spec`), interface and reason; for `schema_unknown` upgrade the router or move the source back; otherwise compare the source with the release's `SPEC_COMMIT` and fix the source |
| `sr-spec-source-fallback`, "{tenant}: the spec source failed to load" | `max by (cluster, location, router, reason) (smartrouter_spec_source_fallback{cluster="{tenant}"}) == bool 1` | 30m | warning | every chain is served from the release's bundled specs, not from the configured source (the chart default, lava-specs `main`, unless overridden); the reason; check the source URL and the host |
| `sr-spec-lock-enforced`, "{tenant}: a spec tried to weaken a write" | `increase(smartrouter_spec_lock_enforced_total{cluster="{tenant}"}[30m]) > bool 0` | 0s | warning | the chain and rule; `stateful` means a spec cleared the flag on a write, `stateful_default` means clients are calling a write the spec doesn't declare |

Each summary follows the per-tenant form, for example `{tenant}: {{ $labels.spec }}
({{ $labels.apiInterface }}) on {{ $labels.router }} is on its bundled error policy:
{{ $labels.reason }}`. The ticket asks for alerts on an unreadable schema and on a rejected
section; the first rule covers both through its `reason` label. The third is a should-have: a spec
that tried to clear `stateful` is a review failure somebody should hear about. 5.4 says how each
rule is proven before it's published.

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
- **The reserved code range and the docs that list verdicts.** Pick the range for spec-defined
  verdict codes. The proposal is 5000 to 5999, but the router's tests tie 1000 to 1999 to the
  internal category and 2000 to 4999 to the external one (`TestRegistry_CodeRanges`,
  `error_registry_test.go:42-57`), and `docs/ERROR-REGISTRY-DESIGN.md` documents the layers by
  range, so the spec range wants the same split (say 5000 to 5499 for `NODE_*`, 5500 to 5999 for
  `CHAIN_*`) and the test learns it. Decide too how the docs follow a verdict defined in a spec:
  `docs/ERROR-REGISTRY-DESIGN.md` and the public docs list the router's registry, and
  `docs/RELAY-END-EVENT.md:65, 138` define the event's `error_code` as "the error registry name";
  with revision 5 the source is the spec as well. Also name the R&D owners team for 5.2.
- **Approval artifacts and trust.** Choose the artifact delivery location, signing service and
  owners, canonical format, candidate trust context, and rotation/revocation mechanism. The router
  carries verification keys, never private signing keys. These are implementation decisions within
  the contract of 3.4 and 5.2, and must be closed before external policies act.
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
  request-time stateful lock and the wider prefix. Revision 4 also requires approval of complete
  policies and tested promotion, revocation, dependency fallback and rollback. Revision 5 adds the
  per-chain verdict registry: loading verdicts from specs beside the router-owned ones, the
  exported owned set, flag comparison in the side-by-side run, and the verdict review gate. R&D
  must size that work explicitly; the earlier estimate is not a commitment for this scope.

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

- Nimrod or Tom approves Must-haves 2 to 4, the format and the split, with the departures in 1.3
  in view.
- MAG-4910 is updated to record that Must-have 1 is out of scope, that chain rows may read text
  inside the node-answer HTTP statuses (3.2), and that a chain's spec defines the verdicts its rows
  point at (2.3).
- MAG-4898 is unblocked, with section 2 and Appendix B linked as the approved format and the first
  commit's coverage.
- The router work is ticketed: Step 0, Step 1 (reader, inheritance, side-by-side metrics), Step 2
  (the setting, the switch and the deletion), and the open items of section 6.
- The approval record distinguishes PRD approval from rollout readiness. Sections 3 to 5 are
  implementation requirements; the historical scratch probes do not satisfy the release gates.

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

**The registry.** `error_codes.go` registers 133 verdicts through `registerError`, plus
`UNKNOWN_ERROR` wired in directly (`error_registry.go:589-596, 608-630`): 65 `CHAIN_*` (codes 3001
to 3363), 23 `NODE_*` (2001 to 2150), 7 `USER_*` (4001 to 4007) and 38 `PROTOCOL_*` (1001 to
1053). Each carries a category (`CategoryExternal` for everything a node or caller answered,
`CategoryInternal` for the router's own 38), a sub-category (`none`, `data_scope` on 16,
`node_capability` on 7, `rate_limit` on 3; `unsupported_method` is in the enum but on no verdict
since MAG-2771, `error_registry.go:41`), `Retryable` and `MayHaveReachedNode` (true on five of the
nine connection verdicts: timeout, reset, closed, context deadline and context canceled; and on
`NODE_GATEWAY_TIMEOUT`, `NODE_BAD_GATEWAY` and `NODE_ABORTED`; the struct is at
`error_registry.go:157-170`). `EndpointAtFault` is derived from those flags in one place
(`:136-152`). Two maps hold the
registry, by code and by name, filled at init and read without locks; `registerError` rejects a
duplicate of either (`:602-629`). Router logic outside the registry and classifier files
references 25 registered verdicts by identity or code: 16 protocol, 4 node (`NODE_RATE_LIMITED`,
`NODE_GATEWAY_TIMEOUT`, `NODE_BAD_GATEWAY`, `NODE_ENDPOINT_NOT_FOUND`), 3 chain
(`CHAIN_TX_ALREADY_KNOWN`, `CHAIN_BLOCK_NOT_FOUND`, `CHAIN_SOLANA_MISSING_LONG_TERM`) and 2 user,
26 with `UNKNOWN_ERROR`. (A grep for `LavaError…` identifiers returns 29; four hits are a
Tendermint reply constant and two method-name fragments, not verdicts.) Of the 58 verdicts the six
family tables use, 17 are also used by transport rows or that logic, and 41 are used by a family
table alone (Solana 10, Bitcoin 8, Cosmos SDK 3, Starknet 16, NEAR 4; none of XRP's, which all
point at shared verdicts). Those 41 are the ones Step 1 moves into specs with their codes and flags
unchanged.

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

This PRD leaves all of this as it is (1.3).

**The release image ships no specs.** `docker/Dockerfile.release:26` and `Dockerfile.ci:13` copy
only the binary, and the goreleaser archives are binary-only too (`.goreleaser.yaml:48-57`). The
dev image copies the repo's `specs/` folder (`docker/Dockerfile:30`), and `docker-compose.yml:53`
defaults to it. That folder holds 15 files; 14 of them also exist in lava-specs, and all 14 differ
from their lava-specs copies.

**No source, no boot.** The router refuses to start without a spec source
(`rpcsmartrouter.go:2569-2581`). The error message still names the retired `--static-spec-paths`.

**A bad source is fatal, a bad file is silent.** A spec that can't be loaded or expanded stops the
boot (`rpcsmartrouter.go:2567-2581`, `:3244-3249`) and the pod crash-loops. In
smart-router-infrastructure the only alert that points at the pod is `sr-pod-crashlooping`, whose
own description says a rejected config and an OOM look the same from there; if every replica is
down, `sr-workload-down` and the hub's `sp-host-not-serving` page as well, and none of the three
names the spec. Files that fail beside good ones in a folder or a remote repo are skipped with a
warning and nothing else (`utils/keeper/spec.go:111-128`, `utils/specfetcher/fetcher.go:380-392`).
Neither state is exported anywhere.

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
They map to the structure introduced in revision 3 and kept since like this:

| Revision 2 | Revisions 3 to 5 |
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

This table records revision 3's changes, not a claim that its safeguards were complete. C.4 records
the additional constraints needed for policy approval and activation.

### C.3 Changes in revision 2

Section numbers and the tag-based wording in this table are revision 2's: C.1 maps the numbers,
and tags left with Must-have 1 (1.3).

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

### C.4 Changes in revision 4

| Gap | Required design change | Where |
|---|---|---|
| A set of approved row hashes does not approve their order or inheritance | Sign each chain's complete effective policy, its interfaces and dependencies, including deliberately empty policies | 2.2, 2.5, 3.4 |
| A regex's longest literal does not establish the origin of its whole matcher | Require official evidence, representative replies and owner review for the complete matcher; retain literal checks as lint | 3.4, 5.2, 5.3 |
| Signing only after merge conflicts with PR boot tests and can expose a policy before canary | Separate test trust, controlled candidate evaluation and production promotion of the same policy digests | 3.4, 5.2, 5.5 |
| A switch can request a stage whose observation window has not passed | Bind candidate/general context and activation stage in the signed approval; clamp the setting to that permission | 3.4, 5.1, 5.4 |
| Running the full transport table can still let an RPC code or body phrase mask a locked HTTP status | Classify the isolated, transport-origin status; preserve per-interface status meanings and gRPC precedence | 2.4, 5.3 |
| Replacing one failed parent can create an unapproved mixed policy | Resolve complete graphs; fall back each affected chain as a whole while preserving unrelated valid chains | 2.5, 4.4, 5.4 |
| The stateful default needs protection before cache and policy use | Guard the original method across interfaces and accepted batches; preserve disabled-method rejection and bypass cache | 3.4, 5.4 |
| A slot-used result must prevent another send, not only change the final response | Record the flag before retry decisions, including the all-rate-limited exception and prefixed XRP replies | 3.3, 5.3 |
| A local row diff misses descendant, ordering and interface changes | Diff complete effective policies and require current-head owner approval independently of labels | 5.2, 5.3 |
| Zero mismatches and a switch setting do not establish a safe rollout or rollback | Require observed coverage and stable policy windows; restore the prior router after code-table deletion | 5.1, 5.5 |
| Scope departures and scratch probes could be mistaken for approval | Preserve the versioning deferral, state its required ticket approval, and separate design evidence from implementation gates | 1.3, 1.4, 2.4, 9 |
| Review of the revision-4 draft, 1: the HTTP carve-out admitted any unlocked status (408, 402/407/426/451 included) | Chain rows read text inside 400, 404, 410, 422 and 500 on REST and 400 and 500 elsewhere, following the split the router's own table already makes; every other status with a router rule joins the lock; the router's four route-refusal text rows run ahead of the status decision; inside the four 4xx statuses a row can't blame the endpoint; the 404 route-refusal phrases are refused | 1.3, 2.4, 3.2, 5.3 |
| Review of the revision-4 draft, 2: the fallback had logs and counters but no alert or acceptance test | Two state gauges with a closed reason enum (coverage states included), three warning-level alert rules in the infra house style, readiness left alone, promtool-proven before publication; the canary requires the gauges at 0 | 1.2, 4.4, 5.4, 5.5, 5.6 |
| Review of the revision-4 draft: stale approval wording | The versioning deferral is the author's decision; MAG-4910 is updated to match instead of waiting on an approver to accept it | Header, 1.3, 1.4, 9 |

The last three rows were fixed in the revision-4 draft before publication; nothing in them is open.

**Validation of the revision-4 draft fixes.** A second scratch replay at `6c237073`, with the
widened HTTP lock and the status-only decision of 2.4: all 102 fixtures keep their verdict; all 72
keep-set counterexamples keep theirs; the 9 change-set cases take the status verdict as 2.4 lists;
and of 18 HTTP lock cases, the 10 locked ones (REST 402, 408, 451, 501 and 405 with family text,
and a JSON-RPC `HTTP 408:` prefix) take the status's answer while the 8 readable ones (REST 400,
500, 410, 422, 404 and the unruled 418) keep today's. Treating a REST code-0 `HTTP <n>: ` message
as carrying that status changed nothing. No fixture or counterexample carries an HTTP 404, 410 or
422 prefix off REST, so the narrower carve-out there moves nothing in the replay either. Two
JSON-RPC fixtures (`evm_http_429`, `evm_http_503`) carry the wording `HTTP status <n>:`, which the
router doesn't produce (its prefix is `HTTP <n>: `, `direct_rpc_relay.go:718`,
`node_error_handler.go:80`); the anchored prefix doesn't read it, and they stay with the transport
rows as today. Not replayed: the route-refusal rows inside the prefix, which the existing
`TestRESTMessageRowsBeatTheStatusRow` pins instead. As before, this is evidence for the ordering,
not validation of an implementation.

**Revision-4 validation.** An isolated ordering model using the matchers at smart-router `6c237073`
preserved all 102 classifier fixtures and passed 14 targeted cases: 12 protected-decision cases
and the two unrelated-number counterexamples for Bitcoin and Solana. It included the HTTP/RPC-code
collision, the REST versus JSON-RPC 401/409 distinctions, gRPC precedence and already-known
protection. This validates the proposed ordering against those inputs; it does not test transport
metadata provenance, the unimplemented loader, signatures, activation permissions or the write
state machine. Those remain acceptance gates in section 5.

### C.5 Changes in revision 5

| Decision | Change | Where |
|---|---|---|
| Author's decision (2026-11-15): a chain's spec defines the verdicts its rows point at, per chain, not in a shared file | A `verdicts` list beside `rows` with name, code, sub-category, retryable and class; a verdict has one origin spec and chains that inherit it override its flags by name, never its code; names and codes unique across lava-specs, with a `verdict_collision` fallback at load; the router keeps the flag vocabulary, what each flag does, the transport-failure flag `may_have_reached_node`, and the router-owned verdicts (every transport-row verdict, the 25 its logic references by identity, protocol, connection, status and unknown); no spec verdict may carry `rate_limit` or `unsupported_method`, be internal, or pair `data_scope`/`node_capability` with `retryable: false`; the 41 family-only verdicts move in Step 1 with codes and flags unchanged and must match the router's copy field for field; R&D reviews every verdict change through a required check. Listed as a departure from the ticket's "the router keeps what each class does" | 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 2.3, 2.5, 2.6, 2.7, 3.1, 3.2, 3.4, 4.1, 4.4, 5.1, 5.2, 5.3, 5.4, 5.6, 6, 9, A.1 |

**Revision-5 evidence.** The split of today's verdicts was computed at `6c237073` with `git grep`
over `protocol/**/*.go`, excluding the registry and classifier files and tests, and then checked
by hand: 25 registered verdicts are referenced by identity or code in router logic (the raw
identifier grep returns 29; four hits are a Tendermint reply constant and method-name fragments);
the family tables use 58, of which 17 are also transport-row or logic verdicts and 41 are
family-only. The vocabulary, flags and code ranges in 2.3 and A.1 are read from
`error_registry.go` and `error_codes.go`; every example verdict's code and flags in 2.6 were
checked against `error_codes.go`. A review of the draft found that a node's error body reaches
the client unchanged (`failed_relay_reply.go:35-38`), so a verdict's name and code are an
operator's view, not a client's; that `may_have_reached_node` is read only on transport failures;
and that the router's tests require `data_scope` and `node_capability` verdicts to be retryable.
All three are in the text above. Nothing about loading verdicts from a spec has been prototyped;
the relay path's use of the flags is today's code, read through the registry.

## References

- Smart Router error rulebook v1.0, R&D review (Notion `40cf5ce8bcd38108bcf3fd93f85e8af5`):
  - R-M: rows keyed by chain, node software and interface; no vendor layer;
  - R-L: writes;
  - R-K: all-fail ranking;
  - R-0.2: no vendor wording;
  - Q45: slot already used;
  - and the rulebook's own change log (the 4 Nov Q45 change).
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
