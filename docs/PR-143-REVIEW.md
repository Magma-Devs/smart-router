# PR #143 Review — ChainTracker & Probing redesign (MAG-2157)

> Review of [Magma-Devs/smart-router#143](https://github.com/Magma-Devs/smart-router/pull/143)
> (`chaintracker-redesign` → `main`). Medium-effort precision review: 8 finder
> angles → candidate findings → 1-vote verification → ranked findings below.

## What the PR does

Replaces the global single-tip ChainTracker + synthetic probe with a two-plane
architecture:

- **Data plane** — side-effect-free per-endpoint observations (latest block,
  poll-health, last-successful poll) feeding an in-tree `chainstate.ChainState`
  with a monotonic, outlier-guarded, strict-majority consensus tip.
- **Decision plane** — a proactive per-chain prober + the relay path read that
  state to drive QoS (relay:probe EWMA 4:1) and a single `endpoint.Enabled`
  state machine (relay disables fast at 50 consecutive failures; probe
  re-enables after K=3 distinct healthy polls).

Deletes the global tracker, the block estimator, and a dead gRPC service, and
folds in a large `-race` hardening pass across 7 packages.

**Spec conformance:** the F1–F7 claims and the recovery flow were checked against
the code and **all match** — K=3 distinct post-`disabledAt` polls, `disabledAt`
stamped at both disable sites, F2 `RestoreRecoveredProvider`, F4 per-interface
sync reference, F5 sync-omit, 4:1 EWMA, strict-majority tip, URL-keyed trackers
independent of `endpoint.Enabled`. The findings below are behavioral
consequences and cleanups, **not** spec violations.

## Root cause linking the top findings

`getLatestBlock()` (`protocol/rpcsmartrouter/rpcsmartrouter_server.go:781`) now
**deliberately returns `0`** when `ChainState.Initialized()` is true but the tip
has aged past TTL (`:793-794`, documented as "honest unknown"). That choice is
defensible in isolation, but **four downstream consumers mishandle the `0`**, and
a config-defaulting mismatch can make the `0`-window persistent on some chains.

The cheapest single mitigation is to decide whether these consumers should treat
`0` as "unknown → skip the heuristic" rather than each defaulting toward
archive / not-finalized.

## Findings (most severe first)

### 1. TTL-stale tip forces every concrete-block request to the archive pool
`protocol/chainlib/extensionslib/archive_parser_rule.go:33` — **CONFIRMED — FIXED**

`getLatestBlock()==0` flows unguarded into `ExtensionInfo{LatestBlock}`
(`rpcsmartrouter_server.go:3087/3091/3095`) → `isPassingRule`, where
`if latestBlock == 0 { return true }` forces the archive extension. During any
window where both polls and relay harvests stall past TTL, **all** concrete-block
requests route to the smaller/costlier archive pool — or fail outright if no
archive providers are configured. The pre-PR estimator/atomic ladder never
yielded `0` after first init.

**Fix applied.** Added `getLatestBlockBestEffort()` (`rpcsmartrouter_server.go`,
after `getLatestBlock`): returns the strict ChainState tip when fresh, otherwise
falls back to the last-known monotonic atomic tip (`latestBlockHeight`, seeded by
the init relay + tip-eligible relay harvest), yielding `0` only at genuine
cold-start. The three archive-routing returns in `getExtensionsFromDirectiveHeaders`
(3087/3091/3095) now use it, so a TTL-stale window no longer over-routes to
archive. Cache finalization (#4, `:2372`) intentionally keeps the **strict**
`getLatestBlock()` — a stale tip there can falsely finalize — so the two
consumers' opposite needs are now served by separate accessors. The conservative
force-archive-on-true-cold-start behavior (and its `archive_parser_rule_test.go`
test) is preserved. `go build ./...` clean; `extensionslib` and `rpcsmartrouter`
tests pass.

### 2. Integer underflow on the same `0` forces archive for nearly all `eth_call`
`protocol/chainlib/jsonRPC.go:183` — **CONFIRMED — FIXED**

`uint64(parsedBlock) < extensionInfo.LatestBlock-126` with `LatestBlock==0`
computes `uint64(0)-126`, underflowing to ~`2^64`, making the comparison true for
essentially every `eth_call` block. Same trigger window as #1, distinct site and
mechanism — there is no `==0` guard here at all. (Independent of #1, the same
underflow also fires for any `LatestBlock` in `[1,126)` — a young chain.)

**Fix applied.** Replaced the bare `126` with a named const `ethCallArchiveBlockDepth`
and guarded the subtraction: the heuristic now applies only when
`parsedBlock >= 0 && extensionInfo.LatestBlock > ethCallArchiveBlockDepth`. So an
unknown head (`0`) or a chain younger than the depth no longer underflows and
force-archives every `eth_call`, and negative block sentinels (latest/pending) are
excluded explicitly rather than relying on the `uint64(negative)`-is-huge
accident. `go build ./...` clean; `chainlib` tests pass.

### 3. `averageBlockTime` defaulted inconsistently — can make the `0`-window persistent
`protocol/rpcsmartrouter/rpcsmartrouter_server.go:172` & `:196` — **PLAUSIBLE — FIXED**

`chainstate.New(..., DefaultConfig(averageBlockTime))` receives the raw value and,
for `avg==0`, computes a **2s** StalenessWindow/TTL (`chain_state.go:124`,
`minStalenessWindow`), while `NewEndpointMonitor` independently floors `avg==0` to
**12s** → ~6s poll cadence (`endpoint_monitor.go:154-157`). On a spec omitting
`average_block_time`, under poll-dominated/low-traffic conditions, observations
arrive every ~6s but the 2s consensus window drops them →
`GetConsensusBaseline`/`GetLatestBlock` report not-fresh continuously → sync
scoring disabled **and** `getLatestBlock()==0` (feeding #1/#2).

(Live relay traffic also refreshes observations, so this bites mainly on quiet
chains / cold start.)

**Fix applied.** Promoted the monitor's inline `12 * time.Second` default to an
exported single-source-of-truth constant `endpointstate.DefaultAverageBlockTime`,
and floored the spec block time **once** at the server
(`rpcsmartrouter_server.go`, new `effectiveBlockTime`) before feeding the **same**
value to both `chainstate.DefaultConfig` and `NewEndpointMonitor`. With `avg==0`
both now see `12s`, so the consensus window (`max(10×12s, 2s)=120s`) comfortably
exceeds the `~6s` poll cadence and observations stay fresh. `consistencyConfig`
deliberately keeps the **raw** `averageBlockTime` (flooring its input would change
its clamped `MaxWaitTime` — unrelated behavior). `go build ./...` clean;
`endpointstate`, `chainstate`, and `rpcsmartrouter` tests pass.

### 4. Same `0` degrades cache finalization
`protocol/rpcsmartrouter/rpcsmartrouter_server.go:2372` (def `:2268`) — **CONFIRMED — BY DESIGN (won't-fix), comment hardened**

`isFinalizedForCacheWrite` picks `max(replyLatestBlock, trackedLatestBlock)`; with
`trackedLatestBlock==0`, for block-echoing methods (`eth_getBlockByNumber`)
"latest" collapses to the echoed block, so a genuinely finalized old block is
classified not-finalized and written to the short-TTL temp store instead of the
long-TTL finalized store. Cache-effectiveness loss during the stale window (not
data corruption).

**Resolution: no behavior change — strict-`0` is correct here, and that is now the
deliberate, documented stance.** Unlike archive routing (#1), finalization must
**not** fall back to the best-effort atomic:

- **Direction of error is safe.** A missing/`0` tip makes the method under-finalize
  (finalized data lands in the short-TTL store) — never the reverse. Falsely
  finalizing (writing a not-yet-final block to the long-TTL store) would be a real
  correctness bug; under-finalizing is only a cache-hit-rate cost.
- **The atomic is strictly worse than `replyLatestBlock` as a finalization base.**
  The bootstrap atomic is monotonic-max with no downward correction, so one
  lying-high observation would false-finalize **every** provider's responses
  **persistently** until the real head catches up — global and durable. A bad
  `replyLatestBlock` only mis-finalizes that one provider's own reply, once. The
  consensus tip is safe (strict-majority, realigns downward, `chain_state.go:330`)
  but `getLatestBlock()` already returns it when fresh.
- **No safe stale fallback exists.** A consensus-guarded stale floor would be the
  one defensible source, but `Recompute` zeroes `cs.baseline` on any
  sub-majority/empty snapshot (`chain_state.go:318`), so it is wiped within one
  tick of a stale window. Retaining it would be a consensus-engine change with its
  own reorg-outliving risk — unwarranted for a cache-effectiveness loss.
- **The persistent trigger is already gone** via the #3 fix; the residual is
  transient under-finalization in genuinely degraded / cold-start windows, where
  declining to finalize under uncertainty is the *correct* behavior.

**Done:** hardened the `isFinalizedForCacheWrite` doc comment to state that
finalization must use the strict `getLatestBlock()` and must NOT adopt #1's
best-effort atomic, with the monotonic-max reason — so a future reader doesn't
"fix" this into a correctness regression. `go build ./...` clean.

> **Separate latent finding — VERIFIED CONFIRMED (mechanism), but PRE-EXISTING and
> out of PR #143's scope.** A single malicious/buggy provider can poison the
> long-TTL finalized cache via `Reply.LatestBlock`.
>
> - **Ungated provider self-report.** `tryCacheWrite` is called per successful
>   provider response inside `sendRelayToDirectEndpoints` (`:1488`), **before** the
>   tip-harvest (`:1506`) and site-B fallback (`:1530`) — so the finalization base
>   `latestBlock := relayResult.Reply.LatestBlock` (`:2410`) is the raw, provider-
>   controlled value, with no consensus gate.
> - **`max()` lets the lie win.** `isFinalizedForCacheWrite` uses
>   `latest = max(replyLatestBlock, trackedLatestBlock)`. A lying-high
>   `replyLatestBlock` overrides even a fresh, accurate consensus tip — so the
>   consensus tip cannot cap it. This is the structural root.
> - **Cross-validation does NOT protect it.** CV hashes `reply.data` (the body),
>   not the `LatestBlock` metadata field, is opt-in, and each provider goroutine
>   caches its own response — so quorum agreement on the data never validates the
>   `LatestBlock` used for finalization.
> - **Impact.** A provider returns a valid response for a concrete, near-head
>   (reorg-able) block N but reports `LatestBlock = N + finalizationDistance + k`.
>   `IsFinalizedBlock(N, …)` → true → the response is written to the long-TTL
>   *finalized* store. If N then reorgs, the shared cache serves the stale
>   pre-reorg response to **all** callers of that key for the long TTL. Constraints:
>   cache active, cacheable concrete-block 2xx method, the lying provider selected.
> - **Scope.** Introduced in the initial smart-router implementation
>   (`isFinalizedForCacheWrite`, commit `22cbf32`), NOT by PR #143 — this PR only
>   changed the `trackedLatestBlock` *source* (`getLatestBlock`). Real latent
>   cache-poisoning vector, but a fix (e.g. treat a fresh consensus tip as the
>   authority / cap `replyLatestBlock` at `consensusTip + BucketWidth` instead of
>   `max`) is a behavior change outside this PR's diff and should be its own item.

### 5. Endpoint flap: heavy-method relay failures disable, a cheap poll re-enables
`protocol/lavasession/consumer_types.go:349` (`RecordProbeVerdict`) — **PLAUSIBLE — FIXED (dampened)**

Application 5xx and timeouts on heavy methods (large `eth_getLogs`) increment
`ConnectionRefusals` toward the 50-disable threshold (`error_mapper.go:77-87`,
`classifyHTTPStatus` `code>=500`), but re-enable evidence (`probing/verdict.go`)
is **poll-only** — the cheap `GET_BLOCKNUM` poll. A successful relay resets the
counter via `ResetHealth`, but a successful poll never touches
`ConnectionRefusals`, so a node that 5xx's heavy traffic while answering cheap
polls can loop disable→re-enable.

**Narrow trigger:** needs ~50 consecutive heavy failures to that endpoint with no
interleaved relay success (429 does **not** count — it's backoff-only).

**Fix applied (dampening, not elimination).** Extended the existing re-enable
hysteresis with a capped, decaying escalation. Two race-safe `Endpoint` fields
(under `e.mu`): `probeReenabled` (the current Enabled state was granted by the
probe and not yet validated by a successful relay) and `reenableProbeFlaps`. A
disable that follows an unvalidated probe grant is a flap and escalates the
effective K (`reEnableAfterK << reenableProbeFlaps`: `3 → 6 → 12`), capped at
`maxReenableProbeFlaps = 2`; a successful relay (`ResetHealth`) decays it to `0`.
The cap is a deliberate **product-policy** choice: at a ~5s probe cadence the
escalated re-enable stays ≤ ~60s, far below the ~15-minute epoch re-probe, so a
node that is genuinely healthy for cheap traffic is never parked — the flap
frequency is reduced, not the endpoint permanently disabled. New tests
(`endpoint_probe_reenable_test.go`) drive the full flap→escalate→cap→decay cycle
and the genuine-recovery (no-escalation) case; full `lavasession` suite green
under `go test -race`.

*Policy knobs to confirm:* the `3 → 6 → 12` escalation and the `2`-flap cap are
defaults chosen to keep the worst case well under the epoch; adjust if a
different flap/availability trade-off is wanted.

### 6. 2-endpoint pods with a chronically-lagging peer permanently lose relay sync scoring
`protocol/chainstate/chain_state.go:397` (`computeMajorityBaseline`) — **CONFIRMED — BY DESIGN (won't-fix)**

The consensus getter is installed unconditionally
(`rpcsmartrouter_server.go:181`), so `ConsensusConfigured` is always true and
there is no fallback to the legacy max-across-providers reference. A 2-endpoint
pod whose endpoints persistently differ by more than `BucketWidth` (2) can never
form a majority (`bestCount>=2 && bestCount*2>total` fails), so every relay omits
the sync update indefinitely — the lagging provider is never demoted on sync.
(1-endpoint pods also omit, but there is nothing to demote against, so it is inert.)

**Resolution: no behavior change — F5's omit is correct, and *more* defensible at
N=2, not less.** The analysis that decides it:

- **The tempting fix is an attack vector.** With exactly 2 endpoints a strict
  majority requires both to agree; if they disagree there is no way to tell the
  lagger from the liar. Any non-majority reference (max-across-providers, or
  "use the higher block") makes the **higher reporter the reference and exempts
  it**, demoting only its single honest competitor on sync. A provider that lies
  high would thus get its honest peer demoted *and* elevate itself — a targeted
  demotion of an honest peer. F5's omit defends exactly this; reintroducing a
  small-pod reference hands an attacker the attack.
- **Rejected fix (named so it isn't re-proposed):** "only install the consensus
  getter for ≥3 endpoints, let 2-pods use the legacy max reference." This is
  backwards — it routes the *smallest, most-exposed* pods (where one liar is 50%
  of the signal) onto the poisonable reference.
- **The harm is bounded anyway.** A genuinely-lagging endpoint is already filtered
  independent of the optimizer's sync dimension: `ValidateEndpointCapability` →
  `IsEndpointTooFarBehind` (`consistency_validation.go:100`) is wired into
  pre-request selection (`rpcsmartrouter_server.go:1252`, `:1706`) and drops an
  endpoint whose lag (`seenBlock - endpointLatestBlock`) exceeds
  `EndpointLagThreshold`; its relay failures also feed availability/latency QoS.
  So sync-omit's practical cost on an N=2 chronic lagger is small.

> **Separate question (NOT a #6 fix — opposes its goal).** `BucketWidth` is a flat
> constant (`DefaultBucketWidth`) while `StalenessWindow`/`OutlierThreshold` scale
> with block time. On a fast chain (e.g. Solana ~0.4s) two *honest* endpoints can
> jitter > 2 blocks apart from propagation timing alone, so consensus may fail to
> form even for an honest small pod. Should `BucketWidth` scale with block time?
> Note this *widens* lag tolerance — the opposite of what #6 would want — so it is
> a distinct trade-off, possibly its own ticket, not a fix here.

### 7. [cleanup/efficiency] Two parser-RWLock acquisitions per successful relay in the hot path
`protocol/rpcsmartrouter/rpcsmartrouter_server.go:2109` (`tipBlockFromRelay`) — **CONFIRMED — FIXED**

`isGetBlockNumMethod` + `isGetBlockByNumMethod` each call `GetParsingByTag`, which
takes `bcp.rwLock.RLock()` (`base_chain_parser.go:299`) — the same lock the relay
path contends on — even for non-tip methods (non-Solana). The tag→ApiName mapping
is immutable after parser construction; resolve both ApiNames once at startup and
compare `chainMessage.GetApi().Name` to avoid the per-relay lock contention.

**Fix applied.** Added cached `getBlockNumApiName` / `getBlockByNumApiName` fields,
resolved exactly once via a `sync.Once` (`resolveTipApiNames`) on the first relay,
and removed `isMethodTagged`. The two helpers now do a lock-free string compare
against the cached names. Resolution is lazy (not in `ServeRPCRequests`) so it also
covers servers built directly in tests; `sync.Once` gives the field reads a
happens-after on the write (race-clean). After the first relay, the per-relay
parser RWLock acquisitions are gone. `go build`/`vet` clean; `rpcsmartrouter`
suite green (incl. `-race` on the harvest/tip tests).

### 8. [cleanup/reuse] `ObservationSource` enum duplicates `endpointtip.Source` verbatim
`protocol/endpointstate/observation.go:10` — **CONFIRMED — FIXED**

Same iota order, identical `String()` switch, plus an `observationSourceFromTip`
converter (`:301`); `endpointtip/store.go:28` even comments that it "mirrors
endpointstate's ObservationSource." Since `endpointstate` already imports
`endpointtip`, a type alias (`type ObservationSource = endpointtip.Source`) would
delete the duplicate enum, its `String()`, and the converter — removing a
lockstep-maintenance hazard (a future third source must be edited in two packages
or it silently mis-maps).

**Fix applied.** Made `ObservationSource` a type alias for `endpointtip.Source` and
the three `ObservationSource*` constants aliases of the `endpointtip.Source*`
values, then deleted the duplicate `String()` and the `observationSourceFromTip`
converter (the two call sites now assign `tip.Source` directly — it is the same
type). All existing call sites/tests across `endpointstate` and `rpcsmartrouter`
compile unchanged; there is now one definition, so a future source can't drift.
`go build`/`vet` clean; `endpointstate` suite green under `-race`.

## Also noted (below the top-8 cut)

- **Cold-start cache-warm bootstrap window** can also produce
  `getLatestBlock()==0` (cache hits no longer seed the atomic — intentional
  anti-stale-poisoning; narrow window). Same consumer-mishandling as #1/#2/#4.
- **`recomputeChainStateConsensus`** (`:1801`) builds a full throwaway
  observation map (full struct copies + per-URL tip-store RLock) every tick, then
  re-iterates keeping only `{URL, Block, ObservedAt}`. Lower impact than #7 since
  it is per-tick, not per-relay.
- **Altitude:** `providerSyncFloor` side-table (`provider_optimizer.go:119`)
  patches an async ristretto cache rather than using a synchronous authoritative
  store; the `atomic.Pointer[string]` listening-address fix is copy-pasted into 4
  listener types (`grpc.go`/`jsonRPC.go`/`rest.go`/`tendermintRPC.go`).

## Method

8 finder angles (line-by-line, removed-behavior, cross-file tracer,
reuse/simplify/efficiency, altitude, CLAUDE.md conventions, and a spec-vs-code
oracle for F1–F7) surfaced candidates; each surviving candidate was checked by an
independent verifier returning CONFIRMED / PLAUSIBLE / REFUTED with quoted lines.
No CLAUDE.md governs the changed Go code, so no convention findings. The
spec-oracle found zero F1–F7 mismatches.