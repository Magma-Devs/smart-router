# Adding a New Enterprise Feature

This guide walks through the pattern for adding a new edition-gated capability
to the smart router. It captures the system as it sits at the end of Sprint 4
(Phase 2). Every step has a concrete file, a concrete line of code to add, and
a concrete test target.

The pattern has five steps. Skip none of them — each closes a different
regression class.

## The mental model in one sentence

Every gated capability flows through one interface (`SmartRouterConfig`); the
community implementation rejects with a pinned error string, the enterprise
implementation permits, the runtime calls through `ActiveConfig().Method(...)`,
and CI guards block any new caller that bypasses the dispatcher.

## When this guide applies

You are adding capability X, where X is one of:

- A new API interface (analogous to `rest`, `grpc`, `tendermintrpc`)
- A new transport (analogous to `ws://`, `grpcs://`)
- A new spec category (analogous to non-EVM chains)
- A new subscription mechanism (analogous to `DirectWSSubscriptionManager`)
- Anything else where community must reject and enterprise must accept

If the capability is something both editions get for free (e.g., a new metric,
a new log line, a new HTTP middleware), this guide does not apply. Just write
it.

## Step 1 — Extend the `SmartRouterConfig` interface

**File:** `protocol/rpcsmartrouter/config.go`

Add a method to the interface that captures the gate's question. Three patterns
to choose from:

- **Validate-style** — `ValidateXxx(input) error`. Returns `nil` if community
  is allowed to proceed; returns a pinned error otherwise. Example:
  `ValidateAPIInterface(apiInterface string) error`.
- **Supports-style** — `SupportsXxx() bool`. Returns whether the *edition*
  supports the capability, independent of any input. Example:
  `SupportsWSSubscriptions() bool`.
- **Factory-style** — `CreateXxx(opts) (T, error)`. Returns a no-op stub in
  community, the real implementation in enterprise. Example:
  `CreateGRPCSubscriptionManager(opts) (GRPCSubscriptionManager, error)`.

Default to `Validate-style` unless the capability requires constructing a
runtime object (factory) or needs to be inspected before reaching a gate
(supports-style).

**Always-compiled.** This file has no build tag. Both editions see the
interface definition.

## Step 2 — Implement restrictively in `communityConfig`

**Same file:** `protocol/rpcsmartrouter/config.go`, in the `communityConfig`
methods section.

The community implementation rejects with a *pinned error message*. The string
becomes part of the contract — the §3.4 contract test asserts exact substrings,
the binary verification script (Sprint 4.2) checks for forbidden strings, and
operators read the rejection in their YAML-startup logs.

**Pinned-message convention** (from §3.3.5):

```
"<Capability> requires an enterprise license — see https://github.com/Magma-Devs/smart-router#enterprise"
```

For URL-related rejections, echo the offending URL with `%q`:

```
"<TransportName> transport (url=%q) requires an enterprise license"
```

Example (REST rejection in `ValidateAPIInterface`):

```go
case spectypes.APIInterfaceRest:
    return fmt.Errorf("REST interface requires an enterprise license — see https://github.com/Magma-Devs/smart-router#enterprise")
```

**Always-compiled.** Community sees the rejection text; enterprise compiles it
in too but never executes it (the `enterpriseConfig` method short-circuits).

## Step 3 — Implement permissively in `enterpriseConfig`

**File:** `protocol/rpcsmartrouter/enterprise_config.go` (`//go:build enterprise`).

The enterprise implementation accepts. For `Validate*` methods, return `nil`
(or `nil` with a typo-guard default arm — see `ValidateAPIInterface` for the
pattern). For `Create*` factories, return the real `Direct*` implementation as
a *pure constructor* — do not call `Start(ctx)` inside. Lifecycle is the
caller's responsibility.

Example:

```go
func (enterpriseConfig) ValidateAPIInterface(apiInterface string) error {
    switch apiInterface {
    case spectypes.APIInterfaceJsonRPC,
         spectypes.APIInterfaceRest,
         spectypes.APIInterfaceGrpc,
         spectypes.APIInterfaceTendermintRPC:
        return nil
    default:
        // Even enterprise refuses unknown values — catches YAML typos.
        return fmt.Errorf("unsupported api-interface %q", apiInterface)
    }
}
```

**`//go:build enterprise` only.** Community never compiles this file. The
build-tag invariant from §3.2 is what keeps the enterprise symbol set out of
the community binary; verify with `make verify-binaries`.

## Step 4 — Call `ActiveConfig().Method(...)` at the runtime gate site

**Two locations:**

1. **Centralized validator** — `validateSmartRouterConfigAgainstEdition` in
   `protocol/rpcsmartrouter/rpcsmartrouter.go`. Add the new gate call here so
   it fires once at startup, before any side-effecting work (pprof, pyroscope,
   cache, server bind). This is the user-facing fail-fast point.

2. **Runtime call site (defense-in-depth)** — wherever the capability is
   actually used (e.g., right before a constructor that's gated). Inline gates
   exist at lines like §3.3.6 row #1 (line 441, before `chainlib.NewChainParser`)
   and row #4 (before `NewDirectWSSubscriptionManager`). The centralized pass
   should always catch a violation first; inline gates are the safety net for
   non-startup code paths.

Both call patterns:

```go
if err := ActiveConfig().ValidateXxx(input); err != nil {
    err = utils.LavaFormatError("Xxx rejected by edition", err,
        utils.Attribute{Key: "input", Value: input})
    errCh <- err   // or: return err — match local error-handling convention
    return err
}
```

For `Create*` factories the call replaces a `NewDirect*` call directly:

```go
mgr, err := ActiveConfig().CreateXxxSubscriptionManager(opts)
if err != nil { /* fatal */ }

if starter, ok := mgr.(interface{ Start(context.Context) }); ok {
    starter.Start(ctx)   // pure-constructor pattern: caller starts
}
```

## Step 5 — Update the CI guards if you introduced a new constructor

**Source-level guard:** `scripts/check_gated_symbols.sh`. If your new factory
returns a *new constructor symbol* that should only be reachable through
`ActiveConfig()`, add an entry to `CHECKS`:

```bash
"NewYourNewSubscriptionManager|protocol/rpcsmartrouter/your_new_manager.go|protocol/rpcsmartrouter/enterprise_config.go"
```

Format: `regex|allowlist1|allowlist2|...`. Test files (`*_test.go`) are always
allowlisted. Allowlist must include the constructor's definition site and the
enterprise factory delegation site.

**Post-build guard:** `scripts/verify_binaries.sh`. If your enterprise
implementation introduces a new function symbol or string literal that must
not appear in the community binary, add to:

- `FORBIDDEN_SYMBOLS=( ... )` — names that must be absent from the community
  binary's symbol table (e.g., `enterpriseConfig`, `EmbeddedLicense`).
- `FORBIDDEN_STRINGS=( ... )` — string literals that must be absent from the
  community binary's string table (e.g., `"Smart Router ENTERPRISE Edition"`).

Run both guards locally:

```bash
make check-gates       # source grep
make verify-binaries   # post-build inspection
```

## Step 6 (mandatory) — Add tests

You should write *both* a unit test and a contract-matrix entry.

**Unit test** in `protocol/rpcsmartrouter/config_test.go` (community gate):

```go
func TestCommunityConfig_ValidateXxx(t *testing.T) {
    err := communityConfig{}.ValidateXxx(rejectInput)
    require.Error(t, err)
    assert.Contains(t, err.Error(), "<your pinned substring>")
}
```

Plus the enterprise counterpart in
`protocol/rpcsmartrouter/enterprise_config_test.go` (`//go:build enterprise`).

**Contract-matrix entry** in
`protocol/rpcsmartrouter/contract_test.go::TestSprint4ContractMatrix`. Add a
subtest under either `Community must pass` or `Community must fail with pinned
error substrings`. The contract test is the single navigable location reviewers
walk to confirm §3.4 is enforced — every new gate must show up there.

For factories, also add to:
- `validate_config_test.go` — centralized validator coverage.
- `cmd/smartrouter/startup_*_test.go` — only if the new gate touches
  license/edition initialization (rare).

## Step 7 — Verify dual-build cleanliness

Before opening a PR, run:

```bash
make build-both      # both editions compile
make test-both       # both test suites pass
make check-gates     # 0 source-level violations
make verify-binaries # 0 binary-level violations
```

All four must be green. CI runs the same set on every PR (Sprint 5 owns the
workflow YAML; the Makefile targets above are what CI calls).

## Worked example: adding row #4 (WS subscription factory) in Sprint 3

Sprint 3 added `CreateWSSubscriptionManager` to `SmartRouterConfig`. The five
steps as they actually played out:

1. **Interface extension** — `config.go:35` added the method to the interface.
   Used the `Create*` factory pattern because the result is a runtime object.

2. **Community implementation** — `config.go:130` returned a
   `NoOpWSSubscriptionManager` (which existed before the gating system; reused
   as-is). No pinned error message because the factory accepts; the rejection
   for WS lives at the *transport* gate, not the factory.

3. **Enterprise implementation** — `enterprise_config.go:50` returned a
   `*DirectWSSubscriptionManager` as a pure constructor. `Start(ctx)` lives at
   the call site.

4. **Runtime call** — `rpcsmartrouter.go:843` replaced the existing
   `NewDirectWSSubscriptionManager(...)` call with
   `ActiveConfig().CreateWSSubscriptionManager(opts)`. The same `Start(ctx)`
   call below it stays, gated by an inline `interface{ Start(context.Context) }`
   type-assert.

5. **CI guards** — `scripts/check_gated_symbols.sh` already had
   `NewDirectWSSubscriptionManager` in its `CHECKS` list (added in Sprint 3.8);
   `enterprise_config.go` was added to its allowlist as the legitimate factory
   delegation site.

6. **Tests** — `config_test.go::TestCommunityConfig_FactoriesReturnNoops`,
   `enterprise_config_test.go::TestEnterpriseConfig_FactoriesReturnDirectImpls`,
   plus the contract matrix entry in `contract_test.go`.

7. **Verification** — both `make build-both` and `make verify-binaries`
   confirmed the noop stayed in community and the Direct* implementation only
   landed in enterprise.

## Common mistakes

- **Forgetting the pinned error message.** A custom error string defeats the
  contract test's substring assertion. Always reuse the §3.3.5 template format.

- **Calling `Start(ctx)` inside the factory.** Couples lifecycle to
  construction, makes tests harder, and forces every caller to provide a real
  context. Sprint 2 explicitly chose pure constructors. Don't break that.

- **Skipping the centralized validator.** Inline gates alone mean side-
  effecting startup work happens before rejection. The centralized pass is
  what makes "fail-fast on bad YAML" the user experience.

- **Forgetting `make verify-binaries`.** Source-level guards (`check-gates`)
  catch source-grep-detectable patterns. Binary-level checks catch the rest
  (e.g., a missed build tag that lets enterprise strings leak into community).
  Run both before pushing.

- **Adding a unit test but no contract-matrix entry.** Unit tests verify
  implementation correctness; the contract matrix verifies the §3.4 promise.
  Both protect against different regression classes. Skipping the contract
  entry is the most common review-feedback item.

## Where to look when something breaks

| Symptom | Most likely cause |
|---|---|
| Community binary has a new enterprise symbol | Build-tag missing on a new file. Check `//go:build enterprise` line. |
| `make check-gates` fails | New caller of a gated constructor outside its allowlist. Either route the call through `ActiveConfig()` or update the allowlist (with justification). |
| `make verify-binaries` fails on size sanity | Build-tag invariant slipped — community binary grew to match enterprise. |
| Contract test passes but production rejects/accepts wrong input | Inline gate placement is wrong — the gate fires too late (after a side effect) or wrong scope. Check `validateSmartRouterConfigAgainstEdition`. |
| Enterprise license validation fails after a refactor | `resolveLicense` in `cmd/smartrouter/startup_enterprise.go` is the pure-logic entry point — unit-test it directly without subprocess gymnastics. |

## Related docs

- `agent_docs/smart-router-repo-enterprise.md` — full implementation plan,
  authoritative for §3.x sections referenced above.
- `protocol/rpcsmartrouter/config.go` — interface definition + community
  implementation. Read this file end-to-end before adding a method.
- `protocol/rpcsmartrouter/enterprise_config.go` (`//go:build enterprise`) —
  enterprise implementation. Read alongside `config.go` to understand the
  community/enterprise pairing for each method.
- `scripts/check_gated_symbols.sh` and `scripts/verify_binaries.sh` — the two
  CI guards. Read both to understand what's caught at PR time vs build time.
