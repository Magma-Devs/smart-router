# Adding a New Enterprise Feature

Pattern for adding an edition-gated capability. Every gated capability flows through one interface (`SmartRouterConfig`); community rejects with a pinned error string, enterprise permits, runtime calls through `ActiveConfig().Method(...)`, and CI guards block any caller bypassing the dispatcher.

## When this applies

A new API interface, transport, spec category, or subscription mechanism — anything where community must reject and enterprise must accept. If both editions get it for free (metric, log line, middleware), this guide doesn't apply.

## Step 1 — Extend `SmartRouterConfig` (no build tag)

`protocol/rpcsmartrouter/config.go`. Pick:

- **Validate-style** — `ValidateXxx(input) error`. Default. Returns `nil` if community-allowed, pinned error otherwise.
- **Supports-style** — `SupportsXxx() bool`. When the caller inspects capability before reaching a gate.
- **Factory-style** — `CreateXxx(opts) (T, error)`. No-op stub in community, real impl in enterprise.

## Step 2 — Reject in `communityConfig` with a pinned message

Same file. The string is contract — contract tests assert exact substrings, the binary verification script checks for forbidden strings, operators read it in startup logs.

```
"<Capability> requires an enterprise license — see https://github.com/Magma-Devs/smart-router#enterprise"
```

For URL rejections, echo with `%q`: `"<Transport> transport (url=%q) requires an enterprise license"`.

## Step 3 — Permit in `enterpriseConfig` (`//go:build enterprise`)

`protocol/rpcsmartrouter/enterprise_config.go`.

- `Validate*`: return `nil`, with a typo-guard `default` arm (see `ValidateAPIInterface`).
- `Create*`: return the real `Direct*` impl as a **pure constructor** — never call `Start(ctx)` inside. Lifecycle is the caller's job.

## Step 4 — Gate at the call site

Two locations, both required:

1. **Centralized validator** — `validateSmartRouterConfigAgainstEdition` in `rpcsmartrouter.go`. Fires once at startup before pprof/cache/server bind. The user-facing fail-fast point.
2. **Inline runtime gate** — at the capability's actual use site. Defense-in-depth for non-startup paths.

```go
if err := ActiveConfig().ValidateXxx(input); err != nil {
    return utils.LavaFormatError("Xxx rejected by edition", err,
        utils.Attribute{Key: "input", Value: input})
}
```

For factories: replace the `NewDirect*` call with `ActiveConfig().CreateXxx(opts)` and start the result if it implements `Start(context.Context)`.

## Step 5 — Update CI guards if you added a new constructor

- `scripts/check_gated_symbols.sh` `CHECKS`: `regex|allowlist1|allowlist2|...` (definition site + enterprise factory delegation site; test files always allowlisted).
- `scripts/verify_binaries.sh`: add new symbols to `FORBIDDEN_SYMBOLS`, new strings to `FORBIDDEN_STRINGS`.

## Step 6 — Add tests (mandatory)

- Unit: `config_test.go` (community pinned-error assert) + `enterprise_config_test.go` (enterprise permit).
- Contract: `contract_test.go::TestSprint4ContractMatrix` — the single navigable spot reviewers walk to confirm gating. Every new gate must appear there.

## Step 7 — Verify before PR

```bash
make build-both test-both check-gates verify-binaries
```

All four must be green. CI runs the same set on every PR.

## Worked example — `CreateWSSubscriptionManager` (factory)

1. **Interface**: added to `config.go`. Factory pattern because result is a runtime object.
2. **Community**: returns `NoOpWSSubscriptionManager` (pre-existing stub).
3. **Enterprise**: returns `*DirectWSSubscriptionManager` as pure constructor; `Start(ctx)` at call site.
4. **Call site**: `rpcsmartrouter.go` swaps `NewDirectWSSubscriptionManager(...)` for `ActiveConfig().CreateWSSubscriptionManager(opts)`.
5. **Guards**: `enterprise_config.go` added to the existing `NewDirectWSSubscriptionManager` allowlist.

## Common mistakes

- **Custom error string** — defeats contract-test substring asserts. Use the pinned template.
- **`Start(ctx)` inside a factory** — couples lifecycle to construction; breaks tests. Pure constructors only.
- **Skipping the centralized validator** — inline-only gates fire after side effects have started.
- **Skipping `make verify-binaries`** — source guards miss build-tag misapplication, indirect dispatch, string concat.
- **Unit test without a contract entry** — both protect different regression classes.

## When something breaks

| Symptom | Cause |
|---|---|
| Community binary has an enterprise symbol | Build tag missing. Check `//go:build enterprise`. |
| `make check-gates` fails | New caller outside allowlist. Route via `ActiveConfig()` or update allowlist. |
| `make verify-binaries` fails on size | Build-tag invariant slipped. |
| Contract test passes, production wrong | Inline gate placement wrong. Check `validateSmartRouterConfigAgainstEdition`. |
| Enterprise license fails after refactor | Unit-test `resolveLicense` directly (pure logic, no subprocess). |

## Files to read first

`config.go`, `enterprise_config.go`, `check_gated_symbols.sh`, `verify_binaries.sh`.
