#!/usr/bin/env bash
#
# scripts/verify_binaries.sh
#
# Sprint 4.2 — post-build isolation guard. Builds both editions, then asserts
# the linked community binary excludes every enterprise-only symbol, every
# enterprise-only string literal, every Cosmos-SDK dependency, and is smaller
# than the enterprise binary.
#
# This is the post-link counterpart to scripts/check_gated_symbols.sh:
#
#   - check-gates       : source-level git grep — catches a forbidden CALL
#                         at PR review time, before the binary is built.
#   - verify-binaries   : post-build inspection of the linked artifact —
#                         catches a regression that source-grep missed
#                         (e.g., string concatenation, indirect dispatch,
#                         or a build-tag misapplication that lets enterprise
#                         code into the community binary anyway).
#
# Both checks are needed. Removing either weakens the contract.
#
# Forbidden lists are built from empirically-observed enterprise-only
# symbols/strings (see Sprint 3.9 nm baseline + Sprint 4.2 strings audit).
# Each list entry is documented with its source.
#
# Exit codes:
#   0  all checks pass
#   1  at least one violation (details printed to stderr)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$REPO_ROOT/build"
COMMUNITY_BIN="$BUILD_DIR/smartrouter"
ENTERPRISE_BIN="$BUILD_DIR/smartrouter-enterprise"

violations=0

# --- Build both editions (idempotent — uses Makefile targets) -----------------
echo "==> Building community + enterprise binaries"
(cd "$REPO_ROOT" && make -s build build-enterprise)

if [[ ! -x "$COMMUNITY_BIN" || ! -x "$ENTERPRISE_BIN" ]]; then
    echo "ERROR: expected $COMMUNITY_BIN and $ENTERPRISE_BIN to exist after build" >&2
    exit 1
fi

# --- 1. Symbol isolation: go tool nm on community ----------------------------
# Each entry must NOT appear in the community binary. Built from the
# Sprint 3.9 nm baseline that confirmed these are enterprise-only symbols.
declare -a FORBIDDEN_SYMBOLS=(
    'enterpriseConfig'             # Sprint 2 type — enterprise-only
    'EmbeddedLicense'              # licensing/embed.go (//go:build enterprise)
    'expiryWatcher'                # cmd/smartrouter/startup_enterprise.go
    'resolveLicense'               # cmd/smartrouter/startup_enterprise.go
    'validateAndActivateLicense'   # cmd/smartrouter/startup_enterprise.go
    'logExpiryWarning'             # cmd/smartrouter/startup_enterprise.go
    'watcherCadence'               # cmd/smartrouter/startup_enterprise.go
)

echo "==> Checking community binary symbols"
for sym in "${FORBIDDEN_SYMBOLS[@]}"; do
    if go tool nm "$COMMUNITY_BIN" 2>/dev/null | grep -q "$sym"; then
        echo "  VIOLATION: community binary contains symbol matching '$sym'" >&2
        go tool nm "$COMMUNITY_BIN" | grep "$sym" | head -3 >&2
        violations=$((violations + 1))
    fi
done

# --- 2. String isolation: strings on community -------------------------------
# Each entry must NOT appear in the community binary's string table. Built
# from the empirical strings audit in Sprint 4.2 — every quoted literal in
# cmd/smartrouter/startup_enterprise.go that is not a structural-attribute
# key (license_id/customer/expires/days_until_expiry are also used by
# unrelated logging in production code, so they're excluded from the list).
declare -a FORBIDDEN_STRINGS=(
    'Smart Router ENTERPRISE Edition'   # banner in startup_enterprise.go
    'LICENSE IN GRACE PERIOD'           # grace log
    'stops accepting new starts on'     # grace template
    'license expired on'                # fatal template
    'license validation failed'         # fatal template
    'license invalid'                   # fatal template
    'license approaching expiry'        # warning log
    'key_prod_2026_04'                  # production key_id — linker drops it in community via tree-shaking; verify
)

echo "==> Checking community binary strings"
for s in "${FORBIDDEN_STRINGS[@]}"; do
    if strings "$COMMUNITY_BIN" 2>/dev/null | grep -qF "$s"; then
        echo "  VIOLATION: community binary contains string '$s'" >&2
        violations=$((violations + 1))
    fi
done

# --- 3. Size sanity: community binary must be smaller than enterprise --------
echo "==> Checking binary size delta"
community_size=$(stat -f%z "$COMMUNITY_BIN" 2>/dev/null || stat -c%s "$COMMUNITY_BIN")
enterprise_size=$(stat -f%z "$ENTERPRISE_BIN" 2>/dev/null || stat -c%s "$ENTERPRISE_BIN")
if [[ "$community_size" -ge "$enterprise_size" ]]; then
    echo "  VIOLATION: community binary ($community_size) is not smaller than enterprise ($enterprise_size)" >&2
    echo "    A non-zero size delta is the linker's signal that the build-tagged" >&2
    echo "    code paths are actually excluded. Equal/larger size means a build-tag" >&2
    echo "    invariant has slipped." >&2
    violations=$((violations + 1))
else
    delta=$((enterprise_size - community_size))
    echo "  OK: community $community_size bytes, enterprise $enterprise_size bytes (delta +$delta)"
fi

# --- 4. Dependency graph: no Cosmos SDK leakage ------------------------------
# Phase 1 removed cosmossdk.io and github.com/cosmos/cosmos-sdk from the
# dependency closure. Any reintroduction is a regression — even if it's a
# transitive dep brought in by a new direct dep.
echo "==> Checking go mod graph for Cosmos SDK leakage"
forbidden_modules=$(cd "$REPO_ROOT" && go mod graph | grep -E '^(github\.com/cosmos/cosmos-sdk|cosmossdk\.io)' || true)
if [[ -n "$forbidden_modules" ]]; then
    echo "  VIOLATION: go mod graph contains Cosmos SDK modules" >&2
    echo "$forbidden_modules" | head -5 >&2
    violations=$((violations + 1))
else
    echo "  OK: no Cosmos SDK modules in dependency graph"
fi

# --- Summary -----------------------------------------------------------------
echo ""
if [[ $violations -gt 0 ]]; then
    echo "Binary verification: FAILED ($violations violation(s))" >&2
    exit 1
fi

echo "Binary verification: OK (symbols, strings, size, deps)"
