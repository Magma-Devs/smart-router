#!/usr/bin/env bash
#
# scripts/check_gated_symbols.sh
#
# Sprint 3.8 — CI guard for the §3.3.6 gated symbols.
#
# Every constructor / discriminator listed below is the kind of thing whose
# misuse silently bypasses the community/enterprise gating system. Once Sprint 3
# wires `ActiveConfig().Validate*()` / `Create*()` into the runtime, any NEW
# call site that reaches for one of these symbols directly is almost certainly
# a regression — a new code path that doesn't ask the active edition for
# permission.
#
# This script enumerates every such symbol and asserts each occurrence lives
# in an allowlisted file (the definition, the enterprise factory delegation,
# or the canonical gated call site). _test.go files are always allowed
# because test fixtures legitimately reach for these symbols.
#
# To extend: add a new line to CHECKS in the form
#   "regex|allow1|allow2|..."
# Use forward-slash file paths from repo root.

set -euo pipefail

violations=0

declare -a CHECKS=(
    "NewDirectWSSubscriptionManager|protocol/rpcsmartrouter/direct_ws_subscription_manager.go|protocol/rpcsmartrouter/enterprise_config.go"
    "NewDirectGRPCSubscriptionManager|protocol/rpcsmartrouter/direct_grpc_subscription_manager.go|protocol/rpcsmartrouter/enterprise_config.go"
    'lavasession\.NewDirectRPCConnection|protocol/lavasession/direct_rpc_connection.go|protocol/rpcsmartrouter/rpcsmartrouter.go'
    'chainlib\.NewChainParser|protocol/chainlib/chainlib.go|protocol/rpcsmartrouter/rpcsmartrouter.go|protocol/rpcsmartrouter/testing.go'
    'RegisterEnterpriseConfig|protocol/rpcsmartrouter/config.go|protocol/rpcsmartrouter/enterprise_features.go'
    'strings\.HasPrefix\([^,]+, *"ws[s]?://"|protocol/rpcsmartrouter/rpcsmartrouter.go'
)

check_symbol() {
    local entry="$1"
    local pattern="${entry%%|*}"
    local allowlist="${entry#*|}"

    # `grep -E` treats `|` as alternation natively — no escaping. Escape `.` so
    # file extensions like `.go` aren't interpreted as "any char + go".
    local allow_regex
    allow_regex=$(echo "$allowlist" | sed 's/\./\\./g')

    local hits
    hits=$(git grep -nE "$pattern" -- '*.go' || true)
    [[ -z "$hits" ]] && return 0

    # Always allow test files; otherwise require an allowlist match.
    local bad
    bad=$(echo "$hits" | grep -v '_test\.go:' | grep -vE "^($allow_regex):" || true)

    if [[ -n "$bad" ]]; then
        echo "" >&2
        echo "GATED SYMBOL VIOLATION: $pattern" >&2
        echo "$bad" >&2
        violations=$((violations + 1))
    fi
}

for entry in "${CHECKS[@]}"; do
    check_symbol "$entry"
done

if [[ $violations -gt 0 ]]; then
    echo "" >&2
    echo "Found $violations gated-symbol violation(s)." >&2
    echo "Each must go through rpcsmartrouter.ActiveConfig().Validate*() or .Create*()" >&2
    echo "instead of reaching for the constructor directly." >&2
    echo "See agent_docs/smart-router-repo-enterprise.md §3.3.6." >&2
    exit 1
fi

echo "Gated symbols check: OK (0 violations across ${#CHECKS[@]} symbols)"
