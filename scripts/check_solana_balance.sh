#!/bin/bash
# Check the SOL balance of one or more Solana addresses.
#
# Queries the getBalance JSON-RPC method (result.value is lamports;
# 1 SOL = 1_000_000_000 lamports). Prefers the `solana` CLI if installed,
# otherwise falls back to a direct curl RPC call (no dependencies).
#
# Usage:
#   ./check_solana_balance.sh <ADDRESS> [<ADDRESS> ...]                 # via Smart Router
#   RPC_URL=https://api.testnet.solana.com ./check_solana_balance.sh <ADDRESS>  # direct testnet
#   RPC_URL=https://api.devnet.solana.com ./check_solana_balance.sh <ADDRESS>   # direct devnet
#
# Default target is the local Smart Router (so reads are exercised through it).
set -euo pipefail

RPC_URL="${RPC_URL:-http://127.0.0.1:3360}"

if [[ "$#" -lt 1 ]]; then
    echo "Usage: $0 <SOLANA_ADDRESS> [<SOLANA_ADDRESS> ...]" >&2
    echo "  (optionally set RPC_URL=... ; default: $RPC_URL)" >&2
    exit 1
fi

echo "============================================"
echo "Solana balance check"
echo "RPC: $RPC_URL"
echo "============================================"
echo ""

# Basic base58 sanity check (Solana addresses are 32-44 base58 chars, no 0OIl).
is_valid_address() {
    [[ "$1" =~ ^[1-9A-HJ-NP-Za-km-z]{32,44}$ ]]
}

# lamports -> SOL with 9 decimals, without floating-point drift (awk).
lamports_to_sol() {
    awk "BEGIN{printf \"%.9f\", $1 / 1000000000}"
}

check_one() {
    local addr="$1"
    echo "Address: $addr"

    if ! is_valid_address "$addr"; then
        echo "  SKIPPED: not a valid base58 Solana address."
        echo ""
        return
    fi

    if command -v solana >/dev/null 2>&1; then
        # CLI prints e.g. "1.5 SOL"; --output prints lamports when piped, so keep human form.
        local out
        if out=$(solana balance "$addr" --url "$RPC_URL" 2>&1); then
            echo "  Balance: $out"
        else
            echo "  ERROR querying balance: $out"
        fi
        echo ""
        return
    fi

    if ! command -v curl >/dev/null 2>&1; then
        echo "  ERROR: need either 'solana' or 'curl' on PATH." >&2
        echo ""
        return
    fi

    # Don't let a connection failure (curl exit != 0) abort the whole run under set -e.
    local resp rc=0
    resp=$(curl -s --max-time 20 "$RPC_URL" \
        -X POST -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBalance\",\"params\":[\"$addr\"]}") || rc=$?

    if [[ "$rc" -ne 0 || -z "$resp" ]]; then
        echo "  ERROR: could not reach RPC at $RPC_URL (curl exit $rc)."
        echo "         Is the Smart Router running? Start it: scripts/pre_setups/init_smartrouter_sol.sh"
        echo "         Or query a cluster directly: RPC_URL=https://api.testnet.solana.com $0 $addr"
        echo ""
        return
    fi

    # Echo the raw JSON-RPC response so the caller can see exactly what the endpoint returned.
    echo "  RPC response: $resp"

    if [[ "$resp" == *'"error"'* ]]; then
        echo "  ERROR: $resp"
        echo ""
        return
    fi

    # result.value holds the lamport balance.
    local lamports
    lamports=$(printf '%s' "$resp" | sed -n 's/.*"value":\([0-9]*\).*/\1/p')

    if [[ -z "$lamports" ]]; then
        echo "  Unexpected response: $resp"
        echo ""
        return
    fi

    echo "  Balance: $(lamports_to_sol "$lamports") SOL ($lamports lamports)"
    if [[ "$lamports" == "0" ]]; then
        echo "  Note: 0 balance — account is empty or has never been funded on this cluster."
    fi
    echo ""
}

for addr in "$@"; do
    check_one "$addr"
done

echo "============================================"
echo "Done."
echo "============================================"