#!/bin/bash
# Smart Router — Solana DIRECT RPC test setup.
#
# Purpose: reproduce / verify the Solana consistency bug fixed in PR #21
# (fix: solana slot unit mismatch). On Solana, `context.slot` and
# `value.lastValidBlockHeight` diverge by the skip rate (~21-22M on mainnet).
# The old SVMChainTracker stored lastValidBlockHeight into `seenBlock` while
# provider spec parsing (GET_BLOCKNUM -> context.slot) used the slot, so the
# consistency engine compared two different units and produced
#   "Consistency Error code 3368 ... Requested a block that is too new".
# The fix tracks context.slot on both sides. With a fixed binary, the requests
# below should succeed with NO 3368 / "block too new" / "No pairings" errors.
__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "$__dir"/../useful_commands.sh
# Optional vars file (not present in all checkouts) — source only if it exists.
[ -f "${__dir}/../vars/variables.sh" ] && . "${__dir}/../vars/variables.sh"

# Use absolute paths for logs
LOGS_DIR=${__dir}/../../debugging/logs
mkdir -p $LOGS_DIR
LOGS_DIR=$(cd "$LOGS_DIR" && pwd)
rm $LOGS_DIR/*.log 2>/dev/null || true

# Save project root for later use
PROJECT_ROOT=$(cd ${__dir}/../.. && pwd)
CONFIG_FILE="$PROJECT_ROOT/config/smartrouter_examples/smartrouter_sol.yml"

# Resolve the smartrouter binary by absolute path so the screen session does not
# depend on the user's shell profile putting $(go env GOPATH)/bin on PATH.
SMARTROUTER_BIN="$(go env GOPATH)/bin/smartrouter"

# Only remove config when explicitly regenerating (keeps manual edits for e.g. timeout testing)
if [[ "$REGENERATE_CONFIG" == "1" ]]; then
    echo "REGENERATE_CONFIG=1: removing existing smart router config..."
    rm -f "$CONFIG_FILE" 2>/dev/null || true
fi

# Kill any running smartrouter processes
killall smartrouter 2>/dev/null || true
sleep 1

# Kill all screen sessions
killall screen 2>/dev/null || true
sleep 1
screen -wipe
sleep 1  # Give processes time to fully shut down before starting new ones

echo "============================================"
echo "Smart Router Solana Direct RPC Test Setup"
echo "============================================"
echo "Chain: SOLANA (jsonrpc)"
echo "Mode: DIRECT RPC (no providers!)"
echo "Goal: verify PR #21 slot/blockHeight consistency fix"
echo "============================================"
echo ""

echo "[Test Setup] installing smartrouter binary"
make -C "$PROJECT_ROOT" install

# Start cache service (the consistency / shared-state path used by the bug
# flows through the cache, so we run it to mirror production).
echo "[Test Setup] starting smart router cache service"
screen -d -m -S cache bash -c "source ~/.bashrc; \"$SMARTROUTER_BIN\" cache \
127.0.0.1:20100 --metrics_address 0.0.0.0:20200 --log_level debug 2>&1 | tee $LOGS_DIR/CACHE.log" && sleep 0.25

sleep 2

echo "Verifying cache service..."
if screen -list | grep -q "cache"; then
    echo "  Cache screen session: RUNNING"
    sleep 1
    if nc -z 127.0.0.1 20100 2>/dev/null; then
        echo "  Cache port 20100: LISTENING"
    else
        echo "  WARNING: Cache port 20100 not yet listening (may still be starting)"
    fi
else
    echo "  ERROR: Cache screen failed to start! Check $LOGS_DIR/CACHE.log"
fi
echo ""

# Static Solana spec (SOLANA index). Copied from lava specs/mainnet-1/specs/solana.json.
SPECS_DIR="$PROJECT_ROOT/specs/solana.json"
echo "Using static specs: $SPECS_DIR"
if [ ! -f "$SPECS_DIR" ]; then
    echo "ERROR: Solana spec not found at $SPECS_DIR"
    exit 1
fi

# Solana HTTP RPC endpoints.
#   export SOL_RPC_URL_1="https://your-solana-rpc/..."
# At least one is required; 2-3 enable cross-validation / consistency testing.
export SOL_RPC_URL_1="${SOL_RPC_URL_1:-https://g.w.lavanet.xyz:443/gateway/solana/rpc-http/YOUR_LAVA_GATEWAY_KEY}"
export SOL_RPC_URL_2="${SOL_RPC_URL_2:-}"
export SOL_RPC_URL_3="${SOL_RPC_URL_3:-}"

if [[ -z "$SOL_RPC_URL_1" ]]; then
    echo "ERROR: SOL_RPC_URL_1 must be set to a Solana JSON-RPC endpoint."
    exit 1
fi

# Generate config only if missing or REGENERATE_CONFIG=1 (keeps manual edits otherwise)
if [[ -f "$CONFIG_FILE" && "$REGENERATE_CONFIG" != "1" ]]; then
    echo "Using existing config: $CONFIG_FILE"
    echo "  (Set REGENERATE_CONFIG=1 to regenerate from env vars)"
    echo ""
else
echo "Generating smart router config: $CONFIG_FILE"
echo "  HTTP Endpoint 1: ${SOL_RPC_URL_1:0:60}..."
[[ -n "$SOL_RPC_URL_2" ]] && echo "  HTTP Endpoint 2: ${SOL_RPC_URL_2:0:60}..."
[[ -n "$SOL_RPC_URL_3" ]] && echo "  HTTP Endpoint 3: ${SOL_RPC_URL_3:0:60}..."
echo ""

cat > $CONFIG_FILE <<EOF
# Smart Router Direct RPC Configuration — Solana (SOLANA)
# Mode: Direct connections to Solana JSON-RPC endpoints (no Lava providers!)

endpoints:
  - listen-address: "0.0.0.0:3360"
    chain-id: "SOLANA"
    api-interface: "jsonrpc"
    network-address: "0.0.0.0:3360"

direct-rpc:
  # HTTP Endpoint 1
  - name: "sol-rpc-1"
    chain-id: "SOLANA"
    api-interface: "jsonrpc"
    node-urls:
      - url: "$SOL_RPC_URL_1"
        skip-verifications:
          - chain-id
          - pruning
EOF

if [[ -n "$SOL_RPC_URL_2" ]]; then
cat >> $CONFIG_FILE <<EOF

  # HTTP Endpoint 2
  - name: "sol-rpc-2"
    chain-id: "SOLANA"
    api-interface: "jsonrpc"
    node-urls:
      - url: "$SOL_RPC_URL_2"
        skip-verifications:
          - chain-id
          - pruning
EOF
fi

if [[ -n "$SOL_RPC_URL_3" ]]; then
cat >> $CONFIG_FILE <<EOF

  # HTTP Endpoint 3
  - name: "sol-rpc-3"
    chain-id: "SOLANA"
    api-interface: "jsonrpc"
    node-urls:
      - url: "$SOL_RPC_URL_3"
        skip-verifications:
          - chain-id
          - pruning
EOF
fi

echo ""
echo "Verifying generated config file..."
if [ -f "$CONFIG_FILE" ]; then
    echo "Smart router config exists: $CONFIG_FILE (size: $(wc -c < "$CONFIG_FILE") bytes)"
    echo ""
    echo "Config:"
    sed 's/^/  /' "$CONFIG_FILE"
else
    echo "ERROR: Smart router config NOT found: $CONFIG_FILE"
    exit 1
fi
echo ""
fi  # end: regenerate config or use existing

# Start Smart Router with DIRECT RPC (no providers!)
echo "[Test Setup] starting Smart Router (DIRECT RPC mode, standalone)"
echo "   - Chain: SOLANA   Listen: 0.0.0.0:3360   Cache: 127.0.0.1:20100"
echo ""

screen -d -m -S smartrouter bash -c "cd $PROJECT_ROOT && source ~/.bashrc; \"$SMARTROUTER_BIN\" \
config/smartrouter_examples/smartrouter_sol.yml \
--geolocation 1 \
--log-level debug \
--cache-be \"127.0.0.1:20100\" \
--use-static-spec $SPECS_DIR \
--skip-websocket-verification \
--metrics-listen-address ':7779' 2>&1 | tee $LOGS_DIR/SMARTROUTER.log" && sleep 0.25

sleep 3

echo "Verifying smart router screen session..."
if screen -list | grep -q "smartrouter"; then
    echo "Smart router screen is running"
else
    echo "ERROR: Smart router screen failed to start! Check $LOGS_DIR/SMARTROUTER.log"
    exit 1
fi
echo ""

echo "--- setting up screens done ---"
screen -ls

echo ""
echo "============================================"
echo "Smart Router Solana Setup Complete!"
echo "============================================"
echo "Cache:         127.0.0.1:20100 (metrics: 20200)"
echo "Smart Router:  0.0.0.0:3360 (metrics: 7779)"
echo ""
echo "Test Commands (HTTP / JSON-RPC):"
echo ""
echo "  # getSlot — should return the current slot (~425M range)"
echo "  curl -s -X POST http://127.0.0.1:3360 \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"jsonrpc\":\"2.0\",\"method\":\"getSlot\",\"params\":[],\"id\":1}'"
echo ""
echo "  # getLatestBlockhash — slot (context.slot) vs lastValidBlockHeight differ by ~21M"
echo "  curl -s -X POST http://127.0.0.1:3360 \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"jsonrpc\":\"2.0\",\"method\":\"getLatestBlockhash\",\"params\":[{\"commitment\":\"finalized\"}],\"id\":1}'"
echo ""
echo "  # getBlockHeight"
echo "  curl -s -X POST http://127.0.0.1:3360 \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"jsonrpc\":\"2.0\",\"method\":\"getBlockHeight\",\"params\":[],\"id\":1}'"
echo ""
echo "  # getAccountInfo (consistency-sensitive read)"
echo "  curl -s -X POST http://127.0.0.1:3360 \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"jsonrpc\":\"2.0\",\"method\":\"getAccountInfo\",\"params\":[\"11111111111111111111111111111111\",{\"encoding\":\"base64\"}],\"id\":1}'"
echo ""
echo "============================================"
echo "VERIFYING THE PR #21 CONSISTENCY FIX"
echo "============================================"
echo ""
echo "Hammer the router so the SVMChainTracker polls latest repeatedly and the"
echo "shared-state seen-block path is exercised (this is what tripped the bug):"
echo ""
echo "  for i in \$(seq 1 20); do"
echo "    curl -s -o /dev/null -w '%{http_code}\\n' -X POST http://127.0.0.1:3360 \\"
echo "      -H 'Content-Type: application/json' \\"
echo "      -d '{\"jsonrpc\":\"2.0\",\"method\":\"getSlot\",\"params\":[],\"id\":1}'"
echo "    sleep 0.5"
echo "  done"
echo ""
echo "PASS  (fix working): all requests return slot/height values, and the grep"
echo "      below finds NOTHING."
echo "FAIL  (bug present): responses carry 'No pairings available' and the log"
echo "      shows 3368 / 'block that is too new' with blockGap ~21-22M."
echo ""
echo "  # This MUST be empty on a fixed binary:"
echo "  grep -Ei 'consistency error|too new|No pairings|blockGap' $LOGS_DIR/SMARTROUTER.log"
echo ""
echo "  # Confirm the tracker reports a SLOT (~425M), not block height (~403M):"
echo "  grep -i 'SVMChainTracker' $LOGS_DIR/SMARTROUTER.log | tail"
echo ""
echo "Monitor logs:"
echo "  tail -f $LOGS_DIR/SMARTROUTER.log | grep -Ei 'slot|consistency|relay|pairings'"
echo ""
echo "To Stop All Services:"
echo "  killall smartrouter; screen -wipe"
echo "============================================"
