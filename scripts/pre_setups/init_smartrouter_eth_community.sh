#!/bin/bash
# Smart Router — Ethereum (Community Edition) test setup
#
# Builds the community binary (no `enterprise` build tag), starts a cache
# server and a smart router in screen sessions, generates a community-shaped
# config from env vars, and prints HTTP-only test commands.
#
# Differences from init_smartrouter_eth.sh (the enterprise variant):
#   - generates smartrouter_eth_community.yml — no WebSocket entries
#   - launches with --skip-websocket-verification (required because the ETH1
#     spec declares WS subscriptions; community can't satisfy them)
#   - omits the WebSocket test commands and Phase 5 framing
#
# Override the default RPC providers via env vars before running:
#   export ETH_RPC_URL_1="https://your-rpc-1"
#   export ETH_RPC_URL_2="https://your-rpc-2"
#   export ETH_RPC_URL_3="https://your-rpc-3"
#   export ETH_RPC_URL_4="https://your-backup"   # optional emergency-fallback tier

__dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "$__dir"/../useful_commands.sh
. "${__dir}"/../vars/variables.sh

# Use absolute paths for logs
LOGS_DIR=${__dir}/../../debugging/logs
mkdir -p $LOGS_DIR
LOGS_DIR=$(cd "$LOGS_DIR" && pwd)
rm $LOGS_DIR/*.log 2>/dev/null || true

# Save project root for later use
PROJECT_ROOT=$(cd ${__dir}/../.. && pwd)
CONFIG_FILE="$PROJECT_ROOT/config/smartrouter_examples/smartrouter_eth_community.yml"

# Only remove config when explicitly regenerating (keeps manual edits otherwise)
if [[ "$REGENERATE_CONFIG" == "1" ]]; then
    echo "REGENERATE_CONFIG=1: removing existing smart router config..."
    rm -f "$CONFIG_FILE" 2>/dev/null || true
fi

# Kill any running smart router processes (binary first, then screen wrappers).
# Order matters: killing the binaries first releases bound ports before the
# screen wrappers exit, avoiding orphaned listeners on retry-loops.
killall smartrouter 2>/dev/null || true
sleep 1
killall screen 2>/dev/null || true
sleep 1
screen -wipe
sleep 1

echo "============================================"
echo "Smart Router Community Edition — Test Setup"
echo "============================================"
echo "Edition: COMMUNITY (no enterprise build tag, no license required)"
echo "Mode:    DIRECT RPC (no providers!)"
echo "Phases:  1-3 (JSON-RPC over HTTP/HTTPS only)"
echo "============================================"
echo ""

echo "[Test Setup] installing community binary"
make install-all   # builds ./cmd/smartrouter without -tags enterprise

# Start cache services (cache is community-compatible; not gated)
echo "[Test Setup] starting smart router cache service"
screen -d -m -S cache bash -c "source ~/.bashrc; smartrouter cache \
127.0.0.1:20100 --metrics_address 0.0.0.0:20200 --log_level debug 2>&1 | tee $LOGS_DIR/CACHE.log" && sleep 0.25

sleep 2

# Verify cache service started
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
    echo "  ERROR: Cache screen failed to start!"
    echo "  Check $LOGS_DIR/CACHE.log for errors"
fi
echo ""

# Use absolute path for specs
SPECS_DIR="$PROJECT_ROOT/specs/ethereum.json"
echo "Using static specs: $SPECS_DIR"

# Default to 3x publicnode — verified reliable for the startup verification probe.
# Override these if you want to use your own RPC providers; see header comment.
export ETH_RPC_URL_1="${ETH_RPC_URL_1:-https://ethereum-rpc.publicnode.com}"
export ETH_RPC_URL_2="${ETH_RPC_URL_2:-https://ethereum-rpc.publicnode.com}"
export ETH_RPC_URL_3="${ETH_RPC_URL_3:-https://ethereum-rpc.publicnode.com}"
# Optional emergency-fallback tier — emitted under `backup-direct-rpc:` only when set.
export ETH_RPC_URL_4="${ETH_RPC_URL_4:-}"

# Validate that real URLs are set (not placeholders)
for var_name in ETH_RPC_URL_1 ETH_RPC_URL_2 ETH_RPC_URL_3; do
    if [[ "${!var_name}" == *"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"* ]]; then
        echo "ERROR: $var_name contains placeholder!"
        echo ""
        echo "Set real Ethereum endpoints before running:"
        echo "  export ETH_RPC_URL_1='https://mainnet.infura.io/v3/YOUR_KEY'"
        echo "  export ETH_RPC_URL_2='https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY'"
        echo "  export ETH_RPC_URL_3='https://ethereum-rpc.publicnode.com'"
        echo ""
        echo "Then run: $0"
        exit 1
    fi
done

# Reject WebSocket URLs at the script level — community would fail at gating
# anyway, but a clear error here is more legible than the runtime rejection.
for var_name in ETH_RPC_URL_1 ETH_RPC_URL_2 ETH_RPC_URL_3 ETH_RPC_URL_4; do
    val="${!var_name}"
    if [[ "$val" == ws://* || "$val" == wss://* ]]; then
        echo "ERROR: $var_name is a WebSocket URL ($val)."
        echo "Community edition rejects WebSocket transports."
        echo "Use init_smartrouter_eth.sh for the enterprise variant with WebSocket support."
        exit 1
    fi
done

# Generate smart router config only if missing or REGENERATE_CONFIG=1
if [[ -f "$CONFIG_FILE" && "$REGENERATE_CONFIG" != "1" ]]; then
    echo "Using existing config: $CONFIG_FILE"
    echo "  (Set REGENERATE_CONFIG=1 to regenerate from env vars)"
    echo ""
else
echo "Generating smart router config: $CONFIG_FILE"
echo ""
echo "Direct RPC Configuration (community: HTTP only):"
echo "  HTTP Endpoint 1: ${ETH_RPC_URL_1:0:50}..."
echo "  HTTP Endpoint 2: ${ETH_RPC_URL_2:0:50}..."
echo "  HTTP Endpoint 3: ${ETH_RPC_URL_3:0:50}..."
if [[ -n "$ETH_RPC_URL_4" ]]; then
    echo "  Backup Endpoint: ${ETH_RPC_URL_4:0:50}... (fallback-only)"
fi
echo ""
echo "IMPORTANT: This is COMMUNITY DIRECT RPC mode"
echo "    - No license required, no enterprise build tag"
echo "    - JSON-RPC over HTTP/HTTPS only — WebSocket / gRPC / REST are gated"
echo "    - 3 endpoints enable cross-validation testing (2-of-3 / 3-of-3)"
echo ""

# Build the config — each provider is a separate direct-rpc entry.
# All providers point at JSON-RPC HTTP endpoints; no api-interface: websocket.
cat > $CONFIG_FILE <<EOF
# Smart Router — Ethereum (Community Edition)
# JSON-RPC over HTTPS only. Generated by init_smartrouter_eth_community.sh.
# Run with: smartrouter <this-file> --geolocation 1 --use-static-spec specs/ \\
#                       --skip-websocket-verification

endpoints:
  - listen-address: "0.0.0.0:3360"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    network-address: "0.0.0.0:3360"

direct-rpc:
  # HTTP Endpoint 1
  - name: "eth-rpc-1"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    node-urls:
      - url: "$ETH_RPC_URL_1"
        timeout: 10s
        skip-verifications:
          - chain-id
          - pruning

  # HTTP Endpoint 2
  - name: "eth-rpc-2"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    node-urls:
      - url: "$ETH_RPC_URL_2"
        timeout: 10s
        skip-verifications:
          - chain-id
          - pruning

  # HTTP Endpoint 3
  - name: "eth-rpc-3"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    node-urls:
      - url: "$ETH_RPC_URL_3"
        timeout: 10s
        skip-verifications:
          - chain-id
          - pruning
EOF

# Optional: emergency-fallback tier
if [[ -n "$ETH_RPC_URL_4" ]]; then
cat >> $CONFIG_FILE <<EOF

backup-direct-rpc:
  # Backup HTTP Endpoint (used only when all primary direct-rpc peers are exhausted)
  - name: "eth-rpc-backup"
    chain-id: "ETH1"
    api-interface: "jsonrpc"
    node-urls:
      - url: "$ETH_RPC_URL_4"
        timeout: 10s
        skip-verifications:
          - chain-id
          - pruning
EOF
fi

# Verify config file was created
echo ""
echo "Verifying generated config file..."
if [ -f "$CONFIG_FILE" ]; then
    FILE_SIZE=$(wc -c < "$CONFIG_FILE")
    echo "Smart router config exists: $CONFIG_FILE (size: $FILE_SIZE bytes)"
    echo ""
    echo "Config preview (first 30 lines):"
    head -n 30 "$CONFIG_FILE" | sed 's/^/  /'
    echo "  ..."
else
    echo "ERROR: Smart router config NOT found: $CONFIG_FILE"
    exit 1
fi
echo ""
fi  # end: regenerate config or use existing

# Start smart router with --skip-websocket-verification.
# The flag is required because the ETH1 spec declares WS subscriptions as
# applicable; the chain-router verification step otherwise panics when no
# ws:// provider is configured. Community can't include WebSocket providers
# (transport-gated), so this flag is the explicit "I don't need subscriptions" opt-out.
echo "[Test Setup] starting Smart Router (COMMUNITY, DIRECT RPC mode, standalone)"
echo ""
echo "Smart Router Configuration:"
echo "   - Edition: COMMUNITY (no license)"
echo "   - Mode: DIRECT RPC (bypasses Lava providers)"
echo "   - Protocols: JSON-RPC over HTTP/HTTPS"
echo "   - WebSocket: GATED OFF (community)"
echo "   - HTTP Endpoints: 3 endpoints (parallel relay + cross-validation)"
echo "     Endpoint 1: ${ETH_RPC_URL_1:0:40}..."
echo "     Endpoint 2: ${ETH_RPC_URL_2:0:40}..."
echo "     Endpoint 3: ${ETH_RPC_URL_3:0:40}..."
if [[ -n "$ETH_RPC_URL_4" ]]; then
    echo "   - Backup:    1 endpoint (used only when all primaries exhausted)"
    echo "     Endpoint 4: ${ETH_RPC_URL_4:0:40}..."
fi
echo "   - Cache: Enabled (127.0.0.1:20100)"
echo "   - Specs: Static (no blockchain connection)"
echo "   - Listen: 0.0.0.0:3360"
echo ""

screen -d -m -S smartrouter bash -c "cd $PROJECT_ROOT && source ~/.bashrc; smartrouter \
config/smartrouter_examples/smartrouter_eth_community.yml \
--geolocation 1 \
--log-level debug \
--cache-be \"127.0.0.1:20100\" \
--use-static-spec $SPECS_DIR \
--skip-websocket-verification \
--metrics-listen-address ':7779' 2>&1 | tee $LOGS_DIR/SMARTROUTER.log" && sleep 0.25

sleep 3

# Verify smart router started successfully
echo "Verifying smart router screen session..."
if screen -list | grep -q "smartrouter"; then
    echo "Smart router screen is running"
else
    echo "ERROR: Smart router screen failed to start!"
    echo "  Check $LOGS_DIR/SMARTROUTER.log for errors"
    exit 1
fi
echo ""

echo "--- setting up screens done ---"
screen -ls

echo ""
echo "============================================"
echo "Smart Router Community Setup Complete!"
echo "============================================"
echo "Cache:         127.0.0.1:20100 (metrics: 20200)"
echo "Smart Router:  0.0.0.0:3360 (metrics: 7779)"
echo ""
echo "Direct RPC Endpoints (Parallel Relay + Cross-Validation):"
echo "  HTTP 1: ${ETH_RPC_URL_1:0:50}..."
echo "  HTTP 2: ${ETH_RPC_URL_2:0:50}..."
echo "  HTTP 3: ${ETH_RPC_URL_3:0:50}..."
if [[ -n "$ETH_RPC_URL_4" ]]; then
    echo ""
    echo "Backup Endpoint (emergency fallback, not queried in parallel):"
    echo "  HTTP 4: ${ETH_RPC_URL_4:0:50}..."
fi
echo ""
echo "Parallel Relay: Requests sent to ALL 3 endpoints simultaneously"
echo "   First successful response wins (lower latency!)"
echo "   Cross-validation: use lava-cross-validation-* headers for consensus"
echo ""
echo "TESTING Phases 1-3 (HTTP/HTTPS)"
echo "  Phase 1: DirectRPCConnection foundation"
echo "  Phase 2: Session integration"
echo "  Phase 3: JSON-RPC relay logic"
echo "  (Phases 4-5 — REST/WebSocket — require enterprise edition)"
echo ""
echo "Test Commands (HTTP/JSON-RPC):"
echo "  # Get latest block number"
echo "  curl -X POST http://127.0.0.1:3360 \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"jsonrpc\":\"2.0\",\"method\":\"eth_blockNumber\",\"params\":[],\"id\":1}'"
echo ""
echo "  # Get block by number"
echo "  curl -X POST http://127.0.0.1:3360 \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"latest\",false],\"id\":1}'"
echo ""
echo "  # Get balance"
echo "  curl -X POST http://127.0.0.1:3360 \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBalance\",\"params\":[\"0xYOUR_ADDRESS\",\"latest\"],\"id\":1}'"
echo ""

echo "============================================"
echo "CROSS-VALIDATION TESTING (3 endpoints)"
echo "============================================"
echo ""
echo "  # 2-of-3 consensus (query 3 providers, 2 must agree):"
echo '  curl -v -X POST http://127.0.0.1:3360 \'
echo '    -H "Content-Type: application/json" \'
echo '    -H "lava-cross-validation-max-participants: 3" \'
echo '    -H "lava-cross-validation-agreement-threshold: 2" \'
echo '    -d '\''{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'\'''
echo ""
echo "  # 3-of-3 strict consensus (all must agree):"
echo '  curl -v -X POST http://127.0.0.1:3360 \'
echo '    -H "Content-Type: application/json" \'
echo '    -H "lava-cross-validation-max-participants: 3" \'
echo '    -H "lava-cross-validation-agreement-threshold: 3" \'
echo '    -d '\''{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}'\'''
echo ""
echo "  Response headers to check:"
echo "    lava-cross-validation-status          — consensus result"
echo "    lava-cross-validation-agreeing-providers — which providers agreed"
echo "    lava-cross-validation-all-providers    — all participants"
echo ""
echo "============================================"
echo "CACHE TESTING (use these to verify cache)"
echo "============================================"
echo ""
echo "Step 1: Send first request (expect CACHE MISS, then WRITE SUCCESS):"
echo '  curl -s -X POST http://127.0.0.1:3360 \'
echo '    -H "Content-Type: application/json" \'
echo '    -d '\''{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'\'''
echo ""
echo "Step 2: Send SAME request again (expect CACHE HIT):"
echo '  curl -s -X POST http://127.0.0.1:3360 \'
echo '    -H "Content-Type: application/json" \'
echo '    -d '\''{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'\'''
echo ""
echo "Step 3: Force cache bypass (expect CACHE MISS even with cached data):"
echo '  curl -s -X POST http://127.0.0.1:3360 \'
echo '    -H "Content-Type: application/json" \'
echo '    -H "lava-force-cache-refresh: true" \'
echo '    -d '\''{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'\'''
echo ""
echo "Monitor CACHE logs (look for HIT/MISS/WRITE):"
echo "  tail -f $LOGS_DIR/SMARTROUTER.log | grep -i 'CACHE'"
echo ""
echo "Expected log patterns:"
echo "  [CACHE] ✗ MISS - will relay to endpoint    <- First request"
echo "  [CACHE] ✓ WRITE SUCCESS - response cached  <- After relay"
echo "  [CACHE] ✓ HIT - returning cached response  <- Second request"
echo ""
echo "============================================"
echo ""
echo "Monitor Logs:"
echo "  tail -f $LOGS_DIR/SMARTROUTER.log | grep -i 'direct\\|endpoint\\|relay\\|CACHE'"
echo ""
echo "Metrics:"
echo "  Smart Router: http://localhost:7779/metrics"
echo "  Cache: http://localhost:20200/metrics"
echo ""
echo "What to Look For in Logs:"
echo "  - 'Smart Router Community Edition' (edition confirmation)"
echo "  - 'sending direct RPC request' (Phase 3 working!)"
echo "  - 'direct RPC request succeeded' (successful relay)"
echo "  - 'protocol: https' (HTTP protocol detection working)"
echo ""
echo "To Stop All Services:"
echo "  killall smartrouter"
echo "  screen -wipe"
echo ""
echo "============================================"
echo "Ready to test!"
echo "============================================"
