#!/bin/bash
# Smart Router — Ethereum (Enterprise Edition) test setup
#
# Builds the enterprise binary (`-tags enterprise`), resolves a license file,
# starts a cache server and a smart router in screen sessions, generates a
# config with both JSON-RPC and WebSocket entries, and prints test commands
# (HTTP + WebSocket subscriptions).
#
# Differences from init_smartrouter_eth_community.sh:
#   - `make install-enterprise` (community uses `make install-all`)
#   - launches with --license-file resolved from $LICENSE_FILE / ./license.key
#   - generates smartrouter_eth.yml with WS entries; runs Phase 5 (subscriptions)
#
# Override the default RPC providers and license path via env vars before running:
#   export ETH_RPC_URL_1="https://your-rpc-1"
#   export ETH_RPC_URL_2="https://your-rpc-2"
#   export ETH_RPC_URL_3="https://your-rpc-3"
#   export ETH_WS_URL_1="wss://your-ws-1"
#   export ETH_WS_URL_2="wss://your-ws-2"
#   export ETH_WS_URL_3="wss://your-ws-3"
#   export ETH_RPC_URL_4="https://your-backup"   # optional emergency-fallback tier
#   export LICENSE_FILE="/path/to/license.key"   # default: ./license.key

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
CONFIG_FILE="$PROJECT_ROOT/config/smartrouter_examples/smartrouter_eth.yml"

# Resolve license file. Precedence: $LICENSE_FILE, then $PROJECT_ROOT/license.key.
# Resolved to absolute path so the smart router (which reads relative paths
# from CWD, not the binary's directory) finds it deterministically.
LICENSE_FILE_PATH="${LICENSE_FILE:-$PROJECT_ROOT/license.key}"
if [[ ! -f "$LICENSE_FILE_PATH" ]]; then
    echo "ERROR: license file not found at $LICENSE_FILE_PATH"
    echo ""
    echo "The enterprise binary refuses to start without a valid license."
    echo "Either:"
    echo "  - Place a license at $PROJECT_ROOT/license.key"
    echo "  - Set \$LICENSE_FILE to point at one (export LICENSE_FILE=/path/to/license.key)"
    echo ""
    echo "To run without a license, use the community variant:"
    echo "  ./scripts/pre_setups/init_smartrouter_eth_community.sh"
    exit 1
fi
LICENSE_FILE_PATH=$(cd "$(dirname "$LICENSE_FILE_PATH")" && pwd)/$(basename "$LICENSE_FILE_PATH")

# Only remove config when explicitly regenerating (keeps manual edits otherwise)
if [[ "$REGENERATE_CONFIG" == "1" ]]; then
    echo "REGENERATE_CONFIG=1: removing existing smart router config..."
    rm -f "$CONFIG_FILE" 2>/dev/null || true
fi

# Kill any running smart router processes (binary first, then screen wrappers).
killall smartrouter 2>/dev/null || true
sleep 1
killall screen 2>/dev/null || true
sleep 1
screen -wipe
sleep 1

echo "============================================"
echo "Smart Router Enterprise Edition — Test Setup"
echo "============================================"
echo "Edition: ENTERPRISE (-tags enterprise, license required)"
echo "License: $LICENSE_FILE_PATH"
echo "Mode:    DIRECT RPC (no providers!)"
echo "Phases:  1-5 (JSON-RPC + WebSocket subscriptions)"
echo "============================================"
echo ""

echo "[Test Setup] installing enterprise binary"
make install-enterprise   # builds ./cmd/smartrouter with -tags enterprise

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

# HTTP defaults — same as community defaults; users override per-deployment
export ETH_RPC_URL_1="${ETH_RPC_URL_1:-https://ethereum-rpc.publicnode.com}"
export ETH_RPC_URL_2="${ETH_RPC_URL_2:-https://ethereum-rpc.publicnode.com}"
export ETH_RPC_URL_3="${ETH_RPC_URL_3:-https://ethereum-rpc.publicnode.com}"
# Optional emergency-fallback tier — emitted under `backup-direct-rpc:` only when set.
export ETH_RPC_URL_4="${ETH_RPC_URL_4:-}"

# WebSocket endpoints (required for Phase 5 subscriptions; enterprise-gated)
export ETH_WS_URL_1="${ETH_WS_URL_1:-wss://ethereum-rpc.publicnode.com}"
export ETH_WS_URL_2="${ETH_WS_URL_2:-wss://ethereum-rpc.publicnode.com}"
export ETH_WS_URL_3="${ETH_WS_URL_3:-wss://ethereum-rpc.publicnode.com}"

# Validate that real URLs are set (not placeholders)
for var_name in ETH_RPC_URL_1 ETH_RPC_URL_2 ETH_RPC_URL_3 ETH_WS_URL_1 ETH_WS_URL_2 ETH_WS_URL_3; do
    if [[ "${!var_name}" == *"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"* ]]; then
        echo "ERROR: $var_name contains placeholder!"
        echo ""
        echo "Set real Ethereum endpoints before running:"
        echo "  export ETH_RPC_URL_1='https://mainnet.infura.io/v3/YOUR_KEY'"
        echo "  export ETH_RPC_URL_2='https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY'"
        echo "  export ETH_RPC_URL_3='https://ethereum-rpc.publicnode.com'"
        echo "  export ETH_WS_URL_1='wss://mainnet.infura.io/ws/v3/YOUR_KEY'"
        echo "  export ETH_WS_URL_2='wss://eth-mainnet.g.alchemy.com/v2/YOUR_KEY'"
        echo "  export ETH_WS_URL_3='wss://ethereum-rpc.publicnode.com'"
        echo ""
        echo "Then run: $0"
        exit 1
    fi
done

# Type-check WS URLs to catch the "I pasted an https URL" mistake early
for var_name in ETH_WS_URL_1 ETH_WS_URL_2 ETH_WS_URL_3; do
    val="${!var_name}"
    if [[ "$val" != ws://* && "$val" != wss://* ]]; then
        echo "ERROR: $var_name is not a WebSocket URL ($val) — must start with ws:// or wss://"
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
echo "Direct RPC Configuration (enterprise: HTTP + WebSocket):"
echo "  HTTP Endpoint 1: ${ETH_RPC_URL_1:0:50}..."
echo "  HTTP Endpoint 2: ${ETH_RPC_URL_2:0:50}..."
echo "  HTTP Endpoint 3: ${ETH_RPC_URL_3:0:50}..."
if [[ -n "$ETH_RPC_URL_4" ]]; then
    echo "  Backup Endpoint: ${ETH_RPC_URL_4:0:50}... (fallback-only)"
fi
echo ""
echo "  WebSocket Endpoints (Phase 5 - Subscriptions):"
echo "    WS Endpoint 1: ${ETH_WS_URL_1:0:50}..."
echo "    WS Endpoint 2: ${ETH_WS_URL_2:0:50}..."
echo "    WS Endpoint 3: ${ETH_WS_URL_3:0:50}..."
echo ""

# Build the config — JSON-RPC over HTTP + WebSocket subscription endpoints.
cat > $CONFIG_FILE <<EOF
# Smart Router — Ethereum (Enterprise Edition)
# JSON-RPC over HTTP/HTTPS + WebSocket subscriptions. Generated by
# init_smartrouter_eth_enterprise.sh. Requires the enterprise binary and a
# valid license — community variant rejects WebSocket entries at startup.
#
# Run with: smartrouter <this-file> --geolocation 1 --use-static-spec specs/ \\
#                       --license-file=/path/to/license.key

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

  # WebSocket Endpoint 1 (Phase 5 - Subscriptions)
  - name: "eth-ws-1"
    chain-id: "ETH1"
    api-interface: "websocket"
    node-urls:
      - url: "$ETH_WS_URL_1"

  # WebSocket Endpoint 2 (Phase 5 - Subscriptions)
  - name: "eth-ws-2"
    chain-id: "ETH1"
    api-interface: "websocket"
    node-urls:
      - url: "$ETH_WS_URL_2"

  # WebSocket Endpoint 3 (Phase 5 - Subscriptions)
  - name: "eth-ws-3"
    chain-id: "ETH1"
    api-interface: "websocket"
    node-urls:
      - url: "$ETH_WS_URL_3"
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

echo "[Test Setup] starting Smart Router (ENTERPRISE, DIRECT RPC mode, standalone)"
echo ""
echo "Smart Router Configuration:"
echo "   - Edition: ENTERPRISE"
echo "   - License: $LICENSE_FILE_PATH"
echo "   - Mode: DIRECT RPC (bypasses Lava providers)"
echo "   - Protocols: JSON-RPC over HTTP/HTTPS + WebSocket"
echo "   - WebSocket: ENABLED (Phase 5 subscriptions)"
echo "   - HTTP Endpoints: 3 endpoints (parallel relay + cross-validation)"
echo "     Endpoint 1: ${ETH_RPC_URL_1:0:40}..."
echo "     Endpoint 2: ${ETH_RPC_URL_2:0:40}..."
echo "     Endpoint 3: ${ETH_RPC_URL_3:0:40}..."
if [[ -n "$ETH_RPC_URL_4" ]]; then
    echo "   - Backup:    1 endpoint (used only when all primaries exhausted)"
    echo "     Endpoint 4: ${ETH_RPC_URL_4:0:40}..."
fi
echo "   - WS Endpoint 1: ${ETH_WS_URL_1:0:40}..."
echo "   - WS Endpoint 2: ${ETH_WS_URL_2:0:40}..."
echo "   - WS Endpoint 3: ${ETH_WS_URL_3:0:40}..."
echo "   - Cache: Enabled (127.0.0.1:20100)"
echo "   - Specs: Static (no blockchain connection)"
echo "   - Listen: 0.0.0.0:3360"
echo ""

# --skip-websocket-verification kept defensively; with WS entries in the
# config the verification should pass without it, but leaving it in matches
# the existing pattern and tolerates partial WS availability during dev.
screen -d -m -S smartrouter bash -c "cd $PROJECT_ROOT && source ~/.bashrc; smartrouter \
config/smartrouter_examples/smartrouter_eth.yml \
--geolocation 1 \
--log-level debug \
--cache-be \"127.0.0.1:20100\" \
--use-static-spec $SPECS_DIR \
--skip-websocket-verification \
--license-file=\"$LICENSE_FILE_PATH\" \
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
echo "Smart Router Enterprise Setup Complete!"
echo "============================================"
echo "License:       $LICENSE_FILE_PATH"
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
echo "WebSocket Endpoints (Subscriptions):"
echo "  WS 1: ${ETH_WS_URL_1:0:50}..."
echo "  WS 2: ${ETH_WS_URL_2:0:50}..."
echo "  WS 3: ${ETH_WS_URL_3:0:50}..."
echo ""
echo "Parallel Relay: Requests sent to ALL 3 endpoints simultaneously"
echo "   First successful response wins (lower latency!)"
echo "   Cross-validation: use lava-cross-validation-* headers for consensus"
echo ""
echo "TESTING Phases 1-5 (HTTP/HTTPS + WebSocket)"
echo "  Phase 1: DirectRPCConnection foundation"
echo "  Phase 2: Session integration"
echo "  Phase 3: JSON-RPC relay logic"
echo "  Phase 4: REST relay logic"
echo "  Phase 5: WebSocket subscriptions"
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

echo "Test Commands (WebSocket Subscriptions - Phase 5):"
echo "  # Install wscat if needed: npm install -g wscat"
echo ""
echo "  # Connect to WebSocket endpoint"
echo "  wscat -c ws://127.0.0.1:3360/ws"
echo ""
echo "  # Once connected, subscribe to new blocks:"
echo '  > {"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}'
echo ""
echo "  # Subscribe to pending transactions:"
echo '  > {"jsonrpc":"2.0","id":2,"method":"eth_subscribe","params":["newPendingTransactions"]}'
echo ""
echo "  # Subscribe to logs (e.g., USDT transfers):"
echo '  > {"jsonrpc":"2.0","id":3,"method":"eth_subscribe","params":["logs",{"address":"0xdAC17F958D2ee523a2206206994597C13D831ec7"}]}'
echo ""
echo "  # Unsubscribe (use subscription ID from response):"
echo '  > {"jsonrpc":"2.0","id":4,"method":"eth_unsubscribe","params":["0xSUBSCRIPTION_ID"]}'
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
echo "  tail -f $LOGS_DIR/SMARTROUTER.log | grep -i 'direct\\|endpoint\\|relay\\|subscription\\|CACHE'"
echo ""
echo "Metrics:"
echo "  Smart Router: http://localhost:7779/metrics"
echo "  Cache: http://localhost:20200/metrics"
echo ""
echo "What to Look For in Logs:"
echo "  - 'Smart Router ENTERPRISE Edition' (license validated)"
echo "  - 'Loading enterprise license source=...'  (license-file resolution)"
echo "  - 'sending direct RPC request' (Phase 3 working!)"
echo "  - 'direct RPC request succeeded' (successful relay)"
echo "  - 'protocol: https' (HTTP protocol detection working)"
echo "  - 'DirectWS: subscription started' (Phase 5 working!)"
echo "  - 'WebSocket pool: connection added' (connection pooling)"
echo "  - 'DirectWS: client joined existing subscription' (deduplication)"
echo ""
echo "To Stop All Services:"
echo "  killall smartrouter"
echo "  screen -wipe"
echo ""
echo "============================================"
echo "Ready to test!"
echo "============================================"
