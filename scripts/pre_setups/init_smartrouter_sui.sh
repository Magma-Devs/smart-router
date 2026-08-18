#!/bin/bash
# Sui gRPC counterpart of init_smartrouter_eth.sh.
#
# Where the ETH script exercises JSON-RPC + WebSocket subscriptions, this one
# exercises the gRPC interface and its server-streaming subscriptions (MAG-2643).
#
# Two things differ from every other pre-setup script, both deliberate:
#
#  1. SUI is not bundled in this repo's specs/ directory. The spec is fetched
#     from magma-Devs/lava-specs at run time into a local, gitignored directory.
#
#  2. That fetched spec is PATCHED before use — a no-op against current
#     lava-specs, kept as a safety net. Streaming is served off the SUBSCRIBE
#     parse directive; lava-specs#115 added it for Sui's three
#     SubscriptionService methods, so a fresh fetch already has it. The patch
#     only bites on an older snapshot, where it prevents a confusing refusal.
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

# The config lands under debugging/ (gitignored), NOT config/smartrouter_examples/
# like the other pre-setup scripts. This one renders SUI_GRPC_API_KEY into the
# node-url auth-headers, and a rendered config carrying a secret must not sit in
# a tracked directory (AGENTS.md, security rules).
CONFIG_REL="debugging/smartrouter_sui.yml"
CONFIG_FILE="$PROJECT_ROOT/$CONFIG_REL"

# The router resolves its config argument with viper.SetConfigName plus a search
# path of "." and "./config", so the argument is a NAME looked up under those —
# an absolute path is searched for *inside* them and never resolves. Every
# pre-setup script therefore cds to the project root and passes a relative path.

# Specs are fetched here rather than into specs/, which holds only the bundled set.
SPECS_DIR="$PROJECT_ROOT/debugging/specs_sui"
SPEC_FILE="$SPECS_DIR/sui.json"

mkdir -p "$PROJECT_ROOT/debugging"

# Only remove config when explicitly regenerating (keeps manual edits for e.g. timeout testing)
if [[ "$REGENERATE_CONFIG" == "1" ]]; then
    echo "REGENERATE_CONFIG=1: removing existing smart router config and spec..."
    rm -f "$CONFIG_FILE" 2>/dev/null || true
    rm -f "$SPEC_FILE" 2>/dev/null || true
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
echo "Smart Router Direct RPC Test Setup — Sui gRPC"
echo "============================================"
echo "Testing: gRPC unary relays + gRPC server-streaming subscriptions"
echo "Mode: DIRECT RPC (no providers — the router only talks to endpoints)"
echo "============================================"
echo ""

echo "[Test Setup] installing all binaries"
make install

# Start cache services (required for cache testing)
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

# ============================================================================
# SPEC FETCH + PATCH
# ============================================================================
#
# sui.json is not bundled in this repo (specs/ carries only the bundled set),
# so fetch it from lava-specs. --use-static-spec also accepts the remote repo
# URL directly, but we need a local copy in order to patch it.
mkdir -p "$SPECS_DIR"
if [[ ! -f "$SPEC_FILE" ]]; then
    echo "Fetching sui.json from lava-specs..."
    if ! curl -sSL --fail -o "$SPEC_FILE" \
        "https://raw.githubusercontent.com/magma-Devs/lava-specs/main/sui.json"; then
        echo "ERROR: could not fetch sui.json from lava-specs."
        echo "  Check network access, or drop a copy at: $SPEC_FILE"
        exit 1
    fi
    echo "  Fetched: $SPEC_FILE"
else
    echo "Using existing spec: $SPEC_FILE"
    echo "  (Set REGENERATE_CONFIG=1 to re-fetch)"
fi

# ---- The patch -------------------------------------------------------------
#
# The router decides whether a gRPC method is server-streaming from the spec's
# SUBSCRIBE parse directive — the same signal the WebSocket path uses — not from
# a live reflection lookup. That is deliberate: reflection is throttled or
# disabled on many public gRPC gateways, and a router that could only learn
# streaming-ness from reflection silently fell back to a unary Invoke whenever
# the lookup failed, returning a truncated stream after the full timeout instead
# of refusing (MAG-2643).
#
# lava-specs#115 added that directive for all three of:
#   sui.rpc.v2.SubscriptionService/SubscribeCheckpoints
#   sui.rpc.v2.SubscriptionService/SubscribeEvents
#   sui.rpc.v2.SubscriptionService/SubscribeTransactions
#
# so a fresh fetch needs nothing and this reports "already patched". It stays as
# a safety net for a pinned or cached snapshot from before that landed: without
# the directive the router refuses these methods — correct and safe, but not what
# this script is here to demonstrate. It also sets hanging_api on them, which the
# spec does not, keeping an unbounded stream out of latency scoring.
echo ""
echo "Checking sui.json for the SUBSCRIBE directives streaming needs..."
python3 - "$SPEC_FILE" <<'PYEOF'
import json, sys

path = sys.argv[1]
with open(path) as handle:
    document = json.load(handle)

patched = []
for spec in document["proposal"]["specs"]:
    for collection in spec.get("api_collections", []):
        if collection["collection_data"]["api_interface"] != "grpc":
            continue
        directives = collection.setdefault("parse_directives", [])
        declared = {
            d.get("api_name") for d in directives if d.get("function_tag") == "SUBSCRIBE"
        }
        for api in collection.get("apis", []):
            if "SubscriptionService/" not in api["name"]:
                continue
            # Streaming calls are long-lived; keep their latency out of QoS scoring
            # and give them the long timeout, same as any hanging API.
            api.setdefault("category", {})["hanging_api"] = True
            if api["name"] in declared:
                continue
            directives.append({"function_tag": "SUBSCRIBE", "api_name": api["name"]})
            patched.append(f'{spec["index"]}:{api["name"]}')

with open(path, "w") as handle:
    json.dump(document, handle, indent=2)

if patched:
    for name in patched:
        print(f"  + SUBSCRIBE directive  {name}")
else:
    print("  (already patched — nothing to do)")
PYEOF
echo ""

# ============================================================================
# ENDPOINTS
# ============================================================================
#
# Sui gRPC endpoints. SUIT is the Sui Testnet spec (SUI = mainnet, SUID = devnet).
#
# The default is Sui's own public testnet fullnode, because it works out of the
# box: it answers unary calls AND streams SubscribeCheckpoints anonymously.
#
# The Tatum gateway is supported but is NOT the default, for two reasons found by
# running it: its free tier allows 5 requests per minute, which the router's own
# startup probing exhausts before you can issue a call (every request then comes
# back 429), and SubscribeCheckpoints refuses anonymous access outright
# ("Method is not available for anonymous access"). To use it, supply a key:
#
#   export SUI_GRPC_URL_1="grpcs://sui-testnet-grpc.gateway.tatum.io:443"
#   export SUI_GRPC_API_KEY="<your tatum key>"
#
# The key is sent as the x-api-key gRPC metadata header via auth-config.
export SUI_GRPC_URL_1="${SUI_GRPC_URL_1:-grpcs://fullnode.testnet.sui.io:443}"
export SUI_GRPC_URL_2="${SUI_GRPC_URL_2:-}"
export SUI_GRPC_URL_3="${SUI_GRPC_URL_3:-}"
# Optional backup endpoint — emitted under `backup-direct-rpc:` only when set.
# Backups are consulted only when every primary `direct-rpc` peer is exhausted.
export SUI_GRPC_URL_4="${SUI_GRPC_URL_4:-}"
export SUI_CHAIN_ID="${SUI_CHAIN_ID:-SUIT}"

# API keys are PER ENDPOINT, deliberately. A single shared key would be rendered
# into every node-url, which means handing your Tatum key to whatever unrelated
# gateway happens to be endpoint 2. SUI_GRPC_API_KEY is accepted as a convenience
# alias for endpoint 1 only, since endpoint 1 is the Tatum default.
export SUI_GRPC_API_KEY_1="${SUI_GRPC_API_KEY_1:-${SUI_GRPC_API_KEY:-}}"
export SUI_GRPC_API_KEY_2="${SUI_GRPC_API_KEY_2:-}"
export SUI_GRPC_API_KEY_3="${SUI_GRPC_API_KEY_3:-}"
export SUI_GRPC_API_KEY_4="${SUI_GRPC_API_KEY_4:-}"

# emit_endpoint <name> <url> [api-key] — one direct-rpc entry. The key, when
# given, rides as an x-api-key gRPC metadata header on that endpoint alone.
emit_endpoint() {
    local name="$1" url="$2" api_key="$3"
    cat <<EOF

  - name: "$name"
    chain-id: "$SUI_CHAIN_ID"
    api-interface: "grpc"
    node-urls:
      - url: "$url"
        auth-config:
          use-tls: true
EOF
    if [[ -n "$api_key" ]]; then
    cat <<EOF
          auth-headers:
            x-api-key: "$api_key"
EOF
    fi
    cat <<EOF
        skip-verifications:
          - chain-id
          - pruning
EOF
}

# Generate smart router config only if missing or REGENERATE_CONFIG=1
if [[ -f "$CONFIG_FILE" && "$REGENERATE_CONFIG" != "1" ]]; then
    echo "Using existing config: $CONFIG_FILE"
    echo "  (Set REGENERATE_CONFIG=1 to regenerate from env vars)"
    echo ""
else
echo "Generating smart router config: $CONFIG_FILE"
echo ""
echo "Direct RPC Configuration (gRPC):"
echo "  Chain ID:   $SUI_CHAIN_ID"
echo "  Endpoint 1: ${SUI_GRPC_URL_1:0:60}..."
[[ -n "$SUI_GRPC_URL_2" ]] && echo "  Endpoint 2: ${SUI_GRPC_URL_2:0:60}..."
[[ -n "$SUI_GRPC_URL_3" ]] && echo "  Endpoint 3: ${SUI_GRPC_URL_3:0:60}..."
[[ -n "$SUI_GRPC_URL_4" ]] && echo "  Backup:     ${SUI_GRPC_URL_4:0:60}... (fallback-only)"
if [[ -n "$SUI_GRPC_API_KEY_1" ]]; then
    echo "  API key 1:  set (sent to endpoint 1 as x-api-key)"
else
    echo "  API key 1:  none (the default endpoint needs none)"
fi
[[ -n "$SUI_GRPC_API_KEY_2" ]] && echo "  API key 2:  set"
[[ -n "$SUI_GRPC_API_KEY_3" ]] && echo "  API key 3:  set"
echo ""
echo "IMPORTANT: This is DIRECT RPC mode"
echo "    - Smart router connects DIRECTLY to Sui gRPC endpoints"
echo "    - Add SUI_GRPC_URL_2/3 to enable cross-validation across endpoints"
echo ""

cat > $CONFIG_FILE <<EOF
# Smart Router Direct RPC Configuration — Sui gRPC
# Exercises gRPC unary relays and gRPC server-streaming subscriptions.
# Generated by scripts/pre_setups/init_smartrouter_sui.sh — do not hand-edit
# unless you also stop passing REGENERATE_CONFIG=1.
#
# MAY CONTAIN AN API KEY. Lives under debugging/ (gitignored) for that reason —
# do not copy it into config/smartrouter_examples/.

endpoints:
  - listen-address: "0.0.0.0:3370"
    chain-id: "$SUI_CHAIN_ID"
    api-interface: "grpc"
    network-address: "0.0.0.0:3370"

direct-rpc:
EOF

emit_endpoint "sui-grpc-1" "$SUI_GRPC_URL_1" "$SUI_GRPC_API_KEY_1" >> $CONFIG_FILE
[[ -n "$SUI_GRPC_URL_2" ]] && emit_endpoint "sui-grpc-2" "$SUI_GRPC_URL_2" "$SUI_GRPC_API_KEY_2" >> $CONFIG_FILE
[[ -n "$SUI_GRPC_URL_3" ]] && emit_endpoint "sui-grpc-3" "$SUI_GRPC_URL_3" "$SUI_GRPC_API_KEY_3" >> $CONFIG_FILE

if [[ -n "$SUI_GRPC_URL_4" ]]; then
    echo "" >> $CONFIG_FILE
    echo "backup-direct-rpc:" >> $CONFIG_FILE
    emit_endpoint "sui-grpc-backup" "$SUI_GRPC_URL_4" "$SUI_GRPC_API_KEY_4" >> $CONFIG_FILE
fi

echo ""
echo "Verifying generated config file..."
if [ -f "$CONFIG_FILE" ]; then
    FILE_SIZE=$(wc -c < "$CONFIG_FILE")
    echo "Smart router config exists: $CONFIG_FILE (size: $FILE_SIZE bytes)"
    echo ""
    echo "Config preview:"
    sed 's/^/  /' "$CONFIG_FILE"
else
    echo "ERROR: Smart router config NOT found: $CONFIG_FILE"
    exit 1
fi
echo ""
fi  # end: regenerate config or use existing

# Start Smart Router
echo "[Test Setup] starting Smart Router (DIRECT RPC mode, gRPC)"
echo ""
echo "Smart Router Configuration:"
echo "   - Mode: DIRECT RPC"
echo "   - Protocol: gRPC over HTTP/2 (TLS)"
echo "   - Streaming: gRPC server-streaming subscriptions"
echo "   - Cache: Enabled (127.0.0.1:20100)"
echo "   - Specs: $SPECS_DIR"
echo "   - Listen: 0.0.0.0:3370"
echo ""

screen -d -m -S smartrouter bash -c "cd $PROJECT_ROOT && source ~/.bashrc; smartrouter \
$CONFIG_REL \
--log-level debug \
--cache-be \"127.0.0.1:20100\" \
--use-static-spec $SPECS_DIR \
--metrics-listen-address ':7779' 2>&1 | tee $LOGS_DIR/SMARTROUTER.log" && sleep 0.25

sleep 4

# Verify smart router started successfully
echo "Verifying smart router screen session..."
if screen -list | grep -q "smartrouter"; then
    echo "Smart router screen is running"
else
    echo "ERROR: Smart router screen failed to start!"
    echo "  Check $LOGS_DIR/SMARTROUTER.log for errors"
    exit 1
fi

# The listener only installs the streaming callback when the loaded spec declares
# at least one subscription — this line is the proof the patch above took effect.
echo ""
echo "Checking that gRPC server-streaming was enabled..."
if grep -q "gRPC server-streaming support enabled" "$LOGS_DIR/SMARTROUTER.log" 2>/dev/null; then
    echo "  gRPC server-streaming support: ENABLED"
else
    echo "  WARNING: streaming support not reported as enabled yet."
    echo "  Either the router is still starting, or the spec patch did not apply."
    echo "  Check: grep 'server-streaming' $LOGS_DIR/SMARTROUTER.log"
fi
echo ""

echo "--- setting up screens done ---"
screen -ls

echo ""
echo "============================================"
echo "Smart Router Sui gRPC Setup Complete!"
echo "============================================"
echo "Cache:         127.0.0.1:20100 (metrics: 20200)"
echo "Smart Router:  0.0.0.0:3370 (metrics: 7779)"
echo "Chain:         $SUI_CHAIN_ID"
echo ""
echo "Direct gRPC Endpoints:"
echo "  1: ${SUI_GRPC_URL_1:0:60}..."
[[ -n "$SUI_GRPC_URL_2" ]] && echo "  2: ${SUI_GRPC_URL_2:0:60}..."
[[ -n "$SUI_GRPC_URL_3" ]] && echo "  3: ${SUI_GRPC_URL_3:0:60}..."
[[ -n "$SUI_GRPC_URL_4" ]] && echo "  backup: ${SUI_GRPC_URL_4:0:60}..."
echo ""
echo "============================================"
echo "TEST COMMANDS (grpcurl — plaintext to the local router)"
echo "============================================"
echo ""
echo "  # Install grpcurl if needed: brew install grpcurl"
echo ""
echo "  # 1. Reflection through the router (proves the reflection proxy works)"
echo "  grpcurl -plaintext 127.0.0.1:3370 list"
echo ""
echo "  # 2. Unary relay — node identity and current checkpoint height"
echo "  grpcurl -plaintext -d '{}' 127.0.0.1:3370 \\"
echo "    sui.rpc.v2.LedgerService/GetServiceInfo"
echo ""
echo "  # 3. Unary relay — fetch a checkpoint by sequence number"
echo "  grpcurl -plaintext -d '{\"sequence_number\":1}' 127.0.0.1:3370 \\"
echo "    sui.rpc.v2.LedgerService/GetCheckpoint"
echo ""
echo "============================================"
echo "SERVER-STREAMING SUBSCRIPTIONS (MAG-2643)"
echo "============================================"
echo ""
echo "  # 4. Subscribe to checkpoints — this is the streaming path."
echo "  #    Expect a continuous stream of messages, NOT a single reply."
echo "  #    Ctrl-C to stop; the router releases the upstream subscription."
echo "  #"
echo "  #    The FIRST subscribe after startup takes a few seconds: the method"
echo "  #    descriptor is fetched from the upstream by reflection and cached."
echo "  #    Do not pass a short -max-time on that first call or it will look"
echo "  #    like an empty stream. Later subscribes start immediately."
echo "  grpcurl -plaintext -d '{}' 127.0.0.1:3370 \\"
echo "    sui.rpc.v2.SubscriptionService/SubscribeCheckpoints"
echo ""
echo "  # 5. Subscribe to events"
echo "  grpcurl -plaintext -d '{}' 127.0.0.1:3370 \\"
echo "    sui.rpc.v2.SubscriptionService/SubscribeEvents"
echo ""
echo "  # 6. Subscription sharing — run #4 in TWO terminals at once."
echo "  #    Identical parameters share ONE upstream stream; the log shows"
echo "  #    'client joined existing subscription' for the second client."
echo "  #    Killing the first client must NOT stop the second."
echo ""
echo "  # 7. Response headers carry the router's subscription id:"
echo "  grpcurl -plaintext -v -d '{}' 127.0.0.1:3370 \\"
echo "    sui.rpc.v2.SubscriptionService/SubscribeCheckpoints 2>&1 | grep -i sub-id"
echo ""
case "$SUI_GRPC_URL_1" in
  *tatum*)
    if [[ -z "$SUI_GRPC_API_KEY_1" ]]; then
echo "  WARNING: endpoint 1 is a Tatum gateway with no API key set. Its free"
echo "           tier caps at 5 requests/minute — the router's startup probing"
echo "           alone exceeds that, so calls come back 429 — and it refuses"
echo "           SubscribeCheckpoints anonymously. Set SUI_GRPC_API_KEY."
echo ""
    fi
    ;;
esac
echo "============================================"
echo "WHAT TO LOOK FOR IN LOGS"
echo "============================================"
echo ""
echo "  tail -f $LOGS_DIR/SMARTROUTER.log | grep -i 'stream\\|subscri\\|grpc'"
echo ""
echo "Startup:"
echo "  - 'gRPC server-streaming support enabled'   <- spec declares a subscription"
echo "  - 'Using DirectGRPCSubscriptionManager'     <- manager constructed"
echo ""
echo "On subscribe:"
echo "  - 'in <<< GRPC stream subscribe'            <- listener recognised it"
echo "  - 'DirectGRPC: created new subscription'    <- upstream stream opened"
echo "  - 'DirectGRPC: client joined existing subscription' <- sharing (test #6)"
echo ""
echo "On disconnect:"
echo "  - 'DirectGRPC: subscription cleaned up'     <- released, no leak"
echo ""
echo "Refusals (both are correct behaviour, not bugs):"
echo "  - 'must be served through the streaming listener'"
echo "        a spec-declared subscription reached the unary relay path"
echo "  - 'is server-streaming upstream but ... no SUBSCRIBE parse directive'"
echo "        the spec patch above did not apply — streaming stays refused"
echo "        rather than being invoked as a unary call and truncated"
echo ""
echo "============================================"
echo "CROSS-VALIDATION (needs 2+ endpoints)"
echo "============================================"
echo ""
echo "  # Cross-validation needs at least as many endpoints as max-participants,"
echo "  # so configure a second one and regenerate:"
echo ""
echo "  #   export SUI_GRPC_URL_2=\"grpcs://<another sui grpc endpoint>\""
echo "  #   REGENERATE_CONFIG=1 scripts/pre_setups/init_smartrouter_sui.sh"
echo ""
echo "  # KNOWN ISSUE — cross-validation over gRPC does not currently pass here."
echo "  # Each provider keeps its own protobuf descriptor cache and fills it by"
echo "  # reflection on first use. Against this endpoint that lookup takes longer"
echo "  # than the 2s relay timeout, so a cold provider fails at exactly 2.000s,"
echo "  # never caches the descriptor, and fails the same way next time. CV needs"
echo "  # N successes, so it stalls at successCount:1 while the warm provider"
echo "  # answers in ~140ms. Unary and streaming are unaffected — this is the"
echo "  # unary CV path only, and it predates the streaming work."
echo "  #"
echo "  # Look for this in the log to confirm you are hitting it:"
echo "  #   direct RPC relay failed ... failed to find service via reflection"
echo "  #   ... DeadlineExceeded ... latency=2.000s"
echo ""
echo "  # Pick a checkpoint that still exists. Sui testnet prunes, so sequence 1"
echo "  # is long gone (NotFound), and lowestAvailableCheckpoint races with the"
echo "  # pruner. A recent-but-settled checkpoint is both present and immutable:"
echo ""
echo "  CP=\$(grpcurl -plaintext -d '{}' 127.0.0.1:3370 \\"
echo "    sui.rpc.v2.LedgerService/GetServiceInfo \\"
echo "    | python3 -c \"import sys,json; print(int(json.load(sys.stdin)['checkpointHeight'])-200)\")"
echo ""
echo "  grpcurl -plaintext \\"
echo "    -H 'lava-cross-validation-max-participants: 2' \\"
echo "    -H 'lava-cross-validation-agreement-threshold: 2' \\"
echo "    -d \"{\\\"sequence_number\\\":\$CP}\" 127.0.0.1:3370 \\"
echo "    sui.rpc.v2.LedgerService/GetCheckpoint"
echo ""
echo "  (GetCheckpoint by sequence number is deterministic, so it is a sound"
echo "   cross-validation target. GetServiceInfo is not — it reports live head.)"
echo ""
echo "Metrics:"
echo "  Smart Router: http://localhost:7779/metrics"
echo "  Cache: http://localhost:20200/metrics"
echo ""
echo "To Stop All Services:"
echo "  killall smartrouter"
echo "  screen -wipe"
echo ""
echo "============================================"
echo "Ready to test!"
echo "============================================"
