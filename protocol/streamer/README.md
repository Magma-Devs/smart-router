# Lava Event Streamer

A **stateless**, real-time blockchain event streaming service for EVM-compatible chains that integrates with Lava's smart router.

## 🎯 Design Philosophy

Unlike traditional indexers that store blockchain data in databases, the **Lava Event Streamer** is:

- ✅ **100% Stateless** - No database, no cache, pure streaming
- ✅ **Real-time** - Events delivered as they happen
- ✅ **Lightweight** - Minimal resource usage, highly scalable
- ✅ **Event-driven** - Push-based architecture via WebSockets & webhooks
- ✅ **Flexible** - Forward to external systems via webhooks

## 🚀 Features

### Event Streaming
- ✅ **Native Transactions** - All EVM transactions
- ✅ **Internal Transactions** - Contract-to-contract calls
- ✅ **Event Logs** - All contract events
- ✅ **Decoded Events** - Auto-decode ERC20/ERC721/custom ABIs
- ✅ **Block Events** - New blocks in real-time

### Delivery Methods
- ✅ **WebSocket Server** - Real-time bidirectional streaming
- ✅ **Webhooks** - HTTP callbacks with retries & HMAC signing
### Event Decoding
- ✅ **ERC20 Events** - Transfer, Approval, Mint, Burn
- ✅ **ERC721 Events** - Transfer, Approval, ApprovalForAll
- ✅ **Custom ABIs** - Register any contract ABI
- ✅ **Event Type Recognition** - Automatic categorization

### Advanced Features
- ✅ **Subscription Filters** - Filter by address, contract, event type
- ✅ **Rate Limiting** - Per-subscription event throttling
- ✅ **Batch Delivery** - Batch webhooks for efficiency
- ✅ **Smart Router Integration** - Use Lava's failover & load balancing

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/magma-Devs/smart-router.git
cd smart-router

# Install all binaries
make install
```

## 🎬 Quick Start

### 1. Generate Example Configuration

```bash
smartrouter streamer --example-config > streamer.yml
```

### 2. Edit Configuration

```yaml
# streamer.yml
rpc_endpoint: "http://localhost:3333"  # Smart router or direct RPC
chain_id: "ETH1"

# Streaming settings
stream_transactions: true
stream_logs: true
decode_events: true
track_common_events: true

# WebSocket server
enable_websocket: true
websocket_addr: ":8080"
websocket_path: "/stream"

# Webhooks
enable_webhooks: true
webhook_workers: 10

# Watch specific contracts
watched_contracts:
  - name: "USDC"
    address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    stream_all_txs: true
```

### 3. Run the Streamer

```bash
smartrouter streamer streamer.yml
```

## 💡 Usage Examples

### WebSocket Streaming

Connect to the WebSocket server and subscribe to events:

```javascript
const ws = new WebSocket('ws://localhost:8080/stream');

// Subscribe to USDC transfers
ws.send(JSON.stringify({
  action: 'subscribe',
  filters: {
    event_types: ['token_transfer'],
    contract_address: '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
  }
}));

// Receive events
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Event:', data);
};
```

### HTTP Subscription API

```bash
# Create subscription with webhook
curl -X POST http://localhost:8080/subscribe \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "my-app",
    "filters": {
      "event_types": ["token_transfer", "token_approval"],
      "contract_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    },
    "webhook": {
      "url": "https://myapp.com/webhook",
      "secret": "my-secret-key",
      "retry_attempts": 3
    }
  }'

# Response
{
  "id": "sub_123456",
  "client_id": "my-app",
  "active": true,
  "created_at": "2024-..."
}
```

### Event Filters

```json
{
  "filters": {
    "event_types": ["token_transfer", "nft_transfer"],
    "addresses": ["0xabc...", "0xdef..."],
    "from_block": 18000000,
    "min_value": "1000000000000000000",
    "decode_events": true
  }
}
```

## 📊 Event Types

The streamer emits these event types:

| Event Type | Description |
|-----------|-------------|
| `new_block` | New block detected |
| `transaction` | Native transaction |
| `internal_transaction` | Contract-to-contract call |
| `log` | Raw contract event log |
| `decoded_event` | ABI-decoded event |
| `token_transfer` | ERC20/ERC721 transfer |
| `token_approval` | ERC20/ERC721 approval |
| `nft_transfer` | ERC721 transfer |
| `contract_deployment` | New contract deployed |

## 🔧 Configuration Reference

### Core Settings

```yaml
rpc_endpoint: "http://localhost:3333"  # RPC endpoint
chain_id: "ETH1"                       # Chain identifier
start_block: 0                          # Start from block (0 = latest)
polling_interval: 6s                    # Block polling frequency
confirmation_blocks: 0                  # Wait for N confirmations
```

### Streaming Options

```yaml
stream_transactions: true      # Stream all transactions
stream_internal_txs: false     # Stream internal transactions (requires debug API)
stream_logs: true              # Stream event logs
decode_events: true            # Decode events with ABIs
track_common_events: true      # Auto-recognize ERC20/ERC721
max_events_buffer_size: 10000  # Event buffer size
```

### WebSocket Server

```yaml
enable_websocket: true
websocket_addr: ":8080"
websocket_path: "/stream"
max_websocket_clients: 1000
websocket_ping_interval: 30s
```

### Webhooks

```yaml
enable_webhooks: true
webhook_workers: 10           # Concurrent webhook workers
webhook_queue_size: 10000     # Webhook queue size
webhook_timeout: 30s          # HTTP timeout
webhook_max_retries: 3        # Retry attempts
webhook_retry_delay: 1s       # Delay between retries
```

### Contract Watching

```yaml
watched_contracts:
  - name: "USDC"
    address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    start_block: 6082465
    stream_all_txs: true
    event_filter:
      - "0xddf252ad..."  # Transfer event signature
    abi: |                # Optional: custom ABI for decoding
      [
        {
          "name": "Transfer",
          "type": "event",
          "inputs": [...]
        }
      ]
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Lava Event Streamer                       │
│                                                             │
│  ┌──────────────┐   ┌───────────────┐   ┌──────────────┐  │
│  │   Event      │──▶│  Subscription │──▶│  WebSocket   │  │
│  │  Processor   │   │    Manager    │   │    Server    │  │
│  └──────┬───────┘   └───────┬───────┘   └──────────────┘  │
│         │                   │                              │
│         │                   │                              │
│         │           ┌───────▼────────┐                     │
│         │           │    Webhook     │                     │
│         │           │     Sender     │                     │
│         │           └────────────────┘                     │
│         │                                                  │
│    ┌────▼────┐                                             │
│    │   ABI   │                                             │
│    │ Decoder │                                             │
│    └─────────┘                                             │
│         ▲                                                  │
└─────────┼──────────────────────────────────────────────────┘
          │
   ┌──────▼───────┐
   │ RPC Client   │
   │              │
   │ • Retry      │
   │ • Backoff    │
   └──────┬───────┘
          │
   ┌──────▼────────┐
   │ Smart Router  │
   │  or EVM Node  │
   └───────────────┘
```

## 📡 API Endpoints

### WebSocket

```
ws://localhost:8080/stream
```

**Actions:**
- `subscribe` - Create subscription
- `unsubscribe` - Cancel subscription
- `ping` - Keep-alive ping

### HTTP

```bash
POST /subscribe          # Create subscription
POST /unsubscribe        # Cancel subscription
GET  /health            # Health check
```

## 🔒 Webhook Security

Webhooks include HMAC-SHA256 signatures:

```bash
# Verify webhook signature
signature=$(echo -n "$payload" | openssl dgst -sha256 -hmac "$secret" | cut -d' ' -f2)
if [ "$signature" == "$X_LAVA_SIGNATURE" ]; then
  echo "Valid webhook"
fi
```

## 🎯 Use Cases

### 1. Real-time DeFi Monitoring

```yaml
watched_contracts:
  - name: "Uniswap V2 Router"
    address: "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
    stream_all_txs: true
```

### 2. NFT Marketplace Tracking

```yaml
filters:
  event_types: ["nft_transfer", "nft_approval"]
  addresses: ["0x..."]  # Your marketplace contract
```

### 3. Token Transfer Notifications

```yaml
webhook:
  url: "https://myapp.com/webhook"
filters:
  event_types: ["token_transfer"]
  min_value: "1000000000000000000"  # 1 ETH
```

### 4. Contract Deployment Alerts

```yaml
filters:
  event_types: ["contract_deployment"]
```

## 📈 Performance

### Resource Usage
- **Memory**: ~100-500MB (no database)
- **CPU**: Low (event-driven)
- **Network**: Scales with event volume

### Throughput
- **Events/sec**: 10,000+ (depends on hardware)
- **WebSocket Clients**: 1,000+ concurrent
- **Webhook Workers**: Configurable (10-100)

## 🔄 vs. Traditional Indexer

| Feature | Streamer (This) | Indexer |
|---------|----------------|---------|
| Storage | None (stateless) | Database required |
| Latency | Real-time (<1s) | Variable |
| Historical Queries | ❌ No | ✅ Yes |
| Resource Usage | Very Low | High |
| Scalability | Excellent | Limited by DB |
| Use Case | Real-time events | Analytics, history |

## 🛠️ Development

### Running Tests

```bash
cd protocol/streamer
go test -v ./...
```

### Building

```bash
make install
```

## 📚 Examples

See `config/streamer_example.yml` for complete configuration examples.

## 🤝 Contributing

See [CODING_GUIDELINES.md](../../CODING_GUIDELINES.md).

## 📄 License

See [LICENSE.md](../../LICENSE.md).

## 🔗 Related

- [Smart Router](../rpcsmartrouter/README.md) - RPC routing with failover
- [RPC Consumer](../rpcconsumer/README.md) - Decentralized RPC consumer
- [Lava Protocol](https://docs.lavanet.xyz) - Full documentation


