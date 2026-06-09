# RPC Smart Router

The RPC Smart Router is a centralised RPC gateway that routes requests to pre-configured provider endpoints with QoS-based selection, caching, and automatic failover.

## Key Features

### Provider Configuration
- **Static Providers**: Define trusted RPC endpoints in YAML config
- **Backup Providers**: Automatic failover to backup tier when primaries fail
- **Multi-provider Support**: Mix Alchemy, Infura, self-hosted, and other providers

### Intelligent Routing
- **QoS-based Selection**: Routes to best-performing providers
- **Automatic Failover**: Seamlessly switches to backups on provider failure
- **Health Monitoring**: Continuous provider health checks
- **Strategy Options**: Balanced, latency, sync-freshness

### Features
- **Smart Caching**: Two-layer caching reduces provider load
- **Transaction Broadcasting**: Sends transactions to all providers for faster propagation
- **WebSocket Support**: Full support for subscription-based APIs
- **Metrics & Monitoring**: Prometheus metrics and health endpoints

## Configuration

Create a YAML config file (see `config/smartrouter_examples/smartrouter_lava.yml` for a full example):

```yaml
endpoints:
  - chain-id: ETH1
    api-interface: jsonrpc
    network-address: 0.0.0.0:3333

direct-rpc:
  - name: alchemy-primary
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY

  - name: infura-primary
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: https://mainnet.infura.io/v3/YOUR_KEY

backup-providers:
  - name: backup-alchemy
    chain-id: ETH1
    api-interface: jsonrpc
    node-urls:
      - url: https://eth-mainnet.g.alchemy.com/v2/BACKUP_KEY
```

## Usage

```bash
# Using standalone binary
smartrouter config.yml --geolocation 1 --use-static-spec specs/

# Using lavap
lavap rpcsmartrouter config.yml --geolocation 1 --use-static-spec specs/
```

### Common Flags

```bash
--geolocation 1                      # Geographic location code
--cache-be "127.0.0.1:7778"          # Enable caching
--strategy balanced                   # Provider selection strategy
--metrics-listen-address ":7779"     # Prometheus metrics
--log_level debug                    # Log verbosity
--concurrent-providers 3             # Max parallel provider attempts
```

### Usage telemetry (OTel)

Off by default. When enabled, the smart router emits two event types as
OTLP/HTTP logs to a host-local OpenTelemetry collector — `relay_usage` (one
per relay) and `optimizer_qos` (one per (chain, provider) per sampling tick).
The collector fans out to whatever backend(s) you choose (S3 / Kafka /
ClickHouse) via exporter YAML — no smart-router code change to swap
destinations. With `--usage-otel-enabled=false` the relay/QoS paths pay one
inlinable no-op call and nothing else.

```bash
--usage-otel-enabled                       # master switch (both event types); off by default
--usage-otel-endpoint "127.0.0.1:4318"     # OTLP/HTTP collector endpoint
--usage-otel-service-name "lava-rpcsmartrouter"
--usage-otel-service-instance-id "$HOSTNAME-eth"  # default: hostname-pid
```

## Architecture

```
User Request --> Smart Router --> Provider Selection (QoS-based)
                      |
                Try Primary Providers
                      |
               [If all fail] --> Try Backup Providers
                      |
               Cache Response (optional)
                      |
               Return to User
```

## Failover Flow

1. **Primary Attempt**: Tries direct-rpc providers first (best QoS selected)
2. **Failure Detection**: Detects errors, timeouts, or unavailability
3. **Automatic Failover**: Switches to backup providers transparently
4. **Recovery**: Monitors primary providers and switches back when healthy

## Monitoring

```bash
# Prometheus metrics
curl http://localhost:7779/metrics

# Health check
curl http://localhost:3333/lava/health
```
