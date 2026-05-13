# Smart Router

Centralised RPC routing gateway. Routes JSON-RPC, REST, gRPC, and Tendermint RPC requests to statically configured provider endpoints with QoS-based selection, caching, and automatic failover...

## Quick Start

### Prerequisites

- [Go 1.26+](https://go.dev/dl/)

### Build

```bash
make install-all
```

This installs two binaries:
- `smartrouter` — the main smart router binary
- `lavap` — alias exposing the same router subcommands (`rpcsmartrouter`, `cache`, `test`) for compatibility with existing tooling

### Run

```bash
smartrouter config/smartrouter_examples/smartrouter_lava.yml --geolocation 1 --use-static-spec specs/
```

### Configuration

Provider endpoints are configured in a YAML file. See `config/smartrouter_examples/smartrouter_lava.yml` for an example targeting the Lava blockchain via PublicNode.

Setup scripts are available in `scripts/pre_setups/`:

```bash
# Lava blockchain (REST + gRPC + Tendermint RPC)
./scripts/pre_setups/init_smartrouter_lava.sh

# Ethereum (JSON-RPC)
./scripts/pre_setups/init_smartrouter_eth.sh
```

## Supported Chains

Specs are in the `specs/` directory:

| Chain | Index | Interfaces |
|-------|-------|------------|
| Lava | LAVA | REST, gRPC, TendermintRPC |
| Ethereum | ETH1 | JSON-RPC |

## Development

### Build targets

```bash
make build          # Build smartrouter binary to build/
make build-all      # Build all binaries to build/
make test           # Run all tests
make test-short     # Run smart router tests only
make lint           # Run go vet
make tidy           # Run go mod tidy
make clean          # Remove build artifacts
```

### Project structure

```
cmd/smartrouter/    — Standalone smart router binary
cmd/lavap/          — Alias binary exposing the router subcommands
protocol/           — Core protocol implementation
  chainlib/         — Chain-specific parsers and proxies
  rpcsmartrouter/   — Smart router server and relay logic
  lavasession/      — Session and connection management
  provideroptimizer/ — QoS-based provider selection
  relaycore/        — Relay processing pipeline
  metrics/          — Prometheus metrics
types/              — Shared type definitions
specs/              — Chain specification JSON files
```

## Community

- [Issues](https://github.com/magma-Devs/smart-router/issues)
