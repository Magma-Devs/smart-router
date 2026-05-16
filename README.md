# Smart Router

Centralised RPC routing gateway. Routes JSON-RPC, REST, gRPC, and Tendermint RPC requests to statically configured provider endpoints with QoS-based selection, caching, and automatic failover.

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

## Releases

Releases are cut by pushing a semver tag matching `vX.Y.Z` (optionally with a pre-release suffix, e.g. `v1.2.0-rc1`). Pushing the tag triggers the release workflow, which builds the binaries, publishes a multi-arch Docker image to `ghcr.io/magma-devs/smart-router`, and creates the corresponding entry on the [Releases page](https://github.com/Magma-Devs/smart-router/releases).

To cut a new version:

```bash
git checkout main
git pull
git tag v1.2.0
git push origin v1.2.0
```

Alternatively, an existing tag can be released manually from GitHub → Actions → **Publish Smart Router Release** → *Run workflow*, passing the tag name as `release_tag`.

## Community

- [Issues](https://github.com/magma-Devs/smart-router/issues)
