# Smart Router

Centralised RPC routing gateway. Routes JSON-RPC, REST, gRPC, and Tendermint RPC requests to statically configured provider endpoints with QoS-based selection, caching, and automatic failover.

## Quick Start

### Prerequisites

- [Go 1.26+](https://go.dev/dl/)

### Build

```bash
make install-all
```

This installs the `smartrouter` binary.

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

Releases are cut by pushing a semver tag matching `vX.Y.Z` (pre-release suffixes like `v1.2.0-rc1` are allowed). The tag push triggers `.github/workflows/release.yml`, which builds the release artifacts.

```bash
git checkout main
git pull
git tag v1.2.0
git push origin v1.2.0
```

The release is created as a **draft pre-release**. After CI completes, open the [Releases page](https://github.com/Magma-Devs/smart-router/releases), find the draft, and click **Publish release** to make it visible. The draft gate is deliberate; flip the `draft` / `prerelease` flags in `.goreleaser.yaml` to automate.

To re-run the release for an existing tag, go to GitHub → Actions → **Publish Smart Router Release** → **Run workflow**, passing the tag name as `release_tag`.

### Artifacts

A release publishes:

- **Four statically-linked binaries** attached to the GitHub Release: `smartrouter-vX.Y.Z-{linux,darwin}-{amd64,arm64}`, plus a `sha256sum.txt` checksum file.
- **A multi-arch Docker image** at `ghcr.io/magma-devs/smart-router:vX.Y.Z` for `linux/amd64` and `linux/arm64`.

The standalone Linux binaries and the binaries inside the Docker image are produced by the same `go build` invocation — same toolchain, same flags, byte-identical. GoReleaser owns the entire release-time build via the `dockers:` and `docker_manifests:` blocks in `.goreleaser.yaml`.

The version string is injected at build time from the git tag — `smartrouter version` prints the tag verbatim, including the `v` prefix. Builds from non-tagged commits carry `git describe` output (e.g. `v1.2.0-3-gabc1234`), so a dev binary cannot masquerade as a release.

### Reproducing a release locally

Install [GoReleaser](https://goreleaser.com/install/) and Docker, then run:

```bash
make snapshot   # shortcut for: goreleaser release --snapshot --clean --skip=publish
```

This produces every release artifact — the four binaries, the multi-arch Docker image, and the checksum file — under `dist/`, without pushing anything to GitHub or GHCR. Because the release pipeline runs a single `go build` per arch (via `.goreleaser.yaml`'s `builds:` block) and feeds that binary into both the standalone archive and the Docker image, a local snapshot build produces the same bytes CI would for the same commit.

### Tag conventions

- Trigger pattern: `v[0-9]+.[0-9]+.[0-9]+*`. A tag like `1.2.0` (without the leading `v`) does *not* fire the workflow.
- Follow [semver](https://semver.org/) `MAJOR.MINOR.PATCH`. For smart-router specifically:
  - **MAJOR** — breaking change to the wire surface customers integrate with: HTTP metadata headers (e.g. `Smart-Router-Version`, `Lava-Provider-Address`), JSON-RPC envelope shape, removed/renamed CLI flags, removed/renamed config fields.
  - **MINOR** — new capabilities: additional supported chains in `specs/`, new CLI flags, new metrics, new config fields with safe defaults, new HTTP metadata headers.
  - **PATCH** — internal-only changes: bug fixes, performance improvements, refactors, dependency bumps, docs.

## Community

- [Issues](https://github.com/magma-Devs/smart-router/issues)
