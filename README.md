<div align="center">

<a href="https://github.com/Magma-Devs/smart-router" target="_blank" rel="noopener noreferrer">
  <img
    src="./docs/assets/banner.png"
    alt="Smart Router — Centralised RPC routing gateway"
    width="100%"
    style="cursor: pointer;"
  >
</a>

# Smart Router

[![Build and Test](https://github.com/Magma-Devs/smart-router/actions/workflows/smartrouter.yml/badge.svg?branch=main)](https://github.com/Magma-Devs/smart-router/actions/workflows/smartrouter.yml)
[![Release](https://img.shields.io/badge/release-v1.0.0-brightgreen)](https://github.com/Magma-Devs/smart-router/releases/latest)
[![Go](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.md)

</div>

Centralised RPC routing gateway. Routes JSON-RPC, REST, gRPC, and Tendermint RPC requests to statically configured provider endpoints with QoS-based selection, caching, and automatic failover.

<div align="center">

[Quick Start](#quick-start) · [How it works](#how-it-works) · [Supported Chains](#supported-chains) · [Releases](#releases) · [Contributing](./CONTRIBUTING.md) · [Security](./SECURITY.md)

</div>

---

## What is smart router

Smart router is a reverse proxy specialised for blockchain RPC. Applications point at one stable endpoint; under the hood, it handles the provider multiplexing, RPC-aware retry, response caching, and observability that you'd otherwise rebuild in every service that touches RPC.

- **Multi-protocol** — JSON-RPC, REST, gRPC, and Tendermint RPC on the same router.
- **QoS-based provider selection** — picks the healthiest of N configured upstreams per request; flaky providers get backed off automatically.
- **RPC-aware retry + failover** — distinguishes transient failures, "block not yet produced" responses, and malformed envelopes; retries only the first.
- **Response caching** — caches what's safe to cache, keyed by method, params, and block height.
- **First-class observability** — Prometheus metrics fine-grained enough to see which provider is letting you down.

## Editions

Smart router ships in two editions from the same source tree:

- **Community** (default build) — JSON-RPC over HTTP/HTTPS, EVM-family specs. No license required.
- **Enterprise** (built with `-tags enterprise`) — adds REST, gRPC, Tendermint RPC, WebSocket subscriptions, and non-EVM specs. Requires a valid `license.key` from Magma at runtime; see [`ENTERPRISE_LICENSING.md`](ENTERPRISE_LICENSING.md) for the operator guide.

Both editions share the same routing core, QoS engine, and observability stack — the difference is which transports and spec types the binary accepts.

## Quick Start

The fastest way to start: install the binary, point it at a YAML config, run.

### Prerequisites

- [Go 1.26+](https://go.dev/dl/)

### Build & run

```bash
make install
smartrouter config/smartrouter_examples/smartrouter_lava.yml --geolocation 1 --use-static-spec specs/
```

After running, you get:

- An RPC endpoint per chain interface (ports from the YAML config; conventional default `:3360`).
- Prometheus metrics on `:7779`.
- A health endpoint at `/lava/health`.
- Provider rotation, RPC-aware retry, response caching, and metrics — all driven by the YAML config.

### Configuration

Provider endpoints are configured in a YAML file. See `config/smartrouter_examples/smartrouter_lava.yml` for an example targeting the Lava blockchain via PublicNode.

Setup scripts are available in `scripts/pre_setups/`:

```bash
# Lava blockchain (REST + gRPC + Tendermint RPC)
./scripts/pre_setups/init_smartrouter_lava.sh

# Ethereum (JSON-RPC)
./scripts/pre_setups/init_smartrouter_eth.sh
```

## How it works

```
   Clients  (JSON-RPC, REST, gRPC, Tendermint RPC)
                            │
                            ▼
   ┌──────────────────────────────────────────────────────────┐
   │                     Smart Router                         │
   │                                                          │
   │   per-interface listener  ─▶  cache check  ─▶  hit? ──┐  │
   │                                       │ miss          │  │
   │                                       ▼               │  │
   │                              QoS-weighted provider    │  │
   │                                   selection           │  │
   │                                       │               │  │
   │                                       ▼               │  │
   │                              relay + retry / failover │  │
   │                                       │               │  │
   │                                       ▼               │  │
   │   ◀─── response (+ metadata headers, metrics) ────────┘  │
   └────────────────────────────┬─────────────────────────────┘
                                │
                                ▼
            Statically-configured upstream RPC providers
              (Lava chain providers, PublicNode, Infura,
               self-hosted nodes, ...)
```

The hot path for a single request:

1. **Listen** — a per-interface listener (JSON-RPC, REST, gRPC, or Tendermint RPC) accepts the request and parses it into a normalised internal shape.
2. **Cache lookup** — for cacheable methods (historical block data, immutable receipts, etc.), the cache layer (`ecosystem/cache`) checks for a recent response. Hits return immediately.
3. **Provider selection** — on cache miss, the provider optimiser (`protocol/provideroptimizer`) picks an upstream from the configured pool using QoS-weighted scoring. Healthy/fast providers are preferred; flaky ones get backed off automatically.
4. **Relay + failover** — the request is sent to the chosen provider. On failure (timeout, malformed response, certain status codes), the retry state machine picks an alternate provider and retries within a configurable budget.
5. **Response** — returned to the client with metadata headers (`Smart-Router-Version`, `Lava-Provider-Address`, retry counts, etc.) annotating which provider served the response. Prometheus metrics are emitted in parallel.

### What it's not

- **Not a load balancer.** Generic L4/L7 balancers don't speak RPC. They can't distinguish a transient timeout (retry against another provider), "block not yet produced" (retrying anywhere won't help), and a malformed JSON-RPC envelope (return the error, don't retry). They can't cache by method+params, and they can't back off a provider that's silently serving stale block data while still returning `200 OK`. Smart router does all of these.
- **Not a node.** Smart router doesn't sync chain state or hold a block tree. It forwards requests to upstream providers (managed services or self-hosted nodes) configured statically via YAML and scores them on response quality. If every configured upstream goes dark, the router has nothing to fall back on — pair it with a provider set wide enough to survive operator-level outages.

## Supported Chains

Smart router ships with specs for **120 chain networks** — EVM L1s and L2s, Cosmos SDK chains, non-EVM L1s (Solana, Sui, TON, Starknet, NEAR, Aptos, Move, …), Bitcoin-family chains, Ethereum Beacon, and more. The **Index** column is the value to reference in your YAML config or `--chain-id`.

<details>
<summary>Full list (click to expand)</summary>

| Chain | Index | Interfaces |
|-------|-------|------------|
| Agoric Mainnet | AGR | gRPC, REST, Tendermint RPC |
| Agoric Testnet | AGRT | gRPC, REST, Tendermint RPC |
| Aptos Mainnet | APT1 | REST |
| Arbitrum Mainnet | ARBITRUM | JSON-RPC |
| Arbitrum Nova Testnet | ARBITRUMN | JSON-RPC |
| Arbitrum Sepolia Testnet | ARBITRUMS | JSON-RPC |
| Avalanche C Chain Mainnet | AVALANCHEC | JSON-RPC |
| Avalanche C Chain Testnet | AVALANCHECT | JSON-RPC |
| Avalanche Mainnet | AVAX | JSON-RPC |
| Avalanche P Chain Mainnet | AVALANCHEP | JSON-RPC |
| Avalanche P Chain Testnet | AVALANCHEPT | JSON-RPC |
| Avalanche Testnet | AVAXT | JSON-RPC |
| Axelar Mainnet | AXELAR | gRPC, REST, Tendermint RPC |
| Axelar Testnet | AXELART | gRPC, REST, Tendermint RPC |
| Base Mainnet | BASE | JSON-RPC |
| Base Sepolia Testnet | BASES | JSON-RPC |
| Berachain Artio Mainnet | BERA | JSON-RPC |
| Berachain Artio Testnet | BERAT | JSON-RPC |
| Berachain Bartio Testnet | BERAT2 | JSON-RPC |
| Bitcoin | BTC | JSON-RPC |
| Bitcoin Cash Mainnet | BCH | JSON-RPC |
| Bitcoin Cash Testnet | BCHT | JSON-RPC |
| Bitcoin Testnet | BTCT | JSON-RPC |
| Blast Mainnet | BLAST | JSON-RPC |
| Blast Sepolia Testnet | BLASTSP | JSON-RPC |
| BSC Mainnet | BSC | JSON-RPC |
| BSC Testnet | BSCT | JSON-RPC |
| Canto Mainnet | CANTO | gRPC, JSON-RPC, REST, Tendermint RPC |
| Cardano Mainnet | CARDANO | REST |
| Cardano Preprod Testnet | CARDANOT | REST |
| Casper Mainnet | CASPER | JSON-RPC |
| Casper Testnet | CASPERT | JSON-RPC |
| Celestia Arabica Testnet | CELESTIATA | gRPC, JSON-RPC, REST, Tendermint RPC |
| Celestia Mainnet | CELESTIA | gRPC, JSON-RPC, REST, Tendermint RPC |
| Celestia Mocha Testnet | CELESTIATM | gRPC, JSON-RPC, REST, Tendermint RPC |
| Celo Alfajores Testnet | ALFAJORES | JSON-RPC |
| Celo Mainnet | CELO | JSON-RPC |
| Cosmos Hub Mainnet | COSMOSHUB | gRPC, REST, Tendermint RPC |
| Cosmos Hub Testnet | COSMOSHUBT | gRPC, REST, Tendermint RPC |
| Dogecoin Mainnet | DOGE | JSON-RPC |
| Dogecoin Testnet | DOGET | JSON-RPC |
| Elys Testnet | ELYS | gRPC, REST, Tendermint RPC |
| Ethereum Beacon Mainnet | ETHBEACON | REST |
| Ethereum Mainnet | ETH1 | JSON-RPC |
| Ethereum Testnet Holesky | HOL1 | JSON-RPC |
| Ethereum Testnet Sepolia | SEP1 | JSON-RPC |
| Evmos Mainnet | EVMOS | gRPC, JSON-RPC, REST, Tendermint RPC |
| Evmos Testnet | EVMOST | gRPC, JSON-RPC, REST, Tendermint RPC |
| Fantom Mainnet | FTM250 | JSON-RPC |
| Fantom Testnet | FTM4002 | JSON-RPC |
| Filecoin Mainnet | FVM | JSON-RPC |
| Filecoin Testnet | FVMT | JSON-RPC |
| Fuel Network Graphql | FUELNETWORK | REST |
| Fuse Mainnet | FUSE | JSON-RPC |
| Fuse Testnet | SPARK | JSON-RPC |
| Hedera Mainnet | HEDERA | JSON-RPC |
| Hedera Testnet | HEDERAT | JSON-RPC |
| Hyperliquid Mainnet | HYPERLIQUID | JSON-RPC |
| Hyperliquid Testnet | HYPERLIQUIDT | JSON-RPC |
| Injective Mainnet | INJECTIVE | gRPC, REST, Tendermint RPC |
| Injective Testnet | INJECTIVET | gRPC, REST, Tendermint RPC |
| IOTA Mainnet | IOTA | JSON-RPC |
| IOTA Testnet | IOTAT | JSON-RPC |
| Juno Mainnet | JUN1 | gRPC, REST, Tendermint RPC |
| Juno Testnet | JUNT1 | gRPC, REST, Tendermint RPC |
| Kakarot Sepolia Testnet | KAKAROTT | JSON-RPC |
| Lava Mainnet | LAVA | gRPC, REST, Tendermint RPC |
| Lava Testnet | LAV1 | gRPC, REST, Tendermint RPC |
| Litecoin Mainnet | LTC | JSON-RPC |
| Litecoin Testnet | LTCT | JSON-RPC |
| Manta Pacific Mainnet | MANTAPACIFIC | JSON-RPC |
| Manta Pacific Testnet | MANTAPACIFICT | JSON-RPC |
| Mantle Testnet | MANTLE | JSON-RPC |
| Monad Mainnet | MONAD | JSON-RPC |
| Monad Testnet | MONADT | JSON-RPC |
| Moralis Advanced API | MORALIS | REST |
| Movement Mainnet | MOVEMENT | REST |
| Movement Testnet Bardock | MOVEMENTT | REST |
| Namada SE Testnet | NAMTSE | Tendermint RPC |
| NEAR Mainnet | NEAR | JSON-RPC |
| NEAR Testnet | NEART | JSON-RPC |
| Optimism Mainnet | OPTM | JSON-RPC |
| Optimism Sepolia Testnet | OPTMS | JSON-RPC |
| Osmosis Mainnet | OSMOSIS | gRPC, REST, Tendermint RPC |
| Osmosis Testnet | OSMOSIST | gRPC, REST, Tendermint RPC |
| Polkadot Asset Hub Mainnet | POLKADOTASSETHUB | JSON-RPC |
| Polygon Amoy Testnet | POLYGONA | JSON-RPC |
| Polygon Mainnet | POLYGON | JSON-RPC |
| Ripple Mainnet | XRP | JSON-RPC |
| Ripple Testnet | XRPT | JSON-RPC |
| Scroll Mainnet | SCROLL | JSON-RPC |
| Scroll Sepolia Testnet | SCROLLS | JSON-RPC |
| Secret Mainnet | SECRET | gRPC, REST, Tendermint RPC |
| Secret Testnet | SECRETP | gRPC, REST, Tendermint RPC |
| Side Testnet | SIDET | gRPC, REST, Tendermint RPC |
| Solana Mainnet | SOLANA | JSON-RPC |
| Sonic Blaze Testnet | SONICT | JSON-RPC |
| Sonic Mainnet | SONIC | JSON-RPC |
| Stargaze Mainnet | STRGZ | gRPC, REST, Tendermint RPC |
| Stargaze Testnet | STRGZT | gRPC, REST, Tendermint RPC |
| Starknet Mainnet | STRK | JSON-RPC |
| Starknet Sepolia Testnet | STRKS | JSON-RPC |
| Stellar Mainnet | XLM | JSON-RPC, REST |
| Stellar Testnet | XLMT | JSON-RPC, REST |
| Stride Mainnet | STRIDE | gRPC, REST, Tendermint RPC |
| Stride Testnet | STRIDET | gRPC, REST, Tendermint RPC |
| Subsquid-Powered Subgraph | SQDSUBGRAPH | REST |
| Sui Devnet | SUIT | JSON-RPC |
| Tezos Mainnet | TEZOS | REST |
| Tezos Shadownet Testnet | TEZOST | REST |
| TON Mainnet | TON | REST |
| TON Testnet | TONT | REST |
| Tron Mainnet | TRX | REST |
| Tron Shasta Testnet | TRXT | REST |
| Union Testnet | UNIONT | gRPC, REST, Tendermint RPC |
| Westend Asset Hub Testnet | POLKADOTASSETHUBT | JSON-RPC |
| Worldchain Mainnet | WORLDCHAIN | JSON-RPC |
| Worldchain Sepolia Testnet | WORLDCHAINS | JSON-RPC |
| zkSync Era Mainnet | ZKSYNC | JSON-RPC |
| zkSync Era Sepolia Testnet | ZKSYNCSP | JSON-RPC |

</details>

## Development

### Build targets

```bash
make build          # Build smartrouter binary to build/
make install        # Install smartrouter to $GOPATH/bin
make snapshot       # Reproduce a release locally in dist/ (binaries + multi-arch Docker image)
make setup          # One-time: ensure docker buildx is configured (auto-run by `make snapshot`)
make test           # Run all tests
make test-short     # Run smart router tests only
make lint           # Run go vet
make tidy           # Run go mod tidy
make clean          # Remove build artifacts
```

`make build` and `make install` inject the same version metadata via ldflags that CI uses (`git describe` for `Version`, `git rev-parse HEAD` for `Commit`), so a local build on a given commit is byte-identical to CI's for that commit (under the same Go toolchain).

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

The release is created as a **draft**. After CI completes, open the [Releases page](https://github.com/Magma-Devs/smart-router/releases), find the draft, and click **Publish release** to make it visible. Whether the release is marked as a **pre-release** is derived from the tag suffix: `vX.Y.Z` is a final release, `vX.Y.Z-rc1` / `-beta.2` / etc. are pre-releases. The draft gate is deliberate; flip the `draft` flag in `.goreleaser.yaml` to automate.

The `:latest` Docker tag only moves on final releases — RC and beta tags publish their per-version images but do not overwrite `ghcr.io/magma-devs/smart-router:latest`.

To re-run the release for an existing tag, go to GitHub → Actions → **Publish Smart Router Release** → **Run workflow**, passing the tag name as `release_tag`.

### Artifacts

A release publishes:

- **Four statically-linked binaries** attached to the GitHub Release: `smartrouter-vX.Y.Z-{linux,darwin}-{amd64,arm64}`, plus a `sha256sum.txt` checksum file.
- **A multi-arch Docker image** at `ghcr.io/magma-devs/smart-router:vX.Y.Z` for `linux/amd64` and `linux/arm64`.

The standalone Linux binaries and the binaries inside the Docker image are produced by the same `go build` invocation — same toolchain, same flags, byte-identical. GoReleaser owns the entire release-time build via the `dockers_v2:` block in `.goreleaser.yaml`.

#### Docker image tags

| Tag                   | Source                 | Stability                                                                |
| --------------------- | ---------------------- | ------------------------------------------------------------------------ |
| `:vX.Y.Z`             | release tag            | immutable, byte-identical to the standalone binary at that version       |
| `:latest`             | release tag            | floating — points at the most recent **final** release (not RC/beta)     |
| `:main`               | push to `main` branch  | floating — most recent dev build from `main`, **not** a release artifact |
| `:<branch>-<version>` | push to other branches | per-branch build for testing                                             |

Customers should pin to `:vX.Y.Z`. `:latest` is for non-production "just give me the newest stable" use; `:main` is for previewing unreleased work from `main`.

The version string is injected at build time from the git tag — `smartrouter version` prints the tag verbatim, including the `v` prefix. Builds from non-tagged commits carry `git describe` output (e.g. `v1.2.0-3-gabc1234`), so a dev binary cannot masquerade as a release.

#### Pulling the image (private repo)

This repository is private, so the GHCR package inherits private visibility. Anonymous `docker pull` returns `unauthorized`. To pull:

```bash
# Create a PAT at https://github.com/settings/tokens with at least the `read:packages` scope.
echo "<your-PAT>" | docker login ghcr.io -u <your-github-username> --password-stdin

docker pull ghcr.io/magma-devs/smart-router:vX.Y.Z
```

If you'd rather customers pull without authenticating, change the **package** visibility (not the repo) to public at `https://github.com/orgs/Magma-Devs/packages` → smart-router → Package settings → Change visibility. The Go source stays private; only the published images become public.

### Tag conventions

- Trigger pattern: `v[0-9]+.[0-9]+.[0-9]+*`. A tag like `1.2.0` (without the leading `v`) does _not_ fire the workflow.
- Follow [semver](https://semver.org/) `MAJOR.MINOR.PATCH`. For smart-router specifically:
  - **MAJOR** — breaking change to the wire surface customers integrate with: HTTP metadata headers (e.g. `Smart-Router-Version`, `Lava-Provider-Address`), JSON-RPC envelope shape, removed/renamed CLI flags, removed/renamed config fields.
  - **MINOR** — new capabilities: additional supported chains in `specs/`, new CLI flags, new metrics, new config fields with safe defaults, new HTTP metadata headers.
  - **PATCH** — internal-only changes: bug fixes, performance improvements, refactors, dependency bumps, docs.

## Security

For vulnerability reporting, see [SECURITY.md](SECURITY.md). Do **not** open public issues for security concerns.

## Community

- [Issues](https://github.com/Magma-Devs/smart-router/issues)
