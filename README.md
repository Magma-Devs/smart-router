<div align="center">

<a href="https://github.com/Magma-Devs/smart-router" target="_blank" rel="noopener noreferrer">
  <img
    src="./docs/assets/banner.png"
    alt="Smart Router — the reliability and security layer for blockchain RPC"
    width="100%"
    style="cursor: pointer;"
  >
</a>

# Smart Router

[![Build and Test](https://github.com/Magma-Devs/smart-router/actions/workflows/smartrouter.yml/badge.svg?branch=main)](https://github.com/Magma-Devs/smart-router/actions/workflows/smartrouter.yml)
[![Release](https://img.shields.io/badge/release-v1.0.6-brightgreen)](https://github.com/Magma-Devs/smart-router/releases/latest)
[![Go](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License](https://img.shields.io/badge/license-source--available-orange.svg)](LICENSE.md)

</div>

The reliability and security layer for blockchain RPC. Smart Router monitors and orchestrates multiple RPC upstreams in real-time, providing failover, cross-validation, caching, observability & more — across EVM and non-EVM networks.

<div align="center">

[Docs](https://docs.magmadevs.com) · [Quick Start](#quick-start) · [How it works](#how-it-works) · [Supported Chains](#supported-chains) · [Releases](#releases) · [License](#license) · [Contributing](./CONTRIBUTING.md) · [Security](./SECURITY.md)

</div>

---

## What is Smart Router

RPC proxies have been built in-house thousands of times — along with the failover, caching, and monitoring that surround them. Smart Router is that layer as a standard, actively maintained component: a single endpoint in front of all your RPC upstreams. Instead of building and maintaining your own, you get:

- **Automatic failover** — retries a bad upstream on another, hedges slow ones in parallel, skips out-of-sync upstreams, and trips a circuit breaker when the pool is exhausted.
- **Data cross-validation** — fans a read out to several upstreams and returns only once a quorum agrees, stopping conflicting or malicious responses before they reach you.
- **Block-aware caching** — serves repeat reads without hitting an upstream and without serving stale data; shareable across replicas.
- **Transaction broadcasting** — sends writes (`eth_sendRawTransaction` and equivalents) to all eligible upstreams in parallel to raise success rate and speed.
- **Multi-chain, multi-protocol** — JSON-RPC, REST, gRPC, Tendermint RPC, and WebSocket across EVM chains, Solana, UTXO chains, Cosmos chains, and more; chains are defined by JSON specs.
- **Built-in observability** — Prometheus metrics, OpenTelemetry traces, structured logs, and a typed error taxonomy, with a prebuilt dashboard.

## Quick Start

The fastest way to start: install the binary, point it at a YAML config, run.

### Prerequisites

- [Go 1.26+](https://go.dev/dl/)

### Build & run

```bash
make install
smartrouter config/smartrouter_examples/smartrouter_eth.yml --use-static-spec specs/
```

After running, you get:

- An RPC endpoint per chain interface (ports from the YAML config; conventional default `:3360`).
- Prometheus metrics on `:7779` — see [docs/METRICS.md](docs/METRICS.md) for the full reference.
- A health endpoint at `/lava/health`.
- Provider rotation, RPC-aware retry, response caching, and metrics — all driven by the YAML config.

### Config wizard

Don't want to hand-write the YAML? A Charm-based TUI builds a smartrouter config
and runs the local docker compose stack — from "which chains?" to a running,
health-verified router.

```bash
make wizard          # from the repo root (builds the router, then launches)
# or
cd tools/wizard && go run . --repo /path/to/smart-router
```

See [tools/wizard/README.md](tools/wizard/README.md) for the full walkthrough.

### Health check (`smartrouter health`)

A spec-driven, one-shot diagnostic that crafts and sends the relays each spec defines to every
configured upstream node URL, then prints a single JSON report to stdout. It's **chain-agnostic** —
it relies entirely on the loaded specs, so any chain or interface with a spec works out of the box
with no per-chain code. For each node URL it runs the standard latest-block call plus every
verification the spec declares for that node's `addons`/`extensions` (archive / debug / trace and,
when the spec supports subscriptions, a websocket check on `wss://` URLs).

```bash
# Probe every node-url under direct-rpc in a config file
smartrouter health config/smartrouter_examples/smartrouter_eth.yml --use-static-spec specs/

# Or probe an ad-hoc endpoint inline (address chain-id api-interface)
smartrouter health https://ethereum-rpc.publicnode.com ETH1 jsonrpc --use-static-spec specs/
```

The report is the only thing on **stdout** (all logs go to stderr), so it pipes cleanly into `jq`
or a downstream verifier:

```bash
smartrouter health smartrouter_eth.yml --use-static-spec specs/ 2>/dev/null | jq '.results[] | {name, url, ok}'
```

The document is a uniform envelope — consumers always `JSON.parse` stdout and read `.ok` / `.error` /
`.results`; **they never inspect the exit code**, which is `0` for any completed run (endpoint
failures are reported as data, not as a non-zero exit). Only a fatal setup error (bad config,
missing `--use-static-spec`) exits non-zero, and even then a JSON envelope with a populated `error`
is printed first.

```json
{
  "ok": false,
  "error": null,
  "results": [
    {
      "name": "eth-publicnode",
      "chainId": "ETH1",
      "apiInterface": "jsonrpc",
      "url": "wss://ethereum-rpc.publicnode.com",
      "transport": "ws",
      "addons": ["debug"],
      "extensions": ["archive"],
      "specValid": true,
      "latestBlock": 25374584,
      "ok": false,
      "verifications": [
        { "name": "chain-id", "addon": "",      "extension": "",        "ok": true },
        { "name": "archive",  "addon": "",      "extension": "archive", "ok": false, "error": "block not found" }
      ]
    }
  ]
}
```

An upstream with multiple node URLs (e.g. an `https://` and a `wss://` endpoint) yields **one row per
URL**, distinguished by `url`/`transport`. An endpoint's `ok` is `true` only when the spec loaded
(`specValid`) **and** every verification passed. Top-level `ok` is the AND of all rows.

| Flag | Default | Purpose |
| --- | --- | --- |
| `--use-static-spec` | (required) | Spec source(s) — file, directory, or remote URL (same paths as the main command) |
| `--include-backup` | `false` | Also probe upstreams under `backup-direct-rpc` |
| `--timeout` | `30s` | Per-upstream timeout, and the basis for the global wall-clock cap (upstreams probe concurrently; the run never exceeds `timeout + 5s`). A slow/blocked node aborts instead of hanging |
| `--skip-websocket-verification` | `false` | Exclude `ws://`/`wss://` endpoints and the spec's websocket verification (see note) |
| `--log-level` | `info` | Log verbosity (written to stderr) |

> **Websocket is verified by default.** For any chain whose spec supports subscriptions, the command
> probes the configured `ws://`/`wss://` endpoints and runs the spec's websocket verification — so the
> health check exercises the full surface a supported chain exposes. A blocked or slow ws node can't
> stall the run: each upstream is bounded by `--timeout`, upstreams probe concurrently, and a global
> wall-clock cap (`timeout + 5s`) guarantees the command returns even if a connector wedges (an upstream
> that doesn't finish in time is reported as a timed-out row). Pass `--skip-websocket-verification` to
> exclude ws endpoints — useful for a fast HTTP-only sanity check; each excluded URL is then reported as
> a row marked `"websocket verification skipped"`.

### Run with Docker Compose

No host Go toolchain needed — build and run the binary (plus an optional cache) in Docker:

```bash
docker compose -f docker/docker-compose.yml up --build
```

A single parameterized stack serves every example config (`SR_CONFIG=…`), with the cache added by layering an overlay compose file. See [`docs/LOCAL-COMPOSE.md`](docs/LOCAL-COMPOSE.md) for the full guide — config switching, the cache overlay, multi-chain examples, and logging/metrics.

#### With the monitoring dashboard

To bring up the router together with Prometheus and the [Smart Router Dashboard](https://github.com/Magma-Devs/smart-router-dashboard) (pre-built GHCR images), use the dashboard compose file:

```bash
docker compose -f docker/docker-compose.dashboard.yml up
```

This starts the router, a Prometheus that scrapes its `:7779` metrics, and the dashboard backend + frontend — fully self-contained, no dashboard source checkout needed. The UI is at http://localhost:3000 and Prometheus at http://localhost:9090.

The dashboard is protected by HTTP basic auth. The **default credentials are `admin` / `password`** — override them (and the image tag / router config) via environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DASHBOARD_USERNAME` | `admin` | Dashboard login username |
| `DASHBOARD_PASSWORD` | `password` | Dashboard login password |
| `DASHBOARD_TAG` | `latest` | Dashboard backend/frontend image tag |
| `SR_CONFIG` | `config/smartrouter_examples/smartrouter_eth.yml` | Router config (mounted into the dashboard too) |

```bash
DASHBOARD_USERNAME=ops DASHBOARD_PASSWORD='change-me' \
  docker compose -f docker/docker-compose.dashboard.yml up
```

The compose sets `NEXT_PUBLIC_LOCAL_MODE=true`, so the dashboard's live-test panel targets each chain directly at `http://localhost:<port>` (the port from `SR_CONFIG`) instead of the production gateway's `<chain>-<interface>.<domain>` URLs. The generated `curl` commands work as-is against the local stack — e.g. `curl -X POST -H "Content-Type: application/json" http://localhost:3360 -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'`.

> The `admin` / `password` default is for local use only — set `DASHBOARD_PASSWORD` to a real secret for any non-local deployment.

## AI / Codex setup

Using Codex or another AI coding agent to run Smart Router locally?

See [AGENTS.md](./AGENTS.md).

### Configuration

Upstream endpoints are configured in a YAML file. See `config/smartrouter_examples/smartrouter_cosmos.yml` for an example targeting Cosmos Hub with two distinct public sources per interface (REST + gRPC + Tendermint RPC), and `config/smartrouter_examples/smartrouter_multichain_cross_validation.yml` for a multi-chain fleet with an active [cross-validation](docs/CROSS-VALIDATION.md) policy block. Every bundled example points at public RPC vendors (PublicNode and each chain's official/community endpoints) — no API key required.

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
   │                              QoS-weighted upstream    │  │
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
3. **Upstream selection** — on cache miss, the upstream optimiser (`protocol/provideroptimizer`) picks an upstream from the configured pool using QoS-weighted scoring. Healthy/fast upstreams are preferred; flaky ones get backed off automatically.
4. **Relay + failover** — the request is sent to the chosen upstream. On failure (timeout, malformed response, certain status codes), the retry state machine picks an alternate upstream and retries within a configurable budget.
5. **Response** — returned to the client with metadata headers (`Smart-Router-Version`, `Lava-Provider-Address`, retry counts, etc.) annotating which upstream served the response. Prometheus metrics are emitted in parallel.

**Cross-validation (optional).** For read methods that warrant extra assurance, the relay step can instead fan out to several upstreams in parallel and only return an answer once a quorum agree on an identical response — optionally requiring the quorum to span multiple distinct upstream groups (or each group to reach its own quorum). It defends against a single upstream returning a wrong-but-well-formed answer, and surfaces dissent via response headers and a bounded mismatch metric. See [`docs/CROSS-VALIDATION.md`](docs/CROSS-VALIDATION.md) for an operator setup guide with runnable example configs, or [`protocol/rpcsmartrouter/README.md`](protocol/rpcsmartrouter/README.md#cross-validation) for the full knob/header reference.

### What it's not

- **Not a load balancer.** Generic L4/L7 balancers don't speak RPC. They can't distinguish a transient timeout (retry against another upstream), "block not yet produced" (retrying anywhere won't help), and a malformed JSON-RPC envelope (return the error, don't retry). They can't cache by method+params, and they can't back off an upstream that's silently serving stale block data while still returning `200 OK`. Smart Router does all of these.
- **Not a node.** Smart Router doesn't sync chain state or hold a block tree. It forwards requests to upstreams (managed services or self-hosted nodes) configured statically via YAML and scores them on response quality. If every configured upstream goes dark, the router has nothing to fall back on — pair it with an upstream set wide enough to survive operator-level outages.

## Supported Chains

Smart Router ships with specs for **120+ chain networks** — EVM L1s and L2s, Cosmos SDK chains, non-EVM L1s (Solana, Sui, TON, Starknet, NEAR, Aptos, Move, …), Bitcoin-family chains, Ethereum Beacon, and more. The **Index** column is the value to reference in your YAML config or `--chain-id`.

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
cmd/smartrouter/    — Standalone Smart Router binary
protocol/           — Core protocol implementation
  chainlib/         — Chain-specific parsers and proxies
  rpcsmartrouter/   — Smart router server and relay logic
  lavasession/      — Session and connection management
  provideroptimizer/ — QoS-based upstream selection
  relaycore/        — Relay processing pipeline
  metrics/          — Prometheus metrics
types/              — Shared type definitions
specs/              — Chain specification JSON files
```

### Debug endpoints

When started with `--debug-address <addr>` (and `devMode.enabled=true`), the router serves a small reset HTTP API for integration tests. It is **off by default and absent from production builds**.

The reset tests rely on is **`POST /debug/reset-all`**, which drains the router's internal state stores (optimizer scores, Ristretto, seen-block caches, retry bans, session-manager state) and — so a single call returns the router to a serving state after an all-providers-down stress burst — also **re-enables endpoint health and cold-rebuilds pairing**.

Why that matters: an endpoint that hits `MaxConsecutiveConnectionAttempts` consecutive connection failures is disabled (`Endpoint.Enabled=false`), and the only paths back are a successful relay or the ~15-minute epoch tick. After a stress test drives every provider down, those endpoints stay disabled and contaminate later tests until a pod restart. `reset-all` now re-enables them (mirroring the reset onto the Prometheus health gauge) and re-admits demoted providers via a cold `rebuildPairingFromConfig` (no re-probing). Every existing `reset-all` caller inherits this — no test migration.

For tests that only need the endpoint-health reset, **`POST /debug/reset-endpoint-health`** does exactly that and nothing else; its name mirrors `Endpoint.ResetHealth()` in the source. Response (any method other than `POST` returns 405):

```json
{"reset": true, "endpoints_reenabled": 3}
```

`/debug/reset-pairing` and `/debug/reset-scores` remain available for targeted cleanup.

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

#### Pulling the image

Smart Router Docker images are published to GHCR:

```bash
docker pull ghcr.io/magma-devs/smart-router:vX.Y.Z
```

### Tag conventions

- Trigger pattern: `v[0-9]+.[0-9]+.[0-9]+*`. A tag like `1.2.0` (without the leading `v`) does _not_ fire the workflow.
- Follow [semver](https://semver.org/) `MAJOR.MINOR.PATCH`. For smart-router specifically:
  - **MAJOR** — breaking change to the wire surface customers integrate with: HTTP metadata headers (e.g. `Smart-Router-Version`, `Lava-Provider-Address`), JSON-RPC envelope shape, removed/renamed CLI flags, removed/renamed config fields.
  - **MINOR** — new capabilities: additional supported chains in `specs/`, new CLI flags, new metrics, new config fields with safe defaults, new HTTP metadata headers.
  - **PATCH** — internal-only changes: bug fixes, performance improvements, refactors, dependency bumps, docs.

## Security

For vulnerability reporting, see [SECURITY.md](SECURITY.md). Do **not** open public issues for security concerns.

## License

Smart Router is **dual-licensed**. Noncommercial use is free under the [PolyForm Noncommercial License 1.0.0](LICENSE.md), including personal, educational, research, evaluation, development, and testing use. Commercial use requires a separate written Enterprise License from Magma Devs. See [LICENSING.md](LICENSING.md) for the dual-license summary and the full commercial terms.

Any commercial use, production use by or for a commercial entity, hosted/SaaS or managed-service use offered to customers or other third parties as part of a commercial product or service, resale, redistribution as part of a commercial offering, use as part of a paid product, or use of premium/enterprise features requires a separate written Enterprise License from Magma Devs. For Enterprise licensing, contact Magma Devs at [sales@magmadevs.com](mailto:sales@magmadevs.com).

**Enterprise** — production support, SLAs, and custom features beyond the open edition are available under that license. [Talk to us](https://magmadevs.com/contact).

## Community

- [Issues](https://github.com/Magma-Devs/smart-router/issues)
