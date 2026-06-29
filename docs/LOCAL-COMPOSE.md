# Local docker compose

One parameterized stack for running the smart-router binary locally from this
repo — no host Go toolchain, no Traefik, no code generation. The base compose
file (`docker/docker-compose.yml`) serves **every** example config; an optional
overlay (`docker/docker-compose.cache.yml`) adds the cache. There is no
per-config compose file.

Two axes of variation:

| Axis              | How                                                       | Default                          |
|-------------------|-----------------------------------------------------------|----------------------------------|
| Which example     | `SR_CONFIG=<path>`                                        | `smartrouter_eth.yml` (Ethereum) |
| Cache on/off      | a `*_cached.yml` config + the cache overlay (`-f …cache.yml`) | off (router only)            |

```bash
# Default: Ethereum example, no cache
docker compose -f docker/docker-compose.yml up --build

curl -X POST http://localhost:3360 \
     -H 'content-type: application/json' \
     -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

# Tear down
docker compose -f docker/docker-compose.yml down
```

## Choosing an example config

Set `SR_CONFIG` to any file under `config/smartrouter_examples/`:

```bash
# Multi-chain (Ethereum + Arbitrum + Base)
SR_CONFIG=config/smartrouter_examples/smartrouter_multichain.yml \
  docker compose -f docker/docker-compose.yml up --build
```

Adding a new example is just a new YAML in `config/smartrouter_examples/` —
no compose change. The file publishes the **superset** of ports the bundled
examples use (`3360`–`3367`, `7779`); a config only binds the listeners it
declares, the rest sit idle.

| Port   | Used by                                              |
|--------|-----------------------------------------------------|
| `3360` | ETH1 jsonrpc (eth, multichain)                      |
| `3361` | SOLANA jsonrpc (solana example uses 3360; multichain) |
| `3362` | BTC jsonrpc (bitcoin example uses 3360; multichain) |
| `3363` | HYPERLIQUID jsonrpc (hyperliquid example uses 3360; multichain) |
| `3364` | COSMOSHUB rest (cosmos example uses 3360; multichain) |
| `3365` | COSMOSHUB tendermintrpc (cosmos example uses 3362; multichain) |
| `3366` | COSMOSHUB grpc (cosmos example uses 3361; multichain) |
| `3367` | APT1 rest (aptos example uses 3360; multichain)     |
| `7779` | router prometheus metrics                           |

## Enabling the cache

The cache is the same binary's `cache` subcommand, run as a separate service
in an **overlay** compose file (`docker/docker-compose.cache.yml`) layered on
top of the base. The cache **address lives in the config file** (`cache-be:`),
so a cached scenario is a config that declares it — e.g.
`smartrouter_eth_cached.yml`. Run that config with the overlay (which starts
the cache service):

```bash
SR_CONFIG=config/smartrouter_examples/smartrouter_eth_cached.yml \
  docker compose -f docker/docker-compose.yml \
                 -f docker/docker-compose.cache.yml up --build
```

Confirm the cache is in the path (cache metrics on `:5555`):

```bash
curl -s http://localhost:5555/metrics | grep cache_total_hits
```

Without the overlay the cache service never starts. The base router command
intentionally does **not** pass `--cache-be` — an explicitly-passed flag (even
`""`) outranks the config-file value in viper, so it would silently defeat the
YAML `cache-be:`. To make any config cached, add `cache-be: "cache:20100"` to
it (see `smartrouter_eth_cached.yml`) and run it with the overlay.

## Example configs

### `smartrouter_eth.yml` — Ethereum (default)

2 upstreams (PublicNode + Tenderly), each HTTP + WS. No Lava endpoints.

### `smartrouter_eth_cached.yml` — Ethereum with cache

Same as `smartrouter_eth.yml` plus `cache-be: cache:20100`. Run with
the cache overlay (see [Enabling the cache](#enabling-the-cache)).

### `smartrouter_solana.yml` / `smartrouter_bitcoin.yml` / `smartrouter_hyperliquid.yml` / `smartrouter_aptos.yml`

Single-chain JSON-RPC (Solana, Bitcoin, Hyperliquid) or REST (Aptos) examples,
each on port `3360` and HTTP-only — run with `--skip-websocket-verification`
(the compose command already passes it). Upstreams are PublicNode and each
chain's official/community endpoint; no Lava endpoints.

### `smartrouter_cosmos.yml` — Cosmos Hub (REST + gRPC + Tendermint RPC)

Cosmos Hub across all three interfaces, two distinct public vendor groups each
(PublicNode + Polkachu). The `COSMOSHUB` spec imports `COSMOSSDK50` +
`COSMOSWASM` (which pull in `COSMOSSDK` → `IBC` + `TENDERMINT`); all ship in
`specs/`.

### `smartrouter_multichain.yml` — Ethereum + Solana + Bitcoin + Hyperliquid + Cosmos + Aptos

Every bundled example chain at once, each on its own port; two distinct public
vendor groups per `chain<>interface` where a second source exists.

| Chain       | Port   | Interface     | Identity        |
|-------------|--------|---------------|-----------------|
| Ethereum    | `3360` | jsonrpc       | `eth_chainId 0x1`   |
| Solana      | `3361` | jsonrpc       | mainnet-beta    |
| Bitcoin     | `3362` | jsonrpc       | mainnet         |
| Hyperliquid | `3363` | jsonrpc       | `eth_chainId 0x3e7` |
| Cosmos Hub  | `3364` | rest          | `cosmoshub-4`   |
| Cosmos Hub  | `3365` | tendermintrpc | `cosmoshub-4`   |
| Cosmos Hub  | `3366` | grpc          | `cosmoshub-4`   |
| Aptos       | `3367` | rest          | `chain_id 1`    |

Requires `specs/btc.json`, `specs/hyperliquid.json`, `specs/aptos.json`,
`specs/cosmoshub.json` (+ its `specs/cosmossdkv50.json` / `specs/cosmoswasm.json`
import closure), all sourced from
[`Magma-Devs/lava-specs`](https://github.com/Magma-Devs/lava-specs) and already
bundled in `specs/`.

> Only ETH1 requires websocket support (its providers pair an `http` url with a
> `wss` one). Solana, Bitcoin, Hyperliquid (jsonrpc) and Aptos (rest) are
> HTTP-only, so the compose command runs with `--skip-websocket-verification`.

### `smartrouter_multichain_cached.yml` — multi-chain with cache

A trimmed multi-chain fleet (Ethereum + Solana + Cosmos REST) plus
`cache-be: cache:20100`. Run with the cache overlay (see
[Enabling the cache](#enabling-the-cache)).

## Rebuilding after code changes

Re-run with `--build`; the Go build cache makes warm rebuilds fast:

```bash
docker compose -f docker/docker-compose.yml up --build
```

## Relationship to the other Docker files

- `docker/Dockerfile` — builds from source in-image (this stack only).
- `docker/Dockerfile.ci` / `docker/Dockerfile.release` — copy a **prebuilt**
  binary; used by CI and GoReleaser respectively.

## Logging & metrics

Tune logging with env vars:

- `SR_LOG_LEVEL` — `debug|info|warn|error` (default `info`)
- `SR_LOG_FORMAT` — `json|text` (default `json`; the first few bootstrap
  lines print as text before the format flag is applied — expected)

Prometheus metrics are enabled in the example configs themselves
(`metrics-listen-address: 0.0.0.0:7779`) and published on host port `7779`:

```bash
curl -s http://localhost:7779/metrics | head
```

Set `metrics-listen-address: disabled` in the config to turn it off, or
override with the `--metrics-listen-address` flag.
