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
examples use (`3360`–`3362`, `7779`); a config only binds the listeners it
declares, the rest sit idle.

| Port   | Used by                                         |
|--------|-------------------------------------------------|
| `3360` | ETH1 jsonrpc (eth, multichain)                  |
| `3361` | ARBITRUM jsonrpc (multichain)                   |
| `3362` | BASE jsonrpc (multichain)                       |
| `7779` | router prometheus metrics                       |

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

3 upstreams (`eth1.lava.build` + PublicNode + Tenderly), each HTTP + WS.

### `smartrouter_eth_cached.yml` — Ethereum with cache

Same as `smartrouter_eth.yml` plus `cache-be: cache:20100`. Run with
the cache overlay (see [Enabling the cache](#enabling-the-cache)).

### `smartrouter_multichain.yml` — Ethereum + Arbitrum + Base

Three JSON-RPC chains at once, 2 upstreams each (`<chain>.lava.build` + a
public RPC).

| Chain    | Port   | eth_chainId |
|----------|--------|-------------|
| Ethereum | `3360` | `0x1`       |
| Arbitrum | `3361` | `0xa4b1`    |
| Base     | `3362` | `0x2105`    |

Requires `specs/arbitrum.json` + `specs/base.json` (both import `ETH1` from
`specs/ethereum.json`), sourced from
[`Magma-Devs/lava-specs`](https://github.com/Magma-Devs/lava-specs).

> The ETH1 spec — which Arbitrum and Base import — requires websocket support,
> so every provider pairs an `http` url with a `wss` url. The `lava.build`
> gateways serve ws at the `/websocket` path.

### `smartrouter_multichain_cached.yml` — multi-chain with cache

Same as `smartrouter_multichain.yml` plus `cache-be: cache:20100`. Run with
the cache overlay (see [Enabling the cache](#enabling-the-cache)).

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
