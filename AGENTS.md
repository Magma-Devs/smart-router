# AGENTS.md

Fast operational guide for Codex, Claude Code, Cursor, and other AI coding
agents working in this repository.

This file is not a replacement for the public docs. Its job is to get a local
Smart Router instance running quickly, verify that it is healthy, and avoid
common agent mistakes.

## Source of truth

Use both sources:

- Public docs: https://docs.magmadevs.com/
- Repository files, especially:
  - `README.md`
  - `docs/LOCAL-COMPOSE.md`
  - `docs/METRICS.md`
  - `docker/docker-compose.yml`
  - `config/smartrouter_examples/`

If public docs and the repository disagree, prefer the checked-in repository
files for local commands and ports. Mention the mismatch in your final answer
instead of silently choosing one.

Known mismatch to watch for: some docs pages may still list older local example
ports. The current Docker Compose file publishes `3360-3367` and `7779`.

## Fast local setup with Docker Compose

The verified local command is:

```bash
docker compose -f docker/docker-compose.yml up --build
```

The default config is `config/smartrouter_examples/smartrouter_eth.yml`, which
starts the Ethereum JSON-RPC listener on `localhost:3360` and metrics on
`localhost:7779`.

For agent automation, detached mode is also fine:

```bash
docker compose -f docker/docker-compose.yml up --build -d
docker compose -f docker/docker-compose.yml logs -f router
```

To run another bundled example, set `SR_CONFIG` instead of editing the compose
file:

```bash
SR_CONFIG=config/smartrouter_examples/smartrouter_multichain.yml \
  docker compose -f docker/docker-compose.yml up --build
```

## Prerequisites

- Docker with the Compose plugin (`docker compose`, not legacy
  `docker-compose`).
- Network access to pull base images and to reach the public upstream RPC nodes
  used by the example configs.
- Free local ports for the selected config. The base compose file publishes
  `3360-3367` and `7779`.

No host Go toolchain is required for the Docker Compose flow.

## Docker readiness checks

Run these before changing code if Docker behavior is part of the task:

```bash
command -v docker
docker version
docker compose version
docker info >/dev/null
```

If `command -v docker` fails, Docker is not installed or is not in `PATH`.

If `docker info` fails while `docker version` finds a client, Docker Desktop may
be installed but the daemon is not running. Start Docker Desktop and retry.

Check for port collisions:

```bash
lsof -iTCP:3360-3367 -sTCP:LISTEN
lsof -iTCP:7779 -sTCP:LISTEN
```

## Health verification

After the compose stack starts, verify the final healthy state with these
commands.

Metrics health:

```bash
curl -i http://localhost:7779/metrics/overall-health
```

Expected success:

```text
HTTP/1.1 200 OK
Health status OK
```

Router listener health:

```bash
curl -i http://localhost:3360/lava/health
```

Expected success:

```text
HTTP/1.1 200 OK
Health status OK
```

Ethereum JSON-RPC smoke test:

```bash
curl -sS -X POST http://localhost:3360 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'
```

Expected success: HTTP 200 and a JSON-RPC response with a real hex block number,
for example:

```json
{"jsonrpc":"2.0","id":1,"result":"0x123abc"}
```

Container health command:

```bash
docker compose -f docker/docker-compose.yml exec -T router \
  smart-router health config/smartrouter_examples/smartrouter_eth.yml \
  --use-static-spec specs/
```

Expected success: the JSON output has top-level `"ok": true`. If you started the
router with a different `SR_CONFIG`, pass that same config path to the health
command.

Useful status command:

```bash
docker compose -f docker/docker-compose.yml ps
```

## Ports

The current base compose file publishes this superset of local ports. A given
config only binds the listeners it declares; unused published ports may sit idle.

| Port | Current local compose use |
| --- | --- |
| `3360` | ETH1 JSON-RPC; also used by several single-chain examples |
| `3361` | SOLANA JSON-RPC in multichain |
| `3362` | BTC JSON-RPC in multichain |
| `3363` | HYPERLIQUID JSON-RPC in multichain |
| `3364` | COSMOSHUB REST in multichain |
| `3365` | COSMOSHUB Tendermint RPC in multichain |
| `3366` | COSMOSHUB gRPC in multichain |
| `3367` | APT1 REST in multichain |
| `7779` | Smart Router Prometheus metrics |

## Dashboard

The observability dashboard lives in its own repo,
[Magma-Devs/smart-router-dashboard](https://github.com/Magma-Devs/smart-router-dashboard),
and ships a self-contained stack (router + Prometheus + dashboard). It is **not**
part of this repo — run it from there:

```bash
git clone https://github.com/Magma-Devs/smart-router-dashboard
cd smart-router-dashboard && make up
```

See that repo's README for configuration (auth, values file, logs, and pointing
it at an already-running router on `:7779`).

## Cleanup

Router-only stack:

```bash
docker compose -f docker/docker-compose.yml down
```

Router plus cache overlay:

```bash
docker compose -f docker/docker-compose.yml \
  -f docker/docker-compose.cache.yml down
```

To also remove compose-created volumes for the stack you are stopping:

```bash
docker compose -f docker/docker-compose.yml down --volumes --remove-orphans
```

Avoid broad cleanup commands such as `docker system prune` unless the user
explicitly asks for them.

## Security rules

- Do not add secrets, API keys, private RPC URLs, tokens, or rendered secret
  configs to the repository.
- Use environment placeholders such as `${RPC_KEY_ETH}` or `auth-config` for
  credentials.
- Keep generated local configs under gitignored local paths when they contain
  secrets.
- The router listens over HTTP locally. Put TLS, client authentication, and rate
  limiting in a reverse proxy for non-local deployments.
- Do not expose the local dashboard with default `admin` / `password`
  credentials.

## Agent behavior rules

- Prefer the Docker Compose flow above for local validation unless the user asks
  for native binary work.
- Do not edit compose files just to select a different example config; use
  `SR_CONFIG`.
- Do not "fix" unrelated files or clean up untracked files unless the user asks.
- Preserve user changes in the working tree.
- If Docker is unavailable or the daemon is stopped, report the exact failed
  readiness check and do not pretend the stack was verified.
- If docs and repo conflict, use the safest current repo instruction and call
  out the conflict.
- Keep commands copy-pasteable and avoid machine-specific absolute paths in
  docs.

## Done definition

A local run is healthy when all of these are true:

- `docker compose -f docker/docker-compose.yml up --build` starts the router.
- The compose stack exposes `3360-3367` and `7779`.
- `curl -i http://localhost:7779/metrics/overall-health` returns `200` with
  `Health status OK`.
- `curl -i http://localhost:3360/lava/health` returns `200` with
  `Health status OK`.
- `eth_blockNumber` against `http://localhost:3360` returns HTTP 200 and a real
  block number.
- `smart-router health ...` inside the container reports top-level `"ok": true`.
- No secrets or unrelated file changes were introduced.
