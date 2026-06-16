# Docker

The repo ships a multi-stage Dockerfile that produces a [distroless](https://github.com/GoogleContainerTools/distroless) image with the `smart-router` binary as the entrypoint.

Source: [`Dockerfile`](https://github.com/Magma-Devs/smart-router/blob/main/Dockerfile).

## Build

```bash
docker build -t smart-router:local .
```

Optional build args:

| Arg | Default | Purpose |
|---|---|---|
| `GO_VERSION` | `1.26` | Go toolchain version |
| `ALPINE_VERSION` | `3.22` | builder Alpine version |
| `RUNNER_IMAGE` | `gcr.io/distroless/static-debian12:debug` | final image base |
| `GIT_VERSION` | `dev` | embedded into the binary as `main.version` |
| `GIT_COMMIT` | `unknown` | embedded into the binary as `main.commit` |

For reproducible builds, pass real values:

```bash
docker build \
  --build-arg GIT_VERSION=$(git describe --tags --always) \
  --build-arg GIT_COMMIT=$(git rev-parse HEAD) \
  -t smart-router:$(git rev-parse --short HEAD) .
```

## Run

```bash
docker run --rm \
  -p 3360:3360 \
  -p 7779:7779 \
  -v "$(pwd)/config:/smart-router/config:ro" \
  -v "$(pwd)/specs:/smart-router/specs:ro" \
  smart-router:local \
  rpcsmartrouter \
  /smart-router/config/smartrouter_examples/smartrouter_eth.yml \
  --geolocation 1 \
  --use-static-spec /smart-router/specs/
```

Key points:

- `3360` is the request listener (override per endpoint in YAML).
- `7779` is the Prometheus metrics endpoint.
- Mount your config and `specs/` directory as read-only volumes.
- Pass the subcommand explicitly (`rpcsmartrouter`) — the entrypoint is the bare binary.

## With a shared cache

Run the cache server in its own container, then point the router at it.

```bash
docker network create smartrouter-net

docker run -d --name smartrouter-cache --network smartrouter-net \
  smart-router:local cache --port 7778

docker run --rm --network smartrouter-net \
  -p 3360:3360 -p 7779:7779 \
  -v "$(pwd)/config:/smart-router/config:ro" \
  -v "$(pwd)/specs:/smart-router/specs:ro" \
  smart-router:local \
  rpcsmartrouter /smart-router/config/.../smartrouter_eth.yml \
  --geolocation 1 \
  --use-static-spec /smart-router/specs/ \
  --cache-be smartrouter-cache:7778
```

## docker-compose

```yaml
services:
  cache:
    image: smart-router:local
    command: cache --port 7778
    expose:
      - "7778"

  router:
    image: smart-router:local
    depends_on: [cache]
    ports:
      - "3360:3360"   # router listener
      - "7779:7779"   # metrics
    volumes:
      - ./config:/smart-router/config:ro
      - ./specs:/smart-router/specs:ro
    command:
      - rpcsmartrouter
      - /smart-router/config/smartrouter_examples/smartrouter_eth.yml
      - --geolocation=1
      - --use-static-spec=/smart-router/specs/
      - --cache-be=cache:7778
      - --log_level=info
```

## Image hardening

The default `RUNNER_IMAGE` is `gcr.io/distroless/static-debian12:debug` — distroless with a debug shell. For production, override to the non-debug variant:

```bash
docker build \
  --build-arg RUNNER_IMAGE=gcr.io/distroless/static-debian12:nonroot \
  -t smart-router:prod .
```

The image runs as `nonroot` and contains only the binary, CA certificates, and `/tmp`.

## Common pitfalls

- **No subcommand passed** → the binary prints help and exits. Always include `rpcsmartrouter` (or `cache`).
- **Spec directory missing** → router fails at startup with "spec not found". Mount `specs/`.
- **Cache unreachable** → router runs without cache (warns). Confirm `--cache-be` resolves and the cache port is open.
