# Local dev via docker compose

Minimal stack for iterating on the smart-router binary from this repo: routers +
cache + Traefik on a single docker network, HTTP-only, no observability. For
the full app stack (dashboard, Prometheus, Grafana, simulator, etc.) use the
[`smart-router-standalone`](../../smart-router-standalone/) repo's compose path.

## Quick start

```bash
# 1. Build the static binary into build/smartrouter and start the stack
./scripts/compose_up.sh

# 2. Hit the first router
curl -X POST http://localhost:3000 \
     -H 'content-type: application/json' \
     -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

# 3. Or via Traefik with hostname routing
curl -X POST http://eth-jsonrpc.localhost/ \
     -H 'content-type: application/json' \
     -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

# 4. Tear down
./scripts/compose_down.sh
```

After editing `cmd/` or `protocol/`, just re-run `./scripts/compose_up.sh` —
it rebuilds the binary (Go incremental cache makes it ~instant on warm builds)
and the docker image picks it up because `build/smartrouter` changed.

## Interface coverage

`config/values.yml` includes one router per supported smart-router interface so
the stack exercises every code path end-to-end:

| Router id        | Chain     | Interface       | Example tool / payload                                      |
|------------------|-----------|-----------------|-------------------------------------------------------------|
| `eth`            | ETH1      | `jsonrpc` (+wss)| `curl -X POST … eth_blockNumber` &nbsp;/&nbsp; `wscat -c ws://127.0.0.1/ws -H "Host: eth-jsonrpc.localhost"` |
| `lava-rest`      | LAVA      | `rest`          | `curl http://lava-rest-rest.localhost/<chain-specific-path>` |
| `lava-tm`        | LAVA      | `tendermintrpc` | `curl -X POST … {"method":"status"}`                        |
| `lava-grpc`      | LAVA      | `grpc`          | `grpcurl -plaintext localhost:<port> cosmos.base.tendermint.v1beta1.Service.GetLatestBlock` (LAVA is Cosmos-SDK based) — see "gRPC upstream caveat" below |

The wscat example is auto-emitted only when a router has at least one
`ws://` / `wss://` upstream URL. `./scripts/compose_up.sh` prints the full
cheat sheet (also written to `compose/usage.txt`).

### gRPC upstream caveat (binary-level, not compose)

`lava-grpc-router` reaches the spec-load + chain-id verification phases fine
(specs are bundled locally: `specs/{cosmoshub,cosmossdkv50,cosmoswasm}.json`),
but then crash-loops against `*.g.w.lavanet.xyz:443` gateways because
smart-router opens **up to 100 simultaneous TLS handshakes at startup**
(10 parallel connections × 10 retry attempts; see
[`protocol/chainlib/chainproxy/connector.go:194`](../protocol/chainlib/chainproxy/connector.go)
and [`protocol/lavasession/direct_rpc_connection.go:648`](../protocol/lavasession/direct_rpc_connection.go)).
The gateway closes the burst as a rate-limit response → the binary exits fatal
→ host port `:3003` never binds → `grpcurl localhost:3003 …` returns
"connection refused".

Direct `grpcurl <gateway>:443 …` works fine — only the parallel-startup burst
gets throttled.

The renderer output (`compose/usage.txt`), per-router config, and bundled
chain specs are all correct — verified by the renderer tests below. To exercise
this router end-to-end either patch the binary's connection-pool defaults
(linked above), or point at a gRPC endpoint that won't rate-limit the burst
(your own gateway, a self-hosted node). All other interfaces (`jsonrpc`,
`rest`, `tendermintrpc`) work out of the box.

## Renderer tests

The renderer has a Python unit-test suite covering each interface type and the
"all four interfaces present in values.yml" invariant:

```bash
python3 -m unittest scripts/test_render_compose.py
```

Run after editing `scripts/render_compose.py` or `config/values.yml`.

## Scripts

| Script | What |
|---|---|
| `./scripts/compose_up.sh` | Render `compose/*` from `config/values.yml`, build the thin image from `build/smartrouter`, start the stack. `--reinstall` drops volumes first; `--render-only` skips docker entirely |
| `./scripts/compose_down.sh` | Tear down. `--reinstall` also drops named volumes |

The image is `smart-router:local`. It comes from `compose/Dockerfile`, which is
just `COPY build/smartrouter /bin/smart-router` on top of distroless — the full
multi-stage Go-in-Docker build at `../Dockerfile` is reserved for CI.

## Port assignments

| Host port | Service |
|---|---|
| 80 | Traefik HTTP entrypoint — `http://<chain>-<iface>.localhost/` |
| 3000 | First router (index 0 in `config/values.yml`) — direct |
| 3001 | Second router — direct |
| 3002… | Additional routers — direct |
| 8090 | Traefik admin dashboard |

`*.localhost` resolves to `127.0.0.1` in browsers and `curl ≥ 7.77` per
RFC 6761 — no `/etc/hosts` edits needed.

## Editing config

`config/values.yml` is the source of truth. After editing:

```bash
./scripts/compose_up.sh --render-only   # regenerate compose/* from values, no docker
./scripts/compose_up.sh                 # render + (re-)start; only changed services recreate
./scripts/compose_up.sh --reinstall     # full reset (drops volumes/networks first)
```

Don't hand-edit `compose/docker-compose.yml`, `compose/configs/router-*.yml`,
`compose/traefik/dynamic.yml`, or `compose/usage.txt` — they're regenerated
and gitignored. Persistent local tweaks go in `compose/docker-compose.override.yml`
(auto-merged by compose, also gitignored).
