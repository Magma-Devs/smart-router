# Configuration

Smart Router is driven by:

1. A single YAML file that defines listeners (endpoints) and upstream provider URLs.
2. A directory of JSON **chain specs** describing methods, categories, and parser rules.

```bash
smartrouter path/to/config.yml --use-static-spec specs/
```

Working examples ship under [`config/smartrouter_examples/`](https://github.com/Magma-Devs/smart-router/tree/main/config/smartrouter_examples). The fastest path to a real config is to copy one and edit it.

## YAML shape

A config file is two top-level lists:

| Section | What it controls |
|---|---|
| `endpoints` | The listeners Smart Router opens — one entry per chain × API interface (e.g. Ethereum JSON-RPC on `:3360`). |
| `direct-rpc` | The upstream provider URLs for each chain × interface, with per-upstream timeout, TLS, and capability flags. |

See the [Lava example](https://github.com/Magma-Devs/smart-router/blob/main/config/smartrouter_examples/smartrouter_lava.yml) for the canonical shape. The Lava setup script generates this file from defaults.

## What you can configure today

| Concern | How |
|---|---|
| **Routing strategy** | per-endpoint in YAML — see [Selection policies](projects/selection-policies.md) |
| **Failover behaviour** | a mix of CLI flags and chain-spec values — see [Failover & retry](failover/index.md) |
| **Per-attempt timeout** | `--min-relay-timeout` flag, `lava-relay-timeout` header — see [Timeout](failover/timeout.md) |
| **Cache** | run the standalone cache server alongside, point the router at it with `--cache-be host:port` |
| **Metrics** | `--metrics-listen-address` (default `:7779`); scrape with Prometheus |
| **Tracing** | standard OTel env vars (`OTEL_EXPORTER_OTLP_ENDPOINT`, etc.) |

Server-side concerns like inbound auth, CORS, and rate limiting are not configurable in v1 of Smart Router — put a reverse proxy (NGINX, your cloud LB) in front to handle them. Future releases will surface these as YAML knobs.

## Chain specs

Chain specs live in [`specs/`](https://github.com/Magma-Devs/smart-router/tree/main/specs). Smart Router ships with two ready-to-use chain specs and three reusable building blocks for the Cosmos ecosystem — see [Supported chains](../reference/chains/index.md).

Pass the spec directory at startup with `--use-static-spec specs/`.

## Secrets

Sample configs are templates. Use environment variables for upstream API keys and any other sensitive values. Never commit a config with real secrets.
