# Directives

Override Smart Router's default behaviour for a single request by setting HTTP headers. Use them when the default routing, caching, or timeout policy isn't what you want for this specific call.

## Pin to a specific provider

```
Lava-Provider-Address: <upstream-name>
```

Bypasses the QoS optimizer for this request. The named upstream serves it directly. If it fails, [failover](../configuration/failover/index.md) policies still apply against the rest of the pool.

**When to use:** debugging an upstream, sticky requests within a session, A/B comparing responses.

```bash
curl -X POST http://127.0.0.1:3360 \
  -H 'Content-Type: application/json' \
  -H 'Lava-Provider-Address: my-eth-upstream-1' \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'
```

## Force a cache refresh

```
lava-force-cache-refresh: true
```

Bypasses the cache for this request. The relay goes upstream, the response is returned, and the cache entry is refreshed.

**When to use:** known-stale entry, suspected reorg, post-deploy verification.

```bash
curl -X POST http://127.0.0.1:3360 \
  -H 'Content-Type: application/json' \
  -H 'lava-force-cache-refresh: true' \
  -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1234567",false],"id":1}'
```

## Override the per-attempt timeout

```
lava-relay-timeout: 12s
```

Sets the timeout for each upstream attempt of this request. Format: any Go duration string (`500ms`, `5s`, `1m30s`). Subject to the floor set by `--min-relay-timeout` (default 1s).

**When to use:** known-slow methods (`debug_traceTransaction` on a deep block), or known-fast methods you don't want to wait for.

## Enable debug logging for one request

```
lava-debug-relay: true
```

Emits verbose per-attempt logs for this request only — without changing the global log level.

**When to use:** investigating a specific user-visible failure without flooding logs.

## Combining directives

Headers stack:

```bash
curl -X POST http://127.0.0.1:3360 \
  -H 'Content-Type: application/json' \
  -H 'Lava-Provider-Address: my-eth-upstream-2' \
  -H 'lava-relay-timeout: 30s' \
  -H 'lava-debug-relay: true' \
  -d '{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["0x..."],"id":1}'
```

## Operator restrictions

Whether each directive is honoured can be restricted server-side. Public-facing deployments often disable `Lava-Provider-Address` to prevent clients from pinning traffic to one upstream. See [Configuration](../configuration/index.md).
