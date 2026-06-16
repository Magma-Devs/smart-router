# Endpoint URL

Smart Router opens **one HTTP listener per chain × API interface**. Your client connects to that listener and speaks the chain's native protocol.

## URL format

```
http://<host>:<port><chain-specific-path>
```

| Component | What it is |
|---|---|
| `<host>:<port>` | a listener you defined in `endpoints[]` of your YAML config |
| `<chain-specific-path>` | what the chain itself expects — `/` for Ethereum JSON-RPC, `/cosmos/...` for Cosmos REST, `/status` for Tendermint RPC |

## Default ports

The example configs use these ports:

| Chain × interface | Default port |
|---|---|
| Ethereum (JSON-RPC) | `3360` |
| Lava REST | `3360` |
| Lava gRPC | `3361` |
| Lava Tendermint RPC | `3362` |

Each `endpoints[]` entry has a `network-address` field (e.g. `0.0.0.0:3360`) — change it freely.

## Pointing a client at it

=== "Ethereum — viem"

    ```ts
    import { createPublicClient, http } from 'viem';
    import { mainnet } from 'viem/chains';

    const client = createPublicClient({
      chain: mainnet,
      transport: http('http://127.0.0.1:3360'),
    });
    ```

=== "Ethereum — ethers v6"

    ```ts
    import { JsonRpcProvider } from 'ethers';
    const provider = new JsonRpcProvider('http://127.0.0.1:3360');
    ```

=== "Ethereum — web3.py"

    ```python
    from web3 import Web3
    w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:3360'))
    ```

=== "Cosmos REST — curl"

    ```bash
    curl http://127.0.0.1:3360/cosmos/base/tendermint/v1beta1/blocks/latest
    ```

=== "Cosmos gRPC — grpcurl"

    ```bash
    grpcurl -plaintext 127.0.0.1:3361 \
      cosmos.bank.v1beta1.Query/TotalSupply
    ```

=== "Cosmos — cosmjs"

    ```ts
    import { StargateClient } from '@cosmjs/stargate';
    const client = await StargateClient.connect('http://127.0.0.1:3362');
    ```

## TLS

The router listens HTTP. For TLS termination, put a reverse proxy in front (NGINX, HAProxy, or your cloud load balancer). See [Bare metal → Behind a reverse proxy](../deployment/bare-metal.md#behind-a-reverse-proxy) for an NGINX example.

## CORS, auth, rate limiting

These are configured server-side, not per-client. See [Configuration](../configuration/index.md).

## Forwarded client IP

`X-Forwarded-For` is honoured. If you put a proxy in front, set the header upstream and the router will use it for IP-based logic (rate limiting, audit logs).
