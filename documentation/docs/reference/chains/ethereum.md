# Ethereum

Ethereum mainnet over JSON-RPC.

## Endpoint

| | |
|---|---|
| Default port | `3360` (override per `endpoints[]` entry) |
| Calling convention | `POST /` with a JSON-RPC body |
| Spec | [`specs/ethereum.json`](https://github.com/Magma-Devs/smart-router/blob/main/specs/ethereum.json) |

## Supported method families

| Family | Examples |
|---|---|
| Standard | `eth_blockNumber`, `eth_call`, `eth_getBalance`, `eth_getBlockByNumber`, `eth_getLogs`, `eth_estimateGas`, `eth_gasPrice`, `eth_feeHistory`, `eth_sendRawTransaction` |
| Tracing (archive) | `debug_traceTransaction`, `debug_traceCall`, `debug_traceBlockByNumber`, `debug_storageRangeAt`, `debug_getRawBlock`, `debug_getRawReceipts` |
| Mempool | `txpool_*` (provider-dependent) |
| Account abstraction (ERC-4337) | `eth_estimateUserOperationGas`, `eth_sendUserOperation`, etc. (provider-dependent) |
| Network / client | `web3_*`, `net_*` |

The full method list lives in the spec file linked above.

## Upstream capabilities

Some methods only work on upstreams with specific capabilities. Mark these as add-ons in your config; the router only routes matching methods to capable upstreams.

| Add-on | Required for |
|---|---|
| `archive` | `debug_*`, deep `eth_getLogs`, historical state |
| `bundler` | ERC-4337 user-operation methods |

## Connect a client

=== "viem"

    ```ts
    import { createPublicClient, http } from 'viem';
    import { mainnet } from 'viem/chains';

    const client = createPublicClient({
      chain: mainnet,
      transport: http('http://127.0.0.1:3360'),
    });

    const block = await client.getBlockNumber();
    ```

=== "ethers v6"

    ```ts
    import { JsonRpcProvider } from 'ethers';
    const provider = new JsonRpcProvider('http://127.0.0.1:3360');
    const block = await provider.getBlockNumber();
    ```

=== "web3.py"

    ```python
    from web3 import Web3
    w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:3360'))
    print(w3.eth.block_number)
    ```

=== "curl"

    ```bash
    curl -X POST http://127.0.0.1:3360 \
      -H 'Content-Type: application/json' \
      -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'
    ```

## Migrating from Alchemy / Infura / QuickNode

Swap the provider URL for your Smart Router URL. The JSON-RPC protocol is identical — your existing client code doesn't change.

If you relied on vendor-specific extensions (Alchemy enhanced APIs, QuickNode add-ons, etc.), check whether your upstream providers expose the equivalent methods. Coverage for a method is determined by the chain spec, which is maintained in the catalog — if something you need isn't covered, [request it](https://github.com/Magma-Devs/smart-router/issues).

## Setup

```bash
./scripts/pre_setups/init_smartrouter_eth.sh
```

Generates [`config/smartrouter_examples/smartrouter_eth.yml`](https://github.com/Magma-Devs/smart-router/blob/main/config/smartrouter_examples) and starts the router on port 3360.
