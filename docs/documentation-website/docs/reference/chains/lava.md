# Lava

Lava mainnet over REST, gRPC, and Tendermint RPC. Includes the full Cosmos-ecosystem method surface (Cosmos SDK, IBC, Tendermint).

## Endpoints

The default Lava setup runs three listeners — one per API interface:

| Port | Interface | Calling convention |
|---|---|---|
| `3360` | REST | `GET /cosmos/...`, `GET /lavanet/...` |
| `3361` | gRPC | gRPC services from the Cosmos and Lava proto trees |
| `3362` | Tendermint RPC | URI (`/status?...`) or JSON-RPC over POST |

Spec: [`specs/lava.json`](https://github.com/Magma-Devs/smart-router/blob/main/specs/lava.json).

## Supported method families

The Lava spec includes the Cosmos-ecosystem surface plus Lava-specific paths.

| Family | Examples |
|---|---|
| Lava-specific | `/lavanet/lava/pairing/...`, `/lavanet/lava/spec/...`, `/lavanet/lava/epochstorage/...`, `/lavanet/lava/conflict/...`, `/lavanet/lava/dualstaking/...`, `/lavanet/lava/plans/...`, `/lavanet/lava/rewards/...`, `/lavanet/lava/subscription/...` |
| Cosmos SDK | `/cosmos/auth/`, `/cosmos/bank/`, `/cosmos/staking/`, `/cosmos/gov/`, `/cosmos/distribution/`, `/cosmos/slashing/`, `/cosmos/tx/`, etc. |
| IBC | `/ibc/apps/transfer/`, `/ibc/core/channel/`, `/ibc/core/client/`, `/ibc/core/connection/` |
| Tendermint RPC | `status`, `block`, `tx`, `abci_query`, `validators`, `broadcast_tx_*`, etc. |

## Connect a client

=== "REST — curl"

    ```bash
    # Latest block (Cosmos REST)
    curl http://127.0.0.1:3360/cosmos/base/tendermint/v1beta1/blocks/latest

    # Lava providers list
    curl http://127.0.0.1:3360/lavanet/lava/pairing/providers/LAVA
    ```

=== "Tendermint RPC — curl"

    ```bash
    # URI style
    curl http://127.0.0.1:3362/status

    # JSON-RPC style
    curl -X POST http://127.0.0.1:3362 \
      -H 'Content-Type: application/json' \
      -d '{"jsonrpc":"2.0","method":"status","params":[],"id":1}'
    ```

=== "gRPC — grpcurl"

    ```bash
    grpcurl -plaintext 127.0.0.1:3361 list
    grpcurl -plaintext 127.0.0.1:3361 \
      cosmos.bank.v1beta1.Query/TotalSupply
    ```

=== "cosmjs"

    ```ts
    import { StargateClient } from '@cosmjs/stargate';
    const client = await StargateClient.connect('http://127.0.0.1:3362');
    const height = await client.getHeight();
    ```

## Other Cosmos chains

The spec catalog already covers the major Cosmos-SDK chains — Osmosis, Juno, Cosmos Hub, Celestia, Stargaze, Secret, Stride, and more (see [Supported chains](index.md)). They all build on the shared Cosmos / IBC / Tendermint method surface, so serving one is a matter of pointing your config at its spec and upstreams — no spec authoring on your side.

If a Cosmos chain you need isn't in the catalog, [request it](https://github.com/Magma-Devs/smart-router/issues) and Magma will add the spec.

## Setup

```bash
./scripts/pre_setups/init_smartrouter_lava.sh
```

Runs against PublicNode endpoints by default. Edit [`config/smartrouter_examples/smartrouter_lava.yml`](https://github.com/Magma-Devs/smart-router/blob/main/config/smartrouter_examples/smartrouter_lava.yml) to point at your own upstreams.
