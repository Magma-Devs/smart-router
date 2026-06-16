# Supported chains

Smart Router is **chain-agnostic** — every chain it serves is described by a JSON spec. Specs declare the chain's API methods, parser rules, finality parameters, and capability flags. Pass a directory of specs at startup:

```bash
smartrouter config.yml --use-static-spec specs/
```

## Where specs live

There are two sources, served by the same loader:

| Source | What's in it | Use when |
|---|---|---|
| **Bundled** — [`specs/`](https://github.com/Magma-Devs/smart-router/tree/main/specs) in this repo | 2 production chains + 3 reusable Cosmos building blocks (the ones used by the example configs) | quickest start; what `./scripts/pre_setups/init_smartrouter_*.sh` uses |
| **Catalog** — [`Magma-Devs/lava-specs`](https://github.com/Magma-Devs/lava-specs) | 75 chains across every major ecosystem, kept up-to-date | production deployments serving any chain beyond Ethereum + Lava |

Smart Router ships with a spec fetcher ([`utils/specfetcher`](https://github.com/Magma-Devs/smart-router/tree/main/utils/specfetcher)) that pulls a chain spec directly from a GitHub URL at startup — point it at any subdirectory of `lava-specs`, no manual copy step needed.

## What ships in the catalog

The 75 specs in [`Magma-Devs/lava-specs`](https://github.com/Magma-Devs/lava-specs), grouped by ecosystem:

### EVM chains (JSON-RPC)

| Chain | Spec |
|---|---|
| [Ethereum mainnet](ethereum.md) | [`ethereum.json`](https://github.com/Magma-Devs/lava-specs/blob/main/ethereum.json) |
| Arbitrum | [`arbitrum.json`](https://github.com/Magma-Devs/lava-specs/blob/main/arbitrum.json) |
| Avalanche (C-Chain) | [`avalanche.json`](https://github.com/Magma-Devs/lava-specs/blob/main/avalanche.json) · [`avalanche_c.json`](https://github.com/Magma-Devs/lava-specs/blob/main/avalanche_c.json) |
| Avalanche (P-Chain) | [`avalanche_p.json`](https://github.com/Magma-Devs/lava-specs/blob/main/avalanche_p.json) |
| Base | [`base.json`](https://github.com/Magma-Devs/lava-specs/blob/main/base.json) |
| Berachain | [`bera.json`](https://github.com/Magma-Devs/lava-specs/blob/main/bera.json) |
| Blast | [`blast.json`](https://github.com/Magma-Devs/lava-specs/blob/main/blast.json) |
| BNB Smart Chain | [`bsc.json`](https://github.com/Magma-Devs/lava-specs/blob/main/bsc.json) |
| Canto | [`canto.json`](https://github.com/Magma-Devs/lava-specs/blob/main/canto.json) |
| Celo | [`celo.json`](https://github.com/Magma-Devs/lava-specs/blob/main/celo.json) |
| Fantom | [`fantom.json`](https://github.com/Magma-Devs/lava-specs/blob/main/fantom.json) |
| Fuse | [`fuse.json`](https://github.com/Magma-Devs/lava-specs/blob/main/fuse.json) |
| Hyperliquid | [`hyperliquid.json`](https://github.com/Magma-Devs/lava-specs/blob/main/hyperliquid.json) |
| Kakarot | [`kakarot.json`](https://github.com/Magma-Devs/lava-specs/blob/main/kakarot.json) |
| Manta Pacific | [`manta_pacific.json`](https://github.com/Magma-Devs/lava-specs/blob/main/manta_pacific.json) |
| Mantle | [`mantle.json`](https://github.com/Magma-Devs/lava-specs/blob/main/mantle.json) |
| Monad | [`monad.json`](https://github.com/Magma-Devs/lava-specs/blob/main/monad.json) |
| Optimism | [`optimism.json`](https://github.com/Magma-Devs/lava-specs/blob/main/optimism.json) |
| Polygon | [`polygon.json`](https://github.com/Magma-Devs/lava-specs/blob/main/polygon.json) |
| Scroll | [`scroll.json`](https://github.com/Magma-Devs/lava-specs/blob/main/scroll.json) |
| Sonic | [`sonic.json`](https://github.com/Magma-Devs/lava-specs/blob/main/sonic.json) |
| Worldchain | [`worldchain.json`](https://github.com/Magma-Devs/lava-specs/blob/main/worldchain.json) |
| zkSync | [`zksync.json`](https://github.com/Magma-Devs/lava-specs/blob/main/zksync.json) |
| Ethermint | [`ethermint.json`](https://github.com/Magma-Devs/lava-specs/blob/main/ethermint.json) |
| Evmos | [`evmos.json`](https://github.com/Magma-Devs/lava-specs/blob/main/evmos.json) |

### Cosmos ecosystem (REST + gRPC + Tendermint RPC)

| Chain | Spec |
|---|---|
| [Lava mainnet](lava.md) | [`lava.json`](https://github.com/Magma-Devs/lava-specs/blob/main/lava.json) · [`lava-mainnet.json`](https://github.com/Magma-Devs/lava-specs/blob/main/lava-mainnet.json) |
| Cosmos Hub | [`cosmoshub.json`](https://github.com/Magma-Devs/lava-specs/blob/main/cosmoshub.json) |
| Osmosis | [`osmosis.json`](https://github.com/Magma-Devs/lava-specs/blob/main/osmosis.json) |
| Juno | [`juno.json`](https://github.com/Magma-Devs/lava-specs/blob/main/juno.json) |
| Axelar | [`axelar.json`](https://github.com/Magma-Devs/lava-specs/blob/main/axelar.json) |
| Celestia | [`celestia.json`](https://github.com/Magma-Devs/lava-specs/blob/main/celestia.json) |
| Stargaze | [`stargaze.json`](https://github.com/Magma-Devs/lava-specs/blob/main/stargaze.json) |
| Secret Network | [`secret.json`](https://github.com/Magma-Devs/lava-specs/blob/main/secret.json) |
| Stride | [`stride.json`](https://github.com/Magma-Devs/lava-specs/blob/main/stride.json) |
| Namada | [`namada.json`](https://github.com/Magma-Devs/lava-specs/blob/main/namada.json) |
| Agoric | [`agoric.json`](https://github.com/Magma-Devs/lava-specs/blob/main/agoric.json) |
| Elys | [`elys.json`](https://github.com/Magma-Devs/lava-specs/blob/main/elys.json) |
| Side | [`side.json`](https://github.com/Magma-Devs/lava-specs/blob/main/side.json) |
| Union | [`union.json`](https://github.com/Magma-Devs/lava-specs/blob/main/union.json) |
| Injective | [`spec_add_injective.json`](https://github.com/Magma-Devs/lava-specs/blob/main/spec_add_injective.json) |

### Non-EVM L1s

| Chain | Spec |
|---|---|
| Solana | [`solana.json`](https://github.com/Magma-Devs/lava-specs/blob/main/solana.json) |
| Near | [`near.json`](https://github.com/Magma-Devs/lava-specs/blob/main/near.json) |
| Aptos | [`aptos.json`](https://github.com/Magma-Devs/lava-specs/blob/main/aptos.json) |
| Sui | [`sui.json`](https://github.com/Magma-Devs/lava-specs/blob/main/sui.json) |
| Starknet | [`starknet.json`](https://github.com/Magma-Devs/lava-specs/blob/main/starknet.json) |
| Movement | [`movement.json`](https://github.com/Magma-Devs/lava-specs/blob/main/movement.json) |
| TON | [`ton.json`](https://github.com/Magma-Devs/lava-specs/blob/main/ton.json) |
| Tron | [`tron.json`](https://github.com/Magma-Devs/lava-specs/blob/main/tron.json) |
| Cardano | [`cardano.json`](https://github.com/Magma-Devs/lava-specs/blob/main/cardano.json) |
| Polkadot Asset Hub | [`polkadot_asset_hub.json`](https://github.com/Magma-Devs/lava-specs/blob/main/polkadot_asset_hub.json) |
| Tezos | [`tezos.json`](https://github.com/Magma-Devs/lava-specs/blob/main/tezos.json) |
| Hedera | [`hedera.json`](https://github.com/Magma-Devs/lava-specs/blob/main/hedera.json) |
| Stellar | [`stellar.json`](https://github.com/Magma-Devs/lava-specs/blob/main/stellar.json) |
| Ripple (XRP) | [`ripple.json`](https://github.com/Magma-Devs/lava-specs/blob/main/ripple.json) |
| Filecoin | [`filecoin.json`](https://github.com/Magma-Devs/lava-specs/blob/main/filecoin.json) |
| Casper | [`casper.json`](https://github.com/Magma-Devs/lava-specs/blob/main/casper.json) |
| Fuel | [`fuel.json`](https://github.com/Magma-Devs/lava-specs/blob/main/fuel.json) |
| IOTA | [`iota.json`](https://github.com/Magma-Devs/lava-specs/blob/main/iota.json) |

### Bitcoin family

| Chain | Spec |
|---|---|
| Bitcoin | [`btc.json`](https://github.com/Magma-Devs/lava-specs/blob/main/btc.json) |
| Bitcoin Cash | [`bch.json`](https://github.com/Magma-Devs/lava-specs/blob/main/bch.json) |
| Dogecoin | [`doge.json`](https://github.com/Magma-Devs/lava-specs/blob/main/doge.json) |
| Litecoin | [`litecoin.json`](https://github.com/Magma-Devs/lava-specs/blob/main/litecoin.json) |

### Specialty

| Spec | Purpose |
|---|---|
| [`beacon.json`](https://github.com/Magma-Devs/lava-specs/blob/main/beacon.json) | Ethereum Beacon Chain (consensus layer) |
| [`moralis.json`](https://github.com/Magma-Devs/lava-specs/blob/main/moralis.json) | Moralis multi-chain provider passthrough |
| [`sqdsubgraph.json`](https://github.com/Magma-Devs/lava-specs/blob/main/sqdsubgraph.json) | Subsquid Subgraph endpoint |
| [`koii.json`](https://github.com/Magma-Devs/lava-specs/blob/main/koii.json) | Koii Network |

### Building blocks (composed by other specs, not served directly)

| Spec | Used by |
|---|---|
| [`cosmossdk.json`](https://github.com/Magma-Devs/lava-specs/blob/main/cosmossdk.json) | every Cosmos chain |
| [`cosmossdk_full.json`](https://github.com/Magma-Devs/lava-specs/blob/main/cosmossdk_full.json) | Cosmos chains needing extended methods |
| [`cosmossdkv45.json`](https://github.com/Magma-Devs/lava-specs/blob/main/cosmossdkv45.json) | older Cosmos SDK v0.45 chains |
| [`cosmossdkv50.json`](https://github.com/Magma-Devs/lava-specs/blob/main/cosmossdkv50.json) | newer Cosmos SDK v0.50 chains |
| [`cosmoswasm.json`](https://github.com/Magma-Devs/lava-specs/blob/main/cosmoswasm.json) | CosmWasm-enabled chains |
| [`tendermint.json`](https://github.com/Magma-Devs/lava-specs/blob/main/tendermint.json) | every Tendermint-based chain |
| [`ibc.json`](https://github.com/Magma-Devs/lava-specs/blob/main/ibc.json) | every IBC-capable chain |

## Need a chain that isn't listed?

The spec catalog is maintained by Magma — Smart Router loads specs from it. If the chain you need isn't listed, [open an issue](https://github.com/Magma-Devs/smart-router/issues) describing the chain, the API interface (JSON-RPC / REST / gRPC / Tendermint RPC), and one or two upstream providers we can use as a reference, and we'll add it to the catalog.
