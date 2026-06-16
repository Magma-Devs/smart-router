<p class="eyebrow">GETTING STARTED</p>

# Quickstart

Smart Router is a centralised RPC routing gateway. Point it at upstream RPC endpoints; it serves your traffic with QoS-based provider selection, caching, hedging, retries, and cross-validation.

<div class="grid cards card-grid" markdown>

-   ![Quickstart](assets/illustrations/quickstart.svg){.card-illustration}

    **Run your first relay**

    Install the binary, point it at an RPC, send a curl request. Three commands.

    [Get started →](#three-steps-to-a-running-router)

-   ![Using the API](assets/illustrations/api.svg){.card-illustration}

    **Integrate with your app**

    Endpoint URLs, header directives, viem / ethers / web3.py / cosmjs samples.

    [Use the API →](api/index.md)

-   ![Configure](assets/illustrations/configure.svg){.card-illustration}

    **Configure routing & failover**

    Selection strategies, retry, hedging, consensus, timeouts.

    [Configuration →](configuration/index.md)

-   ![Supported chains](assets/illustrations/chains.svg){.card-illustration}

    **Supported chains**

    Ethereum, Lava, and 75+ chains across every major ecosystem out of the box.

    [Supported chains →](reference/chains/index.md)

</div>

!!! tip "Want it managed?"
    These docs cover the **self-hosted** Smart Router. If you'd rather not operate it yourself, [talk to us](https://magmadevs.com/contact).

## Three steps to a running router

### 1. Get the binary

```bash
git clone https://github.com/Magma-Devs/smart-router.git
cd smart-router
make install-all
```

Or use Docker — see [Deployment → Docker](deployment/docker.md).

### 2. Run

```bash
./scripts/pre_setups/init_smartrouter_eth.sh
```

This generates a config from [`config/smartrouter_examples/`](https://github.com/Magma-Devs/smart-router/tree/main/config/smartrouter_examples) and starts the router on port 3360.

### 3. Make a request

```bash
curl -X POST http://127.0.0.1:3360 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'
```

## Where to next

- **Integrating with an app** → [Using the API](api/index.md)
- **Operating the router** → [Configuration](configuration/index.md), [Deployment](deployment/index.md)
- **Evaluating** → [Why Smart Router?](why.md)
