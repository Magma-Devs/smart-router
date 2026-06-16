# Using the API

Once Smart Router is running, your client points at a URL and speaks the same protocol it would speak to any other RPC endpoint — JSON-RPC for Ethereum, REST or gRPC for Cosmos, Tendermint RPC for Tendermint chains. No bespoke SDK, no proprietary protocol.

This section covers what an **integrator** needs to know:

- **[Endpoint URL](url.md)** — how to construct the URL your client connects to and how to point common client libraries at it.
- **[Directives](directives.md)** — per-request HTTP headers to override default behaviour (skip cache, pin a provider, bump a timeout, enable debug logs).

For the list of supported chains and methods, see [Reference → Supported chains](../reference/chains/index.md). For server-side configuration, see [Configuration](../configuration/index.md).
