# Native Blockbook WebSocket sessions

The JSON-RPC listener serves native Blockbook frames from an existing
`direct-rpc` or `backup-direct-rpc` node URL when its scheme is `wss://` (or
`ws://` for local testing) and its addons include `blockbook`. HTTP Blockbook
URLs keep the existing `bb_*` request/response path; WebSocket URLs without
that add-on keep ordinary JSON-RPC subscription handling.

```yaml
direct-rpc:
  - chain-id: BTCT4
    api-interface: jsonrpc
    name: indexer
    node-urls:
      - url: https://rpc.example.invalid/
      - url: wss://blockbook.example.invalid/
        addons: [blockbook]
        auth-config:
          auth-headers:
            x-api-key: ${BLOCKBOOK_KEY}
```

In Helm values, use the existing router `nodes[].endpoints[]` fields: set
`interface: jsonrpc`, the native `wss://` URL and `addons: [blockbook]`.
No chart extension or separate configuration section is needed. Keep an
ordinary HTTP RPC endpoint for chain tracking and request/response relays.
Store actual URLs and credentials in encrypted configuration.

Native URLs are excluded from ordinary JSON-RPC boot verification, polling
and health probes. They are selected per chain and JSON-RPC listener, in
primary-list order followed by backup-list order, with at most eight distinct
URL/authentication candidates per listener. Archive/non-archive variants of
one URL/authentication pair produce one native candidate. Provider name
validation still applies to the complete configured primary/backup lists.

Connect to `/ws` or `/websocket` on the normal hostname and send native frames:

```json
{"id":"blocks","method":"subscribeNewBlock","params":{}}
{"id":"addresses","method":"subscribeAddresses","params":{"addresses":["<address>"]}}
```

The provider's acknowledgement is `{id,data:{subscribed:true}}`. Notifications
echo the subscription request id in `{id,data}`. They are forwarded without
inventing a JSON-RPC subscription id or transforming the provider's data. One
client owns one upstream socket: Blockbook replaces an address subscription
per connection, so clients cannot share that socket safely. Closing the client
closes the upstream and cancels pending work.

## Native methods

| Methods | Behavior |
| --- | --- |
| `subscribeNewBlock`, `subscribeAddresses`, `subscribeNewTransaction`, `subscribeFiatRates` | Native subscriptions, subject to provider support and entitlements. |
| Corresponding `unsubscribe*` methods | Native unsubscription; provider responses are preserved. A provider that supports only closing the socket may refuse them. |
| `getInfo`, `getBlockHash`, `getBlock`, `getAccountInfo`, `getAccountUtxo`, `getBalanceHistory`, `getTransaction`, `getTransactionSpecific`, `estimateFee`, fiat-rate reads and `ping` | One-shot native requests, subject to provider support. A provider refusal remains an error. |
| Wallet/admin calls, native `sendTransaction`, unknown methods and `bb_*` HTTP calls | Never forwarded by this native read/subscription path. They retain ordinary routing and its write safeguards. |

IDs are nonempty strings of at most 128 bytes and `params`, when present, is
an object. Client frames are bounded to 64 KiB, upstream frames to 2 MiB, and
the session keeps at most 1024 outstanding ids. Upstream dialing and writes
each have a ten-second deadline. Notifications use the ingress's existing
single writer, write deadline, connection limit and rate-limited input path.
Upstream pings run every thirty seconds and a missing pong ends the session
within ninety seconds, so a dead upstream does not leave a healthy-looking
downstream connection silently waiting forever.
Confirmed provider replies and pushes record method/provider usage; an
upstream error is not recorded as a successful response.

Dialing may try another configured upstream before sending any request.
After sending, an upstream failure closes the downstream with code 1013:
the client reconnects, resubscribes and backfills any gap. The router does
not silently replay a subscription and imply no notifications were lost.

## Validation

`go test -race ./protocol/rpcsmartrouter ./protocol/chainlib -run 'NativeBlockbook|WebsocketWriter|WebsocketConfig'`
checks acknowledgements, event envelopes, per-client isolation, cleanup,
unknown/write-method separation and error redaction. Real-provider verification
also needs a real new-block notification. Subscription acknowledgement alone
does not establish delivery. Testing address notifications needs a real event
affecting one of the subscribed addresses; no transaction is sent by this test.

The regular JSON-RPC health command verifies normal RPC endpoints; it does not
verify this native socket. Provider product activation can be independent of
ordinary Bitcoin RPC and of accepting a subscription. An Alchemy app can have
Bitcoin Testnet4 enabled while its separately billed UTXO add-on is inactive.
Subscription acknowledgement does not establish that indexed information
methods are available. Check each required method and real event delivery
using the account's current entitlements before considering a purchase.
