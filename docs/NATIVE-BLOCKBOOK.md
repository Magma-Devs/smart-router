# Native Blockbook WebSocket sessions

The JSON-RPC listener serves native Blockbook frames when its chain has an
explicit `blockbook-websocket` configuration. Ordinary HTTP and JSON-RPC
WebSocket calls keep the existing relay path. This protocol surface is opt-in;
declaring the HTTP `blockbook` add-on alone does not enable it.

```yaml
blockbook-websocket:
  - chain-id: BTCT4
    name: indexer
    node-url:
      url: wss://blockbook.example.invalid/
      auth-config:
        auth-headers:
          x-api-key: ${BLOCKBOOK_KEY}
```

Load the configuration together with the chain's usual `endpoints`,
`direct-rpc` and `--use-static-spec` settings. Store actual URLs and credentials
in encrypted configuration. The list has at most eight named upstreams; entries
for other chains and non-JSON-RPC listeners do not participate.

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
ordinary Bitcoin RPC and of accepting a subscription. For example, an Alchemy
UTXO REST request returning `UTXO requests are not allowed` requires checking
the app's Bitcoin/UTXO endpoint toggle. Purchasing a different plan is not a
substitute for verifying the supported method and the app's activation.
