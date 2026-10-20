# relay_end event

The router writes one `relay_end` line when a relay ends. It is a declared event: readers select it
by `"event":"relay_end"` and parse it by `schema`, never by message text. This document is the
contract for `schema` 1.

- **Every level.** The line is written at INFO on every enabled sink (stderr, the rolling log file,
  the debug buffer) whatever `--log-level` is: `utils.LogEvent` writes through a copy of each
  logger with its level filter off. A router at `warn` writes it too.
- **Exactly once per relay.** Every request a listener hands to the router ends in one `relay_end`,
  on every exit path, including requests refused before anything was sent upstream, cache hits,
  and subscription requests (see [Subscription requests](#subscription-requests)).
- **Redacted.** `error` and `attempts[].message` go through the router's own redactor
  (`utils.RedactSecrets`) and are capped at 512 bytes plus the `...truncated...` marker, which
  replaces the middle of a longer text.

```json
{"level":"info","event":"relay_end","schema":1,"GUID":"7003","stateful":true,"method":"eth_sendRawTransaction","api_interface":"jsonrpc","selection":"stateful","outcome":"ok","stop_reason":"Success","served_by":"node-b","status":200,"has_reply":true,"error_code":"","error":"","latency_ms":305,"is_batch":false,"tx_hash":"0x2620d49b3fa2ff073b49bce2b5f0382521afd075691e695251929cd67be4f460","attempts":[{"upstream":"node-a","outcome":"node_error","error_code":"CHAIN_NONCE_TOO_LOW","message":"nonce too low: next nonce 7, tx nonce 6","latency_ms":2,"returned_to_client":false},{"upstream":"node-b","outcome":"ok","error_code":"","message":"","latency_ms":304,"returned_to_client":true}],"time":1792323767487910421,"message":"relay_end"}
```

`protocol/rpcsmartrouter/testdata/relay_end/` holds one event per exit path, checked by
`TestRelayEnd_*`.

## Where it is written

| Request | Code path | Writes `relay_end` |
| --- | --- | --- |
| HTTP: JSON-RPC, REST, Tendermint (POST and URI), GraphQL; gRPC unary | `SendRelay` → `ParseRelay` fails | `SendRelay`, then returns |
| the same, parsed | `SendRelay` → `SendParsedRelay` | `SendParsedRelay`, from one `defer` that every return passes through |
| WebSocket frame (JSON-RPC and Tendermint), parsed, not a subscription | `ConsumerWebsocketManager` → `SendParsedRelay` | `SendParsedRelay` |
| WebSocket frame that does not parse | `ConsumerWebsocketManager` → `ParseRelay` fails | `ReportRejectedRelay` (`chainlib.RelayRejectionReporter`) |
| WebSocket subscribe, unsubscribe or unsubscribe-all frame | `ConsumerWebsocketManager` → the WebSocket subscription manager | `ReportSubscriptionRelay` (`chainlib.SubscriptionRelayReporter`), once the manager returns |
| gRPC call to a server-streaming method the spec tags as a subscription | the stream callback → the gRPC subscription manager | `ReportSubscriptionRelay`, once the stream opens or fails to |

A gRPC call on a chain with server-streaming methods is parsed first by the stream callback, which
hands anything that is not a subscription to the unary path above.

Not written for: the messages a subscription pushes to its client; the teardown of a client's
subscriptions when its socket closes or its stream ends; WebSocket frames refused by the
connection's rate limit (refused before they are parsed); requests a listener refuses before calling
the router (an HTTP 405, for one); the router's own health and probe relays.

## Fields

| Field | Type | Present | Value |
| --- | --- | --- | --- |
| `event` | string | always | `"relay_end"` |
| `schema` | number | always | `1` |
| `GUID` | string | always | The request's GUID in decimal: the value every narrative line of the request carries under `GUID`, and the `Error_GUID` its client gets in an error reply. Each WebSocket frame is a relay with a GUID of its own. `""` if the context carries none. |
| `connection` | string | WebSocket relays | The GUID of the WebSocket connection the relay came on, which the connection's own lines carry. Groups a connection's relays. |
| `stateful` | boolean | always | `true` when the parsed api's category is stateful (`chainlib.GetStateful(protocolMessage) == common.CONSISTENCY_SELECT_ALL_PROVIDERS`), whichever selection path served it (chosen, pinned or sticky); `false` otherwise, and for a request that did not parse. |
| `method` | string | always | The parsed api name with any leading `/` removed: `cosmos/tx/v1beta1/txs`, `eth_sendRawTransaction`, `cosmos.bank.v1beta1.Query/Balance`. A batch joins its methods with `&`. A method the spec does not list carries the parser's default name, `Default-` and what the client sent. It is kept where the request names the method itself: `jsonrpc`, `graphql` (a root field), `grpc` (a service method), and a `tendermintrpc` JSON-RPC body over POST or WebSocket. It is written `Default-REDACTED` where the name is the client's URL path and query, which may carry a credential: every `rest` request and a `tendermintrpc` URI request. A REST api named the root path is written `/`, which the collector leaves alone. Capped at 256 bytes plus the `...truncated...` marker. `""` only for a request that did not parse. |
| `chain` | string | always | The listener's chain, as the spec names it: `ETH1`, `COSMOSHUB`. A router serves one chain per listener, so this places a request that never reached an upstream too. |
| `api_interface` | string | always | The listener's interface: `jsonrpc`, `rest`, `tendermintrpc`, `grpc`, `graphql`. |
| `selection` | string | always | `stateless`, `stateful` or `cross_validation`; `subscription` for a subscription request, which a subscription manager serves. `""` when the relay ended before a relay processor existed: a request that did not parse, or invalid cross-validation headers. |
| `outcome` | string | always | What the client got; see [outcome](#outcome). |
| `stop_reason` | string | always | Why there was no further attempt: `Success`, `Stateful`, `CrossValidation`, `NonRetryableNodeError`, `PermanentProtocolError`, `UnlistedMethodRefused`, `MaxRetriesReached`, `BatchDisabled`, `ErrorToleranceExceeded`, `AllProvidersExhausted`, `FirstMessageFailed`, `BatchSendFailed`, `ProcessingTimeout` (the budget ran out), `CallerGone` (a WebSocket or gRPC client hung up), `CrossValidationGroupsQuiet`. `""` when the relay ended before its state machine named a reason: `selection` is `""`, the cross-validation capacity check refused the request, or a `router_error` stopped it; always `""` for a subscription request. |
| `served_by` | string | always | The upstream whose reply reached the client; `"Cached"` for a cache hit, as the provider-address reply header says. For a subscription request, the upstream its subscription lives on. `""` when the client got an error, and for a subscription request the router answered itself. |
| `status` | number | always | The HTTP status on the router's result. `0` when the router returned no result or one without a status (every attempt failed in transport); the listener then answers with its own status. A `write_unknown` is always `500`, the status the client gets. `0` for a subscription request. |
| `has_reply` | boolean | always | `true` exactly when `outcome` is `ok` or `node_error`: the client got an upstream's or the cache's reply, or, to a subscription request, the router's own answer. |
| `error_code` | string | always | The error registry name (`protocol/common/error_codes.go`) of why the relay did not end `ok`; see [error_code](#error_code). `""` on `ok`. |
| `error` | string | always | `node_error`: the node's error message. `write_unknown`: the message the client gets ("transaction status unclear: …"). Any other failure: the error the relay ended with. `""` on `ok`. |
| `latency_ms` | number | always | Milliseconds from the router taking the request to the event. A request that parses is timed from `SendParsedRelay` (the parse is not counted); one that does not, from `SendRelay`; a WebSocket frame that does not parse, from its parse. |
| `is_batch` | boolean | always | A JSON-RPC batch. A batch refused while it is parsed (too large, or not accepted by the chain) is `true` too. |
| `tx_hash` | string | on a transaction submit whose reply names the transaction, and on every `eth_sendRawTransaction` whose request carries a signed transaction | The transaction's id on its chain, written as the chain writes it: `0x` and 64 lowercase hex digits on EVM chains, base58 on Solana and Sui, 64 hex digits in the node's case on Cosmos. Read from the reply the client got. For `eth_sendRawTransaction` it is computed from the signed transaction in the request when the reply returns none, so a refused write, a `write_unknown` or a `no_answer` still names a hash to look up. Not read on a batch, or from a reply over 64 KiB. Where each chain's comes from: [Transaction fields by chain](#transaction-fields-by-chain). |
| `tx_status` | string | when the submit's reply reports the transaction's execution | `success` or `failure`: the chain's execution result, as the send reply reports it, and only that. Sui's submits report it; EVM, Solana and Cosmos submits answer before the transaction executes, so their events never carry it (a Cosmos node's acceptance is `tx_code`). An event without `tx_status` says nothing about whether the transaction executed. |
| `tx_error` | string | when `tx_status` is `failure` and the reply names the error | The chain's execution error, redacted and capped like `error`. |
| `tx_code` | number | on a write over `tendermintrpc` or `rest` whose reply names a transaction code | A Cosmos node answers a refused transaction with a normal reply, so `outcome` stays `ok`; this is the code inside it: `0` when the node took the transaction, the refusal's code otherwise. Read from `result.code` (`broadcast_tx_sync`, `broadcast_tx_async`), `result.check_tx` then `result.tx_result` or `result.deliver_tx` (`broadcast_tx_commit`), or `tx_response.code` (`POST /cosmos/tx/v1beta1/txs`); the first non-zero code wins. `broadcast_tx_async` answers before checking, so its `0` means only that the node received the transaction. Not read over `grpc`, whose reply is protobuf. |
| `tx_codespace` | string | with `tx_code`, when the reply names one | The module the code belongs to, such as `sdk`: a code means something only within its codespace. |
| `tx_log` | string | when `tx_code` is not `0` | The node's reason for the refusal, redacted and capped like `error`. |
| `skipped` | array | when the consistency check passed an upstream over | One entry each time the consistency check passed an upstream over before sending because it was too far behind the chain tip: `upstream`, `lag` (blocks behind the tip), `threshold` (the lag allowed) and `batch` (the batch it was passed over in). A skipped upstream got no request then; it is in `attempts` only if it was sent later, as a stale fallback. |
| `attempts` | array | when `stateful` is `true`, when `outcome` is not `ok`, when the relay took more than one attempt or its only attempt failed (a retry, a hedge, a recovery on another upstream), or when an upstream was skipped | One entry per upstream attempt; see [attempts](#attempts). `[]` when nothing was sent. A read its first attempt answered leaves it out. |

LogQL's `json` parser renders numbers and booleans as strings (`has_reply="false"`, `status="500"`)
and skips arrays; `| json attempts="attempts"` extracts the raw array. A field whose name is already a
stream label is renamed with an `_extracted` suffix: where the collector labels writes `stateful`,
`| json | stateful="true"` matches nothing, so select writes by that stream label or by the line
filter `|= "\"stateful\":true"`, which works either way.

`error` and `attempts[].message` are prose for a person. The same failure can read differently on
routers at different `--log-level`s, because an error built by a log formatter whose level is off
skips the formatting. Match failures on `outcome` and `error_code`, never on that text.

## Transaction fields by chain

`tx_hash`, `tx_status` and `tx_error` are read for the methods below, and only on a request that is
not a batch. A reply over 64 KiB is not read. A value whose shape is not the chain's (a hash of the
wrong length, a digest that is not base58) is not written.

| Chain family | `api_interface` | `method` | `tx_hash` from | `tx_status` and `tx_error` from |
| --- | --- | --- | --- | --- |
| EVM | `jsonrpc` | `eth_sendRawTransaction` | The reply's `result` when it is `0x` and 32 bytes of hex; otherwise computed from the signed transaction in `params[0]`, the hash the chain gives it. A transaction that does not decode, or whose signature does not verify, gives none. | Not read: the send reply does not report execution. |
| Solana | `jsonrpc` | `sendTransaction` | The reply's `result`, the transaction's signature (64 bytes in base58). Stellar's `sendTransaction`, which answers with an object, is not read. | Not read: the send reply does not report execution. |
| Sui | `jsonrpc` | `sui_executeTransactionBlock` | `result.digest`. | `result.effects.status.status` (`success`, `failure`) and `result.effects.status.error`, present when the request asked for `showEffects`. |
| Sui | `graphql` | `executeTransaction`, as the operation's only root field | `effects.transaction.digest`, else `effects.digest` (the same digest), in `data`'s one entry: the root field's name, or the client's alias for it. | `effects.status` (`SUCCESS` is `success`, `FAILURE` is `failure`) and `effects.executionError.message`. Only what the mutation selected is in the reply, and what it did not select is not written. |
| Sui | `grpc` | `sui.rpc.v2.TransactionExecutionService/ExecuteTransaction` | Not read: the reply is protobuf. | Not read. |
| Cosmos SDK | `tendermintrpc` | `broadcast_tx_sync`, `broadcast_tx_async`, `broadcast_tx_commit` | `result.hash`. | Not read; `tx_code` says whether the node took the transaction. |
| Cosmos SDK | `rest` | `cosmos/tx/v1beta1/txs` (`POST`) | `tx_response.txhash`. | Not read; see `tx_code`. |
| Cosmos SDK | `grpc` | `cosmos.tx.v1beta1.Service/BroadcastTx` | Not read: the reply is protobuf. | Not read. |
| Any other chain or method | | | Not read. | Not read. |

On Cosmos the fields are read, as `tx_code` is, only on a write the spec marks stateful: a `GET` of
`cosmos/tx/v1beta1/txs` is the transaction query by events, a read under the same path.

## outcome

| `outcome` | The client got | Produced by |
| --- | --- | --- |
| `ok` | An upstream's or the cache's reply that is not a node error | `SendParsedRelay` returns a reply and no error: the first success (stateless, stateful), the quorum winner (cross-validation), or a cache hit |
| `node_error` | A node's error reply, passed through | `SendParsedRelay` returns a node error with no error: `processNonCrossValidationResult` when nothing succeeded, or a cached node error. A node's rate-limit reply is one too. |
| `write_unknown` | "transaction status unclear", HTTP 500 | `SendParsedRelay` returns `errUnknownWriteOutcome` (`writeOutcomeIsUnknown`): a stateful relay where no upstream accepted, and one was still silent when the budget ran out or its connection died after the request went out |
| `no_answer` | An error, after at least one upstream was asked | `SendParsedRelay` returns an error with a dispatched attempt: every attempt failed in transport, timed out or was rate limited, or cross-validation found no quorum |
| `rejected_before_dispatch` | An error; no upstream was asked | `SendRelay` or the WebSocket manager: the request does not parse (unsupported shape, refused batch). `SendParsedRelay` with nothing dispatched: invalid cross-validation headers, the cross-validation capacity check, an empty provider pool, an unavailable pinned provider (the select-provider directive), every endpoint failing consistency pre-validation, or the deadline expiring before dispatch |
| `router_error` | An error, or nothing usable | The router's own machinery failed (`routerFault`): its task loop could not start, the task channel closed without a final instruction, or a relay had no request data. Also `SendParsedRelay` returning neither a reply nor an error, which includes a panic inside it |

## attempts

One entry per session the relay dispatched, in dispatch order, each paired with the result its
upstream recorded. Sessions of one batch go out together, so a stateful fan-out lists them in
arbitrary order. A result recorded without a dispatch (a cache hit) comes last. An upstream asked
twice appears twice; its results are paired with its dispatches in the order protocol errors, node
errors, successes, not by time.

| Field | Type | Value |
| --- | --- | --- |
| `upstream` | string | The provider name from the router's configuration; `"Cached"` for a cache hit. |
| `outcome` | string | See below. |
| `error_code` | string | The error registry name of the attempt's error, the value the narrative lines call `error_name`; `UNKNOWN_ERROR` when it was not classified. `""` for `ok` and for an attempt with no result. |
| `message` | string | The attempt's error: the node's message for a node error, the transport error otherwise. Redacted and capped. `""` for `ok` and for an attempt with no result. |
| `retryable` | boolean | The router's verdict on the attempt's error, the registry's `Retryable` flag that the narrative lines print as `retryable`: `true` when another upstream may succeed where this one failed. Present when `error_code` is set. |
| `batch` | number | The batch the attempt went out in, counted from 0. A write's broadcast sends every upstream in one batch; each retry or hedge is the next batch, so a relay with more than one batch was retried. A batch that was taken and then not sent, such as one the consistency check emptied, leaves its number unused. Absent on an attempt that was not sent: a cache hit. |
| `sent_ms` | number | Milliseconds from the relay's start, the origin of the event's `latency_ms`, to when the attempt was taken for sending. The attempt ended at `sent_ms` plus its `latency_ms`. Absent on an attempt that was not sent. |
| `stale_fallback` | boolean | `true` on an attempt sent although the consistency check had passed its upstream over, because no other upstream was left: its answer may be stale. Absent otherwise. |
| `latency_ms` | number | How long the attempt took. Absent when unknown: a cache hit, or an attempt with no result. |
| `returned_to_client` | boolean | `true` for the one attempt whose reply the client got. `false` on every attempt when the client got an error. |

| Attempt `outcome` | Meaning |
| --- | --- |
| `ok` | The upstream answered with a reply that is not a node error. |
| `node_error` | The upstream answered with a node error: a reverted call, a rejected transaction, a refused method. |
| `rate_limited` | The upstream refused for load: an HTTP 429, or a node error the registry marks as a rate limit. |
| `timeout` | No answer in time: a deadline or connect timeout, or no result yet when the relay's budget ran out. |
| `cancelled` | The router stopped waiting: a hedge or broadcast sibling answered first, or the caller hung up. |
| `transport_error` | Any other failure to get an answer: refused, reset, closed, DNS, TLS. |

An attempt with no result when the relay ends is `timeout` if the budget ran out and `cancelled`
otherwise. Its upstream may still act on the request: a stateful write keeps delivering for up to
5 s after the relay ends, so a transaction reported this way may still land.

## error_code

For `node_error`, the code of the node error the client got. For `write_unknown`, the first of:
`PROTOCOL_CONTEXT_DEADLINE` when the budget ran out; the code of the first transport error that may
have reached the node after the request went out (a node's reply settles its upstream, as in
`writeOutcomeIsUnknown`); the connection-level code of the relay's error.

For `no_answer`, `rejected_before_dispatch` and `router_error`, the first rule that applies:

| Rule | `error_code` |
| --- | --- |
| Cross-validation could not be staffed (`insufficient-capacity`, `insufficient-groups`) | `PROTOCOL_INSUFFICIENT_PROVIDERS` |
| Cross-validation found no quorum (`no-agreement`, `diversity-unmet`, `group-quorum-unmet`) | `PROTOCOL_HASH_CONSENSUS_ERROR` |
| A `routerFault` | `PROTOCOL_RELAY_PROCESSING_FAILED` |
| Empty provider pool (`lavasession.PairingListEmptyError`) | `PROTOCOL_NO_PROVIDERS` |
| Every endpoint disabled (`lavasession.AllProviderEndpointsDisabledError`) | `PROTOCOL_ALL_ENDPOINTS_DISABLED` |
| Pinned provider unavailable or already failed | `PROTOCOL_PROVIDER_UNAVAILABLE` |
| Batch over the configured size (`chainlib.ErrBatchRequestSizeExceeded`) | `PROTOCOL_BATCH_SIZE_EXCEEDED` |
| An unsubscribe of a subscription the client does not hold (`common.SubscriptionNotFoundError`) | `PROTOCOL_SUBSCRIPTION_NOT_FOUND` |
| A connection-level error (deadline, cancel, refused, reset, DNS, TLS, EOF) | its code, e.g. `PROTOCOL_CONTEXT_DEADLINE` |
| Otherwise, the last attempt that has a code (a cross-validation `insufficient-responses` lands here) | that attempt's `error_code` |
| Nothing applies (a request that does not parse) | `""`; `error` says why |

## Subscription requests

A WebSocket subscribe, unsubscribe or unsubscribe-all frame, and the open of a gRPC server stream,
is a relay too: it is a request a client waits on, and Substrate's submit-and-watch methods submit
a transaction through one. Its event has `selection` `subscription`, `stateful` from the spec like
any relay, and its fields from what the subscription manager did:

| The manager | `outcome` | `served_by` | `attempts` |
| --- | --- | --- | --- |
| sent the request upstream and it answered: a new subscription, the last client's unsubscribe | `ok` | that upstream | one, the request it sent |
| answered from a subscription already open upstream: a second client's identical subscribe | `ok` | the open subscription's upstream | none |
| answered itself: an unsubscribe while other clients share the subscription, an unsubscribe-all | `ok` | `""` | none |
| sent the request and it failed upstream, including a node's JSON-RPC refusal, which the router does not pass through | `no_answer` | `""` | one, with the failure |
| sent nothing: its rate limit or subscription caps, no WebSocket upstream configured, an unknown subscription id | `rejected_before_dispatch` | `""` | none |
| got an answer upstream and then failed to set the subscription up | `router_error`, `PROTOCOL_SUBSCRIPTION_INIT_FAILED` | `""` | one |
| failed to tear a client's subscriptions down for an unsubscribe-all | `router_error`, `PROTOCOL_SUBSCRIPTION_CLEANUP_FAILED` | `""` | none |

The upstream is named by provider, as on every relay; the manager picks it by node URL, which may
carry a credential. The messages a subscription pushes afterwards, and a subscription restored after
its upstream connection drops, write no event.

## Collector

The fleet's log collector rewrites any value that starts with `/` after a quote, space, `=` or `(`
as `/REDACTED`, so `method` never starts with `/`; slashes inside it survive. `error` and
`attempts[].message` may carry upstream URLs, whose paths both the router and the collector redact.

## Versioning

Adding a field leaves `schema` at 1. Removing a field, changing a field's type or meaning, or adding
a value to `outcome` or to an attempt's `outcome` bumps it.
