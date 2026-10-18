# relay_end event

The router writes one `relay_end` line when a relay ends. It is a declared event: readers select it
by `"event":"relay_end"` and parse it by `schema`, never by message text. This document is the
contract for `schema` 1.

- **Every level.** The line is written at INFO on every enabled sink (stderr, the rolling log file,
  the debug buffer) whatever `--log-level` is: `utils.LogEvent` writes through a copy of each
  logger with its level filter off. A router at `warn` writes it too.
- **Exactly once per relay.** Every request a listener hands to the router ends in one `relay_end`,
  on every exit path, including requests refused before anything was sent upstream and cache hits.
- **Redacted.** `error` and `attempts[].message` go through the router's own redactor
  (`utils.RedactSecrets`) and are capped at 512 bytes plus the `...truncated...` marker, which
  replaces the middle of a longer text.

```json
{"level":"info","event":"relay_end","schema":1,"GUID":"7003","stateful":"1","method":"eth_sendRawTransaction","api_interface":"jsonrpc","selection":"stateful","outcome":"ok","stop_reason":"Success","served_by":"node-b","status":200,"has_reply":true,"error_code":"","error":"","latency_ms":305,"is_batch":false,"attempts":[{"upstream":"node-a","outcome":"node_error","error_code":"CHAIN_NONCE_TOO_LOW","message":"nonce too low: next nonce 7, tx nonce 6","latency_ms":2,"returned_to_client":false},{"upstream":"node-b","outcome":"ok","error_code":"","message":"","latency_ms":304,"returned_to_client":true}],"time":1792323767487910421,"message":"relay_end"}
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

A gRPC call on a chain with server-streaming methods is parsed first by the stream callback, which
writes nothing and hands anything that is not a subscription to the unary path above.

Not written for: subscribe, unsubscribe and unsubscribe-all frames over WebSocket and gRPC
server-streaming subscriptions (not relays); WebSocket frames refused by the connection's rate limit
(refused before they are parsed); requests a listener refuses before calling the router (an HTTP
405, for one); the router's own health and probe relays.

## Fields

| Field | Type | Present | Value |
| --- | --- | --- | --- |
| `event` | string | always | `"relay_end"` |
| `schema` | number | always | `1` |
| `GUID` | string | always | The request's GUID in decimal, the value every narrative line of the request carries under `GUID`. Every relay on one WebSocket connection shares the connection's GUID. `""` if the context carries none. |
| `stateful` | string | always | `"1"` when the parsed api's category is stateful (`chainlib.GetStateful(protocolMessage) == common.CONSISTENCY_SELECT_ALL_PROVIDERS`), whichever selection path served it (chosen, pinned or sticky); `"0"` otherwise, and for a request that did not parse. |
| `method` | string | always | The parsed api name with any leading `/` removed: `cosmos/tx/v1beta1/txs`, `eth_sendRawTransaction`, `cosmos.bank.v1beta1.Query/Balance`. A batch joins its methods with `&`. A method the spec does not list carries the parser's default name, `Default-` and what the client sent: kept on `jsonrpc` (a name from the body) and `grpc` (a service method), written `Default-REDACTED` on every other interface, where it is the client's URL path and query and may carry a credential; each member of a batch is judged alone. Capped at 256 bytes plus the `...truncated...` marker. `""` for a request that did not parse. |
| `api_interface` | string | always | The listener's interface: `jsonrpc`, `rest`, `tendermintrpc`, `grpc`, `graphql`. |
| `selection` | string | always | `stateless`, `stateful` or `cross_validation`. `""` when the relay ended before a relay processor existed: a request that did not parse, or invalid cross-validation headers. |
| `outcome` | string | always | What the client got; see [outcome](#outcome). |
| `stop_reason` | string | always | Why there was no further attempt: `Success`, `Stateful`, `CrossValidation`, `NonRetryableNodeError`, `PermanentProtocolError`, `UnlistedMethodRefused`, `MaxRetriesReached`, `BatchDisabled`, `ErrorToleranceExceeded`, `AllProvidersExhausted`, `FirstMessageFailed`, `BatchSendFailed`, `ProcessingTimeout` (the budget ran out), `CallerGone` (a WebSocket or gRPC client hung up), `CrossValidationGroupsQuiet`. `""` when the relay ended before its state machine named a reason: `selection` is `""`, the cross-validation capacity check refused the request, or a `router_error` stopped it. |
| `served_by` | string | always | The upstream whose reply reached the client; `"Cached"` for a cache hit, as the provider-address reply header says. `""` when the client got an error. |
| `status` | number | always | The HTTP status on the router's result. `0` when the router returned no result or one without a status (every attempt failed in transport); the listener then answers with its own status. A `write_unknown` is always `500`, the status the client gets. |
| `has_reply` | boolean | always | `true` exactly when `outcome` is `ok` or `node_error`: the client got an upstream's or the cache's reply. |
| `error_code` | string | always | The error registry name (`protocol/common/error_codes.go`) of why the relay did not end `ok`; see [error_code](#error_code). `""` on `ok`. |
| `error` | string | always | `node_error`: the node's error message. `write_unknown`: the message the client gets ("transaction status unclear: …"). Any other failure: the error the relay ended with. `""` on `ok`. |
| `latency_ms` | number | always | Milliseconds from the router taking the request to the event. A request that parses is timed from `SendParsedRelay` (the parse is not counted); one that does not, from `SendRelay`; a WebSocket frame that does not parse, from its parse. |
| `is_batch` | boolean | always | A JSON-RPC batch. A batch refused while it is parsed (too large, or not accepted by the chain) is `true` too. |
| `attempts` | array | when `stateful` is `"1"` or `outcome` is not `ok` | One entry per upstream attempt; see [attempts](#attempts). `[]` when nothing was sent. |

LogQL's `json` parser renders numbers and booleans as strings (`has_reply="false"`, `status="500"`)
and skips arrays; `| json attempts="attempts"` extracts the raw array.

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
| A connection-level error (deadline, cancel, refused, reset, DNS, TLS, EOF) | its code, e.g. `PROTOCOL_CONTEXT_DEADLINE` |
| Otherwise, the last attempt that has a code (a cross-validation `insufficient-responses` lands here) | that attempt's `error_code` |
| Nothing applies (a request that does not parse) | `""`; `error` says why |

## Collector

The fleet's log collector rewrites any value that starts with `/` after a quote, space, `=` or `(`
as `/REDACTED`, so `method` never starts with `/`; slashes inside it survive. `error` and
`attempts[].message` may carry upstream URLs, whose paths both the router and the collector redact.

## Versioning

Adding a field leaves `schema` at 1. Removing a field, changing a field's type or meaning, or adding
a value to `outcome` or to an attempt's `outcome` bumps it.
