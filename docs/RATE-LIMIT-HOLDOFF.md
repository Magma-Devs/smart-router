# Rate-limit hold-off registry

`protocol/holdoff` is the shared answer to "an upstream said 429 — when may we ask it
again?". Every path that sends requests upstream records rate limits into one
`holdoff.Registry` and consults it before sending, instead of growing a private backoff.
Production consumers use the process-wide `holdoff.Shared`; composition roots may inject
another registry, and tests use `NewRegistry` or the jitter-free `NewRegistryWithClock`.

## Semantics

- **Tiered keying.** A 429 holds off the endpoint URL that returned it. When 2 distinct
  URLs of one provider name are held off at once, the hold-off escalates to the provider
  name — a vendor cap is account-wide, and per-URL keying alone would let the process
  keep hammering the account through its other chains. The provider-wide hold-off ends
  when its longest-held member does.
- **Retry-After is the floor.** When the upstream said how long to wait
  (`common.RetryAfterFrom` on the typed error), the hold-off is at least that long,
  bounded at 1h including jitter. Otherwise a per-URL exponential: 30s doubling per
  consecutive strike, capped at 30m before jitter. Jitter extends each hold-off by up to
  +20% and never shortens it, so a fleet held off by one vendor-wide limit does not return
  in a synchronized burst.
- **One strike per hold-off.** A strike is a hold-off that ended in another 429, not a
  429. A 429 that arrives while the URL is still held off adds no strike and leaves the
  hold-off where it is — `RecordRateLimit` returns what is left of it — unless its
  Retry-After asks for longer, which extends the hold-off to the ask, still without a
  strike. A vendor's per-second quota answers a burst with one 429 per request already in
  flight; counted one strike each, seven 429s inside one second took a 30s hold-off to
  the 30m cap (MAG-4386: QuickNode, 28 real 429s in about 2s, 31-35 min). The
  provider-name escalation is still checked on every 429.
- **Only Retry-After is read.** Vendor quota headers are not: QuickNode answers a 429 with
  `x-ratelimit-reset: 1` and no Retry-After. Under the floor rule a window shorter than
  the 30s first hold-off changes nothing; the header's unit differs by vendor
  (delta-seconds, epoch seconds, milliseconds), and the floor rule means a misread can
  only lengthen a hold-off; and the relay path never sees it, because the reply-header
  filter (MAG-3104) keeps only Retry-After and Date before the 429 is typed.
- **Any answer clears.** An upstream that answered a request — success or a genuine
  failure — is no longer refusing us for load: the URL's strikes and the provider
  escalation are dropped, and later failures reach their normal handling at the normal
  cadence. An answer recorded under a provider's name clears the URL's url-only entry too
  (the key the chain tracker and the WS / gRPC subscriptions record under), so a relay
  served by the URL lifts their hold-off as well. Strike memory becomes eligible for reclamation after 1h of quiet and is
  reclaimed lazily on the next recorded 429 anywhere in the registry. The one consumer
  that does not clear on every answer is the chain tracker: a poll clears only a hold-off
  the tracker's own 429 set — it clears only what it recorded, so its poll never drops a
  subscription's hold-off on the same URL — and it never clears the relay path's entry (see
  its row in the consumer map). A probe or re-verify that got no answer (timeout, refused or
  reset connection) clears nothing either.
- **Internal only.** The registry and the Retry-After values drive internal retry and
  scheduling decisions. Nothing here is surfaced to the customer.
- **A rate limit is safe to retry once the attempt has completed.** The upstream refused
  before executing anything, so the retry policy retries a stateful relay or a batch when
  every failure so far was a rate limit — the one exception to "never retry a
  possibly-executed write". The normal retry limits still bound it. The exception does not
  extend to a ticker hedge: that fires on a timer with a relay still in flight, and an
  in-flight attempt may already have broadcast, so a hedged stateful relay or batch stops
  exactly as before. Nor does it extend to a stateful batch that the upstream answered: a
  batch reply is one node error even when some of its elements executed
  (`--batch-node-error-on-any`), so only a refusal of the whole batch at the transport (HTTP
  429) lets a batch carrying a write be sent again.
- **A fully capped chain answers with the node's own 429.** When every attempt was refused
  for rate, the client gets the upstream's 429 and its body as is, as for any other answer a
  node gave (MAG-4481; it was a router-made 503 before). The node's Retry-After is not passed
  on (that stays internal). Next to another upstream's error status the other status wins: a
  503 beside two 429s is the 503.
- **What counts as a 429.** An HTTP 429 status on any transport, a WebSocket upgrade the
  upstream rejected with 429, a gRPC status whose text names a rate limit (or
  RESOURCE_EXHAUSTED with a retry delay), and a JSON-RPC error body with code 429 or a
  "too many requests" / "rate limit" message — the shape a gateway answers with on an open
  WebSocket connection or inside a 200, where no status reaches the transport layer
  (`common/error_classifier.go`, JSON-RPC table). On GraphQL, an error coded
  RESOURCE_EXHAUSTED inside a 200 (Sui's rate and query-cost limit), read through the request
  message's own classification on the relay path and by both head-poll fetchers alike
  (MAG-4378).

## Query APIs

- `HeldOff(provider, url)` answers the endpoint-level yes/no question.
- `ReadyAt(provider, url)` returns the later of the URL and provider-wide deadlines for
  soonest-to-expire selection. It returns the zero time for absent **and expired** state.
- `ProviderReadyAt(provider)` answers whether the provider is held as a whole and when it
  next has a recorded URL worth trying. The registry only knows URLs that have answered
  429; if a provider has additional never-limited URLs, this query can conservatively
  over-hold until the earliest recorded deadline expires.

## Consumer map

The registry never sleeps and never decides policy — each consumer defines what "held
off" means on its path. This table is the catalog; add a row when wiring a new consumer.

| Path | What "held off" means there | Records into the registry | Status |
| --- | --- | --- | --- |
| Spec re-verification (`spec_reverifier.go`) | Skip the probe, return the rate-limit error so reconciliation stays inconclusive — membership unchanged, streak untouched — without spending a request | 429 from `Validate` | live |
| Hot relay path (endpoint selection) | A rate-limited relay releases its session with no QoS sample in either direction, and selection prefers providers that are not held off; when every candidate is held off, the soonest-to-expire one stays in — the customer is never answered with a synthesized 429. A write broadcast is the exception: a stateful request goes to every enabled provider, held off or not — primaries, and backups under `--stateful-to-backup` — because the hold-off chooses which provider a request goes to and a broadcast goes to all of them. A provider still limiting answers the write with a 429, which is not the answer, so the broadcast waits for the others; one that has recovered lands it (MAG-4388). A write that reaches a single provider keeps the hold-off: a sticky write, the fleet claim a write proposes for a new sticky session, and every write under the cost strategy, which does not broadcast | 429 relay results, both shapes (typed HTTP error, 2xx-body/gRPC classification) | live |
| Recovery probe (`recovery_probe.go`) | Skip the replay while held off (verdict stays inconclusive), instead of burning replay-attempt budget on a vendor that said stop | 429 probe responses (Retry-After floor honoured) | live |
| Chain tracker / endpoint poller (`chaintracker/chain_tracker.go`, `endpointstate/endpoint_monitor.go`) | Records its own 429s under the url alone — the key the WS and gRPC subscription paths use — never under the relay path's (provider, url) key: relay selection holds a provider off while every url the registry knows for it is held, so a tracker entry there would pull the provider's healthy legs out of selection whenever its one polled leg is limited, and a backup-tier leg with no relay traffic would never be cleared. Reads both keys: the first fetch waits out an existing hold-off before each attempt, and a rate-limited attempt ends the init burst instead of retrying into the limit — the applied hold-off rides the start error as its Retry-After, and the monitor's start retry waits it out with the tracker in `retrying_start` naming the 429, never `starting` with no error; the dedicated poll is floored with the applied hold-off (Retry-After when sent, else the strike schedule) and with whatever the registry holds for the url — a relay's, a probe's or a subscription's hold-off, or the provider escalation. The idle wake burst does not poll a url that is held off: `WakePoll` sends nothing and returns `chaintracker.ErrorPollNowHeldOff` (also the typed 429, remaining wait as Retry-After), and the burst counts the url as not polled. An explicit `/debug/poll-now` still polls, as an explicit `lava-select-provider` outranks a relay hold-off. The poll floor reads the registry, so an answer recorded for the url by anyone — a relay included — lifts it. Clears its url-only entry only after its own 429 was answered; a fetch that got no answer (timeout, refused or reset connection) clears nothing | The typed 429 from the head or block-hash fetch — an HTTP 429, a refused ws handshake, or a JSON-RPC error body with code 429 / "too many requests", which `chainlib.RateLimitedReplyError` types for the body form on both fetchers — `chainlib.ChainFetcher` and the per-endpoint `endpointstate.EndpointPoller` behind every per-url tracker, including its `CustomMessage` behind the SVM (Solana / KOII) latest-slot poll via `chainlib.RateLimitedErrorBody` (MAG-4165) | live |
| WS subscriptions (`direct_ws_subscription_manager.go`) | A rate-limited connect/subscribe is held off instead of scored (no availability sample), selection skips held endpoints while something ready remains, and the pool's reconnect ladder is floored by the dial's Retry-After. Keyed per WS URL — no provider name exists on this path, so vendor-tier escalation does not apply | 429 handshakes and subscribe failures | live |
| WS / gRPC transports | Recognition first: a 429 on the WS upgrade or a corroborated gRPC rate limit produces the typed sentinel these consumers key on | handshake / metadata 429s | live |
| gRPC streaming subscriptions (`direct_grpc_subscription_manager.go`) | Selection skips held-off endpoints while something ready remains; the full tier stays when nothing is. A stream the upstream ends for rate holds its endpoint off, so the client's resubscribe goes elsewhere; a stream's first message clears it. Keyed per URL, as on the WS path | Corroborated rate limits that end a stream (`common.RateLimitFromGRPC`, retry delay read from the stream's trailer) | live |

## Metrics

The registry emits its own events, so every consumer is covered without wiring
(labels, buckets, and PromQL recipes in [METRICS.md](METRICS.md#rate-limit-hold-off)):

- `smartrouter_rate_limit_holdoffs_total{provider, event}` — `recorded` (a 429 held an
  endpoint off), `escalated` (counted on the transition to a provider-wide hold-off),
  `cleared` (an answer dropped a standing penalty).
- `smartrouter_rate_limit_holdoff_seconds{provider}` — histogram of applied hold-offs,
  showing upstream Retry-After magnitudes against the exponential default.

URL-shaped registry keys (the ws path) are reduced to scheme://host before they become a
label — node URLs can embed API keys and must never reach a Prometheus series.

## Relation to `common/retry_after.go`

`protocol/common/retry_after.go` captures — it parses `Retry-After` and attaches it to
the typed rate-limit error at every HTTP call site. `protocol/holdoff` consumes — it
turns those values into "don't ask this endpoint again yet". Capture without a consumer
is inert; consumers without capture guess. Keep both wired.
