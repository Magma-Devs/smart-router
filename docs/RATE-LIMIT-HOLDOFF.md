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
- **Any answer clears.** An upstream that answered a request — success or a genuine
  failure — is no longer refusing us for load: the URL's strikes and the provider
  escalation are dropped, and later failures reach their normal handling at the normal
  cadence. Strike memory becomes eligible for reclamation after 1h of quiet and is
  reclaimed lazily on the next recorded 429 anywhere in the registry.
- **Internal only.** The registry and the Retry-After values drive internal retry and
  scheduling decisions. Nothing here is surfaced to the customer.

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
| Hot relay path (endpoint selection) | A rate-limited relay releases its session with no QoS sample in either direction, and selection prefers providers that are not held off; when every candidate is held off, the soonest-to-expire one stays in — the customer is never answered with a synthesized 429 | 429 relay results, both shapes (typed HTTP error, 2xx-body/gRPC classification) | live |
| Recovery probe (`recovery_probe.go`) | Skip the replay while held off (verdict stays inconclusive), instead of burning replay-attempt budget on a vendor that said stop | 429 probe responses (Retry-After floor honoured) | live |
| Chain tracker / endpoint poller | The poll backoff takes the upstream's Retry-After as a floor instead of guessing with the fail-count doubling | 429 poll responses | lands with MAG-2949 |
| WS / gRPC transports | Recognition first: a 429 on the WS upgrade or a corroborated gRPC rate limit produces the typed sentinel these consumers key on | handshake / metadata 429s | lands with MAG-2949 |

## Relation to `common/retry_after.go`

`protocol/common/retry_after.go` captures — it parses `Retry-After` and attaches it to
the typed rate-limit error at every HTTP call site. `protocol/holdoff` consumes — it
turns those values into "don't ask this endpoint again yet". Capture without a consumer
is inert; consumers without capture guess. Keep both wired.
