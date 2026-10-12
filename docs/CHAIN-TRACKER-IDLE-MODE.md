# Chain tracker idle mode

The router keeps one chain tracker per upstream URL, per chain and API interface. Each tracker asks
its node for the latest block every `average_block_time / --chain-tracker-poll-divisor` (half the
block time by default: 6.5 s on Ethereum, 1 s on Base, 0.2 s on Solana), so that when a client
request arrives the router already knows which node is at the head.

That polling does not depend on traffic. A router that serves nothing still sends the full poll
volume to every node — on a metered node the customer pays for answers nobody reads. The relay
traffic gate only helps a busy endpoint (served relays refresh its tip, so its poll is skipped);
an endpoint with no traffic polls every cycle. Idle mode covers that case.

Idle mode is **off by default**.

**Before turning it on for a tenant:** an idle chain's `smartrouter_latest_block` stands still on
purpose, and the `sr-tip-frozen` alert (critical, pages the phone) fires when that gauge has not
moved for its window (15 min on most chains). Do not enable idle mode on a tenant until
[smart-router-infrastructure#844](https://github.com/Magma-Devs/smart-router-infrastructure/pull/844)
is merged; it skips routers whose every pod is idle (`smartrouter_chain_idle`) and adds a
"Chain Idle" panel to the fleet war room.

## How it works

- **The signal.** Each chain + API interface keeps the time of its last *client* request: HTTP,
  REST, gRPC, and unary JSON-RPC over a websocket. The router's own health relay does not count.
  A freshly started process counts as active for one full idle window.
- **Going idle.** When no client request has arrived for `--chain-tracker-idle-after`, every
  tracker of that chain + interface skips its cycles: no upstream request, no poll-health change,
  failure backoff preserved. The first skipped cycle logs `chain idle`;
  `smartrouter_chain_idle` reads 1 from the next 5 s probe cycle.
- **Heartbeat (optional).** With `--chain-tracker-idle-poll-interval`, an idle tracker still
  polls once per that interval, so a node that dies while idle is noticed and the stored block at
  wake is never older than one interval.
- **Waking.** The client request that ends an idle period polls every endpoint of the chain once,
  in parallel (`chain awake`, then `wake burst finished` with the counts). That request — and any
  that arrive while the burst runs — waits until the first poll has recorded a fresh block, at most
  **1 second**, then is routed as usual. Past 1 s it routes as a freshly started router would,
  while the burst finishes in the background. Normal polling resumes after the wake.
- **Resume.** Routing at the wake works as it always does: the consistency check compares each
  endpoint's last polled block with the chain tip. An endpoint the burst has already re-polled is
  current; one whose wake poll has not landed yet, or failed, still holds its pre-idle block and
  is treated as lagging, exactly as with idle mode off.
- **Scores do not decay over silence.** While a chain is idle, and until its wake burst finishes,
  the prober withholds its QoS feed: with no polls every endpoint would read "not alive" and every
  provider would be scored unavailable for being unpolled.

## Settings

Both are process-wide flags and config-file keys (same name, no `--`). The state they produce is
per chain + API interface.

| Flag | Default | Effect |
| --- | --- | --- |
| `--chain-tracker-idle-after` | `0` (off) | Quiet time after which polls pause. Minimum `10s`; a smaller non-zero value logs a warning and leaves idle mode off. |
| `--chain-tracker-idle-poll-interval` | `0` | While idle, poll each endpoint once per this interval. `0` polls nothing. A value faster than the normal cadence is raised to the cadence. No effect without `--chain-tracker-idle-after`. |

Choosing `--chain-tracker-idle-after`: longer than the customer's normal gap between requests.
A window shorter than that gap makes every request a wake (one poll per endpoint, plus up to one
round trip of extra latency). `5m` is a reasonable starting point.

Fixed values: the wake wait cap is 1 s; the minimum idle window is 10 s.

Related settings: `--relays-health-interval` (default 5m) keeps running while idle and sends one
request to one endpoint per interval without waking the chain; `--epoch-duration` re-verification
(one small request per endpoint per epoch) also continues; with `--enable-fork-detection` a wake
also fetches block hashes, so it costs more per endpoint.

## What it costs and saves (measured locally)

| | Without idle mode | Idle mode |
| --- | --- | --- |
| Ethereum, tracker requests per node over 180 s quiet | 25–28 | 0 (3 with a 60 s heartbeat) |
| Solana, tracker requests per node per minute quiet | 92 | 0 |
| First request after a quiet period | 0.12 s (Ethereum) / 0.30 s (Solana) | 0.22 s / 0.55 s |
| First request when every node takes 2.5 s | 2.5 s | 3.5 s (the full cap) |

## Limits

- While a chain is idle, `smartrouter_latest_block` does not move (no polls), and a provider that
  dies is not noticed until the next heartbeat or wake. The frozen-tip alert is skipped for idle
  routers (see the note at the top), so a dead pool on a quiet chain pages only once traffic
  returns. A heartbeat (`--chain-tracker-idle-poll-interval`, e.g. `5m`) keeps the tip moving
  and notices a dead provider, at one poll per endpoint per interval.

- A websocket **subscription** (`eth_subscribe` and its pushes) is not activity: it neither keeps
  the chain awake nor wakes it. The next unary request pays one wake.
- An external monitor that sends a real RPC request more often than the idle window keeps the
  chain awake permanently.
- The first request after a long pause is routed once the first endpoint is re-polled (or after
  1 s); endpoints whose wake poll has not landed yet are treated as lagging for that request.
- A **disabled endpoint does not recover while idle.** Re-enabling it needs fresh polls, and idle
  mode skips them. It recovers at the next epoch boundary (every endpoint is re-enabled each
  epoch, 15 min by default), when a request it is lent as the last chance succeeds (MAG-4232), or
  through the normal probe re-enable after the wake.

## Observing it

| Where | Reading |
| --- | --- |
| `smartrouter_chain_idle{spec,apiInterface}` | 1 idle, 0 active, absent when idle mode is off. Published every 5 s (probe cycle), so it is at most one cycle behind. `sr-tip-frozen` skips a router whose every pod reads 1; the fleet war room's "Chain Idle" panel shows it |
| `rpc_endpoint_tracker_gate_skips_total{source="idle"}` | one per skipped cycle |
| `rpc_endpoint_tracker_requests_total` | flat while idle, one per endpoint at each wake |
| `/debug/endpoint-state` | `IdleMode` (`off` / `active` / `idle`), `LastClientRelay`, `IdleSince`, `WakeInProgress` |
| `/debug/probe-loop` | `QoSFeedPausedIdle` |
| Log | `chain tracker idle mode enabled`, `chain idle`, `chain awake`, `wake burst finished`, `routing before the first poll landed` (debug) |
