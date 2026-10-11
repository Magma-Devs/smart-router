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

## How it works

- **The signal.** Each chain + API interface keeps the time of its last *client* request: HTTP,
  REST, gRPC, and unary JSON-RPC over a websocket. The router's own health relay does not count.
  A freshly started process counts as active for one full idle window.
- **Going idle.** When no client request has arrived for `--chain-tracker-idle-after`, every
  tracker of that chain + interface skips its cycles: no upstream request, no poll-health change,
  failure backoff preserved. The first skipped cycle logs `chain idle` and sets
  `smartrouter_chain_idle` to 1.
- **Heartbeat (optional).** With `--chain-tracker-idle-poll-interval`, an idle tracker still
  polls once per that interval, so a node that dies while idle is noticed and the stored block at
  wake is never older than one interval.
- **Waking.** The client request that ends an idle period polls every endpoint of the chain once,
  in parallel (`chain awake`, then `wake burst finished` with the counts). That request — and any
  that arrive while the burst runs — waits until the first poll has recorded a fresh block, at most
  **1 second**, then is routed as usual. Past 1 s it routes as a freshly started router would,
  while the burst finishes in the background. Normal polling resumes after the wake.
- **Safe resume.** A per-endpoint block older than the staleness window (10 block times) is
  treated as *unknown* by the consistency check and the sync-gap score, not as lag. Without that,
  the first request after a long pause would reject every endpoint not yet re-polled.
- **Scores do not decay over silence.** While a chain is idle the prober withholds its QoS feed:
  with no polls every endpoint would read "not alive" and every provider would be scored
  unavailable for being unpolled. Proactive re-enable of a disabled endpoint needs a poll, so it
  happens after the wake.

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

- A websocket **subscription** (`eth_subscribe` and its pushes) is not activity: it neither keeps
  the chain awake nor wakes it. The next unary request pays one wake.
- An external monitor that sends a real RPC request more often than the idle window keeps the
  chain awake permanently.
- The first request after a long pause is routed before every endpoint has been re-polled; its
  consistency check sees only the endpoints already polled as known.

## Observing it

| Where | Reading |
| --- | --- |
| `smartrouter_chain_idle{spec,apiInterface}` | 1 idle, 0 active, absent when idle mode is off |
| `rpc_endpoint_tracker_gate_skips_total{source="idle"}` | one per skipped cycle |
| `rpc_endpoint_tracker_requests_total` | flat while idle, one per endpoint at each wake |
| `/debug/endpoint-state` | `IdleMode` (`off` / `active` / `idle`), `LastClientRelay`, `IdleSince`, `WakeInProgress` |
| `/debug/probe-loop` | `QoSFeedPausedIdle` |
| Log | `chain tracker idle mode enabled`, `chain idle`, `chain awake`, `wake burst finished`, `routing before the first poll landed` (debug) |
