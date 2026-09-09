#!/usr/bin/env python3
"""Production-readiness scenarios for cross-pod sticky sessions.

Runs against an already-running falsegap stack. Each scenario is one named check with a
pass/fail line, so a QA engineer can read the output without knowing the internals.

Every request closes its connection: a reused connection pins to one replica through the
round-robin ingress, which would make several of these pass for the wrong reason.
"""
import argparse
import collections
import concurrent.futures
import json
import sys
import time
import urllib.error
import urllib.request

INGRESS = "http://127.0.0.1:18081"
PRIMARIES = ["upstream-primary-0", "upstream-primary-1", "upstream-primary-2"]


def rpc(method, params, sticky=None, select=None, request_id=1, timeout=30):
    body = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}).encode()
    headers = {"Content-Type": "application/json", "Connection": "close"}
    if sticky:
        headers["lava-stickiness"] = sticky
    if select:
        headers["lava-select-provider"] = select
    req = urllib.request.Request(INGRESS, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read()), response.headers.get("X-Router-Pod", "?")


def head(sticky=None):
    """The head call must carry the session id too — see s_id_required_on_every_call."""
    payload, _ = rpc("eth_blockNumber", [], sticky=sticky)
    return int(payload["result"], 16)


def fetch_block(number, sticky=None, select=None, request_id=1):
    payload, pod = rpc("eth_getBlockByNumber", [hex(number), False], sticky=sticky, select=select, request_id=request_id)
    result = payload.get("result")
    return (result or {}).get("servedBy"), pod, result is None


def ingestion_round(index, sticky=None, pin_head=True):
    """The customer's own sequence: ask for the head, then ask for that block.

    pin_head=False reproduces the half-pinned mistake: the fetch carries the session id but
    the head call does not, so the head can come from an upstream the fetch never goes to.
    """
    h = head(sticky=sticky if pin_head else None)
    served, pod, gap = fetch_block(h, sticky=sticky, request_id=index)
    return served, pod, gap


def report(name, ok, detail):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    for line in detail:
        print(f"         {line}")
    return ok


# --------------------------------------------------------------------------------------

def s_customer_sequence(rounds=12, block_time=1.0):
    """The reported bug: head from one node, block from another that is behind."""
    gaps, pods, servers = 0, set(), collections.Counter()
    for i in range(rounds):
        if i:
            time.sleep(block_time)
        served, pod, gap = ingestion_round(i, sticky=f"ingest-{i}")
        gaps += gap
        pods.add(pod)
        if served:
            servers[served] += 1
    return report("Customer sequence across replicas", gaps == 0 and len(pods) > 1, [
        f"{rounds} head-then-fetch rounds, one session id each",
        f"replicas that served: {len(pods)}",
        f"upstreams used: {dict(servers)}",
        f"false gaps: {gaps} (must be 0)",
    ])


def s_concurrency(parallel=24):
    """Many requests for ONE session id at once must not split across upstreams."""
    h = head()
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as pool:
        futures = [pool.submit(fetch_block, h - 1 - i, "concurrent-session", None, i) for i in range(parallel)]
        results = [f.result() for f in futures]
    servers = collections.Counter(r[0] for r in results if r[0])
    pods = {r[1] for r in results}
    return report("Concurrent requests on one session id", len(servers) == 1, [
        f"{parallel} simultaneous requests, same session id",
        f"replicas that served: {len(pods)}",
        f"upstreams used: {dict(servers)} (must be exactly 1)",
    ])


def s_select_provider_wins():
    """lava-select-provider is the more specific ask and must beat a sticky claim."""
    h = head()
    # Establish a claim first.
    pinned, _, _ = fetch_block(h - 100, sticky="both-headers", request_id=1)
    other = next(p for p in PRIMARIES if p != pinned)
    served, _, _ = fetch_block(h - 101, sticky="both-headers", select=other, request_id=2)
    return report("lava-select-provider beats a sticky claim", served == other, [
        f"claim resolved to: {pinned}",
        f"explicitly asked for: {other}",
        f"served by: {served} (must be the explicitly named one)",
    ])


def s_restarted_pod_adopts(sticky_id="survives-restart"):
    """A replica with an empty local table must adopt the fleet's existing claim."""
    h = head()
    before, _, _ = fetch_block(h - 200, sticky=sticky_id, request_id=1)
    servers = collections.Counter()
    for i in range(9):
        served, _, _ = fetch_block(h - 201 - i, sticky=sticky_id, request_id=i + 2)
        if served:
            servers[served] += 1
    return report("Restarted replica adopts the existing claim", set(servers) == {before}, [
        f"claim before the restart: {before}",
        f"upstreams after the restart: {dict(servers)} (must be only the original)",
    ])


def s_cache_down_fails_closed():
    """With the registry unreachable, sticky traffic fails and plain traffic does not."""
    h_ok, plain_err = None, None
    try:
        h_ok = head()  # plain traffic, no sticky header
    except Exception as exc:
        plain_err = repr(exc)

    sticky_failed, sticky_detail = False, ""
    try:
        fetch_block((h_ok or 21000000) - 300, sticky="cache-is-down", request_id=1)
        sticky_detail = "sticky request SUCCEEDED (it must fail while the registry is unreachable)"
    except urllib.error.HTTPError as exc:
        sticky_failed, sticky_detail = True, f"sticky request failed with HTTP {exc.code}, as intended"
    except Exception as exc:
        sticky_failed, sticky_detail = True, f"sticky request failed: {type(exc).__name__}"

    return report("Registry unreachable: fail closed, plain traffic unaffected",
                  sticky_failed and plain_err is None,
                  [sticky_detail,
                   f"plain (no header) request: {'served normally' if plain_err is None else 'FAILED: ' + plain_err}"])


def s_cache_recovers():
    """After the registry comes back, sticky traffic works again."""
    h = head()
    servers = collections.Counter()
    for i in range(8):
        served, _, _ = fetch_block(h - 400 - i, sticky="after-recovery", request_id=i)
        if served:
            servers[served] += 1
    return report("Registry restored: sticky sessions work again", len(servers) == 1, [
        f"upstreams used: {dict(servers)} (must be exactly 1)",
    ])


def s_id_required_on_every_call(rounds=10, block_time=1.0):
    """The session id has to be on EVERY call in the sequence, not just the fetch.

    This is a usage requirement rather than a defect, and it is easy to get wrong: if the head
    call goes out unpinned it can be answered by an upstream that is ahead, while the pinned
    fetch goes to the claimed one, which may be behind. The result is the original bug, with
    the header apparently in use. The check proves both halves.
    """
    both, fetch_only = 0, 0
    for i in range(rounds):
        if i:
            time.sleep(block_time)
        _, _, gap = ingestion_round(i, sticky=f"both-{i}", pin_head=True)
        both += gap
    for i in range(rounds):
        if i:
            time.sleep(block_time)
        _, _, gap = ingestion_round(1000 + i, sticky=f"fetchonly-{i}", pin_head=False)
        fetch_only += gap
    return report("Session id required on every call in the sequence", both == 0 and fetch_only > 0, [
        f"id on BOTH calls          -> false gaps: {both} (must be 0)",
        f"id on the FETCH call only -> false gaps: {fetch_only} (must be >0; proves the requirement is real)",
        "Customers must send the header on the head request too, not only the block fetch.",
    ])


SCENARIOS = {
    "id-on-every-call": s_id_required_on_every_call,
    "customer": s_customer_sequence,
    "concurrency": s_concurrency,
    "select-provider": s_select_provider_wins,
    "restarted-pod": s_restarted_pod_adopts,
    "cache-down": s_cache_down_fails_closed,
    "cache-recovered": s_cache_recovers,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    args = parser.parse_args()
    sys.exit(0 if SCENARIOS[args.scenario]() else 1)
