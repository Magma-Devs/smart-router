#!/usr/bin/env python3
"""Reproduces the customer's false-gap bug, then shows affinity fixing it.

Their ingestion service does this, over and over:

    eth_blockNumber                -> H
    eth_getBlockByNumber(H)        -> the block

If the two calls land on different nodes and the second one is behind, it answers null inside
a perfectly successful response. Nothing retries. The caller records a gap that does not exist.

Phase A runs that sequence WITHOUT affinity and must observe at least one null. That is the
load-bearing phase: if the harness cannot produce the bug, a clean run in phase B proves
nothing at all.
"""
import argparse
import collections
import json
import sys
import time
import urllib.request

INGRESS = "http://127.0.0.1:18081"

# Where each primary fake is reachable from the host, so phase D can break the exact node a
# session got pinned to.
NODE_ADMIN = {
    "chain-evm-1-erigon-lighthouse-active-0": "http://127.0.0.1:18545",
    "chain-evm-1-erigon-lighthouse-test-0": "http://127.0.0.1:18546",
    "chain-evm-1-reth-lighthouse-public-active-0": "http://127.0.0.1:18547",
}


def set_failing(node_name, on):
    base = NODE_ADMIN.get(node_name)
    if not base:
        return False
    urllib.request.urlopen(f"{base}/__fail={'on' if on else 'off'}", timeout=10).read()
    return True


def rpc(method, params, sticky=None, request_id=1):
    body = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}).encode()
    headers = {"Content-Type": "application/json", "Connection": "close"}
    if sticky:
        headers["lava-stickiness"] = sticky
    req = urllib.request.Request(INGRESS, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read()), response.headers.get("X-Router-Pod", "unknown")


def ingestion_round(index, sticky=None):
    """One head-then-fetch cycle, exactly as the customer describes it."""
    head_reply, head_pod = rpc("eth_blockNumber", [], sticky=sticky, request_id=index * 2)
    head = int(head_reply["result"], 16)

    block_reply, block_pod = rpc("eth_getBlockByNumber", [hex(head), False], sticky=sticky, request_id=index * 2 + 1)
    block = block_reply.get("result")

    return {
        "head": head,
        "gap": block is None,
        "served_by": (block or {}).get("servedBy"),
        "pods": {head_pod, block_pod},
    }


def run(rounds, block_time, sticky_prefix=None):
    gaps, heads, pods, servers = 0, collections.Counter(), set(), collections.Counter()
    for index in range(rounds):
        # A fresh session id per ingestion round, which is what we recommend to the customer:
        # affinity where it matters, without pinning the whole service to one node for an hour.
        sticky = f"{sticky_prefix}-{index}" if sticky_prefix else None
        if index:
            # One round per block. Without this several rounds fall inside the same block,
            # ask for the same one, and are answered from cache instead of by a node — which
            # would measure the cache rather than which upstream was selected.
            time.sleep(block_time)
        result = ingestion_round(index, sticky=sticky)
        gaps += result["gap"]
        heads[result["head"]] += 1
        pods |= result["pods"]
        if result["served_by"]:
            servers[result["served_by"]] += 1
    return gaps, heads, pods, servers


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--block-time", type=float, default=1.0)
    args = parser.parse_args()
    failures = []

    print(f"running {args.rounds} ingestion rounds per phase, one per {args.block_time}s block\n")

    gaps, heads, pods, servers = run(args.rounds, args.block_time)
    print("  PHASE A - no affinity (reproduce the bug)")
    print(f"    heads seen:   {dict(heads)}")
    print(f"    served by:    {dict(servers)}")
    print(f"    replicas:     {len(pods)}")
    print(f"    FALSE GAPS:   {gaps} / {args.rounds}")
    if len(pods) < 2:
        print("\nABORT: every request went to one replica; this proves nothing about cross-pod behaviour.")
        return 2
    if len(heads) < 2:
        print("\nABORT: the pool reported a single head, so it is not actually out of lockstep.")
        print("       Check HEAD_OFFSET, and whether the head is being served from cache.")
        return 2
    if gaps == 0:
        print("\nABORT: could not reproduce a false gap without affinity.")
        print("       The harness cannot detect the defect it exists to test.")
        return 2
    print(f"    OK: the bug reproduces ({gaps} empty results for blocks that exist)")

    gaps, heads, pods, servers = run(args.rounds, args.block_time, sticky_prefix="ingest")
    print("\n  PHASE B - with affinity (one session id per round)")
    print(f"    heads seen:   {dict(heads)}")
    print(f"    served by:    {dict(servers)}")
    print(f"    replicas:     {len(pods)}")
    print(f"    FALSE GAPS:   {gaps} / {args.rounds}")
    if len(pods) < 2:
        failures.append("affinity rounds hit a single replica; the run proves nothing")
    if gaps:
        failures.append(f"{gaps} false gaps survived WITH affinity - the head and the fetch are "
                        f"not landing on the same node (a cached head would do this)")
    else:
        print(f"    PASS: no false gaps across {len(pods)} replicas")

    if len(servers) > 1:
        print(f"    PASS: work still spread over {len(servers)} upstreams")
    else:
        failures.append("every round collapsed onto one upstream; affinity must not cost spreading")

    # ---- Phase D: the residual risk, reported not asserted -------------------------
    # A pin is dropped on retry (MAG-2228, unchanged by this work). So a GENUINE error on the
    # pinned node sends the retry elsewhere — possibly to a node that is behind, which answers
    # null. This documents that hole against the real stack rather than leaving it a claim in
    # a design note. It does not fail the suite: it is known, intended behaviour today.
    print("\n  PHASE D - transient failure on the pinned node (informational)")
    probe = ingestion_round(9000, sticky="pinned-then-broken")
    pinned = probe["served_by"]
    if not pinned or not set_failing(pinned, True):
        print("    skipped: could not identify or reach the pinned node")
    else:
        print(f"    session pinned to {pinned}; injecting HTTP 500 on it")
        try:
            gaps_after = 0
            for attempt in range(5):
                time.sleep(args.block_time)
                try:
                    result = ingestion_round(9001 + attempt, sticky="pinned-then-broken")
                    gaps_after += result["gap"]
                except Exception as exc:  # the request may fail outright, which is the safe outcome
                    print(f"    round {attempt}: request failed ({type(exc).__name__}) - fails closed, no false gap")
            print(f"    false gaps while the pinned node was down: {gaps_after} / 5")
            if gaps_after:
                print("    NOTE: the retry left the pinned node and landed on one that is behind.")
                print("          This is the known MAG-2228 hole, not a regression.")
            else:
                print("    no false gaps: the retry happened to land on nodes that had the block")
        finally:
            set_failing(pinned, False)

    print()
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1
    print("ALL CHECKS PASSED - the customer's bug reproduces without affinity and disappears with it")
    return 0


if __name__ == "__main__":
    sys.exit(main())
