#!/usr/bin/env python3
"""Drives the cross-pod sticky-session test and asserts the result.

Order matters. The control phases run FIRST and abort on failure, because every later
assertion is meaningless if they do not hold:

  * If the ingress does not spread requests across pods, a sticky id sees one pod, and
    pod-local stickiness — which has always worked — would make the run pass while proving
    nothing about cross-pod behaviour.
  * If selection is degenerate (one upstream serves everything anyway), "the same upstream
    every time" is not evidence of stickiness either.

Every request closes its connection. A reused connection pins to one pod through the
round-robin ingress, which is the most convincing way to get a false pass here.
"""
import argparse
import collections
import json
import sys
import urllib.request

INGRESS = "http://127.0.0.1:18080"


def rpc(method, params, sticky=None, request_id=1):
    """One JSON-RPC call. Returns (result, pod) where pod is the replica that served it."""
    body = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}).encode()
    headers = {"Content-Type": "application/json", "Connection": "close"}
    if sticky:
        headers["lava-stickiness"] = sticky
    req = urllib.request.Request(INGRESS, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=30) as response:
        payload = json.loads(response.read())
        return payload, response.headers.get("X-Router-Pod", "unknown")


def served_by(payload):
    """Which upstream answered. The fake nodes stamp their own name into every block."""
    result = payload.get("result")
    if isinstance(result, dict) and "servedBy" in result:
        return result["servedBy"]
    raise AssertionError(f"no servedBy in reply, cannot attribute it to an upstream: {payload}")


def sample(count, head, sticky=None, offset=0):
    """Fetch `count` DISTINCT recent blocks, so no request can be answered from cache.

    A repeated request would be a cache hit on the second pod and never reach an upstream,
    which would hide which upstream the router actually selected.
    """
    upstreams, pods = collections.Counter(), collections.Counter()
    for i in range(count):
        block = head - 1 - offset - i
        payload, pod = rpc("eth_getBlockByNumber", [hex(block), False], sticky=sticky, request_id=i + 1)
        upstreams[served_by(payload)] += 1
        pods[pod] += 1
    return upstreams, pods


def report(title, upstreams, pods):
    print(f"\n  {title}")
    print(f"    upstreams: {dict(upstreams)}")
    print(f"    pods:      {dict(pods)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=30)
    parser.add_argument("--sessions", type=int, default=12)
    args = parser.parse_args()

    failures = []

    head_payload, _ = rpc("eth_blockNumber", [])
    head = int(head_payload["result"], 16)
    print(f"chain head: {head}")

    # ---- Control 1: the ingress must spread across replicas -------------------------
    upstreams, pods = sample(args.requests, head, offset=0)
    report("CONTROL 1 - no sticky header", upstreams, pods)
    if len(pods) < 2:
        print("\nABORT: the ingress served every request from one replica.")
        print("       Cross-pod stickiness cannot be demonstrated against a single pod.")
        return 2
    print(f"    OK: traffic reached {len(pods)} replicas")

    # ---- Control 2: selection must not be degenerate --------------------------------
    if len(upstreams) < 2:
        print("\nABORT: one upstream served everything even without a sticky header.")
        print("       'Always the same upstream' would then be trivially true.")
        return 2
    print(f"    OK: selection used {len(upstreams)} upstreams")

    # ---- The requirement: one session id, one upstream, across replicas -------------
    upstreams, pods = sample(args.requests, head, sticky="session-fixed", offset=1000)
    report("STICKY - one session id", upstreams, pods)
    if len(pods) < 2:
        failures.append("sticky traffic hit only one replica; the run proves nothing")
    if len(upstreams) == 1:
        print(f"    PASS: all {args.requests} requests across {len(pods)} replicas -> {next(iter(upstreams))}")
    else:
        failures.append(f"one session id was served by {len(upstreams)} upstreams: {dict(upstreams)}")

    # ---- No regression: distinct sessions must still spread -------------------------
    chosen = collections.Counter()
    for index in range(args.sessions):
        upstreams, _ = sample(2, head, sticky=f"session-{index}", offset=2000 + index * 10)
        if len(upstreams) != 1:
            failures.append(f"session-{index} split across {dict(upstreams)}")
        chosen[next(iter(upstreams))] += 1
    print(f"\n  SPREAD - {args.sessions} distinct session ids")
    print(f"    upstreams: {dict(chosen)}")
    if len(chosen) < 2:
        failures.append("every session id collapsed onto one upstream; affinity must not cost load spreading")
    else:
        print(f"    PASS: spread over {len(chosen)} upstreams")

    print()
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
