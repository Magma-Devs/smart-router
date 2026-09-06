#!/usr/bin/env python3
"""A minimal fake ETH JSON-RPC upstream for the cross-pod sticky-session test.

Each instance answers as a distinct node and stamps its own name into every reply
(`servedBy`), which is how the driver tells which upstream actually served a request.

Block hashes are derived from the block NUMBER alone, never from the node name, so all
instances agree on history. If they disagreed the router's fork detection would react to the
harness rather than to the behaviour under test.

Heads are identical across nodes by default, which is what the plain stickiness test wants: a
node that is behind can be scored down and stop being selected, and that would skew which
upstream a session lands on. HEAD_OFFSET puts a node deliberately behind, which is how the
customer use-case test recreates the real failure.

A node asked for a block ABOVE its own head answers `{"result": null}` with HTTP 200 — exactly
what the customer reported. That reply carries no error field, so nothing in the router treats
it as a failure.
"""
import hashlib
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NAME = os.environ.get("NODE_NAME", "node-?")
BASE_HEAD = int(os.environ.get("BASE_HEAD", "21000000"))
HEAD_OFFSET = int(os.environ.get("HEAD_OFFSET", "0"))
# Seconds per block. 0 freezes the chain, which is what the affinity test wants: a frozen
# chain keeps every node interchangeable so the only variable is which node answers.
#
# The false-gap test needs a MOVING chain, and not for realism. With a frozen chain every
# round asks for the same block, so the router answers the second round from cache and never
# contacts a node at all — including caching an empty result and replaying it. The measurement
# then describes the cache rather than provider selection.
BLOCK_TIME = float(os.environ.get("BLOCK_TIME", "0"))
_STARTED_AT = time.time()


def current_head() -> int:
    """This node's head right now, keeping its fixed distance behind the tip."""
    if BLOCK_TIME <= 0:
        return BASE_HEAD - HEAD_OFFSET
    return BASE_HEAD + int((time.time() - _STARTED_AT) / BLOCK_TIME) - HEAD_OFFSET

_lock = threading.Lock()
_counts = {}
# When set, this node answers block fetches with HTTP 500. Used to show what a transient
# failure on a PINNED node does to the guarantee (the retry deliberately drops the pin).
_failing = False


def block_hash(number: int) -> str:
    return "0x" + hashlib.sha256(str(number).encode()).hexdigest()


def block_object(number: int) -> dict:
    return {
        "number": hex(number),
        "hash": block_hash(number),
        "parentHash": block_hash(number - 1),
        "timestamp": hex(1700000000 + number * 12),
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x0",
        "miner": "0x0000000000000000000000000000000000000000",
        "transactions": [],
        "servedBy": NAME,
    }


def handle(method: str, params: list):
    head = current_head()
    if method == "eth_chainId":
        return "0x1"
    if method == "net_version":
        return "1"
    if method == "eth_syncing":
        return False
    if method == "eth_blockNumber":
        return hex(head)
    if method == "eth_getBlockByNumber":
        tag = params[0] if params else "latest"
        number = head if tag in ("latest", "pending", "safe", "finalized") else int(tag, 16)
        if number > head:
            # THE CUSTOMER'S BUG, reproduced exactly. A node that has not reached this block
            # yet does not fail — it answers HTTP 200 with a null result. There is no error
            # field, so nothing downstream treats it as a failure: no retry, no QoS penalty,
            # no metric. The caller just sees an empty block that really exists.
            return None
        return block_object(number)
    if method == "eth_getBlockByHash":
        return block_object(head)
    return {"servedBy": NAME, "method": method}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _send(self, code: int, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        global _failing
        if self.path == "/__stats":
            with _lock:
                self._send(200, {"node": NAME, "head": current_head(), "failing": _failing, "by_method": dict(_counts)})
        elif self.path == "/__reset":
            with _lock:
                _counts.clear()
            self._send(200, {"reset": True})
        elif self.path.startswith("/__fail"):
            _failing = self.path.endswith("=on")
            self._send(200, {"node": NAME, "failing": _failing})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        try:
            request = json.loads(raw)
        except ValueError:
            self._send(400, {"error": "bad json"})
            return

        if _failing:
            # A genuine upstream failure, unlike the null above. This one IS seen as an error,
            # which is what makes the router retry — and the retry drops the sticky pin.
            self._send(500, {"error": "injected failure"})
            return

        batch = isinstance(request, list)
        items = request if batch else [request]
        replies = []
        for item in items:
            method = item.get("method", "")
            # Count BEFORE answering, so a reply can never be produced without being counted.
            with _lock:
                _counts[method] = _counts.get(method, 0) + 1
            replies.append({"jsonrpc": "2.0", "id": item.get("id"), "result": handle(method, item.get("params") or [])})
        self._send(200, replies if batch else replies[0])


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8545), Handler).serve_forever()
