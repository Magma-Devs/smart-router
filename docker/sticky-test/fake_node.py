#!/usr/bin/env python3
"""A minimal fake ETH JSON-RPC upstream for the cross-pod sticky-session test.

Each instance answers as a distinct node and stamps its own name into every reply
(`servedBy`), which is how the driver tells which upstream actually served a request.

Block hashes are derived from the block NUMBER alone, never from the node name, so all
instances agree on history. If they disagreed the router's fork detection would react to the
harness rather than to the behaviour under test.

Heads are identical across nodes by default. The customer's original bug involves one replica
lagging, but lag is a separate variable: a node that is behind can be scored down and stop
being selected, which would skew which upstream a session lands on and quietly weaken the
stickiness measurement. Set HEAD_OFFSET to reintroduce it deliberately.
"""
import hashlib
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NAME = os.environ.get("NODE_NAME", "node-?")
BASE_HEAD = int(os.environ.get("BASE_HEAD", "21000000"))
HEAD_OFFSET = int(os.environ.get("HEAD_OFFSET", "0"))
HEAD = BASE_HEAD - HEAD_OFFSET

_lock = threading.Lock()
_counts = {}


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
    if method == "eth_chainId":
        return "0x1"
    if method == "net_version":
        return "1"
    if method == "eth_syncing":
        return False
    if method == "eth_blockNumber":
        return hex(HEAD)
    if method == "eth_getBlockByNumber":
        tag = params[0] if params else "latest"
        number = HEAD if tag in ("latest", "pending", "safe", "finalized") else int(tag, 16)
        return block_object(number)
    if method == "eth_getBlockByHash":
        return block_object(HEAD)
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
        if self.path == "/__stats":
            with _lock:
                self._send(200, {"node": NAME, "head": HEAD, "by_method": dict(_counts)})
        elif self.path == "/__reset":
            with _lock:
                _counts.clear()
            self._send(200, {"reset": True})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        try:
            request = json.loads(raw)
        except ValueError:
            self._send(400, {"error": "bad json"})
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
