#!/usr/bin/env python3
"""
Unit tests for scripts/render_compose.py.

Run with:
    python3 -m unittest scripts/test_render_compose.py

Covers the per-interface logic in render_usage() and has_websocket_upstream(),
plus a smoke test that config/values.yml exercises every interface type
(jsonrpc, rest, grpc, tendermintrpc).
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import yaml
from render_compose import (
    has_websocket_upstream,
    render_usage,
    collect_interfaces,
    _example_payload,
    _example_rest_path,
)

VALUES_FILE = Path(__file__).resolve().parents[1] / "config/values.yml"


def _router(rid, iface, urls, network=None):
    """Helper to build a router dict with one node per url, all on the same interface."""
    return {
        "id": rid,
        "network": network or rid,
        "nodes": [
            {"name": f"node-{i}", "endpoints": [{"url": u, "interface": iface}]}
            for i, u in enumerate(urls)
        ],
    }


class TestRenderUsage(unittest.TestCase):
    # ---------- jsonrpc ----------
    def test_jsonrpc_only_emits_curl_post(self):
        r = _router("eth", "jsonrpc", ["https://eth.example.com"])
        out = render_usage([(r, ["jsonrpc"])])
        self.assertIn("eth-jsonrpc", out)
        self.assertIn("curl -X POST", out)
        self.assertIn("eth_blockNumber", out)
        self.assertNotIn("wscat", out)
        self.assertNotIn("grpcurl", out)

    def test_jsonrpc_with_wss_also_emits_wscat(self):
        r = _router("eth", "jsonrpc", ["https://eth.example.com", "wss://eth-ws.example.com"])
        out = render_usage([(r, ["jsonrpc"])])
        self.assertIn("curl -X POST", out)
        self.assertIn('wscat -c ws://127.0.0.1/ws -H "Host: eth-jsonrpc.localhost"', out)
        self.assertIn("eth_blockNumber", out)
        self.assertNotIn("grpcurl", out)

    # ---------- rest ----------
    def test_rest_lava_emits_real_chain_path(self):
        r = _router("lava-rest", "rest", ["https://lava-rest.example.com"], network="lava")
        out = render_usage([(r, ["rest"])])
        self.assertIn("lava-rest-rest", out)
        # LAVA has a known-good path that returns chain info
        self.assertIn("curl http://lava-rest-rest.localhost/lavanet/lava/spec/show_all_chains", out)
        self.assertNotIn("<chain-specific-path>", out)
        self.assertNotIn("eth_blockNumber", out)
        self.assertNotIn("wscat", out)
        self.assertNotIn("grpcurl", out)

    def test_rest_unknown_network_falls_back_to_placeholder(self):
        r = _router("weird", "rest", ["https://x"], network="unknown-chain")
        out = render_usage([(r, ["rest"])])
        self.assertIn("/<chain-specific-path>", out)

    # ---------- grpc ----------
    def test_grpc_emits_grpcurl_plaintext_with_method(self):
        r = _router("lava-grpc", "grpc", ["grpcs://lava.example.com"])
        out = render_usage([(r, ["grpc"])])
        self.assertIn("grpcurl -plaintext localhost:3000", out)
        self.assertIn("cosmos.base.tendermint.v1beta1.Service.GetLatestBlock", out)
        self.assertNotIn("curl -X POST", out)
        self.assertNotIn("wscat", out)

    def test_grpc_does_not_emit_wscat_even_if_wss_upstream(self):
        # Sanity: a misconfigured wss upstream on a grpc interface shouldn't
        # produce a wscat line — wscat is jsonrpc-only.
        r = _router("weird", "grpc", ["wss://x.example.com"])
        out = render_usage([(r, ["grpc"])])
        self.assertNotIn("wscat", out)

    # ---------- tendermintrpc ----------
    def test_tendermintrpc_emits_curl_post_with_status_method(self):
        r = _router("lava-tm", "tendermintrpc", ["https://lava-tm.example.com"])
        out = render_usage([(r, ["tendermintrpc"])])
        self.assertIn("lava-tm-tendermintrpc", out)
        self.assertIn("curl -X POST", out)
        # tendermintrpc uses different methods than ETH jsonrpc
        self.assertIn('"method":"status"', out)
        self.assertNotIn("eth_blockNumber", out)
        self.assertNotIn("grpcurl", out)

    # ---------- host port assignment ----------
    def test_host_ports_increment_per_router_index(self):
        rs = [
            _router("a", "jsonrpc", ["https://a"]),
            _router("b", "rest", ["https://b"]),
            _router("c", "grpc", ["grpcs://c"]),
        ]
        out = render_usage([(rs[0], ["jsonrpc"]), (rs[1], ["rest"]), (rs[2], ["grpc"])])
        self.assertIn("direct: localhost:3000", out)
        self.assertIn("direct: localhost:3001", out)
        self.assertIn("direct: localhost:3002", out)


class TestHasWebSocketUpstream(unittest.TestCase):
    def test_returns_true_for_wss_scheme(self):
        r = _router("x", "jsonrpc", ["wss://x.example.com"])
        self.assertTrue(has_websocket_upstream(r))

    def test_returns_true_for_ws_scheme(self):
        r = _router("x", "jsonrpc", ["ws://x.example.com"])
        self.assertTrue(has_websocket_upstream(r))

    def test_returns_false_for_https_only(self):
        r = _router("x", "jsonrpc", ["https://x.example.com"])
        self.assertFalse(has_websocket_upstream(r))

    def test_returns_false_for_grpcs(self):
        r = _router("x", "grpc", ["grpcs://x.example.com"])
        self.assertFalse(has_websocket_upstream(r))

    def test_returns_true_when_mixed_upstreams(self):
        r = {"id": "x", "nodes": [
            {"endpoints": [{"url": "https://a", "interface": "jsonrpc"}]},
            {"endpoints": [{"url": "wss://b", "interface": "jsonrpc"}]},
        ]}
        self.assertTrue(has_websocket_upstream(r))


class TestExamplePayload(unittest.TestCase):
    def test_jsonrpc_uses_eth_blockNumber(self):
        self.assertIn('"method":"eth_blockNumber"', _example_payload("jsonrpc"))

    def test_tendermintrpc_uses_status(self):
        self.assertIn('"method":"status"', _example_payload("tendermintrpc"))

    def test_unknown_interface_falls_back_to_eth_blockNumber(self):
        self.assertIn('"method":"eth_blockNumber"', _example_payload("unknown"))


class TestExampleRestPath(unittest.TestCase):
    def test_lava_returns_show_all_chains(self):
        self.assertEqual(_example_rest_path("lava"), "/lavanet/lava/spec/show_all_chains")

    def test_cosmoshub_returns_validators(self):
        self.assertEqual(_example_rest_path("cosmoshub"), "/cosmos/staking/v1beta1/validators")

    def test_case_insensitive_match(self):
        self.assertEqual(_example_rest_path("LAVA"), "/lavanet/lava/spec/show_all_chains")

    def test_unknown_network_falls_back_to_placeholder(self):
        self.assertEqual(_example_rest_path("solana"), "/<chain-specific-path>")

    def test_none_network_falls_back_to_placeholder(self):
        self.assertEqual(_example_rest_path(None), "/<chain-specific-path>")


class TestValuesFileCoversAllInterfaces(unittest.TestCase):
    """Smoke test: config/values.yml should exercise every interface type."""

    @classmethod
    def setUpClass(cls):
        with open(VALUES_FILE) as f:
            cls.values = yaml.safe_load(f)

    def test_routers_cover_all_four_interfaces(self):
        seen = set()
        for router in self.values.get("routers", []) or []:
            for iface in collect_interfaces(router):
                seen.add(iface)
        # All four interface types the smart-router supports
        for required in ("jsonrpc", "rest", "grpc", "tendermintrpc"):
            self.assertIn(required, seen, f"config/values.yml missing a router with interface={required}")


if __name__ == "__main__":
    unittest.main()
